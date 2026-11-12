#![forbid(unsafe_code)]

//! The OpenAI-compatible LLM REST surface
//! (`cmd/frontend/internal/llmapi/{httpapi,handler_models,
//! handler_models_modelid,handler_chat_completions}.go`).
//!
//! Registration is `llm := m.PathPrefix("/llm/").Subrouter()` on top of the
//! `/.api/` subrouter (`httpapi.go:325-326`), so the public paths are
//! `/.api/llm/chat/completions`, `/.api/llm/models` and
//! `/.api/llm/models/{modelId}`. These handlers are registered *without*
//! `JsonMiddleware`: `serveJSON` sets `Content-Type: application/json`, while
//! failures use plain `http.Error` (so no `cache-control` header).
//!
//! The chat handler validates the OpenAI request, transforms it into the
//! internal `CodyCompletionRequestParameters`, and forwards it over HTTP to
//! `/.api/completions/stream`, keeping one source of truth for completions.
//! Forwarding is behind [`CompletionForwarder`]; the production
//! [`ApiHandlerForwarder`] calls the same `external_api::api_response`
//! dispatcher in-process.
//!
//! Not ported: the embedded `cmd/frontend/internal/modelconfig/embedded/
//! models.json` defaults and the full site-config merge. [`LiveModelConfigSource`]
//! reads the site's `modelConfiguration.models[].modelRef` subset; when the
//! site has no models the list is empty (the baseline would also include the
//! embedded defaults).

use std::fmt;
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use http::{header, HeaderMap, HeaderValue, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Body;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tonic::body::BoxBody;

use sg_config::LiveConfig;

use crate::internal_api::text_response;

// ---------------------------------------------------------------------------
// Model configuration (`internal/modelconfig/types`)
// ---------------------------------------------------------------------------

/// `types.ModelRef`: `${ProviderID}::${APIVersionID}::${ModelID}`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelRef(pub String);

impl ModelRef {
    /// `ModelRef.ProviderID`: everything before the first `::`.
    pub fn provider_id(&self) -> &str {
        self.0
            .split_once("::")
            .map(|(provider, _)| provider)
            .unwrap_or("error-invalid-modelref")
    }
}

/// `types.Model`'s observable subset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Model {
    pub model_ref: ModelRef,
}

/// `types.ModelConfiguration`'s observable subset.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ModelConfiguration {
    pub models: Vec<Model>,
}

/// `GetModelConfigurationFunc`.
pub trait ModelConfigSource: Send + Sync {
    fn get(&self) -> Result<ModelConfiguration, String>;
}

/// The production source: the current site configuration's
/// `modelConfiguration.models` (`cmd/frontend/internal/modelconfig/siteconfig.go`
/// subset). The embedded defaults are not ported.
#[derive(Clone)]
pub struct LiveModelConfigSource {
    live: Arc<LiveConfig>,
}

impl LiveModelConfigSource {
    pub fn new(live: Arc<LiveConfig>) -> Self {
        Self { live }
    }
}

impl ModelConfigSource for LiveModelConfigSource {
    fn get(&self) -> Result<ModelConfiguration, String> {
        let unified = self.live.current();
        Ok(model_configuration_from_site(&unified.site))
    }
}

/// Read `modelConfiguration.models[].modelRef` from the raw site JSON.
pub fn model_configuration_from_site(site: &sg_config::SiteConfiguration) -> ModelConfiguration {
    let raw = site.raw();
    let models = raw
        .get("modelConfiguration")
        .and_then(|config| config.get("models"))
        .and_then(Value::as_array)
        .map(|models| {
            models
                .iter()
                .filter_map(|model| model.get("modelRef").and_then(Value::as_str))
                .map(|model_ref| Model {
                    model_ref: ModelRef(model_ref.to_owned()),
                })
                .collect()
        })
        .unwrap_or_default();
    ModelConfiguration { models }
}

// ---------------------------------------------------------------------------
// HTTP responses (`serveJSON` / `http.Error`)
// ---------------------------------------------------------------------------

/// `serveJSON` (`llmapi/httpapi.go:32-39`): `application/json` plus the
/// encoder's trailing newline.
fn serve_json(value: &Value) -> Response<BoxBody> {
    let mut body = serde_json::to_vec(value).expect("JSON value serializes");
    body.push(b'\n');
    let mut response = Response::new(tonic::body::boxed(Full::new(Bytes::from(body))));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

/// `http.Error(w, message, status)`.
fn http_error(status: StatusCode, message: &str) -> Response<BoxBody> {
    text_response(
        status,
        "text/plain; charset=utf-8",
        &format!("{message}\n"),
        true,
    )
}

fn model_json(model: &Model) -> Value {
    serde_json::json!({
        "id": model.model_ref.0,
        "object": "model",
        "created": 0,
        "owned_by": model.model_ref.provider_id(),
    })
}

/// `modelsHandler.ServeHTTP` (`handler_models.go:16-43`).
pub fn models_response(source: &dyn ModelConfigSource) -> Response<BoxBody> {
    let configuration = match source.get() {
        Ok(configuration) => configuration,
        Err(error) => {
            return http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("modelConfigSvc.Get: {error}"),
            )
        }
    };
    let data: Vec<Value> = configuration.models.iter().map(model_json).collect();
    serve_json(&serde_json::json!({ "object": "list", "data": data }))
}

/// `modelsModelIDHandler.ServeHTTP` (`handler_models_modelid.go:19-47`).
pub fn model_response(source: &dyn ModelConfigSource, model_id: &str) -> Response<BoxBody> {
    if model_id.is_empty() {
        return http_error(StatusCode::BAD_REQUEST, "modelId is required");
    }
    let configuration = match source.get() {
        Ok(configuration) => configuration,
        Err(error) => {
            return http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("modelConfigSvc.Get: {error}"),
            )
        }
    };
    for model in &configuration.models {
        if model.model_ref.0 == model_id {
            return serve_json(&model_json(model));
        }
    }
    http_error(StatusCode::NOT_FOUND, "Model not found")
}

// ---------------------------------------------------------------------------
// Chat completions
// ---------------------------------------------------------------------------

/// `modelconfig.ValidateModelRef` (`internal/modelconfig/validation.go`).
pub fn validate_model_ref(model: &str) -> Result<(), String> {
    if model.is_empty() {
        return Err("modelRef is blank".to_owned());
    }
    let parts: Vec<&str> = model.split("::").collect();
    if parts.len() != 3 {
        return Err("modelRef syntax error".to_owned());
    }
    if !resource_id_valid(parts[0]) {
        return Err("invalid ProviderID".to_owned());
    }
    if !resource_id_valid(parts[1]) {
        return Err("invalid APIVersionID".to_owned());
    }
    if !resource_id_valid(parts[2]) {
        return Err("invalid ModelID".to_owned());
    }
    Ok(())
}

/// `resourceIDRE` (`internal/modelconfig/validation.go:42`):
/// `^[a-zA-Z0-9-._~/?#[\]@!$&'()*+,;=%]+$`.
fn resource_id_valid(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(
                    byte,
                    b'-' | b'.'
                        | b'_'
                        | b'~'
                        | b'/'
                        | b'?'
                        | b'#'
                        | b'['
                        | b']'
                        | b'@'
                        | b'!'
                        | b'$'
                        | b'&'
                        | b'\''
                        | b'('
                        | b')'
                        | b'*'
                        | b'+'
                        | b','
                        | b';'
                        | b'='
                        | b'%'
                )
        })
}

/// `validateChatCompletionRequest` (`handler_chat_completions.go:95-126`).
fn validate_chat_completion_request(request: &ChatCompletionRequest) -> Result<(), &'static str> {
    if request.n.is_some_and(|n| n != 1) {
        return Err("n must be nil or 1");
    }
    if request.stream == Some(true) {
        return Err("stream is not supported");
    }
    if request.seed.is_some() {
        return Err("seed is not supported");
    }
    if request.service_tier.is_some() {
        return Err("service_tier is not supported");
    }
    if request.response_format.is_some() {
        return Err("response_format is not supported");
    }
    if request.stream_options.is_some() {
        return Err("stream_options is not supported");
    }
    if request.user.is_some() {
        return Err("user is not supported");
    }
    for message in &request.messages {
        if message.role == "system" {
            return Err("system role is not supported");
        }
    }
    Ok(())
}

/// `transformMessages` (`handler_chat_completions.go:218-241`): concatenate
/// the message's text parts and rename the `user` speaker to `human`.
fn transform_messages(messages: &[ChatCompletionRequestMessage]) -> Vec<SgMessage> {
    messages
        .iter()
        .map(|message| {
            let text = message.content.texts().into_iter().collect::<String>();
            let speaker = if message.role == "user" {
                "human".to_owned()
            } else {
                message.role.clone()
            };
            SgMessage { speaker, text }
        })
        .collect()
}

/// The internal completion request forwarded to `/.api/completions/stream`
/// (`completions.CodyCompletionRequestParameters`); the serde names mirror its
/// JSON tags.
#[derive(Debug, Serialize)]
pub struct SgCompletionRequest {
    /// `RequestedModel TaintedModelRef `json:"model,omitempty"``.
    #[serde(rename = "model", skip_serializing_if = "String::is_empty")]
    pub requested_model: String,
    pub messages: Vec<SgMessage>,
    #[serde(rename = "maxTokensToSample", skip_serializing_if = "is_zero_i64")]
    pub max_tokens_to_sample: i64,
    #[serde(skip_serializing_if = "is_zero_f32")]
    pub temperature: f32,
    #[serde(rename = "stopSequences", skip_serializing_if = "Vec::is_empty")]
    pub stop_sequences: Vec<String>,
    #[serde(rename = "topP", skip_serializing_if = "is_zero_f32")]
    pub top_p: f32,
    /// `Stream *bool `json:"stream,omitempty"``: a non-nil pointer to `false`
    /// still serializes.
    pub stream: bool,
    /// `Logprobs *uint8 `json:"logprobs"`` (no `omitempty`): `nil` -> `null`.
    pub logprobs: Option<u8>,
    /// No JSON tag on the Go field: `encoding/json` emits `"Fast": false`.
    #[serde(rename = "Fast")]
    pub fast: bool,
}

fn is_zero_i64(value: &i64) -> bool {
    *value == 0
}

fn is_zero_f32(value: &f32) -> bool {
    *value == 0.0
}

#[derive(Debug, Serialize)]
pub struct SgMessage {
    pub speaker: String,
    pub text: String,
}

impl ChatCompletionRequest {
    /// `transformToSGRequest` (`handler_chat_completions.go:128-161`).
    fn to_sg_request(&self) -> SgCompletionRequest {
        SgCompletionRequest {
            max_tokens_to_sample: self.max_tokens.unwrap_or(16),
            messages: transform_messages(&self.messages),
            requested_model: self.model.clone(),
            temperature: self.temperature.unwrap_or(0.0),
            top_p: self.top_p.unwrap_or(0.0),
            stream: false,
            logprobs: None,
            stop_sequences: self.stop.as_ref().map(Stop::list).unwrap_or_default(),
            fast: false,
        }
    }
}

/// The parsed OpenAI request (`goapi.CreateChatCompletionRequest` subset used
/// by validation and transformation).
#[derive(Clone, Debug, Default, Deserialize)]
pub struct ChatCompletionRequest {
    #[serde(default)]
    pub messages: Vec<ChatCompletionRequestMessage>,
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub max_tokens: Option<i64>,
    #[serde(default)]
    pub n: Option<i64>,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub top_p: Option<f32>,
    #[serde(default)]
    pub stop: Option<Stop>,
    #[serde(default)]
    pub stream: Option<bool>,
    #[serde(default)]
    pub seed: Option<i64>,
    #[serde(default)]
    pub service_tier: Option<String>,
    #[serde(default)]
    pub response_format: Option<Value>,
    #[serde(default)]
    pub stream_options: Option<Value>,
    #[serde(default)]
    pub user: Option<String>,
}

/// `CreateChatCompletionRequestStopProperty`: `string | string[]`.
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum Stop {
    One(String),
    Many(Vec<String>),
}

impl Stop {
    fn list(&self) -> Vec<String> {
        match self {
            Stop::One(value) => vec![value.clone()],
            Stop::Many(values) => values.clone(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ChatCompletionRequestMessage {
    #[serde(default)]
    pub role: String,
    #[serde(default)]
    pub content: ContentProperty,
}

/// `ChatCompletionRequestContentProperty`: a bare string is one text part.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(untagged)]
pub enum ContentProperty {
    Text(String),
    Parts(Vec<ContentPart>),
    #[default]
    None,
}

impl ContentProperty {
    fn texts(&self) -> Vec<&str> {
        match self {
            ContentProperty::Text(text) => vec![text.as_str()],
            ContentProperty::Parts(parts) => parts.iter().map(|part| part.text.as_str()).collect(),
            ContentProperty::None => Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ContentPart {
    #[serde(default)]
    pub text: String,
}

/// `completions.CompletionResponse`.
#[derive(Clone, Debug, Default, Deserialize)]
pub struct CompletionResponse {
    #[serde(default)]
    pub completion: String,
    #[serde(default, rename = "stopReason")]
    pub stop_reason: String,
}

/// The ID/time sources (`handler_chat_completions.go:300-323`).
pub trait ChatCompletionIds: Send + Sync {
    fn uuid(&self) -> String;
    fn now_unix(&self) -> i64;
}

/// `generateUUID()` / `time.Now().Unix()`.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemIds;

impl ChatCompletionIds for SystemIds {
    fn uuid(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }

    fn now_unix(&self) -> i64 {
        time::OffsetDateTime::now_utc().unix_timestamp()
    }
}

impl CompletionResponse {
    /// `transformToOpenAIResponse` (`handler_chat_completions.go:273-298`).
    fn to_openai_response(
        &self,
        request: &ChatCompletionRequest,
        ids: &dyn ChatCompletionIds,
    ) -> Value {
        serde_json::json!({
            "id": format!("chat-{}", ids.uuid()),
            "object": "chat.completion",
            "created": ids.now_unix(),
            "model": request.model,
            "system_fingerprint": "",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": self.completion,
                },
                "finish_reason": self.stop_reason,
            }],
            "usage": {
                "completion_tokens": 0,
                "prompt_tokens": 0,
                "total_tokens": 0,
            },
        })
    }
}

/// `chatCompletionsHandler.forwardToAPIHandler`: send the transformed body to
/// `/.api/completions/stream` and parse the internal response.
#[async_trait]
pub trait CompletionForwarder: Send + Sync {
    async fn forward(&self, headers: &HeaderMap, body: Vec<u8>)
        -> Result<CompletionResponse, String>;
}

/// Production forwarder: the same-process `/.api/completions/stream` route.
pub struct ApiHandlerForwarder {
    live: Arc<LiveConfig>,
}

impl ApiHandlerForwarder {
    pub fn new(live: Arc<LiveConfig>) -> Self {
        Self { live }
    }
}

#[async_trait]
impl CompletionForwarder for ApiHandlerForwarder {
    async fn forward(
        &self,
        headers: &HeaderMap,
        body: Vec<u8>,
    ) -> Result<CompletionResponse, String> {
        let mut builder = Request::builder().method("POST").uri(
            "/.api/completions/stream?api-version=1&client-name=openai-rest-api&client-version=6.0.0",
        );
        // `forwardToAPIHandler` copies `r.Header`; Go's request header map
        // excludes Host and Content-Length, so skip those.
        for (name, value) in headers {
            if name == header::CONTENT_LENGTH
                || name == header::HOST
                || name == header::TRANSFER_ENCODING
                || name == header::CONNECTION
            {
                continue;
            }
            if name == header::AUTHORIZATION {
                // "Bearer <token>" -> "token <token>" (the internal format).
                if let Some(token) = value.to_str().ok().and_then(|v| v.strip_prefix("Bearer ")) {
                    builder = builder.header(name.clone(), format!("token {token}"));
                    continue;
                }
            }
            builder = builder.header(name.clone(), value.clone());
        }
        let request = builder
            .body(Full::new(Bytes::from(body)))
            .map_err(|error| format!("failed to create request: {error}"))?;
        let response = crate::external_api::api_response(&request, self.live.as_ref());
        if response.status() != StatusCode::OK {
            let status = response.status();
            let body = response
                .into_body()
                .collect()
                .await
                .map(|collected| collected.to_bytes())
                .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
                .unwrap_or_default();
            return Err(format!(
                "handler returned unexpected status code: got {status} want 200, response body: {body}"
            ));
        }
        let bytes = response
            .into_body()
            .collect()
            .await
            .map_err(|error| format!("failed to read response body: {error}"))?
            .to_bytes();
        serde_json::from_slice(&bytes).map_err(|error| {
            format!(
                "failed to unmarshal response body {}: {error}",
                String::from_utf8_lossy(&bytes)
            )
        })
    }
}

/// A model source that always fails, for tests that only exercise other
/// routes.
pub struct UnavailableModelConfig;

impl ModelConfigSource for UnavailableModelConfig {
    fn get(&self) -> Result<ModelConfiguration, String> {
        Err("model configuration service is not available".to_owned())
    }
}

/// A forwarder that always fails, for tests that only exercise other routes.
pub struct UnavailableCompletionForwarder;

#[async_trait]
impl CompletionForwarder for UnavailableCompletionForwarder {
    async fn forward(
        &self,
        _headers: &HeaderMap,
        _body: Vec<u8>,
    ) -> Result<CompletionResponse, String> {
        Err("completions handler is not available".to_owned())
    }
}

/// The dependency bundle the external listener hands to the LLM routes.
#[derive(Clone)]
pub struct LlmDeps {
    pub config: Arc<dyn ModelConfigSource>,
    pub forwarder: Arc<dyn CompletionForwarder>,
    pub ids: Arc<dyn ChatCompletionIds>,
}

impl LlmDeps {
    /// A dependency-free bundle for tests of unrelated routes.
    pub fn unavailable() -> Self {
        Self {
            config: Arc::new(UnavailableModelConfig),
            forwarder: Arc::new(UnavailableCompletionForwarder),
            ids: Arc::new(SystemIds),
        }
    }

    /// Production wiring: site-config model source, in-process forwarder and
    /// the system clock/UUID.
    pub fn production(live: Arc<LiveConfig>) -> Self {
        Self {
            config: Arc::new(LiveModelConfigSource::new(Arc::clone(&live))),
            forwarder: Arc::new(ApiHandlerForwarder::new(live)),
            ids: Arc::new(SystemIds),
        }
    }
}

/// `chatCompletionsHandler.ServeHTTP` (`handler_chat_completions.go:39-81`).
pub async fn chat_completions_response<B>(req: &mut Request<B>, deps: &LlmDeps) -> Response<BoxBody>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: fmt::Display,
{
    let body = match read_request_body(req.body_mut()).await {
        Ok(body) => body,
        Err(error) => {
            return http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("io.ReadAll: {error}"),
            )
        }
    };
    let request: ChatCompletionRequest = match serde_json::from_slice(&body) {
        Ok(request) => request,
        Err(error) => {
            return http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("decoder.Decode: {error}"),
            )
        }
    };

    if let Err(message) = validate_chat_completion_request(&request) {
        return http_error(StatusCode::BAD_REQUEST, message);
    }
    if let Err(error) = validate_model_ref(&request.model) {
        return http_error(
            StatusCode::BAD_REQUEST,
            &format!(
                "requested model '{}' failed validation: {error}. Expected format '${{ProviderID}}::${{APIVersionID}}::${{ModelID}}'. To fix this problem, send a request to `GET /.api/llm/models` to see the list of supported models.",
                request.model
            ),
        );
    }

    let sg_body = serde_json::to_vec(&request.to_sg_request()).expect("request serializes");
    let response = match deps.forwarder.forward(req.headers(), sg_body).await {
        Ok(response) => response,
        Err(error) => {
            return http_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("failed to forward request to apiHandler: {error}"),
            )
        }
    };
    serve_json(&response.to_openai_response(&request, deps.ids.as_ref()))
}

/// `io.ReadAll(r.Body)`.
async fn read_request_body<B>(body: &mut B) -> Result<Bytes, String>
where
    B: Body<Data = Bytes> + Unpin,
    B::Error: fmt::Display,
{
    let mut out = BytesMut::new();
    let mut body = Pin::new(body);
    while let Some(frame) = poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
        let frame = frame.map_err(|error| error.to_string())?;
        if let Some(data) = frame.data_ref() {
            out.extend_from_slice(data);
        }
    }
    Ok(out.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    struct Fixed(Result<ModelConfiguration, String>);

    impl ModelConfigSource for Fixed {
        fn get(&self) -> Result<ModelConfiguration, String> {
            self.0.clone()
        }
    }

    struct Recording {
        response: CompletionResponse,
    }

    #[async_trait]
    impl CompletionForwarder for Recording {
        async fn forward(
            &self,
            headers: &HeaderMap,
            body: Vec<u8>,
        ) -> Result<CompletionResponse, String> {
            let sent: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(sent["maxTokensToSample"], 5);
            assert!(sent.get("logprobs").is_some_and(serde_json::Value::is_null));
            assert_eq!(sent["model"], "anthropic::1::claude");
            assert_eq!(sent["messages"][0]["speaker"], "human");
            assert_eq!(sent["messages"][0]["text"], "a b");
            assert_eq!(sent["stream"], false);
            assert_eq!(headers.get(header::AUTHORIZATION).unwrap(), "Bearer secret");
            Ok(self.response.clone())
        }
    }

    struct FixedIds;

    impl ChatCompletionIds for FixedIds {
        fn uuid(&self) -> String {
            "fixed".to_owned()
        }

        fn now_unix(&self) -> i64 {
            7
        }
    }

    fn config() -> ModelConfiguration {
        ModelConfiguration {
            models: vec![
                Model {
                    model_ref: ModelRef("anthropic::1::claude".to_owned()),
                },
                Model {
                    model_ref: ModelRef("openai::1::gpt".to_owned()),
                },
            ],
        }
    }

    async fn body_string(response: Response<BoxBody>) -> (StatusCode, String) {
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    #[tokio::test]
    async fn models_list_shape() {
        let response = models_response(&Fixed(Ok(config())));
        let (status, body) = body_string(response).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            "{\"data\":[{\"created\":0,\"id\":\"anthropic::1::claude\",\"object\":\"model\",\"owned_by\":\"anthropic\"},{\"created\":0,\"id\":\"openai::1::gpt\",\"object\":\"model\",\"owned_by\":\"openai\"}],\"object\":\"list\"}\n"
        );
    }

    #[tokio::test]
    async fn model_by_id_not_found() {
        let response = model_response(&Fixed(Ok(config())), "nope::1::x");
        let (status, body) = body_string(response).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body, "Model not found\n");
    }

    #[test]
    fn model_ref_validation_matches_baseline() {
        assert_eq!(validate_model_ref(""), Err("modelRef is blank".to_owned()));
        assert_eq!(
            validate_model_ref("anthropic/claude"),
            Err("modelRef syntax error".to_owned())
        );
        assert!(validate_model_ref("anthropic::1::claude").is_ok());
    }

    fn deps_with(response: CompletionResponse) -> LlmDeps {
        LlmDeps {
            config: Arc::new(Fixed(Ok(config()))),
            forwarder: Arc::new(Recording { response }),
            ids: Arc::new(FixedIds),
        }
    }

    #[tokio::test]
    async fn chat_completions_transforms_and_answers() {
        let deps = deps_with(CompletionResponse {
            completion: "hello".to_owned(),
            stop_reason: "stop".to_owned(),
        });
        let mut request = Request::builder()
            .method("POST")
            .uri("/.api/llm/chat/completions")
            .header(header::AUTHORIZATION, "Bearer secret")
            .body(Full::new(Bytes::from_static(
                br#"{"model":"anthropic::1::claude","max_tokens":5,"messages":[{"role":"user","content":[{"type":"text","text":"a"},{"type":"text","text":" b"}]}]}"#,
            )))
            .unwrap();
        let response = chat_completions_response(&mut request, &deps).await;
        let (status, body) = body_string(response).await;
        assert_eq!(status, StatusCode::OK);
        let value: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["id"], "chat-fixed");
        assert_eq!(value["created"], 7);
        assert_eq!(value["choices"][0]["message"]["content"], "hello");
        assert_eq!(value["choices"][0]["finish_reason"], "stop");
        assert!(body.ends_with('\n'));
    }

    #[tokio::test]
    async fn chat_completions_rejects_system_role() {
        let deps = deps_with(CompletionResponse::default());
        let mut request = Request::builder()
            .method("POST")
            .uri("/.api/llm/chat/completions")
            .body(Full::new(Bytes::from_static(
                br#"{"model":"a::1::b","messages":[{"role":"system","content":"x"}]}"#,
            )))
            .unwrap();
        let response = chat_completions_response(&mut request, &deps).await;
        let (status, body) = body_string(response).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body, "system role is not supported\n");
    }
}
