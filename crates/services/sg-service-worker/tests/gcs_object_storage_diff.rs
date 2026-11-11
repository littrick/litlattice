//! M9.8jz differential harness: GCS object storage and composition.
//!
//! The Go oracle (`internal/object/zz_m9jz_oracle_test.go`, build tag
//! `m9jz_oracle`) drives the real `gcsStore` against an httptest fake of the
//! GCS JSON API. This harness replays the same corpus against
//! [`sg_objectstorage::GcsStore`] with an injected transport that implements
//! the same fake server, then renders the identical transcript. The runner
//! `test_ws/p9-go/run_gcs_object_storage_diff.sh` diffs the two files.
//!
//! No network access: the transport is in-process.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use sg_objectstorage::gcs::{
    for_each_string, pool_limit, GcsHttpRequest, GcsHttpResponse, GcsStore, GcsTransport,
    RetryPolicy,
};
use sha2::{Digest, Sha256};

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------

#[derive(serde::Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(serde::Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase", default)]
struct Case {
    name: String,
    kind: String,
    key: String,
    payload: Option<Payload>,
    seed: Vec<Seed>,
    prefix: String,
    pages: Vec<Vec<String>>,
    sources: Vec<String>,
    source_prefix: String,
    source_count: usize,
    dest: String,
    #[serde(rename = "composeStatus")]
    compose_status: u16,
    manage_bucket: bool,
    bucket_exists: Option<bool>,
    create_status: u16,
    op: String,
    fail_status: u16,
    fail_times: i32,
    values: Vec<String>,
    fail_values: Vec<String>,
    sleep_ms: u64,
}

#[derive(serde::Deserialize, Clone, Default)]
#[serde(default)]
struct Payload {
    kind: String,
    value: String,
    byte: String,
    count: usize,
}

#[derive(serde::Deserialize, Clone)]
struct Seed {
    key: String,
    content: String,
}

fn payload_bytes(payload: Option<&Payload>) -> Vec<u8> {
    let Some(payload) = payload else {
        return Vec::new();
    };
    match payload.kind.as_str() {
        "text" => payload.value.as_bytes().to_vec(),
        "repeat" => {
            let byte = payload.byte.as_bytes().first().copied().unwrap_or(b'x');
            vec![byte; payload.count]
        }
        "base64" => {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(payload.value.as_bytes())
                .unwrap_or_default()
        }
        _ => Vec::new(),
    }
}

fn sources_for(case: &Case) -> Vec<String> {
    if !case.sources.is_empty() {
        return case.sources.clone();
    }
    if case.source_count > 0 {
        return (0..case.source_count)
            .map(|index| format!("{}{index}", case.source_prefix))
            .collect();
    }
    Vec::new()
}

// ---------------------------------------------------------------------------
// Fake GCS server, behind the injectable transport
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Recorded {
    method: String,
    path: String,
    query: Vec<(String, String)>,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Recorded {
    fn header(&self, name: &str) -> &str {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
            .unwrap_or("")
    }
}

struct FakeState {
    bucket_exists: bool,
    objects: HashMap<String, Vec<u8>>,
    pages: Vec<Vec<String>>,
    reqs: Vec<Recorded>,
    fail_status: u16,
    fail_times: i32,
    fail_kind: String,
    compose_code: u16,
    create_status: u16,
    sessions: HashMap<String, Vec<u8>>,
    session_counter: usize,
}

impl FakeState {
    fn should_fail(&mut self, kind: &str) -> bool {
        if self.fail_kind == kind && self.fail_times > 0 {
            self.fail_times -= 1;
            return true;
        }
        false
    }
}

struct FakeTransport {
    state: Mutex<FakeState>,
    endpoint: String,
}

fn json_response(status: u16, value: serde_json::Value) -> GcsHttpResponse {
    GcsHttpResponse {
        status,
        headers: vec![("Content-Type".to_owned(), "application/json".to_owned())],
        body: serde_json::to_vec(&value).unwrap_or_default(),
    }
}

fn error_response(status: u16, message: &str) -> GcsHttpResponse {
    json_response(
        status,
        serde_json::json!({ "error": { "code": status, "message": message } }),
    )
}

impl FakeTransport {
    fn record(&self, state: &mut FakeState, request: &GcsHttpRequest, path: &str) {
        let url = url::Url::parse(&request.url).expect("request url");
        state.reqs.push(Recorded {
            method: request.method.clone(),
            path: path.to_owned(),
            query: url
                .query_pairs()
                .map(|(key, value)| (key.into_owned(), value.into_owned()))
                .collect(),
            headers: request.headers.clone(),
            body: request.body.clone(),
        });
    }

    fn handle(&self, request: &GcsHttpRequest) -> GcsHttpResponse {
        let url = url::Url::parse(&request.url).expect("request url");
        let path = url.path().to_owned();
        let query: Vec<(String, String)> = url
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        let query_value = |name: &str| {
            query
                .iter()
                .find(|(key, _)| key == name)
                .map(|(_, value)| value.clone())
                .unwrap_or_default()
        };
        let bucket = "test-bucket".to_owned();
        let mut state = self.state.lock().expect("fake state");

        // POST /upload/session/{id}: chunk.
        if request.method == "POST" && path.starts_with("/upload/session/") {
            self.record(&mut state, request, &path);
            let id = path.trim_start_matches("/upload/session/").to_owned();
            let Some(data) = state.sessions.get_mut(&id) else {
                return error_response(404, "not found");
            };
            data.extend_from_slice(&request.body);
            let content_range = request
                .headers
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case("Content-Range"))
                .map(|(_, value)| value.as_str())
                .unwrap_or("");
            if content_range.ends_with("/*") {
                return GcsHttpResponse {
                    status: 200,
                    headers: vec![("X-Http-Status-Code-Override".to_owned(), "308".to_owned())],
                    body: Vec::new(),
                };
            }
            let size = state.sessions.get(&id).map(|data| data.len()).unwrap_or(0);
            return json_response(
                200,
                serde_json::json!({
                    "bucket": bucket,
                    "name": "uploaded",
                    "size": size.to_string(),
                }),
            );
        }

        // POST /upload/storage/v1/b/{bucket}/o: upload (multipart or resumable
        // initiation).
        if request.method == "POST" && path.starts_with("/upload/storage/v1/b/") {
            self.record(&mut state, request, &path);
            match query_value("uploadType").as_str() {
                "multipart" => {
                    let content_type = request
                        .headers
                        .iter()
                        .find(|(key, _)| key.eq_ignore_ascii_case("Content-Type"))
                        .map(|(_, value)| value.as_str())
                        .unwrap_or("");
                    let boundary = content_type
                        .split("boundary=")
                        .nth(1)
                        .unwrap_or("")
                        .trim_matches('"');
                    let parts = parse_multipart(&request.body, boundary);
                    let name = parts
                        .first()
                        .and_then(|(_, data)| {
                            serde_json::from_slice::<serde_json::Value>(data).ok()
                        })
                        .and_then(|value| {
                            value
                                .get("name")
                                .and_then(|name| name.as_str())
                                .map(str::to_owned)
                        })
                        .unwrap_or_default();
                    let media = parts
                        .get(1)
                        .map(|(_, data)| data.clone())
                        .unwrap_or_default();
                    let size = media.len();
                    state.objects.insert(name.clone(), media);
                    json_response(
                        200,
                        serde_json::json!({
                            "bucket": bucket,
                            "name": name,
                            "size": size.to_string(),
                        }),
                    )
                }
                "resumable" => {
                    state.session_counter += 1;
                    let id = format!("s{}", state.session_counter);
                    state.sessions.insert(id.clone(), Vec::new());
                    GcsHttpResponse {
                        status: 200,
                        headers: vec![(
                            "Location".to_owned(),
                            format!("{}/upload/session/{id}", self.endpoint),
                        )],
                        body: Vec::new(),
                    }
                }
                _ => error_response(400, "unhandled upload type"),
            }
        } else if request.method == "POST" && path == "/storage/v1/b" {
            self.record(&mut state, request, &path);
            let code = if state.create_status == 0 {
                200
            } else {
                state.create_status
            };
            if code == 200 {
                state.bucket_exists = true;
                json_response(200, serde_json::json!({ "name": bucket }))
            } else {
                error_response(code, "create failed")
            }
        } else if request.method == "GET"
            && path.starts_with("/storage/v1/b/")
            && path.ends_with("/o")
        {
            self.record(&mut state, request, &path);
            if !state.bucket_exists {
                return error_response(404, "not found");
            }
            if state.should_fail("list") {
                let status = state.fail_status;
                return error_response(status, "list failed");
            }
            let token = query_value("pageToken");
            let mut idx = 0usize;
            if let Some(rest) = token.strip_prefix('t') {
                idx = rest.parse().unwrap_or(0);
            }
            if idx >= state.pages.len() {
                return json_response(200, serde_json::json!({ "items": [] }));
            }
            let items: Vec<serde_json::Value> = state.pages[idx]
                .iter()
                .map(|name| serde_json::json!({ "name": name }))
                .collect();
            let mut response = serde_json::json!({ "items": items });
            if idx + 1 < state.pages.len() {
                response = serde_json::json!({
                    "items": items,
                    "nextPageToken": format!("t{}", idx + 1),
                });
            }
            json_response(200, response)
        } else if request.method == "GET"
            && path.starts_with("/storage/v1/b/")
            && !path.contains("/o/")
        {
            self.record(&mut state, request, &path);
            if !state.bucket_exists {
                return error_response(404, "not found");
            }
            json_response(200, serde_json::json!({ "name": bucket }))
        } else if request.method == "DELETE" && path.starts_with("/storage/v1/b/") {
            self.record(&mut state, request, &path);
            if state.should_fail("delete") {
                let status = state.fail_status;
                return error_response(status, "delete failed");
            }
            let prefix = format!("/storage/v1/b/{bucket}/o/");
            let key = path.trim_start_matches(&prefix).to_owned();
            if state.objects.remove(&key).is_none() {
                return error_response(404, "not found");
            }
            GcsHttpResponse {
                status: 204,
                headers: Vec::new(),
                body: Vec::new(),
            }
        } else if request.method == "POST" && path.ends_with("/compose") {
            self.record(&mut state, request, &path);
            let payload: serde_json::Value =
                serde_json::from_slice(&request.body).unwrap_or(serde_json::Value::Null);
            let code = if state.compose_code == 0 {
                200
            } else {
                state.compose_code
            };
            if code != 200 {
                return error_response(code, "compose failed");
            }
            let dest = path
                .trim_start_matches(&format!("/storage/v1/b/{bucket}/o/"))
                .trim_end_matches("/compose")
                .to_owned();
            let mut size = 0usize;
            if let Some(sources) = payload
                .get("sourceObjects")
                .and_then(|value| value.as_array())
            {
                for source in sources {
                    if let Some(name) = source.get("name").and_then(|value| value.as_str()) {
                        size += state.objects.get(name).map(Vec::len).unwrap_or(0);
                    }
                }
            }
            state.objects.insert(dest.clone(), vec![0; size]);
            json_response(
                200,
                serde_json::json!({
                    "bucket": bucket,
                    "name": dest,
                    "size": size.to_string(),
                }),
            )
        } else if request.method == "GET" {
            self.record(&mut state, request, &path);
            if state.should_fail("get") {
                let status = state.fail_status;
                return GcsHttpResponse {
                    status,
                    headers: Vec::new(),
                    body: b"boom".to_vec(),
                };
            }
            let prefix = format!("/{bucket}/");
            let key = path.trim_start_matches(&prefix).to_owned();
            match state.objects.get(&key) {
                Some(data) => GcsHttpResponse {
                    status: 200,
                    headers: vec![("Content-Length".to_owned(), data.len().to_string())],
                    body: data.clone(),
                },
                None => error_response(404, "not found"),
            }
        } else {
            GcsHttpResponse {
                status: 400,
                headers: Vec::new(),
                body: b"unhandled".to_vec(),
            }
        }
    }
}

#[async_trait]
impl GcsTransport for FakeTransport {
    async fn execute(&self, request: GcsHttpRequest) -> Result<GcsHttpResponse, String> {
        Ok(self.handle(&request))
    }
}

/// `(part headers, part body)`.
type MultipartPart = (Vec<(String, String)>, Vec<u8>);

/// Split a `multipart/related` body into `(part headers, part body)`.
fn parse_multipart(body: &[u8], boundary: &str) -> Vec<MultipartPart> {
    let delimiter = format!("--{boundary}");
    let delimiter = delimiter.as_bytes();
    let mut parts = Vec::new();
    let mut positions = Vec::new();
    let mut index = 0usize;
    while index + delimiter.len() <= body.len() {
        if &body[index..index + delimiter.len()] == delimiter {
            positions.push(index);
            index += delimiter.len();
        } else {
            index += 1;
        }
    }
    for window in positions.windows(2) {
        let start = window[0] + delimiter.len();
        let end = window[1];
        let mut part = &body[start..end];
        part = part.strip_prefix(b"\r\n").unwrap_or(part);
        part = part.strip_suffix(b"\r\n").unwrap_or(part);
        if part.starts_with(b"--") {
            continue;
        }
        let Some(split) = find_subslice(part, b"\r\n\r\n") else {
            continue;
        };
        let header_bytes = &part[..split];
        let data = part[split + 4..].to_vec();
        let headers = String::from_utf8_lossy(header_bytes)
            .lines()
            .filter_map(|line| line.split_once(':'))
            .map(|(key, value)| (key.trim().to_owned(), value.trim().to_owned()))
            .collect();
        parts.push((headers, data));
    }
    parts
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

// ---------------------------------------------------------------------------
// Rendering (must match m9jzRender in the Go oracle)
// ---------------------------------------------------------------------------

fn canonical_query(query: &[(String, String)]) -> String {
    let mut pairs = query.to_vec();
    pairs.sort();
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn canonical_json(data: &[u8]) -> String {
    match serde_json::from_slice::<serde_json::Value>(data) {
        Ok(value) => serde_json::to_string(&value).unwrap_or_else(|_| "invalid".to_owned()),
        Err(_) => "invalid".to_owned(),
    }
}

fn normalize_content_type(value: &str) -> &str {
    if value.starts_with("multipart/related") {
        "multipart/related"
    } else {
        value
    }
}

fn render_body(request: &Recorded) -> String {
    let content_type = request.header("Content-Type");
    if content_type.starts_with("multipart/related") {
        let boundary = content_type
            .split("boundary=")
            .nth(1)
            .unwrap_or("")
            .trim_matches('"');
        let parts = parse_multipart(&request.body, boundary);
        let mut rendered = Vec::new();
        for (index, (_, data)) in parts.iter().enumerate() {
            if index == 0 {
                rendered.push(format!("meta={}", canonical_json(data)));
            } else {
                let part_type = parts[index]
                    .0
                    .iter()
                    .find(|(key, _)| key.eq_ignore_ascii_case("Content-Type"))
                    .map(|(_, value)| value.as_str())
                    .unwrap_or("");
                rendered.push(format!(
                    "media-ct={part_type} media-len={} media-sha256={}",
                    data.len(),
                    sha256_hex(data)
                ));
            }
        }
        return format!("multipart {}", rendered.join(" "));
    }
    if content_type.contains("application/json") {
        return format!("json={}", canonical_json(&request.body));
    }
    if !request.body.is_empty() {
        return format!(
            "media-len={} media-sha256={}",
            request.body.len(),
            sha256_hex(&request.body)
        );
    }
    "empty".to_owned()
}

fn render(name: &str, reqs: &[Recorded], result: &str) -> String {
    let mut deletes: Vec<&Recorded> = reqs.iter().filter(|req| req.method == "DELETE").collect();
    let regular: Vec<&Recorded> = reqs.iter().filter(|req| req.method != "DELETE").collect();
    deletes.sort_by(|left, right| {
        format!("{} {}", left.method, left.path).cmp(&format!("{} {}", right.method, right.path))
    });
    let mut out = String::new();
    out.push_str(&format!("case {name}\n"));
    for (index, request) in regular.into_iter().chain(deletes).enumerate() {
        out.push_str(&format!(
            "req {index} {} {} query={}\n",
            request.method,
            request.path,
            canonical_query(&request.query)
        ));
        out.push_str(&format!(
            "hdr {index} ct={} cr={} ut={} auth={}\n",
            normalize_content_type(request.header("Content-Type")),
            request.header("Content-Range"),
            request.header("X-Upload-Content-Type"),
            if request.header("Authorization").is_empty() {
                "none"
            } else {
                "bearer"
            }
        ));
        out.push_str(&format!("body {index} {}\n", render_body(request)));
    }
    out.push_str(result);
    out.push('\n');
    out.push_str(&format!("end {name}\n"));
    out
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let digest = hasher.finalize();
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn class(err: &sg_objectstorage::GcsError) -> &'static str {
    if err.is_not_found() {
        "not_found"
    } else {
        "other"
    }
}

// ---------------------------------------------------------------------------
// Case execution
// ---------------------------------------------------------------------------

fn build_fake(case: &Case) -> Arc<FakeTransport> {
    let mut state = FakeState {
        bucket_exists: case.bucket_exists.unwrap_or(true),
        objects: HashMap::new(),
        pages: case.pages.clone(),
        reqs: Vec::new(),
        fail_status: case.fail_status,
        fail_times: case.fail_times,
        fail_kind: case.op.clone(),
        compose_code: case.compose_status,
        create_status: case.create_status,
        sessions: HashMap::new(),
        session_counter: 0,
    };
    for seed in &case.seed {
        state
            .objects
            .insert(seed.key.clone(), seed.content.as_bytes().to_vec());
    }
    Arc::new(FakeTransport {
        state: Mutex::new(state),
        endpoint: "http://fake".to_owned(),
    })
}

async fn run_case(case: &Case) -> String {
    let transport = build_fake(case);
    let store = GcsStore::with_transport(
        "test-bucket",
        case.manage_bucket,
        "pid",
        "http://fake",
        transport.clone(),
    )
    .with_retry_policy(RetryPolicy::immediate());

    let result = match case.kind.as_str() {
        "upload" => {
            let data = payload_bytes(case.payload.as_ref());
            match store.upload(&case.key, &data).await {
                Ok(bytes) => format!("result ok bytes={bytes}"),
                Err(err) => format!("result err class={}", class(&err)),
            }
        }
        "get" => match store.get(&case.key).await {
            Ok(data) => format!("result ok len={} sha256={}", data.len(), sha256_hex(&data)),
            Err(err) => format!("result err class={}", class(&err)),
        },
        "list" => {
            let (keys, err) = store.list_objects_partial(&case.prefix).await;
            let joined = keys.join(",");
            let digest = sha256_hex(joined.as_bytes());
            match err {
                Some(err) => format!(
                    "result err class={} keys={} count={} sha256={}",
                    class(&err),
                    joined,
                    keys.len(),
                    digest
                ),
                None => format!(
                    "result ok keys={} count={} sha256={}",
                    joined,
                    keys.len(),
                    digest
                ),
            }
        }
        "delete" => match store.delete(&case.key).await {
            Ok(()) => "result ok".to_owned(),
            Err(err) => format!("result err class={}", class(&err)),
        },
        "compose" => {
            let sources = sources_for(case);
            match store.compose(&case.dest, &sources).await {
                Ok(size) => format!("result ok size={size}"),
                Err(err) => format!("result err class={}", class(&err)),
            }
        }
        "init" => match store.init().await {
            Ok(()) => "result ok".to_owned(),
            Err(err) => format!("result err class={}", class(&err)),
        },
        "pool" => run_pool(case).await,
        _ => "result err class=other unknown-kind".to_owned(),
    };

    let transcript = {
        let state = transport.state.lock().expect("fake state");
        render(&case.name, &state.reqs, &result)
    };
    transcript
}

async fn run_pool(case: &Case) -> String {
    let fail: HashSet<String> = case.fail_values.iter().cloned().collect();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let inflight = Arc::new(Mutex::new(0i64));
    let max_inflight = Arc::new(Mutex::new(0i64));
    let sleep = Duration::from_millis(case.sleep_ms);

    let seen_task = Arc::clone(&seen);
    let inflight_task = Arc::clone(&inflight);
    let max_task = Arc::clone(&max_inflight);
    let fail_task = fail.clone();
    let outcome = for_each_string(case.values.clone(), move |_, value| {
        let seen = Arc::clone(&seen_task);
        let inflight = Arc::clone(&inflight_task);
        let max_inflight = Arc::clone(&max_task);
        let fail = fail_task.clone();
        async move {
            {
                seen.lock().unwrap().push(value.clone());
                let mut current = inflight.lock().unwrap();
                *current += 1;
                let mut max = max_inflight.lock().unwrap();
                if *current > *max {
                    *max = *current;
                }
            }
            if !sleep.is_zero() {
                tokio::time::sleep(sleep).await;
            }
            {
                let mut current = inflight.lock().unwrap();
                *current -= 1;
            }
            if fail.contains(&value) {
                Err(format!("boom {value}"))
            } else {
                Ok(())
            }
        }
    })
    .await;

    let mut values = seen.lock().unwrap().clone();
    values.sort();
    let max = *max_inflight.lock().unwrap();
    let limit = pool_limit();
    match outcome {
        Ok(()) => format!(
            "result kind=ok values={} limit={limit} max_inflight={max}",
            values.join(",")
        ),
        Err(err) => {
            let mut lines: Vec<&str> = err.split('\n').collect();
            lines.sort();
            format!(
                "result kind=err values={} errors={} limit={limit} max_inflight={max}",
                values.join(","),
                lines.join(";")
            )
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn gcs_object_storage_diff() {
    let Ok(corpus_path) = std::env::var("M9JZ_CORPUS") else {
        eprintln!("M9JZ_CORPUS not set; skipping GCS object storage diff");
        return;
    };
    let Ok(out_path) = std::env::var("M9JZ_OUT") else {
        eprintln!("M9JZ_OUT not set; skipping GCS object storage diff");
        return;
    };
    let raw = std::fs::read(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_slice(&raw).expect("parse corpus");

    let mut transcript = String::new();
    for case in &corpus.cases {
        transcript.push_str(&run_case(case).await);
    }
    std::fs::write(&out_path, transcript).expect("write transcript");
    println!("RUST M9JZ_GCS_HARNESS_OK ({} cases)", corpus.cases.len());
}
