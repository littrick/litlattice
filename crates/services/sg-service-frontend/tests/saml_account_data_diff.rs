//! M9.8jx differential harness for the SAML public account data surface
//! against the Go oracle (`cmd/frontend/internal/auth/saml/zz_m9jx_oracle_test.go`).
//!
//! Set `SG_SAML_ACCOUNT_DATA_HARNESS=1`, `SAML_ACCOUNT_DATA_CORPUS=<corpus.json>`
//! and `SAML_ACCOUNT_DATA_OUT=<dir>`; for every corpus case this writes
//! `<name>.json` in the same format as the oracle.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;

use sg_service_frontend::saml_auth::{
    get_public_external_account_data, with_config_defaults, SamlAccountData, SamlEncryptableData,
    SamlHttpClient, SamlMetadataResponse, SamlProvider, SamlProviderConfig,
};

struct NoHttp;

#[async_trait]
impl SamlHttpClient for NoHttp {
    async fn fetch(&self, _url: &str) -> Result<SamlMetadataResponse, String> {
        Err("the account-data harness never fetches IdP metadata".to_owned())
    }
}

#[derive(Serialize)]
struct Cached<'a> {
    #[serde(rename = "displayName")]
    display_name: &'a str,
    #[serde(rename = "authenticationURL")]
    authentication_url: &'a str,
    #[serde(rename = "serviceID")]
    service_id: &'a str,
    #[serde(rename = "clientID")]
    client_id: &'a str,
}

#[derive(Serialize)]
struct Public<'a> {
    #[serde(rename = "displayName")]
    display_name: &'a str,
    login: &'a str,
    url: &'a str,
}

#[derive(Serialize)]
struct Output<'a> {
    name: &'a str,
    kind: &'a str,
    #[serde(rename = "type")]
    provider_type: &'a str,
    #[serde(rename = "configJSON")]
    config_json: &'a str,
    #[serde(rename = "configIDSingle")]
    config_id_single: &'a str,
    #[serde(rename = "configIDMultiple")]
    config_id_multiple: &'a str,
    cached: Cached<'a>,
    #[serde(rename = "hasPublic")]
    has_public: Option<bool>,
    public: Option<Public<'a>>,
    error: &'a str,
}

fn string(case: &Value, key: &str) -> String {
    case.get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn flag(case: &Value, key: &str) -> bool {
    case.get(key).and_then(Value::as_bool).unwrap_or(false)
}

fn account_data(case: &Value) -> SamlAccountData {
    let encryptable = |has: bool, value: String| {
        if has {
            Some(SamlEncryptableData::Unencrypted(value.into_bytes()))
        } else {
            None
        }
    };
    SamlAccountData {
        auth_data: encryptable(flag(case, "hasAuthData"), string(case, "authData")),
        data: encryptable(flag(case, "hasData"), string(case, "data")),
    }
}

fn config_from_case(case: &Value, external_url: &str) -> SamlProviderConfig {
    let provider = case
        .get("provider")
        .and_then(Value::as_object)
        .expect("case provider object");
    with_config_defaults(provider, external_url)
}

fn run_case(case: &Value, external_url: &str) -> Output<'static> {
    // The output borrows from leaked owned strings to keep the struct simple;
    // the harness is a short-lived test process.
    let name: &'static str = Box::leak(string(case, "name").into_boxed_str());
    let kind: &'static str = Box::leak(string(case, "kind").into_boxed_str());
    let config = config_from_case(case, external_url);
    let multiple = flag(case, "multiple");

    let provider = SamlProvider::new(config.clone(), multiple, Arc::new(NoHttp));
    let config_json: &'static str = Box::leak(provider.config_json().to_owned().into_boxed_str());
    let config_id_single: &'static str =
        Box::leak(config.provider_config_id(false).into_boxed_str());
    let config_id_multiple: &'static str =
        Box::leak(config.provider_config_id(true).into_boxed_str());
    let provider_type: &'static str = provider.provider_type();

    let (info, _error) = block_on(provider.cached_info());
    let display_name: &'static str = Box::leak(info.display_name.into_boxed_str());
    let authentication_url: &'static str = Box::leak(info.authentication_url.into_boxed_str());
    let service_id: &'static str = Box::leak(info.service_id.into_boxed_str());
    let client_id: &'static str = Box::leak(info.client_id.into_boxed_str());

    if kind == "provider" {
        return Output {
            name,
            kind,
            provider_type,
            config_json,
            config_id_single,
            config_id_multiple,
            cached: Cached {
                display_name,
                authentication_url,
                service_id,
                client_id,
            },
            has_public: None,
            public: None,
            error: "",
        };
    }

    let account = account_data(case);
    let (direct, provider_result) = block_on(async {
        let direct = get_public_external_account_data(&account, None).await;
        let provider_result = provider.external_account_info(&account, None).await;
        (direct, provider_result)
    });
    assert_eq!(
        direct.is_ok(),
        provider_result.is_ok(),
        "case {name}: error presence mismatch"
    );
    assert_eq!(
        direct.as_ref().err(),
        provider_result.as_ref().err(),
        "case {name}: error text mismatch"
    );
    assert_eq!(
        direct.as_ref().ok(),
        provider_result.as_ref().ok(),
        "case {name}: public data mismatch"
    );

    let (has_public, public_display_name, error): (Option<bool>, Option<String>, &'static str) =
        match direct {
            Ok(Some(public)) => (Some(true), Some(public.display_name), ""),
            Ok(None) => (Some(false), None, ""),
            Err(error) => (Some(false), None, Box::leak(error.into_boxed_str())),
        };
    let public = public_display_name.map(|public_display_name| Public {
        display_name: Box::leak(public_display_name.into_boxed_str()),
        login: "",
        url: "",
    });

    Output {
        name,
        kind,
        provider_type,
        config_json,
        config_id_single,
        config_id_multiple,
        cached: Cached {
            display_name,
            authentication_url,
            service_id,
            client_id,
        },
        has_public,
        public,
        error,
    }
}

/// A minimal blocking executor: the harness runs inside `#[tokio::test]`, so
/// this is only a convenience wrapper to keep `run_case` synchronous.
fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(future))
}

#[tokio::test(flavor = "multi_thread")]
async fn saml_account_data_diff() {
    if std::env::var("SG_SAML_ACCOUNT_DATA_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("SAML_ACCOUNT_DATA_CORPUS").expect("SAML_ACCOUNT_DATA_CORPUS");
    let out_dir = std::env::var("SAML_ACCOUNT_DATA_OUT").expect("SAML_ACCOUNT_DATA_OUT");
    let corpus: Value = serde_json::from_slice(&fs::read(&corpus_path).expect("read corpus"))
        .expect("parse corpus");
    let external_url = corpus
        .get("externalURL")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let cases = corpus
        .get("cases")
        .and_then(Value::as_array)
        .expect("cases array");
    fs::create_dir_all(&out_dir).expect("create output dir");

    for case in cases {
        let output = run_case(case, &external_url);
        let name = output.name.to_owned();
        let encoded = serde_json::to_string(&output).expect("serialize output");
        let path = Path::new(&out_dir).join(format!("{name}.json"));
        fs::write(path, format!("{encoded}\n")).expect("write output");
        println!("RUST {name}");
    }
}
