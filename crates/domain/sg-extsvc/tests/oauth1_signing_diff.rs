//! Differential harness for OAuth 1.0a request signing
//! (`internal/extsvc/auth/oauth1.go`, ported in `sg_extsvc::auth::OAuthClient`).
//!
//! The untracked Go oracle
//! (`internal/extsvc/auth/zz_m9_16b_oauth1_oracle_test.go`) signs each corpus
//! case with the real `gomodule/oauth1` client and records the `Authorization`
//! header plus the nonce/timestamp it chose. The Rust side re-signs with those
//! same nonce and timestamp (the baseline's only nondeterministic inputs) and
//! must produce a byte-identical header. Everything is offline: the corpus
//! embeds a fixed RSA key.
//!
//! Inert unless `M916B_CORPUS` and `M916B_OUT` are set.

use std::fs;
use std::path::Path;

use serde::Deserialize;
use serde_json::json;

use sg_extsvc::auth::{Authenticator, OAuth1SignatureMethod, OAuthClient};

#[derive(Debug, Clone, Deserialize)]
struct Case {
    name: String,
    method: String,
    url: String,
    consumer_key: String,
    #[serde(default)]
    consumer_secret: String,
    #[serde(default)]
    token: String,
    signature_method: String,
    private_key_pem: String,
}

#[derive(Debug, Deserialize)]
struct GoEnvelope {
    // The script compares this with the Rust header; the harness only needs
    // the nondeterministic inputs.
    #[allow(dead_code)]
    header: String,
    nonce: String,
    timestamp: String,
}

#[test]
fn oauth1_signing_diff() {
    let (Ok(corpus_path), Ok(out)) = (
        std::env::var("M916B_CORPUS"),
        std::env::var("M916B_OUT"),
    ) else {
        return;
    };
    let raw = fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&raw).expect("parse corpus");
    for case in &cases {
        let go_path = Path::new(&out).join("go").join(format!("{}.json", case.name));
        let go_raw = fs::read_to_string(&go_path).expect("read go envelope");
        let go: GoEnvelope = serde_json::from_str(&go_raw).expect("parse go envelope");
        let timestamp: i64 = go.timestamp.parse().expect("numeric oauth_timestamp");

        let signature_method = if case.signature_method == "HMAC-SHA1" {
            OAuth1SignatureMethod::HmacSha1
        } else {
            OAuth1SignatureMethod::RsaSha1
        };
        let client = OAuthClient {
            consumer_token: case.consumer_key.clone(),
            consumer_secret: case.consumer_secret.clone(),
            private_key_pem: case.private_key_pem.clone(),
            signature_method,
            token: case.token.clone(),
            token_secret: String::new(),
            nonce: Some(go.nonce.clone()),
            timestamp: Some(timestamp),
        };
        let header = client
            .authorization_header(&case.method, &case.url, &go.nonce, timestamp)
            .expect("sign request");

        // Also exercise the `Authenticator` seam used by the sources.
        let mut headers = Vec::new();
        client
            .authenticate(&case.method, &case.url, &mut headers)
            .expect("authenticate");
        assert_eq!(
            headers,
            vec![("Authorization".to_owned(), header.clone())],
            "{}",
            case.name
        );

        let envelope = json!({ "header": header });
        fs::write(
            Path::new(&out)
                .join("rust")
                .join(format!("{}.json", case.name)),
            serde_json::to_vec(&envelope).expect("serialize envelope"),
        )
        .expect("write rust envelope");
        println!("RUST {}", case.name);
    }
}
