//! Differential harness for GitHub App authentication
//! (`internal/github_apps/auth/auth.go`, ported in
//! `sg_extsvc::auth::GitHubAppAuthenticator`).
//!
//! The untracked Go oracle
//! (`internal/github_apps/auth/zz_m9_16b_github_app_oracle_test.go`) signs a
//! request with the real App authenticator, then records the JWT and its
//! claims. Since `iat` is the only time-dependent input, the Rust side
//! regenerates the JWT at `iat + 60` (`generateJwt` backdates by one minute)
//! and compares full token bytes, claims and `Hash`.
//!
//! Inert unless `M916B_CORPUS` and `M916B_OUT` are set.

use std::fs;
use std::path::Path;

use serde::Deserialize;
use serde_json::json;

use sg_extsvc::auth::{Authenticator, GitHubAppAuthenticator};

#[derive(Debug, Clone, Deserialize)]
struct Case {
    name: String,
    app_id: i32,
    private_key_pem: String,
}

#[derive(Debug, Deserialize)]
struct GoEnvelope {
    // Compared by the script; the harness only consumes the inputs/claims.
    #[allow(dead_code)]
    jwt: String,
    iat: i64,
    // The Rust side recomputes these claims; the script compares them.
    #[allow(dead_code)]
    exp: i64,
    #[allow(dead_code)]
    iss: String,
    #[allow(dead_code)]
    hash: String,
}

#[test]
fn github_app_auth_diff() {
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

        let authenticator =
            GitHubAppAuthenticator::new(case.app_id, case.private_key_pem.clone())
                .expect("valid app key");
        // `generateJwt` uses iat = now - 60; feed `now = iat + 60`.
        let jwt = authenticator
            .generate_jwt_at(go.iat + 60)
            .expect("generate jwt");

        let envelope = json!({
            "jwt": jwt,
            "iat": go.iat,
            "exp": go.iat + 600,
            "iss": case.app_id.to_string(),
            "hash": authenticator.hash(),
        });
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
