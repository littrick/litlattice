//! M9.35c differential: the remaining code-host webhook dispatches
//! (GITLAB / BITBUCKETCLOUD / AZUREDEVOPS) plus the 501 default, driven through
//! the real `/.api/webhooks/{uuid}` handler
//! (`cmd/frontend/internal/webhooks/zz_m935c_dispatch_oracle_test.go`, driven
//! by `run_webhooks_dispatch_diff.sh`).
//!
//! Env:
//!
//! * `SG_M935C_WEBHOOKS_DISPATCH_DIFF=1`
//! * `M935C_CORPUS` — `corpus.json`
//! * `M935C_OUT` — output directory (one `<name>.json` per case)
//! * `SG_QTEST_DSN` — scratch database DSN

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use http::{header, Method, Request};
use http_body_util::{BodyExt, Full};
use serde::{Deserialize, Serialize};

use sg_service_frontend::githubapp_inbound::{
    inbound_webhook_response, sign as sign_inbound, PgInboundDeps,
};

#[derive(Deserialize)]
struct Fixtures {
    webhooks: Vec<FixtureWebhook>,
}

#[derive(Deserialize)]
struct FixtureWebhook {
    uuid: String,
    secret: String,
    #[serde(rename = "codeHostKind")]
    code_host_kind: String,
    #[serde(rename = "codeHostURN")]
    code_host_urn: String,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    uuid: String,
    #[serde(default)]
    body: String,
    #[serde(rename = "gitlabToken", default)]
    gitlab_token: String,
    #[serde(rename = "eventKey", default)]
    event_key: String,
    #[serde(rename = "signatureMode", default)]
    signature_mode: String,
}

#[derive(Deserialize)]
struct Corpus {
    fixtures: Fixtures,
    cases: Vec<Case>,
}

#[derive(Serialize)]
struct Output {
    status: u16,
    body: String,
}

async fn seed_database(pool: &sqlx::PgPool, corpus: &Corpus) {
    sqlx::query("DELETE FROM webhooks")
        .execute(pool)
        .await
        .unwrap();
    for hook in &corpus.fixtures.webhooks {
        let secret: Option<&str> = if hook.secret.is_empty() {
            None
        } else {
            Some(hook.secret.as_str())
        };
        sqlx::query(
            "INSERT INTO webhooks (uuid, code_host_kind, code_host_urn, name, secret) \
             VALUES ($1::uuid, $2, $3, 'seed-webhook', $4)",
        )
        .bind(&hook.uuid)
        .bind(&hook.code_host_kind)
        .bind(&hook.code_host_urn)
        .bind(secret)
        .execute(pool)
        .await
        .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn webhooks_dispatch_diff() {
    if std::env::var("SG_M935C_WEBHOOKS_DISPATCH_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M935C_CORPUS").expect("M935C_CORPUS");
    let out_dir = std::env::var("M935C_OUT").expect("M935C_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    std::fs::create_dir_all(&out_dir).unwrap();
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).unwrap()).unwrap();

    let pool = sqlx::PgPool::connect(&dsn).await.expect("connect postgres");
    seed_database(&pool, &corpus).await;

    let keyring = Arc::new(sg_keyring::Keyring::default());
    let inbound = PgInboundDeps::new(pool.clone(), keyring);

    for case in &corpus.cases {
        let secret = corpus
            .fixtures
            .webhooks
            .iter()
            .find(|hook| hook.uuid == case.uuid)
            .map(|hook| hook.secret.clone())
            .unwrap_or_default();
        let body = Bytes::from(case.body.clone().into_bytes());
        let path = format!("/.api/webhooks/{}", case.uuid);
        let mut request = Request::builder()
            .method(Method::POST)
            .uri(path)
            .body(Full::new(body.clone()))
            .unwrap();
        request
            .headers_mut()
            .insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        if !case.gitlab_token.is_empty() {
            request
                .headers_mut()
                .insert("x-gitlab-token", case.gitlab_token.parse().unwrap());
        }
        if !case.event_key.is_empty() {
            request
                .headers_mut()
                .insert("x-event-key", case.event_key.parse().unwrap());
        }
        if let Some(signature) = sign_inbound(&case.signature_mode, secret.as_bytes(), &body) {
            // Bitbucket Cloud reads `X-Hub-Signature`, not the -256 variant.
            request
                .headers_mut()
                .insert("x-hub-signature", signature.parse().unwrap());
        }

        let response = inbound_webhook_response(&mut request, &inbound).await;
        let status = response.status().as_u16();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let output = Output {
            status,
            body: String::from_utf8_lossy(&body).into_owned(),
        };
        let encoded = serde_json::to_vec(&output).unwrap();
        std::fs::write(
            std::path::Path::new(&out_dir).join(format!("{}.json", case.name)),
            encoded,
        )
        .unwrap();
        println!("RUST {} status={status}", case.name);
    }
}
