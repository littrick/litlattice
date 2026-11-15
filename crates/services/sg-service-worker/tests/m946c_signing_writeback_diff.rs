
//! M9.46c differential harness: commit-verification writeback and
//! installation-token refresh on the Batch push path.
//!
//! Mirrors the UNTRACKED Go oracle
//! `internal/batches/reconciler/zz_m946c_signing_writeback_oracle_test.go`.
//! Both sides consume `test_ws/p9-go/data/batches_push_commit/m946c_corpus.json`,
//! write one normalized JSON document per case, and
//! `run_batches_push_commit_diff.sh` compares them. Gated by
//! `SG_M946C_SIGNING_WRITEBACK=1`.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use sg_extsvc::auth::{GitHubAppAuthenticator, InstallationAuthenticator};
use sg_extsvc::batches_sources as extsvc;
use sg_extsvc::{HttpDoer, HttpRequest, HttpResponse};

const M946C_TEST_KEY: &str = "-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEA1LqMqnchtoTHiRfFds2RWWji43R5mHT65WZpZXpBuBwsgSWr
rN5VwTHZ4dxWk+XlyVDYsri7vlVWNX4EIt0jwxvh/OBXCFJXTL+byNHCimRIKvur
ofoT1eF3z+5WpH5ddHNPOkGZV0Chyd5kvUcNuFA7q203HRVEOloHEs4fqJrGPHIF
zc8Sug5qkOtZTS5xiHgTtmZkDLuLZ26H5Gfx3zZk5Gv2Jy/+fLsGninaiwTRsZf6
RgPgmdlkuM8OhSm4GtlpzoK0D3iZEhf4pITo1CK2U4Cs7vzkU0UkQ+J/z6dDmVBJ
gkalH1SHsboRqjNxkStEqGnbWwdtal01skbGOwIDAQABAoIBAQCls54oll17V5g5
0Htu3BdxBsNdG3gv6kcY85n7gqy4ZbHA83/zSsiPkW4/gasqzzQbiU8Sf9U2IDDj
wAImygy2SPzSRklk4QbBcKs/VSztMcoJOTprFGno+xShsexpe0j+kWdQYJK6JU0g
+ouL6FHmlRC1qn/4tn0L2t6Rpl+Aq4peDLqdwFHXj8GxGv0S10qMQ4/ER7onP6f0
99WDTvNQR5DugKqHxooOV5HfUP70scqhCcFhp2zc7/aYQFVt/k4hDOMu/w4HhkD3
r34y4EJoZsugGD1kPaJCw2rbSdoTtQHCqG5tfidY+XUIoC9mfmN8243jeRrhayeT
4ewiDuNhAoGBAPszeqN/+V8EVrlbBiBG+xVYGzWU0KgHu1TUiIrOSmKa6rTwaYMb
dKI8N4gYwFtb24AeDLfGnpaZAKSvNnrf8OapyLik7zXDylY0DBU7tRxQiUvNsVTs
7CYjxih5GWzUeP/xgpfVbHIIGdTHaZ6JWiDHWOolAw3hQyw6V/uQTDtxAoGBANjK
6vlctX55MjE2tuPk3ZCtCjgDFmWQjvFuiYYE/2cP4v4UBqgZn1vOaLRCnFm10ycl
peBLxPVpeeNBWc2ep2YNnJ+hm+SavhIDesLJTxuhC4wtcKMVAtq83VQmMQTU5wRO
KcUpviXLv2Z0UfbMWcohR4fJY1SkREwaxneHZc5rAoGBAIpT8c/BNBhPslYFutzh
WXiKeQlLdo9hGpZ/JuWQ7cNY3bBfxyqMXvDLyiSmxJ5KehgV9BjrRf9WJ9WIKq8F
TIooqsCLCrMHqw9HP/QdWgFKlCBrF6DVisEB6Cf3b7nPUwZV/v0PaNVugpL6cL39
kuUEAYGGeiUVi8D6K+L6tg/xAoGATlQqyAQ+Mz8Y6n0pYXfssfxDh+9dpT6w1vyo
RbsCiLtNuZ2EtjHjySjv3cl/ck5mx2sr3rmhpUYB2yFekBN1ykK6x1Z93AApEpsd
PMm9gm8SnAhC/Tl3OY8prODLr0I5Ye3X27v0TvWp5xu6DaDSBF032hDiic98Ob8m
3EMYfpcCgYBySPGnPmwqimiSyZRn+gJh+cZRF1aOKBqdqsfdcQrNpaZuZuQ4aYLo
cEoKFPr8HjXXUVCa3Q84tf9nGb4iUFslRSbS6RCP6Nm+JsfbCTtzyglYuPRKITGm
jSzka5UER3Dj1lSLMk9DkU+jrBxUsFeeiQOYhzQBaZxguvwYRIYHpg==
-----END RSA PRIVATE KEY-----";

#[derive(Debug, Clone, Deserialize, Default)]
struct Case {
    name: String,
    kind: String,
    #[serde(default)]
    refresh_mode: String,
    #[serde(default)]
    external_type: String,
    #[serde(default)]
    seed_token: String,
    #[serde(default)]
    seed_expiry_minutes: i64,
    #[serde(default)]
    new_token: String,
    #[serde(default)]
    status: u16,
    #[serde(default)]
    transport_error: String,
    #[serde(default)]
    changeset_id: i64,
    #[serde(default)]
    verified: bool,
    #[serde(default)]
    reason: String,
    #[serde(default)]
    signature: String,
    #[serde(default)]
    payload: String,
}

#[derive(Debug, Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

struct QueueHttp {
    responses: Mutex<VecDeque<Result<HttpResponse, String>>>,
    calls: AtomicUsize,
}

#[async_trait]
impl HttpDoer for QueueHttp {
    async fn do_request(&self, _request: HttpRequest) -> Result<HttpResponse, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.responses
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| Err("no queued response".to_owned()))
    }
}

fn token_response(token: &str, expiry: &str) -> Result<HttpResponse, String> {
    Ok(HttpResponse {
        status: 201,
        headers: Vec::new(),
        body: serde_json::to_vec(&json!({"token": token, "expires_at": expiry})).unwrap(),
    })
}

fn m946c_repo(external_type: &str) -> extsvc::Repo {
    extsvc::Repo {
        id: 1,
        name: "github.example.com/m946c/example".to_owned(),
        external_repo: extsvc::ExternalRepoSpec {
            id: String::new(),
            service_type: external_type.to_owned(),
            service_id: String::new(),
        },
        sources: std::collections::BTreeMap::from([(
            "1".to_owned(),
            extsvc::RepoSource {
                id: "1".to_owned(),
                clone_url: "https://github.example.com/m946c/example.git".to_owned(),
            },
        )]),
        metadata: json!({}),
    }
}

async fn run_refresh(case: &Case) -> Value {
    let http = Arc::new(QueueHttp {
        responses: Mutex::new(VecDeque::new()),
        calls: AtomicUsize::new(0),
    });
    let app = Arc::new(GitHubAppAuthenticator::new(12345, M946C_TEST_KEY).unwrap());
    let base = url::Url::parse("https://github.example.com/").unwrap();
    let mut installation =
        InstallationAuthenticator::new(base, 9000, app, None, http.clone());

    // Seed the current token exactly like the Go oracle does (a fresh token
    // for the no-refresh/bad-type cases, an already-expired token for
    // near_expiry).
    let seed_expiry = if case.seed_expiry_minutes <= 5 {
        "2000-01-01T00:00:00Z"
    } else {
        "2999-01-01T00:00:00Z"
    };
    match case.refresh_mode.as_str() {
        "none" | "near_expiry" | "bad_type" => {
            http.responses
                .lock()
                .unwrap()
                .push_back(token_response(&case.seed_token, seed_expiry));
            installation.refresh().await.expect("seed refresh");
        }
        _ => {}
    }

    let external = if !case.transport_error.is_empty() {
        Err(case.transport_error.clone())
    } else if case.status == 201 {
        token_response(&case.new_token, "2999-01-01T00:00:00Z")
    } else {
        Ok(HttpResponse {
            status: case.status,
            headers: Vec::new(),
            body: Vec::new(),
        })
    };
    http.responses.lock().unwrap().push_back(external);
    http.calls.store(0, Ordering::SeqCst);

    let auth = extsvc::SourceAuth::GitHubAppInstallation(Arc::new(installation));
    let external_type = if case.external_type.is_empty() {
        "github"
    } else {
        case.external_type.as_str()
    };
    let mut out = json!({"name": case.name});
    match extsvc::push_authenticator_for_source_auth_refreshed(&auth).await {
        Ok(Some(push_auth)) => {
            match extsvc::gitserver_push_config(&m946c_repo(external_type), Some(&push_auth)) {
                Ok(config) => out["remote_url"] = json!(config.remote_url),
                Err(err) => out["error"] = json!(err.message),
            }
        }
        Ok(None) => out["error"] = json!("no push authenticator"),
        Err(err) => out["error"] = json!(err.message),
    }
    out["refresh_calls"] = json!(http.calls.load(Ordering::SeqCst));
    out
}

async fn run_writeback(pool: &sqlx::PgPool, case: &Case) -> Value {
    let mut conn = pool.acquire().await.expect("acquire");
    let before: sg_db::OffsetDateTime = sqlx::query_scalar(
        "SELECT updated_at FROM changesets WHERE id = $1",
    )
    .bind(case.changeset_id)
    .fetch_one(&mut *conn)
    .await
    .expect("read updated_at");

    let value = if case.verified {
        json!({
            "verified": true,
            "reason": case.reason,
            "signature": case.signature,
            "payload": case.payload,
        })
    } else {
        // `jsonbColumn(nil)` for an unverified commit.
        json!({})
    };
    sg_db::batches::update_reconciler_changeset_commit_verification(
        &mut conn,
        case.changeset_id,
        &value,
    )
    .await
    .expect("write verification");

    use sqlx::Row;
    let row = sqlx::query(
        "SELECT commit_verification, updated_at, batch_change_ids FROM changesets WHERE id = $1",
    )
    .bind(case.changeset_id)
    .fetch_one(&mut *conn)
    .await
    .expect("read back");
    let verification: Value = row.get(0);
    let after: sg_db::OffsetDateTime = row.get(1);
    let batch_change_ids: Value = row.get(2);
    json!({
        "name": case.name,
        "commit_verification": verification,
        "updated_at_advanced": after > before,
        "batch_change_ids": batch_change_ids,
    })
}

#[tokio::test]
async fn m946c_signing_writeback_corpus() {
    if std::env::var("SG_M946C_SIGNING_WRITEBACK").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M946C_CORPUS").expect("M946C_CORPUS");
    let out_dir = std::env::var("M946C_OUT").expect("M946C_OUT");
    let dsn = std::env::var("SG_QTEST_DSN").expect("SG_QTEST_DSN");
    std::fs::create_dir_all(&out_dir).expect("create out dir");
    let raw = std::fs::read_to_string(corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&dsn)
        .await
        .expect("connect postgres");

    for case in &corpus.cases {
        let out = match case.kind.as_str() {
            "refresh" => run_refresh(case).await,
            "writeback" => run_writeback(&pool, case).await,
            other => panic!("unknown kind {other}"),
        };
        let encoded = serde_json::to_string_pretty(&out).expect("encode");
        std::fs::write(format!("{out_dir}/{}.json", case.name), encoded).expect("write case");
    }
}
