//! M9.8jb differential harness: replays the same corpus as the Go oracle
//! (`cmd/frontend/internal/app/zz_m9jb_oracle_test.go`) through
//! `app_router::dispatch_async_with_body` with injected handler seams, and
//! emits the same normalized JSON (raw body hex, so the zip archives compare
//! byte for byte).
//!
//! Gated on `M9JB_CORPUS`; `run_app_handlers_more_diff.sh` sets it and points
//! `M9JB_OUT` at the output file.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use http::{Method, Request};
use http_body_util::{BodyExt, Full};
use sg_graphql::exec::Actor;
use sg_service_frontend::app_handlers::{AppConfig, AppDeps, AppEnv};
use sg_service_frontend::app_handlers_more::{
    CurrentUser, EditorBackend, LatestPing, LatestPingResult, MoreDeps, OneClickExport,
    OneClickExportRequest, SiteAdminCheck, UsageStatsArchive, VerifyEmailDb,
};
use sg_service_frontend::app_router;

#[derive(Debug, Clone, Default, serde::Deserialize)]
struct EditorCase {
    #[serde(default)]
    clone: String,
    #[serde(default)]
    repo: String,
    #[serde(default)]
    get_by_name: String,
    #[serde(default)]
    default_rev: String,
    #[serde(default)]
    revs: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
struct Case {
    #[serde(default)]
    name: String,
    #[serde(default)]
    method: String,
    #[serde(default)]
    target: String,
    #[serde(default)]
    body: String,
    #[serde(default)]
    actor: String,
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    users_get: String,
    #[serde(default)]
    email_get: String,
    #[serde(default)]
    verify: String,
    #[serde(default)]
    primary: String,
    #[serde(default)]
    set_primary: String,
    #[serde(default)]
    sec_event: String,
    #[serde(default)]
    authz: String,
    #[serde(default)]
    ping: String,
    #[serde(default)]
    ping_argument: String,
    #[serde(default)]
    editor: Option<EditorCase>,
}

#[derive(Debug, serde::Serialize)]
struct ResultRow {
    name: String,
    status: u16,
    location: String,
    content_type: String,
    content_disposition: String,
    cache_control: String,
    x_content_type_options: String,
    body_hex: String,
}

fn header_value(headers: &http::HeaderMap, name: &str) -> String {
    let values: Vec<String> = headers
        .get_all(name)
        .iter()
        .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned())
        .collect();
    if values.is_empty() {
        "-".to_owned()
    } else {
        values.join(",")
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// The `archive/zip` end-of-central-directory record for an empty archive,
/// which is what `oneclickexport.Export` returns for `{}`.
fn empty_zip() -> Vec<u8> {
    let mut zip = vec![0x50, 0x4b, 0x05, 0x06];
    zip.resize(22, 0);
    zip
}

struct Deps {
    case: Case,
}

#[async_trait]
impl SiteAdminCheck for Deps {
    async fn check_site_admin(&self, _actor: Actor) -> Result<(), String> {
        match self.case.users_get.as_str() {
            "admin" => Ok(()),
            "user" => Err("must be site admin".to_owned()),
            "error" => Err("boom".to_owned()),
            _ => Err("not authenticated".to_owned()),
        }
    }
}

#[async_trait]
impl VerifyEmailDb for Deps {
    async fn current_user(&self) -> Result<CurrentUser, String> {
        match self.case.users_get.as_str() {
            "error" => Err("boom".to_owned()),
            "no_current" => Err("not authenticated".to_owned()),
            _ => Ok(CurrentUser { id: 7 }),
        }
    }

    async fn get_email(&self, _user_id: i32, _email: &str) -> Result<(String, bool), String> {
        match self.case.email_get.as_str() {
            "error" => Err("email boom".to_owned()),
            "verified" => Ok(("User@Example.com".to_owned(), true)),
            _ => Ok(("User@Example.com".to_owned(), false)),
        }
    }

    async fn verify(&self, _user_id: i32, _email: &str, _code: &str) -> Result<bool, String> {
        match self.case.verify.as_str() {
            "mismatch" => Ok(false),
            "error" => Err("verify boom".to_owned()),
            _ => Ok(true),
        }
    }

    async fn get_primary_email(&self, _user_id: i32) -> Result<(String, bool), String> {
        if self.case.primary == "error" {
            Err("primary boom".to_owned())
        } else {
            Ok(("User@Example.com".to_owned(), true))
        }
    }

    async fn set_primary_email(&self, _user_id: i32, _email: &str) -> Result<(), String> {
        if self.case.set_primary == "error" {
            Err("set primary boom".to_owned())
        } else {
            Ok(())
        }
    }

    async fn log_security_event(
        &self,
        _user_id: i32,
        _url_path: &str,
        _email: &str,
    ) -> Result<(), String> {
        if self.case.sec_event == "error" {
            Err("security event boom".to_owned())
        } else {
            Ok(())
        }
    }

    async fn grant_pending_permissions(&self, _user_id: i32) -> Result<(), String> {
        if self.case.authz == "error" {
            Err("authz boom".to_owned())
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl UsageStatsArchive for Deps {
    async fn get_archive(&self) -> Result<Vec<u8>, String> {
        Ok(b"usage-archive".to_vec())
    }
}

#[async_trait]
impl OneClickExport for Deps {
    async fn export(&self, _request: &OneClickExportRequest) -> Result<Vec<u8>, String> {
        Ok(empty_zip())
    }
}

#[async_trait]
impl LatestPing for Deps {
    async fn latest_ping(&self) -> Result<LatestPingResult, String> {
        match self.case.ping.as_str() {
            "found" => Ok(LatestPingResult::Found(
                self.case.ping_argument.as_bytes().to_vec(),
            )),
            "error" => Err("ping boom".to_owned()),
            _ => Ok(LatestPingResult::NoRows),
        }
    }
}

#[async_trait]
impl EditorBackend for Deps {
    async fn clone_url_to_repo_name(&self, clone_url: &str) -> Result<Option<String>, String> {
        match self
            .case
            .editor
            .as_ref()
            .map(|editor| editor.clone.as_str())
        {
            Some("error") => Err("clone boom".to_owned()),
            Some("github") => {
                let repo = self
                    .case
                    .editor
                    .as_ref()
                    .map(|editor| editor.repo.clone())
                    .filter(|repo| !repo.is_empty())
                    .unwrap_or_else(|| "github.com/foo/bar".to_owned());
                Ok(Some(repo))
            }
            // `cloneurls.RepoSourceCloneURLToRepoName`'s GitHub fallback runs
            // even when the database has no matching repo.
            _ => Ok(github_fallback(clone_url)),
        }
    }

    async fn get_by_name(&self, _repo: &str) -> Result<(), String> {
        match self
            .case
            .editor
            .as_ref()
            .map(|editor| editor.get_by_name.as_str())
        {
            Some("error") => Err("repo not found".to_owned()),
            _ => Ok(()),
        }
    }

    async fn resolve_rev(&self, _repo: &str, rev: &str) -> Result<String, String> {
        let Some(editor) = self.case.editor.as_ref() else {
            return Err("rev not found".to_owned());
        };
        if let Some(commit) = editor.revs.get(rev) {
            return Ok(commit.clone());
        }
        if rev.is_empty() && !editor.default_rev.is_empty() {
            return Ok(editor.default_rev.clone());
        }
        Err("rev not found".to_owned())
    }
}

/// `reposource.GitHub{Url: "https://github.com"}.CloneURLToRepoName`.
fn github_fallback(clone_url: &str) -> Option<String> {
    let rest = clone_url.strip_prefix("https://github.com/")?;
    let rest = rest.strip_suffix(".git").unwrap_or(rest);
    if rest.is_empty() {
        None
    } else {
        Some(format!("github.com/{rest}"))
    }
}

fn more_deps(case: &Case) -> MoreDeps {
    let deps = Arc::new(Deps { case: case.clone() });
    let admin: Arc<dyn SiteAdminCheck> = deps.clone();
    let verify_email: Arc<dyn VerifyEmailDb> = deps.clone();
    let usage_stats: Arc<dyn UsageStatsArchive> = deps.clone();
    let one_click_export: Arc<dyn OneClickExport> = deps.clone();
    let latest_ping: Arc<dyn LatestPing> = deps.clone();
    let editor: Arc<dyn EditorBackend> = deps;
    MoreDeps {
        actor: if case.actor == "user" {
            Actor::User { uid: case.uid }
        } else {
            Actor::Anonymous
        },
        admin: Some(admin),
        verify_email: Some(verify_email),
        usage_stats: Some(usage_stats),
        one_click_export: Some(one_click_export),
        latest_ping: Some(latest_ping),
        editor: Some(editor),
    }
}

#[tokio::test]
async fn app_handlers_more_diff() {
    let Ok(corpus_path) = std::env::var("M9JB_CORPUS") else {
        eprintln!("M9JB_CORPUS not set; skipping app handlers differential");
        return;
    };
    let corpus = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let cases: Vec<Case> = serde_json::from_str(&corpus).expect("parse corpus");

    let mut rows = Vec::with_capacity(cases.len());
    for case in &cases {
        let env = AppEnv {
            deploy_type: "dev".to_owned(),
            ..AppEnv::default()
        };
        let config = AppConfig::default();
        let deps = AppDeps {
            more: more_deps(case),
            ..AppDeps::default()
        };
        let method = Method::from_bytes(case.method.as_bytes()).expect("method");
        let mut req = Request::builder()
            .method(method)
            .uri(case.target.as_str())
            .body(Full::new(Bytes::from(case.body.clone())))
            .expect("request");
        let response = app_router::dispatch_async_with_body(&mut req, &env, &config, &deps).await;

        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        rows.push(ResultRow {
            name: case.name.clone(),
            status,
            location: header_value(&headers, "location"),
            content_type: header_value(&headers, "content-type"),
            content_disposition: header_value(&headers, "content-disposition"),
            cache_control: header_value(&headers, "cache-control"),
            x_content_type_options: header_value(&headers, "x-content-type-options"),
            body_hex: hex(&body),
        });
    }

    let encoded = serde_json::to_string_pretty(&rows).expect("serialize results");
    // Go's `encoding/json` escapes these three characters by default; match it
    // so the diff script can compare the files byte for byte.
    let encoded = encoded
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026");

    if let Ok(out_path) = std::env::var("M9JB_OUT") {
        std::fs::write(out_path, encoded).expect("write results");
    } else {
        println!("{encoded}");
    }
}
