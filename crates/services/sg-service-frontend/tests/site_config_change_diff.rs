//! M9.8hj differential of `SiteConfigurationChange` fields and the
//! `SiteConfigurationChangeConnection` pagination
//! (`cmd/frontend/graphqlbackend/site_config_change.go:1-78`,
//! `site_config_change_connection.go:1-169`).
//!
//! Both sides consume the same corpus. The Go oracle
//! (`zz_m9hj_oracle_test.go`, untracked) drives the real
//! `SiteConfigurationChangeConnectionStore` and `gqlutil.ConnectionResolver`
//! with `dbmocks` returning the corpus history; this harness drives
//! [`SiteConfigurationChangeConnection`] through a [`FakeHistory`]. A case
//! emits `<name>.json` (compact, sorted keys) and the runner compares the two
//! directories byte for byte.
//!
//! Env:
//!
//! * `SG_M9HJ_DIFF=1`
//! * `M9HJ_CORPUS` — JSON array of cases (see [`Case`])
//! * `M9HJ_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_service_frontend::site_config_change::{
    visible_history, ConnectionResolverArgs, ConnectionResolverOptions, SiteConfigChangeUserStore,
    SiteConfigHistoryStore, SiteConfigRow, SiteConfigurationChangeConnection, UserRef,
};

#[derive(Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    history: Vec<HistoryRow>,
    #[serde(default)]
    users: Vec<i32>,
    args: Args,
}

#[derive(Deserialize)]
struct HistoryRow {
    id: i32,
    #[serde(default)]
    author_user_id: i32,
    #[serde(default)]
    contents: String,
    redacted_contents: String,
    created_at: String,
    updated_at: String,
}

#[derive(Deserialize, Default)]
struct Args {
    #[serde(default)]
    first: Option<i32>,
    #[serde(default)]
    last: Option<i32>,
    #[serde(default)]
    after: Option<String>,
    #[serde(default)]
    before: Option<String>,
}

struct FakeHistory {
    rows: Vec<SiteConfigRow>,
    count: usize,
}

#[async_trait]
impl SiteConfigHistoryStore for FakeHistory {
    async fn get_site_configs(&self) -> Result<Vec<SiteConfigRow>, String> {
        Ok(self.rows.clone())
    }

    async fn get_site_config_count(&self) -> Result<usize, String> {
        Ok(self.count)
    }
}

struct FakeUsers {
    ids: Vec<i32>,
}

#[async_trait]
impl SiteConfigChangeUserStore for FakeUsers {
    async fn user_by_id(&self, id: i32) -> Result<Option<UserRef>, String> {
        if self.ids.contains(&id) {
            Ok(Some(UserRef { id }))
        } else {
            Ok(None)
        }
    }
}

fn parse_time(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339).expect("RFC3339 timestamp")
}

#[tokio::test]
async fn site_config_change_matches_the_go_baseline() {
    if std::env::var("SG_M9HJ_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9HJ_CORPUS").expect("M9HJ_CORPUS");
    let out_dir = std::env::var("M9HJ_OUT").expect("M9HJ_OUT");
    std::fs::create_dir_all(&out_dir).expect("create output dir");

    let cases: Vec<Case> =
        serde_json::from_str(&std::fs::read_to_string(&corpus_path).expect("read corpus"))
            .expect("parse corpus");

    for case in &cases {
        let rows: Vec<SiteConfigRow> = case
            .history
            .iter()
            .map(|row| SiteConfigRow {
                id: row.id,
                author_user_id: row.author_user_id,
                contents: row.contents.clone(),
                redacted_contents: Some(row.redacted_contents.clone()),
                created_at: parse_time(&row.created_at),
                updated_at: parse_time(&row.updated_at),
            })
            .collect();
        let count = visible_history(&rows).len();

        let store = FakeHistory { rows, count };
        let users = FakeUsers {
            ids: case.users.clone(),
        };
        let connection = SiteConfigurationChangeConnection::new(
            &store,
            Some(ConnectionResolverArgs {
                first: case.args.first,
                last: case.args.last,
                after: case.args.after.clone(),
                before: case.args.before.clone(),
            }),
            ConnectionResolverOptions::default(),
        );

        let total_count = match connection.total_count().await {
            Ok(value) => json!({"ok": true, "value": value}),
            Err(error) => json!({"ok": false, "error": error}),
        };

        let nodes = match connection.nodes().await {
            Ok(nodes) => {
                let mut resolved = Vec::with_capacity(nodes.len());
                for node in &nodes {
                    let author = match node.author(&users).await {
                        Ok(Some(user)) => json!({"user_id": user.id}),
                        Ok(None) => Value::Null,
                        Err(error) => json!({"error": error}),
                    };
                    resolved.push(json!({
                        "id": node.id(),
                        "author": author,
                        "diff": node.diff(),
                        "created_at": node.created_at(),
                        "updated_at": node.updated_at(),
                    }));
                }
                json!({"ok": true, "value": resolved})
            }
            Err(error) => json!({"ok": false, "error": error}),
        };

        let page_info = match connection.page_info().await {
            Ok(page_info) => json!({
                "ok": true,
                "value": {
                    "hasNextPage": page_info.has_next_page,
                    "hasPreviousPage": page_info.has_previous_page,
                    "startCursor": page_info.start_cursor,
                    "endCursor": page_info.end_cursor,
                },
            }),
            Err(error) => json!({"ok": false, "error": error}),
        };

        let envelope = json!({
            "name": case.name,
            "total_count": total_count,
            "nodes": nodes,
            "page_info": page_info,
        });
        let text = serde_json::to_string(&envelope).expect("serialize output");
        std::fs::write(out_dir_path(&out_dir, &case.name), text).expect("write case output");
    }

    println!("RUST site-config-change cases={}", cases.len());
}

fn out_dir_path(dir: &str, name: &str) -> std::path::PathBuf {
    std::path::Path::new(dir).join(format!("{name}.json"))
}
