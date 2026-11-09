#![forbid(unsafe_code)]

//! M9.8eg differential harness (Rust side): the frontend's read side of
//! `critical_and_site_config` (`sg_db::conf`).
//!
//! Mirrors `internal/database/conf_site_config_harness_test.go`. Set
//! `SG_CONF_DB_SOURCE_HARNESS=1` and `SG_CONF_DB_SOURCE_DSN` to a scratch
//! database cloned from the `sourcegraph` schema;
//! `test_ws/p9-go/run_conf_db_source_diff.sh` runs both sides and diffs the
//! canonical `cfg ` lines. Timestamps are not printed, so the output is
//! deterministic.

use sg_db::conf::{get_latest, site_get_latest, SiteConfig};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;

/// `confdefaults.KubernetesOrDockerComposeOrPureDocker.Site`, the deployment
/// default `confdefaults.Default` resolves to when `DEPLOY_TYPE` is unset
/// (which is how the Go harness runs).
const KUBERNETES_DEFAULT: &str = r#"{
	// The externally accessible URL for Sourcegraph (i.e., what you type into your browser)
	// This is required to be configured for Sourcegraph to work correctly.
	// "externalURL": "https://sourcegraph.example.com",

	// The authentication provider to use for identifying and signing in users.
	// Only one entry is supported.
	//
	// The builtin auth provider with signup disallowed (shown below) means that
	// after the initial site admin signs in, all other users must be invited.
	//
	// Other providers are documented at https://sourcegraph.com/docs/admin/auth.
	"auth.providers": [
		{
			"type": "builtin",
			"allowSignup": false
		}
	],
}"#;

/// `confdefaults.KubernetesOrDockerComposeOrPureDocker.Site` after
/// `conf.RedactAndHashSecrets`.
const KUBERNETES_DEFAULT_REDACTED: &str = r#"{
  // The externally accessible URL for Sourcegraph (i.e., what you type into your browser)
  // This is required to be configured for Sourcegraph to work correctly.
  // "externalURL": "https://sourcegraph.example.com",
  // The authentication provider to use for identifying and signing in users.
  // Only one entry is supported.
  //
  // The builtin auth provider with signup disallowed (shown below) means that
  // after the initial site admin signs in, all other users must be invited.
  //
  // Other providers are documented at https://sourcegraph.com/docs/admin/auth.
  "auth.providers": [
    {
      "type": "builtin"
    }
  ],
}"#;

fn flat(document: &str) -> String {
    document
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('\t', "\\t")
        .replace('\r', "\\r")
}

async fn reset(pool: &PgPool) {
    sqlx::query("TRUNCATE critical_and_site_config RESTART IDENTITY")
        .execute(pool)
        .await
        .expect("truncate");
}

async fn print_rows(pool: &PgPool) {
    let rows = sqlx::query(
        "SELECT id, author_user_id, type::text AS type, contents, redacted_contents \
         FROM critical_and_site_config ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("select rows");
    println!("cfg rows count={}", rows.len());
    for row in rows {
        let id: i32 = row.try_get("id").expect("id");
        let author: Option<i32> = row.try_get("author_user_id").expect("author");
        let kind: String = row.try_get("type").expect("type");
        let contents: String = row.try_get("contents").expect("contents");
        let redacted: Option<String> = row.try_get("redacted_contents").expect("redacted");
        println!(
            "cfg row id={} author={} type={} contents={} redacted={}",
            id,
            author.unwrap_or(0),
            kind,
            flat(&contents),
            flat(&redacted.unwrap_or_default())
        );
    }
}

fn print_site(tag: &str, site: &SiteConfig) {
    println!(
        "cfg {} id={} author={} contents={} redacted={}",
        tag,
        site.id,
        site.author_user_id,
        flat(&site.contents),
        flat(&site.redacted_contents)
    );
}

#[tokio::test]
async fn conf_site_config_harness() {
    if std::env::var("SG_CONF_DB_SOURCE_HARNESS").as_deref() != Ok("1") {
        eprintln!("SG_CONF_DB_SOURCE_HARNESS is not set");
        return;
    }
    let dsn = std::env::var("SG_CONF_DB_SOURCE_DSN").expect("SG_CONF_DB_SOURCE_DSN is not set");
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&dsn)
        .await
        .expect("connect database");

    // 1. Empty table: `get_latest` is nil; `site_get_latest` seeds the
    // deployment default with the actor's user ID; a second call does not
    // insert.
    reset(&pool).await;
    let latest = get_latest(&pool).await.expect("get_latest empty");
    println!("cfg gl-empty has={}", u8::from(latest.is_some()));
    print_rows(&pool).await;

    let site = site_get_latest(&pool, 1234, KUBERNETES_DEFAULT, KUBERNETES_DEFAULT_REDACTED)
        .await
        .expect("site_get_latest seed");
    print_site("sgl-seed", &site);
    print_rows(&pool).await;

    let again = site_get_latest(&pool, 1234, KUBERNETES_DEFAULT, KUBERNETES_DEFAULT_REDACTED)
        .await
        .expect("site_get_latest again");
    print_site("sgl-again", &again);
    print_rows(&pool).await;

    // 2. Existing rows: highest `type='site'` id wins; NULL author/redacted
    // scan as 0/""; `type='critical'` rows are ignored.
    reset(&pool).await;
    for fixture in [
        "INSERT INTO critical_and_site_config (id, type, author_user_id, contents, redacted_contents) \
         VALUES (41, 'site', NULL, '{\"externalURL\":\"https://a.example.com\"}', NULL)",
        "INSERT INTO critical_and_site_config (id, type, author_user_id, contents, redacted_contents) \
         VALUES (42, 'site', 7, '{\"externalURL\":\"https://b.example.com\"}', '{\"externalURL\":\"https://b.example.com\"}')",
        "INSERT INTO critical_and_site_config (id, type, author_user_id, contents, redacted_contents) \
         VALUES (43, 'critical', 9, 'critical', NULL)",
        "INSERT INTO critical_and_site_config (id, type, author_user_id, contents, redacted_contents) \
         VALUES (40, 'site', NULL, '{\"externalURL\":\"https://c.example.com\"}', 'c-redacted')",
    ] {
        sqlx::query(fixture).execute(&pool).await.expect("fixture");
    }
    let site = site_get_latest(&pool, 0, KUBERNETES_DEFAULT, KUBERNETES_DEFAULT_REDACTED)
        .await
        .expect("site_get_latest existing");
    print_site("sgl-existing", &site);
    print_rows(&pool).await;

    // 3. Only `type='critical'` rows: the default is still seeded, with a
    // NULL author (no actor).
    reset(&pool).await;
    sqlx::query(
        "INSERT INTO critical_and_site_config (type, contents) VALUES ('critical', 'critical')",
    )
    .execute(&pool)
    .await
    .expect("critical fixture");
    let site = site_get_latest(&pool, 0, KUBERNETES_DEFAULT, KUBERNETES_DEFAULT_REDACTED)
        .await
        .expect("site_get_latest critical-only");
    print_site("sgl-critical-only", &site);
    print_rows(&pool).await;
}
