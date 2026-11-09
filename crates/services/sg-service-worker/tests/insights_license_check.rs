#![forbid(unsafe_code)]

//! Differential harness for the insights license monitor (M9.8cf).
//!
//! Mirror of the Go harness
//! `internal/insights/background/parity_license_check_harness_test.go`: same
//! fixture, same corpus, same canonical `lc ` lines. Both sides reset the
//! scratch codeinsights database before every corpus operation, script the
//! license outcome, and read the resulting view/dashboard state back.
//!
//! Gated by `SG_INSIGHTS_LICENSE_CHECK_CORPUS`; connections come from
//! `SG_QTEST_INSIGHTS_DSN` and `SG_LC_EMPTY_DSN`.

use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;

use sg_service_worker::jobs::insights_license_check::{
    LicenseCheck, LICENSE_CHECK_DESCRIPTION, LICENSE_CHECK_INTERVAL, LICENSE_CHECK_NAME,
};

type LicenseCheckFn = Box<dyn Fn() -> Result<(), String> + Send + Sync>;

async fn reset(pool: &PgPool, fixture: &str) {
    sqlx::raw_sql(fixture)
        .execute(pool)
        .await
        .expect("reset fixture");
}

/// The error text differs between lib/pq and sqlx; only the stage is
/// compared.
fn stage_of(message: &str) -> &'static str {
    const STAGES: [&str; 7] = [
        "UnfreezeAllInsights",
        "GetUnfrozenInsightCount",
        "FreezeAllInsights",
        "UnfreezeGlobalInsights",
        "EnsureLimitedAccessModeDashboard",
        "GetUnfrozenInsightIds",
        "AddViewsToDashboard",
    ];
    for stage in STAGES {
        if message.contains(stage) {
            return stage;
        }
    }
    "other"
}

fn null_string(value: Option<String>) -> String {
    value.unwrap_or_else(|| "null".to_owned())
}

fn null_int(value: Option<i32>) -> String {
    value.map_or_else(|| "null".to_owned(), |value| value.to_string())
}

fn null_bool(value: Option<bool>) -> String {
    value.map_or_else(|| "null".to_owned(), |value| value.to_string())
}

async fn print_state(insights: &PgPool, label: &str) {
    let views = sqlx::query("SELECT id, unique_id, is_frozen FROM insight_view ORDER BY id")
        .fetch_all(insights)
        .await
        .expect("read views");
    for row in &views {
        let id: i32 = row.try_get("id").expect("id");
        let unique_id: String = row.try_get("unique_id").expect("unique_id");
        let frozen: bool = row.try_get("is_frozen").expect("is_frozen");
        println!("lc {label} view id={id} unique_id={unique_id} frozen={frozen}");
    }
    println!("lc {label} view count={}", views.len());

    let dashboards = sqlx::query(
        "SELECT id, title, type, deleted_at IS NOT NULL AS deleted FROM dashboard ORDER BY id",
    )
    .fetch_all(insights)
    .await
    .expect("read dashboards");
    for row in &dashboards {
        let id: i32 = row.try_get("id").expect("id");
        let title: Option<String> = row.try_get("title").expect("title");
        let dashboard_type: String = row.try_get("type").expect("type");
        let deleted: bool = row.try_get("deleted").expect("deleted");
        println!(
            "lc {label} dashboard id={id} title={} type={dashboard_type} deleted={deleted}",
            null_string(title)
        );
    }
    println!("lc {label} dashboard count={}", dashboards.len());

    let grants = sqlx::query(
        "SELECT dashboard_id, user_id, org_id, global FROM dashboard_grants \
         ORDER BY dashboard_id, user_id NULLS FIRST, org_id NULLS FIRST, global NULLS FIRST",
    )
    .fetch_all(insights)
    .await
    .expect("read grants");
    for row in &grants {
        let dashboard_id: i32 = row.try_get("dashboard_id").expect("dashboard_id");
        let user_id: Option<i32> = row.try_get("user_id").expect("user_id");
        let org_id: Option<i32> = row.try_get("org_id").expect("org_id");
        let global: Option<bool> = row.try_get("global").expect("global");
        println!(
            "lc {label} grant dashboard_id={dashboard_id} user_id={} org_id={} global={}",
            null_int(user_id),
            null_int(org_id),
            null_bool(global)
        );
    }
    println!("lc {label} grant count={}", grants.len());

    let links = sqlx::query(
        "SELECT dashboard_id, insight_view_id FROM dashboard_insight_view \
         ORDER BY dashboard_id, insight_view_id",
    )
    .fetch_all(insights)
    .await
    .expect("read links");
    for row in &links {
        let dashboard_id: i32 = row.try_get("dashboard_id").expect("dashboard_id");
        let insight_view_id: i32 = row.try_get("insight_view_id").expect("insight_view_id");
        println!("lc {label} link dashboard_id={dashboard_id} insight_view_id={insight_view_id}");
    }
    println!("lc {label} link count={}", links.len());
}

#[tokio::test]
async fn insights_license_check_differential() {
    let corpus_path = match std::env::var("SG_INSIGHTS_LICENSE_CHECK_CORPUS") {
        Ok(path) if !path.is_empty() => path,
        _ => return,
    };
    let insights_dsn = std::env::var("SG_QTEST_INSIGHTS_DSN").expect("insights dsn");
    let empty_dsn = std::env::var("SG_LC_EMPTY_DSN").expect("empty dsn");
    let fixture_path =
        std::env::var("SG_INSIGHTS_LICENSE_CHECK_FIXTURE").expect("license check fixture");

    let insights = PgPoolOptions::new()
        .max_connections(4)
        .connect(&insights_dsn)
        .await
        .expect("connect codeinsights");
    let empty = PgPoolOptions::new()
        .max_connections(2)
        .connect(&empty_dsn)
        .await
        .expect("connect empty");

    let fixture = std::fs::read_to_string(fixture_path).expect("fixture");

    println!(
        "lc routine name={:?} description={:?} interval={}",
        LICENSE_CHECK_NAME,
        LICENSE_CHECK_DESCRIPTION,
        LICENSE_CHECK_INTERVAL.as_secs()
    );

    let corpus = std::fs::read_to_string(&corpus_path).expect("read corpus");
    for line in corpus.lines() {
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        let op = parts[0];
        let label = parts[1];
        reset(&insights, &fixture).await;

        let (check, target): (LicenseCheckFn, &PgPool) = match op {
            "licensed" => (Box::new(|| Ok(())), &insights),
            "unlicensed" => {
                sqlx::raw_sql("UPDATE insight_view SET is_frozen = FALSE")
                    .execute(&insights)
                    .await
                    .expect("freeze prep");
                (Box::new(|| Err("error".to_owned())), &insights)
            }
            "unlicensed-org" => {
                sqlx::raw_sql("UPDATE insight_view SET is_frozen = FALSE WHERE id = 1")
                    .execute(&insights)
                    .await
                    .expect("org prep");
                (Box::new(|| Err("error".to_owned())), &insights)
            }
            "unlicensed-existing" => {
                sqlx::raw_sql(
                    "UPDATE insight_view SET is_frozen = FALSE;\n                     INSERT INTO dashboard (id, title, type, deleted_at) VALUES \n                     (5, 'Old LAM', 'limited_access_mode', '2021-01-01 00:00:00');",
                )
                .execute(&insights)
                .await
                .expect("existing prep");
                (Box::new(|| Err("error".to_owned())), &insights)
            }
            "unlicensed-noop" => (Box::new(|| Err("error".to_owned())), &insights),
            "broken" => (Box::new(|| Err("error".to_owned())), &empty),
            other => panic!("unknown op {other:?}"),
        };

        let routine = LicenseCheck::new(target.clone(), check);
        match routine.check_and_enforce_license().await {
            Ok(()) => println!("lc {label} result=ok"),
            Err(message) => println!("lc {label} result=error stage={}", stage_of(&message)),
        }
        print_state(&insights, label).await;
    }
}
