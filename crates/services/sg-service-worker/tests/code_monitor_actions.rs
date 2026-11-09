//! M9.8bp differential harness: code monitor action store operations.
//!
//! Mirrors `cmd/worker/internal/codemonitors/cm_actions_harness_test.go`.
//! Only runs when `SG_CM_TEST_DSN` names the scratch frontend database.
//! Prints canonical `cma ` lines.

use sg_db::code_monitors;
use sqlx::{PgPool, Row};

async fn exec(pool: &PgPool, query: &str) {
    sqlx::query(query).execute(pool).await.expect("exec");
}

fn escape_err(err: Option<String>) -> String {
    match err {
        None => "-".to_owned(),
        // Go's `%q` for the ASCII error strings the baseline produces.
        Some(err) => format!("{err:?}"),
    }
}

fn format_state(state: &Option<String>) -> String {
    match state {
        Some(state) => format!("{state:?}"),
        None => "\"\"".to_owned(),
    }
}

fn format_id(value: Option<i64>) -> String {
    match value {
        Some(value) => value.to_string(),
        None => "null".to_owned(),
    }
}

async fn print_action_jobs(pool: &PgPool, label: &str) {
    let rows = sqlx::query(
        "SELECT id, email, webhook, slack_webhook, trigger_event, state
         FROM cm_action_jobs ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("action jobs");
    for row in rows {
        let id: i32 = row.try_get("id").expect("id");
        let email: Option<i64> = row.try_get("email").expect("email");
        let webhook: Option<i64> = row.try_get("webhook").expect("webhook");
        let slack: Option<i64> = row.try_get("slack_webhook").expect("slack_webhook");
        let trigger: i32 = row.try_get("trigger_event").expect("trigger_event");
        let state: Option<String> = row.try_get("state").expect("state");
        println!(
            "cma {label} action id={id} email={} webhook={} slack={} trigger={trigger} state={}",
            format_id(email),
            format_id(webhook),
            format_id(slack),
            format_state(&state)
        );
    }
}

#[tokio::test]
async fn cm_actions_harness() {
    let Ok(dsn) = std::env::var("SG_CM_TEST_DSN") else {
        return;
    };
    let pool = sg_db::connect(&dsn, 2).await.expect("connect");

    exec(&pool, "DELETE FROM cm_action_jobs").await;
    exec(&pool, "DELETE FROM cm_trigger_jobs").await;
    exec(&pool, "DELETE FROM cm_queries").await;
    exec(&pool, "DELETE FROM cm_emails").await;
    exec(&pool, "DELETE FROM cm_webhooks").await;
    exec(&pool, "DELETE FROM cm_slack_webhooks").await;
    exec(&pool, "DELETE FROM cm_monitors").await;
    exec(&pool, "DELETE FROM users WHERE id BETWEEN 8400 AND 8499").await;
    exec(
        &pool,
        "ALTER SEQUENCE cm_action_jobs_id_seq RESTART WITH 9100",
    )
    .await;

    exec(
        &pool,
        "INSERT INTO users (id, username, display_name)
         VALUES (8401, 'cm-owner', 'Owner Person')",
    )
    .await;
    exec(
        &pool,
        "INSERT INTO cm_monitors (id, created_by, changed_by, description, enabled, namespace_user_id)
         VALUES (7201, 8401, 8401, 'mon-actions', true, 8401)",
    )
    .await;
    exec(
        &pool,
        "INSERT INTO cm_queries (id, monitor, query, created_by, changed_by)
         VALUES (8301, 7201, 'qmon', 8401, 8401)",
    )
    .await;
    exec(
        &pool,
        "INSERT INTO cm_trigger_jobs (id, query, state, query_string, search_results)
         VALUES (8601, 8301, 'completed', 'qmon after:2020-01-01', '[]'::jsonb)",
    )
    .await;
    exec(
        &pool,
        "INSERT INTO cm_emails (id, monitor, enabled, priority, header, created_by, changed_by)
         VALUES (6101, 7201, true, 'NORMAL', '', 8401, 8401),
                (6102, 7201, false, 'NORMAL', '', 8401, 8401)",
    )
    .await;
    exec(
        &pool,
        "INSERT INTO cm_webhooks (id, monitor, url, enabled, created_by, changed_by)
         VALUES (6201, 7201, 'https://example.com/hook', true, 8401, 8401),
                (6202, 7201, 'https://example.com/off', false, 8401, 8401)",
    )
    .await;
    exec(
        &pool,
        "INSERT INTO cm_slack_webhooks (id, monitor, url, enabled, created_by, changed_by)
         VALUES (6301, 7201, 'https://example.com/slack', true, 8401, 8401)",
    )
    .await;

    let mut email_job_id = 0;
    match code_monitors::enqueue_action_jobs_for_monitor(&pool, 7201, 8601).await {
        Ok(jobs) => {
            println!("cma enqueue err=- count={}", jobs.len());
            for job in &jobs {
                if job.email.is_some() {
                    email_job_id = job.id;
                }
            }
        }
        Err(err) => println!(
            "cma enqueue err={} count=0",
            escape_err(Some(err.to_string()))
        ),
    }
    print_action_jobs(&pool, "enqueue").await;

    match code_monitors::enqueue_action_jobs_for_monitor(&pool, 7201, 8601).await {
        Ok(jobs) => println!("cma enqueue-again err=- count={}", jobs.len()),
        Err(err) => println!(
            "cma enqueue-again err={} count=0",
            escape_err(Some(err.to_string()))
        ),
    }

    match code_monitors::get_action_job_metadata(&pool, email_job_id).await {
        Ok(meta) => {
            let results_len = meta.results.as_array().map_or(0, |rows| rows.len());
            // Go scans `query_string` into a plain string; a NULL would error
            // there, so the fallback only keeps the Rust print total.
            let query = meta.query.as_deref().unwrap_or("");
            println!(
                "cma metadata id={email_job_id} err=- description={:?} monitor={} query={query:?} owner={:?} results_len={results_len}",
                meta.description, meta.monitor_id, meta.owner_name
            );
        }
        Err(err) => println!(
            "cma metadata id={email_job_id} err={}",
            escape_err(Some(err.to_string()))
        ),
    }

    // Complete the webhook job; only that action type is eligible again.
    exec(
        &pool,
        "UPDATE cm_action_jobs SET state = 'completed', finished_at = now() WHERE webhook IS NOT NULL",
    )
    .await;
    match code_monitors::enqueue_action_jobs_for_monitor(&pool, 7201, 8601).await {
        Ok(jobs) => println!("cma enqueue-after-complete err=- count={}", jobs.len()),
        Err(err) => println!(
            "cma enqueue-after-complete err={} count=0",
            escape_err(Some(err.to_string()))
        ),
    }
    print_action_jobs(&pool, "after-complete").await;
}
