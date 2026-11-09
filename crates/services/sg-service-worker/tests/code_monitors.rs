//! M9.8bo differential harness: code monitor trigger store operations.
//!
//! Mirrors `cmd/worker/internal/codemonitors/cm_harness_test.go`. Only runs
//! when `SG_CM_TEST_DSN` names the scratch frontend database. Prints
//! canonical `cm ` lines.

use sg_db::code_monitors::{self, TriggerJob};
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

async fn print_trigger_jobs(pool: &PgPool, label: &str) {
    let rows = sqlx::query(
        "SELECT id, query, state, num_resets, num_failures,
            query_string IS NULL AS query_string_null,
            search_results IS NULL AS results_null
         FROM cm_trigger_jobs ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("trigger jobs");
    for row in rows {
        let id: i32 = row.try_get("id").expect("id");
        let query: i64 = row.try_get("query").expect("query");
        let state: Option<String> = row.try_get("state").expect("state");
        let num_resets: i32 = row.try_get("num_resets").expect("num_resets");
        let num_failures: i32 = row.try_get("num_failures").expect("num_failures");
        let query_string_null: bool = row.try_get("query_string_null").expect("qsn");
        let results_null: bool = row.try_get("results_null").expect("rn");
        println!(
            "cm {label} job id={id} query={query} state={} num_resets={num_resets} num_failures={num_failures} query_string_null={query_string_null} results_null={results_null}",
            format_state(&state)
        );
    }
}

async fn print_action_jobs(pool: &PgPool, label: &str) {
    let rows = sqlx::query("SELECT id, trigger_event FROM cm_action_jobs ORDER BY id")
        .fetch_all(pool)
        .await
        .expect("action jobs");
    for row in rows {
        let id: i32 = row.try_get("id").expect("id");
        let trigger_event: Option<i32> = row.try_get("trigger_event").expect("trigger_event");
        let value = match trigger_event {
            Some(value) => value.to_string(),
            None => "null".to_owned(),
        };
        println!("cm {label} action id={id} trigger_event={value}");
    }
}

fn print_enqueue(label: &str, err: Option<String>, jobs: &[TriggerJob]) {
    println!("cm {label} err={} count={}", escape_err(err), jobs.len());
    for job in jobs {
        println!(
            "cm {label} job id={} query={} state={}",
            job.id,
            job.query,
            format_state(&job.state)
        );
    }
}

#[tokio::test]
async fn cm_harness() {
    let Ok(dsn) = std::env::var("SG_CM_TEST_DSN") else {
        return;
    };
    let pool = sg_db::connect(&dsn, 2).await.expect("connect");

    exec(&pool, "DELETE FROM cm_action_jobs").await;
    exec(&pool, "DELETE FROM cm_trigger_jobs").await;
    exec(&pool, "DELETE FROM cm_queries").await;
    exec(&pool, "DELETE FROM cm_emails").await;
    exec(&pool, "DELETE FROM cm_monitors").await;
    exec(&pool, "DELETE FROM users WHERE id BETWEEN 8200 AND 8299").await;
    exec(
        &pool,
        "ALTER SEQUENCE cm_trigger_jobs_id_seq RESTART WITH 9000",
    )
    .await;
    exec(
        &pool,
        "ALTER SEQUENCE cm_action_jobs_id_seq RESTART WITH 9000",
    )
    .await;

    exec(
        &pool,
        "INSERT INTO users (id, username) VALUES (8201, 'cm-user'), (8202, 'cm-gone')",
    )
    .await;
    exec(&pool, "UPDATE users SET deleted_at = now() WHERE id = 8202").await;
    exec(
        &pool,
        "INSERT INTO cm_monitors (id, created_by, changed_by, description, enabled, namespace_user_id) VALUES
        (7101, 8201, 8201, 'm1', true, 8201),
        (7102, 8201, 8201, 'm2', false, 8201),
        (7103, 8201, 8201, 'm3', true, 8202)",
    )
    .await;
    exec(
        &pool,
        "INSERT INTO cm_queries (id, monitor, query, created_by, changed_by, next_run) VALUES
        (8101, 7101, 'q1', 8201, 8201, now() - interval '1 hour'),
        (8102, 7101, 'q2', 8201, 8201, NULL),
        (8103, 7101, 'q3', 8201, 8201, now() + interval '1 hour'),
        (8104, 7102, 'q4', 8201, 8201, now() - interval '1 hour'),
        (8105, 7103, 'q5', 8201, 8201, now() - interval '1 hour'),
        (8106, 7101, 'q6', 8201, 8201, now() - interval '1 hour')",
    )
    .await;
    exec(
        &pool,
        "INSERT INTO cm_trigger_jobs (id, query, state) VALUES
        (8001, 8102, 'queued'),
        (8002, 8101, 'processing'),
        (8003, 8101, 'completed')",
    )
    .await;

    let jobs = code_monitors::enqueue_query_trigger_jobs(&pool).await;
    match &jobs {
        Ok(jobs) => print_enqueue("enqueue", None, jobs),
        Err(err) => println!(
            "cm enqueue err={} count=0",
            escape_err(Some(err.to_string()))
        ),
    }

    match code_monitors::enqueue_query_trigger_jobs(&pool).await {
        Ok(jobs) => print_enqueue("enqueue-again", None, &jobs),
        Err(err) => println!(
            "cm enqueue-again err={} count=0",
            escape_err(Some(err.to_string()))
        ),
    }

    exec(
        &pool,
        "UPDATE cm_trigger_jobs SET finished_at = now() - interval '40 days' WHERE id IN (8001, 8003)",
    )
    .await;
    exec(
        &pool,
        "UPDATE cm_trigger_jobs SET finished_at = now() - interval '10 days' WHERE query = 8106",
    )
    .await;

    exec(
        &pool,
        "INSERT INTO cm_emails (id, monitor, enabled, priority, header, created_by, changed_by)
        VALUES (6001, 7101, true, 'NORMAL', '', 8201, 8201)",
    )
    .await;
    let new_job_id: i32 = sqlx::query("SELECT id FROM cm_trigger_jobs WHERE query = 8106")
        .fetch_one(&pool)
        .await
        .expect("new job")
        .try_get("id")
        .expect("id");
    exec(
        &pool,
        &format!(
            "INSERT INTO cm_action_jobs (id, email, trigger_event) VALUES
            (7001, 6001, 8001), (7002, 6001, 8003), (7003, 6001, {new_job_id})"
        ),
    )
    .await;

    print_trigger_jobs(&pool, "before-delete").await;
    print_action_jobs(&pool, "before-delete").await;

    match code_monitors::delete_old_trigger_jobs(&pool, 30).await {
        Ok(_) => println!("cm delete-old err=-"),
        Err(err) => println!("cm delete-old err={}", escape_err(Some(err.to_string()))),
    }

    print_trigger_jobs(&pool, "after-delete").await;
    print_action_jobs(&pool, "after-delete").await;
}
