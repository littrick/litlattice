#![forbid(unsafe_code)]

//! Database-backed tests for the Batch Changes changeset syncer's queries
//! (`sg_db::batches`), against the baseline schema.
//!
//! Gated on `SG_TEST_DATABASE_DSN` (isolated `sourcegraph_rust_test`).

use sg_db::batches::BatchesStore;
use sqlx::postgres::PgPool;

fn unique_suffix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("csync{nanos}")
}

#[tokio::test]
async fn list_code_hosts_returns_only_supported_types() {
    let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
        eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
        return;
    };
    let pool: PgPool = sg_db::connect(&dsn, 5).await.expect("connect");
    let store = BatchesStore::new(pool.clone());
    let suffix = unique_suffix();
    let supported_id = format!("https://github-{suffix}.example.com");
    let unsupported_id = format!("https://other-{suffix}.example.com");

    let supported_repo: i32 = sqlx::query_scalar(
        "INSERT INTO repo(name, uri, external_service_type, external_service_id) \
         VALUES ($1, $1, 'github', $2) RETURNING id",
    )
    .bind(format!("{suffix}.example.com/owner/supported"))
    .bind(&supported_id)
    .fetch_one(&pool)
    .await
    .expect("insert supported repo");
    let unsupported_repo: i32 = sqlx::query_scalar(
        "INSERT INTO repo(name, uri, external_service_type, external_service_id) \
         VALUES ($1, $1, 'other', $2) RETURNING id",
    )
    .bind(format!("{suffix}.example.com/owner/unsupported"))
    .bind(&unsupported_id)
    .fetch_one(&pool)
    .await
    .expect("insert unsupported repo");

    let result = async {
        let hosts = store
            .list_code_hosts(&[], false, false)
            .await
            .expect("list");
        let ours: Vec<_> = hosts
            .iter()
            .filter(|host| host.external_service_id.contains(&suffix))
            .collect();
        assert_eq!(ours.len(), 1, "only the github repo is a code host");
        assert_eq!(ours[0].external_service_type, "github");
        assert_eq!(ours[0].external_service_id, supported_id);
        assert!(!ours[0].requires_ssh);
        assert!(!ours[0].has_webhooks);

        // `ListCodeHostsOpts.RepoIDs` filters within the supported set.
        let hosts = store
            .list_code_hosts(&[supported_repo, unsupported_repo], false, false)
            .await
            .expect("list by repo ids");
        let ours: Vec<_> = hosts
            .iter()
            .filter(|host| host.external_service_id.contains(&suffix))
            .collect();
        assert_eq!(ours.len(), 1);
        Ok::<_, sqlx::Error>(())
    }
    .await;

    sqlx::query("DELETE FROM repo WHERE id = ANY($1)")
        .bind(vec![supported_repo, unsupported_repo])
        .execute(&pool)
        .await
        .expect("cleanup");
    result.expect("assertions");
}

#[tokio::test]
async fn list_changeset_sync_data_and_precondition_queries_match_the_baseline_sql() {
    let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
        eprintln!("SG_TEST_DATABASE_DSN unset; skipping");
        return;
    };
    let pool: PgPool = sg_db::connect(&dsn, 5).await.expect("connect");
    let store = BatchesStore::new(pool.clone());
    let suffix = unique_suffix();
    let service_id = format!("https://github-{suffix}.example.com");

    let repo_id: i32 = sqlx::query_scalar(
        "INSERT INTO repo(name, uri, external_service_type, external_service_id) \
         VALUES ($1, $1, 'github', $2) RETURNING id",
    )
    .bind(format!("{suffix}.example.com/owner/repo"))
    .bind(&service_id)
    .fetch_one(&pool)
    .await
    .expect("insert repo");

    let user_id: i32 = sqlx::query_scalar("INSERT INTO users(username) VALUES ($1) RETURNING id")
        .bind(format!("user-{suffix}"))
        .fetch_one(&pool)
        .await
        .expect("insert user");

    // `batch_changes.batch_spec_id` and `batch_specs.batch_change_id`
    // reference each other; both FKs are DEFERRABLE.
    let mut tx = pool.begin().await.expect("begin");
    sqlx::query("SET CONSTRAINTS ALL DEFERRED")
        .execute(&mut *tx)
        .await
        .expect("defer constraints");
    let spec_id: i64 = sqlx::query_scalar(
        "INSERT INTO batch_specs(rand_id, raw_spec, namespace_user_id) \
         VALUES ($1, '', $2) RETURNING id",
    )
    .bind(format!("spec-{suffix}"))
    .bind(user_id)
    .fetch_one(&mut *tx)
    .await
    .expect("insert batch spec");
    let batch_change_id: i64 = sqlx::query_scalar(
        "INSERT INTO batch_changes(name, namespace_user_id, batch_spec_id) \
         VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(format!("bc.{suffix}"))
    .bind(user_id)
    .bind(spec_id)
    .fetch_one(&mut *tx)
    .await
    .expect("insert batch change");
    sqlx::query("UPDATE batch_specs SET batch_change_id = $1 WHERE id = $2")
        .bind(batch_change_id)
        .bind(spec_id)
        .execute(&mut *tx)
        .await
        .expect("link batch spec");
    let changeset_id: i64 = sqlx::query_scalar(
        "INSERT INTO changesets( \
             repo_id, external_service_type, batch_change_ids, \
             publication_state, reconciler_state, computed_state, external_state, external_updated_at) \
         VALUES ($1, 'github', jsonb_build_object($2::text, '{}'::jsonb), \
                 'PUBLISHED', 'completed', 'OPEN', 'OPEN', now() - '1 hour'::interval) \
         RETURNING id",
    )
    .bind(repo_id)
    .bind(batch_change_id.to_string())
    .fetch_one(&mut *tx)
    .await
    .expect("insert changeset");
    tx.commit().await.expect("commit");

    let result = async {
        // All changesets on the code host.
        let data = store
            .list_changeset_sync_data(&[], &service_id)
            .await
            .expect("sync data by host");
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].changeset_id, changeset_id);
        assert_eq!(data[0].repo_external_service_id, service_id);
        assert!(data[0].external_updated_at.is_some());

        // Explicit id filter.
        let data = store
            .list_changeset_sync_data(&[changeset_id], "")
            .await
            .expect("sync data by ids");
        assert_eq!(data.len(), 1);

        // Preconditions match: published + completed.
        assert_eq!(
            store
                .get_changeset_for_sync(changeset_id)
                .await
                .expect("get"),
            Some(repo_id)
        );
        sqlx::query("UPDATE changesets SET reconciler_state = 'queued' WHERE id = $1")
            .bind(changeset_id)
            .execute(&pool)
            .await
            .expect("move state");
        assert_eq!(
            store
                .get_changeset_for_sync(changeset_id)
                .await
                .expect("get"),
            None
        );
        // ... and the sync data query keeps the same precondition.
        assert!(store
            .list_changeset_sync_data(&[changeset_id], "")
            .await
            .expect("sync data")
            .is_empty());

        // `EnqueueChangesetSyncsForRepos` lists the changeset regardless of
        // its reconciler state (`ListChangesets` has no state filter).
        assert_eq!(
            store
                .list_changeset_ids_for_repos(&[repo_id])
                .await
                .expect("ids"),
            vec![changeset_id]
        );
        Ok::<_, sqlx::Error>(())
    }
    .await;

    sqlx::query("DELETE FROM changesets WHERE id = $1")
        .bind(changeset_id)
        .execute(&pool)
        .await
        .expect("delete changeset");
    sqlx::query("DELETE FROM batch_changes WHERE id = $1")
        .bind(batch_change_id)
        .execute(&pool)
        .await
        .expect("delete batch change");
    sqlx::query("DELETE FROM batch_specs WHERE id = $1")
        .bind(spec_id)
        .execute(&pool)
        .await
        .expect("delete batch spec");
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("delete user");
    sqlx::query("DELETE FROM repo WHERE id = $1")
        .bind(repo_id)
        .execute(&pool)
        .await
        .expect("delete repo");

    result.expect("assertions");
}
