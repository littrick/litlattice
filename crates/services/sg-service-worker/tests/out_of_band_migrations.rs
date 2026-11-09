//! M9.8bn differential harness: the `out-of-band-migrations` job.
//!
//! Mirrors `cmd/worker/internal/migrations/oob_harness_test.go`. Only runs
//! when `SG_OOB_TEST_DSN` names the scratch frontend database. Prints
//! canonical `oob ` lines.

use sg_db::out_of_band_migrations as store;
use sg_db::OffsetDateTime;
use sg_service_worker::jobs::out_of_band_migrations::{
    current_version, first_service_version, new_version_and_patch_from_string, parse_version,
    schedule_migration_interrupts, upgrade_range, ExternalServiceWebhookMigrator,
    MigrationInterrupt, Migrator, Runner, UserRoleAssignmentMigrator, Version,
};
use sqlx::{PgPool, Row};

fn v(major: i32, minor: i32) -> Version {
    Version::new(major, minor)
}

fn escape_err(err: Option<String>) -> String {
    match err {
        None => "-".to_owned(),
        Some(err) => err.replace('\n', "\\n"),
    }
}

fn ids_string(ids: &[i32]) -> String {
    format!(
        "[{}]",
        ids.iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(" ")
    )
}

fn format_interrupts(interrupts: &[MigrationInterrupt]) -> String {
    interrupts
        .iter()
        .map(|interrupt| {
            format!(
                "{}:{}",
                interrupt.version.to_version_string(),
                ids_string(&interrupt.migration_ids)
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

async fn exec(pool: &PgPool, query: &str) {
    sqlx::query(query).execute(pool).await.expect("exec");
}

fn deprecated(migration: &store::Migration) -> (i32, i32) {
    (
        migration.deprecated_major.unwrap_or_default(),
        migration.deprecated_minor.unwrap_or_default(),
    )
}

async fn print_external_service_rows(pool: &PgPool) {
    let rows = sqlx::query(
        "SELECT id, has_webhooks FROM external_services WHERE id BETWEEN 9000 AND 9099 ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("external services");
    for row in rows {
        let id: i64 = row.try_get("id").expect("id");
        let has_webhooks: Option<bool> = row.try_get("has_webhooks").expect("has_webhooks");
        let value = match has_webhooks {
            Some(value) => value.to_string(),
            None => "null".to_owned(),
        };
        println!("oob extsvc row id={id} has_webhooks={value}");
    }
}

async fn print_user_role_rows(pool: &PgPool) {
    let rows = sqlx::query(
        "SELECT ur.user_id, r.name FROM user_roles ur JOIN roles r ON r.id = ur.role_id
		WHERE ur.user_id BETWEEN 8300 AND 8399 ORDER BY ur.user_id, r.name",
    )
    .fetch_all(pool)
    .await
    .expect("user roles");
    let mut parts: Vec<String> = rows
        .iter()
        .map(|row| {
            let user_id: i32 = row.try_get("user_id").expect("user_id");
            let name: String = row.try_get("name").expect("name");
            format!("{user_id}:{name}")
        })
        .collect();
    parts.sort();
    println!("oob roles rows={}", parts.join(","));
}

#[tokio::test]
async fn oob_harness() {
    let Ok(dsn) = std::env::var("SG_OOB_TEST_DSN") else {
        return;
    };
    let pool = sg_db::connect(&dsn, 2).await.expect("connect");

    // ---- versions / currentVersion ---------------------------------------
    for input in [
        "1.2.3",
        "v5.2",
        "5.2+dev",
        "1.2.3-rc.1",
        "2023.03.23+204874.db2922",
        "v4.5",
        "",
    ] {
        let (version, patch, ok) = new_version_and_patch_from_string(input);
        println!(
            "oob parse input={input:?} ok={ok} major={} minor={} patch={patch} dev={}",
            version.major, version.minor, version.dev
        );
    }

    println!(
        "oob next major=4 minor=5 -> {}",
        v(4, 5).next().to_version_string()
    );
    println!(
        "oob next major=3 minor=43 -> {}",
        v(3, 43).next().to_version_string()
    );
    for (major, minor) in [(4, 0), (5, 0), (2, 0)] {
        let (previous, ok) = v(major, minor).previous();
        println!(
            "oob previous major={major} minor={minor} -> {} ok={ok}",
            previous.to_version_string()
        );
    }

    for (from, to) in [(v(4, 3), v(4, 5)), (v(4, 4), v(4, 6)), (v(4, 6), v(4, 6))] {
        match upgrade_range(from, to) {
            Ok(versions) => {
                let parts: Vec<String> = versions
                    .iter()
                    .map(|version| version.to_version_string())
                    .collect();
                println!(
                    "oob range from={} to={} versions={} err=-",
                    from.to_version_string(),
                    to.to_version_string(),
                    parts.join(",")
                );
            }
            Err(err) => {
                println!(
                    "oob range from={} to={} versions= err={}",
                    from.to_version_string(),
                    to.to_version_string(),
                    escape_err(Some(err))
                );
            }
        }
    }

    for (from, to) in [(v(3, 30), v(4, 1)), (v(4, 1), v(3, 30)), (v(4, 6), v(4, 6))] {
        let interrupts = schedule_migration_interrupts(from, to).expect("interrupts");
        println!(
            "oob schedule from={} to={} interrupts={} err=-",
            from.to_version_string(),
            to.to_version_string(),
            format_interrupts(&interrupts)
        );
    }

    let panic = std::panic::catch_unwind(|| {
        let _ = schedule_migration_interrupts(v(4, 6), v(4, 0));
    });
    match panic {
        Ok(_) => println!("oob schedule from=4.6 to=4.0 panic=-"),
        Err(payload) => {
            let message = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
                .unwrap_or_else(|| "<non-string panic>".to_owned());
            println!("oob schedule from=4.6 to=4.0 panic={message}");
        }
    }

    for forced in ["4.5", "nope"] {
        std::env::set_var("SRC_OOBMIGRATION_CURRENT_VERSION", forced);
        match current_version() {
            Ok(version) => println!(
                "oob current env={forced:?} -> {} err=-",
                version.to_version_string()
            ),
            Err(err) => println!(
                "oob current env={forced:?} -> 0.0 err={}",
                escape_err(Some(err))
            ),
        }
    }
    std::env::remove_var("SRC_OOBMIGRATION_CURRENT_VERSION");

    for input in ["v4.5.0", "4.5.1", "2023.03.23+204874.db2922", "1.2.3-rc.1"] {
        match parse_version(input) {
            Some(version) => println!(
                "oob parse-release input={input:?} -> {} ok=true",
                version.to_version_string()
            ),
            None => println!("oob parse-release input={input:?} -> 0.0 ok=false"),
        }
    }

    // ---- store: clean slate, then synchronize metadata -------------------
    exec(&pool, "DELETE FROM out_of_band_migrations").await;
    exec(
        &pool,
        r#"INSERT INTO out_of_band_migrations
		(id, team, component, description, created, non_destructive, introduced_version_major, introduced_version_minor)
		VALUES (999, 'unknown', 'unknown.component', 'not in the yaml', now(), true, 1, 0)"#,
    )
    .await;
    exec(&pool, "DELETE FROM versions WHERE service = 'frontend'").await;

    let first = first_service_version(&pool).await.expect("first");
    println!(
        "oob first-version -> {:?} ok={} err=-",
        first.clone().unwrap_or_default(),
        first.is_some()
    );
    exec(
        &pool,
        "INSERT INTO versions (service, version, updated_at, first_version) VALUES ('frontend', '4.6.0', now(), '4.6.0')",
    )
    .await;
    exec(
        &pool,
        "UPDATE versions SET first_version = '4.0.0' WHERE service = 'frontend'",
    )
    .await;
    let first = first_service_version(&pool).await.expect("first");
    println!(
        "oob first-version -> {:?} ok={} err=-",
        first.clone().unwrap_or_default(),
        first.is_some()
    );

    let sync_err = store::synchronize_metadata(&pool).await.err();
    println!("oob store sync err={}", escape_err(sync_err));

    let migrations = store::list(&pool).await.expect("list");
    println!("oob store list count={} err=-", migrations.len());
    for id in [13, 19] {
        let migration = store::get_by_id(&pool, id)
            .await
            .expect("get")
            .expect("found");
        let (deprecated_major, deprecated_minor) = deprecated(&migration);
        println!(
            "oob store get id={id} found=true team={} component={} introduced={}.{} deprecated={deprecated_major}.{deprecated_minor} non_destructive={} enterprise={} progress={:.6} err=-",
            migration.team,
            migration.component,
            migration.introduced_major,
            migration.introduced_minor,
            migration.non_destructive,
            migration.is_enterprise,
            migration.progress
        );
    }
    let found_unknown = store::get_by_id(&pool, 999).await.expect("get").is_some();
    println!("oob store get id=999 found={found_unknown}");

    store::update_progress(&pool, 19, 0.25)
        .await
        .expect("progress");
    let migration = store::get_by_id(&pool, 19)
        .await
        .expect("get")
        .expect("found");
    println!("oob store progress id=19 value={:.6}", migration.progress);
    let before = migration.last_updated;
    store::update_progress(&pool, 19, 0.25)
        .await
        .expect("progress");
    let migration = store::get_by_id(&pool, 19)
        .await
        .expect("get")
        .expect("found");
    println!(
        "oob store progress-again unchanged={}",
        before.is_some() && migration.last_updated.is_some() && before == migration.last_updated
    );

    store::update_direction(&pool, 19, true)
        .await
        .expect("direction");
    let migration = store::get_by_id(&pool, 19)
        .await
        .expect("get")
        .expect("found");
    println!(
        "oob store direction id=19 apply_reverse={}",
        migration.apply_reverse
    );

    store::update_metadata(&pool, 19, &serde_json::json!({"hello": 1}))
        .await
        .expect("metadata");
    let migration = store::get_by_id(&pool, 19)
        .await
        .expect("get")
        .expect("found");
    let hello = migration.metadata["hello"].clone();
    println!("oob store metadata id=19 hello={hello}");

    for ids in [vec![13, 19], vec![13, 19, 998]] {
        match store::get_by_ids(&pool, &ids).await {
            Ok(migrations) => {
                let found: Vec<i32> = migrations.iter().map(|m| m.id).collect();
                println!(
                    "oob store get-by-ids ids={} found={} err=-",
                    ids_string(&ids),
                    ids_string(&found)
                );
            }
            Err(err) => println!(
                "oob store get-by-ids ids={} found=[] err={}",
                ids_string(&ids),
                escape_err(Some(err))
            ),
        }
    }

    exec(&pool, "DELETE FROM out_of_band_migrations_errors").await;
    let base = OffsetDateTime::now_utc() - time::Duration::hours(1);
    for index in 0..100 {
        sqlx::query("INSERT INTO out_of_band_migrations_errors (migration_id, message, created) VALUES (19, $1, $2)")
            .bind(format!("e{index:03}"))
            .bind(base + time::Duration::seconds(index))
            .execute(&pool)
            .await
            .expect("insert error");
    }
    store::add_error(&pool, 19, "newest")
        .await
        .expect("add error");
    let migration = store::get_by_id(&pool, 19)
        .await
        .expect("get")
        .expect("found");
    let has_oldest = migration.errors.iter().any(|e| e.message == "e000");
    let has_newest = migration.errors.iter().any(|e| e.message == "newest");
    let oldest = migration
        .errors
        .last()
        .map(|e| e.message.clone())
        .unwrap_or_else(|| "-".to_owned());
    println!(
        "oob store errors count={} has_e000={} has_newest={} oldest={}",
        migration.errors.len(),
        has_oldest,
        has_newest,
        oldest
    );

    // ---- validation ------------------------------------------------------
    let runner = Runner::new(pool.clone());
    for (label, current, first, progress_ids) in [
        ("unfinished", v(4, 6), v(4, 0), vec![19]),
        ("rollback", v(3, 30), v(3, 20), vec![13]),
        ("two-errors", v(4, 6), v(4, 0), vec![19, 20]),
        ("fresh", v(4, 6), v(4, 6), vec![19]),
        ("finished", v(4, 6), v(4, 0), vec![19]),
    ] {
        exec(
            &pool,
            "UPDATE out_of_band_migrations SET progress = 0, apply_reverse = false",
        )
        .await;
        match label {
            "finished" => {
                exec(
                    &pool,
                    "UPDATE out_of_band_migrations SET progress = 1 WHERE id = 19",
                )
                .await
            }
            _ => {
                for id in progress_ids {
                    sqlx::query("UPDATE out_of_band_migrations SET progress = 0.5 WHERE id = $1")
                        .bind(id)
                        .execute(&pool)
                        .await
                        .expect("set progress");
                }
            }
        }
        let err = runner.validate(current, first).await.err();
        println!("oob validate case={label} err={}", escape_err(err));
    }
    exec(
        &pool,
        "UPDATE out_of_band_migrations SET progress = 0, apply_reverse = false",
    )
    .await;

    // ---- migrator 13: external service webhooks --------------------------
    exec(
        &pool,
        "DELETE FROM external_services WHERE id BETWEEN 9000 AND 9099",
    )
    .await;
    let rows: [(i64, &str, &str, bool); 9] = [
        (9001, "github", r#"{"webhooks":[{"secret":"s"}]}"#, false),
        (9002, "github", r#"{"webhooks":[]}"#, false),
        (9003, "gitlab", r#"{"webhooks":[{"secret":""}]}"#, false),
        (
            9004,
            "bitbucketserver",
            r#"{"webhooks":{"secret":"s"}}"#,
            false,
        ),
        (
            9005,
            "bitbucketserver",
            r#"{"plugin":{"webhooks":{"secret":"s"}}}"#,
            false,
        ),
        (
            9006,
            "bitbucketserver",
            r#"{"webhooks":{"secret":""},"plugin":{"webhooks":{"secret":""}}}"#,
            false,
        ),
        (9007, "github", r#"{"webhooks":[{"secret":"s"}]}"#, true),
        (9008, "other", r#"{"anything":1}"#, false),
        (
            9009,
            "github",
            "{\"webhooks\": [/* comment */ {\"secret\": \"s\"}]}",
            false,
        ),
    ];
    for (id, kind, config, deleted) in rows {
        let deleted_at: Option<&str> = deleted.then_some("2035-01-01T00:00:00Z");
        sqlx::query(
            "INSERT INTO external_services (id, kind, display_name, config, deleted_at) VALUES ($1, $2, $3, $4, $5::timestamptz)",
        )
        .bind(id)
        .bind(kind)
        .bind(format!("svc-{id}"))
        .bind(config)
        .bind(deleted_at)
        .execute(&pool)
        .await
        .expect("insert external service");
    }

    let webhook_migrator = ExternalServiceWebhookMigrator::new(pool.clone(), None, 50);
    let progress = webhook_migrator.progress(false).await.expect("progress");
    println!("oob extsvc progress before={progress:.6} err=-");
    println!(
        "oob extsvc up err={}",
        escape_err(webhook_migrator.up().await.err())
    );
    let progress = webhook_migrator.progress(false).await.expect("progress");
    println!("oob extsvc progress after={progress:.6} err=-");
    print_external_service_rows(&pool).await;
    println!(
        "oob extsvc up2 err={}",
        escape_err(webhook_migrator.up().await.err())
    );
    print_external_service_rows(&pool).await;
    println!(
        "oob extsvc down err={}",
        escape_err(webhook_migrator.down().await.err())
    );

    // ---- migrator 19: user role assignment -------------------------------
    exec(&pool, "DELETE FROM user_roles").await;
    exec(
        &pool,
        "DELETE FROM roles WHERE name IN ('USER', 'SITE_ADMINISTRATOR', 'OOB_CUSTOM')",
    )
    .await;
    exec(&pool, "DELETE FROM users WHERE id BETWEEN 8300 AND 8399").await;
    exec(
        &pool,
        "INSERT INTO roles (name, system) VALUES ('USER', true), ('SITE_ADMINISTRATOR', true), ('OOB_CUSTOM', false)",
    )
    .await;
    for (id, site_admin) in [(8301, false), (8302, true), (8303, false), (8304, false)] {
        sqlx::query("INSERT INTO users (id, username, site_admin) VALUES ($1, $2, $3)")
            .bind(id)
            .bind(format!("oob-user-{id}"))
            .bind(site_admin)
            .execute(&pool)
            .await
            .expect("insert user");
    }
    exec(
        &pool,
        "INSERT INTO user_roles (user_id, role_id) SELECT 8303, id FROM roles WHERE name = 'USER'",
    )
    .await;
    exec(
        &pool,
        "INSERT INTO user_roles (user_id, role_id) SELECT 8304, id FROM roles WHERE name = 'OOB_CUSTOM'",
    )
    .await;

    let role_migrator = UserRoleAssignmentMigrator::new(pool.clone(), 250);
    let progress = role_migrator.progress(false).await.expect("progress");
    println!("oob roles progress before={progress:.6} err=-");
    println!(
        "oob roles up err={}",
        escape_err(role_migrator.up().await.err())
    );
    let progress = role_migrator.progress(false).await.expect("progress");
    println!("oob roles progress after={progress:.6} err=-");
    print_user_role_rows(&pool).await;
    println!(
        "oob roles up2 err={}",
        escape_err(role_migrator.up().await.err())
    );
    print_user_role_rows(&pool).await;
    println!(
        "oob roles down err={}",
        escape_err(role_migrator.down().await.err())
    );
}
