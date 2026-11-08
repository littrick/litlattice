#![forbid(unsafe_code)]

//! M9.8am differential harness (Rust side): the `permission-sync-job-scheduler`
//! job.
//!
//! Mirrors `cmd/worker/internal/permissions/scheduler_harness_test.go`.
//! `test_ws/p9-go/run_permission_sync_job_scheduler_diff.sh` applies
//! `permission_sync_job_scheduler_fixtures.sql` immediately before each side
//! and diffs the two canonical `ps ` outputs.

use sg_config::SiteConfiguration;
use sg_db::permission_sync_jobs::{create_repo_sync_job, create_user_sync_job};
use sg_db::OffsetDateTime;
use sg_keyring::Keyring;
use sg_service_worker::jobs::permissions::{
    authz_provider_urns, get_schedule, oldest_repo_permissions_batch_size,
    oldest_user_permissions_batch_size, permission_syncing_disabled, schedule_interval,
    schedule_jobs, Backoff, PermissionSyncJobScheduler, Schedule, ScheduledRepo, ScheduledUser,
    MEDIUM_PRIORITY_PERMISSIONS_SYNC, REASON_REPO_NO_PERMS, REASON_REPO_OUTDATED_PERMS,
    REASON_USER_NO_PERMS, REASON_USER_OUTDATED_PERMS, SCHEDULER_ROUTINE_NAME,
};
use sg_service_worker::jobs::Job;
use sqlx::{PgPool, Row};

fn flag(value: bool) -> u8 {
    u8::from(value)
}

fn site(json: &str) -> SiteConfiguration {
    SiteConfiguration::parse(json).expect("valid site config")
}

fn print_schedule(tag: &str, schedule: &Schedule) {
    let mut users: Vec<&ScheduledUser> = schedule.users.iter().collect();
    users.sort_by(|a, b| (a.user_id, a.reason).cmp(&(b.user_id, b.reason)));
    for user in users {
        println!(
            "ps sched {tag} user={} reason={} priority={} no_perms={}",
            user.user_id,
            user.reason,
            user.priority,
            flag(user.no_perms),
        );
    }

    let mut repos: Vec<&ScheduledRepo> = schedule.repos.iter().collect();
    repos.sort_by(|a, b| (a.repo_id, a.reason).cmp(&(b.repo_id, b.reason)));
    for repo in repos {
        println!(
            "ps sched {tag} repo={} reason={} priority={} no_perms={}",
            repo.repo_id,
            repo.reason,
            repo.priority,
            flag(repo.no_perms),
        );
    }

    println!(
        "ps sched_count {tag} users={} repos={}",
        schedule.users.len(),
        schedule.repos.len()
    );
}

fn opt_id(value: Option<i32>) -> String {
    value.map_or_else(|| "-".to_owned(), |id| id.to_string())
}

async fn print_database_harness(pool: &PgPool) {
    let default_site = site("{}");

    let zero = get_schedule(pool, &default_site, Backoff::Zero)
        .await
        .expect("get_schedule zero");
    print_schedule("zero", &zero);

    let conf = get_schedule(pool, &default_site, Backoff::Conf)
        .await
        .expect("get_schedule conf");
    let mut conf_users: Vec<i32> = conf
        .users
        .iter()
        .filter(|user| user.reason == REASON_USER_OUTDATED_PERMS)
        .map(|user| user.user_id)
        .collect();
    let mut conf_repos: Vec<i32> = conf
        .repos
        .iter()
        .filter(|repo| repo.reason == REASON_REPO_OUTDATED_PERMS)
        .map(|repo| repo.repo_id)
        .collect();
    conf_users.sort_unstable();
    conf_repos.sort_unstable();
    let list = |ids: &[i32]| ids.iter().map(i32::to_string).collect::<Vec<_>>().join(" ");
    println!(
        "ps oldest_conf users=[{}] repos=[{}]",
        list(&conf_users),
        list(&conf_repos)
    );

    let count = schedule_jobs(pool, &default_site, Backoff::Zero)
        .await
        .expect("schedule_jobs");
    println!("ps scheduled count={count}");

    // Duplicate handling of `Create{User,Repo}SyncJob`.
    let now = OffsetDateTime::now_utc();
    let print_create = |kind: &str, id: i32, result: Result<(), sqlx::Error>| {
        let status = match result {
            Ok(()) => "ok".to_owned(),
            Err(err) => err.to_string(),
        };
        println!(
            "ps create kind={kind} id={id} priority={MEDIUM_PRIORITY_PERMISSIONS_SYNC} => {status}"
        );
    };
    print_create(
        "user",
        9910,
        create_user_sync_job(
            pool,
            9910,
            REASON_USER_NO_PERMS,
            MEDIUM_PRIORITY_PERMISSIONS_SYNC,
            true,
            now,
        )
        .await,
    );
    print_create(
        "user",
        9910,
        create_user_sync_job(
            pool,
            9910,
            REASON_USER_NO_PERMS,
            MEDIUM_PRIORITY_PERMISSIONS_SYNC,
            true,
            now,
        )
        .await,
    );
    print_create(
        "repo",
        9911,
        create_repo_sync_job(
            pool,
            9911,
            REASON_REPO_NO_PERMS,
            MEDIUM_PRIORITY_PERMISSIONS_SYNC,
            true,
            now,
        )
        .await,
    );

    let rows = sqlx::query(
        r#"
SELECT user_id, repository_id, reason, priority, no_perms, state, cancel,
       cancellation_reason
FROM permission_sync_jobs
WHERE (user_id BETWEEN 9900 AND 9999) OR (repository_id BETWEEN 9900 AND 9999)
ORDER BY COALESCE(user_id, 0), COALESCE(repository_id, 0), reason, priority
"#,
    )
    .fetch_all(pool)
    .await
    .expect("select jobs");
    for row in rows {
        let user_id: Option<i32> = row.try_get("user_id").expect("user_id");
        let repo_id: Option<i32> = row.try_get("repository_id").expect("repository_id");
        let reason: String = row.try_get("reason").expect("reason");
        let priority: i32 = row.try_get("priority").expect("priority");
        let no_perms: bool = row.try_get("no_perms").expect("no_perms");
        let state: Option<String> = row.try_get("state").expect("state");
        let cancel: bool = row.try_get("cancel").expect("cancel");
        let cancellation_reason: Option<String> = row
            .try_get("cancellation_reason")
            .expect("cancellation_reason");
        println!(
            "ps job user={} repo={} reason={} priority={} no_perms={} state={} cancel={} cancellation_reason={}",
            opt_id(user_id),
            opt_id(repo_id),
            reason,
            priority,
            flag(no_perms),
            state.unwrap_or_else(|| "-".to_owned()),
            flag(cancel),
            cancellation_reason.unwrap_or_else(|| "-".to_owned()),
        );
    }
}

async fn print_provider_harness(pool: &PgPool) {
    let provider_site =
        site(r#"{"auth.providers":[{"type":"gitlab","url":"https://gitlab.example.com"}]}"#);
    let keyring = Keyring::default();
    let mut urns = authz_provider_urns(pool, &provider_site, &keyring).await;
    urns.sort();
    for urn in &urns {
        println!("ps provider urn={urn}");
    }
    println!("ps providers count={}", urns.len());
}

fn print_config_harness() {
    let cases = [
        ("empty", "{}"),
        ("interval_zero", r#"{"permissions.syncScheduleInterval":0}"#),
        (
            "interval_negative",
            r#"{"permissions.syncScheduleInterval":-1}"#,
        ),
        ("interval_30", r#"{"permissions.syncScheduleInterval":30}"#),
        (
            "oldest_zero_three",
            r#"{"permissions.syncOldestUsers":0,"permissions.syncOldestRepos":3}"#,
        ),
        ("oldest_negative", r#"{"permissions.syncOldestUsers":-1}"#),
        (
            "backoff_mixed",
            r#"{"permissions.syncUsersBackoffSeconds":90,"permissions.syncReposBackoffSeconds":0}"#,
        ),
    ];

    for (name, json) in cases {
        let site = site(json);
        println!(
            "ps interval site={name} seconds={}",
            schedule_interval(&site).as_secs()
        );
        println!(
            "ps batch site={name} users={} repos={}",
            oldest_user_permissions_batch_size(&site),
            oldest_repo_permissions_batch_size(&site),
        );
        println!(
            "ps backoff site={name} backoff=conf user={} repo={}",
            Backoff::Conf.user(&site).as_secs(),
            Backoff::Conf.repo(&site).as_secs(),
        );
        println!(
            "ps backoff site={name} backoff=zero user={} repo={}",
            Backoff::Zero.user(&site).as_secs(),
            Backoff::Zero.repo(&site).as_secs(),
        );
    }
}

fn print_disabled_harness() {
    for disable in [false, true] {
        let site = site(&format!(r#"{{"disableAutoCodeHostSyncs":{disable}}}"#));
        for acls in [true, false] {
            for providers in [0_usize, 1] {
                println!(
                    "ps disabled providers={providers} acls={} disable={} => {}",
                    flag(acls),
                    flag(disable),
                    flag(permission_syncing_disabled(&site, providers, acls)),
                );
            }
        }
    }
}

fn print_routine_metadata() {
    let job = PermissionSyncJobScheduler;
    println!("ps routine name={SCHEDULER_ROUTINE_NAME}");
    println!("ps routine description={:?}", job.description());
    println!("ps routine configs={}", job.configs().len());
}

#[tokio::test(flavor = "multi_thread")]
async fn permission_sync_job_scheduler_matches_go_harness() {
    let Ok(dsn) = std::env::var("SG_QTEST_DSN") else {
        return;
    };

    print_config_harness();

    let pool = PgPool::connect(&dsn).await.expect("connect");
    print_database_harness(&pool).await;
    print_provider_harness(&pool).await;
    print_disabled_harness();
    print_routine_metadata();
}
