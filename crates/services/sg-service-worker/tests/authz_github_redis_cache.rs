#![forbid(unsafe_code)]

//! M9.8ay differential harness (Rust side): `RedisGroupsCache` against a real
//! Redis, mirroring `internal/authz/providers/github/cache_harness_test.go`.
//! Outputs are compared line for line by
//! `test_ws/p9-go/run_authz_github_redis_cache_diff.sh`.
//!
//! Both sides use the same service ID / URN, so the Redis keys
//! (`v2:gh_groups_perms:<serviceID>:<urn>:v1/...`) are identical. The harness
//! deletes its keys before and after the run.
//!
//! Requires Redis at `REDIS_CACHE_ENDPOINT` (default 127.0.0.1:6379).

use sg_redis::KeyValue;
use sg_service_worker::jobs::authz_github::{
    groups_cache_key_prefix, CachedGroup, GroupsCache, RedisGroupsCache,
};

fn join(values: &Option<Vec<String>>) -> String {
    match values {
        None => "<nil>".to_owned(),
        Some(values) if values.is_empty() => "<empty>".to_owned(),
        Some(values) => values.join(","),
    }
}

fn hit(found: bool) -> &'static str {
    if found {
        "hit"
    } else {
        "miss"
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn redis_groups_cache_matches_go_harness() {
    let endpoint =
        std::env::var("REDIS_CACHE_ENDPOINT").unwrap_or_else(|_| "127.0.0.1:6379".to_owned());
    let kv = KeyValue::connect(&endpoint)
        .await
        .expect("redis is required (set REDIS_CACHE_ENDPOINT)");

    const SERVICE_ID: &str = "https://diff.example.com/";
    const URN: &str = "extsvc:github:1";
    const TTL_SECONDS: i64 = 60;

    let prefix = groups_cache_key_prefix(SERVICE_ID, URN);
    let groups = RedisGroupsCache::with_ttl(kv, SERVICE_ID, URN, TTL_SECONDS);
    let inner = groups.cache();
    let org_key = inner.redis_key("v1/acme");
    let team_key = inner.redis_key("v1/acme/t1");

    inner.delete("v1/acme").await.expect("cleanup org key");
    inner.delete("v1/acme/t1").await.expect("cleanup team key");

    println!("ghc key-prefix prefix={prefix} key-org={org_key} key-team={team_key}");

    async fn raw_value(cache: &sg_rcache::Cache, key: &str) -> String {
        match cache.get(key).await {
            Ok(Some(bytes)) => String::from_utf8_lossy(&bytes).into_owned(),
            _ => "<nil>".to_owned(),
        }
    }

    // Miss: the raw group keeps org/team.
    let (group, found) = groups.get("acme", "").await;
    println!(
        "ghc op=get-missing status={} org={} team={} repos={} users={}",
        hit(found),
        group.org,
        group.team,
        join(&group.repositories),
        join(&group.users)
    );

    // Org group: Repositories populated, Users nil (serialized null).
    let group = CachedGroup {
        org: "acme".to_owned(),
        team: String::new(),
        repositories: Some(vec!["R_1".to_owned()]),
        users: None,
    };
    let status = match groups.set(&group).await {
        Ok(()) => "ok",
        Err(_) => "err",
    };
    println!(
        "ghc op=set-org status={status} json={}",
        raw_value(inner, "v1/acme").await
    );

    let (group, found) = groups.get("acme", "").await;
    println!(
        "ghc op=get-org status={} org={} team={} repos={} users={}",
        hit(found),
        group.org,
        group.team,
        join(&group.repositories),
        join(&group.users)
    );
    let ttl = inner.key_ttl("v1/acme").await.unwrap_or(-2);
    let ttl_bucket = if ttl > 0 {
        "positive"
    } else if ttl == -1 {
        "persistent"
    } else {
        "<missing>"
    };
    println!("ghc op=ttl-org value={ttl_bucket}");

    // Team group: Users populated, Repositories nil.
    let group = CachedGroup {
        org: "acme".to_owned(),
        team: "t1".to_owned(),
        repositories: None,
        users: Some(vec!["U_1".to_owned()]),
    };
    let status = match groups.set(&group).await {
        Ok(()) => "ok",
        Err(_) => "err",
    };
    println!(
        "ghc op=set-team status={status} json={}",
        raw_value(inner, "v1/acme/t1").await
    );

    let (group, found) = groups.get("acme", "t1").await;
    println!(
        "ghc op=get-team status={} org={} team={} repos={} users={}",
        hit(found),
        group.org,
        group.team,
        join(&group.repositories),
        join(&group.users)
    );

    // Malformed JSON is a miss, and the raw group is returned.
    inner
        .set("v1/acme", b"{not json")
        .await
        .expect("seed malformed value");
    let (group, found) = groups.get("acme", "").await;
    println!(
        "ghc op=malformed status={} org={} team={} repos={} users={}",
        hit(found),
        group.org,
        group.team,
        join(&group.repositories),
        join(&group.users)
    );

    // Invalidate deletes the key and clears both slices on the passed group.
    let mut group = CachedGroup {
        org: "acme".to_owned(),
        team: String::new(),
        repositories: Some(vec!["R_old".to_owned()]),
        users: Some(vec!["U_old".to_owned()]),
    };
    groups.set(&group).await.expect("seed invalidate group");
    groups.invalidate(&mut group).await;
    println!(
        "ghc op=invalidate status=ok org={} team={} repos={} users={} redis={}",
        group.org,
        group.team,
        join(&group.repositories),
        join(&group.users),
        raw_value(inner, "v1/acme").await
    );

    inner.delete("v1/acme").await.expect("cleanup org key");
    inner.delete("v1/acme/t1").await.expect("cleanup team key");
}
