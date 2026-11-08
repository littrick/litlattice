#![forbid(unsafe_code)]

//! M9.8g differential harness (Rust side): `sg-rcache` against a real Redis
//! server.
//!
//! Exercises the same command sequence as
//! `internal/rcache/harness_test.go` and prints the canonical lines both
//! sides must agree on. TTLs are reported as booleans because the remaining
//! time differs by a second between the two runs.
//!
//! `SG_QTEST_REDIS` is the endpoint (default `127.0.0.1:6379`); the cache
//! prefix `__diff_rcache` is cleared before and after the run.

use std::collections::HashMap;

use sg_rcache::Cache;
use sg_redis::KeyValue;

#[tokio::test]
async fn rcache_matches_go_harness() {
    let Ok(endpoint) = std::env::var("SG_QTEST_REDIS") else {
        return;
    };

    let kv = KeyValue::connect(&endpoint).await.expect("connect redis");
    clear_diff_keys(&kv).await;
    let result = run(&kv).await;
    clear_diff_keys(&kv).await;
    result.expect("run differential");
}

async fn run(kv: &KeyValue) -> Result<(), Box<dyn std::error::Error>> {
    let cache = Cache::new_with_ttl(kv.clone(), "__diff_rcache", 604800);

    cache.set("a", "hello").await?;
    println!("set a ok");

    assert_eq!(cache.get_string("a").await?.as_deref(), Some("hello"));
    println!("get a = hello");

    let ttl = cache.key_ttl("a").await?;
    println!("ttl a positive = {}", ttl > 0);

    cache.set_hash_item("hash", "f1", "v1").await?;
    cache.set_hash_item("hash", "f2", "v2").await?;
    let value = cache
        .get_hash_item("hash", "f1")
        .await?
        .expect("f1 present");
    println!("hget hash f1 = {value}");

    println!(
        "hgetall hash = {}",
        format_map(&cache.get_hash_all("hash").await?)
    );

    let list = cache.fifo_list("list", 3);
    for item in ["r1", "r2", "r3", "r4", "r5"] {
        list.insert(item).await?;
    }
    println!("list size = {}", list.size().await?);
    println!("list all = {}", format_items(&list.all().await?));
    println!(
        "list slice 0 1 = {}",
        format_items(&list.slice(0, 1).await?)
    );

    assert_eq!(cache.increase("counter").await?, 1);
    println!("counter incr = 1");
    assert_eq!(cache.incr_by("counter", 5).await?, 6);
    println!("counter incrby 5 = 6");
    assert_eq!(cache.decr_by("counter", 2).await?, 4);
    println!("counter decrby 2 = 4");
    println!(
        "counter ttl positive = {}",
        cache.key_ttl("counter").await? > 0
    );

    println!(
        "hdel hash f1 = {}",
        cache.delete_hash_item("hash", "f1").await?
    );
    println!(
        "hgetall hash = {}",
        format_map(&cache.get_hash_all("hash").await?)
    );

    let mut keys = cache.list_all_keys().await?;
    keys.sort();
    println!("keys = {}", keys.join(","));

    cache.delete("a").await?;
    assert!(cache.get("a").await?.is_none());
    println!("get a = <none>");

    let empty = cache.fifo_list("empty", 0);
    empty.insert("x").await?;
    println!("empty list size = {}", empty.size().await?);

    Ok(())
}

fn format_map(map: &HashMap<String, String>) -> String {
    let mut entries: Vec<_> = map.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    entries
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn format_items(items: &[Vec<u8>]) -> String {
    items
        .iter()
        .map(|item| String::from_utf8_lossy(item).into_owned())
        .collect::<Vec<_>>()
        .join(",")
}

async fn clear_diff_keys(kv: &KeyValue) {
    for pattern in ["v2:__diff_rcache*", "v2:v2:__diff_rcache*"] {
        for key in kv.keys(pattern).await.expect("scan keys") {
            kv.del(&key).await.expect("delete key");
        }
    }
}
