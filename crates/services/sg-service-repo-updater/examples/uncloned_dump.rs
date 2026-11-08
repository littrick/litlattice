#![forbid(unsafe_code)]

//! Prints the rows returned by `sg_db::repos::RepoStore::list_uncloned`,
//! filtered by a name suffix, for differential comparison with the baseline
//! (`p9uncloned` in the sourcegraph checkout).
//!
//! Usage: `SG_TEST_DATABASE_DSN=postgres://... cargo run --example uncloned_dump <suffix>`

use sg_db::repos::RepoStore;

#[tokio::main]
async fn main() {
    let Ok(dsn) = std::env::var("SG_TEST_DATABASE_DSN") else {
        eprintln!("SG_TEST_DATABASE_DSN is required");
        std::process::exit(2);
    };
    let suffix = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "p9uncloned".to_owned());

    let pool = sg_db::connect(&dsn, 5).await.expect("connect");
    let repos = RepoStore::new(pool)
        .list_uncloned()
        .await
        .expect("list uncloned");
    for repo in repos {
        if !repo.name.contains(&suffix) {
            continue;
        }
        println!("{} {}", repo.id, repo.name);
    }
}
