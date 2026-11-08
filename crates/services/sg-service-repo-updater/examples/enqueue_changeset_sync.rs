#![forbid(unsafe_code)]

//! Differential harness for `repoupdater.v1.RepoUpdaterService/EnqueueChangesetSync`.
//!
//! Calls the RPC on any repo-updater address (Go or Rust) and prints the gRPC
//! status so two processes can be compared:
//!
//! ```text
//! cargo run --example enqueue_changeset_sync -- <addr> [id ...]
//! ```
//!
//! Output: `code=<n> message=<message>` (`code=0` means OK).

use sg_proto::repoupdater as pb;

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let addr = args.next().expect("usage: <addr> [id ...]");
    let ids: Vec<i64> = args
        .map(|id| id.parse().expect("changeset id must be an integer"))
        .collect();

    let mut client = pb::repo_updater_service_client::RepoUpdaterServiceClient::connect(format!(
        "http://{addr}"
    ))
    .await
    .expect("connect");
    match client
        .enqueue_changeset_sync(pb::EnqueueChangesetSyncRequest { ids })
        .await
    {
        Ok(_) => println!("code=0 message="),
        Err(status) => println!("code={} message={}", status.code() as i32, status.message()),
    }
}
