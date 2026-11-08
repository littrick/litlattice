#![forbid(unsafe_code)]

//! Differential harness for `gitserver.v1.GitserverService/IsRepoCloneable`,
//! `ListGitolite` and `CreateCommitFromPatchBinary`.
//!
//! ```text
//! cargo run --example gitserver_probe -- <addr> is-cloneable [repo]
//! cargo run --example gitserver_probe -- <addr> list-gitolite [host]
//! cargo run --example gitserver_probe -- <addr> create-commit <spec.json>
//! ```
//!
//! `is-cloneable` prints `code=<n> message=<msg>` or
//! `code=0 cloneable=<b> cloned=<b> reason=<r>`.
//!
//! The `create-commit` spec is JSON:
//!
//! ```json
//! {
//!   "repo": "example.com/o/r", "base_commit": "<sha>", "target_ref": "refs/heads/branch",
//!   "messages": ["first", "second"],
//!   "author_name": "", "author_email": "", "committer_name": "", "committer_email": "",
//!   "date_seconds": 1789548132, "date_nanos": 0,
//!   "push": {"remote_url": "file:///tmp/bare", "private_key": "", "passphrase": ""},
//!   "push_ref": null, "patch_filenames_no_prefix": false,
//!   "patch_file": "/tmp/change.patch",
//!   "stream": "normal"
//! }
//! ```
//!
//! `stream` is `normal` (metadata, one patch message), `two-chunks` (the patch
//! split in two), `empty` (no messages), `patch-first`, `empty-message-first`
//! or `non-patch-after-metadata`.

use sg_proto::gitserver as pb;

use pb::create_commit_from_patch_binary_request::{Metadata, Patch, Payload};

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let addr = args
        .next()
        .expect("usage: <addr> is-cloneable|list-gitolite [arg]");
    let command = args.next().expect("command");
    let arg = args.next().unwrap_or_default();

    let mut client =
        pb::gitserver_service_client::GitserverServiceClient::connect(format!("http://{addr}"))
            .await
            .expect("connect");

    match command.as_str() {
        "is-cloneable" => {
            match client
                .is_repo_cloneable(pb::IsRepoCloneableRequest { repo: arg })
                .await
            {
                Ok(response) => {
                    let response = response.into_inner();
                    println!(
                        "code=0 cloneable={} cloned={} reason={}",
                        response.cloneable, response.cloned, response.reason
                    );
                }
                Err(status) => {
                    println!("code={} message={}", status.code() as i32, status.message())
                }
            }
        }
        "list-gitolite" => {
            match client
                .list_gitolite(pb::ListGitoliteRequest { gitolite_host: arg })
                .await
            {
                Ok(response) => {
                    let mut repos: Vec<String> = response
                        .into_inner()
                        .repos
                        .into_iter()
                        .map(|repo| format!("{}|{}", repo.name, repo.url))
                        .collect();
                    repos.sort();
                    println!("code=0 count={}", repos.len());
                    for repo in repos {
                        println!("repo={repo}");
                    }
                }
                Err(status) => {
                    println!("code={} message={}", status.code() as i32, status.message())
                }
            }
        }
        "create-commit" => {
            let spec: serde_json::Value = serde_json::from_str(
                &std::fs::read_to_string(&arg).expect("read create-commit spec"),
            )
            .expect("parse create-commit spec");
            let text = |key: &str| {
                spec.get(key)
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_owned()
            };
            let commit_info = pb::PatchCommitInfo {
                messages: spec
                    .get("messages")
                    .and_then(serde_json::Value::as_array)
                    .map(|messages| {
                        messages
                            .iter()
                            .filter_map(serde_json::Value::as_str)
                            .map(str::to_owned)
                            .collect()
                    })
                    .unwrap_or_default(),
                author_name: text("author_name"),
                author_email: text("author_email"),
                committer_name: text("committer_name"),
                committer_email: text("committer_email"),
                date: spec
                    .get("date_seconds")
                    .and_then(serde_json::Value::as_i64)
                    .map(|seconds| prost_types::Timestamp {
                        seconds,
                        nanos: spec
                            .get("date_nanos")
                            .and_then(serde_json::Value::as_i64)
                            .unwrap_or(0) as i32,
                    }),
            };
            let metadata = Metadata {
                repo: text("repo"),
                base_commit: text("base_commit"),
                target_ref: text("target_ref"),
                commit_info: Some(commit_info),
                push: spec.get("push").map(|push| pb::PushConfig {
                    remote_url: push
                        .get("remote_url")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    private_key: push
                        .get("private_key")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    passphrase: push
                        .get("passphrase")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                }),
                push_ref: spec
                    .get("push_ref")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned),
                patch_filenames_no_prefix: spec
                    .get("patch_filenames_no_prefix")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false),
            };
            let patch = std::fs::read(text("patch_file")).unwrap_or_default();
            let stream = text("stream");
            let message = |payload| pb::CreateCommitFromPatchBinaryRequest {
                payload: Some(payload),
            };
            let requests: Vec<pb::CreateCommitFromPatchBinaryRequest> = match stream.as_str() {
                "empty" => Vec::new(),
                "patch-first" => vec![message(Payload::Patch(Patch {
                    data: patch.clone(),
                }))],
                "empty-message-first" => {
                    vec![pb::CreateCommitFromPatchBinaryRequest { payload: None }]
                }
                "non-patch-after-metadata" => vec![
                    message(Payload::Metadata(metadata.clone())),
                    message(Payload::Metadata(metadata)),
                ],
                "two-chunks" => {
                    let split = patch.len() / 2;
                    vec![
                        message(Payload::Metadata(metadata)),
                        message(Payload::Patch(Patch {
                            data: patch[..split].to_vec(),
                        })),
                        message(Payload::Patch(Patch {
                            data: patch[split..].to_vec(),
                        })),
                    ]
                }
                _ => vec![
                    message(Payload::Metadata(metadata)),
                    message(Payload::Patch(Patch { data: patch })),
                ],
            };

            match client
                .create_commit_from_patch_binary(tokio_stream::iter(requests))
                .await
            {
                Ok(response) => {
                    let response = response.into_inner();
                    println!(
                        "code=0 rev={} changelist_id={}",
                        response.rev, response.changelist_id
                    );
                }
                Err(status) => {
                    println!(
                        "code={} message={} details={}",
                        status.code() as i32,
                        status.message(),
                        hex(status.details())
                    );
                }
            }
        }
        other => panic!("unknown command {other}"),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
