#![forbid(unsafe_code)]

//! Differential harness for `gitserver.v1.GitserverService/IsRepoCloneable`,
//! `ListGitolite` and `CreateCommitFromPatchBinary`.
//!
//! ```text
//! cargo run --example gitserver_probe -- <addr> is-cloneable [repo]
//! cargo run --example gitserver_probe -- <addr> list-gitolite [host]
//! cargo run --example gitserver_probe -- <addr> create-commit <spec.json>
//! cargo run --example gitserver_probe -- <addr> perforce <spec.json>
//!
//! The perforce spec is JSON:
//!
//! ```json
//! {"op": "users", "p4port": "ok", "p4user": "alice", "p4passwd": "secret",
//!  "depot_path": "//depot1/main", "username": "alice", "depot": "depot1",
//!  "group": "devs", "changelist_id": "1234"}
//! ```
//!
//! `op` is `cloneable`, `check-credentials`, `users`, `protects-user`,
//! `protects-depot`, `group-members`, `super-user` or `changelist`.
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
        "perforce" => {
            let spec: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&arg).expect("read perforce spec"))
                    .expect("parse perforce spec");
            let text = |key: &str| {
                spec.get(key)
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_owned()
            };
            let conn = pb::PerforceConnectionDetails {
                p4port: text("p4port"),
                p4user: text("p4user"),
                p4passwd: text("p4passwd"),
            };
            let print_error = |status: tonic::Status| {
                println!(
                    "code={} message={}",
                    status.code() as i32,
                    escape(status.message())
                );
            };
            match text("op").as_str() {
                "cloneable" => {
                    match client
                        .is_perforce_path_cloneable(pb::IsPerforcePathCloneableRequest {
                            connection_details: Some(conn),
                            depot_path: text("depot_path"),
                        })
                        .await
                    {
                        Ok(_) => println!("code=0"),
                        Err(status) => print_error(status),
                    }
                }
                "check-credentials" => {
                    match client
                        .check_perforce_credentials(pb::CheckPerforceCredentialsRequest {
                            connection_details: Some(conn),
                        })
                        .await
                    {
                        Ok(_) => println!("code=0"),
                        Err(status) => print_error(status),
                    }
                }
                "users" => {
                    match client
                        .perforce_users(pb::PerforceUsersRequest {
                            connection_details: Some(conn),
                        })
                        .await
                    {
                        Ok(response) => {
                            let response = response.into_inner();
                            println!("code=0 count={}", response.users.len());
                            for user in response.users {
                                println!("user={}|{}", escape(&user.username), escape(&user.email));
                            }
                        }
                        Err(status) => print_error(status),
                    }
                }
                "protects-user" => {
                    match client
                        .perforce_protects_for_user(pb::PerforceProtectsForUserRequest {
                            connection_details: Some(conn),
                            username: text("username"),
                        })
                        .await
                    {
                        Ok(response) => {
                            let response = response.into_inner();
                            println!("code=0 count={}", response.protects.len());
                            for protect in response.protects {
                                println!("{}", protect_line(&protect));
                            }
                        }
                        Err(status) => print_error(status),
                    }
                }
                "protects-depot" => {
                    match client
                        .perforce_protects_for_depot(pb::PerforceProtectsForDepotRequest {
                            connection_details: Some(conn),
                            depot: text("depot"),
                        })
                        .await
                    {
                        Ok(response) => {
                            let response = response.into_inner();
                            println!("code=0 count={}", response.protects.len());
                            for protect in response.protects {
                                println!("{}", protect_line(&protect));
                            }
                        }
                        Err(status) => print_error(status),
                    }
                }
                "group-members" => {
                    match client
                        .perforce_group_members(pb::PerforceGroupMembersRequest {
                            connection_details: Some(conn),
                            group: text("group"),
                        })
                        .await
                    {
                        Ok(response) => {
                            let response = response.into_inner();
                            println!("code=0 count={}", response.usernames.len());
                            for username in response.usernames {
                                println!("username={}", escape(&username));
                            }
                        }
                        Err(status) => print_error(status),
                    }
                }
                "super-user" => {
                    match client
                        .is_perforce_super_user(pb::IsPerforceSuperUserRequest {
                            connection_details: Some(conn),
                        })
                        .await
                    {
                        Ok(_) => println!("code=0"),
                        Err(status) => print_error(status),
                    }
                }
                "changelist" => {
                    match client
                        .perforce_get_changelist(pb::PerforceGetChangelistRequest {
                            connection_details: Some(conn),
                            changelist_id: text("changelist_id"),
                        })
                        .await
                    {
                        Ok(response) => {
                            let changelist = response.into_inner().changelist.unwrap_or_default();
                            let (seconds, nanos) = changelist
                                .creation_date
                                .map(|date| (date.seconds, date.nanos))
                                .unwrap_or_default();
                            println!(
                                "code=0 id={} seconds={} nanos={} state={} author={} title={} message={}",
                                escape(&changelist.id),
                                seconds,
                                nanos,
                                changelist.state,
                                escape(&changelist.author),
                                escape(&changelist.title),
                                escape(&changelist.message),
                            );
                        }
                        Err(status) => print_error(status),
                    }
                }
                other => panic!("unknown perforce op {other}"),
            }
        }
        other => panic!("unknown command {other}"),
    }
}

fn protect_line(protect: &pb::PerforceProtect) -> String {
    format!(
        "protect={}|{}|{}|{}|{}|{}",
        escape(&protect.level),
        escape(&protect.entity_type),
        escape(&protect.entity_name),
        escape(&protect.r#match),
        protect.is_exclusion,
        escape(&protect.host),
    )
}

/// Make multi-line error messages diffable line by line.
fn escape(text: &str) -> String {
    text.replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
