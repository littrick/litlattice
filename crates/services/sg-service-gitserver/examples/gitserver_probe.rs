#![forbid(unsafe_code)]

//! Differential harness for `gitserver.v1.GitserverService/IsRepoCloneable`
//! and `ListGitolite`.
//!
//! ```text
//! cargo run --example gitserver_probe -- <addr> is-cloneable [repo]
//! cargo run --example gitserver_probe -- <addr> list-gitolite [host]
//! ```
//!
//! `is-cloneable` prints `code=<n> message=<msg>` or
//! `code=0 cloneable=<b> cloned=<b> reason=<r>`.

use sg_proto::gitserver as pb;

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
        other => panic!("unknown command {other}"),
    }
}
