//! M9.7d Go/Rust differential for the gitserver gRPC client.
//!
//! Runs only when `SG_GIT_DIFF` is set (the driver script sets it). The Go
//! counterpart is `internal/codeintel/gitclientharness`; both print the same
//! canonical lines, and `test_ws/p9-go/run_git_client_diff.sh` compares them.

use sg_git::{Commit, Error, GitserverClient};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

const ZERO_SHA: &str = "0000000000000000000000000000000000000000";

fn addr() -> String {
    std::env::var("SG_GIT_ADDR").unwrap_or_else(|_| "127.0.0.1:3178".to_owned())
}

fn repo() -> String {
    std::env::var("SG_GIT_REPO").unwrap_or_else(|_| "test/blame-fixture".to_owned())
}

fn refs_repo() -> String {
    std::env::var("SG_GIT_REFS_REPO").unwrap_or_else(|_| "test/git-refs-fixture".to_owned())
}

/// Mirrors `gitdomain`'s error taxonomy used by the Go harness.
fn classify(err: &Error) -> &'static str {
    match err {
        Error::RevisionNotFound { .. } => "revision_not_found",
        Error::RepoNotFound {
            clone_in_progress: true,
            ..
        } => "clone_in_progress",
        Error::RepoNotFound { .. } => "repo_not_found",
        Error::NotFound(_) => "not_found",
        Error::Message(_) => "other",
    }
}

fn join(values: &[String]) -> String {
    if values.is_empty() {
        "-".to_owned()
    } else {
        values.join(",")
    }
}

async fn print_resolve(client: &GitserverClient, name: &str, spec: &str) {
    match client.resolve_revision(name, spec, true).await {
        Ok(sha) => println!("resolve repo={name} spec={spec} ensure=true -> sha={sha} err=ok"),
        Err(err) => println!(
            "resolve repo={name} spec={spec} ensure=true -> sha= err={}",
            classify(&err)
        ),
    }
}

async fn print_list_refs(client: &GitserverClient, contains: &str) {
    let (names, result) = match client
        .list_refs(&refs_repo(), true, false, &[], Some(contains))
        .await
    {
        Ok(refs) => {
            let mut names: Vec<String> = refs.into_iter().map(|git_ref| git_ref.name).collect();
            names.sort();
            (names, "ok".to_owned())
        }
        Err(err) => (Vec::new(), classify(&err).to_owned()),
    };
    println!(
        "list_refs repo={} heads=true contains={contains} -> names={} err={result}",
        refs_repo(),
        join(&names)
    );
}

async fn read_dir(client: &GitserverClient, path: &str) -> (Vec<String>, Result<(), Error>) {
    match client.read_dir(&repo(), "HEAD", path).await {
        Ok(files) => {
            let mut names: Vec<String> = files.into_iter().map(|file| file.name).collect();
            names.sort();
            (names, Ok(()))
        }
        Err(err) => (Vec::new(), Err(err)),
    }
}

fn format_date(commit: &Commit) -> String {
    let Some(date) = commit.committer_date else {
        return "none".to_owned();
    };
    OffsetDateTime::from_unix_timestamp(date.seconds)
        .ok()
        .and_then(|value| value.format(&Rfc3339).ok())
        .unwrap_or_else(|| format!("seconds={}", date.seconds))
}

#[tokio::test]
async fn git_client_harness() {
    if std::env::var("SG_GIT_DIFF").is_err() {
        return;
    }

    let client = GitserverClient::new(vec![addr()]);
    let repo = repo();
    let refs_repo = refs_repo();

    let head = client
        .resolve_revision(&repo, "HEAD", true)
        .await
        .expect("resolve HEAD");
    println!("head={head}");
    println!("resolve repo={repo} spec=HEAD ensure=true -> sha={head} err=ok");
    print_resolve(&client, &repo, &head).await;
    print_resolve(&client, &repo, ZERO_SHA).await;
    print_resolve(&client, "test/does-not-exist-m97d", "HEAD").await;

    match client.default_branch(&repo).await {
        Ok(reference) => {
            println!("default_branch repo={repo} -> ref={reference} err=ok");
        }
        Err(err) => {
            println!("default_branch repo={repo} -> ref= err={}", classify(&err));
        }
    }

    let refs_head = client
        .resolve_revision(&refs_repo, "HEAD", false)
        .await
        .expect("resolve refs HEAD");
    let refs_root = client
        .resolve_revision(&refs_repo, "HEAD~1", false)
        .await
        .expect("resolve refs HEAD~1");
    print_list_refs(&client, &refs_head).await;
    print_list_refs(&client, &refs_root).await;

    let (files, result) = read_dir(&client, "").await;
    let class = match &result {
        Ok(()) => "ok".to_owned(),
        Err(err) => classify(err).to_owned(),
    };
    println!(
        "read_dir repo={repo} path= -> files={} err={class}",
        join(&files)
    );
    let (files, result) = read_dir(&client, "nonexistent-dir-m97d").await;
    let class = match &result {
        Ok(()) => "ok".to_owned(),
        Err(err) => classify(err).to_owned(),
    };
    println!(
        "read_dir repo={repo} path=nonexistent-dir-m97d -> files={} err={class}",
        join(&files)
    );

    match client.get_commit(&repo, &head).await {
        Ok(commit) => println!(
            "get_commit repo={repo} commit={head} -> oid={} date={} err=ok",
            commit.oid,
            format_date(&commit)
        ),
        Err(err) => println!(
            "get_commit repo={repo} commit={head} -> err={}",
            classify(&err)
        ),
    }
    match client.get_commit(&repo, ZERO_SHA).await {
        Ok(_) => println!("get_commit repo={repo} commit={ZERO_SHA} -> err=ok"),
        Err(err) => println!(
            "get_commit repo={repo} commit={ZERO_SHA} -> err={}",
            classify(&err)
        ),
    }
}
