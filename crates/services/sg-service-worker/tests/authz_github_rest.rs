#![forbid(unsafe_code)]

//! M9.8ax differential harness (Rust side): the GitHub REST subset the authz
//! provider uses, exercised against a scripted local HTTP server.
//!
//! Mirrors `internal/extsvc/github/v3_rest_harness_test.go`. Outputs are
//! compared line for line by `test_ws/p9-go/run_authz_github_rest_diff.sh`.
//! The server URL is normalised to `SERVER` in error messages because the two
//! harnesses bind different ports.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use http_body_util::BodyExt;
use sg_http::HttpHandler;
use sg_service_worker::jobs::authz_github::{Collaborator, GithubClient, GithubClientError, Team};
use sg_service_worker::jobs::authz_github_client::GithubRestClient;
use sg_service_worker::jobs::github_apps_installation::api_root;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct Route {
    status: u16,
    link: Option<String>,
    body: String,
}

fn route(body: &str) -> Route {
    Route {
        status: 200,
        link: None,
        body: body.to_owned(),
    }
}

fn routes() -> BTreeMap<String, Route> {
    let collaborators = "/api/v3/repos/acme/repo/collaborators";
    let mut routes = BTreeMap::new();
    routes.insert(
        format!("{collaborators}?page=1&per_page=100&affiliation=direct"),
        Route {
            status: 200,
            link: Some(format!(
                "<{collaborators}?page=2&per_page=100&affiliation=direct>; rel=\"next\", \
                 <{collaborators}?page=2&per_page=100&affiliation=direct>; rel=\"last\""
            )),
            body: r#"[{"id":11,"node_id":"U_11"},{"id":12,"node_id":"U_12"}]"#.to_owned(),
        },
    );
    routes.insert(
        format!("{collaborators}?page=2&per_page=100&affiliation=direct"),
        Route {
            status: 200,
            link: Some(format!(
                "<{collaborators}?page=1&per_page=100&affiliation=direct>; rel=\"prev\", \
                 <{collaborators}?page=1&per_page=100&affiliation=direct>; rel=\"last\""
            )),
            body: r#"[{"id":13,"node_id":"U_13"}]"#.to_owned(),
        },
    );
    routes.insert(
        format!("{collaborators}?page=3&per_page=100&affiliation=direct"),
        Route {
            status: 404,
            link: None,
            body: r#"{"message":"Not Found"}"#.to_owned(),
        },
    );
    routes.insert(
        "/api/v3/repos/acme/repo/teams?page=1&per_page=100".to_owned(),
        route(
            r#"[{"name":"Alpha","slug":"alpha","url":"https://ghe.example.com/api/v3/teams/1","repos_count":2,"organization":{"login":"acme"}},{"name":"Beta","slug":"beta","url":"https://ghe.example.com/api/v3/teams/2","repos_count":0,"organization":null}]"#,
        ),
    );
    routes.insert(
        "/api/v3/orgs/acme/members?page=1&per_page=100".to_owned(),
        route(r#"[{"id":21,"node_id":"U_21"}]"#),
    );
    routes.insert(
        "/api/v3/orgs/acme/members?page=1&per_page=100&role=admin".to_owned(),
        route(r#"[{"id":22,"node_id":"U_22"}]"#),
    );
    routes.insert(
        "/api/v3/orgs/acme/teams/alpha/members?page=1&per_page=100".to_owned(),
        route(r#"[{"id":31,"node_id":"U_31"}]"#),
    );
    routes.insert(
        "/api/v3/orgs/acme".to_owned(),
        route(r#"{"login":"acme","default_repository_permission":"admin"}"#),
    );
    routes.insert(
        "/api/v3/orgs/missing".to_owned(),
        Route {
            status: 404,
            link: None,
            body: r#"{"message":"Not Found"}"#.to_owned(),
        },
    );
    routes.insert(
        "/api/v3/orgs/boom".to_owned(),
        Route {
            status: 500,
            link: None,
            body: r#"{"message":"boom","documentation_url":"https://docs.example/boom"}"#
                .to_owned(),
        },
    );
    routes.insert(
        "/api/v3/repos/acme/repo".to_owned(),
        route(r#"{"node_id":"R_repo","id":42,"full_name":"acme/repo","visibility":"internal"}"#),
    );
    routes.insert(
        "/api/v3/repos/acme/missing".to_owned(),
        Route {
            status: 404,
            link: None,
            body: r#"{"message":"Not Found"}"#.to_owned(),
        },
    );
    routes
}

async fn start_server(
    routes: BTreeMap<String, Route>,
    records: Arc<Mutex<Vec<String>>>,
) -> (String, CancellationToken) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind scripted server");
    let addr = listener.local_addr().expect("server addr");
    let http: HttpHandler = Arc::new(move |req: http::Request<hyper::body::Incoming>| {
        let routes = routes.clone();
        let records = Arc::clone(&records);
        Box::pin(async move {
            let target = req
                .uri()
                .path_and_query()
                .map(|path| path.as_str())
                .unwrap_or("")
                .to_owned();
            let accept = req
                .headers()
                .get_all(http::header::ACCEPT)
                .iter()
                .filter_map(|value| value.to_str().ok())
                .collect::<Vec<_>>()
                .join(",");
            let header = |name: http::header::HeaderName| {
                req.headers()
                    .get(name)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or("")
                    .to_owned()
            };
            records.lock().expect("lock").push(format!(
                "req={} {target} accept={accept} auth={} ctype={} cache={}",
                req.method(),
                header(http::header::AUTHORIZATION),
                header(http::header::CONTENT_TYPE),
                header(http::header::CACHE_CONTROL),
            ));
            let _ = req.into_body().collect().await;

            let fallback = Route {
                status: 404,
                link: None,
                body: r#"{"message":"Not Found"}"#.to_owned(),
            };
            let route = routes.get(&target).cloned().unwrap_or(fallback);
            let mut response = http::Response::builder().status(route.status);
            if let Some(link) = &route.link {
                response = response.header("Link", link.clone());
            }
            response
                .header("Content-Type", "application/json")
                .body(tonic::body::boxed(http_body_util::Full::new(
                    hyper::body::Bytes::from(route.body),
                )))
                .expect("response")
        }) as sg_http::HttpFuture
    });
    let shutdown = CancellationToken::new();
    tokio::spawn(sg_http::serve_http(listener, http, shutdown.clone()));
    (format!("http://{addr}"), shutdown)
}

fn join_ids(users: &[Collaborator]) -> String {
    if users.is_empty() {
        return "<empty>".to_owned();
    }
    users
        .iter()
        .map(|user| user.database_id.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn join_teams(teams: &[Team]) -> String {
    if teams.is_empty() {
        return "<empty>".to_owned();
    }
    teams
        .iter()
        .map(|team| {
            let org = team
                .organization
                .as_ref()
                .map(|org| org.login.clone())
                .unwrap_or_else(|| "<nil>".to_owned());
            format!("{}:{}:{}", team.slug, team.repos_count, org)
        })
        .collect::<Vec<_>>()
        .join(";")
}

fn normalize(server: &str, err: &GithubClientError) -> String {
    err.message.replace(server, "SERVER")
}

macro_rules! users_line {
    ($op:expr, $result:expr) => {{
        let (status, users, next) = match $result {
            Ok((users, next)) => ("ok", join_ids(&users), next),
            Err(_) => ("err", "<empty>".to_owned(), false),
        };
        println!(
            "ghrc op={} status={} users={} next={}",
            $op, status, users, next
        );
    }};
}

#[tokio::test(flavor = "multi_thread")]
async fn authz_github_rest_matches_go_harness() {
    let records = Arc::new(Mutex::new(Vec::<String>::new()));
    let (server, shutdown) = start_server(routes(), Arc::clone(&records)).await;

    for raw in [
        "https://github.com/",
        "https://www.github.com",
        "https://api.github.com/",
        "https://ghe.example.com/",
        "https://ghe.example.com/sub/",
        "https://ghe.example.com/sub",
    ] {
        let base = reqwest::Url::parse(raw).expect("base URL");
        println!("ghrc apiroot base={raw} api={}", api_root(&base));
    }

    let base = reqwest::Url::parse(&server).expect("server URL");
    let http = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("HTTP client");
    let client = GithubRestClient::with_http_client(http, &base, Some("tok".to_owned()), false);

    let dumped = Arc::new(Mutex::new(0usize));
    let print_requests = |op: &str| {
        let records = records.lock().expect("lock");
        let mut dumped = dumped.lock().expect("lock");
        for record in &records[*dumped..] {
            println!("ghrc op={op} {record}");
        }
        *dumped = records.len();
    };

    users_line!(
        "collaborators-page1",
        client
            .list_repository_collaborators("acme", "repo", 1, Some("direct"))
            .await
    );
    print_requests("collaborators-page1");

    users_line!(
        "collaborators-page2",
        client
            .list_repository_collaborators("acme", "repo", 2, Some("direct"))
            .await
    );
    print_requests("collaborators-page2");

    match client
        .list_repository_collaborators("acme", "repo", 3, Some("direct"))
        .await
    {
        Ok(_) => println!("ghrc op=collaborators-missing status=ok notfound=false err=none"),
        Err(err) => println!(
            "ghrc op=collaborators-missing status=err notfound={} err={}",
            err.not_found,
            normalize(&server, &err)
        ),
    }
    print_requests("collaborators-missing");

    let (status, teams, next) = match client.list_repository_teams("acme", "repo", 1).await {
        Ok((teams, next)) => ("ok", join_teams(&teams), next),
        Err(_) => ("err", "<empty>".to_owned(), false),
    };
    println!("ghrc op=repo-teams status={status} teams={teams} next={next}");
    print_requests("repo-teams");

    users_line!(
        "org-members",
        client.list_organization_members("acme", 1, false).await
    );
    print_requests("org-members");

    users_line!(
        "org-members-admin",
        client.list_organization_members("acme", 1, true).await
    );
    print_requests("org-members-admin");

    users_line!(
        "team-members",
        client.list_team_members("acme", "alpha", 1).await
    );
    print_requests("team-members");

    match client.get_organization("acme").await {
        Ok(org) => println!(
            "ghrc op=org status=ok login={} perm={}",
            org.login, org.default_repository_permission
        ),
        Err(err) => println!(
            "ghrc op=org status=err notfound={} err={}",
            err.not_found,
            normalize(&server, &err)
        ),
    }
    print_requests("org");

    match client.get_organization("missing").await {
        Ok(_) => println!("ghrc op=org-missing status=ok"),
        Err(err) => println!(
            "ghrc op=org-missing status=err notfound={} err={}",
            err.not_found,
            normalize(&server, &err)
        ),
    }
    print_requests("org-missing");

    match client.get_organization("boom").await {
        Ok(_) => println!("ghrc op=org-boom status=ok"),
        Err(err) => println!(
            "ghrc op=org-boom status=err notfound={} err={}",
            err.not_found,
            normalize(&server, &err)
        ),
    }
    print_requests("org-boom");

    match client.get_repository("acme", "repo").await {
        Ok(repository) => println!(
            "ghrc op=repo status=ok id={} visibility={}",
            repository.id, repository.visibility
        ),
        Err(err) => println!(
            "ghrc op=repo status=err notfound={} err={}",
            err.not_found,
            normalize(&server, &err)
        ),
    }
    print_requests("repo");

    match client.get_repository("acme", "missing").await {
        Ok(_) => println!("ghrc op=repo-missing status=ok"),
        Err(err) => println!(
            "ghrc op=repo-missing status=err notfound={} err={}",
            err.not_found,
            normalize(&server, &err)
        ),
    }
    print_requests("repo-missing");

    shutdown.cancel();
}
