//! M9.8em differential harness: prints the `cg api` line for
//! `ConfigService.GetConfig`, matching
//! `cmd/frontend/internal/httpapi/zz_configuration_api_harness_test.go`.
//!
//! Starts the tonic server on an ephemeral port, fetches with
//! `sg_config::ConfigClient` (the same client other services use) and prints
//! the decoded fields.

use std::sync::Arc;
use std::time::Duration;

use sg_config::store::ConfigStore;
use sg_config::{RawUnified, ServiceConnections};
use sg_proto::internalapi::config_service_server::ConfigServiceServer;
use sg_service_frontend::configuration_api::ConfigApi;

const SITE: &str = r#"{"auth.providers":[{"type":"builtin","allowSignup":true}],"externalURL":"https://sourcegraph.example.com"}"#;

fn flat(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('\t', "\\t")
        .replace('\r', "\\r")
}

fn seeded_raw() -> RawUnified {
    RawUnified {
        id: 7,
        site: SITE.to_owned(),
        service_connections: ServiceConnections {
            git_servers: vec![
                "http://gitserver-0:3178".to_owned(),
                "http://gitserver-1:3178".to_owned(),
            ],
            postgres_dsn: "postgres://sourcegraph:sourcegraph@postgres:5432/sourcegraph?sslmode=disable"
                .to_owned(),
            code_intel_postgres_dsn:
                "postgres://sourcegraph:sourcegraph@postgres:5432/sourcegraph-codeintel?sslmode=disable"
                    .to_owned(),
            code_insights_dsn:
                "postgres://sourcegraph:sourcegraph@postgres:5432/sourcegraph-codeinsights?sslmode=disable"
                    .to_owned(),
            searchers: vec!["http://searcher-0:3181".to_owned()],
            symbols: vec!["http://symbols-0:3184".to_owned()],
            embeddings: vec!["http://embeddings-0:9998".to_owned()],
            zoekts: vec!["http://zoekt-0:6070".to_owned()],
            zoekt_list_ttl: Duration::new(30, 5),
        },
    }
}

#[tokio::test]
async fn configuration_api_harness() {
    if std::env::var("SG_CONFIG_API_HARNESS").as_deref() != Ok("1") {
        return;
    }

    let store = Arc::new(ConfigStore::new());
    let (_, result) = store.maybe_update(&seeded_raw());
    result.expect("seed configuration store");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let incoming = tonic::transport::server::TcpIncoming::from_listener(
        listener,
        true,
        Some(Duration::from_secs(5)),
    )
    .expect("incoming");
    let server = tonic::transport::Server::builder()
        .add_service(ConfigServiceServer::new(ConfigApi::new(store)));
    let handle = tokio::spawn(async move { server.serve_with_incoming(incoming).await });

    let client = sg_config::ConfigClient::new(format!("127.0.0.1:{}", addr.port()));
    let raw = client.fetch_raw().await.expect("GetConfig");
    handle.abort();

    let connections = &raw.service_connections;
    println!(
        "cg api id={} site={} pg={} git={} searchers={} symbols={} embeddings={} zoekts={} ttl-sec={} ttl-nanos={}",
        raw.id,
        flat(&raw.site),
        connections.postgres_dsn,
        connections.git_servers.join(","),
        connections.searchers.join(","),
        connections.symbols.join(","),
        connections.embeddings.join(","),
        connections.zoekts.join(","),
        connections.zoekt_list_ttl.as_secs(),
        connections.zoekt_list_ttl.subsec_nanos(),
    );
}
