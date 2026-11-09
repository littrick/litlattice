#![forbid(unsafe_code)]

//! Port of the frontend internal `ConfigService` (`cmd/frontend/internal/httpapi/internal.go:12-27`).
//!
//! `configServer.GetConfig` returns `conf.Raw().ToProto()`, so the other
//! Sourcegraph processes read the frontend's configuration over gRPC
//! (`internalapi.Client.Configuration`) instead of querying the database.
//! The service is registered on the internal listener next to the internal
//! HTTP handler (`httpapi.go:397`, `serve_cmd.go:385-397`).
//!
//! `conf.Raw()` blocks until the store is initialized (`store.go:54-70`); the
//! port calls the same blocking [`ConfigStore::raw`] through
//! `spawn_blocking`. `sg-config`'s `RawUnified` has no `to_proto` (that crate
//! is shared WIP), so the conversion lives here.

use std::sync::Arc;

use sg_config::store::ConfigStore;
use sg_config::RawUnified;
use sg_proto::internalapi as proto;
use tonic::{Request, Response, Status};

/// `configServer` in `cmd/frontend/internal/httpapi/internal.go`.
pub struct ConfigApi {
    store: Arc<ConfigStore>,
}

impl ConfigApi {
    pub fn new(store: Arc<ConfigStore>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl proto::config_service_server::ConfigService for ConfigApi {
    async fn get_config(
        &self,
        _request: Request<proto::GetConfigRequest>,
    ) -> Result<Response<proto::GetConfigResponse>, Status> {
        let store = Arc::clone(&self.store);
        // `store.Raw` waits for the first update on a condvar.
        let raw = tokio::task::spawn_blocking(move || store.raw())
            .await
            .map_err(|err| Status::internal(format!("configuration store task failed: {err}")))?;
        Ok(Response::new(proto::GetConfigResponse {
            raw_unified: Some(raw_to_proto(&raw)),
        }))
    }
}

/// `RawUnified.ToProto` (`internal/conf/conftypes/conftypes.go:85-99`).
pub fn raw_to_proto(raw: &RawUnified) -> proto::RawUnified {
    let connections = &raw.service_connections;
    proto::RawUnified {
        id: raw.id,
        site: raw.site.clone(),
        service_connections: Some(proto::ServiceConnections {
            git_servers: connections.git_servers.clone(),
            postgres_dsn: connections.postgres_dsn.clone(),
            code_intel_postgres_dsn: connections.code_intel_postgres_dsn.clone(),
            code_insights_dsn: connections.code_insights_dsn.clone(),
            searchers: connections.searchers.clone(),
            symbols: connections.symbols.clone(),
            embeddings: connections.embeddings.clone(),
            zoekts: connections.zoekts.clone(),
            zoekt_list_ttl: Some(prost_types::Duration {
                seconds: connections.zoekt_list_ttl.as_secs() as i64,
                nanos: connections.zoekt_list_ttl.subsec_nanos() as i32,
            }),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn raw_to_proto_copies_every_service_connection() {
        let raw = RawUnified {
            id: 9,
            site: r#"{"externalURL":"https://example.com"}"#.to_owned(),
            service_connections: sg_config::ServiceConnections {
                git_servers: vec!["http://git".into()],
                postgres_dsn: "postgres://db/sg".into(),
                code_intel_postgres_dsn: "postgres://db/ci".into(),
                code_insights_dsn: "postgres://db/insights".into(),
                searchers: vec!["http://searcher".into()],
                symbols: vec!["http://symbols".into()],
                embeddings: vec!["http://embeddings".into()],
                zoekts: vec!["http://zoekt".into()],
                zoekt_list_ttl: Duration::new(30, 5),
            },
        };
        let proto = raw_to_proto(&raw);
        let connections = proto.service_connections.expect("connections");
        assert_eq!(proto.id, 9);
        assert_eq!(proto.site, raw.site);
        assert_eq!(connections.git_servers, ["http://git"]);
        assert_eq!(connections.postgres_dsn, "postgres://db/sg");
        assert_eq!(connections.code_intel_postgres_dsn, "postgres://db/ci");
        assert_eq!(connections.code_insights_dsn, "postgres://db/insights");
        assert_eq!(connections.searchers, ["http://searcher"]);
        assert_eq!(connections.symbols, ["http://symbols"]);
        assert_eq!(connections.embeddings, ["http://embeddings"]);
        assert_eq!(connections.zoekts, ["http://zoekt"]);
        let ttl = connections.zoekt_list_ttl.expect("ttl");
        assert_eq!((ttl.seconds, ttl.nanos), (30, 5));
    }
}
