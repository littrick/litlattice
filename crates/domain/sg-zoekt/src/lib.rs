#![forbid(unsafe_code)]

//! `sg-zoekt`: a client for the zoekt-webserver gRPC API
//! (`zoekt.webserver.v1.WebserverService`), plus the query tree that searcher's
//! hybrid search sends to it.
//!
//! The baseline uses `github.com/sourcegraph/zoekt/query` and
//! `.../grpc/protos/zoekt/webserver/v1`. The protos under [`proto`] are copied
//! verbatim from the pinned module version
//! (`v0.0.0-20240808144359-20c496e3680f`).

pub mod cached;
pub mod client;
pub mod go_regexp;
pub mod languages;
pub mod metered;
pub mod proto;
pub mod query;
pub mod streamer;
pub mod types;
