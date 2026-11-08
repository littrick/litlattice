#![forbid(unsafe_code)]
// Generated tonic server traits return `Result<_, tonic::Status>`; the status
// type is large by construction and owned by the generated code.
#![allow(clippy::result_large_err)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::doc_lazy_continuation)]

//! `sg-proto`: generated gRPC clients and servers.
//!
//! M8 generates the two pilot services (`symbols`, `searcher`) from the pinned
//! baseline protos. Additional services land with their replacement milestones.

pub mod gitserver {
    tonic::include_proto!("gitserver.v1");
}

pub mod internalapi {
    tonic::include_proto!("api.internalapi.v1");
}

pub mod searcher {
    tonic::include_proto!("searcher.v1");
}

pub mod symbols {
    tonic::include_proto!("symbols.v1");
}

pub mod repoupdater {
    tonic::include_proto!("repoupdater.v1");
}
