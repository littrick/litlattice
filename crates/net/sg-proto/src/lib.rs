#![forbid(unsafe_code)]
// Generated tonic server traits return `Result<_, tonic::Status>`; the status
// type is large by construction and owned by the generated code.
#![allow(clippy::result_large_err)]

//! `sg-proto`: generated gRPC clients and servers.
//!
//! M8 generates the two pilot services (`symbols`, `searcher`) from the pinned
//! baseline protos. Additional services land with their replacement milestones.

pub mod searcher {
    tonic::include_proto!("searcher.v1");
}

pub mod symbols {
    tonic::include_proto!("symbols.v1");
}
