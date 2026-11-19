#![allow(clippy::result_large_err)]

//! Generated code for `zoekt.webserver.v1`.

pub mod zoekt {
    pub mod webserver {
        pub mod v1 {
            tonic::include_proto!("zoekt.webserver.v1");
        }
    }
}

/// `sourcegraph.zoekt.configuration.v1` — the `zoekt-sourcegraph-indexserver`
/// configuration service the frontend must serve on its internal gRPC
/// listener.
pub mod sourcegraph {
    pub mod zoekt {
        pub mod configuration {
            pub mod v1 {
                tonic::include_proto!("sourcegraph.zoekt.configuration.v1");
            }
        }
    }
}

pub use zoekt::webserver::v1::*;
