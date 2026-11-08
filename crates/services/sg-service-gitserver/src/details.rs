//! `details` payloads for gitserver gRPC errors.
//!
//! The baseline attaches a serialized `google.rpc.Status` to `NotFound`
//! statuses (`server_grpc.go` uses `status.New(...).WithDetails(...)`). Its
//! client converts those payloads back into `gitdomain` errors
//! (`internal/gitserver/errwrap.go`), so they are part of the wire contract.
//! Each payload travels as a `google.protobuf.Any` whose type URL is
//! `type.googleapis.com/gitserver.v1.<Payload>` (the proto package is
//! `gitserver.v1`, see `internal/gitserver/v1/gitserver.proto`).

use bytes::Bytes;
use prost::Message;
use tonic::Status;

/// `google.rpc.Status`, as carried in the `grpc-status-details-bin` header.
///
/// `prost-types` ships the `google.protobuf` well-known types only, so the
/// RPC status message is declared here.
#[derive(Clone, PartialEq, Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}

/// `gitserver.v1.RepoNotFoundPayload`.
#[derive(Clone, PartialEq, Message)]
struct RepoNotFoundPayload {
    #[prost(string, tag = "1")]
    repo: String,
    #[prost(bool, tag = "2")]
    clone_in_progress: bool,
    #[prost(string, tag = "3")]
    clone_progress: String,
}

/// `gitserver.v1.RevisionNotFoundPayload`.
#[derive(Clone, PartialEq, Message)]
struct RevisionNotFoundPayload {
    #[prost(string, tag = "1")]
    repo: String,
    #[prost(string, tag = "2")]
    spec: String,
}

/// `gitserver.v1.FileNotFoundPayload`.
#[derive(Clone, PartialEq, Message)]
struct FileNotFoundPayload {
    #[prost(string, tag = "1")]
    repo: String,
    #[prost(string, tag = "2")]
    commit: String,
    #[prost(bytes = "vec", tag = "3")]
    path: Vec<u8>,
}

fn payload<P: Message>(name: &str, payload: &P) -> prost_types::Any {
    prost_types::Any {
        type_url: format!("type.googleapis.com/gitserver.v1.{name}"),
        value: payload.encode_to_vec(),
    }
}

fn with_payload(code: tonic::Code, message: &str, payload: prost_types::Any) -> Status {
    let status = RpcStatus {
        code: code as i32,
        message: message.to_owned(),
        details: vec![payload],
    };
    Status::with_details(code, message, Bytes::from(status.encode_to_vec()))
}

/// Baseline `newRepoNotFoundError`. The Rust server has no repository locker
/// yet (it has no write path until M9.3), so it never reports a clone in
/// progress, matching `RepoCloneProgress`.
pub(crate) fn repo_not_found(repo: &str) -> Status {
    with_payload(
        tonic::Code::NotFound,
        "repo not found",
        payload(
            "RepoNotFoundPayload",
            &RepoNotFoundPayload {
                repo: repo.to_owned(),
                clone_in_progress: false,
                clone_progress: String::new(),
            },
        ),
    )
}

/// Baseline `codes.NotFound` + `RevisionNotFoundPayload`. `repo` is the raw
/// request repo name and `spec` is the revision spec the backend reported.
pub(crate) fn revision_not_found(repo: &str, spec: &str) -> Status {
    with_payload(
        tonic::Code::NotFound,
        "revision not found",
        payload(
            "RevisionNotFoundPayload",
            &RevisionNotFoundPayload {
                repo: repo.to_owned(),
                spec: spec.to_owned(),
            },
        ),
    )
}

/// Baseline `codes.NotFound` + `FileNotFoundPayload`.
pub(crate) fn file_not_found(repo: &str, commit: &str, path: &[u8]) -> Status {
    with_payload(
        tonic::Code::NotFound,
        "file not found",
        payload(
            "FileNotFoundPayload",
            &FileNotFoundPayload {
                repo: repo.to_owned(),
                commit: commit.to_owned(),
                path: path.to_vec(),
            },
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Decode the details bytes back into `google.rpc.Status`.
    fn decode(status: &Status) -> RpcStatus {
        RpcStatus::decode(status.details()).expect("details decode as google.rpc.Status")
    }

    #[test]
    fn repo_not_found_carries_the_payload() {
        let status = repo_not_found("test/repo");
        assert_eq!(status.code(), tonic::Code::NotFound);
        assert_eq!(status.message(), "repo not found");

        let decoded = decode(&status);
        assert_eq!(decoded.code, tonic::Code::NotFound as i32);
        assert_eq!(decoded.message, "repo not found");
        assert_eq!(decoded.details.len(), 1);
        let any = &decoded.details[0];
        assert_eq!(
            any.type_url,
            "type.googleapis.com/gitserver.v1.RepoNotFoundPayload"
        );
        assert_eq!(
            RepoNotFoundPayload::decode(any.value.as_slice()).expect("payload"),
            RepoNotFoundPayload {
                repo: "test/repo".to_owned(),
                clone_in_progress: false,
                clone_progress: String::new(),
            }
        );
    }

    #[test]
    fn revision_not_found_carries_the_payload() {
        let status = revision_not_found("test/repo", "nope^0");
        let decoded = decode(&status);
        assert_eq!(decoded.code, tonic::Code::NotFound as i32);
        assert_eq!(decoded.details.len(), 1);
        let any = &decoded.details[0];
        assert_eq!(
            any.type_url,
            "type.googleapis.com/gitserver.v1.RevisionNotFoundPayload"
        );
        assert_eq!(
            RevisionNotFoundPayload::decode(any.value.as_slice()).expect("payload"),
            RevisionNotFoundPayload {
                repo: "test/repo".to_owned(),
                spec: "nope^0".to_owned(),
            }
        );
    }

    #[test]
    fn file_not_found_carries_raw_path_bytes() {
        let path = b"dir/h\xc3\xa9llo.txt";
        let status = file_not_found("test/repo", "abc123", path);
        let decoded = decode(&status);
        assert_eq!(decoded.code, tonic::Code::NotFound as i32);
        assert_eq!(decoded.details.len(), 1);
        let any = &decoded.details[0];
        assert_eq!(
            any.type_url,
            "type.googleapis.com/gitserver.v1.FileNotFoundPayload"
        );
        assert_eq!(
            FileNotFoundPayload::decode(any.value.as_slice()).expect("payload"),
            FileNotFoundPayload {
                repo: "test/repo".to_owned(),
                commit: "abc123".to_owned(),
                path: path.to_vec(),
            }
        );
    }

    #[test]
    fn proto3_default_field_values_are_omitted() {
        // `gitserver.v1.RepoNotFoundPayload{repo: ""}` marshals to zero bytes,
        // exactly like the Go server.
        assert!(payload(
            "RepoNotFoundPayload",
            &RepoNotFoundPayload {
                repo: String::new(),
                clone_in_progress: false,
                clone_progress: String::new(),
            }
        )
        .value
        .is_empty());
    }
}
