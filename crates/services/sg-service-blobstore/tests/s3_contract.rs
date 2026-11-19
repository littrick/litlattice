//! AWS SDK contract tests: drive the Rust blobstore with the same
//! `aws-sdk-s3` client Sourcegraph uses (`internal/object/s3_client.go`).

use std::sync::Arc;

use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use sg_service_blobstore::backend::MultipartBackend;
use sg_service_blobstore::routes;
use sg_service_blobstore::FsBlobStore;
use tokio_util::sync::CancellationToken;

struct Server {
    endpoint: String,
    shutdown: CancellationToken,
    handle: tokio::task::JoinHandle<()>,
}

impl Server {
    async fn start(dir: &std::path::Path) -> Self {
        let backend: Arc<dyn MultipartBackend> = Arc::new(FsBlobStore::new(dir).unwrap());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let handler = routes::handler(backend);
        let token = shutdown.clone();
        let handle = tokio::spawn(async move {
            sg_http::serve_http(listener, handler, token).await.unwrap();
        });
        Self {
            endpoint: format!("http://{addr}"),
            shutdown,
            handle,
        }
    }

    fn client(&self) -> Client {
        let config = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                "AKIAIOSFODNN7EXAMPLE",
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                None,
                None,
                "test",
            ))
            .endpoint_url(self.endpoint.clone())
            .force_path_style(true)
            .build();
        Client::from_conf(config)
    }

    async fn stop(self) {
        self.shutdown.cancel();
        self.handle.await.unwrap();
    }
}

async fn body(client: &Client, bucket: &str, key: &str) -> Vec<u8> {
    let object = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .expect("get object");
    object
        .body
        .collect()
        .await
        .expect("collect body")
        .into_bytes()
        .to_vec()
}

#[tokio::test]
async fn object_roundtrip_and_list() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path()).await;
    let client = server.client();

    client
        .create_bucket()
        .bucket("lsif-uploads")
        .send()
        .await
        .expect("create bucket");
    client
        .put_object()
        .bucket("lsif-uploads")
        .key("dir/obj ümlaut.dump")
        .body(ByteStream::from_static(b"hello blobstore"))
        .send()
        .await
        .expect("put object");

    assert_eq!(
        body(&client, "lsif-uploads", "dir/obj ümlaut.dump").await,
        b"hello blobstore"
    );

    let head = client
        .head_object()
        .bucket("lsif-uploads")
        .key("dir/obj ümlaut.dump")
        .send()
        .await
        .expect("head object");
    assert_eq!(head.content_length(), Some(15));

    let listed = client
        .list_objects_v2()
        .bucket("lsif-uploads")
        .send()
        .await
        .expect("list objects");
    let keys: Vec<_> = listed
        .contents()
        .iter()
        .filter_map(|object| object.key())
        .collect();
    assert_eq!(keys, vec!["dir/obj ümlaut.dump"]);

    client
        .delete_object()
        .bucket("lsif-uploads")
        .key("dir/obj ümlaut.dump")
        .send()
        .await
        .expect("delete object");
    assert!(
        !dir.path()
            .join("buckets/lsif-uploads/dir%2Fobj+%C3%BCmlaut.dump")
            .exists()
    );

    server.stop().await;
}

#[tokio::test]
async fn multipart_upload_and_complete() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path()).await;
    let client = server.client();

    client
        .create_bucket()
        .bucket("lsif-uploads")
        .send()
        .await
        .expect("create bucket");

    let created = client
        .create_multipart_upload()
        .bucket("lsif-uploads")
        .key("parts.dump")
        .send()
        .await
        .expect("create upload");
    let upload_id = created.upload_id().expect("upload id").to_owned();

    let mut parts = Vec::new();
    for (number, payload) in [(1, "hello "), (2, "multipart")] {
        let part = client
            .upload_part()
            .bucket("lsif-uploads")
            .key("parts.dump")
            .upload_id(&upload_id)
            .part_number(number)
            .body(ByteStream::from_static(payload.as_bytes()))
            .send()
            .await
            .expect("upload part");
        parts.push(
            CompletedPart::builder()
                .part_number(number)
                .set_e_tag(part.e_tag().map(str::to_owned))
                .build(),
        );
    }

    client
        .complete_multipart_upload()
        .bucket("lsif-uploads")
        .key("parts.dump")
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(parts))
                .build(),
        )
        .send()
        .await
        .expect("complete upload");

    assert_eq!(
        body(&client, "lsif-uploads", "parts.dump").await,
        b"hello multipart"
    );

    server.stop().await;
}

#[tokio::test]
async fn multipart_upload_part_copy() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path()).await;
    let client = server.client();

    client
        .create_bucket()
        .bucket("lsif-uploads")
        .send()
        .await
        .expect("create bucket");
    client
        .put_object()
        .bucket("lsif-uploads")
        .key("source")
        .body(ByteStream::from_static(b"copied bytes"))
        .send()
        .await
        .expect("put source");

    let created = client
        .create_multipart_upload()
        .bucket("lsif-uploads")
        .key("composed")
        .send()
        .await
        .expect("create upload");
    let upload_id = created.upload_id().unwrap();
    client
        .upload_part_copy()
        .bucket("lsif-uploads")
        .key("composed")
        .upload_id(upload_id)
        .part_number(1)
        .copy_source("lsif-uploads/source")
        .send()
        .await
        .expect("copy part");
    client
        .complete_multipart_upload()
        .bucket("lsif-uploads")
        .key("composed")
        .upload_id(upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().build())
        .send()
        .await
        .expect("complete upload");

    assert_eq!(
        body(&client, "lsif-uploads", "composed").await,
        b"copied bytes"
    );
    server.stop().await;
}

#[tokio::test]
async fn missing_key_is_404() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(dir.path()).await;
    let client = server.client();

    client
        .create_bucket()
        .bucket("bucket")
        .send()
        .await
        .expect("create bucket");

    let error = client
        .get_object()
        .bucket("bucket")
        .key("missing")
        .send()
        .await
        .expect_err("missing key");
    match error {
        aws_sdk_s3::error::SdkError::ServiceError(service) => {
            assert_eq!(service.raw().status().as_u16(), 404);
        }
        other => panic!("unexpected error: {other:?}"),
    }

    server.stop().await;
}
