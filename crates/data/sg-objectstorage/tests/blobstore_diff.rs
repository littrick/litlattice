//! M9.7d Go/Rust differential for the S3/blobstore object store.
//!
//! Runs only when `SG_OBJ_DIFF` is set. The Go counterpart is
//! `internal/codeintel/objectstoreharness`; both talk to the live blobstore and
//! print the same canonical lines, compared by
//! `test_ws/p9-go/run_objectstore_diff.sh`.

use aws_sdk_s3::primitives::ByteStream;
use sg_objectstorage::{ObjectStore, S3Config, StorageConfig};
use tokio::io::AsyncReadExt;

const PAYLOAD: &str = "hello-m97d";

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn class<T>(result: &Result<T, impl std::fmt::Display>) -> &'static str {
    if result.is_ok() {
        "ok"
    } else {
        "error"
    }
}

#[tokio::test]
async fn blobstore_roundtrip() {
    if std::env::var("SG_OBJ_DIFF").is_err() {
        return;
    }

    let bucket = env_or("SG_OBJ_BUCKET", "sg-m97d-bucket");
    let key = env_or("SG_OBJ_KEY", "m97d-roundtrip.txt");
    let endpoint = env_or("SG_OBJ_ENDPOINT", "http://127.0.0.1:9000");

    let config = StorageConfig {
        backend: "blobstore".to_owned(),
        manage_bucket: false,
        bucket: bucket.clone(),
        s3: S3Config {
            region: env_or("SG_OBJ_REGION", "us-east-1"),
            endpoint,
            access_key_id: env_or("SG_OBJ_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE"),
            secret_access_key: env_or(
                "SG_OBJ_SECRET_ACCESS_KEY",
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            ),
            ..S3Config::default()
        },
    };
    let store = ObjectStore::new(config).await.expect("client construction");

    let init = store.init().await;
    println!("init bucket={bucket} err={}", class(&init));

    let upload = store
        .upload(&key, ByteStream::from(PAYLOAD.as_bytes().to_vec()))
        .await;
    match &upload {
        Ok(bytes) => println!("upload key={key} bytes={bytes} err=ok"),
        Err(_) => println!("upload key={key} bytes=0 err=error"),
    }

    let get = async {
        let body = store.get(&key).await.map_err(|err| err.to_string())?;
        let mut data = Vec::new();
        body.into_async_read()
            .read_to_end(&mut data)
            .await
            .map_err(|err| err.to_string())?;
        Ok::<_, String>(data)
    };
    match get.await {
        Ok(data) => {
            let text = String::from_utf8_lossy(&data);
            let matches = if text == PAYLOAD { "ok" } else { "mismatch" };
            println!(
                "get key={key} len={} content={text} matches={matches} err=ok",
                data.len()
            );
        }
        Err(err) => println!("get key={key} err=error detail={err}"),
    }

    let delete = store.delete(&key).await;
    println!("delete key={key} err={}", class(&delete));

    let missing = async {
        let body = store.get(&key).await.map_err(|err| err.to_string())?;
        let mut data = Vec::new();
        body.into_async_read()
            .read_to_end(&mut data)
            .await
            .map_err(|err| err.to_string())?;
        Ok::<_, String>(data)
    };
    let class = if missing.await.is_ok() { "ok" } else { "error" };
    println!("get_missing key={key} err={class}");
}
