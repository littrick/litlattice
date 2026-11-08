#![forbid(unsafe_code)]

//! M9.8j differential harness (Rust side): `framework::metrics_server`
//! (`DistributedStore` + the promhttp handler) against a real Redis server and
//! a real HTTP listener.
//!
//! Exercises the same fixture sequence as
//! `cmd/worker/internal/executors/metrics_harness_test.go` and prints the
//! canonical lines both sides must agree on: the stored blob bytes, the
//! decoded-then-re-encoded families (sorted), the handler's status /
//! `Content-Type` / body lines / gzip behaviour, and the 500 response for a
//! blob that cannot be decoded.
//!
//! `SG_QTEST_REDIS` is the endpoint. `DistributedStore` resolves the cache
//! endpoint from the environment (`REDIS_CACHE_ENDPOINT`, else `REDIS_ENDPOINT`),
//! exactly like `redispool.Cache`, so the harness sets
//! `REDIS_CACHE_ENDPOINT` from `SG_QTEST_REDIS` before the store is built.

use std::collections::HashMap;
use std::io::Read;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use flate2::read::GzDecoder;
use sg_http::serve_http;
use sg_metrics::textfmt::{self, Bucket, Histogram, LabelPair, Metric, MetricFamily, MetricType};
use sg_metrics::textfmt::{Quantile, Summary};
use sg_redis::KeyValue;
use sg_service_worker::framework::metrics_server::{self, DistributedStore};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

const PREFIX: &str = "__diff_executors:";

#[tokio::test(flavor = "multi_thread")]
async fn metrics_server_matches_go_harness() {
    let Ok(endpoint) = std::env::var("SG_QTEST_REDIS") else {
        return;
    };
    if std::env::var("REDIS_CACHE_ENDPOINT").is_err() {
        std::env::set_var("REDIS_CACHE_ENDPOINT", &endpoint);
    }

    let kv = KeyValue::connect(&endpoint).await.expect("connect redis");
    clear_diff_keys(&kv).await;
    let result = run(&kv).await;
    clear_diff_keys(&kv).await;
    result.expect("run differential");
}

async fn run(kv: &KeyValue) -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(DistributedStore::new(PREFIX));

    println!("empty gather families = {}", store.gather().await?.len());

    let blobs: [(&str, Vec<MetricFamily>); 3] = [
        ("alpha", counter_and_gauge_families()),
        ("beta", summary_and_histogram_families()),
        ("gamma", vec![untyped_family()]),
    ];
    for (instance, families) in &blobs {
        store.ingest(instance, families).await?;
    }
    for (instance, _) in &blobs {
        let blob = kv
            .get_string(&format!("{PREFIX}{instance}"))
            .await?
            .unwrap_or_default();
        println!("blob {instance} = {blob:?}");
    }

    let mut encoded = Vec::new();
    for family in store.gather().await? {
        encoded.push(textfmt::encode_family(&family).expect("encode family"));
    }
    println!("gather families = {}", encoded.len());
    encoded.sort();
    for text in &encoded {
        println!("family = {text:?}");
    }

    // Serve the handler on a real listener, like `httpserver.NewFromAddr`.
    let handler = metrics_server::handler(Arc::clone(&store));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let shutdown = CancellationToken::new();
    let mut tasks = JoinSet::new();
    let server_shutdown = shutdown.clone();
    tasks.spawn(async move { serve_http(listener, handler, server_shutdown).await });

    let plain = get(addr, None).await?;
    println!("http status = {}", plain.status);
    println!("http content-type = {}", plain.content_type);
    for line in sorted_lines(&plain.body) {
        println!("http body line = {line:?}");
    }

    let gzipped = get(addr, Some("gzip")).await?;
    println!("http gzip status = {}", gzipped.status);
    println!("http gzip content-encoding = {}", gzipped.content_encoding);
    println!(
        "http gzip body equals plain = {}",
        sorted_lines(&gzipped.body) == sorted_lines(&plain.body)
    );

    // A blob that cannot be decoded makes `Gather` fail, and promhttp turns
    // that into an `internal server error` with the wrapped error text.
    kv.set_ex(
        &format!("{PREFIX}bad"),
        30,
        "# TYPE diff_bad bogus\ndiff_bad 1\n",
    )
    .await?;
    let malformed = store.gather().await.expect_err("malformed blob");
    println!("malformed gather error = {malformed}");

    let failure = get(addr, None).await?;
    println!("http error status = {}", failure.status);
    println!("http error content-type = {}", failure.content_type);
    println!(
        "http error x-content-type-options = {}",
        failure.x_content_type_options
    );
    println!("http error body = {:?}", failure.body);

    shutdown.cancel();
    let _ = tasks.join_next().await;
    Ok(())
}

struct MetricsResponse {
    status: u16,
    content_type: String,
    content_encoding: String,
    x_content_type_options: String,
    body: String,
}

/// A minimal HTTP/1.1 GET with `Connection: close`.
async fn get(
    addr: SocketAddr,
    accept_encoding: Option<&str>,
) -> Result<MetricsResponse, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect(addr).await?;
    let mut request =
        String::from("GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    if let Some(value) = accept_encoding {
        request.push_str(&format!("Accept-Encoding: {value}\r\n"));
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).await?;

    let mut raw = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), stream.read_to_end(&mut raw))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "response timed out"))??;

    let split = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("response headers");
    let head = String::from_utf8_lossy(&raw[..split]).into_owned();
    let body = &raw[split + 4..];

    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status code");
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(": ") {
            headers.insert(name.to_ascii_lowercase(), value.to_owned());
        }
    }

    let content_encoding = headers.get("content-encoding").cloned().unwrap_or_default();
    let body: Vec<u8> = if content_encoding == "gzip" {
        let mut decoded = Vec::new();
        GzDecoder::new(body).read_to_end(&mut decoded)?;
        decoded
    } else {
        body.to_vec()
    };

    Ok(MetricsResponse {
        status,
        content_type: headers.get("content-type").cloned().unwrap_or_default(),
        content_encoding,
        x_content_type_options: headers
            .get("x-content-type-options")
            .cloned()
            .unwrap_or_default(),
        body: String::from_utf8_lossy(&body).into_owned(),
    })
}

fn sorted_lines(body: &str) -> Vec<String> {
    let mut lines: Vec<String> = body
        .trim_end_matches('\n')
        .split('\n')
        .map(str::to_owned)
        .collect();
    lines.sort();
    lines
}

async fn clear_diff_keys(kv: &KeyValue) {
    let keys = kv
        .keys(&format!("{PREFIX}*"))
        .await
        .expect("list diff keys");
    for key in keys {
        kv.del(&key).await.expect("delete diff key");
    }
}

fn label(name: &str, value: &str) -> LabelPair {
    LabelPair {
        name: name.to_owned(),
        value: value.to_owned(),
    }
}

fn counter_and_gauge_families() -> Vec<MetricFamily> {
    let counter = MetricFamily {
        name: "diff_counter_total".to_owned(),
        help: Some("Counter help with \"quotes\", \\ and\nnewline".to_owned()),
        metric_type: MetricType::Counter,
        metrics: vec![Metric {
            labels: vec![
                label("path", "/a b"),
                label("quote", "\""),
                label("newline", "a\nb"),
                label("backslash", "\\"),
            ],
            timestamp_ms: Some(1_700_000_000_000),
            counter: Some(3.0),
            ..Metric::default()
        }],
    };

    let gauge = MetricFamily {
        name: "diff_gauge".to_owned(),
        help: Some("Gauge help".to_owned()),
        metric_type: MetricType::Gauge,
        metrics: vec![
            Metric {
                gauge: Some(1e-05),
                ..Metric::default()
            },
            gauge_metric("big", 1e21),
            gauge_metric("nan", f64::NAN),
            gauge_metric("pinf", f64::INFINITY),
            gauge_metric("ninf", f64::NEG_INFINITY),
            gauge_metric("zero", 0.0),
            gauge_metric("one", 1.0),
            gauge_metric("negone", -1.0),
            gauge_metric("tiny", 5e-324),
            gauge_metric("frac", 0.000123456789),
        ],
    };

    vec![counter, gauge]
}

fn gauge_metric(kind: &str, value: f64) -> Metric {
    Metric {
        labels: vec![label("kind", kind)],
        gauge: Some(value),
        ..Metric::default()
    }
}

fn summary_and_histogram_families() -> Vec<MetricFamily> {
    let summary = MetricFamily {
        name: "diff_summary".to_owned(),
        help: Some("Summary help".to_owned()),
        metric_type: MetricType::Summary,
        metrics: vec![Metric {
            summary: Some(Summary {
                quantiles: vec![
                    Quantile {
                        quantile: 0.5,
                        value: 5.4e-05,
                    },
                    Quantile {
                        quantile: 0.9,
                        value: 0.001,
                    },
                    Quantile {
                        quantile: 0.99,
                        value: 0.0015,
                    },
                ],
                sample_sum: 175.60473,
                sample_count: 2693,
            }),
            ..Metric::default()
        }],
    };

    let histogram = MetricFamily {
        name: "diff_histogram".to_owned(),
        help: Some("Histogram help".to_owned()),
        metric_type: MetricType::Histogram,
        metrics: vec![
            Metric {
                labels: vec![label("method", "get")],
                histogram: Some(Histogram {
                    buckets: vec![
                        Bucket {
                            upper_bound: 0.05,
                            cumulative_count: 25,
                        },
                        Bucket {
                            upper_bound: 0.1,
                            cumulative_count: 269,
                        },
                        Bucket {
                            upper_bound: f64::INFINITY,
                            cumulative_count: 2693,
                        },
                    ],
                    sample_sum: 175.60473,
                    sample_count: 2693,
                }),
                ..Metric::default()
            },
            Metric {
                // No +Inf bucket: the encoder must append one.
                histogram: Some(Histogram {
                    buckets: vec![
                        Bucket {
                            upper_bound: 0.5,
                            cumulative_count: 10,
                        },
                        Bucket {
                            upper_bound: 1.0,
                            cumulative_count: 20,
                        },
                    ],
                    sample_sum: 42.5,
                    sample_count: 25,
                }),
                ..Metric::default()
            },
        ],
    };

    vec![summary, histogram]
}

fn untyped_family() -> MetricFamily {
    MetricFamily {
        name: "diff_untyped".to_owned(),
        help: None,
        metric_type: MetricType::Untyped,
        metrics: vec![Metric {
            untyped: Some(42.0),
            ..Metric::default()
        }],
    }
}
