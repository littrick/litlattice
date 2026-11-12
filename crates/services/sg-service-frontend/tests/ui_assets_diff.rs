//! M9.10b differential harness: replays the shared corpora through `sg_ui` and
//! `ui_assets` and emits the same normalized JSON as the Go oracles
//! (`cmd/frontend/internal/app/assetsutil/zz_m910b_oracle_test.go`,
//! `cmd/frontend/internal/app/ui/zz_m910b_ui_oracle_test.go`).
//!
//! Gated on `M910B_ASSETS_CORPUS`/`M910B_ASSETS_OUT` and
//! `M910B_SHELL_CORPUS`/`M910B_SHELL_OUT`; `run_ui_assets_diff.sh` sets them.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use base64::Engine as _;
use http::{HeaderMap, Method, Request};
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize};
use sg_service_frontend::app_handlers::{AppConfig, AppDeps, AppEnv};
use sg_service_frontend::{app_router, ui_assets};
use sg_ui::{BundleField, UiAssets, WebBuildManifest};

// ---------------------------------------------------------------------------
// assets corpus
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct CorpusFile {
    path: String,
    content_b64: String,
}

#[derive(Deserialize)]
struct AssetCase {
    name: String,
    method: String,
    path: String,
    #[serde(default)]
    accept_encoding: String,
    #[serde(default)]
    cors_origin: String,
}

#[derive(Deserialize)]
struct ManifestCase {
    name: String,
    #[serde(default)]
    content_b64: String,
}

#[derive(Deserialize)]
struct UrlCase {
    name: String,
    assets_root: String,
    #[serde(default)]
    path: String,
}

#[derive(Deserialize)]
struct HashedCase {
    name: String,
    assets_root: String,
    #[serde(default)]
    main_js: String,
    #[serde(default)]
    main_css: String,
    #[serde(default)]
    embed_js: String,
    #[serde(default)]
    embed_css: String,
    field: String,
}

#[derive(Deserialize)]
struct AssetsCorpus {
    files: Vec<CorpusFile>,
    #[serde(default)]
    dirs: Vec<String>,
    asset_cases: Vec<AssetCase>,
    manifest_cases: Vec<ManifestCase>,
    url_cases: Vec<UrlCase>,
    hashed_cases: Vec<HashedCase>,
}

#[derive(Serialize)]
struct AssetResult {
    name: String,
    status: u16,
    location: String,
    content_type: String,
    cache_control: String,
    content_encoding: String,
    vary: String,
    allow: String,
    x_frame_options: String,
    content_security_policy: String,
    x_content_type_options: String,
    content_length: String,
    accept_ranges: String,
    body_b64: String,
}

#[derive(Serialize)]
struct ManifestResult {
    name: String,
    class: String,
    main_js: String,
    main_css: String,
    embed_js: String,
    embed_css: String,
}

#[derive(Serialize)]
struct UrlResult {
    name: String,
    url: String,
}

#[derive(Serialize)]
struct AssetsOut {
    asset_results: Vec<AssetResult>,
    manifest_results: Vec<ManifestResult>,
    url_results: Vec<UrlResult>,
    hashed_results: Vec<UrlResult>,
}

struct TempDir(PathBuf);

impl TempDir {
    fn new(prefix: &str) -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "{prefix}_{}_{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn header(headers: &HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_owned()
}

fn materialize(root: &Path, files: &[CorpusFile], dirs: &[String]) {
    for dir in dirs {
        std::fs::create_dir_all(root.join(dir.trim_start_matches('/')))
            .unwrap_or_else(|error| panic!("mkdir {dir}: {error}"));
    }
    for file in files {
        let data = base64::engine::general_purpose::STANDARD
            .decode(&file.content_b64)
            .expect("decode corpus file");
        let target = root.join(file.path.trim_start_matches('/'));
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent).expect("create parent dir");
        }
        std::fs::write(&target, data).expect("write corpus file");
    }
}

fn write_json<T: Serialize>(file: &str, value: &T) {
    let mut text = serde_json::to_string_pretty(value).expect("serialize output");
    text.push('\n');
    std::fs::write(file, text).expect("write output");
}

#[tokio::test]
async fn ui_assets_diff() {
    let (Ok(corpus_path), Ok(out_path)) = (
        std::env::var("M910B_ASSETS_CORPUS"),
        std::env::var("M910B_ASSETS_OUT"),
    ) else {
        eprintln!("M910B_ASSETS_CORPUS/M910B_ASSETS_OUT not set; skipping");
        return;
    };
    let corpus: AssetsCorpus =
        serde_json::from_str(&std::fs::read_to_string(&corpus_path).expect("read corpus"))
            .expect("parse corpus");

    let root = TempDir::new("m910b_assets");
    materialize(root.path(), &corpus.files, &corpus.dirs);
    let assets = UiAssets::for_path(root.path());
    let reader = assets.readonly();

    let mut asset_results = Vec::with_capacity(corpus.asset_cases.len());
    for case in &corpus.asset_cases {
        let mut builder = Request::builder()
            .method(Method::from_bytes(case.method.as_bytes()).expect("method"))
            .uri(case.path.as_str());
        if !case.accept_encoding.is_empty() {
            builder = builder.header(http::header::ACCEPT_ENCODING, &case.accept_encoding);
        }
        let req = builder.body(()).expect("request");
        // Go's handler reads the decoded URL path and recovers the query from
        // the request; mirror that split rather than feeding "path?query".
        let path = req.uri().path().to_owned();
        let response = ui_assets::serve_asset(&req, &path, &case.cors_origin, reader.as_ref());
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        asset_results.push(AssetResult {
            name: case.name.clone(),
            status,
            location: header(&headers, "location"),
            content_type: header(&headers, "content-type"),
            cache_control: header(&headers, "cache-control"),
            content_encoding: header(&headers, "content-encoding"),
            vary: header(&headers, "vary"),
            allow: header(&headers, "allow"),
            x_frame_options: header(&headers, "x-frame-options"),
            content_security_policy: header(&headers, "content-security-policy"),
            x_content_type_options: header(&headers, "x-content-type-options"),
            content_length: header(&headers, "content-length"),
            accept_ranges: header(&headers, "accept-ranges"),
            body_b64: base64::engine::general_purpose::STANDARD.encode(body),
        });
    }

    let mut manifest_results = Vec::with_capacity(corpus.manifest_cases.len());
    for case in &corpus.manifest_cases {
        let dir = root.path().join(format!("manifest_{}", case.name));
        std::fs::create_dir_all(&dir).expect("manifest dir");
        if !case.content_b64.is_empty() {
            let data = base64::engine::general_purpose::STANDARD
                .decode(&case.content_b64)
                .expect("decode manifest");
            std::fs::write(dir.join("web.manifest.json"), data).expect("write manifest");
        }
        let provider = UiAssets::for_path(&dir);
        let mut row = ManifestResult {
            name: case.name.clone(),
            class: String::new(),
            main_js: String::new(),
            main_css: String::new(),
            embed_js: String::new(),
            embed_css: String::new(),
        };
        match provider.load_web_build_manifest() {
            Ok(manifest) => {
                row.class = "ok".to_owned();
                row.main_js = manifest.main_js_bundle_path;
                row.main_css = manifest.main_css_bundle_path;
                row.embed_js = manifest.embed_js_bundle_path;
                row.embed_css = manifest.embed_css_bundle_path;
            }
            Err(error) => {
                row.class = match error {
                    sg_ui::ManifestError::NotFound { .. } => "notfound",
                    sg_ui::ManifestError::Parse { .. } => "parse",
                    sg_ui::ManifestError::Io { .. } => "io",
                    sg_ui::ManifestError::NotConfigured => "notconfigured",
                }
                .to_owned();
            }
        }
        manifest_results.push(row);
    }

    let url_results = corpus
        .url_cases
        .iter()
        .map(|case| UrlResult {
            name: case.name.clone(),
            url: sg_ui::asset_url(&case.assets_root, &case.path),
        })
        .collect();

    let hashed_results = corpus
        .hashed_cases
        .iter()
        .map(|case| {
            let manifest = WebBuildManifest {
                main_js_bundle_path: case.main_js.clone(),
                main_css_bundle_path: case.main_css.clone(),
                embed_js_bundle_path: case.embed_js.clone(),
                embed_css_bundle_path: case.embed_css.clone(),
            };
            let field = match case.field.as_str() {
                "main_js" => BundleField::MainJs,
                "main_css" => BundleField::MainCss,
                "embed_js" => BundleField::EmbedJs,
                "embed_css" => BundleField::EmbedCss,
                other => panic!("unknown bundle field {other}"),
            };
            UrlResult {
                name: case.name.clone(),
                url: sg_ui::hashed_asset_url(&case.assets_root, &manifest, field),
            }
        })
        .collect();

    write_json(
        &out_path,
        &AssetsOut {
            asset_results,
            manifest_results,
            url_results,
            hashed_results,
        },
    );
}

// ---------------------------------------------------------------------------
// shell corpus
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ShellCase {
    name: String,
    method: String,
    target: String,
    #[serde(default)]
    dotcom: bool,
    #[serde(default)]
    build_version: String,
    #[serde(default)]
    brand_name: String,
    #[serde(default)]
    branding_has_theme: bool,
    #[serde(default)]
    manifest_mode: String,
    #[serde(default)]
    referer: String,
    #[serde(default)]
    capture_body: bool,
}

#[derive(Deserialize)]
struct ShellCorpus {
    cases: Vec<ShellCase>,
}

#[derive(Serialize)]
struct Preload {
    #[serde(rename = "as")]
    as_: String,
    href: String,
}

#[derive(Serialize)]
struct ShellResult {
    name: String,
    status: u16,
    location: String,
    x_robots_tag: String,
    title: String,
    css_href: String,
    js_src: String,
    preloads: Vec<Preload>,
    content_type: String,
    access_control_allow_origin: String,
    access_control_allow_credentials: String,
    body: String,
}

fn capture(re: &regex::Regex, body: &str) -> String {
    re.captures(body)
        .and_then(|captures| captures.get(1))
        .map(|m| m.as_str().to_owned())
        .unwrap_or_default()
}

#[tokio::test]
async fn ui_shell_diff() {
    let (Ok(corpus_path), Ok(out_path)) = (
        std::env::var("M910B_SHELL_CORPUS"),
        std::env::var("M910B_SHELL_OUT"),
    ) else {
        eprintln!("M910B_SHELL_CORPUS/M910B_SHELL_OUT not set; skipping");
        return;
    };
    let corpus: ShellCorpus =
        serde_json::from_str(&std::fs::read_to_string(&corpus_path).expect("read corpus"))
            .expect("parse corpus");

    let title_re = regex::Regex::new(r"<title>([^<]*)</title>").unwrap();
    let css_re = regex::Regex::new(r#"<link rel="stylesheet" href="([^"]*)">"#).unwrap();
    let js_re =
        regex::Regex::new(r#"<script src="([^"]*)" type="module"></script>"#).unwrap();
    let preload_re =
        regex::Regex::new(r#"<link rel="preload" href="([^"]*)" as="([^"]*)" />"#).unwrap();

    let root = TempDir::new("m910b_shell");
    let mut results = Vec::with_capacity(corpus.cases.len());
    for case in &corpus.cases {
        let dir = root.path().join(&case.name);
        std::fs::create_dir_all(&dir).expect("case dir");
        match case.manifest_mode.as_str() {
            "missing" => {}
            "parse" => {
                std::fs::write(dir.join("web.manifest.json"), b"{").expect("write manifest");
            }
            _ => {
                std::fs::write(
                    dir.join("web.manifest.json"),
                    br#"{"main.js":"main.abc.js","main.css":"main.def.css"}"#,
                )
                .expect("write manifest");
            }
        }

        let env = AppEnv {
            dotcom: case.dotcom,
            build_version: if case.build_version.is_empty() {
                "0.0.0+dev".to_owned()
            } else {
                case.build_version.clone()
            },
            ..AppEnv::default()
        };
        let config = AppConfig {
            brand_name: if case.brand_name.is_empty() {
                "Sourcegraph".to_owned()
            } else {
                case.brand_name.clone()
            },
            branding_has_theme: case.branding_has_theme,
            ..AppConfig::default()
        };
        let deps = AppDeps {
            ui_assets: Some(Arc::new(UiAssets::for_path(&dir))),
            ..AppDeps::default()
        };
        let mut builder = Request::builder()
            .method(Method::from_bytes(case.method.as_bytes()).expect("method"))
            .uri(case.target.as_str());
        if !case.referer.is_empty() {
            builder = builder.header(http::header::REFERER, case.referer.as_str());
        }
        let mut req = builder.body(()).expect("request");
        let response = app_router::dispatch_async(&mut req, &env, &config, &deps).await;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let body = String::from_utf8_lossy(&body).into_owned();
        let preloads = preload_re
            .captures_iter(&body)
            .map(|captures| Preload {
                href: captures.get(1).map(|m| m.as_str().to_owned()).unwrap_or_default(),
                as_: captures.get(2).map(|m| m.as_str().to_owned()).unwrap_or_default(),
            })
            .collect();
        results.push(ShellResult {
            name: case.name.clone(),
            status,
            location: header(&headers, "location"),
            x_robots_tag: header(&headers, "x-robots-tag"),
            title: capture(&title_re, &body),
            css_href: capture(&css_re, &body),
            js_src: capture(&js_re, &body),
            preloads,
            content_type: if case.capture_body {
                header(&headers, "content-type")
            } else {
                String::new()
            },
            access_control_allow_origin: header(
                &headers,
                "access-control-allow-origin",
            ),
            access_control_allow_credentials: header(
                &headers,
                "access-control-allow-credentials",
            ),
            body: if case.capture_body { body } else { String::new() },
        });
    }

    write_json(&out_path, &results);
}
