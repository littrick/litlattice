//! M9.49b: `/.assets` + SPA shell smoke over a fixture `web.manifest.json` and
//! a fake `assets-dist` directory.
//!
//! No port is bound: the handlers are called directly, exactly as the mux
//! calls them after stripping `/.assets`. The fixture directory is either
//! supplied by `test_ws/p9-go/run_assets_dist_smoke.sh` through
//! `ASSETS_DIST_SMOKE_FIXTURE`, or created in a temp directory when the test
//! runs standalone (e.g. `cargo test --all-targets`).
//!
//! Semantics pinned here (see `dev/rs/docs/overviews/m9_49b_web_assets_nginx_overview.md`):
//! 200 on a hit with immutable caching, 404/403/405, `.br`/`.gz` precompressed
//! variants, directory slash redirects, `path.Clean` semantics, MIME overrides,
//! the 200 shell with hashed bundles, and the Go `DirProvider` 500 when the
//! manifest is absent (vs. the explicit embedded `{}` manifest → 200).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use http::header::{self, HeaderMap};
use http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use sg_service_frontend::app_handlers::{AppConfig, AppDeps, AppEnv};
use sg_service_frontend::ui_assets::{serve_asset, serve_ui};
use sg_ui::{AssetsProvider, DirAssets, EmbeddedProvider, UiAssets};
use tonic::body::BoxBody;

const MANIFEST: &str = r#"{"main.js":"main.abc.js","main.css":"main.def.css","embed.js":"embed.abc.js","embed.css":"embed.def.css"}"#;

/// Counts the assertions it observes and prints the acceptance marker once.
struct Checker {
    count: usize,
}

impl Checker {
    fn new() -> Self {
        Self { count: 0 }
    }

    fn check(&mut self, label: &str, condition: bool) {
        assert!(condition, "assertion failed: {label}");
        self.count += 1;
    }

    fn finish(self) {
        assert!(
            self.count >= 12,
            "expected at least 12 assertions, got {}",
            self.count
        );
        println!("ASSETS_DIST_SMOKE_OK ({} assertions)", self.count);
    }
}

fn create_fixture(root: &Path) {
    fs::create_dir_all(root.join("img")).unwrap();
    fs::create_dir_all(root.join("extension/scripts")).unwrap();
    fs::write(root.join("web.manifest.json"), MANIFEST).unwrap();
    // Raw asset plus its precompressed variants. The bytes need not be valid
    // stream encodings: the handler serves precompressed files verbatim.
    fs::write(root.join("main.abc.js"), b"console.log('raw');\n").unwrap();
    fs::write(root.join("main.abc.js.br"), b"BROTLI-BYTES").unwrap();
    fs::write(root.join("main.def.css"), b"body{color:red}\n").unwrap();
    fs::write(root.join("main.def.css.gz"), b"GZIP-BYTES").unwrap();
    fs::write(
        root.join("img/mark.svg"),
        b"<svg xmlns=\"http://www.w3.org/2000/svg\"></svg>",
    )
    .unwrap();
    fs::write(
        root.join("extension/scripts/phabricator.bundle.js"),
        b"phabricator",
    )
    .unwrap();
    fs::write(root.join("blob.bin"), [0x00, 0x01, 0x02, 0xff]).unwrap();
}

fn fixture_dir(test_name: &str) -> (PathBuf, bool) {
    match std::env::var("ASSETS_DIST_SMOKE_FIXTURE") {
        Ok(path) => {
            let path = PathBuf::from(path);
            assert!(
                path.join("web.manifest.json").is_file(),
                "ASSETS_DIST_SMOKE_FIXTURE={} lacks web.manifest.json",
                path.display()
            );
            (path, false)
        }
        Err(_) => {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "assets_dist_smoke_{}_{}",
                test_name,
                COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            create_fixture(&path);
            (path, true)
        }
    }
}

fn request(method: Method, target: &str) -> Request<()> {
    Request::builder()
        .method(method)
        .uri(target)
        .body(())
        .expect("request")
}

fn request_with_encoding(target: &str, encoding: &str) -> Request<()> {
    Request::builder()
        .method(Method::GET)
        .uri(target)
        .header(header::ACCEPT_ENCODING, encoding)
        .body(())
        .expect("request")
}

async fn body(response: http::Response<BoxBody>) -> Vec<u8> {
    response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes()
        .to_vec()
}

fn header_str<'a>(headers: &'a HeaderMap, name: header::HeaderName) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn html_body(response: http::Response<BoxBody>) -> impl std::future::Future<Output = String> {
    async move {
        String::from_utf8(body(response).await).expect("utf-8 html")
    }
}

fn deps_for(ui: Arc<UiAssets>) -> AppDeps {
    AppDeps {
        ui_assets: Some(ui as Arc<dyn AssetsProvider>),
        ..AppDeps::default()
    }
}

fn empty_dir(test_name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "assets_dist_smoke_empty_{}_{}",
        test_name,
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).unwrap();
    path
}

#[tokio::test]
async fn assets_dist_smoke() {
    let (fixture, owned) = fixture_dir("fixture");
    let reader = Arc::new(DirAssets::new(fixture.clone()));
    let ui = Arc::new(UiAssets::new(
        fixture.clone(),
        Arc::clone(&reader) as Arc<dyn sg_ui::AssetReader>,
    ));
    let deps = deps_for(Arc::clone(&ui));
    let env = AppEnv::default();
    let config = AppConfig::default();
    let mut check = Checker::new();

    // 1. Hashed JS: 200, executable MIME override, immutable cache, exact bytes.
    let response = serve_asset(
        &request(Method::GET, "/.assets/main.abc.js"),
        "/main.abc.js",
        "",
        reader.as_ref(),
    );
    check.check("js status 200", response.status() == StatusCode::OK);
    check.check(
        "js content-type application/javascript",
        header_str(response.headers(), header::CONTENT_TYPE) == Some("application/javascript"),
    );
    check.check(
        "js immutable cache",
        header_str(response.headers(), header::CACHE_CONTROL)
            == Some("immutable, max-age=31536000, public"),
    );
    check.check(
        "js body",
        body(response).await == b"console.log('raw');\n",
    );

    // 2. Hashed CSS: 200 with the mime table's charset.
    let response = serve_asset(
        &request(Method::GET, "/.assets/main.def.css"),
        "/main.def.css",
        "",
        reader.as_ref(),
    );
    check.check("css status 200", response.status() == StatusCode::OK);
    check.check(
        "css content-type text/css; charset=utf-8",
        header_str(response.headers(), header::CONTENT_TYPE) == Some("text/css; charset=utf-8"),
    );

    // 3. Missing file: 404 text/plain and no cache header (never cache a 404).
    let response = serve_asset(
        &request(Method::GET, "/.assets/missing.js"),
        "/missing.js",
        "",
        reader.as_ref(),
    );
    check.check("missing status 404", response.status() == StatusCode::NOT_FOUND);
    check.check(
        "missing content-type text/plain",
        header_str(response.headers(), header::CONTENT_TYPE) == Some("text/plain; charset=utf-8"),
    );
    check.check(
        "missing has no cache-control",
        response.headers().get(header::CACHE_CONTROL).is_none(),
    );
    check.check("missing body", body(response).await == b"404 Not Found\n");

    // 4. Non-GET: 405 with Allow: GET (HEAD included, per httpgzip).
    let response = serve_asset(
        &request(Method::POST, "/.assets/main.abc.js"),
        "/main.abc.js",
        "",
        reader.as_ref(),
    );
    check.check(
        "post status 405",
        response.status() == StatusCode::METHOD_NOT_ALLOWED,
    );
    check.check(
        "post Allow: GET",
        header_str(response.headers(), header::ALLOW) == Some("GET"),
    );
    let response = serve_asset(
        &request(Method::HEAD, "/.assets/main.abc.js"),
        "/main.abc.js",
        "",
        reader.as_ref(),
    );
    check.check(
        "head status 405",
        response.status() == StatusCode::METHOD_NOT_ALLOWED,
    );

    // 5. Directories: slashless → 301 `<base>/`; trailing slash → 403
    //    (DisableDirListing); the asset root `/` itself → 403.
    let response = serve_asset(
        &request(Method::GET, "/.assets/img"),
        "/img",
        "",
        reader.as_ref(),
    );
    check.check(
        "dir slashless 301",
        response.status() == StatusCode::MOVED_PERMANENTLY,
    );
    check.check(
        "dir slashless Location img/",
        header_str(response.headers(), header::LOCATION) == Some("img/"),
    );
    let response = serve_asset(
        &request(Method::GET, "/.assets/img/"),
        "/img/",
        "",
        reader.as_ref(),
    );
    check.check(
        "dir with slash 403",
        response.status() == StatusCode::FORBIDDEN,
    );
    let response = serve_asset(
        &request(Method::GET, "/.assets"),
        "",
        "",
        reader.as_ref(),
    );
    check.check(
        "asset root 403",
        response.status() == StatusCode::FORBIDDEN,
    );

    // 6. The manifest is itself served from `/.assets`.
    let response = serve_asset(
        &request(Method::GET, "/.assets/web.manifest.json"),
        "/web.manifest.json",
        "",
        reader.as_ref(),
    );
    check.check(
        "manifest status 200",
        response.status() == StatusCode::OK,
    );
    check.check(
        "manifest content-type application/json",
        header_str(response.headers(), header::CONTENT_TYPE) == Some("application/json"),
    );

    // 7. Brotli precompressed variant: encoding + Vary, no Content-Length.
    let response = serve_asset(
        &request_with_encoding("/.assets/main.abc.js", "br"),
        "/main.abc.js",
        "",
        reader.as_ref(),
    );
    check.check("br status 200", response.status() == StatusCode::OK);
    check.check(
        "br Content-Encoding: br",
        header_str(response.headers(), header::CONTENT_ENCODING) == Some("br"),
    );
    check.check(
        "br Vary contains br",
        header_str(response.headers(), header::VARY).is_some_and(|value| value.contains("br")),
    );
    check.check(
        "br has no Content-Length",
        response.headers().get(header::CONTENT_LENGTH).is_none(),
    );
    check.check("br body", body(response).await == b"BROTLI-BYTES");

    // 8. Gzip precompressed variant for the stylesheet.
    let response = serve_asset(
        &request_with_encoding("/.assets/main.def.css", "gzip"),
        "/main.def.css",
        "",
        reader.as_ref(),
    );
    check.check("gzip status 200", response.status() == StatusCode::OK);
    check.check(
        "gzip Content-Encoding: gzip",
        header_str(response.headers(), header::CONTENT_ENCODING) == Some("gzip"),
    );
    check.check(
        "gzip Vary contains gzip",
        header_str(response.headers(), header::VARY).is_some_and(|value| value.contains("gzip")),
    );
    check.check(
        "gzip has no Content-Length",
        response.headers().get(header::CONTENT_LENGTH).is_none(),
    );
    check.check("gzip body", body(response).await == b"GZIP-BYTES");

    // 9. Compressed file absent → raw bytes with Content-Length, no encoding.
    let response = serve_asset(
        &request_with_encoding("/.assets/main.def.css", "br"),
        "/main.def.css",
        "",
        reader.as_ref(),
    );
    check.check(
        "missing br falls back to 200",
        response.status() == StatusCode::OK,
    );
    check.check(
        "missing br has no Content-Encoding",
        response.headers().get(header::CONTENT_ENCODING).is_none(),
    );
    check.check(
        "missing br sets Content-Length",
        response.headers().get(header::CONTENT_LENGTH).is_some(),
    );

    // 10. The Phabricator bundle keeps its 5-minute cache exception.
    let response = serve_asset(
        &request(Method::GET, "/.assets/extension/scripts/phabricator.bundle.js"),
        "/extension/scripts/phabricator.bundle.js",
        "",
        reader.as_ref(),
    );
    check.check(
        "phabricator cache max-age=300",
        header_str(response.headers(), header::CACHE_CONTROL) == Some("max-age=300, public"),
    );

    // 11. `path.Clean` before opening: `..` is resolved, not served as a
    //     directory traversal.
    let response = serve_asset(
        &request(Method::GET, "/.assets/img/../main.abc.js"),
        "/img/../main.abc.js",
        "",
        reader.as_ref(),
    );
    check.check(
        "cleaned path status 200",
        response.status() == StatusCode::OK,
    );
    check.check(
        "cleaned path body",
        body(response).await == b"console.log('raw');\n",
    );
    let response = serve_asset(
        &request(Method::GET, "/.assets/../etc/passwd"),
        "/../etc/passwd",
        "",
        reader.as_ref(),
    );
    check.check(
        "dotdot escape stays under root (404)",
        response.status() == StatusCode::NOT_FOUND,
    );

    // 12. MIME: explicit `.svg` override and the binary fallback.
    let response = serve_asset(
        &request(Method::GET, "/.assets/img/mark.svg"),
        "/img/mark.svg",
        "",
        reader.as_ref(),
    );
    check.check(
        "svg content-type image/svg+xml",
        header_str(response.headers(), header::CONTENT_TYPE) == Some("image/svg+xml"),
    );
    let response = serve_asset(
        &request(Method::GET, "/.assets/blob.bin"),
        "/blob.bin",
        "",
        reader.as_ref(),
    );
    check.check(
        "binary content-type octet-stream",
        header_str(response.headers(), header::CONTENT_TYPE) == Some("application/octet-stream"),
    );

    // 13. SPA shell: 200 HTML referencing the hashed bundles.
    let response = serve_ui(
        &request(Method::GET, "/search"),
        "/search",
        &env,
        &config,
        &deps,
    );
    check.check(
        "shell status 200",
        response.status() == StatusCode::OK,
    );
    check.check(
        "shell content-type html",
        header_str(response.headers(), header::CONTENT_TYPE) == Some("text/html; charset=utf-8"),
    );
    let html = html_body(response).await;
    check.check(
        "shell stylesheet href",
        html.contains("href=\"/.assets/main.def.css\""),
    );
    check.check(
        "shell module script src",
        html.contains("src=\"/.assets/main.abc.js\" type=\"module\""),
    );

    // 14. `ASSETS_ROOT` only changes the URLs in the shell, never the local
    //     reader path.
    let mut absolute_env = AppEnv::default();
    absolute_env.assets_root = "https://cdn.example/base".to_owned();
    let response = serve_ui(
        &request(Method::GET, "/search"),
        "/search",
        &absolute_env,
        &config,
        &deps,
    );
    check.check(
        "absolute assets_root shell 200",
        response.status() == StatusCode::OK,
    );
    let html = html_body(response).await;
    check.check(
        "absolute css URL",
        html.contains("href=\"https://cdn.example/base/main.def.css\""),
    );
    check.check(
        "absolute js URL",
        html.contains("src=\"https://cdn.example/base/main.abc.js\" type=\"module\""),
    );
    let response = serve_asset(
        &request(Method::GET, "/.assets/main.abc.js"),
        "/main.abc.js",
        "",
        reader.as_ref(),
    );
    check.check(
        "assets_root does not move the local reader",
        response.status() == StatusCode::OK,
    );

    // 15. Go `DirProvider` with no manifest → shell 500; unknown route is still
    //     the 404 error page.
    let missing_dir = empty_dir("no_manifest");
    let missing_ui = Arc::new(UiAssets::for_path(&missing_dir));
    let missing_deps = deps_for(missing_ui);
    let response = serve_ui(
        &request(Method::GET, "/search"),
        "/search",
        &env,
        &config,
        &missing_deps,
    );
    check.check(
        "missing manifest shell 500",
        response.status() == StatusCode::INTERNAL_SERVER_ERROR,
    );
    check.check(
        "missing manifest 500 is HTML",
        header_str(response.headers(), header::CONTENT_TYPE) == Some("text/html; charset=utf-8"),
    );
    let html = html_body(response).await;
    check.check(
        "missing manifest 500 title",
        html.contains("<title>500 Internal Server Error - Sourcegraph</title>"),
    );
    let response = serve_ui(
        &request(Method::GET, "/nope@@x"),
        "/nope@@x",
        &env,
        &config,
        &missing_deps,
    );
    check.check(
        "missing manifest unknown route 404 error page",
        response.status() == StatusCode::NOT_FOUND,
    );
    let html = html_body(response).await;
    check.check(
        "missing manifest 404 error title",
        html.contains("<title>404 Not Found - Sourcegraph</title>"),
    );

    // 16. The explicit Rust-only embedded opt-in (`{}` manifest, Go's
    //     `no_client_bundle.image`) is a 200 shell without bundles.
    let embedded_deps = AppDeps {
        ui_assets: Some(Arc::new(EmbeddedProvider::new()) as Arc<dyn AssetsProvider>),
        ..AppDeps::default()
    };
    let response = serve_ui(
        &request(Method::GET, "/search"),
        "/search",
        &env,
        &config,
        &embedded_deps,
    );
    check.check(
        "embedded dummy manifest shell 200",
        response.status() == StatusCode::OK,
    );
    let html = html_body(response).await;
    check.check(
        "embedded shell keeps the brand title",
        html.contains("<title>Sourcegraph</title>"),
    );
    check.check(
        "embedded shell has no hashed bundle",
        !html.contains("main.abc.js") && !html.contains("main.def.css"),
    );

    check.finish();

    let _ = fs::remove_dir_all(&missing_dir);
    if owned {
        let _ = fs::remove_dir_all(&fixture);
    }
}
