#![forbid(unsafe_code)]

//! `sg-ui`: the static asset surface of the baseline `ui/assets` +
//! `cmd/frontend/internal/app/assetsutil` packages.
//!
//! The crate is deliberately free of HTTP and database dependencies: it loads
//! the web build manifest (`web.manifest.json`), resolves `ASSETS_ROOT`-based
//! URLs (including the hashed bundle names the manifest carries), and exposes a
//! [`AssetReader`] filesystem abstraction that mirrors `http.Dir.Open`. The
//! HTTP response shape is built by `sg-service-frontend::ui_assets`.
//!
//! Baseline references:
//!
//! * `ui/assets/assets.go:39-97` — `DirProvider`, `loadWebBuildManifest`;
//! * `ui/assets/manifest.go:3-8` — `WebBuildManifest`;
//! * `cmd/frontend/internal/app/assetsutil/url.go:8-31` — `URL`.
//!
//! The `path`/`net/url` helpers below are line-by-line ports of Go's
//! `path.Clean`, `path.Join`, `net/url.resolvePath` and `shouldEscape`
//! (`encodePath` mode), because the baseline asset URLs are observable.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};

/// The skeleton crate name kept for the M6 layering check.
pub const CRATE_NAME: &str = "sg-ui";

/// `assets.DefaultAssetPath` (`ui/assets/assets.go:14`).
pub const DEFAULT_ASSET_PATH: &str = "/assets-dist";
/// `assets.DevAssetsPath` (`ui/assets/assets.go:18`).
pub const DEV_ASSETS_PATH: &str = "client/web/dist";
/// `assetsutil.assetsRoot` default (`assetsutil/url.go:12`).
pub const DEFAULT_ASSETS_ROOT: &str = "/.assets";
/// The manifest filename read by `loadWebBuildManifest` (`assets.go:88`).
pub const WEB_MANIFEST_FILE: &str = "web.manifest.json";

/// `assets.WebBuildManifest` (`ui/assets/manifest.go:3-8`).
///
/// Missing JSON fields are empty strings and unknown fields are ignored,
/// matching `encoding/json.Unmarshal` into this struct.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WebBuildManifest {
    pub main_js_bundle_path: String,
    pub main_css_bundle_path: String,
    pub embed_js_bundle_path: String,
    pub embed_css_bundle_path: String,
}

/// Why [`load_web_build_manifest`] failed.
///
/// The variants let the UI handler distinguish the baseline `newCommon`
/// fallback (manifest missing → 500 error page) from a normal asset 404.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ManifestError {
    /// `os.ReadFile` returned `IsNotExist`.
    NotFound { root: PathBuf },
    /// Any other read/IO failure.
    Io { root: PathBuf, message: String },
    /// `json.Unmarshal` failed.
    Parse { message: String },
    /// `FailingAssetsProvider`: `assets.Init()` was never called
    /// (`ui/assets/assets.go:36-47`).
    NotConfigured,
}

impl fmt::Display for ManifestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ManifestError::NotConfigured => write!(
                f,
                "assets are not configured for this binary, please see ui/assets/doc.go"
            ),
            ManifestError::NotFound { root } | ManifestError::Io { root, .. } => write!(
                f,
                "failed loading web build manifest file from disk at {:?}: {}",
                root,
                match self {
                    ManifestError::NotFound { .. } => "no such file or directory".to_owned(),
                    ManifestError::Io { message, .. } => message.clone(),
                    ManifestError::Parse { .. } | ManifestError::NotConfigured => unreachable!(),
                }
            ),
            ManifestError::Parse { message } => write!(f, "parsing manifest json: {message}"),
        }
    }
}

impl std::error::Error for ManifestError {}

impl WebBuildManifest {
    /// `json.Unmarshal` of the manifest body.
    pub fn from_json(bytes: &[u8]) -> Result<Self, ManifestError> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|err| ManifestError::Parse {
                message: err.to_string(),
            })?;
        Self::from_value(&value)
    }

    fn from_value(value: &serde_json::Value) -> Result<Self, ManifestError> {
        // `json.Unmarshal("null", &struct)` succeeds and leaves the zero value.
        match value {
            serde_json::Value::Null => return Ok(Self::default()),
            serde_json::Value::Object(_) => {}
            other => {
                return Err(ManifestError::Parse {
                    message: format!(
                        "json: cannot unmarshal {other} into Go value of type assets.WebBuildManifest"
                    ),
                })
            }
        }
        let field = |key: &str| -> Result<String, ManifestError> {
            match value.get(key) {
                None | Some(serde_json::Value::Null) => Ok(String::new()),
                Some(serde_json::Value::String(text)) => Ok(text.clone()),
                Some(other) => Err(ManifestError::Parse {
                    message: format!(
                        "json: cannot unmarshal {other} into Go struct field WebBuildManifest.{key} of type string"
                    ),
                }),
            }
        };
        Ok(Self {
            main_js_bundle_path: field("main.js")?,
            main_css_bundle_path: field("main.css")?,
            embed_js_bundle_path: field("embed.js")?,
            embed_css_bundle_path: field("embed.css")?,
        })
    }

    /// The hashed bundle path for a manifest field, or the empty string when
    /// the field is absent (baseline `app.html` treats it as unset).
    pub fn bundle_path(&self, field: BundleField) -> &str {
        match field {
            BundleField::MainJs => &self.main_js_bundle_path,
            BundleField::MainCss => &self.main_css_bundle_path,
            BundleField::EmbedJs => &self.embed_js_bundle_path,
            BundleField::EmbedCss => &self.embed_css_bundle_path,
        }
    }

    /// `assetsutil.URL(manifest.<field>)`: the hashed asset URL served by the
    /// `/.assets` handler.
    pub fn bundle_url(&self, assets_root: &str, field: BundleField) -> String {
        asset_url(assets_root, self.bundle_path(field))
    }
}

/// The four hashed bundle names carried by the manifest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BundleField {
    MainJs,
    MainCss,
    EmbedJs,
    EmbedCss,
}

/// `assets.loadWebBuildManifest(rootDir)` (`ui/assets/assets.go:79-97`).
pub fn load_web_build_manifest(root: &Path) -> Result<WebBuildManifest, ManifestError> {
    let path = root.join(WEB_MANIFEST_FILE);
    let bytes = std::fs::read(&path).map_err(|err| {
        if err.kind() == std::io::ErrorKind::NotFound {
            ManifestError::NotFound {
                root: root.to_path_buf(),
            }
        } else {
            ManifestError::Io {
                root: root.to_path_buf(),
                message: err.to_string(),
            }
        }
    })?;
    WebBuildManifest::from_json(&bytes)
}

/// `assets.Provider.Assets().Open(path)` errors (`os.IsNotExist` /
/// `os.IsPermission`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AssetError {
    NotFound,
    PermissionDenied,
    Other(String),
}

/// One opened asset: either a regular file's bytes or a directory marker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AssetEntry {
    pub data: Vec<u8>,
    pub is_dir: bool,
}

impl AssetEntry {
    pub fn file(data: Vec<u8>) -> Self {
        Self {
            data,
            is_dir: false,
        }
    }

    pub fn dir() -> Self {
        Self {
            data: Vec::new(),
            is_dir: true,
        }
    }
}

/// The injectable filesystem seam behind `assets.Provider.Assets()`.
pub trait AssetReader: Send + Sync {
    /// `http.FileSystem.Open(name)`; `name` is the request path (already
    /// stripped of `/.assets` by the caller).
    fn open(&self, name: &str) -> Result<AssetEntry, AssetError>;
}

/// `http.Dir(root)` (`net/http/fs.go:56-79`).
#[derive(Clone, Debug)]
pub struct DirAssets {
    root: PathBuf,
}

impl DirAssets {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

impl AssetReader for DirAssets {
    fn open(&self, name: &str) -> Result<AssetEntry, AssetError> {
        // `http.Dir.Open`: clean `"/"+name`, then join under the root.
        let cleaned = go_path_clean(&format!("/{name}"));
        if cleaned.contains('\0') {
            return Err(AssetError::Other("invalid argument".to_owned()));
        }
        let relative = cleaned.trim_start_matches('/');
        let path = if relative.is_empty() {
            self.root.clone()
        } else {
            self.root.join(relative)
        };
        let metadata = std::fs::metadata(&path).map_err(map_io_error)?;
        if metadata.is_dir() {
            Ok(AssetEntry::dir())
        } else {
            std::fs::read(&path)
                .map(AssetEntry::file)
                .map_err(map_io_error)
        }
    }
}

fn map_io_error(err: std::io::Error) -> AssetError {
    match err.kind() {
        std::io::ErrorKind::NotFound => AssetError::NotFound,
        std::io::ErrorKind::PermissionDenied => AssetError::PermissionDenied,
        _ => AssetError::Other(err.to_string()),
    }
}

/// In-memory [`AssetReader`] for tests and injectable harnesses.
///
/// Files are keyed by their cleaned URL path (`/main.js`); a path is a
/// directory when it is listed in `dirs`, is `/`, or is a prefix of a file.
#[derive(Clone, Debug, Default)]
pub struct MemoryAssets {
    files: BTreeMap<String, Vec<u8>>,
    dirs: BTreeSet<String>,
}

impl MemoryAssets {
    pub fn new(files: Vec<(String, Vec<u8>)>, dirs: Vec<String>) -> Self {
        let mut map = BTreeMap::new();
        for (name, data) in files {
            map.insert(go_path_clean(&format!("/{name}")), data);
        }
        let mut dir_set = BTreeSet::new();
        for dir in dirs {
            dir_set.insert(go_path_clean(&format!("/{dir}")));
        }
        Self {
            files: map,
            dirs: dir_set,
        }
    }

    pub fn from_paths<'a>(
        files: impl IntoIterator<Item = (&'a str, &'a [u8])>,
        dirs: impl IntoIterator<Item = &'a str>,
    ) -> Self {
        Self::new(
            files
                .into_iter()
                .map(|(name, data)| (name.to_owned(), data.to_vec()))
                .collect(),
            dirs.into_iter().map(str::to_owned).collect(),
        )
    }
}

impl AssetReader for MemoryAssets {
    fn open(&self, name: &str) -> Result<AssetEntry, AssetError> {
        let cleaned = go_path_clean(&format!("/{name}"));
        if cleaned.contains('\0') {
            return Err(AssetError::Other("invalid argument".to_owned()));
        }
        if cleaned == "/" || self.dirs.contains(&cleaned) {
            return Ok(AssetEntry::dir());
        }
        let prefix = format!("{cleaned}/");
        if self.files.keys().any(|key| key.starts_with(&prefix))
            || self.dirs.iter().any(|key| key.starts_with(&prefix))
        {
            return Ok(AssetEntry::dir());
        }
        self.files
            .get(&cleaned)
            .cloned()
            .map(AssetEntry::file)
            .ok_or(AssetError::NotFound)
    }
}

/// Convenience provider wrapping an [`AssetReader`] plus the manifest root.
#[derive(Clone)]
pub struct UiAssets {
    root: PathBuf,
    reader: Arc<dyn AssetReader>,
}

impl fmt::Debug for UiAssets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UiAssets")
            .field("root", &self.root)
            .finish()
    }
}

impl UiAssets {
    pub fn new(root: impl Into<PathBuf>, reader: Arc<dyn AssetReader>) -> Self {
        Self {
            root: root.into(),
            reader,
        }
    }

    /// The baseline `assets.Init()`/`UseDevAssetsProvider()` choice.
    pub fn for_path(root: impl Into<PathBuf>) -> Self {
        let root = root.into();
        let reader = Arc::new(DirAssets::new(root.clone()));
        Self { root, reader }
    }

    pub fn readonly(&self) -> Arc<dyn AssetReader> {
        Arc::clone(&self.reader)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn load_web_build_manifest(&self) -> Result<WebBuildManifest, ManifestError> {
        load_web_build_manifest(&self.root)
    }
}

/// `assets.AssetsProvider` (`ui/assets/assets.go:27-31`).
pub trait AssetsProvider: Send + Sync {
    fn load_web_build_manifest(&self) -> Result<WebBuildManifest, ManifestError>;
    fn assets(&self) -> Arc<dyn AssetReader>;
}

impl AssetsProvider for UiAssets {
    fn load_web_build_manifest(&self) -> Result<WebBuildManifest, ManifestError> {
        UiAssets::load_web_build_manifest(self)
    }

    fn assets(&self) -> Arc<dyn AssetReader> {
        self.readonly()
    }
}

/// `assets.DirProvider` (`ui/assets/assets.go:60-73`): assets read from a
/// directory on disk.
#[derive(Clone, Debug)]
pub struct DirProvider {
    assets: UiAssets,
}

impl DirProvider {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            assets: UiAssets::for_path(root),
        }
    }
}

impl AssetsProvider for DirProvider {
    fn load_web_build_manifest(&self) -> Result<WebBuildManifest, ManifestError> {
        self.assets.load_web_build_manifest()
    }

    fn assets(&self) -> Arc<dyn AssetReader> {
        self.assets.readonly()
    }
}

/// `assets.FailingAssetsProvider` (`ui/assets/assets.go:36-47`): the explicit
/// "not configured" sentinel. The baseline panics from `Assets()`; the Rust
/// port keeps the error return and makes `open` fail instead.
#[derive(Clone, Copy, Debug, Default)]
pub struct FailingAssetsProvider;

impl AssetsProvider for FailingAssetsProvider {
    fn load_web_build_manifest(&self) -> Result<WebBuildManifest, ManifestError> {
        Err(ManifestError::NotConfigured)
    }

    fn assets(&self) -> Arc<dyn AssetReader> {
        Arc::new(FailingAssetReader)
    }
}

/// The [`AssetReader`] behind [`FailingAssetsProvider`].
#[derive(Clone, Copy, Debug, Default)]
pub struct FailingAssetReader;

impl AssetReader for FailingAssetReader {
    fn open(&self, _name: &str) -> Result<AssetEntry, AssetError> {
        Err(AssetError::Other(
            "assets are not configured for this binary, please see ui/assets/doc.go".to_owned(),
        ))
    }
}

static PROVIDER: OnceLock<RwLock<Arc<dyn AssetsProvider>>> = OnceLock::new();
static CONFIGURED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn provider_cell() -> &'static RwLock<Arc<dyn AssetsProvider>> {
    PROVIDER.get_or_init(|| RwLock::new(Arc::new(FailingAssetsProvider)))
}

/// `assets.Provider` (`ui/assets/assets.go:33`): the process-wide provider.
pub fn provider() -> Arc<dyn AssetsProvider> {
    Arc::clone(&provider_cell().read().expect("assets provider lock"))
}

/// Whether `Init`/`Use*Provider` (or an explicit [`set_provider`]) has run.
pub fn is_configured() -> bool {
    CONFIGURED.load(std::sync::atomic::Ordering::Relaxed)
}

/// Install a provider (tests and alternative embedders).
pub fn set_provider(provider: Arc<dyn AssetsProvider>) {
    *provider_cell().write().expect("assets provider lock") = provider;
    CONFIGURED.store(true, std::sync::atomic::Ordering::Relaxed);
}

/// `assets.Init()` (`ui/assets/assets.go:19-21`, called from
/// `cmd/frontend/main.go:11`).
pub fn init() {
    use_assets_provider_for_path(DEFAULT_ASSET_PATH);
}

/// `assets.UseDevAssetsProvider()` (`ui/assets/assets.go:53-56`, called from
/// `cmd/frontend/shared/service.go:24-26` when `WEB_BUILDER_DEV_SERVER=1`).
pub fn use_dev_assets_provider() {
    use_assets_provider_for_path(DEV_ASSETS_PATH);
}

/// `assets.UseAssetsProviderForPath` (`ui/assets/assets.go:58-61`).
pub fn use_assets_provider_for_path(path: impl Into<PathBuf>) {
    set_provider(Arc::new(DirProvider::new(path)));
}

/// The shared `cmd/frontend/shared/service.go:24-26` choice: dev assets when
/// `WEB_BUILDER_DEV_SERVER=1`, otherwise the binary's default path.
pub fn init_from_env() {
    if std::env::var("WEB_BUILDER_DEV_SERVER").as_deref() == Ok("1") {
        use_dev_assets_provider();
    } else {
        init();
    }
}

/// `assetsutil.URL(p)` (`assetsutil/url.go:19-22`).
///
/// `assets_root` is parsed like `url.Parse`; the result is
/// `<scheme>://<host><resolvePath(basePath, path.Join(basePath, p))>` for an
/// absolute root, or the resolved path for a relative root.
pub fn asset_url(assets_root: &str, p: &str) -> String {
    let (scheme, host, base_path) = split_root(assets_root);
    let joined = path_join(&[base_path, p]);
    if joined.is_empty() && base_path.is_empty() {
        return String::new();
    }
    let resolved = resolve_path(base_path, &joined);
    let escaped = encode_path(&resolved);
    if scheme.is_empty() {
        escaped
    } else if host.is_empty() {
        format!("{scheme}:{escaped}")
    } else {
        format!("{scheme}://{host}{escaped}")
    }
}

/// The hashed asset URL for a manifest field (`main.js` etc.).
pub fn hashed_asset_url(
    assets_root: &str,
    manifest: &WebBuildManifest,
    field: BundleField,
) -> String {
    manifest.bundle_url(assets_root, field)
}

fn split_root(root: &str) -> (&str, &str, &str) {
    if let Some(index) = root.find("://") {
        let scheme = &root[..index];
        let rest = &root[index + 3..];
        return match rest.find('/') {
            Some(slash) => (scheme, &rest[..slash], &rest[slash..]),
            None => (scheme, rest, ""),
        };
    }
    ("", "", root)
}

/// Go `path.Clean` (`src/path/path.go`).
pub fn go_path_clean(path: &str) -> String {
    if path.is_empty() {
        return ".".to_owned();
    }
    let bytes = path.as_bytes();
    let rooted = bytes[0] == b'/';
    let mut out: Vec<&str> = Vec::new();
    let mut index = if rooted { 1 } else { 0 };
    while index < bytes.len() {
        while index < bytes.len() && bytes[index] == b'/' {
            index += 1;
        }
        if index >= bytes.len() {
            break;
        }
        let start = index;
        while index < bytes.len() && bytes[index] != b'/' {
            index += 1;
        }
        let segment = &path[start..index];
        match segment {
            "." => {}
            ".." => {
                if !rooted && (out.is_empty() || out.last() == Some(&"..")) {
                    out.push("..");
                } else {
                    out.pop();
                }
            }
            segment => out.push(segment),
        }
    }
    if out.is_empty() {
        return if rooted { "/" } else { "." }.to_owned();
    }
    let joined = out.join("/");
    if rooted {
        format!("/{joined}")
    } else {
        joined
    }
}

/// Go `path.Join` (`src/path/path.go:155-173`).
pub fn path_join(elements: &[&str]) -> String {
    let size: usize = elements.iter().map(|element| element.len()).sum();
    if size == 0 {
        return String::new();
    }
    let mut buffer = String::with_capacity(size + elements.len());
    for element in elements {
        if !buffer.is_empty() || !element.is_empty() {
            if !buffer.is_empty() {
                buffer.push('/');
            }
            buffer.push_str(element);
        }
    }
    go_path_clean(&buffer)
}

/// Go `net/url.resolvePath` (`src/net/url/url.go:999-1065`).
fn resolve_path(base: &str, reference: &str) -> String {
    let full = if reference.is_empty() {
        base.to_owned()
    } else if !reference.starts_with('/') {
        match base.rfind('/') {
            Some(index) => format!("{}{}", &base[..=index], reference),
            None => reference.to_owned(),
        }
    } else {
        reference.to_owned()
    };
    if full.is_empty() {
        return String::new();
    }

    let mut dest: Vec<&str> = Vec::new();
    let mut last_element = "";
    let mut remaining = full.as_str();
    loop {
        let (element, rest) = match remaining.find('/') {
            Some(index) => (&remaining[..index], &remaining[index + 1..]),
            None => (remaining, ""),
        };
        let has_more = remaining.contains('/');
        last_element = element;
        remaining = rest;
        match element {
            "." => {}
            // Go resets to a single leading '/' when there is nothing left to
            // pop, so leading ".." segments are discarded; they are never kept
            // in the resolved (always-rooted) path.
            ".." => {
                dest.pop();
            }
            element => dest.push(element),
        }
        if !has_more {
            break;
        }
    }

    let mut resolved = format!("/{}", dest.join("/"));
    // Go writes an initial '/', then removes one if the first element was
    // empty (which yields "//").
    if resolved.len() > 1 && resolved.as_bytes()[1] == b'/' {
        resolved.remove(0);
    }
    if (last_element == "." || last_element == "..") && !resolved.ends_with('/') {
        resolved.push('/');
    }
    resolved
}

/// Go `shouldEscape(c, encodePath)` + `URL.EscapedPath`.
fn encode_path(path: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut escaped = String::with_capacity(path.len());
    for byte in path.bytes() {
        if should_escape_path(byte) {
            escaped.push('%');
            escaped.push(HEX[(byte >> 4) as usize] as char);
            escaped.push(HEX[(byte & 0x0f) as usize] as char);
        } else {
            escaped.push(byte as char);
        }
    }
    escaped
}

fn should_escape_path(byte: u8) -> bool {
    if byte.is_ascii_alphanumeric() {
        return false;
    }
    match byte {
        b'-' | b'_' | b'.' | b'~' => false,
        // encodePath allows the reserved characters except '?'.
        b'$' | b'&' | b'+' | b',' | b'/' | b':' | b';' | b'=' | b'@' => false,
        b'?' => true,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_parses_and_defaults() {
        let manifest =
            WebBuildManifest::from_json(br#"{"main.js":"main.abc.js","main.css":"main.def.css"}"#)
                .expect("manifest");
        assert_eq!(manifest.main_js_bundle_path, "main.abc.js");
        assert_eq!(manifest.main_css_bundle_path, "main.def.css");
        assert_eq!(manifest.embed_js_bundle_path, "");
        assert_eq!(manifest.embed_css_bundle_path, "");

        let empty = WebBuildManifest::from_json(b"null").expect("null manifest");
        assert_eq!(empty, WebBuildManifest::default());
        assert!(matches!(
            WebBuildManifest::from_json(b"{"),
            Err(ManifestError::Parse { .. })
        ));
        assert!(matches!(
            WebBuildManifest::from_json(br#"{"main.js":1}"#),
            Err(ManifestError::Parse { .. })
        ));
    }

    #[test]
    fn asset_url_matches_go_resolve_reference() {
        assert_eq!(asset_url("/.assets", "/img/x.svg"), "/.assets/img/x.svg");
        assert_eq!(asset_url("/.assets/", "/img/x.svg"), "/.assets/img/x.svg");
        assert_eq!(asset_url("/.assets", ""), "/.assets");
        assert_eq!(
            asset_url("https://cdn.example/base", "/img/x.svg"),
            "https://cdn.example/base/img/x.svg"
        );
        assert_eq!(
            asset_url("https://cdn.example/base/", "main.abc.js"),
            "https://cdn.example/base/main.abc.js"
        );
        assert_eq!(asset_url("assets", "/img/x.svg"), "/assets/img/x.svg");
        assert_eq!(asset_url("a/b", "/img/x.svg"), "/a/a/b/img/x.svg");
        assert_eq!(asset_url("", "/img/x.svg"), "/img/x.svg");
        assert_eq!(asset_url("", ""), "");
        assert_eq!(asset_url("/.assets", "?v2"), "/.assets/%3Fv2");
    }

    #[test]
    fn clean_and_join_match_go() {
        assert_eq!(go_path_clean(""), ".");
        assert_eq!(go_path_clean("/"), "/");
        assert_eq!(go_path_clean("//"), "/");
        assert_eq!(go_path_clean("/a//b/"), "/a/b");
        assert_eq!(go_path_clean("/foo/../bar"), "/bar");
        assert_eq!(go_path_clean("../a"), "../a");
        assert_eq!(path_join(&["/a/b", "/c"]), "/a/b/c");
        assert_eq!(path_join(&["", ""]), "");
        assert_eq!(path_join(&["a", ""]), "a");
    }

    #[test]
    fn memory_assets_directory_and_file_semantics() {
        let assets = MemoryAssets::from_paths(
            [
                ("main.js", b"console.log(1);".as_slice()),
                ("img/logo.svg", b"<svg/>".as_slice()),
            ],
            [],
        );
        assert_eq!(assets.open("/main.js").unwrap().data, b"console.log(1);");
        assert!(assets.open("/img").unwrap().is_dir);
        assert!(assets.open("/img/").unwrap().is_dir);
        assert!(assets.open("/").unwrap().is_dir);
        assert_eq!(assets.open("/missing.js"), Err(AssetError::NotFound));
    }

    #[test]
    fn dir_assets_reads_under_root() {
        let root = std::env::temp_dir().join(format!("sg_ui_test_{}", std::process::id()));
        std::fs::create_dir_all(root.join("img")).unwrap();
        std::fs::write(root.join("main.js"), b"x").unwrap();
        let reader = DirAssets::new(root.clone());
        assert_eq!(reader.open("/main.js").unwrap().data, b"x");
        assert!(reader.open("/img").unwrap().is_dir);
        assert_eq!(reader.open("/nope"), Err(AssetError::NotFound));
        std::fs::remove_dir_all(root).unwrap();
    }
}
