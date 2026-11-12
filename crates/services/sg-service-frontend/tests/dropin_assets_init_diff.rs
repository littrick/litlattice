//! M9.18c differential harness: `assets.Init` equivalence.
//!
//! Replays the corpus tree through `sg_ui::EmbeddedProvider` (the compiled-in
//! counterpart of `assets.DirProvider`) and emits the same JSON shape as
//! `ui/assets/zz_m918c_assets_init_oracle_test.go`, which drives the real Go
//! `DirProvider`/`http.Dir`/`loadWebBuildManifest`.
//!
//! Gated on `SG_M918C_ASSETS_HARNESS=1` plus
//! `M918C_ASSETS_CORPUS`/`M918C_ASSETS_OUT`.

use std::fs;

use serde::{Deserialize, Serialize};
use sg_ui::{
    AssetEntry, AssetError, AssetsProvider, EmbeddedProvider, ManifestError, WebBuildManifest,
};

#[derive(Deserialize)]
struct CorpusFile {
    path: String,
    content: String,
}

#[derive(Deserialize)]
struct ManifestCase {
    name: String,
    content: String,
}

#[derive(Deserialize)]
struct Corpus {
    tree: Vec<CorpusFile>,
    opens: Vec<String>,
    manifests: Vec<ManifestCase>,
}

#[derive(Serialize)]
struct OpenResult {
    path: String,
    kind: String,
    data: String,
}

#[derive(Serialize)]
struct ManifestOut {
    #[serde(rename = "main.js")]
    main_js: String,
    #[serde(rename = "main.css")]
    main_css: String,
    #[serde(rename = "embed.js")]
    embed_js: String,
    #[serde(rename = "embed.css")]
    embed_css: String,
}

impl From<WebBuildManifest> for ManifestOut {
    fn from(manifest: WebBuildManifest) -> Self {
        Self {
            main_js: manifest.main_js_bundle_path,
            main_css: manifest.main_css_bundle_path,
            embed_js: manifest.embed_js_bundle_path,
            embed_css: manifest.embed_css_bundle_path,
        }
    }
}

#[derive(Serialize)]
struct ManifestResult {
    name: String,
    ok: bool,
    manifest: ManifestOut,
    error_class: String,
}

#[derive(Serialize)]
struct Output {
    opens: Vec<OpenResult>,
    manifests: Vec<ManifestResult>,
}

fn error_class(error: &ManifestError) -> &'static str {
    match error {
        ManifestError::NotFound { .. } => "not_found",
        ManifestError::Parse { .. } => "parse",
        ManifestError::Io { .. } => "io",
        ManifestError::NotConfigured => "not_configured",
    }
}

#[test]
fn dump_assets_init_equivalence() {
    if std::env::var("SG_M918C_ASSETS_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M918C_ASSETS_CORPUS").expect("M918C_ASSETS_CORPUS");
    let output_path = std::env::var("M918C_ASSETS_OUT").expect("M918C_ASSETS_OUT");

    let corpus: Corpus = serde_json::from_slice(&fs::read(corpus_path).expect("corpus"))
        .expect("corpus json");

    let files: Vec<(String, Vec<u8>)> = corpus
        .tree
        .iter()
        .map(|file| (file.path.clone(), file.content.as_bytes().to_vec()))
        .collect();
    let table: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(path, data)| (path.as_str(), data.as_slice()))
        .collect();
    let provider = EmbeddedProvider::from_table("/assets-dist", &table);
    let reader = provider.assets();

    let mut output = Output {
        opens: Vec::new(),
        manifests: Vec::new(),
    };

    for path in &corpus.opens {
        let result = match reader.open(path) {
            Err(AssetError::NotFound) => OpenResult {
                path: path.clone(),
                kind: "missing".to_owned(),
                data: String::new(),
            },
            Err(AssetError::PermissionDenied) => OpenResult {
                path: path.clone(),
                kind: "permission".to_owned(),
                data: String::new(),
            },
            Err(AssetError::Other(_)) => OpenResult {
                path: path.clone(),
                kind: "other".to_owned(),
                data: String::new(),
            },
            Ok(AssetEntry { is_dir: true, .. }) => OpenResult {
                path: path.clone(),
                kind: "dir".to_owned(),
                data: String::new(),
            },
            Ok(entry) => OpenResult {
                path: path.clone(),
                kind: "file".to_owned(),
                data: String::from_utf8_lossy(&entry.data).into_owned(),
            },
        };
        output.opens.push(result);
    }

    for manifest_case in &corpus.manifests {
        let case_files: Vec<(String, Vec<u8>)> = if manifest_case.name == "missing" {
            Vec::new()
        } else {
            vec![(
                sg_ui::WEB_MANIFEST_FILE.to_owned(),
                manifest_case.content.as_bytes().to_vec(),
            )]
        };
        let case_table: Vec<(&str, &[u8])> = case_files
            .iter()
            .map(|(path, data)| (path.as_str(), data.as_slice()))
            .collect();
        let case_provider = EmbeddedProvider::from_table("/assets-dist", &case_table);
        let result = match case_provider.load_web_build_manifest() {
            Ok(manifest) => ManifestResult {
                name: manifest_case.name.clone(),
                ok: true,
                manifest: manifest.into(),
                error_class: String::new(),
            },
            Err(error) => ManifestResult {
                name: manifest_case.name.clone(),
                ok: false,
                manifest: WebBuildManifest::default().into(),
                error_class: error_class(&error).to_owned(),
            },
        };
        output.manifests.push(result);
    }

    let encoded = serde_json::to_vec(&output).expect("serialize");
    fs::write(output_path, encoded).expect("write output");
}
