#![forbid(unsafe_code)]

//! Port of `cmd/frontend/internal/cli/config.go:63-107`:
//! `readSiteConfigFile` merges the `SITE_CONFIG_FILE` paths into one JSONC
//! document (`os.PathListSeparator`-separated; a single path is passed
//! through verbatim). The baseline then writes the merged document to the
//! `critical_and_site_config` table and watches the files; without the
//! configuration service this port reads the file once at startup and uses it
//! as the in-process [`SiteConfiguration`] (documented deviation).

use std::fmt;
use std::fs;
use std::io;
use std::path::PathBuf;

use sg_config::SiteConfiguration;

/// `SITE_CONFIG_FILE` (not registered with `env.Get` in the baseline).
pub const SITE_CONFIG_FILE_VAR: &str = "SITE_CONFIG_FILE";

#[derive(Debug)]
pub enum SiteConfigFileError {
    /// `os.ReadFile` failed.
    Read { path: PathBuf, source: io::Error },
    /// `jsonc.Unmarshal` failed: `failed to parse JSON in %s: %v`.
    Parse {
        path: PathBuf,
        message: String,
    },
    /// The file's top-level JSON value is not an object (`json.Unmarshal`
    /// into `map[string]*json.RawMessage`).
    NotObject { path: PathBuf },
    /// The merged document could not be parsed as a site configuration.
    SiteConfig(Box<sg_config::ConfigError>),
}

impl fmt::Display for SiteConfigFileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read { path, source } => write!(f, "{}: {source}", path.display()),
            Self::Parse { path, message } => {
                write!(f, "failed to parse JSON in {}: {message}", path.display())
            }
            Self::NotObject { path } => write!(
                f,
                "json: cannot unmarshal top-level value into an object: {}",
                path.display()
            ),
            Self::SiteConfig(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for SiteConfigFileError {}

/// `filepath.SplitList`: the empty value is an empty list, otherwise split on
/// the Unix path-list separator.
pub fn split_paths(value: &str) -> Vec<PathBuf> {
    if value.is_empty() {
        return Vec::new();
    }
    value.split(':').map(PathBuf::from).collect()
}

/// `readSiteConfigFile`: a single path is returned as-is; multiple paths are
/// merged into one pretty-printed JSONC document (top-level keys sorted, two
/// spaces, arrays one element per line).
pub fn read_site_config_file(paths: &[PathBuf]) -> Result<Vec<u8>, SiteConfigFileError> {
    if paths.len() == 1 {
        return fs::read(&paths[0]).map_err(|source| SiteConfigFileError::Read {
            path: paths[0].clone(),
            source,
        });
    }

    // `jsonx.Format` collapses the empty object of a zero-path merge.
    if paths.is_empty() {
        return Ok(b"// merged SITE_CONFIG_FILE\n{}".to_vec());
    }

    let mut merged = String::from("// merged SITE_CONFIG_FILE\n{\n");
    for path in paths {
        let bytes = fs::read(path).map_err(|source| SiteConfigFileError::Read {
            path: path.clone(),
            source,
        })?;
        let text = String::from_utf8_lossy(&bytes);
        let value = sg_config::parse_jsonc(&text).map_err(|err| SiteConfigFileError::Parse {
            path: path.clone(),
            message: err.to_string(),
        })?;
        let object = value
            .as_object()
            .ok_or_else(|| SiteConfigFileError::NotObject { path: path.clone() })?;

        let mut keys: Vec<&String> = object.keys().collect();
        keys.sort();

        merged.push_str(&format!("  // BEGIN {}\n", path.display()));
        for key in keys {
            let pretty = pretty_value(&object[key]);
            let indented = indent_after_first_line(&pretty, 2);
            let key = serde_json::to_string(key).expect("JSON object keys are strings");
            merged.push_str(&format!("  {key}: {indented},\n"));
        }
        merged.push_str(&format!("  // END {}\n", path.display()));
    }
    merged.push('}');
    Ok(merged.into_bytes())
}

/// Read `SITE_CONFIG_FILE`, merge and parse it. `None` when the variable is
/// empty or unset (`overrideSiteConfig` is a no-op without paths).
pub fn load_from_env() -> Result<Option<SiteConfiguration>, SiteConfigFileError> {
    load(&std::env::var(SITE_CONFIG_FILE_VAR).unwrap_or_default())
}

/// Implementation of [`load_from_env`] with the variable value injected.
pub fn load(value: &str) -> Result<Option<SiteConfiguration>, SiteConfigFileError> {
    let paths = split_paths(value);
    if paths.is_empty() {
        return Ok(None);
    }
    let merged = read_site_config_file(&paths)?;
    let text = String::from_utf8_lossy(&merged);
    let site_config = SiteConfiguration::parse(&text)
        .map_err(|err| SiteConfigFileError::SiteConfig(Box::new(err)))?;
    Ok(Some(site_config))
}

/// Recursively sort object keys (the baseline's `jsonx` formatter does this
/// for the generated document).
fn sorted_value(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => {
            let mut keys: Vec<&String> = object.keys().collect();
            keys.sort();
            let mut sorted = serde_json::Map::new();
            for key in keys {
                sorted.insert(key.clone(), sorted_value(&object[key]));
            }
            serde_json::Value::Object(sorted)
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(sorted_value).collect())
        }
        other => other.clone(),
    }
}

fn pretty_value(value: &serde_json::Value) -> String {
    serde_json::to_string_pretty(&sorted_value(value)).expect("serializing a JSON value succeeds")
}

/// `jsonx`'s formatter nests values two spaces under their key.
fn indent_after_first_line(text: &str, spaces: usize) -> String {
    let padding = " ".repeat(spaces);
    let mut lines = text.split('\n');
    let mut out = String::from(lines.next().unwrap_or_default());
    for line in lines {
        out.push('\n');
        out.push_str(&padding);
        out.push_str(line);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("sg_m98eb_{name}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    fn write(path: &Path, contents: &str) {
        fs::write(path, contents).expect("write");
    }

    #[test]
    fn split_paths_matches_filepath_splitlist() {
        assert!(split_paths("").is_empty());
        assert_eq!(split_paths("/a.json"), vec![PathBuf::from("/a.json")]);
        assert_eq!(
            split_paths("/a.json:/b.json"),
            vec![PathBuf::from("/a.json"), PathBuf::from("/b.json")]
        );
    }

    #[test]
    fn single_path_is_passed_through_verbatim() {
        let dir = temp_dir("single");
        let path = dir.join("0.json");
        write(&path, "// comment\n{\"hello\": \"world\"}\n");
        let merged = read_site_config_file(&[path]).expect("read");
        assert_eq!(merged, b"// comment\n{\"hello\": \"world\"}\n");
    }

    #[test]
    fn multiple_paths_are_merged_and_sorted() {
        let dir = temp_dir("multi");
        let first = dir.join("0.json");
        let second = dir.join("1.json");
        write(
            &first,
            "// leading comment\n{\n  // first comment\n  \"first\": \"file\",\n} // trailing comment\n",
        );
        write(&second, "{\"second\": \"file\"}");
        let merged =
            String::from_utf8(read_site_config_file(&[first.clone(), second.clone()]).expect("read"))
                .expect("utf8");
        let expected = format!(
            "// merged SITE_CONFIG_FILE\n{{\n  // BEGIN {}\n  \"first\": \"file\",\n  // END {}\n  // BEGIN {}\n  \"second\": \"file\",\n  // END {}\n}}",
            first.display(),
            first.display(),
            second.display(),
            second.display(),
        );
        assert_eq!(merged, expected);
    }

    #[test]
    fn nested_values_are_sorted_and_expanded() {
        let dir = temp_dir("nested");
        let path = dir.join("0.json");
        let other = dir.join("1.json");
        write(
            &path,
            "{\n  \"search.index.branches\": {\n    \"github.com/sourcegraph/sourcegraph\": [\"3.17\", \"v3.0.0\"],\n    \"github.com/go-yaml/yaml\": [\"v2\", \"v3\"]\n  }\n}",
        );
        write(&other, "{\"observability.alerts\": [ {\"level\":\"warning\"} ]}");
        let merged =
            String::from_utf8(read_site_config_file(&[path.clone(), other]).expect("read"))
                .expect("utf8");
        assert!(merged.contains(
            "  \"search.index.branches\": {\n    \"github.com/go-yaml/yaml\": [\n      \"v2\",\n      \"v3\"\n    ],\n    \"github.com/sourcegraph/sourcegraph\": [\n      \"3.17\",\n      \"v3.0.0\"\n    ]\n  },"
        ), "{merged}");
        assert!(merged.contains(
            "  \"observability.alerts\": [\n    {\n      \"level\": \"warning\"\n    }\n  ],"
        ), "{merged}");
    }

    #[test]
    fn parse_errors_name_the_file() {
        let dir = temp_dir("parse");
        let first = dir.join("0.json");
        let second = dir.join("1.json");
        write(&first, "{}");
        write(&second, "{");
        let err = read_site_config_file(&[first, second.clone()]).expect_err("error");
        assert!(matches!(err, SiteConfigFileError::Parse { .. }));
        assert!(err.to_string().contains("failed to parse JSON in"));
        assert!(err.to_string().contains(&second.display().to_string()));
    }

    #[test]
    fn non_object_top_level_is_an_error() {
        let dir = temp_dir("array");
        let path = dir.join("0.json");
        let other = dir.join("1.json");
        write(&path, "{}");
        write(&other, "[]");
        let err = read_site_config_file(&[path, other]).expect_err("error");
        assert!(matches!(err, SiteConfigFileError::NotObject { .. }));
    }

    #[test]
    fn zero_paths_merge_to_an_empty_object() {
        let merged = read_site_config_file(&[]).expect("read");
        assert_eq!(merged, b"// merged SITE_CONFIG_FILE\n{}");
    }

    #[test]
    fn load_returns_none_without_paths() {
        assert!(load("").expect("load").is_none());
    }

    #[test]
    fn load_parses_the_merged_document() {
        let dir = temp_dir("load");
        let path = dir.join("0.json");
        write(
            &path,
            "{\n  \"externalURL\": \"https://example.com\",\n  // comment\n  \"auth.providers\": [{\"type\": \"builtin\"}],\n}",
        );
        let site = load(path.to_str().expect("utf8 path"))
            .expect("load")
            .expect("configured");
        assert_eq!(
            site.raw().get("externalURL").and_then(|v| v.as_str()),
            Some("https://example.com")
        );
        assert_eq!(
            site.raw()
                .get("auth.providers")
                .and_then(|v| v.as_array())
                .map(Vec::len),
            Some(1)
        );
    }
}
