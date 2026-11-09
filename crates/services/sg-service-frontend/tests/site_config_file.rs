//! M9.8eb differential harness: prints the same normalized `sc` lines as
//! `cmd/frontend/internal/cli/zz_frontend_site_config_harness_test.go`.
//! Errors are normalized to `read` because jsonx and serde report different
//! messages.

use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

use sg_service_frontend::site_config_file::read_site_config_file;

fn temp_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("sg_m98eb_harness_{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

fn run_case(dir: &std::path::Path, name: &str, files: &[&str]) {
    let mut paths = Vec::new();
    for (index, contents) in files.iter().enumerate() {
        let path = dir.join(format!("{index}.json"));
        fs::write(&path, contents).expect("write");
        paths.push(path);
    }

    let mut line = String::new();
    match read_site_config_file(&paths) {
        Ok(out) => {
            let out = String::from_utf8_lossy(&out);
            let normalized = out.replace(&dir.display().to_string(), "$tmp");
            let _ = write!(line, "sc {name} err=nil out={normalized:?}");
        }
        Err(err) => {
            let _ = write!(line, "sc {name} err=read out={:?}", "");
            assert!(!err.to_string().is_empty());
        }
    }
    println!("{line}");
}

#[test]
fn site_config_file_harness() {
    let dir = temp_dir();
    run_case(&dir, "single", &["// comment\n{\"hello\": \"world\"}\n"]);
    run_case(&dir, "single-empty", &[""]);
    run_case(
        &dir,
        "two",
        &[
            "// leading comment\n{\n  // first comment\n  \"first\": \"file\",\n} // trailing comment\n",
            "{\"second\": \"file\"}",
        ],
    );
    run_case(
        &dir,
        "three",
        &[
            "{\n    \"search.index.branches\": {\n      \"github.com/sourcegraph/sourcegraph\": [\"3.17\", \"v3.0.0\"],\n      \"github.com/kubernetes/kubernetes\": [\"release-1.17\"],\n      \"github.com/go-yaml/yaml\": [\"v2\", \"v3\"]\n    }\n}",
            "{\n  \"observability.alerts\": [ {\"level\":\"warning\"}, { \"level\": \"critical\"} ]\n}",
            "{\n  \"auth.providers\": [{\"type\": \"builtin\"}],\n  // a comment\n  \"externalURL\": \"https://example.com\",\n}",
        ],
    );
    run_case(&dir, "empty-objects", &["{}", "{}"]);
    run_case(&dir, "parse-error", &["{}", "{"]);
    run_case(&dir, "empty-list", &[]);
}
