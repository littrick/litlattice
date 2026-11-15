//! M9.37c differential: `GitBlob.highlight` / `GitTreeEntry.highlight` and
//! the `HighlightedFile` object (`html`/`aborted`/`lsif`/`lineRanges`).
//!
//! The Go oracle
//! (`cmd/frontend/graphqlbackend/zz_m937c_highlighted_file_oracle_test.go`,
//! untracked, `M937C_*`-gated) drives the real `GitTreeEntryResolver.Highlight`
//! with `highlight.Mocks.Code`: it records the `highlight.Params` the resolver
//! produces (proving content is sliced before highlighting, the lock-file
//! blocklist and the pagination guard) and returns a deterministic
//! `HighlightedCode`. This harness injects the equivalent `GitObjectSource`
//! and `HighlightSource`, runs the real prefetch/resolver path, and writes the
//! same JSON schema.
//!
//! Corpus fields: `name`, `repo`, `oid`, `path`, `content` or `content_hex`,
//! `format`, `disable_timeout`, `highlight_long_lines`, `start_line`,
//! `end_line`, `mock` (`mode`: `html`/`plain`/`binary`/`error`, `aborted`) and
//! `line_ranges` (`[{startLine,endLine}]`).
//!
//! Env: `M937C_HL_CORPUS`, `M937C_HL_OUT`.

#![allow(clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};

use sg_git::{CommitDetails, FileStat, GitObject};
use sg_graphql::exec::{Actor, Arguments, FieldValue, InputValue, ObjectValue, RootField};
use sg_graphql::json::Json;
use sg_service_frontend::git_comparison_graphql::{
    HighlightError, HighlightRequest, HighlightSource, HighlightedFile,
};
use sg_service_frontend::git_graphql::{GitObjectSource, GitPrefetch, GitSourceError};
use sg_service_frontend::git_tree_entry_graphql::{page_content, prefetch_commit_entries};
use sg_service_frontend::search_stream::convert::MinimalRepo;

#[derive(Deserialize, Clone)]
struct Mock {
    mode: String,
    #[serde(default)]
    aborted: bool,
}

#[derive(Deserialize, Clone)]
struct LineRange {
    #[serde(rename = "startLine")]
    start_line: i32,
    #[serde(rename = "endLine")]
    end_line: i32,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    repo: String,
    oid: String,
    path: String,
    #[serde(default)]
    content: String,
    #[serde(default)]
    content_hex: String,
    format: String,
    #[serde(default)]
    disable_timeout: bool,
    #[serde(default)]
    highlight_long_lines: bool,
    start_line: Option<i32>,
    end_line: Option<i32>,
    mock: Mock,
    #[serde(default)]
    line_ranges: Vec<LineRange>,
}

fn decode_hex(input: &str) -> Vec<u8> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut index = 0;
    while index + 1 < bytes.len() {
        let high = (bytes[index] as char).to_digit(16).expect("hex") as u8;
        let low = (bytes[index + 1] as char).to_digit(16).expect("hex") as u8;
        out.push(high << 4 | low);
        index += 2;
    }
    out
}

fn escape_html(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&#34;"),
            '\'' => out.push_str("&#39;"),
            ch => out.push(ch),
        }
    }
    out
}

/// Mirrors the Go oracle's deterministic `HighlightedCode` table.
fn highlighted_table(filepath: &str, code: &str) -> String {
    let mut out = String::from("<table><tbody>");
    for (row, line) in code.split('\n').enumerate() {
        out.push_str("<tr><td class=\"line\" data-line=\"");
        out.push_str(&(row + 1).to_string());
        out.push_str("\"></td><td class=\"code\"><div><span class=\"hljs\" data-fp=\"");
        out.push_str(&escape_html(filepath));
        out.push_str("\">");
        out.push_str(&escape_html(line));
        out.push_str("</span></div></td></tr>");
    }
    out.push_str("</tbody></table>");
    out
}

fn plain_table(code: &str) -> String {
    let mut out = String::from("<table>");
    for (row, line) in code.split('\n').enumerate() {
        let line = line.strip_suffix('\r').unwrap_or(line);
        let text = if line.is_empty() { "\n" } else { line };
        out.push_str("<tr><td class=\"line\" data-line=\"");
        out.push_str(&(row + 1).to_string());
        out.push_str("\"></td><td class=\"code\"><span>");
        out.push_str(&escape_html(text));
        out.push_str("</span></td></tr>");
    }
    out.push_str("</table>");
    out
}

#[derive(Clone)]
struct Recorded {
    content: String,
    filepath: String,
    format: String,
    disable_timeout: bool,
    highlight_long_lines: bool,
    keep_final_newline: bool,
}

struct FakeHighlighter {
    mock: Mock,
    recorded: Arc<Mutex<Option<Recorded>>>,
}

fn strip_final_newline(code: &str) -> String {
    code.strip_suffix('\n').unwrap_or(code).to_owned()
}

#[async_trait]
impl HighlightSource for FakeHighlighter {
    async fn highlight(
        &self,
        request: &HighlightRequest,
    ) -> Result<HighlightedFile, HighlightError> {
        let sliced = page_content(&request.content, request.start_line, request.end_line);
        let sliced_text = String::from_utf8_lossy(sliced).into_owned();
        *self.recorded.lock().unwrap() = Some(Recorded {
            content: sliced_text.clone(),
            filepath: request.filepath.clone(),
            format: request.format.clone(),
            disable_timeout: request.disable_timeout,
            highlight_long_lines: request.highlight_long_lines,
            keep_final_newline: request.keep_final_newline,
        });
        match self.mock.mode.as_str() {
            "binary" => Err(HighlightError::Binary),
            "error" => Err(HighlightError::Message(
                "syntax highlighting failed".to_owned(),
            )),
            _ => {
                let mut code = sliced_text;
                if !request.keep_final_newline {
                    code = strip_final_newline(&code);
                }
                let html = if self.mock.mode == "plain" {
                    plain_table(&code)
                } else {
                    highlighted_table(&request.filepath, &code)
                };
                Ok(HighlightedFile {
                    lines: Vec::new(),
                    aborted: self.mock.aborted,
                    html: Some(html),
                    code: Some(code),
                    lsif: None,
                })
            }
        }
    }
}

struct FakeGit {
    path: String,
    content: Vec<u8>,
    highlighter: Arc<FakeHighlighter>,
}

#[async_trait]
impl GitObjectSource for FakeGit {
    async fn resolve_revision(
        &self,
        _repo_id: i32,
        repo_name: &str,
        rev: &str,
    ) -> Result<String, GitSourceError> {
        Err(GitSourceError::revision_not_found(format!(
            "revision not found: {repo_name}@{rev}"
        )))
    }

    async fn get_commit(
        &self,
        _repo_id: i32,
        repo_name: &str,
        oid: &str,
    ) -> Result<CommitDetails, GitSourceError> {
        Err(GitSourceError::revision_not_found(format!(
            "revision not found: {repo_name}@{oid}"
        )))
    }

    async fn get_object(
        &self,
        _repo_id: i32,
        repo_name: &str,
        revspec: &str,
    ) -> Result<GitObject, GitSourceError> {
        Err(GitSourceError::revision_not_found(format!(
            "revision not found: {repo_name}@{revspec}"
        )))
    }

    async fn stat(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        _oid: &str,
        _path: &str,
    ) -> Result<FileStat, GitSourceError> {
        Ok(FileStat {
            name: self.path.clone(),
            size: self.content.len() as i64,
            mode: 0,
            blob_oid: String::new(),
            submodule: None,
        })
    }

    async fn read_file(
        &self,
        _repo_id: i32,
        _repo_name: &str,
        _oid: &str,
        _path: &str,
    ) -> Result<Vec<u8>, GitSourceError> {
        Ok(self.content.clone())
    }

    fn highlighter(&self) -> Option<&dyn HighlightSource> {
        Some(self.highlighter.as_ref())
    }
}

fn highlight_arguments(case: &Case) -> Arguments {
    let mut entries: Vec<(String, InputValue)> = vec![
        (
            "disableTimeout".to_owned(),
            InputValue::Bool(case.disable_timeout),
        ),
        (
            "highlightLongLines".to_owned(),
            InputValue::Bool(case.highlight_long_lines),
        ),
        ("format".to_owned(), InputValue::Enum(case.format.clone())),
    ];
    if let Some(start) = case.start_line {
        entries.push(("startLine".to_owned(), InputValue::Int(i64::from(start))));
    }
    if let Some(end) = case.end_line {
        entries.push(("endLine".to_owned(), InputValue::Int(i64::from(end))));
    }
    Arguments::from_entries(entries)
}

fn ranges_arguments(case: &Case) -> Arguments {
    let items: Vec<InputValue> = case
        .line_ranges
        .iter()
        .map(|range| {
            let mut map = std::collections::BTreeMap::new();
            map.insert(
                "startLine".to_owned(),
                InputValue::Int(i64::from(range.start_line)),
            );
            map.insert(
                "endLine".to_owned(),
                InputValue::Int(i64::from(range.end_line)),
            );
            InputValue::Object(map)
        })
        .collect();
    Arguments::from_entries(vec![("ranges".to_owned(), InputValue::List(items))])
}

fn json_string(value: &FieldValue) -> String {
    match value {
        FieldValue::Json(Json::Str(text)) => text.clone(),
        _ => panic!("expected string field"),
    }
}

fn json_bool(value: &FieldValue) -> bool {
    match value {
        FieldValue::Json(Json::Bool(value)) => *value,
        _ => panic!("expected bool field"),
    }
}

fn json_line_ranges(value: &FieldValue) -> Vec<Vec<String>> {
    match value {
        FieldValue::List { items, .. } => items
            .iter()
            .map(|item| match item {
                FieldValue::List { items, .. } => {
                    items.iter().map(json_string).collect::<Vec<String>>()
                }
                _ => panic!("expected nested list"),
            })
            .collect(),
        _ => panic!("expected list field"),
    }
}

fn write_case(out: &str, case: &Case, recorded: Option<Recorded>, output: Value) {
    let params = match recorded {
        Some(recorded) => json!({
            "content": recorded.content,
            "disable_timeout": recorded.disable_timeout,
            "filepath": recorded.filepath,
            "format": recorded.format,
            "highlight_long_lines": recorded.highlight_long_lines,
            "keep_final_newline": recorded.keep_final_newline,
        }),
        None => Value::Null,
    };
    let document = json!({"params": params, "output": output});
    std::fs::write(
        std::path::Path::new(out).join(format!("{}.json", case.name)),
        document.to_string(),
    )
    .expect("write");
}

#[tokio::test]
async fn highlighted_file_diff() {
    let corpus = match std::env::var("M937C_HL_CORPUS") {
        Ok(path) => path,
        Err(_) => return,
    };
    let out = std::env::var("M937C_HL_OUT").expect("M937C_HL_OUT");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");

    for case in &cases {
        let content = if !case.content_hex.is_empty() {
            decode_hex(&case.content_hex)
        } else {
            case.content.clone().into_bytes()
        };
        let recorded = Arc::new(Mutex::new(None));
        let highlighter = Arc::new(FakeHighlighter {
            mock: case.mock.clone(),
            recorded: recorded.clone(),
        });
        let source = FakeGit {
            path: case.path.clone(),
            content,
            highlighter,
        };
        let repo = MinimalRepo {
            id: 1,
            name: case.repo.clone(),
        };
        let arguments = highlight_arguments(case);
        let selection = RootField {
            name: "blob".to_owned(),
            arguments: Arguments::from_entries(vec![(
                "path".to_owned(),
                InputValue::Str(case.path.clone()),
            )]),
            selections: vec![RootField {
                name: "highlight".to_owned(),
                arguments: arguments.clone(),
                selections: Vec::new(),
                type_conditions: Vec::new(),
            }],
            type_conditions: Vec::new(),
        };
        let mut prefetch = GitPrefetch::default();
        prefetch_commit_entries(&source, &mut prefetch, &repo, &case.oid, &[selection], 0).await;
        let entry = sg_service_frontend::git_tree_entry_graphql::GitTreeEntryObject {
            repo,
            oid: case.oid.clone(),
            input_rev: None,
            stat: FileStat {
                name: case.path.clone(),
                size: 0,
                mode: 0,
                blob_oid: String::new(),
                submodule: None,
            },
            git: Some(Arc::new(prefetch)),
        };

        let recorded_value = recorded.lock().unwrap().clone();
        let resolved = entry.field("highlight", &arguments, Actor::Anonymous);
        let output = match resolved {
            Err(error) => json!({
                "aborted": false,
                "error": error.message,
                "html": "",
                "line_ranges": Value::Null,
                "lsif": "",
                "ok": false,
            }),
            Ok(Some(FieldValue::Object(object))) => {
                let none = Arguments::default();
                let html = object
                    .field("html", &none, Actor::Anonymous)
                    .expect("html")
                    .expect("html value");
                let aborted = object
                    .field("aborted", &none, Actor::Anonymous)
                    .expect("aborted")
                    .expect("aborted value");
                let lsif = object
                    .field("lsif", &none, Actor::Anonymous)
                    .expect("lsif")
                    .expect("lsif value");
                let line_ranges = object
                    .field("lineRanges", &ranges_arguments(case), Actor::Anonymous)
                    .expect("lineRanges")
                    .expect("lineRanges value");
                json!({
                    "aborted": json_bool(&aborted),
                    "error": "",
                    "html": json_string(&html),
                    "line_ranges": json_line_ranges(&line_ranges),
                    "lsif": json_string(&lsif),
                    "ok": true,
                })
            }
            Ok(_) => panic!("unexpected highlight resolver value"),
        };
        write_case(&out, case, recorded_value, output);
        println!("RUST {}", case.name);
    }
}
