#![forbid(unsafe_code)]

//! SSE decoding for the code-insights query runner.
//!
//! Two layers, both transcribed from the baseline:
//!
//! * [`parse_events`] is `internal/search/streaming/http.(*Decoder)`: events
//!   are separated by `\n\n`, each event is `event: <name>\ndata: <json>`, and
//!   the two malformed-event errors are reproduced verbatim;
//! * [`decode_search`] / [`decode_compute_match_context`] /
//!   [`decode_compute_text`] are the three
//!   `FrontendStreamDecoder`/compute decoder dispatch loops plus the
//!   `TabulationDecoder`, `MatchContextComputeDecoder` and
//!   `ComputeTextDecoder` tabulators from
//!   `internal/insights/query/streaming/decoder.go`.
//!
//! Like the baseline this is not a general-purpose SSE decoder: it expects
//! Sourcegraph's `event:`/`data:` shape and stops at the `done` event.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_json::Value;

/// The maximum captured value length (`capturedValueMaxLength`).
pub const CAPTURED_VALUE_MAX_LENGTH: usize = 100;

/// `streamapi.Progress` subset used by the decoders.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct Progress {
    #[serde(default)]
    pub done: bool,
    #[serde(default)]
    pub skipped: Vec<Skipped>,
}

/// `streamapi.Skipped`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct Skipped {
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub message: String,
}

/// `streamapi.ShardTimeout`.
pub const SKIPPED_SHARD_TIMEOUT: &str = "shard-timeout";
/// `streamapi.BackendMissing`.
pub const SKIPPED_BACKEND_MISSING: &str = "backend-missing";
/// The alert title the tabulators ignore.
pub const NO_REPOSITORIES_FOUND: &str = "No repositories found";

/// `StreamDecoderEvents`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamDecoderEvents {
    pub skipped_reasons: Vec<String>,
    pub errors: Vec<String>,
    pub alerts: Vec<String>,
    pub did_timeout: bool,
}

impl StreamDecoderEvents {
    /// `(*StreamDecoderEvents).onProgress`.
    pub fn on_progress(&mut self, progress: &Progress) {
        if !progress.done {
            return;
        }
        for skipped in &progress.skipped {
            match skipped.reason.as_str() {
                SKIPPED_SHARD_TIMEOUT => {
                    self.alerts
                        .push(format!("{}: {}", skipped.reason, skipped.message));
                    self.did_timeout = true;
                }
                SKIPPED_BACKEND_MISSING => {
                    self.alerts
                        .push(format!("{}: {}", skipped.reason, skipped.message));
                }
                _ => self
                    .skipped_reasons
                    .push(format!("{}: {}", skipped.reason, skipped.message)),
            }
        }
    }

    /// The `OnAlert` body shared by all editors.
    pub fn on_alert(&mut self, title: &str, description: &str) {
        if title == NO_REPOSITORIES_FOUND {
            return;
        }
        self.alerts.push(format!("{title}: {description}"));
    }
}

/// `streamhttp.EventAlert` subset.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct EventAlert {
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub description: String,
}

/// `streamhttp.EventError`.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct EventError {
    #[serde(default)]
    pub message: String,
}

/// `streamhttp.MatchType` discriminator names.
const CONTENT_MATCH_TYPE: &str = "content";
const PATH_MATCH_TYPE: &str = "path";
const REPO_MATCH_TYPE: &str = "repo";
const SYMBOL_MATCH_TYPE: &str = "symbol";
const COMMIT_MATCH_TYPE: &str = "commit";
const PERSON_MATCH_TYPE: &str = "person";
const TEAM_MATCH_TYPE: &str = "team";

/// `streaming.SearchMatch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchMatch {
    pub repository_id: i32,
    pub repository_name: String,
    pub match_count: i32,
}

/// `streaming.TabulationResult`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TabulationResult {
    pub events: StreamDecoderEvents,
    pub repo_counts: BTreeMap<String, SearchMatch>,
    pub total_count: i32,
}

impl TabulationResult {
    fn add_count(&mut self, repo: &str, repo_id: i32, count: i32) {
        match self.repo_counts.get_mut(repo) {
            Some(existing) => existing.match_count += count,
            None => {
                self.repo_counts.insert(
                    repo.to_owned(),
                    SearchMatch {
                        repository_id: repo_id,
                        repository_name: repo.to_owned(),
                        match_count: count,
                    },
                );
            }
        }
    }
}

/// `streaming.RepoResult`: the repository search tabulator behind
/// `query.StreamingRepoQueryExecutor`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoResult {
    pub events: StreamDecoderEvents,
    pub repos: Vec<sg_insights::discovery::MinimalRepo>,
}

/// `streaming.ComputeMatch`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComputeMatch {
    pub repository_id: i32,
    pub repository_name: String,
    pub value_counts: BTreeMap<String, i32>,
}

impl ComputeMatch {
    fn new(repo_name: &str, repo_id: i32) -> Self {
        Self {
            repository_id: repo_id,
            repository_name: repo_name.to_owned(),
            value_counts: BTreeMap::new(),
        }
    }
}

/// `streaming.ComputeTabulationResult`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ComputeTabulationResult {
    pub events: StreamDecoderEvents,
    pub repo_counts: BTreeMap<String, ComputeMatch>,
    pub total_count: i32,
}

impl ComputeTabulationResult {
    fn repo_counts_for(&mut self, repo_name: &str, repo_id: i32) -> &mut ComputeMatch {
        self.repo_counts
            .entry(repo_name.to_owned())
            .or_insert_with(|| ComputeMatch::new(repo_name, repo_id))
    }
}

/// `capturedValueMaxLength` truncation. Go slices bytes (`value[:100]`); this
/// keeps a UTF-8 boundary, which is identical for ASCII captures and never
/// produces invalid UTF-8.
fn truncate_capture(value: &str) -> &str {
    if value.len() <= CAPTURED_VALUE_MAX_LENGTH {
        return value;
    }
    let mut end = CAPTURED_VALUE_MAX_LENGTH;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

/// `http.Decoder`: split the body into `(event, data)` pairs.
pub fn parse_events(body: &str) -> Result<Vec<(String, String)>, String> {
    let mut events = Vec::new();
    let mut rest = body;
    loop {
        match rest.find("\n\n") {
            Some(index) => {
                let chunk = &rest[..index];
                parse_event(chunk, &mut events)?;
                rest = &rest[index + 2..];
            }
            None => {
                // `atEOF && len(data) == 0` ends the stream; a final event
                // without the trailing blank line is still emitted.
                if !rest.is_empty() {
                    parse_event(rest, &mut events)?;
                }
                break;
            }
        }
    }
    Ok(events)
}

fn parse_event(chunk: &str, events: &mut Vec<(String, String)>) -> Result<(), String> {
    let Some(newline) = chunk.find('\n') else {
        return Err(format!("malformed event, no newline: {chunk}"));
    };
    let (event_key, event) = split_colon(&chunk[..newline]);
    let (data_key, data) = split_colon(&chunk[newline + 1..]);
    if event_key != "event" {
        return Err(format!("malformed event, expected event: {event_key}"));
    }
    if data_key != "data" {
        return Err(format!(
            "malformed event {event}: expected data: {data_key}"
        ));
    }
    events.push((event.to_owned(), data.to_owned()));
    Ok(())
}

fn split_colon(line: &str) -> (&str, &str) {
    match line.find(':') {
        Some(index) => (line[..index].trim(), line[index + 1..].trim()),
        None => (line.trim(), ""),
    }
}

fn json_err(kind: &str, err: serde_json::Error) -> String {
    format!("failed to decode {kind} payload: {err}")
}

fn decode_progress(data: &str) -> Result<Progress, String> {
    serde_json::from_str(data).map_err(|err| json_err("progress", err))
}

/// Decode one `matches` payload, returning `(type, value)` pairs for the
/// tabulators. Reproduces `eventMatchUnmarshaller`: only the five match types
/// the insights tabulators understand are accepted.
fn decode_matches(data: &str) -> Result<Vec<Value>, String> {
    let values: Vec<Value> = serde_json::from_str(data).map_err(|err| json_err("matches", err))?;
    for value in &values {
        let match_type = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or(CONTENT_MATCH_TYPE);
        match match_type {
            CONTENT_MATCH_TYPE | PATH_MATCH_TYPE | REPO_MATCH_TYPE | SYMBOL_MATCH_TYPE
            | COMMIT_MATCH_TYPE => {}
            PERSON_MATCH_TYPE => return Err("unknown MatchType 5".to_owned()),
            TEAM_MATCH_TYPE => return Err("unknown MatchType 6".to_owned()),
            other => return Err(format!("unknown MatchType: \"{other}\"")),
        }
    }
    Ok(values)
}

fn json_i32(value: &Value, key: &str) -> i32 {
    value
        .get(key)
        .and_then(Value::as_i64)
        .map(|number| number as i32)
        .unwrap_or(0)
}

fn json_str<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or("")
}

fn apply_matches(result: &mut TabulationResult, matches: Vec<Value>) {
    for value in matches {
        let match_type = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or(CONTENT_MATCH_TYPE);
        let repo = json_str(&value, "repository");
        let repo_id = json_i32(&value, "repositoryID");
        let count = match match_type {
            CONTENT_MATCH_TYPE => value
                .get("chunkMatches")
                .and_then(Value::as_array)
                .map(|chunks| {
                    chunks
                        .iter()
                        .map(|chunk| {
                            chunk
                                .get("ranges")
                                .and_then(Value::as_array)
                                .map_or(0, |ranges| ranges.len() as i32)
                        })
                        .sum()
                })
                .unwrap_or(0),
            SYMBOL_MATCH_TYPE => value
                .get("symbols")
                .and_then(Value::as_array)
                .map_or(0, |symbols| symbols.len() as i32),
            PATH_MATCH_TYPE | REPO_MATCH_TYPE | COMMIT_MATCH_TYPE => 1,
            _ => 0,
        };
        result.total_count += count;
        result.add_count(repo, repo_id, count);
    }
}

/// `TabulationDecoder` over a streamed search response body.
pub fn decode_search(body: &str) -> Result<TabulationResult, String> {
    let mut result = TabulationResult::default();
    for (event, data) in parse_events(body)? {
        match event.as_str() {
            "progress" => {
                let progress = decode_progress(&data)?;
                result.events.on_progress(&progress);
            }
            "matches" => {
                let matches = decode_matches(&data)?;
                apply_matches(&mut result, matches);
            }
            "alert" => {
                let alert: EventAlert =
                    serde_json::from_str(&data).map_err(|err| json_err("alert", err))?;
                result.events.on_alert(&alert.title, &alert.description);
            }
            "error" => {
                let error: EventError =
                    serde_json::from_str(&data).map_err(|err| json_err("error", err))?;
                result.events.errors.push(error.message);
            }
            "done" => break,
            _ => {}
        }
    }
    Ok(result)
}

/// `RepoDecoder` over a streamed search response body: collects the
/// `EventRepoMatch` entries (the repo-scope search returns one match per
/// repository). Alerts and errors are surfaced through the shared
/// `StreamDecoderEvents`; `No repositories found` stays silent.
pub fn decode_repo(body: &str) -> Result<RepoResult, String> {
    let mut result = RepoResult::default();
    for (event, data) in parse_events(body)? {
        match event.as_str() {
            "progress" => {
                let progress = decode_progress(&data)?;
                result.events.on_progress(&progress);
            }
            "matches" => {
                for value in decode_matches(&data)? {
                    let match_type = value
                        .get("type")
                        .and_then(Value::as_str)
                        .unwrap_or(CONTENT_MATCH_TYPE);
                    if match_type != REPO_MATCH_TYPE {
                        continue;
                    }
                    result.repos.push(sg_insights::discovery::MinimalRepo {
                        id: i64::from(json_i32(&value, "repositoryID")),
                        name: json_str(&value, "repository").to_owned(),
                    });
                }
            }
            "alert" => {
                let alert: EventAlert =
                    serde_json::from_str(&data).map_err(|err| json_err("alert", err))?;
                result.events.on_alert(&alert.title, &alert.description);
            }
            "error" => {
                let error: EventError =
                    serde_json::from_str(&data).map_err(|err| json_err("error", err))?;
                result.events.errors.push(error.message);
            }
            "done" => break,
            _ => {}
        }
    }
    Ok(result)
}

/// `MatchContextComputeDecoder` over a streamed compute response body.
pub fn decode_compute_match_context(body: &str) -> Result<ComputeTabulationResult, String> {
    let mut result = ComputeTabulationResult::default();
    for (event, data) in parse_events(body)? {
        match event.as_str() {
            "progress" => {
                let progress = decode_progress(&data)?;
                result.events.on_progress(&progress);
            }
            "results" => {
                let values: Vec<Value> = serde_json::from_str(&data)
                    .map_err(|err| json_err("compute match context", err))?;
                for value in values {
                    let repo = json_str(&value, "repository").to_owned();
                    let repo_id = json_i32(&value, "repositoryID");
                    let matches = value
                        .get("matches")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    for item in matches {
                        let environment = item
                            .get("environment")
                            .and_then(Value::as_object)
                            .cloned()
                            .unwrap_or_default();
                        for data in environment.values() {
                            let captured = data.get("value").and_then(Value::as_str).unwrap_or("");
                            if captured.is_empty() {
                                continue;
                            }
                            let captured = truncate_capture(captured).to_owned();
                            result.total_count += 1;
                            result
                                .repo_counts_for(&repo, repo_id)
                                .value_counts
                                .entry(captured)
                                .and_modify(|count| *count += 1)
                                .or_insert(1);
                        }
                    }
                }
            }
            "alert" => {
                let alert: EventAlert =
                    serde_json::from_str(&data).map_err(|err| json_err("alert", err))?;
                result.events.on_alert(&alert.title, &alert.description);
            }
            "error" => {
                let error: EventError =
                    serde_json::from_str(&data).map_err(|err| json_err("error", err))?;
                result.events.errors.push(error.message);
            }
            "done" => break,
            _ => {}
        }
    }
    Ok(result)
}

/// `ComputeTextDecoder` over a streamed compute response body. Note that this
/// decoder does not increment `TotalCount` (baseline behaviour).
pub fn decode_compute_text(body: &str) -> Result<ComputeTabulationResult, String> {
    let mut result = ComputeTabulationResult::default();
    for (event, data) in parse_events(body)? {
        match event.as_str() {
            "results" => {
                let values: Vec<Value> = serde_json::from_str(&data)
                    .map_err(|err| json_err("compute compute text", err))?;
                for value in values {
                    let repo = json_str(&value, "repository").to_owned();
                    let repo_id = json_i32(&value, "repositoryID");
                    let text = value.get("value").and_then(Value::as_str).unwrap_or("");
                    for line in text.split('\n') {
                        if line.is_empty() {
                            continue;
                        }
                        let line = truncate_capture(line).to_owned();
                        result
                            .repo_counts_for(&repo, repo_id)
                            .value_counts
                            .entry(line)
                            .and_modify(|count| *count += 1)
                            .or_insert(1);
                    }
                }
            }
            "alert" => {
                let alert: EventAlert =
                    serde_json::from_str(&data).map_err(|err| json_err("alert", err))?;
                result.events.on_alert(&alert.title, &alert.description);
            }
            "error" => {
                let error: EventError =
                    serde_json::from_str(&data).map_err(|err| json_err("error", err))?;
                result.events.errors.push(error.message);
            }
            "done" => break,
            _ => {}
        }
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SEARCH_BODY: &str = "event: matches\ndata: [{\"type\":\"content\",\"repository\":\"github.com/a/b\",\"repositoryID\":1,\"chunkMatches\":[{\"ranges\":[{},{}]},{\"ranges\":[{}]}]},{\"type\":\"path\",\"repository\":\"github.com/a/b\",\"repositoryID\":1},{\"type\":\"repo\",\"repository\":\"github.com/c/d\",\"repositoryID\":2},{\"type\":\"commit\",\"repository\":\"github.com/c/d\",\"repositoryID\":2},{\"type\":\"symbol\",\"repository\":\"github.com/e/f\",\"repositoryID\":3,\"symbols\":[{},{},{}]}]\n\nevent: progress\ndata: {\"done\":false,\"skipped\":[{\"reason\":\"shard-timeout\",\"message\":\"ignored-until-done\"}]}\n\nevent: progress\ndata: {\"done\":true,\"skipped\":[{\"reason\":\"shard-timeout\",\"message\":\"1 timed out\"},{\"reason\":\"backend-missing\",\"message\":\"zoekt-a\"},{\"reason\":\"display\",\"message\":\"limit\"}]}\n\nevent: alert\ndata: {\"title\":\"No repositories found\",\"description\":\"ignored\"}\n\nevent: alert\ndata: {\"title\":\"Alert\",\"description\":\"boom\"}\n\nevent: error\ndata: {\"message\":\"bad thing\"}\n\nevent: done\ndata: {}\n\nevent: matches\ndata: [{\"type\":\"repo\",\"repository\":\"ignored\",\"repositoryID\":9}]\n\n";

    #[test]
    fn search_tabulation_matches_the_baseline() {
        let result = decode_search(SEARCH_BODY).expect("decode");
        assert_eq!(result.total_count, 3 + 1 + 1 + 1 + 3);
        assert_eq!(result.repo_counts["github.com/a/b"].match_count, 4);
        assert_eq!(result.repo_counts["github.com/c/d"].match_count, 2);
        assert_eq!(result.repo_counts["github.com/e/f"].match_count, 3);
        assert_eq!(
            result.events.alerts,
            vec![
                "shard-timeout: 1 timed out",
                "backend-missing: zoekt-a",
                "Alert: boom"
            ]
        );
        assert_eq!(result.events.skipped_reasons, vec!["display: limit"]);
        assert!(result.events.did_timeout);
        assert_eq!(result.events.errors, vec!["bad thing"]);
        // The event after `done` is not decoded.
        assert!(!result.repo_counts.contains_key("ignored"));
    }

    #[test]
    fn repo_decoder_collects_repo_matches() {
        let result = decode_repo(SEARCH_BODY).expect("decode");
        assert_eq!(
            result.repos,
            vec![sg_insights::discovery::MinimalRepo {
                id: 2,
                name: "github.com/c/d".to_owned(),
            }]
        );
        // RepoDecoder's OnAlert uses the same `No repositories found`
        // suppression as the tabulators.
        assert_eq!(
            result.events.alerts,
            vec![
                "shard-timeout: 1 timed out",
                "backend-missing: zoekt-a",
                "Alert: boom"
            ]
        );
        assert_eq!(result.events.skipped_reasons, vec!["display: limit"]);
        assert!(result.events.did_timeout);
        assert_eq!(result.events.errors, vec!["bad thing"]);
        // The event after `done` is not decoded.
        assert!(result.repos.iter().all(|repo| repo.id != 9));
    }

    #[test]
    fn malformed_events_use_the_baseline_messages() {
        assert_eq!(
            parse_events("event: matches").unwrap_err(),
            "malformed event, no newline: event: matches"
        );
        assert_eq!(
            parse_events("foo: bar\ndata: {}\n\n").unwrap_err(),
            "malformed event, expected event: foo"
        );
        assert_eq!(
            parse_events("event: x\nfoo: {}\n\n").unwrap_err(),
            "malformed event x: expected data: foo"
        );
    }

    #[test]
    fn person_and_team_matches_are_unsupported() {
        let err =
            decode_search("event: matches\ndata: [{\"type\":\"person\",\"handle\":\"a\"}]\n\n")
                .unwrap_err();
        assert_eq!(err, "unknown MatchType 5");
        let err = decode_search("event: matches\ndata: [{\"type\":\"team\"}]\n\n").unwrap_err();
        assert_eq!(err, "unknown MatchType 6");
        let err = decode_search("event: matches\ndata: [{\"type\":\"bogus\"}]\n\n").unwrap_err();
        assert_eq!(err, "unknown MatchType: \"bogus\"");
    }

    #[test]
    fn compute_match_context_counts_environment_values() {
        let body = "event: results\ndata: [{\"repository\":\"github.com/a/b\",\"repositoryID\":1,\"matches\":[{\"environment\":{\"v\":{\"value\":\"x\"},\"w\":{\"value\":\"x\"},\"e\":{\"value\":\"\"}}},{\"environment\":{\"v\":{\"value\":\"y\"}}}]}]\n\nevent: results\ndata: [{\"repository\":\"github.com/a/b\",\"repositoryID\":1,\"matches\":[{\"environment\":{\"v\":{\"value\":\"x\"}}}]}]\n\nevent: done\ndata: {}\n\n";
        let result = decode_compute_match_context(body).expect("decode");
        assert_eq!(result.total_count, 4);
        let repo = &result.repo_counts["github.com/a/b"];
        assert_eq!(repo.value_counts["x"], 3);
        assert_eq!(repo.value_counts["y"], 1);
    }

    #[test]
    fn compute_text_counts_non_empty_lines_without_total() {
        let long = "a".repeat(120);
        let body = format!(
            "event: results\ndata: [{{\"repository\":\"r\",\"repositoryID\":1,\"value\":\"one\\n\\ntwo\\n{long}\"}}]\n\nevent: done\ndata: {{}}\n\n"
        );
        let result = decode_compute_text(&body).expect("decode");
        assert_eq!(result.total_count, 0);
        let repo = &result.repo_counts["r"];
        assert_eq!(repo.value_counts["one"], 1);
        assert_eq!(repo.value_counts["two"], 1);
        assert_eq!(repo.value_counts[&"a".repeat(100)], 1);
        assert_eq!(repo.value_counts.len(), 3);
    }
}
