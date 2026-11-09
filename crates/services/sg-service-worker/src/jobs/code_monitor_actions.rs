#![forbid(unsafe_code)]

//! Code monitor action payloads and delivery
//! (`internal/codemonitors/background`: `action.go`, `webhook.go`,
//! `slack.go` and the payload half of `email.go`).
//!
//! This is the M9.8cl slice: the pure payload/template layer both code
//! monitor action workers use, plus the HTTP delivery for webhooks and Slack.
//! It is deliberately database-free — callers pass a [`CommitMatch`] slice.
//!
//! Ported 1:1 from the baseline (including the quirks):
//!
//! * `truncateResults` limits **matched ranges**, not matches:
//!   `CommitMatch.ResultCount` is the number of matched ranges (or 1 when a
//!   match has none), and ranges inside a retained match are truncated when
//!   they would exceed the limit;
//! * webhook/Slack URLs use `utm_source` per action kind and the GraphQL
//!   relay id for code monitor links (`base64("CodeMonitor:<id>")`);
//! * Slack blocks have the shape `slack-go` produces (a flattened
//!   `{"blocks":[...]}` envelope, `mrkdwn` sections);
//! * `truncateMatchContent` keeps at most 10 lines / 2500 bytes and appends
//!   `"...\n"` when content is cut;
//! * JSON sent over the wire is escaped exactly like Go's `encoding/json`
//!   (`<`, `>`, `&`, U+2028, U+2029 become `\uXXXX`), while the differential
//!   harness prints payloads with HTML escaping disabled.
//!
//! Not ported in this slice: the e-mail transport (`internal/txemail`), the
//! e-mail HTML/text templates, and the `ActionRunner` that fetches actions
//! and recipients from the database (M9.8cm).

use base64::Engine as _;
use serde::Serialize;
use url::Url;

/// `actionArgs` without the pointers: the inputs any action needs.
#[derive(Clone, Debug)]
pub struct ActionArgs<'a> {
    pub monitor_description: &'a str,
    pub external_url: &'a Url,
    pub monitor_id: i64,
    pub utm_source: &'a str,
    pub monitor_owner_name: &'a str,
    pub query: &'a str,
    pub results: &'a [CommitMatch],
    pub include_results: bool,
}

/// The payload-relevant subset of a commit search match.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitMatch {
    pub repo_name: String,
    pub commit_id: String,
    /// `MessagePreview` and `DiffPreview` are mutually exclusive in the
    /// baseline; when both are `None`, `ResultCount` panics.
    pub message_preview: Option<MatchedString>,
    pub diff_preview: Option<MatchedString>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MatchedString {
    pub content: String,
    pub matched_ranges: Vec<MatchedRange>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MatchedRange {
    pub start_offset: usize,
    pub end_offset: usize,
}

impl CommitMatch {
    /// `(*result.CommitMatch).ResultCount`.
    pub fn result_count(&self) -> usize {
        let count = match (&self.diff_preview, &self.message_preview) {
            (Some(diff), _) => diff.matched_ranges.len(),
            (None, Some(message)) => message.matched_ranges.len(),
            (None, None) => panic!("exactly one of DiffPreview or Message must be set"),
        };
        if count > 0 {
            count
        } else {
            1
        }
    }

    /// `(*result.CommitMatch).Limit`: returns the remaining limit.
    fn limit(&mut self, limit: usize) -> usize {
        match (&mut self.diff_preview, &mut self.message_preview) {
            (Some(diff), _) => limit_matched_string(diff, limit),
            (None, Some(message)) => limit_matched_string(message, limit),
            (None, None) => panic!("exactly one of DiffPreview or Message must be set"),
        }
    }

    /// `api.CommitID.Short`.
    pub fn short_commit_id(&self) -> String {
        short_commit_id(&self.commit_id).to_owned()
    }
}

fn limit_matched_string(matched: &mut MatchedString, limit: usize) -> usize {
    let count = matched.matched_ranges.len();
    if count == 0 {
        limit - 1
    } else if count > limit {
        matched.matched_ranges.truncate(limit);
        0
    } else {
        limit - count
    }
}

fn short_commit_id(id: &str) -> &str {
    if id.len() < 7 {
        return id;
    }
    let mut end = 7;
    while !id.is_char_boundary(end) {
        end += 1;
    }
    &id[..end]
}

/// `truncateResults`: returns the retained matches (with truncated ranges),
/// the total result count and how many results were dropped.
pub fn truncate_results(
    matches: &[CommitMatch],
    max_results: usize,
) -> (Vec<CommitMatch>, usize, usize) {
    let total: usize = matches.iter().map(CommitMatch::result_count).sum();

    let mut limit = max_results as i64;
    let mut kept = Vec::new();
    for matched in matches {
        if limit <= 0 {
            break;
        }
        let mut matched = matched.clone();
        limit = matched.limit(limit as usize) as i64;
        kept.push(matched);
    }

    let output: usize = kept.iter().map(CommitMatch::result_count).sum();
    (kept, total, total - output)
}

/// `truncateMatchContent`: at most 10 lines and 2500 bytes, appending
/// `"...\n"` when the content was cut.
pub fn truncate_match_content(matched: &CommitMatch) -> String {
    let text = match (&matched.diff_preview, &matched.message_preview) {
        (Some(diff), _) => diff,
        (None, Some(message)) => message,
        (None, None) => panic!("exactly one of DiffPreview or Message must be set"),
    };

    const MAX_BYTES: usize = 2500;
    const MAX_LINES: usize = 10;

    let mut lines = split_after_newline(&text.content);
    let limit = lines.len().min(MAX_LINES);
    let mut chars = 0usize;
    let mut index = 0usize;
    while index < limit {
        chars += lines[index].len();
        if chars > MAX_BYTES {
            break;
        }
        index += 1;
    }
    if lines.len() > index {
        lines.truncate(index);
        lines.push("...\n");
    }
    lines.concat()
}

/// `strings.SplitAfter(s, "\n")`: always yields at least one element and keeps
/// the trailing empty element when the content ends with a newline.
fn split_after_newline(content: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    while let Some(offset) = content[start..].find('\n') {
        let end = start + offset + 1;
        parts.push(&content[start..end]);
        start = end;
    }
    parts.push(&content[start..]);
    parts
}

/// `formatCodeBlock`.
pub fn format_code_block(content: &str) -> String {
    format!("```{}```", content.replace("```", "\\`\\`\\`"))
}

/// `relay.MarshalID("CodeMonitor", id)`.
pub fn relay_marshal_id(id: i64) -> String {
    base64::engine::general_purpose::STANDARD.encode(format!("CodeMonitor:{id}"))
}

/// `sourcegraphURL`: relative path resolution plus `q`/`utm_source` query
/// parameters in Go's (sorted) encoding order.
pub fn sourcegraph_url(base: &Url, path: &str, query: &str, utm_source: &str) -> String {
    let mut resolved = base.join(path).unwrap_or_else(|_| base.clone());
    let mut pairs: Vec<(String, String)> = resolved
        .query_pairs()
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .filter(|(key, _)| key != "q" && key != "utm_source")
        .collect();
    if !query.is_empty() {
        pairs.push(("q".to_owned(), query.to_owned()));
    }
    pairs.push(("utm_source".to_owned(), utm_source.to_owned()));
    pairs.sort_by(|left, right| left.0.cmp(&right.0));

    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in &pairs {
        serializer.append_pair(key, value);
    }
    resolved.set_query(Some(&serializer.finish()));
    resolved.to_string()
}

pub fn get_search_url(base: &Url, query: &str, utm_source: &str) -> String {
    sourcegraph_url(base, "search", query, utm_source)
}

pub fn get_code_monitor_url(base: &Url, monitor_id: i64, utm_source: &str) -> String {
    sourcegraph_url(
        base,
        &format!("code-monitoring/{}", relay_marshal_id(monitor_id)),
        "",
        utm_source,
    )
}

pub fn get_commit_url(base: &Url, repo_name: &str, oid: &str, utm_source: &str) -> String {
    sourcegraph_url(base, &format!("{repo_name}/-/commit/{oid}"), "", utm_source)
}

/// `webhookPayload`.
#[derive(Debug, Serialize)]
pub struct WebhookPayload {
    #[serde(rename = "monitorDescription")]
    pub monitor_description: String,
    #[serde(rename = "monitorURL")]
    pub monitor_url: String,
    pub query: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub results: Vec<WebhookResult>,
}

/// `webhookResult`.
#[derive(Debug, Serialize)]
pub struct WebhookResult {
    pub repository: String,
    pub commit: String,
    #[serde(skip_serializing_if = "skip_empty_string")]
    pub message: Option<String>,
    #[serde(
        rename = "matchedMessageRanges",
        skip_serializing_if = "skip_empty_ranges"
    )]
    pub matched_message_ranges: Option<Vec<[usize; 2]>>,
    #[serde(skip_serializing_if = "skip_empty_string")]
    pub diff: Option<String>,
    #[serde(
        rename = "matchedDiffRanges",
        skip_serializing_if = "skip_empty_ranges"
    )]
    pub matched_diff_ranges: Option<Vec<[usize; 2]>>,
}

/// `generateWebhookPayload`.
pub fn generate_webhook_payload(args: &ActionArgs<'_>) -> WebhookPayload {
    let mut payload = WebhookPayload {
        monitor_description: args.monitor_description.to_owned(),
        monitor_url: get_code_monitor_url(args.external_url, args.monitor_id, args.utm_source),
        query: args.query.to_owned(),
        results: Vec::new(),
    };
    if args.include_results {
        payload.results = args.results.iter().map(generate_result).collect();
    }
    payload
}

fn generate_result(matched: &CommitMatch) -> WebhookResult {
    let mut result = WebhookResult {
        repository: matched.repo_name.clone(),
        commit: matched.commit_id.clone(),
        message: None,
        matched_message_ranges: None,
        diff: None,
        matched_diff_ranges: None,
    };
    if let Some(preview) = &matched.message_preview {
        result.message = Some(preview.content.clone());
        result.matched_message_ranges = Some(ranges_to_ints(&preview.matched_ranges));
    }
    if let Some(preview) = &matched.diff_preview {
        result.diff = Some(preview.content.clone());
        result.matched_diff_ranges = Some(ranges_to_ints(&preview.matched_ranges));
    }
    result
}

/// Go's `omitempty` for an optional string drops `Some("")` too.
fn skip_empty_string(value: &Option<String>) -> bool {
    value.as_deref().is_none_or(str::is_empty)
}

/// Go's `omitempty` for a slice drops zero-length slices.
fn skip_empty_ranges(value: &Option<Vec<[usize; 2]>>) -> bool {
    value.as_ref().is_none_or(Vec::is_empty)
}

fn ranges_to_ints(ranges: &[MatchedRange]) -> Vec<[usize; 2]> {
    ranges
        .iter()
        .map(|range| [range.start_offset, range.end_offset])
        .collect()
}

/// A Slack block. Only `section` blocks with a `mrkdwn` text object are
/// produced by code monitors, matching `slack-go`.
#[derive(Debug, Serialize)]
pub struct SlackBlock {
    #[serde(rename = "type")]
    pub block_type: &'static str,
    pub text: SlackTextObject,
}

#[derive(Debug, Serialize)]
pub struct SlackTextObject {
    #[serde(rename = "type")]
    pub text_type: &'static str,
    pub text: String,
}

/// `slack.WebhookMessage` as produced for code monitors (the `Blocks`
/// marshaler flattens `BlockSet` into `"blocks"`).
#[derive(Debug, Serialize)]
pub struct SlackWebhookMessage {
    pub blocks: Vec<SlackBlock>,
}

fn markdown_section(text: String) -> SlackBlock {
    SlackBlock {
        block_type: "section",
        text: SlackTextObject {
            text_type: "mrkdwn",
            text,
        },
    }
}

/// `slackPayload`.
pub fn slack_payload(args: &ActionArgs<'_>) -> SlackWebhookMessage {
    let (truncated, total_count, truncated_count) = truncate_results(args.results, 5);

    let mut blocks = vec![markdown_section(format!(
        "{}'s Sourcegraph Code monitor, *{}*, detected *{}* new matches.",
        args.monitor_owner_name, args.monitor_description, total_count
    ))];

    if args.include_results {
        for matched in &truncated {
            let result_type = if matched.diff_preview.is_some() {
                "Diff"
            } else {
                "Message"
            };
            blocks.push(markdown_section(format!(
                "{} match: <{}|{}@{}>",
                result_type,
                get_commit_url(
                    args.external_url,
                    &matched.repo_name,
                    &matched.commit_id,
                    args.utm_source
                ),
                matched.repo_name,
                matched.short_commit_id()
            )));

            let content_raw = truncate_match_content(matched);
            blocks.push(markdown_section(format_code_block(&content_raw)));
        }
        if truncated_count > 0 {
            blocks.push(markdown_section(format!(
                "...and <{}|{} more matches>.",
                get_search_url(args.external_url, args.query, args.utm_source),
                truncated_count
            )));
        }
    } else {
        blocks.push(markdown_section(format!(
            "<{}|View results>",
            get_search_url(args.external_url, args.query, args.utm_source)
        )));
    }

    blocks.push(markdown_section(format!(
        "If you are {}, you can <{}|edit your code monitor>",
        args.monitor_owner_name,
        get_code_monitor_url(args.external_url, args.monitor_id, args.utm_source)
    )));

    SlackWebhookMessage { blocks }
}

/// The payload-relevant subset of `database.EmailAction`.
#[derive(Clone, Debug)]
pub struct EmailActionInput {
    pub monitor: i64,
    pub priority: String,
}

/// `TemplateDataNewSearchResults`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TemplateDataNewSearchResults {
    pub priority: String,
    pub code_monitor_url: String,
    pub search_url: String,
    pub description: String,
    pub include_results: bool,
    pub truncated_results: Vec<DisplayResult>,
    pub total_count: usize,
    pub truncated_count: usize,
    pub result_pluralized: String,
    pub truncated_result_pluralized: String,
    pub display_more_link: bool,
    pub is_test: bool,
}

/// `DisplayResult`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DisplayResult {
    pub result_type: String,
    pub commit_url: String,
    pub repo_name: String,
    pub commit_id: String,
    pub content: String,
}

pub const UTM_SOURCE_EMAIL: &str = "code-monitoring-email";
pub const PRIORITY_CRITICAL: &str = "CRITICAL";

/// `NewTemplateDataForNewSearchResults`.
pub fn new_template_data_for_new_search_results(
    args: &ActionArgs<'_>,
    email: &EmailActionInput,
) -> TemplateDataNewSearchResults {
    let search_url = get_search_url(args.external_url, args.query, UTM_SOURCE_EMAIL);
    let code_monitor_url = get_code_monitor_url(args.external_url, email.monitor, UTM_SOURCE_EMAIL);
    let priority = if email.priority == PRIORITY_CRITICAL {
        "[Critical] "
    } else {
        ""
    };

    let (truncated, total_count, truncated_count) = truncate_results(args.results, 5);
    let display_results: Vec<DisplayResult> = truncated
        .iter()
        .map(|matched| to_display_result(matched, args.external_url))
        .collect();

    TemplateDataNewSearchResults {
        priority: priority.to_owned(),
        code_monitor_url,
        search_url,
        description: args.monitor_description.to_owned(),
        include_results: args.include_results,
        truncated_results: display_results,
        total_count,
        truncated_count,
        result_pluralized: pluralize("result", total_count),
        truncated_result_pluralized: pluralize("result", truncated_count),
        display_more_link: args.include_results && truncated_count > 0,
        is_test: false,
    }
}

fn pluralize(word: &str, count: usize) -> String {
    if count == 1 {
        word.to_owned()
    } else {
        format!("{word}s")
    }
}

fn to_display_result(matched: &CommitMatch, external_url: &Url) -> DisplayResult {
    let result_type = if matched.diff_preview.is_some() {
        "Diff"
    } else {
        "Message"
    };
    DisplayResult {
        result_type: result_type.to_owned(),
        commit_url: get_commit_url(
            external_url,
            &matched.repo_name,
            &matched.commit_id,
            UTM_SOURCE_EMAIL,
        ),
        repo_name: matched.repo_name.clone(),
        commit_id: matched.short_commit_id(),
        content: truncate_match_content(matched),
    }
}

/// `json.Marshal` compatibility: Go's encoder escapes `<`, `>`, `&`, U+2028
/// and U+2029 even inside strings.
pub fn go_marshal_json<T: Serialize>(value: &T) -> Result<String, String> {
    let raw = serde_json::to_string(value).map_err(|err| err.to_string())?;
    Ok(raw
        .replace('&', "\\u0026")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029"))
}

/// Go's `%q` for the common cases (the status-code error body).
fn go_quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if (ch as u32) < 0x20 => out.push_str(&format!("\\x{:02x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

/// `StatusCodeError.Error`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusCodeError {
    pub code: u16,
    pub status: String,
    pub body: String,
}

impl std::fmt::Display for StatusCodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "non-200 response {} {} with body {}",
            self.code,
            self.status,
            go_quote(&self.body)
        )
    }
}

fn response_error(status: reqwest::StatusCode, body: String) -> String {
    // Go's `http.Response.Status` is `"<code> <reason phrase>"`.
    let status_text = status
        .canonical_reason()
        .map(|reason| format!("{} {reason}", status.as_u16()))
        .unwrap_or_else(|| status.as_u16().to_string());
    StatusCodeError {
        code: status.as_u16(),
        status: status_text,
        body,
    }
    .to_string()
}

/// `postWebhook` / `postSlackWebhook` body: POST `application/json`, accept
/// only `200 OK`.
async fn post_json(
    client: &reqwest::Client,
    url: &str,
    body: String,
    what: &str,
) -> Result<(), String> {
    let request = client
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body);
    let response = request
        .send()
        .await
        .map_err(|err| format!("failed to post {what}: {err}"))?;
    let status = response.status();
    if status != reqwest::StatusCode::OK {
        let body = response.text().await.unwrap_or_default();
        return Err(response_error(status, body));
    }
    Ok(())
}

/// `sendWebhookNotification` (with the doer client injected).
pub async fn send_webhook_notification(
    client: &reqwest::Client,
    url: &str,
    args: &ActionArgs<'_>,
) -> Result<(), String> {
    let payload = generate_webhook_payload(args);
    let body = go_marshal_json(&payload).map_err(|err| format!("marshal failed: {err}"))?;
    post_json(client, url, body, "webhook").await
}

/// `sendSlackNotification` (with the doer client injected).
pub async fn send_slack_notification(
    client: &reqwest::Client,
    url: &str,
    args: &ActionArgs<'_>,
) -> Result<(), String> {
    let payload = slack_payload(args);
    let body = go_marshal_json(&payload).map_err(|err| format!("marshal failed: {err}"))?;
    post_json(client, url, body, "webhook").await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matched(ranges: Vec<(usize, usize)>) -> CommitMatch {
        CommitMatch {
            repo_name: "r".to_owned(),
            commit_id: "0123456789abcdef".to_owned(),
            message_preview: Some(MatchedString {
                content: "content".to_owned(),
                matched_ranges: ranges
                    .into_iter()
                    .map(|(start_offset, end_offset)| MatchedRange {
                        start_offset,
                        end_offset,
                    })
                    .collect(),
            }),
            diff_preview: None,
        }
    }

    #[test]
    fn result_count_counts_ranges_and_defaults_to_one() {
        assert_eq!(matched(vec![]).result_count(), 1);
        assert_eq!(matched(vec![(0, 1), (2, 3)]).result_count(), 2);
    }

    #[test]
    fn truncate_results_limits_ranges_not_matches() {
        let matches = vec![
            matched(vec![(0, 1), (2, 3)]),
            matched(vec![(0, 1)]),
            matched(vec![]),
            matched(vec![(0, 1), (2, 3), (4, 5)]),
            matched(vec![(0, 1)]),
        ];
        let (kept, total, truncated) = truncate_results(&matches, 5);
        assert_eq!(total, 8);
        assert_eq!(truncated, 3);
        assert_eq!(
            kept.iter()
                .map(CommitMatch::result_count)
                .collect::<Vec<_>>(),
            vec![2, 1, 1, 1]
        );
    }

    #[test]
    fn split_after_keeps_trailing_empty_part() {
        assert_eq!(split_after_newline("a\n"), vec!["a\n", ""]);
        assert_eq!(split_after_newline(""), vec![""]);
        assert_eq!(split_after_newline("a\nb"), vec!["a\n", "b"]);
    }

    #[test]
    fn truncate_content_keeps_at_most_ten_lines() {
        let content = "0123456789abcde\n".repeat(200);
        let result = truncate_match_content(&CommitMatch {
            repo_name: "r".to_owned(),
            commit_id: "c".to_owned(),
            message_preview: Some(MatchedString {
                content,
                matched_ranges: Vec::new(),
            }),
            diff_preview: None,
        });
        assert_eq!(result, format!("{}...\n", "0123456789abcde\n".repeat(10)));
    }

    #[test]
    fn code_block_escapes_nested_fences() {
        assert_eq!(
            format_code_block("x ```y``` z"),
            "```x \\`\\`\\`y\\`\\`\\` z```"
        );
    }

    #[test]
    fn sourcegraph_url_matches_go_sorting() {
        let base = Url::parse("https://sg.example.com/sg/?foo=bar&z=1").unwrap();
        // A relative reference drops the base query, exactly like
        // `url.URL.ResolveReference`.
        assert_eq!(
            get_search_url(&base, "type:commit crash", "code-monitoring-email"),
            "https://sg.example.com/sg/search?q=type%3Acommit+crash&utm_source=code-monitoring-email"
        );
        assert_eq!(
            get_code_monitor_url(&base, 123, "code-monitoring-email"),
            "https://sg.example.com/sg/code-monitoring/Q29kZU1vbml0b3I6MTIz?utm_source=code-monitoring-email"
        );
    }

    #[test]
    fn go_marshal_escapes_html_characters() {
        let payload = WebhookPayload {
            monitor_description: "<a>&".to_owned(),
            monitor_url: String::new(),
            query: String::new(),
            results: Vec::new(),
        };
        let raw = go_marshal_json(&payload).unwrap();
        assert!(raw.contains("\\u003ca\\u003e\\u0026"));
    }

    #[test]
    fn status_error_matches_go_message() {
        let error = StatusCodeError {
            code: 500,
            status: "500 Internal Server Error".to_owned(),
            body: "boom".to_owned(),
        };
        assert_eq!(
            error.to_string(),
            "non-200 response 500 500 Internal Server Error with body \"boom\""
        );
    }
}
