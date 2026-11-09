#![forbid(unsafe_code)]

//! Differential harness for the code monitor action payloads (M9.8cl).
//!
//! Mirror of the Go harness
//! `internal/codemonitors/background/cm_payload_harness_test.go`: builds the
//! same synthetic commit matches and prints the canonical `cmp ` lines
//! (webhook/Slack payloads, e-mail template data, truncation, URLs and the
//! bytes/errors of the HTTP delivery).
//!
//! Gated by `SG_CM_PAYLOAD_HARNESS_ON`; no database is involved.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc;

use serde::Serialize;
use url::Url;

use sg_service_worker::jobs::code_monitor_actions::{
    generate_webhook_payload, get_code_monitor_url, get_commit_url, get_search_url,
    go_marshal_json, new_template_data_for_new_search_results, send_slack_notification,
    send_webhook_notification, slack_payload, truncate_match_content, truncate_results, ActionArgs,
    CommitMatch, EmailActionInput, MatchedRange, MatchedString, TemplateDataNewSearchResults,
    PRIORITY_CRITICAL,
};

fn enc<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).expect("serialize")
}

fn message_match(
    repo: &str,
    commit: &str,
    content: &str,
    ranges: &[(usize, usize)],
) -> CommitMatch {
    CommitMatch {
        repo_name: repo.to_owned(),
        commit_id: commit.to_owned(),
        message_preview: Some(MatchedString {
            content: content.to_owned(),
            matched_ranges: ranges
                .iter()
                .map(|&(start_offset, end_offset)| MatchedRange {
                    start_offset,
                    end_offset,
                })
                .collect(),
        }),
        diff_preview: None,
    }
}

fn diff_match(repo: &str, commit: &str, content: &str, ranges: &[(usize, usize)]) -> CommitMatch {
    CommitMatch {
        repo_name: repo.to_owned(),
        commit_id: commit.to_owned(),
        message_preview: None,
        diff_preview: Some(MatchedString {
            content: content.to_owned(),
            matched_ranges: ranges
                .iter()
                .map(|&(start_offset, end_offset)| MatchedRange {
                    start_offset,
                    end_offset,
                })
                .collect(),
        }),
    }
}

fn results() -> Vec<CommitMatch> {
    vec![
        message_match(
            "github.com/acme/alpha",
            "a1b2c3d4e5f60718",
            "line1\nline2 crash here\nline3\n",
            &[(6, 11), (17, 22)],
        ),
        diff_match(
            "github.com/acme/beta",
            "b1b2c3d4e5f60718",
            "--- a/x.go\n+++ b/x.go\n@@\n-panic()\n+recover()\n",
            &[(20, 27)],
        ),
        message_match("github.com/acme/gamma", "c1", "no ranges", &[]),
        message_match(
            "github.com/acme/delta",
            "d1b2c3d4e5f60718",
            "one\ntwo\nthree\n",
            &[(0, 3), (4, 7), (8, 13)],
        ),
        message_match(
            "github.com/acme/epsilon",
            "e1b2c3d4e5f60718",
            "x crash y",
            &[(2, 7)],
        ),
        message_match(
            "github.com/acme/zeta",
            "f1b2c3d4e5f60718",
            "q crash r",
            &[(2, 7)],
        ),
        message_match(
            "github.com/acme/eta",
            "01b2c3d4e5f60718",
            "s crash t",
            &[(2, 7)],
        ),
    ]
}

fn external_url() -> Url {
    Url::parse("https://sg.example.com/sg/?foo=bar&z=1").expect("url")
}

fn base_args<'a>(results: &'a [CommitMatch], include: bool) -> ActionArgs<'a> {
    ActionArgs {
        monitor_description: r#"Weekly <crash> & "quotes""#,
        external_url: Box::leak(Box::new(external_url())),
        monitor_id: 1234567890123,
        utm_source: "code-monitor-webhook",
        monitor_owner_name: "Alice O'Neil",
        query: r#"type:commit file:\.go crash & "panic""#,
        results,
        include_results: include,
    }
}

#[derive(Serialize)]
struct CmpDisplay {
    #[serde(rename = "resultType")]
    result_type: String,
    #[serde(rename = "commitURL")]
    commit_url: String,
    #[serde(rename = "repoName")]
    repo_name: String,
    #[serde(rename = "commitID")]
    commit_id: String,
    content: String,
}

#[derive(Serialize)]
struct CmpData {
    priority: String,
    #[serde(rename = "codeMonitorURL")]
    code_monitor_url: String,
    #[serde(rename = "searchURL")]
    search_url: String,
    description: String,
    #[serde(rename = "includeResults")]
    include_results: bool,
    #[serde(rename = "truncatedResults")]
    truncated_results: Vec<CmpDisplay>,
    #[serde(rename = "totalCount")]
    total_count: usize,
    #[serde(rename = "truncatedCount")]
    truncated_count: usize,
    #[serde(rename = "resultPluralized")]
    result_pluralized: String,
    #[serde(rename = "truncatedResultPluralized")]
    truncated_result_pluralized: String,
    #[serde(rename = "displayMoreLink")]
    display_more_link: bool,
    #[serde(rename = "isTest")]
    is_test: bool,
}

fn cmp_data(data: &TemplateDataNewSearchResults) -> CmpData {
    CmpData {
        priority: data.priority.clone(),
        code_monitor_url: data.code_monitor_url.clone(),
        search_url: data.search_url.clone(),
        description: data.description.clone(),
        include_results: data.include_results,
        truncated_results: data
            .truncated_results
            .iter()
            .map(|display| CmpDisplay {
                result_type: display.result_type.clone(),
                commit_url: display.commit_url.clone(),
                repo_name: display.repo_name.clone(),
                commit_id: display.commit_id.clone(),
                content: display.content.clone(),
            })
            .collect(),
        total_count: data.total_count,
        truncated_count: data.truncated_count,
        result_pluralized: data.result_pluralized.clone(),
        truncated_result_pluralized: data.truncated_result_pluralized.clone(),
        display_more_link: data.display_more_link,
        is_test: data.is_test,
    }
}

fn kept_ranges(matches: &[CommitMatch]) -> Vec<usize> {
    matches.iter().map(CommitMatch::result_count).collect()
}

struct Captured {
    content_type: String,
    body: String,
}

fn find_header_end(data: &[u8]) -> Option<usize> {
    data.windows(4).position(|window| window == b"\r\n\r\n")
}

/// Minimal HTTP/1.1 server: accepts one request, captures its Content-Type
/// and body, replies with the given status line/body.
fn spawn_capture(
    status_line: &'static str,
    response_body: &'static str,
) -> (String, mpsc::Receiver<Captured>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut data = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            let read = stream.read(&mut buffer).expect("read");
            if read == 0 {
                break;
            }
            data.extend_from_slice(&buffer[..read]);
            if let Some(end) = find_header_end(&data) {
                let header_end = end + 4;
                let headers = String::from_utf8_lossy(&data[..header_end]).to_string();
                let content_length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        if name.eq_ignore_ascii_case("content-length") {
                            value.trim().parse().ok()
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);
                if data.len() >= header_end + content_length {
                    break;
                }
            }
        }
        let header_end = find_header_end(&data).map(|end| end + 4).unwrap_or(0);
        let headers = String::from_utf8_lossy(&data[..header_end]).to_string();
        let content_type = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                if name.eq_ignore_ascii_case("content-type") {
                    Some(value.trim().to_owned())
                } else {
                    None
                }
            })
            .unwrap_or_default();
        let body = String::from_utf8_lossy(&data[header_end..]).to_string();
        let response = format!(
            "{status_line}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response_body}",
            response_body.len()
        );
        let _ = stream.write_all(response.as_bytes());
        let _ = sender.send(Captured { content_type, body });
    });
    (format!("http://{addr}/hook"), receiver)
}

#[tokio::test]
async fn code_monitor_actions_payload_differential() {
    if std::env::var("SG_CM_PAYLOAD_HARNESS_ON")
        .map(|value| value.is_empty())
        .unwrap_or(true)
    {
        return;
    }

    let include_results = results();
    let exclude_results = results();
    let include = base_args(&include_results, true);
    let exclude = base_args(&exclude_results, false);

    println!(
        "cmp webhook include=true {}",
        enc(&generate_webhook_payload(&include))
    );
    println!(
        "cmp webhook include=false {}",
        enc(&generate_webhook_payload(&exclude))
    );
    // Slack-go's `Blocks` custom marshaler escapes HTML characters even when
    // the outer encoder has escaping disabled; mirror that with go_marshal_json.
    println!(
        "cmp slack include=true {}",
        go_marshal_json(&slack_payload(&include)).expect("slack json")
    );
    println!(
        "cmp slack include=false {}",
        go_marshal_json(&slack_payload(&exclude)).expect("slack json")
    );

    let email = EmailActionInput {
        monitor: 424242,
        priority: String::new(),
    };
    let critical = EmailActionInput {
        monitor: 424242,
        priority: PRIORITY_CRITICAL.to_owned(),
    };
    println!(
        "cmp data include=true {}",
        enc(&cmp_data(&new_template_data_for_new_search_results(
            &include, &email
        )))
    );
    println!(
        "cmp data include=false {}",
        enc(&cmp_data(&new_template_data_for_new_search_results(
            &exclude, &email
        )))
    );
    println!(
        "cmp data critical {}",
        enc(&cmp_data(&new_template_data_for_new_search_results(
            &include, &critical
        )))
    );

    let many = results();
    let (kept, total, truncated) = truncate_results(&many, 5);
    println!(
        "cmp truncate many total={} kept={} truncated={} kept_ranges={}",
        total,
        total - truncated,
        truncated,
        enc(&kept_ranges(&kept))
    );

    let single = vec![message_match(
        "github.com/acme/one",
        "1111111",
        "abcdefghijklmnop",
        &[
            (0, 1),
            (1, 2),
            (2, 3),
            (3, 4),
            (4, 5),
            (5, 6),
            (6, 7),
            (7, 8),
        ],
    )];
    let (kept, total, truncated) = truncate_results(&single, 5);
    println!(
        "cmp truncate single total={} kept={} truncated={} kept_ranges={}",
        total,
        total - truncated,
        truncated,
        enc(&kept_ranges(&kept))
    );

    let empty: Vec<CommitMatch> = Vec::new();
    let (kept, total, truncated) = truncate_results(&empty, 5);
    println!(
        "cmp truncate none total={} kept={} truncated={} kept_ranges={}",
        total,
        total - truncated,
        truncated,
        enc(&kept_ranges(&kept))
    );

    let long = "0123456789abcde\n".repeat(200);
    println!(
        "cmp content long {}",
        enc(&truncate_match_content(&message_match(
            "r",
            "c",
            &long,
            &[]
        )))
    );
    println!(
        "cmp content backticks {}",
        enc(&truncate_match_content(&message_match(
            "r",
            "c",
            "x ```y``` z",
            &[]
        )))
    );
    println!(
        "cmp content trailing_newline {}",
        enc(&truncate_match_content(&message_match(
            "r",
            "c",
            "a\n",
            &[]
        )))
    );
    println!(
        "cmp content empty {}",
        enc(&truncate_match_content(&message_match("r", "c", "", &[])))
    );
    println!(
        "cmp content diff {}",
        enc(&truncate_match_content(&diff_match(
            "r",
            "c",
            "---\n+++\npanic\n",
            &[]
        )))
    );

    let base = external_url();
    println!(
        "cmp url monitor {}",
        enc(&get_code_monitor_url(
            &base,
            1234567890123,
            "code-monitoring-email"
        ))
    );
    println!(
        "cmp url search {}",
        enc(&get_search_url(
            &base,
            r#"type:commit crash & "panic""#,
            "code-monitoring-email"
        ))
    );
    println!(
        "cmp url commit {}",
        enc(&get_commit_url(
            &base,
            "github.com/acme/alpha",
            "a1b2c3d4",
            "code-monitor-slack-webhook"
        ))
    );

    let client = reqwest::Client::new();

    let (url, receiver) = spawn_capture("HTTP/1.1 200 OK", "");
    let post_results = results();
    let args = base_args(&post_results, true);
    send_webhook_notification(&client, &url, &args)
        .await
        .expect("post webhook");
    let captured = receiver.recv().expect("capture");
    println!(
        "cmp post webhook ok=true content_type={} body={}",
        enc(&captured.content_type),
        enc(&captured.body)
    );

    let (url, receiver) = spawn_capture("HTTP/1.1 200 OK", "");
    let post_results = results();
    let args = base_args(&post_results, true);
    send_slack_notification(&client, &url, &args)
        .await
        .expect("post slack");
    let captured = receiver.recv().expect("capture");
    println!(
        "cmp post slack ok=true content_type={} body={}",
        enc(&captured.content_type),
        enc(&captured.body)
    );

    let (url, _receiver) = spawn_capture("HTTP/1.1 500 Internal Server Error", "boom");
    let post_results = results();
    let args = base_args(&post_results, true);
    let error = send_webhook_notification(&client, &url, &args)
        .await
        .expect_err("expected webhook error");
    println!("cmp post error webhook {}", enc(&error));

    let (url, _receiver) = spawn_capture("HTTP/1.1 500 Internal Server Error", "boom");
    let post_results = results();
    let args = base_args(&post_results, true);
    let error = send_slack_notification(&client, &url, &args)
        .await
        .expect_err("expected slack error");
    println!("cmp post error slack {}", enc(&error));
}
