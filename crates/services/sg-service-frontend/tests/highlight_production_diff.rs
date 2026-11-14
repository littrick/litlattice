//! M9.36d differential: production `highlight.CodeAsLines` adapter
//! (`CodeHighlighterSource`), the seam behind `FileDiffHunk.highlight` and
//! `GitBlob.highlight`.
//!
//! The Go oracle
//! (`cmd/frontend/internal/highlight/zz_m936d_highlight_production_oracle_test.go`,
//! untracked, `M936D_*`-gated) drives the real `CodeAsLines` against a local
//! syntect stand-in, recording the request, the stand-in response and the
//! per-line output. This harness replays the recorded response from a local TCP
//! server, points the production [`CodeHighlighterSource`] at it, and writes the
//! same JSON schema so the two files can be compared byte-for-byte.
//!
//! Corpus fields: `name`, `content` or `content_hex` (invalid UTF-8/binary),
//! `filepath`, `start_line`, `end_line`, `keep_final_newline`,
//! `disable_timeout`, `highlight_long_lines`, `format`, `server`.

#![allow(clippy::unwrap_used)]

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;

use sg_service_frontend::git_comparison_graphql::{
    CodeHighlighterSource, HighlightError, HighlightRequest, HighlightSource,
};
use sg_service_frontend::markdown_graphql::SyntectCodeHighlighter;

#[derive(Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    content: String,
    #[serde(default)]
    content_hex: String,
    filepath: String,
    start_line: Option<i32>,
    end_line: Option<i32>,
    #[serde(default)]
    keep_final_newline: bool,
    #[serde(default)]
    disable_timeout: bool,
    #[serde(default)]
    highlight_long_lines: bool,
    format: String,
    #[serde(default)]
    #[allow(dead_code)]
    server: String,
}

#[derive(Deserialize)]
struct Golden {
    request: CapturedJson,
    response: GoldenResponse,
    output: GoldenOutput,
}

#[derive(Deserialize, Default)]
#[allow(dead_code)]
struct CapturedJson {
    method: String,
    path: String,
    content_type: String,
    stabilize_timeout: String,
    body: String,
}

#[derive(Deserialize)]
struct GoldenResponse {
    status: u16,
    body: String,
}

#[derive(Deserialize, Default)]
struct GoldenOutput {
    #[allow(dead_code)]
    ok: bool,
    #[allow(dead_code)]
    binary: bool,
    #[allow(dead_code)]
    aborted: bool,
    #[allow(dead_code)]
    error: String,
}

struct Captured {
    method: String,
    path: String,
    content_type: String,
    stabilize_timeout: String,
    body: String,
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

#[tokio::test]
async fn highlight_production_diff() {
    let corpus = match std::env::var("M936D_HL_CORPUS") {
        Ok(path) => path,
        Err(_) => return,
    };
    let gold = std::env::var("M936D_HL_GOLD").expect("M936D_HL_GOLD");
    let out = std::env::var("M936D_HL_OUT").expect("M936D_HL_OUT");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus).expect("corpus")).expect("corpus json");

    for case in &cases {
        let golden: Golden = serde_json::from_slice(
            &std::fs::read(std::path::Path::new(&gold).join(format!("{}.json", case.name)))
                .expect("golden"),
        )
        .expect("golden json");

        let content = if !case.content_hex.is_empty() {
            decode_hex(&case.content_hex)
        } else {
            case.content.clone().into_bytes()
        };

        // Only cases the Go oracle sent over the wire get a replay server.
        let networked = !golden.request.method.is_empty();
        let mut server = None;
        let mut rx = None;
        let base_url = if networked {
            let response_body = golden.response.body.clone();
            let status = golden.response.status;
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
            let address = listener.local_addr().expect("addr");
            let (tx, receiver) = std::sync::mpsc::channel();
            rx = Some(receiver);
            server = Some(std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().expect("accept");
                stream.set_read_timeout(Some(Duration::from_secs(10))).ok();
                let captured = read_request(&mut stream);
                let payload = format!(
                    "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    status,
                    response_body.len(),
                    response_body
                );
                stream.write_all(payload.as_bytes()).expect("write");
                stream.flush().ok();
                tx.send(captured).expect("send capture");
            }));
            format!("http://{address}")
        } else {
            "http://127.0.0.1:1".to_string()
        };

        let adapter = CodeHighlighterSource::new(Arc::new(
            SyntectCodeHighlighter::with_base_url(&base_url),
        ));
        let request = HighlightRequest {
            content,
            filepath: case.filepath.clone(),
            disable_timeout: case.disable_timeout,
            highlight_long_lines: case.highlight_long_lines,
            format: case.format.clone(),
            start_line: case.start_line,
            end_line: case.end_line,
            keep_final_newline: case.keep_final_newline,
        };
        let result = adapter.highlight(&request).await;

        let captured = match rx {
            Some(receiver) => {
                let captured = receiver
                    .recv_timeout(Duration::from_secs(10))
                    .expect("request captured");
                server.take().unwrap().join().expect("server");
                captured
            }
            None => Captured {
                method: String::new(),
                path: String::new(),
                content_type: String::new(),
                stabilize_timeout: String::new(),
                body: String::new(),
            },
        };

        let (ok, binary, aborted, lines, error) = match result {
            Ok(highlighted) => (true, false, highlighted.aborted, Some(highlighted.lines), String::new()),
            Err(HighlightError::Binary) => (
                false,
                true,
                false,
                None,
                "cannot render binary file".to_string(),
            ),
            Err(HighlightError::Message(message)) => (false, false, false, None, message),
        };

        let lines_json = match lines {
            Some(lines) => format!(
                "[{}]",
                lines
                    .iter()
                    .map(|line| go_json_string(line))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            None => "null".to_string(),
        };
        let json = format!(
            "{{\"request\":{{\"method\":{},\"path\":{},\"content_type\":{},\"stabilize_timeout\":{},\"body\":{}}},\"response\":{{\"status\":{},\"body\":{}}},\"output\":{{\"ok\":{},\"binary\":{},\"aborted\":{},\"lines\":{},\"error\":{}}}}}",
            go_json_string(&captured.method),
            go_json_string(&captured.path),
            go_json_string(&captured.content_type),
            go_json_string(&captured.stabilize_timeout),
            go_json_string(&captured.body),
            golden.response.status,
            go_json_string(&golden.response.body),
            ok,
            binary,
            aborted,
            lines_json,
            go_json_string(&error),
        );
        std::fs::write(
            std::path::Path::new(&out).join(format!("{}.json", case.name)),
            json,
        )
        .expect("write output");
    }
    println!("RUST {} cases", cases.len());
}

fn read_request(stream: &mut impl Read) -> Captured {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        if let Some(position) = find_header_end(&buffer) {
            break position;
        }
        let read = stream.read(&mut chunk).expect("read");
        if read == 0 {
            break buffer.len();
        }
        buffer.extend_from_slice(&chunk[..read]);
    };
    let headers = String::from_utf8_lossy(&buffer[..header_end]).into_owned();
    let mut lines = headers.lines();
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();
    let mut content_type = String::new();
    let mut stabilize_timeout = String::new();
    let mut content_length = 0usize;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            let value = value.trim();
            match name.to_ascii_lowercase().as_str() {
                "content-type" => content_type = value.to_string(),
                "x-stabilize-timeout" => stabilize_timeout = value.to_string(),
                "content-length" => content_length = value.parse().unwrap_or(0),
                _ => {}
            }
        }
    }
    let body_start = (header_end + 4).min(buffer.len());
    while buffer.len() < body_start + content_length {
        let read = stream.read(&mut chunk).expect("read body");
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
    let body = String::from_utf8_lossy(&buffer[body_start..]).into_owned();
    Captured {
        method,
        path,
        content_type,
        stabilize_timeout,
        body,
    }
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

/// Go `encoding/json` string escaping (`json.Marshal`).
fn go_json_string(input: &str) -> String {
    let mut out = String::with_capacity(input.len() + 2);
    out.push('"');
    for ch in input.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
