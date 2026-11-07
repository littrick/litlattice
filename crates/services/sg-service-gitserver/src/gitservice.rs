//! Baseline `lib/gitservice`: the smart Git HTTP transfer protocol served
//! under `/git/`, so other services can clone from (and fetch against) a
//! gitserver replica directly.
//!
//! Only `git-upload-pack` is supported, i.e. clones and fetches. The request
//! body may be gzip-compressed (`Content-Encoding: gzip`); git's stdout is
//! streamed to the client unbuffered, like the baseline's `cmd.Stdout = w`.

use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use flate2::{Decompress, FlushDecompress, Status as GzipStatus};
use http::{header, HeaderValue, Request, Response, StatusCode};
use http_body_util::{BodyExt, StreamBody};
use hyper::body::{Frame, Incoming};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tokio_util::task::TaskTracker;

use crate::repo::repo_dir;

/// Baseline `uploadPackArgs`.
const UPLOAD_PACK_ARGS: &[&str] = &[
    "-c",
    "uploadpack.allowFilter=true",
    "-c",
    "uploadpack.allowAnySHA1InWant=true",
    "-c",
    "pack.windowMemory=100m",
    "upload-pack",
    "--stateless-rpc",
    "--strict",
];

const ADVERTISEMENT_CONTENT_TYPE: &str = "application/x-git-upload-pack-advertisement";
const RESULT_CONTENT_TYPE: &str = "application/x-git-upload-pack-result";

/// State needed to serve `/git/`.
#[derive(Clone)]
pub struct GitService {
    pub repos_dir: Arc<std::path::PathBuf>,
    pub tasks: TaskTracker,
}

impl GitService {
    /// Baseline `Handler.ServeHTTP`. `path` has already had the `/git` prefix
    /// stripped (`http.StripPrefix`).
    pub async fn handle(
        &self,
        req: Request<Incoming>,
        path: &str,
    ) -> Response<tonic::body::BoxBody> {
        // Only clones and fetches: the `service` query parameter selects the
        // upload-pack service.
        if let Some(service) = query_value(req.uri().query().unwrap_or_default(), "service") {
            if !service.is_empty() && service != "git-upload-pack" {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "only support service git-upload-pack",
                );
            }
        }

        let mut repo = "";
        let mut svc = "";
        for suffix in ["/info/refs", "/git-upload-pack"] {
            if let Some(stripped) = path.strip_suffix(suffix) {
                svc = suffix;
                repo = stripped.strip_prefix('/').unwrap_or(stripped);
                break;
            }
        }

        let Some(dir) = repo_dir(&self.repos_dir, repo) else {
            return error_response(StatusCode::NOT_FOUND, "repository not found");
        };
        match std::fs::metadata(&dir) {
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return error_response(StatusCode::NOT_FOUND, "repository not found");
            }
            Err(err) => {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("failed to stat repo: {err}"),
                );
            }
            Ok(_) => {}
        }

        let (content_type, advertisement) = match svc {
            "/info/refs" => (
                ADVERTISEMENT_CONTENT_TYPE,
                // `packetWrite("# service=git-upload-pack\n")` + flush packet.
                Some(Bytes::from_static(b"001e# service=git-upload-pack\n0000")),
            ),
            "/git-upload-pack" => (RESULT_CONTENT_TYPE, None),
            _ => {
                return error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!(
                        "unexpected subpath (want /info/refs or /git-upload-pack): {}",
                        go_quote(svc.as_bytes())
                    ),
                );
            }
        };

        let mut args: Vec<String> = UPLOAD_PACK_ARGS
            .iter()
            .map(|arg| (*arg).to_owned())
            .collect();
        if svc == "/info/refs" {
            args.push("--advertise-refs".to_owned());
        }
        args.push(dir.to_string_lossy().into_owned());

        let (tx, rx) = mpsc::channel::<Frame<Bytes>>(4);
        let gzip = req
            .headers()
            .get(header::CONTENT_ENCODING)
            .is_some_and(|value| value == "gzip");
        let git_protocol = req
            .headers()
            .get("git-protocol")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let mut body = req.into_body();
        // `gzip.NewReader` reads and validates the header before the git
        // command starts, so a malformed payload is a 400 and never reaches
        // git. Keep the consumed frames for the writer.
        let mut buffered: Vec<Bytes> = Vec::new();
        let mut gzip_header_len = 0usize;
        if gzip {
            let mut header = Vec::new();
            loop {
                match body.frame().await {
                    Some(Ok(frame)) => {
                        if let Ok(data) = frame.into_data() {
                            header.extend_from_slice(&data);
                            buffered.push(data);
                        }
                    }
                    Some(Err(err)) => {
                        return error_response(
                            StatusCode::BAD_REQUEST,
                            &format!("malformed payload: {err}"),
                        );
                    }
                    None => {
                        return error_response(
                            StatusCode::BAD_REQUEST,
                            "malformed payload: unexpected EOF",
                        );
                    }
                }
                match parse_gzip_header_len(&header) {
                    Ok(Some(len)) => {
                        gzip_header_len = len;
                        break;
                    }
                    Ok(None) => {}
                    Err(message) => {
                        return error_response(
                            StatusCode::BAD_REQUEST,
                            &format!("malformed payload: {message}"),
                        );
                    }
                }
            }
        }
        let rate_limit = max_egress_bytes_per_second();
        let args = Arc::new(args);
        self.tasks.spawn(async move {
            if let Some(advertisement) = advertisement {
                if tx.send(Frame::data(advertisement)).await.is_err() {
                    return;
                }
            }
            let mut command = Command::new("git");
            command
                .args(args.iter())
                .stdin(std::process::Stdio::piped());
            command.stdout(std::process::Stdio::piped());
            command.stderr(std::process::Stdio::piped());
            if let Some(protocol) = git_protocol {
                command.env("GIT_PROTOCOL", protocol);
            }
            let child = match command.spawn() {
                Ok(child) => child,
                Err(err) => {
                    let _ = tx
                        .send(Frame::data(Bytes::from(format!(
                            "\nerror running git service command args={}: {err}\n",
                            go_quote_slice(&args)
                        ))))
                        .await;
                    return;
                }
            };
            serve_child(
                child,
                buffered,
                body,
                gzip,
                gzip_header_len,
                tx,
                rate_limit,
                args,
            )
            .await;
        });

        let stream = ReceiverStream::new(rx).map(Ok::<_, io::Error>);
        let mut response = Response::new(tonic::body::boxed(StreamBody::new(stream)));
        *response.status_mut() = StatusCode::OK;
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
        response
    }
}

/// Pump the request body into git's stdin while streaming stdout back.
#[allow(clippy::too_many_arguments)]
async fn serve_child(
    mut child: Child,
    buffered: Vec<Bytes>,
    body: Incoming,
    gzip: bool,
    gzip_header_len: usize,
    tx: mpsc::Sender<Frame<Bytes>>,
    rate_limit: Option<i64>,
    args: Arc<Vec<String>>,
) {
    let stdin = child.stdin.take();
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    let writer = write_request_body(buffered, body, gzip, gzip_header_len, stdin);
    let reader = stream_stdout(stdout, &tx, rate_limit);
    let errors = read_stderr(stderr);
    let (write_result, read_result, stderr) = tokio::join!(writer, reader, errors);

    // The baseline ignores the exit status when streaming succeeded; it only
    // appends the wrapped `cmd.Run` error to the already-written response.
    let status = child.wait().await;
    if write_result.is_err() || read_result.is_err() {
        let _ = child.start_kill();
        return;
    }
    match status {
        Ok(status) if status.success() => {}
        Ok(status) => {
            tracing::error!(
                args = %go_quote_slice(&args),
                %status,
                stderr = %String::from_utf8_lossy(&stderr),
                "git-service error"
            );
            let text = format!(
                "\nerror running git service command args={}: {status}\n",
                go_quote_slice(&args),
            );
            let _ = tx.send(Frame::data(Bytes::from(text))).await;
        }
        Err(err) => {
            let _ = tx
                .send(Frame::data(Bytes::from(format!(
                    "\nerror running git service command args={}: {err}\n",
                    go_quote_slice(&args)
                ))))
                .await;
        }
    }
}

async fn write_request_body(
    buffered: Vec<Bytes>,
    mut body: Incoming,
    gzip: bool,
    gzip_header_len: usize,
    stdin: Option<ChildStdin>,
) -> io::Result<()> {
    let Some(mut stdin) = stdin else {
        // Nothing to write, but drain the body so the connection can be
        // reused, like the baseline closing the request body.
        while let Some(frame) = body.frame().await {
            frame.map_err(io::Error::other)?;
        }
        return Ok(());
    };
    // `Decompress::new(false)`: the gzip payload is raw DEFLATE, not a zlib
    // stream, so the gzip header is skipped and the trailer ignored.
    let mut decompressor = Decompress::new(false);
    let mut scratch = Vec::with_capacity(64 * 1024);
    let mut skip = gzip_header_len;
    let mut finished = false;
    for data in &buffered {
        if gzip {
            write_decompressed(
                &mut decompressor,
                &mut scratch,
                data,
                &mut skip,
                &mut finished,
                &mut stdin,
            )
            .await?;
        } else {
            stdin.write_all(data).await?;
        }
    }
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(io::Error::other)?;
        let Some(data) = frame.data_ref() else {
            continue;
        };
        if gzip {
            write_decompressed(
                &mut decompressor,
                &mut scratch,
                data,
                &mut skip,
                &mut finished,
                &mut stdin,
            )
            .await?;
        } else {
            stdin.write_all(data).await?;
        }
    }
    stdin.shutdown().await
}

/// Feed one compressed chunk through the incremental decompressor.
async fn write_decompressed(
    decompressor: &mut Decompress,
    scratch: &mut Vec<u8>,
    mut input: &[u8],
    skip: &mut usize,
    finished: &mut bool,
    stdin: &mut ChildStdin,
) -> io::Result<()> {
    if *finished {
        return Ok(());
    }
    if *skip > 0 {
        let taken = (*skip).min(input.len());
        input = &input[taken..];
        *skip -= taken;
        if input.is_empty() {
            return Ok(());
        }
    }
    while !input.is_empty() {
        let before_in = decompressor.total_in();
        scratch.clear();
        scratch.reserve(64 * 1024);
        let status = decompressor
            .decompress_vec(input, scratch, FlushDecompress::None)
            .map_err(io::Error::other)?;
        let consumed = (decompressor.total_in() - before_in) as usize;
        input = &input[consumed..];
        if !scratch.is_empty() {
            stdin.write_all(scratch).await?;
        }
        if consumed == 0 && scratch.is_empty() {
            break;
        }
        if status == GzipStatus::StreamEnd {
            *finished = true;
            break;
        }
    }
    Ok(())
}

/// Parse the gzip header, including optional fields. `Ok(Some(len))` is the
/// number of header bytes; `Ok(None)` means more input is needed. Mirrors
/// `compress/gzip`'s header checks.
fn parse_gzip_header_len(header: &[u8]) -> Result<Option<usize>, &'static str> {
    if header.len() < 10 {
        return Ok(None);
    }
    if header[0] != 0x1f || header[1] != 0x8b {
        return Err("gzip: invalid header");
    }
    if header[2] != 8 {
        return Err("gzip: unknown compression method");
    }
    let flags = header[3];
    // Bits 5-7 are reserved.
    if flags & 0xe0 != 0 {
        return Err("gzip: invalid header");
    }
    let mut index = 10;
    if flags & 0x04 != 0 {
        // FEXTRA: 2-byte length followed by that many bytes.
        if header.len() < index + 2 {
            return Ok(None);
        }
        let extra = u16::from_le_bytes([header[index], header[index + 1]]) as usize;
        index += 2 + extra;
        if header.len() < index {
            return Ok(None);
        }
    }
    for flag in [0x08, 0x10] {
        // FNAME / FCOMMENT: NUL-terminated strings.
        if flags & flag != 0 {
            match header[index..].iter().position(|byte| *byte == 0) {
                Some(position) => index += position + 1,
                None => return Ok(None),
            }
        }
    }
    if flags & 0x02 != 0 {
        // FHCRC: 2-byte header CRC16 (not verified, like a lossy decoder).
        index += 2;
        if header.len() < index {
            return Ok(None);
        }
    }
    Ok(Some(index))
}

async fn stream_stdout(
    stdout: Option<ChildStdout>,
    tx: &mpsc::Sender<Frame<Bytes>>,
    rate_limit: Option<i64>,
) -> io::Result<()> {
    let Some(mut stdout) = stdout else {
        return Ok(());
    };
    let mut limiter = RateLimiter::new(rate_limit);
    let mut buffer = vec![0u8; 32 * 1024];
    loop {
        let read = stdout.read(&mut buffer).await?;
        if read == 0 {
            return Ok(());
        }
        limiter.wait(read as u64).await;
        if tx
            .send(Frame::data(Bytes::copy_from_slice(&buffer[..read])))
            .await
            .is_err()
        {
            return Ok(());
        }
    }
}

async fn read_stderr(stderr: Option<ChildStderr>) -> Vec<u8> {
    let mut output = Vec::new();
    if let Some(mut stderr) = stderr {
        let mut buffer = vec![0u8; 4096];
        loop {
            match stderr.read(&mut buffer).await {
                Ok(0) => break,
                Ok(read) => output.extend_from_slice(&buffer[..read]),
                Err(_) => break,
            }
        }
    }
    output
}

/// Baseline `flowrate.NewWriter`: pace stdout to the configured average rate.
struct RateLimiter {
    bytes_per_second: Option<f64>,
    start: Instant,
    bytes: u64,
}

impl RateLimiter {
    fn new(bytes_per_second: Option<i64>) -> Self {
        Self {
            bytes_per_second: bytes_per_second
                .filter(|rate| *rate > 0)
                .map(|rate| rate as f64),
            start: Instant::now(),
            bytes: 0,
        }
    }

    async fn wait(&mut self, bytes: u64) {
        self.bytes += bytes;
        let Some(rate) = self.bytes_per_second else {
            return;
        };
        let target = Duration::from_secs_f64(self.bytes as f64 / rate);
        let elapsed = self.start.elapsed();
        if target > elapsed {
            tokio::time::sleep(target - elapsed).await;
        }
    }
}

/// `SRC_GIT_SERVICE_MAX_EGRESS_BYTES_PER_SECOND` (default 10 Gbps).
fn max_egress_bytes_per_second() -> Option<i64> {
    static LIMIT: std::sync::OnceLock<Option<i64>> = std::sync::OnceLock::new();
    *LIMIT.get_or_init(|| {
        match std::env::var("SRC_GIT_SERVICE_MAX_EGRESS_BYTES_PER_SECOND") {
            Ok(value) => value.trim().parse().ok().or(Some(10_000_000_000)),
            // `env.Get`'s default is 10000000000.
            Err(_) => Some(10_000_000_000),
        }
    })
}

/// Go `url.Values.Get`: the first value, form-decoded (`+` is a space).
fn query_value(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        let (name, value) = match pair.split_once('=') {
            Some((name, value)) => (name, value),
            None => (pair, ""),
        };
        if decode_form_component(name) == key {
            return Some(decode_form_component(value));
        }
    }
    None
}

fn decode_form_component(input: &str) -> String {
    let replaced = input.replace('+', " ");
    percent_decode(replaced.as_bytes())
}

fn percent_decode(input: &[u8]) -> String {
    percent_encoding::percent_decode(input)
        .decode_utf8_lossy()
        .into_owned()
}

/// Go `http.Error`: message plus newline, `text/plain; charset=utf-8` and
/// `X-Content-Type-Options: nosniff`.
fn error_response(status: StatusCode, message: &str) -> Response<tonic::body::BoxBody> {
    let body = Bytes::from(format!("{message}\n"));
    let mut response = Response::new(tonic::body::boxed(http_body_util::Full::new(body)));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response.headers_mut().insert(
        header::HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    response
}

/// `strconv.Quote` for the ASCII argv the baseline logs.
fn go_quote(bytes: &[u8]) -> String {
    let mut out = String::from("\"");
    for &byte in bytes {
        match byte {
            b'\\' => out.push_str("\\\\"),
            b'"' => out.push_str("\\\""),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            0x20..=0x7e => out.push(byte as char),
            other => out.push_str(&format!("\\x{other:02x}")),
        }
    }
    out.push('"');
    out
}

/// Go `fmt`'s `%q` for a `[]string`: `["a" "b"]`.
fn go_quote_slice(args: &[String]) -> String {
    let quoted = args
        .iter()
        .map(|arg| go_quote(arg.as_bytes()))
        .collect::<Vec<_>>()
        .join(" ");
    format!("[{quoted}]")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_the_advertisement_packet() {
        // `packetWrite("# service=git-upload-pack\n")` = 30 = 0x1e.
        assert_eq!(
            b"001e# service=git-upload-pack\n0000".len(),
            4 + "# service=git-upload-pack\n".len() + 4
        );
    }

    #[test]
    fn reads_the_first_service_query_value() {
        assert_eq!(
            query_value("service=git-upload-pack", "service").as_deref(),
            Some("git-upload-pack")
        );
        assert_eq!(
            query_value("a=b&service=git%2Dupload-pack", "service").as_deref(),
            Some("git-upload-pack")
        );
        assert_eq!(query_value("service", "service").as_deref(), Some(""));
        assert_eq!(query_value("other=1", "service"), None);
        assert_eq!(
            query_value("service=git-upload-pack+bogus", "service").as_deref(),
            Some("git-upload-pack bogus")
        );
    }

    #[test]
    fn parses_gzip_headers_like_go() {
        assert_eq!(parse_gzip_header_len(b""), Ok(None));
        assert_eq!(parse_gzip_header_len(b"\x1f\x8b"), Ok(None));
        assert_eq!(
            parse_gzip_header_len(b"not-gzip!!"),
            Err("gzip: invalid header")
        );
        assert_eq!(
            parse_gzip_header_len(b"\x1f\x8b\x07\x00\x00\x00\x00\x00\x00\x00"),
            Err("gzip: unknown compression method")
        );
        assert_eq!(
            parse_gzip_header_len(b"\x1f\x8b\x08\xe0\x00\x00\x00\x00\x00\x00"),
            Err("gzip: invalid header")
        );
        // Plain header.
        assert_eq!(
            parse_gzip_header_len(b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x00"),
            Ok(Some(10))
        );
        // FEXTRA (0x04): XLEN = 2, then two bytes.
        assert_eq!(
            parse_gzip_header_len(b"\x1f\x8b\x08\x04\x00\x00\x00\x00\x00\x00\x02\x00ab"),
            Ok(Some(14))
        );
        assert_eq!(
            parse_gzip_header_len(b"\x1f\x8b\x08\x04\x00\x00\x00\x00\x00\x00\x02\x00a"),
            Ok(None)
        );
        // FNAME (0x08) and FCOMMENT (0x10).
        assert_eq!(
            parse_gzip_header_len(b"\x1f\x8b\x08\x18\x00\x00\x00\x00\x00\x00n\x00c\x00"),
            Ok(Some(14))
        );
    }

    #[test]
    fn quotes_arguments_like_go() {
        assert_eq!(go_quote(b"upload-pack"), "\"upload-pack\"");
        assert_eq!(go_quote(b"a\"b\\c"), "\"a\\\"b\\\\c\"");
        assert_eq!(
            go_quote_slice(&["-c".to_owned(), "x=y".to_owned()]),
            "[\"-c\" \"x=y\"]"
        );
    }
}
