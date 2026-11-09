//! Subset of Go's `net/smtp` client used by `txemail.Send`.
//!
//! The Go baseline dials the server itself and drives it through
//! `smtp.NewClient`, `Hello`, `Extension("STARTTLS")`, `StartTLS`, `Auth`,
//! `Mail`, `Rcpt`, `Data`, `Quit`. This module ports those methods with the
//! same observable protocol behaviour:
//!
//! * `EHLO` first, `HELO` fallback when `EHLO` fails; `MAIL FROM` gains
//!   `BODY=8BITMIME` / `SMTPUTF8` when the server advertised them.
//! * `STARTTLS` re-issues `EHLO` after the TLS handshake.
//! * `AUTH PLAIN` and `AUTH CRAM-MD5`, including the `334` challenge loop.
//! * `DATA` uses Go's `textproto.dotWriter` state machine (leading-dot
//!   stuffing, `\n` -> `\r\n`, final `.\r\n`) and then reads the `250`.
//!
//! Error strings that the baseline forms itself (invalid response lines,
//! status mismatches, `Hello` misuse) are reproduced exactly; underlying I/O
//! and TLS errors are Rust's, which necessarily differ from Go's
//! `net.OpError` / `x509` text (see `docs/modules/06_worker.md` §110).
//!
//! Not ported: `Dial`, `Close` semantics beyond dropping the socket, `Reset`,
//! `Noop`, the generic `Auth` extension interface, and `textproto`'s
//! pipelining (the baseline never pipelines SMTP commands).

use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConnection, DigitallySignedStruct, SignatureScheme, StreamOwned};

/// Errors produced by the SMTP client.
#[derive(Debug)]
pub enum Error {
    /// Socket I/O, including TLS handshake failures surfaced as `io::Error`.
    Io(io::Error),
    /// `textproto.ProtocolError`: the text is the error itself in Go.
    Protocol(String),
    /// `textproto.Error`: `%03d %s`.
    Status { code: u16, message: String },
    /// TLS setup failed before the handshake (configuration or server name).
    Tls(String),
    /// Errors the baseline creates with `errors.New` / `fmt.Errorf`.
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(error) => write!(f, "{error}"),
            Error::Protocol(message) => f.write_str(message),
            Error::Status { code, message } => write!(f, "{code:03} {message}"),
            Error::Tls(message) => f.write_str(message),
            Error::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::Io(error)
    }
}

/// The connection, before or after `STARTTLS`. `Closed` only exists while the
/// socket is being moved into a TLS stream (see [`Client::start_tls`]).
enum Stream {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ClientConnection, TcpStream>>),
    Closed,
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(stream) => stream.read(buf),
            Stream::Tls(stream) => stream.read(buf),
            Stream::Closed => Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "smtp: connection is closed",
            )),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(stream) => stream.write(buf),
            Stream::Tls(stream) => stream.write(buf),
            Stream::Closed => Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "smtp: connection is closed",
            )),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Plain(stream) => stream.flush(),
            Stream::Tls(stream) => stream.flush(),
            Stream::Closed => Ok(()),
        }
    }
}

/// `net/smtp.Client`.
pub struct Client {
    stream: Stream,
    read_buf: Vec<u8>,
    server_name: String,
    tls: bool,
    /// `Client.ext`: supported extensions from the last successful `EHLO`.
    ext: Option<BTreeMap<String, String>>,
    /// `Client.auth`: mechanisms advertised in the `AUTH` extension.
    auth_mechanisms: Vec<String>,
    local_name: String,
    did_hello: bool,
    /// `Client.helloError`, rendered to text (the Go value is returned again on
    /// repeated `hello()` calls).
    hello_error: Option<String>,
}

impl Client {
    /// `smtp.NewClient`: takes over an established connection, reads the `220`
    /// greeting and remembers `server_name` for TLS and `PLAIN` checks.
    pub fn new(stream: TcpStream, server_name: &str) -> Result<Self, Error> {
        let mut client = Client {
            stream: Stream::Plain(stream),
            read_buf: Vec::new(),
            server_name: server_name.to_owned(),
            tls: false,
            ext: None,
            auth_mechanisms: Vec::new(),
            local_name: "localhost".to_owned(),
            did_hello: false,
            hello_error: None,
        };
        client.read_response(220)?;
        Ok(client)
    }

    /// Drops the socket, like `Client.Close` (which only calls
    /// `textproto.Conn.Close`).
    pub fn close(&mut self) {
        self.stream = Stream::Closed;
    }

    /// `Client.Hello`: must be called before any other method.
    pub fn hello(&mut self, local_name: &str) -> Result<(), Error> {
        validate_line(local_name)?;
        if self.did_hello {
            return Err(Error::Other(
                "smtp: Hello called after other methods".to_owned(),
            ));
        }
        self.local_name = local_name.to_owned();
        self.ensure_hello()
    }

    /// `Client.hello`.
    fn ensure_hello(&mut self) -> Result<(), Error> {
        if !self.did_hello {
            self.did_hello = true;
            if self.ehlo().is_err() {
                self.hello_error = match self.helo() {
                    Ok(()) => None,
                    Err(error) => Some(error.to_string()),
                };
            }
        }
        match &self.hello_error {
            Some(message) => Err(Error::Other(message.clone())),
            None => Ok(()),
        }
    }

    /// `Client.ehlo`.
    fn ehlo(&mut self) -> Result<(), Error> {
        let (_, message) = self.cmd(250, &format!("EHLO {}", self.local_name))?;
        let mut ext = BTreeMap::new();
        let lines: Vec<&str> = message.split('\n').collect();
        // The first line is the server's greeting ("<domain> Hello ...").
        if lines.len() > 1 {
            for line in &lines[1..] {
                let (key, value) = match line.split_once(' ') {
                    Some((key, value)) => (key, value),
                    None => (*line, ""),
                };
                ext.insert(key.to_owned(), value.to_owned());
            }
        }
        if let Some(mechanisms) = ext.get("AUTH") {
            self.auth_mechanisms = mechanisms.split(' ').map(str::to_owned).collect();
        }
        self.ext = Some(ext);
        Ok(())
    }

    /// `Client.helo`.
    fn helo(&mut self) -> Result<(), Error> {
        self.ext = None;
        self.cmd(250, &format!("HELO {}", self.local_name))?;
        Ok(())
    }

    /// `Client.Extension`.
    pub fn extension(&mut self, extension: &str) -> (bool, String) {
        if self.ensure_hello().is_err() {
            return (false, String::new());
        }
        let Some(ext) = &self.ext else {
            return (false, String::new());
        };
        match ext.get(&extension.to_ascii_uppercase()) {
            Some(value) => (true, value.clone()),
            None => (false, String::new()),
        }
    }

    /// `Client.StartTLS`: issues `STARTTLS`, upgrades the socket and repeats
    /// `EHLO`.
    pub fn start_tls(&mut self, no_verify_tls: bool) -> Result<(), Error> {
        self.ensure_hello()?;
        self.cmd(220, "STARTTLS")?;

        let config = client_tls_config(no_verify_tls)?;
        let server_name = ServerName::try_from(self.server_name.clone())
            .map_err(|error| Error::Tls(error.to_string()))?;
        let connection = ClientConnection::new(Arc::new(config), server_name)
            .map_err(|error| Error::Tls(error.to_string()))?;

        let stream = std::mem::replace(&mut self.stream, Stream::Closed);
        let Stream::Plain(tcp) = stream else {
            return Err(Error::Tls(
                "smtp: STARTTLS on an already encrypted connection".to_owned(),
            ));
        };
        self.stream = Stream::Tls(Box::new(StreamOwned::new(connection, tcp)));
        self.tls = true;
        self.ehlo()?;
        Ok(())
    }

    /// `Client.Auth`.
    pub fn auth(&mut self, auth: &Auth) -> Result<(), Error> {
        self.ensure_hello()?;

        let name = self.server_name.clone();
        let tls = self.tls;
        let mechanisms = self.auth_mechanisms.clone();
        let started = {
            let info = ServerInfo {
                name: &name,
                tls,
                auth: &mechanisms,
            };
            auth.start(&info)
        };
        let (mechanism, mut response) = match started {
            Ok(started) => started,
            Err(error) => {
                let _ = self.quit();
                return Err(error);
            }
        };

        let mut response64 = BASE64.encode(&response);
        let command = format!("AUTH {mechanism} {response64}");
        let (mut code, mut message64) = self.cmd(0, command.trim())?;
        loop {
            let mut failure = None;
            let message = match code {
                334 => match BASE64.decode(&message64) {
                    Ok(decoded) => decoded,
                    Err(error) => {
                        // Go returns base64.CorruptInputError here.
                        failure = Some(Error::Other(error.to_string()));
                        Vec::new()
                    }
                },
                235 => message64.clone().into_bytes(),
                _ => {
                    // The last message is not base64 because it is not a
                    // challenge.
                    failure = Some(Error::Status {
                        code,
                        message: message64.clone(),
                    });
                    Vec::new()
                }
            };
            if failure.is_none() {
                match auth.next(&message, code == 334) {
                    Ok(Some(next)) => response = next,
                    Ok(None) => break,
                    Err(error) => failure = Some(error),
                }
            }
            if let Some(error) = failure {
                // Abort the AUTH like the baseline: `cmd(501, "*")` then quit.
                let _ = self.cmd(501, "*");
                let _ = self.quit();
                return Err(error);
            }
            response64 = BASE64.encode(&response);
            let (next_code, next_message) = self.cmd(0, &response64)?;
            code = next_code;
            message64 = next_message;
        }
        Ok(())
    }

    /// `Client.Mail`.
    pub fn mail(&mut self, from: &str) -> Result<(), Error> {
        validate_line(from)?;
        self.ensure_hello()?;
        let mut command = format!("MAIL FROM:<{from}>");
        if let Some(ext) = &self.ext {
            if ext.contains_key("8BITMIME") {
                command.push_str(" BODY=8BITMIME");
            }
            if ext.contains_key("SMTPUTF8") {
                command.push_str(" SMTPUTF8");
            }
        }
        self.cmd(250, &command)?;
        Ok(())
    }

    /// `Client.Rcpt`.
    pub fn rcpt(&mut self, to: &str) -> Result<(), Error> {
        validate_line(to)?;
        self.cmd(25, &format!("RCPT TO:<{to}>"))?;
        Ok(())
    }

    /// `Client.Data`: returns the dot-encoding writer. Call
    /// [`DataWriter::finish`] to write the final `.` and read the `250`.
    pub fn data(&mut self) -> Result<DataWriter<'_>, Error> {
        self.cmd(354, "DATA")?;
        Ok(DataWriter {
            client: self,
            state: BEGIN,
        })
    }

    /// `Client.Quit`.
    pub fn quit(&mut self) -> Result<(), Error> {
        self.ensure_hello()?;
        self.cmd(221, "QUIT")?;
        self.close();
        Ok(())
    }

    /// `Client.cmd`: write one command line, then read its response.
    fn cmd(&mut self, expect_code: u16, command: &str) -> Result<(u16, String), Error> {
        let mut line = command.as_bytes().to_vec();
        line.extend_from_slice(b"\r\n");
        self.stream.write_all(&line)?;
        self.stream.flush()?;
        self.read_response(expect_code)
    }

    /// `textproto.Reader.ReadResponse`.
    fn read_response(&mut self, expect_code: u16) -> Result<(u16, String), Error> {
        let first = self.read_code_line(expect_code)?;
        let code = first.code;
        let multiple = first.continued;
        let mut continued = multiple;
        let mut message = first.message.clone();
        while continued {
            let line = self.read_line()?;
            match parse_code_line(&line, 0) {
                Ok(parsed) if parsed.code == code => {
                    message.push('\n');
                    message.push_str(&parsed.message);
                    continued = parsed.continued;
                }
                _ => {
                    // Go keeps the raw line when the code does not match (or
                    // does not parse) and keeps reading until a matching one.
                    message.push('\n');
                    message.push_str(&line);
                    continued = true;
                }
            }
        }
        if first.expect_mismatch {
            if multiple && !message.is_empty() {
                // Replace the one-line message with the whole response.
                return Err(Error::Status { code, message });
            }
            return Err(Error::Status {
                code,
                message: first.message,
            });
        }
        Ok((code, message))
    }

    /// `textproto.Reader.readCodeLine`: one response line plus the `expectCode`
    /// check, without the multi-line loop.
    fn read_code_line(&mut self, expect_code: u16) -> Result<CodeLine, Error> {
        let line = self.read_line()?;
        parse_code_line(&line, expect_code)
    }

    /// `bufio.Reader.ReadLine`: returns the line without its `\r\n` / `\n`.
    fn read_line(&mut self) -> Result<String, Error> {
        loop {
            if let Some(position) = self.read_buf.iter().position(|&byte| byte == b'\n') {
                let mut line: Vec<u8> = self.read_buf.drain(..=position).collect();
                line.pop(); // '\n'
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                return Ok(String::from_utf8_lossy(&line).into_owned());
            }
            let mut chunk = [0u8; 4096];
            let read = self.stream.read(&mut chunk)?;
            if read == 0 {
                return Err(Error::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "EOF",
                )));
            }
            self.read_buf.extend_from_slice(&chunk[..read]);
        }
    }
}

#[derive(Debug)]
struct CodeLine {
    code: u16,
    continued: bool,
    message: String,
    expect_mismatch: bool,
}

/// `textproto.parseCodeLine`.
fn parse_code_line(line: &str, expect_code: u16) -> Result<CodeLine, Error> {
    let bytes = line.as_bytes();
    if bytes.len() < 4 || (bytes[3] != b' ' && bytes[3] != b'-') {
        return Err(Error::Protocol(format!("short response: {line}")));
    }
    let continued = bytes[3] == b'-';
    let code = match line[..3].parse::<u16>() {
        Ok(code) if code >= 100 => code,
        _ => return Err(Error::Protocol(format!("invalid response code: {line}"))),
    };
    let message = line[4..].to_owned();
    let expect_mismatch = !expect_code_matches(expect_code, code);
    Ok(CodeLine {
        code,
        continued,
        message,
        expect_mismatch,
    })
}

/// `textproto.parseCodeLine`'s `expectCode` comparison.
fn expect_code_matches(expect_code: u16, code: u16) -> bool {
    if (1..10).contains(&expect_code) {
        code / 100 == expect_code
    } else if (10..100).contains(&expect_code) {
        code / 10 == expect_code
    } else if (100..1000).contains(&expect_code) {
        code == expect_code
    } else {
        true
    }
}

/// `smtp.validateLine`.
fn validate_line(line: &str) -> Result<(), Error> {
    if line.contains(['\r', '\n']) {
        Err(Error::Other(
            "smtp: A line must not contain CR or LF".to_owned(),
        ))
    } else {
        Ok(())
    }
}

const BEGIN: u8 = 0;
const BEGIN_LINE: u8 = 1;
const CR: u8 = 2;
const DATA: u8 = 3;

/// `textproto.dotWriter`, including its final-line state table.
pub struct DataWriter<'a> {
    client: &'a mut Client,
    state: u8,
}

impl DataWriter<'_> {
    /// Writes the terminating `.` line (after closing a partial line exactly
    /// like Go's `dotWriter.Close`) and reads the final `250`.
    pub fn finish(self) -> Result<(), Error> {
        let client = self.client;
        match self.state {
            CR => client.stream.write_all(b"\n.\r\n")?,
            BEGIN_LINE => client.stream.write_all(b".\r\n")?,
            // BEGIN (no data at all) and DATA both take Go's `default` branch.
            _ => client.stream.write_all(b"\r\n.\r\n")?,
        }
        client.stream.flush()?;
        client.read_response(250)?;
        Ok(())
    }

    /// The `wstateData` branch of `dotWriter.Write`.
    fn data_byte(&mut self, byte: u8) -> io::Result<()> {
        if byte == b'\r' {
            self.state = CR;
        }
        if byte == b'\n' {
            self.client.stream.write_all(b"\r")?;
            self.state = BEGIN_LINE;
        }
        Ok(())
    }
}

impl Write for DataWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut written = 0;
        while written < buf.len() {
            let byte = buf[written];
            match self.state {
                BEGIN | BEGIN_LINE => {
                    self.state = DATA;
                    if byte == b'.' {
                        self.client.stream.write_all(b".")?;
                    }
                    self.data_byte(byte)?;
                }
                DATA => self.data_byte(byte)?,
                CR => {
                    self.state = DATA;
                    if byte == b'\n' {
                        self.state = BEGIN_LINE;
                    }
                }
                _ => unreachable!("invalid dot writer state"),
            }
            self.client.stream.write_all(&[byte])?;
            written += 1;
        }
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.client.stream.flush()
    }
}

/// `smtp.ServerInfo`.
pub struct ServerInfo<'a> {
    pub name: &'a str,
    pub tls: bool,
    pub auth: &'a [String],
}

/// The two authentication mechanisms `txemail.Send` can select.
pub enum Auth {
    /// `smtp.PlainAuth`.
    Plain {
        identity: String,
        username: String,
        password: String,
        host: String,
    },
    /// `smtp.CRAMMD5Auth`.
    CramMd5 { username: String, secret: String },
}

impl Auth {
    /// `smtp.PlainAuth`.
    pub fn plain(identity: &str, username: &str, password: &str, host: &str) -> Self {
        Auth::Plain {
            identity: identity.to_owned(),
            username: username.to_owned(),
            password: password.to_owned(),
            host: host.to_owned(),
        }
    }

    /// `smtp.CRAMMD5Auth`.
    pub fn cram_md5(username: &str, secret: &str) -> Self {
        Auth::CramMd5 {
            username: username.to_owned(),
            secret: secret.to_owned(),
        }
    }

    /// `Auth.Start`.
    fn start(&self, server: &ServerInfo<'_>) -> Result<(String, Vec<u8>), Error> {
        match self {
            Auth::Plain {
                identity,
                username,
                password,
                host,
            } => {
                // Must have TLS, or else a localhost server. Without TLS the
                // extension list cannot be trusted, so it is not consulted.
                if !server.tls && !is_localhost(server.name) {
                    return Err(Error::Other("unencrypted connection".to_owned()));
                }
                if server.name != host {
                    return Err(Error::Other("wrong host name".to_owned()));
                }
                let response = format!("{identity}\u{0}{username}\u{0}{password}");
                Ok(("PLAIN".to_owned(), response.into_bytes()))
            }
            Auth::CramMd5 { .. } => Ok(("CRAM-MD5".to_owned(), Vec::new())),
        }
    }

    /// `Auth.Next`.
    fn next(&self, from_server: &[u8], more: bool) -> Result<Option<Vec<u8>>, Error> {
        match self {
            Auth::Plain { .. } => {
                if more {
                    // Everything was already sent.
                    return Err(Error::Other("unexpected server challenge".to_owned()));
                }
                Ok(None)
            }
            Auth::CramMd5 { username, secret } => {
                if more {
                    let digest = hmac_md5(secret.as_bytes(), from_server);
                    Ok(Some(
                        format!("{username} {}", hex_lower(&digest)).into_bytes(),
                    ))
                } else {
                    Ok(None)
                }
            }
        }
    }
}

/// `smtp.isLocalhost`.
fn is_localhost(name: &str) -> bool {
    matches!(name, "localhost" | "127.0.0.1" | "::1")
}

/// `hmac.New(md5.New, key)` over `data`.
fn hmac_md5(key: &[u8], data: &[u8]) -> [u8; 16] {
    const BLOCK: usize = 64;
    let mut padded = [0u8; BLOCK];
    if key.len() > BLOCK {
        padded[..16].copy_from_slice(&md5::compute(key).0);
    } else {
        padded[..key.len()].copy_from_slice(key);
    }
    let mut inner = [0x36u8; BLOCK];
    let mut outer = [0x5cu8; BLOCK];
    for index in 0..BLOCK {
        inner[index] ^= padded[index];
        outer[index] ^= padded[index];
    }
    let mut inner_input = Vec::with_capacity(BLOCK + data.len());
    inner_input.extend_from_slice(&inner);
    inner_input.extend_from_slice(data);
    let inner_digest = md5::compute(&inner_input);
    let mut outer_input = Vec::with_capacity(BLOCK + 16);
    outer_input.extend_from_slice(&outer);
    outer_input.extend_from_slice(&inner_digest.0);
    md5::compute(&outer_input).0
}

/// `fmt.Sprintf("%x", sum)`.
fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// Builds the client TLS configuration. Go's `tls.Config` always uses the
/// platform roots (unless `InsecureSkipVerify`).
fn client_tls_config(no_verify_tls: bool) -> Result<rustls::ClientConfig, Error> {
    let builder = rustls::ClientConfig::builder();
    if no_verify_tls {
        Ok(builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerification))
            .with_no_client_auth())
    } else {
        let mut roots = rustls::RootCertStore::empty();
        roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
        if roots.is_empty() {
            return Err(Error::Tls(
                "smtp: no system root certificates available".to_owned(),
            ));
        }
        Ok(builder.with_root_certificates(roots).with_no_client_auth())
    }
}

/// `tls.Config{InsecureSkipVerify: true}`: accept the chain and every
/// handshake signature, but still advertise the schemes a real verifier would
/// support so the server picks a usable cipher suite.
#[derive(Debug)]
struct NoVerification;

impl ServerCertVerifier for NoVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::ED25519,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_md5_matches_rfc_2202_case_1() {
        let key = [0x0bu8; 16];
        assert_eq!(
            hex_lower(&hmac_md5(&key, b"Hi There")),
            "9294727a3638bb1c13f48ef8158bfc9d"
        );
    }

    #[test]
    fn parses_code_lines_like_textproto() {
        let parsed = parse_code_line("250 OK", 250).expect("parse");
        assert_eq!(parsed.code, 250);
        assert!(!parsed.continued);
        assert_eq!(parsed.message, "OK");
        assert!(!parsed.expect_mismatch);

        let parsed = parse_code_line("250-OK", 25).expect("parse");
        assert!(parsed.continued);
        assert!(!parsed.expect_mismatch);

        let error = parse_code_line("25 OK", 250)
            .expect_err("protocol error")
            .to_string();
        assert_eq!(error, "short response: 25 OK");

        // A status mismatch is reported by `read_response`, not by the line
        // parser (which still needs the code/message to continue multi-line
        // responses).
        let parsed = parse_code_line("250 OK", 550).expect("parse");
        assert!(parsed.expect_mismatch);
    }

    #[test]
    fn plain_auth_requires_tls_or_localhost() {
        let server = ServerInfo {
            name: "mail.example.test",
            tls: false,
            auth: &[],
        };
        let auth = Auth::plain("", "user", "pass", "mail.example.test");
        assert_eq!(
            auth.start(&server).expect_err("unencrypted").to_string(),
            "unencrypted connection"
        );

        let server = ServerInfo {
            name: "127.0.0.1",
            tls: false,
            auth: &[],
        };
        let auth = Auth::plain("", "user", "pass", "127.0.0.1");
        let (mechanism, response) = auth.start(&server).expect("start");
        assert_eq!(mechanism, "PLAIN");
        assert_eq!(response, b"\0user\0pass");
    }

    #[test]
    fn cram_md5_answers_the_challenge() {
        // RFC 2195 §2 worked example.
        let auth = Auth::cram_md5("tim", "tanstaaftanstaaf");
        let response = auth
            .next(b"<1896.697170952@postoffice.reston.mci.net>", true)
            .expect("next")
            .expect("response");
        assert_eq!(
            String::from_utf8(response).expect("utf8"),
            "tim b913a602c7eda7a495b4e6e7334d3890"
        );
    }
}
