#![forbid(unsafe_code)]

//! `internal/extsvc/pypi/client.go`: the PyPI simple-API (PEP 503) client.
//!
//! Ports `Project`, `Version`, `Download`, `FindVersion`, `ToWheel`,
//! `ToSDist` and the multi-URL fallback (only 404 responses advance to the
//! next base URL), with the baseline's error strings.
//!
//! The HTML parser is a minimal anchor tokenizer instead of
//! `golang.org/x/net/html`: it understands start/end tags, quoted and unquoted
//! attributes, comments and the common character entities. PEP 503 pages only
//! contain plain anchor tags, so this is sufficient for the simple API.
//!
//! Rate limiting, the cached transport and observability are not ported; the
//! transport is injected as [`HttpDoer`](crate::HttpDoer).

use std::sync::Arc;

use url::Url;

use crate::gitlab::go_path_join;
use crate::reposource_packages::go_quote;
use crate::{HttpDoer, HttpRequest};

/// `pypi.Error`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PyPiError {
    pub path: String,
    pub code: u16,
    pub message: String,
}

impl PyPiError {
    pub fn not_found(&self) -> bool {
        self.code == 404
    }
}

impl std::fmt::Display for PyPiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "bad response with status code {} for {}: {}",
            self.code, self.path, self.message
        )
    }
}

enum GetError {
    NotFound(PyPiError),
    Fatal(String),
}

impl GetError {
    fn into_string(self) -> String {
        match self {
            Self::NotFound(err) => err.to_string(),
            Self::Fatal(err) => err,
        }
    }
}

/// `pypi.File`: one anchor element from `/<project>/`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct File {
    pub name: String,
    pub url: String,
    /// `data-gpg-sig`; `None` when absent or not a boolean.
    pub data_gpg_sig: Option<bool>,
    pub data_requires_python: String,
}

/// `pypi.SDist`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SDist {
    pub file: File,
    pub distribution: String,
    pub version: String,
}

/// `pypi.Wheel`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Wheel {
    pub file: File,
    pub distribution: String,
    pub version: String,
    pub build_tag: String,
    pub python_tag: String,
    pub abi_tag: String,
    pub platform_tag: String,
}

/// `pypi.Client`.
pub struct PyPiClient {
    urls: Vec<String>,
    doer: Arc<dyn HttpDoer>,
}

impl PyPiClient {
    /// `pypi.NewClient` without the client factory.
    pub fn new(urls: Vec<String>, doer: Arc<dyn HttpDoer>) -> Self {
        Self { urls, doer }
    }

    pub fn urls(&self) -> &[String] {
        &self.urls
    }

    /// `Client.Project`.
    pub async fn project(&self, project: &str) -> Result<Vec<File>, String> {
        let data = self
            .get(&normalize(project))
            .await
            .map_err(|err| format!("PyPI: {}", err.into_string()))?;
        parse_html(&String::from_utf8_lossy(&data))
    }

    /// `Client.Version`.
    pub async fn version(&self, project: &str, version: &str) -> Result<File, String> {
        let files = self.project(project).await?;
        find_version(version, &files)
            .map_err(|err| format!("project: {}: {err}", go_quote(project)))
    }

    /// `Client.Download`.
    pub async fn download(&self, url: &str) -> Result<Vec<u8>, String> {
        do_request(&self.doer, url)
            .await
            .map_err(|err| format!("PyPI: {}", err.into_string()))
    }

    /// `Client.get`.
    async fn get(&self, project: &str) -> Result<Vec<u8>, GetError> {
        let mut last: Option<GetError> = None;
        for base_url in &self.urls {
            let url = match build_url(base_url, project) {
                Ok(url) => url,
                Err(_) => {
                    return Err(GetError::Fatal(format!(
                        "invalid proxy URL {}",
                        go_quote(base_url)
                    )))
                }
            };

            match do_request(&self.doer, &url).await {
                Ok(body) => return Ok(body),
                Err(err) => {
                    let not_found =
                        matches!(&err, GetError::NotFound(inner) if inner.not_found());
                    last = Some(err);
                    if !not_found {
                        break;
                    }
                }
            }
        }

        match last {
            Some(err) => Err(err),
            None => Ok(Vec::new()),
        }
    }
}

/// `Client.get`'s path construction: `/<project>/` appended to the base path,
/// with the base path's trailing slash preserved.
fn build_url(base_url: &str, project: &str) -> Result<String, String> {
    let mut url = Url::parse(base_url).map_err(|err| err.to_string())?;
    let joined = format!("{}/", go_path_join(url.path(), project));
    let path = if joined.starts_with('/') {
        joined
    } else {
        format!("/{joined}")
    };
    url.set_path(&path);
    Ok(url.to_string())
}

async fn do_request(doer: &Arc<dyn HttpDoer>, url: &str) -> Result<Vec<u8>, GetError> {
    let response = doer
        .do_request(HttpRequest {
            method: "GET".to_owned(),
            url: url.to_owned(),
            headers: Vec::new(),
            body: Vec::new(),
        })
        .await
        .map_err(GetError::Fatal)?;

    if response.status != 200 {
        let path = Url::parse(url)
            .map(|parsed| parsed.path().to_owned())
            .map_err(|err| GetError::Fatal(err.to_string()))?;
        let error = PyPiError {
            path,
            code: response.status,
            message: String::from_utf8_lossy(&response.body).into_owned(),
        };
        return Err(if error.not_found() {
            GetError::NotFound(error)
        } else {
            GetError::Fatal(error.to_string())
        });
    }
    Ok(response.body)
}

/// `pypi.FindVersion`.
pub fn find_version(version: &str, files: &[File]) -> Result<File, String> {
    if files.is_empty() {
        return Err("no files".to_owned());
    }

    let mut version = version.to_owned();
    if version.is_empty() {
        for file in files.iter().rev() {
            if let Ok(wheel) = to_wheel(file) {
                version = wheel.version;
                break;
            } else if let Ok(sdist) = to_sdist(file) {
                version = sdist.version;
                break;
            }
        }
    }

    if version.is_empty() {
        return Err(PyPiError {
            path: String::new(),
            code: 404,
            message: "could not find a wheel or source distribution to determine the latest version"
                .to_owned(),
        }
        .to_string());
    }

    let mut min_wheel: Option<&File> = None;
    for file in files {
        match to_wheel(file) {
            Err(_) => {
                if let Ok(sdist) = to_sdist(file) {
                    if sdist.version == version {
                        return Ok(file.clone());
                    }
                }
            }
            Ok(wheel) => {
                if wheel.version == version
                    && min_wheel.map(|min| file.name < min.name).unwrap_or(true)
                {
                    min_wheel = Some(file);
                }
            }
        }
    }

    if let Some(file) = min_wheel {
        return Ok(file.clone());
    }

    Err(PyPiError {
        path: String::new(),
        code: 404,
        message: format!(
            "could not find a wheel or source distribution for version {version}"
        ),
    }
    .to_string())
}

/// `pypi.ToSDist`.
pub fn to_sdist(file: &File) -> Result<SDist, String> {
    let mut name = file.name.clone();
    let ext = is_sdist(&name);
    if ext.is_empty() {
        return Err(format!("{} is not a sdist", go_quote(&name)));
    }
    name.truncate(name.len() - ext.len());

    let Some(index) = name.rfind('-') else {
        return Err(format!("{} has an invalid sdist format", go_quote(&name)));
    };
    Ok(SDist {
        file: file.clone(),
        distribution: name[..index].to_owned(),
        version: name[index + 1..].to_owned(),
    })
}

fn is_sdist(filename: &str) -> String {
    let ext = filepath_ext(filename);
    if ext == ".zip" || ext == ".tar" {
        return ext;
    }
    match ext_n(filename, 2).as_str() {
        ".tar.gz" | ".tar.bz2" | ".tar.xz" | ".tar.Z" => ext_n(filename, 2),
        _ => String::new(),
    }
}

/// `pypi.extN`.
fn ext_n(path: &str, mut n: i32) -> String {
    if n == -1 {
        return match path.find('.') {
            Some(index) => path[index..].to_owned(),
            None => String::new(),
        };
    }
    let bytes = path.as_bytes();
    let mut index = bytes.len() as isize - 1;
    while index >= 0 && bytes[index as usize] != b'/' {
        if bytes[index as usize] == b'.' {
            n -= 1;
            if n == 0 {
                return path[index as usize..].to_owned();
            }
        }
        index -= 1;
    }
    String::new()
}

/// `path/filepath.Ext`.
fn filepath_ext(path: &str) -> String {
    let start = path.rfind('/').map(|index| index + 1).unwrap_or(0);
    match path[start..].rfind('.') {
        Some(index) => path[start + index..].to_owned(),
        None => String::new(),
    }
}

/// `pypi.ToWheel`.
pub fn to_wheel(file: &File) -> Result<Wheel, String> {
    let mut name = file.name.clone();
    let ext = filepath_ext(&name);
    if ext != ".whl" {
        return Err(format!("{name} does not conform to pep 491"));
    }
    name.truncate(name.len() - ext.len());

    let pcs: Vec<&str> = name.split('-').collect();
    match pcs.len() {
        5 => Ok(Wheel {
            file: file.clone(),
            distribution: pcs[0].to_owned(),
            version: pcs[1].to_owned(),
            build_tag: String::new(),
            python_tag: pcs[2].to_owned(),
            abi_tag: pcs[3].to_owned(),
            platform_tag: pcs[4].to_owned(),
        }),
        6 => Ok(Wheel {
            file: file.clone(),
            distribution: pcs[0].to_owned(),
            version: pcs[1].to_owned(),
            build_tag: pcs[2].to_owned(),
            python_tag: pcs[3].to_owned(),
            abi_tag: pcs[4].to_owned(),
            platform_tag: pcs[5].to_owned(),
        }),
        _ => Err(format!("{name} does not conform to pep 491")),
    }
}

/// `pypi.normalize`: lower-case and collapse `[-_.]+` to `-`.
pub fn normalize(path: &str) -> String {
    let lowered = path.to_lowercase();
    let mut out = String::with_capacity(lowered.len());
    let mut in_separator = false;
    for ch in lowered.chars() {
        if ch == '-' || ch == '_' || ch == '.' {
            if !in_separator {
                out.push('-');
                in_separator = true;
            }
        } else {
            out.push(ch);
            in_separator = false;
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Minimal PEP 503 anchor tokenizer (replaces golang.org/x/net/html)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    StartTag {
        name: String,
        attrs: Vec<(String, String)>,
    },
    EndTag,
    Text(String),
}

fn is_html_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | 0x0c)
}

fn tokenize(input: &str) -> Vec<Token> {
    let bytes = input.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'<' {
            let start = i;
            while i < bytes.len() && bytes[i] != b'<' {
                i += 1;
            }
            let text = &input[start..i];
            if !text.is_empty() {
                tokens.push(Token::Text(unescape_html(text)));
            }
            continue;
        }

        if input[i..].starts_with("<!--") {
            i = input[i + 4..]
                .find("-->")
                .map(|offset| i + 4 + offset + 3)
                .unwrap_or(bytes.len());
            continue;
        }
        if i + 1 < bytes.len() && (bytes[i + 1] == b'!' || bytes[i + 1] == b'?') {
            i = input[i..]
                .find('>')
                .map(|offset| i + offset + 1)
                .unwrap_or(bytes.len());
            continue;
        }
        if i + 1 < bytes.len() && bytes[i + 1] == b'/' {
            i = input[i..]
                .find('>')
                .map(|offset| i + offset + 1)
                .unwrap_or(bytes.len());
            tokens.push(Token::EndTag);
            continue;
        }

        // Start tag.
        let mut j = i + 1;
        let name_start = j;
        while j < bytes.len()
            && !is_html_space(bytes[j])
            && bytes[j] != b'>'
            && bytes[j] != b'/'
        {
            j += 1;
        }
        let name = input[name_start..j].to_ascii_lowercase();
        let mut attrs = Vec::new();
        loop {
            while j < bytes.len() && is_html_space(bytes[j]) {
                j += 1;
            }
            if j >= bytes.len() || bytes[j] == b'>' {
                j = (j + 1).min(bytes.len());
                break;
            }
            if bytes[j] == b'/' {
                j += 1;
                continue;
            }
            let attr_start = j;
            while j < bytes.len()
                && !is_html_space(bytes[j])
                && bytes[j] != b'='
                && bytes[j] != b'>'
                && bytes[j] != b'/'
            {
                j += 1;
            }
            let attr_name = input[attr_start..j].to_ascii_lowercase();
            while j < bytes.len() && is_html_space(bytes[j]) {
                j += 1;
            }
            let mut value = String::new();
            if j < bytes.len() && bytes[j] == b'=' {
                j += 1;
                while j < bytes.len() && is_html_space(bytes[j]) {
                    j += 1;
                }
                if j < bytes.len() && (bytes[j] == b'"' || bytes[j] == b'\'') {
                    let quote = bytes[j];
                    j += 1;
                    let value_start = j;
                    while j < bytes.len() && bytes[j] != quote {
                        j += 1;
                    }
                    value = input[value_start..j].to_owned();
                    if j < bytes.len() {
                        j += 1;
                    }
                } else {
                    let value_start = j;
                    while j < bytes.len()
                        && !is_html_space(bytes[j])
                        && bytes[j] != b'>'
                    {
                        j += 1;
                    }
                    value = input[value_start..j].to_owned();
                }
            }
            attrs.push((attr_name, unescape_html(&value)));
        }
        tokens.push(Token::StartTag { name, attrs });
        i = j;
    }
    tokens
}

/// `html.UnescapeString` subset: named entities used by package registries
/// plus decimal and hexadecimal character references.
fn unescape_html(input: &str) -> String {
    if !input.contains('&') {
        return input.to_owned();
    }
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'&' {
            let start = i;
            while i < bytes.len() && bytes[i] != b'&' {
                i += 1;
            }
            out.push_str(&input[start..i]);
            continue;
        }
        let Some(semicolon) = input[i..].find(';').map(|offset| i + offset) else {
            out.push('&');
            i += 1;
            continue;
        };
        let entity = &input[i + 1..semicolon];
        let replacement = match entity {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" | "#39" => Some('\''),
            _ if entity.starts_with("#x") || entity.starts_with("#X") => {
                u32::from_str_radix(&entity[2..], 16)
                    .ok()
                    .and_then(char::from_u32)
            }
            _ if entity.starts_with('#') => entity[1..].parse::<u32>().ok().and_then(char::from_u32),
            _ => None,
        };
        match replacement {
            Some(ch) => {
                out.push(ch);
                i = semicolon + 1;
            }
            None => {
                out.push('&');
                i += 1;
            }
        }
    }
    out
}

fn parse_html(input: &str) -> Result<Vec<File>, String> {
    let tokens = tokenize(input);
    let mut files = Vec::new();
    let mut index = 0;

    loop {
        // Find the next anchor start tag.
        loop {
            if index >= tokens.len() {
                return Ok(files);
            }
            match &tokens[index] {
                Token::StartTag { name, .. } if name == "a" => break,
                _ => index += 1,
            }
        }

        let Token::StartTag { attrs, .. } = &tokens[index] else {
            unreachable!("anchor token")
        };
        let mut file = File::default();
        for (key, value) in attrs {
            match key.as_str() {
                "href" => file.url = value.clone(),
                "data-requires-python" => file.data_requires_python = value.clone(),
                "data-gpg-sig" => {
                    if let Some(parsed) = parse_bool(value) {
                        file.data_gpg_sig = Some(parsed);
                    }
                }
                _ => {}
            }
        }
        index += 1;

        if file.url.is_empty() {
            continue;
        }

        // The first text token is the filename.
        loop {
            if index >= tokens.len() {
                return Ok(files);
            }
            match &tokens[index] {
                Token::Text(text) => {
                    file.name = text.clone();
                    let path = url_path(&file.url);
                    let base = path_base(&path);
                    if base != file.name {
                        return Err(format!(
                            "{} != {}: text does not match final path component",
                            file.name, base
                        ));
                    }
                    files.push(file);
                    index += 1;
                    break;
                }
                _ => index += 1,
            }
        }
    }
}

/// `url.Parse(u).Path`, accepting relative references like Go does.
fn url_path(raw: &str) -> String {
    if let Ok(parsed) = Url::parse(raw) {
        return parsed.path().to_owned();
    }
    let without_fragment = raw.split('#').next().unwrap_or(raw);
    without_fragment
        .split('?')
        .next()
        .unwrap_or(without_fragment)
        .to_owned()
}

/// `path/filepath.Base`.
fn path_base(path: &str) -> String {
    if path.is_empty() {
        return ".".to_owned();
    }
    let mut path = path;
    while path.len() > 1 && path.ends_with('/') {
        path = &path[..path.len() - 1];
    }
    if path == "/" {
        return "/".to_owned();
    }
    match path.rfind('/') {
        Some(index) => path[index + 1..].to_owned(),
        None => path.to_owned(),
    }
}

/// `strconv.ParseBool`.
fn parse_bool(value: &str) -> Option<bool> {
    match value {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HttpResponse;
    use async_trait::async_trait;
    use std::sync::Mutex;

    struct FakeDoer {
        responses: Mutex<Vec<HttpResponse>>,
        requests: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl HttpDoer for FakeDoer {
        async fn do_request(&self, request: HttpRequest) -> Result<HttpResponse, String> {
            self.requests.lock().unwrap().push(request.url);
            let mut responses = self.responses.lock().unwrap();
            if responses.is_empty() {
                return Ok(HttpResponse::default());
            }
            Ok(responses.remove(0))
        }
    }

    fn response(status: u16, body: &str) -> HttpResponse {
        HttpResponse {
            status,
            headers: Vec::new(),
            body: body.as_bytes().to_vec(),
        }
    }

    fn client(responses: Vec<HttpResponse>) -> (PyPiClient, Arc<FakeDoer>) {
        let doer = Arc::new(FakeDoer {
            responses: Mutex::new(responses),
            requests: Mutex::new(Vec::new()),
        });
        (
            PyPiClient::new(vec!["https://pypi.org/simple".to_owned()], doer.clone()),
            doer,
        )
    }

    const PAGE: &str = concat!(
        "<html><body>",
        "<a href=\"https://files.example/requests-2.1.0.tar.gz#sha=1\" ",
        "data-requires-python=\"&gt;=3.7\">requests-2.1.0.tar.gz</a>",
        "<a href=\"https://files.example/requests-2.2.0-py3-none-any.whl\">",
        "requests-2.2.0-py3-none-any.whl</a>",
        "</body></html>"
    );

    #[test]
    fn normalize_matches_pep_503() {
        assert_eq!(normalize("Zope.Interface"), "zope-interface");
        assert_eq!(normalize("a__b..c--d"), "a-b-c-d");
    }

    #[test]
    fn parses_anchor_attributes_and_text() {
        let files = parse_html(PAGE).unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].name, "requests-2.1.0.tar.gz");
        assert_eq!(files[0].data_requires_python, ">=3.7");
        assert_eq!(
            files[0].url,
            "https://files.example/requests-2.1.0.tar.gz#sha=1"
        );
        assert_eq!(files[1].data_requires_python, "");
    }

    #[test]
    fn text_must_match_the_url_basename() {
        let err = parse_html(
            "<a href=\"https://files.example/x.tar.gz\">y.tar.gz</a>",
        )
        .unwrap_err();
        assert_eq!(
            err,
            "y.tar.gz != x.tar.gz: text does not match final path component"
        );
    }

    #[test]
    fn versions_prefer_sdist_then_smallest_wheel() {
        let files = parse_html(PAGE).unwrap();
        let file = find_version("2.1.0", &files).unwrap();
        assert!(file.name.ends_with(".tar.gz"));

        let wheel_page = concat!(
            "<a href=\"https://files.example/pkg-1.0-b-py3-none-any.whl\">pkg-1.0-b-py3-none-any.whl</a>",
            "<a href=\"https://files.example/pkg-1.0-a-py3-none-any.whl\">pkg-1.0-a-py3-none-any.whl</a>",
        );
        let files = parse_html(wheel_page).unwrap();
        let file = find_version("1.0", &files).unwrap();
        assert_eq!(file.name, "pkg-1.0-a-py3-none-any.whl");
    }

    #[test]
    fn latest_version_requires_a_wheel_or_sdist() {
        let files = parse_html("<a href=\"https://files.example/notes.txt\">notes.txt</a>").unwrap();
        assert_eq!(
            find_version("", &files).unwrap_err(),
            "bad response with status code 404 for : could not find a wheel or source distribution to determine the latest version"
        );
    }

    #[test]
    fn wheel_and_sdist_parsing() {
        let file = File {
            name: "pkg-1.0-1-py3-none-any.whl".to_owned(),
            ..File::default()
        };
        let wheel = to_wheel(&file).unwrap();
        assert_eq!(wheel.version, "1.0");
        assert_eq!(wheel.build_tag, "1");

        let file = File {
            name: "pkg-1.0.tar.gz".to_owned(),
            ..File::default()
        };
        let sdist = to_sdist(&file).unwrap();
        assert_eq!(sdist.distribution, "pkg");
        assert_eq!(sdist.version, "1.0");

        let file = File {
            name: "notes.txt".to_owned(),
            ..File::default()
        };
        assert_eq!(to_sdist(&file).unwrap_err(), "\"notes.txt\" is not a sdist");
        assert_eq!(to_wheel(&file).unwrap_err(), "notes.txt does not conform to pep 491");
    }

    #[tokio::test]
    async fn project_requests_the_trailing_slash_url() {
        let (client, doer) = client(vec![response(200, PAGE)]);
        let files = client.project("Zope.Interface").await.unwrap();
        assert_eq!(files.len(), 2);
        let requests = doer.requests.lock().unwrap();
        assert_eq!(requests[0], "https://pypi.org/simple/zope-interface/");
    }

    #[tokio::test]
    async fn project_wraps_errors_with_pypi() {
        let (client, _) = client(vec![response(500, "boom")]);
        assert_eq!(
            client.project("requests").await.unwrap_err(),
            "PyPI: bad response with status code 500 for /simple/requests/: boom"
        );
    }

    #[tokio::test]
    async fn not_found_falls_back_to_the_next_url() {
        let doer = Arc::new(FakeDoer {
            responses: Mutex::new(vec![response(404, "no"), response(200, PAGE)]),
            requests: Mutex::new(Vec::new()),
        });
        let client = PyPiClient::new(
            vec![
                "https://a.example/simple".to_owned(),
                "https://b.example/simple".to_owned(),
            ],
            doer.clone(),
        );
        client.project("requests").await.unwrap();
        let requests = doer.requests.lock().unwrap();
        assert_eq!(requests[0], "https://a.example/simple/requests/");
        assert_eq!(requests[1], "https://b.example/simple/requests/");
    }

    #[tokio::test]
    async fn version_errors_are_wrapped_with_the_project() {
        let (client, _) = client(vec![response(200, PAGE)]);
        assert_eq!(
            client.version("requests", "9.9.9").await.unwrap_err(),
            "project: \"requests\": bad response with status code 404 for : could not find a wheel or source distribution for version 9.9.9"
        );
    }
}
