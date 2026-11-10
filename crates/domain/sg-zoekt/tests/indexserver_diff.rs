//! M9.8hb differential harness: `internal/search/zoekt/reindex.go`
//! (`resolveIndexserver`, `Reindex`, `GetIndexserverHost`) against
//! [`sg_zoekt::indexserver`].
//!
//! Run with `SG_M9HB_DIFF=1 M9HB_CORPUS=<json> M9HB_OUT=<json>` (the wrapper
//! script in `test_ws/p9-go` does this). Both sides emit the same normalized
//! JSON: one array, in corpus category order.

use std::collections::BTreeMap;
use std::sync::Mutex;

use serde::Deserialize;
use serde_json::{json, Map, Value};

use sg_zoekt::indexserver::{
    get_indexserver_host, reindex, resolve_indexserver, Error, HttpClient, IndexerMap, Request,
    Response,
};

#[derive(Deserialize)]
struct Corpus {
    #[serde(default)]
    resolve_cases: Vec<ResolveCase>,
    #[serde(default)]
    reindex_cases: Vec<ReindexCase>,
    #[serde(default)]
    host_cases: Vec<HostCase>,
}

#[derive(Deserialize)]
struct ResolveCase {
    name: String,
    lookup: String,
    #[serde(default)]
    assignments: BTreeMap<String, String>,
    #[serde(default)]
    get_error: String,
    #[serde(default)]
    missing_error: String,
}

#[derive(Deserialize)]
struct ReindexCase {
    name: String,
    lookup: String,
    id: u32,
    #[serde(default)]
    status: u16,
    #[serde(default)]
    body: String,
    #[serde(default)]
    body_error: bool,
    #[serde(default)]
    transport_error: bool,
    #[serde(default)]
    map_miss: bool,
    #[serde(default)]
    endpoint_style: String,
    #[serde(default)]
    missing_error: String,
}

#[derive(Deserialize)]
struct HostCase {
    name: String,
    lookup: String,
    #[serde(default)]
    status: u16,
    #[serde(default)]
    body: String,
    #[serde(default)]
    body_error: bool,
    #[serde(default)]
    transport_error: bool,
    #[serde(default)]
    map_miss: bool,
    #[serde(default)]
    endpoint_style: String,
    #[serde(default)]
    missing_error: String,
}

struct FakeMap {
    assignments: BTreeMap<String, String>,
    get_error: Option<String>,
    missing_error: Option<String>,
}

impl IndexerMap for FakeMap {
    fn get(&self, name: &str) -> Result<String, Error> {
        if let Some(get_error) = &self.get_error {
            return Err(Error::Indexer(get_error.clone()));
        }
        match self.assignments.get(name) {
            Some(endpoint) => Ok(endpoint.clone()),
            None => Err(Error::Indexer(
                self.missing_error
                    .clone()
                    .unwrap_or_else(|| format!("no endpoint found for {name}")),
            )),
        }
    }
}

struct MockHttp {
    response: Result<Response, Error>,
    requests: Mutex<Vec<Request>>,
}

impl MockHttp {
    fn new(response: Result<Response, Error>) -> Self {
        Self {
            response,
            requests: Mutex::new(Vec::new()),
        }
    }

    fn first_request(&self) -> Option<Request> {
        self.requests.lock().expect("mock lock").first().cloned()
    }
}

impl HttpClient for MockHttp {
    fn do_request(&self, req: Request) -> Result<Response, Error> {
        self.requests.lock().expect("mock lock").push(req);
        self.response.clone()
    }
}

fn missing_error(case: &str) -> Option<String> {
    if case.is_empty() {
        None
    } else {
        Some(case.to_owned())
    }
}

fn endpoint_for(style: &str) -> String {
    match style {
        "bare" => "indexer.internal:6070".to_owned(),
        "with-path" => "http://indexer.internal:6070/prefix".to_owned(),
        _ => "http://indexer.internal:6070".to_owned(),
    }
}

fn request_snapshot(request: &Request) -> Value {
    let content_type = request
        .headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
        .map(|(_, value)| value.clone())
        .unwrap_or_default();

    let form = if request.method == "POST" {
        let mut form: BTreeMap<String, Vec<String>> = BTreeMap::new();
        if let Some(body) = &request.body {
            for (key, value) in url::form_urlencoded::parse(body) {
                form.entry(key.into_owned())
                    .or_default()
                    .push(value.into_owned());
            }
        }
        serde_json::to_value(form).expect("encode form")
    } else {
        Value::Null
    };

    json!({
        "method": request.method,
        "path": request.url.path(),
        "content_type": content_type,
        "form": form,
    })
}

fn error_snapshot(error: &Error) -> (String, Option<String>) {
    match error {
        Error::Indexer(message) => ("map".to_owned(), Some(message.clone())),
        Error::Http(_) => ("http".to_owned(), None),
        Error::Body(_) => ("body".to_owned(), None),
        Error::Status(message) => ("status".to_owned(), Some(message.clone())),
        Error::UnexpectedStatus { .. } => ("status".to_owned(), Some(error.to_string())),
        Error::Url(_) => ("url".to_owned(), None),
        Error::Json(_) => ("json".to_owned(), None),
    }
}

fn run_resolve(case: &ResolveCase) -> Value {
    let mut entry = Map::new();
    entry.insert("name".to_owned(), Value::String(case.name.clone()));

    let fake = FakeMap {
        assignments: case.assignments.clone(),
        get_error: if case.get_error.is_empty() {
            None
        } else {
            Some(case.get_error.clone())
        },
        missing_error: missing_error(&case.missing_error),
    };

    match resolve_indexserver(&case.lookup, &fake) {
        Ok(url) => {
            let resolved = url
                .join("/indexserver/debug/host")
                .expect("resolved base URL joins");
            entry.insert("scheme".to_owned(), Value::String(url.scheme().to_owned()));
            entry.insert(
                "host".to_owned(),
                Value::String(url.host_str().unwrap_or_default().to_owned()),
            );
            entry.insert(
                "port".to_owned(),
                Value::String(url.port().map(|port| port.to_string()).unwrap_or_default()),
            );
            entry.insert(
                "resolved_url".to_owned(),
                Value::String(resolved.as_str().to_owned()),
            );
        }
        Err(error) => {
            let (kind, message) = error_snapshot(&error);
            entry.insert("error_kind".to_owned(), Value::String(kind));
            if let Some(message) = message {
                entry.insert("error".to_owned(), Value::String(message));
            }
        }
    }

    Value::Object(entry)
}

fn run_reindex(case: &ReindexCase) -> Value {
    let mut entry = Map::new();
    entry.insert("name".to_owned(), Value::String(case.name.clone()));

    let missing = missing_error(&case.missing_error);
    let mut assignments = BTreeMap::new();
    if !case.map_miss {
        assignments.insert(case.lookup.clone(), endpoint_for(&case.endpoint_style));
    }
    let response = if case.map_miss {
        Ok(Response::new(202, Vec::new()))
    } else if case.transport_error {
        Err(Error::Http("connection refused".to_owned()))
    } else if case.body_error {
        Ok(Response::with_body_error(case.status, "unexpected EOF"))
    } else {
        Ok(Response::new(case.status, case.body.as_bytes().to_vec()))
    };

    let fake = FakeMap {
        assignments,
        get_error: None,
        missing_error: missing,
    };
    let http = MockHttp::new(response);
    let result = reindex(&http, &fake, &case.lookup, case.id);

    if !case.transport_error {
        if let Some(request) = http.first_request() {
            entry.insert("request".to_owned(), request_snapshot(&request));
        }
    }

    if let Err(error) = result {
        let (kind, message) = error_snapshot(&error);
        entry.insert("error_kind".to_owned(), Value::String(kind));
        if let Some(message) = message {
            entry.insert("error".to_owned(), Value::String(message));
        }
    }

    Value::Object(entry)
}

fn run_host(case: &HostCase) -> Value {
    let mut entry = Map::new();
    entry.insert("name".to_owned(), Value::String(case.name.clone()));

    let missing = missing_error(&case.missing_error);
    let mut assignments = BTreeMap::new();
    if !case.map_miss {
        assignments.insert(case.lookup.clone(), endpoint_for(&case.endpoint_style));
    }
    let response = if case.map_miss {
        Ok(Response::new(200, Vec::new()))
    } else if case.transport_error {
        Err(Error::Http("connection refused".to_owned()))
    } else if case.body_error {
        Ok(Response::with_body_error(case.status, "unexpected EOF"))
    } else {
        Ok(Response::new(case.status, case.body.as_bytes().to_vec()))
    };

    let fake = FakeMap {
        assignments,
        get_error: None,
        missing_error: missing,
    };
    let http = MockHttp::new(response);
    let result = get_indexserver_host(&http, &fake, &case.lookup);

    if !case.transport_error {
        if let Some(request) = http.first_request() {
            entry.insert("request".to_owned(), request_snapshot(&request));
        }
    }

    match result {
        Ok(host) => {
            entry.insert("host".to_owned(), Value::String(host.name));
        }
        Err(error) => {
            let (kind, message) = error_snapshot(&error);
            entry.insert("error_kind".to_owned(), Value::String(kind));
            if let Some(message) = message {
                entry.insert("error".to_owned(), Value::String(message));
            }
        }
    }

    Value::Object(entry)
}

#[test]
fn indexserver_diff() {
    if std::env::var("SG_M9HB_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path =
        std::env::var("M9HB_CORPUS").expect("M9HB_CORPUS must be set when SG_M9HB_DIFF=1");
    let out_path = std::env::var("M9HB_OUT").expect("M9HB_OUT must be set when SG_M9HB_DIFF=1");

    let raw = std::fs::read_to_string(&corpus_path).expect("read corpus");
    let corpus: Corpus = serde_json::from_str(&raw).expect("parse corpus");

    let mut entries = Vec::new();
    for case in &corpus.resolve_cases {
        entries.push(run_resolve(case));
    }
    for case in &corpus.reindex_cases {
        entries.push(run_reindex(case));
    }
    for case in &corpus.host_cases {
        entries.push(run_host(case));
    }

    std::fs::write(
        &out_path,
        serde_json::to_string(&entries).expect("encode output"),
    )
    .expect("write output");
}
