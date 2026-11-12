#![forbid(unsafe_code)]

//! `internal/extsvc/pagure`: the read-only Pagure REST surface the repo-updater
//! source needs.
//!
//! Ported: `Client.ListProjects` (cursor pagination), `Project`/`Pagination`
//! shapes, the `token` authorization header, and the exact
//! `Pagure API HTTP error: code=%d url=%q body=%q` text. The self-imposed
//! rate limiter is not ported (no Redis), matching the other source slices.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use url::Url;

use crate::HttpDoer;
use crate::{HttpRequest, HttpResponse};

/// `extsvc.TypePagure`.
pub const TYPE_PAGURE: &str = "pagure";

/// `schema.PagureConnection` (the fields the source/client reads).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PagureConnection {
    #[serde(default)]
    pub forks: bool,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub pattern: String,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub url: String,
}

/// `pagure.Pagination`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pagination {
    #[serde(default)]
    pub first: String,
    #[serde(default)]
    pub last: String,
    #[serde(default)]
    pub next: String,
    #[serde(default)]
    pub page: i64,
    #[serde(default)]
    pub pages: i64,
    #[serde(default)]
    pub per_page: i64,
    #[serde(default)]
    pub prev: String,
}

impl Pagination {
    /// `Pagination.EncodeTo`: always writes `per_page` and `page`.
    pub fn encode_to(&self, query: &mut BTreeMap<String, Vec<String>>) {
        query.insert("per_page".to_owned(), vec![self.per_page.to_string()]);
        query.insert("page".to_owned(), vec![self.page.to_string()]);
    }
}

/// `pagure.Project`, serialized as `repo.metadata`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Project {
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub full_url: String,
    #[serde(default)]
    pub fullname: String,
    #[serde(default)]
    pub id: i64,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub namespace: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<Box<Project>>,
    /// Go `Tags []string` with no `omitempty`: nil marshals as `null`.
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    #[serde(default)]
    pub url_path: String,
}

/// `pagure.ListProjectsArgs`.
#[derive(Debug, Clone, Default)]
pub struct ListProjectsArgs {
    pub cursor: Option<Pagination>,
    pub tags: Vec<String>,
    pub pattern: String,
    pub namespace: String,
    pub fork: bool,
}

/// `pagure.httpError`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpError {
    pub status_code: u16,
    pub url: String,
    pub body: String,
}

impl std::fmt::Display for HttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Pagure API HTTP error: code={} url={:?} body={:?}",
            self.status_code, self.url, self.body
        )
    }
}

/// Client errors. Transport/JSON errors mirror the baseline's unwrapped
/// `error`/`json.Unmarshal` returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PagureError {
    Http(HttpError),
    Transport(String),
    Json(String),
}

impl std::fmt::Display for PagureError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Http(err) => err.fmt(f),
            Self::Transport(err) | Self::Json(err) => f.write_str(err),
        }
    }
}

impl std::error::Error for PagureError {}

/// `pagure.Client`.
#[derive(Clone)]
pub struct Client {
    pub config: PagureConnection,
    pub url: Url,
    http: Arc<dyn HttpDoer>,
}

#[derive(Debug, Default, Deserialize)]
struct ListProjectsResponse {
    #[serde(default)]
    pagination: Option<Pagination>,
    #[serde(default)]
    projects: Vec<Project>,
}

impl Client {
    /// `pagure.NewClient` (rate limiter omitted).
    pub fn new(
        config: PagureConnection,
        http: Arc<dyn HttpDoer>,
    ) -> Result<Self, String> {
        let url = Url::parse(&config.url).map_err(|err| err.to_string())?;
        Ok(Self { config, url, http })
    }

    /// One `Iterator` step: fetch a page and return the projects plus the next
    /// cursor (`None` when the iterator terminates).
    pub async fn list_projects_page(
        &self,
        cursor: Option<&Pagination>,
        args: &ListProjectsArgs,
    ) -> Result<(Vec<Project>, Option<Pagination>), PagureError> {
        let mut query: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let default_cursor;
        let cursor = match cursor {
            Some(cursor) => cursor,
            None => {
                default_cursor = Pagination {
                    per_page: 100,
                    page: 1,
                    ..Pagination::default()
                };
                &default_cursor
            }
        };
        cursor.encode_to(&mut query);
        for tag in &args.tags {
            if !tag.is_empty() {
                query.entry("tags".to_owned()).or_default().push(tag.clone());
            }
        }
        if !args.pattern.is_empty() {
            query.insert("pattern".to_owned(), vec![args.pattern.clone()]);
        }
        if !args.namespace.is_empty() {
            query.insert("namespace".to_owned(), vec![args.namespace.clone()]);
        }
        query.insert("fork".to_owned(), vec![args.fork.to_string()]);

        let encoded = encode_values(&query);
        let relative = if encoded.is_empty() {
            "api/0/projects".to_owned()
        } else {
            format!("api/0/projects?{encoded}")
        };

        let response = self.do_get(&relative).await?;
        let decoded: ListProjectsResponse = serde_json::from_slice(&response.body)
            .map_err(|err| PagureError::Json(err.to_string()))?;

        let next = match decoded.pagination {
            Some(mut pagination) => {
                if pagination.next.is_empty() {
                    None
                } else {
                    pagination.page += 1;
                    Some(pagination)
                }
            }
            None => None,
        };
        Ok((decoded.projects, next))
    }

    /// `Client.do` for a GET without a body.
    async fn do_get(&self, relative: &str) -> Result<HttpResponse, PagureError> {
        // `req.URL = c.URL.ResolveReference(req.URL)`.
        let url = self
            .url
            .join(relative)
            .map_err(|err| PagureError::Transport(err.to_string()))?;
        let url_string = url.to_string();

        let mut headers = Vec::new();
        if !self.config.token.is_empty() {
            headers.push((
                "Authorization".to_owned(),
                format!("token {}", self.config.token),
            ));
        }

        let response = self
            .http
            .do_request(HttpRequest {
                method: "GET".to_owned(),
                url: url_string.clone(),
                headers,
                body: Vec::new(),
            })
            .await
            .map_err(PagureError::Transport)?;

        if !(200..400).contains(&response.status) {
            return Err(PagureError::Http(HttpError {
                status_code: response.status,
                url: url_string,
                body: String::from_utf8_lossy(&response.body).into_owned(),
            }));
        }
        Ok(response)
    }
}

/// `url.Values.Encode`: keys sorted, values in insertion order.
pub fn encode_values(values: &BTreeMap<String, Vec<String>>) -> String {
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, entries) in values {
        for value in entries {
            serializer.append_pair(key, value);
        }
    }
    serializer.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pagination_encodes_per_page_and_page() {
        let mut query = BTreeMap::new();
        Pagination {
            per_page: 100,
            page: 3,
            ..Pagination::default()
        }
        .encode_to(&mut query);
        assert_eq!(encode_values(&query), "page=3&per_page=100");
    }

    #[test]
    fn project_serializes_go_field_names() {
        let project = Project {
            description: "d".to_owned(),
            full_url: "https://pagure.example.com/p".to_owned(),
            fullname: "p".to_owned(),
            id: 5,
            name: "p".to_owned(),
            namespace: "ns".to_owned(),
            parent: None,
            tags: None,
            url_path: "p".to_owned(),
        };
        assert_eq!(
            serde_json::to_value(&project).unwrap(),
            serde_json::json!({
                "description": "d",
                "full_url": "https://pagure.example.com/p",
                "fullname": "p",
                "id": 5,
                "name": "p",
                "namespace": "ns",
                "tags": null,
                "url_path": "p"
            })
        );
    }

    #[test]
    fn http_error_text() {
        let err = HttpError {
            status_code: 404,
            url: "https://p/api/0/projects".to_owned(),
            body: "nope".to_owned(),
        };
        assert_eq!(
            err.to_string(),
            "Pagure API HTTP error: code=404 url=\"https://p/api/0/projects\" body=\"nope\""
        );
    }
}
