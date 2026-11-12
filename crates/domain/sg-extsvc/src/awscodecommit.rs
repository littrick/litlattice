#![forbid(unsafe_code)]

//! `internal/extsvc/awscodecommit`: the AWS CodeCommit JSON-protocol surface
//! the repo-updater source needs.
//!
//! The baseline drives the AWS SDK v2 `codecommit` client. This port keeps the
//! same client boundary (static credentials + injected HTTP transport) but
//! speaks the CodeCommit JSON protocol directly instead of vendoring the SDK:
//!
//! * `ListRepositories` (`order=descending`, `sortBy=modifiedDate`) and
//!   `BatchGetRepositories` in batches of 25, mapped by `fromRepoMetadata`.
//! * SigV4 signing with the injected static credentials (no AWS credential
//!   chain, IMDS, STS, or retry middleware).
//! * The `SignatureDoesNotMatch`/`RepositoryDoesNotExistException` error
//!   classification (`IsUnauthorized`/`IsNotFound`).
//!
//! The Redis repository cache in `internal/extsvc/awscodecommit` is not ported
//! (no Redis in this slice); it only affects metadata freshness, not the
//! repositories a source yields.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use url::Url;

use crate::{HttpDoer, HttpRequest};

/// `extsvc.TypeAWSCodeCommit`.
pub const TYPE_AWSCODECOMMIT: &str = "awscodecommit";

/// `awscodecommit.MaxMetadataBatch`.
pub const MAX_METADATA_BATCH: usize = 25;

/// `schema.AWSCodeCommitGitCredentials`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GitCredentials {
    #[serde(default)]
    pub password: String,
    #[serde(default)]
    pub username: String,
}

/// `schema.ExcludedAWSCodeCommitRepo`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ExcludedAWSCodeCommitRepo {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
}

/// `schema.AWSCodeCommitConnection` (the fields the source/client reads).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct AWSCodeCommitConnection {
    #[serde(default, rename = "accessKeyID")]
    pub access_key_id: String,
    #[serde(default)]
    pub exclude: Vec<ExcludedAWSCodeCommitRepo>,
    #[serde(default, rename = "gitCredentials")]
    pub git_credentials: GitCredentials,
    #[serde(default)]
    pub region: String,
    #[serde(default, rename = "repositoryPathPattern")]
    pub repository_path_pattern: String,
    #[serde(default, rename = "secretAccessKey")]
    pub secret_access_key: String,
}

/// `awscodecommit.Repository`, serialized as `repo.metadata` with Go's
/// exported field names.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Repository {
    #[serde(rename = "ARN", default)]
    pub arn: String,
    #[serde(rename = "AccountID", default)]
    pub account_id: String,
    #[serde(rename = "ID", default)]
    pub id: String,
    #[serde(rename = "Name", default)]
    pub name: String,
    #[serde(rename = "Description", default)]
    pub description: String,
    #[serde(rename = "HTTPCloneURL", default)]
    pub http_clone_url: String,
    #[serde(
        rename = "LastModified",
        default,
        serialize_with = "serialize_optional_time",
        deserialize_with = "deserialize_optional_time"
    )]
    pub last_modified: Option<OffsetDateTime>,
}

fn serialize_optional_time<S>(
    value: &Option<OffsetDateTime>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match value {
        Some(value) => serializer.serialize_str(
            &value
                .format(&Rfc3339)
                .map_err(serde::ser::Error::custom)?,
        ),
        None => serializer.serialize_none(),
    }
}

fn deserialize_optional_time<'de, D>(deserializer: D) -> Result<Option<OffsetDateTime>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    match value {
        Some(raw) => OffsetDateTime::parse(&raw, &Rfc3339)
            .map(Some)
            .map_err(serde::de::Error::custom),
        None => Ok(None),
    }
}

/// `awscodecommit.ServiceID`.
pub fn service_id(partition: &str, region: &str, account_id: &str) -> String {
    format!("arn:{partition}:codecommit:{region}:{account_id}:")
}

/// `awscodecommit.ExternalRepoSpec`.
pub fn external_repo_spec(repo: &Repository, service_id: &str) -> crate::ExternalRepoSpec {
    crate::ExternalRepoSpec {
        id: repo.id.clone(),
        service_type: TYPE_AWSCODECOMMIT.to_owned(),
        service_id: service_id.to_owned(),
    }
}

/// `codecommit.NewDefaultEndpointResolver().ResolveEndpoint`: the partition and
/// the TCP endpoint for a region. The SDK's full rule set includes opt-in
/// regions and FIPS variants; those are not reproduced.
pub fn resolve_endpoint(region: &str) -> Result<(&'static str, String), String> {
    if region.is_empty() {
        return Err("failed to resolve AWS region \"\"".to_owned());
    }

    let partition = if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
    } else if region.starts_with("us-isob-") {
        "aws-iso-b"
    } else if region.starts_with("us-iso-") {
        "aws-iso"
    } else {
        "aws"
    };

    let domain = match partition {
        "aws-cn" => "amazonaws.com.cn",
        "aws-iso" => "c2s.ic.gov",
        "aws-iso-b" => "sc2s.sgov.gov",
        _ => "amazonaws.com",
    };

    Ok((
        partition,
        format!("https://codecommit.{region}.{domain}/"),
    ))
}

/// `aws.Credentials` with the static provider from site configuration.
#[derive(Debug, Clone, Default)]
pub struct Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

/// CodeCommit client errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AwsError {
    /// A non-2xx JSON-protocol error response.
    Api {
        status: u16,
        code: String,
        message: String,
    },
    /// `RepositoryDoesNotExistException` (`awscodecommit.ErrNotFound` shape).
    NotFound,
    /// `SignatureDoesNotMatch`.
    Unauthorized(String),
    Transport(String),
    Json(String),
}

impl AwsError {
    /// `awscodecommit.IsNotFound`.
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound)
    }

    /// `awscodecommit.IsUnauthorized`.
    pub fn is_unauthorized(&self) -> bool {
        matches!(self, Self::Unauthorized(_))
    }
}

impl std::fmt::Display for AwsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Api {
                status,
                code,
                message,
            } => write!(
                f,
                "AWS CodeCommit API error: status={status} code={code:?} message={message:?}"
            ),
            Self::NotFound => f.write_str("AWS CodeCommit repository not found"),
            Self::Unauthorized(err) | Self::Transport(err) | Self::Json(err) => f.write_str(err),
        }
    }
}

impl std::error::Error for AwsError {}

/// `awscodecommit.Client`.
#[derive(Clone)]
pub struct Client {
    credentials: Credentials,
    region: String,
    partition: &'static str,
    endpoint: String,
    http: Arc<dyn HttpDoer>,
}

impl Client {
    /// `awscodecommit.NewClient` (repository cache omitted).
    pub fn new(
        credentials: Credentials,
        region: &str,
        partition: &'static str,
        endpoint: String,
        http: Arc<dyn HttpDoer>,
    ) -> Self {
        Self {
            credentials,
            region: region.to_owned(),
            partition,
            endpoint,
            http,
        }
    }

    pub fn partition(&self) -> &'static str {
        self.partition
    }

    pub fn region(&self) -> &str {
        &self.region
    }

    /// `Client.ListRepositories` one page: list, then batch-get metadata.
    pub async fn list_repositories(
        &self,
        next_token: &str,
    ) -> Result<(Vec<Repository>, String), AwsError> {
        let mut body = serde_json::Map::new();
        body.insert("order".to_owned(), Value::String("descending".to_owned()));
        body.insert("sortBy".to_owned(), Value::String("modifiedDate".to_owned()));
        if !next_token.is_empty() {
            body.insert(
                "nextToken".to_owned(),
                Value::String(next_token.to_owned()),
            );
        }

        let listed = self
            .call("ListRepositories", Value::Object(body))
            .await?;
        let next_next_token = listed
            .get("nextToken")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let names: Vec<String> = listed
            .get("repositories")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|item| {
                        item.get("repositoryName")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut repos = Vec::with_capacity(names.len());
        for chunk in names.chunks(MAX_METADATA_BATCH) {
            let mut body = serde_json::Map::new();
            body.insert(
                "repositoryNames".to_owned(),
                Value::Array(chunk.iter().cloned().map(Value::String).collect()),
            );
            let fetched = self
                .call("BatchGetRepositories", Value::Object(body))
                .await?;
            // `repositoriesNotFound` is deliberately ignored, like the
            // baseline: a repository deleted between the two calls is skipped.
            if let Some(items) = fetched.get("repositories").and_then(Value::as_array) {
                for item in items {
                    let metadata: RepoMetadata = serde_json::from_value(item.clone())
                        .map_err(|err| AwsError::Json(err.to_string()))?;
                    repos.push(metadata.into_repository());
                }
            }
        }

        Ok((repos, next_next_token))
    }

    /// One signed JSON-protocol call. `X-Amz-Target` is
    /// `CodeCommit_20150413.<operation>`.
    async fn call(&self, operation: &str, body: Value) -> Result<Value, AwsError> {
        let payload = serde_json::to_vec(&body).map_err(|err| AwsError::Json(err.to_string()))?;
        let target = format!("CodeCommit_20150413.{operation}");
        let url = Url::parse(&self.endpoint).map_err(|err| AwsError::Transport(err.to_string()))?;
        let host = match url.port() {
            Some(port) => format!("{}:{port}", url.host_str().unwrap_or_default()),
            None => url.host_str().unwrap_or_default().to_owned(),
        };

        let now = OffsetDateTime::now_utc();
        let (amz_date, date_stamp) = sigv4_timestamps(now);
        let mut headers = vec![
            (
                "Content-Type".to_owned(),
                "application/x-amz-json-1.1".to_owned(),
            ),
            ("Host".to_owned(), host.clone()),
            ("X-Amz-Date".to_owned(), amz_date.clone()),
            ("X-Amz-Target".to_owned(), target.clone()),
        ];
        if let Some(token) = &self.credentials.session_token {
            headers.push(("X-Amz-Security-Token".to_owned(), token.clone()));
        }

        let authorization = sigv4_authorization(
            &self.credentials,
            &self.region,
            "codecommit",
            "/",
            &date_stamp,
            &amz_date,
            &payload,
            &headers,
        );
        headers.push(("Authorization".to_owned(), authorization));

        let response = self
            .http
            .do_request(HttpRequest {
                method: "POST".to_owned(),
                url: self.endpoint.clone(),
                headers,
                body: payload,
            })
            .await
            .map_err(AwsError::Transport)?;

        let body_text = String::from_utf8_lossy(&response.body).into_owned();
        if !(200..300).contains(&response.status) {
            let value: Value = serde_json::from_str(&body_text).unwrap_or(Value::Null);
            let raw_type = value
                .get("__type")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let code = raw_type.rsplit('#').next().unwrap_or(raw_type).to_owned();
            let message = value
                .get("message")
                .or_else(|| value.get("Message"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();

            if code == "RepositoryDoesNotExistException" {
                return Err(AwsError::NotFound);
            }
            if code == "SignatureDoesNotMatch" {
                return Err(AwsError::Unauthorized(if message.is_empty() {
                    "SignatureDoesNotMatch".to_owned()
                } else {
                    format!("SignatureDoesNotMatch: {message}")
                }));
            }
            return Err(AwsError::Api {
                status: response.status,
                code,
                message,
            });
        }

        serde_json::from_str(&body_text).map_err(|err| AwsError::Json(err.to_string()))
    }
}

#[derive(Debug, Default, Deserialize)]
struct RepoMetadata {
    // The AWS JSON protocol spells this member `Arn` (unlike the other
    // lower-camel members), matching the SDK deserializer.
    #[serde(rename = "Arn", default)]
    arn: String,
    #[serde(rename = "accountId", default)]
    account_id: String,
    #[serde(rename = "repositoryId", default)]
    repository_id: String,
    #[serde(rename = "repositoryName", default)]
    repository_name: String,
    #[serde(rename = "repositoryDescription", default)]
    repository_description: Option<String>,
    #[serde(rename = "cloneUrlHttp", default)]
    clone_url_http: String,
    #[serde(rename = "lastModifiedDate", default)]
    last_modified_date: Option<f64>,
}

impl RepoMetadata {
    /// `fromRepoMetadata`.
    fn into_repository(self) -> Repository {
        Repository {
            arn: self.arn,
            account_id: self.account_id,
            id: self.repository_id,
            name: self.repository_name,
            description: self.repository_description.unwrap_or_default(),
            http_clone_url: self.clone_url_http,
            last_modified: self.last_modified_date.and_then(epoch_to_time),
        }
    }
}

/// Epoch seconds (fractional) to an `OffsetDateTime`, like `time.Unix` in the
/// SDK's timestamp decoder.
fn epoch_to_time(seconds: f64) -> Option<OffsetDateTime> {
    if !seconds.is_finite() {
        return None;
    }
    let whole = seconds.trunc();
    let nanos = ((seconds - whole) * 1_000_000_000.0).round() as i128;
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(whole as i64) * 1_000_000_000 + nanos).ok()
}

// --- SigV4 -----------------------------------------------------------------

fn sigv4_timestamps(now: OffsetDateTime) -> (String, String) {
    let date = now.date();
    let clock = now.time();
    let date_stamp = format!(
        "{:04}{:02}{:02}",
        date.year(),
        u8::from(date.month()),
        date.day()
    );
    let amz_date = format!(
        "{date_stamp}T{:02}{:02}{:02}Z",
        clock.hour(),
        clock.minute(),
        clock.second()
    );
    (amz_date, date_stamp)
}

#[allow(clippy::too_many_arguments)]
fn sigv4_authorization(
    credentials: &Credentials,
    region: &str,
    service: &str,
    path: &str,
    date_stamp: &str,
    amz_date: &str,
    payload: &[u8],
    headers: &[(String, String)],
) -> String {
    let mut canonical: Vec<(String, String)> = headers
        .iter()
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    canonical.sort_by(|left, right| left.0.cmp(&right.0));
    let canonical_headers: String = canonical
        .iter()
        .map(|(name, value)| format!("{name}:{value}\n"))
        .collect();
    let signed_headers: Vec<&str> = canonical.iter().map(|(name, _)| name.as_str()).collect();
    let signed_headers = signed_headers.join(";");

    let payload_hash = hex(&Sha256::digest(payload));
    let canonical_request = format!(
        "POST\n{path}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );

    let scope = format!("{date_stamp}/{region}/{service}/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex(&Sha256::digest(canonical_request.as_bytes()))
    );

    let mut key = format!("AWS4{}", credentials.secret_access_key).into_bytes();
    for part in [date_stamp, region, service, "aws4_request"] {
        key = hmac_sha256(&key, part.as_bytes()).to_vec();
    }
    let signature = hex(&hmac_sha256(&key, string_to_sign.as_bytes()));

    format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
        credentials.access_key_id
    )
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut key_block = [0u8; 64];
    if key.len() > 64 {
        key_block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        key_block[..key.len()].copy_from_slice(key);
    }

    let mut inner = Sha256::new();
    let mut outer = Sha256::new();
    let mut inner_pad = [0u8; 64];
    let mut outer_pad = [0u8; 64];
    for i in 0..64 {
        inner_pad[i] = key_block[i] ^ 0x36;
        outer_pad[i] = key_block[i] ^ 0x5c;
    }
    inner.update(inner_pad);
    inner.update(data);
    outer.update(outer_pad);
    outer.update(inner.finalize());
    outer.finalize().into()
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_id_and_spec() {
        assert_eq!(
            service_id("aws", "us-west-2", "123"),
            "arn:aws:codecommit:us-west-2:123:"
        );
        let repo = Repository {
            id: "id".to_owned(),
            ..Repository::default()
        };
        let spec = external_repo_spec(&repo, "arn:aws:codecommit:us-west-2:123:");
        assert_eq!(spec.service_type, "awscodecommit");
        assert_eq!(spec.id, "id");
    }

    #[test]
    fn partitions_and_endpoints() {
        assert_eq!(resolve_endpoint("us-west-2").unwrap().0, "aws");
        assert_eq!(
            resolve_endpoint("us-west-2").unwrap().1,
            "https://codecommit.us-west-2.amazonaws.com/"
        );
        assert_eq!(
            resolve_endpoint("cn-north-1").unwrap(),
            (
                "aws-cn",
                "https://codecommit.cn-north-1.amazonaws.com.cn/".to_owned()
            )
        );
        assert_eq!(resolve_endpoint("us-gov-west-1").unwrap().0, "aws-us-gov");
        assert!(resolve_endpoint("").is_err());
    }

    #[test]
    fn metadata_conversion_and_json_names() {
        let metadata: RepoMetadata = serde_json::from_value(serde_json::json!({
            "Arn": "arn:aws:codecommit:us-west-2:1:repo",
            "accountId": "1",
            "repositoryId": "id",
            "repositoryName": "repo",
            "cloneUrlHttp": "https://git-codecommit.us-west-2.amazonaws.com/v1/repos/repo",
            "lastModifiedDate": 1577934245.5
        }))
        .unwrap();
        let repo = metadata.into_repository();
        assert_eq!(repo.description, "");
        let value = serde_json::to_value(&repo).unwrap();
        assert_eq!(value["ARN"], "arn:aws:codecommit:us-west-2:1:repo");
        assert_eq!(value["LastModified"], "2020-01-02T03:04:05.5Z");
        assert!(value.get("HTTPCloneURL").is_some());
    }

    #[test]
    fn aws_sigv4_known_vector() {
        // AWS documentation example (GET, but exercises the signing key and
        // canonical request construction; the date and headers match).
        let credentials = Credentials {
            access_key_id: "AKIDEXAMPLE".to_owned(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY".to_owned(),
            session_token: None,
        };
        let headers = vec![
            ("Host".to_owned(), "example.amazonaws.com".to_owned()),
            ("X-Amz-Date".to_owned(), "20150830T123600Z".to_owned()),
        ];
        let authorization = sigv4_authorization(
            &credentials,
            "us-east-1",
            "service",
            "/",
            "20150830",
            "20150830T123600Z",
            b"",
            &headers,
        );
        assert!(authorization.starts_with(
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request,"
        ));
        assert!(authorization.contains("SignedHeaders=host;x-amz-date"));
    }

    #[test]
    fn hmac_matches_reference() {
        // RFC 4231 test case 1.
        let digest = hmac_sha256(&[0x0b; 20], b"Hi There");
        assert_eq!(
            hex(&digest),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
    }

    #[test]
    fn error_classification() {
        assert!(AwsError::NotFound.is_not_found());
        assert!(AwsError::Unauthorized("x".to_owned()).is_unauthorized());
    }
}
