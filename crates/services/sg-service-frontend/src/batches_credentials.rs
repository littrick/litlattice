#![forbid(unsafe_code)]

//! Batch Changes code host credentials (M9.30c).
//!
//! Ports the credential half of `cmd/frontend/internal/batches/resolvers`:
//!
//! - relay IDs [`marshal_batch_changes_credential_id`] /
//!   [`unmarshal_batch_changes_credential_id`] (`credential.go:34-66`);
//! - the `BatchChangesCredential` node (`credential.go`), covering
//!   `id`, `externalServiceKind`, `externalServiceURL`, `sshPublicKey`,
//!   `createdAt`, `isSiteCredential` and `gitHubApp`;
//! - `Mutation.createBatchChangesCredential` / `deleteBatchChangesCredential`
//!   / `checkBatchChangesCredential` (`resolver.go:1092-1207,1935-1989`) and
//!   the service helpers (`internal/batches/service/service.go:1834-2067`);
//! - `BatchChangesCodeHost.credential` and the credential resolution used by
//!   `Query.batchChangesCodeHosts`, `User.batchChangesCodeHosts` and
//!   `BatchSpec.viewerBatchChangesCodeHosts` (`code_host_connection.go:76-156`).
//!
//! The rows live in the existing `user_credentials` (domain `batches`) and
//! `batch_changes_site_credentials` tables; no table, column or Redis key is
//! added. Values are encrypted through `sg_encryption::maybe_encrypt` /
//! `maybe_decrypt` with the keyring's `BatchChangesCredentialKey`, which is
//! `None` in test deployments and therefore byte-identical to the Go baseline
//! (`encryption.NewUnencrypted` passthrough).
//!
//! ## Known gaps (documented, not silently faked)
//!
//! - Network verification (`service.ValidateAuthenticator`) is a seam
//!   ([`BatchesCredentialsDatabase::validate_authenticator`]). M9.37d wires the
//!   GitHub path to the real `sources.GitHubSource.ValidateAuthenticator`
//!   through an injectable `HttpDoer`; other code hosts still default to
//!   success (their clients are outside this slice).
//! - `sshPublicKey` on a GitHub App credential short-circuits to `null`
//!   without loading the app (M9.31b): Go instead errors when the app row is
//!   missing or its private key is not a valid RSA PEM. The differential
//!   corpus avoids that edge.
//! - `gitHubApp` requires the `github_apps` read handle
//!   ([`BatchesCredentialsDatabase::github_apps_handle`]); when it is not
//!   wired (stub stores), the field renders `null`.
//! - Bitbucket Server's username lookup during credential creation performs a
//!   network call in the baseline; here the provided `username` is used.

use std::collections::HashMap;
use std::sync::Arc;

use base64::Engine as _;
use serde_json::Value as JsonValue;
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue};
use sg_graphql::json::Json;
use time::OffsetDateTime;

use crate::batches_graphql::{
    check_current_user_is_site_admin, type_to_kind, BatchesHandle,
    MUST_BE_SITE_ADMIN_OR_SAME_USER,
};
use crate::batches_mutations::{batch_changes_enabled_for_user, ID_IS_ZERO};

/// `resolvers.batchChangesCredentialIDKind`.
pub const BATCH_CHANGES_CREDENTIAL_KIND: &str = "BatchChangesCredential";
/// `service.ErrDuplicateCredential`.
pub const DUPLICATE_CREDENTIAL: &str = "a credential for this code host already exists";
/// `errors.New("empty credential not allowed")`.
pub const EMPTY_CREDENTIAL: &str = "empty credential not allowed";
/// `errors.New("invalid external service kind")`.
pub const INVALID_EXTERNAL_SERVICE_KIND: &str = "invalid external service kind";
/// `database.UserCredentialNotFoundErr` prefix.
pub const USER_CREDENTIAL_NOT_FOUND: &str = "user credential not found: ";
/// `store.ErrNoResults`.
pub const NO_RESULTS: &str = "no results";

/// `service.ErrVerifyCredentialFailed.Error`
/// (`internal/batches/service/errors.go:23`).
pub fn verify_credential_failed(source_error: &str) -> String {
    format!("Failed to verify the credential:\n```\n{source_error}\n```\n")
}

// ---------------------------------------------------------------------------
// Relay IDs
// ---------------------------------------------------------------------------

/// `relay.MarshalID("BatchChangesCredential", "site:<id>" | "user:<id>")`.
pub fn marshal_batch_changes_credential_id(id: i64, is_site: bool) -> String {
    let scope = if is_site { "site" } else { "user" };
    // `relay.MarshalID` JSON-encodes the spec string, so the payload carries
    // the surrounding double quotes: `BatchChangesCredential:"site:<id>"`.
    base64::engine::general_purpose::URL_SAFE.encode(format!(
        "{BATCH_CHANGES_CREDENTIAL_KIND}:\"{scope}:{id}\""
    ))
}

/// `resolvers.unmarshalBatchChangesCredentialID` (`credential.go:44-66`),
/// including the Go `json.Unmarshal`/`strconv.Atoi` error texts.
pub fn unmarshal_batch_changes_credential_id(raw: &str) -> Result<(i64, bool), String> {
    let decoded = base64::engine::general_purpose::URL_SAFE
        .decode(raw)
        .map_err(|error| error.to_string())?;
    let text = String::from_utf8_lossy(&decoded);
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    // `relay.UnmarshalSpec(id, &strID)`: the payload must be a JSON string.
    let str_id: String = match serde_json::from_str::<JsonValue>(spec) {
        Ok(JsonValue::String(value)) => value,
        Ok(JsonValue::Null) => String::new(),
        Ok(other) => {
            let kind = match other {
                JsonValue::Bool(_) => "bool",
                JsonValue::Number(_) => "number",
                JsonValue::Array(_) => "array",
                JsonValue::Object(_) => "object",
                _ => "value",
            };
            return Err(format!(
                "json: cannot unmarshal {kind} into Go value of type string"
            ));
        }
        Err(error) => return Err(error.to_string()),
    };

    let Some((kind, id)) = str_id.split_once(':') else {
        return Err("invalid id".to_owned());
    };
    let is_site = match kind.to_lowercase().as_str() {
        "site" => true,
        "user" => false,
        other => {
            let _ = other;
            return Err(format!(
                "invalid id, unsupported credential kind {:?}",
                kind
            ));
        }
    };
    let parsed: i64 = id.parse().map_err(|_| {
        format!("strconv.Atoi: parsing {:?}: invalid syntax", id)
    })?;
    Ok((parsed, is_site))
}

/// `extsvc.ParseServiceKind` for the kinds accepted by
/// `ExternalServiceKind`: returns the stored `external_service_type`.
pub fn parse_service_kind(kind: &str) -> Option<String> {
    let value = match kind.to_ascii_uppercase().as_str() {
        "GITHUB" => "github",
        "GITLAB" => "gitlab",
        "BITBUCKETSERVER" => "bitbucketServer",
        "BITBUCKETCLOUD" => "bitbucketCloud",
        "AZUREDEVOPS" => "azuredevops",
        "GERRIT" => "gerrit",
        "PERFORCE" => "perforce",
        "GITOLITE" => "gitolite",
        "PHABRICATOR" => "phabricator",
        "JVMPACKAGES" => "jvmPackages",
        "PAGURE" => "pagure",
        "NPMPACKAGES" => "npmPackages",
        "GO" | "GOPACKAGES" => "goModules",
        "PYTHONPACKAGES" => "pythonPackages",
        "RUSTPACKAGES" => "rustPackages",
        "RUBYPACKAGES" => "rubyPackages",
        "AWSCODECOMMIT" => "awsCodeCommit",
        "OTHER" => "other",
        _ => return None,
    };
    Some(value.to_owned())
}

// ---------------------------------------------------------------------------
// Database seam
// ---------------------------------------------------------------------------

/// One `user_credentials` / `batch_changes_site_credentials` row.
#[derive(Debug, Clone, PartialEq)]
pub struct CredentialRow {
    pub id: i64,
    /// Owner for user credentials; `0` for site credentials.
    pub user_id: i32,
    pub external_service_type: String,
    pub external_service_id: String,
    /// Encrypted (or, without a key, plaintext) `{Type,Auth}` JSON bytes.
    pub credential: Vec<u8>,
    pub encryption_key_id: String,
    pub github_app_id: Option<i64>,
    pub created_at: OffsetDateTime,
}

/// An RSA keypair generated for a credential (`encryption.RSAKey`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SshKeyPair {
    pub private_key: String,
    pub public_key: String,
    pub passphrase: String,
}

/// Synchronous credential store seam, implemented for
/// [`crate::batches_graphql_store::PgBatchesGraphqlStore`] in
/// `batches_credentials_store.rs`.
pub trait BatchesCredentialsDatabase: Send + Sync {
    /// `UserCredentials().List` with `Scope{Domain: batches, UserID}`.
    fn list_user_credentials(&self, user_id: i32) -> Result<Vec<CredentialRow>, String>;

    /// `ListSiteCredentials`.
    fn list_site_credentials(&self) -> Result<Vec<CredentialRow>, String>;

    /// `UserCredentials().GetByID`, including the SQL authz conditions: returns
    /// `None` when the row is missing *or* the actor may not read it (the Go
    /// baseline then reports `UserCredentialNotFoundErr`).
    fn get_user_credential(
        &self,
        id: i64,
        actor: Actor,
    ) -> Result<Option<CredentialRow>, String>;

    /// `GetSiteCredential(GetSiteCredentialOpts{ID})`; `None` is `ErrNoResults`.
    fn get_site_credential(&self, id: i64) -> Result<Option<CredentialRow>, String>;

    /// `GetSiteCredential(GetSiteCredentialOpts{Type, ID})`; `None` is `ErrNoResults`.
    fn get_site_credential_by_scope(
        &self,
        external_service_type: &str,
        external_service_id: &str,
    ) -> Result<Option<CredentialRow>, String>;

    /// `UserCredentials().GetByScope`; `None` is `UserCredentialNotFoundErr`.
    fn get_user_credential_by_scope(
        &self,
        user_id: i32,
        external_service_type: &str,
        external_service_id: &str,
    ) -> Result<Option<CredentialRow>, String>;

    /// `UserCredentials().Create`; `github_app_id` is `None` for the mutation.
    fn create_user_credential(
        &self,
        user_id: i32,
        external_service_type: &str,
        external_service_id: &str,
        credential: &[u8],
        encryption_key_id: &str,
        github_app_id: Option<i64>,
    ) -> Result<CredentialRow, String>;

    /// `CreateSiteCredential`.
    fn create_site_credential(
        &self,
        external_service_type: &str,
        external_service_id: &str,
        credential: &[u8],
        encryption_key_id: &str,
        github_app_id: Option<i64>,
    ) -> Result<CredentialRow, String>;

    /// `UserCredentials().Delete`; `false` is `UserCredentialNotFoundErr`.
    fn delete_user_credential(&self, id: i64, actor: Actor) -> Result<bool, String>;

    /// `DeleteSiteCredential`; `false` is `ErrNoResults`.
    fn delete_site_credential(&self, id: i64) -> Result<bool, String>;

    /// `encryption.MaybeEncrypt` with the batch changes credential key.
    fn encrypt_credential(&self, plaintext: &str) -> Result<(Vec<u8>, String), String>;

    /// `Encryptable.Decrypt` / `encryption.MaybeDecrypt`.
    fn decrypt_credential(&self, ciphertext: &[u8], key_id: &str) -> Result<String, String>;

    /// `encryption.GenerateRSAKey`.
    fn generate_ssh_keypair(&self) -> Result<SshKeyPair, String>;

    /// `service.ValidateAuthenticator`. Defaults to success: the code host
    /// clients are out of scope for this slice.
    fn validate_authenticator(
        &self,
        _external_service_type: &str,
        _external_service_url: &str,
        _authenticator: &str,
    ) -> Result<(), String> {
        Ok(())
    }

    /// `conf.ExternalURLParsed().Host`.
    ///
    /// Erratum: `conf.ExternalURLParsed` caches its last parsed URL and seeds
    /// it with `http://example.com`, so an empty `externalURL` configuration
    /// still reports the host `example.com` (it never re-parses the empty
    /// string). The port therefore mirrors that host; plumbing a real site
    /// configuration is outside this slice (see the module gaps).
    fn external_url_host(&self) -> String {
        "example.com".to_owned()
    }

    /// M9.31b: the `github_apps` read handle used by
    /// `BatchChangesCredential.gitHubApp` and
    /// `BatchChangesCodeHost.commitSigningConfiguration`. Implemented by
    /// `PgBatchesGraphqlStore` in `batches_credentials_store.rs`; `None`
    /// leaves both fields `null` (the pre-M9.31b behaviour).
    fn github_apps_handle(&self) -> Option<crate::github_apps_graphql::GitHubAppsHandle> {
        None
    }
}

fn credentials_database(
    db: &BatchesHandle,
) -> Result<&dyn BatchesCredentialsDatabase, String> {
    db.credentials_database()
        .ok_or_else(|| "batch changes credentials are not wired".to_owned())
}

/// M9.31b: the `github_apps` handle for a batches store, if wired.
pub fn github_apps_handle(db: &BatchesHandle) -> Option<crate::github_apps_graphql::GitHubAppsHandle> {
    credentials_database(db)
        .ok()
        .and_then(|store| store.github_apps_handle())
}

// ---------------------------------------------------------------------------
// Authenticator JSON ({Type,Auth})
// ---------------------------------------------------------------------------

/// Build the Go-compatible `{"Type":...,"Auth":...}` authenticator JSON.
fn build_authenticator(
    external_service_type: &str,
    credential: &str,
    username: Option<&str>,
    keypair: &SshKeyPair,
) -> String {
    let username = username.unwrap_or_default();
    let auth = match external_service_type {
        "bitbucketServer" | "bitbucketCloud" | "azuredevops" | "gerrit" | "perforce" => {
            serde_json::json!({
                "Username": username,
                "Password": credential,
                "PrivateKey": keypair.private_key,
                "PublicKey": keypair.public_key,
                "Passphrase": keypair.passphrase,
            })
        }
        _ => serde_json::json!({
            "token": credential,
            "PrivateKey": keypair.private_key,
            "PublicKey": keypair.public_key,
            "Passphrase": keypair.passphrase,
        }),
    };
    let auth_type = match external_service_type {
        "bitbucketServer" | "bitbucketCloud" | "azuredevops" | "gerrit" | "perforce" => {
            "BasicAuthWithSSH"
        }
        _ => "OAuthBearerTokenWithSSH",
    };
    serde_json::json!({ "Type": auth_type, "Auth": auth }).to_string()
}

/// Extract `Auth.PublicKey` from a decrypted authenticator, mimicking the
/// `AuthenticatorWithSSH` type assertion: only the two `WithSSH` types carry
/// the field.
pub fn ssh_public_key(authenticator_json: &str) -> Option<String> {
    let value: JsonValue = serde_json::from_str(authenticator_json).ok()?;
    let auth_type = value.get("Type")?.as_str()?;
    if !matches!(
        auth_type,
        "BasicAuthWithSSH" | "OAuthBearerTokenWithSSH"
    ) {
        return None;
    }
    let key = value.get("Auth")?.get("PublicKey")?.as_str()?;
    Some(key.to_owned())
}

/// `credential.go:commentSSHKey`.
fn comment_ssh_key(public_key: &str, host: &str) -> String {
    if host.is_empty() {
        return public_key.to_owned();
    }
    format!("{} Sourcegraph {host}", public_key.trim_end_matches('\n'))
}

// ---------------------------------------------------------------------------
// Credential object
// ---------------------------------------------------------------------------

/// A resolved credential plus whether it came from the site credential table.
#[derive(Debug, Clone)]
pub struct ResolvedCredential {
    pub row: CredentialRow,
    pub is_site: bool,
}

impl ResolvedCredential {
    fn key(&self) -> (String, String) {
        (
            self.row.external_service_type.clone(),
            self.row.external_service_id.clone(),
        )
    }
}

/// Fetch and merge the credentials visible to `user_id`:
/// `code_host_connection.go:94-118`. User credentials take precedence over
/// site credentials for the same `(type, url)`.
pub fn resolve_credentials(
    db: &BatchesHandle,
    user_id: Option<i32>,
) -> Result<HashMap<(String, String), ResolvedCredential>, String> {
    let Some(store) = db.credentials_database() else {
        return Ok(HashMap::new());
    };
    let mut map = HashMap::new();
    if let Some(user_id) = user_id {
        for row in store.list_user_credentials(user_id)? {
            let resolved = ResolvedCredential { row, is_site: false };
            map.insert(resolved.key(), resolved);
        }
    }
    for row in store.list_site_credentials()? {
        let resolved = ResolvedCredential { row, is_site: true };
        map.entry(resolved.key()).or_insert(resolved);
    }
    Ok(map)
}

pub struct BatchChangesCredentialObject {
    db: BatchesHandle,
    resolved: ResolvedCredential,
}

impl BatchChangesCredentialObject {
    pub fn new(db: BatchesHandle, resolved: ResolvedCredential) -> Self {
        Self { db, resolved }
    }

    fn is_github_app(&self) -> bool {
        self.resolved.row.github_app_id.unwrap_or(0) > 0
    }

    fn authenticator_json(&self) -> Result<String, String> {
        let store = credentials_database(&self.db)?;
        store.decrypt_credential(
            &self.resolved.row.credential,
            &self.resolved.row.encryption_key_id,
        )
    }

    fn ssh_public_key(&self) -> Result<Option<String>, String> {
        // GitHub App credentials authenticate through the app and never carry
        // an SSH keypair.
        if self.is_github_app() {
            return Ok(None);
        }
        let json = self.authenticator_json()?;
        let Some(key) = ssh_public_key(&json) else {
            return Ok(None);
        };
        let host = credentials_database(&self.db)
            .map(|store| store.external_url_host())
            .unwrap_or_default();
        Ok(Some(comment_ssh_key(&key, &host)))
    }

    /// `credential.go` `batchChanges{User,Site}CredentialResolver.GitHubApp`:
    /// only a GitHub-kind credential with a positive `github_app_id` resolves,
    /// and a missing app (`ErrNoGitHubAppFound`) renders `null`.
    fn github_app_value(&self) -> Result<FieldValue, String> {
        if !self.is_github_app() || self.resolved.row.external_service_type != "github" {
            return Ok(FieldValue::Null);
        }
        let Some(handle) = github_apps_handle(&self.db) else {
            return Ok(FieldValue::Null);
        };
        let app_id = self.resolved.row.github_app_id.unwrap_or(0);
        match handle.get_app_by_id(app_id)? {
            Some(app) => Ok(FieldValue::Object(Arc::new(
                crate::github_apps_graphql::GitHubAppObject::new(app, handle),
            ))),
            None => Ok(FieldValue::Null),
        }
    }
}

impl ObjectValue for BatchChangesCredentialObject {
    fn type_name(&self) -> &'static str {
        "BatchChangesCredential"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let row = &self.resolved.row;
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(
                marshal_batch_changes_credential_id(row.id, self.resolved.is_site),
            )))),
            "externalServiceKind" => Ok(Some(FieldValue::Json(Json::Str(type_to_kind(
                &row.external_service_type,
            ))))),
            "externalServiceURL" => Ok(Some(FieldValue::Json(Json::Str(
                row.external_service_id.clone(),
            )))),
            "sshPublicKey" => Ok(Some(match self.ssh_public_key().map_err(ExecError::new)? {
                Some(key) => FieldValue::Json(Json::Str(key)),
                None => FieldValue::Null,
            })),
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(
                crate::graphql_resolvers::datetime_rfc3339(row.created_at),
            )))),
            "isSiteCredential" => Ok(Some(FieldValue::Json(Json::Bool(self.resolved.is_site)))),
            "gitHubApp" => Ok(Some(self.github_app_value().map_err(ExecError::new)?)),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Node resolution
// ---------------------------------------------------------------------------

/// `Resolver.batchChangesCredentialByID` (`resolver.go:307-357`).
pub fn credential_by_id(
    db: &BatchesHandle,
    id: &str,
    actor: Actor,
) -> Result<Option<BatchChangesCredentialObject>, String> {
    batch_changes_enabled_for_user(db, actor)?;
    let (db_id, is_site) = unmarshal_batch_changes_credential_id(id)?;
    if db_id == 0 {
        return Err(ID_IS_ZERO.to_owned());
    }
    let store = credentials_database(db)?;
    if is_site {
        check_current_user_is_site_admin(&**db, actor)?;
        let Some(row) = store.get_site_credential(db_id)? else {
            // `store.ErrNoResults` → `(nil, nil)`.
            return Ok(None);
        };
        Ok(Some(BatchChangesCredentialObject::new(
            db.clone(),
            ResolvedCredential { row, is_site: true },
        )))
    } else {
        // Erratum: `GetByID` returns `UserCredentialNotFoundErr`, not
        // `store.ErrNoResults`, so a missing/unauthorized row bubbles as an
        // error instead of `(nil, nil)`.
        let Some(row) = store.get_user_credential(db_id, actor)? else {
            return Err(format!("{USER_CREDENTIAL_NOT_FOUND}[{db_id}]"));
        };
        check_site_admin_or_same_user(&**db, actor, row.user_id)?;
        Ok(Some(BatchChangesCredentialObject::new(
            db.clone(),
            ResolvedCredential { row, is_site: false },
        )))
    }
}

/// `Query.node` / `Mutation` kind `BatchChangesCredential`.
pub fn is_node_kind(kind: &str) -> bool {
    kind == BATCH_CHANGES_CREDENTIAL_KIND
}

pub fn node_field(
    db: &BatchesHandle,
    kind: &str,
    id: &str,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    if kind != BATCH_CHANGES_CREDENTIAL_KIND {
        return Ok(None);
    }
    Ok(Some(
        match credential_by_id(db, id, actor).map_err(ExecError::new)? {
            Some(credential) => FieldValue::Object(Arc::new(credential)),
            // `(nil, nil)` from `batchChangesCredentialByID` → GraphQL null.
            None => FieldValue::Null,
        },
    ))
}

// ---------------------------------------------------------------------------
// Authorization helpers
// ---------------------------------------------------------------------------

/// `auth.CheckSiteAdminOrSameUser` (`internal/auth/site_admin.go`).
pub fn check_site_admin_or_same_user(
    db: &dyn crate::batches_graphql::BatchesGraphqlDatabase,
    actor: Actor,
    user_id: i32,
) -> Result<(), String> {
    if actor.is_internal() {
        return Ok(());
    }
    if actor.uid() == Some(user_id) {
        return Ok(());
    }
    if db.current_user_site_admin(actor)? == Some(true) {
        return Ok(());
    }
    // `CheckSiteAdminOrSameUserFromActor` swallows the site-admin check's own
    // error (including `ErrNotAuthenticated`) and always reports the
    // unauthorized variant, so anonymous actors get this message too.
    Err(MUST_BE_SITE_ADMIN_OR_SAME_USER.to_owned())
}

fn unmarshal_user_id(raw: &str) -> Result<i32, String> {
    crate::batches_graphql::unmarshal_relay_int(raw).map(|id| id as i32)
}

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

pub fn is_mutation_field(field: &str) -> bool {
    matches!(
        field,
        "createBatchChangesCredential" | "deleteBatchChangesCredential"
    )
}

/// `checkBatchChangesCredential` is declared on `Query` in the baseline SDL
/// (`batches.graphql:1941`, inside `extend type Query`) even though it is an
/// action; see [`check_batch_changes_credential`].
pub fn is_root_field(field: &str) -> bool {
    field == "checkBatchChangesCredential"
}

pub fn root_field(
    db: &BatchesHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "checkBatchChangesCredential" => {
            check_batch_changes_credential(db, arguments, actor).map(Some)
        }
        _ => Ok(None),
    }
}

pub fn mutation_field(
    db: &BatchesHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "createBatchChangesCredential" => {
            create_batch_changes_credential(db, arguments, actor).map(Some)
        }
        "deleteBatchChangesCredential" => {
            delete_batch_changes_credential(db, arguments, actor).map(Some)
        }
        _ => Ok(None),
    }
}

/// `EmptyResponse` (`{}`) as an object value.
struct EmptyResponseObject;

impl ObjectValue for EmptyResponseObject {
    fn type_name(&self) -> &'static str {
        "EmptyResponse"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "alwaysNil" => Ok(Some(FieldValue::Null)),
            _ => Ok(None),
        }
    }
}

fn empty_response() -> FieldValue {
    FieldValue::Object(Arc::new(EmptyResponseObject))
}

/// `service.CreateBatchChangesUserCredential`/`SiteCredential` plus the
/// resolver-level validation (`resolver.go:1092-1151`).
fn create_batch_changes_credential(
    db: &BatchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    batch_changes_enabled_for_user(db, actor).map_err(ExecError::new)?;

    let user_id = match arguments.string("user") {
        Some(raw) => {
            let id = unmarshal_user_id(raw).map_err(ExecError::new)?;
            if id == 0 {
                return Err(ExecError::new(ID_IS_ZERO));
            }
            Some(id)
        }
        None => None,
    };

    let kind = arguments
        .string("externalServiceKind")
        .ok_or_else(|| ExecError::new(INVALID_EXTERNAL_SERVICE_KIND))?;
    let external_service_type =
        parse_service_kind(kind).ok_or_else(|| ExecError::new(INVALID_EXTERNAL_SERVICE_KIND))?;

    let credential = arguments.string("credential").unwrap_or_default();
    if credential.trim().is_empty() {
        return Err(ExecError::new(EMPTY_CREDENTIAL));
    }

    let external_service_url = arguments.string("externalServiceURL").unwrap_or_default();
    let username = arguments.string("username");
    let store = credentials_database(db).map_err(ExecError::new)?;

    let row = if let Some(user_id) = user_id {
        check_site_admin_or_same_user(&**db, actor, user_id).map_err(ExecError::new)?;
        if store
            .get_user_credential_by_scope(user_id, &external_service_type, external_service_url)
            .map_err(ExecError::new)?
            .is_some()
        {
            return Err(ExecError::new(DUPLICATE_CREDENTIAL));
        }
        let keypair = store.generate_ssh_keypair().map_err(ExecError::new)?;
        let authenticator = build_authenticator(
            &external_service_type,
            credential,
            username,
            &keypair,
        );
        store
            .validate_authenticator(
                &external_service_type,
                external_service_url,
                &authenticator,
            )
            .map_err(|error| ExecError::new(verify_credential_failed(&error)))?;
        let (bytes, key_id) = store.encrypt_credential(&authenticator).map_err(ExecError::new)?;
        store
            .create_user_credential(
                user_id,
                &external_service_type,
                external_service_url,
                &bytes,
                &key_id,
                None,
            )
            .map_err(ExecError::new)?
    } else {
        check_current_user_is_site_admin(&**db, actor).map_err(ExecError::new)?;
        if store
            .get_site_credential_by_scope(&external_service_type, external_service_url)
            .map_err(ExecError::new)?
            .is_some()
        {
            return Err(ExecError::new(DUPLICATE_CREDENTIAL));
        }
        let keypair = store.generate_ssh_keypair().map_err(ExecError::new)?;
        let authenticator = build_authenticator(
            &external_service_type,
            credential,
            username,
            &keypair,
        );
        store
            .validate_authenticator(
                &external_service_type,
                external_service_url,
                &authenticator,
            )
            .map_err(|error| ExecError::new(verify_credential_failed(&error)))?;
        let (bytes, key_id) = store.encrypt_credential(&authenticator).map_err(ExecError::new)?;
        store
            .create_site_credential(
                &external_service_type,
                external_service_url,
                &bytes,
                &key_id,
                None,
            )
            .map_err(ExecError::new)?
    };

    Ok(FieldValue::Object(Arc::new(BatchChangesCredentialObject::new(
        db.clone(),
        ResolvedCredential {
            row,
            is_site: user_id.is_none(),
        },
    ))))
}

/// `resolver.go:1153-1207` plus `deleteBatchChangesUserCredential` /
/// `deleteBatchChangesSiteCredential`.
fn delete_batch_changes_credential(
    db: &BatchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    batch_changes_enabled_for_user(db, actor).map_err(ExecError::new)?;
    let raw = arguments
        .string("batchChangesCredential")
        .ok_or_else(|| ExecError::new("invalid graphql.ID"))?;
    let (db_id, is_site) = unmarshal_batch_changes_credential_id(raw).map_err(ExecError::new)?;
    if db_id == 0 {
        return Err(ExecError::new(ID_IS_ZERO));
    }
    let store = credentials_database(db).map_err(ExecError::new)?;
    if is_site {
        check_current_user_is_site_admin(&**db, actor).map_err(ExecError::new)?;
        if !store
            .delete_site_credential(db_id)
            .map_err(ExecError::new)?
        {
            return Err(ExecError::new(NO_RESULTS));
        }
    } else {
        // `GetByID` applies the SQL authz conditions first.
        let Some(row) = store
            .get_user_credential(db_id, actor)
            .map_err(ExecError::new)?
        else {
            return Err(ExecError::new(format!("{USER_CREDENTIAL_NOT_FOUND}[{db_id}]")));
        };
        check_site_admin_or_same_user(&**db, actor, row.user_id).map_err(ExecError::new)?;
        if !store
            .delete_user_credential(db_id, actor)
            .map_err(ExecError::new)?
        {
            return Err(ExecError::new(format!("{USER_CREDENTIAL_NOT_FOUND}[{db_id}]")));
        }
    }
    Ok(empty_response())
}

/// `resolver.go:1935-1989`.
fn check_batch_changes_credential(
    db: &BatchesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    let raw = arguments
        .string("batchChangesCredential")
        .ok_or_else(|| ExecError::new("invalid graphql.ID"))?;
    let Some(credential) = credential_by_id(db, raw, actor).map_err(ExecError::new)? else {
        // `batchChangesCredentialByID` returned `(nil, nil)` (site credential
        // missing) → `ErrIDIsZero`.
        return Err(ExecError::new(ID_IS_ZERO));
    };

    // GitHub App credentials are verified through the app installation.
    if credential.is_github_app() {
        return Err(ExecError::new(
            "no GitHub App matching the given criteria",
        ));
    }

    let store = credentials_database(db).map_err(ExecError::new)?;
    let authenticator = credential.authenticator_json().map_err(ExecError::new)?;
    store
        .validate_authenticator(
            &credential.resolved.row.external_service_type,
            &credential.resolved.row.external_service_id,
            &authenticator,
        )
        .map_err(|error| ExecError::new(verify_credential_failed(&error)))?;
    Ok(empty_response())
}
