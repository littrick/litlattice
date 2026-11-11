#![forbid(unsafe_code)]

//! The remaining read-only `SiteConfiguration` GraphQL fields
//! (`cmd/frontend/graphqlbackend/site.go:198-292`):
//!
//! - `ValidationMessages` — admin check, then `EffectiveContents` and
//!   `conf.ValidateSite`;
//! - `LicenseInfo` — admin check, then
//!   `licensing.GetConfiguredProductLicenseInfo`;
//! - `History` — admin check, then a `SiteConfigurationChangeConnection`.
//!
//! `ID` and `EffectiveContents` live in [`crate::site_configuration_query`]
//! and are reused, not reimplemented. The write path
//! (`UpdateSiteConfiguration`) lives in
//! [`crate::site_configuration_mutation`] and is untouched.
//!
//! As with the other resolver cores in this crate, the module exposes plain
//! async prefetch functions over injected seams; `graphql_api` injects the
//! results into [`crate::graphql_resolvers::QueryRoot`] (M9.8je for
//! `Query.site`, M9.8jh for `settingsSubject(id: <Site>)`).

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use sg_config::store::ConfigStore;
use sg_config::RawUnified;
use sg_db::conf as conf_db;
use sg_db::feature_flags::{get_global_feature_flags, get_user_flags};
use sg_db::security_event_logs::{
    insert_security_event, security_event_logs_in_database, SecurityEvent as DbSecurityEvent,
};
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue, RootField};
use sg_graphql::json::Json;
use sqlx::types::time::OffsetDateTime;
use sqlx::PgPool;

use crate::graphql_resolvers::{
    check_current_user_is_site_admin, relay_kind, unmarshal_relay_spec_string,
    ConnectionPageInfoObject, UserData, UserKey, UserLookupStore, UserObject,
};
use crate::license_config::{LicenseConfig, PgProductLicenseStore, ProductLicenseStore};
use crate::site_config_change::{
    ConnectionPageInfo, ConnectionResolverArgs, ConnectionResolverOptions, SiteConfigHistoryStore,
    SiteConfigRow as ChangeRow, SiteConfigurationChangeConnection, SiteConfigurationChangeResolver,
};
use crate::site_configuration_query::{
    configuration, effective_contents, id as site_configuration_id, SecurityEvent,
    SiteConfigRow as LatestRow, SiteConfigurationBackend, SiteConfigurationResolver,
};

/// `licenseInfoResolver` (`site.go:233-243`), with `expiresAt` already
/// formatted as `gqlutil.DateTime`'s UTC RFC3339 string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LicenseInfo {
    pub tags: Vec<String>,
    pub user_count: i32,
    pub expires_at: String,
}

/// `licensing.GetConfiguredProductLicenseInfo`.
///
/// `Ok(None)` is the Go `nil` license (GraphQL `null`); `Err` carries the
/// baseline's error text (e.g. an unparseable license key).
#[async_trait]
pub trait LicenseInfoProvider: Send + Sync {
    async fn configured_license(&self) -> Result<Option<LicenseInfo>, String>;
}

/// `conf.ValidateSite` (`internal/conf/validate.go`).
#[async_trait]
pub trait SiteConfigValidator: Send + Sync {
    async fn validate_site(&self, contents: &str) -> Result<Vec<String>, String>;
}

/// `siteConfigurationResolver.ValidationMessages` (`site.go:245-257`).
pub async fn validation_messages(
    backend: &dyn SiteConfigurationBackend,
    validator: &dyn SiteConfigValidator,
    actor: Actor,
    resolver: &SiteConfigurationResolver,
) -> Result<Vec<String>, String> {
    // 🚨 SECURITY: the site configuration contains secret tokens and
    // credentials, so only admins may validate it.
    backend.check_current_user_is_site_admin(actor).await?;
    let contents = effective_contents(backend, actor, resolver).await?;
    validator.validate_site(&contents).await
}

/// `siteConfigurationResolver.LicenseInfo` (`site.go:235-250`).
pub async fn license_info(
    backend: &dyn SiteConfigurationBackend,
    licenses: &dyn LicenseInfoProvider,
    actor: Actor,
) -> Result<Option<LicenseInfo>, String> {
    // 🚨 SECURITY: only site admins can view license information.
    backend.check_current_user_is_site_admin(actor).await?;
    licenses.configured_license().await
}

/// `siteConfigurationResolver.History` (`site.go:259-272`).
///
/// The returned connection borrows `store`; `nodes`, `page_info` and
/// `total_count` are the same methods the `site_config_change` differential
/// already covers.
pub async fn history<'a>(
    backend: &dyn SiteConfigurationBackend,
    store: &'a dyn SiteConfigHistoryStore,
    actor: Actor,
    args: Option<ConnectionResolverArgs>,
    options: ConnectionResolverOptions,
) -> Result<SiteConfigurationChangeConnection<'a>, String> {
    // 🚨 SECURITY: only site admins may view the history.
    backend.check_current_user_is_site_admin(actor).await?;
    Ok(SiteConfigurationChangeConnection::new(store, args, options))
}

// ---------------------------------------------------------------------------
// Production adapters
// ---------------------------------------------------------------------------

/// `conf.ValidateSite` over the process' live raw configuration.
pub struct ConfigStoreSiteConfigValidator {
    store: Arc<ConfigStore>,
}

impl ConfigStoreSiteConfigValidator {
    pub fn new(store: Arc<ConfigStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl SiteConfigValidator for ConfigStoreSiteConfigValidator {
    async fn validate_site(&self, contents: &str) -> Result<Vec<String>, String> {
        let store = Arc::clone(&self.store);
        let contents = contents.to_owned();
        // `conf.Raw()` blocks on the configuration store; keep it off the async
        // executor.
        tokio::task::spawn_blocking(move || validate_site_against_raw(&contents, &store.raw().site))
            .await
            .map_err(|err| format!("validation task failed: {err}"))?
    }
}

/// The synchronous core of `conf.ValidateSite`:
/// `UnredactSecrets(contents, conf.Raw())`, then `conf.Validate`'s JSON Schema
/// problems and the offline subset of `validateCustomRaw`
/// (`internal/conf/validate_custom.go` plus
/// `cmd/frontend/internal/conf/validation/auth.go`).
///
/// The typed `json.Unmarshal` error of `validateCustomRaw` is only reproduced at
/// the top level (see the M9.8jh overview §4.2).
pub fn validate_site_against_raw(contents: &str, raw_site: &str) -> Result<Vec<String>, String> {
    let raw = RawUnified {
        site: raw_site.to_owned(),
        ..RawUnified::default()
    };
    let unredacted = crate::site_configuration_secrets::unredact_secrets(contents, &raw)?;
    let mut messages = sg_site_schema::validate_site(&unredacted).map_err(|err| err.to_string())?;

    // `conf.Validate` parses the JSONC document before `validateCustomRaw`; a
    // parse error would already have surfaced above. `null` is a valid document
    // for `json.Unmarshal` and leaves the typed struct zero-valued.
    let parsed = sg_config::SiteConfiguration::parse(&unredacted).map_err(|err| err.to_string())?;
    messages.extend(validate_custom(parsed.raw(), &parsed)?);
    Ok(messages)
}

/// `validateCustom`'s core rules (email, externalURL, gitUpdateInterval),
/// followed by the contributed validators this port can evaluate offline:
/// auth, encryption (mounted/noop), symbols and txemail. The order mirrors the
/// baseline's `init()` registration order
/// (`cmd/frontend/internal/conf/validation/*.go`).
fn validate_custom(
    site: &Value,
    config: &sg_config::SiteConfiguration,
) -> Result<Vec<String>, String> {
    if !site.is_null() && !site.is_object() {
        // `json.Unmarshal` into `schema.SiteConfiguration` rejects non-objects.
        return Err(format!(
            "json: cannot unmarshal {} into Go value of type schema.SiteConfiguration",
            go_json_type(site)
        ));
    }

    let mut problems = Vec::new();

    let email_address = site
        .get("email.address")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let smtp = site.get("email.smtp").filter(|value| !value.is_null());
    let has_smtp = smtp.is_some();
    let has_smtp_auth = smtp
        .and_then(|value| value.get("authentication"))
        .and_then(Value::as_str)
        .is_some_and(|authentication| authentication != "none");
    if has_smtp && email_address.is_empty() {
        problems.push("should set email.address because email.smtp is set".to_owned());
    }
    if has_smtp_auth {
        let username = smtp
            .and_then(|value| value.get("username"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        let password = smtp
            .and_then(|value| value.get("password"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        if username.is_empty() && password.is_empty() {
            problems.push(
                "must set email.smtp username and password for email.smtp authentication"
                    .to_owned(),
            );
        }
    }

    let external_url = site
        .get("externalURL")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if !external_url.is_empty() {
        match go_url_path(external_url) {
            Err(()) => problems.push("externalURL must be a valid URL".to_owned()),
            Ok(path) if path != "/" && !path.is_empty() => {
                problems.push("externalURL must not be a non-root URL".to_owned());
            }
            Ok(_) => {}
        }
    }

    if let Some(Value::Array(rules)) = site.get("gitUpdateInterval") {
        for rule in rules {
            if let Some(pattern) = rule.get("pattern").and_then(Value::as_str) {
                if regex::Regex::new(pattern).is_err() {
                    problems.push(format!(
                        "GitUpdateIntervalRule pattern is not valid regex: {}",
                        go_quote(pattern)
                    ));
                }
            }
        }
    }

    // Contributed validators registered by
    // `cmd/frontend/internal/conf/validation/auth.go`.
    let empty = Vec::new();
    let providers: &[Value] = match site.get("auth.providers") {
        Some(Value::Array(providers)) => providers,
        _ => &empty,
    };
    if providers.is_empty() {
        problems.push("no auth providers set (all access will be forbidden)".to_owned());
    }
    let has_external_provider = providers
        .iter()
        .any(|provider| provider.get("type").and_then(Value::as_str) != Some("builtin"));
    let enable_username_changes = site
        .get("auth.enableUsernameChanges")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if has_external_provider && enable_username_changes {
        problems.push(
            "`auth.enableUsernameChanges` must not be true if external auth providers are set in `auth.providers`"
                .to_owned(),
        );
    }
    let builtin_providers = providers
        .iter()
        .filter(|provider| provider.get("type").and_then(Value::as_str) == Some("builtin"))
        .count();
    if builtin_providers >= 2 {
        problems.push("at most 1 builtin auth provider may be used".to_owned());
    }

    // `cmd/frontend/internal/conf/validation/encryption.go`.
    if let Some(problem) = contributed_encryption_problem(config) {
        problems.push(problem);
    }
    // `cmd/frontend/internal/conf/validation/symbols.go`.
    if let Some(problem) = contributed_symbols_problem(site) {
        problems.push(problem);
    }
    // `cmd/frontend/internal/conf/validation/txemail.go`.
    problems.extend(contributed_txemail_problems(site));

    Ok(problems)
}

/// `validation/encryption.go` + `keyring.NewRing`: build every configured key
/// in the baseline's field order and report the first failure.
///
/// `mounted` (env var / file, 32-byte secret) and `noop` are evaluated. The
/// AWS/GCP KMS constructors probe their services, so they remain a seam here
/// (overview §4.2).
fn contributed_encryption_problem(config: &sg_config::SiteConfiguration) -> Option<String> {
    let keys = config.encryption_keys()?;
    let ordered = [
        &keys.batch_changes_credential_key,
        &keys.external_service_key,
        &keys.github_app_key,
        &keys.user_external_account_key,
        &keys.webhook_key,
        &keys.webhook_log_key,
        &keys.executor_secret_key,
    ];
    for key in ordered.into_iter().flatten() {
        if let Err(error) = build_encryption_key(key) {
            return Some(format!("Invalid encryption.keys config: {error}"));
        }
    }
    None
}

/// `keyring.NewKey` for the offline key kinds.
fn build_encryption_key(config: &sg_config::EncryptionKeyConfig) -> Result<(), String> {
    match config {
        sg_config::EncryptionKeyConfig::Mounted(mounted) => {
            sg_encryption::MountedKey::from_config(&sg_encryption::MountedKeyConfig {
                keyname: mounted.keyname.clone(),
                version: mounted.version.clone(),
                env_var_name: mounted.env_var_name.clone(),
                filepath: mounted.filepath.clone(),
            })
            .map(|_| ())
            .map_err(|err| err.to_string())
        }
        sg_config::EncryptionKeyConfig::Noop => Ok(()),
        // Network-backed KMS key construction is not ported (seam).
        sg_config::EncryptionKeyConfig::Awskms(_) | sg_config::EncryptionKeyConfig::Cloudkms(_) => {
            Ok(())
        }
        sg_config::EncryptionKeyConfig::Unknown(value) => Err(format!(
            "couldn't configure key: {}",
            serde_json::to_string(value).unwrap_or_default()
        )),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SymbolsParser {
    Off,
    Universal,
    Scip,
}

/// `validation/symbols.go`: validate `syntaxHighlighting.symbols.engine`.
/// The baseline iterates a Go map (random order) and returns the first
/// failure; only corpora with a single invalid entry are deterministic.
fn contributed_symbols_problem(site: &Value) -> Option<String> {
    let engines = site
        .get("syntaxHighlighting")?
        .get("symbols")?
        .get("engine")?
        .as_object()?;
    for (language, engine) in engines {
        let engine = engine.as_str().unwrap_or_default();
        let parser = match engine {
            "off" => SymbolsParser::Off,
            "universal-ctags" => SymbolsParser::Universal,
            "scip-ctags" => SymbolsParser::Scip,
            _ => return Some(format!("Not a valid Symbols.Engine: `{engine}`.")),
        };
        let language = normalize_language(language);
        if parser == SymbolsParser::Scip && !symbol_language_supported(&language) {
            return Some(format!(
                "Not a valid Symbols.Engine for language: {language} `{engine}`."
            ));
        }
    }
    None
}

/// `languages.NormalizeLanguage`: lower-case plus the enry `c#` mapping.
fn normalize_language(language: &str) -> String {
    let normalized = language.to_lowercase();
    if normalized == "c#" {
        "c_sharp".to_owned()
    } else {
        normalized
    }
}

/// `ctags_config.supportedLanguages` (`internal/ctags_config/ctags_config.go:60-77`).
fn symbol_language_supported(language: &str) -> bool {
    matches!(
        language,
        "c_sharp"
            | "go"
            | "java"
            | "javascript"
            | "hack"
            | "kotlin"
            | "magik"
            | "python"
            | "ruby"
            | "rust"
            | "scala"
            | "typescript"
            | "zig"
    )
}

/// `validation/txemail.go`: validate the custom `email.templates`.
fn contributed_txemail_problems(site: &Value) -> Vec<String> {
    let mut problems = Vec::new();
    let Some(templates) = site.get("email.templates") else {
        return problems;
    };
    for name in ["resetPassword", "setPassword"] {
        let Some(template) = templates.get(name).filter(|value| !value.is_null()) else {
            continue;
        };
        let subject = template
            .get("subject")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let html = template
            .get("html")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if subject.is_empty() || html.is_empty() {
            problems.push(format!(
                "`email.templates.{name}` is invalid: fields 'subject' and 'html' are required"
            ));
            continue;
        }
        let text = template
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if let Err(error) = sg_txemail::parse_template(&sg_txemail::Templates {
            subject: subject.to_owned(),
            html: html.to_owned(),
            text: text.to_owned(),
        }) {
            problems.push(format!(
                "`email.templates.{name}` is invalid: {}",
                go_template_error(&error)
            ));
        }
    }
    problems
}

/// Go `text/template.Parse` error text for the single-line templates the
/// corpus uses. `sg-txemail` reports the condition without the
/// `template: :<line>:` prefix; `MissingEnd` is Go's EOF error.
fn go_template_error(error: &sg_txemail::TxEmailError) -> String {
    match error {
        sg_txemail::TxEmailError::Template(sg_txemail::TemplateError::MissingEnd) => {
            "template: :1: unexpected EOF".to_owned()
        }
        sg_txemail::TxEmailError::Template(other) => format!("template: :1: {other}"),
    }
}

fn go_json_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// `url.Parse(raw).Path`, approximately. Go accepts relative references; only
/// absolute URLs go through the `url` crate. Validation of percent escapes and
/// control characters matches Go's parser (see overview §4.2 for the remaining
/// divergence).
fn go_url_path(raw: &str) -> Result<String, ()> {
    if raw
        .chars()
        .any(|character| (character as u32) < 0x20 || character as u32 == 0x7f)
    {
        return Err(());
    }
    if !valid_percent_escapes(raw) {
        return Err(());
    }
    if raw.contains("://") || raw.starts_with("//") {
        return url::Url::parse(raw)
            .map(|url| url.path().to_owned())
            .map_err(|_| ());
    }
    // A leading scheme makes the reference opaque in Go, whose `Path` is empty.
    if let Some(colon) = raw.find(':') {
        if is_url_scheme(&raw[..colon]) {
            return Ok(String::new());
        }
    }
    Ok(raw.split(['?', '#']).next().unwrap_or_default().to_owned())
}

fn valid_percent_escapes(raw: &str) -> bool {
    let bytes = raw.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit()
            {
                return false;
            }
            index += 3;
        } else {
            index += 1;
        }
    }
    true
}

fn is_url_scheme(scheme: &str) -> bool {
    let mut characters = scheme.chars();
    match characters.next() {
        Some(character) if character.is_ascii_alphabetic() => {}
        _ => return false,
    }
    characters.all(|character| {
        character.is_ascii_alphanumeric()
            || character == '+'
            || character == '-'
            || character == '.'
    })
}

/// Go `fmt.Sprintf("%q", s)` for the ASCII patterns corpus uses.
fn go_quote(value: &str) -> String {
    let mut quoted = String::with_capacity(value.len() + 2);
    quoted.push('"');
    for character in value.chars() {
        match character {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            '\u{7}' => quoted.push_str("\\a"),
            '\u{8}' => quoted.push_str("\\b"),
            '\u{c}' => quoted.push_str("\\f"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            '\u{b}' => quoted.push_str("\\v"),
            character if (character as u32) < 0x20 || character as u32 == 0x7f => {
                quoted.push_str(&format!("\\x{:02x}", character as u32));
            }
            character => quoted.push(character),
        }
    }
    quoted.push('"');
    quoted
}

/// `siteConfigurationResolver`'s `Database`-backed dependencies
/// (`auth.CheckCurrentUserIsSiteAdmin`, `featureflag`, `SecurityEventLogs`,
/// `Conf().SiteGetLatest`, `conf.Raw()`).
#[derive(Clone)]
pub struct ProductionSiteConfigurationBackend {
    pool: PgPool,
    store: Arc<ConfigStore>,
    viewer_site_admin: bool,
}

impl ProductionSiteConfigurationBackend {
    pub fn new(pool: PgPool, store: Arc<ConfigStore>, viewer_site_admin: bool) -> Self {
        Self {
            pool,
            store,
            viewer_site_admin,
        }
    }
}

#[async_trait]
impl SiteConfigurationBackend for ProductionSiteConfigurationBackend {
    async fn check_current_user_is_site_admin(&self, actor: Actor) -> Result<(), String> {
        check_current_user_is_site_admin(actor, self.viewer_site_admin)
    }

    async fn auditlog_expansion(&self, actor: Actor) -> bool {
        auditlog_expansion(&self.pool, actor).await
    }

    async fn log_security_event(&self, event: SecurityEvent) -> Result<(), String> {
        // `securityEventLogsStore.InsertList` is a no-op unless
        // `log.securityEventLog.location` is `database` or `all`.
        let site = self
            .store
            .last_valid()
            .map(|unified| unified.site.raw().clone())
            .unwrap_or(Value::Null);
        if !security_event_logs_in_database(&site) {
            return Ok(());
        }

        let version = crate::version::Version::load();
        let version = String::from_utf8_lossy(version.as_bytes()).into_owned();
        insert_security_event(
            &self.pool,
            &DbSecurityEvent {
                name: event.name,
                url: &event.url,
                user_id: event.user_id,
                anonymous_user_id: &event.anonymous_user_id,
                source: event.source,
                argument: event.argument,
                version: &version,
                timestamp: OffsetDateTime::now_utc(),
            },
        )
        .await
        .map_err(|err| err.to_string())
    }

    async fn site_get_latest(&self) -> Result<LatestRow, String> {
        let (default_site, default_redacted) = default_site_pair()?;
        let row = conf_db::site_get_latest(&self.pool, 0, default_site, default_redacted)
            .await
            .map_err(|err| format!("ConfStore.SiteGetLatest: {err}"))?;
        Ok(LatestRow { id: row.id })
    }

    fn raw_site(&self) -> String {
        self.store.raw().site
    }
}

/// `defaultConfigForDeployment` resolved from `DEPLOY_TYPE`.
fn default_site_pair() -> Result<(&'static str, &'static str), String> {
    let getenv: &dyn Fn(&str) -> Option<String> = &|key| std::env::var(key).ok();
    let deploy_type = crate::service_connections::deploy_type(getenv);
    crate::deploy_defaults::default_site_config_pair(&deploy_type)
}

/// `featureflag.FromContext(ctx).GetBoolOr("auditlog-expansion", false)`.
async fn auditlog_expansion(pool: &PgPool, actor: Actor) -> bool {
    match actor {
        Actor::User { uid } => match get_user_flags(pool, uid).await {
            Ok(flags) => flags.get("auditlog-expansion").copied().unwrap_or(false),
            Err(_) => global_auditlog_expansion(pool).await,
        },
        // Internal actors have `UID == 0`; the featureflag middleware falls
        // through to the global flags (the anonymous-UID header path is not
        // representable in `sg_graphql::exec::Actor` — see overview §2.3).
        Actor::Anonymous | Actor::Internal => global_auditlog_expansion(pool).await,
    }
}

async fn global_auditlog_expansion(pool: &PgPool) -> bool {
    match get_global_feature_flags(pool).await {
        Ok(flags) => flags.get("auditlog-expansion").copied().unwrap_or(false),
        Err(_) => false,
    }
}

/// `db.Conf().ListSiteConfigs` / `GetSiteConfigCount` over
/// `critical_and_site_config`.
///
/// The portable [`SiteConfigHistoryStore`] layer applies the baseline's `LAG`
/// de-duplication, ordering and pagination, so this store returns the raw
/// history in ascending id order.
#[derive(Clone)]
pub struct PgSiteConfigHistoryStore {
    pool: PgPool,
}

impl PgSiteConfigHistoryStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl SiteConfigHistoryStore for PgSiteConfigHistoryStore {
    async fn get_site_configs(&self) -> Result<Vec<ChangeRow>, String> {
        let rows = sqlx::query_as::<
            _,
            (
                i32,
                i32,
                String,
                Option<String>,
                OffsetDateTime,
                OffsetDateTime,
            ),
        >(
            "SELECT id, author_user_id, contents, redacted_contents, created_at, updated_at \
             FROM critical_and_site_config WHERE type = 'site' ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|err| format!("ConfStore.ListSiteConfigs: {err}"))?;
        Ok(rows
            .into_iter()
            .map(
                |(id, author_user_id, contents, redacted_contents, created_at, updated_at)| {
                    ChangeRow {
                        id,
                        author_user_id,
                        contents,
                        redacted_contents,
                        created_at,
                        updated_at,
                    }
                },
            )
            .collect())
    }

    async fn get_site_config_count(&self) -> Result<usize, String> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM ( \
                 SELECT *, LAG(redacted_contents) OVER (ORDER BY id) AS prev_redacted_contents \
                 FROM critical_and_site_config) t \
             WHERE (prev_redacted_contents IS NULL \
                    OR redacted_contents != prev_redacted_contents) \
             AND redacted_contents IS NOT NULL AND type = 'site'",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|err| format!("ConfStore.GetSiteConfigCount: {err}"))?;
        usize::try_from(count).map_err(|_| format!("negative site config count: {count}"))
    }
}

/// The production seams `SchemaGraphqlDeps` needs for the configuration
/// surface. `licenses` is shared with the M9.8ja PostAuth/access-token gates.
#[derive(Clone)]
pub struct ConfigGraphqlDeps {
    pub pool: PgPool,
    pub store: Arc<ConfigStore>,
    pub licenses: Arc<LicenseConfig>,
}

impl ConfigGraphqlDeps {
    pub fn new(pool: PgPool, store: Arc<ConfigStore>) -> Self {
        let licenses = Arc::new(LicenseConfig::new(Arc::new(PgProductLicenseStore::new(
            pool.clone(),
        ))));
        Self {
            pool,
            store,
            licenses,
        }
    }

    /// Test hook: replace the `product_licenses` source.
    pub fn with_license_store(mut self, store: Arc<dyn ProductLicenseStore>) -> Self {
        self.licenses = Arc::new(LicenseConfig::new(store));
        self
    }
}

// ---------------------------------------------------------------------------
// GraphQL prefetch + synchronous replay
// ---------------------------------------------------------------------------

/// Prefetched `Query.site` data. `configuration` is `None` when the field was
/// not selected.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SiteRoot {
    pub configuration: Option<Result<SiteConfigurationRoot, String>>,
}

/// Prefetched `SiteConfiguration` fields (only the selected ones are `Some`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SiteConfigurationRoot {
    pub return_safe_configs_only: bool,
    pub id: Option<Result<i32, String>>,
    pub effective_contents: Option<Result<String, String>>,
    pub validation_messages: Option<Result<Vec<String>, String>>,
    pub license_info: Option<Result<Option<LicenseInfo>, String>>,
    pub history: Option<Result<HistoryRoot, String>>,
}

/// Prefetched `SiteConfigurationChangeConnection` fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryRoot {
    pub total_count: Option<Result<i32, String>>,
    pub page_info: Option<Result<ConnectionPageInfo, String>>,
    pub nodes: Option<Result<Vec<ChangeNodeRoot>, String>>,
}

/// One prefetched `SiteConfigurationChange` node and its `author`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeNodeRoot {
    pub node: SiteConfigurationChangeResolver,
    pub author: Option<Result<Option<UserData>, String>>,
}

fn selected<'a>(fields: &'a [RootField], name: &str) -> Option<&'a RootField> {
    fields.iter().find(|field| field.name == name)
}

/// Prefetch every read-only field of `Query.site.configuration`.
///
/// `None` means the operation did not select `Query.site` (or the executor has
/// no site resolver). The caller injects the result through
/// `FrontendRootResolver::with_site`.
pub async fn prefetch_site(
    backend: &dyn SiteConfigurationBackend,
    licenses: &dyn LicenseInfoProvider,
    validator: &dyn SiteConfigValidator,
    history_store: &dyn SiteConfigHistoryStore,
    users: Option<&dyn UserLookupStore>,
    fields: &[RootField],
    actor: Actor,
) -> Option<SiteRoot> {
    let site = selected(fields, "site")?;
    let Some(configuration_field) = selected(&site.selections, "configuration") else {
        return Some(SiteRoot::default());
    };

    let configuration = prefetch_site_configuration(
        backend,
        licenses,
        validator,
        history_store,
        users,
        configuration_field,
        actor,
    )
    .await;

    Some(SiteRoot {
        configuration: Some(configuration),
    })
}

/// Prefetch every selected field of one `SiteConfiguration` resolver.
///
/// Shared by `Query.site.configuration` and
/// `Query.settingsSubject(id: <Site>).configuration`; the baseline resolves both
/// through the same `siteResolver.Configuration` method, including the admin
/// check and security-event audit.
pub async fn prefetch_site_configuration(
    backend: &dyn SiteConfigurationBackend,
    licenses: &dyn LicenseInfoProvider,
    validator: &dyn SiteConfigValidator,
    history_store: &dyn SiteConfigHistoryStore,
    users: Option<&dyn UserLookupStore>,
    configuration_field: &RootField,
    actor: Actor,
) -> Result<SiteConfigurationRoot, String> {
    let return_safe_configs_only = configuration_field
        .arguments
        .bool("returnSafeConfigsOnly")
        .unwrap_or(false);
    let resolver = match configuration(
        backend,
        actor,
        configuration_field.arguments.bool("returnSafeConfigsOnly"),
    )
    .await
    {
        Err(message) => return Err(message),
        Ok(resolver) => resolver,
    };

    let selections = &configuration_field.selections;
    let want_id = selected(selections, "id").is_some();
    let want_effective = selected(selections, "effectiveContents").is_some();
    let want_validation = selected(selections, "validationMessages").is_some();
    let want_license = selected(selections, "licenseInfo").is_some();
    let want_history = selected(selections, "history").is_some();

    // `ValidationMessages` calls `EffectiveContents` internally; share the
    // lookup when both fields are selected.
    let effective = if want_effective || want_validation {
        Some(effective_contents(backend, actor, &resolver).await)
    } else {
        None
    };

    let id = if want_id {
        Some(site_configuration_id(backend, actor, &resolver).await)
    } else {
        None
    };

    let validation_messages = if want_validation {
        // `siteConfigurationResolver.ValidationMessages` re-checks the admin
        // (even for `returnSafeConfigsOnly`, where `EffectiveContents` skips
        // it), `site.go:262-272`.
        match backend.check_current_user_is_site_admin(actor).await {
            Err(message) => Some(Err(message)),
            Ok(()) => match &effective {
                Some(Ok(contents)) => Some(validator.validate_site(contents).await),
                Some(Err(message)) => Some(Err(message.clone())),
                None => None,
            },
        }
    } else {
        None
    };

    let license_info = if want_license {
        Some(license_info(backend, licenses, actor).await)
    } else {
        None
    };

    let history = if want_history {
        Some(
            prefetch_history(
                backend,
                history_store,
                users,
                actor,
                selected(selections, "history").expect("history selected"),
            )
            .await,
        )
    } else {
        None
    };

    Ok(SiteConfigurationRoot {
        return_safe_configs_only,
        id,
        effective_contents: effective,
        validation_messages,
        license_info,
        history,
    })
}

/// The `configuration` selection of `settingsSubject(id: <Site>)`, if any.
///
/// Only the canonical site subject (`relay.MarshalID("Site", "site")`) is
/// recognized; any other subject kind keeps the unported `configuration`
/// field.
pub fn settings_subject_site_configuration_field(fields: &[RootField]) -> Option<&RootField> {
    for field in fields {
        if field.name != "settingsSubject" {
            continue;
        }
        let Some(id) = field.arguments.string("id") else {
            continue;
        };
        if relay_kind(id).as_deref() != Some("Site") {
            continue;
        }
        if unmarshal_relay_spec_string(id).as_deref() != Ok("site") {
            continue;
        }
        if let Some(configuration) = selected(&field.selections, "configuration") {
            return Some(configuration);
        }
        // `configuration` is only declared on the `Site` object, so a query
        // against the `SettingsSubject` interface selects it inside an inline
        // fragment.
        for (type_condition, selections) in &field.type_conditions {
            if type_condition == "Site" {
                if let Some(configuration) = selected(selections, "configuration") {
                    return Some(configuration);
                }
            }
        }
    }
    None
}

/// Prefetch `Query.settingsSubject(id: <Site>).configuration`.
///
/// `None` means the operation did not select that field. Each root field is its
/// own prefetch (and therefore its own admin check/audit), matching the
/// baseline's per-resolver `siteResolver.Configuration` call.
pub async fn prefetch_settings_subject_site_configuration(
    backend: &dyn SiteConfigurationBackend,
    licenses: &dyn LicenseInfoProvider,
    validator: &dyn SiteConfigValidator,
    history_store: &dyn SiteConfigHistoryStore,
    users: Option<&dyn UserLookupStore>,
    fields: &[RootField],
    actor: Actor,
) -> Option<Result<SiteConfigurationRoot, String>> {
    let configuration_field = settings_subject_site_configuration_field(fields)?;
    Some(
        prefetch_site_configuration(
            backend,
            licenses,
            validator,
            history_store,
            users,
            configuration_field,
            actor,
        )
        .await,
    )
}

/// The `configuration` selection inside a settings cascade's Site subject, if
/// any.
///
/// `SettingsCascade.subjects` is `[SettingsSubject!]!` and `configuration` is
/// declared only on `Site`, so the selection lives under the `Site` inline
/// fragment (`subjects { ... on Site { configuration } }`). The cascade itself
/// may be reached from several roots (`Query.viewerSettings`,
/// `Query.viewerConfiguration`, `SettingsSubject.settingsCascade`,
/// `SettingsSubject.configurationCascade`), so the scan is recursive.
pub fn settings_cascade_site_configuration_field(fields: &[RootField]) -> Option<&RootField> {
    fields
        .iter()
        .find_map(settings_cascade_site_configuration_in_field)
}

fn settings_cascade_site_configuration_in_field(field: &RootField) -> Option<&RootField> {
    if matches!(
        field.name.as_str(),
        "settingsCascade" | "configurationCascade" | "viewerSettings" | "viewerConfiguration"
    ) {
        if let Some(subjects) = selected(&field.selections, "subjects") {
            if let Some(configuration) = selected(&subjects.selections, "configuration") {
                return Some(configuration);
            }
            for (type_condition, selections) in &subjects.type_conditions {
                if type_condition == "Site" {
                    if let Some(configuration) = selected(selections, "configuration") {
                        return Some(configuration);
                    }
                }
            }
        }
    }
    for child in &field.selections {
        if let Some(configuration) = settings_cascade_site_configuration_in_field(child) {
            return Some(configuration);
        }
    }
    for (_, selections) in &field.type_conditions {
        for child in selections {
            if let Some(configuration) = settings_cascade_site_configuration_in_field(child) {
                return Some(configuration);
            }
        }
    }
    None
}

/// Prefetch `SettingsCascade.subjects[... on Site].configuration`.
///
/// `None` means the operation did not select a Site `configuration` inside a
/// settings cascade. The baseline performs one admin check/audit per
/// `siteResolver.Configuration` call (one per cascade root); this prefetch
/// performs one for the whole operation (overview §4.2).
pub async fn prefetch_cascade_subject_site_configuration(
    backend: &dyn SiteConfigurationBackend,
    licenses: &dyn LicenseInfoProvider,
    validator: &dyn SiteConfigValidator,
    history_store: &dyn SiteConfigHistoryStore,
    users: Option<&dyn UserLookupStore>,
    fields: &[RootField],
    actor: Actor,
) -> Option<Result<SiteConfigurationRoot, String>> {
    let configuration_field = settings_cascade_site_configuration_field(fields)?;
    Some(
        prefetch_site_configuration(
            backend,
            licenses,
            validator,
            history_store,
            users,
            configuration_field,
            actor,
        )
        .await,
    )
}

async fn prefetch_history(
    backend: &dyn SiteConfigurationBackend,
    store: &dyn SiteConfigHistoryStore,
    users: Option<&dyn UserLookupStore>,
    actor: Actor,
    field: &RootField,
) -> Result<HistoryRoot, String> {
    let args = connection_args(field)?;
    let connection = history(
        backend,
        store,
        actor,
        Some(args),
        ConnectionResolverOptions::default(),
    )
    .await?;

    let want_nodes = selected(&field.selections, "nodes").is_some();
    let want_total = selected(&field.selections, "totalCount").is_some();
    let want_page = selected(&field.selections, "pageInfo").is_some();

    let total_count = if want_total {
        Some(connection.total_count().await)
    } else {
        None
    };
    let page_info = if want_page {
        Some(connection.page_info().await)
    } else {
        None
    };

    let nodes = if want_nodes {
        let nodes_field = selected(&field.selections, "nodes").expect("nodes selected");
        let want_author = selected(&nodes_field.selections, "author").is_some();
        match connection.nodes().await {
            Err(message) => Some(Err(message)),
            Ok(nodes) => {
                let mut prefetched = Vec::with_capacity(nodes.len());
                for node in nodes {
                    let author = if want_author {
                        Some(author_lookup(users, node.author_user_id()).await)
                    } else {
                        None
                    };
                    prefetched.push(ChangeNodeRoot { node, author });
                }
                Some(Ok(prefetched))
            }
        }
    } else {
        None
    };

    Ok(HistoryRoot {
        total_count,
        page_info,
        nodes,
    })
}

async fn author_lookup(
    users: Option<&dyn UserLookupStore>,
    author_user_id: i32,
) -> Result<Option<UserData>, String> {
    if author_user_id == 0 {
        return Ok(None);
    }
    let Some(users) = users else {
        return Err("user lookup unavailable".to_owned());
    };
    users.lookup(&UserKey::DatabaseId(author_user_id)).await
}

fn connection_args(field: &RootField) -> Result<ConnectionResolverArgs, String> {
    let int32 = |name: &str| -> Result<Option<i32>, String> {
        field
            .arguments
            .i64(name)
            .map(|value| {
                i32::try_from(value).map_err(|_| format!("{name} is not a 32-bit integer"))
            })
            .transpose()
    };
    Ok(ConnectionResolverArgs {
        first: int32("first")?,
        last: int32("last")?,
        after: field.arguments.string("after").map(str::to_owned),
        before: field.arguments.string("before").map(str::to_owned),
    })
}

/// `siteConfigurationResolver` replay.
#[derive(Debug, Clone, PartialEq)]
pub struct SiteConfigurationObject {
    root: SiteConfigurationRoot,
}

impl SiteConfigurationObject {
    pub fn new(root: SiteConfigurationRoot) -> Self {
        Self { root }
    }
}

impl ObjectValue for SiteConfigurationObject {
    fn type_name(&self) -> &'static str {
        "SiteConfiguration"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => replay(&self.root.id, |value| {
                FieldValue::Json(Json::Int(i64::from(*value)))
            }),
            "effectiveContents" => replay(&self.root.effective_contents, |contents| {
                FieldValue::Json(Json::Str(contents.clone()))
            }),
            "validationMessages" => replay(&self.root.validation_messages, |messages| {
                FieldValue::List {
                    items: messages
                        .iter()
                        .map(|message| FieldValue::Json(Json::Str(message.clone())))
                        .collect(),
                    element_type: Some("String"),
                }
            }),
            "licenseInfo" => match &self.root.license_info {
                None => Ok(None),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                Some(Ok(None)) => Ok(Some(FieldValue::Null)),
                Some(Ok(Some(info))) => Ok(Some(FieldValue::Object(Arc::new(
                    LicenseInfoObject::new(info.clone()),
                )))),
            },
            "history" => match &self.root.history {
                None => Ok(None),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                Some(Ok(root)) => Ok(Some(FieldValue::Object(Arc::new(
                    SiteConfigurationChangeConnectionObject::new(root.clone()),
                )))),
            },
            _ => Ok(None),
        }
    }
}

fn replay<T: Clone>(
    value: &Option<Result<T, String>>,
    build: impl FnOnce(&T) -> FieldValue,
) -> Result<Option<FieldValue>, ExecError> {
    match value {
        None => Ok(None),
        Some(Err(message)) => Err(ExecError::new(message.clone())),
        Some(Ok(value)) => Ok(Some(build(value))),
    }
}

/// `licenseInfoResolver` (`site.go:233-243`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LicenseInfoObject {
    info: LicenseInfo,
}

impl LicenseInfoObject {
    pub fn new(info: LicenseInfo) -> Self {
        Self { info }
    }
}

impl ObjectValue for LicenseInfoObject {
    fn type_name(&self) -> &'static str {
        "LicenseInfo"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "tags" => Ok(Some(FieldValue::List {
                items: self
                    .info
                    .tags
                    .iter()
                    .map(|tag| FieldValue::Json(Json::Str(tag.clone())))
                    .collect(),
                element_type: Some("String"),
            })),
            "userCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.info.user_count,
            ))))),
            "expiresAt" => Ok(Some(FieldValue::Json(Json::Str(
                self.info.expires_at.clone(),
            )))),
            _ => Ok(None),
        }
    }
}

/// `gqlutil.ConnectionResolver[*SiteConfigurationChangeResolver]` replay.
#[derive(Debug, Clone, PartialEq)]
pub struct SiteConfigurationChangeConnectionObject {
    root: HistoryRoot,
}

impl SiteConfigurationChangeConnectionObject {
    pub fn new(root: HistoryRoot) -> Self {
        Self { root }
    }
}

impl ObjectValue for SiteConfigurationChangeConnectionObject {
    fn type_name(&self) -> &'static str {
        "SiteConfigurationChangeConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => match &self.root.nodes {
                None => Ok(None),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                Some(Ok(nodes)) => Ok(Some(FieldValue::List {
                    items: nodes
                        .iter()
                        .map(|node| {
                            FieldValue::Object(Arc::new(SiteConfigurationChangeObject::new(
                                node.clone(),
                            )))
                        })
                        .collect(),
                    element_type: Some("SiteConfigurationChange"),
                })),
            },
            "totalCount" => replay(&self.root.total_count, |count| {
                FieldValue::Json(Json::Int(i64::from(*count)))
            }),
            "pageInfo" => replay(&self.root.page_info, |page_info| {
                FieldValue::Object(Arc::new(ConnectionPageInfoObject::new(
                    page_info.start_cursor.clone(),
                    page_info.end_cursor.clone(),
                    page_info.has_next_page,
                    page_info.has_previous_page,
                )))
            }),
            _ => Ok(None),
        }
    }
}

/// `SiteConfigurationChange` node.
#[derive(Debug, Clone, PartialEq)]
pub struct SiteConfigurationChangeObject {
    root: ChangeNodeRoot,
}

impl SiteConfigurationChangeObject {
    pub fn new(root: ChangeNodeRoot) -> Self {
        Self { root }
    }
}

impl ObjectValue for SiteConfigurationChangeObject {
    fn type_name(&self) -> &'static str {
        "SiteConfigurationChange"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(self.root.node.id())))),
            "diff" => Ok(Some(FieldValue::Json(Json::Str(self.root.node.diff())))),
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(
                self.root.node.created_at(),
            )))),
            "updatedAt" => Ok(Some(FieldValue::Json(Json::Str(
                self.root.node.updated_at(),
            )))),
            "author" => match &self.root.author {
                None => Ok(None),
                Some(Err(message)) => Err(ExecError::new(message.clone())),
                Some(Ok(None)) => Ok(Some(FieldValue::Null)),
                Some(Ok(Some(user))) => Ok(Some(FieldValue::Object(Arc::new(
                    UserObject::from_data(user.clone()),
                )))),
            },
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::site_config_change::SiteConfigRow;
    use crate::site_configuration_query::SiteConfigRow as LatestRow;
    use time::OffsetDateTime;

    struct Backend {
        admin: bool,
        raw: String,
    }

    #[async_trait]
    impl SiteConfigurationBackend for Backend {
        async fn check_current_user_is_site_admin(&self, _actor: Actor) -> Result<(), String> {
            if self.admin {
                Ok(())
            } else {
                Err("must be site admin".to_owned())
            }
        }

        async fn auditlog_expansion(&self, _actor: Actor) -> bool {
            false
        }

        async fn log_security_event(
            &self,
            _event: crate::site_configuration_query::SecurityEvent,
        ) -> Result<(), String> {
            Ok(())
        }

        async fn site_get_latest(&self) -> Result<LatestRow, String> {
            Ok(LatestRow { id: 3 })
        }

        fn raw_site(&self) -> String {
            self.raw.clone()
        }
    }

    struct Validator;

    #[async_trait]
    impl SiteConfigValidator for Validator {
        async fn validate_site(&self, contents: &str) -> Result<Vec<String>, String> {
            if contents.contains("bad") {
                Ok(vec!["bad value".to_owned()])
            } else {
                Ok(Vec::new())
            }
        }
    }

    struct Licenses;

    #[async_trait]
    impl LicenseInfoProvider for Licenses {
        async fn configured_license(&self) -> Result<Option<LicenseInfo>, String> {
            Ok(Some(LicenseInfo {
                tags: vec!["plan:enterprise".to_owned()],
                user_count: 10,
                expires_at: "2030-01-02T03:04:05Z".to_owned(),
            }))
        }
    }

    struct HistoryStore;

    #[async_trait]
    impl SiteConfigHistoryStore for HistoryStore {
        async fn get_site_configs(&self) -> Result<Vec<SiteConfigRow>, String> {
            Ok(vec![SiteConfigRow {
                id: 1,
                author_user_id: 2,
                contents: "{}".to_owned(),
                redacted_contents: Some("{}".to_owned()),
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: OffsetDateTime::UNIX_EPOCH,
            }])
        }

        async fn get_site_config_count(&self) -> Result<usize, String> {
            Ok(1)
        }
    }

    fn resolver() -> SiteConfigurationResolver {
        SiteConfigurationResolver {
            return_safe_configs_only: false,
        }
    }

    #[tokio::test]
    async fn non_admins_are_denied_every_field() {
        let backend = Backend {
            admin: false,
            raw: "{}".to_owned(),
        };
        let actor = Actor::Anonymous;
        assert!(
            validation_messages(&backend, &Validator, actor, &resolver())
                .await
                .is_err()
        );
        assert!(license_info(&backend, &Licenses, actor).await.is_err());
        let store = HistoryStore;
        assert!(history(
            &backend,
            &store,
            actor,
            None,
            ConnectionResolverOptions::default()
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn admins_get_validation_messages() {
        let backend = Backend {
            admin: true,
            raw: r#"{"bad": true}"#.to_owned(),
        };
        let messages =
            validation_messages(&backend, &Validator, Actor::User { uid: 1 }, &resolver())
                .await
                .expect("messages");
        assert_eq!(messages, vec!["bad value".to_owned()]);
    }

    #[tokio::test]
    async fn admins_get_license_info() {
        let backend = Backend {
            admin: true,
            raw: "{}".to_owned(),
        };
        let info = license_info(&backend, &Licenses, Actor::User { uid: 1 })
            .await
            .expect("license")
            .expect("some");
        assert_eq!(info.user_count, 10);
        assert_eq!(info.expires_at, "2030-01-02T03:04:05Z");
    }

    #[tokio::test]
    async fn admins_get_history_connection() {
        let backend = Backend {
            admin: true,
            raw: "{}".to_owned(),
        };
        let store = HistoryStore;
        let connection = history(
            &backend,
            &store,
            Actor::User { uid: 1 },
            Some(ConnectionResolverArgs {
                first: Some(10),
                ..ConnectionResolverArgs::default()
            }),
            ConnectionResolverOptions::default(),
        )
        .await
        .expect("history");
        assert_eq!(connection.total_count().await.expect("count"), 1);
        assert_eq!(connection.nodes().await.expect("nodes").len(), 1);
    }

    fn custom(raw: &str) -> Vec<String> {
        validate_site_against_raw(raw, raw).expect("valid")
    }

    #[test]
    fn custom_validation_covers_email_rules() {
        const BUILTIN: &str = r#""auth.providers":[{"allowSignup":true,"type":"builtin"}]"#;
        assert_eq!(
            custom(&format!(
                r#"{{"email.smtp":{{"authentication":"none","host":"h","port":25}},{BUILTIN}}}"#
            )),
            vec!["should set email.address because email.smtp is set".to_owned()]
        );
        assert_eq!(
            custom(&format!(
                r#"{{"email.address":"a@b.c","email.smtp":{{"authentication":"PLAIN","host":"h","port":25}},{BUILTIN}}}"#
            )),
            vec![
                "must set email.smtp username and password for email.smtp authentication"
                    .to_owned()
            ]
        );
        assert_eq!(
            custom(&format!(
                r#"{{"email.address":"a@b.c","email.smtp":{{"authentication":"PLAIN","host":"h","password":"p","port":25,"username":"u"}},{BUILTIN}}}"#
            )),
            Vec::<String>::new()
        );
    }

    #[test]
    fn custom_validation_covers_external_url_and_git_regex() {
        const BUILTIN: &str = r#""auth.providers":[{"allowSignup":true,"type":"builtin"}]"#;
        assert_eq!(
            custom(&format!(
                r#"{{"externalURL":"http://example.com/sub",{BUILTIN}}}"#
            )),
            vec!["externalURL must not be a non-root URL".to_owned()]
        );
        assert_eq!(
            custom(&format!(
                r#"{{"externalURL":"http://example.com/%zz",{BUILTIN}}}"#
            )),
            vec!["externalURL must be a valid URL".to_owned()]
        );
        assert_eq!(
            custom(&format!(
                r#"{{"externalURL":"https://example.com",{BUILTIN}}}"#
            )),
            Vec::<String>::new()
        );
        assert_eq!(
            custom(&format!(
                r#"{{"gitUpdateInterval":[{{"interval":1,"pattern":"["}}],{BUILTIN}}}"#
            )),
            vec![r#"GitUpdateIntervalRule pattern is not valid regex: "[""#.to_owned()]
        );
        assert_eq!(
            custom(&format!(
                r#"{{"gitUpdateInterval":[{{"interval":1,"pattern":"^github\\.com/"}}],{BUILTIN}}}"#
            )),
            Vec::<String>::new()
        );
    }

    #[test]
    fn custom_validation_covers_contributed_auth_rules() {
        // `UnredactSecrets` materializes a nil `auth.providers` as `null`, so
        // the schema reports the type mismatch before the custom validator.
        assert_eq!(
            custom("{}"),
            vec![
                "auth.providers: Invalid type. Expected: array, given: null".to_owned(),
                "no auth providers set (all access will be forbidden)".to_owned(),
            ]
        );
        assert_eq!(
            custom(
                r#"{"auth.enableUsernameChanges":true,"auth.providers":[{"clientID":"cid","clientSecret":"secret","type":"github"}]}"#
            ),
            vec![
                "`auth.enableUsernameChanges` must not be true if external auth providers are set in `auth.providers`".to_owned()
            ]
        );
        assert_eq!(
            custom(
                r#"{"auth.providers":[{"allowSignup":true,"type":"builtin"},{"type":"builtin"}]}"#
            ),
            vec!["at most 1 builtin auth provider may be used".to_owned()]
        );
    }

    #[test]
    fn custom_validation_runs_after_schema_messages() {
        let messages = custom(
            r#"{"auth.providers":[{"allowSignup":true,"type":"builtin"}],"externalURL":"http://example.com/sub","update.channel":"bogus"}"#,
        );
        assert!(messages.len() >= 2, "{messages:?}");
        assert!(messages[0].starts_with("update.channel:"), "{messages:?}");
        assert_eq!(
            messages.last().map(String::as_str),
            Some("externalURL must not be a non-root URL")
        );
    }

    #[test]
    fn go_quote_matches_the_ascii_cases() {
        assert_eq!(go_quote("["), "\"[\"");
        assert_eq!(go_quote("a\"b\\c"), "\"a\\\"b\\\\c\"");
        assert_eq!(go_quote("\n"), "\"\\n\"");
    }
}
