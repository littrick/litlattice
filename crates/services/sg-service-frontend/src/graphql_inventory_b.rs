#![forbid(unsafe_code)]

//! M9.17a frontend GraphQL business surface batch B
//! (`cmd/frontend/graphqlbackend/{package_repos,package_repo_filters,search,
//! outbound_requests,org_invitations,org_invitation,repository_metadata,
//! site_admin,user}.go`).
//!
//! Ported here:
//!
//! * `Query.packageRepoReferences` / `Query.packageRepoReferencesMatchingFilter`
//!   against `lsif_dependency_repos` + `package_repo_versions`;
//! * `Query.indexedSearchInstances` (site-admin gate, empty endpoint seam);
//! * `Query.outboundRequests` (site-admin gate, empty log seam);
//! * `Query.invitationByToken` (auth gate, signing-key seam; the default
//!   `None` config reproduces the baseline "signing key not provided" error);
//! * `Mutation.inviteUserToOrganization` /
//!   `Mutation.respondToOrganizationInvitation` against `org_invitations` and
//!   `org_members` (legacy relative invitation URL, no email delivery);
//! * `add/update/deleteRepoMetadata` and the deprecated
//!   `*RepoKeyValuePair` aliases against `repo_kvps` (RBAC
//!   `REPO_METADATA#WRITE`);
//! * `invalidateSessionsByID(s)`, `recoverUsers`, `updatePassword` and
//!   `createPassword`.
//!
//! Explicitly not ported (see the overview): `PackageRepoReference.repository`
//! (needs the per-ecosystem package-name parsers), `externalServiceNamespaces`
//! / `externalServiceRepositories` (repo-updater source discovery),
//! `renderMarkdown`/`highlightCode` (renderer seam), `randomizeUserPassword`
//! and `sendTestEmail` (SMTP), and the webhooks surface (enterprise resolver).

use std::collections::HashMap;
use std::sync::Arc;

use sg_graphql::exec::{
    Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue, RootField,
};
use sg_graphql::json::Json;
use sqlx::{PgPool, Row};
use time::OffsetDateTime;

use crate::admin_store::check_current_user_is_site_admin;
use crate::graphql_resolvers::{datetime_rfc3339, unmarshal_relay_id};

/// `SigningKeyMessage` (`org_invitations.go:27`) when creating an invitation
/// URL without a signing key.
pub const SIGNING_KEY_CREATE: &str = "signing key not provided, cannot create JWT for invitation URL. Please add organizationInvitations signingKey to site configuration.";

/// The validation-side equivalent (`InvitationByToken`, `org_invitations.go:136`).
pub const SIGNING_KEY_VALIDATE: &str = "signing key not provided, cannot validate JWT on invitation URL. Please add organizationInvitations signingKey to site configuration.";

/// `conf.SiteConfig().OrganizationInvitations` as an injectable seam.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrgInvitationConfig {
    /// Base64 (standard encoding) HMAC key.
    pub signing_key: String,
    /// `conf.ExternalURL()` (empty by default).
    pub external_url: String,
}

// ---------------------------------------------------------------------------
// Relay helpers
// ---------------------------------------------------------------------------

pub fn marshal_relay_id_i64(kind: &str, id: i64) -> String {
    use base64::Engine as _;
    let spec = serde_json::to_string(&id).expect("int json");
    base64::engine::general_purpose::URL_SAFE.encode(format!("{kind}:{spec}"))
}

pub fn marshal_relay_id_str(kind: &str, value: &str) -> String {
    use base64::Engine as _;
    let spec = serde_json::to_string(value).expect("string json");
    base64::engine::general_purpose::URL_SAFE.encode(format!("{kind}:{spec}"))
}

fn decode_url(s: &str) -> Option<Vec<u8>> {
    use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
    use base64::Engine as _;
    URL_SAFE_NO_PAD
        .decode(s)
        .or_else(|_| URL_SAFE.decode(s))
        .ok()
}

/// `relay.UnmarshalSpec` for an `int64` (`package_repo_references.id`,
/// `org_invitations.id`). The kind prefix is not validated, matching the
/// baseline.
pub fn unmarshal_relay_i64(id: &str) -> Result<i64, String> {
    let decoded = decode_url(id).ok_or_else(|| "illegal base64 data".to_owned())?;
    let text = String::from_utf8_lossy(&decoded);
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    serde_json::from_str(spec).map_err(|error| error.to_string())
}

/// `relay.UnmarshalSpec` for a `string` (`outboundRequests.after`).
pub fn unmarshal_relay_string(id: &str) -> Result<String, String> {
    let decoded = decode_url(id).ok_or_else(|| "illegal base64 data".to_owned())?;
    let text = String::from_utf8_lossy(&decoded);
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    serde_json::from_str(spec).map_err(|error| error.to_string())
}

// ---------------------------------------------------------------------------
// Package repo references
// ---------------------------------------------------------------------------

/// `dependencies.PackageRepoRefVersion`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageRepoRefVersionData {
    pub id: i64,
    pub package_ref_id: i64,
    pub version: String,
}

/// `dependencies.PackageRepoReference`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageRepoRefData {
    pub id: i64,
    pub scheme: String,
    pub name: String,
    pub blocked: bool,
    pub versions: Vec<PackageRepoRefVersionData>,
}

/// `packageRepoReferenceConnectionResolver`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageRefsConnection {
    pub deps: Vec<PackageRepoRefData>,
    pub has_more: bool,
    pub total: i64,
}

/// `packageRepoReferenceVersionConnectionResolver`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionConnection {
    pub versions: Vec<PackageRepoRefVersionData>,
    pub has_more: bool,
    pub total: i64,
}

/// The two shapes of `filterMatchingResolver`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchingFilterResult {
    Packages(PackageRefsConnection),
    Versions(VersionConnection),
}

/// `externalServiceToPackageSchemeMap` (`package_repos.go:29-36`).
fn external_service_to_scheme(kind: &str) -> Option<&'static str> {
    Some(match kind {
        "JVMPACKAGES" => "semanticdb",
        "NPMPACKAGES" => "npm",
        "GOMODULES" | "GOPACKAGES" => "go",
        "PYTHONPACKAGES" => "python",
        "RUSTPACKAGES" => "rust-analyzer",
        "RUBYPACKAGES" => "scip-ruby",
        _ => return None,
    })
}

/// `packageSchemeToExternalServiceMap` (`package_repos.go:38-45`).
fn scheme_to_external_service(scheme: &str) -> &'static str {
    match scheme {
        "semanticdb" => "JVMPACKAGES",
        "npm" => "NPMPACKAGES",
        "go" => "GOMODULES",
        "python" => "PYTHONPACKAGES",
        "rust-analyzer" => "RUSTPACKAGES",
        "scip-ruby" => "RUBYPACKAGES",
        _ => "",
    }
}

/// The parsed `Query.packageRepoReferences` arguments.
pub struct PackageRefsArgs {
    pub key: String,
    pub scheme: Result<Option<&'static str>, String>,
    pub name: Option<String>,
    pub after: Option<String>,
    pub first: i64,
}

pub fn package_refs_args(arguments: &Arguments) -> PackageRefsArgs {
    let kind = arguments.string("kind");
    let name = arguments.string("name").map(str::to_owned);
    let after = arguments.string("after").map(str::to_owned);
    let first = arguments.i64("first").unwrap_or(0);
    let key = format!("{kind:?}|{name:?}|{after:?}|{first}");
    let scheme = match kind {
        None => Ok(None),
        Some(kind) => external_service_to_scheme(kind)
            .map(Some)
            .ok_or_else(|| format!("unknown package scheme {kind:?}")),
    };
    PackageRefsArgs {
        key,
        scheme,
        name,
        after,
        first,
    }
}

/// `Query.packageRepoReferences` (`package_repos.go:55-105`).
pub async fn prefetch_package_repo_references(
    pool: &PgPool,
    fields: &[RootField],
) -> HashMap<String, Result<PackageRefsConnection, String>> {
    let mut lookups = HashMap::new();
    for field in fields {
        if field.name != "packageRepoReferences" {
            continue;
        }
        let args = package_refs_args(&field.arguments);
        if lookups.contains_key(&args.key) {
            continue;
        }
        let result = load_package_refs_connection(pool, &args).await;
        lookups.insert(args.key, result);
    }
    lookups
}

async fn load_package_refs_connection(
    pool: &PgPool,
    args: &PackageRefsArgs,
) -> Result<PackageRefsConnection, String> {
    let scheme = args.scheme.clone()?;
    let after = match &args.after {
        Some(raw) => unmarshal_relay_i64(raw)?,
        None => 0,
    };
    let limit = args.first;
    // `PackageRepoReferences` passes `FuzzinessWildcard` (the public
    // `Service.ListPackageRepoRefs` maps a non-`ExactNameOnly` option to
    // `store.FuzzinessWildcard`, `service.go:68-80`): the baseline filters
    // `name LIKE '%<name>%'`, not an exact match.
    let name = match args.name.as_deref() {
        Some(name) => NameCond::Wildcard(name),
        None => NameCond::None,
    };
    let (deps, has_more) = list_package_repo_refs(pool, scheme, name, after, limit).await?;
    let total = count_package_repo_refs(pool, scheme, name).await?;
    Ok(PackageRefsConnection {
        deps,
        has_more,
        total,
    })
}

#[derive(Clone, Copy)]
enum NameCond<'a> {
    None,
    /// `FuzzinessWildcard`: `name LIKE '%<value>%'`.
    Wildcard(&'a str),
    /// `FuzzinessExactMatch`: `name = <value>`.
    Exact(&'a str),
    /// `FuzzinessRegex`: `name ~ <value>`.
    Regex(&'a str),
}

#[derive(Clone, Copy)]
enum NameMatch {
    Wildcard,
    Exact,
    Regex,
}

impl<'a> NameCond<'a> {
    /// The baseline applies both the condition and the exact-match-first
    /// ordering only when `opts.Name != ""` (`store.go:99-102,173-182`).
    fn spec(self) -> Option<(&'a str, NameMatch)> {
        let (value, kind) = match self {
            NameCond::None => return None,
            NameCond::Wildcard(value) => (value, NameMatch::Wildcard),
            NameCond::Exact(value) => (value, NameMatch::Exact),
            NameCond::Regex(value) => (value, NameMatch::Regex),
        };
        if value.is_empty() {
            None
        } else {
            Some((value, kind))
        }
    }
}

enum Param {
    Text(String),
    I64(i64),
}

/// `Store.ListPackageRepoRefs` (`store.go:87-160`): `IncludeBlocked=true`,
/// `JOIN LATERAL` so refs without versions are excluded, `ORDER BY
/// (CASE WHEN lr.name = $name THEN 1 ELSE 2 END) ASC, lr.id ASC`,
/// `LIMIT limit+1` and truncation back to `limit`.
async fn list_package_repo_refs(
    pool: &PgPool,
    scheme: Option<&str>,
    name: NameCond<'_>,
    after: i64,
    limit: i64,
) -> Result<(Vec<PackageRepoRefData>, bool), String> {
    let mut conds: Vec<String> = Vec::new();
    let mut params: Vec<Param> = Vec::new();
    if let Some(scheme) = scheme {
        params.push(Param::Text(scheme.to_owned()));
        conds.push(format!("lr.scheme = ${}", params.len()));
    }
    if let Some((value, kind)) = name.spec() {
        params.push(Param::Text(value.to_owned()));
        conds.push(match kind {
            NameMatch::Wildcard => {
                format!("lr.name LIKE ('%' || ${} || '%')", params.len())
            }
            NameMatch::Exact => format!("lr.name = ${}", params.len()),
            NameMatch::Regex => format!("lr.name ~ ${}", params.len()),
        });
    }
    if after > 0 {
        params.push(Param::I64(after));
        conds.push(format!("lr.id > ${}", params.len()));
    }
    let where_clause = if conds.is_empty() {
        "TRUE".to_owned()
    } else {
        conds.join(" AND ")
    };
    let order = if let Some((value, _)) = name.spec() {
        params.push(Param::Text(value.to_owned()));
        format!(
            "ORDER BY (CASE WHEN lr.name = ${} THEN 1 ELSE 2 END) ASC, lr.id ASC",
            params.len()
        )
    } else {
        "ORDER BY lr.id ASC".to_owned()
    };
    let limit_clause = if limit == 0 {
        "LIMIT ALL".to_owned()
    } else {
        format!("LIMIT {}", limit + 1)
    };
    let sql = format!(
        "SELECT lr.id, lr.scheme, lr.name, lr.blocked, \
         array_agg(prv.id ORDER BY prv.id) AS vid, \
         array_agg(prv.version ORDER BY prv.id) AS version, \
         array_agg(prv.blocked ORDER BY prv.id) AS vers_blocked \
         FROM lsif_dependency_repos lr \
         JOIN LATERAL ( \
             SELECT id, package_id, version, blocked FROM package_repo_versions \
             WHERE package_id = lr.id ORDER BY id \
         ) prv ON lr.id = prv.package_id \
         WHERE {where_clause} GROUP BY lr.id {order} {limit_clause}"
    );
    let mut query = sqlx::query(&sql);
    for param in params {
        query = match param {
            Param::Text(value) => query.bind(value),
            Param::I64(value) => query.bind(value),
        };
    }
    let rows = query
        .fetch_all(pool)
        .await
        .map_err(|error| error.to_string())?;
    let mut deps = Vec::with_capacity(rows.len());
    for row in rows {
        let id: i64 = row.try_get("id").map_err(|error| error.to_string())?;
        let ids: Vec<i64> = row.try_get("vid").map_err(|error| error.to_string())?;
        let versions: Vec<String> = row.try_get("version").map_err(|error| error.to_string())?;
        let mut ref_versions = Vec::with_capacity(versions.len());
        for (index, version) in versions.into_iter().enumerate() {
            ref_versions.push(PackageRepoRefVersionData {
                id: ids.get(index).copied().unwrap_or_default(),
                package_ref_id: id,
                version,
            });
        }
        deps.push(PackageRepoRefData {
            id,
            scheme: row.try_get("scheme").map_err(|error| error.to_string())?,
            name: row.try_get("name").map_err(|error| error.to_string())?,
            blocked: row.try_get("blocked").map_err(|error| error.to_string())?,
            versions: ref_versions,
        });
    }
    let mut has_more = false;
    if limit != 0 && deps.len() as i64 > limit {
        deps.truncate(limit as usize);
        has_more = true;
    }
    Ok((deps, has_more))
}

async fn count_package_repo_refs(
    pool: &PgPool,
    scheme: Option<&str>,
    name: NameCond<'_>,
) -> Result<i64, String> {
    let mut conds: Vec<String> = Vec::new();
    let mut params: Vec<Param> = Vec::new();
    if let Some(scheme) = scheme {
        params.push(Param::Text(scheme.to_owned()));
        conds.push(format!("lr.scheme = ${}", params.len()));
    }
    if let Some((value, kind)) = name.spec() {
        params.push(Param::Text(value.to_owned()));
        conds.push(match kind {
            NameMatch::Wildcard => {
                format!("lr.name LIKE ('%' || ${} || '%')", params.len())
            }
            NameMatch::Exact => format!("lr.name = ${}", params.len()),
            NameMatch::Regex => format!("lr.name ~ ${}", params.len()),
        });
    }
    let where_clause = if conds.is_empty() {
        "TRUE".to_owned()
    } else {
        conds.join(" AND ")
    };
    let sql = format!(
        "SELECT COUNT(DISTINCT lr.id) AS total FROM lsif_dependency_repos lr \
         JOIN LATERAL ( \
             SELECT id, package_id, version, blocked FROM package_repo_versions \
             WHERE package_id = lr.id ORDER BY id \
         ) prv ON lr.id = prv.package_id \
         WHERE {where_clause}"
    );
    let mut query = sqlx::query(&sql);
    for param in params {
        query = match param {
            Param::Text(value) => query.bind(value),
            Param::I64(value) => query.bind(value),
        };
    }
    let row = query
        .fetch_one(pool)
        .await
        .map_err(|error| error.to_string())?;
    row.try_get("total").map_err(|error| error.to_string())
}

// -- glob -> regex (`internal/packagefilters/glob.go`) -----------------------

/// `packagefilters.GlobToRegex` for the constructs the package filters use
/// (`*`, `**`, `?`, `{a,b}`, `[...]`, `\` escapes).
pub fn glob_to_regex(pattern: &str) -> Result<String, String> {
    let chars: Vec<char> = pattern.chars().collect();
    let (body, rest) = glob_sequence(&chars, 0, None)?;
    if rest != chars.len() {
        return Err("unexpected end of glob".to_owned());
    }
    Ok(format!("^{body}$"))
}

/// Convert a glob sequence, stopping at an unescaped `}` when `stop_on_close`.
fn glob_sequence(
    chars: &[char],
    mut index: usize,
    stop_on_close: Option<()>,
) -> Result<(String, usize), String> {
    let mut out = String::new();
    while index < chars.len() {
        let ch = chars[index];
        if stop_on_close.is_some() && ch == '}' {
            return Ok((out, index));
        }
        match ch {
            '*' => {
                // `**` (KindSuper) and `*` (KindAny) both become `.*`.
                if index + 1 < chars.len() && chars[index + 1] == '*' {
                    index += 2;
                } else {
                    index += 1;
                }
                out.push_str(".*");
            }
            '?' => {
                out.push('.');
                index += 1;
            }
            '{' => {
                index += 1;
                let mut alternatives = Vec::new();
                loop {
                    let (alternative, next) = glob_sequence(chars, index, Some(()))?;
                    alternatives.push(alternative);
                    if next >= chars.len() {
                        return Err("unexpected end of glob".to_owned());
                    }
                    if chars[next] == '}' {
                        index = next + 1;
                        break;
                    }
                    // A comma separates alternatives.
                    index = next + 1;
                }
                out.push_str("(?:");
                out.push_str(&alternatives.join("|"));
                out.push(')');
            }
            '[' => {
                let mut class = String::from("[");
                index += 1;
                if index < chars.len() && (chars[index] == '!' || chars[index] == '^') {
                    class.push('^');
                    index += 1;
                }
                let mut closed = false;
                while index < chars.len() {
                    if chars[index] == ']' {
                        closed = true;
                        index += 1;
                        break;
                    }
                    push_quoted(&mut class, chars[index]);
                    index += 1;
                }
                if !closed {
                    return Err("unexpected end of glob".to_owned());
                }
                class.push(']');
                out.push_str(&class);
            }
            '\\' => {
                index += 1;
                if index >= chars.len() {
                    return Err("unexpected end of glob".to_owned());
                }
                push_quoted(&mut out, chars[index]);
                index += 1;
            }
            ',' if stop_on_close.is_some() => {
                return Ok((out, index));
            }
            _ => {
                push_quoted(&mut out, ch);
                index += 1;
            }
        }
    }
    Ok((out, index))
}

fn push_quoted(out: &mut String, ch: char) {
    // `regexp.QuoteMeta`: `.` `+` `*` `?` `(` `)` `|` `[` `]` `{` `}` `^` `$`
    // and `\`. Note `-` is *not* escaped, so `[a-z]` keeps its range meaning
    // (`internal/packagefilters/glob.go:34,54,61`).
    if matches!(
        ch,
        '.' | '+' | '*' | '?' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$' | '\\'
    ) {
        out.push('\\');
    }
    out.push(ch);
}

/// `packagefilters.NewVersionGlob`'s `Matches` (`package_filters.go:134-146`):
/// the package name must match the filter's name exactly (`string(pkg) ==
/// v.packageName`; an empty name is *not* special) and the version must match
/// the glob as a whole string.
fn version_glob_matches(
    package_name: &str,
    expected_name: &str,
    glob: &str,
    version: &str,
) -> bool {
    if package_name != expected_name {
        return false;
    }
    match glob_to_regex(glob)
        .ok()
        .and_then(|regex| regex::Regex::new(&regex).ok())
    {
        Some(regex) => regex.is_match(version),
        None => false,
    }
}

// -- matching-filter arguments ----------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageNameFilterData {
    pub package_glob: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageVersionFilterData {
    pub package_name: String,
    pub version_glob: String,
}

pub struct MatchingFilterArgs {
    pub key: String,
    pub scheme: Option<&'static str>,
    pub name: Option<PackageNameFilterData>,
    pub version: Option<PackageVersionFilterData>,
    pub after: Option<String>,
    pub first: i64,
}

pub fn matching_filter_args(arguments: &Arguments) -> MatchingFilterArgs {
    let kind = arguments.string("kind").unwrap_or_default().to_owned();
    let mut name = None;
    let mut version = None;
    if let Some(InputValue::Object(filter)) = arguments.get("filter") {
        if let Some(InputValue::Object(name_filter)) = filter.get("nameFilter") {
            name = name_filter
                .get("packageGlob")
                .and_then(InputValue::as_str)
                .map(|value| PackageNameFilterData {
                    package_glob: value.to_owned(),
                });
        }
        if let Some(InputValue::Object(version_filter)) = filter.get("versionFilter") {
            let package_name = version_filter
                .get("packageName")
                .and_then(InputValue::as_str)
                .map(str::to_owned);
            let version_glob = version_filter
                .get("versionGlob")
                .and_then(InputValue::as_str)
                .map(str::to_owned);
            if let (Some(package_name), Some(version_glob)) = (package_name, version_glob) {
                version = Some(PackageVersionFilterData {
                    package_name,
                    version_glob,
                });
            } else {
                // A nil member would have failed GraphQL validation; keep an
                // explicit placeholder so the shape mirrors the input.
                version = Some(PackageVersionFilterData {
                    package_name: String::new(),
                    version_glob: String::new(),
                });
            }
        }
    }
    let after = arguments.string("after").map(str::to_owned);
    let first = arguments.i64("first").unwrap_or(0);
    let key = format!(
        "{kind:?}|{:?}|{:?}|{after:?}|{first}",
        name.as_ref().map(|name| name.package_glob.clone()),
        version
            .as_ref()
            .map(|version| (version.package_name.clone(), version.version_glob.clone())),
    );
    MatchingFilterArgs {
        key,
        scheme: external_service_to_scheme(&kind),
        name,
        version,
        after,
        first,
    }
}

/// `Query.packageRepoReferencesMatchingFilter` (`package_repo_filters.go:37-95`).
pub async fn prefetch_matching_filter(
    pool: &PgPool,
    fields: &[RootField],
) -> HashMap<String, Result<MatchingFilterResult, String>> {
    let mut lookups = HashMap::new();
    for field in fields {
        if field.name != "packageRepoReferencesMatchingFilter" {
            continue;
        }
        let args = matching_filter_args(&field.arguments);
        if lookups.contains_key(&args.key) {
            continue;
        }
        let result = load_matching_filter(pool, &args).await;
        lookups.insert(args.key, result);
    }
    lookups
}

async fn load_matching_filter(
    pool: &PgPool,
    args: &MatchingFilterArgs,
) -> Result<MatchingFilterResult, String> {
    if args.name.is_none() && args.version.is_none() {
        return Err("must provide either nameFilter or versionFilter".to_owned());
    }
    if args.name.is_some() && args.version.is_some() {
        return Err("cannot provide both a name filter and version filter".to_owned());
    }
    let after = match &args.after {
        Some(raw) => unmarshal_relay_i64(raw)?,
        None => 0,
    };
    let limit = args.first;

    if let Some(name) = &args.name {
        let regex = glob_to_regex(&name.package_glob)
            .map_err(|error| format!("failed to compile glob: {error}"))?;
        let mut matching = Vec::new();
        let mut total = 0i64;
        let mut has_more = false;
        let mut last_id = 0i64;
        loop {
            let (pkgs, _) =
                list_package_repo_refs(pool, args.scheme, NameCond::Regex(&regex), last_id, 500)
                    .await?;
            if pkgs.is_empty() {
                break;
            }
            last_id = pkgs[pkgs.len() - 1].id;
            total += pkgs.len() as i64;
            for mut pkg in pkgs {
                if pkg.id <= after {
                    continue;
                }
                if matching.len() as i64 == limit {
                    has_more = true;
                    continue;
                }
                pkg.versions = Vec::new();
                matching.push(pkg);
            }
        }
        return Ok(MatchingFilterResult::Packages(PackageRefsConnection {
            deps: matching,
            has_more,
            total,
        }));
    }

    let version = args.version.clone().expect("version filter");
    // Compile the glob up front; a malformed glob fails before any query.
    if glob_to_regex(&version.version_glob).is_err() {
        return Err("failed to compile glob".to_owned());
    }
    let (pkgs, _) = list_package_repo_refs(
        pool,
        args.scheme,
        NameCond::Exact(&version.package_name),
        0,
        1,
    )
    .await?;
    if pkgs.is_empty() {
        return Err(format!(
            "package repo reference not found for name {:?}",
            version.package_name
        ));
    }
    let pkg = pkgs.into_iter().next().expect("one package");
    let mut versions = Vec::new();
    let mut total = 0i64;
    let mut has_more = false;
    for candidate in pkg.versions {
        if !version_glob_matches(
            &pkg.name,
            &version.package_name,
            &version.version_glob,
            &candidate.version,
        ) {
            continue;
        }
        total += 1;
        if candidate.id <= after {
            continue;
        }
        if versions.len() as i64 == limit {
            has_more = true;
            continue;
        }
        versions.push(candidate);
    }
    Ok(MatchingFilterResult::Versions(VersionConnection {
        versions,
        has_more,
        total,
    }))
}

// -- package reference objects ----------------------------------------------

pub struct PackageRepoReferenceConnectionObject {
    pub data: PackageRefsConnection,
}

impl ObjectValue for PackageRepoReferenceConnectionObject {
    fn type_name(&self) -> &'static str {
        "PackageRepoReferenceConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: Some("PackageRepoReference"),
                items: self
                    .data
                    .deps
                    .iter()
                    .cloned()
                    .map(|dep| FieldValue::Object(Arc::new(PackageRepoReferenceObject { dep })))
                    .collect(),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(self.data.total)))),
            "pageInfo" => {
                let (has_next_page, end_cursor) =
                    if self.data.deps.is_empty() || !self.data.has_more {
                        (false, None)
                    } else {
                        let next = self.data.deps[self.data.deps.len() - 1].id;
                        (
                            true,
                            Some(marshal_relay_id_i64("PackageRepoReference", next)),
                        )
                    };
                Ok(Some(FieldValue::Object(Arc::new(
                    crate::graphql_resolvers::PageInfoObject::new(has_next_page, end_cursor),
                ))))
            }
            _ => Ok(None),
        }
    }
}

pub struct PackageRepoReferenceObject {
    pub dep: PackageRepoRefData,
}

impl ObjectValue for PackageRepoReferenceObject {
    fn type_name(&self) -> &'static str {
        "PackageRepoReference"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_relay_id_i64(
                "PackageRepoReference",
                self.dep.id,
            ))))),
            "kind" => Ok(Some(FieldValue::Json(Json::Str(
                scheme_to_external_service(&self.dep.scheme).to_owned(),
            )))),
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.dep.name.clone())))),
            "blocked" => Ok(Some(FieldValue::Json(Json::Bool(self.dep.blocked)))),
            // The matching-filter name path clears `pkg.Versions` before it
            // stores the reference (`service.go:276`), so `self.dep.versions`
            // is already empty there.
            "versions" => Ok(Some(FieldValue::List {
                element_type: Some("PackageRepoReferenceVersion"),
                items: self
                    .dep
                    .versions
                    .iter()
                    .cloned()
                    .map(|version| {
                        FieldValue::Object(Arc::new(PackageRepoReferenceVersionObject { version }))
                    })
                    .collect(),
            })),
            // `PackageRepoReference.repository`: needs the per-ecosystem
            // package name parsers; not ported in this slice.
            "repository" => Ok(None),
            _ => Ok(None),
        }
    }
}

pub struct PackageRepoReferenceVersionObject {
    pub version: PackageRepoRefVersionData,
}

impl ObjectValue for PackageRepoReferenceVersionObject {
    fn type_name(&self) -> &'static str {
        "PackageRepoReferenceVersion"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_relay_id_i64(
                "PackageRepoRefVersion",
                self.version.id,
            ))))),
            "packageRepoReferenceID" => Ok(Some(FieldValue::Json(Json::Str(
                marshal_relay_id_i64("PackageRepoReference", self.version.package_ref_id),
            )))),
            "version" => Ok(Some(FieldValue::Json(Json::Str(
                self.version.version.clone(),
            )))),
            _ => Ok(None),
        }
    }
}

pub struct PackageRepoReferenceVersionConnectionObject {
    pub data: VersionConnection,
}

impl ObjectValue for PackageRepoReferenceVersionConnectionObject {
    fn type_name(&self) -> &'static str {
        "PackageRepoReferenceVersionConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: Some("PackageRepoReferenceVersion"),
                items: self
                    .data
                    .versions
                    .iter()
                    .cloned()
                    .map(|version| {
                        FieldValue::Object(Arc::new(PackageRepoReferenceVersionObject { version }))
                    })
                    .collect(),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(self.data.total)))),
            "pageInfo" => {
                let (has_next_page, end_cursor) =
                    if self.data.versions.is_empty() || !self.data.has_more {
                        (false, None)
                    } else {
                        let next = self.data.versions[self.data.versions.len() - 1].id;
                        (
                            true,
                            Some(marshal_relay_id_i64("PackageRepoReferenceVersion", next)),
                        )
                    };
                Ok(Some(FieldValue::Object(Arc::new(
                    crate::graphql_resolvers::PageInfoObject::new(has_next_page, end_cursor),
                ))))
            }
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Indexed search instances
// ---------------------------------------------------------------------------

/// `Query.indexedSearchInstances` (`search.go:100-120`): site-admin gated;
/// `search.Indexers().Map.Endpoints()` is a configuration seam with no
/// endpoints in a bare frontend.
pub async fn prefetch_indexed_search_instances(
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
    endpoints: Option<&[String]>,
) -> Option<Result<Vec<String>, String>> {
    if !fields
        .iter()
        .any(|field| field.name == "indexedSearchInstances")
    {
        return None;
    }
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return Some(Err(message));
    }
    let mut addresses = endpoints.map(<[String]>::to_vec).unwrap_or_default();
    addresses.sort();
    Some(Ok(addresses))
}

pub struct IndexedSearchInstanceConnectionObject {
    pub addresses: Vec<String>,
}

impl ObjectValue for IndexedSearchInstanceConnectionObject {
    fn type_name(&self) -> &'static str {
        "IndexedSearchInstanceConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: Some("IndexedSearchInstance"),
                items: self
                    .addresses
                    .iter()
                    .cloned()
                    .map(|address| {
                        FieldValue::Object(Arc::new(IndexedSearchInstanceObject { address }))
                    })
                    .collect(),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(
                self.addresses.len() as i64
            )))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(
                crate::graphql_resolvers::PageInfoObject::new(false, None),
            )))),
            _ => Ok(None),
        }
    }
}

pub struct IndexedSearchInstanceObject {
    pub address: String,
}

impl ObjectValue for IndexedSearchInstanceObject {
    fn type_name(&self) -> &'static str {
        "IndexedSearchInstance"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_relay_id_str(
                "IndexedSearchInstance",
                &self.address,
            ))))),
            "address" => Ok(Some(FieldValue::Json(Json::Str(self.address.clone())))),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Outbound requests
// ---------------------------------------------------------------------------

/// One `httpcli` outbound request log entry (`types.OutboundRequestLogItem`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundRequestItem {
    pub id: String,
    pub started_at: OffsetDateTime,
    pub method: String,
    pub url: String,
    pub status_code: i32,
    pub duration_ms: i32,
}

/// `Query.outboundRequests` (`outbound_requests.go:36-75`): site-admin gated;
/// the in-process `httpcli` log is a seam (`None` = no entries).
pub async fn prefetch_outbound_requests(
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
    items: Option<&[OutboundRequestItem]>,
) -> Option<Result<Vec<OutboundRequestItem>, String>> {
    let field = fields
        .iter()
        .find(|field| field.name == "outboundRequests")?;
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return Some(Err(message));
    }
    let after = match field.arguments.string("after") {
        Some(raw) => match unmarshal_relay_string(raw) {
            Ok(after) => after,
            Err(message) => return Some(Err(message)),
        },
        None => String::new(),
    };
    let mut all = items
        .map(<[OutboundRequestItem]>::to_vec)
        .unwrap_or_default();
    if !after.is_empty() {
        all.retain(|item| item.id > after);
    }
    Some(Ok(all))
}

pub struct OutboundRequestConnectionObject {
    pub items: Vec<OutboundRequestItem>,
    pub first: Option<i64>,
}

impl ObjectValue for OutboundRequestConnectionObject {
    fn type_name(&self) -> &'static str {
        "OutboundRequestConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => {
                let mut items = self.items.clone();
                if let Some(first) = self.first {
                    if first > -1 && items.len() as i64 > first {
                        items.truncate(first as usize);
                    }
                }
                Ok(Some(FieldValue::List {
                    element_type: Some("OutboundRequest"),
                    items: items
                        .into_iter()
                        .map(|item| FieldValue::Object(Arc::new(OutboundRequestObject { item })))
                        .collect(),
                }))
            }
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(self.items.len() as i64)))),
            "pageInfo" => {
                let (has_next_page, end_cursor) = match self.first {
                    Some(first) if first > -1 && self.items.len() as i64 > first => {
                        let index = (first - 1).max(0) as usize;
                        let cursor = self
                            .items
                            .get(index)
                            .map(|item| marshal_relay_id_str("OutboundRequest", &item.id));
                        (true, cursor)
                    }
                    _ => (false, None),
                };
                Ok(Some(FieldValue::Object(Arc::new(
                    crate::graphql_resolvers::PageInfoObject::new(has_next_page, end_cursor),
                ))))
            }
            _ => Ok(None),
        }
    }
}

pub struct OutboundRequestObject {
    pub item: OutboundRequestItem,
}

impl ObjectValue for OutboundRequestObject {
    fn type_name(&self) -> &'static str {
        "OutboundRequest"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_relay_id_str(
                "OutboundRequest",
                &self.item.id,
            ))))),
            "startedAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.item.started_at,
            ))))),
            "method" => Ok(Some(FieldValue::Json(Json::Str(self.item.method.clone())))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(self.item.url.clone())))),
            "statusCode" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.item.status_code,
            ))))),
            "durationMs" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.item.duration_ms,
            ))))),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Organization invitations
// ---------------------------------------------------------------------------

/// A pending `org_invitations` row (`database.OrgInvitation`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvitationData {
    pub id: i64,
    pub org_id: i32,
    /// `orgs.name` for `RespondURL`'s legacy branch (`GetByID` at field
    /// resolution time in the baseline).
    pub org_name: Option<String>,
    pub sender_user_id: i32,
    pub recipient_user_id: Option<i32>,
    pub recipient_email: Option<String>,
    pub created_at: OffsetDateTime,
    pub notified_at: Option<OffsetDateTime>,
    pub responded_at: Option<OffsetDateTime>,
    pub response_type: Option<bool>,
    pub revoked_at: Option<OffsetDateTime>,
    pub expires_at: Option<OffsetDateTime>,
    pub is_verified_email: bool,
}

impl InvitationData {
    fn pending(&self) -> bool {
        self.responded_at.is_none()
            && self.revoked_at.is_none()
            && !self
                .expires_at
                .is_some_and(|expires_at| OffsetDateTime::now_utc() > expires_at)
    }
}

/// Load one pending invitation by id (`GetPendingByID`, `org_invitations.go:203`).
async fn get_pending_by_id(pool: &PgPool, id: i64) -> Result<InvitationData, String> {
    let row = sqlx::query(
        "SELECT i.id, i.org_id, i.sender_user_id, i.recipient_user_id, i.recipient_email, \
                i.created_at, i.notified_at, i.responded_at, i.response_type, i.revoked_at, \
                i.expires_at, o.name AS org_name \
         FROM org_invitations i LEFT JOIN orgs o ON o.id = i.org_id \
         WHERE i.id = $1 AND i.responded_at IS NULL AND i.revoked_at IS NULL \
           AND i.deleted_at IS NULL ORDER BY i.id ASC",
    )
    .bind(id)
    .fetch_optional(pool)
    .await
    .map_err(|error| error.to_string())?;
    let Some(row) = row else {
        return Err(format!("org invitation not found: [{id}]"));
    };
    let invitation = scan_invitation(&row)?;
    if invitation
        .expires_at
        .is_some_and(|expires_at| OffsetDateTime::now_utc() > expires_at)
    {
        return Err(format!("invitation with id {id} is expired"));
    }
    Ok(invitation)
}

fn scan_invitation(row: &sqlx::postgres::PgRow) -> Result<InvitationData, String> {
    Ok(InvitationData {
        id: row.try_get("id").map_err(|error| error.to_string())?,
        org_id: row.try_get("org_id").map_err(|error| error.to_string())?,
        org_name: row
            .try_get::<Option<String>, _>("org_name")
            .map_err(|error| error.to_string())?,
        sender_user_id: row
            .try_get("sender_user_id")
            .map_err(|error| error.to_string())?,
        recipient_user_id: row
            .try_get("recipient_user_id")
            .map_err(|error| error.to_string())?,
        recipient_email: row
            .try_get("recipient_email")
            .map_err(|error| error.to_string())?,
        created_at: row
            .try_get("created_at")
            .map_err(|error| error.to_string())?,
        notified_at: row
            .try_get("notified_at")
            .map_err(|error| error.to_string())?,
        responded_at: row
            .try_get("responded_at")
            .map_err(|error| error.to_string())?,
        response_type: row
            .try_get("response_type")
            .map_err(|error| error.to_string())?,
        revoked_at: row
            .try_get("revoked_at")
            .map_err(|error| error.to_string())?,
        expires_at: row
            .try_get("expires_at")
            .map_err(|error| error.to_string())?,
        is_verified_email: false,
    })
}

/// `checkEmail` (`org_invitations.go:67-105`): case-insensitive match against
/// the current user's emails; `Ok(true)` means the matching email is unverified.
async fn check_email(pool: &PgPool, uid: i32, invite_email: &str) -> Result<bool, String> {
    let rows = sqlx::query(
        "SELECT email, verified_at FROM user_emails WHERE user_id = $1 \
         ORDER BY created_at ASC, email ASC",
    )
    .bind(uid)
    .fetch_all(pool)
    .await
    .map_err(|error| error.to_string())?;
    let mut emails = Vec::with_capacity(rows.len());
    let mut matched: Option<Option<OffsetDateTime>> = None;
    for row in rows {
        let email: String = row.try_get("email").map_err(|error| error.to_string())?;
        let verified_at: Option<OffsetDateTime> = row
            .try_get("verified_at")
            .map_err(|error| error.to_string())?;
        if email.eq_ignore_ascii_case(invite_email) {
            matched = Some(verified_at);
        }
        emails.push(email);
    }
    match matched {
        None => Err(format!(
            "your email addresses [{}] do not match the email address on the invitation.",
            emails.join(" ")
        )),
        Some(None) => Ok(true),
        Some(Some(_)) => Ok(false),
    }
}

/// Verify an HS512 JWT and return `invite_id` (`orgInvitationClaims`).
fn verify_invitation_token(token: &str, signing_key: &str) -> Result<i64, String> {
    use base64::Engine as _;
    use hmac::Mac;

    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err("token contains an invalid number of segments".to_owned());
    }
    let key = base64::engine::general_purpose::STANDARD
        .decode(signing_key)
        .map_err(|error| error.to_string())?;
    type HmacSha512 = hmac::Hmac<sha2::Sha512>;
    let mut mac = HmacSha512::new_from_slice(&key).map_err(|error| error.to_string())?;
    mac.update(format!("{}.{}", parts[0], parts[1]).as_bytes());
    let signature = decode_url(parts[2]).ok_or_else(|| "illegal base64 data".to_owned())?;
    mac.verify_slice(&signature)
        .map_err(|_| "signature is invalid".to_owned())?;

    let header_bytes = decode_url(parts[0]).ok_or_else(|| "illegal base64 data".to_owned())?;
    let header: serde_json::Value =
        serde_json::from_slice(&header_bytes).map_err(|error| error.to_string())?;
    if header.get("alg").and_then(serde_json::Value::as_str) != Some("HS512") {
        return Err("signing method is invalid".to_owned());
    }
    let payload_bytes = decode_url(parts[1]).ok_or_else(|| "illegal base64 data".to_owned())?;
    let payload: serde_json::Value =
        serde_json::from_slice(&payload_bytes).map_err(|error| error.to_string())?;
    if let Some(exp) = payload.get("exp").and_then(serde_json::Value::as_i64) {
        if OffsetDateTime::now_utc().unix_timestamp() > exp {
            return Err("token is expired".to_owned());
        }
    }
    payload
        .get("invite_id")
        .and_then(serde_json::Value::as_i64)
        .ok_or_else(|| "invite_id missing".to_owned())
}

/// `Query.invitationByToken` (`org_invitations.go:125-164`).
pub async fn prefetch_invitation_by_token(
    pool: &PgPool,
    fields: &[RootField],
    actor: Actor,
    config: Option<&OrgInvitationConfig>,
) -> Option<Result<Option<InvitationData>, String>> {
    let field = fields
        .iter()
        .find(|field| field.name == "invitationByToken")?;
    let token = field.arguments.string("token").unwrap_or_default();
    if !actor.is_authenticated() {
        return Some(Err("no current user".to_owned()));
    }
    let Some(config) = config else {
        return Some(Err(SIGNING_KEY_VALIDATE.to_owned()));
    };
    let uid = actor.uid().unwrap_or_default();
    let invite_id = match verify_invitation_token(token, &config.signing_key) {
        Ok(id) => id,
        Err(message) => return Some(Err(message)),
    };
    let mut invitation = match get_pending_by_id(pool, invite_id).await {
        Ok(invitation) => invitation,
        Err(message) => return Some(Err(message)),
    };
    if invitation.recipient_user_id.unwrap_or_default() > 0
        && invitation.recipient_user_id != Some(uid)
    {
        return Some(Err(format!("org invitation not found: [{invite_id}]")));
    }
    if let Some(email) = invitation.recipient_email.clone() {
        if !email.is_empty() {
            match check_email(pool, uid, &email).await {
                Ok(will_verify) => invitation.is_verified_email = !will_verify,
                Err(message) => return Some(Err(message)),
            }
        }
    }
    Some(Ok(Some(invitation)))
}

pub struct OrganizationInvitationObject {
    pub data: InvitationData,
    pub config: Option<OrgInvitationConfig>,
}

impl ObjectValue for OrganizationInvitationObject {
    fn type_name(&self) -> &'static str {
        "OrganizationInvitation"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_relay_id_i64(
                "OrgInvitation",
                self.data.id,
            ))))),
            // `organization`/`sender`/`recipient` need prefetched org/user
            // lookups that only the (seam-gated) success path would use.
            "organization" | "sender" | "recipient" => Ok(None),
            "recipientEmail" => Ok(Some(optional_string(self.data.recipient_email.as_deref()))),
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.data.created_at,
            ))))),
            "notifiedAt" => Ok(Some(optional_datetime(self.data.notified_at))),
            "respondedAt" => Ok(Some(optional_datetime(self.data.responded_at))),
            "responseType" => Ok(Some(match self.data.response_type {
                Some(true) => FieldValue::Json(Json::Str("ACCEPT".to_owned())),
                Some(false) => FieldValue::Json(Json::Str("REJECT".to_owned())),
                None => FieldValue::Null,
            })),
            "respondURL" => {
                if !self.data.pending() {
                    return Ok(Some(FieldValue::Null));
                }
                match &self.config {
                    None => {
                        let Some(org_name) = &self.data.org_name else {
                            // `db.Orgs().GetByID(ctx, r.v.OrgID)` at field
                            // resolution time (`org_invitation.go:98-101`).
                            return Err(ExecError::new(format!(
                                "org not found: id {}",
                                self.data.org_id
                            )));
                        };
                        Ok(Some(FieldValue::Json(Json::Str(format!(
                            "/organizations/{org_name}/invitation"
                        )))))
                    }
                    Some(config) => {
                        let expires_at = match self.data.expires_at {
                            Some(expires_at) => expires_at,
                            None => {
                                return Err(ExecError::new(
                                    "invitation does not have expiry time defined",
                                ))
                            }
                        };
                        let token = create_invitation_jwt(
                            self.data.org_id,
                            self.data.id,
                            self.data.sender_user_id,
                            expires_at,
                            config,
                        )?;
                        Ok(Some(FieldValue::Json(Json::Str(format!(
                            "/organizations/invitation/{token}"
                        )))))
                    }
                }
            }
            "revokedAt" => Ok(Some(optional_datetime(self.data.revoked_at))),
            "expiresAt" => Ok(Some(optional_datetime(self.data.expires_at))),
            "isVerifiedEmail" => Ok(Some(FieldValue::Json(Json::Bool(
                self.data.is_verified_email,
            )))),
            _ => Ok(None),
        }
    }
}

fn optional_string(value: Option<&str>) -> FieldValue {
    match value {
        Some(value) => FieldValue::Json(Json::Str(value.to_owned())),
        None => FieldValue::Null,
    }
}

fn optional_datetime(value: Option<OffsetDateTime>) -> FieldValue {
    match value {
        Some(value) => FieldValue::Json(Json::Str(datetime_rfc3339(value))),
        None => FieldValue::Null,
    }
}

fn create_invitation_jwt(
    org_id: i32,
    invitation_id: i64,
    sender_id: i32,
    expires_at: OffsetDateTime,
    config: &OrgInvitationConfig,
) -> Result<String, ExecError> {
    use base64::Engine as _;
    use hmac::Mac;

    // `golang-jwt/jwt/v4` marshals the embedded `RegisteredClaims` after the
    // two custom fields and applies `omitempty` to `iss`, so the token is only
    // byte-identical when the field order and the omitted empty issuer match.
    #[derive(serde::Serialize)]
    struct InvitationClaims<'a> {
        invite_id: i64,
        sender_id: i32,
        #[serde(skip_serializing_if = "str::is_empty")]
        iss: &'a str,
        sub: String,
        exp: i64,
    }

    let header = serde_json::json!({"alg": "HS512", "typ": "JWT"});
    let payload = InvitationClaims {
        invite_id: invitation_id,
        sender_id,
        iss: &config.external_url,
        sub: org_id.to_string(),
        exp: expires_at.unix_timestamp(),
    };
    let encode = |value: &serde_json::Value| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(value).expect("jwt json"))
    };
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&payload).expect("jwt json"));
    let signing_input = format!("{}.{}", encode(&header), payload);
    let key = base64::engine::general_purpose::STANDARD
        .decode(&config.signing_key)
        .map_err(|error| ExecError::new(error.to_string()))?;
    type HmacSha512 = hmac::Hmac<sha2::Sha512>;
    let mut mac =
        HmacSha512::new_from_slice(&key).map_err(|error| ExecError::new(error.to_string()))?;
    mac.update(signing_input.as_bytes());
    let signature =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
    Ok(format!("{signing_input}.{signature}"))
}

// ---------------------------------------------------------------------------
// Mutation outcomes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InventoryBOutcome {
    /// `EmptyResponse`.
    Empty,
    /// `InviteUserToOrganizationResult`.
    InviteUser {
        sent_invitation_email: bool,
        invitation_url: String,
    },
    Error(String),
}

pub struct InviteUserToOrganizationResultObject {
    pub sent_invitation_email: bool,
    pub invitation_url: String,
}

impl ObjectValue for InviteUserToOrganizationResultObject {
    fn type_name(&self) -> &'static str {
        "InviteUserToOrganizationResult"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "sentInvitationEmail" => Ok(Some(FieldValue::Json(Json::Bool(
                self.sent_invitation_email,
            )))),
            "invitationURL" => Ok(Some(FieldValue::Json(Json::Str(
                self.invitation_url.clone(),
            )))),
            _ => Ok(None),
        }
    }
}

/// Prefetch the batch-B mutations in document order.
pub async fn prefetch_mutations(
    pool: &PgPool,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
    config: Option<&OrgInvitationConfig>,
) -> Vec<(String, InventoryBOutcome)> {
    let mut outcomes = Vec::new();
    for field in fields {
        let outcome = match field.name.as_str() {
            "inviteUserToOrganization" => {
                invite_user_to_organization(pool, field, actor, config).await
            }
            "respondToOrganizationInvitation" => {
                respond_to_organization_invitation(pool, field, actor).await
            }
            "addRepoMetadata" | "addRepoKeyValuePair" => {
                repo_metadata(pool, field, actor, viewer_site_admin, MetadataOp::Add).await
            }
            "updateRepoMetadata" | "updateRepoKeyValuePair" => {
                repo_metadata(pool, field, actor, viewer_site_admin, MetadataOp::Update).await
            }
            "deleteRepoMetadata" | "deleteRepoKeyValuePair" => {
                repo_metadata(pool, field, actor, viewer_site_admin, MetadataOp::Delete).await
            }
            "invalidateSessionsByID" | "invalidateSessionsByIDs" => {
                invalidate_sessions(pool, field, actor, viewer_site_admin).await
            }
            "recoverUsers" => recover_users(pool, field, actor, viewer_site_admin).await,
            "updatePassword" => update_password(pool, field, actor).await,
            "createPassword" => {
                InventoryBOutcome::Error("only allowed from user session".to_owned())
            }
            _ => continue,
        };
        outcomes.push((field.name.clone(), outcome));
    }
    outcomes
}

/// `auth.CheckOrgAccessOrSiteAdmin` (`auth/orgs.go:20-41`): internal actors
/// pass; otherwise the current user must exist and be a site admin or a member
/// of the organization. Returns the acting user's id.
async fn check_org_access(pool: &PgPool, actor: Actor, org_id: i32) -> Result<i32, String> {
    if actor.is_internal() {
        // The baseline returns nil without resolving a user; the caller then
        // dereferences a nil sender, so an internal actor cannot complete the
        // flow (`org_invitations.go:185,203`).
        return Err("internal actor has no current user".to_owned());
    }
    let (uid, site_admin) = current_user(pool, actor).await?;
    if site_admin {
        return Ok(uid);
    }
    match is_org_member(pool, org_id, uid).await? {
        true => Ok(uid),
        false => Err("current user is not an org member".to_owned()),
    }
}

/// `auth.GetByCurrentAuthUser` as an error string: anonymous → `no current
/// user`; a missing row → `user not found: [<uid>]`.
async fn current_user(pool: &PgPool, actor: Actor) -> Result<(i32, bool), String> {
    let Some(uid) = actor.uid() else {
        return Err("no current user".to_owned());
    };
    let row = sqlx::query("SELECT id, site_admin FROM users WHERE id = $1 AND deleted_at IS NULL")
        .bind(uid)
        .fetch_optional(pool)
        .await
        .map_err(|error| error.to_string())?;
    let Some(row) = row else {
        return Err(format!("user not found: [{uid}]"));
    };
    let site_admin: bool = row
        .try_get("site_admin")
        .map_err(|error| error.to_string())?;
    Ok((uid, site_admin))
}

async fn invite_user_to_organization(
    pool: &PgPool,
    field: &RootField,
    actor: Actor,
    config: Option<&OrgInvitationConfig>,
) -> InventoryBOutcome {
    let org_id = match field
        .arguments
        .string("organization")
        .map(unmarshal_relay_id)
    {
        Some(Ok(id)) => id,
        Some(Err(message)) => return InventoryBOutcome::Error(message),
        None => return InventoryBOutcome::Error("invalid graphql.ID".to_owned()),
    };
    // `auth.CheckOrgAccessOrSiteAdmin` (`auth/orgs.go:20-41`).
    let sender_id = match check_org_access(pool, actor, org_id).await {
        Ok(sender_id) => sender_id,
        Err(message) => return InventoryBOutcome::Error(message),
    };
    // `db.Orgs().GetByID` (`orgs.go:109`).
    let org_name = match org_name_by_id(pool, org_id).await {
        Ok(name) => name,
        Err(message) => return InventoryBOutcome::Error(message),
    };
    // `db.Users().GetByCurrentAuthUser` (`org_invitations.go:185`).
    if let Err(message) = current_user(pool, actor).await {
        return InventoryBOutcome::Error(message);
    }
    let username = match field.arguments.string("username") {
        Some(username) => username,
        None => return InventoryBOutcome::Error("missing username".to_owned()),
    };
    // `getUserToInviteToOrganization` (`org_invitations.go:33-58`); email
    // verification is skipped because `conf.CanSendEmail()` is false.
    let recipient_id = match user_id_by_username(pool, username).await {
        Ok(Some(id)) => id,
        Ok(None) => return InventoryBOutcome::Error(format!("user not found: [{username}]")),
        Err(message) => return InventoryBOutcome::Error(message),
    };
    match is_org_member(pool, org_id, recipient_id).await {
        Ok(true) => {
            return InventoryBOutcome::Error(
                "user is already a member of the organization".to_owned(),
            )
        }
        Ok(false) => {}
        Err(message) => return InventoryBOutcome::Error(message),
    }
    // `OrgInvitations().Create` (`org_invitations.go:110`): reuse a pending
    // invitation for the same (org, recipient), else insert a new one.
    let existing = sqlx::query(
        "SELECT id, org_id, sender_user_id, recipient_user_id, recipient_email, created_at, \
                notified_at, responded_at, response_type, revoked_at, expires_at, \
                NULL::text AS org_name \
         FROM org_invitations \
         WHERE org_id = $1 AND recipient_user_id = $2 AND responded_at IS NULL \
           AND revoked_at IS NULL AND expires_at > now() AND deleted_at IS NULL \
         ORDER BY id ASC",
    )
    .bind(org_id)
    .bind(recipient_id)
    .fetch_all(pool)
    .await;
    let invitation = match existing {
        Ok(rows) => match rows.last() {
            Some(row) => match scan_invitation(row) {
                Ok(mut invitation) => {
                    invitation.org_name = Some(org_name.clone());
                    invitation
                }
                Err(message) => return InventoryBOutcome::Error(message),
            },
            None => {
                let expires_at = OffsetDateTime::now_utc() + time::Duration::hours(48);
                match sqlx::query(
                    "INSERT INTO org_invitations(org_id, sender_user_id, recipient_user_id, expires_at) \
                     VALUES($1, $2, $3, $4) RETURNING id, created_at",
                )
                .bind(org_id)
                .bind(sender_id)
                .bind(recipient_id)
                .bind(expires_at)
                .fetch_one(pool)
                .await
                {
                    Ok(row) => {
                        let id: i64 = match row.try_get("id") {
                            Ok(id) => id,
                            Err(error) => {
                                return InventoryBOutcome::Error(error.to_string())
                            }
                        };
                        let created_at: OffsetDateTime = match row.try_get("created_at") {
                            Ok(value) => value,
                            Err(error) => {
                                return InventoryBOutcome::Error(error.to_string())
                            }
                        };
                        InvitationData {
                            id,
                            org_id,
                            org_name: Some(org_name.clone()),
                            sender_user_id: sender_id,
                            recipient_user_id: Some(recipient_id),
                            recipient_email: None,
                            created_at,
                            notified_at: None,
                            responded_at: None,
                            response_type: None,
                            revoked_at: None,
                            expires_at: Some(expires_at),
                            is_verified_email: false,
                        }
                    }
                    Err(error) => return InventoryBOutcome::Error(error.to_string()),
                }
            }
        },
        Err(error) => return InventoryBOutcome::Error(error.to_string()),
    };
    // `orgInvitationURL`/`orgInvitationURLLegacy` with `relative=false`
    // resolve the path against `conf.ExternalURLParsed()`
    // (`org_invitations.go:305-329`).
    let absolute = |path: String| {
        match config {
            Some(config) if !config.external_url.is_empty() => {
                format!("{}{path}", config.external_url.trim_end_matches('/'))
            }
            // Without the seam the configured external URL is not modelled.
            _ => path,
        }
    };
    let invitation_url = match config {
        None => absolute(format!("/organizations/{org_name}/invitation")),
        Some(config) => {
            let expires_at = invitation
                .expires_at
                .unwrap_or_else(OffsetDateTime::now_utc);
            match create_invitation_jwt(
                invitation.org_id,
                invitation.id,
                invitation.sender_user_id,
                expires_at,
                config,
            ) {
                Ok(token) => absolute(format!("/organizations/invitation/{token}")),
                Err(error) => return InventoryBOutcome::Error(error.message),
            }
        }
    };
    InventoryBOutcome::InviteUser {
        // `conf.CanSendEmail()` is false without SMTP configuration.
        sent_invitation_email: false,
        invitation_url,
    }
}

async fn respond_to_organization_invitation(
    pool: &PgPool,
    field: &RootField,
    actor: Actor,
) -> InventoryBOutcome {
    let Some(uid) = actor.uid() else {
        return InventoryBOutcome::Error("no current user".to_owned());
    };
    let id = match field
        .arguments
        .string("organizationInvitation")
        .map(unmarshal_relay_i64)
    {
        Some(Ok(id)) => id,
        Some(Err(message)) => return InventoryBOutcome::Error(message),
        None => return InventoryBOutcome::Error("invalid graphql.ID".to_owned()),
    };
    let response_type = field.arguments.string("responseType").unwrap_or_default();
    let accept = match response_type {
        "ACCEPT" => true,
        "REJECT" => false,
        other => {
            return InventoryBOutcome::Error(format!(
                "invalid OrganizationInvitationResponseType value {other:?}"
            ))
        }
    };
    let invitation = match get_pending_by_id(pool, id).await {
        Ok(invitation) => invitation,
        Err(message) => return InventoryBOutcome::Error(message),
    };
    if let Some(email) = invitation.recipient_email.clone() {
        if !email.is_empty() {
            match check_email(pool, uid, &email).await {
                Ok(should_mark_verified) => {
                    if should_mark_verified && accept {
                        // Best-effort `SetVerified` (`org_invitations.go:262`).
                        let _ = sqlx::query(
                            "UPDATE user_emails SET verified_at = now() \
                             WHERE user_id = $1 AND email = $2 AND verified_at IS NULL",
                        )
                        .bind(uid)
                        .bind(&email)
                        .execute(pool)
                        .await;
                    }
                }
                Err(message) => return InventoryBOutcome::Error(message),
            }
        }
    } else if invitation.recipient_user_id.unwrap_or_default() > 0
        && invitation.recipient_user_id != Some(uid)
    {
        return InventoryBOutcome::Error(format!("org invitation not found: [{id}]"));
    }
    let org_id: Option<i32> = match sqlx::query_scalar(
        "UPDATE org_invitations SET responded_at = now(), response_type = $2 \
         WHERE id = $1 AND responded_at IS NULL AND revoked_at IS NULL \
           AND deleted_at IS NULL AND expires_at > now() RETURNING org_id",
    )
    .bind(id)
    .bind(accept)
    .fetch_optional(pool)
    .await
    {
        Ok(org_id) => org_id,
        Err(error) => return InventoryBOutcome::Error(error.to_string()),
    };
    let Some(org_id) = org_id else {
        return InventoryBOutcome::Error(format!(
            "org invitation not found: [id {id} recipient {uid}]"
        ));
    };
    if accept {
        if let Err(error) = sqlx::query("INSERT INTO org_members(org_id, user_id) VALUES($1, $2)")
            .bind(org_id)
            .bind(uid)
            .execute(pool)
            .await
        {
            return InventoryBOutcome::Error(error.to_string());
        }
    }
    InventoryBOutcome::Empty
}

async fn org_name_by_id(pool: &PgPool, org_id: i32) -> Result<String, String> {
    let name: Option<String> =
        sqlx::query_scalar("SELECT name FROM orgs WHERE id = $1 AND deleted_at IS NULL")
            .bind(org_id)
            .fetch_optional(pool)
            .await
            .map_err(|error| error.to_string())?;
    name.ok_or_else(|| format!("org not found: id {org_id}"))
}

async fn user_id_by_username(pool: &PgPool, username: &str) -> Result<Option<i32>, String> {
    sqlx::query_scalar("SELECT id FROM users WHERE username = $1 AND deleted_at IS NULL LIMIT 1")
        .bind(username)
        .fetch_optional(pool)
        .await
        .map_err(|error| error.to_string())
}

async fn is_org_member(pool: &PgPool, org_id: i32, user_id: i32) -> Result<bool, String> {
    let row: Option<(i32,)> =
        sqlx::query_as("SELECT id FROM org_members WHERE org_id = $1 AND user_id = $2")
            .bind(org_id)
            .bind(user_id)
            .fetch_optional(pool)
            .await
            .map_err(|error| error.to_string())?;
    Ok(row.is_some())
}

// -- repo metadata ----------------------------------------------------------

enum MetadataOp {
    Add,
    Update,
    Delete,
}

/// `rbac.CheckCurrentUserHasPermission(ctx, db, "REPO_METADATA#WRITE")`
/// (`internal/rbac/permission.go:28`).
async fn check_repo_metadata_permission(pool: &PgPool, actor: Actor) -> Result<(), String> {
    if actor.is_internal() {
        return Ok(());
    }
    let (uid, _) = current_user(pool, actor).await?;
    let row: Option<(i32,)> = sqlx::query_as(
        "SELECT permissions.id FROM permissions \
         INNER JOIN role_permissions ON role_permissions.permission_id = permissions.id \
         INNER JOIN user_roles ON user_roles.role_id = role_permissions.role_id \
         WHERE permissions.action = 'WRITE' AND permissions.namespace = 'REPO_METADATA' \
           AND user_roles.user_id = $1 LIMIT 1",
    )
    .bind(uid)
    .fetch_optional(pool)
    .await
    .map_err(|error| error.to_string())?;
    if row.is_none() {
        return Err("user is missing permission REPO_METADATA#WRITE".to_owned());
    }
    Ok(())
}

async fn repo_metadata(
    pool: &PgPool,
    field: &RootField,
    actor: Actor,
    _viewer_site_admin: bool,
    op: MetadataOp,
) -> InventoryBOutcome {
    if let Err(message) = check_repo_metadata_permission(pool, actor).await {
        return InventoryBOutcome::Error(message);
    }
    // `featureflag.GetBoolOr("repository-metadata", true)`: a missing flag row
    // is enabled; the database flag store is not consulted here.
    let repo_id = match field.arguments.string("repo").map(unmarshal_relay_id) {
        Some(Ok(id)) => id,
        Some(Err(message)) => return InventoryBOutcome::Error(message),
        None => return InventoryBOutcome::Error("invalid graphql.ID".to_owned()),
    };
    let key = match field.arguments.string("key") {
        Some(key) => key.to_owned(),
        None => return InventoryBOutcome::Error("missing key".to_owned()),
    };
    let value = field.arguments.string("value").map(str::to_owned);
    if let Some(value) = &value {
        if value.trim().is_empty() {
            return InventoryBOutcome::Error(format!(
                "value should be null or non-empty string, got {value:?}"
            ));
        }
    }
    match op {
        MetadataOp::Add => {
            match sqlx::query("INSERT INTO repo_kvps (repo_id, key, value) VALUES ($1, $2, $3)")
                .bind(repo_id)
                .bind(&key)
                .bind(value)
                .execute(pool)
                .await
            {
                Ok(_) => InventoryBOutcome::Empty,
                Err(error) => {
                    if error
                        .as_database_error()
                        .and_then(|error| error.code())
                        .as_deref()
                        == Some("23505")
                    {
                        InventoryBOutcome::Error(format!(
                            "metadata key {key:?} already exists for the given repository"
                        ))
                    } else {
                        InventoryBOutcome::Error(error.to_string())
                    }
                }
            }
        }
        MetadataOp::Update => {
            let updated: Option<(String,)> = match sqlx::query_as(
                "UPDATE repo_kvps SET value = $1 WHERE repo_id = $2 AND key = $3 \
                 RETURNING key",
            )
            .bind(value)
            .bind(repo_id)
            .bind(&key)
            .fetch_optional(pool)
            .await
            {
                Ok(row) => row,
                Err(error) => return InventoryBOutcome::Error(error.to_string()),
            };
            if updated.is_none() {
                InventoryBOutcome::Error(format!(
                    "metadata key {key:?} does not exist for the given repository"
                ))
            } else {
                InventoryBOutcome::Empty
            }
        }
        MetadataOp::Delete => {
            match sqlx::query("DELETE FROM repo_kvps WHERE repo_id = $1 AND key = $2")
                .bind(repo_id)
                .bind(&key)
                .execute(pool)
                .await
            {
                Ok(_) => InventoryBOutcome::Empty,
                Err(error) => InventoryBOutcome::Error(error.to_string()),
            }
        }
    }
}

// -- session invalidation / recovery / passwords ----------------------------

async fn invalidate_sessions(
    pool: &PgPool,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> InventoryBOutcome {
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return InventoryBOutcome::Error(message);
    }
    let raw_ids: Vec<String> = match field.arguments.get("userIDs") {
        Some(InputValue::List(items)) => items
            .iter()
            .filter_map(|item| item.as_str().map(str::to_owned))
            .collect(),
        _ => Vec::new(),
    };
    let raw_ids = if raw_ids.is_empty() {
        match field.arguments.string("userID") {
            Some(id) => vec![id.to_owned()],
            None => raw_ids,
        }
    } else {
        raw_ids
    };
    if raw_ids.is_empty() {
        return InventoryBOutcome::Error("must specify at least one user ID".to_owned());
    }
    let mut ids = Vec::with_capacity(raw_ids.len());
    for raw in &raw_ids {
        match unmarshal_relay_id(raw) {
            Ok(id) => ids.push(id),
            Err(message) => return InventoryBOutcome::Error(message),
        }
    }
    let placeholders = (1..=ids.len())
        .map(|index| format!("${index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "UPDATE users SET updated_at = now(), invalidated_sessions_at = now() \
         WHERE id IN ({placeholders})"
    );
    let mut query = sqlx::query(&sql);
    for id in &ids {
        query = query.bind(*id);
    }
    let result = match query.execute(pool).await {
        Ok(result) => result,
        Err(error) => return InventoryBOutcome::Error(error.to_string()),
    };
    let affected = result.rows_affected() as i64;
    if affected != ids.len() as i64 {
        // `userStore.InvalidateSessionsByIDs` wraps the message in
        // `userNotFoundErr` (`users.go:1056-1058`): `user not found: [...]`.
        return InventoryBOutcome::Error(format!(
            "user not found: [Some users were not found. Expected to invalidate sessions of {} \
             users, but invalidated sessions only {}]",
            ids.len(),
            affected
        ));
    }
    InventoryBOutcome::Empty
}

async fn recover_users(
    pool: &PgPool,
    field: &RootField,
    actor: Actor,
    viewer_site_admin: bool,
) -> InventoryBOutcome {
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return InventoryBOutcome::Error(message);
    }
    let raw_ids: Vec<String> = match field.arguments.get("userIDs") {
        Some(InputValue::List(items)) => items
            .iter()
            .filter_map(|item| item.as_str().map(str::to_owned))
            .collect(),
        _ => Vec::new(),
    };
    if raw_ids.is_empty() {
        return InventoryBOutcome::Error("must specify at least one user ID".to_owned());
    }
    let mut ids = Vec::with_capacity(raw_ids.len());
    for raw in &raw_ids {
        match unmarshal_relay_id(raw) {
            Ok(id) => ids.push(id),
            Err(message) => return InventoryBOutcome::Error(message),
        }
    }
    let uid = actor.uid().unwrap_or_default();
    if ids.contains(&uid) {
        return InventoryBOutcome::Error("unable to recover current user".to_owned());
    }
    let placeholders = (1..=ids.len())
        .map(|index| format!("${index}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut transaction = match pool.begin().await {
        Ok(transaction) => transaction,
        Err(error) => return InventoryBOutcome::Error(error.to_string()),
    };
    macro_rules! run {
        ($sql:expr) => {{
            let mut query = sqlx::query($sql);
            for id in &ids {
                query = query.bind(*id);
            }
            if let Err(error) = query.execute(&mut *transaction).await {
                return InventoryBOutcome::Error(error.to_string());
            }
        }};
    }
    // `INSERT INTO names(name, user_id) SELECT username, id ...`
    {
        let sql = format!(
            "INSERT INTO names(name, user_id) SELECT username, id FROM users \
             WHERE id IN ({placeholders})"
        );
        let mut query = sqlx::query(&sql);
        for id in &ids {
            query = query.bind(*id);
        }
        if let Err(error) = query.execute(&mut *transaction).await {
            return InventoryBOutcome::Error(error.to_string());
        }
    }
    for template in [
        "UPDATE access_tokens AS a SET deleted_at = NULL FROM users AS u \
         WHERE a.creator_user_id = u.id AND a.deleted_at >= u.deleted_at \
           AND a.deleted_at <= u.deleted_at + interval '10 second' \
           AND (a.creator_user_id IN (IDS) OR a.subject_user_id IN (IDS))",
        "UPDATE user_external_accounts AS a SET deleted_at = NULL, updated_at = now() \
         FROM users AS u WHERE a.user_id = u.id AND a.deleted_at >= u.deleted_at \
           AND a.deleted_at <= u.deleted_at + interval '10 second' AND a.user_id IN (IDS)",
        "UPDATE org_invitations AS o SET deleted_at = NULL FROM users AS u \
         WHERE o.recipient_user_id = u.id AND o.deleted_at >= u.deleted_at \
           AND o.deleted_at <= u.deleted_at + interval '10 second' \
           AND (o.sender_user_id IN (IDS) OR o.recipient_user_id IN (IDS))",
        "UPDATE registry_extensions AS r SET deleted_at = NULL, updated_at = now() \
         FROM users AS u WHERE r.publisher_user_id = u.id AND r.deleted_at >= u.deleted_at \
           AND r.deleted_at <= u.deleted_at + interval '10 second' \
           AND r.publisher_user_id IN (IDS)",
    ] {
        let sql = template.replace("IDS", &placeholders);
        run!(&sql);
    }
    let update_sql = format!(
        "UPDATE users SET deleted_at = NULL, updated_at = now() \
         WHERE id IN ({placeholders}) AND deleted_at IS NOT NULL RETURNING id"
    );
    let returned: Vec<(i32,)> = {
        let mut query = sqlx::query_as(&update_sql);
        for id in &ids {
            query = query.bind(*id);
        }
        match query.fetch_all(&mut *transaction).await {
            Ok(rows) => rows,
            Err(error) => return InventoryBOutcome::Error(error.to_string()),
        }
    };
    if let Err(error) = transaction.commit().await {
        return InventoryBOutcome::Error(error.to_string());
    }
    if returned.len() != ids.len() {
        let recovered: Vec<i32> = returned.into_iter().map(|(id,)| id).collect();
        let missing: Vec<String> = ids
            .iter()
            .filter(|id| !recovered.contains(id))
            .map(|id| marshal_relay_id_i64("User", i64::from(*id)))
            .collect();
        return InventoryBOutcome::Error(format!(
            "some users were not found, expected to recover {} users, but found only {} users. \
             Missing user IDs: [{}]",
            ids.len(),
            recovered.len(),
            missing.join(" ")
        ));
    }
    InventoryBOutcome::Empty
}

async fn update_password(pool: &PgPool, field: &RootField, actor: Actor) -> InventoryBOutcome {
    let uid = match current_user(pool, actor).await {
        Ok((uid, _)) => uid,
        Err(message) => return InventoryBOutcome::Error(message),
    };
    let old_password = field.arguments.string("oldPassword").unwrap_or_default();
    let new_password = field.arguments.string("newPassword").unwrap_or_default();
    if old_password.is_empty() {
        return InventoryBOutcome::Error("old password was empty".to_owned());
    }
    let passwd: Option<Option<String>> =
        match sqlx::query_scalar("SELECT passwd FROM users WHERE deleted_at IS NULL AND id = $1")
            .bind(uid)
            .fetch_optional(pool)
            .await
        {
            Ok(passwd) => passwd,
            Err(error) => return InventoryBOutcome::Error(error.to_string()),
        };
    // `IsPassword` (`users.go:1423-1432`): a missing row is a query error
    // (`sql: no rows in result set`), a NULL `passwd` compares as false
    // (`validPassword` is not called), which surfaces as `wrong old password`.
    let passwd = match passwd {
        Some(Some(passwd)) => passwd,
        Some(None) => return InventoryBOutcome::Error("wrong old password".to_owned()),
        None => return InventoryBOutcome::Error("sql: no rows in result set".to_owned()),
    };
    match bcrypt::verify(old_password, &passwd) {
        Ok(true) => {}
        Ok(false) => return InventoryBOutcome::Error("wrong old password".to_owned()),
        Err(error) => return InventoryBOutcome::Error(error.to_string()),
    }
    if let Err(message) = validate_password(new_password) {
        return InventoryBOutcome::Error(message);
    }
    let hash = match bcrypt::hash(new_password, bcrypt::DEFAULT_COST) {
        Ok(hash) => hash,
        Err(error) => return InventoryBOutcome::Error(error.to_string()),
    };
    match sqlx::query(
        "UPDATE users SET passwd_reset_code = NULL, passwd_reset_time = NULL, passwd = $1 \
         WHERE id = $2",
    )
    .bind(hash)
    .bind(uid)
    .execute(pool)
    .await
    {
        Ok(_) => InventoryBOutcome::Empty,
        Err(error) => InventoryBOutcome::Error(error.to_string()),
    }
}

/// `security.ValidatePassword` with the default (no password policy) method.
fn validate_password(password: &str) -> Result<(), String> {
    if password.is_empty() {
        return Err("Your password may not be empty.".to_owned());
    }
    let length = password.chars().count();
    if !(12..=256).contains(&length) {
        return Err(
            "Your password may not be less than 12 or be more than 256 characters.".to_owned(),
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Request-level aggregation
// ---------------------------------------------------------------------------

/// The M9.17a batch-B roots, prefetched once per GraphQL request.
#[derive(Debug, Clone, Default)]
pub struct InventoryBRoots {
    /// `Query.packageRepoReferences`, keyed by [`package_refs_args`].
    pub package_references: HashMap<String, Result<PackageRefsConnection, String>>,
    /// `Query.packageRepoReferencesMatchingFilter`, keyed by
    /// [`matching_filter_args`].
    pub matching_filter: HashMap<String, Result<MatchingFilterResult, String>>,
    /// `Query.indexedSearchInstances` (site-admin gated; endpoint seam).
    pub indexed_search_instances: Option<Result<Vec<String>, String>>,
    /// `Query.outboundRequests` (site-admin gated; log seam).
    pub outbound_requests: Option<Result<Vec<OutboundRequestItem>, String>>,
    /// `Query.invitationByToken` (auth gate; signing-key seam).
    pub invitation_by_token: Option<Result<Option<InvitationData>, String>>,
    /// Batch-B mutations keyed by field name.
    pub mutations: HashMap<String, InventoryBOutcome>,
    /// `conf.SiteConfig().OrganizationInvitations` (seam; `None` = no signing
    /// key). Replayed by `OrganizationInvitation.respondURL`.
    pub org_invitation_config: Option<OrgInvitationConfig>,
}

impl InventoryBRoots {
    /// Resolve one prefetched mutation outcome as the GraphQL value.
    pub fn mutation_value(outcome: &InventoryBOutcome) -> Result<Option<FieldValue>, ExecError> {
        match outcome {
            InventoryBOutcome::Empty => Ok(Some(FieldValue::Object(Arc::new(
                crate::temporary_settings::EmptyResponseObject,
            )))),
            InventoryBOutcome::InviteUser {
                sent_invitation_email,
                invitation_url,
            } => Ok(Some(FieldValue::Object(Arc::new(
                InviteUserToOrganizationResultObject {
                    sent_invitation_email: *sent_invitation_email,
                    invitation_url: invitation_url.clone(),
                },
            )))),
            InventoryBOutcome::Error(message) => Err(ExecError::new(message.clone())),
        }
    }
}

/// Prefetch every selected batch-B root. `indexed_search_endpoints` and
/// `outbound_request_log` are the two process-local seams (the baseline reads
/// `search.Indexers().Map.Endpoints()` and the `httpcli` request log); `None`
/// reproduces an empty configuration/log. `org_invitation_config` is the
/// `conf.SiteConfig().OrganizationInvitations` seam (`None` = no signing key).
pub async fn prefetch_inventory_b(
    pool: &PgPool,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
    org_invitation_config: Option<&OrgInvitationConfig>,
    indexed_search_endpoints: Option<&[String]>,
    outbound_request_log: Option<&[OutboundRequestItem]>,
) -> InventoryBRoots {
    let mut mutations = HashMap::new();
    for (field, outcome) in prefetch_mutations(
        pool,
        fields,
        actor,
        viewer_site_admin,
        org_invitation_config,
    )
    .await
    {
        mutations.insert(field, outcome);
    }
    InventoryBRoots {
        package_references: prefetch_package_repo_references(pool, fields).await,
        matching_filter: prefetch_matching_filter(pool, fields).await,
        indexed_search_instances: prefetch_indexed_search_instances(
            fields,
            actor,
            viewer_site_admin,
            indexed_search_endpoints,
        )
        .await,
        outbound_requests: prefetch_outbound_requests(
            fields,
            actor,
            viewer_site_admin,
            outbound_request_log,
        )
        .await,
        invitation_by_token: prefetch_invitation_by_token(
            pool,
            fields,
            actor,
            org_invitation_config,
        )
        .await,
        mutations,
        org_invitation_config: org_invitation_config.cloned(),
    }
}
