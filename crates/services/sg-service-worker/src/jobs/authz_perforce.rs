#![forbid(unsafe_code)]

//! `internal/authz/providers/perforce`: the Perforce authz provider.
//!
//! `p4 protects` rules are fetched from gitserver (the worker never talks to
//! the Perforce server directly) and converted to:
//!
//! * depot-prefix inclusions/exclusions (`FetchUserPerms` when no `depots`
//!   are configured), using the same Postgres `SIMILAR TO` translation as the
//!   baseline (`...` -> `%`, `*` -> `[^/]+`);
//! * per-depot sub-repository paths with client IPs (`FetchUserPerms` when
//!   `authorization.subRepoPermissions` is set), using an implementation of
//!   the gobwas/glob semantics the baseline compiles the rules with;
//! * the set of usernames with access to a depot (`FetchRepoPerms`), resolving
//!   users and groups through gitserver and mapping usernames to verified
//!   email addresses.
//!
//! The gitserver RPCs are behind [`PerforceGitserver`] so tests can inject
//! fixed responses; production uses [`GrpcPerforceGitserver`], which selects
//! the shard with [`GitserverAddresses::addr_for_repo`] on the `p4port` key
//! exactly like `gitserver.Client.Perforce*` (`ClientForRepo(conn.P4Port)`).
//!
//! `FetchRepoPerms` with `depots` configured is `authz.ErrUnimplemented`, as in
//! the baseline.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use regex::Regex;
use sg_db::sub_repo_perms::PathWithIp;
use sg_db::user_emails;
use sg_db::user_external_accounts::{ExternalAccount, ExternalAccountSpec};
use sg_db::users::UserRecord;
use sg_encryption::Key;
use sg_git::addresses::GitserverAddresses;
use sg_proto::gitserver as pb;
use sqlx::PgPool;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use url::Url;

use super::perms_syncer::{
    AuthzProvider, ExternalUserPermissions, FetchError, FetchPermsOptions, FetchRepo,
    FetchedExternalAccount, UserPermsError,
};

/// `extsvc.TypePerforce`.
pub const TYPE_PERFORCE: &str = "perforce";
/// `cacheTTL`.
const CACHE_TTL: Duration = Duration::from_secs(60 * 60);

/// `protocol.PerforceConnectionDetails`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PerforceConnectionDetails {
    pub p4port: String,
    pub p4user: String,
    pub p4passwd: String,
}

impl PerforceConnectionDetails {
    fn to_proto(&self) -> pb::PerforceConnectionDetails {
        pb::PerforceConnectionDetails {
            p4port: self.p4port.clone(),
            p4user: self.p4user.clone(),
            p4passwd: self.p4passwd.clone(),
        }
    }
}

/// `perforce.User`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PerforceUser {
    pub username: String,
    pub email: String,
}

/// `perforce.Protect`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PerforceProtect {
    pub level: String,
    pub entity_type: String,
    pub entity_name: String,
    pub match_: String,
    pub is_exclusion: bool,
    pub host: String,
}

/// `perforce.AccountData` (`internal/extsvc/perforce/account.go`).
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct AccountData {
    username: String,
    email: String,
}

/// The subset of `gitserver.Client` the Perforce provider uses.
#[async_trait]
pub trait PerforceGitserver: Send + Sync {
    async fn perforce_users(
        &self,
        conn: &PerforceConnectionDetails,
    ) -> Result<Vec<PerforceUser>, String>;
    async fn perforce_protects_for_user(
        &self,
        conn: &PerforceConnectionDetails,
        username: &str,
    ) -> Result<Vec<PerforceProtect>, String>;
    async fn perforce_protects_for_depot(
        &self,
        conn: &PerforceConnectionDetails,
        depot: &str,
    ) -> Result<Vec<PerforceProtect>, String>;
    async fn perforce_group_members(
        &self,
        conn: &PerforceConnectionDetails,
        group: &str,
    ) -> Result<Vec<String>, String>;
    async fn is_perforce_super_user(&self, conn: &PerforceConnectionDetails)
        -> Result<(), String>;
}

type GrpcClient = pb::gitserver_service_client::GitserverServiceClient<Channel>;

/// Production [`PerforceGitserver`] over the gitserver gRPC API.
pub struct GrpcPerforceGitserver {
    addresses: GitserverAddresses,
    clients: Mutex<HashMap<String, GrpcClient>>,
}

impl GrpcPerforceGitserver {
    pub fn new(addresses: GitserverAddresses) -> Self {
        Self {
            addresses,
            clients: Mutex::new(HashMap::new()),
        }
    }

    /// `clientSource.ClientForRepo(api.RepoName(conn.P4Port))`: `p4port` is
    /// used purely as a consistent-hashing key, like the baseline.
    async fn client(&self, p4port: &str) -> Result<GrpcClient, String> {
        let Some(addr) = self.addresses.addr_for_repo(p4port) else {
            return Err("no gitserver address configured".to_owned());
        };
        if let Some(client) = self.clients.lock().await.get(addr) {
            return Ok(client.clone());
        }
        let client = GrpcClient::connect(format!("http://{addr}"))
            .await
            .map_err(|err| format!("connect to gitserver {addr}: {err}"))?;
        self.clients
            .lock()
            .await
            .insert(addr.to_owned(), client.clone());
        Ok(client)
    }
}

#[async_trait]
impl PerforceGitserver for GrpcPerforceGitserver {
    async fn perforce_users(
        &self,
        conn: &PerforceConnectionDetails,
    ) -> Result<Vec<PerforceUser>, String> {
        let mut client = self.client(&conn.p4port).await?;
        let response = client
            .perforce_users(pb::PerforceUsersRequest {
                connection_details: Some(conn.to_proto()),
            })
            .await
            .map_err(|err| err.to_string())?
            .into_inner();
        Ok(response
            .users
            .into_iter()
            .map(|user| PerforceUser {
                username: user.username,
                email: user.email,
            })
            .collect())
    }

    async fn perforce_protects_for_user(
        &self,
        conn: &PerforceConnectionDetails,
        username: &str,
    ) -> Result<Vec<PerforceProtect>, String> {
        let mut client = self.client(&conn.p4port).await?;
        let response = client
            .perforce_protects_for_user(pb::PerforceProtectsForUserRequest {
                connection_details: Some(conn.to_proto()),
                username: username.to_owned(),
            })
            .await
            .map_err(|err| err.to_string())?
            .into_inner();
        Ok(response.protects.into_iter().map(protect_from_proto).collect())
    }

    async fn perforce_protects_for_depot(
        &self,
        conn: &PerforceConnectionDetails,
        depot: &str,
    ) -> Result<Vec<PerforceProtect>, String> {
        let mut client = self.client(&conn.p4port).await?;
        let response = client
            .perforce_protects_for_depot(pb::PerforceProtectsForDepotRequest {
                connection_details: Some(conn.to_proto()),
                depot: depot.to_owned(),
            })
            .await
            .map_err(|err| err.to_string())?
            .into_inner();
        Ok(response.protects.into_iter().map(protect_from_proto).collect())
    }

    async fn perforce_group_members(
        &self,
        conn: &PerforceConnectionDetails,
        group: &str,
    ) -> Result<Vec<String>, String> {
        let mut client = self.client(&conn.p4port).await?;
        let response = client
            .perforce_group_members(pb::PerforceGroupMembersRequest {
                connection_details: Some(conn.to_proto()),
                group: group.to_owned(),
            })
            .await
            .map_err(|err| err.to_string())?
            .into_inner();
        Ok(response.usernames)
    }

    async fn is_perforce_super_user(
        &self,
        conn: &PerforceConnectionDetails,
    ) -> Result<(), String> {
        let mut client = self.client(&conn.p4port).await?;
        client
            .is_perforce_super_user(pb::IsPerforceSuperUserRequest {
                connection_details: Some(conn.to_proto()),
            })
            .await
            .map(|_| ())
            .map_err(|err| err.to_string())
    }
}

fn protect_from_proto(protect: pb::PerforceProtect) -> PerforceProtect {
    PerforceProtect {
        level: protect.level,
        entity_type: protect.entity_type,
        entity_name: protect.entity_name,
        match_: protect.r#match,
        is_exclusion: protect.is_exclusion,
        host: protect.host,
    }
}

/// `extsvc.NormalizeBaseURL(url).String()` for the code host service ID.
///
/// Perforce `p4port` values such as `ssl:host:1666` parse as opaque URLs; Go
/// serializes those without the trailing slash it adds to `Path`, so the
/// cannot-be-a-base case is returned unchanged.
fn code_host_service_id(host: &str) -> String {
    match Url::parse(host) {
        Ok(mut url) => {
            if url.cannot_be_a_base() {
                return url.to_string();
            }
            if let Some(host) = url.host_str() {
                let host = host.to_ascii_lowercase();
                let _ = url.set_host(Some(&host));
            }
            if !url.path().ends_with('/') {
                let path = format!("{}/", url.path());
                url.set_path(&path);
            }
            url.to_string()
        }
        Err(_) => String::new(),
    }
}

/// `p4ProtectLine`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct P4ProtectLine {
    level: String,
    entity_type: String,
    name: String,
    match_: String,
    host: String,
    is_exclusion: bool,
}

impl P4ProtectLine {
    fn revokes_read_access(&self) -> bool {
        matches!(
            self.level.as_str(),
            "list" | "read" | "=read" | "open" | "write" | "review" | "owner" | "admin" | "super"
        )
    }

    fn grants_read_access(&self) -> bool {
        matches!(
            self.level.as_str(),
            "read"
                | "=read"
                | "open"
                | "=open"
                | "write"
                | "=write"
                | "review"
                | "owner"
                | "admin"
                | "super"
        )
    }

    fn affects_read_access(&self) -> bool {
        (self.is_exclusion && self.revokes_read_access())
            || (!self.is_exclusion && self.grants_read_access())
    }
}

/// `perforceWildcardMatchAll`.
const PERFORCE_WILDCARD_MATCH_ALL: &str = "...";
/// `perforceWildcardMatchDirectory`.
const PERFORCE_WILDCARD_MATCH_DIRECTORY: char = '*';

fn has_perforce_wildcard(match_: &str) -> bool {
    match_.contains(PERFORCE_WILDCARD_MATCH_ALL) || match_.contains(PERFORCE_WILDCARD_MATCH_DIRECTORY)
}

/// `convertToPostgresMatch`: `strings.NewReplacer("...", "%", "*", "[^/]+")`.
fn convert_to_postgres_match(match_: &str) -> String {
    let mut out = String::with_capacity(match_.len());
    let mut rest = match_;
    while !rest.is_empty() {
        if let Some(stripped) = rest.strip_prefix(PERFORCE_WILDCARD_MATCH_ALL) {
            out.push('%');
            rest = stripped;
        } else if let Some(stripped) = rest.strip_prefix('*') {
            out.push_str("[^/]+");
            rest = stripped;
        } else {
            let ch = rest.chars().next().expect("non-empty");
            out.push(ch);
            rest = &rest[ch.len_utf8()..];
        }
    }
    out
}

/// A compiled glob rule. `pattern` is the gobwas *escaped* pattern, `original`
/// is the raw Perforce match (used by `matchesAgainstDepot`).
#[derive(Debug, Clone)]
struct GlobMatch {
    regex: Regex,
    pattern: String,
    original: String,
}

impl GlobMatch {
    fn is_match(&self, value: &str) -> bool {
        self.regex.is_match(value)
    }
}

/// Translate the gobwas glob syntax used by these rules to an anchored regex.
///
/// Rules only ever contain literals escaped by `QuoteMeta` (which escapes
/// `* ? \ [ ] { }`), the wildcards `*` / `**`, and the `{/,}` suffix this
/// module appends, so character classes and ranges are always escaped.
fn glob_to_regex(pattern: &str) -> Result<Regex, String> {
    Regex::new(&format!("^(?:{})$", glob_to_regex_source(pattern)?))
        .map_err(|err| err.to_string())
}

fn glob_to_regex_source(pattern: &str) -> Result<String, String> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '\\' => {
                i += 1;
                let Some(ch) = chars.get(i) else {
                    return Err("unexpected end of pattern".to_owned());
                };
                out.push_str(&regex::escape(&ch.to_string()));
                i += 1;
            }
            '*' => {
                if chars.get(i + 1) == Some(&'*') {
                    out.push_str(".*");
                    i += 2;
                } else {
                    out.push_str("[^/]*");
                    i += 1;
                }
            }
            '{' => {
                i += 1;
                let mut alternatives: Vec<String> = Vec::new();
                let mut current = String::new();
                let mut closed = false;
                while i < chars.len() {
                    match chars[i] {
                        '}' => {
                            alternatives.push(current);
                            closed = true;
                            i += 1;
                            break;
                        }
                        ',' => {
                            alternatives.push(current);
                            current = String::new();
                            i += 1;
                        }
                        '\\' => {
                            current.push('\\');
                            i += 1;
                            if let Some(ch) = chars.get(i) {
                                current.push(*ch);
                            }
                            i += 1;
                        }
                        ch => {
                            current.push(ch);
                            i += 1;
                        }
                    }
                }
                if !closed {
                    return Err("unexpected end of pattern".to_owned());
                }
                let mut translated = Vec::with_capacity(alternatives.len());
                for alternative in alternatives {
                    translated.push(glob_to_regex_source(&alternative)?);
                }
                out.push_str("(?:");
                out.push_str(&translated.join("|"));
                out.push(')');
            }
            ch => {
                out.push_str(&regex::escape(&ch.to_string()));
                i += 1;
            }
        }
    }
    Ok(out)
}

/// `convertToGlobMatch`: escape everything, turn `...` into `**` and `*` into
/// `*`, allow a trailing slash on trailing single wildcards, then compile.
fn convert_to_glob_match(match_: &str) -> Result<GlobMatch, String> {
    let chars: Vec<char> = match_.chars().collect();
    let mut pattern = String::with_capacity(match_.len() + 4);
    let mut i = 0;
    while i < chars.len() {
        let ch = chars[i];
        if ch == '.'
            && chars.get(i + 1) == Some(&'.')
            && chars.get(i + 2) == Some(&'.')
        {
            pattern.push_str("**");
            i += 3;
            continue;
        }
        match ch {
            '*' => {
                pattern.push('*');
                i += 1;
            }
            '\\' | '?' | '[' | ']' | '{' | '}' => {
                pattern.push('\\');
                pattern.push(ch);
                i += 1;
            }
            _ => {
                pattern.push(ch);
                i += 1;
            }
        }
    }
    if pattern.ends_with('*') && !pattern.ends_with("**") && !pattern.ends_with("\\*") {
        pattern.push_str("{/,}");
    }
    let regex = glob_to_regex(&pattern).map_err(|err| format!("invalid pattern: {err}"))?;
    Ok(GlobMatch {
        regex,
        pattern,
        original: match_.to_owned(),
    })
}

/// `glob.Compile(pattern, '/')` for an existing glob pattern (the raw
/// `maybeDepotMatch` fragments in `convertRulesForWildcardDepotMatch`).
fn compile_glob(pattern: &str) -> Result<GlobMatch, String> {
    let regex = glob_to_regex(pattern)?;
    Ok(GlobMatch {
        regex,
        pattern: pattern.to_owned(),
        original: pattern.to_owned(),
    })
}

/// `matchesAgainstDepot`.
fn matches_against_depot(match_: &GlobMatch, depot: &str) -> bool {
    if match_.is_match(depot) {
        return true;
    }
    if !has_perforce_wildcard(&match_.original) {
        return false;
    }
    let parts: Vec<&str> = match_.original.split(PERFORCE_WILDCARD_MATCH_ALL).collect();
    if let Some(first) = parts.first() {
        if let Ok(prefix) = convert_to_glob_match(&format!("{first}...")) {
            if prefix.is_match(depot) {
                return true;
            }
        }
    }
    let parts: Vec<&str> = match_.original.split('/').collect();
    let mut i = parts.len() as isize - 1;
    while i >= 0 && (i > 2 || parts[i as usize].contains(PERFORCE_WILDCARD_MATCH_DIRECTORY)) {
        let prefix = format!("{}/", parts[..i as usize].join("/"));
        if let Ok(prefix) = convert_to_glob_match(&prefix) {
            if prefix.is_match(depot) {
                return true;
            }
        }
        i -= 1;
    }
    false
}

/// `trimDepotNameAndSlashes`.
fn trim_depot_name_and_slashes(s: &str, depot_name: &str) -> String {
    let depot_name = depot_name.strip_suffix('/').unwrap_or(depot_name);
    let s = s.strip_prefix(depot_name).unwrap_or(s);
    let s = s.strip_prefix("//").unwrap_or(s);
    if s.starts_with('/') {
        s.to_owned()
    } else {
        format!("/{s}")
    }
}

/// `convertRulesForWildcardDepotMatch`.
fn convert_rules_for_wildcard_depot_match(
    match_: &GlobMatch,
    depot: &str,
    patterns_to_glob: &mut BTreeMap<String, GlobMatch>,
) -> Vec<String> {
    if !match_.pattern.contains('*') {
        return vec![match_.pattern.clone()];
    }
    let trimmed_rule = match_.pattern.strip_prefix("//").unwrap_or(&match_.pattern);
    let trimmed_depot = depot.strip_prefix("//").unwrap_or(depot);
    let trimmed_depot = trimmed_depot.strip_suffix('/').unwrap_or(trimmed_depot);
    let parts: Vec<&str> = trimmed_rule.split('/').collect();
    let mut new_rules = Vec::with_capacity(parts.len());
    let mut depot_only_matches_double_wildcard = true;
    for i in 0..parts.len() {
        let maybe_depot_match = parts[..=i].join("/");
        let mut maybe_path_rule = parts[i + 1..].join("/");
        // `glob.Compile`; a failure is logged and skipped in the baseline.
        let Ok(depot_match_glob) = compile_glob(&maybe_depot_match) else {
            continue;
        };
        if !depot_match_glob.is_match(trimmed_depot) {
            continue;
        }
        // Special case: depot match ends with `**`.
        if maybe_depot_match.ends_with("**") {
            maybe_path_rule = if maybe_path_rule.is_empty() {
                "**".to_owned()
            } else {
                format!("**/{maybe_path_rule}")
            };
        }
        if maybe_depot_match != "**" {
            depot_only_matches_double_wildcard = false;
            if let Ok(new_glob_match) = convert_to_glob_match(&maybe_path_rule) {
                patterns_to_glob.insert(new_glob_match.pattern.clone(), new_glob_match);
            }
        }
        new_rules.push(maybe_path_rule);
    }
    if depot_only_matches_double_wildcard {
        return vec![match_.pattern.clone()];
    }
    new_rules
}

/// Callbacks for walking `p4 protects` output.
#[async_trait]
trait ProtectsScanner: Send {
    async fn process_line(&mut self, line: &P4ProtectLine) -> Result<(), String>;
    async fn finalize(&mut self) -> Result<(), String> {
        Ok(())
    }
}

/// `scanProtects`.
async fn scan_protects(
    protects: &[PerforceProtect],
    scanner: &mut dyn ProtectsScanner,
    ignore_rules_with_host: bool,
) -> Result<(), String> {
    for protect in protects {
        // Skip rules that rely on particular client IPs/hostnames.
        if ignore_rules_with_host && protect.host != "*" {
            continue;
        }
        let line = P4ProtectLine {
            level: protect.level.clone(),
            entity_type: protect.entity_type.clone(),
            name: protect.entity_name.clone(),
            match_: protect.match_.clone(),
            host: protect.host.clone(),
            is_exclusion: protect.is_exclusion,
        };
        if !line.affects_read_access() {
            continue;
        }
        scanner.process_line(&line).await?;
    }
    scanner.finalize().await
}

/// `repoIncludesExcludesScanner`.
struct RepoIncludesExcludesScanner<'a> {
    perms: &'a mut ExternalUserPermissions,
}

#[async_trait]
impl ProtectsScanner for RepoIncludesExcludesScanner<'_> {
    async fn process_line(&mut self, line: &P4ProtectLine) -> Result<(), String> {
        // Drop trailing `...` so prefix checks can be done.
        let match_ = line.match_.trim_end_matches('.');
        let depot_contains = convert_to_postgres_match(match_);

        if !line.is_exclusion {
            self.perms.include_contains.push(depot_contains);
            return Ok(());
        }

        if has_perforce_wildcard(match_) {
            // Always include wildcard exclusions: we cannot know what they
            // might match.
            self.perms.exclude_contains.push(depot_contains);
            return Ok(());
        }

        // Otherwise only include an exclusion when a corresponding inclusion
        // exists.
        let mut index = 0;
        while index < self.perms.include_contains.len() {
            let prefix = self.perms.include_contains[index].clone();
            if !depot_contains.starts_with(&prefix) {
                index += 1;
                continue;
            }
            // Perforce ACLs can conflict and the later rule wins: an exact
            // match removes the inclusion.
            if depot_contains == prefix {
                self.perms.include_contains.remove(index);
            } else {
                self.perms.exclude_contains.push(depot_contains);
            }
            break;
        }
        Ok(())
    }

    async fn finalize(&mut self) -> Result<(), String> {
        for include in &mut self.perms.include_contains {
            *include = convert_to_postgres_match(&format!("{include}..."));
        }
        for exclude in &mut self.perms.exclude_contains {
            *exclude = convert_to_postgres_match(&format!("{exclude}..."));
        }
        Ok(())
    }
}

/// `fullRepoPermsScanner`.
struct FullRepoPermsScanner<'a> {
    perms: &'a mut ExternalUserPermissions,
    configured_depots: &'a [String],
    configured_depot_matches: Vec<GlobMatch>,
    patterns_to_glob: BTreeMap<String, GlobMatch>,
}

impl<'a> FullRepoPermsScanner<'a> {
    fn new(perms: &'a mut ExternalUserPermissions, configured_depots: &'a [String]) -> Self {
        let mut configured_depot_matches = Vec::with_capacity(configured_depots.len());
        for depot in configured_depots {
            // Treat depots as wildcards.
            let Ok(mut match_) = convert_to_glob_match(&format!("{depot}**")) else {
                continue;
            };
            // Preserve the original name by overriding the wildcard version.
            match_.original = depot.clone();
            configured_depot_matches.push(match_);
        }
        Self {
            perms,
            configured_depots,
            configured_depot_matches,
            patterns_to_glob: BTreeMap::new(),
        }
    }
}

#[async_trait]
impl ProtectsScanner for FullRepoPermsScanner<'_> {
    async fn process_line(&mut self, line: &P4ProtectLine) -> Result<(), String> {
        let match_ = convert_to_glob_match(&line.match_)?;
        self.patterns_to_glob
            .insert(match_.pattern.clone(), match_.clone());

        // Depots this match pertains to.
        let mut depots = Vec::new();
        for (i, depot_match) in self.configured_depot_matches.iter().enumerate() {
            if depot_match.is_match(&match_.original)
                || matches_against_depot(&match_, &depot_match.original)
            {
                if let Some(depot) = self.configured_depots.get(i) {
                    depots.push(depot.clone());
                }
            }
        }
        if depots.is_empty() {
            return Ok(());
        }

        let patterns = &mut self.patterns_to_glob;
        let perms = &mut *self.perms;
        for depot in depots {
            let entry = perms.sub_repo_permissions.entry(depot.clone()).or_default();

            // Special case: matching the entire depot overrides all previous
            // rules.
            if match_.original.strip_prefix(&depot).unwrap_or(&match_.original)
                == PERFORCE_WILDCARD_MATCH_ALL
            {
                entry.paths.clear();
            }

            let mut new_paths = convert_rules_for_wildcard_depot_match(&match_, &depot, patterns);
            if line.is_exclusion {
                for path in &mut new_paths {
                    *path = format!("-{path}");
                }
            }
            for path in new_paths {
                entry.paths.push(PathWithIp {
                    path,
                    ip: line.host.clone(),
                });
            }
        }
        Ok(())
    }

    async fn finalize(&mut self) -> Result<(), String> {
        let perms = &mut *self.perms;
        let sub_repo_permissions = &mut perms.sub_repo_permissions;
        // Iterate over the configured depots for determinism.
        for depot in self.configured_depots {
            let Some(srp) = sub_repo_permissions.get_mut(depot) else {
                continue;
            };

            let only_exclusions = srp.paths.iter().all(|path| path.path.starts_with('-'));
            if only_exclusions {
                // Depots with no inclusions can be dropped.
                sub_repo_permissions.remove(depot);
                continue;
            }

            // Rules must be relative to the depot so repository path
            // transformations (repositoryPathPattern) still work.
            for path in &mut srp.paths {
                path.path = if let Some(rest) = path.path.strip_prefix('-') {
                    format!("-{}", trim_depot_name_and_slashes(rest, depot))
                } else {
                    trim_depot_name_and_slashes(&path.path, depot)
                };
            }
            perms.exacts.push(depot.clone());
        }
        Ok(())
    }
}

/// `allUsersScanner`.
struct AllUsersScanner<'a> {
    provider: &'a PerforceProvider,
    users: &'a mut BTreeSet<String>,
}

#[async_trait]
impl ProtectsScanner for AllUsersScanner<'_> {
    async fn process_line(&mut self, line: &P4ProtectLine) -> Result<(), String> {
        if line.is_exclusion {
            match line.entity_type.as_str() {
                "user" => {
                    if line.name == "*" {
                        self.users.clear();
                    } else {
                        self.users.remove(&line.name);
                    }
                }
                "group" => self
                    .provider
                    .exclude_group_members(&line.name, self.users)
                    .await?,
                _ => tracing::warn!(
                    entity_type = %line.entity_type,
                    "authz.perforce.Provider.FetchRepoPerms.unrecognizedType"
                ),
            }
            return Ok(());
        }

        match line.entity_type.as_str() {
            "user" => {
                if line.name == "*" {
                    let all = self
                        .provider
                        .get_all_users()
                        .await
                        .map_err(|err| format!("list all users: {err}"))?;
                    self.users.extend(all);
                } else {
                    self.users.insert(line.name.clone());
                }
            }
            "group" => {
                self.provider
                    .include_group_members(&line.name, self.users)
                    .await?
            }
            _ => tracing::warn!(
                entity_type = %line.entity_type,
                "authz.perforce.Provider.FetchRepoPerms.unrecognizedType"
            ),
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
struct PerforceCaches {
    all_user_emails: Option<HashMap<String, String>>,
    emails_last_update: Option<Instant>,
    group_members: HashMap<String, Vec<String>>,
    groups_last_update: Option<Instant>,
}

fn cache_is_up_to_date(last_update: Option<Instant>) -> bool {
    last_update.is_some_and(|last_update| last_update.elapsed() < CACHE_TTL)
}

/// `perforce.Provider`.
pub struct PerforceProvider {
    urn: String,
    service_id: String,
    depots: Vec<String>,
    host: String,
    user: String,
    password: String,
    gitserver: Arc<dyn PerforceGitserver>,
    user_external_account_key: Option<Arc<dyn Key>>,
    pool: PgPool,
    ignore_rules_with_host: bool,
    caches: Mutex<PerforceCaches>,
}

impl PerforceProvider {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        urn: impl Into<String>,
        host: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
        depots: Vec<String>,
        ignore_rules_with_host: bool,
        pool: PgPool,
        user_external_account_key: Option<Arc<dyn Key>>,
        gitserver: Arc<dyn PerforceGitserver>,
    ) -> Self {
        let host = host.into();
        let service_id = code_host_service_id(&host);
        Self {
            urn: urn.into(),
            service_id,
            depots,
            host,
            user: user.into(),
            password: password.into(),
            gitserver,
            user_external_account_key,
            pool,
            ignore_rules_with_host,
            caches: Mutex::new(PerforceCaches::default()),
        }
    }

    fn connection(&self) -> PerforceConnectionDetails {
        PerforceConnectionDetails {
            p4port: self.host.clone(),
            p4user: self.user.clone(),
            p4passwd: self.password.clone(),
        }
    }

    /// `getAllUserEmails`: username -> email for every Perforce user with
    /// both fields populated, cached for an hour.
    async fn get_all_user_emails(&self) -> Result<HashMap<String, String>, String> {
        {
            let caches = self.caches.lock().await;
            if let Some(emails) = &caches.all_user_emails {
                if cache_is_up_to_date(caches.emails_last_update) {
                    return Ok(emails.clone());
                }
            }
        }

        let users = self.gitserver.perforce_users(&self.connection()).await?;
        let mut user_emails = HashMap::new();
        for user in users {
            if user.username.is_empty() || user.email.is_empty() {
                continue;
            }
            user_emails.insert(user.username, user.email);
        }

        let mut caches = self.caches.lock().await;
        caches.all_user_emails = Some(user_emails.clone());
        caches.emails_last_update = Some(Instant::now());
        Ok(user_emails)
    }

    /// `getAllUsers`.
    async fn get_all_users(&self) -> Result<Vec<String>, String> {
        let user_emails = self
            .get_all_user_emails()
            .await
            .map_err(|err| format!("get all user emails: {err}"))?;
        Ok(user_emails.into_keys().collect())
    }

    /// `getGroupMembers`.
    async fn get_group_members(&self, group: &str) -> Result<Vec<String>, String> {
        {
            let caches = self.caches.lock().await;
            if let Some(members) = caches.group_members.get(group) {
                if cache_is_up_to_date(caches.groups_last_update) {
                    return Ok(members.clone());
                }
            }
        }

        let members = self
            .gitserver
            .perforce_group_members(&self.connection(), group)
            .await?;
        let mut caches = self.caches.lock().await;
        caches.group_members.insert(group.to_owned(), members.clone());
        caches.groups_last_update = Some(Instant::now());
        Ok(members)
    }

    async fn exclude_group_members(
        &self,
        group: &str,
        users: &mut BTreeSet<String>,
    ) -> Result<(), String> {
        let members = self
            .get_group_members(group)
            .await
            .map_err(|err| format!("list members of group {group:?}: {err}"))?;
        for member in members {
            users.remove(&member);
        }
        Ok(())
    }

    async fn include_group_members(
        &self,
        group: &str,
        users: &mut BTreeSet<String>,
    ) -> Result<(), String> {
        let members = self
            .get_group_members(group)
            .await
            .map_err(|err| format!("list members of group {group:?}: {err}"))?;
        users.extend(members);
        Ok(())
    }

    /// `perforce.GetExternalAccountData`: `nil` when the account has no
    /// encrypted data, otherwise the decrypted `AccountData`.
    async fn external_account_data(
        &self,
        account: &ExternalAccount,
    ) -> Result<Option<AccountData>, String> {
        let Some(data) = account.account_data.as_deref() else {
            return Ok(None);
        };
        // `extsvc.NewEncryptedData` returns nil for an empty cipher *and* key.
        if data.is_empty() && account.encryption_key_id.is_empty() {
            return Ok(None);
        }
        let plain = sg_encryption::maybe_decrypt(
            self.user_external_account_key.as_deref(),
            data,
            &account.encryption_key_id,
        )
        .await
        .map_err(|err| err.to_string())?;
        serde_json::from_str(&plain)
            .map(Some)
            .map_err(|err| err.to_string())
    }
}

fn failed(message: String) -> UserPermsError {
    UserPermsError::Failed {
        message,
        partial: None,
    }
}

fn failed_with_partial(
    message: String,
    partial: ExternalUserPermissions,
) -> UserPermsError {
    UserPermsError::Failed {
        message,
        partial: Some(partial),
    }
}

#[async_trait]
impl AuthzProvider for PerforceProvider {
    fn service_type(&self) -> &str {
        TYPE_PERFORCE
    }

    fn service_id(&self) -> &str {
        &self.service_id
    }

    fn urn(&self) -> &str {
        &self.urn
    }

    /// `Provider.FetchAccount`: match a verified Sourcegraph email against the
    /// Perforce server's users.
    async fn fetch_account(
        &self,
        user: &UserRecord,
    ) -> Result<Option<ExternalAccountSpec>, String> {
        Ok(self
            .fetch_account_with_data(user)
            .await?
            .map(|fetched| fetched.spec))
    }

    async fn fetch_account_with_data(
        &self,
        user: &UserRecord,
    ) -> Result<Option<FetchedExternalAccount>, String> {
        let user_emails = user_emails::list_by_user(&self.pool, user.id, true)
            .await
            .map_err(|err| format!("could not fetch user emails: {err}"))?;
        let email_set: BTreeSet<String> = user_emails
            .into_iter()
            .map(|email| email.email.to_lowercase())
            .collect();

        let users = self
            .gitserver
            .perforce_users(&self.connection())
            .await
            .map_err(|err| format!("list users: {err}"))?;

        for p4_user in users {
            if p4_user.email.is_empty() || p4_user.username.is_empty() {
                continue;
            }
            let p4_email = p4_user.email.to_lowercase();
            if !email_set.contains(&p4_email) {
                continue;
            }
            let account_data = serde_json::to_vec(&AccountData {
                username: p4_user.username,
                email: p4_email.clone(),
            })
            .map_err(|err| err.to_string())?;
            return Ok(Some(FetchedExternalAccount {
                spec: ExternalAccountSpec {
                    service_type: TYPE_PERFORCE.to_owned(),
                    service_id: self.service_id.clone(),
                    client_id: String::new(),
                    account_id: p4_email,
                },
                account_data,
                auth_data: Vec::new(),
            }));
        }
        Ok(None)
    }

    /// `Provider.FetchUserPerms`.
    async fn fetch_user_perms(
        &self,
        account: &ExternalAccount,
        _opts: &FetchPermsOptions,
    ) -> Result<Option<ExternalUserPermissions>, UserPermsError> {
        if account.service_type != self.service_type() || account.service_id != self.service_id() {
            return Err(failed(format!(
                "not a code host of the account: want {:?} but have {:?}",
                account.service_id,
                self.service_id()
            )));
        }

        let user = self
            .external_account_data(account)
            .await
            .map_err(|err| failed(format!("getting external account data: {err}")))?;
        let Some(user) = user else {
            return Err(failed(
                "no user found in the external account data".to_owned(),
            ));
        };

        let protects = self
            .gitserver
            .perforce_protects_for_user(&self.connection(), &user.username)
            .await
            .map_err(|err| failed(format!("list ACLs by user: {err}")))?;

        // Pull permissions from the protects file.
        let mut perms = ExternalUserPermissions::default();
        if self.depots.is_empty() {
            let result = {
                let mut scanner = RepoIncludesExcludesScanner { perms: &mut perms };
                scan_protects(&protects, &mut scanner, self.ignore_rules_with_host).await
            };
            if let Err(err) = result {
                return Err(failed_with_partial(
                    format!("FetchUserPerms: repoIncludesExcludesScanner: {err}"),
                    perms,
                ));
            }
        } else {
            // SubRepoPermissions-enabled code path.
            for depot in &self.depots {
                perms.sub_repo_permissions.insert(depot.clone(), Default::default());
            }
            let result = {
                let mut scanner = FullRepoPermsScanner::new(&mut perms, &self.depots);
                scan_protects(&protects, &mut scanner, self.ignore_rules_with_host).await
            };
            if let Err(err) = result {
                return Err(failed_with_partial(
                    format!("FetchUserPerms: fullRepoPermsScanner: {err}"),
                    perms,
                ));
            }
        }
        Ok(Some(perms))
    }

    /// `Provider.FetchRepoPerms`.
    async fn fetch_repo_perms(
        &self,
        repo: &FetchRepo,
        _opts: &FetchPermsOptions,
    ) -> Result<Vec<String>, FetchError> {
        if repo.service_type != self.service_type() || repo.service_id != self.service_id() {
            return Err(FetchError::Failed {
                message: format!(
                    "not a code host of the repository: want {:?} but have {:?}",
                    repo.service_id,
                    self.service_id()
                ),
                partial_account_ids: Vec::new(),
            });
        }

        // Sub-repo permissions deliberately disable repo-centric syncing.
        if !self.depots.is_empty() {
            return Err(FetchError::Unimplemented);
        }

        let protects = self
            .gitserver
            .perforce_protects_for_depot(&self.connection(), &repo.external_repo_id)
            .await
            .map_err(|err| FetchError::Failed {
                message: format!("list ACLs by depot: {err}"),
                partial_account_ids: Vec::new(),
            })?;

        let mut users = BTreeSet::new();
        {
            let mut scanner = AllUsersScanner {
                provider: self,
                users: &mut users,
            };
            scan_protects(&protects, &mut scanner, self.ignore_rules_with_host)
                .await
                .map_err(|err| FetchError::Failed {
                    message: format!("scanning protects: {err}"),
                    partial_account_ids: Vec::new(),
                })?;
        }

        let user_emails = self
            .get_all_user_emails()
            .await
            .map_err(|err| FetchError::Failed {
                message: format!("get all user emails: {err}"),
                partial_account_ids: Vec::new(),
            })?;
        let mut account_ids = Vec::with_capacity(users.len());
        for user in users {
            if let Some(email) = user_emails.get(&user) {
                account_ids.push(email.clone());
            }
        }
        Ok(account_ids)
    }

    async fn validate_connection(&self) -> Result<(), String> {
        self.gitserver
            .is_perforce_super_user(&self.connection())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_perforce_wildcards_to_postgres() {
        assert_eq!(convert_to_postgres_match("//depot/..."), "//depot/%");
        assert_eq!(convert_to_postgres_match("//depot/*"), "//depot/[^/]+");
        assert_eq!(
            convert_to_postgres_match("//depot/x.../y*"),
            "//depot/x%/y[^/]+"
        );
    }

    #[test]
    fn glob_semantics_match_the_baseline_experiments() {
        let all = convert_to_glob_match("//depot/...").expect("glob");
        assert!(all.is_match("//depot/"));
        assert!(all.is_match("//depot/a"));
        assert!(all.is_match("//depot/a/b"));
        assert!(!all.is_match("//depot"));

        let single = convert_to_glob_match("//depot/*").expect("glob");
        assert!(single.is_match("//depot/"));
        assert!(single.is_match("//depot/a"));
        assert!(!single.is_match("//depot/a/b"));

        // The trailing `{/,}` this module appends to single wildcards allows
        // a trailing slash on the wildcard segment.
        assert!(single.is_match("//depot/x/"));
        assert_eq!(single.pattern, "//depot/*{/,}");

        let double_star = convert_to_glob_match("**").expect("glob");
        assert!(double_star.is_match(""));
        assert!(double_star.is_match("anything/at/all"));

        let prefixed = convert_to_glob_match("**/file").expect("glob");
        assert!(prefixed.is_match("a/file"));
        assert!(!prefixed.is_match("file"));
    }

    #[test]
    fn repo_includes_excludes_scanner_matches_baseline_rules() {
        let protects = vec![
            protect("read", "user", "alice", "//depot/...", false, "*"),
            protect("read", "user", "alice", "//depot/*", true, "*"),
            protect("read", "user", "alice", "//other/...", true, "*"),
            protect("list", "user", "alice", "//ignored/...", false, "*"),
        ];
        let mut perms = ExternalUserPermissions::default();
        let mut scanner = RepoIncludesExcludesScanner { perms: &mut perms };
        futures_lite_block_on(scan_protects(&protects, &mut scanner, true)).expect("scan");

        // The exact wildcard exclusion is emitted before the exact-match
        // include removal rule can apply (`hasPerforceWildcard` applies after
        // trimming trailing dots).
        assert_eq!(perms.include_contains, vec!["//depot/%"]);
        assert_eq!(perms.exclude_contains, vec!["//depot/[^/]+%"]);
    }

    #[test]
    fn full_repo_scanner_produces_relative_sub_repo_paths() {
        let depots = vec!["//depot/".to_owned()];
        let protects = vec![
            protect("read", "user", "alice", "//depot/...", false, "*"),
            protect("read", "user", "alice", "//depot/secret/...", true, "*"),
        ];
        let mut perms = ExternalUserPermissions::default();
        let mut scanner = FullRepoPermsScanner::new(&mut perms, &depots);
        futures_lite_block_on(scan_protects(&protects, &mut scanner, true)).expect("scan");
        let srp = perms
            .sub_repo_permissions
            .get("//depot/")
            .expect("depot permissions");
        // The whole-depot rule clears earlier paths and expands to `**`; the
        // exclusion is appended and both paths are made relative.
        assert_eq!(
            srp.paths,
            vec![
                PathWithIp {
                    path: "/**".to_owned(),
                    ip: "*".to_owned(),
                },
                PathWithIp {
                    path: "-/secret/**".to_owned(),
                    ip: "*".to_owned(),
                },
            ]
        );
        assert_eq!(perms.exacts, depots);
    }

    fn protect(
        level: &str,
        entity_type: &str,
        entity_name: &str,
        match_: &str,
        is_exclusion: bool,
        host: &str,
    ) -> PerforceProtect {
        PerforceProtect {
            level: level.to_owned(),
            entity_type: entity_type.to_owned(),
            entity_name: entity_name.to_owned(),
            match_: match_.to_owned(),
            is_exclusion,
            host: host.to_owned(),
        }
    }

    /// Minimal executor for the scanner tests. The async futures in this
    /// module never yield (the scanners used here contain no gitserver
    /// calls), so a no-op waker is enough and keeps the tests dependency-free.
    fn futures_lite_block_on<F: std::future::Future>(future: F) -> F::Output {
        use std::task::{Context, Poll, Wake, Waker};
        struct Noop;
        impl Wake for Noop {
            fn wake(self: Arc<Self>) {}
        }
        let waker = Waker::from(Arc::new(Noop));
        let mut context = Context::from_waker(&waker);
        let mut future = std::pin::pin!(future);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(value) => return value,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }
}
