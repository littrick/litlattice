#![forbid(unsafe_code)]

//! Sub-repository permission checker seam
//! (`internal/authz/sub_repo_perms.go` +
//! `internal/authz/subrepoperms/sub_repo_perms.go`).
//!
//! This module provides the production-shaped checker the search job needs:
//!
//! * [`SubRepoPermissionChecker`](crate::search_jobutil_misc_jobs::SubRepoPermissionChecker)
//!   is implemented by [`SubRepoPermsClient`], which mirrors
//!   `subrepoperms.SubRepoPermsClient`;
//! * [`SubRepoPermissionsGetter`] is the injected store seam
//!   (`database.SubRepoPermsStore` in the baseline);
//! * [`production_checker`] / [`configure_from_site`] construct the checker
//!   from the site configuration. When sub-repository permissions are not
//!   enabled, or the production store is missing (not wired yet), they return
//!   `None` and callers fall back to pass-through, matching
//!   `authz.DefaultSubRepoPermsChecker`'s `noopPermsChecker`;
//! * [`sub_repo_runtime`] / [`set_sub_repo_runtime`] are the port of the
//!   `authz.DefaultSubRepoPermsChecker` global plus the request actor the
//!   filter job reads from `context.Context` in Go.
//!
//! The path-rule matcher reproduces the parts of `gobwas/glob` (compiled with
//! `/` as separator) that sub-repository permission rules use: `*` never
//! crosses `/`, `**` does, `?` is one non-`/` character, `[...]` ranges and
//! `{a,b}` alternatives. Escapes/nested braces are translated best effort; the
//! differential corpus only uses the implemented subset.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, OnceLock, RwLock};

use regex::Regex;

use crate::search_jobutil_misc_jobs::{
    Actor, CheckerError, Perms, RepoContent, SubRepoPermissionChecker,
};

/// `authz.SubRepoPermissionsWithIPs` reduced to the paths the checker reads.
pub type RepoRules = Vec<String>;

/// `subrepoperms.SubRepoPermissionsGetter` (`sub_repo_perms.go:25`).
pub trait SubRepoPermissionsGetter: Send + Sync {
    /// `GetByUserWithIPs`: rules keyed by repository name.
    fn get_by_user_with_ips(
        &self,
        user_id: i32,
        backfill_with_wildcard_ip: bool,
    ) -> Result<BTreeMap<String, RepoRules>, String>;

    /// `RepoIDSupported`: private Perforce repositories only.
    fn repo_id_supported(&self, repo_id: i32) -> Result<bool, String>;

    /// `RepoSupported`: same predicate keyed by repository name.
    fn repo_supported(&self, repo: &str) -> Result<bool, String>;
}

/// `authz.ErrUnauthenticated.Error()`.
pub const UNAUTHENTICATED_MESSAGE: &str = "request is unauthenticated";
/// `FilePermissionsFunc`'s nil-getter error.
pub const NIL_GETTER_MESSAGE: &str = "permissionsGetter is nil";

/// One compiled rule. The baseline keeps the original text too; only the
/// matcher and exclusion flag affect results.
#[derive(Debug, Clone)]
struct PathRule {
    matcher: Regex,
    exclusion: bool,
}

/// `compiledRules.GetPermissionsForPath` (`sub_repo_perms.go:77`): the last
/// matching rule wins; an exclusion yields `None`, an include `Read`.
fn permissions_for_path(rules: &[PathRule], path: &str) -> Perms {
    for rule in rules.iter().rev() {
        if rule.matcher.is_match(path) {
            return if rule.exclusion { Perms::NONE } else { Perms::READ };
        }
    }
    Perms::NONE
}

/// `expandDirs` (`sub_repo_perms.go:367`): include every parent directory of
/// an include rule so the directories can be browsed.
fn expand_dirs(rule: &str) -> Vec<String> {
    let mut dirs = Vec::new();
    let mut rule = if rule.starts_with('/') {
        rule.to_owned()
    } else {
        format!("/{rule}")
    };
    if rule.starts_with("/*") {
        dirs.push("**/".to_owned());
        return dirs;
    }
    loop {
        match rule.rfind('/') {
            Some(last_slash) if last_slash > 0 => {
                rule.truncate(last_slash);
                dirs.push(format!("{rule}/"));
            }
            _ => break,
        }
    }
    dirs
}

/// Body of `glob_to_regex`: translate a gobwas pattern compiled with `/` as
/// separator into an unanchored regex fragment.
fn translate_glob(pattern: &str) -> Result<String, String> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '*' => {
                if i + 1 < chars.len() && chars[i + 1] == '*' {
                    out.push_str(".*");
                    i += 2;
                    while i < chars.len() && chars[i] == '*' {
                        i += 1;
                    }
                } else {
                    out.push_str("[^/]*");
                    i += 1;
                }
            }
            '?' => {
                out.push_str("[^/]");
                i += 1;
            }
            '[' => {
                let start = i;
                i += 1;
                let mut negated = false;
                if i < chars.len() && (chars[i] == '!' || chars[i] == '^') {
                    negated = true;
                    i += 1;
                }
                let mut body = String::new();
                let mut closed = false;
                while i < chars.len() {
                    if chars[i] == ']' && !body.is_empty() {
                        closed = true;
                        i += 1;
                        break;
                    }
                    // Escape regex class metacharacters conservatively.
                    match chars[i] {
                        '\\' | ']' | '^' => {
                            body.push('\\');
                            body.push(chars[i]);
                        }
                        c => body.push(c),
                    }
                    i += 1;
                }
                if !closed {
                    return Err(format!(
                        "could not parse range in pattern {pattern:?} at {start}"
                    ));
                }
                out.push('[');
                if negated {
                    out.push('^');
                }
                out.push_str(&body);
                out.push(']');
            }
            '{' => {
                // Split top-level alternatives and translate each recursively.
                let mut depth = 1;
                let mut j = i + 1;
                let mut parts = Vec::new();
                let mut current = String::new();
                while j < chars.len() {
                    match chars[j] {
                        '{' => {
                            depth += 1;
                            current.push('{');
                        }
                        '}' => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                            current.push('}');
                        }
                        ',' if depth == 1 => {
                            parts.push(std::mem::take(&mut current));
                        }
                        c => current.push(c),
                    }
                    j += 1;
                }
                if depth != 0 {
                    return Err(format!("unexpected end of input in pattern {pattern:?}"));
                }
                parts.push(current);
                out.push_str("(?:");
                for (idx, part) in parts.iter().enumerate() {
                    if idx > 0 {
                        out.push('|');
                    }
                    out.push_str(&translate_glob(part)?);
                }
                out.push(')');
                i = j + 1;
            }
            '\\' => {
                // gobwas drops a trailing escape; otherwise the next char is
                // literal.
                if i + 1 < chars.len() {
                    push_literal(&mut out, chars[i + 1]);
                    i += 2;
                } else {
                    i += 1;
                }
            }
            c => {
                push_literal(&mut out, c);
                i += 1;
            }
        }
    }
    Ok(out)
}

fn push_literal(out: &mut String, c: char) {
    if "\\.+()|^$[]{}".contains(c) {
        out.push('\\');
    }
    out.push(c);
}

/// `glob.Compile(pattern, '/')` restricted to the implemented syntax.
fn glob_to_regex(pattern: &str) -> Result<Regex, String> {
    let body = translate_glob(pattern)?;
    Regex::new(&format!("^{body}$")).map_err(|err| err.to_string())
}

/// `SubRepoPermsClient` (`internal/authz/subrepoperms/sub_repo_perms.go:35`).
pub struct SubRepoPermsClient {
    enabled: bool,
    getter: Arc<dyn SubRepoPermissionsGetter>,
}

impl SubRepoPermsClient {
    /// `NewSubRepoPermsClient` with the site-level `enabled` flag injected
    /// instead of watching `conf`.
    pub fn new(enabled: bool, getter: Arc<dyn SubRepoPermissionsGetter>) -> Self {
        Self { enabled, getter }
    }

    fn get_compiled_rules(
        &self,
        user_id: i32,
    ) -> Result<BTreeMap<String, Vec<PathRule>>, CheckerError> {
        let perms = self
            .getter
            .get_by_user_with_ips(user_id, true)
            .map_err(|err| CheckerError::other(format!("fetching rules: {err}")))?;
        let mut compiled = BTreeMap::new();
        for (repo, paths) in perms {
            let mut rules = Vec::new();
            for raw in paths {
                let (exclusion, rule) = match raw.strip_prefix('-') {
                    Some(rest) => (true, rest.to_owned()),
                    None => (false, raw.clone()),
                };
                let rule = if rule.starts_with('/') {
                    rule
                } else {
                    format!("/{rule}")
                };
                rules.push(PathRule {
                    matcher: glob_to_regex(&rule).map_err(|err| {
                        CheckerError::other(format!("building include matcher: {err}"))
                    })?,
                    exclusion,
                });
                // gobwas does not match a leading `/**/` at the root; add the
                // trimmed rule (`TrimPrefix(rule, "/**")` keeps the slash).
                if let Some(stripped) = rule.strip_prefix("/**/") {
                    let mut trimmed = format!("/{stripped}");
                    while let Some(rest) = trimmed.strip_prefix("/**/") {
                        trimmed = format!("/{rest}");
                    }
                    rules.push(PathRule {
                        matcher: glob_to_regex(&trimmed).map_err(|err| {
                            CheckerError::other(format!("building include matcher: {err}"))
                        })?,
                        exclusion,
                    });
                }
                if exclusion {
                    continue;
                }
                for dir in expand_dirs(&rule) {
                    rules.push(PathRule {
                        matcher: glob_to_regex(&dir).map_err(|err| {
                            CheckerError::other(format!("building include matcher: {err}"))
                        })?,
                        exclusion: false,
                    });
                }
            }
            compiled.insert(repo, rules);
        }
        Ok(compiled)
    }
}

impl SubRepoPermissionChecker for SubRepoPermsClient {
    fn enabled(&self) -> bool {
        self.enabled
    }

    fn enabled_for_repo_id(&self, repo_id: i32) -> Result<bool, CheckerError> {
        self.getter
            .repo_id_supported(repo_id)
            .map_err(CheckerError::other)
    }

    fn permissions(&self, user_id: i32, content: &RepoContent) -> Result<Perms, CheckerError> {
        // `Permissions`: disabled is a no-op returning Read.
        if !self.enabled {
            return Ok(Perms::READ);
        }
        let check = self.file_permissions_func(user_id, &content.repo)?;
        check(&content.path)
    }

    fn file_permissions_func(
        &self,
        user_id: i32,
        repo: &str,
    ) -> Result<Box<crate::search_jobutil_misc_jobs::FilePermissionFunc>, CheckerError> {
        if !self.enabled {
            return Ok(Box::new(|_path| Ok(Perms::READ)));
        }
        if user_id == 0 {
            return Err(CheckerError::other(UNAUTHENTICATED_MESSAGE));
        }
        let rules = self
            .get_compiled_rules(user_id)
            .map_err(|err| CheckerError {
                message: format!("compiling match rules: {}", err.message),
                is_context: err.is_context,
            })?;
        let Some(repo_rules) = rules.get(repo).cloned() else {
            // No entry means repo-level access covers every path.
            return Ok(Box::new(|_path| Ok(Perms::READ)));
        };
        Ok(Box::new(move |path: &str| {
            // An empty path is equivalent to repo permissions.
            if path.is_empty() {
                return Ok(Perms::READ);
            }
            let path = if path.starts_with('/') {
                path.to_owned()
            } else {
                format!("/{path}")
            };
            Ok(permissions_for_path(&repo_rules, &path))
        }))
    }
}

/// Forward a shared checker through `Arc` so the job's boxed checker can keep
/// pointing at the same instance.
impl<T: SubRepoPermissionChecker + ?Sized> SubRepoPermissionChecker for Arc<T> {
    fn enabled(&self) -> bool {
        (**self).enabled()
    }

    fn enabled_for_repo_id(&self, repo_id: i32) -> Result<bool, CheckerError> {
        (**self).enabled_for_repo_id(repo_id)
    }

    fn permissions(&self, user_id: i32, content: &RepoContent) -> Result<Perms, CheckerError> {
        (**self).permissions(user_id, content)
    }

    fn file_permissions_func(
        &self,
        user_id: i32,
        repo: &str,
    ) -> Result<Box<crate::search_jobutil_misc_jobs::FilePermissionFunc>, CheckerError> {
        (**self).file_permissions_func(user_id, repo)
    }
}

/// The checker plus the request-scoped actor and commit `ModifiedFiles` map
/// the tree's `SubRepoPerms` node reads.
///
/// The baseline reads the checker from `authz.DefaultSubRepoPermsChecker` and
/// the actor from `context.Context`; until actor middleware is ported the
/// actor travels with this runtime value.
pub struct SubRepoPermsRuntime {
    pub checker: Arc<SubRepoPermsClient>,
    pub actor: Actor,
    /// `result.CommitMatch.ModifiedFiles` keyed by `(repo id, commit id)`.
    pub modified_files: HashMap<(i32, String), Vec<String>>,
}

impl SubRepoPermsRuntime {
    pub fn new(checker: Arc<SubRepoPermsClient>, actor: Actor) -> Self {
        Self {
            checker,
            actor,
            modified_files: HashMap::new(),
        }
    }

    pub fn with_modified_files(
        mut self,
        modified_files: HashMap<(i32, String), Vec<String>>,
    ) -> Self {
        self.modified_files = modified_files;
        self
    }
}

static RUNTIME: OnceLock<RwLock<Option<Arc<SubRepoPermsRuntime>>>> = OnceLock::new();

fn runtime_cell() -> &'static RwLock<Option<Arc<SubRepoPermsRuntime>>> {
    RUNTIME.get_or_init(|| RwLock::new(None))
}

/// Set the process-wide sub-repository permission runtime
/// (`authz.DefaultSubRepoPermsChecker`, plus the actor). `None` disables the
/// filter and restores pass-through.
pub fn set_sub_repo_runtime(runtime: Option<Arc<SubRepoPermsRuntime>>) {
    *runtime_cell().write().unwrap_or_else(|e| e.into_inner()) = runtime;
}

/// The configured runtime, if any. `None` means sub-repository permissions are
/// off (the baseline default `noopPermsChecker`).
pub fn sub_repo_runtime() -> Option<Arc<SubRepoPermsRuntime>> {
    runtime_cell()
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
}

/// Construct the production checker from the site configuration and an
/// optional store.
///
/// Returns `None` (pass-through) when `experimentalFeatures.subRepoPermissions
/// .enabled` is false **or** the production store/getter is not configured,
/// matching `authz.DefaultSubRepoPermsChecker` remaining the noop checker.
pub fn production_checker(
    site_enabled: bool,
    getter: Option<Arc<dyn SubRepoPermissionsGetter>>,
) -> Option<Arc<SubRepoPermsClient>> {
    match getter {
        Some(getter) if site_enabled => Some(Arc::new(SubRepoPermsClient::new(true, getter))),
        _ => None,
    }
}

/// `authz` startup wiring helper: read the flag from the site configuration,
/// build the checker and install it (or clear it for pass-through).
///
/// Returns whether sub-repository checking is active.
pub fn configure_from_site(
    site: &sg_config::SiteConfiguration,
    getter: Option<Arc<dyn SubRepoPermissionsGetter>>,
    actor: Actor,
) -> bool {
    match production_checker(site.sub_repo_permissions_enabled(), getter) {
        Some(checker) => {
            set_sub_repo_runtime(Some(Arc::new(SubRepoPermsRuntime::new(checker, actor))));
            true
        }
        None => {
            set_sub_repo_runtime(None);
            false
        }
    }
}

/// `conf.Watch` for `experimentalFeatures.subRepoPermissions.enabled`
/// (`subrepoperms/sub_repo_perms.go:119-133`). Go re-reads the site config on
/// every replacement and updates the checker's `enabled` flag in place; the
/// Rust runtime is rebuilt (it has no separate cache to preserve), which is
/// observationally identical to `authz.SubRepoEnabled(checker)` flipping.
///
/// The initial configuration is installed by [`configure_from_site`] before
/// this task starts, so only later generation changes call it again.
pub fn spawn_sub_repo_config_watch(
    tasks: &mut tokio::task::JoinSet<()>,
    live: Arc<sg_config::LiveConfig>,
    getter: Arc<dyn SubRepoPermissionsGetter>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    let mut receiver = live.subscribe();
    tasks.spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                changed = receiver.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    let site = live.current().site.clone();
                    let enabled = configure_from_site(
                        &site,
                        Some(Arc::clone(&getter)),
                        Actor::default(),
                    );
                    tracing::info!(
                        enabled = enabled,
                        "reconfigured sub-repository permission checker from site config"
                    );
                }
            }
        }
    });
}

/// Production `database.SubRepoPermsStore`-backed getter.
///
/// Go's store is synchronous and reads `conf.Get()` on every query
/// (`sub_repo_perms_store.go:295`); the Rust DB helpers are async and the job
/// tree runs synchronously, so this bridges with `block_in_place` (the same
/// approach as [`crate::saved_searches_store`]). `enforce_for_site_admins` is
/// evaluated per call so `authz.enforceForSiteAdmins` changes are observed
/// without rebuilding the checker.
pub struct PgSubRepoPermissionsGetter {
    pool: sqlx::PgPool,
    enforce_for_site_admins: Arc<dyn Fn() -> bool + Send + Sync>,
}

impl PgSubRepoPermissionsGetter {
    pub fn new(
        pool: sqlx::PgPool,
        enforce_for_site_admins: impl Fn() -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            pool,
            enforce_for_site_admins: Arc::new(enforce_for_site_admins),
        }
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("sub-repo permissions blocking runtime")
            .block_on(future),
    }
}

impl SubRepoPermissionsGetter for PgSubRepoPermissionsGetter {
    fn get_by_user_with_ips(
        &self,
        user_id: i32,
        backfill_with_wildcard_ip: bool,
    ) -> Result<BTreeMap<String, RepoRules>, String> {
        let enforce = (self.enforce_for_site_admins)();
        let rows = block_on(sg_db::sub_repo_perms::get_by_user_with_ips(
            &self.pool,
            user_id,
            enforce,
            backfill_with_wildcard_ip,
        ))
        .map_err(|error| error.to_string())?;
        Ok(rows
            .into_iter()
            .map(|(repo, perms)| {
                (
                    repo,
                    perms.paths.into_iter().map(|path| path.path).collect(),
                )
            })
            .collect())
    }

    fn repo_id_supported(&self, repo_id: i32) -> Result<bool, String> {
        block_on(sg_db::sub_repo_perms::repo_id_supported(
            &self.pool,
            repo_id,
        ))
        .map_err(|error| error.to_string())
    }

    fn repo_supported(&self, repo: &str) -> Result<bool, String> {
        block_on(sg_db::sub_repo_perms::repo_supported(&self.pool, repo))
            .map_err(|error| error.to_string())
    }
}

/// A fixed in-memory getter, used by tests and the differential harness.
#[derive(Debug, Clone, Default)]
pub struct FixtureGetter {
    /// Rules keyed by `(user id, repo name)`.
    pub rules: BTreeMap<i32, BTreeMap<String, RepoRules>>,
    pub supported_repo_ids: HashSet<i32>,
    pub supported_repos: HashSet<String>,
}

impl SubRepoPermissionsGetter for FixtureGetter {
    fn get_by_user_with_ips(
        &self,
        user_id: i32,
        _backfill_with_wildcard_ip: bool,
    ) -> Result<BTreeMap<String, RepoRules>, String> {
        Ok(self.rules.get(&user_id).cloned().unwrap_or_default())
    }

    fn repo_id_supported(&self, repo_id: i32) -> Result<bool, String> {
        Ok(self.supported_repo_ids.contains(&repo_id))
    }

    fn repo_supported(&self, repo: &str) -> Result<bool, String> {
        Ok(self.supported_repos.contains(repo))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(enabled: bool, rules: &[(&str, &[&str])]) -> SubRepoPermsClient {
        let mut map = BTreeMap::new();
        for (repo, paths) in rules {
            map.insert(
                (*repo).to_owned(),
                paths.iter().map(|p| (*p).to_owned()).collect(),
            );
        }
        let mut rules = BTreeMap::new();
        rules.insert(7, map);
        let mut getter = FixtureGetter {
            rules,
            ..FixtureGetter::default()
        };
        getter.supported_repo_ids.insert(1);
        getter.supported_repos.insert("repo".to_owned());
        SubRepoPermsClient::new(enabled, Arc::new(getter))
    }

    fn content(repo: &str, path: &str) -> RepoContent {
        RepoContent {
            repo: repo.to_owned(),
            path: path.to_owned(),
        }
    }

    #[test]
    fn disabled_is_passthrough() {
        let checker = client(false, &[("repo", &["src/**"])]);
        assert!(!checker.enabled());
        assert_eq!(
            checker.permissions(7, &content("repo", "other/x")),
            Ok(Perms::READ)
        );
    }

    #[test]
    fn include_exclude_and_last_match_wins() {
        let checker = client(true, &[("repo", &["src/**", "-src/secret/**"])]);
        assert_eq!(
            checker.permissions(7, &content("repo", "src/a.go")),
            Ok(Perms::READ)
        );
        assert_eq!(
            checker.permissions(7, &content("repo", "src/secret/a.go")),
            Ok(Perms::NONE)
        );
        assert_eq!(
            checker.permissions(7, &content("repo", "README.md")),
            Ok(Perms::NONE)
        );
    }

    #[test]
    fn double_star_matches_root_files_and_dirs_are_browsable() {
        let checker = client(true, &[("repo", &["/**/*.java", "deep/file.go"])]);
        assert_eq!(
            checker.permissions(7, &content("repo", "Main.java")),
            Ok(Perms::READ)
        );
        assert_eq!(
            checker.permissions(7, &content("repo", "a/b/C.java")),
            Ok(Perms::READ)
        );
        assert_eq!(
            checker.permissions(7, &content("repo", "deep/")),
            Ok(Perms::READ)
        );
        assert_eq!(
            checker.permissions(7, &content("repo", "")),
            Ok(Perms::READ)
        );
    }

    #[test]
    fn repo_without_rules_is_all_read_and_anonymous_errors() {
        let checker = client(true, &[("other", &["src/**"])]);
        assert_eq!(
            checker.permissions(7, &content("repo", "anything")),
            Ok(Perms::READ)
        );
        let err = checker
            .permissions(0, &content("repo", "anything"))
            .unwrap_err();
        assert_eq!(err.message, UNAUTHENTICATED_MESSAGE);
    }

    #[test]
    fn enabled_for_repo_id_uses_store_support() {
        let checker = client(true, &[]);
        assert_eq!(checker.enabled_for_repo_id(1), Ok(true));
        assert_eq!(checker.enabled_for_repo_id(2), Ok(false));
        assert_eq!(checker.getter.repo_supported("repo"), Ok(true));
        assert_eq!(checker.getter.repo_supported("other"), Ok(false));
    }

    #[test]
    fn production_checker_passthrough_when_config_or_store_missing() {
        assert!(production_checker(false, Some(Arc::new(FixtureGetter::default()))).is_none());
        assert!(production_checker(true, None).is_none());
        assert!(production_checker(true, Some(Arc::new(FixtureGetter::default()))).is_some());
    }

    #[test]
    fn runtime_global_round_trips() {
        set_sub_repo_runtime(None);
        assert!(sub_repo_runtime().is_none());
        let runtime = Arc::new(SubRepoPermsRuntime::new(
            Arc::new(client(true, &[])),
            Actor::default(),
        ));
        set_sub_repo_runtime(Some(Arc::clone(&runtime)));
        assert!(sub_repo_runtime().is_some());
        set_sub_repo_runtime(None);
    }
}
