#![forbid(unsafe_code)]

//! `internal/search/searchcontexts/search_contexts.go` and `conf.go`: search
//! context spec parsing, resolution, write-access checks, repository-list
//! parsing and the create/update/delete pre-validation chain.
//!
//! Everything that touches the database is injected through
//! [`SearchContextsDatabase`]; HTTP/GraphQL wiring, real migrations and the
//! cache layer are out of scope. `RepositoryRevisions` for [`repo_revs`] is
//! aggregated sequentially instead of with the baseline's bounded errgroup
//! (the result set is identical; only cross-query append order, which the
//! baseline already leaves nondeterministic, differs).
//!
//! `conf.go`'s `IsEnabled` reads `DISABLE_SEARCH_CONTEXTS` at call time, like
//! the baseline, so it can be exercised without a configuration snapshot.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use base64::Engine as _;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_search_query::basic::{plan_to_q, q_string_value};
use sg_search_query::fields::{
    FIELD_ARCHIVED, FIELD_CASE, FIELD_FILE, FIELD_FORK, FIELD_LANG, FIELD_REPO, FIELD_VISIBILITY,
};
use sg_search_query::labels::Labels;
use sg_search_query::node::{go_quote, visit_field, visit_parameter, visit_pattern, Node};
use sg_search_query::parser::SearchType;
use sg_search_query::pipeline::pipeline_query;
use sg_search_query::predicate::parse_as_predicate;
use sg_search_query::validate::YesNoOnly;

/// `conf.go:9`: the environment variable read by `IsEnabled`.
pub const DISABLE_SEARCH_CONTEXTS_VAR: &str = "DISABLE_SEARCH_CONTEXTS";
/// `search_contexts.go:31`.
pub const GLOBAL_SEARCH_CONTEXT_NAME: &str = "global";
/// `search_contexts.go:32`.
pub const SEARCH_CONTEXT_SPEC_PREFIX: &str = "@";
/// `search_contexts.go:33`.
pub const MAX_SEARCH_CONTEXT_NAME_LENGTH: usize = 32;
/// `search_contexts.go:34`.
pub const MAX_SEARCH_CONTEXT_DESCRIPTION_LENGTH: usize = 1024;
/// `search_contexts.go:35`.
pub const MAX_REVISION_LENGTH: usize = 255;

// ---------------------------------------------------------------------------
// Baseline types
// ---------------------------------------------------------------------------

/// `types.SearchContext` (the fields this slice reads/writes).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchContext {
    pub id: i64,
    pub name: String,
    pub description: String,
    pub public: bool,
    pub namespace_user_id: i32,
    pub namespace_org_id: i32,
    pub namespace_user_name: String,
    pub namespace_org_name: String,
    pub query: String,
    pub auto_defined: bool,
}

/// `types.MinimalRepo`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MinimalRepo {
    pub id: i32,
    pub name: String,
}

/// `types.SearchContextRepositoryRevisions`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchContextRepositoryRevisions {
    pub repo: MinimalRepo,
    pub revisions: Vec<String>,
}

/// `search.RepositoryRevisions` as returned by [`get_repository_revisions`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepositoryRevisions {
    pub repo: MinimalRepo,
    pub revs: Vec<String>,
}

/// `ParsedSearchContextSpec`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParsedSearchContextSpec {
    pub namespace_name: String,
    pub search_context_name: String,
}

/// `database.Namespace` (the fields `ResolveSearchContextSpec` reads).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Namespace {
    pub user: i32,
    pub organization: i32,
}

/// `types.User` (the fields the write-access check reads).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct User {
    pub id: i32,
    pub site_admin: bool,
}

/// `types.OrgMembership`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OrgMembership {
    pub org_id: i32,
    pub user_id: i32,
}

/// `database.GetSearchContextOptions`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GetSearchContextOptions {
    pub name: String,
    pub namespace_user_id: i32,
    pub namespace_org_id: i32,
}

/// `database.ReposListOptions` (the fields `ParseRepoOpts` fills).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReposListOptions {
    pub include_patterns: Vec<String>,
    pub exclude_pattern: String,
    pub case_sensitive_patterns: bool,
    pub only_forks: bool,
    pub no_forks: bool,
    pub only_archived: bool,
    pub no_archived: bool,
    pub no_private: bool,
    pub only_private: bool,
    pub ids: Vec<i32>,
}

/// `searchcontexts.RepoOpts`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepoOpts {
    pub repos_list_options: ReposListOptions,
    pub rev_specs: Vec<String>,
}

/// `query.RevisionSpecifier` (`internal/search/query/repo_revs.go:15-42`).
///
/// `rev_at_time_payload` models the baseline's `*RevAtTime`. It is boxed in an
/// [`Arc`] because the baseline uses `map[query.RevisionSpecifier]int` in
/// `getRevsForMatchedRepo`, where the pointer identity of `RevAtTime` is part
/// of the key (`internal/search/repos/repos.go:1150`).
///
/// `rev_at_time` stays as a boolean placeholder for cross-crate constructors
/// (the exhaustive worker) that only carry the encoded payload text; parsed
/// frontend filters set both fields.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RevisionSpecifier {
    pub rev_spec: String,
    pub ref_glob: String,
    pub exclude_ref_glob: String,
    pub rev_at_time: bool,
    pub rev_at_time_payload: Option<std::sync::Arc<RevAtTime>>,
}

impl RevisionSpecifier {
    /// `RevisionSpecifier.HasRefGlob`.
    pub fn has_ref_glob(&self) -> bool {
        !self.ref_glob.is_empty() || !self.exclude_ref_glob.is_empty()
    }

    /// The parsed `RevAtTime`, when the specifier came from a `rat=` filter.
    pub fn rev_at_time_value(&self) -> Option<&RevAtTime> {
        self.rev_at_time_payload.as_deref()
    }

    /// `RevisionSpecifier.String()` (`repo_revs.go:42-53`).
    pub fn spec_string(&self) -> String {
        if !self.exclude_ref_glob.is_empty() {
            return format!("*!{}", self.exclude_ref_glob);
        }
        if !self.ref_glob.is_empty() {
            return format!("*{}", self.ref_glob);
        }
        if let Some(rev_at_time) = self.rev_at_time_value() {
            return rev_at_time.to_string();
        }
        if self.rev_at_time {
            // Cross-crate constructors store the already-encoded payload in
            // `rev_spec`, so `rat=<payload>` round-trips the baseline string.
            return format!("rat={}", self.rev_spec);
        }
        self.rev_spec.clone()
    }
}

/// `query.RevAtTime` (`internal/search/query/repo_revs.go:139-187`): the most
/// recent ancestor of `rev_spec` with a committer timestamp before
/// `timestamp`.
///
/// The baseline serializes the whole struct as JSON and URL-base64 encodes it:
///
/// ```text
/// rat=<base64.URLEncoding({"RevSpec":"…","Timestamp":"…"})>
/// ```
///
/// The timestamp text is kept verbatim so [`Self::to_string`] round-trips the
/// payload exactly; `timestamp` is the parsed value handed to gitserver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevAtTime {
    pub rev_spec: String,
    pub timestamp: OffsetDateTime,
    /// The RFC3339 text from the payload (kept for lossless re-encoding).
    timestamp_text: String,
}

impl RevAtTime {
    /// `ParseRevAtTime`: base64-URL decode, then JSON `{"RevSpec","Timestamp"}`.
    pub fn parse(input: &str) -> Result<RevAtTime, Error> {
        let decoded = base64::engine::general_purpose::URL_SAFE
            .decode(input)
            .map_err(|error| Error::other(error.to_string()))?;
        let payload =
            String::from_utf8(decoded).map_err(|error| Error::other(error.to_string()))?;
        Self::from_payload(&payload)
    }

    fn from_payload(payload: &str) -> Result<RevAtTime, Error> {
        let value: serde_json::Value =
            serde_json::from_str(payload).map_err(|error| Error::other(error.to_string()))?;
        let rev_spec = value
            .get("RevSpec")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| Error::other("missing RevSpec"))?
            .to_owned();
        let timestamp_text = value
            .get("Timestamp")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| Error::other("missing Timestamp"))?
            .to_owned();
        let timestamp = OffsetDateTime::parse(&timestamp_text, &Rfc3339)
            .map_err(|error| Error::other(error.to_string()))?;
        Ok(RevAtTime {
            rev_spec,
            timestamp,
            timestamp_text,
        })
    }

    /// Construct from an already-parsed timestamp (tests and callers that do
    /// not go through the `rat=` encoding).
    pub fn new(rev_spec: impl Into<String>, timestamp: OffsetDateTime) -> RevAtTime {
        let timestamp_text = timestamp
            .format(&Rfc3339)
            .expect("RFC3339 formatting cannot fail");
        RevAtTime {
            rev_spec: rev_spec.into(),
            timestamp,
            timestamp_text,
        }
    }

    /// The JSON body `(*RevAtTime).String` base64-encodes.
    pub fn payload(&self) -> String {
        // Go marshals the struct with `encoding/json`, fields in declaration
        // order. `serde_json::to_string` adds the same quoting (ASCII HTML
        // escaping differs only for `<`, `>`, `&`, which rev specs cannot
        // contain).
        let rev_spec = serde_json::to_string(&self.rev_spec).expect("string serialization");
        let timestamp = serde_json::to_string(&self.timestamp_text).expect("string serialization");
        format!("{{\"RevSpec\":{rev_spec},\"Timestamp\":{timestamp}}}")
    }
}

impl std::fmt::Display for RevAtTime {
    /// `(*RevAtTime).String` (`repo_revs.go:145-151`), including the nil
    /// receiver case handled by [`revision_specifier_rev_at_time_string`].
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("rat=")?;
        formatter.write_str(&base64::engine::general_purpose::URL_SAFE.encode(self.payload()))
    }
}

/// `(*query.RevAtTime)(nil).String()`: `json.Marshal(nil)` is `null`, so the
/// baseline's comparison key for an absent payload is `rat=bnVsbA==`
/// (`repo_revs.go:46`).
pub fn revision_specifier_rev_at_time_string(rev: &RevisionSpecifier) -> String {
    if let Some(rev_at_time) = rev.rev_at_time_value() {
        return rev_at_time.to_string();
    }
    if rev.rev_at_time {
        return format!("rat={}", rev.rev_spec);
    }
    "rat=bnVsbA==".to_owned()
}

/// `query.ParsedRepoFilter` (without the compiled case-insensitive regex).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParsedRepoFilter {
    pub repo: String,
    pub revs: Vec<RevisionSpecifier>,
}

/// `query.RepoVisibility`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RepoVisibility {
    #[default]
    Any,
    Private,
    Public,
}

/// The per-request state the baseline pulls out of `context.Context`
/// (`actor.FromContext`, `dotcom.SourcegraphDotComMode`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RequestContext {
    pub actor_uid: i32,
    pub internal: bool,
    pub sourcegraph_dot_com_mode: bool,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Which sentinel (if any) an [`Error`] corresponds to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// `database.ErrNamespaceNotFound`.
    NamespaceNotFound,
    /// `database.ErrSearchContextNotFound`.
    SearchContextNotFound,
    /// `database.ErrOrgMemberNotFound`.
    OrgMemberNotFound,
    Other,
}

/// An error whose `Display` text matches the baseline byte-for-byte.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub kind: ErrorKind,
    pub message: String,
}

impl Error {
    fn new(kind: ErrorKind, message: impl Into<String>) -> Error {
        Error {
            kind,
            message: message.into(),
        }
    }

    /// `database.ErrNamespaceNotFound`.
    pub fn namespace_not_found() -> Error {
        Error::new(ErrorKind::NamespaceNotFound, "namespace not found")
    }

    /// `database.ErrSearchContextNotFound`.
    pub fn search_context_not_found() -> Error {
        Error::new(ErrorKind::SearchContextNotFound, "search context not found")
    }

    /// `&database.ErrOrgMemberNotFound{}` (the fake stores emit the bare text).
    pub fn org_member_not_found() -> Error {
        Error::new(ErrorKind::OrgMemberNotFound, "org member not found")
    }

    /// An arbitrary baseline `errors.New`/`errors.Errorf` message.
    pub fn other(message: impl Into<String>) -> Error {
        Error::new(ErrorKind::Other, message)
    }

    /// `errors.Wrap(err, prefix)`: keeps the sentinel kind so callers can still
    /// distinguish not-found after wrapping.
    pub fn wrap(self, prefix: &str) -> Error {
        Error::new(self.kind, format!("{prefix}: {}", self.message))
    }

    pub fn is_search_context_not_found(&self) -> bool {
        self.kind == ErrorKind::SearchContextNotFound
    }

    /// `lib/errors.Append`: one error keeps its text, several use the
    /// cockroachdb-style `"N errors occurred:\n\t* …"` rendering.
    pub fn aggregate(errs: Vec<Error>) -> Option<Error> {
        match errs.len() {
            0 => None,
            1 => errs.into_iter().next(),
            _ => {
                let count = errs.len();
                let mut message = format!("{count} errors occurred:");
                for err in errs {
                    message.push_str("\n\t* ");
                    message.push_str(&err.message);
                }
                Some(Error::other(message))
            }
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

// ---------------------------------------------------------------------------
// Injected database
// ---------------------------------------------------------------------------

/// The database surface these functions use. A real implementation backs the
/// `database.SearchContextsStore`/`NamespaceStore`/`OrgMemberStore`/`UserStore`
/// and `RepoStore` methods; tests provide an in-memory fake.
pub trait SearchContextsDatabase {
    fn get_namespace_by_name(&self, name: &str) -> Result<Namespace, Error>;
    fn get_org_member_by_org_id_and_user_id(
        &self,
        org_id: i32,
        user_id: i32,
    ) -> Result<Option<OrgMembership>, Error>;
    /// `auth.CurrentUser`'s lookup: `None` for an anonymous or missing user.
    fn get_current_user(&self, uid: i32) -> Result<Option<User>, Error>;

    fn get_search_context(&self, opts: &GetSearchContextOptions) -> Result<SearchContext, Error>;
    fn create_search_context_with_repository_revisions(
        &self,
        search_context: &SearchContext,
        repository_revisions: &[SearchContextRepositoryRevisions],
    ) -> Result<SearchContext, Error>;
    fn update_search_context_with_repository_revisions(
        &self,
        search_context: &SearchContext,
        repository_revisions: &[SearchContextRepositoryRevisions],
    ) -> Result<SearchContext, Error>;
    fn delete_search_context(&self, search_context_id: i64) -> Result<(), Error>;

    fn get_all_revisions_for_repos(
        &self,
        repo_ids: &[i32],
    ) -> Result<BTreeMap<i32, Vec<String>>, Error>;
    fn get_all_queries(&self) -> Result<Vec<String>, Error>;
    fn get_search_context_repository_revisions(
        &self,
        search_context_id: i64,
    ) -> Result<Vec<SearchContextRepositoryRevisions>, Error>;
    fn list_minimal_repos(&self, opts: &ReposListOptions) -> Result<Vec<MinimalRepo>, Error>;

    fn create_search_context_star_for_user(
        &self,
        user_id: i32,
        search_context_id: i64,
    ) -> Result<(), Error>;
    fn delete_search_context_star_for_user(
        &self,
        user_id: i32,
        search_context_id: i64,
    ) -> Result<(), Error>;
    fn set_user_default_search_context_id(
        &self,
        user_id: i32,
        search_context_id: i64,
    ) -> Result<(), Error>;
}

// ---------------------------------------------------------------------------
// Spec parsing (search_contexts.go:47-57)
// ---------------------------------------------------------------------------

fn namespaced_search_context_spec_regexp() -> &'static regex::Regex {
    static REGEXP: OnceLock<regex::Regex> = OnceLock::new();
    REGEXP.get_or_init(|| {
        // `namespacedSearchContextSpecRegexp` = `@(.*?)/(.*)`.
        regex::Regex::new(r"^@(.*?)/(.*)").expect("namespaced search context regexp")
    })
}

/// `ParseSearchContextSpec`.
pub fn parse_search_context_spec(search_context_spec: &str) -> ParsedSearchContextSpec {
    if let Some(captures) = namespaced_search_context_spec_regexp().captures(search_context_spec) {
        let namespace_name = captures.get(1).map_or("", |m| m.as_str());
        let search_context_name = captures.get(2).map_or("", |m| m.as_str());
        return ParsedSearchContextSpec {
            namespace_name: namespace_name.to_owned(),
            search_context_name: search_context_name.to_owned(),
        };
    }
    if let Some(namespace_name) = search_context_spec.strip_prefix(SEARCH_CONTEXT_SPEC_PREFIX) {
        return ParsedSearchContextSpec {
            namespace_name: namespace_name.to_owned(),
            search_context_name: String::new(),
        };
    }
    ParsedSearchContextSpec {
        namespace_name: String::new(),
        search_context_name: search_context_spec.to_owned(),
    }
}

// ---------------------------------------------------------------------------
// Resolution (search_contexts.go:59-112)
// ---------------------------------------------------------------------------

/// `ResolveSearchContextSpec`.
pub fn resolve_search_context_spec(
    db: &dyn SearchContextsDatabase,
    ctx: &RequestContext,
    search_context_spec: &str,
) -> Result<SearchContext, Error> {
    let parsed = parse_search_context_spec(search_context_spec);
    let has_namespace_name = !parsed.namespace_name.is_empty();
    let has_search_context_name = !parsed.search_context_name.is_empty();

    if is_global_search_context_spec(search_context_spec) {
        return Ok(get_global_search_context());
    }

    if has_namespace_name {
        let namespace = db
            .get_namespace_by_name(&parsed.namespace_name)
            .map_err(|err| err.wrap("get namespace by name"))?;

        // Only members of the organization can use search contexts under the
        // organization namespace on Sourcegraph Cloud.
        if ctx.sourcegraph_dot_com_mode
            && namespace.organization > 0
            && db
                .get_org_member_by_org_id_and_user_id(namespace.organization, ctx.actor_uid)
                .is_err()
        {
            // Every failure path returns `database.ErrNamespaceNotFound` so the
            // lookup does not leak whether a hidden namespace exists.
            return Err(Error::namespace_not_found());
        }

        if has_search_context_name {
            return db.get_search_context(&GetSearchContextOptions {
                name: parsed.search_context_name,
                namespace_user_id: namespace.user,
                namespace_org_id: namespace.organization,
            });
        }

        return Err(Error::other(format!(
            "search context {} not found",
            go_quote(search_context_spec)
        )));
    }

    // Check if instance-level context.
    db.get_search_context(&GetSearchContextOptions {
        name: parsed.search_context_name,
        namespace_user_id: 0,
        namespace_org_id: 0,
    })
}

// ---------------------------------------------------------------------------
// Write access (search_contexts.go:114-150)
// ---------------------------------------------------------------------------

/// `ValidateSearchContextWriteAccessForCurrentUser`.
pub fn validate_search_context_write_access_for_current_user(
    db: &dyn SearchContextsDatabase,
    ctx: &RequestContext,
    namespace_user_id: i32,
    namespace_org_id: i32,
    public: bool,
) -> Result<(), Error> {
    if namespace_user_id != 0 && namespace_org_id != 0 {
        return Err(Error::other(
            "namespaceUserID and namespaceOrgID are mutually exclusive",
        ));
    }

    let Some(user) = db.get_current_user(ctx.actor_uid)? else {
        return Err(Error::other("current user not found"));
    };

    // Site-admins have write access to all public search contexts.
    if user.site_admin && public {
        return Ok(());
    }

    if namespace_user_id == 0 && namespace_org_id == 0 && !user.site_admin {
        // Only site-admins have write access to instance-level search contexts.
        return Err(Error::other("current user must be site-admin"));
    } else if namespace_user_id != 0 && namespace_user_id != user.id {
        // Only the creator of the search context has write access to it.
        return Err(Error::other(
            "search context user does not match current user",
        ));
    } else if namespace_org_id != 0 {
        // Only members of the org have write access to org search contexts.
        let membership = db.get_org_member_by_org_id_and_user_id(namespace_org_id, user.id)?;
        if membership.is_none() {
            return Err(Error::other("current user is not an org member"));
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Validation helpers (search_contexts.go:152-266)
// ---------------------------------------------------------------------------

fn is_valid_search_context_name(name: &str) -> bool {
    // `^[a-zA-Z0-9_\-\/\.]+$`: byte-oriented (Go regexp classes are ASCII).
    !name.is_empty()
        && name.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || byte == b'_'
                || byte == b'-'
                || byte == b'/'
                || byte == b'.'
        })
}

/// `validateSearchContextName`.
pub fn validate_search_context_name(name: &str) -> Result<(), Error> {
    if name.len() > MAX_SEARCH_CONTEXT_NAME_LENGTH {
        return Err(Error::other(format!(
            "search context name {} exceeds maximum allowed length ({MAX_SEARCH_CONTEXT_NAME_LENGTH})",
            go_quote(name)
        )));
    }
    if !is_valid_search_context_name(name) {
        return Err(Error::other(format!(
            "{} is not a valid search context name",
            go_quote(name)
        )));
    }
    Ok(())
}

/// `validateSearchContextDescription`.
pub fn validate_search_context_description(description: &str) -> Result<(), Error> {
    if description.len() > MAX_SEARCH_CONTEXT_DESCRIPTION_LENGTH {
        return Err(Error::other(format!(
            "search context description exceeds maximum allowed length \
             ({MAX_SEARCH_CONTEXT_DESCRIPTION_LENGTH})"
        )));
    }
    Ok(())
}

/// `validateSearchContextRepositoryRevisions`.
pub fn validate_search_context_repository_revisions(
    repository_revisions: &[SearchContextRepositoryRevisions],
) -> Result<(), Error> {
    for repository in repository_revisions {
        for revision in &repository.revisions {
            if revision.len() > MAX_REVISION_LENGTH {
                return Err(Error::other(format!(
                    "revision {} exceeds maximum allowed length ({MAX_REVISION_LENGTH})",
                    go_quote(revision)
                )));
            }
        }
    }
    Ok(())
}

fn is_supported_repo_predicate(name: &str) -> bool {
    matches!(
        name,
        "has" | "has.tag" | "has.key" | "has.meta" | "has.topic" | "has.description"
    )
}

/// `validateSearchContextQuery`.
pub fn validate_search_context_query(context_query: &str) -> Result<(), Error> {
    if context_query.is_empty() {
        return Ok(());
    }

    let plan = pipeline_query(context_query, SearchType::Regex).map_err(Error::other)?;
    let q = plan_to_q(&plan);
    let mut errs: Vec<Error> = Vec::new();

    visit_parameter(&q, &mut |field, value, _negated, annotation| match field {
        FIELD_REPO => {
            if annotation.labels.is_set(Labels::IS_PREDICATE) {
                let pred_name = parse_as_predicate(value)
                    .map(|(name, _)| name)
                    .unwrap_or_default();
                if !is_supported_repo_predicate(&pred_name) {
                    errs.push(Error::other(format!(
                        "unsupported repo field predicate in search context query: {}",
                        go_quote(value)
                    )));
                }
                return;
            }

            match parse_repository_revisions(value) {
                Err(err) => errs.push(Error::other(format!(
                    "repo field regex {} is invalid: {err}",
                    go_quote(value)
                ))),
                Ok(repo_revs) => {
                    for rev in &repo_revs.revs {
                        if rev.has_ref_glob() {
                            errs.push(Error::other(format!(
                                "unsupported rev glob in search context query: {}",
                                go_quote(value)
                            )));
                            return;
                        }
                    }
                }
            }
        }
        FIELD_FORK | FIELD_ARCHIVED | FIELD_VISIBILITY | FIELD_CASE | FIELD_FILE | FIELD_LANG => {}
        _ => errs.push(Error::other(format!(
            "unsupported field in search context query: {}",
            go_quote(field)
        ))),
    });

    visit_pattern(&q, &mut |value, _negated, _annotation| {
        if !value.is_empty() {
            errs.push(Error::other(format!(
                "unsupported pattern in search context query: {}",
                go_quote(value)
            )));
        }
    });

    match Error::aggregate(errs) {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

/// `validateSearchContextDoesNotExist`.
fn validate_search_context_does_not_exist(
    db: &dyn SearchContextsDatabase,
    search_context: &SearchContext,
) -> Result<(), Error> {
    match db.get_search_context(&GetSearchContextOptions {
        name: search_context.name.clone(),
        namespace_user_id: search_context.namespace_user_id,
        namespace_org_id: search_context.namespace_org_id,
    }) {
        Ok(_) => Err(Error::other("search context already exists")),
        Err(err) if err.is_search_context_not_found() => Ok(()),
        Err(err) => Err(err),
    }
}

// ---------------------------------------------------------------------------
// Create / update / delete (search_contexts.go:268-372)
// ---------------------------------------------------------------------------

/// `CreateSearchContextWithRepositoryRevisions`.
pub fn create_search_context_with_repository_revisions(
    db: &dyn SearchContextsDatabase,
    ctx: &RequestContext,
    search_context: &SearchContext,
    repository_revisions: &[SearchContextRepositoryRevisions],
) -> Result<SearchContext, Error> {
    if is_global_search_context(search_context) {
        return Err(Error::other("cannot override global search context"));
    }

    validate_search_context_write_access_for_current_user(
        db,
        ctx,
        search_context.namespace_user_id,
        search_context.namespace_org_id,
        search_context.public,
    )?;
    validate_search_context_name(&search_context.name)?;
    validate_search_context_description(&search_context.description)?;

    if !search_context.query.is_empty() && !repository_revisions.is_empty() {
        return Err(Error::other(
            "search context query and repository revisions are mutually exclusive",
        ));
    }

    validate_search_context_repository_revisions(repository_revisions)?;
    validate_search_context_query(&search_context.query)?;
    validate_search_context_does_not_exist(db, search_context)?;

    db.create_search_context_with_repository_revisions(search_context, repository_revisions)
}

/// `UpdateSearchContextWithRepositoryRevisions`.
pub fn update_search_context_with_repository_revisions(
    db: &dyn SearchContextsDatabase,
    ctx: &RequestContext,
    search_context: &SearchContext,
    repository_revisions: &[SearchContextRepositoryRevisions],
) -> Result<SearchContext, Error> {
    if is_global_search_context(search_context) {
        return Err(Error::other("cannot update global search context"));
    }

    validate_search_context_write_access_for_current_user(
        db,
        ctx,
        search_context.namespace_user_id,
        search_context.namespace_org_id,
        search_context.public,
    )?;
    validate_search_context_name(&search_context.name)?;
    validate_search_context_description(&search_context.description)?;

    if !search_context.query.is_empty() && !repository_revisions.is_empty() {
        return Err(Error::other(
            "search context query and repository revisions are mutually exclusive",
        ));
    }

    validate_search_context_repository_revisions(repository_revisions)?;
    validate_search_context_query(&search_context.query)?;

    db.update_search_context_with_repository_revisions(search_context, repository_revisions)
}

/// `DeleteSearchContext`.
pub fn delete_search_context(
    db: &dyn SearchContextsDatabase,
    ctx: &RequestContext,
    search_context: &SearchContext,
) -> Result<(), Error> {
    if is_auto_defined_search_context(search_context) {
        return Err(Error::other("cannot delete auto-defined search context"));
    }

    validate_search_context_write_access_for_current_user(
        db,
        ctx,
        search_context.namespace_user_id,
        search_context.namespace_org_id,
        search_context.public,
    )?;

    db.delete_search_context(search_context.id)
}

// ---------------------------------------------------------------------------
// Repo revisions (search_contexts.go:374-520)
// ---------------------------------------------------------------------------

/// `RepoRevs`: every revision for the given repo IDs across all search
/// contexts. `search_index_query_contexts` is the injected
/// `conf.ExperimentalFeatures().SearchIndexQueryContexts` flag.
pub fn repo_revs(
    db: &dyn SearchContextsDatabase,
    ctx: &RequestContext,
    repo_ids: &[i32],
    search_index_query_contexts: bool,
) -> Result<BTreeMap<i32, Vec<String>>, Error> {
    if !ctx.internal {
        return Err(Error::other(
            "searchcontexts.RepoRevs can only be accessed by an internal actor",
        ));
    }

    let mut revs = db.get_all_revisions_for_repos(repo_ids)?;

    if !search_index_query_contexts {
        return Ok(revs);
    }

    let context_queries = db.get_all_queries()?;
    let mut opts: Vec<RepoOpts> = Vec::new();
    for q in context_queries {
        opts.extend(parse_repo_opts(&q)?);
    }

    for opt in opts {
        let mut list_options = opt.repos_list_options.clone();
        list_options.ids = repo_ids.to_vec();
        let repos = db.list_minimal_repos(&list_options)?;
        for repo in repos {
            revs.entry(repo.id)
                .or_default()
                .extend(opt.rev_specs.clone());
        }
    }

    Ok(revs)
}

/// `ParseRepoOpts`.
pub fn parse_repo_opts(context_query: &str) -> Result<Vec<RepoOpts>, Error> {
    let plan = pipeline_query(context_query, SearchType::Regex).map_err(Error::other)?;
    let mut qs = Vec::with_capacity(plan.len());

    for basic in &plan {
        let q = basic.to_parse_tree();
        let (repo_filters, minus_repo_filters) = repositories(&q)?;

        let fork = basic.parameters.fork().unwrap_or(YesNoOnly::No);
        let archived = basic.parameters.archived().unwrap_or(YesNoOnly::No);
        let visibility = parse_visibility(&q_string_value(&q, FIELD_VISIBILITY));

        let mut rq = RepoOpts {
            repos_list_options: ReposListOptions {
                case_sensitive_patterns: basic.parameters.is_case_sensitive(),
                exclude_pattern: union_regexps(&minus_repo_filters),
                only_forks: fork == YesNoOnly::Only,
                no_forks: fork == YesNoOnly::No,
                only_archived: archived == YesNoOnly::Only,
                no_archived: archived == YesNoOnly::No,
                no_private: visibility == RepoVisibility::Public,
                only_private: visibility == RepoVisibility::Private,
                ..ReposListOptions::default()
            },
            rev_specs: Vec::new(),
        };

        for repo_filter in repo_filters {
            for rev in &repo_filter.revs {
                if !rev.has_ref_glob() {
                    rq.rev_specs.push(rev.rev_spec.clone());
                }
            }
            rq.repos_list_options
                .include_patterns
                .push(repo_filter.repo);
        }

        qs.push(rq);
    }

    Ok(qs)
}

fn repositories(nodes: &[Node]) -> Result<(Vec<ParsedRepoFilter>, Vec<String>), Error> {
    let mut repos = Vec::new();
    let mut negated_repos = Vec::new();
    let mut error: Option<Error> = None;

    visit_field(nodes, FIELD_REPO, &mut |value, negated, annotation| {
        if annotation.labels.is_set(Labels::IS_PREDICATE) {
            return;
        }
        if negated {
            negated_repos.push(value.to_owned());
            return;
        }
        match parse_repository_revisions(value) {
            Ok(repo_filter) => repos.push(repo_filter),
            Err(err) => {
                error = Some(Error::other(format!(
                    "repo field {} is an invalid regex: {err}",
                    go_quote(value)
                )));
            }
        }
    });

    match error {
        Some(err) => Err(err),
        None => Ok((repos, negated_repos)),
    }
}

/// `query.ParseVisibility`.
pub fn parse_visibility(value: &str) -> RepoVisibility {
    match value.to_lowercase().as_str() {
        "private" => RepoVisibility::Private,
        "public" => RepoVisibility::Public,
        _ => RepoVisibility::Any,
    }
}

/// `query.UnionRegExps`.
pub fn union_regexps(values: &[String]) -> String {
    match values.len() {
        0 => String::new(),
        1 => values[0].clone(),
        _ => format!("(?:{})", values.join(")|(?:")),
    }
}

/// `query.ParseRepositoryRevisions`.
pub fn parse_repository_revisions(repo_and_optional_rev: &str) -> Result<ParsedRepoFilter, Error> {
    let repo;
    let mut revs;

    match repo_and_optional_rev.find('@') {
        None => {
            repo = repo_and_optional_rev.to_owned();
            revs = Vec::new();
        }
        Some(index) => {
            repo = repo_and_optional_rev[..index].to_owned();
            revs = Vec::new();
            for part in repo_and_optional_rev[index + 1..].split(':') {
                if part.is_empty() {
                    continue;
                }
                revs.push(parse_revision_specifier(part)?);
            }
            if revs.is_empty() {
                // Default branch.
                revs.push(RevisionSpecifier::default());
            }
        }
    }

    // Repo filters don't currently support case sensitivity, so the baseline
    // compiles a case-insensitive regex to reject invalid patterns early.
    regex::Regex::new(&format!("(?i){repo}")).map_err(|err| Error::other(err.to_string()))?;

    Ok(ParsedRepoFilter { repo, revs })
}

/// `query.ParseRevisionSpecifier` (`repo_revs.go:120-137`).
pub fn parse_revision_specifier(spec: &str) -> Result<RevisionSpecifier, Error> {
    if let Some(glob) = spec.strip_prefix("*!") {
        Ok(RevisionSpecifier {
            exclude_ref_glob: glob.to_owned(),
            ..RevisionSpecifier::default()
        })
    } else if let Some(glob) = spec.strip_prefix('*') {
        Ok(RevisionSpecifier {
            ref_glob: glob.to_owned(),
            ..RevisionSpecifier::default()
        })
    } else if let Some(encoded) = spec.strip_prefix("rat=") {
        let rev_at_time = RevAtTime::parse(encoded)?;
        Ok(RevisionSpecifier {
            rev_at_time: true,
            rev_at_time_payload: Some(std::sync::Arc::new(rev_at_time)),
            ..RevisionSpecifier::default()
        })
    } else {
        Ok(RevisionSpecifier {
            rev_spec: spec.to_owned(),
            ..RevisionSpecifier::default()
        })
    }
}

/// `GetRepositoryRevisions`.
pub fn get_repository_revisions(
    db: &dyn SearchContextsDatabase,
    search_context_id: i64,
) -> Result<Vec<RepositoryRevisions>, Error> {
    let search_context_repository_revisions =
        db.get_search_context_repository_revisions(search_context_id)?;
    Ok(search_context_repository_revisions
        .into_iter()
        .map(|entry| RepositoryRevisions {
            repo: entry.repo,
            revs: entry.revisions,
        })
        .collect())
}

// ---------------------------------------------------------------------------
// Predicates / construction (search_contexts.go:522-569)
// ---------------------------------------------------------------------------

/// `IsAutoDefinedSearchContext`.
pub fn is_auto_defined_search_context(search_context: &SearchContext) -> bool {
    search_context.auto_defined
}

/// `IsInstanceLevelSearchContext`.
pub fn is_instance_level_search_context(search_context: &SearchContext) -> bool {
    search_context.namespace_user_id == 0 && search_context.namespace_org_id == 0
}

/// `IsGlobalSearchContextSpec`: empty and `global` both resolve to global.
pub fn is_global_search_context_spec(search_context_spec: &str) -> bool {
    search_context_spec.is_empty() || search_context_spec == GLOBAL_SEARCH_CONTEXT_NAME
}

/// `IsGlobalSearchContext`.
pub fn is_global_search_context(search_context: &SearchContext) -> bool {
    search_context.name == GLOBAL_SEARCH_CONTEXT_NAME
}

/// `GetGlobalSearchContext`.
pub fn get_global_search_context() -> SearchContext {
    SearchContext {
        name: GLOBAL_SEARCH_CONTEXT_NAME.to_owned(),
        public: true,
        description: "All repositories on Sourcegraph".to_owned(),
        auto_defined: true,
        ..SearchContext::default()
    }
}

/// `GetSearchContextSpec`.
pub fn get_search_context_spec(search_context: &SearchContext) -> String {
    if is_instance_level_search_context(search_context) {
        search_context.name.clone()
    } else if is_auto_defined_search_context(search_context) {
        format!("{SEARCH_CONTEXT_SPEC_PREFIX}{}", search_context.name)
    } else {
        let namespace_name = if !search_context.namespace_user_name.is_empty() {
            &search_context.namespace_user_name
        } else {
            &search_context.namespace_org_name
        };
        format!(
            "{SEARCH_CONTEXT_SPEC_PREFIX}{namespace_name}/{}",
            search_context.name
        )
    }
}

/// `CreateSearchContextStarForUser`.
pub fn create_search_context_star_for_user(
    db: &dyn SearchContextsDatabase,
    search_context: &SearchContext,
    user_id: i32,
) -> Result<(), Error> {
    db.create_search_context_star_for_user(user_id, search_context.id)
}

/// `DeleteSearchContextStarForUser`.
pub fn delete_search_context_star_for_user(
    db: &dyn SearchContextsDatabase,
    search_context: &SearchContext,
    user_id: i32,
) -> Result<(), Error> {
    db.delete_search_context_star_for_user(user_id, search_context.id)
}

/// `SetDefaultSearchContextForUser`.
pub fn set_default_search_context_for_user(
    db: &dyn SearchContextsDatabase,
    search_context: &SearchContext,
    user_id: i32,
) -> Result<(), Error> {
    db.set_user_default_search_context_id(user_id, search_context.id)
}

// ---------------------------------------------------------------------------
// conf.go
// ---------------------------------------------------------------------------

/// `strconv.ParseBool`'s accepted spellings.
fn parse_go_bool(value: &str) -> Option<bool> {
    match value {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

/// `conf.go:IsEnabled`: `DISABLE_SEARCH_CONTEXTS` parsed with
/// `strconv.ParseBool`; an unset or unparsable value leaves contexts enabled.
pub fn is_enabled() -> bool {
    match std::env::var(DISABLE_SEARCH_CONTEXTS_VAR) {
        Ok(value) => !parse_go_bool(&value).unwrap_or(false),
        Err(_) => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_namespaced_specs() {
        assert_eq!(
            parse_search_context_spec("@user/test"),
            ParsedSearchContextSpec {
                namespace_name: "user".to_owned(),
                search_context_name: "test".to_owned(),
            }
        );
        assert_eq!(
            parse_search_context_spec("@user/test/version"),
            ParsedSearchContextSpec {
                namespace_name: "user".to_owned(),
                search_context_name: "test/version".to_owned(),
            }
        );
        assert_eq!(
            parse_search_context_spec("global"),
            ParsedSearchContextSpec {
                namespace_name: String::new(),
                search_context_name: "global".to_owned(),
            }
        );
        assert_eq!(
            parse_search_context_spec("@user"),
            ParsedSearchContextSpec {
                namespace_name: "user".to_owned(),
                search_context_name: String::new(),
            }
        );
        assert_eq!(
            parse_search_context_spec("@"),
            ParsedSearchContextSpec::default()
        );
    }

    #[test]
    fn builds_specs() {
        assert_eq!(
            get_search_context_spec(&get_global_search_context()),
            "global"
        );
        assert_eq!(
            get_search_context_spec(&SearchContext {
                name: "user".to_owned(),
                namespace_user_id: 1,
                auto_defined: true,
                ..SearchContext::default()
            }),
            "@user"
        );
        assert_eq!(
            get_search_context_spec(&SearchContext {
                name: "context".to_owned(),
                namespace_user_id: 1,
                namespace_user_name: "user".to_owned(),
                ..SearchContext::default()
            }),
            "@user/context"
        );
    }

    #[test]
    fn validates_names() {
        assert!(validate_search_context_name("version_1.2-final/3").is_ok());
        assert_eq!(
            validate_search_context_name("invalid name")
                .expect_err("space")
                .message,
            "\"invalid name\" is not a valid search context name"
        );
        assert!(validate_search_context_name(&"x".repeat(32)).is_ok());
        assert!(validate_search_context_name(&"x".repeat(33)).is_err());
    }

    #[test]
    fn parses_repo_revisions() {
        let filter = parse_repository_revisions("foo/bar@HEAD:TAIL").expect("parse");
        assert_eq!(filter.repo, "foo/bar");
        assert_eq!(
            filter
                .revs
                .iter()
                .map(|rev| rev.rev_spec.as_str())
                .collect::<Vec<_>>(),
            vec!["HEAD", "TAIL"]
        );

        let glob = parse_repository_revisions("foo@*refs/tags/*").expect("parse");
        assert!(glob.revs[0].has_ref_glob());

        let empty = parse_repository_revisions("foo@").expect("parse");
        assert_eq!(empty.revs.len(), 1);
        assert!(!empty.revs[0].has_ref_glob());
    }

    #[test]
    fn parses_repo_opts() {
        let opts = parse_repo_opts("r:foo|bar@HEAD:TAIL archived:yes").expect("parse");
        assert_eq!(opts.len(), 1);
        assert_eq!(opts[0].repos_list_options.include_patterns, vec!["foo|bar"]);
        assert!(opts[0].repos_list_options.no_forks);
        assert_eq!(opts[0].rev_specs, vec!["HEAD", "TAIL"]);
    }

    #[test]
    fn rejects_unsupported_query_fields() {
        assert_eq!(
            validate_search_context_query("type:commit author:camden")
                .expect_err("unsupported")
                .message,
            "2 errors occurred:\n\t* unsupported field in search context query: \"type\"\n\t* \
             unsupported field in search context query: \"author\""
        );
        assert!(validate_search_context_query("repo:has(key:value)").is_ok());
        assert!(validate_search_context_query("repo:foo/bar@*refs/tags/*").is_err());
    }

    #[test]
    fn union_regexps_matches_baseline() {
        assert_eq!(union_regexps(&[]), "");
        assert_eq!(union_regexps(&["a".to_owned()]), "a");
        assert_eq!(
            union_regexps(&["a".to_owned(), "b".to_owned()]),
            "(?:a)|(?:b)"
        );
    }
}
