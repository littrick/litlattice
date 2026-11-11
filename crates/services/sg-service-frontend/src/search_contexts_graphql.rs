#![forbid(unsafe_code)]

//! Search contexts GraphQL resolver layer
//! (`cmd/frontend/internal/searchcontexts/resolvers/resolvers.go:1-550`,
//! `cmd/frontend/graphqlbackend/search_contexts.go:1-131`).
//!
//! The M9.8hg [`crate::search_contexts`] module already ports spec parsing,
//! write-access checks and the create/update/delete pre-validation chain. This
//! module reuses those functions and the [`SearchContextsDatabase`] trait, and
//! adds the GraphQL-facing surface:
//!
//! - Relay ID / cursor encoding (`relay.MarshalID`/`UnmarshalSpec`);
//! - the root resolver ([`Resolver`]) with spec/ID lookups, CRUD, stars,
//!   defaults, `IsSearchContextAvailable` and the paginated `SearchContexts`
//!   connection;
//! - [`SearchContextResolver`] field resolvers and the connection/repository
//!   resolvers.
//!
//! Database, permission and user/org lookups are injected through
//! [`SearchContextsGraphqlDatabase`]. GraphQL schema registration, execution,
//! real migrations and the read-permission SQL are out of scope (see the
//! overview at `docs/overviews/m9_8hq_search_contexts_graphql_overview.md`).
//!
//! Ordering/filtering for `SearchContexts` mirrors the baseline SQL
//! (`internal/database/search_contexts.go:140-297`): callers receive the
//! constructed [`ListSearchContextsOptions`] and the injected store is expected
//! to apply them. The differential fake stores do so; the resolver still owns
//! the `First+1` over-fetch, trimming and `pageInfo`/`totalCount` computation.

use std::collections::BTreeMap;

use base64::Engine as _;
use serde_json::Value;
use time::{OffsetDateTime, UtcOffset};

use sg_search_query::node::go_quote;

use crate::search_contexts::{
    self, create_search_context_star_for_user, delete_search_context_star_for_user,
    get_global_search_context, get_search_context_spec, is_auto_defined_search_context,
    is_instance_level_search_context, parse_search_context_spec, resolve_search_context_spec,
    set_default_search_context_for_user, validate_search_context_write_access_for_current_user,
    Error, RequestContext, SearchContext as BaseSearchContext, SearchContextRepositoryRevisions,
    SearchContextsDatabase, User,
};

/// `graphqlbackend.SearchContextCursorKind` (`search_contexts.go:15`).
pub const SEARCH_CONTEXT_CURSOR_KIND: &str = "SearchContextCursor";
/// `relay.MarshalID` kind for search context IDs.
pub const SEARCH_CONTEXT_KIND: &str = "SearchContext";

/// Go's zero `time.Time` as Unix seconds (`0001-01-01T00:00:00Z`).
const GO_ZERO_UNIX_SECONDS: i64 = -62_135_596_800;

/// `time.Time{}`.
pub fn go_zero_time() -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(GO_ZERO_UNIX_SECONDS).expect("year 1 is representable")
}

// ---------------------------------------------------------------------------
// Relay ID / cursor encoding (`graph-gophers/graphql-go/relay`)
// ---------------------------------------------------------------------------

/// `graphql.ID` helpers. Go uses `base64.URLEncoding` (with padding) over
/// `kind + ":" + json.Marshal(spec)`.
pub fn marshal_search_context_id(search_context_spec: &str) -> String {
    let spec = serde_json::to_string(search_context_spec).expect("string is JSON encodable");
    base64::engine::general_purpose::URL_SAFE.encode(format!("{SEARCH_CONTEXT_KIND}:{spec}"))
}

/// `graphqlbackend.MarshalUserID`.
pub fn marshal_user_id(id: i32) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("User:{id}"))
}

/// `graphqlbackend.MarshalOrgID`.
pub fn marshal_org_id(id: i32) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("Org:{id}"))
}

/// `graphqlbackend.MarshalRepositoryID`.
pub fn marshal_repository_id(id: i32) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!("Repository:{id}"))
}

/// `marshalSearchContextCursor` (`resolvers.go:46-48`).
pub fn marshal_search_context_cursor(cursor: i32) -> String {
    base64::engine::general_purpose::URL_SAFE
        .encode(format!("{SEARCH_CONTEXT_CURSOR_KIND}:{cursor}"))
}

/// `relay.UnmarshalSpec(id, &string)` (`resolvers.go:41-44`). The kind is not
/// validated.
pub fn unmarshal_search_context_id(id: &str) -> Result<String, String> {
    let spec = relay_spec(id)?;
    unmarshal_go_string(&spec)
}

/// `unmarshalSearchContextCursor` (`resolvers.go:285-296`).
pub fn unmarshal_search_context_cursor(cursor: Option<&str>) -> Result<i32, String> {
    match cursor {
        None => Ok(0),
        Some(cursor) => relay_spec_i32(cursor),
    }
}

/// `relay.UnmarshalSpec(id, &int32)` for a user ID.
pub fn unmarshal_user_id(id: &str) -> Result<i32, String> {
    relay_spec_i32(id)
}

/// `relay.UnmarshalSpec(id, &api.RepoID)` for a repository ID.
pub fn unmarshal_repository_id(id: &str) -> Result<i32, String> {
    relay_spec_i32(id)
}

/// `graphqlbackend.UnmarshalNamespaceID` (`namespaces.go:55-66`).
pub fn unmarshal_namespace_id(id: &str) -> Result<(i32, i32), String> {
    match relay_kind(id).as_deref() {
        Some("User") => Ok((relay_spec_i32(id)?, 0)),
        Some("Org") => Ok((0, relay_spec_i32(id)?)),
        _ => Err(format!("invalid ID {} for namespace", go_quote(id))),
    }
}

/// `relay.UnmarshalKind`: URL-safe base64 decode and take the prefix before the
/// first colon; `None` when decoding fails or the colon is missing.
pub fn relay_kind(id: &str) -> Option<String> {
    let decoded = decode_go_base64(id).ok()?;
    let text = String::from_utf8_lossy(&decoded);
    let (kind, _) = text.split_once(':')?;
    Some(kind.to_owned())
}

/// `relay.UnmarshalSpec`'s common prologue: base64 decode, then take
/// everything after the first `:`.
fn relay_spec(id: &str) -> Result<String, String> {
    let decoded = decode_go_base64(id)?;
    let text = String::from_utf8_lossy(&decoded);
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    Ok(spec.to_owned())
}

/// `relay.UnmarshalSpec(id, &int32)`.
fn relay_spec_i32(id: &str) -> Result<i32, String> {
    let spec = relay_spec(id)?;
    unmarshal_go_int32(&spec)
}

/// `base64.URLEncoding.DecodeString` with Go's
/// `illegal base64 data at input byte N` errors (newlines ignored).
fn decode_go_base64(input: &str) -> Result<Vec<u8>, String> {
    let cleaned: String = input.chars().filter(|c| *c != '\r' && *c != '\n').collect();
    let bytes = cleaned.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    let mut index = 0;
    while index < bytes.len() {
        let remaining = bytes.len() - index;
        if remaining < 4 {
            for (offset, byte) in bytes[index..].iter().enumerate() {
                if !is_url_safe_alphabet(*byte) && *byte != b'=' {
                    return Err(format!(
                        "illegal base64 data at input byte {}",
                        index + offset
                    ));
                }
            }
            return Err(format!("illegal base64 data at input byte {index}"));
        }
        let chunk = &bytes[index..index + 4];
        let final_quantum = remaining == 4;
        let (decoded, padded) = match decode_quantum(chunk, final_quantum) {
            Ok(value) => value,
            Err(offset) => {
                return Err(format!(
                    "illegal base64 data at input byte {}",
                    index + offset
                ))
            }
        };
        out.extend(decoded);
        index += 4;
        if padded {
            return Ok(out);
        }
    }
    Ok(out)
}

fn decode_quantum(chunk: &[u8], final_quantum: bool) -> Result<(Vec<u8>, bool), usize> {
    let value = |byte: u8| -> Option<u8> {
        match byte {
            b'A'..=b'Z' => Some(byte - b'A'),
            b'a'..=b'z' => Some(byte - b'a' + 26),
            b'0'..=b'9' => Some(byte - b'0' + 52),
            b'-' => Some(62),
            b'_' => Some(63),
            _ => None,
        }
    };
    let v0 = value(chunk[0]).ok_or(0usize)?;
    let v1 = value(chunk[1]).ok_or(1usize)?;
    match (chunk[2], chunk[3]) {
        (b'=', b'=') => {
            if !final_quantum {
                return Err(2usize);
            }
            Ok((vec![(v0 << 2) | (v1 >> 4)], true))
        }
        (b'=', _) => Err(2usize),
        (_, b'=') => {
            if !final_quantum {
                return Err(3usize);
            }
            let v2 = value(chunk[2]).ok_or(2usize)?;
            Ok((vec![(v0 << 2) | (v1 >> 4), (v1 << 4) | (v2 >> 2)], true))
        }
        _ => {
            let v2 = value(chunk[2]).ok_or(2usize)?;
            let v3 = value(chunk[3]).ok_or(3usize)?;
            Ok((
                vec![(v0 << 2) | (v1 >> 4), (v1 << 4) | (v2 >> 2), (v2 << 6) | v3],
                false,
            ))
        }
    }
}

fn is_url_safe_alphabet(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'
}

/// `json.Unmarshal(spec, &int32)` with Go's error messages.
fn unmarshal_go_int32(spec: &str) -> Result<i32, String> {
    let trimmed = spec.trim();
    if trimmed.is_empty() {
        return Err("unexpected end of JSON input".to_owned());
    }
    let first = trimmed.as_bytes()[0];
    match first {
        b'"' => Err("json: cannot unmarshal string into Go value of type int32".to_owned()),
        b't' | b'f' => {
            if trimmed.starts_with("true") || trimmed.starts_with("false") {
                Err("json: cannot unmarshal bool into Go value of type int32".to_owned())
            } else {
                Err(invalid_literal(
                    trimmed,
                    if first == b't' { "true" } else { "false" },
                ))
            }
        }
        b'n' => {
            if trimmed == "null" {
                Ok(0)
            } else {
                Err(invalid_literal(trimmed, "null"))
            }
        }
        b'[' => Err("json: cannot unmarshal array into Go value of type int32".to_owned()),
        b'{' => Err("json: cannot unmarshal object into Go value of type int32".to_owned()),
        b'-' | b'0'..=b'9' => {
            let number = trimmed;
            let integer = !number.contains(['.', 'e', 'E']);
            if integer {
                if let Ok(value) = number.parse::<i64>() {
                    if let Ok(value) = i32::try_from(value) {
                        return Ok(value);
                    }
                }
            }
            Err(format!(
                "json: cannot unmarshal number {number} into Go value of type int32"
            ))
        }
        _ => Err(format!(
            "invalid character {} looking for beginning of value",
            go_char(first)
        )),
    }
}

/// `json.Unmarshal(spec, &string)` with the common Go error messages.
fn unmarshal_go_string(spec: &str) -> Result<String, String> {
    let trimmed = spec.trim();
    if trimmed.is_empty() {
        return Err("unexpected end of JSON input".to_owned());
    }
    match serde_json::from_str::<Value>(trimmed) {
        Ok(Value::String(value)) => Ok(value),
        Ok(Value::Null) => Ok(String::new()),
        Ok(Value::Bool(_)) => {
            Err("json: cannot unmarshal bool into Go value of type string".to_owned())
        }
        Ok(Value::Number(_)) => {
            Err("json: cannot unmarshal number into Go value of type string".to_owned())
        }
        Ok(Value::Array(_)) => {
            Err("json: cannot unmarshal array into Go value of type string".to_owned())
        }
        Ok(Value::Object(_)) => {
            Err("json: cannot unmarshal object into Go value of type string".to_owned())
        }
        Err(_) => Err(go_json_syntax_error(trimmed)),
    }
}

fn go_json_syntax_error(input: &str) -> String {
    let bytes = input.as_bytes();
    match bytes.first() {
        None => "unexpected end of JSON input".to_owned(),
        Some(b'"') => "unexpected end of JSON input".to_owned(),
        Some(byte) => format!(
            "invalid character {} looking for beginning of value",
            go_char(*byte)
        ),
    }
}

fn invalid_literal(input: &str, literal: &str) -> String {
    for (index, expected) in literal.bytes().enumerate() {
        match input.as_bytes().get(index) {
            Some(actual) if *actual == expected => {}
            Some(actual) => {
                return format!(
                    "invalid character {} in literal {literal} (expecting {})",
                    go_char(*actual),
                    go_char(expected)
                )
            }
            None => return "unexpected end of JSON input".to_owned(),
        }
    }
    "unexpected end of JSON input".to_owned()
}

fn go_char(byte: u8) -> String {
    match byte {
        b'\'' => "'\\''".to_owned(),
        b'\\' => "'\\\\'".to_owned(),
        b'\n' => "'\\n'".to_owned(),
        b'\r' => "'\\r'".to_owned(),
        b'\t' => "'\\t'".to_owned(),
        _ => format!("'{}'", byte as char),
    }
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// `types.SearchContext` including the GraphQL-only metadata that the M9.8hg
/// type omits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchContextRecord {
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
    pub updated_at: OffsetDateTime,
    pub default: bool,
    pub starred: bool,
}

impl Default for SearchContextRecord {
    fn default() -> Self {
        Self {
            id: 0,
            name: String::new(),
            description: String::new(),
            public: false,
            namespace_user_id: 0,
            namespace_org_id: 0,
            namespace_user_name: String::new(),
            namespace_org_name: String::new(),
            query: String::new(),
            auto_defined: false,
            updated_at: go_zero_time(),
            default: false,
            starred: false,
        }
    }
}

impl SearchContextRecord {
    /// Build the M9.8hg view (metadata dropped).
    pub fn to_base(&self) -> BaseSearchContext {
        BaseSearchContext {
            id: self.id,
            name: self.name.clone(),
            description: self.description.clone(),
            public: self.public,
            namespace_user_id: self.namespace_user_id,
            namespace_org_id: self.namespace_org_id,
            namespace_user_name: self.namespace_user_name.clone(),
            namespace_org_name: self.namespace_org_name.clone(),
            query: self.query.clone(),
            auto_defined: self.auto_defined,
        }
    }

    /// Enrich a record that came from `search_contexts` (which has no
    /// `updated_at`/`default`/`starred`). Auto-defined global contexts are
    /// constructed in memory by the baseline and carry a zero timestamp; all
    /// others are reloaded by ID, mirroring the store's `GetSearchContext`.
    fn from_base(base: BaseSearchContext) -> Self {
        Self {
            id: base.id,
            name: base.name,
            description: base.description,
            public: base.public,
            namespace_user_id: base.namespace_user_id,
            namespace_org_id: base.namespace_org_id,
            namespace_user_name: base.namespace_user_name,
            namespace_org_name: base.namespace_org_name,
            query: base.query,
            auto_defined: base.auto_defined,
            ..Self::default()
        }
    }
}

/// `types.User` fields used by namespaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceUser {
    pub id: i32,
    pub username: String,
}

/// `types.Org` fields used by namespaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceOrg {
    pub id: i32,
    pub name: String,
}

/// `database.SearchContextsOrderByOption` as selected by the resolver.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SearchContextsOrderBy {
    #[default]
    Spec,
    UpdatedAt,
}

/// `database.ListSearchContextsPageOptions`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ListSearchContextsPageOptions {
    pub first: i32,
    pub after: i32,
}

/// `database.ListSearchContextsOptions`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListSearchContextsOptions {
    pub name: String,
    pub namespace_name: String,
    pub namespace_user_ids: Vec<i32>,
    pub namespace_org_ids: Vec<i32>,
    pub no_namespace: bool,
    pub order_by: SearchContextsOrderBy,
    pub order_by_descending: bool,
}

// ---------------------------------------------------------------------------
// Injected database
// ---------------------------------------------------------------------------

/// GraphQL-layer additions on top of
/// [`crate::search_contexts::SearchContextsDatabase`].
pub trait SearchContextsGraphqlDatabase: SearchContextsDatabase {
    /// Upcast without relying on unstable-in-1.85 trait upcasting.
    fn as_search_contexts_database(&self) -> &dyn SearchContextsDatabase;

    /// The full row after resolution (`GetSearchContext` plus joins).
    fn get_search_context_by_id(
        &self,
        ctx: &RequestContext,
        id: i64,
    ) -> Result<SearchContextRecord, Error>;
    fn list_search_contexts(
        &self,
        ctx: &RequestContext,
        page: &ListSearchContextsPageOptions,
        opts: &ListSearchContextsOptions,
    ) -> Result<Vec<SearchContextRecord>, Error>;
    fn count_search_contexts(
        &self,
        ctx: &RequestContext,
        opts: &ListSearchContextsOptions,
    ) -> Result<i32, Error>;
    fn get_default_search_context_for_current_user(
        &self,
        ctx: &RequestContext,
    ) -> Result<SearchContextRecord, Error>;

    fn get_repos_set_by_ids(&self, ids: &[i32]) -> Result<BTreeMap<i32, MinimalRepoRef>, Error>;
    fn get_user_by_id(&self, id: i32) -> Result<NamespaceUser, Error>;
    fn get_org_by_id(&self, id: i32) -> Result<NamespaceOrg, Error>;
    fn get_orgs_by_user_id(&self, user_id: i32) -> Result<Vec<NamespaceOrg>, Error>;
}

/// `types.MinimalRepo` (re-exported to keep the trait signature independent of
/// the M9.8hg module's internal naming).
pub type MinimalRepoRef = crate::search_contexts::MinimalRepo;

// ---------------------------------------------------------------------------
// Argument types (`cmd/frontend/graphqlbackend/search_contexts.go:66-131`)
// ---------------------------------------------------------------------------

/// `SearchContextInputArgs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchContextInputArgs {
    pub name: String,
    pub description: String,
    pub public: bool,
    /// Raw GraphQL namespace ID (`User`/`Org`).
    pub namespace: Option<String>,
    pub query: String,
}

/// `SearchContextEditInputArgs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchContextEditInputArgs {
    pub name: String,
    pub description: String,
    pub public: bool,
    pub query: String,
}

/// `SearchContextRepositoryRevisionsInputArgs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchContextRepositoryRevisionsInputArgs {
    pub repository_id: String,
    pub revisions: Vec<String>,
}

/// `CreateSearchContextArgs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CreateSearchContextArgs {
    pub search_context: SearchContextInputArgs,
    pub repositories: Vec<SearchContextRepositoryRevisionsInputArgs>,
}

/// `UpdateSearchContextArgs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UpdateSearchContextArgs {
    pub id: String,
    pub search_context: SearchContextEditInputArgs,
    pub repositories: Vec<SearchContextRepositoryRevisionsInputArgs>,
}

/// `DeleteSearchContextArgs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeleteSearchContextArgs {
    pub id: String,
}

/// `Create/DeleteSearchContextStarArgs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchContextStarArgs {
    pub search_context_id: String,
    pub user_id: String,
}

/// `SetDefaultSearchContextArgs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SetDefaultSearchContextArgs {
    pub search_context_id: String,
    pub user_id: String,
}

/// `ListSearchContextsArgs`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListSearchContextsArgs {
    pub first: i32,
    pub after: Option<String>,
    pub query: Option<String>,
    /// Raw GraphQL namespace IDs; `None` is the instance-level namespace.
    pub namespaces: Vec<Option<String>>,
    pub order_by: SearchContextsOrderBy,
    pub descending: bool,
}

// ---------------------------------------------------------------------------
// Root resolver
// ---------------------------------------------------------------------------

/// `resolvers.Resolver`.
pub struct Resolver<'a> {
    db: &'a dyn SearchContextsGraphqlDatabase,
}

impl<'a> Resolver<'a> {
    pub fn new(db: &'a dyn SearchContextsGraphqlDatabase) -> Self {
        Self { db }
    }

    /// `NodeResolvers` (`resolvers.go:29-35`).
    pub fn node_resolvers(&self) -> NodeResolvers<'a> {
        NodeResolvers { db: self.db }
    }

    /// `SearchContextsToResolvers` (`resolvers.go:50-56`).
    pub fn search_contexts_to_resolvers(
        &self,
        search_contexts: Vec<SearchContextRecord>,
    ) -> Vec<SearchContextResolver<'a>> {
        search_contexts
            .into_iter()
            .map(|search_context| SearchContextResolver {
                search_context,
                db: self.db,
            })
            .collect()
    }

    /// `SearchContextBySpec` (`resolvers.go:58-64`).
    pub fn search_context_by_spec(
        &self,
        ctx: &RequestContext,
        spec: &str,
    ) -> Result<SearchContextResolver<'a>, Error> {
        let search_context = self.resolve_full(ctx, spec)?;
        Ok(SearchContextResolver {
            search_context,
            db: self.db,
        })
    }

    /// `SearchContextByID` (`resolvers.go:412-424`).
    pub fn search_context_by_id(
        &self,
        ctx: &RequestContext,
        id: &str,
    ) -> Result<SearchContextResolver<'a>, Error> {
        let spec = unmarshal_search_context_id(id).map_err(Error::other)?;
        self.search_context_by_spec(ctx, &spec)
    }

    /// `IsSearchContextAvailable` (`resolvers.go:378-410`).
    pub fn is_search_context_available(
        &self,
        ctx: &RequestContext,
        spec: &str,
    ) -> Result<bool, Error> {
        let search_context =
            resolve_search_context_spec(self.db.as_search_contexts_database(), ctx, spec)?;

        if is_instance_level_search_context(&search_context) {
            return Ok(true);
        }

        if ctx.actor_uid == 0 {
            return Ok(false);
        }

        if search_context.namespace_user_id != 0 {
            return Ok(ctx.actor_uid == search_context.namespace_user_id);
        }

        let orgs = self.db.get_orgs_by_user_id(ctx.actor_uid)?;
        Ok(orgs
            .iter()
            .any(|org| org.id == search_context.namespace_org_id))
    }

    /// `DefaultSearchContext` (`resolvers.go:277-283`).
    pub fn default_search_context(
        &self,
        ctx: &RequestContext,
    ) -> Result<SearchContextResolver<'a>, Error> {
        let search_context = self.db.get_default_search_context_for_current_user(ctx)?;
        Ok(SearchContextResolver {
            search_context,
            db: self.db,
        })
    }

    /// `CreateSearchContext` (`resolvers.go:66-100`).
    pub fn create_search_context(
        &self,
        ctx: &RequestContext,
        args: &CreateSearchContextArgs,
    ) -> Result<SearchContextResolver<'a>, Error> {
        let (namespace_user_id, namespace_org_id) = match args.search_context.namespace.as_deref() {
            Some(namespace) => unmarshal_namespace_id(namespace).map_err(Error::other)?,
            None => (0, 0),
        };

        let repository_revisions = if args.repositories.is_empty() {
            Vec::new()
        } else {
            self.repository_revisions_from_input_args(ctx, &args.repositories)?
        };

        let input = BaseSearchContext {
            name: args.search_context.name.clone(),
            description: args.search_context.description.clone(),
            public: args.search_context.public,
            namespace_user_id,
            namespace_org_id,
            query: args.search_context.query.clone(),
            ..BaseSearchContext::default()
        };
        let search_context = search_contexts::create_search_context_with_repository_revisions(
            self.db.as_search_contexts_database(),
            ctx,
            &input,
            &repository_revisions,
        )?;
        let record = self.enrich(ctx, search_context)?;
        Ok(SearchContextResolver {
            search_context: record,
            db: self.db,
        })
    }

    /// `UpdateSearchContext` (`resolvers.go:102-137`).
    pub fn update_search_context(
        &self,
        ctx: &RequestContext,
        args: &UpdateSearchContextArgs,
    ) -> Result<SearchContextResolver<'a>, Error> {
        let search_context_spec = unmarshal_search_context_id(&args.id).map_err(Error::other)?;

        let repository_revisions = if args.repositories.is_empty() {
            Vec::new()
        } else {
            self.repository_revisions_from_input_args(ctx, &args.repositories)?
        };

        let original = self.resolve_full(ctx, &search_context_spec)?;
        let mut updated = original;
        updated.name = args.search_context.name.clone();
        updated.description = args.search_context.description.clone();
        updated.public = args.search_context.public;
        updated.query = args.search_context.query.clone();

        let search_context = search_contexts::update_search_context_with_repository_revisions(
            self.db.as_search_contexts_database(),
            ctx,
            &updated.to_base(),
            &repository_revisions,
        )?;
        let record = self.enrich(ctx, search_context)?;
        Ok(SearchContextResolver {
            search_context: record,
            db: self.db,
        })
    }

    /// `DeleteSearchContext` (`resolvers.go:171-188`).
    pub fn delete_search_context(
        &self,
        ctx: &RequestContext,
        args: &DeleteSearchContextArgs,
    ) -> Result<(), Error> {
        let search_context_spec = unmarshal_search_context_id(&args.id).map_err(Error::other)?;
        let search_context = self.resolve_full(ctx, &search_context_spec)?;
        search_contexts::delete_search_context(
            self.db.as_search_contexts_database(),
            ctx,
            &search_context.to_base(),
        )
    }

    /// `CreateSearchContextStar` (`resolvers.go:190-217`).
    pub fn create_search_context_star(
        &self,
        ctx: &RequestContext,
        args: &SearchContextStarArgs,
    ) -> Result<(), Error> {
        let user_id = unmarshal_user_id(&args.user_id).map_err(Error::other)?;
        self.check_site_admin_or_same_user(ctx, user_id)?;

        let search_context_spec =
            unmarshal_search_context_id(&args.search_context_id).map_err(Error::other)?;
        let search_context = self.resolve_full(ctx, &search_context_spec)?;
        create_search_context_star_for_user(
            self.db.as_search_contexts_database(),
            &search_context.to_base(),
            user_id,
        )
    }

    /// `DeleteSearchContextStar` (`resolvers.go:219-246`).
    pub fn delete_search_context_star(
        &self,
        ctx: &RequestContext,
        args: &SearchContextStarArgs,
    ) -> Result<(), Error> {
        let user_id = unmarshal_user_id(&args.user_id).map_err(Error::other)?;
        self.check_site_admin_or_same_user(ctx, user_id)?;

        let search_context_spec =
            unmarshal_search_context_id(&args.search_context_id).map_err(Error::other)?;
        let search_context = self.resolve_full(ctx, &search_context_spec)?;
        delete_search_context_star_for_user(
            self.db.as_search_contexts_database(),
            &search_context.to_base(),
            user_id,
        )
    }

    /// `SetDefaultSearchContext` (`resolvers.go:248-275`).
    pub fn set_default_search_context(
        &self,
        ctx: &RequestContext,
        args: &SetDefaultSearchContextArgs,
    ) -> Result<(), Error> {
        let user_id = unmarshal_user_id(&args.user_id).map_err(Error::other)?;
        self.check_site_admin_or_same_user(ctx, user_id)?;

        let search_context_spec =
            unmarshal_search_context_id(&args.search_context_id).map_err(Error::other)?;
        let search_context = self.resolve_full(ctx, &search_context_spec)?;
        set_default_search_context_for_user(
            self.db.as_search_contexts_database(),
            &search_context.to_base(),
            user_id,
        )
    }

    /// `SearchContexts` (`resolvers.go:298-376`).
    pub fn search_contexts(
        &self,
        ctx: &RequestContext,
        args: &ListSearchContextsArgs,
    ) -> Result<SearchContextConnection<'a>, Error> {
        let order_by = args.order_by;

        // Request one extra to determine if there are more pages.
        let first = args.first.wrapping_add(1);

        let (search_context_name, namespace_name) = match &args.query {
            Some(query) => {
                let parsed = parse_search_context_spec(query);
                (parsed.search_context_name, parsed.namespace_name)
            }
            None => (String::new(), String::new()),
        };

        let after_cursor =
            unmarshal_search_context_cursor(args.after.as_deref()).map_err(Error::other)?;

        let mut namespace_user_ids = Vec::new();
        let mut namespace_org_ids = Vec::new();
        let mut no_namespace = false;
        for namespace in &args.namespaces {
            match namespace {
                None => no_namespace = true,
                Some(raw) => {
                    let (namespace_user_id, namespace_org_id) =
                        unmarshal_namespace_id(raw).map_err(Error::other)?;
                    if namespace_user_id != 0 {
                        namespace_user_ids.push(namespace_user_id);
                    }
                    if namespace_org_id != 0 {
                        namespace_org_ids.push(namespace_org_id);
                    }
                }
            }
        }

        let opts = ListSearchContextsOptions {
            namespace_name,
            name: search_context_name,
            namespace_user_ids,
            namespace_org_ids,
            no_namespace,
            order_by,
            order_by_descending: args.descending,
        };
        let page = ListSearchContextsPageOptions {
            first,
            after: after_cursor,
        };

        let mut search_contexts = self.db.list_search_contexts(ctx, &page, &opts)?;
        let count = self.db.count_search_contexts(ctx, &opts)?;

        let mut has_next_page = false;
        if search_contexts.len() as i64 == i64::from(args.first) + 1 {
            has_next_page = true;
            search_contexts.pop();
        }

        Ok(SearchContextConnection {
            after_cursor,
            search_contexts: self.search_contexts_to_resolvers(search_contexts),
            total_count: count,
            has_next_page,
        })
    }

    /// `repositoryRevisionsFromInputArgs` (`resolvers.go:139-169`).
    fn repository_revisions_from_input_args(
        &self,
        _ctx: &RequestContext,
        args: &[SearchContextRepositoryRevisionsInputArgs],
    ) -> Result<Vec<SearchContextRepositoryRevisions>, Error> {
        let mut repo_ids = Vec::with_capacity(args.len());
        for repository in args {
            let repo_id =
                unmarshal_repository_id(&repository.repository_id).map_err(Error::other)?;
            repo_ids.push(repo_id);
        }
        let id_to_repo = self.db.get_repos_set_by_ids(&repo_ids)?;

        let mut repository_revisions = Vec::with_capacity(args.len());
        for repository in args {
            let repo_id =
                unmarshal_repository_id(&repository.repository_id).map_err(Error::other)?;
            let Some(repo) = id_to_repo.get(&repo_id) else {
                return Err(Error::other(format!(
                    "cannot find repo with id: {}",
                    go_quote(&repository.repository_id)
                )));
            };
            repository_revisions.push(SearchContextRepositoryRevisions {
                repo: repo.clone(),
                revisions: repository.revisions.clone(),
            });
        }
        Ok(repository_revisions)
    }

    /// `ResolveSearchContextSpec`, then load the full record
    /// (`updated_at`/`default`/`starred`/namespace names).
    fn resolve_full(&self, ctx: &RequestContext, spec: &str) -> Result<SearchContextRecord, Error> {
        let base = resolve_search_context_spec(self.db.as_search_contexts_database(), ctx, spec)?;
        self.enrich(ctx, base)
    }

    fn enrich(
        &self,
        ctx: &RequestContext,
        base: BaseSearchContext,
    ) -> Result<SearchContextRecord, Error> {
        if base.auto_defined {
            // The baseline constructs the global context in memory.
            Ok(SearchContextRecord::from_base(base))
        } else {
            self.db.get_search_context_by_id(ctx, base.id)
        }
    }

    /// `auth.CheckSiteAdminOrSameUser` (`internal/auth/site_admin.go:127-136`),
    /// whose failures are all collapsed to one message.
    fn check_site_admin_or_same_user(
        &self,
        ctx: &RequestContext,
        subject_user_id: i32,
    ) -> Result<(), Error> {
        if ctx.internal || (ctx.actor_uid != 0 && ctx.actor_uid == subject_user_id) {
            return Ok(());
        }
        if ctx.actor_uid != 0 {
            if let Ok(Some(User {
                site_admin: true, ..
            })) = self.db.get_current_user(ctx.actor_uid)
            {
                return Ok(());
            }
        }
        Err(Error::other(
            "must be authenticated as the authorized user or site admin",
        ))
    }
}

/// `Resolver.NodeResolvers()` map, reduced to the dispatch operation.
pub struct NodeResolvers<'a> {
    db: &'a dyn SearchContextsGraphqlDatabase,
}

impl<'a> NodeResolvers<'a> {
    /// Look up a node by GraphQL kind. Returns `Ok(None)` for kinds this
    /// resolver does not own (the baseline map simply has no entry).
    pub fn node_by_id(
        &self,
        ctx: &RequestContext,
        kind: &str,
        id: &str,
    ) -> Result<Option<SearchContextResolver<'a>>, Error> {
        if kind != SEARCH_CONTEXT_KIND {
            return Ok(None);
        }
        let resolver = Resolver { db: self.db };
        resolver.search_context_by_id(ctx, id).map(Some)
    }
}

// ---------------------------------------------------------------------------
// SearchContext resolver
// ---------------------------------------------------------------------------

/// `searchContextResolver` (`resolvers.go:426-514`).
pub struct SearchContextResolver<'a> {
    search_context: SearchContextRecord,
    db: &'a dyn SearchContextsGraphqlDatabase,
}

impl SearchContextResolver<'_> {
    /// `ID()` (`resolvers.go:431-433`).
    pub fn id(&self) -> String {
        marshal_search_context_id(&self.spec())
    }

    pub fn name(&self) -> &str {
        &self.search_context.name
    }

    pub fn description(&self) -> &str {
        &self.search_context.description
    }

    pub fn public(&self) -> bool {
        self.search_context.public
    }

    /// `AutoDefined()` (`resolvers.go:447-449`).
    pub fn auto_defined(&self) -> bool {
        is_auto_defined_search_context(&self.search_context.to_base())
    }

    /// `Spec()` (`resolvers.go:451-453`).
    pub fn spec(&self) -> String {
        get_search_context_spec(&self.search_context.to_base())
    }

    /// `UpdatedAt()` (`resolvers.go:455-457`).
    pub fn updated_at(&self) -> OffsetDateTime {
        self.search_context.updated_at
    }

    /// `Namespace(ctx)` (`resolvers.go:459-475`).
    pub fn namespace(&self, _ctx: &RequestContext) -> Result<Option<NamespaceResolver>, Error> {
        if self.search_context.namespace_user_id != 0 {
            let user = self
                .db
                .get_user_by_id(self.search_context.namespace_user_id)?;
            return Ok(Some(NamespaceResolver::User(user)));
        }
        if self.search_context.namespace_org_id != 0 {
            let org = self
                .db
                .get_org_by_id(self.search_context.namespace_org_id)?;
            return Ok(Some(NamespaceResolver::Org(org)));
        }
        Ok(None)
    }

    /// `ViewerCanManage(ctx)` (`resolvers.go:477-480`).
    pub fn viewer_can_manage(&self, ctx: &RequestContext) -> bool {
        let has_write_access = validate_search_context_write_access_for_current_user(
            self.db.as_search_contexts_database(),
            ctx,
            self.search_context.namespace_user_id,
            self.search_context.namespace_org_id,
            self.search_context.public,
        )
        .is_ok();
        !is_auto_defined_search_context(&self.search_context.to_base()) && has_write_access
    }

    /// `ViewerHasAsDefault(ctx)` (`resolvers.go:482-484`).
    pub fn viewer_has_as_default(&self) -> bool {
        self.search_context.default
    }

    /// `ViewerHasStarred(ctx)` (`resolvers.go:486-488`).
    pub fn viewer_has_starred(&self) -> bool {
        self.search_context.starred
    }

    /// `Repositories(ctx)` (`resolvers.go:490-510`).
    pub fn repositories(&self) -> Result<Vec<SearchContextRepositoryRevisionsResolver>, Error> {
        if self.auto_defined() {
            return Ok(Vec::new());
        }

        let repository_revisions = self
            .db
            .get_search_context_repository_revisions(self.search_context.id)?;
        Ok(repository_revisions
            .into_iter()
            .map(|entry| SearchContextRepositoryRevisionsResolver {
                repository: RepositoryResolver {
                    id: entry.repo.id,
                    name: entry.repo.name,
                },
                revisions: entry.revisions,
            })
            .collect())
    }

    /// `Query()` (`resolvers.go:512-514`).
    pub fn query(&self) -> &str {
        &self.search_context.query
    }
}

/// `graphqlbackend.NamespaceResolver` for a user or organization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NamespaceResolver {
    User(NamespaceUser),
    Org(NamespaceOrg),
}

impl NamespaceResolver {
    /// `Namespace.ID()`.
    pub fn id(&self) -> String {
        match self {
            NamespaceResolver::User(user) => marshal_user_id(user.id),
            NamespaceResolver::Org(org) => marshal_org_id(org.id),
        }
    }

    /// `UserResolver.URL()` / `OrgResolver.URL()`.
    pub fn url(&self) -> String {
        match self {
            NamespaceResolver::User(user) => format!("/users/{}", user.username),
            NamespaceResolver::Org(org) => format!("/organizations/{}", org.name),
        }
    }

    /// `NamespaceName()`.
    pub fn namespace_name(&self) -> &str {
        match self {
            NamespaceResolver::User(user) => &user.username,
            NamespaceResolver::Org(org) => &org.name,
        }
    }

    /// `NamespaceResolver.ToUser()`.
    pub fn as_user(&self) -> Option<&NamespaceUser> {
        match self {
            NamespaceResolver::User(user) => Some(user),
            NamespaceResolver::Org(_) => None,
        }
    }

    /// `NamespaceResolver.ToOrg()`.
    pub fn as_org(&self) -> Option<&NamespaceOrg> {
        match self {
            NamespaceResolver::Org(org) => Some(org),
            NamespaceResolver::User(_) => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Connection / repository resolvers
// ---------------------------------------------------------------------------

/// `searchContextConnectionResolver` (`resolvers.go:516-537`).
pub struct SearchContextConnection<'a> {
    pub after_cursor: i32,
    pub search_contexts: Vec<SearchContextResolver<'a>>,
    pub total_count: i32,
    pub has_next_page: bool,
}

impl SearchContextConnection<'_> {
    /// `Nodes()`.
    pub fn nodes(&self) -> &[SearchContextResolver<'_>] {
        &self.search_contexts
    }

    /// `TotalCount()`.
    pub fn total_count(&self) -> i32 {
        self.total_count
    }

    /// `PageInfo()` (`resolvers.go:531-537`), mirroring
    /// `gqlutil.HasNextPage`/`NextPageCursor`.
    pub fn page_info(&self) -> PageInfo {
        if self.search_contexts.is_empty() || !self.has_next_page {
            return PageInfo {
                has_next_page: false,
                end_cursor: None,
            };
        }
        // The after value (offset) for the next page is the current after value
        // plus the number of retrieved search contexts.
        PageInfo {
            has_next_page: true,
            end_cursor: Some(marshal_search_context_cursor(
                self.after_cursor
                    .wrapping_add(self.search_contexts.len() as i32),
            )),
        }
    }
}

/// `gqlutil.PageInfo` (`internal/gqlutil/page_info.go:3-21`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageInfo {
    pub has_next_page: bool,
    pub end_cursor: Option<String>,
}

/// `searchContextRepositoryRevisionsResolver` (`resolvers.go:539-550`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchContextRepositoryRevisionsResolver {
    pub repository: RepositoryResolver,
    pub revisions: Vec<String>,
}

impl SearchContextRepositoryRevisionsResolver {
    /// `Repository()`.
    pub fn repository(&self) -> &RepositoryResolver {
        &self.repository
    }

    /// `Revisions()`.
    pub fn revisions(&self) -> &[String] {
        &self.revisions
    }
}

/// `graphqlbackend.RepositoryResolver`, limited to the minimal fields the
/// repository-revisions resolver exposes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryResolver {
    pub id: i32,
    pub name: String,
}

impl RepositoryResolver {
    /// `RepositoryResolver.ID()`.
    pub fn id(&self) -> String {
        marshal_repository_id(self.id)
    }

    /// `RepositoryResolver.Name()`.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// `RepositoryResolver.URL()`: `url.URL{Path: "/" + name}`.
    pub fn url(&self) -> String {
        format!("/{}", self.name)
    }
}

// ---------------------------------------------------------------------------
// `gqlutil.DateTime`
// ---------------------------------------------------------------------------

/// `gqlutil.DateTime.MarshalJSON`: UTC `time.RFC3339`, whole seconds.
pub fn format_gqlutil_datetime(t: OffsetDateTime) -> String {
    let t = t.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        t.year(),
        t.month() as u8,
        t.day(),
        t.hour(),
        t.minute(),
        t.second()
    )
}

// ---------------------------------------------------------------------------
// Helpers shared with tests
// ---------------------------------------------------------------------------

/// `GetGlobalSearchContext()` surfaced as a full record (zero timestamp, no
/// default/star).
pub fn global_search_context_record() -> SearchContextRecord {
    SearchContextRecord::from_base(get_global_search_context())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::search_contexts::GetSearchContextOptions;
    use time::format_description::well_known::Rfc3339;

    fn parse(value: &str) -> OffsetDateTime {
        OffsetDateTime::parse(value, &Rfc3339).expect("rfc3339")
    }

    #[test]
    fn relays_search_context_ids() {
        let id = marshal_search_context_id("global");
        assert_eq!(unmarshal_search_context_id(&id).expect("spec"), "global");
        assert_eq!(relay_kind(&id).as_deref(), Some("SearchContext"));

        let id = marshal_search_context_id("@alice/ctx");
        assert_eq!(
            unmarshal_search_context_id(&id).expect("spec"),
            "@alice/ctx"
        );
    }

    #[test]
    fn relays_cursors_and_namespaces() {
        assert_eq!(unmarshal_search_context_cursor(None).expect("nil"), 0);
        let cursor = marshal_search_context_cursor(42);
        assert_eq!(
            unmarshal_search_context_cursor(Some(&cursor)).expect("cursor"),
            42
        );

        let user = marshal_user_id(7);
        assert_eq!(unmarshal_namespace_id(&user).expect("user"), (7, 0));
        let org = marshal_org_id(9);
        assert_eq!(unmarshal_namespace_id(&org).expect("org"), (0, 9));
        assert_eq!(
            unmarshal_namespace_id("!!!").expect_err("invalid"),
            "invalid ID \"!!!\" for namespace"
        );
    }

    #[test]
    fn rejects_invalid_relay_ids_like_go() {
        assert_eq!(
            unmarshal_search_context_id("not-base64!!").expect_err("base64"),
            "illegal base64 data at input byte 10"
        );
        let no_colon = base64::engine::general_purpose::URL_SAFE.encode("SearchContext");
        assert_eq!(
            unmarshal_search_context_id(&no_colon).expect_err("colon"),
            "invalid graphql.ID"
        );
        let number = base64::engine::general_purpose::URL_SAFE.encode("SearchContext:123");
        assert_eq!(
            unmarshal_search_context_id(&number).expect_err("type"),
            "json: cannot unmarshal number into Go value of type string"
        );
    }

    #[test]
    fn formats_updated_at_like_gqlutil() {
        assert_eq!(
            format_gqlutil_datetime(parse("2024-02-03T04:05:06Z")),
            "2024-02-03T04:05:06Z"
        );
        assert_eq!(
            format_gqlutil_datetime(go_zero_time()),
            "0001-01-01T00:00:00Z"
        );
        assert_eq!(
            format_gqlutil_datetime(OffsetDateTime::UNIX_EPOCH),
            "1970-01-01T00:00:00Z"
        );
    }

    #[test]
    fn builds_specs_from_records() {
        let record = SearchContextRecord {
            name: "ctx".to_owned(),
            namespace_user_id: 1,
            namespace_user_name: "alice".to_owned(),
            ..SearchContextRecord::default()
        };
        let resolver = SearchContextResolver {
            search_context: record,
            db: &NoopDb,
        };
        assert_eq!(resolver.spec(), "@alice/ctx");
        assert_eq!(resolver.id(), marshal_search_context_id("@alice/ctx"));
    }

    struct NoopDb;

    impl SearchContextsDatabase for NoopDb {
        fn get_namespace_by_name(
            &self,
            _name: &str,
        ) -> Result<crate::search_contexts::Namespace, Error> {
            Err(Error::namespace_not_found())
        }
        fn get_org_member_by_org_id_and_user_id(
            &self,
            _org_id: i32,
            _user_id: i32,
        ) -> Result<Option<crate::search_contexts::OrgMembership>, Error> {
            Err(Error::org_member_not_found())
        }
        fn get_current_user(&self, _uid: i32) -> Result<Option<User>, Error> {
            Ok(None)
        }
        fn get_search_context(
            &self,
            _opts: &GetSearchContextOptions,
        ) -> Result<BaseSearchContext, Error> {
            Err(Error::search_context_not_found())
        }
        fn create_search_context_with_repository_revisions(
            &self,
            _search_context: &BaseSearchContext,
            _repository_revisions: &[SearchContextRepositoryRevisions],
        ) -> Result<BaseSearchContext, Error> {
            Err(Error::other("unused"))
        }
        fn update_search_context_with_repository_revisions(
            &self,
            _search_context: &BaseSearchContext,
            _repository_revisions: &[SearchContextRepositoryRevisions],
        ) -> Result<BaseSearchContext, Error> {
            Err(Error::other("unused"))
        }
        fn delete_search_context(&self, _search_context_id: i64) -> Result<(), Error> {
            Err(Error::other("unused"))
        }
        fn get_all_revisions_for_repos(
            &self,
            _repo_ids: &[i32],
        ) -> Result<BTreeMap<i32, Vec<String>>, Error> {
            Ok(BTreeMap::new())
        }
        fn get_all_queries(&self) -> Result<Vec<String>, Error> {
            Ok(Vec::new())
        }
        fn get_search_context_repository_revisions(
            &self,
            _search_context_id: i64,
        ) -> Result<Vec<SearchContextRepositoryRevisions>, Error> {
            Ok(Vec::new())
        }
        fn list_minimal_repos(
            &self,
            _opts: &crate::search_contexts::ReposListOptions,
        ) -> Result<Vec<crate::search_contexts::MinimalRepo>, Error> {
            Ok(Vec::new())
        }
        fn create_search_context_star_for_user(
            &self,
            _user_id: i32,
            _search_context_id: i64,
        ) -> Result<(), Error> {
            Ok(())
        }
        fn delete_search_context_star_for_user(
            &self,
            _user_id: i32,
            _search_context_id: i64,
        ) -> Result<(), Error> {
            Ok(())
        }
        fn set_user_default_search_context_id(
            &self,
            _user_id: i32,
            _search_context_id: i64,
        ) -> Result<(), Error> {
            Ok(())
        }
    }

    impl SearchContextsGraphqlDatabase for NoopDb {
        fn as_search_contexts_database(&self) -> &dyn SearchContextsDatabase {
            self
        }
        fn get_search_context_by_id(
            &self,
            _ctx: &RequestContext,
            _id: i64,
        ) -> Result<SearchContextRecord, Error> {
            Err(Error::search_context_not_found())
        }
        fn list_search_contexts(
            &self,
            _ctx: &RequestContext,
            _page: &ListSearchContextsPageOptions,
            _opts: &ListSearchContextsOptions,
        ) -> Result<Vec<SearchContextRecord>, Error> {
            Ok(Vec::new())
        }
        fn count_search_contexts(
            &self,
            _ctx: &RequestContext,
            _opts: &ListSearchContextsOptions,
        ) -> Result<i32, Error> {
            Ok(0)
        }
        fn get_default_search_context_for_current_user(
            &self,
            _ctx: &RequestContext,
        ) -> Result<SearchContextRecord, Error> {
            Ok(global_search_context_record())
        }
        fn get_repos_set_by_ids(
            &self,
            _ids: &[i32],
        ) -> Result<BTreeMap<i32, MinimalRepoRef>, Error> {
            Ok(BTreeMap::new())
        }
        fn get_user_by_id(&self, _id: i32) -> Result<NamespaceUser, Error> {
            Err(Error::other("user not found"))
        }
        fn get_org_by_id(&self, _id: i32) -> Result<NamespaceOrg, Error> {
            Err(Error::other("org not found"))
        }
        fn get_orgs_by_user_id(&self, _user_id: i32) -> Result<Vec<NamespaceOrg>, Error> {
            Ok(Vec::new())
        }
    }
}
