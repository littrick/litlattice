#![forbid(unsafe_code)]

//! `SiteConfigurationChange` node and `SiteConfigurationChangeConnection`
//! (`cmd/frontend/graphqlbackend/site_config_change.go:1-78`,
//! `site_config_change_connection.go:1-169`).
//!
//! The baseline connection is a `gqlutil.ConnectionResolver` over
//! `SiteConfigurationChangeConnectionStore`:
//!
//! - [`SiteConfigurationChangeConnection`] reproduces the generic resolver's
//!   `pageSize`/`paginationArgs`/`Nodes`/`PageInfo`/`TotalCount` behaviour
//!   (`internal/gqlutil/connection_resolver.go:33-296`);
//! - [`compute_nodes`] reproduces the store's clone + `modifyArgs` +
//!   `ListSiteConfigs` + `generateResolversForFirst/Last` + trim logic;
//! - [`visible_history`]/[`list_site_configs`] reproduce the SQL's
//!   `LAG(redacted_contents)` de-duplication, ordering and `after`/`before`
//!   filtering (`internal/database/conf.go:140-216`);
//! - [`SiteConfigurationChangeResolver::diff`] ports the Myers diff and
//!   `gotextdiff.ToUnified` formatter (`gotextdiff@v1.0.3`), using only the
//!   redacted contents.
//!
//! The configuration history and the author lookup are injected through
//! [`SiteConfigHistoryStore`] and [`SiteConfigChangeUserStore`]; no real
//! database, GraphQL schema wiring or audit write is done here.

use async_trait::async_trait;
use base64::Engine as _;
use time::OffsetDateTime;

use crate::graphql_resolvers::datetime_rfc3339;

/// `siteConfigurationChangeKind` (`site_config_change.go:17`).
pub const SITE_CONFIGURATION_CHANGE_KIND: &str = "SiteConfigurationChange";

/// `gqlutil.DefaultMaxPageSize` (`connection_resolver.go:11`).
pub const DEFAULT_MAX_PAGE_SIZE: i32 = 100;

/// One raw `critical_and_site_config` row of type `site`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SiteConfigRow {
    pub id: i32,
    pub author_user_id: i32,
    pub contents: String,
    /// `redacted_contents` may be `NULL`; such rows never surface as nodes
    /// (`conf.go:169-173`), but they affect the `LAG` window of the next row.
    pub redacted_contents: Option<String>,
    pub created_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
}

/// The `database.SiteConfig` facts a node needs after the SQL filter.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SiteConfigNodeData {
    id: i32,
    author_user_id: i32,
    redacted_contents: String,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

impl SiteConfigNodeData {
    fn from_row(row: &SiteConfigRow) -> Self {
        Self {
            id: row.id,
            author_user_id: row.author_user_id,
            // Only rows with non-NULL redacted contents pass
            // `visible_history`, so this default is never observable.
            redacted_contents: row.redacted_contents.clone().unwrap_or_default(),
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

/// `database.PaginationArgs` reduced to the fields this store observes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PaginationArgs {
    pub first: Option<i32>,
    pub last: Option<i32>,
    /// `After` in Go is `[]any` holding one `int32`.
    pub after: Vec<i32>,
    /// `Before` in Go is `[]any` holding one `int32`.
    pub before: Vec<i32>,
    pub ascending: bool,
}

/// The configuration-history seam (`db.Conf().ListSiteConfigs` /
/// `GetSiteConfigCount`).
///
/// `get_site_configs` returns the complete raw history in ascending id order;
/// the portable layer applies the baseline's `LAG` de-duplication, ordering,
/// `after`/`before` filtering and `LIMIT`. `get_site_config_count` mirrors the
/// independent `GetSiteConfigCount` query, i.e. the number of rows that survive
/// that de-duplication (not affected by pagination).
#[async_trait]
pub trait SiteConfigHistoryStore: Send + Sync {
    async fn get_site_configs(&self) -> Result<Vec<SiteConfigRow>, String>;
    async fn get_site_config_count(&self) -> Result<usize, String>;
}

/// A user returned by `UserByIDInt32` (`user.go:114-120`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserRef {
    pub id: i32,
}

/// `UserByIDInt32`: `Ok(None)` means "not found" (the baseline maps
/// `errcode.IsNotFound` to a `null` author); `Err` is any other error.
#[async_trait]
pub trait SiteConfigChangeUserStore: Send + Sync {
    async fn user_by_id(&self, id: i32) -> Result<Option<UserRef>, String>;
}

/// `marshalSiteConfigurationChangeID` (`site_config_change.go:76-78`):
/// `relay.MarshalID("SiteConfigurationChange", &id)`.
pub fn marshal_site_configuration_change_id(id: i32) -> String {
    base64::engine::general_purpose::URL_SAFE
        .encode(format!("{SITE_CONFIGURATION_CHANGE_KIND}:{id}"))
}

/// `SiteConfigurationChangeConnectionStore.UnmarshalCursor`
/// (`site_config_change_connection.go:89-97`): `relay.UnmarshalSpec` into an
/// `int32` with the Go error texts.
pub fn unmarshal_site_configuration_change_id(cursor: &str) -> Result<i32, String> {
    let decoded = decode_go_base64(cursor)?;
    let text = String::from_utf8_lossy(&decoded);
    let Some((_, spec)) = text.split_once(':') else {
        return Err("invalid graphql.ID".to_owned());
    };
    unmarshal_go_int32(spec)
}

/// `relay.UnmarshalSpec`'s base64 step, reproducing
/// `base64.URLEncoding.DecodeString`'s `illegal base64 data at input byte N`
/// message (newlines are ignored, as in Go).
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
            // Go reports the start of the incomplete quantum.
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

/// `json.Unmarshal(spec, &int32)` with Go's error messages (the cursor corpus
/// only exercises the documented cases).
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

/// `ListSiteConfigs`' outer `WHERE`: de-duplicate consecutive equal
/// `redacted_contents`, drop `NULL` contents, ordered by id (`conf.go:151-216`).
pub fn visible_history(rows: &[SiteConfigRow]) -> Vec<SiteConfigRow> {
    let mut sorted: Vec<&SiteConfigRow> = rows.iter().collect();
    sorted.sort_by_key(|row| row.id);
    let mut previous: Option<&str> = None;
    let mut visible = Vec::new();
    for row in sorted {
        let redacted = row.redacted_contents.as_deref();
        let changed = match (previous, redacted) {
            (None, _) => true,
            (Some(previous), Some(current)) => previous != current,
            // SQL's `NULL != value` is NULL, i.e. not true, and the
            // `redacted_contents IS NOT NULL` predicate rejects it anyway.
            (Some(_), None) => false,
        };
        if changed && redacted.is_some() {
            visible.push(row.clone());
        }
        previous = redacted;
    }
    visible
}

/// `ListSiteConfigs(ctx, args)` over the injected raw history. The `after` /
/// `before` comparison directions, the `first`/`last` ordering direction and
/// the `LIMIT` follow `database.PaginationArgs.SQL()` (`helpers.go:145-205`).
pub fn list_site_configs(rows: &[SiteConfigRow], args: &PaginationArgs) -> Vec<SiteConfigRow> {
    let mut history = visible_history(rows);
    let ascending = if args.first.is_some() {
        args.ascending
    } else if args.last.is_some() {
        !args.ascending
    } else {
        args.ascending
    };
    // `after`/`before` directions depend on `PaginationArgs.Ascending`, not on
    // the effective ORDER BY direction chosen for `first`/`last`
    // (`helpers.go:157-188`).
    if let Some(after) = args.after.first() {
        history.retain(|row| {
            if args.ascending {
                row.id > *after
            } else {
                row.id < *after
            }
        });
    }
    if let Some(before) = args.before.first() {
        history.retain(|row| {
            if args.ascending {
                row.id < *before
            } else {
                row.id > *before
            }
        });
    }
    history.sort_by_key(|row| row.id);
    if !ascending {
        history.reverse();
    }
    let limit = if args.first.is_some() {
        args.first
    } else {
        args.last
    };
    if let Some(limit) = limit {
        if limit <= 0 {
            history.clear();
        } else {
            history.truncate(limit as usize);
        }
    }
    history
}

/// `modifyArgs` (`site_config_change_connection.go:99-117`): fetch one extra
/// older item so the oldest requested node still has a `previousSiteConfig`.
pub fn modify_args(args: &mut PaginationArgs) -> bool {
    if let Some(first) = &mut args.first {
        *first = first.wrapping_add(1);
        true
    } else if let Some(last) = &mut args.last {
        if let Some(before) = args.before.first_mut() {
            if *before > 0 {
                *last = last.wrapping_add(1);
                *before = before.wrapping_sub(1);
                return true;
            }
        }
        false
    } else {
        false
    }
}

fn generate_resolvers_for_first(history: &[SiteConfigRow]) -> Vec<SiteConfigurationChangeResolver> {
    let total = history.len();
    (0..total)
        .map(|index| SiteConfigurationChangeResolver {
            site_config: SiteConfigNodeData::from_row(&history[index]),
            previous_site_config: history.get(index + 1).map(SiteConfigNodeData::from_row),
        })
        .collect()
}

fn generate_resolvers_for_last(history: &[SiteConfigRow]) -> Vec<SiteConfigurationChangeResolver> {
    (0..history.len())
        .map(|index| SiteConfigurationChangeResolver {
            site_config: SiteConfigNodeData::from_row(&history[index]),
            previous_site_config: index
                .checked_sub(1)
                .map(|previous| SiteConfigNodeData::from_row(&history[previous])),
        })
        .collect()
}

/// `SiteConfigurationChangeConnectionStore.ComputeTotal`
/// (`site_config_change_connection.go:39-42`).
pub async fn compute_total(store: &dyn SiteConfigHistoryStore) -> Result<i32, String> {
    let count = store.get_site_config_count().await?;
    Ok(count as i32)
}

/// `SiteConfigurationChangeConnectionStore.ComputeNodes`
/// (`site_config_change_connection.go:44-82`).
pub async fn compute_nodes(
    store: &dyn SiteConfigHistoryStore,
    args: Option<&PaginationArgs>,
) -> Result<Vec<SiteConfigurationChangeResolver>, String> {
    let Some(args) = args else {
        return Err("pagination args cannot be nil".to_owned());
    };

    let mut pagination_args = args.clone();
    let modified = modify_args(&mut pagination_args);

    let rows = store.get_site_configs().await?;
    let history = list_site_configs(&rows, &pagination_args);
    let total_fetched = history.len();
    if total_fetched == 0 {
        return Ok(Vec::new());
    }

    let mut resolvers = if pagination_args.first.is_some() {
        generate_resolvers_for_first(&history)
    } else if pagination_args.last.is_some() {
        generate_resolvers_for_last(&history)
    } else {
        Vec::new()
    };

    if modified {
        if pagination_args.last.is_some() {
            if !resolvers.is_empty() {
                resolvers.remove(0);
            }
        } else if pagination_args.first.is_some()
            && Some(total_fetched as i32) == pagination_args.first
        {
            resolvers.pop();
        }
    }

    Ok(resolvers)
}

/// `SiteConfigurationChangeResolver` (`site_config_change.go:19-73`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SiteConfigurationChangeResolver {
    site_config: SiteConfigNodeData,
    previous_site_config: Option<SiteConfigNodeData>,
}

impl SiteConfigurationChangeResolver {
    /// `ID()` (`site_config_change.go:25-27`).
    pub fn id(&self) -> String {
        marshal_site_configuration_change_id(self.site_config.id)
    }

    /// The underlying row id (used by page cursors and tests).
    pub fn site_config_id(&self) -> i32 {
        self.site_config.id
    }

    pub fn author_user_id(&self) -> i32 {
        self.site_config.author_user_id
    }

    /// `Author(ctx)` (`site_config_change.go:29-44`).
    pub async fn author(
        &self,
        users: &dyn SiteConfigChangeUserStore,
    ) -> Result<Option<UserRef>, String> {
        if self.site_config.author_user_id == 0 {
            return Ok(None);
        }
        users.user_by_id(self.site_config.author_user_id).await
    }

    /// `Diff()` (`site_config_change.go:46-65`). Only redacted contents are
    /// read, mirroring the baseline security note.
    pub fn diff(&self) -> String {
        let (previous_id, previous_contents) = match &self.previous_site_config {
            Some(previous) => (previous.id, previous.redacted_contents.as_str()),
            None => (0, ""),
        };
        let edits = myers_compute_edits(previous_contents, &self.site_config.redacted_contents);
        to_unified(
            &format!("ID: {previous_id}"),
            &format!("ID: {}", self.site_config.id),
            previous_contents,
            edits,
        )
    }

    /// `CreatedAt()` (`site_config_change.go:67-69`), marshalled as
    /// `gqlutil.DateTime` (UTC RFC3339).
    pub fn created_at(&self) -> String {
        datetime_rfc3339(self.site_config.created_at)
    }

    /// `UpdatedAt()` (`site_config_change.go:71-73`).
    pub fn updated_at(&self) -> String {
        datetime_rfc3339(self.site_config.updated_at)
    }
}

/// `gqlutil.ConnectionResolverArgs` (`connection_resolver.go:33-39`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnectionResolverArgs {
    pub first: Option<i32>,
    pub last: Option<i32>,
    pub after: Option<String>,
    pub before: Option<String>,
}

/// `gqlutil.ConnectionResolverOptions` (`connection_resolver.go:51-72`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnectionResolverOptions {
    /// 0 means [`DEFAULT_MAX_PAGE_SIZE`].
    pub max_page_size: i32,
    /// `None` behaves like the Go nil pointer (reversal enabled).
    pub reverse: Option<bool>,
    pub ascending: bool,
    pub allow_no_limit: bool,
}

impl ConnectionResolverOptions {
    fn max_page_size_or_default(&self) -> i32 {
        if self.max_page_size == 0 {
            DEFAULT_MAX_PAGE_SIZE
        } else {
            self.max_page_size
        }
    }

    fn apply_max_page_size(&self, limit: i32) -> i32 {
        let max_page_size = self.max_page_size_or_default();
        if limit == 0 {
            max_page_size
        } else if limit < max_page_size {
            limit
        } else {
            max_page_size
        }
    }

    fn reverses(&self) -> bool {
        self.reverse.unwrap_or(true)
    }
}

/// `gqlutil.ConnectionPageInfo` (`connection_resolver.go:218-273`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConnectionPageInfo {
    pub has_next_page: bool,
    pub has_previous_page: bool,
    pub start_cursor: Option<String>,
    pub end_cursor: Option<String>,
}

/// `gqlutil.ConnectionResolver[*SiteConfigurationChangeResolver]` over
/// [`SiteConfigurationChangeConnectionStore`]'s semantics.
#[derive(Clone)]
pub struct SiteConfigurationChangeConnection<'a> {
    store: &'a dyn SiteConfigHistoryStore,
    args: Option<ConnectionResolverArgs>,
    options: ConnectionResolverOptions,
}

impl<'a> SiteConfigurationChangeConnection<'a> {
    pub fn new(
        store: &'a dyn SiteConfigHistoryStore,
        args: Option<ConnectionResolverArgs>,
        options: ConnectionResolverOptions,
    ) -> Self {
        Self {
            store,
            args,
            options,
        }
    }

    /// `ConnectionResolver.pageSize` (`connection_resolver.go:151-153`).
    fn page_size(&self) -> i32 {
        let limit = match &self.args {
            Some(args) => args.first.or(args.last).unwrap_or(0),
            None => 0,
        };
        self.options.apply_max_page_size(limit)
    }

    /// `ConnectionResolver.Nodes` before the generic trim: returns the nodes
    /// and `fetchedNodesCount` (`connection_resolver.go:165-205`).
    async fn fetched_nodes(&self) -> Result<(Vec<SiteConfigurationChangeResolver>, usize), String> {
        let Some(args) = &self.args else {
            // `paginationArgs()` returns nil for nil args, then
            // `ComputeNodes(nil)` fails with this message.
            return compute_nodes(self.store, None)
                .await
                .map(|nodes| (nodes, 0));
        };

        let page_size = self.page_size();
        let limit = page_size.wrapping_add(1);
        let (first, last) = if args.first.is_some() {
            (Some(limit), None)
        } else if args.last.is_some() {
            (None, Some(limit))
        } else if self.options.allow_no_limit {
            (None, None)
        } else {
            return Err(
                "you must provide a `first` or `last` value to properly paginate".to_owned(),
            );
        };

        let after = match &args.after {
            Some(raw) => vec![unmarshal_site_configuration_change_id(raw)?],
            None => Vec::new(),
        };
        let before = match &args.before {
            Some(raw) => vec![unmarshal_site_configuration_change_id(raw)?],
            None => Vec::new(),
        };
        let pagination_args = PaginationArgs {
            first,
            last,
            after,
            before,
            ascending: self.options.ascending,
        };

        let mut nodes = compute_nodes(self.store, Some(&pagination_args)).await?;
        let fetched_count = nodes.len();

        if self.options.reverses() && args.last.is_some() {
            nodes.reverse();
        }
        if nodes.len() as i64 > i64::from(page_size) {
            if args.last.is_some() {
                if !nodes.is_empty() {
                    nodes.remove(0);
                }
            } else {
                nodes.pop();
            }
        }

        Ok((nodes, fetched_count))
    }

    /// `ConnectionResolver.Nodes` after the generic trim.
    pub async fn nodes(&self) -> Result<Vec<SiteConfigurationChangeResolver>, String> {
        Ok(self.fetched_nodes().await?.0)
    }

    /// `ConnectionResolver.PageInfo` (`connection_resolver.go:207-271`).
    pub async fn page_info(&self) -> Result<ConnectionPageInfo, String> {
        let Some(args) = &self.args else {
            return compute_nodes(self.store, None)
                .await
                .map(|_| ConnectionPageInfo::default());
        };

        let page_size = self.page_size();
        let (nodes, fetched_count) = self.fetched_nodes().await?;
        let fetched_count = fetched_count as i64;
        let page_size = i64::from(page_size);

        let has_next_page = if args.first.is_some() {
            fetched_count > page_size
        } else if fetched_count == 0 {
            false
        } else {
            args.before.is_some()
        };
        let has_previous_page = if args.last.is_some() {
            fetched_count > page_size
        } else if fetched_count == 0 {
            false
        } else {
            args.after.is_some()
        };

        Ok(ConnectionPageInfo {
            has_next_page,
            has_previous_page,
            start_cursor: nodes.first().map(SiteConfigurationChangeResolver::id),
            end_cursor: nodes.last().map(SiteConfigurationChangeResolver::id),
        })
    }

    /// `ConnectionResolver.TotalCount` (`connection_resolver.go:156-162`).
    pub async fn total_count(&self) -> Result<i32, String> {
        compute_total(self.store).await
    }
}

// ---------------------------------------------------------------------------
// Myers diff + gotextdiff unified formatter (`gotextdiff@v1.0.3`)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiffOp {
    Delete,
    Insert,
    Equal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TextEdit {
    start_line: i64,
    start_col: i64,
    start_offset: i64,
    end_line: i64,
    end_col: i64,
    end_offset: i64,
    new_text: String,
}

#[derive(Debug, Clone)]
struct Operation {
    kind: DiffOp,
    content: Vec<String>,
    i1: i64,
    i2: i64,
    j1: i64,
}

/// `gotextdiff.splitLines` (`unified.go` and `myers/diff.go`).
fn split_lines(text: &str) -> Vec<String> {
    let mut lines = Vec::new();
    let mut start = 0;
    for (index, byte) in text.bytes().enumerate() {
        if byte == b'\n' {
            lines.push(text[start..=index].to_owned());
            start = index + 1;
        }
    }
    if start < text.len() {
        lines.push(text[start..].to_owned());
    }
    lines
}

/// Start offset of a (1-based) line, matching `go/token.File`'s conversion.
fn line_offset(content: &str, line: i64) -> i64 {
    let mut starts = Vec::new();
    if !content.is_empty() {
        starts.push(0i64);
        for (index, byte) in content.bytes().enumerate() {
            if byte == b'\n' && index + 1 < content.len() {
                starts.push((index + 1) as i64);
            }
        }
    }
    let line_max = starts.len() as i64 + 1;
    if line == line_max {
        content.len() as i64
    } else {
        starts[(line - 1) as usize]
    }
}

/// `myers.ComputeEdits` (`myers/diff.go:18-33`).
fn myers_compute_edits(before: &str, after: &str) -> Vec<TextEdit> {
    let a = split_lines(before);
    let b = split_lines(after);
    operations(&a, &b)
        .into_iter()
        .filter_map(|operation| {
            let start_line = operation.i1 + 1;
            let end_line = operation.i2 + 1;
            let new_text = match operation.kind {
                DiffOp::Delete => String::new(),
                DiffOp::Insert => operation.content.concat(),
                DiffOp::Equal => return None,
            };
            if operation.kind == DiffOp::Insert && new_text.is_empty() {
                return None;
            }
            Some(TextEdit {
                start_line,
                start_col: 1,
                start_offset: 0,
                end_line,
                end_col: 1,
                end_offset: 0,
                new_text,
            })
        })
        .collect()
}

/// `myers.operations` (`myers/diff.go:47-105`).
fn operations(a: &[String], b: &[String]) -> Vec<Operation> {
    if a.is_empty() && b.is_empty() {
        return Vec::new();
    }
    let Some((trace, offset)) = shortest_edit_sequence(a, b) else {
        return Vec::new();
    };
    let snakes = backtrack(&trace, a.len() as i64, b.len() as i64, offset);
    let m = a.len() as i64;
    let n = b.len() as i64;
    let mut solution: Vec<Option<Operation>> = (0..a.len() + b.len()).map(|_| None).collect();
    let mut i = 0usize;
    let mut add = |operation: Option<Operation>, i2: i64, j2: i64| {
        let Some(mut operation) = operation else {
            return;
        };
        operation.i2 = i2;
        if operation.kind == DiffOp::Insert {
            operation.content = b[operation.j1 as usize..j2 as usize].to_vec();
        }
        solution[i] = Some(operation);
        i += 1;
    };

    let mut x = 0i64;
    let mut y = 0i64;
    for snake in snakes {
        if snake.len() < 2 {
            continue;
        }
        let mut operation = None;
        while snake[0] - snake[1] > x - y {
            if operation.is_none() {
                operation = Some(Operation {
                    kind: DiffOp::Delete,
                    content: Vec::new(),
                    i1: x,
                    i2: 0,
                    j1: y,
                });
            }
            x += 1;
            if x == m {
                break;
            }
        }
        add(operation, x, y);
        operation = None;
        while snake[0] - snake[1] < x - y {
            if operation.is_none() {
                operation = Some(Operation {
                    kind: DiffOp::Insert,
                    content: Vec::new(),
                    i1: x,
                    i2: 0,
                    j1: y,
                });
            }
            y += 1;
        }
        add(operation, x, y);
        while x < snake[0] {
            x += 1;
            y += 1;
        }
        if x >= m && y >= n {
            break;
        }
    }
    solution.into_iter().flatten().collect()
}

/// `myers.backtrack` (`myers/diff.go:107-141`).
fn backtrack(trace: &[Vec<i64>], mut x: i64, mut y: i64, offset: i64) -> Vec<Vec<i64>> {
    let mut snakes = vec![Vec::new(); trace.len()];
    let mut d = trace.len() as i64 - 1;
    while x > 0 && y > 0 && d > 0 {
        let v = &trace[d as usize];
        if v.is_empty() {
            d -= 1;
            continue;
        }
        snakes[d as usize] = vec![x, y];
        let k = x - y;
        let k_prev =
            if k == -d || (k != d && v[(k - 1 + offset) as usize] < v[(k + 1 + offset) as usize]) {
                k + 1
            } else {
                k - 1
            };
        x = v[(k_prev + offset) as usize];
        y = x - k_prev;
        d -= 1;
    }
    if x < 0 || y < 0 {
        return snakes;
    }
    if d >= 0 {
        snakes[d as usize] = vec![x, y];
    }
    snakes
}

/// `myers.shortestEditSequence` (`myers/diff.go:143-198`).
fn shortest_edit_sequence(a: &[String], b: &[String]) -> Option<(Vec<Vec<i64>>, i64)> {
    let m = a.len() as i64;
    let n = b.len() as i64;
    let offset = n + m;
    let mut v = vec![0i64; (2 * (m + n) + 1) as usize];
    let mut trace = vec![Vec::new(); (m + n + 1) as usize];
    for d in 0..=(m + n) {
        let mut copy_v = vec![0i64; v.len()];
        let mut k = -d;
        while k <= d {
            let mut x = if k == -d
                || (k != d && v[(k - 1 + offset) as usize] < v[(k + 1 + offset) as usize])
            {
                v[(k + 1 + offset) as usize]
            } else {
                v[(k - 1 + offset) as usize] + 1
            };
            let mut y = x - k;
            while x < m && y < n && a[x as usize] == b[y as usize] {
                x += 1;
                y += 1;
            }
            v[(k + offset) as usize] = x;
            if x == m && y == n {
                copy_v.copy_from_slice(&v);
                trace[d as usize] = copy_v;
                return Some((trace, offset));
            }
            k += 2;
        }
        copy_v.copy_from_slice(&v);
        trace[d as usize] = copy_v;
    }
    None
}

/// `gotextdiff.prepareEdits` + `WithAll` (`diff.go:66-85`, `span/span.go`).
fn prepare_edits(content: &str, mut edits: Vec<TextEdit>) -> (Vec<TextEdit>, bool) {
    let mut partial = false;
    for edit in &mut edits {
        edit.start_offset = line_offset(content, edit.start_line);
        edit.end_offset = line_offset(content, edit.end_line);
        partial = partial
            || edit.start_offset >= content.len() as i64
            || edit.start_col > 1
            || edit.end_col > 1;
    }
    edits.sort_by(|a, b| {
        (a.start_line, a.start_col, a.end_line, a.end_col).cmp(&(
            b.start_line,
            b.start_col,
            b.end_line,
            b.end_col,
        ))
    });
    (edits, partial)
}

/// `gotextdiff.lineEdits` (`diff.go:88-110`).
fn line_edits(content: &str, edits: Vec<TextEdit>) -> Vec<TextEdit> {
    let mut adjusted = Vec::new();
    let mut current: Option<TextEdit> = None;
    for edit in edits {
        if let Some(existing) = &current {
            if edit.start_line <= existing.end_line {
                let gap = &content[existing.end_offset as usize..edit.start_offset as usize];
                let mut merged = existing.clone();
                merged.new_text.push_str(gap);
                merged.new_text.push_str(&edit.new_text);
                merged.end_line = edit.end_line;
                merged.end_col = edit.end_col;
                merged.end_offset = edit.end_offset;
                current = Some(merged);
                continue;
            }
        }
        if let Some(existing) = current.take() {
            adjusted = add_edit(content, adjusted, existing);
        }
        current = Some(edit);
    }
    if let Some(existing) = current {
        adjusted = add_edit(content, adjusted, existing);
    }
    adjusted
}

/// `gotextdiff.addEdit` (`diff.go:112-160`).
fn add_edit(content: &str, mut edits: Vec<TextEdit>, mut edit: TextEdit) -> Vec<TextEdit> {
    if edit.start_col > 1 {
        let delta = (edit.start_col - 1) as usize;
        edit.start_col = 1;
        edit.start_offset -= delta as i64;
        let start = edit.start_offset as usize;
        edit.new_text = format!("{}{}", &content[start..start + delta], edit.new_text);
    }
    if edit.start_offset >= content.len() as i64 && edit.start_line > 1 && !content.ends_with('\n')
    {
        let eol = content.rfind('\n').map(|index| index as i64).unwrap_or(0);
        let delta = content.len() as i64 - eol;
        edit.start_line -= 1;
        edit.start_col = 1;
        edit.start_offset -= delta;
        let start = edit.start_offset as usize;
        edit.new_text = format!(
            "{}{}",
            &content[start..start + delta as usize],
            edit.new_text
        );
    }
    if edit.end_col > 1 {
        let remains = &content[edit.end_offset as usize..];
        let eol = remains
            .find('\n')
            .map(|index| index + 1)
            .unwrap_or(remains.len());
        edit.end_line += 1;
        edit.end_col = 1;
        edit.end_offset += eol as i64;
        edit.new_text.push_str(&remains[..eol]);
    }
    edits.push(edit);
    edits
}

#[derive(Debug)]
struct UnifiedHunk {
    from_line: i64,
    to_line: i64,
    lines: Vec<(DiffOp, String)>,
}

/// `gotextdiff.ToUnified` (`unified.go:62-115`).
fn to_unified(from: &str, to: &str, content: &str, edits: Vec<TextEdit>) -> String {
    if edits.is_empty() {
        return String::new();
    }
    let (edits, partial) = prepare_edits(content, edits);
    let edits = if partial {
        line_edits(content, edits)
    } else {
        edits
    };
    let lines = split_lines(content);
    let mut hunks = Vec::new();
    let mut hunk: Option<UnifiedHunk> = None;
    let mut last = 0i64;
    let mut to_line = 0i64;
    const EDGE: i64 = 3;
    const GAP: i64 = EDGE * 2;

    for edit in edits {
        let start = edit.start_line - 1;
        let end = edit.end_line - 1;
        let direct_extension = hunk.is_some() && start == last;
        if !direct_extension {
            let join_previous = hunk.is_some() && start <= last + GAP;
            if join_previous {
                add_equal_lines(hunk.as_mut().expect("hunk exists"), &lines, last, start);
            } else {
                if let Some(mut previous) = hunk.take() {
                    add_equal_lines(&mut previous, &lines, last, last + EDGE);
                    hunks.push(previous);
                }
                to_line += start - last;
                let mut new_hunk = UnifiedHunk {
                    from_line: start + 1,
                    to_line: to_line + 1,
                    lines: Vec::new(),
                };
                let delta = add_equal_lines(&mut new_hunk, &lines, start - EDGE, start);
                new_hunk.from_line -= delta;
                new_hunk.to_line -= delta;
                hunk = Some(new_hunk);
            }
        }

        last = start;
        let existing = hunk.as_mut().expect("hunk exists");
        let mut index = start;
        while index < end {
            existing
                .lines
                .push((DiffOp::Delete, lines[index as usize].clone()));
            index += 1;
            last += 1;
        }
        if !edit.new_text.is_empty() {
            for line in split_lines(&edit.new_text) {
                existing.lines.push((DiffOp::Insert, line));
                to_line += 1;
            }
        }
    }

    if let Some(mut final_hunk) = hunk {
        add_equal_lines(&mut final_hunk, &lines, last, last + EDGE);
        hunks.push(final_hunk);
    }
    format_unified(from, to, &hunks)
}

/// `gotextdiff.addEqualLines` (`unified.go:123-137`).
fn add_equal_lines(hunk: &mut UnifiedHunk, lines: &[String], start: i64, end: i64) -> i64 {
    let mut delta = 0;
    let mut index = start;
    while index < end {
        if index < 0 {
            index += 1;
            continue;
        }
        if index >= lines.len() as i64 {
            return delta;
        }
        hunk.lines
            .push((DiffOp::Equal, lines[index as usize].clone()));
        delta += 1;
        index += 1;
    }
    delta
}

/// `gotextdiff.Unified.Format` (`unified.go:139-190`).
fn format_unified(from: &str, to: &str, hunks: &[UnifiedHunk]) -> String {
    if hunks.is_empty() {
        return String::new();
    }
    let mut output = format!("--- {from}\n+++ {to}\n");
    for hunk in hunks {
        let mut from_count = 0;
        let mut to_count = 0;
        for (kind, _) in &hunk.lines {
            match kind {
                DiffOp::Delete => from_count += 1,
                DiffOp::Insert => to_count += 1,
                DiffOp::Equal => {
                    from_count += 1;
                    to_count += 1;
                }
            }
        }
        output.push_str("@@");
        if from_count > 1 {
            output.push_str(&format!(" -{},{}", hunk.from_line, from_count));
        } else {
            output.push_str(&format!(" -{}", hunk.from_line));
        }
        if to_count > 1 {
            output.push_str(&format!(" +{},{}", hunk.to_line, to_count));
        } else {
            output.push_str(&format!(" +{}", hunk.to_line));
        }
        output.push_str(" @@\n");
        for (kind, content) in &hunk.lines {
            match kind {
                DiffOp::Delete => output.push('-'),
                DiffOp::Insert => output.push('+'),
                DiffOp::Equal => output.push(' '),
            }
            output.push_str(content);
            if !content.ends_with('\n') {
                output.push_str("\n\\ No newline at end of file\n");
            }
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::format_description::well_known::Rfc3339;

    fn row(id: i32, redacted: &str) -> SiteConfigRow {
        let time = OffsetDateTime::parse("2020-01-01T00:00:00Z", &Rfc3339).expect("time");
        SiteConfigRow {
            id,
            author_user_id: 0,
            contents: redacted.to_owned(),
            redacted_contents: Some(redacted.to_owned()),
            created_at: time,
            updated_at: time,
        }
    }

    #[test]
    fn cursor_round_trips() {
        let cursor = marshal_site_configuration_change_id(42);
        assert_eq!(unmarshal_site_configuration_change_id(&cursor).unwrap(), 42);
    }

    #[test]
    fn invalid_cursors_match_go_messages() {
        assert_eq!(
            unmarshal_site_configuration_change_id("not-base64!!").unwrap_err(),
            "illegal base64 data at input byte 10"
        );
        assert_eq!(
            unmarshal_site_configuration_change_id(
                &base64::engine::general_purpose::URL_SAFE.encode("SiteConfigurationChange")
            )
            .unwrap_err(),
            "invalid graphql.ID"
        );
        assert_eq!(
            unmarshal_site_configuration_change_id(
                &base64::engine::general_purpose::URL_SAFE.encode("SiteConfigurationChange:1.5")
            )
            .unwrap_err(),
            "json: cannot unmarshal number 1.5 into Go value of type int32"
        );
    }

    #[test]
    fn modify_args_matches_baseline() {
        let mut args = PaginationArgs {
            first: Some(5),
            ..PaginationArgs::default()
        };
        assert!(modify_args(&mut args));
        assert_eq!(args.first, Some(6));

        let mut args = PaginationArgs {
            last: Some(5),
            before: vec![10],
            ..PaginationArgs::default()
        };
        assert!(modify_args(&mut args));
        assert_eq!(args.last, Some(6));
        assert_eq!(args.before, vec![9]);

        let mut args = PaginationArgs {
            last: Some(5),
            before: vec![0],
            ..PaginationArgs::default()
        };
        assert!(!modify_args(&mut args));
    }

    #[test]
    fn visible_history_dedupes_and_drops_null() {
        let mut rows = vec![row(3, "b"), row(1, "a"), row(2, "a"), row(5, "b")];
        rows.push(SiteConfigRow {
            redacted_contents: None,
            ..row(4, "")
        });
        let visible = visible_history(&rows);
        let ids: Vec<i32> = visible.iter().map(|row| row.id).collect();
        assert_eq!(ids, vec![1, 3, 5]);
    }

    #[test]
    fn diff_matches_gotextdiff() {
        let previous = SiteConfigNodeData {
            id: 2,
            author_user_id: 0,
            redacted_contents: "{\n  \"a\": true,\n  \"b\": 1,\n}\n".to_owned(),
            created_at: OffsetDateTime::parse("2020-01-01T00:00:00Z", &Rfc3339).expect("time"),
            updated_at: OffsetDateTime::parse("2020-01-01T00:00:00Z", &Rfc3339).expect("time"),
        };
        let current = SiteConfigNodeData {
            id: 3,
            redacted_contents: "{\n  \"a\": false,\n  \"b\": 1,\n}\n".to_owned(),
            ..previous.clone()
        };
        let node = SiteConfigurationChangeResolver {
            site_config: current,
            previous_site_config: Some(previous),
        };
        assert_eq!(
            node.diff(),
            "--- ID: 2\n+++ ID: 3\n@@ -1,4 +1,4 @@\n {\n-  \"a\": true,\n+  \"a\": false,\n   \"b\": 1,\n }\n"
        );
    }

    #[test]
    fn diff_marks_missing_trailing_newline() {
        let node = SiteConfigurationChangeResolver {
            site_config: SiteConfigNodeData {
                id: 1,
                author_user_id: 0,
                redacted_contents: "a".to_owned(),
                created_at: OffsetDateTime::parse("2020-01-01T00:00:00Z", &Rfc3339).expect("time"),
                updated_at: OffsetDateTime::parse("2020-01-01T00:00:00Z", &Rfc3339).expect("time"),
            },
            previous_site_config: None,
        };
        assert_eq!(
            node.diff(),
            "--- ID: 0\n+++ ID: 1\n@@ -1 +1 @@\n+a\n\\ No newline at end of file\n"
        );
    }
}
