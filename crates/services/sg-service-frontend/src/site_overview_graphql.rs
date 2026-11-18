#![forbid(unsafe_code)]

//! Site-admin overview surfaces needed by the web app:
//!
//! * `Site.productVersion` (`site.go:183`);
//! * `Site.productSubscription` / `ProductSubscriptionStatus`
//!   (`product_subscription_status.go`): the unlicensed free plan;
//! * `Site.updateCheck` (`UpdateCheck`): no update checker is ported, so it
//!   reports "not checked" (`pending: false`, no timestamps, no version);
//! * `Site.analytics`: **shape-only**. The baseline's `internal/adminanalytics`
//!   aggregations (1993 lines, Redis-cached) are not ported yet; the resolvers
//!   return zero-valued summaries/empty nodes so the admin pages render. This
//!   is an explicit `ENV_GAP` (M9.57), not a data-faithful port.

use std::sync::Arc;

use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue};
use sg_graphql::json::Json;
use time::{Duration, OffsetDateTime};

/// Free-plan constants (`internal/licensing/licensing.go:67-74`,
/// `user_count.go:62-68`).
const FREE_PLAN_USER_COUNT: i64 = 10;
const NO_LICENSE_MAXIMUM_ALLOWED_USER_COUNT: i64 = 10;
const NO_LICENSE_WARNING_USER_COUNT: i64 = 10;

fn datetime(value: OffsetDateTime) -> String {
    let value = value.to_offset(time::UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        value.year(),
        value.month() as u8,
        value.day(),
        value.hour(),
        value.minute(),
        value.second()
    )
}

/// `ProductLicenseInfo` for the implicit free plan.
pub struct ProductLicenseInfoObject;

impl ObjectValue for ProductLicenseInfoObject {
    fn type_name(&self) -> &'static str {
        "ProductLicenseInfo"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "productNameWithBrand" => Ok(Some(FieldValue::Json(Json::Str(
                "Sourcegraph Free".to_owned(),
            )))),
            "isFreePlan" => Ok(Some(FieldValue::Json(Json::Bool(true)))),
            "tags" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("String"),
            })),
            "userCount" => Ok(Some(FieldValue::Json(Json::Int(FREE_PLAN_USER_COUNT)))),
            // `GetFreeLicenseInfo` expires one year from "now" on every call
            // (`licensing.go:72`).
            "expiresAt" => Ok(Some(FieldValue::Json(Json::Str(datetime(
                OffsetDateTime::now_utc() + Duration::hours(8760),
            ))))),
            "salesforceSubscriptionID" | "salesforceOpportunityID" | "hashedKey"
            | "licenseInvalidityReason" => Ok(Some(FieldValue::Json(Json::Null))),
            "isValid" => Ok(Some(FieldValue::Json(Json::Bool(true)))),
            _ => Ok(None),
        }
    }
}

/// `ProductSubscriptionStatus` (`product_subscription_status.go:16-61`).
pub struct ProductSubscriptionStatusObject;

impl ObjectValue for ProductSubscriptionStatusObject {
    fn type_name(&self) -> &'static str {
        "ProductSubscriptionStatus"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "productNameWithBrand" => Ok(Some(FieldValue::Json(Json::Str(
                "Sourcegraph Free".to_owned(),
            )))),
            // No license is in use, so the doc comments say zero / empty.
            "actualUserCount" => Ok(Some(FieldValue::Json(Json::Int(0)))),
            "actualUserCountDate" => Ok(Some(FieldValue::Json(Json::Str(String::new())))),
            "maximumAllowedUserCount" => Ok(Some(FieldValue::Json(Json::Int(
                NO_LICENSE_MAXIMUM_ALLOWED_USER_COUNT,
            )))),
            "noLicenseWarningUserCount" => Ok(Some(FieldValue::Json(Json::Int(
                NO_LICENSE_WARNING_USER_COUNT,
            )))),
            "license" => Ok(Some(FieldValue::Object(Arc::new(
                ProductLicenseInfoObject,
            )))),
            _ => Ok(None),
        }
    }
}

/// `UpdateCheck`: no update checker runs in this image.
pub struct UpdateCheckObject;

impl ObjectValue for UpdateCheckObject {
    fn type_name(&self) -> &'static str {
        "UpdateCheck"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "pending" => Ok(Some(FieldValue::Json(Json::Bool(false)))),
            "checkedAt" | "errorMessage" | "updateVersionAvailable" => {
                Ok(Some(FieldValue::Json(Json::Null)))
            }
            _ => Ok(None),
        }
    }
}

/// `siteResolver.UpgradeReadiness` (`site.go:358-368`): admin-only. Schema
/// drift detection and the out-of-band migration store are not ported; both
/// lists are empty (no drift / nothing required), which is the state of a
/// healthy instance at the installed version.
pub struct UpgradeReadinessObject;

impl ObjectValue for UpgradeReadinessObject {
    fn type_name(&self) -> &'static str {
        "UpgradeReadiness"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "schemaDrift" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("SchemaDriftSummary"),
            })),
            "requiredOutOfBandMigrations" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("OutOfBandMigration"),
            })),
            _ => Ok(None),
        }
    }
}

/// An empty `RecordedCommandConnection` (`recorded_commands.go:34-90`). The
/// command recorder's Redis FIFO is not ported; when `gitRecorder` is unset
/// the baseline also returns an empty connection.
pub struct EmptyRecordedCommandsConnectionObject;

impl ObjectValue for EmptyRecordedCommandsConnectionObject {
    fn type_name(&self) -> &'static str {
        "RecordedCommandConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("RecordedCommand"),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(0)))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(
                crate::graphql_resolvers::PageInfoObject::new(false, None),
            )))),
            _ => Ok(None),
        }
    }
}

/// An empty `AccessRequestConnection` (the requests store is not ported; a
/// fresh instance has no requests).
pub struct EmptyAccessRequestsConnectionObject;

impl ObjectValue for EmptyAccessRequestsConnectionObject {
    fn type_name(&self) -> &'static str {
        "AccessRequestConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(0)))),
            "nodes" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("AccessRequest"),
            })),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(
                crate::graphql_resolvers::PageInfoObject::new(false, None),
            )))),
            _ => Ok(None),
        }
    }
}

/// The site flags `SiteObject` replays.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SiteFlags {
    pub site_id: Option<String>,
    pub needs_repository_configuration: Option<bool>,
    pub free_users_exceeded: Option<bool>,
}

/// True when any of `names` is selected anywhere under `fields`.
pub fn selects(fields: &[sg_graphql::exec::RootField], names: &[&str]) -> bool {
    fields.iter().any(|field| {
        names.contains(&field.name.as_str())
            || selects(&field.selections, names)
            || field
                .type_conditions
                .iter()
                .any(|(_, nested)| selects(nested, names))
    })
}

/// `database.ExternalServiceKinds` entries with `CodeHost: true`
/// (`internal/database/external_services.go:205-224`).
const CODE_HOST_KINDS: &[&str] = &[
    "AWSCodeCommit",
    "AzureDevOps",
    "BitbucketCloud",
    "BitbucketServer",
    "Gerrit",
    "GitHub",
    "GitLab",
    "Gitolite",
    "GoModules",
    "JVMPackages",
    "NpmPackages",
    "OTHER",
    "Pagure",
    "Perforce",
    "Phabricator",
    "PythonPackages",
    "RustPackages",
    "RubyPackages",
];

/// Prefetch `Site.siteID`, `needsRepositoryConfiguration` and
/// `freeUsersExceeded` (`site.go:94`, `site_flags.go:15-75`).
pub async fn prefetch_site_flags(pool: &sqlx::PgPool) -> SiteFlags {
    let site_id = match std::env::var("TRACKING_APP_ID") {
        Ok(value) if !value.is_empty() => Some(value),
        _ => sg_db::global_state::get(pool)
            .await
            .ok()
            .map(|state| state.site_id)
            .filter(|value| !value.is_empty()),
    };
    let needs = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM external_services \
         WHERE deleted_at IS NULL AND kind = ANY($1)",
    )
    .bind(CODE_HOST_KINDS)
    .fetch_one(pool)
    .await
    .ok()
    .map(|count| count == 0);
    let users = sg_db::users::count_users(
        pool,
        &sg_db::users::UsersListOptions {
            exclude_sourcegraph_operators: true,
            ..Default::default()
        },
    )
    .await
    .ok();
    SiteFlags {
        site_id,
        needs_repository_configuration: needs,
        free_users_exceeded: users.map(|count| count >= 10),
    }
}

/// `internal/users.UsersStats` date-range filter
/// (`UsersStatsDateTimeRange`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DateRangeFilter {
    pub lte: Option<String>,
    pub gte: Option<String>,
    pub empty: bool,
    pub not: bool,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct NumberRangeFilter {
    pub gte: Option<f64>,
    pub lte: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct SiteUsersFilters {
    pub query: Option<String>,
    pub site_admin: Option<bool>,
    pub username: Option<String>,
    pub email: Option<String>,
    pub last_active_at: Option<DateRangeFilter>,
    pub deleted_at: Option<DateRangeFilter>,
    pub created_at: Option<DateRangeFilter>,
    pub events_count: Option<NumberRangeFilter>,
}

fn parse_date_range(value: Option<&sg_graphql::exec::InputValue>) -> Option<DateRangeFilter> {
    let sg_graphql::exec::InputValue::Object(fields) = value? else {
        return None;
    };
    let bool_field = |name: &str| {
        fields
            .get(name)
            .and_then(sg_graphql::exec::InputValue::as_bool)
            .unwrap_or(false)
    };
    Some(DateRangeFilter {
        lte: fields
            .get("lte")
            .and_then(sg_graphql::exec::InputValue::as_str)
            .map(str::to_owned),
        gte: fields
            .get("gte")
            .and_then(sg_graphql::exec::InputValue::as_str)
            .map(str::to_owned),
        empty: bool_field("empty"),
        not: bool_field("not"),
    })
}

fn parse_number_range(value: Option<&sg_graphql::exec::InputValue>) -> Option<NumberRangeFilter> {
    let sg_graphql::exec::InputValue::Object(fields) = value? else {
        return None;
    };
    Some(NumberRangeFilter {
        gte: fields
            .get("gte")
            .and_then(sg_graphql::exec::InputValue::as_f64),
        lte: fields
            .get("lte")
            .and_then(sg_graphql::exec::InputValue::as_f64),
    })
}

pub fn site_users_filters(arguments: &Arguments) -> SiteUsersFilters {
    SiteUsersFilters {
        query: arguments.string("query").map(str::to_owned),
        site_admin: arguments.bool("siteAdmin"),
        username: arguments.string("username").map(str::to_owned),
        email: arguments.string("email").map(str::to_owned),
        last_active_at: parse_date_range(arguments.get("lastActiveAt")),
        deleted_at: parse_date_range(arguments.get("deletedAt")),
        created_at: parse_date_range(arguments.get("createdAt")),
        events_count: parse_number_range(arguments.get("eventsCount")),
    }
}

/// `statsCTEQuery` (`internal/users/stats.go:150-172`).
const SITE_USERS_CTE: &str = "
WITH aggregated_stats AS (
    SELECT
        users.id AS id,
        users.username,
        users.display_name,
        emails.email AS primary_email,
        users.created_at,
        stats.user_last_active_at AS last_active_at,
        users.deleted_at,
        users.site_admin,
        (SELECT COUNT(user_id) FROM user_external_accounts WHERE user_id = users.id AND service_type = 'scim' AND deleted_at IS NULL) >= 1 AS scim_controlled,
        COALESCE(stats.user_events_count, 0) AS events_count
    FROM users
        LEFT JOIN aggregated_user_statistics stats ON stats.user_id = users.id
        LEFT JOIN user_emails emails ON emails.user_id = users.id AND emails.is_primary = true
)";

fn push_date_conds(
    builder: &mut sqlx::QueryBuilder<'_, sqlx::Postgres>,
    filter: &DateRangeFilter,
    column: &str,
) -> Result<(), String> {
    let parse = |value: &str| {
        time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
            .map_err(|error| error.to_string())
    };
    if filter.empty {
        builder.push(format!("{column} IS NULL"));
    } else {
        let mut first = true;
        if let Some(lte) = &filter.lte {
            builder.push(format!("{column} <= ")).push_bind(parse(lte)?);
            first = false;
        }
        if let Some(gte) = &filter.gte {
            if !first {
                builder.push(" AND ");
            }
            builder.push(format!("{column} >= ")).push_bind(parse(gte)?);
        }
    }
    Ok(())
}

fn push_site_users_where(
    builder: &mut sqlx::QueryBuilder<'_, sqlx::Postgres>,
    filters: &SiteUsersFilters,
) -> Result<(), String> {
    builder.push("TRUE");
    if let Some(query) = filters.query.as_deref().filter(|value| !value.is_empty()) {
        let pattern = format!("%{query}%");
        builder
            .push(" AND (username ILIKE ")
            .push_bind(pattern.clone())
            .push(" OR display_name ILIKE ")
            .push_bind(pattern.clone())
            .push(" OR primary_email ILIKE ")
            .push_bind(pattern)
            .push(")");
    }
    if let Some(site_admin) = filters.site_admin {
        builder.push(" AND site_admin = ").push_bind(site_admin);
    }
    if let Some(username) = &filters.username {
        builder
            .push(" AND username ILIKE ")
            .push_bind(format!("%{username}%"));
    }
    if let Some(email) = &filters.email {
        builder
            .push(" AND primary_email ILIKE ")
            .push_bind(format!("%{email}%"));
    }
    for (filter, column) in [
        (&filters.deleted_at, "deleted_at"),
        (&filters.last_active_at, "last_active_at"),
        (&filters.created_at, "created_at"),
    ] {
        let Some(filter) = filter else { continue };
        // The UI sends `{empty: null}` for unused date filters; such a filter
        // contributes no condition (Go's `toSQLConds` yields an empty slice).
        if !filter.empty && filter.lte.is_none() && filter.gte.is_none() {
            continue;
        }
        builder.push(" AND ");
        if filter.not {
            builder.push("NOT (");
        }
        push_date_conds(builder, filter, column)?;
        if filter.not {
            builder.push(")");
        }
    }
    if let Some(events) = &filters.events_count {
        if let Some(lte) = events.lte {
            builder.push(" AND events_count <= ").push_bind(lte);
        }
        if let Some(gte) = events.gte {
            builder.push(" AND events_count >= ").push_bind(gte);
        }
    }
    builder.push(
        " AND NOT EXISTS (SELECT FROM user_external_accounts WHERE service_type = 'sourcegraph-operator' AND user_id = aggregated_stats.id)",
    );
    Ok(())
}

pub async fn site_users_total_count(
    pool: &sqlx::PgPool,
    filters: &SiteUsersFilters,
) -> Result<f64, String> {
    let mut builder: sqlx::QueryBuilder<'_, sqlx::Postgres> =
        sqlx::QueryBuilder::new(SITE_USERS_CTE);
    builder.push(" SELECT COUNT(id)::float8 FROM aggregated_stats WHERE ");
    push_site_users_where(&mut builder, filters)?;
    builder
        .build_query_scalar::<f64>()
        .fetch_one(pool)
        .await
        .map_err(|error| error.to_string())
}

#[derive(Debug, Clone, PartialEq)]
pub struct SiteUserData {
    pub id: i32,
    pub username: String,
    pub display_name: Option<String>,
    pub primary_email: Option<String>,
    pub created_at: OffsetDateTime,
    pub last_active_at: Option<OffsetDateTime>,
    pub deleted_at: Option<OffsetDateTime>,
    pub site_admin: bool,
    pub scim_controlled: bool,
    pub events_count: f64,
}

pub async fn site_users_list(
    pool: &sqlx::PgPool,
    filters: &SiteUsersFilters,
    order_by: Option<&str>,
    descending: bool,
    limit: i64,
    offset: i64,
) -> Result<Vec<SiteUserData>, String> {
    let column = match order_by.unwrap_or("EVENTS_COUNT") {
        "USERNAME" => "username",
        "EMAIL" => "primary_email",
        "CREATED_AT" => "created_at",
        "LAST_ACTIVE_AT" => "last_active_at",
        "DELETED_AT" => "deleted_at",
        "EVENTS_COUNT" => "events_count",
        "SITE_ADMIN" => "site_admin",
        _ => "events_count",
    };
    let direction = if descending { "DESC" } else { "ASC" };
    let mut builder: sqlx::QueryBuilder<'_, sqlx::Postgres> =
        sqlx::QueryBuilder::new(SITE_USERS_CTE);
    builder.push(format!(
        " SELECT id, username, display_name, primary_email, created_at, last_active_at, deleted_at, site_admin, scim_controlled, events_count FROM aggregated_stats WHERE "
    ));
    push_site_users_where(&mut builder, filters)?;
    builder
        .push(format!(" ORDER BY {column} {direction} NULLS LAST LIMIT "))
        .push_bind(limit)
        .push(" OFFSET ")
        .push_bind(offset);
    let rows = builder
        .build()
        .fetch_all(pool)
        .await
        .map_err(|error| error.to_string())?;
    rows.into_iter()
        .map(|row| {
            use sqlx::Row;
            Ok(SiteUserData {
                id: row.try_get("id").map_err(|e| e.to_string())?,
                username: row.try_get("username").map_err(|e| e.to_string())?,
                display_name: row.try_get("display_name").ok(),
                primary_email: row.try_get("primary_email").ok(),
                created_at: row.try_get("created_at").map_err(|e| e.to_string())?,
                last_active_at: row.try_get("last_active_at").ok().flatten(),
                deleted_at: row.try_get("deleted_at").ok().flatten(),
                site_admin: row.try_get("site_admin").unwrap_or(false),
                scim_controlled: row.try_get("scim_controlled").unwrap_or(false),
                events_count: row.try_get("events_count").unwrap_or(0.0),
            })
        })
        .collect()
}

/// `Site.users` (`site_users.go:15-42`).
pub struct SiteUsersObject {
    pub pool: Option<sqlx::PgPool>,
    pub filters: SiteUsersFilters,
}

impl ObjectValue for SiteUsersObject {
    fn type_name(&self) -> &'static str {
        "SiteUsers"
    }

    fn field(
        &self,
        field: &str,
        arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let Some(pool) = &self.pool else {
            return Ok(None);
        };
        match field {
            "totalCount" => Ok(Some(FieldValue::Json(Json::Float(
                crate::codeintel_uploads_graphql_store::block_on(site_users_total_count(
                    pool,
                    &self.filters,
                ))
                .map_err(ExecError::new)?,
            )))),
            "nodes" => {
                let limit = arguments
                    .i64("limit")
                    .unwrap_or(100)
                    .clamp(0, 1000);
                let offset = arguments.i64("offset").unwrap_or(0).max(0);
                let order_by = arguments.string("orderBy").map(str::to_owned);
                let descending = arguments.bool("descending").unwrap_or(false);
                let users = crate::codeintel_uploads_graphql_store::block_on(site_users_list(
                    pool,
                    &self.filters,
                    order_by.as_deref(),
                    descending,
                    limit,
                    offset,
                ))
                .map_err(ExecError::new)?;
                Ok(Some(FieldValue::List {
                    items: users
                        .iter()
                        .map(|user| {
                            FieldValue::Object(Arc::new(SiteUserObject {
                                user: user.clone(),
                            }))
                        })
                        .collect(),
                    element_type: Some("SiteUser"),
                }))
            }
            _ => Ok(None),
        }
    }
}

/// A `PgPool` holder that keeps `SiteObject`'s `PartialEq` usable (the pool
/// itself does not implement `PartialEq`).
#[derive(Debug, Clone, Default)]
pub struct PoolHandle(pub Option<sqlx::PgPool>);

impl PartialEq for PoolHandle {
    fn eq(&self, other: &Self) -> bool {
        self.0.is_some() == other.0.is_some()
    }
}

/// `siteUserResolver` (`site_users.go:79-110`).
pub struct SiteUserObject {
    user: SiteUserData,
}

impl ObjectValue for SiteUserObject {
    fn type_name(&self) -> &'static str {
        "SiteUser"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let opt_str = |value: &Option<String>| match value {
            Some(value) => FieldValue::Json(Json::Str(value.clone())),
            None => FieldValue::Null,
        };
        let opt_date = |value: &Option<OffsetDateTime>| match value {
            Some(value) => FieldValue::Json(Json::Str(datetime(*value))),
            None => FieldValue::Null,
        };
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(
                crate::graphql_resolvers::UserObject::relay_id(self.user.id),
            )))),
            "username" => Ok(Some(FieldValue::Json(Json::Str(
                self.user.username.clone(),
            )))),
            "displayName" => Ok(Some(opt_str(&self.user.display_name))),
            "email" => Ok(Some(opt_str(&self.user.primary_email))),
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime(
                self.user.created_at,
            ))))),
            "lastActiveAt" => Ok(Some(opt_date(&self.user.last_active_at))),
            "deletedAt" => Ok(Some(opt_date(&self.user.deleted_at))),
            "siteAdmin" => Ok(Some(FieldValue::Json(Json::Bool(self.user.site_admin)))),
            "scimControlled" => Ok(Some(FieldValue::Json(Json::Bool(
                self.user.scim_controlled,
            )))),
            "eventsCount" => Ok(Some(FieldValue::Json(Json::Float(
                self.user.events_count,
            )))),
            // `siteUserResolver.Locked` reads the built-in lockout store
            // (`userpasswd` rate limiter); not wired here.
            "locked" => Ok(Some(FieldValue::Json(Json::Bool(false)))),
            _ => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// Admin analytics placeholders (shape-only; ENV_GAP)
// ---------------------------------------------------------------------------

struct SummaryObject;

impl ObjectValue for SummaryObject {
    fn type_name(&self) -> &'static str {
        "AnalyticsStatItemSummary"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "totalCount" | "totalUniqueUsers" | "totalRegisteredUsers" => {
                Ok(Some(FieldValue::Json(Json::Float(0.0))))
            }
            _ => Ok(None),
        }
    }
}

struct StatItemObject;

impl ObjectValue for StatItemObject {
    fn type_name(&self) -> &'static str {
        "AnalyticsStatItem"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "summary" => Ok(Some(FieldValue::Object(Arc::new(SummaryObject)))),
            "nodes" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("AnalyticsStatItemNode"),
            })),
            _ => Ok(None),
        }
    }
}

fn items(_fields: &[&'static str]) -> FieldValue {
    FieldValue::Object(Arc::new(StatItemObject))
}


struct AnalyticsSearchResult;

impl ObjectValue for AnalyticsSearchResult {
    fn type_name(&self) -> &'static str {
        "AnalyticsSearchResult"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "searches" | "resultClicks" | "fileViews" | "fileOpens" | "codeCopied" => {
                Ok(Some(items(&[])))
            }
            _ => Ok(None),
        }
    }
}

struct AnalyticsCodeIntelResult;

impl ObjectValue for AnalyticsCodeIntelResult {
    fn type_name(&self) -> &'static str {
        "AnalyticsCodeIntelResult"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "referenceClicks" | "definitionClicks" | "inAppEvents" | "codeHostEvents"
            | "searchBasedEvents" | "preciseEvents" | "crossRepoEvents" => Ok(Some(items(&[]))),
            _ => Ok(None),
        }
    }
}

struct AnalyticsNotebooksResults;

impl ObjectValue for AnalyticsNotebooksResults {
    fn type_name(&self) -> &'static str {
        "AnalyticsNotebooksResults"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "creations" | "views" | "blockRuns" => Ok(Some(items(&[]))),
            _ => Ok(None),
        }
    }
}

struct AnalyticsUsersResult;

impl ObjectValue for AnalyticsUsersResult {
    fn type_name(&self) -> &'static str {
        "AnalyticsUsersResult"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "activity" => Ok(Some(items(&[]))),
            "frequencies" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("AnalyticsUsersFrequencyItem"),
            })),
            "monthlyActiveUsers" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("AnalyticsMonthlyActiveUsers"),
            })),
            _ => Ok(None),
        }
    }
}

struct AnalyticsReposSummary;

impl ObjectValue for AnalyticsReposSummary {
    fn type_name(&self) -> &'static str {
        "AnalyticsReposSummartResult"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "count" | "preciseCodeIntelCount" => Ok(Some(FieldValue::Json(Json::Float(0.0)))),
            _ => Ok(None),
        }
    }
}

struct AnalyticsBatchChangesResult;

impl ObjectValue for AnalyticsBatchChangesResult {
    fn type_name(&self) -> &'static str {
        "AnalyticsBatchChangesResult"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "changesetsCreated" | "changesetsMerged" => Ok(Some(items(&[]))),
            _ => Ok(None),
        }
    }
}

struct AnalyticsExtensionsResult;

impl ObjectValue for AnalyticsExtensionsResult {
    fn type_name(&self) -> &'static str {
        "AnalyticsExtensionsResult"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "jetbrains" | "vscode" | "browser" => Ok(Some(items(&[]))),
            _ => Ok(None),
        }
    }
}

struct AnalyticsCodeInsightsResult;

impl ObjectValue for AnalyticsCodeInsightsResult {
    fn type_name(&self) -> &'static str {
        "AnalyticsCodesInsightsResult"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "insightHovers" | "insightDataPointClicks" => Ok(Some(items(&[]))),
            _ => Ok(None),
        }
    }
}

/// `Site.analytics` (`site_analytics.go:19-30`): site-admin gated; the
/// aggregates are placeholders until `internal/adminanalytics` is ported.
pub struct AnalyticsObject;

impl ObjectValue for AnalyticsObject {
    fn type_name(&self) -> &'static str {
        "Analytics"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "search" => Ok(Some(FieldValue::Object(Arc::new(AnalyticsSearchResult)))),
            "notebooks" => Ok(Some(FieldValue::Object(Arc::new(AnalyticsNotebooksResults)))),
            "users" => Ok(Some(FieldValue::Object(Arc::new(AnalyticsUsersResult)))),
            "codeIntel" => Ok(Some(FieldValue::Object(Arc::new(
                AnalyticsCodeIntelResult,
            )))),
            "codeIntelByLanguage" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("AnalyticsCodeIntelByLanguageResult"),
            })),
            "codeIntelTopRepositories" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("AnalyticsCodeIntelRepositoryResult"),
            })),
            "repos" => Ok(Some(FieldValue::Object(Arc::new(AnalyticsReposSummary)))),
            "batchChanges" => Ok(Some(FieldValue::Object(Arc::new(
                AnalyticsBatchChangesResult,
            )))),
            "extensions" => Ok(Some(FieldValue::Object(Arc::new(
                AnalyticsExtensionsResult,
            )))),
            "codeInsights" => Ok(Some(FieldValue::Object(Arc::new(
                AnalyticsCodeInsightsResult,
            )))),
            _ => Ok(None),
        }
    }
}
