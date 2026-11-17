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

/// `Site.users` (`site_users.go:15-42`): `totalCount` (Float in the schema);
/// the node list is empty until `internal/users.UsersStats` is ported (the
/// admin overview only reads the count).
pub struct SiteUsersObject {
    pub total_count: i64,
}

impl ObjectValue for SiteUsersObject {
    fn type_name(&self) -> &'static str {
        "SiteUsers"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "totalCount" => Ok(Some(FieldValue::Json(Json::Float(
                self.total_count as f64,
            )))),
            "nodes" => Ok(Some(FieldValue::List {
                items: Vec::new(),
                element_type: Some("SiteUser"),
            })),
            _ => Ok(None),
        }
    }
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

    // `needsRepositoryConfiguration`: no code-host external services exist.
    let needs = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM external_services \
         WHERE deleted_at IS NULL AND kind = ANY($1)",
    )
    .bind(CODE_HOST_KINDS)
    .fetch_one(pool)
    .await
    .ok()
    .map(|count| count == 0);

    // `freeUsersExceeded`: the free plan warns from 10 users
    // (`licensing/user_count.go:68`).
    let users = sg_db::users::count_users(
        pool,
        &sg_db::users::UsersListOptions {
            exclude_sourcegraph_operators: true,
            ..Default::default()
        },
    )
    .await
    .ok();
    let free_exceeded = users.map(|count| count >= 10);

    SiteFlags {
        site_id,
        needs_repository_configuration: needs,
        free_users_exceeded: free_exceeded,
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
