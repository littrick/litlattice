#![forbid(unsafe_code)]

//! `Query.outOfBandMigrations` and `Mutation.setMigrationDirection`
//! (`cmd/frontend/graphqlbackend/oobmigrations.go:38-168`,
//! `internal/oobmigration/{store,version}.go`).
//!
//! Storage stays in `sg-db::out_of_band_migrations` (the same read helpers the
//! worker uses). `List()` only filters to ids present in the embedded YAML
//! metadata (`internal/oobmigration/store.go:436-455`); it never writes.
//! `Version.String()` is `major.minor`.

use std::sync::Arc;

use sg_db::out_of_band_migrations::{self, Migration};
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue, RootField};
use sg_graphql::json::Json;
use sqlx::PgPool;

use crate::admin_store::check_current_user_is_site_admin;
use crate::graphql_resolvers::datetime_rfc3339;

/// The GraphQL-facing copy of a migration row (plus its joined errors).
#[derive(Debug, Clone, PartialEq)]
pub struct MigrationData {
    pub id: i32,
    pub team: String,
    pub component: String,
    pub description: String,
    pub introduced_major: i32,
    pub introduced_minor: i32,
    pub deprecated_major: Option<i32>,
    pub deprecated_minor: Option<i32>,
    pub progress: f64,
    pub created: time::OffsetDateTime,
    pub last_updated: Option<time::OffsetDateTime>,
    pub non_destructive: bool,
    pub apply_reverse: bool,
    pub errors: Vec<MigrationErrorData>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationErrorData {
    pub message: String,
    pub created: time::OffsetDateTime,
}

impl From<&Migration> for MigrationData {
    fn from(migration: &Migration) -> Self {
        Self {
            id: migration.id,
            team: migration.team.clone(),
            component: migration.component.clone(),
            description: migration.description.clone(),
            introduced_major: migration.introduced_major,
            introduced_minor: migration.introduced_minor,
            deprecated_major: migration.deprecated_major,
            deprecated_minor: migration.deprecated_minor,
            progress: migration.progress,
            created: migration.created,
            last_updated: migration.last_updated,
            non_destructive: migration.non_destructive,
            apply_reverse: migration.apply_reverse,
            errors: migration
                .errors
                .iter()
                .map(|error| MigrationErrorData {
                    message: error.message.clone(),
                    created: error.created,
                })
                .collect(),
        }
    }
}

/// `Version.String()` (`internal/oobmigration/version.go:73-75`).
fn version_string(major: i32, minor: i32) -> String {
    format!("{major}.{minor}")
}

/// `upgradestore.GetFirstServiceVersion` (`internal/version/upgradestore/store.go:47-54`).
async fn first_service_version(pool: &PgPool) -> Result<Option<(i32, i32)>, String> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT first_version FROM versions WHERE service = 'frontend'")
            .fetch_optional(pool)
            .await
            .map_err(|error| error.to_string())?;
    match row {
        // `oobmigrations.go:56-58`: a present but unparseable first version is
        // an error, not "no first version".
        Some((version,)) => match parse_version(&version) {
            Some(parsed) => Ok(Some(parsed)),
            None => Err(format!("invalid first version {version:?}")),
        },
        None => Ok(None),
    }
}

/// `oobmigration.NewVersionFromString`: `v?major.minor[.patch][-suffix][+meta]`.
fn parse_version(raw: &str) -> Option<(i32, i32)> {
    let raw = raw.strip_suffix("+dev").unwrap_or(raw);
    let rest = raw.strip_prefix('v').unwrap_or(raw);
    let core = rest.split(['-', '+']).next().unwrap_or(rest);
    let mut parts = core.split('.');
    let major = parts.next()?.parse::<i32>().ok()?;
    let minor = parts.next()?.parse::<i32>().ok()?;
    Some((major, minor))
}

/// `oobmigration.CompareVersions`: `0` before, `1` equal, `2` after.
fn compare_versions(left: (i32, i32), right: (i32, i32)) -> i32 {
    if left.0 != right.0 {
        if left.0 < right.0 {
            return 0;
        }
        return 2;
    }
    if left.1 != right.1 {
        if left.1 < right.1 {
            return 0;
        }
        return 2;
    }
    1
}

/// Prefetch `Query.outOfBandMigrations` when the operation selected it.
pub async fn prefetch_out_of_band_migrations(
    pool: &PgPool,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> Option<Result<Vec<MigrationData>, String>> {
    let field = fields
        .iter()
        .find(|field| field.name == "outOfBandMigrations")?;
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return Some(Err(message));
    }
    let exclude = field
        .arguments
        .bool("ExcludeDeprecatedBeforeFirstVersion")
        .unwrap_or(false);
    let migrations = match out_of_band_migrations::list(pool).await {
        Ok(migrations) => migrations,
        Err(message) => return Some(Err(message)),
    };
    let mut data: Vec<MigrationData> = migrations.iter().map(MigrationData::from).collect();
    if exclude {
        let first = match first_service_version(pool).await {
            Ok(Some(first)) => first,
            Ok(None) => return Some(Ok(data)),
            Err(message) => return Some(Err(message)),
        };
        data.retain(
            |migration| match (migration.deprecated_major, migration.deprecated_minor) {
                (Some(major), Some(minor)) => compare_versions(first, (major, minor)) != 2,
                _ => true,
            },
        );
    }
    Some(Ok(data))
}

/// `Mutation.setMigrationDirection` (`oobmigrations.go:88-107`).
pub async fn prefetch_set_migration_direction(
    pool: &PgPool,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> Option<Result<(), String>> {
    let field = fields
        .iter()
        .find(|field| field.name == "setMigrationDirection")?;
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        return Some(Err(message));
    }
    let Some(id) = field.arguments.string("id") else {
        return Some(Err("invalid graphql.ID".to_owned()));
    };
    let id = match crate::graphql_resolvers::unmarshal_relay_id(id) {
        Ok(id) => id,
        Err(message) => return Some(Err(message)),
    };
    let apply_reverse = field.arguments.bool("applyReverse").unwrap_or(false);
    Some(
        out_of_band_migrations::update_direction(pool, id, apply_reverse)
            .await
            .map(|_| ()),
    )
}

pub struct OutOfBandMigrationObject {
    pub migration: MigrationData,
}

fn optional_datetime(value: Option<time::OffsetDateTime>) -> FieldValue {
    match value {
        Some(value) => FieldValue::Json(Json::Str(datetime_rfc3339(value))),
        None => FieldValue::Null,
    }
}

impl ObjectValue for OutOfBandMigrationObject {
    fn type_name(&self) -> &'static str {
        "OutOfBandMigration"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_relay_id(
                "OutOfBandMigration",
                self.migration.id,
            ))))),
            "team" => Ok(Some(FieldValue::Json(Json::Str(
                self.migration.team.clone(),
            )))),
            "component" => Ok(Some(FieldValue::Json(Json::Str(
                self.migration.component.clone(),
            )))),
            "description" => Ok(Some(FieldValue::Json(Json::Str(
                self.migration.description.clone(),
            )))),
            "introduced" => Ok(Some(FieldValue::Json(Json::Str(version_string(
                self.migration.introduced_major,
                self.migration.introduced_minor,
            ))))),
            "deprecated" => Ok(Some(
                match (
                    self.migration.deprecated_major,
                    self.migration.deprecated_minor,
                ) {
                    (Some(major), Some(minor)) => {
                        FieldValue::Json(Json::Str(version_string(major, minor)))
                    }
                    _ => FieldValue::Null,
                },
            )),
            "progress" => Ok(Some(FieldValue::Json(Json::Float(self.migration.progress)))),
            "created" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.migration.created,
            ))))),
            "lastUpdated" => Ok(Some(optional_datetime(self.migration.last_updated))),
            "nonDestructive" => Ok(Some(FieldValue::Json(Json::Bool(
                self.migration.non_destructive,
            )))),
            "applyReverse" => Ok(Some(FieldValue::Json(Json::Bool(
                self.migration.apply_reverse,
            )))),
            "errors" => Ok(Some(FieldValue::List {
                element_type: Some("OutOfBandMigrationError"),
                items: self
                    .migration
                    .errors
                    .iter()
                    .cloned()
                    .map(|error| {
                        FieldValue::Object(Arc::new(OutOfBandMigrationErrorObject { error }))
                    })
                    .collect(),
            })),
            _ => Ok(None),
        }
    }
}

pub struct OutOfBandMigrationErrorObject {
    pub error: MigrationErrorData,
}

impl ObjectValue for OutOfBandMigrationErrorObject {
    fn type_name(&self) -> &'static str {
        "OutOfBandMigrationError"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "message" => Ok(Some(FieldValue::Json(Json::Str(
                self.error.message.clone(),
            )))),
            "created" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.error.created,
            ))))),
            _ => Ok(None),
        }
    }
}

/// `relay.MarshalID` for the int32 migration id.
fn marshal_relay_id(kind: &str, id: i32) -> String {
    use base64::Engine as _;
    let spec = serde_json::to_string(&id).expect("int json");
    base64::engine::general_purpose::URL_SAFE.encode(format!("{kind}:{spec}"))
}
