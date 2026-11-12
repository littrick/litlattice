#![forbid(unsafe_code)]

//! `Query.packageRepoFilters` (`cmd/frontend/graphqlbackend/package_repo_filters.go:215-250`,
//! `internal/codeintel/dependencies/internal/store/store.go:475-534`).
//!
//! `package_repo_references` (`Query.packageRepoReferences`,
//! `packageRepoReferencesMatchingFilter`) and the package-filter mutations are
//! **not ported** here: they need the dependencies service's join/repo-name
//! derivation and the inbound-sync semantics that are out of this slice.
//! This module only reads the persisted filters, which is a plain table scan.

use std::collections::HashMap;
use std::sync::Arc;

use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue, RootField};
use sg_graphql::json::Json;
use sqlx::{PgPool, Row};

/// `PackageRepoFilter` (`internal/codeintel/dependencies/shared/types.go:58-71`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageRepoFilterData {
    pub id: i32,
    pub behaviour: String,
    pub package_scheme: String,
    pub name_filter: Option<String>,
    pub version_filter: Option<(String, String)>,
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

/// `Query.packageRepoFilters` returns a nullable list; an unknown `kind`
/// argument maps to the empty scheme and matches nothing (the baseline does
/// not validate it for this field).
pub async fn prefetch_package_repo_filters(
    pool: &PgPool,
    fields: &[RootField],
) -> HashMap<String, Result<Vec<PackageRepoFilterData>, String>> {
    let mut lookups = HashMap::new();
    for field in fields {
        if field.name != "packageRepoFilters" {
            continue;
        }
        let key = format!(
            "{:?}:{:?}",
            field.arguments.string("behaviour"),
            field.arguments.string("kind")
        );
        if lookups.contains_key(&key) {
            continue;
        }
        let behaviour = field.arguments.string("behaviour");
        // An unknown `kind` maps to the empty scheme, which the baseline
        // treats as "no scheme filter" (`opts.PackageScheme == ""`).
        let scheme = field
            .arguments
            .string("kind")
            .and_then(external_service_to_scheme);
        let lookup = list_filters(pool, behaviour, scheme).await;
        lookups.insert(key, lookup);
    }
    lookups
}

async fn list_filters(
    pool: &PgPool,
    behaviour: Option<&str>,
    scheme: Option<&str>,
) -> Result<Vec<PackageRepoFilterData>, String> {
    let rows = sqlx::query(
        "SELECT id, behaviour, scheme, matcher FROM package_repo_filters \
         WHERE deleted_at IS NULL \
           AND ($1::text IS NULL OR behaviour = $1) \
           AND ($2::text IS NULL OR scheme = $2) \
         ORDER BY id",
    )
    .bind(behaviour)
    .bind(scheme)
    .fetch_all(pool)
    .await
    .map_err(|error| error.to_string())?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let matcher: serde_json::Value = row.get("matcher");
            let name_filter = matcher
                .get("PackageGlob")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            let version_filter = match (
                matcher
                    .get("PackageName")
                    .and_then(serde_json::Value::as_str),
                matcher
                    .get("VersionGlob")
                    .and_then(serde_json::Value::as_str),
            ) {
                (Some(package), Some(version)) => Some((package.to_owned(), version.to_owned())),
                _ => None,
            };
            PackageRepoFilterData {
                id: row.get("id"),
                behaviour: row.get("behaviour"),
                package_scheme: row.get("scheme"),
                name_filter,
                version_filter,
            }
        })
        .collect())
}

pub struct PackageRepoFilterObject {
    pub filter: PackageRepoFilterData,
}

impl ObjectValue for PackageRepoFilterObject {
    fn type_name(&self) -> &'static str {
        "PackageFilter"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_relay_id(
                "PackageRepoFilter",
                self.filter.id,
            ))))),
            "behaviour" => Ok(Some(FieldValue::Json(Json::Str(
                self.filter.behaviour.clone(),
            )))),
            "kind" => Ok(Some(FieldValue::Json(Json::Str(
                scheme_to_external_service(&self.filter.package_scheme).to_owned(),
            )))),
            "nameFilter" => Ok(Some(match &self.filter.name_filter {
                Some(glob) => FieldValue::Object(Arc::new(PackageNameFilterObject {
                    package_glob: glob.clone(),
                })),
                None => FieldValue::Null,
            })),
            "versionFilter" => Ok(Some(match &self.filter.version_filter {
                Some((package, version)) => {
                    FieldValue::Object(Arc::new(PackageVersionFilterObject {
                        package_name: package.clone(),
                        version_glob: version.clone(),
                    }))
                }
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}

pub struct PackageNameFilterObject {
    pub package_glob: String,
}

impl ObjectValue for PackageNameFilterObject {
    fn type_name(&self) -> &'static str {
        "PackageNameFilter"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "packageGlob" => Ok(Some(FieldValue::Json(Json::Str(self.package_glob.clone())))),
            _ => Ok(None),
        }
    }
}

pub struct PackageVersionFilterObject {
    pub package_name: String,
    pub version_glob: String,
}

impl ObjectValue for PackageVersionFilterObject {
    fn type_name(&self) -> &'static str {
        "PackageVersionFilter"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "packageName" => Ok(Some(FieldValue::Json(Json::Str(self.package_name.clone())))),
            "versionGlob" => Ok(Some(FieldValue::Json(Json::Str(self.version_glob.clone())))),
            _ => Ok(None),
        }
    }
}

fn marshal_relay_id(kind: &str, id: i32) -> String {
    use base64::Engine as _;
    let spec = serde_json::to_string(&id).expect("int json");
    base64::engine::general_purpose::URL_SAFE.encode(format!("{kind}:{spec}"))
}
