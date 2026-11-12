#![forbid(unsafe_code)]

//! `Query.phabricatorRepo` (`cmd/frontend/graphqlbackend/graphqlbackend.go:1003`,
//! `phabricator.go`, `internal/database/phabricator.go:GetByName`).

use std::collections::HashMap;

use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue, RootField};
use sg_graphql::json::Json;
use sqlx::{PgPool, Row};

/// One `phabricator_repos` row (`types.PhabricatorRepo`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhabricatorRepoData {
    pub callsign: String,
    pub name: String,
    pub url: String,
}

/// `GetByName` returns `errPhabricatorRepoNotFound` text.
pub const NOT_FOUND_PREFIX: &str = "phabricator repo not found";

pub async fn prefetch_phabricator_repos(
    pool: &PgPool,
    fields: &[RootField],
) -> HashMap<String, Result<PhabricatorRepoData, String>> {
    let mut lookups = HashMap::new();
    for field in fields {
        if field.name != "phabricatorRepo" {
            continue;
        }
        // `args.Name` aliases `args.URI`; the SQL fallback compares the exact
        // `repo_name` and requires exactly one row (`getOneBySQL`). The
        // external-service config branch is not consulted because Rust does
        // not parse `phabricator` service configs in this slice; fixtures have
        // no phabricator services so both sides take the SQL branch.
        let name = field
            .arguments
            .string("name")
            .or_else(|| field.arguments.string("uri"));
        let Some(name) = name else {
            continue;
        };
        if lookups.contains_key(name) {
            continue;
        }
        let lookup = match sqlx::query(
            "SELECT callsign, repo_name, url FROM phabricator_repos WHERE repo_name = $1",
        )
        .bind(name)
        .fetch_all(pool)
        .await
        {
            Ok(rows) if rows.len() == 1 => {
                let row = &rows[0];
                Ok(PhabricatorRepoData {
                    callsign: row.get("callsign"),
                    name: row.get("repo_name"),
                    url: row.get("url"),
                })
            }
            Ok(_) => Err(format!("{NOT_FOUND_PREFIX}: name={name}")),
            Err(error) => Err(error.to_string()),
        };
        lookups.insert(name.to_owned(), lookup);
    }
    lookups
}

pub struct PhabricatorRepoObject {
    pub repo: PhabricatorRepoData,
}

impl ObjectValue for PhabricatorRepoObject {
    fn type_name(&self) -> &'static str {
        "PhabricatorRepo"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.repo.name.clone())))),
            "uri" => Ok(Some(FieldValue::Json(Json::Str(self.repo.name.clone())))),
            "callsign" => Ok(Some(FieldValue::Json(Json::Str(
                self.repo.callsign.clone(),
            )))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(self.repo.url.clone())))),
            _ => Ok(None),
        }
    }
}
