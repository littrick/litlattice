#![forbid(unsafe_code)]

//! Code Intelligence configuration-policy GraphQL surface (M9.28c).
//!
//! Ports `internal/codeintel/policies/transport/graphql`:
//!
//! - `Query.codeIntelligenceConfigurationPolicies` and `Query.node`'s
//!   `CodeIntelligenceConfigurationPolicy` kind
//!   (`root_resolver_policy_queries.go`);
//! - `create`/`update`/`deleteCodeIntelligenceConfigurationPolicy`
//!   (`root_resolver_policy_mutations.go`), including the site-admin gate and
//!   the verbatim validation messages;
//! - `Query.previewRepositoryFilter` and
//!   `Repository.previewGitObjectFilter` (`root_resolver_previews.go`);
//! - the policy/preview object resolvers (`configuration_policy_resolver.go`).
//!
//! The SQL lives in [`sg_db::codeintel_policies`]; everything asynchronous is
//! bridged by the store adapter behind [`CodeIntelPoliciesDatabase`]. Git
//! objects are enumerated through the same seam, because refs/commits live in
//! gitserver rather than Postgres (see
//! `dev/rs/docs/overviews/m9_28c_codeintel_policies_overview.md`).

use std::sync::Arc;

use base64::Engine as _;
use sg_db::codeintel_policies::{
    ConfigurationPolicy, ConfigurationPolicyPatch, NewConfigurationPolicy, RepositoryRow,
};
use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue};
use sg_graphql::Json;
use time::OffsetDateTime;

pub type CodeIntelPoliciesHandle = Arc<dyn CodeIntelPoliciesDatabase>;

/// A git object returned by `Repository.previewGitObjectFilter`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitObjectRow {
    pub name: String,
    pub rev: String,
    pub committed_at: OffsetDateTime,
}

/// The database seam of this module. The production implementation is
/// [`crate::codeintel_policies_graphql_store::PgCodeIntelPoliciesStore`].
pub trait CodeIntelPoliciesDatabase: Send + Sync + std::fmt::Debug {
    fn get_configuration_policies(
        &self,
        opts: &sg_db::codeintel_policies::GetConfigurationPoliciesOptions,
    ) -> Result<(Vec<ConfigurationPolicy>, i64), String>;
    fn get_configuration_policy_by_id(
        &self,
        id: i32,
        actor: Actor,
    ) -> Result<Option<ConfigurationPolicy>, String>;
    fn create_configuration_policy(
        &self,
        policy: &NewConfigurationPolicy,
    ) -> Result<ConfigurationPolicy, String>;
    fn update_configuration_policy(&self, patch: &ConfigurationPolicyPatch) -> Result<(), String>;
    fn delete_configuration_policy_by_id(&self, id: i32) -> Result<(), String>;
    fn update_repos_matching_patterns(
        &self,
        patterns: &[String],
        policy_id: i32,
        repository_match_limit: Option<i64>,
    ) -> Result<(), String>;
    fn get_repo_ids_by_glob_patterns(
        &self,
        patterns: &[String],
        limit: i64,
        offset: i64,
        actor: Actor,
    ) -> Result<(Vec<i32>, i64), String>;
    fn repo_count(&self) -> Result<i64, String>;
    fn get_repository(&self, id: i32) -> Result<RepositoryRow, String>;
    fn get_repository_names(&self, ids: &[i32]) -> Result<Vec<RepositoryRow>, String>;
    /// All git objects described by a synthetic `{type, pattern}` policy,
    /// before the service's sort/count/truncate steps.
    fn git_objects_described_by_policy(
        &self,
        repository_id: i32,
        repo_name: &str,
        policy_type: &str,
        pattern: &str,
    ) -> Result<Vec<GitObjectRow>, String>;
    /// `auth.CheckCurrentUserIsSiteAdmin`: `Ok(true)` for internal actors,
    /// otherwise the verbatim `not authenticated` / `must be site admin`
    /// errors.
    fn current_user_site_admin(&self, actor: Actor) -> Result<bool, String>;
    /// `conf.CodeIntelAutoIndexingPolicyRepositoryMatchLimit()`; `None` is the
    /// unconfigured (`-1`) value.
    fn repository_match_limit(&self) -> Option<i64> {
        None
    }
}

//
// Relay IDs / argument helpers.
//

/// `resolverstubs.MarshalID("CodeIntelligenceConfigurationPolicy", id)`.
pub fn marshal_configuration_policy_id(id: i32) -> String {
    base64::engine::general_purpose::URL_SAFE.encode(format!(
        "CodeIntelligenceConfigurationPolicy:{id}"
    ))
}

/// `resolverstubs.UnmarshalID[int]`: decode, skip the kind prefix and parse
/// the remainder as an int.
fn unmarshal_policy_id(id: &str) -> Result<i32, String> {
    crate::graphql_resolvers::unmarshal_relay_id(id)
}

fn string_list(arguments: &Arguments, name: &str) -> Option<Vec<String>> {
    match arguments.get(name) {
        Some(InputValue::List(items)) => Some(
            items
                .iter()
                .filter_map(|item| match item {
                    InputValue::Str(value) => Some(value.clone()),
                    _ => None,
                })
                .collect(),
        ),
        Some(InputValue::Null) | None => None,
        Some(InputValue::Str(value)) => Some(vec![value.clone()]),
        _ => None,
    }
}

fn int_arg(arguments: &Arguments, name: &str) -> Option<i32> {
    arguments.i64(name).map(|value| value as i32)
}

/// `PagedConnectionArgs.ParseLimitOffset`: `after` is a decimal offset.
fn parse_limit_offset(arguments: &Arguments, default_limit: i32) -> Result<(i32, i32), String> {
    let offset = match arguments.string("after") {
        None => 0,
        Some(after) => go_parse_int32(after)?,
    };
    let limit = int_arg(arguments, "first").unwrap_or(default_limit);
    Ok((limit, offset))
}

/// `strconv.ParseInt` error text, reproduced for the `after` cursor.
fn go_parse_int32(value: &str) -> Result<i32, String> {
    match value.parse::<i32>() {
        Ok(parsed) => Ok(parsed),
        Err(_) => {
            let reason = if is_integer_syntax(value) {
                "value out of range"
            } else {
                "invalid syntax"
            };
            Err(format!("strconv.ParseInt: parsing {value:?}: {reason}"))
        }
    }
}

fn is_integer_syntax(value: &str) -> bool {
    let digits = value.strip_prefix(['+', '-']).unwrap_or(value);
    !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
}

const DEFAULT_CONFIGURATION_POLICY_PAGE_SIZE: i32 = 50;
const DEFAULT_REPOSITORY_FILTER_PREVIEW_PAGE_SIZE: i32 = 15;
const DEFAULT_GIT_OBJECT_FILTER_PREVIEW_PAGE_SIZE: i32 = 15;
const MAX_DURATION_HOURS: i32 = 87600; // 10 years

//
// Root/mutation/node dispatch.
//

pub fn is_root_field(field: &str) -> bool {
    matches!(
        field,
        "codeIntelligenceConfigurationPolicies" | "previewRepositoryFilter"
    )
}

pub fn root_field(
    database: &CodeIntelPoliciesHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "codeIntelligenceConfigurationPolicies" => Ok(Some(
            code_intelligence_configuration_policies(database, arguments, actor)?,
        )),
        "previewRepositoryFilter" => {
            Ok(Some(preview_repository_filter(database, arguments, actor)?))
        }
        _ => Ok(None),
    }
}

pub fn is_mutation_field(field: &str) -> bool {
    matches!(
        field,
        "createCodeIntelligenceConfigurationPolicy"
            | "updateCodeIntelligenceConfigurationPolicy"
            | "deleteCodeIntelligenceConfigurationPolicy"
    )
}

pub fn mutation_field(
    database: &CodeIntelPoliciesHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "createCodeIntelligenceConfigurationPolicy" => Ok(Some(create_configuration_policy(
            database, arguments, actor,
        )?)),
        "updateCodeIntelligenceConfigurationPolicy" => {
            update_configuration_policy(database, arguments, actor)?;
            Ok(Some(FieldValue::Object(Arc::new(
                crate::temporary_settings::EmptyResponseObject,
            ))))
        }
        "deleteCodeIntelligenceConfigurationPolicy" => {
            delete_configuration_policy(database, arguments, actor)?;
            Ok(Some(FieldValue::Object(Arc::new(
                crate::temporary_settings::EmptyResponseObject,
            ))))
        }
        _ => Ok(None),
    }
}

pub fn is_node_kind(kind: &str) -> bool {
    kind == "CodeIntelligenceConfigurationPolicy"
}

pub fn node_field(
    database: &CodeIntelPoliciesHandle,
    id: &str,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    let policy_id = unmarshal_policy_id(id).map_err(ExecError::new)?;
    match database
        .get_configuration_policy_by_id(policy_id, actor)
        .map_err(ExecError::new)?
    {
        Some(policy) => Ok(Some(FieldValue::Object(Arc::new(
            ConfigurationPolicyObject {
                policy,
                database: database.clone(),
            },
        )))),
        None => Ok(Some(FieldValue::Null)),
    }
}

//
// Query resolvers.
//

/// `rootResolver.CodeIntelligenceConfigurationPolicies`.
pub fn code_intelligence_configuration_policies(
    database: &CodeIntelPoliciesHandle,
    arguments: &Arguments,
    _actor: Actor,
) -> Result<FieldValue, ExecError> {
    let (limit, offset) = parse_limit_offset(arguments, DEFAULT_CONFIGURATION_POLICY_PAGE_SIZE)
        .map_err(ExecError::new)?;

    let mut opts = sg_db::codeintel_policies::GetConfigurationPoliciesOptions {
        limit: i64::from(limit),
        offset: i64::from(offset),
        ..Default::default()
    };
    if let Some(repository) = arguments.string("repository") {
        // `UnmarshalID[int64]`: the parsed value is narrowed to `int`.
        let id = crate::graphql_resolvers::unmarshal_relay_id(repository)
            .map_err(ExecError::new)?;
        opts.repository_id = id;
    }
    if let Some(query) = arguments.string("query") {
        opts.term = query.to_owned();
    }
    opts.protected = arguments.bool("protected");
    opts.for_data_retention = arguments.bool("forDataRetention");
    opts.for_precise_indexing = arguments.bool("forPreciseIndexing");
    // NOTE: `forSyntacticIndexing` is declared in the schema but the baseline
    // resolver never copies it into the store options.
    opts.for_embeddings = arguments.bool("forEmbeddings");

    let (policies, total_count) = database
        .get_configuration_policies(&opts)
        .map_err(ExecError::new)?;

    Ok(FieldValue::Object(Arc::new(
        ConfigurationPolicyConnectionObject {
            policies,
            total_count: total_count as i32,
            database: database.clone(),
        },
    )))
}

/// `rootResolver.PreviewRepositoryFilter`.
pub fn preview_repository_filter(
    database: &CodeIntelPoliciesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    let mut page_size =
        int_arg(arguments, "first").unwrap_or(DEFAULT_REPOSITORY_FILTER_PREVIEW_PAGE_SIZE);

    let patterns = string_list(arguments, "patterns").unwrap_or_default();

    // `Service.GetPreviewRepositoryFilter`.
    let repository_match_limit = database.repository_match_limit();
    if let Some(limit) = repository_match_limit {
        if i64::from(page_size) > limit {
            page_size = limit as i32;
        }
    }

    let (ids, total_matches) = database
        .get_repo_ids_by_glob_patterns(&patterns, i64::from(page_size), 0, actor)
        .map_err(ExecError::new)?;
    let total_matches = total_matches as i32;
    let total_repo_count = database.repo_count().map_err(ExecError::new)?;
    let matches_all = i64::from(total_matches) == total_repo_count;

    let rows = database
        .get_repository_names(&ids)
        .map_err(ExecError::new)?;

    let limited_count = match repository_match_limit {
        Some(limit) if limit < i64::from(total_matches) => limit as i32,
        _ => total_matches,
    };

    Ok(FieldValue::Object(Arc::new(
        RepositoryFilterPreviewObject {
            nodes: rows,
            total_count: limited_count,
            total_matches,
            matches_all,
            limit: repository_match_limit.map(|limit| limit as i32),
        },
    )))
}

/// `Repository.previewGitObjectFilter`: `repository_id` has already been
/// unmarshalled from the Relay ID by the caller.
pub fn preview_git_object_filter(
    database: &CodeIntelPoliciesHandle,
    repository_id: i32,
    arguments: &Arguments,
) -> Result<FieldValue, ExecError> {
    let policy_type = arguments
        .string("type")
        .ok_or_else(|| ExecError::new("missing type argument"))?
        .to_owned();
    let pattern = arguments
        .string("pattern")
        .ok_or_else(|| ExecError::new("missing pattern argument"))?
        .to_owned();
    // `args.Limit(DefaultGitObjectFilterPreviewPageSize)`.
    let limit =
        int_arg(arguments, "first").unwrap_or(DEFAULT_GIT_OBJECT_FILTER_PREVIEW_PAGE_SIZE);
    let threshold = int_arg(arguments, "countObjectsYoungerThanHours");

    // `Service.GetPreviewGitObjectFilter`: `repoStore.Get` first, which
    // surfaces `repo not found` / `blocked` verbatim.
    let repo = database
        .get_repository(repository_id)
        .map_err(ExecError::new)?;

    let mut objects = database
        .git_objects_described_by_policy(repository_id, &repo.name, &policy_type, &pattern)
        .map_err(ExecError::new)?;

    // Go sorts with `sort.Slice`; the comparator is deterministic unless two
    // entries share both name and rev, so a total order under the comparator
    // is enough here.
    objects.sort_by(|a, b| {
        if threshold.is_some() && a.committed_at != b.committed_at {
            return b.committed_at.cmp(&a.committed_at);
        }
        a.name.cmp(&b.name).then_with(|| a.rev.cmp(&b.rev))
    });

    let total_younger = threshold.map(|hours| {
        let now = OffsetDateTime::now_utc();
        objects
            .iter()
            .filter(|object| now - object.committed_at <= time::Duration::hours(i64::from(hours)))
            .count() as i32
    });

    let total_count = objects.len() as i32;
    if (limit as usize) < objects.len() {
        objects.truncate(limit as usize);
    }

    Ok(FieldValue::Object(Arc::new(GitObjectFilterPreviewObject {
        objects,
        total_count,
        total_younger,
    })))
}

//
// Mutations.
//

fn check_site_admin(
    database: &CodeIntelPoliciesHandle,
    actor: Actor,
) -> Result<(), ExecError> {
    match database.current_user_site_admin(actor) {
        Ok(true) => Ok(()),
        Ok(false) => Err(ExecError::new("must be site admin")),
        Err(message) => Err(ExecError::new(message)),
    }
}

/// `rootResolver.CreateCodeIntelligenceConfigurationPolicy`.
pub fn create_configuration_policy(
    database: &CodeIntelPoliciesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    check_site_admin(database, actor)?;

    let name = arguments.string("name").unwrap_or_default().to_owned();
    let policy_type = arguments
        .string("type")
        .unwrap_or_default()
        .to_owned();
    let pattern = arguments.string("pattern").unwrap_or_default().to_owned();
    let protected = false;
    let retention_enabled = arguments.bool("retentionEnabled").unwrap_or(false);
    let retention_duration_hours = int_arg(arguments, "retentionDurationHours");
    let retain_intermediate_commits =
        arguments.bool("retainIntermediateCommits").unwrap_or(false);
    let indexing_enabled = arguments.bool("indexingEnabled").unwrap_or(false);
    let syntactic = arguments.bool("syntacticIndexingEnabled");
    let index_commit_max_age_hours = int_arg(arguments, "indexCommitMaxAgeHours");
    let index_intermediate_commits =
        arguments.bool("indexIntermediateCommits").unwrap_or(false);
    let embeddings_enabled = arguments.bool("embeddingsEnabled").unwrap_or(false);

    validate_configuration_policy(ValidationInput {
        policy_type: &policy_type,
        name: &name,
        pattern: &pattern,
        protected,
        retention_enabled,
        retention_duration_hours,
        indexing_enabled,
        syntactic_indexing_enabled: syntactic,
        index_commit_max_age_hours,
        embeddings_enabled,
    })
    .map_err(ExecError::new)?;

    let repository_id = match arguments.string("repository") {
        Some(repository) => Some(
            crate::graphql_resolvers::unmarshal_relay_id(repository).map_err(ExecError::new)?,
        ),
        None => None,
    };
    let repository_patterns = string_list(arguments, "repositoryPatterns");

    let new_policy = NewConfigurationPolicy {
        repository_id,
        repository_patterns: repository_patterns.clone(),
        name,
        policy_type,
        pattern,
        retention_enabled,
        retention_duration_hours,
        retain_intermediate_commits,
        indexing_enabled,
        // Tools using the old API default this to false.
        syntactic_indexing_enabled: syntactic.unwrap_or(false),
        index_commit_max_age_hours,
        index_intermediate_commits,
        embeddings_enabled,
    };

    let policy = database
        .create_configuration_policy(&new_policy)
        .map_err(ExecError::new)?;

    // `Service.CreateConfigurationPolicy` -> `UpdateReposMatchingPolicyPatterns`.
    if let Some(patterns) = repository_patterns.as_deref() {
        if !patterns.is_empty() {
            database
                .update_repos_matching_patterns(
                    patterns,
                    policy.id,
                    database.repository_match_limit(),
                )
                .map_err(ExecError::new)?;
        }
    }

    Ok(FieldValue::Object(Arc::new(ConfigurationPolicyObject {
        policy,
        database: database.clone(),
    })))
}

/// `rootResolver.UpdateCodeIntelligenceConfigurationPolicy`.
pub fn update_configuration_policy(
    database: &CodeIntelPoliciesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<(), ExecError> {
    check_site_admin(database, actor)?;

    let name = arguments.string("name").unwrap_or_default().to_owned();
    let policy_type = arguments.string("type").unwrap_or_default().to_owned();
    let pattern = arguments.string("pattern").unwrap_or_default().to_owned();
    let retention_enabled = arguments.bool("retentionEnabled").unwrap_or(false);
    let retention_duration_hours = int_arg(arguments, "retentionDurationHours");
    let retain_intermediate_commits =
        arguments.bool("retainIntermediateCommits").unwrap_or(false);
    let indexing_enabled = arguments.bool("indexingEnabled").unwrap_or(false);
    let syntactic = arguments.bool("syntacticIndexingEnabled");
    let index_commit_max_age_hours = int_arg(arguments, "indexCommitMaxAgeHours");
    let index_intermediate_commits =
        arguments.bool("indexIntermediateCommits").unwrap_or(false);
    let embeddings_enabled = arguments.bool("embeddingsEnabled").unwrap_or(false);

    validate_configuration_policy(ValidationInput {
        policy_type: &policy_type,
        name: &name,
        pattern: &pattern,
        protected: false,
        retention_enabled,
        retention_duration_hours,
        indexing_enabled,
        syntactic_indexing_enabled: syntactic,
        index_commit_max_age_hours,
        embeddings_enabled,
    })
    .map_err(ExecError::new)?;

    let id = arguments
        .string("id")
        .ok_or_else(|| ExecError::new("missing id argument"))
        .and_then(|id| unmarshal_policy_id(id).map_err(ExecError::new))?;

    let repository_patterns = string_list(arguments, "repositoryPatterns");

    let patch = ConfigurationPolicyPatch {
        id,
        repository_patterns: repository_patterns.clone(),
        name,
        policy_type,
        pattern,
        retention_enabled,
        retention_duration_hours,
        retain_intermediate_commits,
        indexing_enabled,
        syntactic_indexing_enabled: syntactic,
        index_commit_max_age_hours,
        index_intermediate_commits,
        embeddings_enabled,
    };

    database
        .update_configuration_policy(&patch)
        .map_err(ExecError::new)?;

    if let Some(patterns) = repository_patterns.as_deref() {
        if !patterns.is_empty() {
            database
                .update_repos_matching_patterns(
                    patterns,
                    patch.id,
                    database.repository_match_limit(),
                )
                .map_err(ExecError::new)?;
        }
    }

    Ok(())
}

/// `rootResolver.DeleteCodeIntelligenceConfigurationPolicy`.
pub fn delete_configuration_policy(
    database: &CodeIntelPoliciesHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<(), ExecError> {
    check_site_admin(database, actor)?;
    let id = arguments
        .string("policy")
        .ok_or_else(|| ExecError::new("missing policy argument"))
        .and_then(|policy| unmarshal_policy_id(policy).map_err(ExecError::new))?;
    database
        .delete_configuration_policy_by_id(id)
        .map_err(ExecError::new)
}

struct ValidationInput<'a> {
    policy_type: &'a str,
    name: &'a str,
    pattern: &'a str,
    protected: bool,
    retention_enabled: bool,
    retention_duration_hours: Option<i32>,
    indexing_enabled: bool,
    syntactic_indexing_enabled: Option<bool>,
    index_commit_max_age_hours: Option<i32>,
    embeddings_enabled: bool,
}

/// `validateConfigurationPolicy`, verbatim messages. The baseline
/// nil-dereferences `SyntacticIndexingEnabled` when embeddings are enabled and
/// the caller omitted it; the port treats `None` as `false` (documented
/// erratum).
fn validate_configuration_policy(input: ValidationInput<'_>) -> Result<(), String> {
    let _ = input.protected;
    if !matches!(input.policy_type, "GIT_COMMIT" | "GIT_TAG" | "GIT_TREE") {
        return Err(format!(
            "illegal git object type '{}', expected 'GIT_COMMIT', 'GIT_TAG', or 'GIT_TREE'",
            input.policy_type
        ));
    }
    if input.name.is_empty() {
        return Err("no name supplied".to_owned());
    }
    if input.pattern.is_empty() {
        return Err("no pattern supplied".to_owned());
    }
    if input.policy_type == "GIT_COMMIT" && input.pattern != "HEAD" {
        return Err("pattern must be HEAD for policy type 'GIT_COMMIT'".to_owned());
    }
    if input.retention_enabled {
        if let Some(hours) = input.retention_duration_hours {
            if !(0..=MAX_DURATION_HOURS).contains(&hours) {
                return Err(format!("illegal retention duration '{hours}'"));
            }
        }
    }
    if input.indexing_enabled {
        if let Some(hours) = input.index_commit_max_age_hours {
            if !(0..=MAX_DURATION_HOURS).contains(&hours) {
                return Err(format!("illegal index commit max age '{hours}'"));
            }
        }
    }
    if input.embeddings_enabled {
        if input.retention_enabled
            || input.indexing_enabled
            || input.syntactic_indexing_enabled.unwrap_or(false)
        {
            return Err(
                "configuration policies can apply to indexing (precise or syntactic) or embeddings, but not both"
                    .to_owned(),
            );
        }
        if input.policy_type != "GIT_COMMIT" {
            return Err("embeddings policies must have type 'GIT_COMMIT'".to_owned());
        }
    }
    Ok(())
}

//
// Objects.
//

struct ConfigurationPolicyConnectionObject {
    policies: Vec<ConfigurationPolicy>,
    total_count: i32,
    database: CodeIntelPoliciesHandle,
}

impl ObjectValue for ConfigurationPolicyConnectionObject {
    fn type_name(&self) -> &'static str {
        "CodeIntelligenceConfigurationPolicyConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: Some("CodeIntelligenceConfigurationPolicy"),
                items: self
                    .policies
                    .iter()
                    .map(|policy| {
                        FieldValue::Object(Arc::new(ConfigurationPolicyObject {
                            policy: policy.clone(),
                            database: self.database.clone(),
                        }))
                    })
                    .collect(),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.total_count,
            ))))),
            "pageInfo" => Ok(Some(FieldValue::Object(Arc::new(
                // The baseline passes `offset: 0` to
                // `NewTotalCountConnectionResolver`, so `hasNextPage` only
                // looks at the returned node count.
                crate::graphql_resolvers::PageInfoObject::new(
                    (self.policies.len() as i32) < self.total_count,
                    None,
                ),
            )))),
            _ => Ok(None),
        }
    }
}

pub struct ConfigurationPolicyObject {
    pub policy: ConfigurationPolicy,
    pub database: CodeIntelPoliciesHandle,
}

impl ObjectValue for ConfigurationPolicyObject {
    fn type_name(&self) -> &'static str {
        "CodeIntelligenceConfigurationPolicy"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let policy = &self.policy;
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(
                marshal_configuration_policy_id(policy.id),
            )))),
            "name" => Ok(Some(FieldValue::Json(Json::Str(
                policy.name.clone().unwrap_or_default(),
            )))),
            "repository" => match policy.repository_id {
                None => Ok(Some(FieldValue::Null)),
                Some(id) => {
                    let row = self
                        .database
                        .get_repository(id)
                        .map_err(ExecError::new)?;
                    Ok(Some(FieldValue::Object(Arc::new(
                        CodeIntelRepositoryObject {
                            id: row.id,
                            name: row.name,
                        },
                    ))))
                }
            },
            "repositoryPatterns" => Ok(Some(match &policy.repository_patterns {
                Some(patterns) => FieldValue::List {
                    element_type: Some("String"),
                    items: patterns
                        .iter()
                        .map(|pattern| FieldValue::Json(Json::Str(pattern.clone())))
                        .collect(),
                },
                None => FieldValue::Null,
            })),
            "type" => match policy.policy_type.as_str() {
                "GIT_COMMIT" | "GIT_TAG" | "GIT_TREE" => Ok(Some(FieldValue::Json(Json::Str(
                    policy.policy_type.clone(),
                )))),
                other => Err(ExecError::new(format!("unknown git object type {other}"))),
            },
            "pattern" => Ok(Some(FieldValue::Json(Json::Str(policy.pattern.clone())))),
            "protected" => Ok(Some(FieldValue::Json(Json::Bool(policy.protected)))),
            "retentionEnabled" => Ok(Some(FieldValue::Json(Json::Bool(
                policy.retention_enabled,
            )))),
            "retentionDurationHours" => Ok(Some(match policy.retention_duration_hours {
                Some(hours) => FieldValue::Json(Json::Int(i64::from(hours))),
                None => FieldValue::Null,
            })),
            "retainIntermediateCommits" => Ok(Some(FieldValue::Json(Json::Bool(
                policy.retain_intermediate_commits,
            )))),
            "indexingEnabled" => Ok(Some(FieldValue::Json(Json::Bool(
                policy.indexing_enabled,
            )))),
            "syntacticIndexingEnabled" => Ok(Some(FieldValue::Json(Json::Bool(
                policy.syntactic_indexing_enabled,
            )))),
            "indexCommitMaxAgeHours" => Ok(Some(match policy.index_commit_max_age_hours {
                Some(hours) => FieldValue::Json(Json::Int(i64::from(hours))),
                None => FieldValue::Null,
            })),
            "indexIntermediateCommits" => Ok(Some(FieldValue::Json(Json::Bool(
                policy.index_intermediate_commits,
            )))),
            "embeddingsEnabled" => Ok(Some(FieldValue::Json(Json::Bool(
                policy.embeddings_enabled,
            )))),
            _ => Ok(None),
        }
    }
}

struct CodeIntelRepositoryObject {
    id: i32,
    name: String,
}

impl ObjectValue for CodeIntelRepositoryObject {
    fn type_name(&self) -> &'static str {
        "CodeIntelRepository"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(
                crate::graphql_resolvers::RepositoryObject::relay_id(self.id),
            )))),
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.name.clone())))),
            "url" => Ok(Some(FieldValue::Json(Json::Str(format!("/{}", self.name))))),
            "externalRepository" => Ok(Some(FieldValue::Null)),
            _ => Ok(None),
        }
    }
}

struct RepositoryFilterPreviewObject {
    nodes: Vec<RepositoryRow>,
    total_count: i32,
    total_matches: i32,
    matches_all: bool,
    limit: Option<i32>,
}

impl ObjectValue for RepositoryFilterPreviewObject {
    fn type_name(&self) -> &'static str {
        "RepositoryFilterPreview"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: Some("CodeIntelRepository"),
                items: self
                    .nodes
                    .iter()
                    .map(|row| {
                        FieldValue::Object(Arc::new(CodeIntelRepositoryObject {
                            id: row.id,
                            name: row.name.clone(),
                        }))
                    })
                    .collect(),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.total_count,
            ))))),
            "totalMatches" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.total_matches,
            ))))),
            "matchesAllRepos" => Ok(Some(FieldValue::Json(Json::Bool(self.matches_all)))),
            "limit" => Ok(Some(match self.limit {
                Some(limit) => FieldValue::Json(Json::Int(i64::from(limit))),
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}

struct GitObjectFilterPreviewObject {
    objects: Vec<GitObjectRow>,
    total_count: i32,
    total_younger: Option<i32>,
}

impl ObjectValue for GitObjectFilterPreviewObject {
    fn type_name(&self) -> &'static str {
        "GitObjectFilterPreview"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => Ok(Some(FieldValue::List {
                element_type: Some("CodeIntelGitObject"),
                items: self
                    .objects
                    .iter()
                    .map(|object| {
                        FieldValue::Object(Arc::new(CodeIntelGitObjectObject {
                            name: object.name.clone(),
                            rev: object.rev.clone(),
                            committed_at: object.committed_at,
                        }))
                    })
                    .collect(),
            })),
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.total_count,
            ))))),
            "totalCountYoungerThanThreshold" => Ok(Some(match self.total_younger {
                Some(value) => FieldValue::Json(Json::Int(i64::from(value))),
                None => FieldValue::Null,
            })),
            _ => Ok(None),
        }
    }
}

struct CodeIntelGitObjectObject {
    name: String,
    rev: String,
    committed_at: OffsetDateTime,
}

impl ObjectValue for CodeIntelGitObjectObject {
    fn type_name(&self) -> &'static str {
        "CodeIntelGitObject"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "name" => Ok(Some(FieldValue::Json(Json::Str(self.name.clone())))),
            "rev" => Ok(Some(FieldValue::Json(Json::Str(self.rev.clone())))),
            "committedAt" => Ok(Some(FieldValue::Json(Json::Str(
                crate::graphql_resolvers::datetime_rfc3339(self.committed_at),
            )))),
            _ => Ok(None),
        }
    }
}
