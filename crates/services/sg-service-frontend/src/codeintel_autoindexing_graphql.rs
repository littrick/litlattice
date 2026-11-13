#![forbid(unsafe_code)]

//! Code Intel autoindexing GraphQL surface (M9.28b).
//!
//! Ports `internal/codeintel/autoindexing/transport/graphql`:
//!
//! - `Query.codeIntelligenceInferenceScript` /
//!   `Mutation.updateCodeIntelligenceInferenceScript`
//!   (`root_resolver_configuration_inference.go`);
//! - `Query.inferAutoIndexJobsForRepo` /
//!   `Mutation.queueAutoIndexJobsForRepo` (`root_resolver_inference.go`);
//! - `Repository.indexConfiguration` and the `IndexConfiguration` /
//!   `InferredConfiguration` objects
//!   (`root_resolver_configuration_repository.go`);
//! - the `AutoIndexJobDescription` / `InferAutoIndexJobsResult` objects.
//!
//! The workers live in `sg-service-worker`, which itself depends on this
//! crate, so the inference selection and job enqueuing are injected through
//! [`CodeIntelAutoindexingJobsRuntime`]. The production adapter
//! ([`crate::codeintel_autoindexing_graphql_store`]) bridges the database with
//! `block_in_place`; a differential harness supplies a worker-backed runtime.
//! No table, column or Redis key is added.

use std::sync::Arc;

use base64::Engine as _;
use sha2::{Digest, Sha256};

use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, InputValue, ObjectValue};
use sg_graphql::json::Json;
use time::OffsetDateTime;

use crate::codeintel_uploads_graphql::{
    indexer_field, precise_index_from_auto_index_job, steps_field_from_auto_index_job, AutoIndexJobRow,
    DockerStep,
};
use crate::graphql_resolvers::unmarshal_relay_id;

/// `errAutoIndexingNotEnabled` (`root_resolver.go:41`).
pub const AUTOINDEXING_NOT_ENABLED: &str =
    "precise code intelligence auto-indexing is not enabled";
/// `auth.ErrNotAuthenticated` (`internal/auth/orgs.go`).
pub const NOT_AUTHENTICATED: &str = "not authenticated";
/// `auth.ErrMustBeSiteAdmin` (`internal/auth/site_admin.go`).
pub const MUST_BE_SITE_ADMIN: &str = "must be site admin";

pub type CodeIntelAutoindexingHandle = Arc<dyn CodeIntelAutoindexingDatabase + Send + Sync>;

/// `Debug` wrapper for handles stored in types that derive `Debug`.
#[derive(Clone)]
pub struct CodeIntelAutoindexingHandleWrapper(pub CodeIntelAutoindexingHandle);

impl std::fmt::Debug for CodeIntelAutoindexingHandleWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CodeIntelAutoindexingHandle")
    }
}

/// `config.AutoIndexJobSpec`, reduced to the fields the GraphQL surface reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InferredJobRow {
    pub steps: Vec<DockerStep>,
    pub local_steps: Vec<String>,
    pub root: String,
    pub indexer: String,
    pub indexer_args: Vec<String>,
    pub outfile: String,
    pub requested_env_vars: Option<Vec<String>>,
}

/// `shared.InferenceResult` (`internal/codeintel/shared/types.go`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InferenceOutcome {
    pub jobs: Vec<InferredJobRow>,
    pub inference_output: String,
}

/// The inference/enqueue half of `AutoIndexingService`; supplied by the worker
/// crate because of the crate dependency direction.
pub trait CodeIntelAutoindexingJobsRuntime: Send + Sync {
    fn infer_index_configuration(
        &self,
        repository_id: i32,
        commit: &str,
        local_override_script: &str,
        bypass_limit: bool,
    ) -> Result<InferenceOutcome, String>;

    fn queue_auto_index_jobs(
        &self,
        repository_id: i32,
        rev: &str,
        configuration: &str,
        force: bool,
        bypass_limit: bool,
    ) -> Result<Vec<AutoIndexJobRow>, String>;
}

/// `AutoIndexingService` as reached by the resolvers, plus the site-admin
/// checker and the `conf.CodeIntelAutoIndexingEnabled` gate.
pub trait CodeIntelAutoindexingDatabase: Send + Sync {
    /// `sharedresolvers.SiteAdminChecker.CheckCurrentUserIsSiteAdmin`.
    fn check_site_admin(&self, actor: Actor) -> Result<(), String>;

    /// `conf.CodeIntelAutoIndexingEnabled`.
    fn auto_indexing_enabled(&self) -> bool;

    /// `store.GetIndexConfigurationByRepositoryID`.
    fn get_index_configuration(&self, repository_id: i32)
        -> Result<Option<Vec<u8>>, String>;

    /// `store.UpdateIndexConfigurationByRepositoryID`.
    fn update_index_configuration(&self, repository_id: i32, data: &[u8])
        -> Result<(), String>;

    /// `store.GetInferenceScript`.
    fn get_inference_script(&self) -> Result<String, String>;

    /// `store.SetInferenceScript` (including the Lua validation).
    fn set_inference_script(&self, script: &str) -> Result<(), String>;

    fn infer_index_configuration(
        &self,
        repository_id: i32,
        commit: &str,
        local_override_script: &str,
        bypass_limit: bool,
    ) -> Result<InferenceOutcome, String>;

    fn queue_auto_index_jobs(
        &self,
        repository_id: i32,
        rev: &str,
        configuration: &str,
        force: bool,
        bypass_limit: bool,
    ) -> Result<Vec<AutoIndexJobRow>, String>;
}

// ---------------------------------------------------------------------------
// Root fields
// ---------------------------------------------------------------------------

/// Whether this module owns the `Query` root field.
pub fn is_root_field(field: &str) -> bool {
    matches!(field, "codeIntelligenceInferenceScript" | "inferAutoIndexJobsForRepo")
}

/// `Query` root field dispatch.
pub fn root_field(
    db: &CodeIntelAutoindexingHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "codeIntelligenceInferenceScript" => {
            Ok(Some(FieldValue::Json(Json::Str(code_intelligence_inference_script(
                db, actor,
            )?))))
        }
        "inferAutoIndexJobsForRepo" => {
            Ok(Some(infer_auto_index_jobs_for_repo(db, arguments, actor)?))
        }
        _ => Ok(None),
    }
}

/// Whether this module owns the `Mutation` root field.
pub fn is_mutation_field(field: &str) -> bool {
    matches!(
        field,
        "updateRepositoryIndexConfiguration"
            | "queueAutoIndexJobsForRepo"
            | "updateCodeIntelligenceInferenceScript"
    )
}

/// `Mutation` root field dispatch.
pub fn mutation_field(
    db: &CodeIntelAutoindexingHandle,
    field: &str,
    arguments: &Arguments,
    actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "updateRepositoryIndexConfiguration" => {
            update_repository_index_configuration(db, arguments, actor)?;
            Ok(Some(empty_response()))
        }
        "queueAutoIndexJobsForRepo" => Ok(Some(queue_auto_index_jobs_for_repo(
            db, arguments, actor,
        )?)),
        "updateCodeIntelligenceInferenceScript" => {
            update_code_intelligence_inference_script(db, arguments, actor)?;
            Ok(Some(empty_response()))
        }
        _ => Ok(None),
    }
}

/// Whether this module owns a `Repository` field.
pub fn is_repository_field(field: &str) -> bool {
    field == "indexConfiguration"
}

/// `Repository` field dispatch.
pub fn repository_field(
    db: &CodeIntelAutoindexingHandle,
    repository_id: i32,
    field: &str,
    _arguments: &Arguments,
    _actor: Actor,
) -> Result<Option<FieldValue>, ExecError> {
    match field {
        "indexConfiguration" => Ok(Some(index_configuration(db, repository_id)?)),
        _ => Ok(None),
    }
}

fn empty_response() -> FieldValue {
    FieldValue::Object(Arc::new(crate::temporary_settings::EmptyResponseObject))
}

fn site_admin(db: &CodeIntelAutoindexingHandle, actor: Actor) -> Result<(), ExecError> {
    db.check_site_admin(actor).map_err(ExecError::new)
}

fn ensure_enabled(db: &CodeIntelAutoindexingHandle) -> Result<(), ExecError> {
    if db.auto_indexing_enabled() {
        Ok(())
    } else {
        Err(ExecError::new(AUTOINDEXING_NOT_ENABLED))
    }
}

fn required_string<'a>(arguments: &'a Arguments, name: &str) -> Result<&'a str, ExecError> {
    arguments
        .string(name)
        .ok_or_else(|| ExecError::new(format!("missing argument {name}")))
}

fn optional_string(arguments: &Arguments, name: &str, default: &str) -> String {
    match arguments.get(name) {
        Some(InputValue::Str(value)) => value.clone(),
        Some(InputValue::Null) | None => default.to_owned(),
        Some(_) => default.to_owned(),
    }
}

/// `rootResolver.CodeIntelligenceInferenceScript`
/// (`root_resolver_configuration_inference.go:12`).
fn code_intelligence_inference_script(
    db: &CodeIntelAutoindexingHandle,
    actor: Actor,
) -> Result<String, ExecError> {
    site_admin(db, actor)?;
    db.get_inference_script().map_err(ExecError::new)
}

/// `rootResolver.UpdateCodeIntelligenceInferenceScript`.
fn update_code_intelligence_inference_script(
    db: &CodeIntelAutoindexingHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<(), ExecError> {
    site_admin(db, actor)?;
    let script = required_string(arguments, "script")?;
    db.set_inference_script(script).map_err(ExecError::new)
}

/// `rootResolver.UpdateRepositoryIndexConfiguration`
/// (`root_resolver_configuration_repository.go:36`).
fn update_repository_index_configuration(
    db: &CodeIntelAutoindexingHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<(), ExecError> {
    site_admin(db, actor)?;
    ensure_enabled(db)?;

    let configuration = required_string(arguments, "configuration")?;
    // Validate input as JSON (`config.UnmarshalJSON`).
    parse_configuration(configuration).map_err(ExecError::new)?;

    let repository = required_string(arguments, "repository")?;
    let repository_id = unmarshal_relay_id(repository).map_err(ExecError::new)?;

    db.update_index_configuration(repository_id, configuration.as_bytes())
        .map_err(ExecError::new)
}

/// `rootResolver.InferAutoIndexJobsForRepo`
/// (`root_resolver_inference.go:24`).
fn infer_auto_index_jobs_for_repo(
    db: &CodeIntelAutoindexingHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    site_admin(db, actor)?;
    ensure_enabled(db)?;

    let repository = required_string(arguments, "repository")?;
    let repository_id = unmarshal_relay_id(repository).map_err(ExecError::new)?;

    let rev = optional_string(arguments, "rev", "HEAD");
    let script = optional_string(arguments, "script", "");

    let result = db
        .infer_index_configuration(repository_id, &rev, &script, false)
        .map_err(ExecError::new)?;

    Ok(FieldValue::Object(Arc::new(InferAutoIndexJobsResultObject {
        jobs: result
            .jobs
            .into_iter()
            .map(AutoIndexJobDescriptionObject::new)
            .collect(),
        inference_output: result.inference_output,
    })))
}

/// `rootResolver.QueueAutoIndexJobsForRepo` (`root_resolver_inference.go:81`).
fn queue_auto_index_jobs_for_repo(
    db: &CodeIntelAutoindexingHandle,
    arguments: &Arguments,
    actor: Actor,
) -> Result<FieldValue, ExecError> {
    site_admin(db, actor)?;
    ensure_enabled(db)?;

    let repository = required_string(arguments, "repository")?;
    let repository_id = unmarshal_relay_id(repository).map_err(ExecError::new)?;

    let rev = optional_string(arguments, "rev", "HEAD");
    let configuration = optional_string(arguments, "configuration", "");

    let indexes = db
        .queue_auto_index_jobs(repository_id, &rev, &configuration, true, true)
        .map_err(ExecError::new)?;

    Ok(FieldValue::List {
        items: indexes
            .into_iter()
            .map(precise_index_from_auto_index_job)
            .collect(),
        element_type: Some("PreciseIndex"),
    })
}

/// `rootResolver.IndexConfiguration`
/// (`root_resolver_configuration_repository.go:19`).
fn index_configuration(
    db: &CodeIntelAutoindexingHandle,
    repository_id: i32,
) -> Result<FieldValue, ExecError> {
    ensure_enabled(db)?;
    Ok(FieldValue::Object(Arc::new(IndexConfigurationObject {
        db: db.clone(),
        repository_id,
    })))
}

// ---------------------------------------------------------------------------
// Config parsing
// ---------------------------------------------------------------------------

/// `config.AutoIndexJobSpecList`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfigurationList {
    pub job_specs: Vec<InferredJobRow>,
}

#[derive(serde::Deserialize)]
struct RawConfigList {
    #[serde(default)]
    index_jobs: Option<Vec<RawJobSpec>>,
}

#[derive(serde::Deserialize)]
struct RawJobSpec {
    #[serde(default)]
    steps: Option<Vec<RawDockerStep>>,
    #[serde(default)]
    local_steps: Option<Vec<String>>,
    #[serde(default)]
    root: String,
    #[serde(default)]
    indexer: String,
    #[serde(default)]
    indexer_args: Option<Vec<String>>,
    #[serde(default)]
    outfile: String,
    #[serde(default, rename = "requestedEnvVars")]
    requested_env_vars: Option<Vec<String>>,
}

#[derive(serde::Deserialize)]
struct RawDockerStep {
    #[serde(default)]
    root: String,
    #[serde(default)]
    image: String,
    #[serde(default)]
    commands: Option<Vec<String>>,
}

/// `config.UnmarshalJSON` (`lib/codeintel/autoindex/index.go`):
/// `jsonUnmarshal` first, wrapping every failure in `invalid JSON: %v`. The
/// tolerant parser reports only error codes, so an invalid document yields
/// `invalid JSON: failed to parse JSON: [<codes>]` exactly like `%v` of
/// `[]jsonx.ParseErrorCode`.
pub fn parse_configuration(data: &str) -> Result<ConfigurationList, String> {
    let (value, codes) = sg_jsonx::parse_codes(
        data,
        sg_jsonx::ParseOptions {
            comments: true,
            trailing_commas: true,
        },
    );
    if !codes.is_empty() {
        let formatted: Vec<String> = codes.iter().map(|code| format!("{}", *code as usize)).collect();
        return Err(format!(
            "invalid JSON: failed to parse JSON: [{}]",
            formatted.join(" ")
        ));
    }
    if data.trim().is_empty() {
        return Ok(ConfigurationList::default());
    }
    let Some(value) = value else {
        return Err("invalid JSON: unexpected end of JSON input".to_owned());
    };
    let raw: RawConfigList = serde_json::from_value(json_value_to_serde(value))
        .map_err(|error| format!("invalid JSON: {error}"))?;
    Ok(ConfigurationList {
        job_specs: raw
            .index_jobs
            .unwrap_or_default()
            .into_iter()
            .map(|spec| InferredJobRow {
                steps: spec
                    .steps
                    .unwrap_or_default()
                    .into_iter()
                    .map(|step| DockerStep {
                        root: step.root,
                        image: step.image,
                        commands: step.commands.unwrap_or_default(),
                    })
                    .collect(),
                local_steps: spec.local_steps.unwrap_or_default(),
                root: spec.root,
                indexer: spec.indexer,
                indexer_args: spec.indexer_args.unwrap_or_default(),
                outfile: spec.outfile,
                requested_env_vars: spec.requested_env_vars,
            })
            .collect(),
    })
}

fn json_value_to_serde(value: sg_jsonx::JsonValue) -> serde_json::Value {
    use sg_jsonx::JsonValue as Value;
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(value) => serde_json::Value::Bool(value),
        Value::Number(lexeme) => lexeme
            .parse::<serde_json::Number>()
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(value) => serde_json::Value::String(value),
        Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(json_value_to_serde).collect())
        }
        Value::Object(entries) => serde_json::Value::Object(
            entries
                .into_iter()
                .map(|(key, value)| (key, json_value_to_serde(value)))
                .collect(),
        ),
    }
}

/// `config.MarshalJSON`: `json.MarshalIndent(..., "", "    ")` with nil slices
/// normalized to `[]` (the marshaller in `sg-service-worker` is the reference
/// implementation; the two must stay identical).
pub fn marshal_configuration(list: &ConfigurationList) -> String {
    let mut out = String::new();
    out.push('{');
    out.push('\n');
    out.push_str("    \"index_jobs\": ");
    write_spec_array(&mut out, &list.job_specs, 1);
    out.push('\n');
    out.push('}');
    out
}

/// `json.Indent(&buf, marshaled, "", "\t")` from
/// `root_resolver_configuration_repository.go:107` (the marshalled document is
/// already pretty-printed with four spaces).
pub fn indent_with_tabs(document: &str) -> String {
    let mut out = String::new();
    for (index, line) in document.split('\n').enumerate() {
        if index > 0 {
            out.push('\n');
        }
        let spaces = line.len() - line.trim_start_matches(' ').len();
        let tabs = spaces / 4;
        for _ in 0..tabs {
            out.push('\t');
        }
        out.push_str(&line[spaces..]);
    }
    out
}

fn write_spec_array(out: &mut String, specs: &[InferredJobRow], depth: usize) {
    if specs.is_empty() {
        out.push_str("[]");
        return;
    }
    out.push('[');
    for (index, spec) in specs.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push('\n');
        push_indent(out, depth + 1);
        write_spec(out, spec, depth + 1);
    }
    out.push('\n');
    push_indent(out, depth);
    out.push(']');
}

fn write_spec(out: &mut String, spec: &InferredJobRow, depth: usize) {
    out.push('{');
    out.push('\n');
    push_indent(out, depth + 1);
    out.push_str("\"steps\": ");
    write_step_array(out, &spec.steps, depth + 1);
    out.push_str(",\n");
    push_indent(out, depth + 1);
    out.push_str("\"local_steps\": ");
    write_string_array(out, &spec.local_steps, depth + 1);
    out.push_str(",\n");
    push_indent(out, depth + 1);
    out.push_str("\"root\": ");
    write_string(out, &spec.root);
    out.push_str(",\n");
    push_indent(out, depth + 1);
    out.push_str("\"indexer\": ");
    write_string(out, &spec.indexer);
    out.push_str(",\n");
    push_indent(out, depth + 1);
    out.push_str("\"indexer_args\": ");
    write_string_array(out, &spec.indexer_args, depth + 1);
    out.push_str(",\n");
    push_indent(out, depth + 1);
    out.push_str("\"outfile\": ");
    write_string(out, &spec.outfile);
    out.push_str(",\n");
    push_indent(out, depth + 1);
    out.push_str("\"requestedEnvVars\": ");
    match &spec.requested_env_vars {
        Some(values) => write_string_array(out, values, depth + 1),
        None => out.push_str("null"),
    }
    out.push('\n');
    push_indent(out, depth);
    out.push('}');
}

fn write_step_array(out: &mut String, steps: &[DockerStep], depth: usize) {
    if steps.is_empty() {
        out.push_str("[]");
        return;
    }
    out.push('[');
    for (index, step) in steps.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push('\n');
        push_indent(out, depth + 1);
        write_step(out, step, depth + 1);
    }
    out.push('\n');
    push_indent(out, depth);
    out.push(']');
}

fn write_step(out: &mut String, step: &DockerStep, depth: usize) {
    out.push('{');
    out.push('\n');
    push_indent(out, depth + 1);
    out.push_str("\"root\": ");
    write_string(out, &step.root);
    out.push_str(",\n");
    push_indent(out, depth + 1);
    out.push_str("\"image\": ");
    write_string(out, &step.image);
    out.push_str(",\n");
    push_indent(out, depth + 1);
    out.push_str("\"commands\": ");
    write_string_array(out, &step.commands, depth + 1);
    out.push('\n');
    push_indent(out, depth);
    out.push('}');
}

fn write_string_array(out: &mut String, values: &[String], depth: usize) {
    if values.is_empty() {
        out.push_str("[]");
        return;
    }
    out.push('[');
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push('\n');
        push_indent(out, depth + 1);
        write_string(out, value);
    }
    out.push('\n');
    push_indent(out, depth);
    out.push(']');
}

fn push_indent(out: &mut String, depth: usize) {
    for _ in 0..depth {
        out.push_str("    ");
    }
}

/// `encoding/json` string escaping, including HTML escaping and U+2028/2029.
fn write_string(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '<' => out.push_str("\\u003c"),
            '>' => out.push_str("\\u003e"),
            '&' => out.push_str("\\u0026"),
            '\u{2028}' => out.push_str("\\u2028"),
            '\u{2029}' => out.push_str("\\u2029"),
            ch if (ch as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", ch as u32));
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
}

// ---------------------------------------------------------------------------
// Objects
// ---------------------------------------------------------------------------

/// `comparisonKey` (`root_resolver_inference.go:178`).
pub fn comparison_key(root: &str, indexer: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(root.as_bytes());
    hash.update(b"\x00");
    hash.update(indexer.as_bytes());
    base64::engine::general_purpose::URL_SAFE.encode(hash.finalize())
}

/// `autoIndexJobDescriptionResolver` (`root_resolver_inference.go:145`).
#[derive(Clone)]
struct AutoIndexJobDescriptionObject {
    job: InferredJobRow,
}

impl AutoIndexJobDescriptionObject {
    fn new(job: InferredJobRow) -> Self {
        Self { job }
    }

    /// `r.Indexer().Name()`: the name resolved by `IndexerFromName`.
    fn resolved_indexer_name(&self) -> String {
        crate::codeintel_uploads_graphql::indexer_from_name(&self.job.indexer).name
    }
}

impl ObjectValue for AutoIndexJobDescriptionObject {
    fn type_name(&self) -> &'static str {
        "AutoIndexJobDescription"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        let value = match field {
            "root" => FieldValue::Json(Json::Str(self.job.root.clone())),
            "indexer" => indexer_field(
                &self.job.indexer,
                (!self.job.indexer.is_empty()).then(|| self.job.indexer.clone()),
            ),
            "comparisonKey" => FieldValue::Json(Json::Str(comparison_key(
                &self.job.root,
                &self.resolved_indexer_name(),
            ))),
            "steps" => steps_field_from_auto_index_job(&auto_index_job_row(&self.job)),
            _ => return Ok(None),
        };
        Ok(Some(value))
    }
}

fn auto_index_job_row(job: &InferredJobRow) -> AutoIndexJobRow {
    AutoIndexJobRow {
        id: 0,
        commit: String::new(),
        queued_at: OffsetDateTime::UNIX_EPOCH,
        state: String::new(),
        failure_message: None,
        started_at: None,
        finished_at: None,
        repository_id: 0,
        docker_steps: job.steps.clone(),
        root: job.root.clone(),
        indexer: job.indexer.clone(),
        indexer_args: job.indexer_args.clone(),
        outfile: job.outfile.clone(),
        execution_logs: Vec::new(),
        rank: None,
        local_steps: job.local_steps.clone(),
        associated_upload_id: None,
        should_reindex: false,
        requested_env_vars: job.requested_env_vars.clone().unwrap_or_default(),
    }
}

/// `inferAutoIndexJobsResultResolver` (`root_resolver_inference.go:132`).
struct InferAutoIndexJobsResultObject {
    jobs: Vec<AutoIndexJobDescriptionObject>,
    inference_output: String,
}

impl ObjectValue for InferAutoIndexJobsResultObject {
    fn type_name(&self) -> &'static str {
        "InferAutoIndexJobsResult"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "jobs" => Ok(Some(FieldValue::List {
                items: self
                    .jobs
                    .iter()
                    .map(|job| FieldValue::Object(Arc::new(job.clone())))
                    .collect(),
                element_type: Some("AutoIndexJobDescription"),
            })),
            "inferenceOutput" => Ok(Some(FieldValue::Json(Json::Str(
                self.inference_output.clone(),
            )))),
            _ => Ok(None),
        }
    }
}

/// `indexConfigurationResolver`
/// (`root_resolver_configuration_repository.go:66`).
struct IndexConfigurationObject {
    db: CodeIntelAutoindexingHandle,
    repository_id: i32,
}

impl IndexConfigurationObject {
    fn raw_configuration(&self) -> Result<Option<Vec<u8>>, ExecError> {
        let configuration = self
            .db
            .get_index_configuration(self.repository_id)
            .map_err(ExecError::new)?;
        // `pointers.NonZeroPtr(string(data))`: an empty value is nil.
        Ok(configuration.filter(|data| !data.is_empty()))
    }
}

impl ObjectValue for IndexConfigurationObject {
    fn type_name(&self) -> &'static str {
        "IndexConfiguration"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "configuration" => Ok(Some(match self.raw_configuration()? {
                Some(data) => FieldValue::Json(Json::Str(String::from_utf8_lossy(&data).into())),
                None => FieldValue::Null,
            })),
            "parsedConfiguration" => {
                let Some(data) = self.raw_configuration()? else {
                    return Ok(Some(FieldValue::Null));
                };
                let parsed = parse_configuration(&String::from_utf8_lossy(&data))
                    .map_err(ExecError::new)?;
                Ok(Some(description_list(parsed.job_specs)))
            }
            "inferredConfiguration" => {
                let result = self
                    .db
                    .infer_index_configuration(self.repository_id, "", "", true)
                    .map_err(ExecError::new)?;
                let marshaled = marshal_configuration(&ConfigurationList {
                    job_specs: result.jobs,
                });
                Ok(Some(FieldValue::Object(Arc::new(
                    InferredConfigurationObject {
                        configuration: indent_with_tabs(&marshaled),
                    },
                ))))
            }
            _ => Ok(None),
        }
    }
}

fn description_list(jobs: Vec<InferredJobRow>) -> FieldValue {
    FieldValue::List {
        items: jobs
            .into_iter()
            .map(|job| FieldValue::Object(Arc::new(AutoIndexJobDescriptionObject::new(job))))
            .collect(),
        element_type: Some("AutoIndexJobDescription"),
    }
}

/// `inferredConfigurationResolver`
/// (`root_resolver_configuration_repository.go:140`).
struct InferredConfigurationObject {
    configuration: String,
}

impl ObjectValue for InferredConfigurationObject {
    fn type_name(&self) -> &'static str {
        "InferredConfiguration"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "configuration" => Ok(Some(FieldValue::Json(Json::Str(self.configuration.clone())))),
            "parsedConfiguration" => {
                let parsed =
                    parse_configuration(&self.configuration).map_err(ExecError::new)?;
                Ok(Some(description_list(parsed.job_specs)))
            }
            // `limitErr` is only set together with an inference error, which
            // graph-gophers turns into a nulled `inferredConfiguration` field.
            "limitError" => Ok(Some(FieldValue::Null)),
            _ => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comparison_key_matches_go() {
        // sha256("root\x00scip-go"), URL-safe base64.
        assert_eq!(
            comparison_key("root", "scip-go"),
            "_j5a5d3IaWd6G_vaNu69VhZwVT2Pf5S20YScwbE46U8="
        );
    }

    #[test]
    fn indent_matches_go_json_indent() {
        let document = "{\n    \"index_jobs\": []\n}";
        assert_eq!(indent_with_tabs(document), "{\n\t\"index_jobs\": []\n}");
    }

    #[test]
    fn parse_rejects_invalid_json_like_go() {
        assert_eq!(
            parse_configuration("{").unwrap_err(),
            "invalid JSON: failed to parse JSON: [6]"
        );
    }

    #[test]
    fn parse_normalizes_jobs() {
        let parsed = parse_configuration(
            "{\n // comment\n \"index_jobs\": [{\"root\": \"\", \"indexer\": \"scip-go\", \"local_steps\": [\"a\"],},],\n}",
        )
        .expect("valid");
        assert_eq!(parsed.job_specs.len(), 1);
        assert_eq!(parsed.job_specs[0].indexer, "scip-go");
        assert_eq!(parsed.job_specs[0].local_steps, vec!["a".to_owned()]);
        assert_eq!(parsed.job_specs[0].steps, Vec::new());
    }
}
