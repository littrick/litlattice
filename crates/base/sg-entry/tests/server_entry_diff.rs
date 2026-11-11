//! M9.11c differential harness for the `cmd/server` / `cmd/sourcegraph`
//! combined entry points.
//!
//! Reads a corpus of isolated environments, runs [`sg_entry::server`]'s
//! planning sequence (environment defaults, generated configuration files,
//! Procfile assembly, reindex gate, launch stages) plus the pure goreman
//! lifecycle model, and writes canonical JSON for comparison with the
//! untracked Go oracle in `cmd/server/shared`.
//!
//! The Go harness and this one both record paths relative to a per-case
//! temporary root normalized as `<ROOT>`.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use serde::Deserialize;
use serde_json::{json, Map, Value};

use sg_entry::combined::COMBINED_SERVICES;
use sg_entry::env::{parse_go_bool, EnvMap};
use sg_entry::launcher::{self, DiedAction, ProcfileEntry, SimEvent};
use sg_entry::server::{
    apply_environment, build_plan, default_prof_services, launch_stages, PlanExtras,
};

#[derive(Deserialize)]
struct Corpus {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default)]
    env_file: String,
    #[serde(default)]
    marker: bool,
    #[serde(default)]
    data_subdir: String,
    #[serde(default)]
    nginx_conf: String,
    #[serde(default)]
    events: Vec<Event>,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Event {
    Signal,
    GraceElapsed,
    ChildDied { name: String },
    ChildExited { name: String },
}

impl Event {
    fn to_sim_event(&self) -> SimEvent {
        match self {
            Event::Signal => SimEvent::Signal,
            Event::GraceElapsed => SimEvent::GraceElapsed,
            Event::ChildDied { name } => SimEvent::ChildDied(name.clone()),
            Event::ChildExited { name } => SimEvent::ChildExited(name.clone()),
        }
    }
}

fn entries_json(entries: &[ProcfileEntry]) -> Value {
    Value::Array(
        entries
            .iter()
            .map(|entry| json!({"name": entry.name, "command": entry.command}))
            .collect(),
    )
}

fn names(entries: &[ProcfileEntry]) -> Vec<&str> {
    entries
        .iter()
        .map(|entry| entry.name.as_str())
        .collect()
}

fn normalized_sha256(path: &Path, root: &str) -> String {
    use sha2::{Digest, Sha256};
    let contents = fs::read(path).unwrap_or_default();
    let text = String::from_utf8_lossy(&contents).replace(root, "<ROOT>");
    let digest = Sha256::digest(text.as_bytes());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn config_files_json(
    plan: &sg_entry::server::ServerPlan,
    config_dir: &Path,
    root: &str,
) -> Value {
    Value::Array(
        plan.config_files
            .iter()
            .map(|file| {
                json!({
                    "path": file.path,
                    "sha256": normalized_sha256(&config_dir.join(&file.path), root),
                })
            })
            .collect(),
    )
}

fn lifecycle_json(
    entries: &[ProcfileEntry],
    events: &[Event],
    died_action: DiedAction,
) -> Value {
    let sim_events: Vec<SimEvent> = events.iter().map(Event::to_sim_event).collect();
    let outcome = launcher::simulate(entries, &sim_events, died_action);
    json!({
        "actions": outcome.actions,
        "sigint_targets": outcome.sigint_targets,
        "sigkill_targets": outcome.sigkill_targets,
        "exit_code": outcome.exit_code,
    })
}

fn write_file(path: &Path, contents: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| format!("create {}: {error}", parent.display()))?;
    }
    fs::write(path, contents).map_err(|error| format!("write {}: {error}", path.display()))
}

fn run_case(case: &Case, root: &Path) -> Result<Value, String> {
    let config_dir = root.join("config");
    let data_dir = if case.data_subdir.is_empty() {
        root.join("data")
    } else {
        root.join(&case.data_subdir)
    };
    let home_dir = root.join("home");
    for dir in [&config_dir, &data_dir, &home_dir] {
        fs::create_dir_all(dir).map_err(|error| format!("create {}: {error}", dir.display()))?;
    }

    if !case.nginx_conf.is_empty() {
        write_file(&config_dir.join("nginx.conf"), &case.nginx_conf)?;
    }
    if !case.env_file.is_empty() {
        write_file(&config_dir.join("env"), &case.env_file)?;
    }
    if case.marker {
        let marker_dir = data_dir.join("postgresql");
        fs::create_dir_all(&marker_dir)
            .map_err(|error| format!("create {}: {error}", marker_dir.display()))?;
        write_file(
            &marker_dir.join("5.1-reindex.completed"),
            "done\n",
        )?;
    }

    let mut env = EnvMap::from_pairs([
        ("HOME".to_owned(), home_dir.display().to_string()),
        ("CONFIG_DIR".to_owned(), config_dir.display().to_string()),
        ("DATA_DIR".to_owned(), data_dir.display().to_string()),
    ]);
    for (key, value) in &case.env {
        env.set(key.clone(), value.clone());
    }

    let base_prof_services = default_prof_services();
    let (prof_services, extras) = PlanExtras::from_environment(&env, &base_prof_services);
    let applied = apply_environment(&mut env, &prof_services)?;
    let plan = build_plan(&mut env, &applied, &extras)?;
    let stages = launch_stages(&plan, &applied);

    let mut object = Map::new();
    object.insert("name".to_owned(), Value::String(case.name.clone()));
    object.insert("env".to_owned(), json!(env.as_map()));
    object.insert(
        "prof_services".to_owned(),
        Value::Array(
            prof_services
                .iter()
                .map(|service| service.to_json_value())
                .collect(),
        ),
    );
    object.insert("procfile".to_owned(), entries_json(&plan.procfile));
    object.insert(
        "postgres_procfile".to_owned(),
        entries_json(&plan.postgres_procfile),
    );
    object.insert("run_migrations".to_owned(), Value::Bool(plan.run_migrations));
    object.insert(
        "proc_died_action".to_owned(),
        Value::String(
            match plan.proc_died_action {
                DiedAction::Shutdown => "shutdown",
                DiedAction::Ignore => "ignore",
            }
            .to_owned(),
        ),
    );
    object.insert(
        "reindex".to_owned(),
        json!({
            "required": plan.reindex.required,
            "allowed": plan.reindex.allowed,
            "exit_code": plan.reindex.exit_code,
            "marker_file": plan.reindex.marker_file,
        }),
    );
    object.insert(
        "config_files".to_owned(),
        config_files_json(&plan, &config_dir, &root.display().to_string()),
    );
    object.insert(
        "stages".to_owned(),
        json!({
            "postgres": names(&stages.postgres),
            "application": names(&stages.application),
            "run_migrations": stages.run_migrations,
            "schemas": stages.schemas,
        }),
    );
    object.insert(
        "lifecycle".to_owned(),
        lifecycle_json(&plan.procfile, &case.events, plan.proc_died_action),
    );
    Ok(Value::Object(object))
}

#[test]
fn server_entry_diff() {
    let corpus_path = std::env::var("SERVER_ENTRY_CORPUS")
        .expect("SERVER_ENTRY_CORPUS must point at server_entry_corpus.json");
    let out_path = std::env::var("SERVER_ENTRY_OUT").expect("SERVER_ENTRY_OUT is required");
    let corpus: Corpus = serde_json::from_str(
        &fs::read_to_string(&corpus_path)
            .unwrap_or_else(|error| panic!("read {corpus_path}: {error}")),
    )
    .unwrap_or_else(|error| panic!("parse {corpus_path}: {error}"));

    let mut results = Vec::with_capacity(corpus.cases.len());
    for (index, case) in corpus.cases.iter().enumerate() {
        let root = std::env::temp_dir().join(format!("m911c-{}-{index}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("create case root");

        match run_case(case, &root) {
            Ok(value) => {
                let normalized = serde_json::to_string(&value)
                    .expect("serialize case")
                    .replace(&root.display().to_string(), "<ROOT>");
                results.push(
                    serde_json::from_str::<Value>(&normalized).expect("renormalize case value"),
                );
            }
            Err(error) => {
                results.push(json!({"name": case.name, "error": error}));
            }
        }
        let _ = fs::remove_dir_all(&root);
    }

    let document = json!({
        "combined": COMBINED_SERVICES,
        "cases": results,
    });
    fs::write(
        &out_path,
        serde_json::to_string_pretty(&document).expect("serialize output"),
    )
    .unwrap_or_else(|error| panic!("write {out_path}: {error}"));
}

/// Sanity check for the helpers the harness depends on.
#[test]
fn embeddings_bool_parsing_is_go_compatible() {
    assert_eq!(parse_go_bool("true"), Some(true));
    assert_eq!(parse_go_bool("yes"), None);
}
