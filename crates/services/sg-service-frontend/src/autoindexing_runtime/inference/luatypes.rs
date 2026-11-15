#![forbid(unsafe_code)]

//! `internal/codeintel/autoindexing/internal/inference/luatypes` — the
//! Lua-facing data types of the inference pipeline.
//!
//! * [`PathPattern`] is reused from
//!   [`crate::jobs::codeintel_autoindexing_path_patterns`] (M9.8df); this
//!   module adds the `mlua` userdata identity and
//!   [`path_patterns_from_user_data`].
//! * [`Recognizer`] mirrors `luatypes.Recognizer`: a path recognizer holds
//!   patterns and at most one of a `generate`/`hints` callback, while a
//!   fallback recognizer holds children.
//! * [`index_job_from_table`] / [`docker_step_from_table`] decode the
//!   `config.AutoIndexJobSpec` / `config.DockerStep` tables returned by
//!   `generate`.
//! * [`named_recognizers_from_user_data_map`] decodes the recognizer table
//!   returned by `recognizers.lua` or an override script. A `false` value is
//!   preserved as `None` when `allow_false_as_nil` is set so callers can
//!   disable a default recognizer.
//!
//! Deviations from the Go baseline are listed in
//! `dev/rs/docs/overviews/m9_8dm_autoindexing_inference_overview.md`; the
//! notable one is that table keys are required to be strings (the baseline
//! uses `LValue.String()`, which stringifies numbers).

use std::collections::BTreeMap;

use sg_luasandbox::mlua;
use sg_luasandbox::mlua::{AnyUserData, Error, Function, Table, UserData, Value};
use sg_luasandbox::util::{
    assert_lua_string, check_type_property, decode_table, for_each, map_slice,
    map_slice_or_singleton, new_type_error, set_lua_function, set_string, Decoder,
};

use crate::autoindexing_runtime::autoindex_config::{AutoIndexJobSpec, DockerStep};
use crate::autoindexing_runtime::path_patterns::PathPattern;

// ---------------------------------------------------------------------------
// Userdata identity
// ---------------------------------------------------------------------------

/// `luatypes.PathPattern` is opaque to Lua scripts: they only pass it back to
/// the `internal_patterns` module, which type-checks the userdata by type.
impl UserData for PathPattern {}

/// `luatypes.Recognizer`, likewise opaque to Lua.
#[derive(Clone, Debug)]
pub struct Recognizer {
    pub patterns: Option<Vec<PathPattern>>,
    pub patterns_for_content: Option<Vec<PathPattern>>,
    /// The registered Lua `generate` callback.
    pub generate: Option<Function>,
    /// The registered Lua `hints` callback.
    pub hints: Option<Function>,
    pub fallback: Vec<Recognizer>,
}

impl UserData for Recognizer {}

impl Recognizer {
    /// `Recognizer.Patterns`.
    pub fn patterns(&self, for_content: bool) -> &[PathPattern] {
        if for_content {
            self.patterns_for_content.as_deref().unwrap_or_default()
        } else {
            self.patterns.as_deref().unwrap_or_default()
        }
    }

    /// `Recognizer.Generator`.
    pub fn generator(&self) -> Option<&Function> {
        self.generate.as_ref()
    }

    /// `Recognizer.Hinter`.
    pub fn hinter(&self) -> Option<&Function> {
        self.hints.as_ref()
    }

    /// `luatypes.NewFallback`.
    pub fn new_fallback(fallback: Vec<Recognizer>) -> Self {
        Self {
            patterns: None,
            patterns_for_content: None,
            generate: None,
            hints: None,
            fallback,
        }
    }
}

// ---------------------------------------------------------------------------
// Patterns
// ---------------------------------------------------------------------------

/// `luatypes.PathPatternsFromUserData`: a single pattern userdata or an array
/// of them.
pub fn path_patterns_from_user_data(value: &Value) -> mlua::Result<Vec<PathPattern>> {
    map_slice_or_singleton(value, |value| {
        let user_data = check_path_pattern_user_data(value)?;
        Ok(user_data.borrow::<PathPattern>()?.clone())
    })
}

fn check_path_pattern_user_data(value: &Value) -> mlua::Result<AnyUserData> {
    match value {
        Value::UserData(user_data) if user_data.is::<PathPattern>() => Ok(user_data.clone()),
        other => Err(new_type_error("*PathPattern", other)),
    }
}

// ---------------------------------------------------------------------------
// Recognizer decoding
// ---------------------------------------------------------------------------

/// `luatypes.RecognizerFromTable`.
pub fn recognizer_from_table(table: &Table) -> mlua::Result<Recognizer> {
    let mut patterns: Option<Vec<PathPattern>> = None;
    let mut patterns_for_content: Option<Vec<PathPattern>> = None;
    let mut generate: Option<Function> = None;
    let mut hints: Option<Function> = None;

    let mut decoders: BTreeMap<&str, Decoder> = BTreeMap::new();
    decoders.insert("patterns", set_path_patterns(&mut patterns));
    decoders.insert(
        "patterns_for_content",
        set_path_patterns(&mut patterns_for_content),
    );
    decoders.insert("generate", set_lua_function(&mut generate));
    decoders.insert("hints", set_lua_function(&mut hints));
    decode_table(table, &mut decoders)?;
    drop(decoders);

    if generate.is_none() && hints.is_none() {
        return Err(Error::RuntimeError(
            "no generate or hints function supplied - at least one is required".to_string(),
        ));
    }
    if patterns.is_none() && patterns_for_content.is_none() {
        return Err(Error::RuntimeError("no patterns supplied".to_string()));
    }

    Ok(Recognizer {
        patterns,
        patterns_for_content,
        generate,
        hints,
        fallback: Vec::new(),
    })
}

/// `setPathPatterns`: empty arrays decode to `nil`, matching Go's
/// `MapSliceOrSingleton` (which returns a nil slice for an empty table).
fn set_path_patterns<'a>(target: &'a mut Option<Vec<PathPattern>>) -> Decoder<'a> {
    Box::new(move |value| {
        let patterns = path_patterns_from_user_data(value)?;
        *target = if patterns.is_empty() {
            None
        } else {
            Some(patterns)
        };
        Ok(())
    })
}

/// `luatypes.NamedRecognizersFromUserDataMap`.
pub fn named_recognizers_from_user_data_map(
    value: &Value,
    allow_false_as_nil: bool,
) -> mlua::Result<BTreeMap<String, Option<Recognizer>>> {
    check_type_property(value, "sg.recognizer")?;

    let mut recognizers = BTreeMap::new();
    for_each(value, |key, value| {
        let name = assert_lua_string(&key)?;
        if name == "__type" {
            return Ok(());
        }

        if allow_false_as_nil && matches!(&value, Value::Boolean(false)) {
            recognizers.insert(name, None);
            return Ok(());
        }

        let user_data = match &value {
            Value::UserData(user_data) if user_data.is::<Recognizer>() => user_data.clone(),
            other => return Err(new_type_error("*Recognizer", other)),
        };
        recognizers.insert(name, Some(user_data.borrow::<Recognizer>()?.clone()));
        Ok(())
    })?;

    Ok(recognizers)
}

/// `luatypes.FlattenRecognizerPatterns`.
pub fn flatten_recognizer_patterns(
    recognizers: &[Recognizer],
    for_content: bool,
) -> Vec<PathPattern> {
    let mut out = Vec::new();
    for recognizer in recognizers {
        flatten_recognizer_pattern_into(recognizer, for_content, &mut out);
    }
    out
}

fn flatten_recognizer_pattern_into(
    recognizer: &Recognizer,
    for_content: bool,
    out: &mut Vec<PathPattern>,
) {
    out.extend(recognizer.patterns(for_content).iter().cloned());
    for child in &recognizer.fallback {
        flatten_recognizer_pattern_into(child, for_content, out);
    }
}

/// `luatypes.LinearizeGenerator`: pre-order depth-first list of recognizers
/// with a `generate` callback.
pub fn linearize_generator(recognizer: &Recognizer) -> Vec<Recognizer> {
    let mut out = Vec::new();
    linearize_generator_into(recognizer, &mut out);
    out
}

fn linearize_generator_into(recognizer: &Recognizer, out: &mut Vec<Recognizer>) {
    if recognizer.generate.is_some() {
        out.push(recognizer.clone());
    }
    for child in &recognizer.fallback {
        linearize_generator_into(child, out);
    }
}

// ---------------------------------------------------------------------------
// Index job decoding
// ---------------------------------------------------------------------------

/// `luatypes.IndexJobFromTable`.
pub fn index_job_from_table(value: &Value) -> mlua::Result<AutoIndexJobSpec> {
    let table = match value {
        Value::Table(table) => table,
        other => return Err(new_type_error("table", other)),
    };

    let mut steps: Option<Vec<DockerStep>> = None;
    let mut local_steps: Option<Vec<String>> = None;
    let mut root = String::new();
    let mut indexer = String::new();
    let mut indexer_args: Option<Vec<String>> = None;
    let mut outfile = String::new();
    let mut requested_env_vars: Option<Vec<String>> = None;

    let mut decoders: BTreeMap<&str, Decoder> = BTreeMap::new();
    decoders.insert("steps", set_docker_steps(&mut steps));
    decoders.insert("local_steps", set_strings_option(&mut local_steps));
    decoders.insert("root", set_string(&mut root));
    decoders.insert("indexer", set_string(&mut indexer));
    decoders.insert("indexer_args", set_strings_option(&mut indexer_args));
    decoders.insert("outfile", set_string(&mut outfile));
    decoders.insert(
        "requested_envvars",
        set_strings_option(&mut requested_env_vars),
    );
    decode_table(table, &mut decoders)?;
    drop(decoders);

    if indexer.is_empty() {
        return Err(Error::RuntimeError("no indexer supplied".to_string()));
    }

    Ok(AutoIndexJobSpec {
        steps,
        local_steps,
        root,
        indexer,
        indexer_args,
        outfile,
        requested_env_vars,
    })
}

/// `luatypes.dockerStepFromTable`.
pub fn docker_step_from_table(value: &Value) -> mlua::Result<DockerStep> {
    let table = match value {
        Value::Table(table) => table,
        other => return Err(new_type_error("table", other)),
    };

    let mut root = String::new();
    let mut image = String::new();
    let mut commands: Option<Vec<String>> = None;

    let mut decoders: BTreeMap<&str, Decoder> = BTreeMap::new();
    decoders.insert("root", set_string(&mut root));
    decoders.insert("image", set_string(&mut image));
    decoders.insert("commands", set_strings_option(&mut commands));
    decode_table(table, &mut decoders)?;
    drop(decoders);

    if image.is_empty() {
        return Err(Error::RuntimeError("no image supplied".to_string()));
    }

    Ok(DockerStep {
        root,
        image,
        commands,
    })
}

/// `setDockerSteps`: appends, like Go's `append(*ptr, steps...)`; an empty
/// array leaves the field nil.
fn set_docker_steps<'a>(target: &'a mut Option<Vec<DockerStep>>) -> Decoder<'a> {
    Box::new(move |value| {
        let table = match value {
            Value::Table(table) => table,
            other => return Err(new_type_error("table", other)),
        };
        let steps = map_slice(table, docker_step_from_table)?;
        if !steps.is_empty() {
            target.get_or_insert_with(Vec::new).extend(steps);
        }
        Ok(())
    })
}

/// `util.SetStrings` on a nil-able slice: appends, and an empty array leaves
/// the field nil.
fn set_strings_option<'a>(target: &'a mut Option<Vec<String>>) -> Decoder<'a> {
    Box::new(move |value| {
        let table = match value {
            Value::Table(table) => table,
            other => return Err(new_type_error("table", other)),
        };
        let strings = map_slice(table, assert_lua_string)?;
        if !strings.is_empty() {
            target.get_or_insert_with(Vec::new).extend(strings);
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sg_luasandbox::sandbox::RunOptions;
    use sg_luasandbox::CreateOptions;

    fn sandbox() -> sg_luasandbox::Sandbox {
        let modules = super::super::inference_modules(&BTreeMap::new());
        sg_luasandbox::Service::new()
            .create_sandbox(CreateOptions {
                go_modules: modules,
                lua_modules: super::super::inference_lua_modules(),
            })
            .expect("sandbox")
    }

    #[test]
    fn index_job_requires_indexer() {
        let sandbox = sandbox();
        let result =
            sandbox.run_script(&RunOptions::default(), "return { root = '', outfile = '' }");
        let error = index_job_from_table(&result.expect("value")).expect_err("error");
        assert_eq!(error.to_string(), "runtime error: no indexer supplied");
    }

    #[test]
    fn index_job_decodes_all_fields() {
        let sandbox = sandbox();
        let value = sandbox
            .run_script(
                &RunOptions::default(),
                r#"return {
                    steps = { { root = 'r', image = 'i', commands = { 'a', 'b' } } },
                    local_steps = { 'x' },
                    root = 'root',
                    indexer = 'idx',
                    indexer_args = { 'a' },
                    outfile = 'index.scip',
                    requested_envvars = { 'E' },
                }"#,
            )
            .expect("value");
        let job = index_job_from_table(&value).expect("job");
        assert_eq!(job.root, "root");
        assert_eq!(job.indexer, "idx");
        assert_eq!(job.outfile, "index.scip");
        assert_eq!(
            job.steps.expect("steps"),
            vec![DockerStep {
                root: "r".to_string(),
                image: "i".to_string(),
                commands: Some(vec!["a".to_string(), "b".to_string()]),
            }]
        );
        assert_eq!(job.local_steps, Some(vec!["x".to_string()]));
        assert_eq!(job.indexer_args, Some(vec!["a".to_string()]));
        assert_eq!(job.requested_env_vars, Some(vec!["E".to_string()]));
    }

    #[test]
    fn unknown_field_is_rejected() {
        let sandbox = sandbox();
        let value = sandbox
            .run_script(
                &RunOptions::default(),
                "return { indexer = 'idx', nope = 1 }",
            )
            .expect("value");
        let error = index_job_from_table(&value).expect_err("error");
        assert_eq!(error.to_string(), "runtime error: unexpected field nope");
    }

    #[test]
    fn recognizer_requires_patterns_and_callback() {
        let sandbox = sandbox();
        let value = sandbox
            .run_script(
                &RunOptions::default(),
                "return { generate = function() end }",
            )
            .expect("value");
        let error = recognizer_from_table(value.as_table().expect("table")).expect_err("error");
        assert_eq!(error.to_string(), "runtime error: no patterns supplied");
    }
}
