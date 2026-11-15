#![forbid(unsafe_code)]

//! `internal/codeintel/autoindexing/internal/inference/libs` — the Go-implemented
//! Lua modules exposed to the inference scripts as `internal_patterns`,
//! `internal_recognizers` and `internal_indexes`.
//!
//! * `internal_patterns` mirrors `patternAPI`: `backdoor`, `path_combine`,
//!   `path_exclude`.
//! * `internal_recognizers` mirrors `recognizerAPI`: `path_recognizer`,
//!   `fallback_recognizer`.
//! * `internal_indexes` mirrors `indexesAPI.get`; the site configuration
//!   override map (`codeIntelAutoIndexing.indexerMap`) is injected through
//!   [`IndexerOptions`] instead of the global `conf.SiteConfig()`.
//!
//! Error texts that the baseline builds with `%q` (only
//! `no indexer is registered for %q`) are reproduced for ASCII language
//! names; the differential compares error classes.

use std::collections::BTreeMap;

use sg_luasandbox::mlua;
use sg_luasandbox::mlua::{AnyUserData, Error, Lua, Table, Value};
use sg_luasandbox::util::{assert_lua_string, map_slice};

use super::luatypes::{path_patterns_from_user_data, recognizer_from_table, Recognizer};
use crate::autoindexing_runtime::path_patterns::PathPattern;

/// `defaultIndexers`.
const DEFAULT_INDEXERS: &[(&str, &str)] = &[
    ("go", "sourcegraph/scip-go"),
    ("java", "sourcegraph/scip-java"),
    ("python", "sourcegraph/scip-python"),
    ("rust", "sourcegraph/scip-rust"),
    ("typescript", "sourcegraph/scip-typescript"),
    ("ruby", "sourcegraph/scip-ruby"),
    ("dotnet", "sourcegraph/scip-dotnet"),
];

/// `defaultIndexerSHAs`.
const DEFAULT_INDEXER_SHAS: &[(&str, &str)] = &[
    (
        "sourcegraph/scip-go",
        "sha256:e6ca2d4b55bd1379631d45faab169fc32dc6da2c1939ed11a700261ac4c4d26f",
    ),
    (
        "sourcegraph/scip-rust",
        "sha256:adf0047fc3050ba4f7be71302b42c74b49901f38fb40916d94ac5fc9181ac078",
    ),
    (
        "sourcegraph/scip-java",
        "sha256:3de6ba2221880e2ff3a7dcb9045e6c3e86f6079d6c8dc2f913a2ca8427605c69",
    ),
    (
        "sourcegraph/scip-python",
        "sha256:e3c13f0cadca78098439c541d19a72c21672a3263e22aa706760d941581e068d",
    ),
    (
        "sourcegraph/scip-typescript",
        "sha256:3df8b36a2ad4e073415bfbeaedf38b3cfff3e697614c8f578299f470d140c2c8",
    ),
    (
        "sourcegraph/scip-ruby",
        "sha256:99b1a1adcb1ec1b6e92956f5817dfc6dfc9940f962b999685954b2ba052e1a7b",
    ),
    (
        "sourcegraph/scip-dotnet",
        "sha256:1d8a590edfb3834020fceedacac6608811dd31fcba9092426140093876d8d52e",
    ),
];

/// `libs.Indexes` inputs, derived from `InferenceSettings`.
#[derive(Debug, Clone, Default)]
pub struct IndexerOptions {
    /// `conf.SiteConfig().CodeIntelAutoIndexingIndexerMap`.
    pub overrides: BTreeMap<String, String>,
}

/// `DefaultIndexerForLang`: the image name with its pinned digest.
pub fn default_indexer_for_lang(language: &str) -> Option<String> {
    let indexer = DEFAULT_INDEXERS
        .iter()
        .find(|(name, _)| *name == language)
        .map(|(_, indexer)| *indexer)?;
    let sha = DEFAULT_INDEXER_SHAS
        .iter()
        .find(|(name, _)| *name == indexer)
        .map(|(_, sha)| *sha)
        .expect("no SHA set for indexer");
    Some(format!("{indexer}@{sha}"))
}

/// `patternAPI.LuaAPI()`.
pub fn patterns_module(lua: &Lua) -> mlua::Result<Table> {
    let module = lua.create_table()?;

    // backdoor(glob: string, pathspecs: array[string]) -> pattern
    module.set(
        "backdoor",
        lua.create_function(|lua, (glob, pathspec_table): (String, Table)| {
            let pathspecs = map_slice(&pathspec_table, assert_lua_string)?;
            lua.create_userdata(PathPattern::new(glob, pathspecs))
        })?,
    )?;

    // path_combine(...) / path_exclude(...)
    for (name, exclude) in [("path_combine", false), ("path_exclude", true)] {
        module.set(
            name,
            lua.create_function(move |lua, values: mlua::MultiValue| {
                let mut patterns = Vec::new();
                for value in values.iter() {
                    patterns.extend(path_patterns_from_user_data(value)?);
                }
                let pattern = if exclude {
                    PathPattern::exclude(patterns)
                } else {
                    PathPattern::combined(patterns)
                };
                lua.create_userdata(pattern)
            })?,
        )?;
    }

    Ok(module)
}

/// `recognizerAPI.LuaAPI()`.
pub fn recognizers_module(lua: &Lua) -> mlua::Result<Table> {
    let module = lua.create_table()?;

    // path_recognizer({ patterns, patterns_for_content, generate, hints }) -> recognizer
    module.set(
        "path_recognizer",
        lua.create_function(|lua, table: Table| {
            let recognizer = recognizer_from_table(&table)?;
            lua.create_userdata(recognizer)
        })?,
    )?;

    // fallback_recognizer(array[recognizer]) -> recognizer
    module.set(
        "fallback_recognizer",
        lua.create_function(|lua, table: Table| {
            let recognizers = map_slice(&table, |value| {
                let user_data: AnyUserData = match value {
                    Value::UserData(user_data) if user_data.is::<Recognizer>() => user_data.clone(),
                    other => return Err(sg_luasandbox::util::new_type_error("*Recognizer", other)),
                };
                Ok(user_data.borrow::<Recognizer>()?.clone())
            })?;
            lua.create_userdata(Recognizer::new_fallback(recognizers))
        })?,
    )?;

    Ok(module)
}

/// `indexesAPI.LuaAPI()`.
pub fn indexes_module(lua: &Lua, options: IndexerOptions) -> mlua::Result<Table> {
    let module = lua.create_table()?;
    module.set(
        "get",
        lua.create_function(move |_, language: String| {
            if let Some(indexer) = options.overrides.get(&language) {
                return Ok(indexer.clone());
            }
            default_indexer_for_lang(&language).ok_or_else(|| {
                Error::RuntimeError(format!("no indexer is registered for {language:?}"))
            })
        })?,
    )?;
    Ok(module)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_indexers_are_pinned() {
        assert_eq!(
            default_indexer_for_lang("go").as_deref(),
            Some("sourcegraph/scip-go@sha256:e6ca2d4b55bd1379631d45faab169fc32dc6da2c1939ed11a700261ac4c4d26f")
        );
        assert_eq!(default_indexer_for_lang("cobol"), None);
    }

    #[test]
    fn overrides_take_precedence() {
        let lua = Lua::new();
        let options = IndexerOptions {
            overrides: BTreeMap::from([("go".to_string(), "acme/indexer".to_string())]),
        };
        let module = indexes_module(&lua, options).expect("module");
        let get: mlua::Function = module.get("get").expect("get");
        assert_eq!(get.call::<String>("go").expect("indexer"), "acme/indexer");
        assert_eq!(
            get.call::<String>("java").expect("indexer"),
            "sourcegraph/scip-java@sha256:3de6ba2221880e2ff3a7dcb9045e6c3e86f6079d6c8dc2f913a2ca8427605c69"
        );
        // mlua appends a traceback to errors crossing a `Function::call`
        // boundary; only the leading message is part of the baseline contract.
        let error = get.call::<String>("cobol").expect_err("error");
        assert!(
            error
                .to_string()
                .starts_with("runtime error: no indexer is registered for \"cobol\""),
            "unexpected error: {error}"
        );
    }
}
