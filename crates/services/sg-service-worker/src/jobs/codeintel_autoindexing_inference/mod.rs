#![forbid(unsafe_code)]

//! `internal/codeintel/autoindexing/internal/inference` — the Lua-based
//! autoindexing inference service.
//!
//! * [`service`] ports `service.go`: sandbox assembly, recognizer setup and
//!   overrides, path/content resolution, chained recognizer invocation and the
//!   `InferenceResult` assembly.
//! * [`luatypes`] ports `luatypes/*`: the Lua-facing `IndexJob` /
//!   `PathPattern` / `Recognizer` userdata types.
//! * [`libs`] ports `libs/*`: the `internal_patterns`, `internal_recognizers`
//!   and `internal_indexes` Go modules.
//!
//! The inference Lua scripts under `lua/` are copied byte-for-byte from the
//! baseline and registered with the `sg.autoindex` prefix, exactly like
//! `LuaModulesFromFS(lua.Scripts, ".", "sg.autoindex")`.
//!
//! See `dev/rs/docs/overviews/m9_8dm_autoindexing_inference_overview.md` for
//! the baseline logic index and the documented deviations (injected
//! `GitService`/`RateLimiter`, no observability, no global site config).

use std::collections::BTreeMap;
use std::sync::Arc;

pub mod libs;
pub mod luatypes;
pub mod service;

pub use service::{
    ArchiveOptions, DirEntry, GitService, InferenceError, InferenceService, InferenceSettings,
    RateLimiter, SandboxService, DEFAULT_MAXIMUM_FILES_WITH_CONTENT_COUNT,
    DEFAULT_MAXIMUM_FILE_WITH_CONTENT_SIZE_BYTES,
};

// ---------------------------------------------------------------------------
// Embedded Lua scripts (`//go:embed *`)
// ---------------------------------------------------------------------------

macro_rules! lua_script {
    ($name:literal) => {
        (
            concat!($name, ".lua"),
            include_str!(concat!("lua/", $name, ".lua")),
        )
    };
}

/// The `lua/*.lua` files embedded by the baseline. The baseline's
/// `//go:embed *` also embeds `README.md` and `BUILD.bazel` (module names
/// `sg.autoindex.README` / `sg.autoindex.BUILD`, never required by a script);
/// those are deliberately not registered.
pub fn inference_lua_sources() -> BTreeMap<&'static str, &'static str> {
    BTreeMap::from([
        lua_script!("config"),
        lua_script!("dotnet"),
        lua_script!("go"),
        lua_script!("indexes"),
        lua_script!("java"),
        lua_script!("patterns"),
        lua_script!("python"),
        lua_script!("recognizer"),
        lua_script!("recognizers"),
        lua_script!("ruby"),
        lua_script!("rust"),
        lua_script!("shared"),
        lua_script!("test"),
        lua_script!("typescript"),
        lua_script!("util"),
    ])
}

/// `LuaModulesFromFS(lua.Scripts, ".", "sg.autoindex")`.
pub fn inference_lua_modules() -> BTreeMap<String, String> {
    sg_luasandbox::lua_modules_from_files(inference_lua_sources(), "sg.autoindex")
}

/// `defaultModules` (`libs.go:18`): the sandbox's default Go modules plus the
/// three inference-specific modules.
pub fn inference_modules(
    indexer_overrides: &BTreeMap<String, String>,
) -> BTreeMap<String, sg_luasandbox::GoModule> {
    let mut modules = sg_luasandbox::default_go_modules();

    modules.insert(
        "internal_patterns".to_string(),
        Arc::new(|lua| libs::patterns_module(lua)),
    );
    modules.insert(
        "internal_recognizers".to_string(),
        Arc::new(|lua| libs::recognizers_module(lua)),
    );

    let options = libs::IndexerOptions {
        overrides: indexer_overrides.clone(),
    };
    modules.insert(
        "internal_indexes".to_string(),
        Arc::new(move |lua| libs::indexes_module(lua, options.clone())),
    );

    modules
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lua_modules_use_the_sg_autoindex_prefix() {
        let modules = inference_lua_modules();
        assert_eq!(modules.len(), 15);
        for name in [
            "sg.autoindex.config",
            "sg.autoindex.recognizers",
            "sg.autoindex.typescript",
            "sg.autoindex.util",
        ] {
            assert!(modules.contains_key(name), "missing {name}");
        }
    }

    #[test]
    fn modules_include_the_default_path_module() {
        let modules = inference_modules(&BTreeMap::new());
        assert!(modules.contains_key("internal_path"));
        assert!(modules.contains_key("internal_patterns"));
        assert!(modules.contains_key("internal_recognizers"));
        assert!(modules.contains_key("internal_indexes"));
    }
}
