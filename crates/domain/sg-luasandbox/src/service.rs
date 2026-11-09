//! Port of `internal/luasandbox/service.go` and `modules.go`.
//!
//! A [`Service`] creates isolated [`Sandbox`]es. Every sandbox starts with the
//! runtime Lua modules embedded from `lua/` (`fun`, `json`, `path`), the
//! default Go module `internal_path`, and a restricted standard library.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use mlua::{Lua, LuaOptions, MultiValue, StdLib, Table, Value};

use crate::sandbox::Sandbox;

/// Loader for a Go-implemented Lua module. Invoked once per sandbox when the
/// module is preloaded into `package.preload`; the returned table is what
/// `require(<name>)` evaluates to.
pub type GoModule = Arc<dyn Fn(&Lua) -> mlua::Result<Table> + Send + Sync>;

/// Port of `luasandbox.CreateOptions`.
#[derive(Clone, Default)]
pub struct CreateOptions {
    /// Caller-supplied modules loaded from Go code.
    pub go_modules: BTreeMap<String, GoModule>,
    /// Map of `require("$key") -> $value` Lua source. Conflicting with a
    /// default runtime module is an error.
    pub lua_modules: BTreeMap<String, String>,
}

/// Error returned by [`Service::create_sandbox`].
#[derive(Debug)]
pub enum CreateSandboxError {
    /// A caller-supplied Lua module collides with a runtime module.
    LuaModuleExists(String),
    /// A failure while building the Lua state.
    Lua(mlua::Error),
}

impl fmt::Display for CreateSandboxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // Go: errors.Newf("a Lua module with the name %q already exists", k)
            Self::LuaModuleExists(name) => {
                write!(f, "a Lua module with the name {name:?} already exists")
            }
            Self::Lua(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for CreateSandboxError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::LuaModuleExists(_) => None,
            Self::Lua(err) => Some(err),
        }
    }
}

impl From<mlua::Error> for CreateSandboxError {
    fn from(err: mlua::Error) -> Self {
        Self::Lua(err)
    }
}

/// Port of `luasandbox.Service`.
#[derive(Default)]
pub struct Service;

impl Service {
    pub fn new() -> Self {
        Self
    }

    /// Creates a new sandbox. See `service.go:CreateSandbox`.
    pub fn create_sandbox(&self, opts: CreateOptions) -> Result<Sandbox, CreateSandboxError> {
        let CreateOptions {
            go_modules,
            mut lua_modules,
        } = opts;

        // Default LuaModules to our runtime files; a caller-supplied module
        // shadowing one of them is rejected.
        for (name, contents) in default_lua_modules() {
            if lua_modules.contains_key(&name) {
                return Err(CreateSandboxError::LuaModuleExists(name));
            }
            lua_modules.insert(name, contents);
        }

        // The Go baseline opens the libraries explicitly (gopher-lua
        // `SkipOpenLibs`). mlua always opens the base library (which also
        // provides `coroutine` on Lua 5.1); the rest are opened explicitly.
        // `debug` is only reachable through the unsafe constructor, exactly
        // like the baseline, which registers `lua.OpenDebug`.
        let lua = unsafe {
            Lua::unsafe_new_with(
                StdLib::PACKAGE | StdLib::TABLE | StdLib::STRING | StdLib::MATH | StdLib::DEBUG,
                LuaOptions::default(),
            )
        };

        // Preload caller-supplied Go modules.
        let globals = lua.globals();
        let package: Table = globals.get("package")?;
        let preload: Table = package.get("preload")?;
        for (name, loader) in go_modules {
            let loader = loader.clone();
            preload.set(
                name,
                lua.create_function(move |lua, _: MultiValue| loader(lua))?,
            )?;
        }

        // De-register global functions that could do something unwanted.
        for name in GLOBALS_TO_UNSET {
            globals.set(*name, Value::Nil)?;
        }

        // Insert a new package loader that serves the LuaModules map. The
        // gopher-lua baseline keeps exactly two loaders (preload and the Lua
        // file loader); Lua 5.1 additionally ships a Go/C module loader and an
        // all-in-one C loader, which we drop because the baseline has no C
        // module support.
        let module_lookup = lua_modules.clone();
        let lua_loader = lua.create_function(move |lua, name: String| -> mlua::Result<Value> {
            match module_lookup.get(&name) {
                Some(contents) => {
                    let function = lua
                        .load(contents.as_str())
                        .set_name(&name)
                        .into_function()?;
                    Ok(Value::Function(function))
                }
                // Loaders return nil if they don't handle the module.
                None => Ok(Value::Nil),
            }
        })?;

        let old_loaders: Table = package.get("loaders")?;
        let loaders = lua.create_table()?;
        loaders.raw_set(1, lua_loader)?;
        loaders.raw_set(2, old_loaders.raw_get::<Value>(1)?)?;
        loaders.raw_set(3, old_loaders.raw_get::<Value>(2)?)?;
        package.set("loaders", loaders)?;

        // Parity with gopher-lua's package library: no C paths, and `loadlib`
        // raises instead of loading shared objects.
        package.set("cpath", "")?;
        package.set(
            "loadlib",
            lua.create_function(|_, _: MultiValue| -> mlua::Result<()> {
                Err(mlua::Error::RuntimeError(
                    "loadlib is not supported".to_string(),
                ))
            })?,
        )?;

        Ok(Sandbox::from_lua(lua))
    }
}

/// Port of `globalsToUnset` (`globals.go`).
pub const GLOBALS_TO_UNSET: &[&str] = &["dofile", "load", "loadfile", "loadstring", "print"];

/// The embedded Lua runtime sources (`internal/luasandbox/lua/*`), copied
/// verbatim from the Go baseline.
pub const FUN_LUA: &str = include_str!("../lua/fun.lua");
pub const JSON_LUA: &str = include_str!("../lua/json.lua");
pub const PATH_LUA: &str = include_str!("../lua/path.lua");

/// Port of `DefaultLuaModules` (`modules.go:init`).
pub fn default_lua_modules() -> BTreeMap<String, String> {
    lua_modules_from_files(
        [
            ("lua/fun.lua", FUN_LUA),
            ("lua/json.lua", JSON_LUA),
            ("lua/path.lua", PATH_LUA),
        ],
        "",
    )
}

/// Port of `LuaModulesFromFS` (`modules.go:26`).
///
/// Module names are derived from the base name of each file: the extension is
/// stripped, the remainder is split on `:` and joined with `.`, and an optional
/// `prefix` is prepended.
pub fn lua_modules_from_files<I, P, C>(files: I, prefix: &str) -> BTreeMap<String, String>
where
    I: IntoIterator<Item = (P, C)>,
    P: AsRef<str>,
    C: AsRef<str>,
{
    let mut modules = BTreeMap::new();
    for (path, contents) in files {
        let base = go_base(path.as_ref());
        let stem = match base.rfind('.') {
            Some(index) => &base[..index],
            None => base.as_str(),
        };
        // splitPathList: empty -> empty list; names joined with ".".
        let mut name = if stem.is_empty() {
            String::new()
        } else {
            stem.split(':').collect::<Vec<_>>().join(".")
        };
        if !prefix.is_empty() {
            name = format!("{prefix}.{name}");
        }
        modules.insert(name, contents.as_ref().to_string());
    }
    modules
}

/// Port of `DefaultGoModules` (`libs.go:19`): the `internal_path` module.
pub fn default_go_modules() -> BTreeMap<String, GoModule> {
    let mut modules: BTreeMap<String, GoModule> = BTreeMap::new();
    modules.insert(
        "internal_path".to_string(),
        Arc::new(|lua: &Lua| crate::libs::path_module(lua)),
    );
    modules
}

/// Go's `path/filepath.Base` for slash-separated paths.
///
/// Reference: `path.Base` — `Base("") == "."`, `Base("/") == "/"`, trailing
/// separators are removed before taking the last element.
pub fn go_base(path: &str) -> String {
    if path.is_empty() {
        return ".".to_string();
    }
    let bytes = path.as_bytes();
    let mut end = bytes.len();
    while end > 0 && bytes[end - 1] == b'/' {
        end -= 1;
    }
    if end == 0 {
        // All separators.
        return "/".to_string();
    }
    let trimmed = &path[..end];
    match trimmed.rfind('/') {
        Some(index) => trimmed[index + 1..].to_string(),
        None => trimmed.to_string(),
    }
}

/// Go's `path/filepath.Dir` for slash-separated paths (POSIX semantics).
pub fn go_dir(path: &str) -> String {
    if path.is_empty() {
        return ".".to_string();
    }
    let bytes = path.as_bytes();
    let mut i = bytes.len() as isize - 1;
    while i >= 0 && bytes[i as usize] != b'/' {
        i -= 1;
    }
    if i < 0 {
        return ".".to_string();
    }
    go_clean(&path[..=(i as usize)])
}

/// Go's `path/filepath.Join` for two slash-separated paths.
pub fn go_join(first: &str, second: &str) -> String {
    if first.is_empty() && second.is_empty() {
        return String::new();
    }
    if first.is_empty() {
        return go_clean(second);
    }
    if second.is_empty() {
        return go_clean(first);
    }
    go_clean(&format!("{first}/{second}"))
}

/// Port of `path.Clean` (POSIX), which `filepath.Clean` delegates to on Unix.
pub fn go_clean(path: &str) -> String {
    if path.is_empty() {
        return ".".to_string();
    }
    let rooted = path.starts_with('/');
    let bytes = path.as_bytes();
    let n = bytes.len();

    let mut out = String::with_capacity(n);
    let mut dotdot: usize = 0;
    let mut r: usize = 0;
    if rooted {
        out.push('/');
        r = 1;
        dotdot = 1;
    }

    while r < n {
        if bytes[r] == b'/' || (bytes[r] == b'.' && (r + 1 == n || bytes[r + 1] == b'/')) {
            r += 1;
        } else if bytes[r] == b'.'
            && r + 1 < n
            && bytes[r + 1] == b'.'
            && (r + 2 == n || bytes[r + 2] == b'/')
        {
            r += 2;
            if out.len() > dotdot {
                // Remove the last element.
                let mut w = out.len() - 1;
                while w > dotdot && out.as_bytes()[w] != b'/' {
                    w -= 1;
                }
                out.truncate(w);
            } else if !rooted {
                if !out.is_empty() {
                    out.push('/');
                }
                out.push_str("..");
                dotdot = out.len();
            }
        } else {
            if (rooted && out.len() != 1) || (!rooted && !out.is_empty()) {
                out.push('/');
            }
            let start = r;
            while r < n && bytes[r] != b'/' {
                r += 1;
            }
            out.push_str(&path[start..r]);
        }
    }

    if out.is_empty() {
        ".".to_string()
    } else {
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_base_matches_filepath_base() {
        for (input, expected) in [
            ("", "."),
            ("/", "/"),
            ("a", "a"),
            ("a/b", "b"),
            ("/a", "a"),
            ("/a/b", "b"),
            ("a/b/", "b"),
            ("//", "/"),
            ("//a///b//", "b"),
            (".", "."),
            ("..", ".."),
            ("a/../b", "b"),
            ("/../a", "a"),
            ("foo/bar/baz.lua", "baz.lua"),
            ("x/", "x"),
        ] {
            assert_eq!(go_base(input), expected, "Base({input:?})");
        }
    }

    #[test]
    fn go_dir_matches_filepath_dir() {
        for (input, expected) in [
            ("", "."),
            ("/", "/"),
            ("a", "."),
            ("a/b", "a"),
            ("/a", "/"),
            ("/a/b", "/a"),
            ("a/b/", "a/b"),
            ("//", "/"),
            ("//a///b//", "/a/b"),
            (".", "."),
            ("..", "."),
            ("a/../b", "."),
            ("/../a", "/"),
            ("foo/bar/baz.lua", "foo/bar"),
            ("x/", "x"),
        ] {
            assert_eq!(go_dir(input), expected, "Dir({input:?})");
        }
    }

    #[test]
    fn go_clean_matches_path_clean() {
        for (input, expected) in [
            ("", "."),
            ("/", "/"),
            ("a", "a"),
            ("a/b", "a/b"),
            ("/a", "/a"),
            ("/a/b", "/a/b"),
            ("a/b/", "a/b"),
            ("//", "/"),
            ("//a///b//", "/a/b"),
            (".", "."),
            ("..", ".."),
            ("a/../b", "b"),
            ("/../a", "/a"),
            ("foo/bar/baz.lua", "foo/bar/baz.lua"),
            ("x/", "x"),
        ] {
            assert_eq!(go_clean(input), expected, "Clean({input:?})");
        }
    }

    #[test]
    fn go_join_matches_filepath_join() {
        for ((first, second), expected) in [
            (("", "b"), "b"),
            (("a", ""), "a"),
            (("a", "b/c"), "a/b/c"),
            (("a/", "/b"), "a/b"),
            (("", ""), ""),
            (("a/..", "b"), "b"),
        ] {
            assert_eq!(
                go_join(first, second),
                expected,
                "Join({first:?}, {second:?})"
            );
        }
    }

    #[test]
    fn lua_modules_from_files_uses_basenames_and_prefix() {
        let modules = lua_modules_from_files(
            [("lua/fun.lua", "return 1"), ("lua/sub/x:y.lua", "return 2")],
            "",
        );
        assert_eq!(modules.get("fun").map(String::as_str), Some("return 1"));
        assert_eq!(modules.get("x.y").map(String::as_str), Some("return 2"));

        let prefixed = lua_modules_from_files([("lua/fun.lua", "return 1")], "sg.autoindex");
        assert_eq!(
            prefixed.get("sg.autoindex.fun").map(String::as_str),
            Some("return 1")
        );
    }

    #[test]
    fn default_lua_modules_contain_runtime_files() {
        let modules = default_lua_modules();
        assert_eq!(modules.get("fun").map(String::as_str), Some(FUN_LUA));
        assert_eq!(modules.get("json").map(String::as_str), Some(JSON_LUA));
        assert_eq!(modules.get("path").map(String::as_str), Some(PATH_LUA));
    }
}
