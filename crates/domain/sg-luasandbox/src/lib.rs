//! Rust port of `internal/luasandbox`.
//!
//! The sandbox hosts a Lua 5.1 interpreter (via `mlua`, `lua51` + `vendored`)
//! with the same module system and global-function restrictions as the Go
//! baseline: base/coroutine/debug/package/math/string/table libraries are
//! available, `io`/`os` are not, and `dofile`/`load`/`loadfile`/`loadstring`/
//! `print` are removed from the global environment between and during runs.
//!
//! See `dev/rs/docs/overviews/m9_8dl_luasandbox_core_overview.md` for the
//! baseline logic index and the documented engine deviations.

pub mod libs;
pub mod sandbox;
pub mod service;
pub mod util;

pub use sandbox::{Fs, PrintSink, RunOptions, Sandbox, DEFAULT_TIMEOUT};
pub use service::{
    default_go_modules, default_lua_modules, lua_modules_from_files, CreateOptions,
    CreateSandboxError, GoModule, Service,
};
