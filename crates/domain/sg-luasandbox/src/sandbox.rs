//! Port of `internal/luasandbox/sandbox.go`.

use std::fmt;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mlua::{Function, HookTriggers, Lua, MultiValue, ThreadStatus, Value, VmState};

/// Port of `luasandbox.DefaultTimeout` (200ms).
pub const DEFAULT_TIMEOUT: Duration = Duration::from_millis(200);

/// Instruction count between timeout hook invocations. gopher-lua checks its
/// context on every instruction; the hook-based equivalent is cheap enough at
/// this granularity for the 200ms default.
const TIMEOUT_HOOK_INSTRUCTIONS: u32 = 1_000;

/// Port of `luasandbox.FS`.
pub trait Fs: Send + Sync {
    fn read_file(&self, name: &str) -> io::Result<Vec<u8>>;
}

/// Port of `singleScriptFS`: a filesystem containing exactly `main.lua`.
pub struct SingleScriptFs {
    script: String,
}

impl SingleScriptFs {
    pub fn new(script: impl Into<String>) -> Self {
        Self {
            script: script.into(),
        }
    }
}

impl Fs for SingleScriptFs {
    fn read_file(&self, name: &str) -> io::Result<Vec<u8>> {
        if name != "main.lua" {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "file does not exist",
            ));
        }
        Ok(self.script.clone().into_bytes())
    }
}

/// Shared writer used for [`RunOptions::print_sink`].
pub type PrintSink = Arc<Mutex<dyn Write + Send>>;

/// Port of `luasandbox.RunOptions`.
#[derive(Clone, Default)]
pub struct RunOptions {
    /// `None` means [`DEFAULT_TIMEOUT`], mirroring `RunGoCallback`.
    pub timeout: Option<Duration>,
    pub print_sink: Option<PrintSink>,
}

impl RunOptions {
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn with_print_sink(mut self, sink: PrintSink) -> Self {
        self.print_sink = Some(sink);
        self
    }
}

/// Port of `luasandbox.Sandbox`.
///
/// The Lua state is guarded by a mutex: every entry point takes exclusive
/// access for the duration of the run, like the Go `sync.Mutex`.
pub struct Sandbox {
    lua: Mutex<Lua>,
}

impl fmt::Debug for Sandbox {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sandbox").finish_non_exhaustive()
    }
}

impl Sandbox {
    pub(crate) fn from_lua(lua: Lua) -> Self {
        Self {
            lua: Mutex::new(lua),
        }
    }

    /// Port of `Sandbox.Close`: releases the underlying VM. No calls should be
    /// made after closing.
    pub fn close(self) {}

    /// Runs the given Lua script text in the sandbox (`sandbox.go:RunScript`).
    pub fn run_script(&self, opts: &RunOptions, script: &str) -> mlua::Result<Value> {
        self.run_script_named(opts, Arc::new(SingleScriptFs::new(script)), "main.lua")
    }

    /// Runs the named Lua script read from the given filesystem. A scoped
    /// `loadfile` is installed for the duration of the run
    /// (`sandbox.go:RunScriptNamed`).
    pub fn run_script_named(
        &self,
        opts: &RunOptions,
        fs: Arc<dyn Fs>,
        name: &str,
    ) -> mlua::Result<Value> {
        let contents = fs.read_file(name).map_err(mlua::Error::external)?;
        // The Go baseline converts the bytes to a string without validating
        // UTF-8; Lua source must be valid UTF-8 for mlua, so invalid sequences
        // are replaced rather than rejected (documented deviation).
        let script = String::from_utf8_lossy(&contents).into_owned();
        let name = name.to_string();

        self.run_go_callback(opts, move |lua| {
            let loadfile = make_scoped_loadfile(lua, fs.clone())?;
            lua.globals().set("loadfile", loadfile)?;

            let loaded = lua
                .load(script.as_str())
                .set_name(name.as_str())
                .eval::<MultiValue>();
            let _ = lua.globals().set("loadfile", Value::Nil);
            Ok(first_value(loaded?))
        })
    }

    /// Invokes the given Lua function bound to this sandbox
    /// (`sandbox.go:Call`). Arguments are converted with mlua's
    /// `IntoLuaMulti` (the Go baseline uses `gopher-luar`).
    pub fn call<A: mlua::IntoLuaMulti>(
        &self,
        opts: &RunOptions,
        function: &Function,
        args: A,
    ) -> mlua::Result<Value> {
        self.run_go_callback(opts, move |_lua| {
            let values = function.call::<MultiValue>(args)?;
            Ok(first_value(values))
        })
    }

    /// Invokes the given Lua function as a coroutine and collects every
    /// yielded value, including the final return values
    /// (`sandbox.go:CallGenerator`).
    pub fn call_generator<A: mlua::IntoLuaMulti>(
        &self,
        opts: &RunOptions,
        function: &Function,
        args: A,
    ) -> mlua::Result<Vec<Value>> {
        self.run_go_callback(opts, move |lua| {
            let mut results = Vec::new();
            let thread = lua.create_thread(function.clone())?;
            let arg_values: Vec<Value> = args.into_lua_multi(lua)?.into_iter().collect();

            loop {
                let yielded =
                    thread.resume::<MultiValue>(MultiValue::from_vec(arg_values.clone()))?;
                results.extend(yielded);
                if thread.status() == ThreadStatus::Resumable {
                    continue;
                }
                break;
            }

            Ok(results)
        })
    }

    /// Port of `Sandbox.RunGoCallback`: exclusive access to the state, a
    /// timeout, and the scoped `print` function.
    pub fn run_go_callback<R>(
        &self,
        opts: &RunOptions,
        callback: impl FnOnce(&Lua) -> mlua::Result<R>,
    ) -> mlua::Result<R> {
        let lua = self
            .lua
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let timeout = opts.timeout.unwrap_or(DEFAULT_TIMEOUT);
        let deadline = Instant::now() + timeout;
        lua.set_hook(
            HookTriggers::new().every_nth_instruction(TIMEOUT_HOOK_INSTRUCTIONS),
            move |_lua, _debug| {
                if Instant::now() >= deadline {
                    Err(mlua::Error::RuntimeError(
                        "context deadline exceeded".to_string(),
                    ))
                } else {
                    Ok(VmState::Continue)
                }
            },
        );

        let print = make_scoped_print(&lua, opts.print_sink.clone())?;
        let globals = lua.globals();
        globals.set("print", print)?;

        let result = callback(&lua);

        let _ = globals.set("print", Value::Nil);
        lua.remove_hook();
        result
    }
}

fn first_value(values: MultiValue) -> Value {
    values.into_iter().next().unwrap_or(Value::Nil)
}

/// Port of `makeScopedLoadfile` (`sandbox.go:91`).
fn make_scoped_loadfile(lua: &Lua, fs: Arc<dyn Fs>) -> mlua::Result<Function> {
    lua.create_function(move |lua, filename: String| -> mlua::Result<Function> {
        let contents = fs.read_file(&filename).map_err(mlua::Error::external)?;
        lua.load(contents).set_name(&filename).into_function()
    })
}

/// Port of `makeScopedPrint` (`sandbox.go:205`).
fn make_scoped_print(lua: &Lua, sink: Option<PrintSink>) -> mlua::Result<Function> {
    lua.create_function(move |_, message: String| -> mlua::Result<()> {
        let Some(sink) = &sink else {
            return Ok(());
        };

        let timestamp = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|err| mlua::Error::RuntimeError(err.to_string()))?;
        let formatted = format!("[{timestamp}] {message}\n");
        let mut writer = sink.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        writer
            .write_all(formatted.as_bytes())
            .map_err(mlua::Error::external)
    })
}
