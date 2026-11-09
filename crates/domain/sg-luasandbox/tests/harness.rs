//! Differential harness for M9.8dl: emits one canonical `lbs` line per case.
//!
//! The Go side lives in
//! `internal/luasandbox/zz_m98dl_harness_test.go` and runs the same scripts
//! against the gopher-lua baseline. Keep the case list and script strings in
//! sync between the two files.

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mlua::{Lua, Table, Value};
use sg_luasandbox::{CreateOptions, Fs, GoModule, PrintSink, RunOptions, Sandbox, Service};

#[derive(Default)]
struct SharedVec(Arc<Mutex<Vec<u8>>>);

impl Write for SharedVec {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct MapFs(BTreeMap<String, Vec<u8>>);

impl Fs for MapFs {
    fn read_file(&self, name: &str) -> io::Result<Vec<u8>> {
        self.0
            .get(name)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "file does not exist"))
    }
}

fn quoted(value: &str) -> String {
    serde_json::to_string(value).expect("serialize string")
}

fn serialize_number(number: f64) -> String {
    if number.fract() == 0.0 && number.abs() < 9.007_199_254_740_992e15 {
        format!("{}", number as i64)
    } else {
        format!("{number}")
    }
}

fn serialize(value: &Value) -> String {
    match value {
        Value::Nil => "nil".to_string(),
        Value::Boolean(value) => value.to_string(),
        Value::Integer(value) => value.to_string(),
        Value::Number(value) => serialize_number(*value),
        Value::String(value) => quoted(&value.to_string_lossy()),
        Value::Table(table) => {
            let mut pairs: Vec<String> = table
                .clone()
                .pairs::<Value, Value>()
                .map(|pair| pair.expect("table pair"))
                .map(|(key, value)| format!("{}={}", serialize(&key), serialize(&value)))
                .collect();
            pairs.sort();
            format!("{{{}}}", pairs.join(","))
        }
        Value::Function(_) => "function".to_string(),
        Value::Thread(_) => "thread".to_string(),
        Value::UserData(_) | Value::LightUserData(_) => "userdata".to_string(),
        _ => "unknown".to_string(),
    }
}

fn classify(error: &mlua::Error) -> &'static str {
    let message = error.to_string();
    if message.contains("context deadline exceeded") {
        "timeout"
    } else if message.contains("not found") && message.contains("module") {
        "module-not-found"
    } else if matches!(error, mlua::Error::SyntaxError { .. })
        || message.contains("syntax error")
        || message.contains("compile error")
    {
        "syntax"
    } else {
        "runtime"
    }
}

fn emit_value(lines: &mut Vec<String>, case: &str, result: mlua::Result<Value>) {
    match result {
        Ok(value) => lines.push(format!(
            "{{\"case\":{},\"ok\":true,\"value\":{}}}",
            quoted(case),
            quoted(&serialize(&value))
        )),
        Err(error) => lines.push(format!(
            "{{\"case\":{},\"ok\":false,\"err\":{}}}",
            quoted(case),
            quoted(classify(&error))
        )),
    }
}

fn emit_conflict(lines: &mut Vec<String>, case: &str, message: &str) {
    lines.push(format!(
        "{{\"case\":{},\"ok\":false,\"value\":{},\"err\":\"module-conflict\"}}",
        quoted(case),
        quoted(message)
    ));
}

fn emit_sink(lines: &mut Vec<String>, case: &str, sink: &SharedVec, result: mlua::Result<Value>) {
    match result {
        Ok(_) => {
            let contents = String::from_utf8_lossy(
                &sink
                    .0
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
            )
            .into_owned();
            lines.push(format!(
                "{{\"case\":{},\"ok\":true,\"value\":{}}}",
                quoted(case),
                quoted(&normalize_timestamp(&contents))
            ));
        }
        Err(error) => emit_value(lines, case, Err(error)),
    }
}

fn normalize_timestamp(contents: &str) -> String {
    match (contents.find('['), contents.find(']')) {
        (Some(start), Some(end)) if end > start => {
            format!("[<ts>]{}", &contents[end + 1..])
        }
        _ => contents.to_string(),
    }
}

fn new_sandbox(opts: CreateOptions) -> Result<Sandbox, sg_luasandbox::CreateSandboxError> {
    Service::new().create_sandbox(opts)
}

fn go_module(lua: &Lua) -> mlua::Result<Table> {
    let module = lua.create_table()?;
    module.set(
        "add",
        lua.create_function(|_, (a, b): (f64, f64)| Ok(a + b))?,
    )?;
    module.set("stash", lua.create_function(|_, _: Value| Ok(()))?)?;
    Ok(module)
}

#[test]
fn harness() {
    let mut lines = Vec::new();
    let default = RunOptions::default();

    // arith / string / table
    {
        let sandbox = new_sandbox(CreateOptions::default()).expect("sandbox");
        emit_value(
            &mut lines,
            "arith",
            sandbox.run_script(&default, "return 38 + 4"),
        );
        emit_value(
            &mut lines,
            "string",
            sandbox.run_script(&default, "return \"hello\""),
        );
        emit_value(
            &mut lines,
            "table",
            sandbox.run_script(&default, "return {b = 2, a = {1, 2, x = \"y\"}}"),
        );
    }

    // fun / json / path / internal_path
    {
        let mut opts = CreateOptions::default();
        opts.go_modules = sg_luasandbox::default_go_modules();
        let sandbox = new_sandbox(opts).expect("sandbox");
        emit_value(
            &mut lines,
            "fun",
            sandbox.run_script(
                &default,
                "local fun = require(\"fun\") return fun.totable(fun.map(function(x) return x * 2 end, {1, 2, 3}))",
            ),
        );
        emit_value(
            &mut lines,
            "json",
            sandbox.run_script(
                &default,
                "local json = require(\"json\") return json.encode(json.decode(\"[1,2,3]\"))",
            ),
        );
        emit_value(
            &mut lines,
            "path",
            sandbox.run_script(
                &default,
                "local p = require(\"path\") local d, b = p.split(\"foo/bar/baz.lua\") return {d, b}",
            ),
        );
        emit_value(
            &mut lines,
            "internal-path",
            sandbox.run_script(
                &default,
                "local p = require(\"internal_path\") local path = require(\"path\") local a = p.ancestors(\"a/b/c\") return {p.dirname(\"a/b/c\"), p.basename(\"a/b/c\"), p.join(\"a\", \"b/c\"), a[1], a[2], a[3], #a, path.ancestors(\"a/b/c\")}",
            ),
        );
    }

    // go-module
    {
        let mut opts = CreateOptions::default();
        let loader: GoModule = Arc::new(go_module);
        opts.go_modules.insert("testmod".to_string(), loader);
        let sandbox = new_sandbox(opts).expect("sandbox");
        emit_value(
            &mut lines,
            "go-module",
            sandbox.run_script(
                &default,
                "local m = require(\"testmod\") return m.add(3, m.add(6, 9))",
            ),
        );
    }

    // lua-module
    {
        let mut opts = CreateOptions::default();
        opts.lua_modules
            .insert("custom".to_string(), "return {value = 7}".to_string());
        let sandbox = new_sandbox(opts).expect("sandbox");
        emit_value(
            &mut lines,
            "lua-module",
            sandbox.run_script(&default, "return require(\"custom\").value"),
        );
    }

    // module-conflict
    {
        let mut opts = CreateOptions::default();
        opts.lua_modules
            .insert("fun".to_string(), "return {}".to_string());
        match new_sandbox(opts) {
            Ok(_) => lines.push(format!(
                "{{\"case\":\"module-conflict\",\"ok\":true,\"value\":{}}}",
                quoted("unexpected-success")
            )),
            Err(error) => emit_conflict(&mut lines, "module-conflict", &error.to_string()),
        }
    }

    // module errors / globals
    {
        let sandbox = new_sandbox(CreateOptions::default()).expect("sandbox");
        emit_value(
            &mut lines,
            "missing-module",
            sandbox.run_script(&default, "return require(\"io\")"),
        );
        emit_value(
            &mut lines,
            "io-global",
            sandbox.run_script(&default, "return io.open(\"service_test.go\", \"rb\")"),
        );
        emit_value(
            &mut lines,
            "globals",
            sandbox.run_script(
                &default,
                "return {type(dofile), type(loadfile), type(load), type(loadstring), type(print)}",
            ),
        );
    }

    // print-sink
    {
        let sandbox = new_sandbox(CreateOptions::default()).expect("sandbox");
        let sink = Arc::new(Mutex::new(SharedVec::default()));
        let sink_ref = SharedVec(sink.lock().unwrap_or_else(|e| e.into_inner()).0.clone());
        let print_sink: PrintSink = sink;
        let opts = RunOptions::default().with_print_sink(print_sink);
        let result = sandbox.run_script(&opts, "print(\"hello world\")");
        emit_sink(&mut lines, "print-sink", &sink_ref, result);
    }

    // timeout
    {
        let sandbox = new_sandbox(CreateOptions::default()).expect("sandbox");
        let opts = RunOptions::default().with_timeout(Duration::from_millis(20));
        emit_value(
            &mut lines,
            "timeout",
            sandbox.run_script(&opts, "while true do end"),
        );
    }

    // loadfile
    {
        let sandbox = new_sandbox(CreateOptions::default()).expect("sandbox");
        let mut files = BTreeMap::new();
        files.insert(
            "main.lua".to_string(),
            b"local util = loadfile(\"lib.lua\")() return util.answer".to_vec(),
        );
        files.insert("lib.lua".to_string(), b"return {answer = 42}".to_vec());
        emit_value(
            &mut lines,
            "loadfile",
            sandbox.run_script_named(&default, Arc::new(MapFs(files)), "main.lua"),
        );
    }

    // call
    {
        let sandbox = new_sandbox(CreateOptions::default()).expect("sandbox");
        match sandbox.run_script(&default, "return function(x) return x * 3 end") {
            Ok(Value::Function(function)) => {
                emit_value(&mut lines, "call", sandbox.call(&default, &function, 14.0));
            }
            Ok(_) => emit_value(
                &mut lines,
                "call",
                sandbox.run_script(&default, "return nil"),
            ),
            Err(error) => emit_value(&mut lines, "call", Err(error)),
        }
    }

    // syntax error
    {
        let sandbox = new_sandbox(CreateOptions::default()).expect("sandbox");
        emit_value(
            &mut lines,
            "syntax",
            sandbox.run_script(&default, "return ("),
        );
    }

    for line in lines {
        println!("{line}");
    }
}
