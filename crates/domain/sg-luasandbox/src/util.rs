//! Port of `internal/luasandbox/util/util.go`.
//!
//! The helpers keep the Go call shapes (`DecodeTable` with per-field decoders,
//! `MapSlice`/`MapTableValues`, `MapSliceOrSingleton`, type errors with Go's
//! `%T` names) so the inference layer can be ported with minimal friction.

use std::collections::BTreeMap;

use mlua::{AnyUserData, Error, Function, Lua, Table, Value};

/// Error message produced by the Go baseline's `%T` formatting.
pub fn go_type_name(value: &Value) -> &'static str {
    match value {
        Value::Nil => "lua.LNilType",
        Value::Boolean(_) => "lua.LBool",
        // gopher-lua has a single number type.
        Value::Integer(_) | Value::Number(_) => "lua.LNumber",
        Value::String(_) => "lua.LString",
        Value::Table(_) => "*lua.LTable",
        Value::Function(_) => "*lua.LFunction",
        Value::Thread(_) => "*lua.LState",
        Value::UserData(_) => "*lua.LUserData",
        Value::LightUserData(_) => "lua.LightUserData",
        _ => "lua.LString",
    }
}

/// Port of `NewTypeError`: `wrong type: expecting %s, have %T`.
pub fn new_type_error(expected: &str, value: &Value) -> Error {
    Error::RuntimeError(format!(
        "wrong type: expecting {}, have {}",
        expected,
        go_type_name(value)
    ))
}

/// Port of `ForEach`: invokes the callback on each key/value pair, failing fast.
pub fn for_each(
    value: &Value,
    mut f: impl FnMut(Value, Value) -> mlua::Result<()>,
) -> mlua::Result<()> {
    let table = match value {
        Value::Table(table) => table,
        other => return Err(new_type_error("table", other)),
    };

    for pair in table.clone().pairs::<Value, Value>() {
        let (key, value) = pair?;
        f(key, value)?;
    }
    Ok(())
}

/// Decoder for a single table field, for use with [`decode_table`].
pub type Decoder<'a> = Box<dyn FnMut(&Value) -> mlua::Result<()> + 'a>;

/// Port of `DecodeTable`.
pub fn decode_table<'a>(
    table: &Table,
    decoders: &mut BTreeMap<&str, Decoder<'a>>,
) -> mlua::Result<()> {
    for pair in table.clone().pairs::<Value, Value>() {
        let (key, value) = pair?;
        let field_name = assert_lua_string(&key)?;

        let decoder = decoders
            .get_mut(field_name.as_str())
            .ok_or_else(|| Error::RuntimeError(format!("unexpected field {field_name}")))?;
        decoder(&value)?;
    }
    Ok(())
}

/// Port of `SetString`.
pub fn set_string<'a>(target: &'a mut String) -> Decoder<'a> {
    Box::new(move |value| {
        *target = assert_lua_string(value)?;
        Ok(())
    })
}

/// Port of `SetStrings` (appends to the target).
pub fn set_strings<'a>(target: &'a mut Vec<String>) -> Decoder<'a> {
    Box::new(move |value| {
        let table = match value {
            Value::Table(table) => table,
            other => return Err(new_type_error("table", other)),
        };
        let strings = map_slice(table, assert_lua_string)?;
        target.extend(strings);
        Ok(())
    })
}

/// Port of `SetLuaFunction`.
pub fn set_lua_function<'a>(target: &'a mut Option<Function>) -> Decoder<'a> {
    Box::new(move |value| {
        *target = Some(assert_lua_function(value)?);
        Ok(())
    })
}

enum MapSliceError {
    NotSlice(Value),
    Lua(Error),
}

/// Port of `MapSlice`.
pub fn map_slice<T>(
    table: &Table,
    mut f: impl FnMut(&Value) -> mlua::Result<T>,
) -> mlua::Result<Vec<T>> {
    map_slice_inner(table, &mut f).map_err(|err| match err {
        MapSliceError::NotSlice(value) => new_type_error("array", &value),
        MapSliceError::Lua(err) => err,
    })
}

fn map_slice_inner<T>(
    table: &Table,
    f: &mut dyn FnMut(&Value) -> mlua::Result<T>,
) -> Result<Vec<T>, MapSliceError> {
    let len = table.raw_len();
    let mut values = Vec::new();
    for pair in table.clone().pairs::<Value, Value>() {
        let (_, value) = pair.map_err(MapSliceError::Lua)?;
        if len == 0 {
            // At least one key-value pair is present but Len() == 0:
            // this table is map-like, not slice-like.
            return Err(MapSliceError::NotSlice(value));
        }
        values.push(f(&value).map_err(MapSliceError::Lua)?);
    }
    Ok(values)
}

/// Port of `MapTableValues`.
pub fn map_table_values<T>(
    table: &Table,
    mut f: impl FnMut(&Value) -> mlua::Result<T>,
) -> mlua::Result<Vec<T>> {
    let mut values = Vec::new();
    for pair in table.clone().pairs::<Value, Value>() {
        let (_, value) = pair?;
        values.push(f(&value)?);
    }
    Ok(values)
}

/// Port of `MapSliceOrSingleton`.
pub fn map_slice_or_singleton<T>(
    value: &Value,
    mut f: impl FnMut(&Value) -> mlua::Result<T>,
) -> mlua::Result<Vec<T>> {
    if let Value::Table(table) = value {
        match map_slice_inner(table, &mut f) {
            Ok(values) => return Ok(values),
            Err(MapSliceError::NotSlice(_)) => {}
            Err(MapSliceError::Lua(err)) => return Err(err),
        }
    }
    Ok(vec![f(value)?])
}

/// Port of `MapUserData`.
pub fn map_user_data<T>(
    value: &Value,
    f: impl FnOnce(&AnyUserData) -> mlua::Result<T>,
) -> mlua::Result<T> {
    match value {
        Value::UserData(user_data) => f(user_data),
        other => Err(new_type_error("UserData", other)),
    }
}

/// Port of `TypecheckUserData`.
pub fn typecheck_user_data<T: 'static>(value: &Value, expected: &str) -> mlua::Result<AnyUserData> {
    map_user_data(value, |user_data| {
        if user_data.is::<T>() {
            Ok(user_data.clone())
        } else {
            Err(new_type_error(expected, value))
        }
    })
}

/// Port of `CheckTypeProperty`: fails unless `value.__type == expected`.
pub fn check_type_property(value: &Value, expected: &str) -> mlua::Result<()> {
    let table = match value {
        Value::Table(table) => table,
        other => return Err(new_type_error(expected, other)),
    };
    let raw_type: Value = table.raw_get("__type")?;
    match &raw_type {
        Value::String(string) if string.to_string_lossy() == expected => Ok(()),
        _ => Err(new_type_error(expected, &raw_type)),
    }
}

/// Port of `CreateModule`: builds a module table from a map of functions.
pub fn create_module(lua: &Lua, api: &BTreeMap<&str, Function>) -> mlua::Result<Table> {
    let table = lua.create_table()?;
    for (name, function) in api {
        table.set(*name, function.clone())?;
    }
    Ok(table)
}

/// Port of `assertLuaString`.
pub fn assert_lua_string(value: &Value) -> mlua::Result<String> {
    match value {
        Value::String(string) => Ok(string.to_string_lossy()),
        other => Err(new_type_error("string", other)),
    }
}

/// Port of `assertLuaFunction`.
pub fn assert_lua_function(value: &Value) -> mlua::Result<Function> {
    match value {
        Value::Function(function) => Ok(function.clone()),
        other => Err(new_type_error("function", other)),
    }
}
