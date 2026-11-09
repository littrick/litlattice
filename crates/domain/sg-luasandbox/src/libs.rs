//! Port of `internal/luasandbox/libs/paths.go` (the `internal_path` module).

use mlua::{Lua, Table};

use crate::service::{go_base, go_dir, go_join};

/// `dirWithoutDot`: like `filepath.Dir`, but returns an empty string instead of
/// `.` for an empty directory and strips a single leading `/`.
pub fn dir_without_dot(path: &str) -> String {
    let dir = go_dir(path);
    if dir == "." {
        return String::new();
    }
    if let Some(stripped) = dir.strip_prefix('/') {
        return stripped.to_string();
    }
    dir
}

/// `ancestorDirs`: all ancestor directories, ending with the empty string
/// (repository root).
pub fn ancestor_dirs(path: &str) -> Vec<String> {
    let mut ancestors = Vec::new();
    let mut dir = dir_without_dot(path);
    while !dir.is_empty() {
        ancestors.push(dir.clone());
        dir = dir_without_dot(&dir);
    }
    ancestors.push(String::new());
    ancestors
}

/// Builds the `internal_path` module table.
pub fn path_module(lua: &Lua) -> mlua::Result<Table> {
    let module = lua.create_table()?;

    // type: (string) -> array[string]
    module.set(
        "ancestors",
        lua.create_function(|lua, path: String| -> mlua::Result<Table> {
            let ancestors = ancestor_dirs(&path);
            let table = lua.create_table_with_capacity(ancestors.len(), 0)?;
            for (index, ancestor) in ancestors.iter().enumerate() {
                table.raw_set(index + 1, ancestor.as_str())?;
            }
            Ok(table)
        })?,
    )?;

    // type: (string) -> string
    module.set(
        "basename",
        lua.create_function(|_, path: String| Ok(go_base(&path)))?,
    )?;

    // type: (string) -> string
    module.set(
        "dirname",
        lua.create_function(|_, path: String| Ok(dir_without_dot(&path)))?,
    )?;

    // type: (string, string) -> string
    module.set(
        "join",
        lua.create_function(|_, (first, second): (String, String)| Ok(go_join(&first, &second)))?,
    )?;

    Ok(module)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dir_without_dot_strips_dot_and_leading_slash() {
        assert_eq!(dir_without_dot(""), "");
        assert_eq!(dir_without_dot("a"), "");
        assert_eq!(dir_without_dot("a/b/c"), "a/b");
        assert_eq!(dir_without_dot("/a/b"), "a");
        assert_eq!(dir_without_dot("/a"), "");
    }

    #[test]
    fn ancestor_dirs_ends_with_empty_root() {
        assert_eq!(ancestor_dirs("a/b/c"), vec!["a/b", "a", ""]);
        assert_eq!(ancestor_dirs("c"), vec![""]);
        assert_eq!(ancestor_dirs("/a/b"), vec!["a", ""]);
    }
}
