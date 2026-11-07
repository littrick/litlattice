#![forbid(unsafe_code)]

//! Language specifications for local code intelligence.
//!
//! Port of `cmd/symbols/internal/squirrel/languages.go`. The file-extension
//! table is the baseline's `language-file-extensions.json`; the queries are
//! copied verbatim so that diffs against the Go implementation stay
//! attributable to the term rewriting, not to drifting grammars.

use std::collections::HashMap;
use std::sync::LazyLock;

use regex::Regex;
use tree_sitter::Language;

use super::Error;

pub(crate) struct CommentStyle {
    pub ignore_regex: Option<Regex>,
    pub strip_regex: Option<Regex>,
    pub skip_node_types: &'static [&'static str],
    pub node_types: &'static [&'static str],
    pub code_fence_name: &'static str,
}

pub(crate) struct LangSpec {
    pub name: &'static str,
    pub language: Language,
    pub comment_style: CommentStyle,
    pub locals_query: &'static str,
}

static EXTENSIONS_JSON: &str = include_str!("language-file-extensions.json");

/// The baseline parses the embedded table with `internal/jsonc`; the only
/// JSON-extension it uses are `//` and `/* */` comments.
fn strip_json_comments(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    let mut in_string = false;
    while let Some(ch) = chars.next() {
        if in_string {
            out.push(ch);
            match ch {
                '\\' => {
                    if let Some(escaped) = chars.next() {
                        out.push(escaped);
                    }
                }
                '"' => in_string = false,
                _ => {}
            }
            continue;
        }
        match ch {
            '"' => {
                in_string = true;
                out.push(ch);
            }
            '/' if chars.peek() == Some(&'/') => {
                chars.next();
                while let Some(&next) = chars.peek() {
                    if next == '\n' {
                        break;
                    }
                    chars.next();
                }
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                let mut prev = '\0';
                for next in chars.by_ref() {
                    if prev == '*' && next == '/' {
                        break;
                    }
                    prev = next;
                }
            }
            _ => out.push(ch),
        }
    }
    out
}

static EXT_TO_LANG: LazyLock<HashMap<String, String>> = LazyLock::new(|| {
    let by_language: HashMap<String, Vec<String>> =
        serde_json::from_str(&strip_json_comments(EXTENSIONS_JSON))
            .expect("embedded language extensions");
    let mut ext_to_lang = HashMap::new();
    for (language, extensions) in by_language {
        for extension in extensions {
            let previous = ext_to_lang.insert(extension.clone(), language.clone());
            assert!(previous.is_none(), "duplicate file extension {extension}");
        }
    }
    ext_to_lang
});

fn java_style_strip() -> Regex {
    Regex::new(r"^//|^\s*\*/?|^/\*\*|\*/$").expect("java strip regex")
}

fn java_style_ignore() -> Regex {
    Regex::new(r"^\s*(/\*\*|\*/)\s*$").expect("java ignore regex")
}

fn comment_style(name: &str) -> CommentStyle {
    match name {
        "java" => CommentStyle {
            ignore_regex: Some(java_style_ignore()),
            strip_regex: Some(java_style_strip()),
            skip_node_types: &["modifiers"],
            node_types: &["line_comment", "block_comment"],
            code_fence_name: "java",
        },
        "go" => CommentStyle {
            ignore_regex: None,
            strip_regex: Some(Regex::new(r"^//").expect("go strip regex")),
            skip_node_types: &[],
            node_types: &["comment"],
            code_fence_name: "go",
        },
        "csharp" | "javascript" | "typescript" | "cpp" => CommentStyle {
            ignore_regex: Some(java_style_ignore()),
            strip_regex: Some(java_style_strip()),
            skip_node_types: &[],
            node_types: &["comment"],
            code_fence_name: match name {
                "csharp" => "csharp",
                "javascript" => "javascript",
                "typescript" => "typescript",
                _ => "cpp",
            },
        },
        "python" | "ruby" | "starlark" => CommentStyle {
            ignore_regex: None,
            strip_regex: Some(Regex::new(r"^#").expect("hash strip regex")),
            skip_node_types: &[],
            node_types: &["comment"],
            code_fence_name: match name {
                "python" => "python",
                "ruby" => "ruby",
                _ => "starlark",
            },
        },
        other => unreachable!("unknown comment style {other}"),
    }
}

static LANG_TO_SPEC: LazyLock<HashMap<&'static str, LangSpec>> = LazyLock::new(|| {
    // The Java grammar is the only one with a top-level symbol query; the RPC
    // surface does not need it (only `getSymbols`, used by tests, does).
    let mut specs = HashMap::new();
    specs.insert(
        "java",
        LangSpec {
            name: "java",
            language: tree_sitter_java::language(),
            comment_style: comment_style("java"),
            locals_query: JAVA_LOCALS_QUERY,
        },
    );
    specs.insert(
        "go",
        LangSpec {
            name: "go",
            language: tree_sitter_go::language(),
            comment_style: comment_style("go"),
            locals_query: GO_LOCALS_QUERY,
        },
    );
    specs.insert(
        "csharp",
        LangSpec {
            name: "csharp",
            language: tree_sitter_c_sharp::language(),
            comment_style: comment_style("csharp"),
            locals_query: CSHARP_LOCALS_QUERY,
        },
    );
    specs.insert(
        "python",
        LangSpec {
            name: "python",
            language: tree_sitter_python::language(),
            comment_style: comment_style("python"),
            locals_query: PYTHON_LOCALS_QUERY,
        },
    );
    specs.insert(
        "javascript",
        LangSpec {
            name: "javascript",
            language: tree_sitter_javascript::language(),
            comment_style: comment_style("javascript"),
            locals_query: JAVASCRIPT_LOCALS_QUERY,
        },
    );
    specs.insert(
        "typescript",
        LangSpec {
            name: "typescript",
            // The baseline parses TypeScript files with the TSX grammar.
            language: tree_sitter_typescript::language_tsx(),
            comment_style: comment_style("typescript"),
            locals_query: TYPESCRIPT_LOCALS_QUERY,
        },
    );
    specs.insert(
        "cpp",
        LangSpec {
            name: "cpp",
            language: tree_sitter_cpp::language(),
            comment_style: comment_style("cpp"),
            locals_query: CPP_LOCALS_QUERY,
        },
    );
    specs.insert(
        "ruby",
        LangSpec {
            name: "ruby",
            language: tree_sitter_ruby::language(),
            comment_style: comment_style("ruby"),
            locals_query: RUBY_LOCALS_QUERY,
        },
    );
    specs.insert(
        "starlark",
        LangSpec {
            name: "starlark",
            // Starlark is parsed with the Python grammar, like the baseline.
            language: tree_sitter_python::language(),
            comment_style: comment_style("starlark"),
            locals_query: PYTHON_LOCALS_QUERY,
        },
    );
    specs
});

/// Resolve a repository path to a language, mirroring `SquirrelService.parse`.
pub(crate) fn lang_for_path(path: &str) -> Result<&'static LangSpec, Error> {
    let base = path_base(path);
    let extension = if base.contains('.') {
        path_ext(path)
            .strip_prefix('.')
            .unwrap_or_default()
            .to_owned()
    } else {
        base.to_owned()
    };

    let language = EXT_TO_LANG
        .get(&extension)
        .or_else(|| EXT_TO_LANG.get(&extension.to_lowercase()))
        .ok_or(Error::UnrecognizedFileExtension)?;

    LANG_TO_SPEC
        .get(language.as_str())
        .ok_or(Error::UnsupportedLanguage)
}

/// `filepath.Base`.
pub(crate) fn path_base(path: &str) -> &str {
    if path.is_empty() {
        return ".";
    }
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return "/";
    }
    match trimmed.rfind('/') {
        Some(index) => &trimmed[index + 1..],
        None => trimmed,
    }
}

/// `filepath.Ext` (on the last path element).
pub(crate) fn path_ext(path: &str) -> String {
    let base = path_base(path);
    match base.rfind('.') {
        Some(index) => base[index..].to_owned(),
        None => String::new(),
    }
}

/// `filepath.Dir` for the slash-separated paths used by the service.
pub(crate) fn path_dir(path: &str) -> String {
    if path.is_empty() {
        return ".".to_owned();
    }
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return "/".to_owned();
    }
    match trimmed.rfind('/') {
        Some(0) => "/".to_owned(),
        Some(index) => trimmed[..index].to_owned(),
        None => ".".to_owned(),
    }
}

/// `filepath.Join` for the cases exercised by squirrel (identifiers and path
/// components, no `..` segments).
pub(crate) fn join_path(parts: &[&str]) -> String {
    let mut components: Vec<&str> = Vec::new();
    for part in parts {
        for component in part.split('/') {
            match component {
                "" | "." => {}
                other => components.push(other),
            }
        }
    }
    if components.is_empty() {
        if parts.contains(&".") {
            return ".".to_owned();
        }
        return String::new();
    }
    components.join("/")
}

const JAVA_LOCALS_QUERY: &str = r#"
(block)                   @scope ; { ... }
(lambda_expression)       @scope ; (x, y) -> ...
(catch_clause)            @scope ; try { ... } catch (Exception e) { ... }
(enhanced_for_statement)  @scope ; for (var item : items) ...
(for_statement)           @scope ; for (var i = 0; i < 5; i++) ...
(constructor_declaration) @scope ; public Foo() { ... }
(method_declaration)      @scope ; public void f() { ... }

(local_variable_declaration declarator: (variable_declarator name: (identifier) @definition)) ; int x = ...;
(formal_parameter           name:       (identifier) @definition)                             ; public void f(int x) { ... }
(catch_formal_parameter     name:       (identifier) @definition)                             ; try { ... } catch (Exception e) { ... }
(lambda_expression          parameters: (inferred_parameters (identifier) @definition))       ; (x, y) -> ...
(lambda_expression          parameters: (identifier) @definition)                             ; x -> ...
(enhanced_for_statement     name:       (identifier) @definition)                             ; for (var item : items) ...
"#;

const GO_LOCALS_QUERY: &str = r#"
(block)                   @scope ; { ... }
(function_declaration)    @scope ; func f() { ... }
(method_declaration)      @scope ; func (r R) f() { ... }
(func_literal)            @scope ; func() { ... }
(if_statement)            @scope ; if true { ... }
(for_statement)           @scope ; for x := range xs { ... }
(expression_case)         @scope ; case "foo": ...
(communication_case)      @scope ; case x := <-ch: ...

(var_spec              name: (identifier) @definition)                   ; var x int = ...
(const_spec            name: (identifier) @definition)                   ; const x int = ...
(parameter_declaration name: (identifier) @definition)                   ; func(x int) { ... }
(short_var_declaration left: (expression_list (identifier) @definition)) ; x, y := ...
(range_clause          left: (expression_list (identifier) @definition)) ; for i := range ... { ... }
(receive_statement     left: (expression_list (identifier) @definition)) ; case x := <-ch: ...
"#;

const CSHARP_LOCALS_QUERY: &str = r#"
(block)                   @scope ; { ... }
(method_declaration)      @scope ; void f() { ... }
(for_statement)           @scope ; for (...) ...
(using_statement)         @scope ; using (...) ...
(lambda_expression)       @scope ; (x, y) => ...
(for_each_statement)      @scope ; foreach (int x in xs) ...
(catch_clause)            @scope ; try { ... } catch (Exception e) { ... }
(constructor_declaration) @scope ; public Foo() { ... }

(parameter           name: (identifier) @definition) ; void f(x int) { ... }
(variable_declarator (identifier) @definition)       ; int x = ...
(for_each_statement  left: (identifier) @definition) ; foreach (int x in xs) ...
(catch_declaration   name: (identifier) @definition) ; catch (Exception e) { ... }
"#;

const JAVASCRIPT_LOCALS_QUERY: &str = r#"
(class_declaration)              @scope ; class C { ... }
(method_definition)              @scope ; class ... { f() { ... } }
(statement_block)                @scope ; { ... }
(for_statement)                  @scope ; for (let i = 0; ...) ...
(for_in_statement)               @scope ; for (const x of xs) ...
(catch_clause)                   @scope ; catch (e) ...
(function)                       @scope ; function(x) { ... }
(function_declaration)           @scope ; function f(x) { ... }
(generator_function)             @scope ; function*(x) { ... }
(generator_function_declaration) @scope ; function *f(x) { ... }
(arrow_function)                 @scope ; x => ...

(variable_declarator name: (identifier) @definition)                                   ; const x = ...
(function_declaration name: (identifier) @definition)                                  ; function f() { ... }
(generator_function_declaration name: (identifier) @definition)                        ; function *f() { ... }
(formal_parameters (identifier) @definition)                                           ; function(x) { ... }
(formal_parameters (rest_pattern (identifier) @definition))                            ; function(...x) { ... }
(formal_parameters (assignment_pattern left: (identifier) @definition))                ; function(x = 5) { ... }
(formal_parameters (assignment_pattern left: (rest_pattern (identifier) @definition))) ; function(...x = []) { ... }
(arrow_function parameter: (identifier) @definition)                                   ; x => ...
(for_in_statement left: (identifier) @definition)                                      ; for (const x of xs) ...
(catch_clause parameter: (identifier) @definition)                                     ; catch (e) ...
"#;

const TYPESCRIPT_LOCALS_QUERY: &str = r#"
(class_declaration)              @scope ; class C { ... }
(method_definition)              @scope ; class ... { f() { ... } }
(statement_block)                @scope ; { ... }
(for_statement)                  @scope ; for (let i = 0; ...) ...
(for_in_statement)               @scope ; for (const x of xs) ...
(catch_clause)                   @scope ; catch (e) ...
(function)                       @scope ; function(x) { ... }
(function_declaration)           @scope ; function f(x) { ... }
(generator_function)             @scope ; function*(x) { ... }
(generator_function_declaration) @scope ; function *f(x) { ... }
(arrow_function)                 @scope ; x => ...

(variable_declarator name: (identifier) @definition)            ; const x = ...
(function_declaration name: (identifier) @definition)           ; function f() { ... }
(generator_function_declaration name: (identifier) @definition) ; function *f() { ... }
(required_parameter (identifier) @definition)                   ; function(x) { ... }
(required_parameter (rest_pattern (identifier) @definition))    ; function(...x) { ... }
(optional_parameter (identifier) @definition)                   ; function(x?) { ... }
(optional_parameter (rest_pattern (identifier) @definition))    ; function(...x?) { ... }
(arrow_function parameter: (identifier) @definition)            ; x => ...
(for_in_statement left: (identifier) @definition)               ; for (const x of xs) ...
(catch_clause parameter: (identifier) @definition)              ; catch (e) ...
"#;

const CPP_LOCALS_QUERY: &str = r#"
(compound_statement)  @scope ; { ... }
(for_statement)       @scope ; for (int i = 0; ...) ...
(for_range_loop)      @scope ; for (int x : xs) ...
(catch_clause)        @scope ; catch (e) ...
(lambda_expression)   @scope ; [](auto x) { ... }
(function_definition) @scope ; void f() { ... }

(declaration                    declarator: (identifier) @definition)                        ; int x;
(init_declarator                declarator: (identifier) @definition)                        ; int x = 5;
(parameter_declaration          declarator: (identifier) @definition)                        ; [](auto x) { ... }
(parameter_declaration          declarator: (reference_declarator (identifier) @definition)) ; [](int& x) { ... }
(parameter_declaration          declarator: (pointer_declarator   (identifier) @definition)) ; [](int* x) { ... }
(optional_parameter_declaration declarator: (identifier) @definition)                        ; [](auto x = 5) { ... }
(for_range_loop declarator: (identifier) @definition)									     ; for (int x : xs) ...
"#;

const RUBY_LOCALS_QUERY: &str = r#"
(method)   @scope ; def f() ...
(block)    @scope ; { ... }
(do_block) @scope ; do ... end

(exception_variable   (identifier) @definition)          ; rescue ArgumentError => e
(method_parameters    (identifier) @definition)          ; def f(x) ...
(optional_parameter   name: (identifier) @definition)    ; def f(x = 5) ...
(splat_parameter      name: (identifier) @definition)    ; def f(*x) ...
(hash_splat_parameter name: (identifier) @definition)    ; def f(**x) ...
(block_parameters     (identifier) @definition)          ; |x| ...
(assignment           left: (identifier) @definition)    ; x = ...
(left_assignment_list (identifier) @definition)          ; x, y = ...
(for                  pattern: (identifier) @definition) ; for i in 1..5 ...
"#;

const PYTHON_LOCALS_QUERY: &str = r#"
(function_definition)     @scope ; def f(): ...
(lambda)                  @scope ; lambda ...: ...
(generator_expression)    @scope ; (x for x in xs)
(list_comprehension)      @scope ; [x for x in xs]

(parameters                    (identifier) @definition)                                   ; def f(x): ...
(typed_parameter               (identifier) @definition)                                   ; def f(x: bool): ...
(default_parameter       name: (identifier) @definition)                                   ; def f(x = False): ...
(typed_default_parameter name: (identifier) @definition)                                   ; def f(x: bool = False): ...
(except_clause
  (as_pattern (identifier) alias: (as_pattern_target (identifier) @definition)))           ; except Exception as e: ...
(expression_statement          (assignment left: (identifier) @definition))                ; x = ...
(expression_statement          (assignment left: (pattern_list (identifier) @definition))) ; x, y = ...
(for_statement           left: (identifier) @definition)                                   ; for x in ...: ...
(for_statement           left: (pattern_list (identifier) @definition))                    ; for x, y in ...: ...
(for_in_clause           left: (identifier) @definition)                                   ; (... for x in xs)
(for_in_clause           left: (pattern_list (identifier) @definition))                    ; (... for x, y in xs)
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_extensions_like_the_baseline() {
        assert_eq!(lang_for_path("a/b.java").expect("java").name, "java");
        assert_eq!(lang_for_path("BUILD").expect("starlark").name, "starlark");
        assert_eq!(lang_for_path("a/Sample.JAVA").expect("java").name, "java");
        assert!(matches!(
            lang_for_path("README.txt"),
            Err(Error::UnrecognizedFileExtension)
        ));
        assert!(matches!(
            lang_for_path("a/main.rs"),
            Err(Error::UnsupportedLanguage)
        ));
    }

    #[test]
    fn joins_like_filepath() {
        assert_eq!(join_path(&[".", "a", "b"]), "a/b");
        assert_eq!(join_path(&["."]), ".");
        assert_eq!(join_path(&[""]), "");
        assert_eq!(join_path(&["a/b", "c"]), "a/b/c");
        assert_eq!(path_dir("a/b/c.py"), "a/b");
        assert_eq!(path_dir("c.py"), ".");
        assert_eq!(path_base("a/b.py"), "b.py");
        assert_eq!(path_ext("a/b.py"), ".py");
    }
}
