#![forbid(unsafe_code)]

//! Language detection used to pick a ctags engine, mirroring
//! `lib/codeintel/languages.GetMostLikelyLanguage` plus
//! `ctags_config.GetParserType`.
//!
//! The baseline delegates to go-enry, whose generated extension table covers
//! every language known to GitHub Linguist. The pilot ships a curated subset:
//! enough to classify the languages accepted by the pinned ctags build. A file
//! that is not classified is skipped, exactly like an unsupported language in
//! the baseline.

/// Engine used to parse a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParserType {
    /// `universal-ctags` (`CTAGS_COMMAND`).
    Universal,
    /// `scip-ctags` (`SCIP_CTAGS_COMMAND`).
    Scip,
}

/// `ctags_config.DefaultEngines`; all use scip-ctags by default.
const SCIP_LANGUAGES: &[&str] = &[
    "c_sharp",
    "go",
    "javascript",
    "hack",
    "kotlin",
    "magik",
    "python",
    "ruby",
    "rust",
    "scala",
    "typescript",
    "zig",
];

/// `languages.NormalizeLanguage`.
pub fn normalize(language: &str) -> String {
    let lower = language.to_lowercase();
    if lower == "c#" {
        "c_sharp".to_owned()
    } else {
        lower
    }
}

/// `ctags_config.GetParserType` + `LanguageSupportsParserType`.
pub fn parser_type(language: &str) -> ParserType {
    let normalized = normalize(language);
    if SCIP_LANGUAGES.contains(&normalized.as_str()) {
        ParserType::Scip
    } else {
        ParserType::Universal
    }
}

/// Best-effort equivalent of `languages.GetLanguages` + `GetMostLikelyLanguage`
/// for the languages supported by the pinned ctags build.
pub fn detect(path: &str, content: &[u8]) -> Option<String> {
    let base = path.rsplit('/').next().unwrap_or(path);
    if let Some(language) = by_filename(base) {
        return Some(language.to_owned());
    }

    if let Some(language) = by_extension(base) {
        return Some(language.to_owned());
    }

    by_shebang(content).map(str::to_owned)
}

fn by_filename(base: &str) -> Option<&'static str> {
    match base {
        ".bashrc" | ".bash_profile" | ".profile" | ".zshrc" | ".zshenv" | ".zprofile" => {
            Some("Shell")
        }
        "Rakefile" | "Gemfile" | "Guardfile" | "Podfile" | "Brewfile" => Some("Ruby"),
        "Makefile" | "makefile" | "GNUmakefile" => Some("Makefile"),
        "Dockerfile" | "Containerfile" => Some("Dockerfile"),
        "CMakeLists.txt" => Some("CMake"),
        _ => None,
    }
}

fn by_extension(base: &str) -> Option<&'static str> {
    let extension = base.rsplit('.').next()?;
    if extension == base {
        return None;
    }
    Some(match extension.to_ascii_lowercase().as_str() {
        "go" => "Go",
        "py" | "pyi" | "pyw" => "Python",
        "js" | "mjs" | "cjs" | "jsx" => "JavaScript",
        "ts" => "TypeScript",
        "tsx" => "TSX",
        "java" => "Java",
        "kt" | "kts" => "Kotlin",
        "rb" | "rake" | "gemspec" => "Ruby",
        "cs" => "C#",
        "rs" => "Rust",
        "scala" | "sc" => "Scala",
        "hk" => "Hack",
        "magik" => "Magik",
        "zig" => "Zig",
        "c" => "C",
        // enry reports several candidates for headers and `GetMostLikelyLanguage`
        // takes the first; C comes first.
        "h" => "C",
        "cc" | "cpp" | "cxx" | "c++" | "hpp" | "hh" | "hxx" | "h++" => "C++",
        "m" => "Objective-C",
        "mm" => "Objective-C++",
        "cu" | "cuh" => "CUDA",
        "php" | "php3" | "php4" | "php5" | "phtml" => "PHP",
        "pl" | "pm" | "t" => "Perl",
        "lua" => "Lua",
        "erl" | "hrl" => "Erlang",
        "ex" | "exs" => "Elixir",
        "clj" | "cljs" | "cljc" | "edn" => "Clojure",
        "ml" | "mli" => "OCaml",
        "r" => "R",
        "sh" | "bash" | "bats" | "command" => "Shell",
        "zsh" => "Zsh",
        "md" | "markdown" => "Markdown",
        "proto" => "Protocol Buffer",
        "thrift" => "Thrift",
        "tcl" | "tk" => "Tcl",
        "vhd" | "vhdl" => "VHDL",
        "v" | "vh" => "Verilog",
        "sv" | "svh" => "SystemVerilog",
        "vim" => "Vim",
        "el" => "Emacs Lisp",
        "lisp" | "lsp" | "cl" => "Common Lisp",
        "scm" | "ss" => "Scheme",
        "pas" | "pp" | "dpr" => "Pascal",
        "cbl" | "cob" | "cpy" => "Cobol",
        "groovy" | "gradle" => "Groovy",
        "graphql" | "gql" => "GraphQL",
        "d" => "D",
        "elm" => "Elm",
        "hs" | "lhs" => "Haskell",
        "ps1" | "psm1" | "psd1" => "PowerShell",
        "p6" | "pm6" | "raku" | "rakumod" => "Raku",
        _ => return None,
    })
}

/// Interpreter detection for extension-less scripts. Only consulted when the
/// filename and extension strategies fail.
fn by_shebang(content: &[u8]) -> Option<&'static str> {
    let first_line_end = content
        .iter()
        .position(|byte| *byte == b'\n')
        .unwrap_or(content.len());
    let first_line = std::str::from_utf8(&content[..first_line_end]).ok()?;
    let mut parts = first_line.strip_prefix("#!")?.split_whitespace();
    let mut interpreter = parts.next()?;
    if interpreter.rsplit('/').next() == Some("env") {
        // `#!/usr/bin/env [-S] python3`
        interpreter = parts.find(|part| !part.starts_with('-'))?;
    }
    let interpreter = interpreter.rsplit('/').next().unwrap_or(interpreter);
    Some(match interpreter {
        "sh" | "bash" | "dash" | "ksh" | "zsh" => "Shell",
        "python" | "python2" | "python3" => "Python",
        "ruby" => "Ruby",
        "perl" => "Perl",
        "node" | "nodejs" => "JavaScript",
        "php" => "PHP",
        "lua" => "Lua",
        "tclsh" | "wish" => "Tcl",
        "elixir" => "Elixir",
        "escript" => "Erlang",
        "scala" => "Scala",
        _ => return None,
    })
}

/// `store.convertEnryToCTagsLangs` up to lower-casing.
pub fn enry_to_ctags_langs(language: &str) -> Vec<String> {
    let lower = language.to_lowercase();
    match lower.as_str() {
        "protocol buffer" => vec!["protobuf".to_owned()],
        "objective-c" => vec!["objectivec".to_owned()],
        "assembly" => vec!["asm".to_owned()],
        "raku" => vec!["perl6".to_owned()],
        "shell" => vec!["sh".to_owned(), "zsh".to_owned()],
        _ => vec![lower],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_common_languages() {
        assert_eq!(detect("src/main.go", b""), Some("Go".into()));
        assert_eq!(detect("a/b.tsx", b""), Some("TSX".into()));
        assert_eq!(detect("script.pl", b""), Some("Perl".into()));
        assert_eq!(
            detect("bin/tool", b"#!/usr/bin/env python3\nprint(1)\n"),
            Some("Python".into())
        );
        assert_eq!(detect("README", b"no shebang"), None);
    }

    #[test]
    fn picks_the_baseline_engine() {
        assert_eq!(parser_type("Go"), ParserType::Scip);
        assert_eq!(parser_type("C#"), ParserType::Scip);
        assert_eq!(parser_type("typescript"), ParserType::Scip);
        assert_eq!(parser_type("TSX"), ParserType::Universal);
        assert_eq!(parser_type("Perl"), ParserType::Universal);
    }

    #[test]
    fn maps_enry_names_to_ctags_names() {
        assert_eq!(enry_to_ctags_langs("Shell"), vec!["sh", "zsh"]);
        assert_eq!(enry_to_ctags_langs("Protocol Buffer"), vec!["protobuf"]);
        assert_eq!(enry_to_ctags_langs("Magik"), vec!["magik"]);
    }
}
