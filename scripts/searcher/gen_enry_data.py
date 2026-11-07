#!/usr/bin/env python3
"""Generate `langmatch/enry_data.rs` from go-enry and the Sourcegraph wrapper.

The searcher's `lang:` filters use Sourcegraph's `lib/codeintel/languages`
package, which wraps go-enry v2.8.8 (data from github/linguist commit
4ac734c1). Reimplementing the data by hand would drift, so this script
translates the generated Go tables to Rust:

  * data.LanguagesByExtension / LanguagesByFilename
  * data.ExtensionsByLanguage
  * data.LanguageByAliasMap
  * data.LanguagesByInterpreter
  * data.ContentHeuristics (the RE2 rules; Ruby-only rules become `Ruby`)

plus the Sourcegraph overrides from `lib/codeintel/languages/extensions.go`
(ambiguous extensions, binary extensions, languages enry does not know).

Usage:
    python3 -I scripts/searcher/gen_enry_data.py \
        --enry /path/to/go-enry/v2@v2.8.8/data \
        --languages /path/to/src/lib/codeintel/languages/extensions.go \
        --out crates/services/sg-service-searcher/src/langmatch/enry_data.rs
"""

import argparse
import re


def unquote_go(token: str) -> str:
    """Decode a Go interpreted string literal (the surrounding quotes included)."""
    assert token[0] == '"' and token[-1] == '"', token
    body = token[1:-1]
    out = []
    i = 0
    while i < len(body):
        ch = body[i]
        if ch != "\\":
            out.append(ch)
            i += 1
            continue
        i += 1
        esc = body[i]
        simple = {
            "a": "\a", "b": "\b", "f": "\f", "n": "\n", "r": "\r",
            "t": "\t", "v": "\v", "\\": "\\", '"': '"', "'": "'",
        }
        if esc in simple:
            out.append(simple[esc])
            i += 1
        elif esc == "x":
            out.append(chr(int(body[i + 1:i + 3], 16)))
            i += 3
        elif esc == "u":
            out.append(chr(int(body[i + 1:i + 5], 16)))
            i += 5
        elif esc == "U":
            out.append(chr(int(body[i + 1:i + 9], 16)))
            i += 9
        elif esc.isdigit():
            digits = esc
            i += 1
            while i < len(body) and len(digits) < 3 and body[i].isdigit():
                digits += body[i]
                i += 1
            out.append(chr(int(digits, 8)))
        else:
            raise ValueError(f"unsupported Go escape \\{esc}")
    return "".join(out)


MAP_START = re.compile(r"^var (\w+) = (?:map\[[^\]]+\][^{]+)\{$")
ENTRY_STR_SLICE = re.compile(r'^\t("(?:[^"\\]|\\.)*"):\s*\{(.*)\},$')
ENTRY_STR = re.compile(r'^\t("(?:[^"\\]|\\.)*"):\s*("(?:[^"\\]|\\.)*"),$')


def parse_go_maps(path: str):
    """Return {name: {key: value}} for the generated map literals in `path`."""
    with open(path, encoding="utf-8") as handle:
        lines = handle.read().splitlines()
    maps = {}
    idx = 0
    while idx < len(lines):
        match = MAP_START.match(lines[idx])
        if not match:
            idx += 1
            continue
        name = match.group(1)
        idx += 1
        entries = {}
        while idx < len(lines) and lines[idx] != "}":
            line = lines[idx]
            idx += 1
            slice_entry = ENTRY_STR_SLICE.match(line)
            if slice_entry:
                key = unquote_go(slice_entry.group(1))
                body = slice_entry.group(2).strip()
                values = []
                if body:
                    for token in re.findall(r'"(?:[^"\\]|\\.)*"', body):
                        values.append(unquote_go(token))
                entries[key] = values
                continue
            str_entry = ENTRY_STR.match(line)
            if str_entry:
                entries[unquote_go(str_entry.group(1))] = unquote_go(str_entry.group(2))
                continue
            if line.strip():
                raise ValueError(f"unparsed map line in {path}: {line!r}")
        idx += 1
        maps[name] = entries
    return maps


# ---------------------------------------------------------------------------
# Content heuristics (data/content.go)
# ---------------------------------------------------------------------------

class Tok:
    def __init__(self, kind, value):
        self.kind = kind
        self.value = value


def tokenize(source: str):
    tokens = []
    i = 0
    while i < len(source):
        ch = source[i]
        if ch.isspace():
            i += 1
        elif source.startswith("//", i):
            end = source.find("\n", i)
            i = len(source) if end < 0 else end + 1
        elif ch == "`":
            end = source.find("`", i + 1)
            if end < 0:
                raise ValueError("unterminated raw string")
            tokens.append(Tok("str", source[i + 1:end]))
            i = end + 1
        elif ch == '"':
            end = i + 1
            while source[end] != '"':
                end += 2 if source[end] == "\\" else 1
            tokens.append(Tok("str", unquote_go(source[i:end + 1])))
            i = end + 1
        elif ch.isalnum() or ch in "_.":
            end = i
            while end < len(source) and (source[end].isalnum() or source[end] in "_."):
                end += 1
            tokens.append(Tok("ident", source[i:end]))
            i = end
        elif ch in "(){},&=:*[]+":
            tokens.append(Tok(ch, ch))
            i += 1
        else:
            raise ValueError(f"unexpected character {ch!r} in heuristics")
    return tokens


class Parser:
    def __init__(self, tokens):
        self.tokens = tokens
        self.pos = 0

    def peek(self):
        if self.pos >= len(self.tokens):
            return None
        return self.tokens[self.pos]

    def next(self):
        token = self.peek()
        if token is None:
            raise ValueError("unexpected end of heuristics")
        self.pos += 1
        return token

    def expect(self, kind):
        token = self.next()
        if token.kind != kind:
            raise ValueError(f"expected {kind}, got {token.kind} ({token.value!r})")
        return token

    def parse_heuristics_map(self):
        """`var ContentHeuristics = map[string]*Heuristics{ ... }`"""
        self.expect("ident")  # var
        self.expect("ident")  # ContentHeuristics
        self.expect("=")
        self.expect("ident")  # map
        self.expect("[")
        self.expect("ident")
        self.expect("]")
        self.expect("*")
        self.expect("ident")  # Heuristics
        self.expect("{")
        entries = []
        while self.peek() is not None and self.peek().kind != "}":
            key = self.expect("str").value
            self.expect(":")
            self.expect("&")
            self.expect("ident")  # Heuristics
            self.expect("{")
            rules = []
            while self.peek().kind != "}":
                rules.append(self.parse_call())
                if self.peek() is not None and self.peek().kind == ",":
                    self.next()
            self.expect("}")
            if self.peek() is not None and self.peek().kind == ",":
                self.next()
            entries.append((key, rules))
        self.expect("}")
        return entries

    def parse_call(self):
        name = self.expect("ident").value
        self.expect("(")
        args = []
        while self.peek().kind != ")":
            token = self.next()
            if token.kind == "str":
                # Go source concatenates raw strings with `+` to embed a
                # backtick (`+"`"+`).
                value = token.value
                while self.peek() is not None and self.peek().kind == "+":
                    self.next()
                    value += self.expect("str").value
                args.append(("str", value))
            else:
                self.pos -= 1
                args.append(("call", self.parse_call()))
            if self.peek().kind == ",":
                self.next()
        self.expect(")")
        return (name, args)


def matching_languages(call):
    """Return the language names if `call` is `rule.MatchingLanguages(...)`."""
    name, args = call
    if name != "rule.MatchingLanguages":
        return None
    return [value for kind, value in args if kind == "str"]


def rule_from_call(name, args, patterns):
    if name == "regex.MustCompileRuby":
        return ("ruby", [])
    if name in ("regex.MustCompile", "regex.MustCompileMultiline"):
        lang_args = [value for kind, value in args if kind == "str"]
        assert len(lang_args) == 1, (name, args)
        pattern = lang_args[0]
        if name == "regex.MustCompileMultiline":
            pattern = "(?m)" + pattern
        patterns.append(pattern)
        return ("pattern", [len(patterns) - 1])

    assert name.startswith("rule."), name
    # Every rule constructor takes the rule's language label as its first
    # argument; only the top-level Heuristic uses it.
    if args and args[0][0] == "call" and matching_languages(args[0][1]) is not None:
        args = args[1:]
    children = [
        rule_from_call(child_name, child_args, patterns)
        for child_name, child_args in (value for kind, value in args if kind == "call")
    ]
    if name == "rule.Always":
        return ("always", [])
    if name == "rule.And":
        return ("and", children)
    if name == "rule.Or":
        return ("or", children)
    if name == "rule.Not":
        return ("not", children)
    raise ValueError(f"unknown rule function {name}")


def parse_heuristics(path):
    with open(path, encoding="utf-8") as handle:
        source = handle.read()
    start = source.index("var ContentHeuristics")
    parser = Parser(tokenize(source[start:]))
    raw_entries = parser.parse_heuristics_map()
    patterns = []
    entries = []
    for extension, calls in raw_entries:
        heuristics = []
        for name, args in calls:
            # Each top-level call is one Heuristic whose first argument is
            # `rule.MatchingLanguages(...)` naming the languages it detects.
            first = args[0] if args and args[0][0] == "call" else None
            languages = matching_languages(first[1]) if first is not None else None
            if languages is None:
                raise ValueError(f"heuristic for {extension} has no languages: {name}")
            rule = rule_from_call(name, args, patterns)
            heuristics.append((languages, rule))
        entries.append((extension, heuristics))
    return entries, patterns


# ---------------------------------------------------------------------------
# Emit Rust
# ---------------------------------------------------------------------------

def rs(value: str) -> str:
    out = []
    for ch in value:
        if ch == '"':
            out.append('\\"')
        elif ch == "\\":
            out.append("\\\\")
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\r":
            out.append("\\r")
        elif ch == "\t":
            out.append("\\t")
        elif ord(ch) < 0x20:
            out.append(f"\\u{{{ord(ch):x}}}")
        else:
            out.append(ch)
    return '"' + "".join(out) + '"'


def rs_string(value: str) -> str:
    """Rust string literal; prefers raw strings so regexes stay readable."""
    if '"#' not in value and '"' not in value:
        return 'r#"' + value + '"#'
    return rs(value)


def emit_slice_map(name, mapping, doc):
    lines = [f"/// {doc}", f"pub static {name}: &[(&str, &[&str])] = &["]
    for key in sorted(mapping):
        values = ", ".join(rs_string(value) for value in mapping[key])
        lines.append(f"    ({rs_string(key)}, &[{values}]),")
    lines.append("];")
    lines.append("")
    return "\n".join(lines)


def emit_str_map(name, mapping, doc):
    lines = [f"/// {doc}", f"pub static {name}: &[(&str, &str)] = &["]
    for key in sorted(mapping):
        lines.append(f"    ({rs_string(key)}, {rs_string(mapping[key])}),")
    lines.append("];")
    lines.append("")
    return "\n".join(lines)


def emit_rule(rule, indent):
    kind, value = rule
    pad = "    " * indent
    if kind == "langs":
        # Nested languages are not used by the baseline (only the top-level
        # Heuristic exposes Languages()); emit a no-op.
        return "Rule::Always"
    if kind == "always":
        return "Rule::Always"
    if kind == "ruby":
        return "Rule::Ruby"
    if kind == "pattern":
        return f"Rule::Pattern({value[0]})"
    children = value
    if kind == "and":
        if not children:
            return "Rule::Always"
        inner = ",\n".join(f"{pad}    {emit_rule(child, indent + 1)}" for child in children)
        return f"Rule::And(&[\n{inner}\n{pad}])"
    if kind == "or":
        if not children:
            return "Rule::Ruby"
        inner = ",\n".join(f"{pad}    {emit_rule(child, indent + 1)}" for child in children)
        return f"Rule::Or(&[\n{inner}\n{pad}])"
    if kind == "not":
        if not children:
            return "Rule::Always"
        inner = ",\n".join(f"{pad}    {emit_rule(child, indent + 1)}" for child in children)
        return f"Rule::Not(&[\n{inner}\n{pad}])"
    raise ValueError(kind)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--enry", required=True, help="go-enry v2 data directory")
    parser.add_argument(
        "--languages",
        required=True,
        help="Sourcegraph lib/codeintel/languages/extensions.go",
    )
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    extension_maps = parse_go_maps(args.enry + "/extension.go")
    filename_maps = parse_go_maps(args.enry + "/filename.go")
    alias_maps = parse_go_maps(args.enry + "/alias.go")
    interpreter_maps = parse_go_maps(args.enry + "/interpreter.go")

    by_extension = extension_maps["LanguagesByExtension"]
    by_filename = filename_maps["LanguagesByFilename"]
    extensions_by_language = extension_maps["ExtensionsByLanguage"]
    aliases = alias_maps["LanguageByAliasMap"]
    interpreters = interpreter_maps["LanguagesByInterpreter"]

    heuristics, patterns = parse_heuristics(args.enry + "/content.go")

    with open(args.languages, encoding="utf-8") as handle:
        languages_source = handle.read()

    def block(source, header):
        start = source.index(header)
        end = source.index("\n}", start)
        return source[start:end]

    def go_map(source, name, value_kind):
        if value_kind != "str":
            raise AssertionError(value_kind)
        text = block(source, f"var {name} = map[string]string{{")
        return dict(
            (unquote_go('"' + k + '"'), unquote_go(v))
            for k, v in re.findall(
                r'"((?:[^"\\]|\\.)*)"\s*:\s*("(?:[^"\\]|\\.)*")', text
            )
        )

    def go_list(source, name):
        text = block(source, f"var {name} = []string{{")
        return [
            unquote_go(token)
            for token in re.findall(r'"(?:[^"\\]|\\.)*"', text)
        ]

    overrides = go_map(languages_source, "overrideAmbiguousExtensionsMap", "str")
    unsupported = go_map(languages_source, "unsupportedByEnryExtensionToNameMap", "str")
    binary_extensions = go_list(languages_source, "commonBinaryFileExtensionsList")

    # `ExtensionsByLanguage` is what `enry.GetLanguageExtensions` returns, but
    # Sourcegraph removes "niche" extensions for languages that also claim an
    # extension we override (e.g. TypeScript vs XML for .ts).
    considered = set(overrides.values()) | set(unsupported.values())
    niche = {}
    for extension in list(overrides) + list(unsupported):
        for language in by_extension.get(extension, []):
            if language not in considered:
                niche.setdefault(language, set()).add(extension)
    for language, removed in niche.items():
        if language in extensions_by_language:
            extensions_by_language[language] = [
                ext for ext in extensions_by_language[language] if ext not in removed
            ]
    # `GetLanguageExtensions` special-cases these languages and returns only
    # their unsupported-map extensions (enry does not know them).
    for language, extensions in reversed_unsupported(unsupported).items():
        extensions_by_language[language] = extensions

    out = []
    out.append(
        "// @generated by `scripts/searcher/gen_enry_data.py` — do not edit.\n"
        "//\n"
        "// Source: go-enry v2.8.8 (`data/`), generated from github/linguist\n"
        "// commit 4ac734c15a96f9e16fd12330d0cb8de82274f700, plus the\n"
        "// Sourcegraph wrapper in `lib/codeintel/languages/extensions.go` at\n"
        "// c864f15af264f0f456a6d8a83290b5c940715349.\n"
        "//\n"
        "// Regenerate with the command in the script's doc comment.\n"
        "#![allow(clippy::all)]\n"
    )
    out.append("/// Content heuristic rules (`data.ContentHeuristics`).")
    out.append("pub enum Rule {")
    out.append("    And(&'static [Rule]),")
    out.append("    Or(&'static [Rule]),")
    out.append("    Not(&'static [Rule]),")
    out.append("    Always,")
    out.append("    /// A RE2-compatible pattern, indexed in [`PATTERNS`].")
    out.append("    Pattern(usize),")
    out.append("    /// A Ruby/Oniguruma-only rule; the default go-enry build skips it.")
    out.append("    Ruby,")
    out.append("}")
    out.append("")
    out.append("pub struct Heuristic {")
    out.append("    /// Canonical language names (aliases resolved at generation time).")
    out.append("    pub languages: &'static [&'static str],")
    out.append("    pub rule: Rule,")
    out.append("}")
    out.append("")
    out.append("/// Heuristic lists by file extension (`data.ContentHeuristics`).")
    out.append("pub static HEURISTICS: &[(&str, &[Heuristic])] = &[")
    for extension, heuristic_list in heuristics:
        out.append(f"    ({rs_string(extension)}, &[")
        for languages, rule in heuristic_list:
            out.append("        Heuristic {")
            langs = ", ".join(rs_string(lang) for lang in languages)
            out.append(f"            languages: &[{langs}],")
            out.append(f"            rule: {emit_rule(rule, 3)},")
            out.append("        },")
        out.append("    ]),")
    out.append("];")
    out.append("")
    out.append("/// Every heuristic pattern, compiled lazily by `langmatch`.")
    out.append("pub static PATTERNS: &[&str] = &[")
    for pattern in patterns:
        out.append(f"    {rs_string(pattern)},")
    out.append("];")
    out.append("")
    out.append(emit_slice_map("LANGUAGES_BY_EXTENSION", by_extension, "`data.LanguagesByExtension`."))
    out.append(emit_slice_map("LANGUAGES_BY_FILENAME", by_filename, "`data.LanguagesByFilename`."))
    out.append(emit_slice_map("EXTENSIONS_BY_LANGUAGE", extensions_by_language, "`data.ExtensionsByLanguage` (after Sourcegraph's niche removal)."))
    out.append(emit_str_map("LANGUAGE_ALIASES", aliases, "`data.LanguageByAliasMap`."))
    out.append(emit_slice_map("LANGUAGES_BY_INTERPRETER", interpreters, "`data.LanguagesByInterpreter`."))
    out.append(emit_str_map("OVERRIDE_AMBIGUOUS_EXTENSIONS", overrides, "`overrideAmbiguousExtensionsMap`."))
    out.append(emit_str_map("UNSUPPORTED_EXTENSIONS", unsupported, "`unsupportedByEnryExtensionToNameMap`."))
    out.append("/// `commonBinaryFileExtensionsList` (without the leading dot).")
    out.append("pub static COMMON_BINARY_EXTENSIONS: &[&str] = &[")
    for extension in binary_extensions:
        out.append(f"    {rs_string(extension)},")
    out.append("];")
    out.append("")

    with open(args.out, "w", encoding="utf-8") as handle:
        handle.write("\n".join(out))
    print(
        f"wrote {args.out}: {len(heuristics)} heuristic extensions, "
        f"{len(patterns)} patterns, {len(by_extension)} extensions"
    )


def reversed_unsupported(unsupported):
    out = {}
    for extension, language in unsupported.items():
        out.setdefault(language, []).append(extension)
    return out


if __name__ == "__main__":
    main()
