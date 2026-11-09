#![forbid(unsafe_code)]

//! `bindings/go/scip/symbol.go` (`ParseSymbol`) and
//! `symbol_formatter.go` (`SymbolFormatter`), plus the ranking exporter's
//! `canonicalizeSymbol`
//! (`internal/codeintel/ranking/internal/background/exporter/job.go`).
//!
//! The parser is a verbatim port, including Go's `peekNext` edge case (it
//! returns the *current* character when another character exists) and the
//! error text shape `message\nsymbol\n___^`. The formatter reproduces the
//! escaping rules: package components escape a space by doubling it (and an
//! empty component is `.`), descriptor identifiers use backticks, and
//! segments are joined by a single space.

use std::fmt;

use super::symbol::is_local_symbol;

/// `exporter.skipPrefix`.
pub const SKIP_PREFIX: &str = "lsif .";

/// `scip.Descriptor_Suffix`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DescriptorSuffix {
    Local,
    Namespace,
    Type,
    Term,
    Meta,
    Macro,
    TypeParameter,
    Parameter,
    Method,
}

/// `scip.Descriptor`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Descriptor {
    pub name: String,
    pub disambiguator: String,
    pub suffix: DescriptorSuffix,
}

/// `scip.Package` as parsed from a symbol string (no scheme).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SymbolPackage {
    pub manager: String,
    pub name: String,
    pub version: String,
}

/// `scip.Symbol`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedSymbol {
    pub scheme: String,
    /// `None` for local symbols (the baseline's `Package` field is nil).
    pub package: Option<SymbolPackage>,
    pub descriptors: Vec<Descriptor>,
}

/// A symbol that `ParseSymbol` rejects. The message mirrors the baseline's
/// multi-line error (message, symbol, caret line).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseSymbolError {
    message: String,
}

impl ParseSymbolError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ParseSymbolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ParseSymbolError {}

fn is_simple_identifier_character(c: char) -> bool {
    c == '_'
        || c == '+'
        || c == '-'
        || c == '$'
        || c.is_ascii_lowercase()
        || c.is_ascii_uppercase()
        || c.is_ascii_digit()
}

fn is_simple_identifier(value: &str) -> bool {
    !value.is_empty() && value.chars().all(is_simple_identifier_character)
}

struct SymbolParser<'a> {
    symbol: &'a str,
    chars: Vec<char>,
    index: usize,
}

impl<'a> SymbolParser<'a> {
    fn new(symbol: &'a str) -> Self {
        Self {
            symbol,
            chars: symbol.chars().collect(),
            index: 0,
        }
    }

    fn error(&self, message: &str) -> ParseSymbolError {
        ParseSymbolError::new(format!(
            "{message}\n{}\n{}^",
            self.symbol,
            "_".repeat(self.index)
        ))
    }

    fn current(&self) -> char {
        self.chars.get(self.index).copied().unwrap_or('\0')
    }

    /// Go's `peekNext` checks `index+1 < len` but returns `Symbol[index]`.
    /// Replicated verbatim; the descriptor parser depends on the edge case.
    fn peek_next(&self) -> char {
        if self.index + 1 < self.chars.len() {
            self.current()
        } else {
            '\0'
        }
    }

    fn parse_descriptors(&mut self) -> Result<Vec<Descriptor>, ParseSymbolError> {
        let mut result = Vec::new();
        while self.index < self.chars.len() {
            result.push(self.parse_descriptor()?);
        }
        Ok(result)
    }

    fn parse_descriptor(&mut self) -> Result<Descriptor, ParseSymbolError> {
        let start = self.index;
        match self.peek_next() {
            '(' => {
                self.index += 1;
                let name = self.accept_identifier("parameter name")?;
                self.accept_character(')', "closing parameter name")?;
                return Ok(Descriptor {
                    name,
                    disambiguator: String::new(),
                    suffix: DescriptorSuffix::Parameter,
                });
            }
            '[' => {
                self.index += 1;
                let name = self.accept_identifier("type parameter name")?;
                self.accept_character(']', "closing type parameter name")?;
                return Ok(Descriptor {
                    name,
                    disambiguator: String::new(),
                    suffix: DescriptorSuffix::TypeParameter,
                });
            }
            _ => {
                let name = self.accept_identifier("descriptor name")?;
                let suffix = self.current();
                self.index += 1;
                match suffix {
                    '(' => {
                        let mut disambiguator = String::new();
                        if self.peek_next() != ')' {
                            disambiguator = self.accept_identifier("method disambiguator")?;
                        }
                        self.accept_character(')', "closing method")?;
                        self.accept_character('.', "closing method")?;
                        return Ok(Descriptor {
                            name,
                            disambiguator,
                            suffix: DescriptorSuffix::Method,
                        });
                    }
                    '/' => {
                        return Ok(Descriptor {
                            name,
                            disambiguator: String::new(),
                            suffix: DescriptorSuffix::Namespace,
                        });
                    }
                    '.' => {
                        return Ok(Descriptor {
                            name,
                            disambiguator: String::new(),
                            suffix: DescriptorSuffix::Term,
                        });
                    }
                    '#' => {
                        return Ok(Descriptor {
                            name,
                            disambiguator: String::new(),
                            suffix: DescriptorSuffix::Type,
                        });
                    }
                    ':' => {
                        return Ok(Descriptor {
                            name,
                            disambiguator: String::new(),
                            suffix: DescriptorSuffix::Meta,
                        });
                    }
                    '!' => {
                        return Ok(Descriptor {
                            name,
                            disambiguator: String::new(),
                            suffix: DescriptorSuffix::Macro,
                        });
                    }
                    _ => {}
                }
            }
        }

        let end = self.index.min(self.chars.len());
        let captured: String = self.chars[start.min(end)..end].iter().collect();
        Err(self.error(&format!("unrecognized descriptor {captured:?}")))
    }

    fn accept_identifier(&mut self, what: &str) -> Result<String, ParseSymbolError> {
        if self.current() == '`' {
            self.index += 1;
            return self.accept_escaped_identifier(what, '`');
        }

        let start = self.index;
        while self.index < self.chars.len() && is_simple_identifier_character(self.current()) {
            self.index += 1;
        }
        if start == self.index {
            return Err(self.error("empty identifier"));
        }
        Ok(self.chars[start..self.index].iter().collect())
    }

    fn accept_space_escaped_identifier(&mut self, what: &str) -> Result<String, ParseSymbolError> {
        self.accept_escaped_identifier(what, ' ')
    }

    fn accept_escaped_identifier(
        &mut self,
        what: &str,
        escape_character: char,
    ) -> Result<String, ParseSymbolError> {
        let mut builder = String::new();
        while self.index < self.chars.len() {
            let ch = self.current();
            if ch == escape_character {
                self.index += 1;
                if self.index >= self.chars.len() {
                    break;
                }
                if self.current() == escape_character {
                    // Escaped escape character.
                    builder.push(ch);
                } else {
                    return Ok(builder);
                }
            } else {
                builder.push(ch);
            }
            self.index += 1;
        }

        Err(self.error(&format!(
            "reached end of symbol while parsing <{what}>, expected a '{escape_character}' character"
        )))
    }

    fn accept_character(&mut self, expected: char, what: &str) -> Result<(), ParseSymbolError> {
        if self.current() == expected {
            self.index += 1;
            Ok(())
        } else {
            Err(self.error(&format!(
                "expected '{expected}', obtained '{}', while parsing {what}",
                self.current()
            )))
        }
    }
}

/// `tryParseLocalSymbol` + `newLocalSymbol`.
fn parse_local_symbol(symbol: &str) -> Result<Option<ParsedSymbol>, ParseSymbolError> {
    let Some(suffix) = symbol.strip_prefix("local ") else {
        return Ok(None);
    };
    if is_simple_identifier(suffix) {
        return Ok(Some(ParsedSymbol {
            scheme: "local".to_owned(),
            package: None,
            descriptors: vec![Descriptor {
                name: suffix.to_owned(),
                disambiguator: String::new(),
                suffix: DescriptorSuffix::Local,
            }],
        }));
    }
    Err(ParseSymbolError::new(format!(
        "expected format 'local <simple-identifier>' but got: {symbol}"
    )))
}

/// `scip.ParseSymbol`: parse an SCIP string into the `Symbol` message.
pub fn parse_symbol(symbol: &str) -> Result<ParsedSymbol, ParseSymbolError> {
    if let Some(local) = parse_local_symbol(symbol)? {
        return Ok(local);
    }

    let mut parser = SymbolParser::new(symbol);
    let scheme = parser.accept_space_escaped_identifier("scheme")?;
    let mut manager = parser.accept_space_escaped_identifier("package manager")?;
    if manager == "." {
        manager = String::new();
    }
    let mut name = parser.accept_space_escaped_identifier("package name")?;
    if name == "." {
        name = String::new();
    }
    let mut version = parser.accept_space_escaped_identifier("package version")?;
    if version == "." {
        version = String::new();
    }
    let descriptors = parser.parse_descriptors()?;

    Ok(ParsedSymbol {
        scheme,
        package: Some(SymbolPackage {
            manager,
            name,
            version,
        }),
        descriptors,
    })
}

/// `scip.SymbolFormatter`.
#[derive(Debug, Clone, Copy)]
pub struct SymbolFormatter {
    pub on_error: fn(ParseSymbolError) -> Option<ParseSymbolError>,
    pub include_scheme: fn(&str) -> bool,
    pub include_package_manager: fn(&str) -> bool,
    pub include_package_name: fn(&str) -> bool,
    pub include_package_version: fn(&str) -> bool,
    pub include_descriptor: fn(&str) -> bool,
    pub include_raw_descriptor: fn(&Descriptor) -> bool,
    pub include_disambiguator: fn(&str) -> bool,
}

fn keep_str(_: &str) -> bool {
    true
}

fn drop_str(_: &str) -> bool {
    false
}

fn keep_descriptor(_: &Descriptor) -> bool {
    true
}

fn strict_error(err: ParseSymbolError) -> Option<ParseSymbolError> {
    Some(err)
}

fn lenient_error(_: ParseSymbolError) -> Option<ParseSymbolError> {
    None
}

impl SymbolFormatter {
    /// `VerboseSymbolFormatter`.
    pub fn verbose() -> Self {
        Self {
            on_error: strict_error,
            include_scheme: keep_str,
            include_package_manager: keep_str,
            include_package_name: keep_str,
            include_package_version: keep_str,
            include_descriptor: keep_str,
            include_raw_descriptor: keep_descriptor,
            include_disambiguator: keep_str,
        }
    }

    /// `LenientVerboseSymbolFormatter`.
    pub fn lenient_verbose() -> Self {
        Self {
            on_error: lenient_error,
            ..Self::verbose()
        }
    }

    /// `DescriptorOnlyFormatter`.
    pub fn descriptor_only() -> Self {
        fn local_scheme(scheme: &str) -> bool {
            scheme == "local"
        }

        Self {
            on_error: strict_error,
            include_scheme: local_scheme,
            include_package_manager: drop_str,
            include_package_name: drop_str,
            include_package_version: drop_str,
            include_descriptor: keep_str,
            include_raw_descriptor: keep_descriptor,
            include_disambiguator: keep_str,
        }
    }

    /// The ranking exporter's `noVersionFormatter`: everything but the
    /// package version.
    pub fn no_version() -> Self {
        Self {
            on_error: strict_error,
            include_package_version: drop_str,
            ..Self::verbose()
        }
    }

    /// `Format`: parse, then format. Parse errors are returned verbatim.
    pub fn format(&self, symbol: &str) -> Result<String, ParseSymbolError> {
        let parsed = parse_symbol(symbol)?;
        Ok(self.format_symbol(&parsed))
    }

    /// `FormatSymbol`.
    pub fn format_symbol(&self, symbol: &ParsedSymbol) -> String {
        let mut builder = String::new();
        if (self.include_scheme)(&symbol.scheme) {
            write_escaped_package(&mut builder, &symbol.scheme);
        }
        if let Some(package) = &symbol.package {
            if (self.include_package_manager)(&package.manager) {
                buffer(&mut builder);
                write_escaped_package(&mut builder, &package.manager);
            }
            if (self.include_package_name)(&package.name) {
                buffer(&mut builder);
                write_escaped_package(&mut builder, &package.name);
            }
            if (self.include_package_version)(&package.version) {
                buffer(&mut builder);
                write_escaped_package(&mut builder, &package.version);
            }
        }

        let descriptor_string = self.format_descriptors(&symbol.descriptors);
        if (self.include_descriptor)(&descriptor_string) {
            buffer(&mut builder);
            builder.push_str(&descriptor_string);
        }

        builder
    }

    /// `FormatDescriptors`.
    pub fn format_descriptors(&self, descriptors: &[Descriptor]) -> String {
        let mut builder = String::new();
        for descriptor in descriptors {
            if !(self.include_raw_descriptor)(descriptor) {
                continue;
            }

            match descriptor.suffix {
                DescriptorSuffix::Local => builder.push_str(&descriptor.name),
                DescriptorSuffix::Namespace => {
                    write_suffixed_descriptor(&mut builder, &descriptor.name, &['/'])
                }
                DescriptorSuffix::Type => {
                    write_suffixed_descriptor(&mut builder, &descriptor.name, &['#'])
                }
                DescriptorSuffix::Term => {
                    write_suffixed_descriptor(&mut builder, &descriptor.name, &['.'])
                }
                DescriptorSuffix::Meta => {
                    write_suffixed_descriptor(&mut builder, &descriptor.name, &[':'])
                }
                DescriptorSuffix::Macro => {
                    write_suffixed_descriptor(&mut builder, &descriptor.name, &['!'])
                }
                DescriptorSuffix::TypeParameter => {
                    write_sandwiched_descriptor(&mut builder, '[', &descriptor.name, &[']'])
                }
                DescriptorSuffix::Parameter => {
                    write_sandwiched_descriptor(&mut builder, '(', &descriptor.name, &[')'])
                }
                DescriptorSuffix::Method => {
                    if (self.include_disambiguator)(&descriptor.disambiguator) {
                        write_suffixed_descriptor(&mut builder, &descriptor.name, &['(']);
                        write_suffixed_descriptor(
                            &mut builder,
                            &descriptor.disambiguator,
                            &[')', '.'],
                        );
                    } else {
                        write_suffixed_descriptor(&mut builder, &descriptor.name, &['(', ')', '.']);
                    }
                }
            }
        }

        builder
    }
}

/// `writeEscapedPackage`: an empty component is `.`; spaces are doubled.
fn write_escaped_package(builder: &mut String, name: &str) {
    if name.is_empty() {
        write_generic_escaped_identifier(builder, ".", ' ');
        return;
    }
    write_generic_escaped_identifier(builder, name, ' ');
}

/// `writeSuffixedDescriptor`.
fn write_suffixed_descriptor(builder: &mut String, identifier: &str, suffixes: &[char]) {
    let escape = identifier
        .chars()
        .any(|c| !is_simple_identifier_character(c));
    if escape {
        builder.push('`');
        write_generic_escaped_identifier(builder, identifier, '`');
        builder.push('`');
    } else {
        builder.push_str(identifier);
    }

    for suffix in suffixes {
        builder.push(*suffix);
    }
}

/// `writeSandwichedDescriptor`.
fn write_sandwiched_descriptor(
    builder: &mut String,
    prefix: char,
    identifier: &str,
    suffixes: &[char],
) {
    builder.push(prefix);
    write_suffixed_descriptor(builder, identifier, suffixes);
}

/// `writeGenericEscapedIdentifier`: every occurrence of `escape` is doubled.
fn write_generic_escaped_identifier(builder: &mut String, identifier: &str, escape: char) {
    for ch in identifier.chars() {
        if ch == escape {
            builder.push(escape);
        }
        builder.push(ch);
    }
}

/// `buffer`: a single space between non-empty segments.
fn buffer(builder: &mut String) {
    if !builder.is_empty() {
        builder.push(' ');
    }
}

/// `exporter.canonicalizeSymbol`: the md5 of the symbol formatted without its
/// package version, or `None` when the symbol is skipped or cannot be parsed.
///
/// Skipped: the empty string, local symbols, and symbols under the `"lsif ."`
/// prefix.
pub fn canonicalize_symbol(symbol: &str) -> Option<[u8; 16]> {
    if symbol.is_empty() || is_local_symbol(symbol) || symbol.starts_with(SKIP_PREFIX) {
        return None;
    }

    let formatted = SymbolFormatter::no_version().format(symbol).ok()?;
    Some(md5::compute(formatted.as_bytes()).0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn format(symbol: &str) -> Result<String, ParseSymbolError> {
        SymbolFormatter::no_version().format(symbol)
    }

    #[test]
    fn parses_local_symbols() {
        assert_eq!(
            parse_symbol("local 1"),
            Ok(ParsedSymbol {
                scheme: "local".to_owned(),
                package: None,
                descriptors: vec![Descriptor {
                    name: "1".to_owned(),
                    disambiguator: String::new(),
                    suffix: DescriptorSuffix::Local,
                }],
            })
        );
        assert!(parse_symbol("local ").is_err());
        assert!(parse_symbol("local 1 2").is_err());
    }

    #[test]
    fn dot_package_components_become_empty() {
        let parsed = parse_symbol("s . . . Foo#").expect("valid symbol");
        assert_eq!(
            parsed.package,
            Some(SymbolPackage {
                manager: String::new(),
                name: String::new(),
                version: String::new(),
            })
        );
        assert_eq!(format("s . . . Foo#").expect("formats"), "s . . Foo#");
    }

    #[test]
    fn formats_descriptor_suffixes() {
        let symbol = "s m n v a/b#c.d:e![T](p)h(dis).";
        assert_eq!(
            format(symbol).expect("formats"),
            "s m n a/b#c.d:e![T](p)h(dis)."
        );
        assert_eq!(
            SymbolFormatter::verbose().format(symbol).expect("formats"),
            "s m n v a/b#c.d:e![T](p)h(dis)."
        );
        assert_eq!(format("s m n v g().").expect("formats"), "s m n g().");
    }

    #[test]
    fn escapes_packages_and_descriptors() {
        assert_eq!(format("s m a  b v Foo#").expect("formats"), "s m a  b Foo#");
        assert_eq!(format("s m n v `c d`/").expect("formats"), "s m n `c d`/");
        assert_eq!(format("s m n v `c``d`/").expect("formats"), "s m n `c``d`/");
    }

    #[test]
    fn descriptor_only_formatter_matches_the_baseline() {
        let formatter = SymbolFormatter::descriptor_only();
        assert_eq!(
            formatter.format("s m n v Foo#Bar.").expect("formats"),
            "Foo#Bar."
        );
        assert_eq!(formatter.format("local 1").expect("formats"), "local 1");
    }

    #[test]
    fn malformed_symbols_are_rejected() {
        assert!(parse_symbol("").is_err());
        assert!(parse_symbol("s m n v bad%").is_err());
        assert!(format("not a symbol").is_err());
    }

    #[test]
    fn canonicalization_ignores_versions_and_rejects_skips() {
        assert_eq!(
            canonicalize_symbol("s m n v1 Foo#Bar."),
            canonicalize_symbol("s m n v9 Foo#Bar.")
        );
        assert!(canonicalize_symbol("s m n v1 Foo#Bar.").is_some());
        assert_eq!(canonicalize_symbol(""), None);
        assert_eq!(canonicalize_symbol("local 1"), None);
        assert_eq!(canonicalize_symbol("lsif . something"), None);
        assert_eq!(canonicalize_symbol("not a symbol"), None);
    }
}
