#![forbid(unsafe_code)]

//! Port of the parts of `symbol.go` used by `packageFromSymbol`:
//! `ParseSymbol` including descriptor validation.

/// `precise.Package`: the package a symbol belongs to.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Package {
    pub scheme: String,
    pub manager: String,
    pub name: String,
    pub version: String,
}

/// `scip.IsLocalSymbol`: local symbols are not associated with a package.
pub fn is_local_symbol(symbol: &str) -> bool {
    symbol.starts_with("local ")
}

fn is_simple_identifier_character(c: char) -> bool {
    c == '_'
        || c == '+'
        || c == '-'
        || c == '$'
        || c.is_ascii_lowercase()
        || c.is_ascii_uppercase()
        || c.is_ascii_digit()
}

fn current(chars: &[char], index: usize) -> char {
    chars.get(index).copied().unwrap_or('\0')
}

/// Go's `peekNext` checks `index+1 < len` but returns `Symbol[index]`, i.e. the
/// current character. Replicated verbatim because the descriptor parser depends
/// on the edge case at the end of the symbol.
fn peek_next(chars: &[char], index: usize) -> char {
    if index + 1 < chars.len() {
        current(chars, index)
    } else {
        '\0'
    }
}

/// `acceptEscapedIdentifier`.
fn accept_escaped_identifier(
    chars: &[char],
    index: &mut usize,
    escape_character: char,
) -> Result<String, ()> {
    let mut builder = String::new();
    while *index < chars.len() {
        let ch = chars[*index];
        if ch == escape_character {
            *index += 1;
            if *index >= chars.len() {
                break;
            }
            if chars[*index] == escape_character {
                // Escaped escape character.
                builder.push(ch);
            } else {
                return Ok(builder);
            }
        } else {
            builder.push(ch);
        }
        *index += 1;
    }
    Err(())
}

fn accept_space_escaped_identifier(chars: &[char], index: &mut usize) -> Result<String, ()> {
    accept_escaped_identifier(chars, index, ' ')
}

/// `acceptIdentifier`: backtick-escaped or simple identifier characters.
fn accept_identifier(chars: &[char], index: &mut usize) -> Result<String, ()> {
    if current(chars, *index) == '`' {
        *index += 1;
        return accept_escaped_identifier(chars, index, '`');
    }

    let start = *index;
    while *index < chars.len() && is_simple_identifier_character(chars[*index]) {
        *index += 1;
    }
    if start == *index {
        return Err(());
    }
    Ok(chars[start..*index].iter().collect())
}

fn accept_character(chars: &[char], index: &mut usize, expected: char) -> Result<(), ()> {
    if current(chars, *index) == expected {
        *index += 1;
        Ok(())
    } else {
        Err(())
    }
}

/// `parseDescriptor`.
fn parse_descriptor(chars: &[char], index: &mut usize) -> Result<(), ()> {
    match peek_next(chars, *index) {
        '(' => {
            *index += 1;
            accept_identifier(chars, index)?;
            accept_character(chars, index, ')')
        }
        '[' => {
            *index += 1;
            accept_identifier(chars, index)?;
            accept_character(chars, index, ']')
        }
        _ => {
            accept_identifier(chars, index)?;
            let suffix = current(chars, *index);
            *index += 1;
            match suffix {
                '(' => {
                    if peek_next(chars, *index) != ')' {
                        accept_identifier(chars, index)?;
                    }
                    accept_character(chars, index, ')')?;
                    accept_character(chars, index, '.')
                }
                '/' | '.' | '#' | ':' | '!' => Ok(()),
                _ => Err(()),
            }
        }
    }
}

/// `parseDescriptors`.
fn parse_descriptors(chars: &[char], index: &mut usize) -> Result<(), ()> {
    while *index < chars.len() {
        parse_descriptor(chars, index)?;
    }
    Ok(())
}

/// `packageFromSymbol`: the parsed package, or `None` when the symbol has no
/// package (local symbols) or cannot be parsed.
pub fn package_from_symbol(symbol: &str) -> Option<Package> {
    // `ParseSymbol` succeeds for a well-formed local symbol, but its
    // `Package` field is nil; malformed local symbols fail to parse. Both
    // cases yield no package.
    if is_local_symbol(symbol) {
        return None;
    }

    let chars: Vec<char> = symbol.chars().collect();
    let mut index = 0;
    let scheme = accept_space_escaped_identifier(&chars, &mut index).ok()?;
    let mut manager = accept_space_escaped_identifier(&chars, &mut index).ok()?;
    if manager == "." {
        manager = String::new();
    }
    let mut name = accept_space_escaped_identifier(&chars, &mut index).ok()?;
    if name == "." {
        name = String::new();
    }
    let mut version = accept_space_escaped_identifier(&chars, &mut index).ok()?;
    if version == "." {
        version = String::new();
    }
    parse_descriptors(&chars, &mut index).ok()?;

    if name.is_empty() || version.is_empty() {
        return None;
    }

    Some(Package {
        scheme,
        manager,
        name,
        version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pkg(scheme: &str, manager: &str, name: &str, version: &str) -> Package {
        Package {
            scheme: scheme.to_owned(),
            manager: manager.to_owned(),
            name: name.to_owned(),
            version: version.to_owned(),
        }
    }

    #[test]
    fn parses_package_symbols() {
        assert_eq!(
            package_from_symbol("scip-go gomod github.com/x/y v1.2.3 Foo#Bar."),
            Some(pkg("scip-go", "gomod", "github.com/x/y", "v1.2.3"))
        );
    }

    #[test]
    fn dot_package_components_are_empty() {
        assert_eq!(
            package_from_symbol("scip-go . . . Foo#"),
            None,
            "empty name and version have no package"
        );
    }

    #[test]
    fn local_and_malformed_symbols_have_no_package() {
        assert_eq!(package_from_symbol("local 1"), None);
        assert_eq!(package_from_symbol("scip-go gomod name v1"), None);
        assert_eq!(package_from_symbol("not a symbol"), None);
    }

    #[test]
    fn descriptor_grammar_is_validated() {
        assert_eq!(
            package_from_symbol("s m n 1 pkg/type#method()."),
            Some(pkg("s", "m", "n", "1"))
        );
        assert_eq!(package_from_symbol("s m n 1 type#bogus%"), None);
    }
}
