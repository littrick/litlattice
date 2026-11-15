#![forbid(unsafe_code)]

//! `internal/paths/match.go` — glob patterns over slash-separated file paths.
//!
//! [`compile`] splits a pattern on `/`, turns `**` into "any number of path
//! parts", `*` into "exactly one path part", and any other part containing
//! `*` into a [`WildMatch`] over that single part (the `wildmatch-go`
//! semantics: `?` matches one character, `*` matches any run, no escapes).
//! A pattern without a leading `/` implicitly starts with `/**`; a trailing
//! `/` implies `/**`; and a trailing `**` is always followed by `*` so that
//! `foo/bar/**` does not match the file `foo/bar`.
//!
//! [`GlobPattern::match_path`] is the bit-vector matcher from the baseline:
//! bit `i` of the state means "the first `i` pattern parts match the path
//! prefix consumed so far"; a `**` part makes its predecessor bit persist.
//!
//! Two consecutive slashes are a compile error (`two consecutive forward
//! slashes`). Pathspecs produced by the baseline compiler are not represented
//! here: the inference service only uses the compiled globs.

const SEPARATOR: char = '/';

/// A compiled part of a glob pattern.
#[derive(Debug, Clone)]
enum PatternPart {
    /// `**`: matches any number of path parts.
    AnySubPath,
    /// A literal path part.
    Exact(String),
    /// `*`: matches exactly one path part.
    Any,
    /// A path part containing `*` (and possibly `?`).
    Asterisk(WildMatch),
}

impl PatternPart {
    fn matches(&self, part: &str) -> bool {
        match self {
            PatternPart::AnySubPath => true,
            PatternPart::Exact(exact) => exact == part,
            PatternPart::Any => true,
            PatternPart::Asterisk(pattern) => pattern.is_match(part),
        }
    }
}

/// A compiled glob pattern (`paths.GlobPattern`).
#[derive(Debug, Clone)]
pub struct GlobPattern {
    is_literal: bool,
    pattern: String,
    parts: Vec<PatternPart>,
    initial_state: i64,
}

impl GlobPattern {
    /// The original pattern text.
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    /// `GlobPattern.Match`.
    pub fn match_path(&self, file_path: &str) -> bool {
        if self.is_literal {
            return self.pattern == file_path;
        }

        // The baseline fast path for a trailing `**` is unreachable for
        // compiled patterns (Compile appends `*` after a trailing `**`), but
        // is kept for fidelity.
        if matches!(self.parts.last(), Some(PatternPart::AnySubPath)) {
            if let Some(last) = last_part(file_path) {
                if !self.parts[self.parts.len() - 1].matches(last) {
                    return false;
                }
            }
        }

        let mut rest = file_path;
        if let Some(stripped) = rest.strip_prefix(SEPARATOR) {
            rest = stripped;
        }
        if let Some(stripped) = rest.strip_suffix(SEPARATOR) {
            rest = stripped;
        }

        let size = self.parts.len();
        let mut current_state = self.initial_state;
        let mut next_state: i64 = 0;
        let mut remaining = rest;
        loop {
            let (part, has_next) = next_part(remaining);
            remaining = if has_next {
                &remaining[part.len() + SEPARATOR.len_utf8()..]
            } else {
                ""
            };

            for i in 0..size {
                if (current_state >> i) & 1 == 0 {
                    continue;
                }
                let current_part = &self.parts[i];
                if current_part.matches(part) {
                    next_state |= 1 << (i + 1);
                    if i + 1 < size && matches!(self.parts[i + 1], PatternPart::AnySubPath) {
                        next_state |= 1 << (i + 2);
                    }
                } else {
                    next_state &= !(1 << (i + 1));
                    if i + 1 < size && matches!(self.parts[i + 1], PatternPart::AnySubPath) {
                        next_state &= !(1 << (i + 2));
                    }
                }

                if matches!(current_part, PatternPart::AnySubPath) {
                    next_state |= 1 << i;
                }
            }

            if current_state == 0 {
                return false;
            }
            current_state = next_state;

            if !has_next {
                break;
            }
            next_state = 0;
        }

        (current_state >> size) & 1 == 1
    }
}

/// `paths.Compile`.
pub fn compile(pattern: &str) -> Result<GlobPattern, String> {
    let trimmed = pattern.trim_matches(SEPARATOR);
    let mut parts: Vec<PatternPart> = Vec::new();
    let mut is_literal = true;

    // No leading `/` is equivalent to prefixing with `/**/`.
    if !pattern.starts_with(SEPARATOR) {
        parts.push(PatternPart::AnySubPath);
        is_literal = false;
    }

    for part in trimmed.split(SEPARATOR) {
        match part {
            "" => return Err("two consecutive forward slashes".to_owned()),
            "**" => {
                parts.push(PatternPart::AnySubPath);
                is_literal = false;
            }
            "*" => {
                parts.push(PatternPart::Any);
                is_literal = false;
            }
            _ => {
                if part.contains('*') {
                    parts.push(PatternPart::Asterisk(WildMatch::new(part)));
                    is_literal = false;
                } else {
                    parts.push(PatternPart::Exact(part.to_owned()));
                }
            }
        }
    }

    // A trailing `/` is equivalent to ending the pattern with `/**`.
    if pattern.ends_with(SEPARATOR) {
        parts.push(PatternPart::AnySubPath);
        is_literal = false;
    }

    // A trailing `**` is translated to `/**` + `*`.
    if matches!(parts.last(), Some(PatternPart::AnySubPath)) {
        parts.push(PatternPart::Any);
        is_literal = false;
    }

    // Matches for an empty input (`/`): bit 0, plus every index past the
    // leading `**` parts.
    let mut initial_state: i64 = 1;
    for (index, part) in parts.iter().enumerate() {
        if !matches!(part, PatternPart::AnySubPath) {
            break;
        }
        initial_state |= 1 << (index + 1);
    }

    Ok(GlobPattern {
        is_literal,
        pattern: pattern.to_owned(),
        parts,
        initial_state,
    })
}

/// Splits at the first separator, like the baseline `nextPart`.
fn next_part(value: &str) -> (&str, bool) {
    match value.find(SEPARATOR) {
        Some(index) => (&value[..index], true),
        None => (value, false),
    }
}

/// The last separator-delimited segment (`lastPart`); `None` without a
/// separator.
fn last_part(value: &str) -> Option<&str> {
    value.rfind(SEPARATOR).map(|index| &value[index + 1..])
}

/// `github.com/becheran/wildmatch-go`: `?` matches one character, `*`
/// matches any run (including none), no escape sequences.
#[derive(Debug, Clone)]
struct WildMatch {
    pattern: Vec<WildState>,
}

#[derive(Debug, Clone)]
struct WildState {
    next_char: Option<char>,
    has_wildcard: bool,
}

impl WildMatch {
    fn new(pattern: &str) -> Self {
        let mut simplified = Vec::new();
        let mut previous_was_star = false;
        for current in pattern.chars() {
            if current == '*' {
                previous_was_star = true;
            } else {
                simplified.push(WildState {
                    next_char: Some(current),
                    has_wildcard: previous_was_star,
                });
                previous_was_star = false;
            }
        }
        if !pattern.is_empty() {
            simplified.push(WildState {
                next_char: None,
                has_wildcard: previous_was_star,
            });
        }
        Self {
            pattern: simplified,
        }
    }

    fn is_match(&self, input: &str) -> bool {
        if self.pattern.is_empty() {
            return false;
        }

        let mut pattern_index = 0usize;
        for input_char in input.chars() {
            if pattern_index > self.pattern.len() {
                return false;
            }

            let state = &self.pattern[pattern_index];
            if state
                .next_char
                .is_some_and(|next| next == '?' || next == input_char)
            {
                pattern_index += 1;
            } else if state.has_wildcard {
                if state.next_char.is_none() {
                    return true;
                }
            } else {
                // Go back to the last state with a wildcard.
                loop {
                    let state = &self.pattern[pattern_index];
                    if state.has_wildcard {
                        if state
                            .next_char
                            .is_some_and(|next| next == '?' || next == input_char)
                        {
                            pattern_index += 1;
                        }
                        break;
                    }
                    if pattern_index == 0 {
                        return false;
                    }
                    pattern_index -= 1;
                }
            }
        }
        self.pattern[pattern_index].next_char.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matches(pattern: &str, path: &str) -> bool {
        compile(pattern).expect("pattern compiles").match_path(path)
    }

    #[test]
    fn literal_patterns_match_exactly() {
        assert!(matches("cat", "cat"));
        assert!(!matches("/cat", "cat"));
        assert!(matches("/cat", "/cat"));
        assert!(!matches("cat", "cats"));
    }

    #[test]
    fn wildcards_and_subpaths() {
        assert!(matches("*.tsx", "/src/app/components/Label.tsx"));
        assert!(matches(
            "src/*/test/**/*Test.java",
            "/src/java/test/UnitTest.java"
        ));
        assert!(!matches(
            "src/*/test/**/*Test.java",
            "/src/java/other/UnitTest.java"
        ));
        assert!(matches("foo/", "foo/bar"));
        assert!(!matches("foo/", "foo"));
        assert!(!matches("foo/**", "foo"));
        assert!(matches("**", "anything/at/all"));
    }

    #[test]
    fn consecutive_slashes_are_an_error() {
        assert_eq!(
            compile("a//b").expect_err("error"),
            "two consecutive forward slashes"
        );
        assert_eq!(
            compile("").expect_err("error"),
            "two consecutive forward slashes"
        );
    }

    #[test]
    fn wildmatch_question_mark() {
        let pattern = WildMatch::new("c?t");
        assert!(pattern.is_match("cat"));
        assert!(!pattern.is_match("cat_"));
        assert!(!pattern.is_match("ct"));
    }
}
