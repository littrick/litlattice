#![forbid(unsafe_code)]

//! `internal/insights/query/querybuilder/regexp.go`: capture-group discovery
//! and pattern replacement for insight series.

use sg_search_query::node::{map_pattern, regexp_quote_meta, visit_pattern, Node, Pattern};
use sg_search_query::{pipeline_query, plan_to_q, string_human, Labels, Plan, SearchType};

pub const MULTIPLE_PATTERN_ERR: &str =
    "pattern replacement does not support queries with multiple patterns";
pub const UNSUPPORTED_PATTERN_TYPE_ERR: &str =
    "pattern replacement is only supported for regexp patterns";

/// One parenthesised group in a regexp string (byte offsets).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Group {
    pub start: usize,
    pub end: usize,
    pub capturing: bool,
    pub number: usize,
}

/// `peek`.
fn peek(pattern: &[u8], current_index: usize, peek_offset: isize) -> u8 {
    let index = current_index as isize + peek_offset;
    if index < 0 || index >= pattern.len() as isize {
        return 0;
    }
    pattern[index as usize]
}

/// `findGroups`: all capturing/non-capturing groups of a **valid** regexp,
/// in closing order (nested groups included).
pub fn find_groups(pattern: &str) -> Vec<Group> {
    let bytes = pattern.as_bytes();
    let mut groups = Vec::new();
    let mut opens: Vec<Group> = Vec::new();
    let mut in_char_class = false;
    let mut group_number = 0usize;

    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'\\' {
            // Skip the escaped byte, as the baseline does (`i += 1` then the
            // loop's `i++`).
            i += 2;
            continue;
        }
        if bytes[i] == b'[' {
            in_char_class = true;
        } else if bytes[i] == b']' {
            in_char_class = false;
        }

        if bytes[i] == b'(' && !in_char_class {
            let mut group = Group {
                start: i,
                end: 0,
                capturing: true,
                number: 0,
            };
            if peek(bytes, i, 1) == b'?' {
                group.capturing = false;
                group.number = 0;
            } else {
                group_number += 1;
                group.number = group_number;
            }
            opens.push(group);
        } else if bytes[i] == b')' && !in_char_class {
            let Some(mut current) = opens.pop() else {
                // Unbalanced close parenthesis: the baseline no-ops by
                // returning nil.
                return Vec::new();
            };
            current.end = i;
            groups.push(current);
        }
        i += 1;
    }
    groups
}

/// `replaceCaptureGroupsWithString`: replace the first capturing group with a
/// non-capturing group holding the quoted replacement.
pub fn replace_capture_groups_with_string(
    pattern: &str,
    groups: &[Group],
    replacement: &str,
) -> String {
    if groups.is_empty() {
        return pattern.to_owned();
    }

    let mut first_capturing: Option<&Group> = None;
    for group in groups {
        if !group.capturing {
            continue;
        }
        if first_capturing.is_none_or(|first| group.number < first.number) {
            first_capturing = Some(group);
        }
    }
    let Some(first) = first_capturing else {
        return pattern.to_owned();
    };

    let mut result = String::new();
    result.push_str(&pattern[..first.start]);
    result.push_str("(?:");
    result.push_str(&regexp_quote_meta(replacement));
    result.push(')');
    let offset = first.end + 1;
    if first.end + 1 < pattern.len() {
        result.push_str(&pattern[offset..]);
    }
    result
}

/// `regexpReplacer`.
#[derive(Debug, Clone)]
pub struct RegexpReplacer {
    pub original: Plan,
    pub pattern: String,
    pub groups: Vec<Group>,
}

impl RegexpReplacer {
    /// `RegexpReplacer.Replace`.
    pub fn replace(&self, replacement: &str) -> String {
        if self.groups.is_empty() {
            return self.replace_content(replacement);
        }
        self.replace_content(&replace_capture_groups_with_string(
            &self.pattern,
            &self.groups,
            replacement,
        ))
    }

    /// `RegexpReplacer.replaceContent`: rewrite every pattern leaf.
    fn replace_content(&self, replacement: &str) -> String {
        let nodes = plan_to_q(&self.original);
        let modified = map_pattern(&nodes, &mut |_value, negated, annotation| {
            Node::Pattern(Pattern {
                value: replacement.to_owned(),
                negated,
                annotation: annotation.clone(),
            })
        });
        string_human(&modified)
    }

    /// `RegexpReplacer.HasCaptureGroups`.
    pub fn has_capture_groups(&self) -> bool {
        self.groups.iter().any(|group| group.capturing)
    }
}

/// `NewPatternReplacer`.
pub fn new_pattern_replacer(
    query: &str,
    search_type: SearchType,
) -> Result<RegexpReplacer, String> {
    let plan = pipeline_query(query, search_type)
        .map_err(|err| format!("failed to parse search query: {err}"))?;

    let nodes = plan_to_q(&plan);
    let mut patterns: Vec<Pattern> = Vec::new();
    visit_pattern(&nodes, &mut |value, negated, annotation| {
        patterns.push(Pattern {
            value: value.to_owned(),
            negated,
            annotation: annotation.clone(),
        });
    });

    if patterns.len() > 1 {
        return Err(MULTIPLE_PATTERN_ERR.to_owned());
    }
    if patterns.is_empty() {
        return Err(UNSUPPORTED_PATTERN_TYPE_ERR.to_owned());
    }

    let pattern = patterns.remove(0);
    if !pattern.annotation.labels.is_set(Labels::REGEXP) {
        return Err(UNSUPPORTED_PATTERN_TYPE_ERR.to_owned());
    }

    let groups = find_groups(&pattern.value);
    Ok(RegexpReplacer {
        original: plan,
        groups,
        pattern: pattern.value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_groups_matches_baseline_examples() {
        assert!(find_groups(r"\w*\s").is_empty());
        assert_eq!(
            find_groups("te(s)t"),
            vec![Group {
                start: 2,
                end: 4,
                capturing: true,
                number: 1
            }]
        );
        assert_eq!(
            find_groups("te(s)(t)(?:asdf)[(]"),
            vec![
                Group {
                    start: 2,
                    end: 4,
                    capturing: true,
                    number: 1
                },
                Group {
                    start: 5,
                    end: 7,
                    capturing: true,
                    number: 2
                },
                Group {
                    start: 8,
                    end: 15,
                    capturing: false,
                    number: 0
                },
            ]
        );
    }

    #[test]
    fn replaces_first_capturing_group() {
        let pattern = r"(\w+)-(\w+)";
        assert_eq!(
            replace_capture_groups_with_string(pattern, &find_groups(pattern), "cat"),
            r"(?:cat)-(\w+)"
        );
        // A replacement containing regexp metacharacters is quoted.
        let pattern = r"(.*)";
        assert_eq!(
            replace_capture_groups_with_string(pattern, &find_groups(pattern), r"\w"),
            r"(?:\\w)"
        );
        // No capturing groups leaves the pattern untouched.
        let pattern = "(?:hello)(?:friend)";
        assert_eq!(
            replace_capture_groups_with_string(pattern, &find_groups(pattern), "no"),
            pattern
        );
    }
}
