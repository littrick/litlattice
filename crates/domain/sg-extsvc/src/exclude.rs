#![forbid(unsafe_code)]

//! `internal/repos/exclude.go`, reduced to the rule shapes external-service
//! configs use (`Exact` and `Pattern`).

use std::collections::HashSet;

use regex::Regex;

/// `repoExcluder`.
#[derive(Debug, Default)]
pub struct Excluder {
    /// Rules that only contain exact matches are inlined into one map
    /// (`AddRule`'s optimisation).
    exact_rules: HashSet<String>,
    rules: Vec<Rule>,
}

/// One `ExcludedOtherRepo` entry: `name` and `pattern` both have to match.
#[derive(Debug)]
pub struct ExcludeRule {
    pub name: Option<String>,
    pub pattern: Option<String>,
}

#[derive(Debug)]
struct Rule {
    exact: Vec<String>,
    patterns: Vec<Regex>,
}

impl Excluder {
    /// `AddRule(NewRule().Exact(r.Name).Pattern(r.Pattern))`, for every
    /// configured rule. Pattern compilation failures abort construction like
    /// `RuleErrors()` does in `NewOtherSource`.
    pub fn new(rules: &[ExcludeRule]) -> Result<Self, String> {
        let mut excluder = Self::default();
        for rule in rules {
            let exact: Vec<String> = rule
                .name
                .as_deref()
                .filter(|name| !name.is_empty())
                .map(|name| vec![name.to_lowercase()])
                .unwrap_or_default();
            let patterns: Vec<Regex> = match rule.pattern.as_deref() {
                Some(pattern) if !pattern.is_empty() => {
                    vec![Regex::new(pattern).map_err(|err| err.to_string())?]
                }
                _ => Vec::new(),
            };

            if !exact.is_empty() && patterns.is_empty() {
                excluder.exact_rules.extend(exact);
                continue;
            }
            if exact.is_empty() && patterns.is_empty() {
                continue;
            }
            excluder.rules.push(Rule { exact, patterns });
        }
        Ok(excluder)
    }

    /// `repoExcluder.ShouldExclude` for repository names.
    pub fn should_exclude(&self, input: &str) -> bool {
        let lower = input.to_lowercase();
        if self.exact_rules.contains(&lower) {
            return true;
        }
        self.rules.iter().any(|rule| rule.excludes(&lower, input))
    }
}

impl Rule {
    /// `rule.Excludes`: every configured condition must match.
    fn excludes(&self, lower: &str, input: &str) -> bool {
        let mut exclude = false;
        if !self.exact.is_empty() {
            exclude = self.exact.iter().any(|exact| exact == lower);
            if !exclude {
                return false;
            }
        }
        for pattern in &self.patterns {
            exclude = pattern.is_match(input);
            if !exclude {
                return false;
            }
        }
        exclude
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_names_are_case_insensitive() {
        let excluder = Excluder::new(&[ExcludeRule {
            name: Some("Excluded/Repo".to_owned()),
            pattern: None,
        }])
        .expect("rules");
        assert!(excluder.should_exclude("excluded/repo"));
        assert!(excluder.should_exclude("EXCLUDED/REPO"));
        assert!(!excluder.should_exclude("excluded/repo2"));
    }

    #[test]
    fn name_and_pattern_are_anded() {
        let excluder = Excluder::new(&[ExcludeRule {
            name: Some("owner/repo".to_owned()),
            pattern: Some("secret".to_owned()),
        }])
        .expect("rules");
        // Both conditions are ANDed, so a name that does not match the
        // exact value is not excluded even when the pattern matches.
        assert!(!excluder.should_exclude("owner/repo-secret"));
        assert!(!excluder.should_exclude("owner/repo"));
        assert!(!excluder.should_exclude("owner/other-secret"));
    }

    #[test]
    fn invalid_patterns_fail_construction() {
        let err = Excluder::new(&[ExcludeRule {
            name: None,
            pattern: Some("[".to_owned()),
        }])
        .expect_err("invalid pattern");
        assert!(!err.is_empty());
    }
}
