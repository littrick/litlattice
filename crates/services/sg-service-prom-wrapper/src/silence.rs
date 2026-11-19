#![forbid(unsafe_code)]

//! Site-config silence strings <-> Alertmanager matchers (`silence.go`).
//!
//! The 5.5 baseline is intentionally simple: each configured silence string is
//! treated as an *alert name* and wrapped in a single regex matcher
//! `alertname=~"^(<silence>)$"`. It does not parse matcher expressions.
//! Reproducing that exactly matters for interoperability with silences created
//! by an older prom-wrapper.

/// One Alertmanager matcher as returned/accepted by the v2 API.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Matcher {
    pub name: String,
    pub value: String,
    pub is_regex: bool,
    /// `isEqual` in the v2 API model (defaults to `true`).
    pub is_equal: bool,
}

impl Matcher {
    pub fn new(name: impl Into<String>, value: impl Into<String>, is_regex: bool) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
            is_regex,
            is_equal: true,
        }
    }
}

const MATCHER_REGEX_PREFIX: &str = "^(";
const MATCHER_REGEX_SUFFIX: &str = ")$";
const ALERTNAME: &str = "alertname";

/// `newMatchersFromSilence`: one `alertname` regex matcher around the whole
/// configured string.
pub fn matchers_from_silence(silence: &str) -> Vec<Matcher> {
    vec![Matcher::new(
        ALERTNAME,
        format!("{MATCHER_REGEX_PREFIX}{silence}{MATCHER_REGEX_SUFFIX}"),
        true,
    )]
}

/// `newSilenceFromMatchers`: unwrap the first `alertname` matcher; anything
/// else yields an empty string.
pub fn silence_from_matchers(matchers: &[Matcher]) -> String {
    for matcher in matchers {
        if matcher.name == ALERTNAME {
            // Go applies `TrimSuffix(TrimPrefix(v, "^("), ")$")` independently.
            let without_prefix = matcher
                .value
                .strip_prefix(MATCHER_REGEX_PREFIX)
                .unwrap_or(&matcher.value);
            return without_prefix
                .strip_suffix(MATCHER_REGEX_SUFFIX)
                .unwrap_or(without_prefix)
                .to_owned();
        }
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wraps_every_silence_as_an_alertname_regex() {
        assert_eq!(
            matchers_from_silence("warning_frontend_down"),
            vec![Matcher::new("alertname", "^(warning_frontend_down)$", true)]
        );
        // Matcher expressions are not special-cased; the baseline wraps the
        // whole string.
        assert_eq!(
            matchers_from_silence(r#"level="warning""#),
            vec![Matcher::new("alertname", "^(level=\"warning\")$", true)]
        );
    }

    #[test]
    fn round_trips_alertname_matchers() {
        for silence in ["warning_frontend_down", "", r#"level="warning" service=x"#] {
            let matchers = matchers_from_silence(silence);
            assert_eq!(silence_from_matchers(&matchers), silence);
        }
    }

    #[test]
    fn ignores_non_alertname_matchers() {
        let matchers = vec![Matcher::new("level", "warning", false)];
        assert_eq!(silence_from_matchers(&matchers), "");
        assert_eq!(silence_from_matchers(&[]), "");
    }
}
