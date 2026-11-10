#![forbid(unsafe_code)]

//! `cmd/frontend/internal/search/search.go:341-366` (`GuessSource`): classify
//! a request by its `User-Agent` so the GraphQL rate limiter can tell
//! anonymous non-browser clients (`trace.SourceOther`) from browsers and the
//! SearchBlitz benchmark.

/// `trace.SourceType` values used by the frontend (`internal/trace/httptrace.go:68-75`).
pub const SOURCE_BROWSER: &str = "browser";
pub const SOURCE_OTHER: &str = "other";

/// The browser tokens probed in order by `GuessSource`.
const BROWSER_TOKENS: [&str; 7] = [
    "Mozilla", "WebKit", "Gecko", "Chrome", "Firefox", "Safari", "Edge",
];

/// `search.GuessSource`: browser User-Agents win, then the SearchBlitz
/// benchmark (`^SearchBlitz \(([^\)]+)\)$`), then `"other"`.
///
/// The baseline's regexp is replaced by an exact string check: a match
/// requires the whole UA to be `SearchBlitz (<group>)` with a non-empty group
/// that itself contains no `)` (that is exactly what `[^\)]+` plus the
/// anchored delimiters accept). `regexp.FindStringSubmatch` would not accept
/// a trailing newline either, and neither does `strip_suffix`.
pub fn guess_source(user_agent: &str) -> String {
    for token in BROWSER_TOKENS {
        if user_agent.contains(token) {
            return SOURCE_BROWSER.to_owned();
        }
    }

    if let Some(rest) = user_agent.strip_prefix("SearchBlitz (") {
        if let Some(group) = rest.strip_suffix(')') {
            if !group.is_empty() && !group.contains(')') {
                return format!("searchblitz_{group}");
            }
        }
    }

    SOURCE_OTHER.to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn browser_tokens_win_in_order() {
        assert_eq!(guess_source("Mozilla/5.0 (X11; Linux)"), SOURCE_BROWSER);
        assert_eq!(guess_source("curl/8.0 WebKit"), SOURCE_BROWSER);
        // The baseline probes the literal token "Edge" (not Chromium's "Edg/").
        assert_eq!(guess_source("Edge/18"), SOURCE_BROWSER);
        assert_eq!(guess_source("Edg/120"), SOURCE_OTHER);
        // Token matching is case-sensitive, as in the baseline.
        assert_eq!(guess_source("mozilla/5.0"), SOURCE_OTHER);
    }

    #[test]
    fn searchblitz_user_agents() {
        assert_eq!(guess_source("SearchBlitz (index)"), "searchblitz_index");
        assert_eq!(guess_source("SearchBlitz (a-b_c)"), "searchblitz_a-b_c");
        // The capture group cannot be empty or contain `)`.
        assert_eq!(guess_source("SearchBlitz ()"), SOURCE_OTHER);
        assert_eq!(guess_source("SearchBlitz (a)b)"), SOURCE_OTHER);
        assert_eq!(guess_source("SearchBlitz (a) "), SOURCE_OTHER);
        assert_eq!(guess_source("x SearchBlitz (a)"), SOURCE_OTHER);
        assert_eq!(guess_source("SearchBlitz (a)\n"), SOURCE_OTHER);
    }

    #[test]
    fn everything_else_is_other() {
        assert_eq!(guess_source(""), SOURCE_OTHER);
        assert_eq!(guess_source("curl/8.4.0"), SOURCE_OTHER);
        assert_eq!(guess_source("Go-http-client/1.1"), SOURCE_OTHER);
    }
}
