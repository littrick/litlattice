//! `parseURLQuery` (`cmd/frontend/internal/search/search.go:326-390`).

use super::json;

/// Parsed `/.api/search/stream` URL parameters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Args {
    pub query: String,
    pub version: String,
    pub pattern_type: String,
    pub display: i64,
    pub max_line_len: i64,
    pub enable_chunk_matches: bool,
    pub search_mode: i64,
    pub context_lines: Option<i32>,
    pub zoekt_search_options_override: String,
}

/// `parseURLQuery`. The error strings are part of the wire contract: the
/// handler streams them as `event: error` messages.
pub fn parse_url_query(query: Option<&str>) -> Result<Args, String> {
    let params: Vec<(String, String)> = query
        .map(|query| {
            url::form_urlencoded::parse(query.as_bytes())
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect()
        })
        .unwrap_or_default();
    let get = |key: &str, default: &str| -> String {
        params
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| default.to_owned())
    };

    let query = get("q", "");
    if query.is_empty() {
        return Err("no query found".to_owned());
    }

    let display_raw = get("display", "-1");
    let display = display_raw.parse::<i64>().map_err(|err| {
        format!(
            "display must be an integer, got {}: {}",
            json::go_quote(&display_raw),
            atoi_error(&display_raw, err)
        )
    })?;

    let max_line_len_raw = get("max-line-len", "-1");
    let max_line_len = max_line_len_raw.parse::<i64>().map_err(|err| {
        // The baseline's `%q` verb carries `display` (not `max-line-len`)
        // here; reproduced verbatim.
        format!(
            "max-line-len must be an integer, got {}: {}",
            json::go_quote(&display_raw),
            atoi_error(&max_line_len_raw, err)
        )
    })?;

    let chunk_matches_raw = get("cm", "f");
    let enable_chunk_matches = parse_go_bool(&chunk_matches_raw).ok_or_else(|| {
        format!(
            "chunk matches must be parseable as a boolean, got {}: strconv.ParseBool: parsing {}: invalid syntax",
            json::go_quote(&chunk_matches_raw),
            json::go_quote(&chunk_matches_raw)
        )
    })?;

    let context_lines = match params.iter().find(|(k, _)| k == "cl") {
        None => None,
        Some((_, raw)) if raw.is_empty() => None,
        Some((_, raw)) => Some(parse_go_uint32(raw).map_err(|_| {
            // Baseline message says "boolean" (copy/paste) but the parser is
            // `strconv.ParseUint`. Reproduced verbatim.
            format!(
                "context lines must be parseable as a boolean, got {}: {}",
                json::go_quote(raw),
                parse_uint_error(raw)
            )
        })?),
    };

    let search_mode_raw = get("sm", "0");
    let search_mode = search_mode_raw.parse::<i64>().map_err(|err| {
        format!(
            "search mode must be integer, got {}: {}",
            json::go_quote(&search_mode_raw),
            atoi_error(&search_mode_raw, err)
        )
    })?;

    Ok(Args {
        version: get("v", "V3"),
        pattern_type: get("t", ""),
        zoekt_search_options_override: get("zoekt-search-opts", ""),
        query,
        display,
        max_line_len,
        enable_chunk_matches,
        search_mode,
        context_lines,
    })
}

fn atoi_error(raw: &str, err: std::num::ParseIntError) -> String {
    match err.kind() {
        std::num::IntErrorKind::Empty | std::num::IntErrorKind::InvalidDigit => {
            format!(
                "strconv.Atoi: parsing {}: invalid syntax",
                json::go_quote(raw)
            )
        }
        _ => format!(
            "strconv.Atoi: parsing {}: value out of range",
            json::go_quote(raw)
        ),
    }
}

fn parse_uint_error(raw: &str) -> String {
    // `strconv.ParseUint(raw, 10, 32)` error strings.
    if raw.starts_with('-') {
        format!(
            "strconv.ParseUint: parsing {}: invalid syntax",
            json::go_quote(raw)
        )
    } else {
        match raw.parse::<u64>() {
            Err(err)
                if matches!(
                    err.kind(),
                    std::num::IntErrorKind::Empty | std::num::IntErrorKind::InvalidDigit
                ) =>
            {
                format!(
                    "strconv.ParseUint: parsing {}: invalid syntax",
                    json::go_quote(raw)
                )
            }
            _ => format!(
                "strconv.ParseUint: parsing {}: value out of range",
                json::go_quote(raw)
            ),
        }
    }
}

fn parse_go_uint32(raw: &str) -> Result<i32, ()> {
    if raw.starts_with('-') || raw.is_empty() {
        return Err(());
    }
    match raw.parse::<u64>() {
        Ok(v) if v <= u32::MAX as u64 => Ok(v as i32),
        _ => Err(()),
    }
}

fn parse_go_bool(raw: &str) -> Option<bool> {
    match raw {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Some(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_values() {
        let args = parse_url_query(Some(
            "q=foo&v=V2&t=regexp&display=5&max-line-len=100&cm=t&cl=3&sm=1",
        ))
        .unwrap();
        assert_eq!(args.query, "foo");
        assert_eq!(args.version, "V2");
        assert_eq!(args.pattern_type, "regexp");
        assert_eq!(args.display, 5);
        assert_eq!(args.max_line_len, 100);
        assert!(args.enable_chunk_matches);
        assert_eq!(args.context_lines, Some(3));
        assert_eq!(args.search_mode, 1);

        let args = parse_url_query(Some("q=a+b")).unwrap();
        assert_eq!(args.query, "a b");
        assert_eq!(args.version, "V3");
        assert_eq!(args.display, -1);
        assert_eq!(args.max_line_len, -1);
        assert!(!args.enable_chunk_matches);
        assert_eq!(args.context_lines, None);
        assert_eq!(args.search_mode, 0);
    }

    #[test]
    fn errors_match_go() {
        assert_eq!(parse_url_query(None).unwrap_err(), "no query found");
        assert_eq!(
            parse_url_query(Some("q=x&display=abc")).unwrap_err(),
            "display must be an integer, got \"abc\": strconv.Atoi: parsing \"abc\": invalid syntax"
        );
        // The baseline quotes `display` in the max-line-len error.
        assert_eq!(
            parse_url_query(Some("q=x&display=7&max-line-len=nope")).unwrap_err(),
            "max-line-len must be an integer, got \"7\": strconv.Atoi: parsing \"nope\": invalid syntax"
        );
        assert_eq!(
            parse_url_query(Some("q=x&cm=maybe")).unwrap_err(),
            "chunk matches must be parseable as a boolean, got \"maybe\": strconv.ParseBool: parsing \"maybe\": invalid syntax"
        );
        assert_eq!(
            parse_url_query(Some("q=x&cl=-1")).unwrap_err(),
            "context lines must be parseable as a boolean, got \"-1\": strconv.ParseUint: parsing \"-1\": invalid syntax"
        );
        assert_eq!(
            parse_url_query(Some("q=x&sm=x")).unwrap_err(),
            "search mode must be integer, got \"x\": strconv.Atoi: parsing \"x\": invalid syntax"
        );
    }
}
