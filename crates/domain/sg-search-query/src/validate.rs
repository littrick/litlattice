#![forbid(unsafe_code)]

//! `internal/search/query/validate.go`: plan/parameter validation.
//!
//! Ported: field uniqueness/negation/boolean/number/duration/yes-no checks,
//! regexp/language/git-date/select checks, predicate argument unmarshalling,
//! structural labels, repo-rev pairing, commit parameters, ref-free
//! type:`symbol` pairing, ref globs with `index:only`, and pattern regexp
//! compilation.
//!
//! `gitdomain.ParseGitDate`'s natural-language branch is only partially
//! ported (deterministic date layouts); see the M9.9e overview §3.

use crate::basic::Plan;
use crate::fields;
use crate::node::{exists, go_quote, visit_field, visit_parameter, visit_pattern, Node, Parameter};
use std::collections::HashSet;

/// `parseBool`: `strconv.ParseBool` plus y/yes/n/no.
pub fn parse_bool(s: &str) -> Result<bool, String> {
    match s.to_lowercase().as_str() {
        "y" | "yes" => return Ok(true),
        "n" | "no" => return Ok(false),
        _ => {}
    }
    match s {
        "1" | "t" | "T" | "TRUE" | "true" | "True" => Ok(true),
        "0" | "f" | "F" | "FALSE" | "false" | "False" => Ok(false),
        _ => Err(format!("invalid boolean {}", go_quote(s))),
    }
}

/// `YesNoOnly`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum YesNoOnly {
    Yes,
    No,
    Only,
    Invalid,
}

/// `parseYesNoOnly`.
pub fn parse_yes_no_only(s: &str) -> YesNoOnly {
    match s {
        "y" | "Y" | "yes" | "YES" | "Yes" => YesNoOnly::Yes,
        "n" | "N" | "no" | "NO" | "No" => YesNoOnly::No,
        "o" | "only" | "ONLY" | "Only" => YesNoOnly::Only,
        _ => match parse_bool(s) {
            Ok(true) => YesNoOnly::Yes,
            Ok(false) => YesNoOnly::No,
            Err(_) => YesNoOnly::Invalid,
        },
    }
}

fn is_singular(field: &str, seen: &mut HashSet<String>) -> Result<(), String> {
    if seen.contains(field) {
        return Err(format!(
            "field {} may not be used more than once",
            go_quote(field)
        ));
    }
    Ok(())
}

fn is_not_negated(field: &str, negated: bool) -> Result<(), String> {
    if negated {
        return Err(format!(
            "field {} does not support negation",
            go_quote(field)
        ));
    }
    Ok(())
}

fn is_boolean(value: &str) -> Result<(), String> {
    parse_bool(value).map(|_| ())
}

fn is_number(field: &str, value: &str) -> Result<(), String> {
    match value.parse::<i64>() {
        Ok(count) => {
            if count > i32::MAX as i64 || count < i32::MIN as i64 {
                return Err(format!(
                    "field {field} has a value that is out of range, try making it smaller"
                ));
            }
            if count <= 0 {
                return Err(format!("field {field} requires a positive number"));
            }
            Ok(())
        }
        Err(_) => Err(format!(
            "field {field} has value {value}, {value} is not a number"
        )),
    }
}

/// A `time.ParseDuration`-compatible validator (no value is returned).
fn is_duration(value: &str) -> Result<(), String> {
    let invalid = || {
        "invalid value for field 'timeout' (examples: \"timeout:2s\", \"timeout:200ms\")".to_owned()
    };
    let mut chars = value.chars().peekable();
    if matches!(chars.peek(), Some('+') | Some('-')) {
        chars.next();
    }
    if value == "0" || value == "+0" || value == "-0" {
        return Ok(());
    }
    loop {
        let mut digits = String::new();
        let mut saw_digit = false;
        while let Some(c) = chars.peek() {
            if c.is_ascii_digit() {
                saw_digit = true;
                digits.push(*c);
                chars.next();
            } else {
                break;
            }
        }
        if chars.peek() == Some(&'.') {
            digits.push('.');
            chars.next();
            while let Some(c) = chars.peek() {
                if c.is_ascii_digit() {
                    saw_digit = true;
                    digits.push(*c);
                    chars.next();
                } else {
                    break;
                }
            }
        }
        if !saw_digit {
            return Err(invalid());
        }
        let mut unit = String::new();
        while let Some(c) = chars.peek() {
            if c.is_ascii_alphabetic() || *c == 'µ' {
                unit.push(*c);
                chars.next();
            } else {
                break;
            }
        }
        if unit.is_empty() {
            return Err(invalid());
        }
        match unit.as_str() {
            "ns" | "us" | "µs" | "ms" | "s" | "m" | "h" => {}
            _ => return Err(invalid()),
        }
        if chars.peek().is_none() {
            break;
        }
    }
    Ok(())
}

fn is_yes_no_only(field: &str, value: &str) -> Result<(), String> {
    if parse_yes_no_only(value) == YesNoOnly::Invalid {
        return Err(format!(
            "invalid value {} for field {}. Valid values are: yes, only, no",
            go_quote(value),
            go_quote(field)
        ));
    }
    Ok(())
}

fn is_unrecognized_field(field: &str) -> Result<(), String> {
    Err(format!("unrecognized field {}", go_quote(field)))
}

fn is_valid_regexp(value: &str) -> Result<(), String> {
    crate::go_regexp::compile(value)
}

fn is_language(value: &str) -> Result<(), String> {
    if crate::language::is_valid_language(value) {
        Ok(())
    } else {
        Err(format!("unknown language: {}", go_quote(value)))
    }
}

/// `isValidRepoRegexp`: negated values are plain regexes; non-negated values
/// are `repo@rev` filters, so the repository part is compiled as `(?i)repo`.
fn is_valid_repo_regexp(value: &str, negated: bool) -> Result<(), String> {
    if negated {
        return is_valid_regexp(value);
    }
    parse_repository_revisions(value).map(|_| ())
}

/* gitdomain.ParseGitDate (deterministic layouts) */

fn ascii_digits(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

fn days_in_month(year: u32, month: u32) -> Option<u32> {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => Some(31),
        4 | 6 | 9 | 11 => Some(30),
        2 => {
            let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
            Some(if leap { 29 } else { 28 })
        }
        _ => None,
    }
}

fn valid_ymd(year: u32, month: u32, day: u32) -> bool {
    days_in_month(year, month).is_some_and(|max| day >= 1 && day <= max)
}

fn parse_ymd_parts(year: &str, month: &str, day: &str) -> bool {
    if year.len() != 4 || month.len() != 2 || day.len() != 2 {
        return false;
    }
    match (year.parse::<u32>(), month.parse::<u32>(), day.parse::<u32>()) {
        (Ok(y), Ok(m), Ok(d)) => valid_ymd(y, m, d),
        _ => false,
    }
}

/// `YYYY<sep>MM<sep>DD`.
fn is_date_sep(s: &str, sep: char) -> bool {
    let parts: Vec<&str> = s.split(sep).collect();
    parts.len() == 3 && parse_ymd_parts(parts[0], parts[1], parts[2])
}

fn parse_num_part(s: &str, min_len: usize, max_len: usize) -> Option<u32> {
    if s.len() < min_len || s.len() > max_len || !ascii_digits(s) {
        return None;
    }
    s.parse().ok()
}

/// Git internal format: `<epoch seconds>[ <+|->HHMM]`.
fn is_git_internal_date(s: &str) -> bool {
    let (seconds, valid_zone) = match s.find(' ') {
        Some(space) => {
            let zone = &s[space + 1..];
            let z = zone.as_bytes();
            let valid = z.len() == 5
                && matches!(z[0], b'+' | b'-')
                && z[1..].iter().all(|b| b.is_ascii_digit());
            (&s[..space], valid)
        }
        None => (s, true),
    };
    valid_zone && seconds.len() >= 5 && ascii_digits(seconds)
}

/// Two digits at `i`.
fn two_digits(bytes: &[u8], i: usize) -> Option<u32> {
    if i + 2 > bytes.len() || !bytes[i].is_ascii_digit() || !bytes[i + 1].is_ascii_digit() {
        return None;
    }
    Some(((bytes[i] - b'0') as u32) * 10 + (bytes[i + 1] - b'0') as u32)
}

/// `HH:MM:SS` at `i`, returning the index just past it.
fn parse_hms(bytes: &[u8], i: usize) -> Option<usize> {
    let hour = two_digits(bytes, i)?;
    if bytes.get(i + 2) != Some(&b':') {
        return None;
    }
    let minute = two_digits(bytes, i + 3)?;
    if bytes.get(i + 5) != Some(&b':') {
        return None;
    }
    let second = two_digits(bytes, i + 6)?;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    Some(i + 8)
}

/// Optional `,`/`.` fractional seconds plus an optional zone (`Z` or
/// `+HH:MM`), matching `time.Parse`'s tolerance for either separator.
fn parse_frac_and_zone(rest: &str) -> bool {
    let bytes = rest.as_bytes();
    let mut i = 0;
    if matches!(bytes.first(), Some(b'.') | Some(b',')) {
        i += 1;
        let start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == start {
            return false;
        }
    }
    let rest = &bytes[i..];
    if rest.is_empty() || rest == b"Z" {
        return true;
    }
    if rest.len() == 6 && matches!(rest[0], b'+' | b'-') && rest[3] == b':' {
        let h = two_digits(rest, 1);
        let m = two_digits(rest, 4);
        return h.is_some_and(|h| h <= 23) && m.is_some_and(|m| m <= 59);
    }
    false
}

/// `YYYY-MM-DDThh:mm:ss[.frac][Z|+hh:mm]` (RFC3339 and its zone-less form).
fn is_rfc3339_like(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.len() < 19 || !is_date_sep(&s[..10], '-') {
        return false;
    }
    if bytes[10] != b'T' {
        return false;
    }
    let Some(end) = parse_hms(bytes, 11) else {
        return false;
    };
    parse_frac_and_zone(&s[end..])
}

/// `YYYY-MM-DD hh:mm:ss[.frac][Z|+hh:mm]`.
fn is_space_datetime(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.len() < 19 || !is_date_sep(&s[..10], '-') {
        return false;
    }
    if bytes[10] != b' ' {
        return false;
    }
    let Some(end) = parse_hms(bytes, 11) else {
        return false;
    };
    parse_frac_and_zone(&s[end..])
}

/// RFC 2822 (`Thu, 02 Jan 2006 15:04:05 -0700`).
fn is_rfc2822(s: &str) -> bool {
    let bytes = s.as_bytes();
    if bytes.len() < 31 {
        return false;
    }
    let day_name = &s[..3];
    if !day_name.bytes().all(|b| b.is_ascii_alphabetic()) || bytes[3] != b',' || bytes[4] != b' ' {
        return false;
    }
    let Some(day) = two_digits(bytes, 5) else {
        return false;
    };
    if !(1..=31).contains(&day) || bytes[7] != b' ' {
        return false;
    }
    let month = &s[8..11];
    if !matches!(
        month,
        "Jan" | "Feb" | "Mar" | "Apr" | "May" | "Jun" | "Jul" | "Aug" | "Sep" | "Oct" | "Nov"
            | "Dec"
    ) {
        return false;
    }
    let Some(year) = parse_num_part(&s[12..16], 4, 4) else {
        return false;
    };
    if bytes.get(16) != Some(&b' ') {
        return false;
    }
    let Some(end) = parse_hms(bytes, 17) else {
        return false;
    };
    if bytes.get(end) != Some(&b' ') {
        return false;
    }
    let zone = &s[end + 1..];
    let z = zone.as_bytes();
    if z.len() != 5 || !matches!(z[0], b'+' | b'-') || !z[1..].iter().all(|b| b.is_ascii_digit())
    {
        return false;
    }
    // `time.Parse` validates the month/day combination for the layout's
    // month; day range is already the layout's 02.
    let month_num = match month {
        "Jan" => 1,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        _ => 12,
    };
    valid_ymd(year, month_num, day)
}

/// `MM/DD/YYYY` (layout `1/2/2006`).
fn is_slash_date(s: &str) -> bool {
    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() != 3 {
        return false;
    }
    match (
        parse_num_part(parts[0], 1, 2),
        parse_num_part(parts[1], 1, 2),
        parse_num_part(parts[2], 4, 4),
    ) {
        (Some(m), Some(d), Some(y)) => valid_ymd(y, m, d),
        _ => false,
    }
}

/// `DD.MM.YYYY` (layout `2.1.2006`).
fn is_dotted_date(s: &str) -> bool {
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 3 {
        return false;
    }
    match (
        parse_num_part(parts[0], 1, 2),
        parse_num_part(parts[1], 1, 2),
        parse_num_part(parts[2], 4, 4),
    ) {
        (Some(d), Some(m), Some(y)) => valid_ymd(y, m, d),
        _ => false,
    }
}

fn month_number(name: &str) -> Option<u32> {
    match name.to_ascii_lowercase().as_str() {
        "january" | "jan" => Some(1),
        "february" | "feb" => Some(2),
        "march" | "mar" => Some(3),
        "april" | "apr" => Some(4),
        "may" => Some(5),
        "june" | "jun" => Some(6),
        "july" | "jul" => Some(7),
        "august" | "aug" => Some(8),
        "september" | "sep" => Some(9),
        "october" | "oct" => Some(10),
        "november" | "nov" => Some(11),
        "december" | "dec" => Some(12),
        _ => None,
    }
}

fn skip_ascii_space(bytes: &[u8], mut i: usize) -> Option<usize> {
    let start = i;
    while i < bytes.len() && matches!(bytes[i], b' ' | b'\t' | b'\n' | b'\r' | 0x0c) {
        i += 1;
    }
    (i > start).then_some(i)
}

fn digits_run(bytes: &[u8], i: usize, min: usize, max: usize) -> Option<usize> {
    let mut j = i;
    while j < bytes.len() && bytes[j].is_ascii_digit() {
        j += 1;
    }
    let count = j - i;
    (count >= min).then_some(i + count.min(max))
}

/// `parseSimpleDate`: `january 1 2020` / `1 january 2020`, searched anywhere
/// in the string like the baseline's unanchored regexes.
fn is_simple_date(s: &str) -> bool {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i].is_ascii_alphabetic() && (i == 0 || !bytes[i - 1].is_ascii_alphabetic()) {
            let mut j = i;
            while j < bytes.len() && bytes[j].is_ascii_alphabetic() && j - i < 10 {
                j += 1;
            }
            if (3..=9).contains(&(j - i)) && month_number(&s[i..j]).is_some() {
                if let Some(k) = skip_ascii_space(bytes, j) {
                    if let Some(day_end) = digits_run(bytes, k, 1, 2) {
                        let mut after_day = day_end;
                        if bytes.get(after_day) == Some(&b',') {
                            after_day += 1;
                        }
                        if let Some(year_start) = skip_ascii_space(bytes, after_day) {
                            if digits_run(bytes, year_start, 4, 4).is_some() {
                                return true;
                            }
                        }
                    }
                }
            }
        }
        if bytes[i].is_ascii_digit() {
            if let Some(day_end) = digits_run(bytes, i, 1, 2) {
                if let Some(k) = skip_ascii_space(bytes, day_end) {
                    let mut j = k;
                    while j < bytes.len() && bytes[j].is_ascii_alphabetic() && j - k < 10 {
                        j += 1;
                    }
                    if (3..=9).contains(&(j - k)) && month_number(&s[k..j]).is_some() {
                        let mut after_month = j;
                        if bytes.get(after_month) == Some(&b',') {
                            after_month += 1;
                        }
                        if let Some(year_start) = skip_ascii_space(bytes, after_month) {
                            if digits_run(bytes, year_start, 4, 4).is_some() {
                                return true;
                            }
                        }
                    }
                }
            }
        }
        i += 1;
    }
    false
}

/// `gitdomain.ParseGitDate`. Deterministic layouts only; the natural-language
/// fallback (`tj/go-naturaldate`) is not ported (M9.9e overview §3).
pub fn is_valid_git_date(value: &str) -> Result<(), String> {
    let valid = is_git_internal_date(value)
        || is_date_sep(value, '-')
        || is_rfc3339_like(value)
        || is_space_datetime(value)
        || is_rfc2822(value)
        || is_date_sep(value, '.')
        || is_slash_date(value)
        || is_dotted_date(value)
        || is_simple_date(value);
    if valid {
        Ok(())
    } else {
        Err("invalid date format".to_owned())
    }
}

/* filter.SelectPathFromString */

fn symbol_selectors() -> &'static [&'static str] {
    &[
        "file",
        "module",
        "namespace",
        "package",
        "class",
        "method",
        "property",
        "field",
        "constructor",
        "enum",
        "interface",
        "function",
        "variable",
        "constant",
        "string",
        "number",
        "boolean",
        "array",
        "object",
        "key",
        "null",
        "enum-member",
        "struct",
        "event",
        "operator",
        "type-parameter",
    ]
}

/// The valid children of a `select:` path prefix; an empty slice means the
/// prefix is a valid leaf.
fn select_children(prefix: &[&str]) -> Option<&'static [&'static str]> {
    match prefix {
        [] => Some(&["commit", "content", "file", "repo", "symbol"]),
        ["commit"] => Some(&["diff"]),
        ["commit", "diff"] => Some(&["added", "removed"]),
        ["content"] | ["repo"] => Some(&[]),
        ["file"] => Some(&["directory", "path", "owners"]),
        ["symbol"] => Some(symbol_selectors()),
        _ => None,
    }
}

fn is_valid_select(value: &str) -> Result<(), String> {
    let fields: Vec<&str> = value.split('.').collect();
    for (i, field) in fields.iter().enumerate() {
        match select_children(&fields[..i]) {
            Some(children) if children.contains(field) => {}
            _ => {
                return Err(format!(
                    "invalid field {} on select path {}",
                    go_quote(field),
                    go_quote(value)
                ))
            }
        }
    }
    Ok(())
}

/* ParseRepositoryRevisions */

/// The subset of `ParsedRepoFilter` validation needs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParsedRepoRevisions {
    pub repo: String,
    pub has_ref_globs: bool,
}

/// `ParseRevisionSpecifier`; errors only come from the `rat=` payload.
fn parse_revision_specifier(spec: &str) -> Result<bool, String> {
    if let Some(rest) = spec.strip_prefix("*!") {
        let _ = rest;
        return Ok(true);
    }
    if spec.starts_with('*') {
        return Ok(true);
    }
    if let Some(encoded) = spec.strip_prefix("rat=") {
        // `typedoc:base64.URLEncoding` then JSON; only the base64 step is
        // modelled (the corpus does not use `rat=`, see overview §3).
        crate::go_base64::decode_url_base64(encoded)?;
    }
    Ok(false)
}

/// `ParseRepositoryRevisions`.
pub fn parse_repository_revisions(repo_and_optional_rev: &str) -> Result<ParsedRepoRevisions, String> {
    let mut has_ref_globs = false;
    let repo = match repo_and_optional_rev.find('@') {
        Some(at) => {
            for part in repo_and_optional_rev[at + 1..].split(':') {
                if part.is_empty() {
                    continue;
                }
                has_ref_globs |= parse_revision_specifier(part)?;
            }
            &repo_and_optional_rev[..at]
        }
        None => repo_and_optional_rev,
    };
    crate::go_regexp::compile(&format!("(?i){repo}"))?;
    Ok(ParsedRepoRevisions {
        repo: repo.to_owned(),
        has_ref_globs,
    })
}

/// `ContainsRefGlobs`: non-negated, non-predicate `repo:` values whose parsed
/// revisions include a ref glob.
fn contains_ref_globs(nodes: &[Node]) -> bool {
    let mut found = false;
    visit_field(nodes, fields::FIELD_REPO, &mut |value, negated, annotation| {
        if found
            || negated
            || annotation
                .labels
                .is_set(crate::labels::Labels::IS_PREDICATE)
        {
            return;
        }
        if parse_repository_revisions(value).is_ok_and(|parsed| parsed.has_ref_globs) {
            found = true;
        }
    });
    found
}

/// `validateRefGlobs`.
pub fn validate_ref_globs(nodes: &[Node]) -> Result<(), String> {
    if !contains_ref_globs(nodes) {
        return Ok(());
    }
    let mut index_value = String::new();
    visit_field(nodes, fields::FIELD_INDEX, &mut |value, _negated, _| {
        index_value = value.to_owned();
    });
    if parse_yes_no_only(&index_value) == YesNoOnly::Only {
        return Err(format!(
            "invalid index:{} (revisions with glob pattern cannot be resolved for indexed searches)",
            index_value
        ));
    }
    Ok(())
}

/// `validateField`.
pub fn validate_field(
    field: &str,
    value: &str,
    negated: bool,
    seen: &mut HashSet<String>,
) -> Result<(), String> {
    match field {
        fields::FIELD_DEFAULT => Ok(()),
        fields::FIELD_CASE => {
            is_singular(field, seen)?;
            is_boolean(value)?;
            is_not_negated(field, negated)
        }
        fields::FIELD_REPO => is_valid_repo_regexp(value, negated),
        fields::FIELD_CONTEXT => {
            is_singular(field, seen)?;
            is_not_negated(field, negated)
        }
        fields::FIELD_FILE => is_valid_regexp(value),
        fields::FIELD_LANG => is_language(value),
        fields::FIELD_TYPE => is_not_negated(field, negated),
        fields::FIELD_PATTERN_TYPE | fields::FIELD_CONTENT | fields::FIELD_VISIBILITY => {
            is_singular(field, seen)?;
            is_not_negated(field, negated)
        }
        fields::FIELD_REPO_HAS_FILE => is_valid_regexp(value),
        fields::FIELD_REPO_HAS_COMMIT_AFTER => {
            is_singular(field, seen)?;
            is_not_negated(field, negated)
        }
        fields::FIELD_BEFORE | fields::FIELD_AFTER => {
            is_not_negated(field, negated)?;
            is_valid_git_date(value)
        }
        fields::FIELD_AUTHOR | fields::FIELD_COMMITTER | fields::FIELD_MESSAGE => {
            is_valid_regexp(value)
        }
        fields::FIELD_INDEX | fields::FIELD_FORK | fields::FIELD_ARCHIVED => {
            is_singular(field, seen)?;
            is_not_negated(field, negated)?;
            is_yes_no_only(field, value)
        }
        fields::FIELD_COUNT => {
            is_singular(field, seen)?;
            is_number(field, value)?;
            is_not_negated(field, negated)
        }
        fields::FIELD_COMBY_RULE => {
            is_singular(field, seen)?;
            is_not_negated(field, negated)
        }
        fields::FIELD_TIMEOUT => {
            is_singular(field, seen)?;
            is_not_negated(field, negated)?;
            is_duration(value)
        }
        fields::FIELD_REV => {
            is_singular(field, seen)?;
            is_not_negated(field, negated)
        }
        fields::FIELD_SELECT => {
            is_singular(field, seen)?;
            is_not_negated(field, negated)?;
            is_valid_select(value)
        }
        _ => is_unrecognized_field(field),
    }
}

/// `validateParameters`.
pub fn validate_parameters(nodes: &[Node]) -> Result<(), String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut error: Option<String> = None;
    visit_parameter(nodes, &mut |field, value, negated, annotation| {
        if error.is_some() {
            return;
        }
        let result = if annotation
            .labels
            .is_set(crate::labels::Labels::IS_PREDICATE)
        {
            crate::predicate::validate_predicate(field, value, negated)
        } else {
            validate_field(field, value, negated, &mut seen)
        };
        seen.insert(field.to_owned());
        if let Err(e) = result {
            error = Some(e);
        }
    });
    match error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// `validatePattern`.
pub fn validate_pattern(nodes: &[Node]) -> Result<(), String> {
    let mut error: Option<String> = None;
    visit_pattern(nodes, &mut |value, negated, annotation| {
        if error.is_some() {
            return;
        }
        if annotation.labels.is_set(crate::labels::Labels::REGEXP) {
            if let Err(e) = crate::go_regexp::compile(value) {
                error = Some(e);
            }
        }
        if annotation.labels.is_set(crate::labels::Labels::STRUCTURAL) && negated {
            error = Some(
                "the query contains a negated search pattern. Structural search does not support negated search patterns at the moment"
                    .to_owned(),
            );
        }
    });
    match error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// `validateRepoRevPair`.
pub fn validate_repo_rev_pair(nodes: &[Node]) -> Result<(), String> {
    let mut seen_repo_with_commit = false;
    let mut seen_repo = false;
    let mut seen_empty_repo = false;
    crate::node::visit_field(nodes, fields::FIELD_REPO, &mut |value, negated, _| {
        seen_repo = true;
        seen_empty_repo = value.is_empty();
        if !negated && value.contains('@') {
            seen_repo_with_commit = true;
        }
    });
    let rev_specified = exists(
        nodes,
        &mut |node| matches!(node, Node::Parameter(Parameter { field, .. }) if field == fields::FIELD_REV),
    );
    if seen_repo_with_commit && rev_specified {
        return Err("invalid syntax. You specified both @ and rev: for a repo: filter and I don't know how to interpret this. Remove either @ or rev: and try again".to_owned());
    }
    if !seen_repo && rev_specified {
        return Err(
            "invalid syntax. The query contains `rev:` without `repo:`. Add a `repo:` filter and try again"
                .to_owned(),
        );
    }
    if seen_empty_repo && rev_specified {
        return Err("invalid syntax. The query contains `rev:` but `repo:` is empty. Add a non-empty `repo:` filter and try again".to_owned());
    }
    Ok(())
}

/// `validateCommitParameters`.
pub fn validate_commit_parameters(nodes: &[Node]) -> Result<(), String> {
    let mut seen_commit_param = String::new();
    let mut type_commit_exists = false;
    visit_parameter(nodes, &mut |field, value, _negated, _| {
        if matches!(
            field,
            fields::FIELD_AUTHOR
                | fields::FIELD_BEFORE
                | fields::FIELD_AFTER
                | fields::FIELD_MESSAGE
        ) {
            seen_commit_param = field.to_owned();
        }
        if field == fields::FIELD_TYPE && (value == "commit" || value == "diff") {
            type_commit_exists = true;
        }
    });
    if !seen_commit_param.is_empty() && !type_commit_exists {
        return Err(format!(
            "your query contains the field '{seen_commit_param}', which requires type:commit or type:diff in the query"
        ));
    }
    Ok(())
}

/// `validateTypeStructural`.
pub fn validate_type_structural(nodes: &[Node]) -> Result<(), String> {
    let mut seen_structural = false;
    let mut seen_type = false;
    let mut type_diff = false;
    let invalid = exists(nodes, &mut |node| {
        if let Node::Pattern(p) = node {
            if p.annotation
                .labels
                .is_set(crate::labels::Labels::STRUCTURAL)
            {
                seen_structural = true;
            }
        }
        if let Node::Parameter(p) = node {
            if p.field == fields::FIELD_TYPE {
                seen_type = true;
                type_diff = p.value == "diff";
            }
        }
        seen_structural && seen_type
    });
    if invalid {
        let mut basic = "this structural search query specifies `type:` and is not supported. Structural search syntax only applies to searching file contents".to_owned();
        if type_diff {
            basic.push_str(" and is not currently supported for diff searches");
        }
        return Err(basic);
    }
    Ok(())
}

/// `validateRepoHasFile`.
pub fn validate_repo_has_file(nodes: &[Node]) -> Result<(), String> {
    let mut seen_repo_has_file = false;
    let mut seen_type_symbol = false;
    visit_parameter(nodes, &mut |field, value, _negated, _| {
        if field == fields::FIELD_REPO_HAS_FILE {
            seen_repo_has_file = true;
        }
        if field == fields::FIELD_TYPE && value.eq_ignore_ascii_case("symbol") {
            seen_type_symbol = true;
        }
    });
    if seen_repo_has_file && seen_type_symbol {
        return Err("repohasfile is not compatible for type:symbol. Subscribe to https://github.com/sourcegraph/sourcegraph/issues/4610 for updates".to_owned());
    }
    Ok(())
}

/// `validate`.
pub fn validate(nodes: &[Node]) -> Result<(), String> {
    validate_parameters(nodes)?;
    validate_pattern(nodes)?;
    validate_repo_rev_pair(nodes)?;
    validate_repo_has_file(nodes)?;
    validate_commit_parameters(nodes)?;
    validate_type_structural(nodes)?;
    validate_ref_globs(nodes)
}

/// `ValidatePlan`.
pub fn validate_plan(plan: &Plan) -> Result<(), String> {
    for basic in plan {
        validate(&basic.to_parse_tree())?;
    }
    Ok(())
}
