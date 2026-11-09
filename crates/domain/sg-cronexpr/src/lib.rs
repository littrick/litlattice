#![forbid(unsafe_code)]

//! `github.com/hashicorp/cronexpr` v1.1.1 (`cronexpr.go`, `cronexpr_parse.go`,
//! `cronexpr_next.go`), ported for the code-intel ranking coordinator.
//!
//! The ranking coordinator evaluates one thing from the dependency: the next
//! trigger time after the creation time of the newest derivative graph key
//! (`expr.Next(previous)`), so the port covers the whole library rather than a
//! hand-picked subset of expressions — operators can configure any expression
//! the Go service accepted.
//!
//! Semantics preserved exactly:
//!
//! * aliases (`@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`,
//!   `@hourly`), 5/6/7 fields, lists, ranges, steps, month/day names,
//!   optional seconds and years, `?` as a wildcard;
//! * `L`, `LW`, `nW` in day-of-month and `nL`, `n#k` in day-of-week;
//! * the `Next` search order (year → month → actual days → hour → minute →
//!   second), which decides which occurrence a cron expression describes;
//! * error messages used by the baseline's warning log.
//!
//! Erratum (time zones): the baseline interprets the calendar fields in
//! `fromTime.Location()` and has a `SLOW_CLOCK` branch for days whose UTC
//! offset changes. This port works on [`OffsetDateTime`] and always treats the
//! value as UTC. Both production inputs are UTC in practice — `previous` is
//! decoded from PostgreSQL `timestamptz` (pgx returns UTC) and `now` comes
//! from [`OffsetDateTime::now_utc`] — so the baseline never enters its DST
//! branch for this caller and the two implementations agree. A non-UTC
//! `fromTime` is outside the supported surface.
//!
//! Erratum (zero time): Go returns the zero `time.Time` when no occurrence
//! exists (exhausted year list, zero input). This port returns `None`; the
//! coordinator treats it like the baseline's `time.Until(zero) <= 0` (i.e. a
//! bump), see `crates/services/sg-service-worker`.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::LazyLock;

use regex::Regex;
use time::{Date, Duration, Month, OffsetDateTime, PrimitiveDateTime, Time, Weekday};

/// `Parse` failure. The message is byte-identical to the Go library's, which
/// the baseline only logs (`Illegal ranking cron expression`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    message: String,
}

impl ParseError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ParseError {}

// ---------------------------------------------------------------------------
// Field descriptors (`cronexpr_parse.go`)
// ---------------------------------------------------------------------------

const SECOND_VALUE: &str = r"0?[0-9]|[1-5][0-9]";
const MINUTE_VALUE: &str = r"0?[0-9]|[1-5][0-9]";
const HOUR_VALUE: &str = r"0?[0-9]|1[0-9]|2[0-3]";
const DOM_VALUE: &str = r"0?[1-9]|[12][0-9]|3[01]";
const MONTH_VALUE: &str = r"0?[1-9]|1[012]|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|january|february|march|april|june|july|august|september|october|november|december";
const DOW_VALUE: &str =
    r"0?[0-7]|sun|mon|tue|wed|thu|fri|sat|sunday|monday|tuesday|wednesday|thursday|friday|saturday";
const YEAR_VALUE: &str = r"19[789][0-9]|20[0-9]{2}";

struct FieldDescriptor {
    name: &'static str,
    min: i32,
    max: i32,
    atoi: fn(&str) -> i32,
    layouts: LazyLock<Layouts>,
}

impl FieldDescriptor {
    /// `desc.defaultList`: every descriptor's list is `min..=max`
    /// (`genericDefaultList` slices and `yearDefaultList`).
    fn default_list(&self) -> Vec<i32> {
        (self.min..=self.max).collect()
    }
}

struct Layouts {
    wildcard: Regex,
    value: Regex,
    range: Regex,
    wildcard_and_interval: Regex,
    value_and_interval: Regex,
    range_and_interval: Regex,
}

impl Layouts {
    fn new(value_pattern: &str) -> Self {
        let build = |layout: &str| {
            Regex::new(&layout.replace("%value%", value_pattern)).expect("static cron layout")
        };
        Self {
            wildcard: build(r"^\*$|^\?$"),
            value: build(r"^(%value%)$"),
            range: build(r"^(%value%)-(%value%)$"),
            wildcard_and_interval: build(r"^\*/([0-9]+)$"),
            value_and_interval: build(r"^(%value%)/([0-9]+)$"),
            range_and_interval: build(r"^(%value%)-(%value%)/([0-9]+)$"),
        }
    }
}

static SECOND_DESCRIPTOR: FieldDescriptor = FieldDescriptor {
    name: "second",
    min: 0,
    max: 59,
    atoi: number_atoi,
    layouts: LazyLock::new(|| Layouts::new(SECOND_VALUE)),
};
static MINUTE_DESCRIPTOR: FieldDescriptor = FieldDescriptor {
    name: "minute",
    min: 0,
    max: 59,
    atoi: number_atoi,
    layouts: LazyLock::new(|| Layouts::new(MINUTE_VALUE)),
};
static HOUR_DESCRIPTOR: FieldDescriptor = FieldDescriptor {
    name: "hour",
    min: 0,
    max: 23,
    atoi: number_atoi,
    layouts: LazyLock::new(|| Layouts::new(HOUR_VALUE)),
};
static DOM_DESCRIPTOR: FieldDescriptor = FieldDescriptor {
    name: "day-of-month",
    min: 1,
    max: 31,
    atoi: number_atoi,
    layouts: LazyLock::new(|| Layouts::new(DOM_VALUE)),
};
static MONTH_DESCRIPTOR: FieldDescriptor = FieldDescriptor {
    name: "month",
    min: 1,
    max: 12,
    atoi: month_atoi,
    layouts: LazyLock::new(|| Layouts::new(MONTH_VALUE)),
};
static DOW_DESCRIPTOR: FieldDescriptor = FieldDescriptor {
    name: "day-of-week",
    min: 0,
    max: 6,
    atoi: dow_atoi,
    layouts: LazyLock::new(|| Layouts::new(DOW_VALUE)),
};
static YEAR_DESCRIPTOR: FieldDescriptor = FieldDescriptor {
    name: "year",
    min: 1970,
    max: 2099,
    atoi: number_atoi,
    layouts: LazyLock::new(|| Layouts::new(YEAR_VALUE)),
};

static DOW_OF_LAST_WEEK: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&format!("^({DOW_VALUE})l$")).expect("static cron layout"));
static DOW_OF_SPECIFIC_WEEK: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&format!("^({DOW_VALUE})#([1-5])$")).expect("static cron layout"));
static DOW_LAST_DAY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^l$").expect("static cron layout"));
static DOW_LAST_WORKDAY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^lw$").expect("static cron layout"));
static DOW_WORKDAY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(&format!("^({DOM_VALUE})w$")).expect("static cron layout"));

/// `numberTokens`: `0..=59` (also zero-padded two-digit forms) and the years
/// `1970..=2099`; every other string maps to `0`.
fn number_atoi(s: &str) -> i32 {
    if !s.bytes().all(|byte| byte.is_ascii_digit()) {
        return 0;
    }
    match s.len() {
        1 => s.parse().unwrap_or(0),
        2 => match s.parse::<i32>() {
            Ok(value) if (0..=59).contains(&value) => value,
            _ => 0,
        },
        4 => match s.parse::<i32>() {
            Ok(value) if (1970..=2099).contains(&value) => value,
            _ => 0,
        },
        _ => 0,
    }
}

/// `monthTokens`: numeric months and their three-letter/full names.
fn month_atoi(s: &str) -> i32 {
    match s {
        "1" | "01" | "jan" | "january" => 1,
        "2" | "02" | "feb" | "february" => 2,
        "3" | "03" | "mar" | "march" => 3,
        "4" | "04" | "apr" | "april" => 4,
        "5" | "05" | "may" => 5,
        "6" | "06" | "jun" | "june" => 6,
        "7" | "07" | "jul" | "july" => 7,
        "8" | "08" | "aug" | "august" => 8,
        "9" | "09" | "sep" | "september" => 9,
        "10" | "oct" | "october" => 10,
        "11" | "nov" | "november" => 11,
        "12" | "dec" | "december" => 12,
        _ => 0,
    }
}

/// `dowTokens`: `7` is Sunday; note that `07` is *not* a key in the Go map
/// and therefore also maps to `0`.
fn dow_atoi(s: &str) -> i32 {
    match s {
        "0" | "sun" | "sunday" => 0,
        "1" | "mon" | "monday" => 1,
        "2" | "tue" | "tuesday" => 2,
        "3" | "wed" | "wednesday" => 3,
        "4" | "thu" | "thursday" => 4,
        "5" | "fri" | "friday" => 5,
        "6" | "sat" | "saturday" => 6,
        "7" => 0,
        _ => 0,
    }
}

// ---------------------------------------------------------------------------
// Field parsing (`cronexpr_parse.go`)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DirectiveKind {
    None,
    One,
    Span,
    All,
}

#[derive(Debug, Clone, Copy)]
struct Directive {
    kind: DirectiveKind,
    first: i32,
    last: i32,
    step: i32,
    sbeg: usize,
    send: usize,
}

fn populate_many(values: &mut BTreeSet<i32>, min: i32, max: i32, step: i32) {
    let mut value = min;
    while value <= max {
        values.insert(value);
        value += step;
    }
}

/// `genericFieldHandler`: resolve the directives into a sorted value list.
fn generic_field_handler(s: &str, desc: &FieldDescriptor) -> Result<Vec<i32>, ParseError> {
    let directives = generic_field_parse(s, desc)?;
    let mut values = BTreeSet::new();
    for directive in directives {
        match directive.kind {
            DirectiveKind::None => {
                return Err(ParseError::new(format!(
                    "syntax error in {} field: '{}'",
                    desc.name,
                    &s[directive.sbeg..directive.send]
                )));
            }
            DirectiveKind::One => {
                values.insert(directive.first);
            }
            DirectiveKind::Span => {
                populate_many(&mut values, directive.first, directive.last, directive.step)
            }
            DirectiveKind::All => return Ok(desc.default_list()),
        }
    }
    Ok(values.into_iter().collect())
}

/// `genericFieldParse`: split on `[^,]+` and classify each entry with the
/// baseline's layout order.
fn generic_field_parse(s: &str, desc: &FieldDescriptor) -> Result<Vec<Directive>, ParseError> {
    let entries = entry_indices(s);
    if entries.is_empty() {
        return Err(ParseError::new(format!(
            "{} field: missing directive",
            desc.name
        )));
    }

    let layouts = &desc.layouts;
    let mut directives = Vec::with_capacity(entries.len());
    for (sbeg, send) in entries {
        let mut directive = Directive {
            kind: DirectiveKind::None,
            first: 0,
            last: 0,
            step: 0,
            sbeg,
            send,
        };
        let snormal = s[sbeg..send].to_lowercase();

        if layouts.wildcard.is_match(&snormal) {
            directive.kind = DirectiveKind::All;
            directive.first = desc.min;
            directive.last = desc.max;
            directive.step = 1;
            directives.push(directive);
            continue;
        }
        if layouts.value.is_match(&snormal) {
            directive.kind = DirectiveKind::One;
            directive.first = (desc.atoi)(&snormal);
            directives.push(directive);
            continue;
        }
        if let Some(captures) = layouts.range.captures(&snormal) {
            directive.kind = DirectiveKind::Span;
            directive.first = (desc.atoi)(capture(&captures, 1));
            directive.last = (desc.atoi)(capture(&captures, 2));
            directive.step = 1;
            directives.push(directive);
            continue;
        }
        if let Some(captures) = layouts.wildcard_and_interval.captures(&snormal) {
            directive.kind = DirectiveKind::Span;
            directive.first = desc.min;
            directive.last = desc.max;
            directive.step = number_atoi(capture(&captures, 1));
            if directive.step < 1 || directive.step > desc.max {
                return Err(ParseError::new(format!("invalid interval {snormal}")));
            }
            directives.push(directive);
            continue;
        }
        if let Some(captures) = layouts.value_and_interval.captures(&snormal) {
            directive.kind = DirectiveKind::Span;
            directive.first = (desc.atoi)(capture(&captures, 1));
            directive.last = desc.max;
            directive.step = number_atoi(capture(&captures, 2));
            if directive.step < 1 || directive.step > desc.max {
                return Err(ParseError::new(format!("invalid interval {snormal}")));
            }
            directives.push(directive);
            continue;
        }
        if let Some(captures) = layouts.range_and_interval.captures(&snormal) {
            directive.kind = DirectiveKind::Span;
            directive.first = (desc.atoi)(capture(&captures, 1));
            directive.last = (desc.atoi)(capture(&captures, 2));
            directive.step = number_atoi(capture(&captures, 3));
            if directive.step < 1 || directive.step > desc.max {
                return Err(ParseError::new(format!("invalid interval {snormal}")));
            }
            directives.push(directive);
            continue;
        }

        // No behavior for this one, let the caller deal with it (`L`, `LW`,
        // `15W`, `5L`, `5#3` are recognized by the dom/dow handlers).
        directives.push(directive);
    }
    Ok(directives)
}

fn capture<'t>(captures: &regex::Captures<'t>, index: usize) -> &'t str {
    captures
        .get(index)
        .expect("layout has the expected capture group")
        .as_str()
}

/// `[^,]+`: non-empty comma-free runs, as byte ranges.
fn entry_indices(s: &str) -> Vec<(usize, usize)> {
    let bytes = s.as_bytes();
    let mut entries = Vec::new();
    let mut start = 0;
    for (index, byte) in bytes.iter().enumerate() {
        if *byte == b',' {
            if index > start {
                entries.push((start, index));
            }
            start = index + 1;
        }
    }
    if start < bytes.len() {
        entries.push((start, bytes.len()));
    }
    entries
}

/// `\S+` with Go's ASCII whitespace class (`[\t\n\f\r ]`).
fn field_indices(s: &str) -> Vec<(usize, usize)> {
    let mut fields = Vec::new();
    let mut start = None;
    for (index, character) in s.char_indices() {
        if character.is_ascii_whitespace() {
            if let Some(begin) = start.take() {
                fields.push((begin, index));
            }
        } else if start.is_none() {
            start = Some(index);
        }
    }
    if let Some(begin) = start {
        fields.push((begin, s.len()));
    }
    fields
}

// ---------------------------------------------------------------------------
// Expression
// ---------------------------------------------------------------------------

/// A parsed cron expression (`cronexpr.Expression`).
#[derive(Debug, Clone, Default)]
pub struct Expression {
    second_list: Vec<i32>,
    minute_list: Vec<i32>,
    hour_list: Vec<i32>,
    days_of_month: BTreeSet<i32>,
    workdays_of_month: BTreeSet<i32>,
    last_day_of_month: bool,
    last_workday_of_month: bool,
    days_of_month_restricted: bool,
    month_list: Vec<i32>,
    days_of_week: BTreeSet<i32>,
    specific_week_days_of_week: BTreeSet<i32>,
    last_week_days_of_week: BTreeSet<i32>,
    days_of_week_restricted: bool,
    year_list: Vec<i32>,
}

/// `cronNormalizer`: the built-in aliases expand to 7-field expressions.
fn normalize(cron_line: &str) -> String {
    cron_line
        .replace("@yearly", "0 0 0 1 1 * *")
        .replace("@annually", "0 0 0 1 1 * *")
        .replace("@monthly", "0 0 0 1 * * *")
        .replace("@weekly", "0 0 0 * * 0 *")
        .replace("@daily", "0 0 0 * * * *")
        .replace("@hourly", "0 0 * * * * *")
}

impl Expression {
    /// `Parse`.
    pub fn parse(cron_line: &str) -> Result<Self, ParseError> {
        let cron = normalize(cron_line);
        let indices = field_indices(&cron);
        let field_count = indices.len();
        if field_count < 5 {
            return Err(ParseError::new("missing field(s)"));
        }
        // Ignore fields beyond the 7th.
        let field_count = field_count.min(7);

        let mut expr = Expression::default();
        let mut field = 0;

        // Second field (optional).
        if field_count == 7 {
            expr.second_list =
                generic_field_handler(&cron[indices[0].0..indices[0].1], &SECOND_DESCRIPTOR)?;
            field += 1;
        } else {
            expr.second_list = vec![0];
        }

        expr.minute_list = generic_field_handler(
            &cron[indices[field].0..indices[field].1],
            &MINUTE_DESCRIPTOR,
        )?;
        field += 1;
        expr.hour_list =
            generic_field_handler(&cron[indices[field].0..indices[field].1], &HOUR_DESCRIPTOR)?;
        field += 1;
        expr.dom_field_handler(&cron[indices[field].0..indices[field].1])?;
        field += 1;
        expr.month_list =
            generic_field_handler(&cron[indices[field].0..indices[field].1], &MONTH_DESCRIPTOR)?;
        field += 1;
        expr.dow_field_handler(&cron[indices[field].0..indices[field].1])?;
        field += 1;

        if field < field_count {
            expr.year_list =
                generic_field_handler(&cron[indices[field].0..indices[field].1], &YEAR_DESCRIPTOR)?;
        } else {
            expr.year_list = YEAR_DESCRIPTOR.default_list();
        }

        Ok(expr)
    }

    /// `(*Expression).Next`: the first matching instant strictly after
    /// `from`; `None` mirrors Go's zero `time.Time`.
    pub fn next(&self, from: OffsetDateTime) -> Option<OffsetDateTime> {
        // `fromTime.Add(time.Second - fromTime.Nanosecond())`: the next whole
        // second strictly after `from`.
        let mut t = from + Duration::nanoseconds(1_000_000_000 - i64::from(from.nanosecond()));

        'wrap: loop {
            // Year.
            let value = t.year();
            match search_int(&self.year_list, value) {
                None => return None,
                Some(index) if self.year_list[index] != value => {
                    t = utc_datetime(self.year_list[index], self.month_list[0], 1, 0, 0, 0)?;
                }
                Some(_) => {}
            }

            // Month.
            let value = t.month() as i32;
            match search_int(&self.month_list, value) {
                None => {
                    t = utc_datetime(t.year() + 1, self.month_list[0], 1, 0, 0, 0)?;
                    continue 'wrap;
                }
                Some(index) if self.month_list[index] != value => {
                    t = utc_datetime(t.year(), self.month_list[index], 1, 0, 0, 0)?;
                }
                Some(_) => {}
            }

            // Day of month.
            let actual_days = actual_days_of_month(self, t.year(), t.month());
            if actual_days.is_empty() {
                t = utc_datetime(t.year(), t.month() as i32 + 1, 1, 0, 0, 0)?;
                continue 'wrap;
            }
            let value = i32::from(t.day());
            match search_int(&actual_days, value) {
                None => {
                    t = utc_datetime(t.year(), t.month() as i32 + 1, 1, 0, 0, 0)?;
                    continue 'wrap;
                }
                Some(index) if actual_days[index] != value => {
                    t = utc_datetime(t.year(), t.month() as i32, actual_days[index], 0, 0, 0)?;
                }
                Some(_) => {}
            }

            // `timeZoneInDay` is always false for UTC, so the baseline's
            // SLOW_CLOCK branch cannot be reached here.
            let value = i32::from(t.hour());
            match search_int(&self.hour_list, value) {
                None => {
                    t = utc_datetime(t.year(), t.month() as i32, i32::from(t.day()) + 1, 0, 0, 0)?;
                    continue 'wrap;
                }
                Some(index) if self.hour_list[index] != value => {
                    t = utc_datetime(
                        t.year(),
                        t.month() as i32,
                        i32::from(t.day()),
                        self.hour_list[index],
                        self.minute_list[0],
                        self.second_list[0],
                    )?;
                }
                Some(_) => {}
            }

            let value = i32::from(t.minute());
            match search_int(&self.minute_list, value) {
                None => {
                    t = utc_datetime(
                        t.year(),
                        t.month() as i32,
                        i32::from(t.day()),
                        i32::from(t.hour()) + 1,
                        0,
                        0,
                    )?;
                    continue 'wrap;
                }
                Some(index) if self.minute_list[index] != value => {
                    t = utc_datetime(
                        t.year(),
                        t.month() as i32,
                        i32::from(t.day()),
                        i32::from(t.hour()),
                        self.minute_list[index],
                        self.second_list[0],
                    )?;
                }
                Some(_) => {}
            }

            let value = i32::from(t.second());
            match search_int(&self.second_list, value) {
                None => {
                    t = utc_datetime(
                        t.year(),
                        t.month() as i32,
                        i32::from(t.day()),
                        i32::from(t.hour()),
                        i32::from(t.minute()) + 1,
                        0,
                    )?;
                    continue 'wrap;
                }
                Some(index) if self.second_list[index] != value => {
                    t = utc_datetime(
                        t.year(),
                        t.month() as i32,
                        i32::from(t.day()),
                        i32::from(t.hour()),
                        i32::from(t.minute()),
                        self.second_list[index],
                    )?;
                }
                Some(_) => {}
            }

            return Some(t);
        }
    }

    /// `(*Expression).dayOfWeekFieldHandler`.
    fn dow_field_handler(&mut self, s: &str) -> Result<(), ParseError> {
        self.days_of_week_restricted = true;
        self.days_of_week.clear();
        self.last_week_days_of_week.clear();
        self.specific_week_days_of_week.clear();

        let directives = generic_field_parse(s, &DOW_DESCRIPTOR)?;
        for mut directive in directives {
            match directive.kind {
                DirectiveKind::None => {
                    let sdirective = &s[directive.sbeg..directive.send];
                    let snormal = sdirective.to_lowercase();
                    if let Some(captures) = DOW_OF_LAST_WEEK.captures(&snormal) {
                        // `5L`
                        self.last_week_days_of_week
                            .insert(dow_atoi(capture(&captures, 1)));
                    } else if let Some(captures) = DOW_OF_SPECIFIC_WEEK.captures(&snormal) {
                        // `5#3`
                        let day = dow_atoi(capture(&captures, 1));
                        let week: i32 = capture(&captures, 2).parse().unwrap_or(0);
                        self.specific_week_days_of_week
                            .insert((week - 1) * 7 + (day % 7));
                    } else {
                        return Err(ParseError::new(format!(
                            "syntax error in day-of-week field: '{sdirective}'"
                        )));
                    }
                }
                DirectiveKind::One => {
                    self.days_of_week.insert(directive.first);
                }
                DirectiveKind::Span => {
                    // To properly handle spans that end in 7 (Sunday).
                    if directive.last == 0 {
                        directive.last = 6;
                    }
                    populate_many(
                        &mut self.days_of_week,
                        directive.first,
                        directive.last,
                        directive.step,
                    );
                }
                DirectiveKind::All => {
                    populate_many(
                        &mut self.days_of_week,
                        directive.first,
                        directive.last,
                        directive.step,
                    );
                    self.days_of_week_restricted = false;
                }
            }
        }
        Ok(())
    }

    /// `(*Expression).dayOfMonthFieldHandler`.
    fn dom_field_handler(&mut self, s: &str) -> Result<(), ParseError> {
        self.days_of_month_restricted = true;
        self.last_day_of_month = false;
        self.last_workday_of_month = false;
        self.days_of_month.clear();
        self.workdays_of_month.clear();

        let directives = generic_field_parse(s, &DOM_DESCRIPTOR)?;
        for directive in directives {
            match directive.kind {
                DirectiveKind::None => {
                    let sdirective = &s[directive.sbeg..directive.send];
                    let snormal = sdirective.to_lowercase();
                    if DOW_LAST_DAY.is_match(&snormal) {
                        // `L`
                        self.last_day_of_month = true;
                    } else if DOW_LAST_WORKDAY.is_match(&snormal) {
                        // `LW`
                        self.last_workday_of_month = true;
                    } else if let Some(captures) = DOW_WORKDAY.captures(&snormal) {
                        // `15W`
                        self.workdays_of_month
                            .insert(number_atoi(capture(&captures, 1)));
                    } else {
                        return Err(ParseError::new(format!(
                            "syntax error in day-of-month field: '{sdirective}'"
                        )));
                    }
                }
                DirectiveKind::One => {
                    self.days_of_month.insert(directive.first);
                }
                DirectiveKind::Span => populate_many(
                    &mut self.days_of_month,
                    directive.first,
                    directive.last,
                    directive.step,
                ),
                DirectiveKind::All => {
                    populate_many(
                        &mut self.days_of_month,
                        directive.first,
                        directive.last,
                        directive.step,
                    );
                    self.days_of_month_restricted = false;
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Next helpers (`cronexpr_next.go`)
// ---------------------------------------------------------------------------

const DOW_NORMALIZED_OFFSETS: [&[i32]; 7] = [
    &[1, 8, 15, 22, 29],
    &[2, 9, 16, 23, 30],
    &[3, 10, 17, 24, 31],
    &[4, 11, 18, 25],
    &[5, 12, 19, 26],
    &[6, 13, 20, 27],
    &[7, 14, 21, 28],
];

/// `sort.SearchInts`: index of the first element `>= value`, or `None` when
/// every element is smaller.
fn search_int(values: &[i32], value: i32) -> Option<usize> {
    match values.binary_search(&value) {
        Ok(index) => Some(index),
        Err(index) if index == values.len() => None,
        Err(index) => Some(index),
    }
}

/// `time.Date(year, month, day, hour, minute, second, 0, UTC)` with Go's
/// month/day overflow normalization.
fn utc_datetime(
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    minute: i32,
    second: i32,
) -> Option<OffsetDateTime> {
    let months = i64::from(year) * 12 + i64::from(month) - 1;
    let normalized_year = i32::try_from(months.div_euclid(12)).ok()?;
    let normalized_month = u8::try_from(months.rem_euclid(12) + 1).ok()?;
    let month = Month::try_from(normalized_month).ok()?;
    let first = Date::from_calendar_date(normalized_year, month, 1).ok()?;
    let base = PrimitiveDateTime::new(first, Time::MIDNIGHT).assume_utc();
    Some(
        base + Duration::days(i64::from(day) - 1)
            + Duration::hours(i64::from(hour))
            + Duration::minutes(i64::from(minute))
            + Duration::seconds(i64::from(second)),
    )
}

fn first_of_month(year: i32, month: Month) -> Option<OffsetDateTime> {
    Some(
        PrimitiveDateTime::new(
            Date::from_calendar_date(year, month, 1).ok()?,
            Time::MIDNIGHT,
        )
        .assume_utc(),
    )
}

/// `calculateActualDaysOfMonth`.
fn actual_days_of_month(expr: &Expression, year: i32, month: Month) -> Vec<i32> {
    let Some(first) = first_of_month(year, month) else {
        return Vec::new();
    };
    let last_day = i32::from(time::util::days_in_month(month, year));
    let last = first + Duration::days(i64::from(last_day - 1));

    if !expr.days_of_month_restricted && !expr.days_of_week_restricted {
        return (1..=last_day).collect();
    }

    let mut days = BTreeSet::new();

    if expr.days_of_month_restricted {
        if expr.last_day_of_month {
            days.insert(last_day);
        }
        if expr.last_workday_of_month {
            days.insert(workday_of_month(last, last));
        }
        for day in &expr.days_of_month {
            if *day <= last_day {
                days.insert(*day);
            }
        }
        for day in &expr.workdays_of_month {
            if *day <= last_day {
                days.insert(workday_of_month(
                    first + Duration::days(i64::from(*day) - 1),
                    last,
                ));
            }
        }
    }

    if expr.days_of_week_restricted {
        // How far the first Sunday is from the first day of the month.
        let offset = 7 - i32::from(first.weekday().number_days_from_sunday());
        for day in &expr.days_of_week {
            let week = DOW_NORMALIZED_OFFSETS[((offset + *day) % 7) as usize];
            for candidate in week.iter().take(4) {
                days.insert(*candidate);
            }
            if let Some(candidate) = week.get(4) {
                if *candidate <= last_day {
                    days.insert(*candidate);
                }
            }
        }

        for day in &expr.specific_week_days_of_week {
            let candidate = 1 + 7 * (*day / 7) + (offset + *day) % 7;
            if candidate <= last_day {
                days.insert(candidate);
            }
        }

        let last_week_origin = last - Duration::days(6);
        let offset = 7 - i32::from(last_week_origin.weekday().number_days_from_sunday());
        for day in &expr.last_week_days_of_week {
            let candidate = i32::from(last_week_origin.day()) + (offset + *day) % 7;
            if candidate <= last_day {
                days.insert(candidate);
            }
        }
    }

    days.into_iter().collect()
}

/// `workdayOfMonth`.
fn workday_of_month(target: OffsetDateTime, last: OffsetDateTime) -> i32 {
    let mut day = i32::from(target.day());
    match target.weekday() {
        Weekday::Saturday => {
            if day > 1 {
                day -= 1;
            } else {
                day += 2;
            }
        }
        Weekday::Sunday => {
            if day < i32::from(last.day()) {
                day += 1;
            } else {
                day -= 2;
            }
        }
        _ => {}
    }
    day
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse `YYYY-MM-DDTHH:MM:SS[.fraction]Z` (the crate disables `time`'s
    /// parsing/formatting features to keep the production dependency small).
    fn at(text: &str) -> OffsetDateTime {
        let (date, rest) = text.split_once('T').expect("date and time");
        let mut date_parts = date.split('-');
        let year: i32 = date_parts.next().unwrap().parse().unwrap();
        let month: u8 = date_parts.next().unwrap().parse().unwrap();
        let day: u8 = date_parts.next().unwrap().parse().unwrap();

        let (clock, fraction) = match rest.trim_end_matches('Z').split_once('.') {
            Some((clock, fraction)) => (clock, Some(fraction)),
            None => (rest.trim_end_matches('Z'), None),
        };
        let mut clock_parts = clock.split(':');
        let hour: u8 = clock_parts.next().unwrap().parse().unwrap();
        let minute: u8 = clock_parts.next().unwrap().parse().unwrap();
        let second: u8 = clock_parts.next().unwrap().parse().unwrap();

        let mut value = PrimitiveDateTime::new(
            Date::from_calendar_date(year, Month::try_from(month).unwrap(), day).unwrap(),
            Time::from_hms(hour, minute, second).unwrap(),
        )
        .assume_utc();
        if let Some(fraction) = fraction {
            let padded = format!("{fraction:0<9}");
            let nanos: i64 = padded[..9].parse().expect("fractional seconds");
            value += Duration::nanoseconds(nanos);
        }
        value
    }

    fn show(value: OffsetDateTime) -> String {
        format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
            value.year(),
            u8::from(value.month()),
            value.day(),
            value.hour(),
            value.minute(),
            value.second()
        )
    }

    fn next_of(expression: &str, from: &str) -> String {
        let parsed = Expression::parse(expression).expect("valid expression");
        show(parsed.next(at(from)).expect("expected a next occurrence"))
    }

    /// `(expression, from, expected)` rows from the upstream `crontests`
    /// table (UTC cases; the DST cases are out of scope, see the crate docs).
    const CASES: &[(&str, &str, &str)] = &[
        (
            "* * * * * * *",
            "2013-01-01T00:00:00Z",
            "2013-01-01 00:00:01",
        ),
        (
            "* * * * * * *",
            "2013-01-01T00:00:59Z",
            "2013-01-01 00:01:00",
        ),
        (
            "* * * * * * *",
            "2013-01-01T00:59:59Z",
            "2013-01-01 01:00:00",
        ),
        (
            "* * * * * * *",
            "2013-01-01T23:59:59Z",
            "2013-01-02 00:00:00",
        ),
        (
            "* * * * * * *",
            "2013-02-28T23:59:59Z",
            "2013-03-01 00:00:00",
        ),
        (
            "* * * * * * *",
            "2016-02-28T23:59:59Z",
            "2016-02-29 00:00:00",
        ),
        (
            "* * * * * * *",
            "2012-12-31T23:59:59Z",
            "2013-01-01 00:00:00",
        ),
        (
            "*/5 * * * * * *",
            "2013-01-01T00:00:00Z",
            "2013-01-01 00:00:05",
        ),
        (
            "*/5 * * * * * *",
            "2013-01-01T00:00:59Z",
            "2013-01-01 00:01:00",
        ),
        ("* * * * *", "2013-01-01T00:00:00Z", "2013-01-01 00:01:00"),
        ("* * * * *", "2013-01-01T00:00:59Z", "2013-01-01 00:01:00"),
        ("* * * * *", "2013-01-01T00:59:00Z", "2013-01-01 01:00:00"),
        ("* * * * *", "2016-02-28T23:59:00Z", "2016-02-29 00:00:00"),
        (
            "17-43/5 * * * *",
            "2013-01-01T00:00:00Z",
            "2013-01-01 00:17:00",
        ),
        (
            "17-43/5 * * * *",
            "2013-01-01T00:16:59Z",
            "2013-01-01 00:17:00",
        ),
        (
            "17-43/5 * * * *",
            "2013-01-01T00:30:00Z",
            "2013-01-01 00:32:00",
        ),
        (
            "17-43/5 * * * *",
            "2013-01-01T00:50:00Z",
            "2013-01-01 01:17:00",
        ),
        (
            "15-30/4,55 * * * *",
            "2013-01-01T00:00:00Z",
            "2013-01-01 00:15:00",
        ),
        (
            "15-30/4,55 * * * *",
            "2013-01-01T00:16:00Z",
            "2013-01-01 00:19:00",
        ),
        (
            "15-30/4,55 * * * *",
            "2013-01-01T00:30:00Z",
            "2013-01-01 00:55:00",
        ),
        (
            "15-30/4,55 * * * *",
            "2013-01-01T00:55:00Z",
            "2013-01-01 01:15:00",
        ),
        ("0 0 * * MON", "2013-01-01T00:00:00Z", "2013-01-07 00:00:00"),
        ("0 0 * * MON", "2013-01-28T00:00:00Z", "2013-02-04 00:00:00"),
        (
            "0 0 * * friday",
            "2013-01-01T00:00:00Z",
            "2013-01-04 00:00:00",
        ),
        (
            "0 0 * * friday",
            "2013-12-30T00:30:00Z",
            "2014-01-03 00:00:00",
        ),
        ("0 0 * * 6,7", "2013-01-01T00:00:00Z", "2013-01-05 00:00:00"),
        ("0 0 * * 6,7", "2013-12-30T00:30:00Z", "2014-01-04 00:00:00"),
        ("0 0 * * 5-7", "2013-01-01T00:00:00Z", "2013-01-04 00:00:00"),
        ("0 0 * * 6#5", "2013-09-02T00:00:00Z", "2013-11-30 00:00:00"),
        ("0 0 * * 5L", "2013-09-02T00:00:00Z", "2013-09-27 00:00:00"),
        ("0 0 14W * *", "2013-03-31T00:00:00Z", "2013-04-15 00:00:00"),
        ("0 0 14W * *", "2013-08-31T00:00:00Z", "2013-09-13 00:00:00"),
        ("0 0 30W * *", "2013-03-02T00:00:00Z", "2013-03-29 00:00:00"),
        ("0 0 30W * *", "2013-06-02T00:00:00Z", "2013-06-28 00:00:00"),
        ("0 0 30W * *", "2013-09-02T00:00:00Z", "2013-09-30 00:00:00"),
        ("0 0 30W * *", "2013-11-02T00:00:00Z", "2013-11-29 00:00:00"),
        ("0 0 L * *", "2013-09-02T00:00:00Z", "2013-09-30 00:00:00"),
        ("0 0 L * *", "2014-01-01T00:00:00Z", "2014-01-31 00:00:00"),
        ("0 0 L * *", "2014-02-01T00:00:00Z", "2014-02-28 00:00:00"),
        ("0 0 L * *", "2016-02-15T00:00:00Z", "2016-02-29 00:00:00"),
        ("0 0 LW * *", "2013-09-02T00:00:00Z", "2013-09-30 00:00:00"),
        ("0 0 LW * *", "2013-11-02T00:00:00Z", "2013-11-29 00:00:00"),
        ("0 0 LW * *", "2014-08-15T00:00:00Z", "2014-08-29 00:00:00"),
        (
            "0 0 * 04 * *",
            "2013-09-02T00:00:00Z",
            "2014-04-01 00:00:00",
        ),
        (
            "0 0 * 04 * *",
            "2014-04-03T03:00:00Z",
            "2014-04-04 00:00:00",
        ),
        (
            "0 0 * 04 * *",
            "2014-08-15T00:00:00Z",
            "2015-04-01 00:00:00",
        ),
        // Both day fields restricted: the union of the two fields matches.
        ("0 0 1 * MON", "2013-01-02T00:00:00Z", "2013-01-07 00:00:00"),
        ("0 0 1 * MON", "2013-01-08T00:00:00Z", "2013-01-14 00:00:00"),
        ("0 0 1 * MON", "2013-01-29T00:00:00Z", "2013-02-01 00:00:00"),
        // `?` behaves like `*`: the day field is unrestricted.
        ("0 0 ? * MON", "2013-01-01T00:00:00Z", "2013-01-07 00:00:00"),
        // Sub-second inputs are rounded up to the next whole second first.
        (
            "* * * * * * *",
            "2013-01-01T00:00:00.5Z",
            "2013-01-01 00:00:01",
        ),
    ];

    #[test]
    fn upstream_cases() {
        for (expression, from, expected) in CASES {
            assert_eq!(&next_of(expression, from), expected, "{expression}");
        }
    }

    #[test]
    fn aliases() {
        assert_eq!(
            next_of("@weekly", "2024-11-06T12:00:00Z"),
            "2024-11-10 00:00:00"
        );
        assert_eq!(
            next_of("@daily", "2024-11-06T12:00:00Z"),
            "2024-11-07 00:00:00"
        );
        assert_eq!(
            next_of("@hourly", "2024-11-06T12:00:00Z"),
            "2024-11-06 13:00:00"
        );
        assert_eq!(
            next_of("@monthly", "2024-11-06T12:00:00Z"),
            "2024-12-01 00:00:00"
        );
        assert_eq!(
            next_of("@yearly", "2024-11-06T12:00:00Z"),
            "2025-01-01 00:00:00"
        );
        assert_eq!(
            next_of("@annually", "2024-11-06T12:00:00Z"),
            "2025-01-01 00:00:00"
        );
    }

    #[test]
    fn next_n_for_specific_week() {
        let expr = Expression::parse("0 0 * * 6#5").expect("valid expression");
        let mut cursor = at("2013-09-02T08:44:30Z");
        let mut results = Vec::new();
        for _ in 0..5 {
            cursor = expr.next(cursor).expect("next occurrence");
            results.push(show(cursor));
        }
        assert_eq!(
            results,
            vec![
                "2013-11-30 00:00:00",
                "2014-03-29 00:00:00",
                "2014-05-31 00:00:00",
                "2014-08-30 00:00:00",
                "2014-11-29 00:00:00",
            ]
        );
    }

    #[test]
    fn year_exhaustion_matches_go_zero_time() {
        let from = at("2013-08-31T00:00:00Z");
        assert_eq!(
            Expression::parse("* * * * * 1980")
                .expect("valid expression")
                .next(from),
            None
        );
        assert!(Expression::parse("* * * * * 2050")
            .expect("valid expression")
            .next(from)
            .is_some());
    }

    #[test]
    fn interval_bounds() {
        for expression in [
            "*/60 * * * * *",
            "*/61 * * * * *",
            "2/60 * * * * *",
            "2-20/61 * * * * *",
        ] {
            assert!(
                Expression::parse(expression).is_err(),
                "{expression} must be rejected"
            );
        }
        assert!(Expression::parse("*/59 * * * * *").is_ok());
        assert!(Expression::parse("4-20/3 * * * * *").is_ok());
    }

    #[test]
    fn parse_errors_match_the_baseline_messages() {
        let error = |expression: &str| {
            Expression::parse(expression)
                .expect_err("expression must be rejected")
                .to_string()
        };
        assert_eq!(error(""), "missing field(s)");
        assert_eq!(error("* *"), "missing field(s)");
        assert_eq!(error("* * * *"), "missing field(s)");
        assert_eq!(error("99 * * * *"), "syntax error in minute field: '99'");
        assert_eq!(
            error("0 0 99 * *"),
            "syntax error in day-of-month field: '99'"
        );
        assert_eq!(
            error("0 0 * * 99"),
            "syntax error in day-of-week field: '99'"
        );
        assert_eq!(error("0 0 * 13 *"), "syntax error in month field: '13'");
    }

    #[test]
    fn extra_fields_are_ignored() {
        // 8th and later fields are dropped, so this parses like
        // `0 0 0 * * * *`.
        let expression = Expression::parse("0 0 0 * * * * 1999 2000").expect("valid expression");
        assert_eq!(
            some_next(expression, "2024-11-06T12:00:00Z"),
            "2024-11-07 00:00:00"
        );
    }

    fn some_next(expression: Expression, from: &str) -> String {
        show(expression.next(at(from)).expect("next occurrence"))
    }
}
