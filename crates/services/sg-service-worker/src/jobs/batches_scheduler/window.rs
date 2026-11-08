#![forbid(unsafe_code)]

//! `internal/batches/types/scheduler/window`: rollout window parsing and
//! schedule arithmetic.
//!
//! Ported semantics:
//!
//! * `parseWindow` / `parseConfiguration` — all errors are accumulated with
//!   `errors.Append`'s formatting (single error verbatim, otherwise
//!   `N errors occurred:\n\t* ...`) and configuration errors are wrapped as
//!   `window %d: ...`;
//! * `Window.IsOpen` / `Window.NextOpenAfter`;
//! * `Configuration.{windowFor,scheduleAt,Estimate,HasRolloutWindows}` and
//!   `Schedule.total`.
//!
//! Deliberate deviations (documented in docs/modules/06_worker.md §50):
//!
//! * the baseline's unreachable-in-practice `panic` paths (`windowFor`'s
//!   week-long day search and `NextOpenAfter`'s week guard) return a value
//!   instead of panicking;
//! * `Rate` only carries the fields `(n, unit)`; the uber-go/ratelimit
//!   limiter is not constructed here (the scheduler enforces the spacing).

use std::time::Duration;

use serde_json::Value;
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset, Weekday};

/// `time.Hour` in nanoseconds, the unit `Schedule.total` works in.
const HOUR: i64 = 3600;
/// `7 * 24 * time.Hour`.
const WEEK: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// `24 * time.Hour`.
const DAY: Duration = Duration::from_secs(24 * 60 * 60);

/// `strconv.Quote` for the error messages that embed a token (copied from
/// `sg-metrics`/`textfmt`, which ported the same baseline helper). Public so
/// the differential harness can print errors with the baseline's `%q`.
pub fn go_quote(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{7}' => out.push_str("\\a"),
            '\u{8}' => out.push_str("\\b"),
            '\u{b}' => out.push_str("\\v"),
            '\u{c}' => out.push_str("\\f"),
            ch if (ch as u32) < 0x20 || ch == '\u{7f}' => {
                out.push_str(&format!("\\x{:02x}", ch as u32));
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

/// `errors.Append`'s `%v` formatting: one error is printed verbatim, several
/// as cockroachdb's `N errors occurred:\n\t* ...`.
pub fn join_errors(errs: &[String]) -> String {
    if errs.len() == 1 {
        return errs[0].clone();
    }
    let mut out = format!("{} errors occurred:", errs.len());
    for err in errs {
        out.push_str("\n\t* ");
        out.push_str(err);
    }
    out
}

/// `timeOfDay`: an hour/minute pair compared by minutes since midnight.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeOfDay {
    hour: u8,
    minute: u8,
}

impl TimeOfDay {
    pub const fn from_parts(hour: u8, minute: u8) -> Self {
        Self { hour, minute }
    }

    /// `timeOfDayFromTime` (the caller passes a UTC instant).
    pub fn from_time(at: OffsetDateTime) -> Self {
        Self {
            hour: at.hour(),
            minute: at.minute(),
        }
    }

    pub fn hour(self) -> u8 {
        self.hour
    }

    pub fn minute(self) -> u8 {
        self.minute
    }

    fn cmp_key(self) -> u16 {
        u16::from(self.hour) * 60 + u16::from(self.minute)
    }

    pub fn before(self, other: Self) -> bool {
        self.cmp_key() < other.cmp_key()
    }

    pub fn after(self, other: Self) -> bool {
        self.cmp_key() > other.cmp_key()
    }

    pub fn equal(self, other: Self) -> bool {
        self.cmp_key() == other.cmp_key()
    }
}

/// `weekdaySet`: bits 0..=6 are Sunday..=Saturday; zero means "all days".
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WeekdaySet(i8);

impl WeekdaySet {
    /// `(ws).all`: an empty set and the full mask both mean every day.
    pub fn all(self) -> bool {
        self.0 == 0 || self.0 == 127
    }

    /// `(ws).includes`.
    pub fn includes(self, day: Weekday) -> bool {
        if self.all() {
            return true;
        }
        (self.0 & weekday_to_bit(day)) != 0
    }

    fn add(&mut self, day: Weekday) {
        self.0 |= weekday_to_bit(day);
    }

    /// The raw bitmask, for the differential harness.
    pub fn mask(self) -> i8 {
        self.0
    }
}

/// `weekdayToBit`; `time::Weekday` numbers days from Sunday just like Go.
fn weekday_to_bit(day: Weekday) -> i8 {
    1 << day.number_days_from_sunday()
}

/// `rateUnit`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateUnit {
    Second,
    Minute,
    Hour,
}

impl RateUnit {
    /// `(ru).AsDuration`.
    pub fn as_duration(self) -> Duration {
        match self {
            RateUnit::Second => Duration::from_secs(1),
            RateUnit::Minute => Duration::from_secs(60),
            RateUnit::Hour => Duration::from_secs(3600),
        }
    }
}

/// `rate`: `n == -1` is unlimited, `n == 0` never fires, `n > 0` is
/// `n` events per `unit`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rate {
    n: i64,
    unit: RateUnit,
}

impl Rate {
    pub const fn unlimited() -> Self {
        Self {
            n: -1,
            unit: RateUnit::Second,
        }
    }

    pub const fn zero() -> Self {
        Self {
            n: 0,
            unit: RateUnit::Second,
        }
    }

    /// `(r).IsUnlimited`.
    pub fn is_unlimited(self) -> bool {
        self.n == -1
    }

    pub fn n(self) -> i64 {
        self.n
    }

    pub fn unit(self) -> RateUnit {
        self.unit
    }

    /// The spacing the scheduler enforces between enqueues. `uber-go/ratelimit`
    /// spaces takes by `unit/n` (and allows a one-unit burst); the port uses
    /// the plain spacing and documents the difference.
    pub fn spacing(self) -> Duration {
        if self.n > 0 {
            self.unit.as_duration() / self.n as u32
        } else {
            Duration::ZERO
        }
    }
}

/// `parseTimePart`: `strconv.ParseInt(s, 10, 8)`.
fn parse_time_part(s: &str) -> Option<i8> {
    s.parse::<i8>().ok()
}

/// `parseWindowTime`.
fn parse_window_time(raw: &str) -> Result<Option<TimeOfDay>, String> {
    if raw.is_empty() {
        return Ok(None);
    }

    let malformed = || format!("malformed time: {}", go_quote(raw));

    let Some((hour_part, minute_part)) = raw.split_once(':') else {
        return Err(malformed());
    };
    // `strings.SplitN(raw, ":", 2)`: a second colon stays in the minute part,
    // which then fails the integer parse.
    let hour = parse_time_part(hour_part).filter(|hour| (0..=23).contains(hour));
    let minute = parse_time_part(minute_part).filter(|minute| (0..=59).contains(minute));
    match (hour, minute) {
        (Some(hour), Some(minute)) => Ok(Some(TimeOfDay::from_parts(hour as u8, minute as u8))),
        _ => Err(malformed()),
    }
}

/// `parseWeekday`: lowercases the first three bytes and matches a prefix.
fn parse_weekday(raw: &str) -> Result<Weekday, String> {
    let unknown = || format!("unknown weekday: {}", go_quote(raw));
    if raw.len() < 3 {
        return Err(unknown());
    }
    let lower: Vec<u8> = raw.as_bytes()[..3]
        .iter()
        .map(u8::to_ascii_lowercase)
        .collect();
    match lower.as_slice() {
        b"sun" => Ok(Weekday::Sunday),
        b"mon" => Ok(Weekday::Monday),
        b"tue" => Ok(Weekday::Tuesday),
        b"wed" => Ok(Weekday::Wednesday),
        b"thu" => Ok(Weekday::Thursday),
        b"fri" => Ok(Weekday::Friday),
        b"sat" => Ok(Weekday::Saturday),
        _ => Err(unknown()),
    }
}

/// `parseRateUnit`.
fn parse_rate_unit(raw: &str) -> Option<RateUnit> {
    match raw.as_bytes().first() {
        Some(b's' | b'S') => Some(RateUnit::Second),
        Some(b'm' | b'M') => Some(RateUnit::Minute),
        Some(b'h' | b'H') => Some(RateUnit::Hour),
        _ => None,
    }
}

/// `parseRate`. `raw` is the JSON value of the `rate` field (`None` when the
/// key is absent, which Go decodes as a nil `any`).
pub fn parse_rate(raw: Option<&Value>) -> Result<Rate, String> {
    let malformed = |value: &str| format!("malformed rate: {}", go_quote(value));
    match raw {
        None | Some(Value::Null) => Err("malformed rate: unknown type <nil>".to_owned()),
        // Go decodes every JSON number into a float64 when the target is `any`.
        Some(Value::Number(_)) => Err("malformed rate: unknown type float64".to_owned()),
        Some(Value::Bool(_)) => Err("malformed rate: unknown type bool".to_owned()),
        Some(Value::Array(_)) => Err("malformed rate: unknown type []interface {}".to_owned()),
        Some(Value::Object(_)) => {
            Err("malformed rate: unknown type map[string]interface {}".to_owned())
        }
        Some(Value::String(original)) => {
            // `s := strings.ToLower(v)`; errors quote the original value.
            let lowered = original.to_lowercase();
            if lowered == "unlimited" {
                return Ok(Rate::unlimited());
            }

            let Some((count, unit)) = lowered.split_once('/') else {
                return Err(malformed(original));
            };
            let Some(n) = count.parse::<i64>().ok().filter(|n| *n >= 0) else {
                return Err(malformed(original));
            };
            let Some(unit) = parse_rate_unit(unit) else {
                return Err(malformed(original));
            };
            Ok(Rate { n, unit })
        }
    }
}

/// `Window`: a single rollout window configured on the site.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Window {
    days: WeekdaySet,
    start: Option<TimeOfDay>,
    end: Option<TimeOfDay>,
    rate: Rate,
}

impl Window {
    /// `parseWindow`. Non-object values cannot reach this function through the
    /// site configuration (the schema decodes them first); they are treated as
    /// an empty object.
    pub fn parse(raw: &Value) -> Result<Self, String> {
        let Value::Object(object) = raw else {
            return Err("raw window cannot be nil".to_owned());
        };

        let mut errs = Vec::new();

        let mut days = WeekdaySet::default();
        if let Some(Value::Array(items)) = object.get("days") {
            for item in items {
                match item.as_str() {
                    Some(day) => match parse_weekday(day) {
                        Ok(day) => days.add(day),
                        Err(err) => errs.push(err),
                    },
                    // `[]string` rejects non-strings at decode time.
                    None => errs.push(format!("unknown weekday: {}", go_quote(&item.to_string()))),
                }
            }
        }

        let mut start = None;
        match parse_window_time(object.get("start").and_then(Value::as_str).unwrap_or("")) {
            Ok(value) => start = value,
            Err(err) => errs.push(format!("start time: {err}")),
        }

        let mut end = None;
        match parse_window_time(object.get("end").and_then(Value::as_str).unwrap_or("")) {
            Ok(value) => end = value,
            Err(err) => errs.push(format!("end time: {err}")),
        }

        if start.is_some() != end.is_some() {
            errs.push("both start and end times must be provided".to_owned());
        } else if let (Some(start), Some(end)) = (start, end) {
            if !start.before(end) {
                errs.push("end time must be after the start time".to_owned());
            }
        }

        let rate = match parse_rate(object.get("rate")) {
            Ok(rate) => rate,
            Err(err) => {
                errs.push(err);
                Rate::zero()
            }
        };

        if errs.is_empty() {
            Ok(Self {
                days,
                start,
                end,
                rate,
            })
        } else {
            Err(join_errors(&errs))
        }
    }

    /// `(w).covers`: start is inclusive, end exclusive.
    fn covers(&self, when: TimeOfDay) -> bool {
        match (self.start, self.end) {
            (Some(start), Some(end)) => !(when.before(start) || when.after(end) || when.equal(end)),
            _ => true,
        }
    }

    /// `(w).IsOpen`.
    pub fn is_open(&self, at: OffsetDateTime) -> bool {
        self.days.includes(at.weekday()) && self.covers(TimeOfDay::from_time(at))
    }

    /// `(w).NextOpenAfter`.
    pub fn next_open_after(&self, after: OffsetDateTime) -> OffsetDateTime {
        if self.is_open(after) {
            return after;
        }

        let start = self.start.unwrap_or(TimeOfDay::from_parts(0, 0));
        let mut when = utc_date_time(after, start.hour(), start.minute());
        loop {
            if self.days.includes(when.weekday()) && when > after {
                return when;
            }
            // The baseline panics here ("cannot find the next time ..."); a
            // full weekday set or a valid partial set always terminates, so
            // return the candidate instead of unwinding the worker.
            if when - after > WEEK {
                return when;
            }
            when += DAY;
        }
    }

    pub fn days_mask(&self) -> i8 {
        self.days.mask()
    }

    /// Minutes since midnight, or `None` when no start time is configured.
    pub fn start_minutes(&self) -> Option<u16> {
        self.start.map(|time| time.cmp_key())
    }

    pub fn end_minutes(&self) -> Option<u16> {
        self.end.map(|time| time.cmp_key())
    }

    pub fn rate(&self) -> Rate {
        self.rate
    }
}

/// `end.Sub(start)` as a non-negative `std::time::Duration`.
fn positive_duration(end: OffsetDateTime, start: OffsetDateTime) -> Duration {
    let nanos = end.unix_timestamp_nanos() - start.unix_timestamp_nanos();
    Duration::from_nanos(nanos.max(0) as u64)
}

/// A UTC `OffsetDateTime` on `at`'s calendar day at the given time of day.
fn utc_date_time(at: OffsetDateTime, hour: u8, minute: u8) -> OffsetDateTime {
    let at = at.to_offset(UtcOffset::UTC);
    let date = Date::from_calendar_date(at.year(), at.month(), at.day())
        .expect("OffsetDateTime has a valid calendar date");
    let time = Time::from_hms(hour, minute, 0).expect("validated time of day");
    PrimitiveDateTime::new(date, time).assume_utc()
}

/// `Schedule`: the rate limit in effect until `valid_until`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Schedule {
    duration: Duration,
    rate: Rate,
    until: OffsetDateTime,
}

impl Schedule {
    /// `newSchedule`.
    fn new(base: OffsetDateTime, duration: Duration, rate: Rate) -> Self {
        Self {
            duration,
            rate,
            until: base + duration,
        }
    }

    /// `(s).ValidUntil`.
    pub fn valid_until(&self) -> OffsetDateTime {
        self.until
    }

    pub fn duration(&self) -> Duration {
        self.duration
    }

    pub fn rate(&self) -> Rate {
        self.rate
    }

    /// `(s).total`: -1 when unlimited, 0 when the rate is zero, otherwise the
    /// truncated number of events the schedule can handle while valid.
    pub fn total(&self) -> i64 {
        if self.rate.is_unlimited() {
            return -1;
        }
        if self.rate.n() <= 0 {
            return 0;
        }

        let unit_seconds = self.rate.unit().as_duration().as_secs() as i64;
        // Exact integer arithmetic, like Go's `int(time.Hour / unit)`.
        let per_hour = self.rate.n() * (HOUR / unit_seconds);
        let in_an_hour = self.duration.as_secs_f64() / HOUR as f64;
        // Go truncates the float product with `int(...)`.
        (in_an_hour * per_hour as f64) as i64
    }
}

/// `Configuration`: the rollout windows configured on the site.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Configuration {
    windows: Vec<Window>,
}

impl Configuration {
    /// `NewConfiguration`. The baseline returns `(nil, err)` and its caller
    /// keeps the previous configuration; the port returns the error and leaves
    /// the decision to the caller.
    pub fn parse(raw: Option<&Value>) -> Result<Self, String> {
        let mut windows = Vec::new();
        let mut errs = Vec::new();

        match raw {
            None | Some(Value::Null) => {}
            Some(Value::Array(items)) => {
                for (index, item) in items.iter().enumerate() {
                    match Window::parse(item) {
                        Ok(window) => windows.push(window),
                        Err(err) => errs.push(format!("window {index}: {err}")),
                    }
                }
            }
            // The schema field is a slice; anything else is rejected by the
            // configuration decoder before reaching here.
            Some(_) => {}
        }

        if errs.is_empty() {
            Ok(Self { windows })
        } else {
            Err(join_errors(&errs))
        }
    }

    /// `(cfg).HasRolloutWindows`.
    pub fn has_rollout_windows(&self) -> bool {
        !self.windows.is_empty()
    }

    pub fn windows(&self) -> &[Window] {
        &self.windows
    }

    /// `(cfg).Schedule`: `now` is the baseline's `time.Now()`.
    pub fn schedule(&self, now: OffsetDateTime) -> Schedule {
        if !self.has_rollout_windows() {
            return Schedule::new(now, Duration::from_secs(10 * 60), Rate::unlimited());
        }
        self.schedule_at(now)
    }

    /// `(cfg).scheduleAt`.
    pub fn schedule_at(&self, at: OffsetDateTime) -> Schedule {
        let (window, validity) = self.window_for(at);

        let Some(window) = window else {
            let duration = validity.unwrap_or(Duration::from_secs(60));
            return Schedule::new(at, duration, Rate::zero());
        };

        let duration = validity.unwrap_or(Duration::from_secs(24 * 60 * 60));
        Schedule::new(at, duration, window.rate)
    }

    /// `(cfg).windowFor`: the last matching window and how long it applies.
    pub fn window_for(&self, now: OffsetDateTime) -> (Option<&Window>, Option<Duration>) {
        if self.windows.is_empty() {
            return (None, None);
        }

        let now = now.to_offset(UtcOffset::UTC);

        let mut index = None;
        for (i, window) in self.windows.iter().enumerate() {
            if window.is_open(now) {
                index = Some(i);
            }
        }

        let Some(index) = index else {
            let mut next = None;
            for window in &self.windows {
                let at = window.next_open_after(now);
                if next.is_none_or(|next| at < next) {
                    next = Some(at);
                }
            }
            let Some(next) = next else {
                // Unreachable: each window yields a candidate.
                return (None, None);
            };
            return (None, Some(positive_duration(next, now)));
        };

        let window = &self.windows[index];

        let mut end = if let Some(window_end) = window.end {
            Some(utc_date_time(now, window_end.hour(), window_end.minute()))
        } else if !window.days.all() {
            // Walk forward from midnight until a weekday the window excludes.
            // (The baseline's `start.Add(-1 * time.Second)` result is
            // discarded, so the end is exactly midnight.)
            let mut start = now.replace_time(Time::MIDNIGHT);
            loop {
                start += DAY;
                if !window.days.includes(start.weekday()) {
                    break Some(start);
                }
                if start - now > WEEK {
                    // The baseline panics here; callers always configure at
                    // least one weekday, so this is unreachable.
                    break None;
                }
            }
        } else {
            None
        };

        for next_window in &self.windows[index + 1..] {
            let next_active = next_window.next_open_after(now);
            match end {
                Some(end_time) if next_active >= end_time => {}
                _ => end = Some(next_active),
            }
        }

        match end {
            None => (Some(window), None),
            Some(end) => (Some(window), Some(positive_duration(end, now))),
        }
    }

    /// `(cfg).Estimate`. `None` means "no reasonable estimate".
    pub fn estimate(&self, now: OffsetDateTime, n: i64) -> Option<OffsetDateTime> {
        if !self.has_rollout_windows() {
            return Some(now);
        }

        let mut rem = n;
        let mut at = now.to_offset(UtcOffset::UTC);
        let until = at + WEEK;
        while at < until {
            let schedule = self.schedule_at(at);

            let total = schedule.total();
            if total == -1 {
                return Some(at);
            }
            if total == 0 {
                let next_at = schedule.valid_until();
                if next_at == at {
                    // Protective guard against the loop the baseline logs and
                    // then bails out of.
                    return None;
                }
                at = next_at;
                continue;
            }

            rem -= total;
            if rem < 0 {
                let percentage = 1.0 - (rem as f64).abs() / total as f64;
                let duration =
                    schedule.valid_until().unix_timestamp_nanos() - at.unix_timestamp_nanos();
                let scaled = (duration as f64 * percentage) as i64;
                at += time::Duration::nanoseconds(scaled);
                return Some(at);
            } else if rem == 0 {
                at = schedule.valid_until();
                return Some(at);
            }

            at = schedule.valid_until();
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::Month;

    fn utc(year: i32, month: Month, day: u8, hour: u8, minute: u8) -> OffsetDateTime {
        let date = Date::from_calendar_date(year, month, day).expect("date");
        let time = Time::from_hms(hour, minute, 0).expect("time");
        PrimitiveDateTime::new(date, time).assume_utc()
    }

    fn parse(json: &str) -> Configuration {
        let value: Value = serde_json::from_str(json).expect("json");
        Configuration::parse(Some(&value)).expect("configuration")
    }

    #[test]
    fn parses_windows_like_the_baseline() {
        let cfg = parse(
            r#"[
                {"days":["mon","tue","wed","thu","fri"],"start":"09:00","end":"17:30","rate":"100/h"},
                {"days":["sat","sun"],"rate":"unlimited"},
                {"start":"22:00","end":"23:00","rate":"10/m"}
            ]"#,
        );
        assert!(cfg.has_rollout_windows());
        assert_eq!(cfg.windows()[0].days_mask(), 62);
        assert_eq!(cfg.windows()[0].start_minutes(), Some(9 * 60));
        assert_eq!(cfg.windows()[0].end_minutes(), Some(17 * 60 + 30));
        assert_eq!(
            cfg.windows()[0].rate(),
            Rate {
                n: 100,
                unit: RateUnit::Hour
            }
        );
        assert!(cfg.windows()[1].rate().is_unlimited());
    }

    #[test]
    fn aggregates_parse_errors_like_errors_append() {
        let err = Configuration::parse(Some(
            &serde_json::from_str(r#"[{"days":["xyz"],"rate":"nope"}]"#).expect("json"),
        ))
        .expect_err("invalid configuration");
        assert_eq!(
            err,
            "window 0: 2 errors occurred:\n\t* unknown weekday: \"xyz\"\n\t* malformed rate: \"nope\""
        );
    }

    #[test]
    fn empty_configuration_is_unlimited_with_ten_minute_schedules() {
        let cfg = Configuration::default();
        let now = utc(2026, Month::November, 2, 8, 0);
        assert_eq!(cfg.schedule(now).total(), -1);
        assert_eq!(
            cfg.schedule(now).valid_until(),
            now + Duration::from_secs(600)
        );
        // `scheduleAt` on a configuration without windows is a zero schedule.
        assert_eq!(cfg.schedule_at(now).total(), 0);
    }

    #[test]
    fn schedule_total_truncates_like_go() {
        let at = utc(2026, Month::November, 2, 9, 0);
        let schedule = Schedule::new(
            at,
            Duration::from_secs(8 * 3600 + 1800),
            Rate {
                n: 100,
                unit: RateUnit::Hour,
            },
        );
        // 8.5h * 100/h = 850
        assert_eq!(schedule.total(), 850);
        let schedule = Schedule::new(
            at,
            Duration::from_secs(90),
            Rate {
                n: 10,
                unit: RateUnit::Minute,
            },
        );
        // 1.5min * 10/min = 15
        assert_eq!(schedule.total(), 15);
    }

    #[test]
    fn estimate_walks_windows() {
        let cfg = parse(
            r#"[
                {"days":["mon","tue","wed","thu","fri"],"start":"09:00","end":"17:30","rate":"100/h"},
                {"days":["sat","sun"],"rate":"unlimited"}
            ]"#,
        );
        let now = utc(2026, Month::November, 2, 8, 0);
        // Before the window opens the estimate is the first open instant.
        assert_eq!(
            cfg.estimate(now, 0),
            Some(utc(2026, Month::November, 2, 9, 0))
        );
        // Unlimited windows return the instant they open.
        assert_eq!(
            cfg.estimate(now, 10_000),
            Some(utc(2026, Month::November, 7, 0, 0)),
            "Saturday 00:00 is the unlimited weekend window"
        );
    }
}
