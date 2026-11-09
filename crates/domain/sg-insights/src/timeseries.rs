#![forbid(unsafe_code)]

//! `internal/insights/timeseries`: sample-interval stepping and sample-point
//! construction for Code Insights.
//!
//! A [`TimeInterval`] steps a timestamp by whole years/months/weeks/days/hours;
//! [`build_sample_times`] walks backwards from `now` and returns the points in
//! ascending order; [`make_recordings_from_times`] wraps them as
//! [`RecordingTime`]s.
//!
//! Stepping reproduces Go's `time.Time.AddDate` normalization, including its
//! month-end overflow rule: adding one month to 2021-01-31 yields 2021-03-03
//! (February has 28 days in 2021, so "February 31" normalizes forwards). The
//! civil-date arithmetic below is Hinnant's `days_from_civil` /
//! `civil_from_days`, which matches Go's absolute-day normalization exactly.
//! Year values are limited to `time`'s calendar range (years 1..=9999, or the
//! configured large-dates range); Go panics outside its much wider range, and
//! this port panics likewise on unrepresentable dates.

use crate::types::{IntervalUnit, RecordingTime};
use time::{Duration, OffsetDateTime, PrimitiveDateTime};

/// `timeseries.TimeInterval`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeInterval {
    pub unit: IntervalUnit,
    pub value: i64,
}

/// `timeseries.DefaultInterval`: one month.
pub fn default_interval() -> TimeInterval {
    TimeInterval {
        unit: IntervalUnit::Month,
        value: 1,
    }
}

impl TimeInterval {
    /// `TimeInterval.StepBackwards`.
    pub fn step_backwards(&self, start: OffsetDateTime) -> OffsetDateTime {
        self.step(start, -1)
    }

    /// `TimeInterval.StepForwards`.
    pub fn step_forwards(&self, start: OffsetDateTime) -> OffsetDateTime {
        self.step(start, 1)
    }

    /// `TimeInterval.IsValid`: a known unit and a non-negative value.
    pub fn is_valid(&self) -> bool {
        matches!(
            self.unit,
            IntervalUnit::Year
                | IntervalUnit::Month
                | IntervalUnit::Week
                | IntervalUnit::Day
                | IntervalUnit::Hour
        ) && self.value >= 0
    }

    fn step(&self, start: OffsetDateTime, direction: i64) -> OffsetDateTime {
        match &self.unit {
            IntervalUnit::Year => add_date(start, direction.wrapping_mul(self.value), 0, 0),
            IntervalUnit::Month => add_date(start, 0, direction.wrapping_mul(self.value), 0),
            IntervalUnit::Week => add_date(
                start,
                0,
                0,
                direction.wrapping_mul(7).wrapping_mul(self.value),
            ),
            IntervalUnit::Day => add_date(start, 0, 0, direction.wrapping_mul(self.value)),
            IntervalUnit::Hour => {
                // Go: `time.Hour * time.Duration(t.Value) * time.Duration(direction)`
                // with wrapping int64 arithmetic.
                let nanoseconds = 3_600_000_000_000_i64
                    .wrapping_mul(self.value)
                    .wrapping_mul(direction);
                start + Duration::nanoseconds(nanoseconds)
            }
            // This doesn't really make sense, so return something (baseline
            // falls back to years).
            IntervalUnit::Other(_) => add_date(start, direction.wrapping_mul(self.value), 0, 0),
        }
    }
}

/// `timeseries.BuildSampleTimes`: `num_points` samples ending at `now`,
/// stepping backwards, sorted ascending.
pub fn build_sample_times(
    num_points: i64,
    interval: &TimeInterval,
    now: OffsetDateTime,
) -> Vec<OffsetDateTime> {
    // Go's `make([]time.Time, 0, numPoints)` panics for a negative capacity.
    assert!(num_points >= 0, "makeslice: cap out of range");
    let mut times = Vec::with_capacity(num_points as usize);
    times.push(now);

    let mut current = now;
    for _ in 0..num_points.saturating_sub(1) {
        current = interval.step_backwards(current);
        times.push(current);
    }

    times.sort();
    times
}

/// `timeseries.MakeRecordingsFromTimes`.
pub fn make_recordings_from_times(times: &[OffsetDateTime], snapshot: bool) -> Vec<RecordingTime> {
    times
        .iter()
        .map(|&timestamp| RecordingTime {
            timestamp,
            snapshot,
        })
        .collect()
}

/// Go's `time.Time.AddDate` for a wall-clock date: normalize the year/month,
/// then carry `days` as an absolute-day offset. The time of day and the UTC
/// offset are preserved.
fn add_date(start: OffsetDateTime, years: i64, months: i64, days: i64) -> OffsetDateTime {
    let year = start.year() as i64;
    let month0 = (start.month() as i64 - 1).wrapping_add(months);
    let year = year.wrapping_add(years).wrapping_add(month0.div_euclid(12));
    let month = month0.rem_euclid(12) + 1;

    let absolute_day = days_from_civil(year, month as u32, 1)
        .wrapping_add(start.day() as i64 - 1)
        .wrapping_add(days);
    let (year, month, day) = civil_from_days(absolute_day);

    let date = time::Date::from_calendar_date(
        year as i32,
        time::Month::try_from(month as u8).expect("normalized month"),
        day as u8,
    )
    .unwrap_or_else(|err| panic!("time: Date: {err}"));
    PrimitiveDateTime::new(date, start.time()).assume_offset(start.offset())
}

/// Days since 1970-01-01 for a proleptic Gregorian date (Hinnant).
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400; // [0, 399]
    let month_prime = (month as i64 + 9) % 12; // Mar = 0 .. Feb = 11
    let day_of_year = (153 * month_prime + 2) / 5 + day as i64 - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Inverse of [`days_from_civil`] (Hinnant).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let days = days + 719_468;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let day_of_era = days - era * 146_097; // [0, 146096]
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year + i64::from(month <= 2);
    (year, month as u32, day as u32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    fn interval(unit: IntervalUnit, value: i64) -> TimeInterval {
        TimeInterval { unit, value }
    }

    #[test]
    fn step_forward_matches_baseline() {
        // From `internal/insights/timeseries/interval_test.go`.
        let start = datetime!(2021-12-01 00:00:00 UTC);
        assert_eq!(
            interval(IntervalUnit::Month, 1).step_forwards(start),
            datetime!(2022-01-01 00:00:00 UTC)
        );
        assert_eq!(
            interval(IntervalUnit::Month, 13).step_forwards(start),
            datetime!(2023-01-01 00:00:00 UTC)
        );
        assert_eq!(
            interval(IntervalUnit::Day, 1).step_forwards(start),
            datetime!(2021-12-02 00:00:00 UTC)
        );
        assert_eq!(
            interval(IntervalUnit::Hour, 1).step_forwards(start),
            datetime!(2021-12-01 01:00:00 UTC)
        );
        assert_eq!(
            interval(IntervalUnit::Year, 1).step_forwards(start),
            datetime!(2022-12-01 00:00:00 UTC)
        );
    }

    #[test]
    fn is_valid_matches_baseline() {
        assert!(interval(IntervalUnit::Month, 1).is_valid());
        assert!(interval(IntervalUnit::Day, 1).is_valid());
        assert!(interval(IntervalUnit::Year, 1).is_valid());
        assert!(interval(IntervalUnit::Hour, 1).is_valid());
        assert!(interval(IntervalUnit::Week, 1).is_valid());
        assert!(!interval(IntervalUnit::Other("asdf".to_owned()), 1).is_valid());
        assert!(!interval(IntervalUnit::Week, -1).is_valid());
        assert!(interval(IntervalUnit::Week, 0).is_valid());
    }

    #[test]
    fn add_date_normalizes_month_end_like_go() {
        // January 31 + 1 month: "February 31" normalizes to March 3 (2021
        // is not a leap year).
        assert_eq!(
            interval(IntervalUnit::Month, 1).step_forwards(datetime!(2021-01-31 00:00:00 UTC)),
            datetime!(2021-03-03 00:00:00 UTC)
        );
        // February 29 + 1 year normalizes to March 1.
        assert_eq!(
            interval(IntervalUnit::Year, 1).step_forwards(datetime!(2020-02-29 00:00:00 UTC)),
            datetime!(2021-03-01 00:00:00 UTC)
        );
        // Stepping backwards over a month boundary.
        assert_eq!(
            interval(IntervalUnit::Month, 1).step_backwards(datetime!(2021-03-31 00:00:00 UTC)),
            datetime!(2021-03-03 00:00:00 UTC)
        );
    }

    #[test]
    fn build_sample_times_matches_baseline() {
        // From `internal/insights/timeseries/timeseries_test.go`.
        let start = datetime!(2021-12-01 00:00:00 UTC);
        let times = build_sample_times(1, &interval(IntervalUnit::Month, 1), start);
        assert_eq!(times, vec![start]);

        let times = build_sample_times(2, &interval(IntervalUnit::Month, 1), start);
        assert_eq!(
            times,
            vec![
                datetime!(2021-11-01 00:00:00 UTC),
                datetime!(2021-12-01 00:00:00 UTC)
            ]
        );

        let times = build_sample_times(6, &interval(IntervalUnit::Hour, 2), start);
        assert_eq!(
            times,
            vec![
                datetime!(2021-11-30 14:00:00 UTC),
                datetime!(2021-11-30 16:00:00 UTC),
                datetime!(2021-11-30 18:00:00 UTC),
                datetime!(2021-11-30 20:00:00 UTC),
                datetime!(2021-11-30 22:00:00 UTC),
                datetime!(2021-12-01 00:00:00 UTC),
            ]
        );

        let times = build_sample_times(6, &interval(IntervalUnit::Year, 1), start);
        assert_eq!(times.len(), 6);
        assert_eq!(times[0], datetime!(2016-12-01 00:00:00 UTC));
        assert_eq!(times[5], start);
    }

    #[test]
    fn make_recordings_matches_baseline() {
        let times = vec![
            datetime!(2021-11-01 00:00:00 UTC),
            datetime!(2021-12-01 00:00:00 UTC),
        ];
        let recordings = make_recordings_from_times(&times, true);
        assert_eq!(recordings.len(), 2);
        for (recording, timestamp) in recordings.iter().zip(times) {
            assert_eq!(recording.timestamp, timestamp);
            assert!(recording.snapshot);
        }
    }

    #[test]
    fn default_interval_is_one_month() {
        assert_eq!(default_interval(), interval(IntervalUnit::Month, 1));
        assert_eq!(default_interval().unit.as_str(), "MONTH");
    }
}
