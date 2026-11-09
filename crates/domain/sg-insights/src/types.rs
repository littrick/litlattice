#![forbid(unsafe_code)]

//! The subset of `internal/insights/types` needed by the ported domain logic.
//!
//! The Go `types` package also contains the storage-facing series/view
//! structs and enums; those land here only when the corresponding logic is
//! ported (and must map onto the M6 storage contract unchanged).

/// `types.IntervalUnit`: a string enum. Go allows arbitrary strings (the
/// database column is `text`), so unknown values are preserved in
/// [`IntervalUnit::Other`] and treated as invalid by
/// [`crate::timeseries::TimeInterval::is_valid`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IntervalUnit {
    Month,
    Day,
    Week,
    Year,
    Hour,
    /// Any value not recognized by the baseline. Preserved verbatim so that
    /// stepping behaves like the Go `default` branch.
    Other(String),
}

impl IntervalUnit {
    /// The baseline's string encoding (`types.Month` et al.).
    pub fn as_str(&self) -> &str {
        match self {
            IntervalUnit::Month => "MONTH",
            IntervalUnit::Day => "DAY",
            IntervalUnit::Week => "WEEK",
            IntervalUnit::Year => "YEAR",
            IntervalUnit::Hour => "HOUR",
            IntervalUnit::Other(value) => value,
        }
    }

    /// Maps a stored `sample_interval_unit` string to an enum value.
    pub fn from_str_value(value: &str) -> IntervalUnit {
        match value {
            "MONTH" => IntervalUnit::Month,
            "DAY" => IntervalUnit::Day,
            "WEEK" => IntervalUnit::Week,
            "YEAR" => IntervalUnit::Year,
            "HOUR" => IntervalUnit::Hour,
            other => IntervalUnit::Other(other.to_owned()),
        }
    }
}

/// `types.RecordingTime`: one sample point scheduled for a series.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordingTime {
    pub timestamp: time::OffsetDateTime,
    pub snapshot: bool,
}
