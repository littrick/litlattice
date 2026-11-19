//! Settings-file insight model and parsing (`setting_parse.go`, `types_json.go`,
//! `interval.go`, parts of `insights.go`).
//!
//! Settings are JSONC. Go parses them with `internal/jsonc.Unmarshal`, then
//! unmarshals individual values into Go structs, whose field matching is
//! case-insensitive and which reject type mismatches. The helpers below
//! reproduce those semantics on the tolerant `sg_jsonx` value tree.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};
use sg_jsonx::{parse, JsonValue, ParseOptions};
use time::{Date, Duration as TimeDuration, Month, OffsetDateTime, UtcOffset};

const PARSE_OPTIONS: ParseOptions = ParseOptions {
    comments: true,
    trailing_commas: true,
};

/// `settings` (`types_db.go`): one settings row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    pub id: i32,
    pub org: Option<i32>,
    pub user: Option<i32>,
    pub contents: String,
}

/// `settingDashboard` (`types_json.go`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SettingDashboard {
    pub id: String,
    pub title: String,
    pub insight_ids: Vec<String>,
    pub user_id: Option<i32>,
    pub org_id: Option<i32>,
}

/// `langStatsInsight` (`types_json.go`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LangStatsInsight {
    pub id: String,
    pub title: String,
    pub repository: String,
    pub other_threshold: f64,
    pub org_id: Option<i32>,
    pub user_id: Option<i32>,
}

/// `searchInsight` (`types_json.go`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SearchInsight {
    pub id: String,
    pub title: String,
    pub description: String,
    /// `None` models Go's nil slice (absent or JSON `null`), which the
    /// frontend migration soft-fails on.
    pub repositories: Option<Vec<String>>,
    pub series: Vec<TimeSeries>,
    pub step: Interval,
    pub org_id: Option<i32>,
    pub user_id: Option<i32>,
    pub filters: Option<DefaultFilters>,
}

/// `timeSeries` (`types_json.go`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TimeSeries {
    pub name: String,
    pub stroke: String,
    pub query: String,
}

/// `interval` (`types_json.go`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Interval {
    pub years: Option<i32>,
    pub months: Option<i32>,
    pub weeks: Option<i32>,
    pub days: Option<i32>,
    pub hours: Option<i32>,
}

/// `defaultFilters` (`types_json.go`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DefaultFilters {
    pub include_repo_regexp: Option<String>,
    pub exclude_repo_regexp: Option<String>,
}

/// Everything `getInsightsFromSettings` extracts from the most specific
/// settings row.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ParsedInsights {
    pub dashboards: Vec<SettingDashboard>,
    pub lang_stats: Vec<LangStatsInsight>,
    pub frontend: Vec<SearchInsight>,
    pub backend: Vec<SearchInsight>,
}

/// `getInsightsFromSettings`.
pub fn parse_insights(settings: &Settings) -> ParsedInsights {
    ParsedInsights {
        dashboards: parse_dashboards(settings),
        lang_stats: parse_lang_stats(settings),
        frontend: parse_search_insights(settings, "searchInsights."),
        backend: parse_backend_insights(settings),
    }
}

fn parse_dashboards(settings: &Settings) -> Vec<SettingDashboard> {
    let mut out = Vec::new();
    visit_filtered_json_map(settings, "insights.dashboards", |_, value| {
        let Some(map) = json_map(&value) else {
            return;
        };
        for (id, body) in map {
            let Ok(mut dashboard) = parse_dashboard(&body) else {
                continue;
            };
            // The settings key wins over any `id` embedded in the body.
            dashboard.id = id.clone();
            dashboard.user_id = settings.user;
            dashboard.org_id = settings.org;
            out.push(dashboard);
        }
    });
    out
}

fn parse_lang_stats(settings: &Settings) -> Vec<LangStatsInsight> {
    let mut out = Vec::new();
    visit_filtered_json_map(settings, "codeStatsInsights.", |id, body| {
        let unique_id = make_unique_id(id, settings);
        let Ok(mut insight) = parse_lang_stats_insight(&unique_id, &body) else {
            return;
        };
        insight.user_id = settings.user;
        insight.org_id = settings.org;
        out.push(insight);
    });
    out
}

fn parse_search_insights(settings: &Settings, prefix: &str) -> Vec<SearchInsight> {
    let mut out = Vec::new();
    visit_filtered_json_map(settings, prefix, |id, body| {
        let Ok(mut insight) = parse_search_insight(&body, make_unique_id(id, settings)) else {
            return;
        };
        insight.user_id = settings.user;
        insight.org_id = settings.org;
        out.push(insight);
    });
    out
}

/// `getBackendInsights`: `insights.allrepos` maps insight ID -> body.
fn parse_backend_insights(settings: &Settings) -> Vec<SearchInsight> {
    let mut out = Vec::new();
    visit_filtered_json_map(settings, "insights.allrepos", |_, value| {
        let Some(map) = json_map(&value) else {
            return;
        };
        for (id, body) in map {
            let Ok(mut insight) = parse_search_insight(&body, make_unique_id(&id, settings)) else {
                continue;
            };
            insight.user_id = settings.user;
            insight.org_id = settings.org;
            out.push(insight);
        }
    });
    out
}

/// `visitFilteredJSONMap`: parse the settings once, then call `f` for every
/// top-level key with the given prefix. Parse or schema errors yield nothing.
fn visit_filtered_json_map(settings: &Settings, prefix: &str, mut f: impl FnMut(&str, Value)) {
    let Some(root) = parse_jsonc_root(&settings.contents) else {
        return;
    };
    let Some(object) = root.as_object() else {
        return;
    };
    for (key, value) in object {
        if key.starts_with(prefix) {
            f(key, value.clone());
        }
    }
}

/// `visitJSONMap`: `null` is an empty map; any non-object is a schema error.
fn json_map(value: &Value) -> Option<BTreeMap<String, Value>> {
    match value {
        Value::Null => Some(BTreeMap::new()),
        Value::Object(object) => Some(object.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
        _ => None,
    }
}

/// Parse settings JSONC into plain JSON. `None` models Go's "parse error or
/// non-object root" branches, both of which yield no insights.
fn parse_jsonc_root(contents: &str) -> Option<Value> {
    let (value, errors) = parse(contents, PARSE_OPTIONS);
    if !errors.is_empty() {
        return None;
    }
    match value? {
        JsonValue::Object(object) => {
            let mut out = Map::new();
            for (key, value) in object {
                out.insert(key, convert(value)?);
            }
            Some(Value::Object(out))
        }
        // A non-object root fails to unmarshal into Go's `map[string]RawMessage`.
        _ => None,
    }
}

fn convert(value: JsonValue) -> Option<Value> {
    Some(match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(value) => Value::Bool(value),
        JsonValue::String(value) => Value::String(value),
        JsonValue::Number(lexeme) => number(&lexeme)?,
        JsonValue::Array(items) => {
            Value::Array(items.into_iter().map(convert).collect::<Option<_>>()?)
        }
        JsonValue::Object(object) => {
            let mut out = Map::new();
            for (key, value) in object {
                out.insert(key, convert(value)?);
            }
            Value::Object(out)
        }
    })
}

fn number(lexeme: &str) -> Option<Value> {
    if let Ok(value) = lexeme.parse::<i64>() {
        return Some(Value::Number(value.into()));
    }
    if let Ok(value) = lexeme.parse::<u64>() {
        return Some(Value::Number(value.into()));
    }
    serde_json::Number::from_f64(lexeme.parse().ok()?).map(Value::Number)
}

fn parse_dashboard(body: &Value) -> Result<SettingDashboard, String> {
    match body {
        Value::Null => return Ok(SettingDashboard::default()),
        Value::Object(object) => Ok(SettingDashboard {
            id: string_field(object, "id")?,
            title: string_field(object, "title")?,
            insight_ids: string_array_field(object, "insightIds")?.unwrap_or_default(),
            user_id: int_field(object, "userId")?,
            org_id: int_field(object, "orgId")?,
        }),
        _ => Err("dashboard is not an object".to_owned()),
    }
}

fn parse_lang_stats_insight(id: &str, body: &Value) -> Result<LangStatsInsight, String> {
    match body {
        Value::Null => Ok(LangStatsInsight {
            id: id.to_owned(),
            ..LangStatsInsight::default()
        }),
        Value::Object(object) => Ok(LangStatsInsight {
            id: initial_id(object, id.to_owned())?,
            title: string_field(object, "Title")?,
            repository: string_field(object, "Repository")?,
            other_threshold: float_field(object, "OtherThreshold")?,
            org_id: int_field(object, "OrgID")?,
            user_id: int_field(object, "UserID")?,
        }),
        _ => Err("lang stats insight is not an object".to_owned()),
    }
}

fn parse_search_insight(body: &Value, unique_id: String) -> Result<SearchInsight, String> {
    let body = match body {
        Value::Null => {
            return Ok(SearchInsight {
                id: unique_id,
                ..Default::default()
            })
        }
        Value::Object(object) => object,
        _ => return Err("search insight is not an object".to_owned()),
    };

    let mut series = Vec::new();
    match ci_get(body, "Series") {
        None | Some(Value::Null) => {}
        Some(Value::Array(items)) => {
            for item in items {
                series.push(parse_time_series(item)?);
            }
        }
        Some(_) => return Err("Series is not an array".to_owned()),
    }

    let step = match ci_get(body, "Step") {
        None | Some(Value::Null) => Interval::default(),
        Some(Value::Object(object)) => parse_interval(object)?,
        Some(_) => return Err("Step is not an object".to_owned()),
    };

    let filters = match ci_get(body, "Filters") {
        None | Some(Value::Null) => None,
        Some(Value::Object(object)) => Some(DefaultFilters {
            include_repo_regexp: optional_string_field(object, "IncludeRepoRegexp")?,
            exclude_repo_regexp: optional_string_field(object, "ExcludeRepoRegexp")?,
        }),
        Some(_) => return Err("Filters is not an object".to_owned()),
    };

    Ok(SearchInsight {
        id: initial_id(body, unique_id)?,
        title: string_field(body, "Title")?,
        description: string_field(body, "Description")?,
        repositories: string_array_field(body, "Repositories")?,
        series,
        step,
        org_id: int_field(body, "OrgID")?,
        user_id: int_field(body, "UserID")?,
        filters,
    })
}

fn parse_time_series(value: &Value) -> Result<TimeSeries, String> {
    match value {
        Value::Null => Ok(TimeSeries::default()),
        Value::Object(object) => Ok(TimeSeries {
            name: string_field(object, "Name")?,
            stroke: string_field(object, "Stroke")?,
            query: string_field(object, "Query")?,
        }),
        _ => Err("series entry is not an object".to_owned()),
    }
}

fn parse_interval(object: &Map<String, Value>) -> Result<Interval, String> {
    Ok(Interval {
        years: int_field(object, "Years")?,
        months: int_field(object, "Months")?,
        weeks: int_field(object, "Weeks")?,
        days: int_field(object, "Days")?,
        hours: int_field(object, "Hours")?,
    })
}

/// Go pre-seeds the ID and only overwrites it when the JSON has a string
/// `ID`; `null` leaves the seeded value untouched.
fn initial_id(object: &Map<String, Value>, seed: String) -> Result<String, String> {
    match ci_get(object, "ID") {
        None | Some(Value::Null) => Ok(seed),
        Some(Value::String(value)) => Ok(value.clone()),
        Some(_) => Err("ID is not a string".to_owned()),
    }
}

fn ci_get<'a>(object: &'a Map<String, Value>, name: &str) -> Option<&'a Value> {
    if let Some(value) = object.get(name) {
        return Some(value);
    }
    object
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value)
}

fn string_field(object: &Map<String, Value>, name: &str) -> Result<String, String> {
    match ci_get(object, name) {
        None | Some(Value::Null) => Ok(String::new()),
        Some(Value::String(value)) => Ok(value.clone()),
        Some(_) => Err(format!("{name} is not a string")),
    }
}

fn optional_string_field(
    object: &Map<String, Value>,
    name: &str,
) -> Result<Option<String>, String> {
    match ci_get(object, name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(format!("{name} is not a string")),
    }
}

fn float_field(object: &Map<String, Value>, name: &str) -> Result<f64, String> {
    match ci_get(object, name) {
        None | Some(Value::Null) => Ok(0.0),
        Some(Value::Number(value)) => value
            .as_f64()
            .ok_or_else(|| format!("{name} is not a number")),
        Some(_) => Err(format!("{name} is not a number")),
    }
}

fn int_field(object: &Map<String, Value>, name: &str) -> Result<Option<i32>, String> {
    match ci_get(object, name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(value)) => {
            let value = value
                .as_i64()
                .ok_or_else(|| format!("{name} is not an integer"))?;
            i32::try_from(value)
                .map(Some)
                .map_err(|_| format!("{name} is out of range"))
        }
        Some(_) => Err(format!("{name} is not an integer")),
    }
}

fn string_array_field(
    object: &Map<String, Value>,
    name: &str,
) -> Result<Option<Vec<String>>, String> {
    match ci_get(object, name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(match item {
                    Value::Null => String::new(),
                    Value::String(value) => value.clone(),
                    _ => return Err(format!("{name} contains a non-string")),
                });
            }
            Ok(Some(out))
        }
        Some(_) => Err(format!("{name} is not an array")),
    }
}

/// `makeUniqueID`.
pub fn make_unique_id(id: &str, settings: &Settings) -> String {
    if let Some(user) = settings.user {
        return format!("{id}-user-{user}");
    }
    if let Some(org) = settings.org {
        return format!("{id}-org-{org}");
    }
    id.to_owned()
}

/// `parseTimeIntervalUnit`.
pub fn parse_time_interval_unit(insight: &SearchInsight) -> &'static str {
    if insight.step.days.is_some() {
        "DAY"
    } else if insight.step.hours.is_some() {
        "HOUR"
    } else if insight.step.weeks.is_some() {
        "WEEK"
    } else if insight.step.months.is_some() {
        "MONTH"
    } else if insight.step.years.is_some() {
        "YEAR"
    } else {
        ""
    }
}

/// `parseTimeIntervalValue`.
pub fn parse_time_interval_value(insight: &SearchInsight) -> i32 {
    insight
        .step
        .days
        .or(insight.step.hours)
        .or(insight.step.weeks)
        .or(insight.step.months)
        .or(insight.step.years)
        .unwrap_or(1)
}

/// `specialCaseDashboardName`.
pub fn special_case_dashboard_name(subject_name: &str) -> String {
    if subject_name == "Global" {
        "Global Insights".to_owned()
    } else {
        format!("{subject_name}'s Insights")
    }
}

/// `extractIDsFromInsights`: `(sorted unique IDs, sorted duplicate IDs)`.
pub fn extract_ids_from_insights(parsed: &ParsedInsights) -> (Vec<String>, Vec<String>) {
    let mut seen = BTreeSet::new();
    let mut duplicates = BTreeSet::new();
    let mut add = |id: &str| {
        if !seen.insert(id.to_owned()) {
            duplicates.insert(id.to_owned());
        }
    };

    for insight in &parsed.lang_stats {
        add(&insight.id);
    }
    for insight in &parsed.frontend {
        add(&insight.id);
    }
    for insight in &parsed.backend {
        add(&insight.id);
    }

    (seen.into_iter().collect(), duplicates.into_iter().collect())
}

/// `nextSnapshot`: midnight UTC tomorrow.
pub fn next_snapshot(current: OffsetDateTime) -> OffsetDateTime {
    let next_day = current.date() + TimeDuration::days(1);
    next_day.midnight().assume_utc()
}

/// `nextRecording`: midnight UTC on the first day of next month.
pub fn next_recording(current: OffsetDateTime) -> OffsetDateTime {
    let date = current.date();
    let total_months = i64::from(date.year()) * 12 + i64::from(u8::from(date.month()));
    let year = total_months.div_euclid(12) as i32;
    let month = Month::try_from((total_months.rem_euclid(12) + 1) as u8).expect("month");
    Date::from_calendar_date(year, month, 1)
        .expect("valid date")
        .midnight()
        .assume_utc()
}

/// `stepForward`.
pub fn step_forward(now: OffsetDateTime, unit: &str, value: i32) -> OffsetDateTime {
    match unit {
        "YEAR" => add_months_go(now, i64::from(value) * 12),
        "MONTH" => add_months_go(now, i64::from(value)),
        "WEEK" => now + TimeDuration::days(7 * i64::from(value)),
        "DAY" => now + TimeDuration::days(i64::from(value)),
        "HOUR" => now + TimeDuration::hours(i64::from(value)),
        _ => now,
    }
}

/// `time.AddDate` month arithmetic: day-of-month overflow normalizes into the
/// following month (e.g. Jan 31 + 1 month = Mar 3), unlike the clamping
/// `add_months` used by the recording-times migration.
fn add_months_go(value: OffsetDateTime, months: i64) -> OffsetDateTime {
    let date = value.date();
    let total_months = i64::from(date.year()) * 12 + i64::from(u8::from(date.month())) - 1 + months;
    let year = total_months.div_euclid(12) as i32;
    let month = Month::try_from((total_months.rem_euclid(12) + 1) as u8).expect("month");
    let first = Date::from_calendar_date(year, month, 1).expect("valid first of month");
    let day_offset = i64::from(date.day()) - 1;
    let target = first + TimeDuration::days(day_offset);
    target
        .with_time(value.time())
        .assume_utc()
        .to_offset(UtcOffset::UTC)
}

/// Convert an `OffsetDateTime` to UTC (used before timezone-sensitive math).
pub fn to_utc(value: OffsetDateTime) -> OffsetDateTime {
    value.to_offset(UtcOffset::UTC)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use time::Time;

    fn settings(contents: &str) -> Settings {
        Settings {
            id: 1,
            org: None,
            user: None,
            contents: contents.to_owned(),
        }
    }

    #[test]
    fn parses_comments_and_trailing_commas() {
        let parsed = parse_insights(&settings(
            r#"{
                // a comment
                "searchInsights.insight.a": {
                    "title": "T",
                    "repositories": ["r"],
                    "series": [{"name": "n", "stroke": "s", "query": "q"}],
                    "step": {"months": 2},
                },
            }"#,
        ));
        assert_eq!(parsed.frontend.len(), 1);
        let insight = &parsed.frontend[0];
        assert_eq!(insight.id, "searchInsights.insight.a");
        assert_eq!(insight.repositories, Some(vec!["r".to_owned()]));
        assert_eq!(parse_time_interval_unit(insight), "MONTH");
        assert_eq!(parse_time_interval_value(insight), 2);
    }

    #[test]
    fn applies_subject_suffixes_and_ownership() {
        let mut settings = settings(
            r#"{"codeStatsInsights.a": {"title":"x","repository":"r","otherThreshold":0.75}}"#,
        );
        settings.org = Some(7);
        let parsed = parse_insights(&settings);
        assert_eq!(parsed.lang_stats[0].id, "codeStatsInsights.a-org-7");
        assert_eq!(parsed.lang_stats[0].org_id, Some(7));
        assert!((parsed.lang_stats[0].other_threshold - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn dashboards_use_the_map_key_as_id() {
        let parsed = parse_insights(&settings(
            r#"{"insights.dashboards": {"key": {"id":"body","title":"T","insightIds":["a","b"]}}}"#,
        ));
        assert_eq!(parsed.dashboards.len(), 1);
        assert_eq!(parsed.dashboards[0].id, "key");
        assert_eq!(parsed.dashboards[0].insight_ids, vec!["a", "b"]);
    }

    #[test]
    fn backend_insights_come_from_insights_allrepos() {
        let parsed = parse_insights(&settings(
            r#"{"insights.allrepos": {"id1": {"title": "T", "series": []}}}"#,
        ));
        assert_eq!(parsed.backend.len(), 1);
        assert_eq!(parsed.backend[0].id, "id1");
    }

    #[test]
    fn type_mismatches_drop_the_record() {
        let parsed = parse_insights(&settings(
            r#"{"searchInsights.bad": {"repositories": "not-an-array"}}"#,
        ));
        assert!(parsed.frontend.is_empty());
    }

    #[test]
    fn extracts_unique_and_duplicate_ids() {
        let parsed = ParsedInsights {
            lang_stats: vec![LangStatsInsight {
                id: "b".into(),
                ..Default::default()
            }],
            frontend: vec![SearchInsight {
                id: "a".into(),
                ..Default::default()
            }],
            backend: vec![SearchInsight {
                id: "a".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(
            extract_ids_from_insights(&parsed),
            (vec!["a".to_owned(), "b".to_owned()], vec!["a".to_owned()])
        );
    }

    #[test]
    fn schedules_snapshots_and_recordings() {
        let now = OffsetDateTime::new_utc(
            Date::from_calendar_date(2023, Month::January, 31).unwrap(),
            Time::from_hms(12, 0, 0).unwrap(),
        );
        assert_eq!(
            next_snapshot(now),
            OffsetDateTime::new_utc(
                Date::from_calendar_date(2023, Month::February, 1).unwrap(),
                Time::MIDNIGHT
            )
        );
        assert_eq!(
            next_recording(now),
            OffsetDateTime::new_utc(
                Date::from_calendar_date(2023, Month::February, 1).unwrap(),
                Time::MIDNIGHT
            )
        );
        // Go normalizes day overflow: Jan 31 + 1 month = Mar 3.
        assert_eq!(
            step_forward(now, "MONTH", 1),
            OffsetDateTime::new_utc(
                Date::from_calendar_date(2023, Month::March, 3).unwrap(),
                Time::from_hms(12, 0, 0).unwrap()
            )
        );
    }

    #[test]
    fn builds_special_dashboard_names() {
        assert_eq!(special_case_dashboard_name("Global"), "Global Insights");
        assert_eq!(special_case_dashboard_name("alice"), "alice's Insights");
    }

    #[test]
    fn null_root_or_array_root_yields_nothing() {
        assert_eq!(parse_insights(&settings("null")), ParsedInsights::default());
        assert_eq!(parse_insights(&settings("[]")), ParsedInsights::default());
        assert_eq!(
            parse_insights(&settings("{ broken")),
            ParsedInsights::default()
        );
    }

    #[test]
    fn missing_repositories_is_none_but_empty_array_is_some() {
        let parsed = parse_insights(&settings(
            r#"{"searchInsights.a": {}, "searchInsights.b": {"repositories": []}}"#,
        ));
        assert_eq!(parsed.frontend[0].repositories, None);
        assert_eq!(parsed.frontend[1].repositories, Some(Vec::new()));
    }

    #[test]
    fn parsing_a_dashboard_null_body_is_tolerated() {
        let parsed = parse_insights(&settings(r#"{"insights.dashboards": {"key": null}}"#));
        assert_eq!(parsed.dashboards.len(), 1);
        assert_eq!(parsed.dashboards[0].id, "key");
        assert!(parsed.dashboards[0].title.is_empty());
    }

    #[test]
    fn serde_json_helpers_accept_case_insensitive_keys() {
        let value = json!({"TITLE": "x"});
        assert_eq!(
            string_field(value.as_object().unwrap(), "title").unwrap(),
            "x"
        );
    }
}
