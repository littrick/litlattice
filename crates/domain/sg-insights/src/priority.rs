#![forbid(unsafe_code)]

//! `internal/insights/priority`: a basic priority scheme and a query cost
//! analyzer for insight query execution.
//!
//! `Priority` ranks data points (recent points are cheaper/higher priority);
//! `QueryAnalyzer` approximates the resource demand of a search query from
//! pattern type, `type:`/`index:`/`author:`/`file:`/`lang:`/`archived:`/
//! `fork:` parameters and repository count/size.
//!
//! Numeric behavior is kept bit-identical to the Go baseline (the
//! differential harness compares `f64` bits). `Priority` is `i64`, matching
//! Go's 64-bit `int`; `time.Time.Sub` saturation beyond ±292 years is not
//! reproduced (the harness uses realistic ranges).

use sg_insights_query::parameters_from_query_plan;
use sg_search_query::basic::plan_to_q;
use sg_search_query::fields::{FIELD_AUTHOR, FIELD_FILE, FIELD_LANG};
use sg_search_query::node::visit_parameter;
use sg_search_query::validate::YesNoOnly;
use sg_search_query::Plan;
use time::OffsetDateTime;

/// `priority.Priority`: lower values mean higher priority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Priority(pub i64);

impl Priority {
    pub const CRITICAL: Priority = Priority(1);
    pub const HIGH: Priority = Priority(10);
    pub const MEDIUM: Priority = Priority(100);
    pub const LOW: Priority = Priority(1000);

    /// `Priority.LowerBy`.
    pub fn lower_by(self, val: i64) -> Priority {
        Priority(self.0 - val)
    }

    /// `Priority.RaiseBy`.
    pub fn raise_by(self, val: i64) -> Priority {
        Priority(self.0 + val)
    }

    /// `Priority.Lower`.
    pub fn lower(self) -> Priority {
        self.lower_by(1)
    }

    /// `Priority.Raise`.
    pub fn raise(self) -> Priority {
        self.raise_by(1)
    }
}

/// `priority.FromTimeInterval`: `days + (High + 1)`, truncated toward zero
/// like Go's `int(float64)` conversion.
pub fn from_time_interval(from: OffsetDateTime, to: OffsetDateTime) -> Priority {
    let min_priority = Priority::HIGH.0 + 1;
    let nanoseconds = (to - from).whole_nanoseconds() as f64;
    // Go: `to.Sub(from).Hours() / 24`, with `Hours() = float64(d)/3.6e12`.
    let days = (nanoseconds / 3_600_000_000_000.0) / 24.0;
    Priority((days + min_priority as f64) as i64)
}

/// `priority.Cost`: an approximation of resource demand (Go `int`).
pub type Cost = i64;

pub const INDEXED: Cost = 500;
/// Order-of-magnitude approximation; the baseline documents this as a rough
/// value.
pub const UNINDEXED: Cost = 5000;

pub const LITERAL_COST: f64 = 400.0;
pub const REGEXP_COST: f64 = 500.0;
pub const STRUCTURAL_COST: f64 = 1000.0;

pub const DIFF_MULTIPLIER: f64 = 10.0;
pub const COMMIT_MULTIPLIER: f64 = 8.0;

pub const AUTHOR_MULTIPLIER: f64 = 0.7;

pub const UNINDEXED_MULTIPLIER: f64 = 100.0;
pub const YES_MULTIPLIER: f64 = 1.5;
pub const ONLY_MULTIPLIER: f64 = 0.5;

pub const FILE_MULTIPLIER: f64 = 0.7;
pub const LANG_MULTIPLIER: f64 = 0.8;

pub const MANY_REPOSITORIES_MULTIPLIER: f64 = 10.0;
pub const MEGAREPO_MULTIPLIER: f64 = 100.0;
pub const GIGAREPO_MULTIPLIER: f64 = 1000.0;

/// `priority.QueryObject`: the analyzed query and repository facts. `cost`
/// accumulates across handlers (the baseline keeps it unexported but mutates
/// it in place; it is public here for the differential harness).
#[derive(Debug, Clone)]
pub struct QueryObject {
    pub query: Plan,
    pub number_of_repositories: i64,
    /// Sizes of repositories in bytes, if known.
    pub repository_byte_sizes: Vec<i64>,
    pub cost: f64,
}

impl QueryObject {
    pub fn new(query: Plan) -> QueryObject {
        QueryObject {
            query,
            number_of_repositories: 0,
            repository_byte_sizes: Vec::new(),
            cost: 0.0,
        }
    }
}

/// `priority.CostHeuristic`.
pub type CostHeuristic = fn(&mut QueryObject);

/// `priority.QueryAnalyzer`.
#[derive(Debug, Clone)]
pub struct QueryAnalyzer {
    cost_handlers: Vec<CostHeuristic>,
}

impl QueryAnalyzer {
    /// `NewQueryAnalyzer`.
    pub fn new(cost_handlers: Vec<CostHeuristic>) -> QueryAnalyzer {
        QueryAnalyzer { cost_handlers }
    }

    /// `QueryAnalyzer.Cost`: run every handler, then clamp a negative result
    /// to zero (the accumulated field itself is left negative).
    pub fn cost(&self, object: &mut QueryObject) -> f64 {
        for handler in &self.cost_handlers {
            handler(object);
        }
        if object.cost < 0.0 {
            return 0.0;
        }
        object.cost
    }
}

/// `priority.DefaultQueryAnalyzer`.
pub fn default_query_analyzer() -> QueryAnalyzer {
    QueryAnalyzer::new(vec![query_cost, repositories_cost])
}

/// `priority.QueryCost`.
pub fn query_cost(object: &mut QueryObject) {
    for basic in &object.query {
        if basic.is_structural() {
            object.cost += STRUCTURAL_COST;
        } else if basic.is_regexp() {
            object.cost += REGEXP_COST;
        } else {
            object.cost += LITERAL_COST;
        }
    }

    let mut diff = false;
    let mut commit = false;
    visit_parameter(
        &plan_to_q(&object.query),
        &mut |field, value, _negated, _annotation| {
            if field == "type" {
                if value == "diff" {
                    diff = true;
                } else if value == "commit" {
                    commit = true;
                }
            }
        },
    );
    if diff {
        object.cost *= DIFF_MULTIPLIER;
    }
    if commit {
        object.cost *= COMMIT_MULTIPLIER;
    }

    let parameters = parameters_from_query_plan(&object.query);
    if parameters.index() == YesNoOnly::No {
        object.cost *= UNINDEXED_MULTIPLIER;
    }
    if parameters.exists(FIELD_AUTHOR) {
        object.cost *= AUTHOR_MULTIPLIER;
    }
    if parameters.exists(FIELD_FILE) {
        object.cost *= FILE_MULTIPLIER;
    }
    if parameters.exists(FIELD_LANG) {
        object.cost *= LANG_MULTIPLIER;
    }

    if let Some(archived) = parameters.archived() {
        if archived == YesNoOnly::Yes {
            object.cost *= YES_MULTIPLIER;
        } else if archived == YesNoOnly::Only {
            object.cost *= ONLY_MULTIPLIER;
        }
    }
    if let Some(fork) = parameters.fork() {
        if fork == YesNoOnly::Yes || fork == YesNoOnly::Only {
            if fork == YesNoOnly::Yes {
                object.cost *= YES_MULTIPLIER;
            } else if fork == YesNoOnly::Only {
                object.cost *= ONLY_MULTIPLIER;
            }
        }
    }
}

pub const MEGAREPO_SIZE_THRESHOLD: i64 = 5_368_709_120; // 5GB
pub const GIGAREPO_SIZE_THRESHOLD: i64 = MEGAREPO_SIZE_THRESHOLD * 10; // 50GB

/// `priority.RepositoriesCost`.
pub fn repositories_cost(object: &mut QueryObject) {
    if object.cost <= 0.0 {
        // If this handler runs on its own we still want it to affect cost.
        object.cost = 1.0;
    }

    if object.number_of_repositories > 100 {
        object.cost *= object.number_of_repositories as f64 / 100.0;
    }

    let mut megarepo = false;
    let mut gigarepo = false;
    for &byte_size in &object.repository_byte_sizes {
        if byte_size >= GIGAREPO_SIZE_THRESHOLD {
            gigarepo = true;
        }
        if byte_size >= MEGAREPO_SIZE_THRESHOLD {
            megarepo = true;
        }
    }
    if gigarepo {
        object.cost *= GIGAREPO_MULTIPLIER;
    } else if megarepo {
        object.cost *= MEGAREPO_MULTIPLIER;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sg_insights_query::parse_query;
    use time::format_description::well_known::Rfc3339;

    fn time(value: &str) -> OffsetDateTime {
        OffsetDateTime::parse(value, &Rfc3339).expect("valid RFC3339")
    }

    fn plan(query: &str) -> Plan {
        parse_query(query, "literal").expect("valid query")
    }

    fn cost(query: &str, repos: i64, sizes: &[i64]) -> f64 {
        let mut object = QueryObject::new(plan(query));
        object.number_of_repositories = repos;
        object.repository_byte_sizes = sizes.to_vec();
        default_query_analyzer().cost(&mut object)
    }

    #[test]
    fn from_time_interval_truncates_toward_zero() {
        assert_eq!(
            from_time_interval(time("2023-01-01T00:00:00Z"), time("2023-01-02T00:00:00Z")),
            Priority(12)
        );
        assert_eq!(
            from_time_interval(time("2023-01-01T00:00:00Z"), time("2023-01-11T12:00:00Z")),
            Priority(21)
        );
        assert_eq!(
            from_time_interval(time("2023-01-02T00:00:00Z"), time("2023-01-01T00:00:00Z")),
            Priority(10)
        );
    }

    #[test]
    fn priority_arithmetic() {
        assert_eq!(Priority::HIGH.lower(), Priority(9));
        assert_eq!(Priority::HIGH.raise(), Priority(11));
        assert_eq!(Priority::LOW.lower_by(500), Priority(500));
        assert_eq!(Priority::CRITICAL.raise_by(9), Priority::HIGH);
    }

    #[test]
    fn analyzer_matches_baseline_relationships() {
        // From `internal/insights/priority/analyzer_test.go`.
        assert!(cost("insights", 0, &[]) < cost("Type:diff insights", 0, &[]));
        assert!(
            cost("type:diff author:someone insights", 0, &[]) < cost("type:diff insights", 0, &[])
        );
        assert!(
            cost("patterntype:regexp [0-9]+ lang:go", 0, &[])
                < cost("patterntype:regexp [0-9]+", 0, &[])
        );
        assert!(
            cost("file:insights lang:go DashboardResolver", 0, &[])
                < cost("lang:go DashboardResolver", 0, &[])
        );
        assert_eq!(
            cost("patterntype:regexp [0-9]+ lang:go", 1, &[]),
            cost("patterntype:regexp [0-9]+ lang:go", 5, &[])
        );
        assert!(
            cost("patterntype:regexp [0-9]+ lang:go", 1, &[])
                < cost("patterntype:regexp [0-9]+ lang:go", 20000, &[])
        );
        assert!(
            cost("patterntype:regexp [0-9]+ lang:go", 20000, &[])
                < cost("patterntype:regexp [0-9]+ lang:go", 40000, &[])
        );
        assert!(
            cost(
                "patterntype:structural [a] archived:yes fork:yes index:no",
                3,
                &[100, 100, 100]
            ) < cost(
                "patterntype:structural [a] archived:yes fork:yes index:no",
                3,
                &[100, MEGAREPO_SIZE_THRESHOLD, GIGAREPO_SIZE_THRESHOLD]
            )
        );
    }

    #[test]
    fn repositories_cost_seeds_positive_cost() {
        let mut object = QueryObject::new(plan("insights"));
        repositories_cost(&mut object);
        assert_eq!(object.cost, 1.0);
    }
}
