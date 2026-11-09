#![forbid(unsafe_code)]

//! `sg-insights-query`: the Code Insights query builder.
//!
//! Port of `internal/insights/query/querybuilder`: it turns user-entered
//! insight queries into Sourcegraph search queries by parsing them with
//! [`sg_search_query`] and reconstructing the plan (`withDefaults`,
//! `AggregationQuery`, `SingleRepoQuery`, `PointDiffQuery`, filters, …), plus
//! the search-type detection (`DetectSearchType`/`ParseQuery`), repository
//! scope validation and the regexp capture-group replacer.
//!
//! Not ported in this slice: `ComputeInsightCommandQuery` (needs the compute
//! query language and a gitserver client), and the base64/JSON decode of
//! `rat=` revision specifiers (only the presence of the revision is observed).
//! See `docs/modules/06_worker.md` §84.

pub mod builder;
pub mod parser;
pub mod regexp;
pub mod repo;

pub use builder::{
    add_author_filter, add_file_filter, add_filter, add_repo_filter, add_repo_metadata_filter,
    aggregation_query, build_filter_text, code_insights_query_defaults, for_repo_revision,
    for_repos, global_query, is_single_repo_query, make_query_with_repo_filters, multi_repo_query,
    point_diff_query, point_in_time_query, repository_scope_query, set_case_sensitivity,
    single_repo_query, single_repo_query_indexed, with_count, with_count_all, with_defaults,
    BasicQuery, ComputeInsightQuery, PointDiffQueryOpts, NO_REPO_METADATA_TEXT,
    QUERY_NOT_SUPPORTED,
};
pub use parser::{
    contains_field, detect_search_type, is_valid_scope_query, parameters_from_query_plan,
    parse_query, parse_scope_query, search_type_from_string, search_type_name,
};
pub use regexp::{
    find_groups, new_pattern_replacer, replace_capture_groups_with_string, Group, RegexpReplacer,
    MULTIPLE_PATTERN_ERR, UNSUPPORTED_PATTERN_TYPE_ERR,
};
pub use repo::{
    exactly_one_repo, parse_repository_revisions, parse_revision_specifier, repositories,
    ParsedRepoFilter, RevisionSpecifier,
};
