//! M9.8bt differential: the insights query builder over the corpus, mirroring
//! `internal/insights/query/querybuilder/parity_harness_test.go`.
//! Gated by `SG_INSIGHTS_QUERY_CORPUS`; each corpus line is
//! `<op>\t<arg>...` and produces one output line with a stable prefix.

use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_insights_query::builder::*;
use sg_insights_query::parser::*;
use sg_insights_query::regexp::*;
use sg_insights_query::PointDiffQueryOpts;
use sg_search_query::{plan_to_q, string_human, Parameters, SearchType};

fn harness_parameters(spec: &str) -> Parameters {
    if spec.is_empty() || spec == "-" {
        return Parameters::default();
    }
    Parameters::new(
        spec.split(',')
            .map(|part| {
                let (field, value) = part
                    .split_once(':')
                    .unwrap_or_else(|| panic!("bad default {part:?}"));
                sg_search_query::Parameter {
                    field: field.to_owned(),
                    value: value.to_owned(),
                    negated: false,
                    annotation: Default::default(),
                }
            })
            .collect(),
    )
}

fn harness_strings(spec: &str) -> Vec<String> {
    if spec.is_empty() || spec == "-" {
        return Vec::new();
    }
    spec.split('|').map(str::to_owned).collect()
}

fn harness_search_type(name: &str) -> SearchType {
    match name {
        "regex" => SearchType::Regex,
        "literal" => SearchType::Literal,
        "structural" => SearchType::Structural,
        "standard" => SearchType::Standard,
        "codycontext" => SearchType::CodyContext,
        "keyword" => SearchType::Keyword,
        other => panic!("unknown search type {other}"),
    }
}

fn harness_time(value: &str) -> Option<OffsetDateTime> {
    if value.is_empty() || value == "-" {
        return None;
    }
    Some(OffsetDateTime::parse(value, &Rfc3339).expect("valid RFC3339"))
}

fn harness_string_ptr(value: &str) -> Option<String> {
    if value.is_empty() || value == "-" {
        None
    } else {
        Some(value.to_owned())
    }
}

fn harness_params_string(parameters: &Parameters) -> String {
    parameters
        .0
        .iter()
        .map(|p| format!("{}:{}", p.field, p.value))
        .collect::<Vec<_>>()
        .join(";")
}

fn print_result(prefix: &str, args: &[&str], result: Result<String, String>) {
    match result {
        Ok(got) => println!("{prefix}\t{}\t{got}", args.join("\t")),
        Err(err) => println!("{prefix}\t{}\terror: {err}", args.join("\t")),
    }
}

#[test]
fn insights_query_builder_parity() {
    let Ok(path) = std::env::var("SG_INSIGHTS_QUERY_CORPUS") else {
        return;
    };
    let corpus = std::fs::read_to_string(path).expect("read corpus");
    for line in corpus.lines() {
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split('\t').collect();
        let op = parts[0];
        match op {
            "defaults" => print_result(
                "d",
                &parts[1..3],
                with_defaults(parts[1], &harness_parameters(parts[2])),
            ),
            "aggregation" => {
                let timeout: i32 = parts[2].parse().expect("timeout");
                print_result(
                    "ag",
                    &parts[1..4],
                    aggregation_query(parts[1], timeout, parts[3]),
                )
            }
            "count" => print_result("ct", &parts[1..3], with_count(parts[1], parts[2])),
            "single" => print_result(
                "sr",
                &parts[1..5],
                single_repo_query(parts[1], parts[2], parts[3], &harness_parameters(parts[4])),
            ),
            "singleindexed" => println!(
                "si\t{}\t{}\t{}",
                parts[1],
                parts[2],
                single_repo_query_indexed(parts[1], parts[2])
            ),
            "global" => print_result(
                "gl",
                &parts[1..3],
                global_query(parts[1], &harness_parameters(parts[2])),
            ),
            "multi" => print_result(
                "mr",
                &parts[1..4],
                multi_repo_query(
                    parts[1],
                    &harness_strings(parts[2]),
                    &harness_parameters(parts[3]),
                ),
            ),
            "pointdiff" => {
                let opts = PointDiffQueryOpts {
                    before: harness_time(parts[2]),
                    after: harness_time(parts[3]),
                    filter_repo_includes: harness_strings(parts[4]),
                    filter_repo_excludes: harness_strings(parts[5]),
                    repo_list: harness_strings(parts[6]),
                    repo_search: harness_string_ptr(parts[7]),
                    search_query: parts[1].to_owned(),
                };
                print_result("pd", &parts[1..8], point_diff_query(opts))
            }
            "pointintime" => {
                let opts = PointDiffQueryOpts {
                    before: harness_time(parts[2]),
                    after: None,
                    filter_repo_includes: harness_strings(parts[3]),
                    filter_repo_excludes: harness_strings(parts[4]),
                    repo_list: harness_strings(parts[5]),
                    repo_search: harness_string_ptr(parts[6]),
                    search_query: parts[1].to_owned(),
                };
                print_result("pi", &parts[1..7], point_in_time_query(opts))
            }
            "author" => print_result("au", &parts[1..3], add_author_filter(parts[1], parts[2])),
            "repofilter" => print_result("rf", &parts[1..3], add_repo_filter(parts[1], parts[2])),
            "filefilter" => print_result("ff", &parts[1..3], add_file_filter(parts[1], parts[2])),
            "metafilter" => print_result(
                "mf",
                &parts[1..3],
                add_repo_metadata_filter(parts[1], parts[2]),
            ),
            "addfilter" => print_result(
                "af",
                &parts[1..5],
                add_filter(parts[1], parts[2], parts[3], parts[4] == "true"),
            ),
            "case" => print_result(
                "cs",
                &parts[1..3],
                set_case_sensitivity(parts[1], parts[2] == "true"),
            ),
            "scope" => print_result("sc", &parts[1..2], repository_scope_query(parts[1])),
            "mkrepo" => print_result(
                "mk",
                &parts[1..5],
                make_query_with_repo_filters(
                    parts[1],
                    parts[2],
                    parts[3] == "true",
                    &harness_parameters(parts[4]).0,
                ),
            ),
            "issinglerepo" => match is_single_repo_query(parts[1]) {
                Ok(got) => println!("is\t{}\t{got}", parts[1]),
                Err(err) => println!("is\t{}\terror: {err}", parts[1]),
            },
            "parsequery" => match parse_query(parts[2], parts[1]) {
                Ok(plan) => {
                    let q = plan_to_q(&plan);
                    println!(
                        "pq\t{}\t{}\t{}\t{}\t{}",
                        parts[1],
                        parts[2],
                        plan.len(),
                        sg_search_query::query_to_string(&q),
                        string_human(&q)
                    );
                }
                Err(err) => println!("pq\t{}\t{}\terror: {err}", parts[1], parts[2]),
            },
            "detect" => match detect_search_type(parts[2], parts[1]) {
                Ok(search_type) => {
                    println!(
                        "dt\t{}\t{}\t{}",
                        parts[1],
                        parts[2],
                        search_type_name(search_type)
                    )
                }
                Err(err) => println!("dt\t{}\t{}\terror: {err}", parts[1], parts[2]),
            },
            "containsf" => match contains_field(parts[1], parts[2]) {
                Ok(got) => println!("cf\t{}\t{}\t{got}", parts[1], parts[2]),
                Err(err) => println!("cf\t{}\t{}\terror: {err}", parts[1], parts[2]),
            },
            "scopequery" => match parse_query(parts[1], "literal") {
                Ok(plan) => {
                    let (reason, valid) = is_valid_scope_query(&plan);
                    println!("sq\t{}\t{valid}\t{reason}", parts[1]);
                }
                Err(err) => println!("sq\t{}\terror: {err}", parts[1]),
            },
            "params" => match parse_query(parts[1], "literal") {
                Ok(plan) => {
                    let parameters = parameters_from_query_plan(&plan);
                    println!(
                        "pa\t{}\t{}\t{}",
                        parts[1],
                        parameters.0.len(),
                        harness_params_string(&parameters)
                    );
                }
                Err(err) => println!("pa\t{}\terror: {err}", parts[1]),
            },
            "replacer" => match new_pattern_replacer(parts[2], harness_search_type(parts[1])) {
                Ok(replacer) => {
                    let got = replacer.replace(parts[3]);
                    println!(
                        "rp\t{}\t{}\t{}\t{}\t{}",
                        parts[1],
                        parts[2],
                        parts[3],
                        replacer.has_capture_groups(),
                        got
                    );
                }
                Err(err) => println!("rp\t{}\t{}\t{}\terror: {err}", parts[1], parts[2], parts[3]),
            },
            "cid" => {
                let defaults = code_insights_query_defaults(parts[1] == "true");
                println!("ci\t{}\t{}", parts[1], harness_params_string(&defaults));
            }
            "buildfilter" => println!("bf\t{}\t{}", parts[1], build_filter_text(parts[1])),
            "countall" => println!("ca\t{}\t{}", parts[1], with_count_all(parts[1])),
            "forrev" => println!(
                "fr\t{}\t{}\t{}\t{}",
                parts[1],
                parts[2],
                parts[3],
                for_repo_revision(parts[1], parts[2], parts[3])
            ),
            "forrepos" => println!(
                "fs\t{}\t{}\t{}",
                parts[1],
                parts[2],
                for_repos(parts[1], &harness_strings(parts[2]))
            ),
            other => panic!("unknown op {other:?}"),
        }
    }
}
