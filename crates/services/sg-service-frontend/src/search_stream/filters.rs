//! `internal/search/streaming/{filters,search_filters}.go`: suggested search
//! filters computed from the streamed results.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use regex::Regex;
use time::OffsetDateTime;

use super::convert::{
    go_strconv_quote, most_likely_language, quote_meta, select_kind, timeframe_for, Match,
};
use super::progress::SearchEvent;

/// `streaming.Filter`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filter {
    pub value: String,
    pub label: String,
    pub count: i64,
    pub kind: String,
    important: bool,
}

impl Filter {
    /// `Filter.Less`: more important first, then higher count, then value.
    fn less(&self, other: &Filter) -> bool {
        if self.important != other.important {
            return self.important;
        }
        if self.count != other.count {
            return self.count > other.count;
        }
        self.value < other.value
    }
}

/// `SearchFilters`.
#[derive(Debug, Default)]
pub struct SearchFilters {
    filters: BTreeMap<String, Filter>,
    pub dirty: bool,
}

impl SearchFilters {
    pub fn new() -> Self {
        Self::default()
    }

    /// `filters.Add`.
    fn add(&mut self, value: String, label: &str, count: i64, kind: &str) {
        match self.filters.get_mut(&value) {
            None => {
                self.filters.insert(
                    value.clone(),
                    Filter {
                        value,
                        label: label.to_owned(),
                        count,
                        kind: kind.to_owned(),
                        important: false,
                    },
                );
            }
            Some(filter) => filter.count += count,
        }
    }

    /// `filters.MarkImportant`; the value has just been added.
    fn mark_important(&mut self, value: &str) {
        if let Some(filter) = self.filters.get_mut(value) {
            filter.important = true;
        }
    }

    fn add_repo_filter(&mut self, repo_name: &str, rev: &str, line_match_count: i64) {
        let mut value = format!("repo:^{}$", quote_meta(repo_name));
        if !rev.is_empty() {
            value.push('@');
            value.push_str(rev);
        }
        self.add(value, repo_name, line_match_count, "repo");
    }

    fn add_file_filter(&mut self, path: &str, line_match_count: i64) {
        for (label, regex, regex_filter) in common_file_filters() {
            if regex.is_match(path) {
                self.add(regex_filter.clone(), label, line_match_count, "file");
            }
        }
    }

    fn add_lang_filter(&mut self, raw_language: &str, line_match_count: i64) {
        if raw_language.is_empty() {
            return;
        }
        let mut language = raw_language.to_lowercase();
        if language.contains(' ') {
            language = go_strconv_quote(&language);
        }
        let value = format!("lang:{language}");
        self.add(value, raw_language, line_match_count, "lang");
    }

    fn add_symbol_filter(&mut self, symbols: &[super::convert::SymbolMatch]) {
        for symbol in symbols {
            let Some(select_kind) = select_kind(&symbol.symbol) else {
                continue;
            };
            let value = format!("select:symbol.{select_kind}");
            self.add(value, &title_case(&select_kind), 1, "symbol type");
        }
    }

    fn add_commit_author_filter(&mut self, author: &super::convert::Signature) {
        let value = format!("author:{}", quote_meta(&author.email));
        self.add(value, &author.name, 1, "author");
    }

    fn add_commit_date_filter(&mut self, date: OffsetDateTime) {
        let (timeframe, value, label) = timeframe_for(date, OffsetDateTime::now_utc());
        self.add(format!("{timeframe}:{value}"), label, 1, "commit date");
    }

    fn add_type_filter(&mut self, value: &str, label: &str, count: i64) {
        if count == 0 {
            return;
        }
        self.add(value.to_owned(), label, count, "type");
        self.mark_important(value);
    }

    /// `SearchFilters.Update`.
    pub fn update(&mut self, event: &SearchEvent) {
        if event.stats.excluded_forks > 0 {
            self.add(
                "fork:yes".to_owned(),
                "Include forked repos",
                event.stats.excluded_forks,
                "utility",
            );
            self.mark_important("fork:yes");
            self.dirty = true;
        }
        if event.stats.excluded_archived > 0 {
            self.add(
                "archived:yes".to_owned(),
                "Include archived repos",
                event.stats.excluded_archived,
                "utility",
            );
            self.mark_important("archived:yes");
            self.dirty = true;
        }

        for match_ in &event.results {
            match match_ {
                Match::File(file) => {
                    let rev = file.file.input_rev.clone().unwrap_or_default();
                    let lines = file.result_count();
                    self.add_repo_filter(&file.file.repo.name, &rev, lines);
                    self.add_lang_filter(&most_likely_language(&file.file), lines);
                    self.add_file_filter(&file.file.path, lines);
                    self.add_symbol_filter(&file.symbols);
                    self.add_type_filter(
                        "type:file",
                        "Code",
                        file.chunk_matches
                            .iter()
                            .map(|cm| cm.ranges.len() as i64)
                            .sum(),
                    );
                    self.add_type_filter("type:symbol", "Symbols", file.symbols.len() as i64);
                    if file.symbols.is_empty()
                        && file.chunk_matches.is_empty()
                        && file.path_matches.is_empty()
                    {
                        self.add_type_filter("type:path", "Paths", 1);
                    } else {
                        self.add_type_filter("type:path", "Paths", file.path_matches.len() as i64);
                    }
                    self.dirty = true;
                }
                Match::Repo(_) => {
                    self.add_type_filter("type:repo", "Repositories", 1);
                    self.dirty = true;
                }
                Match::Commit(commit) => {
                    // `rev` is deliberately empty: one filter per repo.
                    self.add_repo_filter(&commit.repo.name, "", commit.result_count());
                    self.add_commit_author_filter(&commit.commit.author);
                    self.add_commit_date_filter(commit.commit.committer.date);
                    if commit.diff_preview.is_some() {
                        self.add_type_filter("type:diff", "Diffs", commit.result_count());
                    } else {
                        self.add_type_filter("type:commit", "Commits", commit.result_count());
                    }
                    self.dirty = true;
                }
                Match::Owner(_) => {}
            }
        }
    }

    /// `SearchFilters.Compute` (`MaxRepos: 1000, MaxOther: 1000`).
    pub fn compute(&mut self) -> Vec<Filter> {
        self.dirty = false;
        let mut repos: Vec<Filter> = Vec::new();
        let mut other: Vec<Filter> = Vec::new();
        for filter in self.filters.values() {
            if filter.kind == "repo" {
                repos.push(filter.clone());
            } else {
                other.push(filter.clone());
            }
        }
        retain_most_important(&mut repos, 1000);
        retain_most_important(&mut other, 1000);
        let mut all = repos;
        all.extend(other);
        all.sort_by(|a, b| {
            if a.less(b) {
                std::cmp::Ordering::Less
            } else if b.less(a) {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        });
        all
    }
}

/// `filterHeap` with `max` slots keeps the `max` most important filters.
fn retain_most_important(filters: &mut Vec<Filter>, max: usize) {
    if max == 0 {
        filters.clear();
        return;
    }
    filters.sort_by(|a, b| {
        if a.less(b) {
            std::cmp::Ordering::Less
        } else if b.less(a) {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    });
    filters.truncate(max);
}

/// `commonFileFilters`.
fn common_file_filters() -> &'static [(String, Regex, String)] {
    static FILTERS: OnceLock<Vec<(String, Regex, String)>> = OnceLock::new();
    FILTERS.get_or_init(|| {
        [
            ("Exclude _test.*", r"_tests?\.\w+$", r"-file:_test\.\w+$"),
            ("Exclude .test.*", r"\.tests?\.\w+$", r"-file:\.test\.\w+$"),
            ("Exclude Ruby tests", r"_spec\.rb$", r"-file:_spec\.rb$"),
            ("Exclude vendor", r"(^|/)vendor/", r"-file:(^|/)vendor/"),
            (
                "Exclude third party",
                r"(^|/)third[_\-]?party/",
                r"-file:(^|/)third[_\-]?party/",
            ),
            (
                "Exclude node_modules",
                r"(^|/)node_modules/",
                r"-file:(^|/)node_modules/",
            ),
            (
                "Exclude minified JavaScript",
                r"\.min\.js$",
                r"-file:\.min\.js$",
            ),
            (
                "Exclude JavaScript maps",
                r"\.js\.map$",
                r"-file:\.js\.map$",
            ),
        ]
        .into_iter()
        .map(|(label, pattern, filter)| {
            (
                label.to_owned(),
                Regex::new(pattern).expect("static regex"),
                filter.to_owned(),
            )
        })
        .collect()
    })
}

/// `cases.Title(language.English, cases.Compact)`: title-case every word.
fn title_case(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut boundary = true;
    for ch in value.chars() {
        if boundary && ch.is_alphabetic() {
            out.extend(ch.to_uppercase());
            boundary = false;
        } else {
            out.push(ch);
            if !ch.is_alphabetic() {
                boundary = true;
            }
        }
    }
    out
}
