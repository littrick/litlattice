#![forbid(unsafe_code)]

//! Port of `internal/search/backend/index_options.go`: build the options
//! `sourcegraph-zoekt-indexserver` needs to index a repository.
//!
//! All database/gitserver lookups are injected as closures; this module never
//! talks to a database, gitserver or HTTP endpoint. The site configuration is
//! the small subset of `schema.SiteConfiguration` read by the baseline,
//! deserialized from the same JSON keys.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use regex::Regex;
use serde::{ser::SerializeStruct, Deserialize, Serialize};

use crate::types::RepositoryBranch;

/// `GetIndexOptions` limits concurrency to 32 to avoid too many active network
/// requests and strain on gitserver.
pub const INDEX_OPTIONS_CONCURRENCY: usize = 32;

/// `ctags_config.UnknownCtags`.
pub const UNKNOWN_CTAGS: u8 = 0;
/// `ctags_config.NoCtags`.
pub const NO_CTAGS: u8 = 1;
/// `ctags_config.UniversalCtags`.
pub const UNIVERSAL_CTAGS: u8 = 2;
/// `ctags_config.ScipCtags`.
pub const SCIP_CTAGS: u8 = 3;

/// `ctags_config.ParserType` (`uint8`).
pub type ParserType = u8;

/// `ZoektIndexOptions` (`index_options.go:20-64`).
///
/// The `Serialize` implementation reproduces the baseline JSON tags exactly:
/// `Branches`, `Priority`, `DocumentRanksVersion`, `Error` and
/// `ShardConcurrency` are omitted when empty (`omitempty`); `LargeFiles` and
/// `LanguageMap` have no `omitempty`, so they serialize as `null` when unset.
#[derive(Debug, Clone, Default, Serialize)]
pub struct ZoektIndexOptions {
    /// Repository name.
    #[serde(rename = "Name")]
    pub name: String,

    /// Sourcegraph repository ID (`api.RepoID`, an `int32`).
    #[serde(rename = "RepoID")]
    pub repo_id: i32,

    /// Public repository (no auth filtering).
    #[serde(rename = "Public")]
    pub public: bool,

    /// Fork.
    #[serde(rename = "Fork")]
    pub fork: bool,

    /// Archived.
    #[serde(rename = "Archived")]
    pub archived: bool,

    /// Glob patterns whose matching files are indexed regardless of size.
    #[serde(rename = "LargeFiles")]
    pub large_files: Option<Vec<String>>,

    /// Index ctags output.
    #[serde(rename = "Symbols")]
    pub symbols: bool,

    /// Branches to index (`json:",omitempty"`).
    #[serde(rename = "Branches", skip_serializing_if = "Vec::is_empty")]
    pub branches: Vec<RepositoryBranch>,

    /// Ranking priority, higher first (`json:",omitempty"`).
    #[serde(rename = "Priority", skip_serializing_if = "is_zero_f64")]
    pub priority: f64,

    /// Offline ranking version (`json:",omitempty"`).
    #[serde(
        rename = "DocumentRanksVersion",
        skip_serializing_if = "String::is_empty"
    )]
    pub document_ranks_version: String,

    /// Error for this repository (`json:",omitempty"`).
    #[serde(rename = "Error", skip_serializing_if = "String::is_empty")]
    pub error: String,

    /// `ctags_config.CreateEngineMap` result. `None` serializes as `null`.
    #[serde(rename = "LanguageMap")]
    pub language_map: Option<BTreeMap<String, ParserType>>,

    /// `int32(siteConfig.SearchIndexShardConcurrency)` (`json:",omitempty"`).
    #[serde(rename = "ShardConcurrency", skip_serializing_if = "is_zero_i32")]
    pub shard_concurrency: i32,
}

fn is_zero_f64(value: &f64) -> bool {
    *value == 0.0
}

fn is_zero_i32(value: &i32) -> bool {
    *value == 0
}

// `zoekt.RepositoryBranch` has no JSON tags, so its fields encode with their
// Go names. Implementing `Serialize` here keeps the shared `types.rs` free of
// slice-specific serde derives.
impl Serialize for RepositoryBranch {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("RepositoryBranch", 2)?;
        state.serialize_field("Name", &self.name)?;
        state.serialize_field("Version", &self.version)?;
        state.end()
    }
}

/// `RepoIndexOptions` (`index_options.go:151-163`): the per-repository inputs
/// before site configuration is applied.
pub type GetVersionFn = Box<dyn Fn(&str) -> Result<String, String> + Send + Sync>;

/// The options used by [`get_index_options`] for a specific repository.
pub struct RepoIndexOptions {
    /// Repository name.
    pub name: String,
    /// Sourcegraph repository ID.
    pub repo_id: i32,
    /// Public repository (no auth filtering).
    pub public: bool,
    /// Ranking priority, higher first.
    pub priority: f64,
    /// Offline ranking version.
    pub document_ranks_version: String,
    /// Fork.
    pub fork: bool,
    /// Archived.
    pub archived: bool,
    /// Resolve a branch to a commit. A missing revision returns an empty
    /// string rather than an error.
    pub get_version: GetVersionFn,
}

impl Default for RepoIndexOptions {
    fn default() -> Self {
        Self {
            name: String::new(),
            repo_id: 0,
            public: false,
            priority: 0.0,
            document_ranks_version: String::new(),
            fork: false,
            archived: false,
            get_version: Box::new(|_| Ok(String::new())),
        }
    }
}

/// `revsRuleFunc`: a rule that returns the revisions to index for a repo.
pub type RevisionsRuleFunc = Box<dyn Fn(&RepoIndexOptions) -> Vec<String> + Send + Sync>;

/// The subset of `schema.SiteConfiguration` read by this module.
///
/// Dotted JSON keys (`search.index.branches`, `search.largeFiles`, ...) are
/// literal root keys in the baseline schema, so they are renamed here.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct SiteConfiguration {
    #[serde(rename = "search.largeFiles")]
    pub search_large_files: Option<Vec<String>>,
    #[serde(rename = "search.index.symbols.enabled")]
    pub search_index_symbols_enabled: Option<bool>,
    #[serde(rename = "search.index.shardConcurrency")]
    pub search_index_shard_concurrency: i64,
    #[serde(rename = "experimentalFeatures")]
    pub experimental_features: Option<ExperimentalFeatures>,
    #[serde(rename = "syntaxHighlighting")]
    pub syntax_highlighting: Option<SyntaxHighlighting>,
}

/// `schema.ExperimentalFeatures` (only the indexing fields).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct ExperimentalFeatures {
    #[serde(rename = "search.index.branches")]
    pub search_index_branches: Option<BTreeMap<String, Vec<String>>>,
    #[serde(rename = "search.index.revisions")]
    pub search_index_revisions: Option<Vec<SearchIndexRevisionsRule>>,
}

/// `schema.SearchIndexRevisionsRule`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct SearchIndexRevisionsRule {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "revisions")]
    pub revisions: Vec<String>,
}

/// `schema.SyntaxHighlighting` (only the symbol engine overrides).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct SyntaxHighlighting {
    #[serde(rename = "symbols")]
    pub symbols: Option<SymbolConfiguration>,
}

/// `schema.SymbolConfiguration`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct SymbolConfiguration {
    #[serde(rename = "engine")]
    pub engine: BTreeMap<String, String>,
}

/// `ctags_config.DefaultEngines`: languages enabled by default, all using
/// `scip-ctags`.
pub fn default_engines() -> BTreeMap<String, ParserType> {
    const LANGUAGES: &[&str] = &[
        "c_sharp",
        "go",
        "javascript",
        "hack",
        "kotlin",
        "magik",
        "python",
        "ruby",
        "rust",
        "scala",
        "typescript",
        "zig",
    ];
    LANGUAGES
        .iter()
        .map(|language| ((*language).to_owned(), SCIP_CTAGS))
        .collect()
}

/// `languages.NormalizeLanguage`.
pub fn normalize_language(language: &str) -> String {
    let normalized = language.to_lowercase();
    if normalized == "c#" {
        "c_sharp".to_owned()
    } else {
        normalized
    }
}

/// `ctags_config.ParserNameToParserType`.
pub fn parser_name_to_parser_type(name: &str) -> Result<ParserType, String> {
    match name {
        "off" => Ok(NO_CTAGS),
        "universal-ctags" => Ok(UNIVERSAL_CTAGS),
        "scip-ctags" => Ok(SCIP_CTAGS),
        other => Err(format!("unknown parser type: {other}")),
    }
}

/// `ctags_config.CreateEngineMap`.
///
/// The baseline only writes an override when parsing the engine name fails
/// (`if engine, err := ...; err != nil`), so a valid `syntaxHighlighting`
/// override is ignored and an invalid one sets `UnknownCtags`. That quirk is
/// reproduced faithfully.
pub fn create_engine_map(config: &SiteConfiguration) -> BTreeMap<String, ParserType> {
    let mut engines = default_engines();
    if let Some(symbols) = config
        .syntax_highlighting
        .as_ref()
        .and_then(|highlighting| highlighting.symbols.as_ref())
    {
        for (language, engine) in &symbols.engine {
            let language = normalize_language(language);
            if parser_name_to_parser_type(engine).is_err() {
                engines.insert(language, UNKNOWN_CTAGS);
            }
        }
    }
    engines
}

/// `getBoolPtr`.
pub fn get_bool_ptr(value: Option<bool>, default_value: bool) -> bool {
    value.unwrap_or(default_value)
}

/// `GetIndexOptions` (`index_options.go:165-193`).
///
/// `get_repo_index_options` is the baseline's `getRepoIndexOptions`, and
/// `get_search_context_revisions` is `getSearchContextRevisions`. Output order
/// matches `repos`.
pub fn get_index_options(
    config: &SiteConfiguration,
    get_repo_index_options: impl Fn(i32) -> Result<RepoIndexOptions, String> + Sync,
    get_search_context_revisions: impl Fn(i32) -> Result<Vec<String>, String> + Sync,
    repos: &[i32],
) -> Vec<ZoektIndexOptions> {
    let get_site_config_revisions = site_config_revisions_rule_func(config);
    let results: Mutex<Vec<Option<ZoektIndexOptions>>> =
        Mutex::new((0..repos.len()).map(|_| None).collect());
    let next = AtomicUsize::new(0);
    let workers = repos.len().min(INDEX_OPTIONS_CONCURRENCY);

    // Scoped threads are a bounded worker pool: each grabs the next index from
    // a shared cursor and writes its result slot, like the baseline's
    // semaphore of 32 goroutines.
    std::thread::scope(|scope| {
        for _ in 0..workers {
            scope.spawn(|| loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= repos.len() {
                    break;
                }
                let options = get_index_options_for_repo(
                    config,
                    repos[index],
                    &get_repo_index_options,
                    &get_search_context_revisions,
                    get_site_config_revisions.as_ref(),
                );
                results
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())[index] = Some(options);
            });
        }
    });

    results
        .into_inner()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .into_iter()
        .map(|options| options.expect("every repository index is filled"))
        .collect()
}

/// `getIndexOptions` (`index_options.go:195-301`): options for one repository.
pub fn get_index_options_for_repo(
    config: &SiteConfiguration,
    repo_id: i32,
    get_repo_index_options: &impl Fn(i32) -> Result<RepoIndexOptions, String>,
    get_search_context_revisions: &impl Fn(i32) -> Result<Vec<String>, String>,
    get_site_config_revisions: Option<&RevisionsRuleFunc>,
) -> ZoektIndexOptions {
    let options = match get_repo_index_options(repo_id) {
        Ok(options) => options,
        Err(error) => {
            return ZoektIndexOptions {
                repo_id,
                error: error.to_string(),
                ..ZoektIndexOptions::default()
            }
        }
    };

    let mut index_options = ZoektIndexOptions {
        name: options.name.clone(),
        repo_id: options.repo_id,
        public: options.public,
        priority: options.priority,
        fork: options.fork,
        archived: options.archived,
        large_files: config.search_large_files.clone(),
        symbols: get_bool_ptr(config.search_index_symbols_enabled, true),
        document_ranks_version: options.document_ranks_version.clone(),
        language_map: Some(create_engine_map(config)),
        shard_concurrency: config.search_index_shard_concurrency as i32,
        ..ZoektIndexOptions::default()
    };

    // Always index HEAD; other branches come from site config and search
    // contexts.
    let mut branches: BTreeSet<String> = BTreeSet::new();
    branches.insert("HEAD".to_owned());
    if let Some(get_revisions) = get_site_config_revisions {
        branches.extend(get_revisions(&options));
    }

    match get_search_context_revisions(options.repo_id) {
        Ok(revisions) => branches.extend(revisions),
        Err(error) => {
            return ZoektIndexOptions {
                repo_id: options.repo_id,
                error: error.to_string(),
                ..ZoektIndexOptions::default()
            }
        }
    }

    // Empty string means HEAD, which is already in the set.
    branches.remove("");

    let mut resolved = Vec::with_capacity(branches.len());
    for branch in branches {
        let version = match (options.get_version)(&branch) {
            Ok(version) => version,
            Err(error) => {
                return ZoektIndexOptions {
                    repo_id: options.repo_id,
                    error,
                    ..ZoektIndexOptions::default()
                }
            }
        };
        // If the branch did not resolve, skip it.
        if version.is_empty() {
            continue;
        }
        resolved.push(RepositoryBranch {
            name: branch,
            version,
        });
    }

    // Zoekt treats the first branch as the default branch, so put HEAD first
    // by sorting it as the empty string.
    resolved.sort_by(|a, b| branch_sort_key(a).cmp(branch_sort_key(b)));

    // If the first branch is not HEAD, index nothing. HEAD should always
    // exist if other branches exist.
    if resolved.first().map(|branch| branch.name.as_str()) != Some("HEAD") {
        resolved.clear();
    }

    // Zoekt has a limit of 64 branches.
    resolved.truncate(64);

    index_options.branches = resolved;
    index_options
}

fn branch_sort_key(branch: &RepositoryBranch) -> &str {
    if branch.name == "HEAD" {
        ""
    } else {
        &branch.name
    }
}

/// `siteConfigRevisionsRuleFunc` (`index_options.go:303-341`).
///
/// Returns `None` when the site config or its experimental features are
/// absent. Invalid regular expressions are skipped; the baseline also logs
/// them, which this port cannot do without a logger dependency.
pub fn site_config_revisions_rule_func(config: &SiteConfiguration) -> Option<RevisionsRuleFunc> {
    let features = config.experimental_features.as_ref()?;

    let mut rules: Vec<(Regex, Vec<String>)> = Vec::new();
    if let Some(revisions) = &features.search_index_revisions {
        for rule in revisions {
            if rule.name.is_empty() {
                continue;
            }
            let Ok(pattern) = Regex::new(&rule.name) else {
                continue;
            };
            rules.push((pattern, rule.revisions.clone()));
        }
    }

    let branches = features.search_index_branches.clone().unwrap_or_default();
    Some(Box::new(move |options: &RepoIndexOptions| {
        let mut matched = Vec::new();
        if !branches.is_empty() {
            if let Some(revisions) = branches.get(&options.name) {
                matched.extend(revisions.iter().cloned());
            }
        }
        for (pattern, revisions) in &rules {
            if pattern.is_match(&options.name) {
                matched.extend(revisions.iter().cloned());
            }
        }
        matched
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bang_version(branch: &str) -> Result<String, String> {
        Ok(format!("!{branch}"))
    }

    fn repo_options(repo_id: i32, name: &str) -> RepoIndexOptions {
        RepoIndexOptions {
            name: name.to_owned(),
            repo_id,
            get_version: Box::new(bang_version),
            ..RepoIndexOptions::default()
        }
    }

    #[test]
    fn default_options_match_the_baseline() {
        let options = get_index_options(
            &SiteConfiguration::default(),
            |repo| Ok(repo_options(repo, &format!("repo-{repo:02}"))),
            |_| Ok(Vec::new()),
            &[1],
        );
        assert_eq!(options.len(), 1);
        let options = &options[0];
        assert_eq!(options.name, "repo-01");
        assert_eq!(options.repo_id, 1);
        assert!(options.symbols);
        assert_eq!(
            options.branches,
            vec![RepositoryBranch {
                name: "HEAD".to_owned(),
                version: "!HEAD".to_owned(),
            }]
        );
        assert_eq!(options.language_map.as_ref(), Some(&default_engines()));
        assert_eq!(options.large_files, None);
        assert_eq!(options.shard_concurrency, 0);
    }

    #[test]
    fn symbols_are_true_by_default_and_can_be_disabled() {
        let config = SiteConfiguration {
            search_index_symbols_enabled: Some(false),
            ..SiteConfiguration::default()
        };
        let options = get_index_options(
            &config,
            |repo| Ok(repo_options(repo, "repo")),
            |_| Ok(Vec::new()),
            &[1],
        );
        assert!(!options[0].symbols);
    }

    #[test]
    fn site_config_branches_and_revisions_are_merged() {
        let config = SiteConfiguration {
            experimental_features: Some(ExperimentalFeatures {
                search_index_branches: Some(BTreeMap::from([(
                    "repo".to_owned(),
                    vec!["a".to_owned(), "b".to_owned()],
                )])),
                search_index_revisions: Some(vec![SearchIndexRevisionsRule {
                    name: ".*".to_owned(),
                    revisions: vec!["a".to_owned(), "c".to_owned()],
                }]),
            }),
            ..SiteConfiguration::default()
        };
        let options = get_index_options(
            &config,
            |repo| Ok(repo_options(repo, "repo")),
            |_| Ok(Vec::new()),
            &[1],
        );
        let names: Vec<&str> = options[0]
            .branches
            .iter()
            .map(|branch| branch.name.as_str())
            .collect();
        assert_eq!(names, ["HEAD", "a", "b", "c"]);
    }

    #[test]
    fn invalid_regex_rules_are_skipped() {
        let config = SiteConfiguration {
            experimental_features: Some(ExperimentalFeatures {
                search_index_revisions: Some(vec![
                    SearchIndexRevisionsRule {
                        name: "[".to_owned(),
                        revisions: vec!["bad".to_owned()],
                    },
                    SearchIndexRevisionsRule {
                        name: "repo".to_owned(),
                        revisions: vec!["good".to_owned()],
                    },
                    SearchIndexRevisionsRule {
                        name: String::new(),
                        revisions: vec!["ignored".to_owned()],
                    },
                ]),
                ..ExperimentalFeatures::default()
            }),
            ..SiteConfiguration::default()
        };
        let options = get_index_options(
            &config,
            |repo| Ok(repo_options(repo, "repo")),
            |_| Ok(Vec::new()),
            &[1],
        );
        let names: Vec<&str> = options[0]
            .branches
            .iter()
            .map(|branch| branch.name.as_str())
            .collect();
        assert_eq!(names, ["HEAD", "good"]);
    }

    #[test]
    fn priority_and_document_ranks_are_passed_through() {
        let options = get_index_options(
            &SiteConfiguration::default(),
            |repo| {
                Ok(RepoIndexOptions {
                    repo_id: repo,
                    priority: 10.0,
                    document_ranks_version: "ranked".to_owned(),
                    ..repo_options(repo, "repo")
                })
            },
            |_| Ok(Vec::new()),
            &[1],
        );
        assert_eq!(options[0].priority, 10.0);
        assert_eq!(options[0].document_ranks_version, "ranked");
    }

    #[test]
    fn context_revisions_include_the_empty_string() {
        let options = get_index_options(
            &SiteConfiguration::default(),
            |repo| Ok(repo_options(repo, "repo")),
            |_| Ok(vec![String::new(), "rev".to_owned()]),
            &[1],
        );
        let names: Vec<&str> = options[0]
            .branches
            .iter()
            .map(|branch| branch.name.as_str())
            .collect();
        assert_eq!(names, ["HEAD", "rev"]);
    }

    #[test]
    fn no_head_clears_all_branches() {
        let options = get_index_options(
            &SiteConfiguration::default(),
            |repo| {
                Ok(RepoIndexOptions {
                    name: "repo".to_owned(),
                    repo_id: repo,
                    get_version: Box::new(|branch| {
                        if branch == "HEAD" {
                            Ok(String::new())
                        } else {
                            Ok(format!("!{branch}"))
                        }
                    }),
                    ..RepoIndexOptions::default()
                })
            },
            |_| Ok(vec!["b1".to_owned()]),
            &[1],
        );
        assert!(options[0].branches.is_empty());
    }

    #[test]
    fn more_than_64_branches_are_truncated() {
        let branches: Vec<String> = (0..100).map(|index| format!("{index:02}")).collect();
        let config = SiteConfiguration {
            experimental_features: Some(ExperimentalFeatures {
                search_index_branches: Some(BTreeMap::from([("repo".to_owned(), branches)])),
                ..ExperimentalFeatures::default()
            }),
            ..SiteConfiguration::default()
        };
        let options = get_index_options(
            &config,
            |repo| Ok(repo_options(repo, "repo")),
            |_| Ok(Vec::new()),
            &[1],
        );
        assert_eq!(options[0].branches.len(), 64);
        assert_eq!(options[0].branches[0].name, "HEAD");
        assert_eq!(options[0].branches[63].name, "62");
    }

    #[test]
    fn get_version_errors_are_reported() {
        let options = get_index_options(
            &SiteConfiguration::default(),
            |repo| {
                Ok(RepoIndexOptions {
                    repo_id: repo,
                    get_version: Box::new(|_| Err("boom".to_owned())),
                    ..RepoIndexOptions::default()
                })
            },
            |_| Ok(Vec::new()),
            &[1],
        );
        assert_eq!(options[0].error, "boom");
        assert_eq!(options[0].repo_id, 1);
        assert!(options[0].language_map.is_none());
        assert!(options[0].branches.is_empty());
    }

    #[test]
    fn repo_lookup_and_context_errors_are_reported() {
        let options = get_index_options(
            &SiteConfiguration::default(),
            |_| Err("lookup failed".to_owned()),
            |_| Ok(Vec::new()),
            &[7],
        );
        assert_eq!(options[0].error, "lookup failed");
        assert_eq!(options[0].repo_id, 7);

        let options = get_index_options(
            &SiteConfiguration::default(),
            |repo| Ok(repo_options(repo, "repo")),
            |_| Err("context failed".to_owned()),
            &[1],
        );
        assert_eq!(options[0].error, "context failed");
        assert_eq!(options[0].repo_id, 1);
    }

    #[test]
    fn empty_repos_returns_an_empty_result() {
        let options = get_index_options(
            &SiteConfiguration::default(),
            |repo| Ok(repo_options(repo, "repo")),
            |_| Ok(Vec::new()),
            &[],
        );
        assert!(options.is_empty());
    }

    #[test]
    fn output_order_matches_input_order() {
        let options = get_index_options(
            &SiteConfiguration::default(),
            |repo| Ok(repo_options(repo, &format!("repo-{repo}"))),
            |_| Ok(Vec::new()),
            &[3, 1, 2],
        );
        let names: Vec<&str> = options
            .iter()
            .map(|options| options.name.as_str())
            .collect();
        assert_eq!(names, ["repo-3", "repo-1", "repo-2"]);
    }

    #[test]
    fn engine_map_reproduces_the_override_quirk() {
        let config = SiteConfiguration {
            syntax_highlighting: Some(SyntaxHighlighting {
                symbols: Some(SymbolConfiguration {
                    engine: BTreeMap::from([
                        // Valid override: ignored by the baseline.
                        ("go".to_owned(), "off".to_owned()),
                        // Invalid override: set to UnknownCtags.
                        ("rust".to_owned(), "bogus".to_owned()),
                        // Language is normalized.
                        ("C#".to_owned(), "bogus".to_owned()),
                    ]),
                }),
            }),
            ..SiteConfiguration::default()
        };
        let engines = create_engine_map(&config);
        assert_eq!(engines["go"], SCIP_CTAGS);
        assert_eq!(engines["rust"], UNKNOWN_CTAGS);
        assert_eq!(engines["c_sharp"], UNKNOWN_CTAGS);
    }

    #[test]
    fn json_matches_the_baseline_omitempty_semantics() {
        let options = get_index_options(
            &SiteConfiguration::default(),
            |repo| Ok(repo_options(repo, "repo")),
            |_| Ok(Vec::new()),
            &[1],
        );
        let value = serde_json::to_value(&options[0]).unwrap();
        assert_eq!(value["Name"], "repo");
        assert_eq!(value["RepoID"], 1);
        assert_eq!(value["Symbols"], true);
        assert_eq!(value["LargeFiles"], serde_json::Value::Null);
        assert_eq!(value["LanguageMap"]["go"], SCIP_CTAGS);
        assert_eq!(value["Branches"][0]["Name"], "HEAD");
        assert_eq!(value["Branches"][0]["Version"], "!HEAD");
        assert!(value.get("Priority").is_none());
        assert!(value.get("DocumentRanksVersion").is_none());
        assert!(value.get("Error").is_none());
        assert!(value.get("ShardConcurrency").is_none());

        // Error path: zero fields, null maps, omitted branches.
        let options = get_index_options(
            &SiteConfiguration::default(),
            |_| Err("boom".to_owned()),
            |_| Ok(Vec::new()),
            &[1],
        );
        let value = serde_json::to_value(&options[0]).unwrap();
        assert_eq!(value["Name"], "");
        assert_eq!(value["RepoID"], 1);
        assert_eq!(value["Symbols"], false);
        assert_eq!(value["LargeFiles"], serde_json::Value::Null);
        assert_eq!(value["LanguageMap"], serde_json::Value::Null);
        assert_eq!(value["Error"], "boom");
        assert!(value.get("Branches").is_none());
        assert!(value.get("Priority").is_none());
        assert!(value.get("ShardConcurrency").is_none());
    }

    #[test]
    fn site_config_deserializes_from_go_json_keys() {
        let config: SiteConfiguration = serde_json::from_value(serde_json::json!({
            "search.largeFiles": ["*.bin"],
            "search.index.symbols.enabled": false,
            "search.index.shardConcurrency": 27,
            "experimentalFeatures": {
                "search.index.branches": {"repo": ["a", "b"]},
                "search.index.revisions": [{"name": ".*", "revisions": ["c"]}],
            },
            "syntaxHighlighting": {"symbols": {"engine": {"go": "off"}}},
        }))
        .unwrap();
        assert_eq!(config.search_large_files, Some(vec!["*.bin".to_owned()]));
        assert_eq!(config.search_index_symbols_enabled, Some(false));
        assert_eq!(config.search_index_shard_concurrency, 27);
        assert_eq!(
            config
                .experimental_features
                .as_ref()
                .and_then(|features| features.search_index_branches.as_ref())
                .and_then(|branches| branches.get("repo"))
                .cloned(),
            Some(vec!["a".to_owned(), "b".to_owned()])
        );
        assert_eq!(
            config
                .syntax_highlighting
                .as_ref()
                .and_then(|highlighting| highlighting.symbols.as_ref())
                .and_then(|symbols| symbols.engine.get("go"))
                .map(String::as_str),
            Some("off")
        );
    }
}
