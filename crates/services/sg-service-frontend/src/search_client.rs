#![forbid(unsafe_code)]

//! `internal/search/client/client.go`: `Plan` and the search-type helpers.
//!
//! Ported: [`SearchClient::plan`], `detectSearchType`, `SearchTypeFromString`,
//! `overrideSearchType`, `ExperimentalPhraseBoost` (keyword only),
//! `ToFeatures`, `getBoolPtr`, `QueryError` and the parts of `search.Inputs`
//! this slice produces.
//!
//! `sanitizeSearchPatterns` is ported as [`sanitize_search_patterns`] over
//! the injected [`SanitizeDeps`] (site config, actor, user and org stores);
//! [`sanitize_patterns_loader`] adapts it to the
//! [`PlanDeps::sanitize_search_patterns`] call site. The production
//! configuration/DB wiring is a parent-layer concern. Search-context
//! resolution, user settings and feature flags are likewise injected so no
//! real database is needed.
//!
//! Not ported: `Execute`/the job tree.

use std::collections::BTreeMap;
use std::sync::Arc;

use sg_search_query::basic::{map_plan, plan_to_q, Plan};
use sg_search_query::fields::{FIELD_PATTERN_TYPE, FIELD_TYPE};
use sg_search_query::labels::Labels;
use sg_search_query::node::{
    go_quote, visit_field, Annotation, Node, Operator, OperatorKind, Pattern,
};
use sg_search_query::parser::{parse, SearchType};
use sg_search_query::pipeline::{init, pipeline, substitute_search_contexts, with_step};
use sg_search_query::transformer::lowercase_field_names;

use crate::actor_middleware::Actor;

/// `search.Mode` (`internal/search/types.go:65-70`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchMode {
    Precise,
    SmartSearch,
}

impl SearchMode {
    /// The baseline integer value (`Precise = 0`, `SmartSearch = 1`).
    pub fn as_int(self) -> i32 {
        match self {
            SearchMode::Precise => 0,
            SearchMode::SmartSearch => 1,
        }
    }
}

/// `search.Protocol` (`internal/search/types.go:72-95`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Streaming,
    Batch,
    Exhaustive,
}

impl Protocol {
    /// `Protocol.String()`.
    pub fn as_str(self) -> &'static str {
        match self {
            Protocol::Streaming => "Streaming",
            Protocol::Batch => "Batch",
            Protocol::Exhaustive => "Exhaustive",
        }
    }
}

/// The subset of `search.Features` this slice models (`types.go:387-400`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Features {
    pub content_based_lang_filters: bool,
    pub debug: bool,
}

/// `schema.Settings` fields read by `Plan`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Settings {
    pub experimental_features: Option<ExperimentalFeatures>,
    pub search_context_lines: Option<i32>,
}

/// `schema.SettingsExperimentalFeatures` field read by `Plan`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExperimentalFeatures {
    pub search_contexts_query: Option<bool>,
}

/// The `search.Inputs` fields produced by this slice (`types.go:31-43`).
#[derive(Debug, Clone, PartialEq)]
pub struct Inputs {
    pub plan: Plan,
    /// `Plan.ToQ()`.
    pub query: Vec<Node>,
    pub original_query: String,
    pub search_mode: SearchMode,
    pub pattern_type: SearchType,
    pub on_sourcegraph_dot_com: bool,
    pub features: Features,
    pub protocol: Protocol,
    pub context_lines: i32,
    pub sanitize_search_patterns: Vec<String>,
}

/// `QueryError` and the errors `Plan` can return (`client.go:227-234,262-279`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanError {
    /// `unrecognized version: want "V1", "V2", or "V3", got %q`.
    UnrecognizedVersion(String),
    /// `unrecognized patternType %q`.
    UnrecognizedPatternType(String),
    /// `failed to resolve user settings: <err>`.
    UserSettings(String),
    /// `invalid query %q: <err>`.
    Query { query: String, err: String },
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanError::UnrecognizedVersion(version) => write!(
                f,
                "unrecognized version: want \"V1\", \"V2\", or \"V3\", got {}",
                go_quote(version)
            ),
            PlanError::UnrecognizedPatternType(pattern_type) => {
                write!(f, "unrecognized patternType {}", go_quote(pattern_type))
            }
            PlanError::UserSettings(err) => {
                write!(f, "failed to resolve user settings: {err}")
            }
            PlanError::Query { query, err } => {
                write!(f, "invalid query {}: {err}", go_quote(query))
            }
        }
    }
}

impl std::error::Error for PlanError {}

// The `Send + Sync` bounds let a `searchClient` live inside the frontend's
// `Arc<dyn SearchDeps>` (M9.8hs). They are additive: every existing loader
// already captures `Send + Sync` state.
pub type SettingsLoader = Box<dyn Fn() -> Result<Settings, String> + Send + Sync>;
pub type FeatureFlagLoader = Box<dyn Fn() -> BTreeMap<String, bool> + Send + Sync>;
pub type SearchContextResolver = Arc<dyn Fn(&str) -> Result<String, String> + Send + Sync>;
pub type SanitizePatternsLoader = Box<dyn Fn() -> Vec<String> + Send + Sync>;

/// `schema.SettingsExperimentalFeatures.SearchSanitization`
/// (`client.go:183-216`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchSanitization {
    pub org_name: String,
    pub sanitize_patterns: Vec<String>,
}

/// The `database.User` fields `sanitizeSearchPatterns` reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SanitizeUser {
    pub id: i32,
    pub site_admin: bool,
}

/// `db.Orgs().GetByUserID` failure; `sanitizeSearchPatterns` swallows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrgLookupError;

/// Injected collaborators of [`sanitize_search_patterns`]: `conf.Get()`,
/// `actor.FromContext`, `db.Users()` and `db.Orgs()`.
pub trait SanitizeDeps: Send + Sync {
    /// `conf.Get().ExperimentalFeatures.SearchSanitization`.
    fn search_sanitization(&self) -> Option<SearchSanitization>;
    /// `actor.FromContext(ctx)`.
    fn actor(&self) -> Actor;
    /// `actr.User(ctx, db.Users())`: `None` models the error path.
    fn user(&self, id: i32) -> Option<SanitizeUser>;
    /// `db.Orgs().GetByUserID` names; `Err` models a DB error.
    fn user_org_names(&self, user_id: i32) -> Result<Vec<String>, OrgLookupError>;
    /// `log.Warn(message)`.
    fn warn(&self, message: &str);
}

/// `sanitizeSearchPatterns` (`internal/search/client/client.go:183-216`).
///
/// Returns the source text of the configured patterns that compile, unless
/// sanitization is disabled, the actor is internal, the user is a site admin
/// or belongs to the exempt org. The baseline returns `[]*regexp.Regexp`;
/// [`crate::search_jobutil_tree::TreeInputs::sanitize_search_patterns`] stores
/// the pattern strings, so only the sources are kept.
pub fn sanitize_search_patterns(deps: &dyn SanitizeDeps) -> Vec<String> {
    let mut sanitize_patterns: Vec<String> = Vec::new();

    let Some(sanitization) = deps.search_sanitization() else {
        return sanitize_patterns;
    };

    let actr = deps.actor();
    if actr.is_internal() {
        return Vec::new();
    }

    for pattern in &sanitization.sanitize_patterns {
        if regex::Regex::new(pattern).is_err() {
            deps.warn("invalid regex pattern provided, ignoring");
        } else {
            sanitize_patterns.push(pattern.clone());
        }
    }

    let Some(user) = deps.user(actr.uid) else {
        deps.warn("search being run as invalid user");
        return sanitize_patterns;
    };

    if user.site_admin {
        return Vec::new();
    }

    if !sanitization.org_name.is_empty() {
        match deps.user_org_names(user.id) {
            // DB error: keep the compiled patterns (the baseline swallows it).
            Err(OrgLookupError) => return sanitize_patterns,
            Ok(org_names) => {
                if org_names.iter().any(|name| name == &sanitization.org_name) {
                    return Vec::new();
                }
            }
        }
    }

    sanitize_patterns
}

/// Adapts [`SanitizeDeps`] to the [`PlanDeps::sanitize_search_patterns`]
/// injection point; the production configuration/DB wiring is a parent-layer
/// concern.
pub fn sanitize_patterns_loader(deps: Arc<dyn SanitizeDeps>) -> SanitizePatternsLoader {
    Box::new(move || sanitize_search_patterns(deps.as_ref()))
}

/// Injected dependencies of [`SearchClient::plan`].
///
/// The baseline reaches these through `settings.Service`, `featureflag` and
/// `searchcontexts.ResolveSearchContextSpec`; here they are plain closures so
/// the port (and its differential harness) needs no real services.
pub struct PlanDeps {
    /// `settingsService.UserFromContext`.
    pub user_settings: SettingsLoader,
    /// `featureflag.FromContext` flag values.
    pub feature_flags: FeatureFlagLoader,
    /// `searchcontexts.ResolveSearchContextSpec` -> `sc.Query`.
    pub resolve_search_context: SearchContextResolver,
    /// `sanitizeSearchPatterns`; wire [`sanitize_patterns_loader`] here.
    pub sanitize_search_patterns: SanitizePatternsLoader,
    /// `searchClient.sourcegraphDotComMode`.
    pub on_sourcegraph_dot_com: bool,
}

/// `searchClient` reduced to the `Plan` path.
pub struct SearchClient {
    deps: PlanDeps,
}

impl SearchClient {
    pub fn new(deps: PlanDeps) -> SearchClient {
        SearchClient { deps }
    }

    /// `searchClient.Plan` (`client.go:83-161`).
    pub fn plan(
        &self,
        version: &str,
        pattern_type: Option<&str>,
        search_query: &str,
        search_mode: SearchMode,
        protocol: Protocol,
        context_lines: Option<i32>,
    ) -> Result<Inputs, PlanError> {
        let search_type = detect_search_type(version, pattern_type)?;
        let search_type = override_search_type(search_query, search_type);

        let settings = (self.deps.user_settings)().map_err(PlanError::UserSettings)?;

        // Beta: replace each `context:` term with the search context's query.
        let search_contexts_query_enabled = settings
            .experimental_features
            .as_ref()
            .is_some_and(|features| get_bool_ptr(features.search_contexts_query, true));
        let resolver = self.deps.resolve_search_context.clone();
        let substitute_contexts_step =
            substitute_search_contexts(move |context: &str| resolver(context));

        let plan = pipeline(vec![
            init(search_query.to_owned(), search_type),
            with_step(search_contexts_query_enabled, substitute_contexts_step),
        ])
        .map_err(|err| PlanError::Query {
            query: search_query.to_owned(),
            err,
        })?;

        let plan = if search_type == SearchType::Keyword {
            map_plan(plan, &mut |basic| {
                experimental_phrase_boost(search_query, basic)
            })
        } else {
            plan
        };

        let final_context_lines = match context_lines {
            Some(context_lines) => context_lines,
            // default
            None => settings.search_context_lines.unwrap_or(1),
        };

        let flags = (self.deps.feature_flags)();
        let sanitize_search_patterns = (self.deps.sanitize_search_patterns)();

        Ok(Inputs {
            query: plan_to_q(&plan),
            plan,
            original_query: search_query.to_owned(),
            search_mode,
            pattern_type: search_type,
            on_sourcegraph_dot_com: self.deps.on_sourcegraph_dot_com,
            features: to_features(Some(&flags)),
            protocol,
            context_lines: final_context_lines,
            sanitize_search_patterns,
        })
    }
}

/// `SearchTypeFromString` (`client.go:236-256`).
pub fn search_type_from_string(pattern_type: &str) -> Result<SearchType, PlanError> {
    match pattern_type {
        "standard" => Ok(SearchType::Standard),
        "literal" => Ok(SearchType::Literal),
        "regexp" => Ok(SearchType::Regex),
        "structural" => Ok(SearchType::Structural),
        "codycontext" => Ok(SearchType::CodyContext),
        "keyword" => Ok(SearchType::Keyword),
        // NOTE: the lucky patterntype is deprecated. For now, we remap it to
        // 'standard' to avoid breaks.
        "lucky" => Ok(SearchType::Standard),
        other => Err(PlanError::UnrecognizedPatternType(other.to_owned())),
    }
}

/// `detectSearchType` (`client.go:262-279`).
pub fn detect_search_type(
    version: &str,
    pattern_type: Option<&str>,
) -> Result<SearchType, PlanError> {
    if let Some(pattern_type) = pattern_type {
        return search_type_from_string(pattern_type);
    }
    match version {
        "V1" => Ok(SearchType::Regex),
        "V2" => Ok(SearchType::Literal),
        "V3" => Ok(SearchType::Standard),
        other => Err(PlanError::UnrecognizedVersion(other.to_owned())),
    }
}

/// `overrideSearchType` (`client.go:281-309`).
pub fn override_search_type(input: &str, search_type: SearchType) -> SearchType {
    let Ok(query) = parse(input, SearchType::Literal) else {
        // If parsing fails, return the default search type. Any actual parse
        // errors will be raised by subsequent parser calls.
        return search_type;
    };
    let query = lowercase_field_names(&query);
    let mut search_type = search_type;
    visit_field(
        &query,
        FIELD_PATTERN_TYPE,
        &mut |value, _negated, _annotation| {
            search_type = match value {
                "standard" => SearchType::Standard,
                "regex" | "regexp" => SearchType::Regex,
                "literal" => SearchType::Literal,
                "structural" => SearchType::Structural,
                "codycontext" => SearchType::CodyContext,
                "keyword" => SearchType::Keyword,
                // NOTE: the lucky patterntype is deprecated. For now, we
                // remap it to 'standard' to avoid breaks.
                "lucky" => SearchType::Standard,
                _ => search_type,
            };
        },
    );
    search_type
}

/// `ExperimentalPhraseBoost` (`transformer.go:685-744`).
pub fn experimental_phrase_boost(
    original_query: &str,
    basic: sg_search_query::Basic,
) -> sg_search_query::Basic {
    let Some(pattern) = basic.pattern.as_ref() else {
        return basic;
    };

    // Only apply the ranking boost for text searches.
    for parameter in &basic.parameters.0 {
        if parameter.field == FIELD_TYPE
            && !(parameter.value == "file" || parameter.value == "path")
        {
            return basic;
        }
    }

    // Check if the pattern is a single top-level AND expression with no
    // negated or regexp clauses.
    match pattern {
        Node::Pattern(pattern) => {
            if !pattern.annotation.labels.is_set(Labels::QUOTED)
                || pattern.negated
                || pattern.annotation.labels.is_set(Labels::REGEXP)
            {
                return basic;
            }
        }
        Node::Operator(operator) => {
            if operator.kind != OperatorKind::And {
                return basic;
            }
            for child in &operator.operands {
                match child {
                    Node::Pattern(child) => {
                        if child.negated || child.annotation.labels.is_set(Labels::REGEXP) {
                            return basic;
                        }
                    }
                    _ => return basic,
                }
            }
        }
        Node::Parameter(_) => return basic,
    }

    // Remove predicates from the original query to keep just the pattern
    // string.
    let terms: Vec<&str> = original_query
        .split_whitespace()
        .filter(|term| !term.contains(':'))
        .collect();
    let phrase = terms.join(" ");

    let boosted = Node::Operator(Operator {
        kind: OperatorKind::Or,
        operands: vec![
            Node::Pattern(Pattern {
                value: phrase,
                negated: false,
                annotation: Annotation {
                    labels: Labels::BOOST | Labels::LITERAL | Labels::STANDARD,
                    range: Default::default(),
                },
            }),
            pattern.clone(),
        ],
        annotation: Annotation::default(),
    });

    sg_search_query::Basic::new(basic.parameters, Some(boosted))
}

/// `ToFeatures` (`client.go:311-324`). `None` models the baseline's nil
/// `FlagSet`; both fields then default to false.
pub fn to_features(flags: Option<&BTreeMap<String, bool>>) -> Features {
    let get = |flag: &str| {
        flags
            .and_then(|flags| flags.get(flag).copied())
            .unwrap_or(false)
    };
    Features {
        content_based_lang_filters: get("search-content-based-lang-detection"),
        debug: get("search-debug"),
    }
}

/// `getBoolPtr` (`client.go:331-336`).
pub fn get_bool_ptr(value: Option<bool>, default: bool) -> bool {
    value.unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_deps() -> PlanDeps {
        PlanDeps {
            user_settings: Box::new(|| Ok(Settings::default())),
            feature_flags: Box::new(BTreeMap::new),
            resolve_search_context: Arc::new(|_: &str| Err("unexpected context lookup".to_owned())),
            sanitize_search_patterns: Box::new(Vec::new),
            on_sourcegraph_dot_com: false,
        }
    }

    fn run_plan(
        deps: PlanDeps,
        version: &str,
        pattern_type: Option<&str>,
        query: &str,
        context_lines: Option<i32>,
    ) -> Result<Inputs, PlanError> {
        SearchClient::new(deps).plan(
            version,
            pattern_type,
            query,
            SearchMode::Precise,
            Protocol::Streaming,
            context_lines,
        )
    }

    fn plan(query: &str, version: &str, pattern_type: Option<&str>) -> Result<Inputs, PlanError> {
        run_plan(test_deps(), version, pattern_type, query, None)
    }

    #[test]
    fn detect_search_type_matches_baseline() {
        assert_eq!(detect_search_type("V1", None).unwrap(), SearchType::Regex);
        assert_eq!(detect_search_type("V2", None).unwrap(), SearchType::Literal);
        assert_eq!(
            detect_search_type("V3", None).unwrap(),
            SearchType::Standard
        );
        assert_eq!(
            detect_search_type("V1", Some("literal")).unwrap(),
            SearchType::Literal
        );
        assert_eq!(
            detect_search_type("V1", Some("lucky")).unwrap(),
            SearchType::Standard
        );
        assert_eq!(
            detect_search_type("V99", None).unwrap_err().to_string(),
            "unrecognized version: want \"V1\", \"V2\", or \"V3\", got \"V99\""
        );
        assert_eq!(
            detect_search_type("V1", Some("invalid"))
                .unwrap_err()
                .to_string(),
            "unrecognized patternType \"invalid\""
        );
    }

    #[test]
    fn override_search_type_matches_baseline() {
        assert_eq!(
            override_search_type("patterntype:regexp", SearchType::Literal),
            SearchType::Regex
        );
        assert_eq!(
            override_search_type("pAtTErNTypE:literal", SearchType::Regex),
            SearchType::Literal
        );
        assert_eq!(
            override_search_type("patternType:lucky", SearchType::Regex),
            SearchType::Standard
        );
        // The last occurrence wins.
        assert_eq!(
            override_search_type(
                "patterntype:literal patterntype:regexp",
                SearchType::Standard
            ),
            SearchType::Regex
        );
        // Unknown values are ignored.
        assert_eq!(
            override_search_type("patterntype:bogus", SearchType::Literal),
            SearchType::Literal
        );
    }

    #[test]
    fn context_lines_default_and_settings() {
        let inputs = plan("a", "V3", None).unwrap();
        assert_eq!(inputs.context_lines, 1);

        let mut deps = test_deps();
        deps.user_settings = Box::new(|| {
            Ok(Settings {
                search_context_lines: Some(5),
                ..Settings::default()
            })
        });
        let inputs = run_plan(deps, "V3", None, "a", Some(2)).unwrap();
        assert_eq!(inputs.context_lines, 2);

        let mut deps = test_deps();
        deps.user_settings = Box::new(|| {
            Ok(Settings {
                search_context_lines: Some(5),
                ..Settings::default()
            })
        });
        let inputs = run_plan(deps, "V3", None, "a", None).unwrap();
        assert_eq!(inputs.context_lines, 5);
    }

    #[test]
    fn substitution_flag_semantics() {
        // ExperimentalFeatures present with SearchContextsQuery unset ->
        // substitution enabled (getBoolPtr default true).
        let mut deps = test_deps();
        deps.user_settings = Box::new(|| {
            Ok(Settings {
                experimental_features: Some(ExperimentalFeatures::default()),
                ..Settings::default()
            })
        });
        deps.resolve_search_context = Arc::new(|context: &str| Ok(format!("repo:{context}")));
        let inputs = run_plan(deps, "V3", None, "context:foo a", None).unwrap();
        assert!(inputs
            .plan
            .iter()
            .any(
                |basic| sg_search_query::string_human(&basic.to_parse_tree()).contains("repo:foo")
            ));

        // ExperimentalFeatures nil -> substitution disabled.
        let mut deps = test_deps();
        deps.resolve_search_context = Arc::new(|context: &str| Ok(format!("repo:{context}")));
        let inputs = run_plan(deps, "V3", None, "context:foo a", None).unwrap();
        assert!(inputs
            .plan
            .iter()
            .any(
                |basic| sg_search_query::string_human(&basic.to_parse_tree())
                    .contains("context:foo")
            ));

        // ExperimentalFeatures present with the flag off -> disabled.
        let mut deps = test_deps();
        deps.user_settings = Box::new(|| {
            Ok(Settings {
                experimental_features: Some(ExperimentalFeatures {
                    search_contexts_query: Some(false),
                }),
                ..Settings::default()
            })
        });
        let inputs = run_plan(deps, "V3", None, "context:foo a", None).unwrap();
        assert!(inputs
            .plan
            .iter()
            .any(
                |basic| sg_search_query::string_human(&basic.to_parse_tree())
                    .contains("context:foo")
            ));
    }

    #[test]
    fn query_errors_are_wrapped() {
        let err = plan("g:foo a", "V3", None).unwrap_err();
        assert!(err
            .to_string()
            .starts_with("invalid query \"g:foo a\": unrecognized field"));
    }

    #[test]
    fn settings_errors_are_wrapped() {
        let mut deps = test_deps();
        deps.user_settings = Box::new(|| Err("boom".to_owned()));
        let err = run_plan(deps, "V3", None, "a", None).unwrap_err();
        assert_eq!(err.to_string(), "failed to resolve user settings: boom");
    }

    #[test]
    fn features_default_to_false() {
        assert_eq!(to_features(None), Features::default());
        let mut flags = BTreeMap::new();
        flags.insert("search-debug".to_owned(), true);
        assert_eq!(
            to_features(Some(&flags)),
            Features {
                content_based_lang_filters: false,
                debug: true,
            }
        );
    }
}
