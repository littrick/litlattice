#![forbid(unsafe_code)]

//! CODEOWNERS parsing, ownership rule matching and the production ownership
//! seams (M9.38b).
//!
//! Baseline:
//!
//! * `internal/own/codeowners/{parse,file,owner_types}.go` — CODEOWNERS
//!   parsing and [`Ruleset`] last-rule-wins matching;
//! * `internal/paths/match.go` — the CODEOWNERS glob compiler/matcher;
//! * `internal/own/service.go` — assigned owners/teams path inheritance;
//! * `internal/own/search/rules_cache.go` — [`RulesCache`];
//! * `internal/own/ownref.go` — [`Bag`] text-reference resolution.
//!
//! [`PgOwnershipSource`] is the production DB/gitserver adapter. It only
//! reads existing tables (`codeowners`, `assigned_owners`, `assigned_teams`,
//! `repo`, `users`, `user_emails`, `teams`); no table, column or Redis key is
//! added. The Go store is synchronous while `sqlx` is async, so the adapter
//! bridges with the same `block_in_place`/`block_on` helper used by
//! `code_monitors_graphql_store`.
//!
//! [`GitserverContributorFetcher`] is the production `file:has.contributor()`
//! seam. M9.39c adds the real `sg_git::GitserverClient::contributor_count`
//! transport (`GitClientContributorCountSource`, and the `LiveConfig`-bound
//! `production_contributor_fetcher`) plus the external-account handle
//! augmentation of `Bag` resolution (`fetchCodeHostHandles`/`linkBack`).
//!
//! See `docs/overviews/m9_38b_owners_parser_overview.md` and
//! `docs/overviews/m9_39c_contributor_grpc_overview.md`.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use regex::Regex;
use sqlx::{PgPool, Row};

use sg_encryption::Key;

use crate::git_graphql::{GitObjectSource, GitSourceError};
use crate::search_job::JobError;
use crate::search_jobutil_filters::{
    ContributorCount, ContributorFetcher, ContributorOptions,
};
use crate::search_leaf_jobs::{OwnershipFetcher, OwnershipRef};

fn block_on<F: Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("owners blocking runtime")
            .block_on(future),
    }
}

// ---------------------------------------------------------------------------
// CODEOWNERS parsing (`internal/own/codeowners/parse.go`)
// ---------------------------------------------------------------------------

/// `codeownerspb.Pattern` + owners, in file order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Rule {
    pub pattern: String,
    pub section_name: String,
    pub line_number: i32,
    pub owners: Vec<OwnershipRef>,
}

/// `codeownerspb.File` reduced to the fields the ownership jobs read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParsedFile {
    pub rules: Vec<Rule>,
}

fn rule_pattern() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^\s*((?:\\.|\S)+)((?:\s+\S+)*)\s*$").expect("rule regexp"))
}

fn section_pattern() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"^\s*\^?\s*\[([^\]]+)\]\s*(?:\[[0-9]+\])?\s*$").expect("section regexp")
    })
}

/// `Parse` (`parse.go:23`). Rules keep file order, which determines matching.
pub fn parse_codeowners(input: &str) -> Result<ParsedFile, String> {
    let mut rules = Vec::new();
    let mut section = String::new();
    for (index, line) in input.lines().enumerate() {
        let line_number = index as i32 + 1;
        let without_comments = line_without_comments(line);
        if without_comments.trim().is_empty() {
            continue;
        }
        if let Some(name) = match_section(&without_comments) {
            section = name;
            continue;
        }
        let Some((pattern, owners)) = match_rule(&without_comments) else {
            return Err(format!("failed to match rule: {line}"));
        };
        rules.push(Rule {
            pattern: unescape(&pattern),
            section_name: section.trim().to_lowercase(),
            line_number,
            owners: owners.iter().map(|owner| parse_owner(owner)).collect(),
        });
    }
    Ok(ParsedFile { rules })
}

/// `ParseOwner` (`parse.go:66`).
pub fn parse_owner(owner_text: &str) -> OwnershipRef {
    if let Some(handle) = owner_text.strip_prefix('@') {
        return OwnershipRef {
            handle: handle.to_owned(),
            ..OwnershipRef::default()
        };
    }
    if is_mail_address(owner_text) {
        return OwnershipRef {
            email: owner_text.to_owned(),
            ..OwnershipRef::default()
        };
    }
    OwnershipRef {
        handle: owner_text.to_owned(),
        ..OwnershipRef::default()
    }
}

/// `net/mail.ParseAddress` approximation for the whitespace-delimited owner
/// tokens CODEOWNERS permits. Display names cannot appear here (they contain
/// whitespace and would have been split into separate owners).
pub(crate) fn is_mail_address(text: &str) -> bool {
    if text.is_empty() || text.starts_with('@') || text.chars().any(|c| c.is_whitespace()) {
        return false;
    }
    let mut parts = text.split('@');
    matches!(
        (parts.next(), parts.next(), parts.next()),
        (Some(local), Some(domain), None) if !local.is_empty() && !domain.is_empty()
    )
}

fn match_section(line: &str) -> Option<String> {
    section_pattern()
        .captures(line)
        .map(|caps| caps.get(1).expect("section").as_str().to_owned())
}

fn match_rule(line: &str) -> Option<(String, Vec<String>)> {
    let caps = rule_pattern().captures(line)?;
    Some((
        caps.get(1)?.as_str().to_owned(),
        caps.get(2)
            .map(|owners| owners.as_str().split_whitespace().map(str::to_owned).collect())
            .unwrap_or_default(),
    ))
}

/// `lineWithoutComments` (`parse.go:149`).
pub(crate) fn line_without_comments(line: &str) -> String {
    let mut comment_start = line.len();
    let mut is_escaped = false;
    for (index, c) in line.char_indices() {
        if c == '#' && !is_escaped {
            comment_start = index;
            break;
        }
        if c == '\\' && !is_escaped {
            is_escaped = true;
            continue;
        }
        is_escaped = false;
    }
    line[..comment_start].to_owned()
}

/// `unescape` (`parse.go:172`).
pub(crate) fn unescape(value: &str) -> String {
    let mut out = String::new();
    let mut is_escaped = false;
    for c in value.chars() {
        if c == '\\' && !is_escaped {
            is_escaped = true;
            continue;
        }
        out.push(c);
        is_escaped = false;
    }
    out
}

/// `Ruleset.Repr` (`repr.go:15`), useful for differential normalization.
pub fn repr(file: &ParsedFile) -> String {
    let mut out = String::new();
    let mut last_section = String::new();
    for rule in &file.rules {
        if rule.section_name != last_section {
            out.push_str(&format!("[{}]\n", rule.section_name));
            last_section = rule.section_name.clone();
        }
        out.push_str(&rule.pattern);
        for owner in &rule.owners {
            if !owner.handle.is_empty() {
                out.push_str(&format!(" @{}", owner.handle));
            }
            if !owner.email.is_empty() {
                out.push_str(&format!(" {}", owner.email));
            }
        }
        out.push('\n');
    }
    out
}

// ---------------------------------------------------------------------------
// Glob (`internal/paths/match.go`)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum PatternPart {
    AnySubPath,
    Exact(String),
    Any,
    Asterisk(String),
}

impl PatternPart {
    fn matches(&self, part: &str) -> bool {
        match self {
            PatternPart::AnySubPath => true,
            PatternPart::Exact(exact) => exact == part,
            PatternPart::Any => true,
            PatternPart::Asterisk(glob) => wildmatch(glob, part),
        }
    }
}

/// `wildmatch.WildMatch`: `*` matches any run of characters, `?` matches one.
fn wildmatch(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some((pi, ti));
            pi += 1;
        } else if let Some((sp, st)) = star {
            pi = sp + 1;
            ti = st + 1;
            star = Some((sp, st + 1));
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

/// `paths.Compile` (`match.go:63`).
#[derive(Debug, Clone)]
pub struct GlobPattern {
    is_literal: bool,
    pattern: String,
    parts: Vec<PatternPart>,
    initial_state: u128,
    size: usize,
}

impl GlobPattern {
    /// `Match` (`match.go:157`).
    pub fn matches(&self, file_path: &str) -> bool {
        if self.is_literal {
            return self.pattern == file_path;
        }
        let mut file_path = file_path;
        if file_path.starts_with('/') {
            file_path = &file_path[1..];
        }
        if file_path.ends_with('/') {
            file_path = &file_path[..file_path.len() - 1];
        }
        let mut current_state = self.initial_state;
        let mut next_state = 0u128;
        loop {
            let (part, has_next, rest) = next_part(file_path);
            file_path = rest;
            for i in 0..self.size {
                if (current_state >> i) & 1 == 0 {
                    continue;
                }
                let current_part = &self.parts[i];
                if current_part.matches(part) {
                    next_state |= 1u128 << (i + 1);
                    if i + 1 < self.size
                        && matches!(self.parts[i + 1], PatternPart::AnySubPath)
                    {
                        next_state |= 1u128 << (i + 2);
                    }
                } else {
                    next_state &= !(1u128 << (i + 1));
                    if i + 1 < self.size
                        && matches!(self.parts[i + 1], PatternPart::AnySubPath)
                    {
                        next_state &= !(1u128 << (i + 2));
                    }
                }
                if matches!(current_part, PatternPart::AnySubPath) {
                    next_state |= 1u128 << i;
                }
            }
            if current_state == 0 {
                return false;
            }
            current_state = next_state;
            if !has_next {
                break;
            }
            next_state = 0;
        }
        (current_state >> self.size) & 1 == 1
    }
}

/// `nextPart` (`match.go:283`).
fn next_part(s: &str) -> (&str, bool, &str) {
    match s.find('/') {
        Some(index) => (&s[..index], true, &s[index + 1..]),
        None => (s, false, s),
    }
}

/// `Compile` (`match.go:63`).
pub fn compile_glob(pattern: &str) -> Result<GlobPattern, String> {
    let trimmed = pattern.trim_matches('/');
    let mut parts: Vec<PatternPart> = Vec::new();
    let mut is_literal = true;
    if !pattern.starts_with('/') {
        parts.push(PatternPart::AnySubPath);
        is_literal = false;
    }
    for part in trimmed.split('/') {
        match part {
            "" => return Err("two consecutive forward slashes".to_owned()),
            "**" => {
                parts.push(PatternPart::AnySubPath);
                is_literal = false;
            }
            "*" => {
                parts.push(PatternPart::Any);
                is_literal = false;
            }
            other if other.contains('*') => {
                parts.push(PatternPart::Asterisk(other.to_owned()));
                is_literal = false;
            }
            other => parts.push(PatternPart::Exact(other.to_owned())),
        }
    }
    if pattern.ends_with('/') {
        parts.push(PatternPart::AnySubPath);
        is_literal = false;
    }
    if matches!(parts.last(), Some(PatternPart::AnySubPath)) {
        parts.push(PatternPart::Any);
        is_literal = false;
    }
    let mut initial_state = 1u128;
    for (index, part) in parts.iter().enumerate() {
        if !matches!(part, PatternPart::AnySubPath) {
            break;
        }
        initial_state |= 1u128 << (index + 1);
    }
    Ok(GlobPattern {
        is_literal,
        pattern: pattern.to_owned(),
        size: parts.len(),
        parts,
        initial_state,
    })
}

// ---------------------------------------------------------------------------
// Ruleset (`internal/own/codeowners/file.go`)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct CompiledRule {
    rule: Rule,
    glob: OnceLock<Option<GlobPattern>>,
}

/// `codeowners.Ruleset`.
#[derive(Debug, Clone, Default)]
pub struct Ruleset {
    rules: Vec<CompiledRule>,
}

impl Ruleset {
    pub fn new(rules: Vec<Rule>) -> Self {
        Self {
            rules: rules
                .into_iter()
                .map(|rule| CompiledRule {
                    rule,
                    glob: OnceLock::new(),
                })
                .collect(),
        }
    }

    pub fn rules(&self) -> Vec<&Rule> {
        self.rules.iter().map(|compiled| &compiled.rule).collect()
    }

    /// `Match` (`file.go:60`): last matching rule wins.
    pub fn match_path(&self, path: &str) -> Option<&Rule> {
        let mut path = path.to_owned();
        if !path.starts_with('/') {
            path.insert(0, '/');
        }
        for compiled in self.rules.iter().rev() {
            let glob = compiled
                .glob
                .get_or_init(|| compile_glob(&compiled.rule.pattern).ok());
            if glob
                .as_ref()
                .is_some_and(|glob| glob.matches(&path))
            {
                return Some(&compiled.rule);
            }
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Assigned owners/teams (`internal/own/service.go`)
// ---------------------------------------------------------------------------

pub type AssignedOwners = HashMap<String, Vec<i32>>;
pub type AssignedTeams = HashMap<String, Vec<i32>>;

/// `match` (`service.go:78`): owners of every ancestor directory, then root.
pub fn match_assigned<T: Clone>(assigned: &HashMap<String, Vec<T>>, path: &str) -> Vec<T> {
    let mut summaries = Vec::new();
    let mut current = path;
    loop {
        summaries.extend(assigned.get(current).cloned().unwrap_or_default());
        match current.rfind('/') {
            Some(index) => current = &current[..index],
            None => break,
        }
    }
    if !current.is_empty() {
        summaries.extend(assigned.get("").cloned().unwrap_or_default());
    }
    summaries
}

// ---------------------------------------------------------------------------
// References and bags (`internal/own/ownref.go`)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RefKey {
    UserId(i32),
    TeamId(i32),
    Handle(String),
    Email(String),
}

/// `own.Bag` over the reference keys relevant to ownership search. Resolution
/// is performed by [`OwnershipSource`] and the resolved references are added
/// with [`Bag::add_reference`], mirroring `bag.linkBack`.
#[derive(Debug, Clone, Default)]
pub struct Bag {
    keys: HashSet<RefKey>,
}

impl Bag {
    pub fn empty() -> Self {
        Self::default()
    }

    /// `ByTextReference` (`ownref.go:135`) before DB resolution.
    pub fn by_text_reference(text: &str) -> Self {
        let mut bag = Self::empty();
        if text.is_empty() {
            return bag;
        }
        if is_mail_address(text) {
            bag.keys.insert(RefKey::Email(text.to_owned()));
        } else {
            bag.keys.insert(RefKey::Handle(
                text.trim_start_matches('@').to_owned(),
            ));
        }
        bag
    }

    /// `Bag.Add`.
    pub fn add_reference(&mut self, reference: &OwnershipRef) {
        if let Some(id) = reference.user_id {
            self.keys.insert(RefKey::UserId(id));
        }
        if let Some(id) = reference.team_id {
            self.keys.insert(RefKey::TeamId(id));
        }
        if !reference.handle.is_empty() {
            self.keys.insert(RefKey::Handle(reference.handle.clone()));
        }
        if !reference.email.is_empty() {
            self.keys.insert(RefKey::Email(reference.email.clone()));
        }
    }

    /// `Bag.Contains`.
    pub fn contains(&self, reference: &OwnershipRef) -> bool {
        let mut keys = Vec::new();
        if let Some(id) = reference.user_id {
            keys.push(RefKey::UserId(id));
        }
        if let Some(id) = reference.team_id {
            keys.push(RefKey::TeamId(id));
        }
        if !reference.handle.is_empty() {
            keys.push(RefKey::Handle(
                reference.handle.trim_start_matches('@').to_owned(),
            ));
        }
        if !reference.email.is_empty() {
            keys.push(RefKey::Email(reference.email.clone()));
        }
        keys.iter().any(|key| self.keys.contains(key))
    }
}

/// `fileOwnershipData` (`rules_cache.go:143`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileOwnershipData {
    pub rule: Option<Rule>,
    pub assigned_owners: Vec<i32>,
    pub assigned_teams: Vec<i32>,
}

impl FileOwnershipData {
    pub fn non_empty(&self) -> bool {
        self.rule
            .as_ref()
            .is_some_and(|rule| !rule.owners.is_empty())
            || !self.assigned_owners.is_empty()
            || !self.assigned_teams.is_empty()
    }

    pub fn empty(&self) -> bool {
        !self.non_empty()
    }

    /// `References` (`rules_cache.go:167`).
    pub fn references(&self) -> Vec<OwnershipRef> {
        let mut refs = Vec::new();
        if let Some(rule) = &self.rule {
            refs.extend(rule.owners.iter().cloned());
        }
        for user_id in &self.assigned_owners {
            refs.push(OwnershipRef {
                user_id: Some(*user_id),
                ..OwnershipRef::default()
            });
        }
        for team_id in &self.assigned_teams {
            refs.push(OwnershipRef {
                team_id: Some(*team_id),
                ..OwnershipRef::default()
            });
        }
        refs
    }

    /// `IsWithin` (`rules_cache.go:191`).
    pub fn is_within(&self, bag: &Bag) -> bool {
        self.references().iter().any(|reference| bag.contains(reference))
    }
}

// ---------------------------------------------------------------------------
// RulesCache and production seams (`internal/own/search/rules_cache.go`)
// ---------------------------------------------------------------------------

/// Production data dependencies for ownership search. Every method is
/// synchronous because the search job tree is synchronous; the production
/// implementation bridges async sqlx/gRPC internally.
pub trait OwnershipSource: Send + Sync {
    fn assigned_owners(&self, repo_id: i32) -> Result<AssignedOwners, JobError>;
    fn assigned_teams(&self, repo_id: i32) -> Result<AssignedTeams, JobError>;
    fn codeowners(
        &self,
        repo_id: i32,
        repo_name: &str,
        commit: &str,
    ) -> Result<Option<Ruleset>, JobError>;
    fn repo_code_host_kind(&self, repo_id: i32) -> Result<String, JobError>;

    /// `users.GetByID` + primary email; `None` when the user is gone.
    fn resolve_user(&self, _user_id: i32) -> Option<(String, String)> {
        None
    }
    /// `teams.GetTeamByID`.
    fn resolve_team(&self, _team_id: i32) -> Option<String> {
        None
    }
    /// `own.ByTextReference(...).Resolve`: all references that resolve to the
    /// same user/team as `text`, including augmented handles/emails.
    fn resolve_text(&self, _text: &str) -> Vec<OwnershipRef> {
        Vec::new()
    }
}

/// `repoOwnershipData` (`rules_cache.go:129`).
#[derive(Debug, Clone, Default)]
pub struct RepoOwnershipData {
    pub codeowners: Ruleset,
    pub assigned: AssignedOwners,
    pub assigned_teams: AssignedTeams,
}

impl RepoOwnershipData {
    pub fn match_path(&self, path: &str) -> FileOwnershipData {
        FileOwnershipData {
            rule: self.codeowners.match_path(path).cloned(),
            assigned_owners: match_assigned(&self.assigned, path),
            assigned_teams: match_assigned(&self.assigned_teams, path),
        }
    }
}

/// `RulesCache` (`rules_cache.go:29`). The cache is keyed like the baseline:
/// rules by `(repoName, commitID)`, assigned owners/teams by `repoID`.
pub struct RulesCache {
    source: Arc<dyn OwnershipSource>,
    rules: Mutex<HashMap<(String, String), Arc<Ruleset>>>,
    assigned: Mutex<HashMap<i32, Arc<RepoOwnershipData>>>,
}

impl RulesCache {
    pub fn new(source: Arc<dyn OwnershipSource>) -> Self {
        Self {
            source,
            rules: Mutex::new(HashMap::new()),
            assigned: Mutex::new(HashMap::new()),
        }
    }

    /// `GetFromCacheOrFetch` (`rules_cache.go:47`).
    pub fn get(
        &self,
        repo_id: i32,
        repo_name: &str,
        commit: &str,
    ) -> Result<Arc<RepoOwnershipData>, JobError> {
        let key = (repo_name.to_owned(), commit.to_owned());
        let rules = {
            let mut cache = self.rules.lock().expect("rules cache");
            match cache.get(&key) {
                Some(ruleset) => Arc::clone(ruleset),
                None => {
                    // `Codeowners` caches an empty ruleset when the file is
                    // missing and still returns the fetch error (if any).
                    let ruleset = self.source.codeowners(repo_id, repo_name, commit)?;
                    let ruleset = Arc::new(ruleset.unwrap_or_default());
                    cache.insert(key, Arc::clone(&ruleset));
                    ruleset
                }
            }
        };
        let mut cache = self.assigned.lock().expect("assigned cache");
        if let Some(data) = cache.get(&repo_id) {
            let mut data = (**data).clone();
            data.codeowners = (*rules).clone();
            return Ok(Arc::new(data));
        }
        let assigned = self.source.assigned_owners(repo_id)?;
        let assigned_teams = self.source.assigned_teams(repo_id)?;
        let data = Arc::new(RepoOwnershipData {
            codeowners: (*rules).clone(),
            assigned,
            assigned_teams,
        });
        cache.insert(repo_id, Arc::clone(&data));
        Ok(data)
    }

    pub fn source(&self) -> &Arc<dyn OwnershipSource> {
        &self.source
    }
}

/// Production `OwnershipFetcher`: `RulesCache` + DB/gitserver source
/// (`own.Service` + `own/search.RulesCache`).
pub struct RulesCacheOwnershipFetcher {
    cache: RulesCache,
}

impl RulesCacheOwnershipFetcher {
    pub fn new(source: Arc<dyn OwnershipSource>) -> Self {
        Self {
            cache: RulesCache::new(source),
        }
    }

    pub fn cache(&self) -> &RulesCache {
        &self.cache
    }
}

impl OwnershipFetcher for RulesCacheOwnershipFetcher {
    fn owners(
        &self,
        repo_id: i32,
        repo_name: &str,
        commit: &str,
        path: &str,
    ) -> Result<Vec<OwnershipRef>, JobError> {
        let data = self.cache.get(repo_id, repo_name, commit)?;
        let ownership = data.match_path(path);
        let code_host_kind = self
            .cache
            .source()
            .repo_code_host_kind(repo_id)
            .unwrap_or_default();
        let mut references = ownership.references();
        for reference in &mut references {
            if let Some(user_id) = reference.user_id {
                if reference.handle.is_empty() && reference.email.is_empty() {
                    if let Some((handle, email)) = self.cache.source().resolve_user(user_id) {
                        reference.handle = handle;
                        reference.email = email;
                    }
                }
            }
            if let Some(team_id) = reference.team_id {
                if reference.handle.is_empty() {
                    if let Some(name) = self.cache.source().resolve_team(team_id) {
                        reference.handle = name;
                    }
                }
            }
            if !code_host_kind.is_empty() {
                reference.code_host_kind = Some(code_host_kind.clone());
                reference.repo_name = Some(repo_name.to_owned());
            }
        }
        Ok(references)
    }

    fn resolve_text(&self, text: &str) -> Vec<OwnershipRef> {
        self.cache.source().resolve_text(text)
    }
}

/// CODEOWNERS locations, in priority order (`own/service.go:87`).
pub const CODEOWNERS_LOCATIONS: [&str; 5] = [
    ".github/test.CODEOWNERS",
    "CODEOWNERS",
    ".github/CODEOWNERS",
    ".gitlab/CODEOWNERS",
    "docs/CODEOWNERS",
];

/// `fetchCodeHostHandles`'s per-provider `PublicAccountData.Login`
/// (`internal/own/ownref.go:429`).
///
/// The stored `account_data` JSON is decoded with the provider structs from
/// `sg-extsvc`; malformed data returns `None` because the baseline's
/// `GetPublicExternalAccountData` error is skipped (`continue`). GitHub only
/// exposes `login` in the baseline snapshot (see the overview's erratum), and
/// an empty login is not a handle.
pub fn external_account_login(service_type: &str, account_data: &str) -> Option<String> {
    let login = match service_type {
        "github" => {
            serde_json::from_str::<sg_extsvc::github::User>(account_data)
                .ok()?
                .login
        }
        "gitlab" => {
            serde_json::from_str::<sg_extsvc::gitlab::AuthUser>(account_data)
                .ok()?
                .username
        }
        "bitbucketcloud" => {
            serde_json::from_str::<sg_extsvc::bitbucketcloud::Account>(account_data)
                .ok()?
                .username
        }
        "azuredevops" => serde_json::from_str::<sg_extsvc::azuredevops::Profile>(account_data)
            .ok()?
            .email_address
            .to_lowercase(),
        _ => return None,
    };
    (!login.is_empty()).then_some(login)
}

/// Production ownership source over gitserver + Postgres (existing tables
/// only).
pub struct PgOwnershipSource {
    pool: PgPool,
    objects: Arc<dyn GitObjectSource>,
    /// `user_external_accounts.account_data` decryption key
    /// (`keyring.Default().UserExternalAccountKey`). `None` leaves encrypted
    /// account data unreadable; plaintext/unmigrated rows still resolve.
    key: Option<Arc<dyn Key>>,
}

impl PgOwnershipSource {
    pub fn new(pool: PgPool, objects: Arc<dyn GitObjectSource>) -> Self {
        Self {
            pool,
            objects,
            key: None,
        }
    }

    /// [`Self::new`] plus the keyring key for `user_external_accounts`.
    pub fn with_encryption_key(mut self, key: Option<Arc<dyn Key>>) -> Self {
        self.key = key;
        self
    }

    fn read_codeowners_file(
        &self,
        repo_id: i32,
        repo_name: &str,
        commit: &str,
        path: &str,
    ) -> Result<Option<String>, JobError> {
        match block_on(self.objects.read_file(repo_id, repo_name, commit, path)) {
            Ok(bytes) => Ok(Some(String::from_utf8_lossy(&bytes).into_owned())),
            Err(err) if err.not_exist => Ok(None),
            Err(err) => Err(JobError(err.message)),
        }
    }

    /// `userReferencesBatch.augment`'s `batchFetchCodeHostHandles` +
    /// `linkBack` (`internal/own/ownref.go:378,419,473`): add every code-host
    /// login linked to the user so the same user resolves through any of its
    /// external handles. Per-account decode/decrypt failures are skipped, as
    /// `fetchCodeHostHandles` does.
    fn augment_external_handles(&self, user_id: i32, refs: &mut Vec<OwnershipRef>) {
        let accounts = block_on(sg_db::user_external_accounts::list(
            &self.pool,
            &sg_db::user_external_accounts::ExternalAccountsListOptions {
                user_id: Some(user_id),
                ..Default::default()
            },
        ))
        .unwrap_or_default();
        for account in accounts {
            let Some(cipher) = account.account_data.as_deref() else {
                continue;
            };
            // `extsvc.NewEncryptedData`: empty cipher with empty key id is nil.
            if cipher.is_empty() && account.encryption_key_id.is_empty() {
                continue;
            }
            let Ok(plaintext) = block_on(sg_encryption::maybe_decrypt(
                self.key.as_deref(),
                cipher,
                &account.encryption_key_id,
            )) else {
                continue;
            };
            let Some(handle) =
                external_account_login(&account.service_type, &plaintext)
            else {
                continue;
            };
            refs.push(OwnershipRef {
                user_id: Some(user_id),
                handle,
                ..OwnershipRef::default()
            });
        }
    }
}

impl OwnershipSource for PgOwnershipSource {
    fn assigned_owners(&self, repo_id: i32) -> Result<AssignedOwners, JobError> {
        let rows = block_on(
            sqlx::query(
                "SELECT a.owner_user_id, p.absolute_path \
                 FROM assigned_owners AS a \
                 INNER JOIN repo_paths AS p ON p.id = a.file_path_id \
                 WHERE p.repo_id = $1",
            )
            .bind(repo_id)
            .fetch_all(&self.pool),
        )
        .map_err(|err| JobError(format!("assigned owners: {err}")))?;
        let mut assigned: AssignedOwners = HashMap::new();
        for row in rows {
            let owner_user_id: i32 = row
                .try_get("owner_user_id")
                .map_err(|err| JobError(err.to_string()))?;
            let file_path: String = row
                .try_get("absolute_path")
                .map_err(|err| JobError(err.to_string()))?;
            assigned.entry(file_path).or_default().push(owner_user_id);
        }
        Ok(assigned)
    }

    fn assigned_teams(&self, repo_id: i32) -> Result<AssignedTeams, JobError> {
        let rows = block_on(sg_db::teams::list_assigned_teams_for_repo(&self.pool, repo_id))
            .map_err(JobError)?;
        let mut assigned: AssignedTeams = HashMap::new();
        for row in rows {
            assigned
                .entry(row.file_path)
                .or_default()
                .push(row.owner_team_id);
        }
        Ok(assigned)
    }

    fn codeowners(
        &self,
        repo_id: i32,
        repo_name: &str,
        commit: &str,
    ) -> Result<Option<Ruleset>, JobError> {
        // `RulesetForRepo`: ingested files take priority over the repo copy.
        let ingested: Option<String> = block_on(
            sqlx::query("SELECT contents FROM codeowners WHERE repo_id = $1 LIMIT 1")
                .bind(repo_id)
                .fetch_optional(&self.pool),
        )
        .map_err(|err| JobError(err.to_string()))?
        .map(|row| row.try_get("contents").map_err(|err| JobError(err.to_string())))
        .transpose()?;
        if let Some(contents) = ingested {
            let parsed = parse_codeowners(&contents).map_err(JobError)?;
            return Ok(Some(Ruleset::new(parsed.rules)));
        }
        for path in CODEOWNERS_LOCATIONS {
            if let Some(contents) =
                self.read_codeowners_file(repo_id, repo_name, commit, path)?
            {
                let parsed = parse_codeowners(&contents).map_err(JobError)?;
                return Ok(Some(Ruleset::new(parsed.rules)));
            }
        }
        Ok(None)
    }

    fn repo_code_host_kind(&self, repo_id: i32) -> Result<String, JobError> {
        let kind: Option<String> = block_on(
            sqlx::query("SELECT external_service_type FROM repo WHERE id = $1")
                .bind(repo_id)
                .fetch_optional(&self.pool),
        )
        .map_err(|err| JobError(err.to_string()))?
        .map(|row| {
            row.try_get::<String, _>("external_service_type")
                .map_err(|err| JobError(err.to_string()))
        })
        .transpose()?;
        Ok(kind.unwrap_or_default())
    }

    fn resolve_user(&self, user_id: i32) -> Option<(String, String)> {
        let user = block_on(sg_db::users::get_by_id(&self.pool, user_id)).ok()??;
        let email = block_on(sg_db::user_emails::get_primary_email(&self.pool, user_id))
            .ok()
            .flatten()
            .map(|(email, _verified)| email)
            .unwrap_or_default();
        Some((user.username, email))
    }

    fn resolve_team(&self, team_id: i32) -> Option<String> {
        block_on(sg_db::teams::get_team_by_id(&self.pool, team_id))
            .ok()
            .map(|team| team.name)
    }

    fn resolve_text(&self, text: &str) -> Vec<OwnershipRef> {
        if text.is_empty() {
            return Vec::new();
        }
        let mut bag = Bag::by_text_reference(text);
        let resolved = if is_mail_address(text) {
            block_on(sg_db::users::find_user_by_verified_email(&self.pool, text))
                .ok()
                .flatten()
        } else {
            block_on(sg_db::users::find_user_by_username(
                &self.pool,
                text.trim_start_matches('@'),
            ))
            .ok()
            .flatten()
        };
        let mut refs = Vec::new();
        if let Some(user) = resolved {
            refs.push(OwnershipRef {
                user_id: Some(user.id),
                handle: user.username.clone(),
                email: user.email.clone(),
                ..OwnershipRef::default()
            });
            // `userReferences.augment`/`linkBack`: all verified emails.
            if let Ok(emails) =
                block_on(sg_db::user_emails::list_by_user(&self.pool, user.id, true))
            {
                for email in emails {
                    refs.push(OwnershipRef {
                        user_id: Some(user.id),
                        email: email.email,
                        ..OwnershipRef::default()
                    });
                }
            }
            // `batchFetchCodeHostHandles`: GitHub/GitLab/Bitbucket/Azure
            // logins linked to this user.
            self.augment_external_handles(user.id, &mut refs);
        } else if !is_mail_address(text) {
            let handle = text.trim_start_matches('@');
            if let Ok(team) = block_on(sg_db::teams::get_team_by_name(&self.pool, handle)) {
                refs.push(OwnershipRef {
                    team_id: Some(team.id),
                    handle: team.name.clone(),
                    ..OwnershipRef::default()
                });
            }
        }
        for reference in &refs {
            bag.add_reference(reference);
        }
        refs
    }
}

// ---------------------------------------------------------------------------
// Production contributor seam
// ---------------------------------------------------------------------------

/// `gitserver.Client.ContributorCount` transport. The Rust gitserver client
/// does not expose this gRPC method yet; a transport implementation can be
/// injected from the frontend server.
pub trait ContributorCountSource: Send + Sync {
    fn contributor_count(
        &self,
        repo_name: &str,
        options: &ContributorOptions,
    ) -> Result<Vec<ContributorCount>, JobError>;
}

/// Production `ContributorFetcher` delegating to
/// [`ContributorCountSource`] (`gitserver.Client.ContributorCount`).
pub struct GitserverContributorFetcher {
    source: Arc<dyn ContributorCountSource>,
}

impl GitserverContributorFetcher {
    pub fn new(source: Arc<dyn ContributorCountSource>) -> Self {
        Self { source }
    }
}

impl ContributorFetcher for GitserverContributorFetcher {
    fn contributor_count(
        &self,
        repo_name: &str,
        options: &ContributorOptions,
    ) -> Result<Vec<ContributorCount>, JobError> {
        self.source.contributor_count(repo_name, options)
    }
}

fn go_contributor_options(options: &ContributorOptions) -> sg_git::ContributorOptions {
    sg_git::ContributorOptions {
        range: options.range.clone(),
        // `filter_file_contributor.go:137` leaves `After` at the zero
        // `time.Time`; the client turns `None` into that timestamp.
        after: None,
        path: options.path.clone(),
    }
}

fn contributor_counts(
    counts: Vec<sg_git::ContributorCount>,
) -> Vec<ContributorCount> {
    counts
        .into_iter()
        .map(|count| ContributorCount {
            name: count.name,
            email: count.email,
            count: count.count,
        })
        .collect()
}

/// Synchronous [`ContributorCountSource`] over a shared
/// [`sg_git::GitserverClient`]. The search job tree is synchronous while the
/// gRPC call is async, so the bridge uses the same blocking helper as the
/// ownership DB reads. Errors keep the gitserver status text (no fabrication).
pub struct GitClientContributorCountSource {
    client: Arc<sg_git::GitserverClient>,
}

impl GitClientContributorCountSource {
    pub fn new(client: Arc<sg_git::GitserverClient>) -> Self {
        Self { client }
    }
}

impl ContributorCountSource for GitClientContributorCountSource {
    fn contributor_count(
        &self,
        repo_name: &str,
        options: &ContributorOptions,
    ) -> Result<Vec<ContributorCount>, JobError> {
        let options = go_contributor_options(options);
        block_on(self.client.contributor_count(repo_name, &options))
            .map(contributor_counts)
            .map_err(|error| JobError(error.to_string()))
    }
}

/// Production [`ContributorCountSource`] bound to the live site
/// configuration (`conf.Get()`), like
/// [`crate::git_graphql::production_git_source`]: shard addresses and pins
/// are re-read on every call, and the `sg-git` clients are cached by address
/// list.
pub struct LiveConfigContributorCountSource {
    live: Arc<sg_config::LiveConfig>,
    clients: Mutex<HashMap<Vec<String>, Arc<sg_git::GitserverClient>>>,
}

impl LiveConfigContributorCountSource {
    pub fn new(live: Arc<sg_config::LiveConfig>) -> Self {
        Self {
            live,
            clients: Mutex::new(HashMap::new()),
        }
    }
}

impl ContributorCountSource for LiveConfigContributorCountSource {
    fn contributor_count(
        &self,
        repo_name: &str,
        options: &ContributorOptions,
    ) -> Result<Vec<ContributorCount>, JobError> {
        let unified = self.live.current();
        let addresses = sg_git::addresses::GitserverAddresses::from_unified(&unified);
        let addr = addresses
            .addr_for_repo(repo_name)
            .ok_or_else(|| JobError("no gitserver address configured".to_owned()))?
            .to_owned();
        let client = {
            let mut clients = self.clients.lock().expect("contributor client cache");
            match clients.get(&addresses.addresses) {
                Some(client) => Arc::clone(client),
                None => {
                    let client = Arc::new(sg_git::GitserverClient::new(
                        addresses.addresses.clone(),
                    ));
                    clients.insert(addresses.addresses.clone(), Arc::clone(&client));
                    client
                }
            }
        };
        let options = go_contributor_options(options);
        block_on(client.contributor_count_at(&addr, repo_name, &options))
            .map(contributor_counts)
            .map_err(|error| JobError(error.to_string()))
    }
}

/// The production `file:has.contributor()` fetcher backed by the live
/// configuration.
pub fn production_contributor_fetcher(
    live: Arc<sg_config::LiveConfig>,
) -> Arc<GitserverContributorFetcher> {
    Arc::new(GitserverContributorFetcher::new(Arc::new(
        LiveConfigContributorCountSource::new(live),
    )))
}

/// [`GitObjectSource`] over a shared [`sg_git::GitserverClient`], used by the
/// production ownership source when only the process-wide client is available
/// (the baseline `own.NewService(gitserver, db)` takes the same client).
pub struct GitClientObjectSource {
    client: Arc<sg_git::GitserverClient>,
}

impl GitClientObjectSource {
    pub fn new(client: Arc<sg_git::GitserverClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl GitObjectSource for GitClientObjectSource {
    async fn resolve_revision(
        &self,
        _repo_id: i32,
        repo_name: &str,
        rev: &str,
    ) -> Result<String, GitSourceError> {
        self.client
            .resolve_revision(repo_name, rev, true)
            .await
            .map_err(GitSourceError::from)
    }

    async fn get_commit(
        &self,
        _repo_id: i32,
        repo_name: &str,
        oid: &str,
    ) -> Result<sg_git::CommitDetails, GitSourceError> {
        self.client
            .get_commit_details(repo_name, oid)
            .await
            .map_err(GitSourceError::from)
    }

    async fn get_object(
        &self,
        _repo_id: i32,
        repo_name: &str,
        revspec: &str,
    ) -> Result<sg_git::GitObject, GitSourceError> {
        self.client
            .get_object(repo_name, revspec)
            .await
            .map_err(GitSourceError::from)
    }

    async fn read_file(
        &self,
        _repo_id: i32,
        repo_name: &str,
        oid: &str,
        path: &str,
    ) -> Result<Vec<u8>, GitSourceError> {
        self.client
            .read_file(repo_name, oid, path)
            .await
            .map_err(GitSourceError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct FakeSource {
        codeowners: Option<Ruleset>,
        assigned_owners: AssignedOwners,
        assigned_teams: AssignedTeams,
    }

    impl OwnershipSource for FakeSource {
        fn assigned_owners(&self, _repo_id: i32) -> Result<AssignedOwners, JobError> {
            Ok(self.assigned_owners.clone())
        }
        fn assigned_teams(&self, _repo_id: i32) -> Result<AssignedTeams, JobError> {
            Ok(self.assigned_teams.clone())
        }
        fn codeowners(
            &self,
            _repo_id: i32,
            _repo_name: &str,
            _commit: &str,
        ) -> Result<Option<Ruleset>, JobError> {
            Ok(self.codeowners.clone())
        }
        fn repo_code_host_kind(&self, _repo_id: i32) -> Result<String, JobError> {
            Ok("github".to_owned())
        }
        fn resolve_user(&self, user_id: i32) -> Option<(String, String)> {
            (user_id == 1).then(|| ("alice".to_owned(), "alice@example.com".to_owned()))
        }
        fn resolve_team(&self, team_id: i32) -> Option<String> {
            (team_id == 10).then(|| "owners".to_owned())
        }
        fn resolve_text(&self, text: &str) -> Vec<OwnershipRef> {
            (text.trim_start_matches('@') == "alice").then(|| OwnershipRef {
                user_id: Some(1),
                handle: "alice".to_owned(),
                email: "alice@example.com".to_owned(),
                ..OwnershipRef::default()
            }).into_iter().collect()
        }
    }

    #[test]
    fn rules_cache_fetcher_resolves_codeowners_and_assigned() {
        let rules = parse_codeowners("*.go @bob\n").expect("parse");
        let mut assigned_owners = AssignedOwners::new();
        assigned_owners.insert("src".to_owned(), vec![1]);
        let mut assigned_teams = AssignedTeams::new();
        assigned_teams.insert("".to_owned(), vec![10]);
        let source = Arc::new(FakeSource {
            codeowners: Some(Ruleset::new(rules.rules)),
            assigned_owners,
            assigned_teams,
        });
        let fetcher = RulesCacheOwnershipFetcher::new(source);
        let refs = fetcher
            .owners(7, "repo", "abc", "src/main.go")
            .expect("owners");
        let handles: Vec<&str> = refs.iter().map(|r| r.handle.as_str()).collect();
        assert_eq!(handles, vec!["bob", "alice", "owners"]);
        assert_eq!(refs[1].email, "alice@example.com");
        assert_eq!(refs[0].code_host_kind.as_deref(), Some("github"));
        // `resolve_text` is the bag augmentation used by file:has.owner().
        assert_eq!(fetcher.resolve_text("alice")[0].user_id, Some(1));
    }

    #[test]
    fn assigned_match_inherits_ancestors() {
        let mut owners = AssignedOwners::new();
        owners.insert("".to_owned(), vec![1]);
        owners.insert("src".to_owned(), vec![2]);
        assert_eq!(
            match_assigned(&owners, "src/abc/main.go"),
            vec![2, 1]
        );
        assert_eq!(match_assigned(&owners, ""), vec![1]);
    }

    #[test]
    fn contributor_fetcher_delegates() {
        struct Source;
        impl ContributorCountSource for Source {
            fn contributor_count(
                &self,
                _repo_name: &str,
                options: &ContributorOptions,
            ) -> Result<Vec<ContributorCount>, JobError> {
                Ok(vec![ContributorCount {
                    name: options.path.clone(),
                    email: String::new(),
                    count: 1,
                }])
            }
        }
        let fetcher = GitserverContributorFetcher::new(Arc::new(Source));
        let counts = fetcher
            .contributor_count(
                "repo",
                &ContributorOptions {
                    range: "abc".to_owned(),
                    path: "src/main.go".to_owned(),
                },
            )
            .expect("counts");
        assert_eq!(counts[0].name, "src/main.go");
    }
}
