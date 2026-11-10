#![forbid(unsafe_code)]

//! Zoekt horizontal-sharding endpoint selection
//! (`internal/search/backend/indexers.go:1-145`).
//!
//! The baseline `Indexers` consults a consistent-hash [`EndpointMap`] to decide
//! which indexer endpoint owns a repository and which indexed repositories a
//! hostname may keep during a rebalance. This port keeps the same public shape
//! but replaces the `internal/types.MinimalRepo`, `zoekt.ReposMap` and
//! `context.Context` dependencies with minimal, injectable equivalents:
//!
//! - [`MinimalRepo`] is `{id, name}`; `id` is the baseline's `api.RepoID`
//!   (`int32`) and is cast with `as u32` exactly like `uint32(r.ID)`.
//! - [`ReposMap`] is just the key set. `zoekt.ReposMap` is
//!   `map[uint32]MinimalRepoListEntry` and `ReposSubset` only tests membership.
//! - `Indexed` loses its `ctx` argument: the baseline never reads it, it only
//!   passes it through.
//!
//! Deliberate difference from the Go source (documented in the M9.8gu
//! overview): the `other` rebalance group is accumulated in a [`BTreeMap`],
//! so its ordering is deterministic (sorted by assigned endpoint). Go ranges
//! over a plain map, whose order is unspecified, so this is one legal order.

use std::collections::{BTreeMap, HashSet};
use std::fmt;

/// The subset of `zoekt.ReposMap` used by [`Indexers::repos_subset`]: a set of
/// repository IDs (the baseline casts `api.RepoID` with `uint32(r.ID)`).
pub type ReposMap = HashSet<u32>;

/// `types.MinimalRepo` reduced to the fields horizontal sharding reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinimalRepo {
    /// `api.RepoID` (`int32`).
    pub id: i32,
    /// `api.RepoName`.
    pub name: String,
}

/// The subset of `endpoint.Map` (a consistent hashmap) `Indexers` uses.
/// Declared as a trait so differential tests can inject a fake.
pub trait EndpointMap: Send + Sync {
    /// `Endpoints`: all addresses which can be searched.
    fn endpoints(&self) -> Result<Vec<String>, IndexersError>;
    /// `Get`: the endpoint assigned to `key` by consistent hashing.
    fn get(&self, key: &str) -> Result<String, IndexersError>;
}

/// A formatted error carrying the baseline's message text. Both
/// [`find_endpoint`] and map failures surface through it; the differential
/// harness compares [`Display`] output directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexersError(pub String);

impl IndexersError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for IndexersError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for IndexersError {}

/// `Indexers`: methods over the set of indexed-search servers in a cluster.
pub struct Indexers {
    /// `Map`: the desired repository-name to endpoint mapping. `None` is the
    /// baseline's `nil` map, meaning horizontal sharding is disabled.
    pub map: Option<Box<dyn EndpointMap>>,
    /// `Indexed`: the repositories currently indexed on an endpoint. The
    /// baseline expects an empty set when it fails.
    pub indexed: Box<dyn Fn(&str) -> ReposMap + Send + Sync>,
}

impl Indexers {
    /// `Enabled`: true when a map is configured.
    pub fn enabled(&self) -> bool {
        self.map.is_some()
    }

    /// `ReposSubset`: the subset of `repos` that `hostname` should index.
    ///
    /// `indexed` is the set of repositories currently indexed by `hostname`
    /// (the baseline's `zoekt.ReposMap` for this host). An error is returned
    /// if `hostname` is not one of the map's endpoints, or if the map fails.
    pub fn repos_subset(
        &self,
        hostname: &str,
        indexed: &ReposMap,
        repos: &[MinimalRepo],
    ) -> Result<Vec<MinimalRepo>, IndexersError> {
        let Some(map) = self.map.as_ref() else {
            // Disabled: single-shard behaviour.
            return Ok(repos.to_vec());
        };

        let endpoints = map.endpoints()?;
        let endpoint = find_endpoint(&endpoints, hostname)?;

        // Rebalancing: `other` holds repositories this endpoint currently
        // indexes but should drop. They are only dropped when the assigned
        // endpoint has indexed them, so a computed index is not lost while a
        // rebalance is in flight. The baseline reuses `repos`' backing array;
        // the order of the retained entries is identical here.
        let mut other: BTreeMap<String, Vec<MinimalRepo>> = BTreeMap::new();

        let mut subset: Vec<MinimalRepo> = Vec::with_capacity(repos.len());
        for repo in repos {
            let assigned = map.get(&repo.name)?;

            if assigned == endpoint {
                subset.push(repo.clone());
            } else if indexed.contains(&(repo.id as u32)) {
                other.entry(assigned).or_default().push(repo.clone());
            }
        }

        // Only include repositories from `other` if the assigned endpoint has
        // not yet indexed them.
        for (assigned, other_repos) in other {
            let drop = (self.indexed)(&assigned);
            for repo in other_repos {
                if !drop.contains(&(repo.id as u32)) {
                    subset.push(repo);
                }
            }
        }

        Ok(subset)
    }
}

/// `findEndpoint`: the endpoint in `endpoints` matching `hostname`.
///
/// The hostname may be less qualified than the endpoint (k8s pod name vs.
/// service address) and an endpoint may carry a port the hostname lacks. The
/// `.`/`:` boundary check keeps those loose matches unambiguous, and more than
/// one match is an error.
pub fn find_endpoint(endpoints: &[String], hostname: &str) -> Result<String, IndexersError> {
    let host = hostname.as_bytes();
    // Mirrors Go's `endpoint := ""` plus `if endpoint != ""` duplicate check:
    // an empty endpoint counts as "not found", not as a match.
    let mut endpoint = "";

    for ep in endpoints {
        let ep_bytes = ep.as_bytes();
        if !ep_bytes.starts_with(host) {
            continue;
        }
        if host.len() < ep_bytes.len() {
            let boundary = ep_bytes[host.len()];
            if boundary != b'.' && boundary != b':' {
                continue;
            }
        }

        if !endpoint.is_empty() {
            return Err(IndexersError(format!(
                "hostname {hostname:?} matches multiple in {}",
                endpoints_string(endpoints)
            )));
        }
        endpoint = ep;
    }

    if !endpoint.is_empty() {
        return Ok(endpoint.to_owned());
    }

    Err(IndexersError(format!(
        "hostname {hostname:?} not found in {}",
        endpoints_string(endpoints)
    )))
}

/// `endpointsString`: a human readable rendering, e.g.
/// `Endpoints{"foo" "bar"}` (space separated, no comma).
pub fn endpoints_string(endpoints: &[String]) -> String {
    let mut rendered = String::from("Endpoints{");
    for (i, ep) in endpoints.iter().enumerate() {
        if i != 0 {
            rendered.push(' ');
        }
        rendered.push_str(&format!("{ep:?}"));
    }
    rendered.push('}');
    rendered
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct FakeMap {
        endpoints: Vec<String>,
        assignments: BTreeMap<String, String>,
        get_error: Option<String>,
    }

    impl EndpointMap for FakeMap {
        fn endpoints(&self) -> Result<Vec<String>, IndexersError> {
            Ok(self.endpoints.clone())
        }

        fn get(&self, key: &str) -> Result<String, IndexersError> {
            if let Some(message) = &self.get_error {
                return Err(IndexersError::new(message.clone()));
            }
            Ok(self.assignments.get(key).cloned().unwrap_or_default())
        }
    }

    fn endpoints(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    fn repo(id: i32, name: &str) -> MinimalRepo {
        MinimalRepo {
            id,
            name: name.to_owned(),
        }
    }

    #[test]
    fn find_endpoint_matches_exact_prefix_and_port() {
        assert_eq!(
            find_endpoint(&endpoints(&["foo", "bar"]), "foo").unwrap(),
            "foo"
        );
        assert_eq!(
            find_endpoint(&endpoints(&["foo.internal", "bar.internal"]), "foo").unwrap(),
            "foo.internal"
        );
        assert_eq!(
            find_endpoint(&endpoints(&["foo.internal:80", "bar.internal:80"]), "foo").unwrap(),
            "foo.internal:80"
        );
        assert_eq!(
            find_endpoint(
                &endpoints(&["foo.internal:80", "bar.internal:80"]),
                "foo.internal"
            )
            .unwrap(),
            "foo.internal:80"
        );
    }

    #[test]
    fn find_endpoint_rejects_bad_prefixes() {
        assert!(find_endpoint(&endpoints(&["foobar", "barfoo"]), "foo").is_err());
        assert_eq!(
            find_endpoint(&endpoints(&["foo.internal", "foobar.internal"]), "foo").unwrap(),
            "foo.internal"
        );
    }

    #[test]
    fn find_endpoint_reports_multiple_and_missing() {
        let error =
            find_endpoint(&endpoints(&["foo.internal", "foo.external"]), "foo").unwrap_err();
        assert_eq!(
            error.to_string(),
            "hostname \"foo\" matches multiple in Endpoints{\"foo.internal\" \"foo.external\"}"
        );

        let error = find_endpoint(&[], "foo").unwrap_err();
        assert_eq!(
            error.to_string(),
            "hostname \"foo\" not found in Endpoints{}"
        );
    }

    #[test]
    fn find_endpoint_is_case_sensitive() {
        assert!(find_endpoint(&endpoints(&["foo.internal"]), "Foo").is_err());
    }

    #[test]
    fn find_endpoint_empty_hostname_never_matches() {
        assert!(find_endpoint(&endpoints(&["foo.internal"]), "").is_err());
        // An empty endpoint also does not count as a match.
        assert!(find_endpoint(&endpoints(&[""]), "").is_err());
    }

    #[test]
    fn endpoints_string_has_no_commas() {
        assert_eq!(endpoints_string(&[]), "Endpoints{}");
        assert_eq!(endpoints_string(&endpoints(&["foo"])), "Endpoints{\"foo\"}");
        assert_eq!(
            endpoints_string(&endpoints(&["a", "b"])),
            "Endpoints{\"a\" \"b\"}"
        );
    }

    fn indexers(
        assignments: &[(&str, &str)],
        indexed: &[(&str, &[i32])],
        calls: &Arc<Mutex<Vec<String>>>,
    ) -> Indexers {
        let mut map = FakeMap {
            endpoints: endpoints(&["foo", "bar", "baz.fully.qualified:80"]),
            assignments: BTreeMap::new(),
            get_error: None,
        };
        for (name, endpoint) in assignments {
            map.assignments
                .insert((*name).to_owned(), (*endpoint).to_owned());
        }
        let indexed: BTreeMap<String, Vec<i32>> = indexed
            .iter()
            .map(|(endpoint, ids)| ((*endpoint).to_owned(), ids.to_vec()))
            .collect();

        let calls = Arc::clone(calls);
        Indexers {
            map: Some(Box::new(map)),
            indexed: Box::new(move |endpoint: &str| {
                calls.lock().unwrap().push(endpoint.to_owned());
                indexed
                    .get(endpoint)
                    .map(|ids| ids.iter().map(|id| *id as u32).collect())
                    .unwrap_or_default()
            }),
        }
    }

    #[test]
    fn repos_subset_is_passthrough_when_disabled() {
        let indexers = Indexers {
            map: None,
            indexed: Box::new(|_: &str| ReposMap::new()),
        };
        let repos = vec![repo(1, "a"), repo(2, "b")];
        let subset = indexers
            .repos_subset("anything", &ReposMap::new(), &repos)
            .unwrap();
        assert_eq!(subset, repos);
    }

    #[test]
    fn repos_subset_filters_by_assigned_endpoint_in_input_order() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let indexers = indexers(
            &[
                ("foo-2", "foo"),
                ("bar-1", "bar"),
                ("foo-1", "foo"),
                ("foo-3", "foo"),
            ],
            &[],
            &calls,
        );
        let repos = vec![
            repo(1, "foo-2"),
            repo(2, "bar-1"),
            repo(3, "foo-1"),
            repo(4, "foo-3"),
        ];
        let subset = indexers
            .repos_subset("foo", &ReposMap::new(), &repos)
            .unwrap();
        let names: Vec<&str> = subset.iter().map(|repo| repo.name.as_str()).collect();
        assert_eq!(names, ["foo-2", "foo-1", "foo-3"]);
    }

    #[test]
    fn repos_subset_rebalances_other_endpoints_only_when_unindexed() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let indexers = indexers(
            &[
                ("foo-1", "foo"),
                ("foo-2", "foo"),
                ("foo-3", "foo"),
                ("bar-drop", "bar"),
                ("bar-keep", "bar"),
            ],
            &[("bar", &[3, 5])],
            &calls,
        );
        // `indexed` for hostname "foo": foo-1, foo-drop, bar-drop, bar-keep.
        let indexed: ReposMap = [1u32, 4, 5, 6].into_iter().collect();
        let repos = vec![
            repo(1, "foo-1"),
            repo(2, "foo-2"),
            repo(3, "foo-3"),
            repo(5, "bar-drop"),
            repo(6, "bar-keep"),
        ];
        let subset = indexers.repos_subset("foo", &indexed, &repos).unwrap();
        let names: Vec<&str> = subset.iter().map(|repo| repo.name.as_str()).collect();
        assert_eq!(names, ["foo-1", "foo-2", "foo-3", "bar-keep"]);
        assert_eq!(*calls.lock().unwrap(), ["bar"]);
    }

    #[test]
    fn repos_subset_drops_unassigned_and_unindexed_repositories() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let indexers = indexers(&[("bar-1", "bar")], &[], &calls);
        let repos = vec![repo(2, "bar-1")];
        // Not assigned to "foo" and not currently indexed by "foo".
        let subset = indexers
            .repos_subset("foo", &ReposMap::new(), &repos)
            .unwrap();
        assert!(subset.is_empty());
        assert!(calls.lock().unwrap().is_empty());
    }

    #[test]
    fn repos_subset_propagates_errors() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let indexers = indexers(&[("foo-1", "foo")], &[], &calls);
        let repos = vec![repo(1, "foo-1")];
        let error = indexers
            .repos_subset("bam", &ReposMap::new(), &repos)
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "hostname \"bam\" not found in Endpoints{\"foo\" \"bar\" \"baz.fully.qualified:80\"}"
        );
    }
}
