#![forbid(unsafe_code)]

//! `internal/codeintel/autoindexing/internal/inference/luatypes/path_patterns.go`
//! and the path-filtering half of
//! `internal/codeintel/autoindexing/internal/inference/util.go`.
//!
//! A [`PathPattern`] is a tree of glob/pathspec patterns with an `invert`
//! flag. [`flatten_patterns`] collects the nodes whose `invert` matches the
//! requested polarity (a node with an empty glob contributes nothing; its
//! children are still visited). The inference service uses the non-inverted
//! flattening as "include" patterns and the inverted one as "exclude"
//! patterns.
//!
//! [`filter_paths_by_patterns`] is `filterPathsByPatterns`: compile both
//! polarities (sorted and de-duplicated), then keep the paths that match an
//! include glob and no exclude glob. [`filter_paths`] returns an empty list
//! when there are no include patterns, and matches against absolute paths
//! (`/` is prepended to relative paths).
//!
//! The Lua-facing decoding (`PathPatternsFromUserData`) is not ported yet;
//! patterns are constructed with [`PathPattern::new`],
//! [`PathPattern::combined`] and [`PathPattern::exclude`].

use crate::autoindexing_runtime::paths::{compile, GlobPattern};

/// `luatypes.GlobAndPathspecPattern`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GlobAndPathspecPattern {
    pub glob: String,
    pub pathspecs: Vec<String>,
}

/// `luatypes.PathPattern`.
#[derive(Debug, Clone)]
pub struct PathPattern {
    pattern: GlobAndPathspecPattern,
    children: Vec<PathPattern>,
    invert: bool,
}

impl PathPattern {
    /// `luatypes.NewPattern`.
    pub fn new(glob: impl Into<String>, pathspecs: Vec<String>) -> Self {
        Self {
            pattern: GlobAndPathspecPattern {
                glob: glob.into(),
                pathspecs,
            },
            children: Vec::new(),
            invert: false,
        }
    }

    /// `luatypes.NewCombinedPattern`: a path must match one of the children.
    pub fn combined(children: Vec<PathPattern>) -> Self {
        Self {
            pattern: GlobAndPathspecPattern::default(),
            children,
            invert: false,
        }
    }

    /// `luatypes.NewExcludePattern`: matching paths are removed.
    pub fn exclude(children: Vec<PathPattern>) -> Self {
        Self {
            pattern: GlobAndPathspecPattern::default(),
            children,
            invert: true,
        }
    }

    fn flatten_into(&self, inverted: bool, out: &mut Vec<GlobAndPathspecPattern>) {
        if self.invert == inverted {
            if !self.pattern.glob.is_empty() {
                out.push(self.pattern.clone());
            }
            for child in &self.children {
                child.flatten_into(inverted, out);
            }
        }
    }
}

/// `luatypes.FlattenPatterns`.
pub fn flatten_patterns(
    path_patterns: &[PathPattern],
    inverted: bool,
) -> Vec<GlobAndPathspecPattern> {
    let mut out = Vec::new();
    for path_pattern in path_patterns {
        path_pattern.flatten_into(inverted, &mut out);
    }
    out
}

/// The globs and pathspecs produced by `flattenPatterns`.
#[derive(Debug, Clone, Default)]
pub struct CompiledPatterns {
    pub globs: Vec<GlobPattern>,
    pub pathspecs: Vec<String>,
}

/// `flattenPatterns`: flatten one polarity, normalise the strings, compile the
/// globs.
pub fn compile_patterns(
    patterns: &[PathPattern],
    inverted: bool,
) -> Result<CompiledPatterns, String> {
    let flattened = flatten_patterns(patterns, inverted);
    let glob_patterns = normalize_patterns(flattened.iter().map(|p| p.glob.clone()).collect());
    let pathspec_patterns = normalize_patterns(
        flattened
            .iter()
            .flat_map(|p| p.pathspecs.iter().cloned())
            .collect(),
    );

    let mut globs = Vec::with_capacity(glob_patterns.len());
    for glob in glob_patterns {
        globs.push(compile(&glob)?);
    }
    Ok(CompiledPatterns {
        globs,
        pathspecs: pathspec_patterns,
    })
}

/// `filterPathsByPatterns`.
pub fn filter_paths_by_patterns(
    paths: &[String],
    raw_patterns: &[PathPattern],
) -> Result<Vec<String>, String> {
    let include = compile_patterns(raw_patterns, false)?;
    let exclude = compile_patterns(raw_patterns, true)?;
    Ok(filter_paths(paths, &include.globs, &exclude.globs))
}

/// `filterPaths`: keep paths matching an include glob and no exclude glob.
pub fn filter_paths(
    paths: &[String],
    patterns: &[GlobPattern],
    inverted_patterns: &[GlobPattern],
) -> Vec<String> {
    if patterns.is_empty() {
        return Vec::new();
    }

    paths
        .iter()
        .filter(|path| filter_path(path, patterns, inverted_patterns))
        .cloned()
        .collect()
}

fn filter_path(path: &str, patterns: &[GlobPattern], inverted: &[GlobPattern]) -> bool {
    let absolute;
    let path = if path.starts_with('/') {
        path
    } else {
        absolute = format!("/{path}");
        &absolute
    };

    for pattern in patterns {
        if pattern.match_path(path) {
            return inverted.is_empty() || !filter_path(path, inverted, &[]);
        }
    }
    false
}

/// `normalizePatterns`: sort and remove duplicates.
fn normalize_patterns(mut patterns: Vec<String>) -> Vec<String> {
    patterns.sort();
    patterns.dedup();
    patterns
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(value: &str) -> String {
        value.to_owned()
    }

    #[test]
    fn flattening_respects_polarity_and_skips_empty_globs() {
        // Flattening never descends into a node of the other polarity, so an
        // exclude pattern nested inside a combined node is invisible.
        let tree = PathPattern::combined(vec![
            PathPattern::new("package.json", vec!["package.json".to_owned()]),
            PathPattern::new("", vec!["ignored.pathspec".to_owned()]),
        ]);
        let exclude = PathPattern::exclude(vec![PathPattern::new("node_modules/**", vec![])]);

        assert_eq!(
            flatten_patterns(std::slice::from_ref(&tree), false),
            vec![GlobAndPathspecPattern {
                glob: "package.json".to_owned(),
                pathspecs: vec!["package.json".to_owned()],
            }]
        );
        // Erratum: the baseline only descends into nodes whose `invert`
        // equals the requested polarity, and `NewExcludePattern`'s children
        // are plain nodes, so exclude patterns flatten to nothing.
        assert_eq!(
            flatten_patterns(std::slice::from_ref(&exclude), true),
            Vec::new()
        );
    }

    #[test]
    fn filtering_uses_include_and_exclude_patterns() {
        let patterns = vec![
            PathPattern::combined(vec![PathPattern::new("**/package.json", vec![])]),
            PathPattern::exclude(vec![PathPattern::new("vendor/**", vec![])]),
        ];
        let paths = vec![
            path("a/package.json"),
            path("vendor/b/package.json"),
            path("c/README.md"),
        ];

        // The exclude pattern has no effect (see the flattening erratum).
        assert_eq!(
            filter_paths_by_patterns(&paths, &patterns).expect("filter"),
            vec![path("a/package.json"), path("vendor/b/package.json")]
        );
    }

    #[test]
    fn no_include_patterns_match_nothing() {
        let patterns = vec![PathPattern::exclude(vec![PathPattern::new(
            "vendor/**",
            vec![],
        )])];
        assert!(filter_paths_by_patterns(&[path("a.txt")], &patterns)
            .expect("filter")
            .is_empty());
    }
}
