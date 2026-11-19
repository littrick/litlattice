#![forbid(unsafe_code)]

//! `internal/oobmigration/version.go`: Sourcegraph version parsing and ordering.
//!
//! Only the major/minor components participate in ordering; the optional
//! `+dev` suffix is preserved for equality (matching Go's struct comparison) but
//! ignored by [`compare_versions`].

use std::fmt;

use crate::error::MigratorError;

/// Suffix that marks a development build (`version.go:14`).
pub const DEV_VERSION_FLAG: &str = "+dev";

/// Last minor version of a major release.
///
/// Mirrors `internal/version.LastMinorVersionInMajorRelease`:
/// `3.43.0 -> 4.0.0`, `4.5.0 -> 5.0.0`.
const LAST_MINOR_VERSION_IN_MAJOR_RELEASE: &[(i32, i32)] = &[(3, 43), (4, 5)];

/// A Sourcegraph version (`oobmigration.Version`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Version {
    pub major: i32,
    pub minor: i32,
    /// Indicates whether the version string carried the `+dev` flag.
    pub dev: bool,
}

impl Version {
    pub const fn new(major: i32, minor: i32) -> Self {
        Self {
            major,
            minor,
            dev: false,
        }
    }

    pub const fn new_dev(major: i32, minor: i32) -> Self {
        Self {
            major,
            minor,
            dev: true,
        }
    }

    /// `NewVersionFromString` (`version.go:41`).
    pub fn from_string(v: &str) -> Option<Self> {
        Self::from_string_and_patch(v).map(|(version, _)| version)
    }

    /// `NewVersionAndPatchFromString` (`version.go:50`).
    ///
    /// Parses the leading `major.minor[.patch]` of a version string, accepting
    /// an optional leading `v`, an optional prerelease suffix and optional build
    /// metadata. Returns `None` if the string does not look like a version.
    pub fn from_string_and_patch(input: &str) -> Option<(Self, i32)> {
        let mut value = input.trim();

        let mut dev = false;
        if let Some(rest) = value.strip_suffix(DEV_VERSION_FLAG) {
            dev = true;
            value = rest;
        }

        if let Some(rest) = value.strip_prefix('v') {
            value = rest;
        }

        // Drop build metadata, then any prerelease suffix. Order mirrors the
        // Go regular expression `(?:-\w*)?(?:\+[\w.]*)?$`.
        if let Some(index) = value.find('+') {
            value = &value[..index];
        }
        if let Some(index) = value.find('-') {
            value = &value[..index];
        }

        let parts: Vec<&str> = value.split('.').collect();
        if parts.len() < 2 || parts.len() > 3 {
            return None;
        }

        let major: i32 = parts[0].parse().ok()?;
        let minor: i32 = parts[1].parse().ok()?;
        let patch: i32 = if parts.len() == 3 {
            parts[2].parse().ok()?
        } else {
            0
        };

        Some((Version { major, minor, dev }, patch))
    }

    /// `Version.String` (`version.go:73`) returns `major.minor`.
    pub fn as_version_string(&self) -> String {
        format!("{}.{}", self.major, self.minor)
    }

    /// `Version.GitTag` (`version.go:77`).
    pub fn git_tag(&self) -> String {
        self.git_tag_with_patch(0)
    }

    /// `Version.GitTagWithPatch` (`version.go:81`).
    pub fn git_tag_with_patch(&self, patch: i32) -> String {
        format!("v{}.{}.{}", self.major, self.minor, patch)
    }

    /// `Version.Next` (`version.go:85`).
    pub fn next(&self) -> Self {
        if last_minor_version_in_major_release(self.major) == Some(self.minor) {
            Self::new(self.major + 1, 0)
        } else {
            Self::new(self.major, self.minor + 1)
        }
    }

    /// `Version.Previous` (`version.go:98`).
    pub fn previous(&self) -> Option<Self> {
        if self.minor == 0 {
            last_minor_version_in_major_release(self.major - 1)
                .map(|minor| Self::new(self.major - 1, minor))
        } else {
            Some(Self::new(self.major, self.minor - 1))
        }
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

fn last_minor_version_in_major_release(major: i32) -> Option<i32> {
    LAST_MINOR_VERSION_IN_MAJOR_RELEASE
        .iter()
        .find_map(|&(candidate, minor)| (candidate == major).then_some(minor))
}

/// Relationship between two versions (`version.go:123`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionOrder {
    Before = 0,
    Equal = 1,
    After = 2,
}

/// `CompareVersions` (`version.go:134`).
pub fn compare_versions(a: Version, b: Version) -> VersionOrder {
    for (left, right) in [(a.major, b.major), (a.minor, b.minor)] {
        if left < right {
            return VersionOrder::Before;
        }
        if left > right {
            return VersionOrder::After;
        }
    }

    VersionOrder::Equal
}

/// `UpgradeRange` (`version.go:110`): every minor version in `[from, to]`.
pub fn upgrade_range(from: Version, to: Version) -> Result<Vec<Version>, MigratorError> {
    if compare_versions(from, to) != VersionOrder::Before {
        return Err(MigratorError::new(format!(
            "invalid range (from={} >= to={})",
            from, to
        )));
    }

    let mut versions = Vec::new();
    let mut current = from;
    while compare_versions(current, to) != VersionOrder::After {
        versions.push(current);
        current = current.next();
    }

    Ok(versions)
}

/// `SortVersions` (`version.go:151`): ascending by major then minor.
pub fn sort_versions(versions: &mut [Version]) {
    versions.sort_by(|a, b| a.major.cmp(&b.major).then_with(|| a.minor.cmp(&b.minor)));
}

/// `pointIntersectsInterval` (`version.go:162`).
pub fn point_intersects_interval(lower: Version, upper: Version, point: Version) -> bool {
    compare_versions(point, lower) != VersionOrder::Before
        && compare_versions(upper, point) != VersionOrder::Before
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_version_strings() {
        // (input, expected version, expected patch, expected ok)
        let cases = [
            ("3.50", Version::new(3, 50), 0, true),
            ("v3.50.3", Version::new(3, 50), 3, true),
            ("v3.50", Version::new(3, 50), 0, true),
            ("3.50.3", Version::new(3, 50), 3, true),
            ("3.50.3+dev", Version::new_dev(3, 50), 3, true),
            ("350", Version::new(0, 0), 0, false),
            ("350+dev", Version::new(0, 0), 0, false),
            ("2023.03.23+204874.db2922", Version::new(2023, 3), 23, true),
            (
                "2023.03.23-insiders+204874.db2922",
                Version::new(2023, 3),
                23,
                true,
            ),
        ];

        for (input, expected_version, expected_patch, expected_ok) in cases {
            let parsed = Version::from_string_and_patch(input);
            match parsed {
                Some((version, patch)) => {
                    assert!(expected_ok, "expected {input:?} to be rejected");
                    assert_eq!(version, expected_version, "version for {input:?}");
                    assert_eq!(patch, expected_patch, "patch for {input:?}");
                }
                None => assert!(!expected_ok, "expected {input:?} to parse"),
            }
        }
    }

    #[test]
    fn compares_versions() {
        let cases = [
            (
                Version::new(3, 12),
                Version::new(3, 12),
                VersionOrder::Equal,
            ),
            (
                Version::new(3, 11),
                Version::new(3, 12),
                VersionOrder::Before,
            ),
            (
                Version::new(3, 12),
                Version::new(3, 11),
                VersionOrder::After,
            ),
            (
                Version::new(3, 12),
                Version::new(4, 11),
                VersionOrder::Before,
            ),
            (
                Version::new(4, 11),
                Version::new(3, 12),
                VersionOrder::After,
            ),
        ];

        for (left, right, expected) in cases {
            assert_eq!(compare_versions(left, right), expected);
        }
    }

    #[test]
    fn computes_upgrade_ranges() {
        assert!(upgrade_range(Version::new(3, 12), Version::new(3, 10)).is_err());
        assert!(upgrade_range(Version::new(3, 12), Version::new(3, 12)).is_err());

        assert_eq!(
            upgrade_range(Version::new(3, 12), Version::new(3, 13)).unwrap(),
            vec![Version::new(3, 12), Version::new(3, 13)]
        );
        assert_eq!(
            upgrade_range(Version::new(3, 42), Version::new(4, 2)).unwrap(),
            vec![
                Version::new(3, 42),
                Version::new(3, 43),
                Version::new(4, 0),
                Version::new(4, 1),
                Version::new(4, 2),
            ]
        );
    }

    #[test]
    fn computes_next_and_previous() {
        let chain = [
            Version::new(3, 41),
            Version::new(3, 42),
            Version::new(3, 43),
            Version::new(4, 0),
            Version::new(4, 1),
            Version::new(4, 2),
        ];

        for (index, version) in chain.iter().enumerate() {
            if index != 0 {
                let previous = version.previous().expect("previous version");
                assert_eq!(
                    previous.as_version_string(),
                    chain[index - 1].as_version_string()
                );
            }
            if index + 1 < chain.len() {
                assert_eq!(
                    version.next().as_version_string(),
                    chain[index + 1].as_version_string()
                );
            }
        }
    }
}
