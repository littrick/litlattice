#![forbid(unsafe_code)]

//! `internal/oobmigration/{interrupts,upgrade,downgrade}.go`: compute the
//! versions at which a multi-version upgrade/downgrade must wait for
//! out-of-band migrations to finish.

use std::collections::BTreeMap;

use crate::error::MigratorError;

use super::metadata::{self, YamlMigration};
use super::version::{compare_versions, point_intersects_interval, Version, VersionOrder};

/// A point in an upgrade/downgrade that has out-of-band completion
/// requirements (`interrupts.go:3`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationInterrupt {
    pub version: Version,
    pub migration_ids: Vec<i32>,
}

#[derive(Debug, Clone, Copy)]
struct MigrationInterval {
    id: i32,
    introduced: Version,
    deprecated: Version,
}

/// `ScheduleMigrationInterrupts` (`interrupts.go:13`) over the canonical
/// definitions.
pub fn schedule_migration_interrupts(
    from: Version,
    to: Version,
) -> Result<Vec<MigrationInterrupt>, MigratorError> {
    schedule_migration_interrupts_with(from, to, metadata::MIGRATIONS)
}

/// Testable form of [`schedule_migration_interrupts`] taking explicit
/// definitions.
pub fn schedule_migration_interrupts_with(
    from: Version,
    to: Version,
    migrations: &[YamlMigration],
) -> Result<Vec<MigrationInterrupt>, MigratorError> {
    match compare_versions(from, to) {
        VersionOrder::Before => schedule_upgrade(from, to, migrations),
        VersionOrder::After => schedule_downgrade(from, to, migrations),
        VersionOrder::Equal => Ok(Vec::new()),
    }
}

/// `scheduleUpgrade` (`upgrade.go:9`).
pub fn schedule_upgrade(
    from: Version,
    to: Version,
    migrations: &[YamlMigration],
) -> Result<Vec<MigrationInterrupt>, MigratorError> {
    let mut intervals = Vec::new();
    for migration in migrations {
        let (Some(deprecated_major), Some(deprecated_minor)) = (
            migration.deprecated_version_major,
            migration.deprecated_version_minor,
        ) else {
            continue;
        };

        let interval = MigrationInterval {
            id: migration.id,
            introduced: migration.introduced(),
            deprecated: Version::new(deprecated_major, deprecated_minor),
        };

        // Only intervals deprecated within the migration range:
        // `from < deprecated <= to`.
        if compare_versions(from, interval.deprecated) == VersionOrder::Before
            && compare_versions(interval.deprecated, to) != VersionOrder::After
        {
            intervals.push(interval);
        }
    }

    // Greedy single scan: order by increasing upper bound, then pick the
    // version immediately before each uncovered interval's deprecation.
    intervals.sort_by(|a, b| order(compare_versions(a.deprecated, b.deprecated)));

    let mut points: Vec<Version> = Vec::new();
    for interval in &intervals {
        if points.is_empty()
            || compare_versions(*points.last().expect("non-empty"), interval.introduced)
                == VersionOrder::Before
        {
            let previous = interval.deprecated.previous().ok_or_else(|| {
                MigratorError::new(format!(
                    "cannot determine version prior to {}",
                    interval.deprecated
                ))
            })?;
            points.push(previous);
        }
    }

    let mut interrupts = make_covering_set(&intervals, points);

    // Sort ascending.
    interrupts.sort_by(|a, b| order(compare_versions(a.version, b.version)));
    Ok(interrupts)
}

/// `scheduleDowngrade` (`downgrade.go:7`).
pub fn schedule_downgrade(
    from: Version,
    to: Version,
    migrations: &[YamlMigration],
) -> Result<Vec<MigrationInterrupt>, MigratorError> {
    let mut intervals = Vec::new();
    for migration in migrations {
        // Undeprecated migrations are assumed to be deprecated right after the
        // target version, matching downgrade.go:15.
        let deprecated = migration.deprecated().unwrap_or_else(|| to.next());

        let interval = MigrationInterval {
            id: migration.id,
            introduced: migration.introduced(),
            deprecated,
        };

        // Only intervals introduced within the migration range:
        // `to <= introduced < from`.
        if compare_versions(to, interval.introduced) != VersionOrder::After
            && compare_versions(interval.introduced, from) == VersionOrder::Before
        {
            intervals.push(interval);
        }
    }

    // Order intervals by decreasing lower bound.
    intervals.sort_by(|a, b| order(compare_versions(b.introduced, a.introduced)));

    let mut points: Vec<Version> = Vec::new();
    for interval in &intervals {
        if points.is_empty()
            || compare_versions(interval.deprecated, *points.last().expect("non-empty"))
                != VersionOrder::After
        {
            points.push(interval.introduced);
        }
    }

    let mut interrupts = make_covering_set(&intervals, points);

    // Sort descending.
    interrupts.sort_by(|a, b| order(compare_versions(b.version, a.version)));
    Ok(interrupts)
}

/// `makeCoveringSet` (`upgrade.go:80`).
fn make_covering_set(
    intervals: &[MigrationInterval],
    mut points: Vec<Version>,
) -> Vec<MigrationInterrupt> {
    // Delay migration runs as late as possible.
    points.reverse();

    let mut covering: BTreeMap<(i32, i32), (Version, Vec<i32>)> = BTreeMap::new();

    'outer: for interval in intervals {
        for point in &points {
            if point_intersects_interval(interval.introduced, interval.deprecated, *point) {
                covering
                    .entry((point.major, point.minor))
                    .or_insert_with(|| (*point, Vec::new()))
                    .1
                    .push(interval.id);
                continue 'outer;
            }
        }

        panic!("unreachable: input interval not covered in output");
    }

    covering
        .into_values()
        .map(|(version, mut migration_ids)| {
            migration_ids.sort_unstable();
            MigrationInterrupt {
                version,
                migration_ids,
            }
        })
        .collect()
}

fn order(order: VersionOrder) -> std::cmp::Ordering {
    match order {
        VersionOrder::Before => std::cmp::Ordering::Less,
        VersionOrder::Equal => std::cmp::Ordering::Equal,
        VersionOrder::After => std::cmp::Ordering::Greater,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_migration(
        id: i32,
        i_major: i32,
        i_minor: i32,
        d_major: i32,
        d_minor: i32,
    ) -> YamlMigration {
        YamlMigration {
            id,
            team: "",
            component: "",
            description: "",
            non_destructive: true,
            is_enterprise: true,
            introduced_version_major: i_major,
            introduced_version_minor: i_minor,
            deprecated_version_major: Some(d_major),
            deprecated_version_minor: Some(d_minor),
        }
    }

    #[test]
    fn schedules_upgrades() {
        let cases: Vec<(
            &str,
            Version,
            Version,
            Vec<YamlMigration>,
            Vec<MigrationInterrupt>,
        )> = vec![
            (
                "empty",
                Version::new(3, 32),
                Version::new(3, 44),
                vec![],
                vec![],
            ),
            (
                "non-overlapping",
                Version::new(3, 32),
                Version::new(3, 44),
                vec![
                    test_migration(1, 3, 32, 3, 34),
                    test_migration(2, 3, 35, 3, 37),
                    test_migration(3, 3, 38, 3, 40),
                    test_migration(4, 3, 41, 3, 43),
                ],
                vec![
                    MigrationInterrupt {
                        version: Version::new(3, 33),
                        migration_ids: vec![1],
                    },
                    MigrationInterrupt {
                        version: Version::new(3, 36),
                        migration_ids: vec![2],
                    },
                    MigrationInterrupt {
                        version: Version::new(3, 39),
                        migration_ids: vec![3],
                    },
                    MigrationInterrupt {
                        version: Version::new(3, 42),
                        migration_ids: vec![4],
                    },
                ],
            ),
            (
                "overlapping",
                Version::new(3, 32),
                Version::new(3, 44),
                vec![
                    test_migration(1, 3, 32, 3, 34),
                    test_migration(2, 3, 33, 3, 35),
                    test_migration(3, 3, 36, 3, 41),
                    test_migration(4, 3, 37, 3, 38),
                    test_migration(5, 3, 39, 3, 40),
                    test_migration(6, 3, 42, 3, 43),
                ],
                vec![
                    MigrationInterrupt {
                        version: Version::new(3, 33),
                        migration_ids: vec![1, 2],
                    },
                    MigrationInterrupt {
                        version: Version::new(3, 37),
                        migration_ids: vec![4],
                    },
                    MigrationInterrupt {
                        version: Version::new(3, 39),
                        migration_ids: vec![3, 5],
                    },
                    MigrationInterrupt {
                        version: Version::new(3, 42),
                        migration_ids: vec![6],
                    },
                ],
            ),
            (
                "partial upgrade",
                Version::new(3, 34),
                Version::new(3, 41),
                vec![
                    test_migration(1, 3, 32, 3, 34),
                    test_migration(2, 3, 33, 3, 35),
                    test_migration(3, 3, 36, 3, 41),
                    test_migration(4, 3, 37, 3, 38),
                    test_migration(5, 3, 39, 3, 40),
                ],
                vec![
                    MigrationInterrupt {
                        version: Version::new(3, 34),
                        migration_ids: vec![2],
                    },
                    MigrationInterrupt {
                        version: Version::new(3, 37),
                        migration_ids: vec![4],
                    },
                    MigrationInterrupt {
                        version: Version::new(3, 39),
                        migration_ids: vec![3, 5],
                    },
                ],
            ),
        ];

        for (name, from, to, migrations, expected) in cases {
            let actual = schedule_migration_interrupts_with(from, to, &migrations).unwrap();
            assert_eq!(actual, expected, "case {name}");
        }
    }

    #[test]
    fn schedules_downgrades() {
        let cases: Vec<(
            &str,
            Version,
            Version,
            Vec<YamlMigration>,
            Vec<MigrationInterrupt>,
        )> = vec![
            (
                "empty",
                Version::new(3, 44),
                Version::new(3, 32),
                vec![],
                vec![],
            ),
            (
                "non-overlapping",
                Version::new(3, 44),
                Version::new(3, 32),
                vec![
                    test_migration(4, 3, 41, 3, 43),
                    test_migration(3, 3, 38, 3, 40),
                    test_migration(2, 3, 35, 3, 37),
                    test_migration(1, 3, 32, 3, 34),
                ],
                vec![
                    MigrationInterrupt {
                        version: Version::new(3, 41),
                        migration_ids: vec![4],
                    },
                    MigrationInterrupt {
                        version: Version::new(3, 38),
                        migration_ids: vec![3],
                    },
                    MigrationInterrupt {
                        version: Version::new(3, 35),
                        migration_ids: vec![2],
                    },
                    MigrationInterrupt {
                        version: Version::new(3, 32),
                        migration_ids: vec![1],
                    },
                ],
            ),
            (
                "overlapping",
                Version::new(3, 44),
                Version::new(3, 32),
                vec![
                    test_migration(1, 3, 32, 3, 34),
                    test_migration(2, 3, 33, 3, 35),
                    test_migration(3, 3, 36, 3, 41),
                    test_migration(4, 3, 37, 3, 38),
                    test_migration(5, 3, 39, 3, 40),
                    test_migration(6, 3, 42, 3, 43),
                ],
                vec![
                    MigrationInterrupt {
                        version: Version::new(3, 42),
                        migration_ids: vec![6],
                    },
                    MigrationInterrupt {
                        version: Version::new(3, 39),
                        migration_ids: vec![5],
                    },
                    MigrationInterrupt {
                        version: Version::new(3, 37),
                        migration_ids: vec![3, 4],
                    },
                    MigrationInterrupt {
                        version: Version::new(3, 33),
                        migration_ids: vec![1, 2],
                    },
                ],
            ),
            (
                "partial downgrade",
                Version::new(3, 41),
                Version::new(3, 34),
                vec![
                    test_migration(1, 3, 32, 3, 42),
                    test_migration(2, 3, 33, 3, 43),
                    test_migration(3, 3, 35, 3, 42),
                    test_migration(4, 3, 36, 3, 41),
                    test_migration(5, 3, 37, 3, 38),
                    test_migration(6, 3, 39, 3, 40),
                ],
                vec![
                    MigrationInterrupt {
                        version: Version::new(3, 39),
                        migration_ids: vec![6],
                    },
                    MigrationInterrupt {
                        version: Version::new(3, 37),
                        migration_ids: vec![3, 4, 5],
                    },
                ],
            ),
        ];

        for (name, from, to, migrations, expected) in cases {
            let actual = schedule_migration_interrupts_with(from, to, &migrations).unwrap();
            assert_eq!(actual, expected, "case {name}");
        }
    }
}
