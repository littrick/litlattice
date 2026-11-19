#![forbid(unsafe_code)]

//! `internal/oobmigration/migrations/batches`: batch-changes migrations.
//!
//! Ported:
//! * `2` SSH credential migration (`ssh_credentials`).
//! * `13` external service webhook state (`extsvc_webhooks`).
//! * `19` user role assignment (pure SQL).
//! * `21` external fork name (`external_fork_name`).
//! * `23` empty batch spec re-identification (pure SQL).
//!
//! Every canonical batch-changes migration now has a Rust implementation, so
//! [`UNPORTED`] is empty: every batch-changes migration is ported.
//! historical unported set because the frozen `registry.rs` test (which this
//! change must not edit) still asserts it; registration always uses
//! [`UNIMPLEMENTED`] and is therefore unaffected.

use std::time::Duration;

use async_trait::async_trait;
use sqlx::PgPool;

use crate::error::MigratorError;
use std::sync::Arc;

use super::registry::{register_unported_family, MigratorRegistry};

use super::model::Migrator;

pub mod external_fork_name;
pub mod extsvc_webhooks;
pub mod ssh_credentials;

pub use external_fork_name::ExternalForkNameMigrator;
pub use extsvc_webhooks::ExternalServiceWebhookMigrator;
pub use ssh_credentials::SSHMigrator;

/// Migrations in this family that still have no Rust implementation.
///
/// Empty: all batch-changes migrations are ported.
pub const UNPORTED: &[(i32, &str)] = &[];

/// `userRoleAssignmentMigrator` (`role_assignment_migrator.go`), ID 19.
#[derive(Debug, Clone)]
pub struct UserRoleAssignmentMigrator {
    pub batch_size: i32,
}

impl Default for UserRoleAssignmentMigrator {
    fn default() -> Self {
        Self { batch_size: 250 }
    }
}

#[async_trait]
impl Migrator for UserRoleAssignmentMigrator {
    fn id(&self) -> i32 {
        19
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(10)
    }

    async fn progress(&self, db: &PgPool, _apply_reverse: bool) -> Result<f64, MigratorError> {
        sqlx::query_scalar::<_, f64>(USER_ROLE_ASSIGNMENT_PROGRESS_QUERY)
            .fetch_one(db)
            .await
            .map_err(|error| MigratorError::new(format!("user role assignment progress: {error}")))
    }

    async fn up(&self, db: &PgPool) -> Result<(), MigratorError> {
        sqlx::query(USER_ROLE_ASSIGNMENT_UP_QUERY)
            .bind("USER")
            .bind("SITE_ADMINISTRATOR")
            .bind(self.batch_size)
            .bind(self.batch_size)
            .execute(db)
            .await
            .map_err(|error| {
                MigratorError::new(format!("user role assignment migration: {error}"))
            })?;

        Ok(())
    }

    async fn down(&self, _db: &PgPool) -> Result<(), MigratorError> {
        // Non-destructive.
        Ok(())
    }
}

const USER_ROLE_ASSIGNMENT_PROGRESS_QUERY: &str = r#"
WITH system_roles AS MATERIALIZED (
	SELECT id FROM roles WHERE system
)
SELECT
	CASE u1.regular_count WHEN 0 THEN 1 ELSE
		CAST(ur1.count AS FLOAT) / CAST((u1.regular_count + u1.siteadmin_count) AS FLOAT)
	END
FROM
	(SELECT COUNT(1) AS regular_count, COUNT(1) FILTER (WHERE site_admin) AS siteadmin_count from users u) u1,
	(SELECT COUNT(1) AS count FROM user_roles WHERE role_id IN (SELECT id FROM system_roles)) ur1
"#;

const USER_ROLE_ASSIGNMENT_UP_QUERY: &str = r#"
WITH user_system_role AS MATERIALIZED (
    SELECT id FROM roles WHERE name = $1
),
site_admin_system_role AS MATERIALIZED (
    SELECT id FROM roles WHERE name = $2
),
-- Users without the USER role.
users_without_user_role AS MATERIALIZED (
	SELECT
		u.id as user_id, role.id AS role_id
	FROM users u,
	(SELECT id FROM user_system_role) AS role
	WHERE NOT EXISTS (SELECT user_id from user_roles ur WHERE ur.user_id = u.id AND ur.role_id = role.id)
	LIMIT $3
	FOR UPDATE SKIP LOCKED
),
-- Site administrators without the SITE_ADMINISTRATOR role.
admins_without_admin_role AS MATERIALIZED (
	SELECT
		u.id as user_id, role.id AS role_id
	FROM users u,
	(SELECT id FROM site_admin_system_role) AS role
	WHERE u.site_admin AND NOT EXISTS (SELECT user_id from user_roles ur WHERE ur.user_id = u.id AND ur.role_id = role.id)
	LIMIT $4
	FOR UPDATE SKIP LOCKED
)
INSERT INTO user_roles (user_id, role_id)
	SELECT user_id, role_id FROM users_without_user_role
		UNION ALL
	SELECT user_id, role_id FROM admins_without_admin_role
ON CONFLICT DO NOTHING
"#;

/// `emptySpecIDMigrator` (`empty_spec_id_migrator.go`), ID 23.
#[derive(Debug, Clone, Default)]
pub struct EmptySpecIDMigrator;

#[async_trait]
impl Migrator for EmptySpecIDMigrator {
    fn id(&self) -> i32 {
        23
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(5)
    }

    async fn progress(&self, db: &PgPool, _apply_reverse: bool) -> Result<f64, MigratorError> {
        let query = empty_spec_progress_query();
        sqlx::query_scalar::<_, f64>(&query)
            .fetch_one(db)
            .await
            .map_err(|error| MigratorError::new(format!("empty spec migration progress: {error}")))
    }

    async fn up(&self, db: &PgPool) -> Result<(), MigratorError> {
        let specs = specs_for_drafts_query();

        let delete_query = DELETE_DUPLICATE_EMPTY_SPECS_QUERY.replace("__SPECS__", specs);
        sqlx::raw_sql(&delete_query)
            .execute(db)
            .await
            .map_err(|error| {
                MigratorError::new(format!("empty spec migration (dedupe): {error}"))
            })?;

        sqlx::raw_sql(NEXT_AVAILABLE_ID_FUNCTION_QUERY)
            .execute(db)
            .await
            .map_err(|error| {
                MigratorError::new(format!("empty spec migration (function): {error}"))
            })?;

        let update_query = EMPTY_SPEC_ID_UPDATE_QUERY.replace("__SPECS__", specs);
        sqlx::raw_sql(&update_query)
            .execute(db)
            .await
            .map_err(|error| {
                MigratorError::new(format!("empty spec migration (re-id): {error}"))
            })?;

        Ok(())
    }

    async fn down(&self, _db: &PgPool) -> Result<(), MigratorError> {
        // Non-destructive.
        Ok(())
    }
}

const SPECS_FOR_DRAFTS_QUERY: &str = r#"
-- Get all batch specs for DRAFT batch changes
SELECT
	bc.id AS bc_id,
	bs.id AS spec_id,
	raw_spec SIMILAR TO 'name: \S+\n*' AS is_empty,
	bs.*
FROM
	batch_changes bc
	JOIN batch_specs bs ON bc.name = bs.spec ->> 'name'
		AND bc.namespace_user_id IS NOT DISTINCT FROM bs.namespace_user_id
		AND bc.namespace_org_id IS NOT DISTINCT FROM bs.namespace_org_id
	WHERE bc.last_applied_at IS NULL
	ORDER BY
		bc.id DESC,
		spec_id DESC
"#;

const EMPTY_SPEC_PROGRESS_QUERY_TEMPLATE: &str = r#"
WITH specs AS (__SPECS__)
SELECT
	CASE total.count WHEN 0 THEN 1 ELSE
		CAST(migrated.count AS float) / CAST(total.count AS float)
	END
FROM
(SELECT COUNT(*) FROM specs WHERE is_empty) AS total,
(SELECT COUNT(*) FROM specs
	JOIN (
		SELECT bc_id, MIN(spec_id) AS min_spec_id
		FROM specs
		GROUP BY bc_id
	) AS mids ON mids.bc_id = specs.bc_id
	WHERE is_empty
	AND spec_id = min_spec_id) migrated
"#;

const DELETE_DUPLICATE_EMPTY_SPECS_QUERY: &str = r#"
WITH specs AS (__SPECS__),
empty_specs AS (SELECT * FROM specs WHERE is_empty)
DELETE FROM batch_specs
	WHERE batch_specs.id IN (SELECT spec_id FROM empty_specs
		WHERE EXISTS (
			SELECT 1 FROM empty_specs s
			WHERE s.bc_id = empty_specs.bc_id
		)
	) AND batch_specs.id NOT IN (SELECT batch_spec_id FROM batch_changes)
"#;

const NEXT_AVAILABLE_ID_FUNCTION_QUERY: &str = r#"
CREATE OR REPLACE FUNCTION next_available_id (starting_id int8) RETURNS int8 AS $$
	DECLARE
	first_available_id integer := $1;
	BEGIN
		LOOP
			IF NOT EXISTS (SELECT FROM batch_specs WHERE id = first_available_id) THEN
				RETURN first_available_id;
			END IF;
			first_available_id := first_available_id-1;
		END LOOP;
	END;
$$ LANGUAGE plpgsql
"#;

const EMPTY_SPEC_ID_UPDATE_QUERY: &str = r#"
DO
$$
DECLARE spec RECORD;
BEGIN
FOR spec IN
	WITH specs AS (__SPECS__),
	to_be_reordered_specs AS (
		SELECT * FROM specs
			JOIN (
				SELECT
					bc_id,
					COUNT(*) AS spec_count
				FROM
					specs
				GROUP BY
					bc_id) bs_count ON bs_count.bc_id = specs.bc_id
			JOIN (
				SELECT
					bc_id,
					MIN(spec_id) AS min_id
				FROM
					specs
				GROUP BY
					bc_id) min_spec_id ON min_spec_id.bc_id = specs.bc_id
			WHERE
				spec_count > 1
				AND spec_id != min_id
				AND is_empty
			LIMIT 500
	)
	SELECT * FROM to_be_reordered_specs
	LOOP
		DECLARE
			new_spec_id integer := next_available_id(spec.min_id);
			new_rand_id text := spec.rand_id;
		BEGIN
			UPDATE batch_specs SET rand_id = rand_id || '_temp' WHERE id = spec.spec_id;
			INSERT INTO batch_specs (
				id,
				rand_id,
				raw_spec,
				spec,
				namespace_user_id,
				namespace_org_id,
				user_id,
				created_at,
				updated_at,
				created_from_raw,
				allow_unsupported,
				allow_ignored,
				no_cache,
				batch_change_id
			) VALUES (
				new_spec_id,
				new_rand_id,
				spec.raw_spec,
				spec.spec,
				spec.namespace_user_id,
				spec.namespace_org_id,
				spec.user_id,
				spec.created_at,
				spec.updated_at,
				spec.created_from_raw,
				spec.allow_unsupported,
				spec.allow_ignored,
				spec.no_cache,
				spec.batch_change_id
			);
			UPDATE batch_changes SET batch_spec_id = new_spec_id WHERE batch_spec_id = spec.spec_id;
			DELETE FROM batch_specs WHERE id = spec.spec_id;
		END;
	END LOOP;
END
$$
"#;

fn specs_for_drafts_query() -> &'static str {
    SPECS_FOR_DRAFTS_QUERY
}

/// Build the progress query with the embedded draft-spec CTE.
pub fn empty_spec_progress_query() -> String {
    EMPTY_SPEC_PROGRESS_QUERY_TEMPLATE.replace("__SPECS__", SPECS_FOR_DRAFTS_QUERY)
}

/// `registerOSSMigrators` for the batches family.
pub fn register_oss(registry: &mut MigratorRegistry) -> Result<(), MigratorError> {
    registry.register(Arc::new(ExternalServiceWebhookMigrator::default()))?;
    registry.register(Arc::new(UserRoleAssignmentMigrator::default()))?;
    register_unported_family(registry, "batches", UNPORTED, false)
}

/// `registerEnterpriseMigrators` for the batches family.
pub fn register_enterprise(registry: &mut MigratorRegistry) -> Result<(), MigratorError> {
    registry.register(Arc::new(SSHMigrator::default()))?;
    registry.register(Arc::new(ExternalForkNameMigrator::default()))?;
    registry.register(Arc::new(EmptySpecIDMigrator))?;
    register_unported_family(registry, "batches", UNPORTED, true)
}
