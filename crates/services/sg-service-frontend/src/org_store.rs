#![forbid(unsafe_code)]

//! Production adapter for [`crate::graphql_resolvers::OrganizationStore`] and
//! [`crate::graphql_resolvers::OrganizationsConnectionStore`]:
//! `db.Orgs().GetByName` / `List` / `Count` plus the viewer's
//! `org_members` membership (`internal/database/orgs.go:103-168`,
//! `internal/database/org_members.go:70`).

use std::collections::HashSet;

use async_trait::async_trait;
use sqlx::PgPool;

use crate::graphql_resolvers::{
    OrgData, OrganizationStore, OrganizationsArgs, OrganizationsConnectionData,
    OrganizationsConnectionStore, UserOrganizationsStore,
};
use sg_db::orgs::{
    count_orgs, find_org_by_name, list_orgs, org_members_for_user, orgs_for_user, OrgRecord,
    OrgsListOptions,
};

pub struct PgOrgStore {
    pool: PgPool,
}

impl PgOrgStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// `orgStore.GetByName` + the viewer's membership flags:
    /// `viewerCanAdminister` (`auth.CheckOrgAccessOrSiteAdmin`: site admin or
    /// member) and `viewerIsMember` (`auth.CheckOrgAccess` without the admin
    /// bypass).
    async fn lookup(
        &self,
        name: &str,
        viewer_uid: i32,
        viewer_site_admin: bool,
    ) -> Result<Option<OrgData>, String> {
        let record = find_org_by_name(&self.pool, name)
            .await
            .map_err(|error| error.to_string())?;
        let Some(record) = record else {
            return Ok(None);
        };
        let member = self.is_member(viewer_uid, &[record.id]).await?;
        Ok(Some(to_org_data(record, viewer_site_admin, member)))
    }

    async fn is_member(&self, viewer_uid: i32, org_ids: &[i32]) -> Result<bool, String> {
        if viewer_uid == 0 || org_ids.is_empty() {
            return Ok(false);
        }
        let orgs = org_members_for_user(&self.pool, viewer_uid, org_ids)
            .await
            .map_err(|error| error.to_string())?;
        Ok(!orgs.is_empty())
    }
}

fn to_org_data(record: OrgRecord, viewer_site_admin: bool, member: bool) -> OrgData {
    OrgData {
        id: record.id,
        name: record.name,
        display_name: record.display_name,
        created_at: record.created_at,
        viewer_can_administer: viewer_site_admin || member,
        viewer_is_member: member,
    }
}

/// `orgsListOptions` from the GraphQL arguments (`orgs.go:16-24`): `first`
/// only ever sets `Offset: 0`.
fn list_options(args: &OrganizationsArgs) -> OrgsListOptions {
    OrgsListOptions {
        query: args.query.clone().unwrap_or_default(),
        limit_offset: args.first.map(|first| (first, 0)),
    }
}

#[async_trait]
impl OrganizationStore for PgOrgStore {
    async fn organization(
        &self,
        name: &str,
        viewer_uid: i32,
        viewer_site_admin: bool,
    ) -> Result<Option<OrgData>, String> {
        self.lookup(name, viewer_uid, viewer_site_admin).await
    }
}

#[async_trait]
impl UserOrganizationsStore for PgOrgStore {
    /// `db.Orgs().GetByUserID` plus the *viewer's* membership flags (the
    /// `Org` nodes' `viewerCanAdminister`/`viewerIsMember` are relative to
    /// the observer, not to `user_id`).
    async fn organizations_for_user(
        &self,
        user_id: i32,
        viewer_uid: i32,
        viewer_site_admin: bool,
    ) -> Result<Vec<OrgData>, String> {
        let records = orgs_for_user(&self.pool, user_id)
            .await
            .map_err(|error| error.to_string())?;
        let ids: Vec<i32> = records.iter().map(|org| org.id).collect();
        let member_ids: HashSet<i32> = if viewer_uid == 0 {
            HashSet::new()
        } else {
            org_members_for_user(&self.pool, viewer_uid, &ids)
                .await
                .map_err(|error| error.to_string())?
                .into_iter()
                .collect()
        };
        Ok(records
            .into_iter()
            .map(|record| {
                let member = member_ids.contains(&record.id);
                to_org_data(record, viewer_site_admin, member)
            })
            .collect())
    }
}

#[async_trait]
impl OrganizationsConnectionStore for PgOrgStore {
    async fn organizations(
        &self,
        args: &OrganizationsArgs,
        viewer_uid: i32,
        viewer_site_admin: bool,
    ) -> Result<OrganizationsConnectionData, String> {
        let options = list_options(args);
        let orgs = list_orgs(&self.pool, &options)
            .await
            .map_err(|error| error.to_string())?;
        let total_count = count_orgs(&self.pool, &options)
            .await
            .map_err(|error| error.to_string())?;

        let ids: Vec<i32> = orgs.iter().map(|org| org.id).collect();
        let member_ids: HashSet<i32> = if viewer_uid == 0 {
            HashSet::new()
        } else {
            org_members_for_user(&self.pool, viewer_uid, &ids)
                .await
                .map_err(|error| error.to_string())?
                .into_iter()
                .collect()
        };

        Ok(OrganizationsConnectionData {
            orgs: orgs
                .into_iter()
                .map(|record| {
                    let member = member_ids.contains(&record.id);
                    to_org_data(record, viewer_site_admin, member)
                })
                .collect(),
            total_count: total_count as i32,
        })
    }
}
