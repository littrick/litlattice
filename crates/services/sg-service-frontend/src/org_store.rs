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
    maybe_query_is_id, MembersArgs, MembersConnectionData, MembershipData, OrgData,
    OrgMembersStore, OrgMembershipsStore, OrganizationStore, OrganizationsArgs,
    OrganizationsConnectionData, OrganizationsConnectionStore, UserData, UserOrganizationsStore,
};
use crate::user_store::to_user_data;
use sg_db::orgs::{
    count_orgs, find_org_by_id, find_org_by_name, list_orgs, org_members_for_user,
    org_memberships_for_user, orgs_for_user, OrgMembershipRecord, OrgRecord, OrgsListOptions,
};
use sg_db::users::{
    count_users, get_session_user, list_users_by_org, UsersListOptions, UsersPaginationArgs,
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

/// `UnmarshalUserID` (`user.go:126-129` + graph-gophers
/// `relay.UnmarshalSpec`): base64 decode, skip the `kind:` prefix (the kind is
/// **not** validated) and decode the remainder as an int32.
fn unmarshal_user_id(cursor: &str) -> Result<i32, String> {
    crate::graphql_resolvers::unmarshal_relay_id(cursor)
}

#[async_trait]
impl OrgMembersStore for PgOrgStore {
    /// `membersConnectionStore` (`org.go:123-173`) through
    /// `gqlutil.ConnectionResolver`: `pageSize + 1` over-fetch, `last`
    /// reversal, trailing/leading trim and the `OrgID` count.
    async fn members(
        &self,
        org_id: i32,
        args: &MembersArgs,
    ) -> Result<MembersConnectionData, String> {
        // `ConnectionResolver.Limit` + `ApplyMaxPageSize`
        // (`connection_resolver.go:57-94`): 0 means 100, capped at 100.
        let raw_limit = args.first.or(args.last).unwrap_or(0);
        let page_size = if raw_limit == 0 {
            100
        } else {
            raw_limit.min(100)
        };
        let sql_limit = page_size + 1;
        // `paginationArgs`: `first` wins over `last` (the baseline's
        // `else if`).
        let (first, last) = if args.first.is_some() {
            (Some(sql_limit), None)
        } else if args.last.is_some() {
            (None, Some(sql_limit))
        } else {
            (None, None)
        };
        let after = args.after.as_deref().map(unmarshal_user_id).transpose()?;
        let before = args.before.as_deref().map(unmarshal_user_id).transpose()?;
        let pagination = UsersPaginationArgs {
            first,
            last,
            after,
            before,
            ascending: false,
        };

        let query = args.query.as_deref();
        let query_id = query.and_then(maybe_query_is_id);
        let total_count = count_users(
            &self.pool,
            &UsersListOptions {
                query: query.map(str::to_owned),
                query_id,
                org_id: Some(org_id),
                ..UsersListOptions::default()
            },
        )
        .await
        .map_err(|error| error.to_string())?;
        let mut records = list_users_by_org(&self.pool, org_id, query, query_id, &pagination)
            .await
            .map_err(|error| error.to_string())?;

        let fetched_count = records.len() as i32;
        if last.is_some() {
            records.reverse();
        }
        if fetched_count > page_size {
            if last.is_some() {
                records.remove(0);
            } else {
                records.pop();
            }
        }

        Ok(MembersConnectionData {
            users: records.into_iter().map(to_user_data).collect(),
            total_count: total_count as i32,
            page_size,
            fetched_count,
            first_present: args.first.is_some(),
            last_present: args.last.is_some(),
            after_present: args.after.is_some(),
            before_present: args.before.is_some(),
        })
    }
}

fn to_membership_data(record: OrgMembershipRecord) -> MembershipData {
    MembershipData {
        id: record.id,
        org_id: record.org_id,
        user_id: record.user_id,
        created_at: record.created_at,
        updated_at: record.updated_at,
    }
}

#[async_trait]
impl OrgMembershipsStore for PgOrgStore {
    /// `orgMembersStore.GetByUserID` (`org_members.go:66-68`): live users only,
    /// no `ORDER BY`.
    async fn memberships_for_user(&self, user_id: i32) -> Result<Vec<MembershipData>, String> {
        let records = org_memberships_for_user(&self.pool, user_id)
            .await
            .map_err(|error| error.to_string())?;
        Ok(records.into_iter().map(to_membership_data).collect())
    }

    /// `OrgByIDInt32` (`org.go:57-63`): `db.Orgs().GetByID` treats a missing
    /// live org as `org not found: id <n>`; the `Org` node's viewer flags are
    /// relative to the observer, like `Query.organization`.
    async fn org_by_id(
        &self,
        org_id: i32,
        viewer_uid: i32,
        viewer_site_admin: bool,
    ) -> Result<OrgData, String> {
        let Some(record) = find_org_by_id(&self.pool, org_id)
            .await
            .map_err(|error| error.to_string())?
        else {
            return Err(format!("org not found: id {org_id}"));
        };
        let member = self.is_member(viewer_uid, &[record.id]).await?;
        Ok(to_org_data(record, viewer_site_admin, member))
    }

    /// `UserByIDInt32` (`user.go:114-120`) with the primary email
    /// (`userStore.getBySQL`); a missing live user is `user not found: [<n>]`.
    async fn user_by_id(&self, user_id: i32) -> Result<UserData, String> {
        let record = get_session_user(&self.pool, user_id)
            .await
            .map_err(|error| error.to_string())?;
        record
            .map(to_user_data)
            .ok_or_else(|| format!("user not found: [{user_id}]"))
    }
}
