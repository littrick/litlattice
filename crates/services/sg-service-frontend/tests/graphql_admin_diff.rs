//! M9.12b differential of the admin/account GraphQL write surface.
//!
//! The Go oracle (`cmd/frontend/graphqlbackend/zz_m9_12b_*_test.go`) drives the
//! real resolvers against `dbmocks.MockDB`; this harness drives the ported
//! prefetch functions against an in-memory [`AdminStore`]/[`AdminEffects`]
//! implementing the same fixture semantics. Both emit one canonical JSON file
//! per case: `{data, errors, effects}` where `effects` records the mutations
//! the resolver performed (store writes, security events, permission syncs).
//!
//! Env:
//!
//! * `SG_M9_12B_DIFF=1`
//! * `M9_12B_CORPUS` — JSON `[{name, query, uid, audit?}]`
//! * `M9_12B_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use sqlx::types::time::OffsetDateTime;

use sg_graphql::exec::{execute_with, root_fields, Actor, ExecRequest};
use sg_graphql::schema::assemble_sdl;
use sg_service_frontend::admin_mutations::{prefetch_admin_mutations, AdminMutationDeps};
use sg_service_frontend::admin_store::{
    AccessTokenExpirationOptions, AccessTokensAllowMode, AdminEffects,
    AdminStore, CloneStatus, CreatedAccessToken, ExternalAccountRef, ExternalServiceRow, NewUser,
    OrgRow, RepoRow, UserEmailRow, UserRow, UserUpdate,
};
use sg_service_frontend::graphql_resolvers::FrontendRootResolver;

const SDL: &str = "
schema { query: Query mutation: Mutation }

type Query { _noop: String }

type Mutation {
  createUser(username: String!, email: String, verifiedEmail: Boolean): CreateUserResult!
  updateUser(user: ID!, username: String, displayName: String, avatarURL: String): User!
  deleteUser(user: ID!, hard: Boolean): EmptyResponse!
  deleteUsers(users: [ID!]!, hard: Boolean): EmptyResponse!

  addUserEmail(user: ID!, email: String!): EmptyResponse!
  removeUserEmail(user: ID!, email: String!): EmptyResponse!
  setUserEmailPrimary(user: ID!, email: String!): EmptyResponse!
  setUserEmailVerified(user: ID!, email: String!, verified: Boolean!): EmptyResponse!
  resendVerificationEmail(user: ID!, email: String!): EmptyResponse!

  createOrganization(name: String!, displayName: String): Org!
  updateOrganization(id: ID!, displayName: String): Org!
  deleteOrganization(organization: ID!): EmptyResponse!
  addUserToOrganization(organization: ID!, username: String!): EmptyResponse!
  removeUserFromOrganization(user: ID!, organization: ID!): EmptyResponse!

  createAccessToken(user: ID!, scopes: [String!]!, note: String!, durationSeconds: Int): CreateAccessTokenResult!
  deleteAccessToken(byID: ID, byToken: String): EmptyResponse!

  addExternalService(input: AddExternalServiceInput!): ExternalService!
  updateExternalService(input: UpdateExternalServiceInput!): ExternalService!
  deleteExternalService(externalService: ID!, async: Boolean): EmptyResponse!
  excludeRepoFromExternalServices(externalServices: [ID!]!, repo: ID!): EmptyResponse!

  recloneRepository(repo: ID!): EmptyResponse!
  deleteRepositoryFromDisk(repo: ID!): EmptyResponse!
  reindexRepository(repository: ID!): EmptyResponse!
}

input AddExternalServiceInput {
  kind: String!
  displayName: String!
  config: String!
  namespace: ID
}

input UpdateExternalServiceInput {
  id: ID!
  displayName: String
  config: String
}

type CreateUserResult {
  user: User!
}

type CreateAccessTokenResult {
  id: ID!
  token: String!
}

type EmptyResponse { alwaysNil: String }

type User {
  id: ID!
  username: String!
  displayName: String
  avatarURL: String
  siteAdmin: Boolean!
}

type Org {
  id: ID!
  name: String!
  displayName: String
  viewerCanAdminister: Boolean!
  viewerIsMember: Boolean!
}

type ExternalService {
  id: ID!
  kind: String!
  displayName: String!
  warning: String
}
";

#[derive(Serialize, Default)]
struct Effects {
    store: Vec<String>,
    audit: Vec<AuditRecord>,
    perms: Vec<PermsRecord>,
}

#[derive(Serialize)]
struct AuditRecord {
    name: String,
    user_id: i32,
    arguments: Value,
}

#[derive(Serialize)]
struct PermsRecord {
    user_ids: Vec<i32>,
    reason: String,
}

#[derive(Serialize)]
struct Envelope {
    data: Value,
    errors: Vec<String>,
    effects: Effects,
}

#[derive(serde::Deserialize)]
struct Case {
    name: String,
    query: String,
    #[serde(default)]
    uid: i32,
    #[serde(default)]
    audit: bool,
}

struct World {
    users: BTreeMap<i32, UserRow>,
    orgs: BTreeMap<i32, OrgRow>,
    members: Vec<(i32, i32)>,
    emails: Vec<UserEmailRow>,
    tokens: Vec<(AccessTokenRowLocal, String)>,
    external: BTreeMap<i64, ExternalServiceRow>,
    repos: BTreeMap<i32, RepoRow>,
    gitserver: BTreeMap<i32, CloneStatus>,
    accounts: BTreeMap<i32, Vec<ExternalAccountRef>>,
    next_user_id: i32,
    next_org_id: i32,
    next_token_id: i64,
    next_external_id: i64,
    effects: Effects,
}

#[derive(Clone)]
struct AccessTokenRowLocal {
    id: i64,
    subject_user_id: i32,
    note: String,
    scopes: Vec<String>,
}

fn user(
    id: i32,
    username: &str,
    display_name: &str,
    site_admin: bool,
) -> UserRow {
    UserRow {
        id,
        username: username.to_owned(),
        display_name: display_name.to_owned(),
        avatar_url: String::new(),
        site_admin,
        tos_accepted: false,
        builtin_auth: false,
        scim_controlled: false,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: OffsetDateTime::UNIX_EPOCH,
        primary_email: String::new(),
    }
}

fn email_row(user_id: i32, email: &str, primary: bool, verified: bool) -> UserEmailRow {
    UserEmailRow {
        user_id,
        email: email.to_owned(),
        is_primary: primary,
        verified,
        verification_code: None,
        last_verification_sent_at: None,
        created_at: OffsetDateTime::UNIX_EPOCH,
        updated_at: OffsetDateTime::UNIX_EPOCH,
    }
}

fn fixture_world() -> World {
    let mut users = BTreeMap::new();
    for row in [
        user(1, "admin", "Admin", true),
        user(3, "alice", "Alice", false),
        user(4, "bob", "Bob", false),
        user(5, "carol", "Carol", false),
        user(6, "soap", "Soap", false),
        user(7, "dave", "Dave", false),
    ] {
        users.insert(row.id, row);
    }
    let mut orgs = BTreeMap::new();
    for (id, name, display_name) in [
        (7, "acme", Some("Acme")),
        (8, "solo", Some("Solo")),
        (9, "adminonly", Some("Admin Only")),
    ] {
        orgs.insert(
            id,
            OrgRow {
                id,
                name: name.to_owned(),
                display_name: display_name.map(str::to_owned),
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: OffsetDateTime::UNIX_EPOCH,
            },
        );
    }
    let mut external = BTreeMap::new();
    external.insert(
        500,
        ExternalServiceRow {
            id: 500,
            kind: "GITHUB".to_owned(),
            display_name: "GH".to_owned(),
            config: r#"{"url":"https://github.com","token":"secret"}"#.to_owned(),
            ..ExternalServiceRow::default()
        },
    );
    external.insert(
        501,
        ExternalServiceRow {
            id: 501,
            kind: "GITLAB".to_owned(),
            display_name: "GL".to_owned(),
            config: r#"{"url":"https://gitlab.com","token":"glsecret"}"#.to_owned(),
            ..ExternalServiceRow::default()
        },
    );
    let mut repos = BTreeMap::new();
    repos.insert(
        200,
        RepoRow {
            id: 200,
            name: "github.com/acme/widget".to_owned(),
        },
    );
    repos.insert(
        201,
        RepoRow {
            id: 201,
            name: "github.com/acme/gizmo".to_owned(),
        },
    );
    repos.insert(
        202,
        RepoRow {
            id: 202,
            name: "github.com/acme/absent".to_owned(),
        },
    );
    repos.insert(
        203,
        RepoRow {
            id: 203,
            name: "github.com/acme/ghost".to_owned(),
        },
    );
    let mut gitserver = BTreeMap::new();
    gitserver.insert(200, CloneStatus::Cloned);
    gitserver.insert(201, CloneStatus::Cloning);
    gitserver.insert(203, CloneStatus::Cloned);
    // Cloned on gitserver but the repo row is gone: exercises the
    // `backend.Repos.RecloneRepository` fetch-error wrapper.
    gitserver.insert(204, CloneStatus::Cloned);
    let mut accounts = BTreeMap::new();
    accounts.insert(
        6,
        vec![ExternalAccountRef {
            service_type: "sourcegraph-operator".to_owned(),
            service_id: "sourcegraph-operator".to_owned(),
            account_id: "soap".to_owned(),
        }],
    );
    World {
        users,
        orgs,
        members: vec![(7, 3), (7, 4), (8, 3), (9, 1)],
        emails: {
            let mut emails = vec![
                email_row(3, "alice@example.com", true, true),
                email_row(3, "alice2@example.com", false, false),
                email_row(4, "bob@example.com", true, true),
                email_row(6, "soap@example.com", true, true),
            ];
            // A recent verification send makes `resendVerificationEmail` hit
            // the one-minute cool-down on both sides.
            emails[1].last_verification_sent_at = Some(OffsetDateTime::now_utc());
            emails
        },
        tokens: vec![
            (
                AccessTokenRowLocal {
                    id: 100,
                    subject_user_id: 3,
                    note: "my token".to_owned(),
                    scopes: vec!["user:all".to_owned()],
                },
                "token-100".to_owned(),
            ),
            (
                AccessTokenRowLocal {
                    id: 101,
                    subject_user_id: 4,
                    note: "bob token".to_owned(),
                    scopes: vec!["user:all".to_owned()],
                },
                "token-101".to_owned(),
            ),
            (
                AccessTokenRowLocal {
                    id: 102,
                    subject_user_id: 6,
                    note: "soap token".to_owned(),
                    scopes: vec!["user:all".to_owned()],
                },
                "token-102".to_owned(),
            ),
        ],
        external,
        repos,
        gitserver,
        accounts,
        next_user_id: 1000,
        next_org_id: 7000,
        next_token_id: 1000,
        next_external_id: 5000,
        effects: Effects::default(),
    }
}

struct Fake {
    world: Mutex<World>,
    auditlog_expansion: bool,
    external_services_writable: bool,
}

impl Fake {
    fn new(auditlog_expansion: bool) -> Self {
        Self {
            world: Mutex::new(fixture_world()),
            auditlog_expansion,
            external_services_writable: true,
        }
    }

    fn store<T>(&self, f: impl FnOnce(&mut World) -> T) -> T {
        let mut world = self.world.lock().unwrap();
        f(&mut world)
    }
}

fn token_row(local: &AccessTokenRowLocal) -> sg_service_frontend::admin_store::AccessTokenRow {
    sg_service_frontend::admin_store::AccessTokenRow {
        id: local.id,
        subject_user_id: local.subject_user_id,
        note: local.note.clone(),
        scopes: local.scopes.clone(),
        created_at: OffsetDateTime::UNIX_EPOCH,
        last_used_at: None,
        expires_at: None,
        value: String::new(),
    }
}

#[async_trait]
impl AdminStore for Fake {
    async fn get_user_by_id(&self, id: i32) -> Result<Option<UserRow>, String> {
        Ok(self.store(|w| w.users.get(&id).cloned()))
    }

    async fn find_user_by_username(&self, username: &str) -> Result<Option<UserRow>, String> {
        Ok(self.store(|w| {
            w.users
                .values()
                .find(|user| user.username == username)
                .cloned()
        }))
    }

    async fn get_users_by_ids(&self, ids: &[i32]) -> Result<Vec<UserRow>, String> {
        Ok(self.store(|w| {
            ids.iter()
                .filter_map(|id| w.users.get(id).cloned())
                .collect()
        }))
    }

    async fn get_by_verified_email(&self, email: &str) -> Result<Option<UserRow>, String> {
        Ok(self.store(|w| {
            let user_id = w
                .emails
                .iter()
                .find(|row| row.email == email && row.verified)
                .map(|row| row.user_id)?;
            w.users.get(&user_id).cloned()
        }))
    }

    async fn create_user(&self, new: &NewUser) -> Result<UserRow, String> {
        Ok(self.store(|w| {
            let id = w.next_user_id;
            w.next_user_id += 1;
            let row = user(id, &new.username, "", false);
            w.users.insert(id, row.clone());
            if !new.email.is_empty() {
                w.emails
                    .push(email_row(id, &new.email, true, new.email_is_verified));
            }
            w.effects.store.push(format!(
                "create_user id={id} username={} email={} email_is_verified={} code={}",
                new.username,
                new.email,
                new.email_is_verified,
                new.email_verification_code.is_some()
            ));
            row
        }))
    }

    async fn update_user(&self, id: i32, update: &UserUpdate) -> Result<UserRow, String> {
        self.store(|w| {
            let Some(row) = w.users.get_mut(&id) else {
                return Err(format!("user not found: [{id}]"));
            };
            if let Some(username) = &update.username {
                row.username = username.clone();
            }
            if let Some(display_name) = &update.display_name {
                row.display_name = display_name.clone().unwrap_or_default();
            }
            if let Some(avatar_url) = &update.avatar_url {
                row.avatar_url = avatar_url.clone().unwrap_or_default();
            }
            let row = row.clone();
            w.effects.store.push(format!(
                "update_user id={id} username={} display_name={} avatar_url={}",
                update.username.clone().unwrap_or_default(),
                update
                    .display_name
                    .clone()
                    .flatten()
                    .unwrap_or_default(),
                update.avatar_url.clone().flatten().unwrap_or_default()
            ));
            Ok(row)
        })
    }

    async fn delete_users(&self, ids: &[i32], hard: bool) -> Result<(), String> {
        self.store(|w| {
            for id in ids {
                w.users.remove(id);
            }
            w.effects
                .store
                .push(format!("delete_users ids={ids:?} hard={hard}"));
            Ok(())
        })
    }

    async fn revoke_user_permissions(&self, ids: &[i32]) -> Result<(), String> {
        self.store(|w| {
            w.effects
                .store
                .push(format!("revoke_user_permissions ids={ids:?}"));
            Ok(())
        })
    }

    async fn grant_pending_permissions(&self, user_id: i32) -> Result<(), String> {
        self.store(|w| {
            w.effects
                .store
                .push(format!("grant_pending_permissions uid={user_id}"));
            Ok(())
        })
    }

    async fn external_accounts_for_user(
        &self,
        user_id: i32,
    ) -> Result<Vec<ExternalAccountRef>, String> {
        Ok(self
            .store(|w| w.accounts.get(&user_id).cloned())
            .unwrap_or_default())
    }

    async fn list_user_emails(
        &self,
        user_id: i32,
        only_verified: bool,
    ) -> Result<Vec<UserEmailRow>, String> {
        Ok(self.store(|w| {
            w.emails
                .iter()
                .filter(|row| row.user_id == user_id && (!only_verified || row.verified))
                .cloned()
                .collect()
        }))
    }

    async fn get_user_email(
        &self,
        user_id: i32,
        email: &str,
    ) -> Result<Option<UserEmailRow>, String> {
        Ok(self.store(|w| {
            w.emails
                .iter()
                .find(|row| row.user_id == user_id && row.email == email)
                .cloned()
        }))
    }

    async fn add_user_email(
        &self,
        user_id: i32,
        email: &str,
        code: Option<&str>,
    ) -> Result<(), String> {
        self.store(|w| {
            if w.emails
                .iter()
                .any(|row| row.user_id == user_id && row.email == email)
            {
                return Err("email address already registered for the user".to_owned());
            }
            let mut row = email_row(user_id, email, false, false);
            row.verification_code = code.map(str::to_owned);
            w.emails.push(row);
            w.effects
                .store
                .push(format!("add_user_email uid={user_id} email={email} code={}", code.is_some()));
            Ok(())
        })
    }

    async fn remove_user_email(&self, user_id: i32, email: &str) -> Result<(), String> {
        self.store(|w| {
            let Some(position) = w
                .emails
                .iter()
                .position(|row| row.user_id == user_id && row.email == email)
            else {
                return Err("fetching email address: sql: no rows in result set".to_owned());
            };
            if w.emails[position].is_primary {
                return Err("can't delete primary email address".to_owned());
            }
            w.emails.remove(position);
            w.effects
                .store
                .push(format!("remove_user_email uid={user_id} email={email}"));
            Ok(())
        })
    }

    async fn set_primary_email(&self, user_id: i32, email: &str) -> Result<(), String> {
        self.store(|w| {
            let Some(position) = w
                .emails
                .iter()
                .position(|row| row.user_id == user_id && row.email == email)
            else {
                return Err("sql: no rows in result set".to_owned());
            };
            if !w.emails[position].verified {
                return Err("primary email must be verified".to_owned());
            }
            for row in &mut w.emails {
                if row.user_id == user_id {
                    row.is_primary = row.email == email;
                }
            }
            w.effects
                .store
                .push(format!("set_primary_email uid={user_id} email={email}"));
            Ok(())
        })
    }

    async fn set_email_verified(
        &self,
        user_id: i32,
        email: &str,
        verified: bool,
    ) -> Result<(), String> {
        self.store(|w| {
            let Some(position) = w
                .emails
                .iter()
                .position(|row| row.user_id == user_id && row.email == email)
            else {
                return Err("user email not found".to_owned());
            };
            w.emails[position].verified = verified;
            w.emails[position].verification_code = None;
            if verified {
                w.emails[position].is_primary = !w
                    .emails
                    .iter()
                    .any(|row| row.user_id == user_id && row.email != email && row.is_primary);
                w.emails.retain(|row| row.verified || row.email != email);
            }
            w.effects.store.push(format!(
                "set_email_verified uid={user_id} email={email} verified={verified}"
            ));
            Ok(())
        })
    }

    async fn set_last_verification(
        &self,
        user_id: i32,
        email: &str,
        code: &str,
        at: OffsetDateTime,
    ) -> Result<(), String> {
        self.store(|w| {
            if let Some(row) = w
                .emails
                .iter_mut()
                .find(|row| row.user_id == user_id && row.email == email)
            {
                row.verification_code = Some(code.to_owned());
                row.last_verification_sent_at = Some(at);
            }
            w.effects
                .store
                .push(format!("set_last_verification uid={user_id} email={email}"));
            Ok(())
        })
    }

    async fn latest_verification_sent_email(
        &self,
        email: &str,
    ) -> Result<Option<UserEmailRow>, String> {
        Ok(self.store(|w| {
            w.emails
                .iter()
                .find(|row| row.email == email && row.last_verification_sent_at.is_some())
                .cloned()
        }))
    }

    async fn get_org_by_id(&self, id: i32) -> Result<Option<OrgRow>, String> {
        Ok(self.store(|w| w.orgs.get(&id).cloned()))
    }

    async fn create_org(
        &self,
        name: &str,
        display_name: Option<&str>,
    ) -> Result<OrgRow, String> {
        Ok(self.store(|w| {
            let id = w.next_org_id;
            w.next_org_id += 1;
            let row = OrgRow {
                id,
                name: name.to_owned(),
                display_name: display_name.map(str::to_owned),
                created_at: OffsetDateTime::UNIX_EPOCH,
                updated_at: OffsetDateTime::UNIX_EPOCH,
            };
            w.orgs.insert(id, row.clone());
            w.effects.store.push(format!(
                "create_org id={id} name={name} display_name={}",
                display_name.unwrap_or_default()
            ));
            row
        }))
    }

    async fn update_org(
        &self,
        id: i32,
        display_name: Option<&str>,
    ) -> Result<OrgRow, String> {
        self.store(|w| {
            let Some(row) = w.orgs.get_mut(&id) else {
                return Err(format!("org not found: id {id}"));
            };
            if let Some(display_name) = display_name {
                row.display_name = Some(display_name.to_owned());
            }
            let row = row.clone();
            w.effects.store.push(format!(
                "update_org id={id} display_name={}",
                display_name.unwrap_or_default()
            ));
            Ok(row)
        })
    }

    async fn delete_org(&self, id: i32) -> Result<(), String> {
        self.store(|w| {
            if w.orgs.remove(&id).is_none() {
                return Err(format!("org not found: id {id}"));
            }
            w.members.retain(|(org_id, _)| *org_id != id);
            w.effects.store.push(format!("delete_org id={id}"));
            Ok(())
        })
    }

    async fn is_org_member(&self, org_id: i32, user_id: i32) -> Result<bool, String> {
        Ok(self.store(|w| w.members.contains(&(org_id, user_id))))
    }

    async fn org_member_count(&self, org_id: i32) -> Result<i64, String> {
        Ok(self.store(|w| {
            w.members
                .iter()
                .filter(|(id, user_id)| *id == org_id && w.users.contains_key(user_id))
                .count() as i64
        }))
    }

    async fn add_org_member(&self, org_id: i32, user_id: i32) -> Result<(), String> {
        self.store(|w| {
            if w.members.contains(&(org_id, user_id)) {
                return Err("user is already a member of the organization".to_owned());
            }
            w.members.push((org_id, user_id));
            w.effects
                .store
                .push(format!("add_org_member org={org_id} user={user_id}"));
            Ok(())
        })
    }

    async fn remove_org_member(&self, org_id: i32, user_id: i32) -> Result<(), String> {
        self.store(|w| {
            w.members.retain(|(id, uid)| !(*id == org_id && *uid == user_id));
            w.effects
                .store
                .push(format!("remove_org_member org={org_id} user={user_id}"));
            Ok(())
        })
    }

    async fn create_access_token(
        &self,
        subject_user_id: i32,
        scopes: &[String],
        note: &str,
        creator_user_id: i32,
        expires_at: Option<OffsetDateTime>,
    ) -> Result<CreatedAccessToken, String> {
        self.store(|w| {
            let id = w.next_token_id;
            w.next_token_id += 1;
            let token = format!("token-{id}");
            w.tokens.push((
                AccessTokenRowLocal {
                    id,
                    subject_user_id,
                    note: note.to_owned(),
                    scopes: scopes.to_vec(),
                },
                token.clone(),
            ));
            let scopes_debug = format!(
                "[{}]",
                scopes
                    .iter()
                    .map(|scope| format!("{scope:?}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            );
            w.effects.store.push(format!(
                "create_access_token id={id} subject={subject_user_id} scopes={scopes_debug} note={note} creator={creator_user_id} expires={}",
                expires_at.is_some()
            ));
            Ok(CreatedAccessToken { id, token })
        })
    }

    async fn get_access_token_by_id(
        &self,
        id: i64,
    ) -> Result<Option<sg_service_frontend::admin_store::AccessTokenRow>, String> {
        Ok(self.store(|w| {
            w.tokens
                .iter()
                .find(|(row, _)| row.id == id)
                .map(|(row, _)| token_row(row))
        }))
    }

    async fn get_access_token_by_token(
        &self,
        token: &str,
    ) -> Result<Option<sg_service_frontend::admin_store::AccessTokenRow>, String> {
        Ok(self.store(|w| {
            w.tokens
                .iter()
                .find(|(_, value)| value == token)
                .map(|(row, _)| token_row(row))
        }))
    }

    async fn delete_access_token_by_id(&self, id: i64) -> Result<(), String> {
        self.store(|w| {
            w.tokens.retain(|(row, _)| row.id != id);
            w.effects
                .store
                .push(format!("delete_access_token_by_id id={id}"));
            Ok(())
        })
    }

    async fn delete_access_token_by_token(&self, token: &str) -> Result<(), String> {
        self.store(|w| {
            w.tokens.retain(|(_, value)| value != token);
            w.effects
                .store
                .push(format!("delete_access_token_by_token token={token}"));
            Ok(())
        })
    }

    async fn get_external_service(
        &self,
        id: i64,
    ) -> Result<Option<ExternalServiceRow>, String> {
        Ok(self.store(|w| w.external.get(&id).cloned()))
    }

    async fn create_external_service(
        &self,
        new: &sg_service_frontend::admin_store::NewExternalService,
    ) -> Result<i64, String> {
        Ok(self.store(|w| {
            let id = w.next_external_id;
            w.next_external_id += 1;
            w.external.insert(
                id,
                ExternalServiceRow {
                    id,
                    kind: new.kind.clone(),
                    display_name: new.display_name.clone(),
                    config: new.config.clone(),
                    creator_id: new.creator_id,
                    last_updater_id: new.last_updater_id,
                    ..ExternalServiceRow::default()
                },
            );
            w.effects.store.push(format!(
                "create_external_service id={id} kind={} display_name={} config={} creator={}",
                new.kind,
                new.display_name,
                new.config,
                new.creator_id.unwrap_or(-1)
            ));
            id
        }))
    }

    async fn update_external_service(
        &self,
        id: i64,
        display_name: Option<&str>,
        config: Option<&str>,
        updater_id: Option<i32>,
    ) -> Result<(), String> {
        self.store(|w| {
            let Some(row) = w.external.get_mut(&id) else {
                return Err(format!("external service not found: {id}"));
            };
            if let Some(display_name) = display_name {
                row.display_name = display_name.to_owned();
            }
            if let Some(config) = config {
                row.config = config.to_owned();
            }
            row.last_updater_id = updater_id;
            w.effects.store.push(format!(
                "update_external_service id={id} display_name={} config={} updater={}",
                display_name.unwrap_or_default(),
                config.unwrap_or_default(),
                updater_id.unwrap_or(-1)
            ));
            Ok(())
        })
    }

    async fn delete_external_service(&self, id: i64, async_delete: bool) -> Result<(), String> {
        self.store(|w| {
            w.external.remove(&id);
            w.effects
                .store
                .push(format!("delete_external_service id={id} async={async_delete}"));
            Ok(())
        })
    }

    async fn exclude_repo_from_external_services(
        &self,
        external_service_ids: &[i64],
        repo_id: i32,
    ) -> Result<(), String> {
        self.store(|w| {
            w.effects.store.push(format!(
                "exclude_repo external_services={external_service_ids:?} repo={repo_id}"
            ));
            Ok(())
        })
    }

    async fn get_repo_by_id(&self, id: i32) -> Result<Option<RepoRow>, String> {
        Ok(self.store(|w| w.repos.get(&id).cloned()))
    }

    async fn get_gitserver_repo(&self, repo_id: i32) -> Result<Option<CloneStatus>, String> {
        Ok(self.store(|w| w.gitserver.get(&repo_id).copied()))
    }

    async fn reclone_repository(&self, repo_id: i32, _name: &str) -> Result<(), String> {
        self.store(|w| {
            w.effects
                .store
                .push(format!("reclone_repository repo={repo_id}"));
            Ok(())
        })
    }

    async fn delete_repository_from_disk(
        &self,
        repo_id: i32,
        name: &str,
    ) -> Result<(), String> {
        self.reclone_repository(repo_id, name).await
    }

    async fn reindex_repository(&self, repo_id: i32, _name: &str) -> Result<(), String> {
        self.store(|w| {
            w.effects
                .store
                .push(format!("reindex_repository repo={repo_id}"));
            Ok(())
        })
    }
}

#[async_trait]
impl AdminEffects for Fake {
    fn can_send_email(&self) -> bool {
        false
    }

    fn email_verification_required(&self) -> bool {
        false
    }

    fn reset_password_enabled(&self) -> bool {
        false
    }

    fn access_tokens_allow(&self) -> AccessTokensAllowMode {
        AccessTokensAllowMode::All
    }

    fn access_token_expiration_options(&self) -> AccessTokenExpirationOptions {
        AccessTokenExpirationOptions {
            allow_no_expiration: true,
            max_duration_seconds: Ok(90 * 86400),
        }
    }

    fn dotcom(&self) -> bool {
        false
    }

    fn external_services_writable(&self) -> bool {
        self.external_services_writable
    }

    fn auth_enable_username_changes(&self) -> bool {
        true
    }

    fn auditlog_expansion(&self) -> bool {
        self.auditlog_expansion
    }

    fn make_email_verification_code(&self) -> Result<String, String> {
        Ok("verification-code".to_owned())
    }

    async fn send_email(&self, _user_id: i32, _email: &str, _kind: &str) -> Result<(), String> {
        Ok(())
    }

    async fn delete_password_reset_code(&self, _user_id: i32) -> Result<(), String> {
        Ok(())
    }

    async fn delete_stale_perforce(&self, _user_id: i32, _email: &str) -> Result<(), String> {
        Ok(())
    }

    async fn schedule_perms_sync(&self, user_ids: &[i32], reason: &str) -> Result<(), String> {
        self.store(|w| {
            w.effects.perms.push(PermsRecord {
                user_ids: user_ids.to_vec(),
                reason: reason.to_owned(),
            });
            Ok(())
        })
    }

    async fn validate_external_service(
        &self,
        _kind: &str,
        config: &str,
    ) -> Result<(), String> {
        if config.contains("invalid") {
            Err("connection check failed".to_owned())
        } else {
            Ok(())
        }
    }

    async fn enqueue_external_service_sync(&self, _id: i64) -> Result<(), String> {
        Ok(())
    }

    async fn reset_password_url(
        &self,
        _user: &UserRow,
        _email: &str,
        _email_verified: bool,
    ) -> Result<Option<String>, String> {
        Ok(None)
    }

    async fn audit(&self, name: &str, user_id: i32, arguments: Value) {
        self.store(|w| {
            w.effects.audit.push(AuditRecord {
                name: name.to_owned(),
                user_id,
                arguments,
            });
        });
    }

    async fn audit_account_modified(&self, user_id: i32, actor_user_id: i32, fields: &[String]) {
        self.store(|w| {
            w.effects.store.push(format!(
                "audit_account_modified user={user_id} actor={actor_user_id} fields={fields:?}"
            ));
        });
    }
}

fn canonical_response(response: sg_graphql::exec::ExecResponse) -> (Value, Vec<String>) {
    let value: Value = serde_json::from_str(&response.to_json()).unwrap_or(Value::Null);
    let data = value.get("data").cloned().unwrap_or(Value::Null);
    let errors = value
        .get("errors")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    item.get("message")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned()
                })
                .collect()
        })
        .unwrap_or_default();
    (data, errors)
}

#[tokio::test]
async fn graphql_admin_diff_harness() {
    if std::env::var("SG_M9_12B_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9_12B_CORPUS").expect("M9_12B_CORPUS");
    let out = std::env::var("M9_12B_OUT").expect("M9_12B_OUT");
    std::fs::create_dir_all(&out).expect("out dir");
    let cases: Vec<Case> =
        serde_json::from_slice(&std::fs::read(corpus_path).expect("corpus")).expect("corpus json");
    let schema = assemble_sdl(SDL).expect("schema");

    for case in cases {
        let actor = if case.uid == 0 {
            Actor::Anonymous
        } else {
            Actor::User { uid: case.uid }
        };
        let viewer_site_admin = matches!(case.uid, 1);
        let request = ExecRequest {
            query: &case.query,
            operation_name: "",
            variables: &serde_json::Map::new(),
            actor,
        };
        let fields = root_fields(&schema, &request);
        let fake = Arc::new(Fake::new(case.audit));
        let deps = AdminMutationDeps::new(fake.clone(), fake.clone());
        let outcomes = prefetch_admin_mutations(&deps, &fields, actor, viewer_site_admin).await;
        let roots = FrontendRootResolver::new().with_admin_mutations(
            viewer_site_admin,
            "http://localhost:3080",
            outcomes,
        );
        let response = execute_with(&schema, request, Some(&roots));
        let (data, errors) = canonical_response(response);
        let effects = fake.store(|w| std::mem::take(&mut w.effects));
        let envelope = Envelope {
            data,
            errors,
            effects,
        };
        let file = serde_json::to_string(&envelope).expect("encode");
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &file).expect("write");
        println!("RUST {} len={}", case.name, file.len());
    }
}
