//! M9.13a differential of the saved-search GraphQL surface against the Go
//! baseline (`cmd/frontend/internal/savedsearches/resolvers` + graph-gophers).
//!
//! Both sides execute the same query strings against an in-memory
//! `SavedSearchStore` built from the corpus fixtures. The Rust side assembles
//! the vendored `saved_searches.graphql` on top of the main OSS schema and
//! executes through the production `FrontendRootResolver`.
//!
//! Env:
//!
//! * `SG_M9_13A_SS_DIFF=1`
//! * `M9_13A_SS_CORPUS` — corpus JSON path
//! * `M9_13A_SS_OUT` — output directory (one `<name>.json` per case)

#![allow(clippy::unwrap_used, clippy::too_many_arguments)]

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use serde_json::Map;
use sg_graphql::exec::{execute_with, Actor, ExecRequest};
use sg_graphql::schema::{assemble, MAIN_SCHEMA, SAVED_SEARCHES_SCHEMA};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use sg_service_frontend::graphql_resolvers::{FrontendRootResolver, OrgData, UserData, UserObject};
use sg_service_frontend::saved_searches_graphql::{
    NamespaceIds, SavedSearchCursorValue, SavedSearchListArgs, SavedSearchRow,
    SavedSearchesGraphqlDatabase, SavedSearchesOrderBy,
};

#[derive(Deserialize)]
struct Corpus {
    now: String,
    #[serde(default)]
    fixtures: Fixtures,
    #[serde(default)]
    cases: Vec<Case>,
}

#[derive(Deserialize, Default)]
struct Fixtures {
    #[serde(default)]
    users: Vec<UserFixture>,
    #[serde(default)]
    orgs: Vec<OrgFixture>,
    #[serde(default)]
    org_members: Vec<(i32, i32)>,
    #[serde(default)]
    saved_searches: Vec<SavedSearchFixture>,
}

#[derive(Deserialize)]
struct UserFixture {
    id: i32,
    username: String,
    #[serde(default)]
    display_name: String,
    #[serde(default)]
    avatar_url: String,
    #[serde(default = "default_time")]
    created_at: String,
    #[serde(default = "default_time")]
    updated_at: String,
    #[serde(default)]
    site_admin: bool,
    #[serde(default)]
    tos_accepted: bool,
    #[serde(default)]
    primary_email: String,
}

#[derive(Deserialize)]
struct OrgFixture {
    id: i32,
    name: String,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default = "default_time")]
    created_at: String,
}

#[derive(Deserialize)]
struct SavedSearchFixture {
    id: i32,
    description: String,
    query: String,
    #[serde(default)]
    draft: bool,
    #[serde(default)]
    user_id: Option<i32>,
    #[serde(default)]
    org_id: Option<i32>,
    #[serde(default)]
    visibility_secret: bool,
    #[serde(default)]
    created_by: Option<i32>,
    #[serde(default = "default_time")]
    created_at: String,
    #[serde(default)]
    updated_by: Option<i32>,
    #[serde(default = "default_time")]
    updated_at: String,
}

fn default_time() -> String {
    "2024-01-01T00:00:00Z".to_owned()
}

#[derive(Deserialize)]
struct Case {
    name: String,
    query: String,
    #[serde(default)]
    viewer: String,
    #[serde(default)]
    uid: i32,
}

fn parse_time(value: &str) -> OffsetDateTime {
    OffsetDateTime::parse(value, &Rfc3339).expect("rfc3339 timestamp")
}

fn user_data(fixture: &UserFixture) -> UserData {
    UserData {
        id: fixture.id,
        username: fixture.username.clone(),
        display_name: fixture.display_name.clone(),
        avatar_url: fixture.avatar_url.clone(),
        created_at: parse_time(&fixture.created_at),
        updated_at: parse_time(&fixture.updated_at),
        site_admin: fixture.site_admin,
        tos_accepted: fixture.tos_accepted,
        primary_email: fixture.primary_email.clone(),
    }
}

fn org_data(fixture: &OrgFixture) -> OrgData {
    OrgData {
        id: fixture.id,
        name: fixture.name.clone(),
        display_name: fixture.display_name.clone(),
        created_at: parse_time(&fixture.created_at),
        viewer_can_administer: false,
        viewer_is_member: false,
    }
}

fn saved_search_row(fixture: &SavedSearchFixture) -> SavedSearchRow {
    SavedSearchRow {
        id: fixture.id,
        description: fixture.description.clone(),
        query: fixture.query.clone(),
        draft: fixture.draft,
        user_id: fixture.user_id,
        org_id: fixture.org_id,
        visibility_secret: fixture.visibility_secret,
        created_by_user: fixture.created_by,
        created_at: parse_time(&fixture.created_at),
        updated_by_user: fixture.updated_by,
        updated_at: parse_time(&fixture.updated_at),
    }
}

/// In-memory `SavedSearchStore` replicating `SavedSearchListArgs.toSQL()`,
/// `PaginationArgs.SQL()` and the CRUD statements.
struct InMemorySavedSearches {
    users: Vec<UserData>,
    orgs: Vec<OrgData>,
    org_members: Vec<(i32, i32)>,
    rows: Mutex<Vec<SavedSearchRow>>,
    now: OffsetDateTime,
}

impl InMemorySavedSearches {
    fn new(corpus: &Corpus) -> Self {
        Self {
            users: corpus.fixtures.users.iter().map(user_data).collect(),
            orgs: corpus.fixtures.orgs.iter().map(org_data).collect(),
            org_members: corpus.fixtures.org_members.clone(),
            rows: Mutex::new(
                corpus
                    .fixtures
                    .saved_searches
                    .iter()
                    .map(saved_search_row)
                    .collect(),
            ),
            now: parse_time(&corpus.now),
        }
    }

    fn user(&self, id: i32) -> Option<UserData> {
        self.users.iter().find(|user| user.id == id).cloned()
    }

    fn org(&self, id: i32) -> Option<OrgData> {
        self.orgs.iter().find(|org| org.id == id).cloned()
    }

    fn matches(&self, row: &SavedSearchRow, args: &SavedSearchListArgs) -> bool {
        if !args.query.is_empty() {
            let needle = args.query.to_lowercase();
            if !row.description.to_lowercase().contains(&needle)
                && !row.query.to_lowercase().contains(&needle)
            {
                return false;
            }
        }
        if let Some(user) = args.affiliated_user {
            let member = row.org_id.is_some_and(|org_id| {
                self.org(org_id).is_some()
                    && self.org_members.contains(&(org_id, user))
            });
            if row.user_id != Some(user) && !member && !row.visibility_secret {
                // matches
            } else if row.user_id != Some(user) && !member {
                return false;
            }
        }
        if args.public_only && row.visibility_secret {
            return false;
        }
        if let Some(owner) = args.owner {
            if let Some(user) = owner.user.filter(|id| *id != 0) {
                if row.user_id != Some(user) {
                    return false;
                }
            } else if let Some(org) = owner.org.filter(|id| *id != 0) {
                if row.org_id != Some(org) {
                    return false;
                }
            } else {
                return false;
            }
        }
        if args.hide_drafts && row.draft {
            return false;
        }
        true
    }

    fn cursor_cmp(
        order: SavedSearchesOrderBy,
        row: &SavedSearchRow,
        values: &[SavedSearchCursorValue],
    ) -> std::cmp::Ordering {
        match (order, values.first()) {
            (SavedSearchesOrderBy::Description, Some(SavedSearchCursorValue::Description(text))) => {
                row.description.cmp(text)
            }
            (SavedSearchesOrderBy::UpdatedAt, Some(SavedSearchCursorValue::UpdatedAt(nanos))) => {
                row.updated_at
                    .unix_timestamp_nanos()
                    .cmp(&(*nanos as i128))
            }
            _ => std::cmp::Ordering::Equal,
        }
    }
}

impl SavedSearchesGraphqlDatabase for InMemorySavedSearches {
    fn get_saved_search(&self, id: i32) -> Result<Option<SavedSearchRow>, String> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .iter()
            .find(|row| row.id == id)
            .cloned())
    }

    fn list_saved_searches(
        &self,
        args: &SavedSearchListArgs,
        order: SavedSearchesOrderBy,
        after: Option<&Vec<SavedSearchCursorValue>>,
        before: Option<&Vec<SavedSearchCursorValue>>,
        first: Option<i64>,
        last: Option<i64>,
    ) -> Result<Vec<SavedSearchRow>, String> {
        let ascending = order.ascending();
        let mut rows: Vec<SavedSearchRow> = self
            .rows
            .lock()
            .unwrap()
            .iter()
            .filter(|row| self.matches(row, args))
            .cloned()
            .collect();
        if let Some(values) = after.filter(|values| !values.is_empty()) {
            rows.retain(|row| {
                let cmp = Self::cursor_cmp(order, row, values);
                if ascending {
                    cmp == std::cmp::Ordering::Greater
                } else {
                    cmp == std::cmp::Ordering::Less
                }
            });
        }
        if let Some(values) = before.filter(|values| !values.is_empty()) {
            rows.retain(|row| {
                let cmp = Self::cursor_cmp(order, row, values);
                if ascending {
                    cmp == std::cmp::Ordering::Less
                } else {
                    cmp == std::cmp::Ordering::Greater
                }
            });
        }
        let order_ascending = if first.is_some() { ascending } else { !ascending };
        rows.sort_by(|left, right| {
            let cmp = match order {
                SavedSearchesOrderBy::Description => left.description.cmp(&right.description),
                SavedSearchesOrderBy::UpdatedAt => left.updated_at.cmp(&right.updated_at),
            };
            if order_ascending {
                cmp
            } else {
                cmp.reverse()
            }
        });
        if let Some(limit) = first.or(last) {
            rows.truncate(limit.max(0) as usize);
        }
        Ok(rows)
    }

    fn count_saved_searches(&self, args: &SavedSearchListArgs) -> Result<i32, String> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .iter()
            .filter(|row| self.matches(row, args))
            .count() as i32)
    }

    fn create_saved_search(
        &self,
        description: &str,
        query: &str,
        draft: bool,
        owner: NamespaceIds,
        visibility_secret: bool,
        actor_uid: i32,
    ) -> Result<SavedSearchRow, String> {
        let mut rows = self.rows.lock().unwrap();
        let id = rows.iter().map(|row| row.id).max().unwrap_or(0) + 1;
        let row = SavedSearchRow {
            id,
            description: description.to_owned(),
            query: query.to_owned(),
            draft,
            user_id: owner.user,
            org_id: owner.org,
            visibility_secret,
            created_by_user: Some(actor_uid),
            created_at: self.now,
            updated_by_user: Some(actor_uid),
            updated_at: self.now,
        };
        rows.push(row.clone());
        Ok(row)
    }

    fn update_saved_search(
        &self,
        id: i32,
        description: &str,
        query: &str,
        draft: bool,
        actor_uid: i32,
    ) -> Result<SavedSearchRow, String> {
        let mut rows = self.rows.lock().unwrap();
        let row = rows
            .iter_mut()
            .find(|row| row.id == id)
            .ok_or_else(|| "saved search not found".to_owned())?;
        row.description = description.to_owned();
        row.query = query.to_owned();
        row.draft = draft;
        row.updated_by_user = Some(actor_uid);
        row.updated_at = self.now;
        Ok(row.clone())
    }

    fn update_saved_search_owner(
        &self,
        id: i32,
        owner: NamespaceIds,
        actor_uid: i32,
    ) -> Result<SavedSearchRow, String> {
        let mut rows = self.rows.lock().unwrap();
        let row = rows
            .iter_mut()
            .find(|row| row.id == id)
            .ok_or_else(|| "saved search not found".to_owned())?;
        row.user_id = owner.user;
        row.org_id = owner.org;
        row.updated_by_user = Some(actor_uid);
        row.updated_at = self.now;
        Ok(row.clone())
    }

    fn update_saved_search_visibility(
        &self,
        id: i32,
        visibility_secret: bool,
        actor_uid: i32,
    ) -> Result<SavedSearchRow, String> {
        let mut rows = self.rows.lock().unwrap();
        let row = rows
            .iter_mut()
            .find(|row| row.id == id)
            .ok_or_else(|| "saved search not found".to_owned())?;
        row.visibility_secret = visibility_secret;
        row.updated_by_user = Some(actor_uid);
        row.updated_at = self.now;
        Ok(row.clone())
    }

    fn delete_saved_search(&self, id: i32) -> Result<(), String> {
        let mut rows = self.rows.lock().unwrap();
        let before = rows.len();
        rows.retain(|row| row.id != id);
        if rows.len() == before {
            return Err("saved search not found".to_owned());
        }
        Ok(())
    }

    fn user_by_id(&self, id: i32) -> Result<Option<UserData>, String> {
        Ok(self.user(id))
    }

    fn org_by_id(&self, id: i32) -> Result<Option<OrgData>, String> {
        Ok(self.org(id))
    }

    fn current_user(&self, uid: i32) -> Result<Option<UserData>, String> {
        Ok(self.user(uid))
    }

    fn is_site_admin(&self, uid: i32) -> Result<bool, String> {
        Ok(self.user(uid).is_some_and(|user| user.site_admin))
    }

    fn is_org_member(&self, org_id: i32, user_id: i32) -> Result<bool, String> {
        Ok(self.org(org_id).is_some() && self.org_members.contains(&(org_id, user_id)))
    }
}

/// Emit `data` compact, or only the error messages (the two implementations
/// differ in error locations/paths, which are not part of the contract under
/// test).
fn normalized(response: &sg_graphql::exec::ExecResponse) -> String {
    let mut entries: Vec<(&str, serde_json::Value)> = Vec::new();
    if !response.errors.is_empty() {
        entries.push((
            "errors",
            serde_json::Value::Array(
                response
                    .errors
                    .iter()
                    .map(|error| serde_json::json!({ "message": error.message }))
                    .collect(),
            ),
        ));
    }
    if let Some(data) = response.data_json() {
        entries.push(("data", serde_json::from_str(&data).unwrap()));
    }
    let map: BTreeMap<&str, serde_json::Value> = entries.into_iter().collect();
    serde_json::to_string(&map).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn saved_searches_harness() {
    if std::env::var("SG_M9_13A_SS_DIFF").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M9_13A_SS_CORPUS").expect("M9_13A_SS_CORPUS");
    let out = std::env::var("M9_13A_SS_OUT").expect("M9_13A_SS_OUT");
    std::fs::create_dir_all(&out).expect("out dir");
    let corpus: Corpus = serde_json::from_slice(&std::fs::read(&corpus_path).expect("corpus"))
        .expect("corpus json");
    let schema = assemble(&[MAIN_SCHEMA, SAVED_SEARCHES_SCHEMA]).expect("schema");

    for case in &corpus.cases {
        let db: Arc<dyn SavedSearchesGraphqlDatabase + Send + Sync> =
            Arc::new(InMemorySavedSearches::new(&corpus));
        let (actor, user) = match case.viewer.as_str() {
            "anonymous" | "" => (Actor::Anonymous, None),
            _ => {
                let fixture = corpus
                    .fixtures
                    .users
                    .iter()
                    .find(|user| user.id == case.uid)
                    .expect("viewer fixture");
                let data = user_data(fixture);
                let object = UserObject::from_data_as_viewer(data.clone(), data.site_admin);
                (Actor::User { uid: case.uid }, Some(object))
            }
        };
        let roots = FrontendRootResolver::with_user(user).with_saved_searches(Some(db));
        let variables = Map::new();
        let request = ExecRequest {
            query: &case.query,
            operation_name: "",
            variables: &variables,
            actor,
        };
        let response = execute_with(&schema, request, Some(&roots));
        let body = normalized(&response);
        std::fs::write(Path::new(&out).join(format!("{}.json", case.name)), &body).expect("write");
        println!("RUST {} len={}", case.name, body.len());
    }
}
