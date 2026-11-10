#![forbid(unsafe_code)]

//! Production adapter for [`crate::graphql_resolvers::UserLookupStore`]:
//! `db.Users().GetByUsername` / `GetByID` / `GetByVerifiedEmail`
//! (`internal/database/users.go:985-998`).

use async_trait::async_trait;
use sqlx::PgPool;

use crate::graphql_resolvers::{UserData, UserKey, UserLookupStore};
use sg_db::users::{find_user_by_username, find_user_by_verified_email, get_session_user};

pub struct PgUserStore {
    pool: PgPool,
}

impl PgUserStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn to_user_data(record: sg_db::users::UserSessionRecord) -> UserData {
    UserData {
        id: record.id,
        username: record.username,
        display_name: record.display_name,
        avatar_url: record.avatar_url,
        created_at: record.created_at,
        updated_at: record.updated_at,
        site_admin: record.site_admin,
        tos_accepted: record.tos_accepted,
        primary_email: record.email,
    }
}

#[async_trait]
impl UserLookupStore for PgUserStore {
    async fn lookup(&self, key: &UserKey) -> Result<Option<UserData>, String> {
        let record = match key {
            UserKey::Username(username) => find_user_by_username(&self.pool, username).await,
            UserKey::DatabaseId(id) => get_session_user(&self.pool, *id).await,
            UserKey::Email(email) => find_user_by_verified_email(&self.pool, email).await,
        }
        .map_err(|error| error.to_string())?;
        Ok(record.map(to_user_data))
    }
}
