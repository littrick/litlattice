#![forbid(unsafe_code)]

//! Production adapter for [`crate::graphql_resolvers::UserLookupStore`] and
//! [`crate::graphql_resolvers::UsersConnectionStore`]:
//! `db.Users().GetByUsername` / `GetByID` / `GetByVerifiedEmail` and
//! `db.Users().List` / `Count` (`internal/database/users.go:985-998,1070-1127`).

use std::num::IntErrorKind;

use async_trait::async_trait;
use sqlx::types::time::OffsetDateTime;
use sqlx::PgPool;
use time::format_description::well_known::Rfc3339;
use time::{Date, Duration, Month};

use crate::graphql_resolvers::{
    maybe_query_is_id, UserData, UserKey, UserLookupStore, UsersArgs, UsersConnectionData,
    UsersConnectionStore,
};
use sg_db::users::{
    count_users, find_user_by_username, find_user_by_verified_email, get_session_user,
    list_unique_users_between, list_users, UsersListOptions,
};

pub struct PgUserStore {
    pool: PgPool,
}

impl PgUserStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

pub(crate) fn to_user_data(record: sg_db::users::UserSessionRecord) -> UserData {
    UserData {
        id: record.id,
        username: record.username,
        display_name: record.display_name,
        avatar_url: record.avatar_url,
        created_at: record.created_at,
        updated_at: record.updated_at,
        site_admin: record.site_admin,
        tos_accepted: record.tos_accepted,
        builtin_auth: record.builtin_auth,
        scim_controlled: record.scim_controlled,
        completions_quota: record.completions_quota,
        code_completions_quota: record.code_completions_quota,
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

/// `strconv.ParseInt(after, 10, 32)`'s error text (`users.go:44-47`).
fn parse_cursor(raw: &str) -> Result<i32, String> {
    raw.parse::<i32>().map_err(|error| {
        let reason = match error.kind() {
            IntErrorKind::PosOverflow | IntErrorKind::NegOverflow => "value out of range",
            _ => "invalid syntax",
        };
        format!("strconv.ParseInt: parsing \"{raw}\": {reason}")
    })
}

/// `gqlutil.DateTime.UnmarshalGraphQL`: RFC 3339, normalised to UTC.
fn parse_datetime(raw: &str) -> Result<OffsetDateTime, String> {
    OffsetDateTime::parse(raw, &Rfc3339)
        .map(|value| value.to_offset(time::UtcOffset::UTC))
        .map_err(|error| error.to_string())
}

/// `timeutil.StartOfWeek(now, 0)`: the latest Sunday at 00:00 UTC.
fn week_start(today: Date) -> Date {
    today - Duration::days(i64::from(today.weekday().number_days_from_sunday()))
}

fn month_start(today: Date) -> Date {
    Date::from_calendar_date(today.year(), today.month(), 1).expect("first of month")
}

/// `start.AddDate(0, 1, 0)`.
fn next_month_start(today: Date) -> Date {
    let year = today.year();
    match today.month() {
        Month::December => {
            Date::from_calendar_date(year + 1, Month::January, 1).expect("first of month")
        }
        month => Date::from_calendar_date(year, month.next(), 1).expect("first of month"),
    }
}

#[async_trait]
impl UsersConnectionStore for PgUserStore {
    async fn list(&self, args: &UsersArgs) -> Result<UsersConnectionData, String> {
        let mut options = UsersListOptions {
            query: args.query.clone(),
            query_id: args.query.as_deref().and_then(maybe_query_is_id),
            exclude_sourcegraph_operators: true,
            ..UsersListOptions::default()
        };
        if let Some(inactive_since) = &args.inactive_since {
            options.inactive_since = Some(parse_datetime(inactive_since)?);
        }
        // `after` only applies when `first` set it (a `LimitOffset` exists).
        let offset = match (&args.after, args.first) {
            (Some(after), Some(_)) => parse_cursor(after)?,
            _ => 0,
        };
        if let Some(first) = args.first {
            options.limit_offset = Some((first, offset));
        }

        // `usagestats.ListRegisteredUsers{T}oday/ThisWeek/ThisMonth`.
        let today = OffsetDateTime::now_utc().date();
        match args.active_period.as_deref() {
            None | Some("ALL_TIME") => {}
            Some("TODAY") => {
                options.user_ids = Some(
                    list_unique_users_between(&self.pool, today, today + Duration::days(1))
                        .await
                        .map_err(|error| error.to_string())?,
                );
            }
            Some("THIS_WEEK") => {
                let start = week_start(today);
                options.user_ids = Some(
                    list_unique_users_between(&self.pool, start, start + Duration::days(7))
                        .await
                        .map_err(|error| error.to_string())?,
                );
            }
            Some("THIS_MONTH") => {
                options.user_ids = Some(
                    list_unique_users_between(
                        &self.pool,
                        month_start(today),
                        next_month_start(today),
                    )
                    .await
                    .map_err(|error| error.to_string())?,
                );
            }
            Some(other) => return Err(format!("unknown user active period {other}")),
        }

        let users = list_users(&self.pool, &options)
            .await
            .map_err(|error| error.to_string())?;
        let total_count = count_users(&self.pool, &options)
            .await
            .map_err(|error| error.to_string())?;

        Ok(UsersConnectionData {
            users: users.into_iter().map(to_user_data).collect(),
            total_count: total_count as i32,
            limit: args.first,
            offset,
        })
    }
}
