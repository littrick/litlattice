#![forbid(unsafe_code)]

//! `Query.surveyResponses` and `Mutation.submitSurvey` /
//! `Mutation.submitHappinessFeedback`
//! (`cmd/frontend/graphqlbackend/{survey_responses,survey_response}.go`,
//! `internal/database/survey_responses.go`).
//!
//! The connection is site-admin gated per field; `GetAll` ignores the
//! `first`/`after` connection arguments (there is no LIMIT/OFFSET in the
//! baseline). The 30-day statistics use UTC midnight minus 30 days and round
//! the promoter/detractor percentages separately before subtracting.

use std::sync::Arc;

use sg_graphql::exec::{Actor, Arguments, ExecError, FieldValue, ObjectValue, RootField};
use sg_graphql::json::Json;
use sqlx::{PgPool, Row};
use time::{Duration, OffsetDateTime};

use crate::admin_store::check_current_user_is_site_admin;
use crate::graphql_resolvers::{datetime_rfc3339, UserData, UserObject};

/// One `survey_responses` row (`types.SurveyResponse`).
#[derive(Debug, Clone, PartialEq)]
pub struct SurveyResponseData {
    pub id: i64,
    pub user_id: Option<i32>,
    pub email: Option<String>,
    pub score: i32,
    pub reason: Option<String>,
    pub better: Option<String>,
    pub other_use_case: Option<String>,
    pub created_at: OffsetDateTime,
}

/// Prefetched `surveyResponses` connection fields.
#[derive(Debug, Clone, PartialEq)]
pub struct SurveyResponsesPrefetch {
    /// `Nodes` (site-admin gated).
    pub responses: Result<Vec<SurveyResponseData>, String>,
    pub total_count: Result<i64, String>,
    pub last30_days_count: Result<i64, String>,
    pub average_score: Result<f64, String>,
    pub net_promoter_score: Result<i32, String>,
    /// `SurveyResponse.user` lookups keyed by `users.id`.
    pub users: std::collections::HashMap<i32, Option<UserData>>,
}

fn thirty_days_ago() -> OffsetDateTime {
    let now = OffsetDateTime::now_utc();
    let midnight = now.replace_time(time::Time::MIDNIGHT);
    midnight - Duration::days(30)
}

/// `SurveyResponses` + `surveyResponseConnectionResolver` field prefetch.
pub async fn prefetch_survey_responses(
    pool: &PgPool,
    users: Option<&dyn crate::graphql_resolvers::UserLookupStore>,
    fields: &[RootField],
    actor: Actor,
    viewer_site_admin: bool,
) -> Option<SurveyResponsesPrefetch> {
    let field = fields
        .iter()
        .find(|field| field.name == "surveyResponses")?;
    let mut data = SurveyResponsesPrefetch {
        responses: Err(String::new()),
        total_count: Err(String::new()),
        last30_days_count: Err(String::new()),
        average_score: Err(String::new()),
        net_promoter_score: Err(String::new()),
        users: std::collections::HashMap::new(),
    };
    if let Err(message) = check_current_user_is_site_admin(actor, viewer_site_admin) {
        data.responses = Err(message.clone());
        data.total_count = Err(message.clone());
        data.last30_days_count = Err(message.clone());
        data.average_score = Err(message.clone());
        data.net_promoter_score = Err(message);
        return Some(data);
    }

    // The connection arguments are accepted but never applied by `GetAll`.
    let _ = field.arguments.i64("first");
    let _ = field.arguments.string("after");

    data.responses = load_all(pool).await;
    data.total_count = count_all(pool).await;
    data.last30_days_count = count_last_30_days(pool).await;
    data.average_score = average_last_30_days(pool).await;
    data.net_promoter_score = async {
        let count = count_last_30_days(pool).await?;
        if count == 0 {
            return Ok(0);
        }
        let promoters = count_score(pool, ">", 8).await?;
        let detractors = count_score(pool, "<", 7).await?;
        let count = count as f64;
        let promoter_percent = ((promoters as f64) / count * 100.0).round();
        let detractor_percent = ((detractors as f64) / count * 100.0).round();
        Ok((promoter_percent - detractor_percent) as i32)
    }
    .await;

    if let (Ok(responses), Some(users)) = (&data.responses, users) {
        for response in responses {
            if let Some(user_id) = response.user_id {
                if !data.users.contains_key(&user_id) {
                    let lookup = match users
                        .lookup(&crate::graphql_resolvers::UserKey::DatabaseId(user_id))
                        .await
                    {
                        Ok(lookup) => lookup,
                        Err(_) => None,
                    };
                    data.users.insert(user_id, lookup);
                }
            }
        }
    }
    Some(data)
}

async fn load_all(pool: &PgPool) -> Result<Vec<SurveyResponseData>, String> {
    let rows = sqlx::query(
        "SELECT id, user_id, email, score, reason, better, other_use_case, created_at \
         FROM survey_responses ORDER BY created_at DESC",
    )
    .fetch_all(pool)
    .await
    .map_err(|error| error.to_string())?;
    Ok(rows
        .into_iter()
        .map(|row| SurveyResponseData {
            id: row.get("id"),
            user_id: row.get("user_id"),
            email: row.get("email"),
            score: row.get("score"),
            reason: row.get("reason"),
            better: row.get("better"),
            other_use_case: row.get("other_use_case"),
            created_at: row.get("created_at"),
        })
        .collect())
}

async fn count_all(pool: &PgPool) -> Result<i64, String> {
    sqlx::query_scalar("SELECT COUNT(*) FROM survey_responses")
        .fetch_one(pool)
        .await
        .map_err(|error| error.to_string())
}

async fn count_last_30_days(pool: &PgPool) -> Result<i64, String> {
    sqlx::query_scalar("SELECT COUNT(*) FROM survey_responses WHERE created_at > $1")
        .bind(thirty_days_ago())
        .fetch_one(pool)
        .await
        .map_err(|error| error.to_string())
}

async fn average_last_30_days(pool: &PgPool) -> Result<f64, String> {
    // `AVG` over no rows is NULL; `sql.NullFloat64` scans it as 0.
    let average: Option<f64> =
        sqlx::query_scalar("SELECT AVG(score)::float8 FROM survey_responses WHERE created_at > $1")
            .bind(thirty_days_ago())
            .fetch_one(pool)
            .await
            .map_err(|error| error.to_string())?;
    Ok(average.unwrap_or(0.0))
}

async fn count_score(pool: &PgPool, op: &str, score: i32) -> Result<i64, String> {
    let query =
        format!("SELECT COUNT(*) FROM survey_responses WHERE created_at > $1 AND score {op} $2");
    sqlx::query_scalar(&query)
        .bind(thirty_days_ago())
        .bind(score)
        .fetch_one(pool)
        .await
        .map_err(|error| error.to_string())
}

pub struct SurveyResponseConnectionObject {
    pub data: SurveyResponsesPrefetch,
}

impl ObjectValue for SurveyResponseConnectionObject {
    fn type_name(&self) -> &'static str {
        "SurveyResponseConnection"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "nodes" => {
                let responses = match &self.data.responses {
                    Ok(responses) => responses,
                    Err(message) => return Err(ExecError::new(message.clone())),
                };
                Ok(Some(FieldValue::List {
                    element_type: Some("SurveyResponse"),
                    items: responses
                        .iter()
                        .map(|response| {
                            let user = response
                                .user_id
                                .and_then(|id| self.data.users.get(&id).cloned().flatten());
                            FieldValue::Object(Arc::new(SurveyResponseObject {
                                response: response.clone(),
                                user,
                            }))
                        })
                        .collect(),
                }))
            }
            "totalCount" => Ok(Some(FieldValue::Json(Json::Int(
                self.data.total_count.clone().map_err(ExecError::new)?,
            )))),
            "last30DaysCount" => Ok(Some(FieldValue::Json(Json::Int(
                self.data
                    .last30_days_count
                    .clone()
                    .map_err(ExecError::new)?,
            )))),
            "averageScore" => Ok(Some(FieldValue::Json(Json::Float(
                self.data.average_score.clone().map_err(ExecError::new)?,
            )))),
            "netPromoterScore" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.data
                    .net_promoter_score
                    .clone()
                    .map_err(ExecError::new)?,
            ))))),
            _ => Ok(None),
        }
    }
}

pub struct SurveyResponseObject {
    pub response: SurveyResponseData,
    pub user: Option<UserData>,
}

impl ObjectValue for SurveyResponseObject {
    fn type_name(&self) -> &'static str {
        "SurveyResponse"
    }

    fn field(
        &self,
        field: &str,
        _arguments: &Arguments,
        _actor: Actor,
    ) -> Result<Option<FieldValue>, ExecError> {
        match field {
            "id" => Ok(Some(FieldValue::Json(Json::Str(marshal_relay_id(
                "SurveyResponse",
                self.response.id,
            ))))),
            "user" => Ok(Some(match &self.user {
                Some(user) => FieldValue::Object(Arc::new(UserObject::from_data_as_viewer(
                    user.clone(),
                    false,
                ))),
                None => FieldValue::Null,
            })),
            "email" => Ok(Some(optional_string(self.response.email.as_deref()))),
            "score" => Ok(Some(FieldValue::Json(Json::Int(i64::from(
                self.response.score,
            ))))),
            "reason" => Ok(Some(optional_string(self.response.reason.as_deref()))),
            "better" => Ok(Some(optional_string(self.response.better.as_deref()))),
            "otherUseCase" => Ok(Some(optional_string(
                self.response.other_use_case.as_deref(),
            ))),
            "createdAt" => Ok(Some(FieldValue::Json(Json::Str(datetime_rfc3339(
                self.response.created_at,
            ))))),
            _ => Ok(None),
        }
    }
}

fn optional_string(value: Option<&str>) -> FieldValue {
    match value {
        Some(value) => FieldValue::Json(Json::Str(value.to_owned())),
        None => FieldValue::Null,
    }
}

/// `Mutation.submitSurvey` / `Mutation.submitHappinessFeedback`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SurveyMutationOutcome {
    Empty,
    Error(String),
}

pub async fn prefetch_survey_mutations(
    pool: &PgPool,
    fields: &[RootField],
    actor: Actor,
) -> Vec<(String, SurveyMutationOutcome)> {
    let mut outcomes = Vec::new();
    for field in fields {
        let outcome = match field.name.as_str() {
            "submitSurvey" => submit_survey(pool, field, actor).await,
            // The baseline only forwards this to HubSpot; the GraphQL result
            // is always the non-nil `EmptyResponse`.
            "submitHappinessFeedback" => SurveyMutationOutcome::Empty,
            _ => continue,
        };
        outcomes.push((field.name.clone(), outcome));
    }
    outcomes
}

async fn submit_survey(pool: &PgPool, field: &RootField, actor: Actor) -> SurveyMutationOutcome {
    let input = match field.arguments.get("input") {
        Some(sg_graphql::exec::InputValue::Object(map)) => map,
        _ => return SurveyMutationOutcome::Error("missing input".to_owned()),
    };
    let Some(score) = input
        .get("score")
        .and_then(sg_graphql::exec::InputValue::as_i64)
    else {
        return SurveyMutationOutcome::Error("missing score".to_owned());
    };
    if !(0..=10).contains(&score) {
        return SurveyMutationOutcome::Error("Score must be a value between 0 and 10".to_owned());
    }
    let other_use_case = input
        .get("otherUseCase")
        .and_then(sg_graphql::exec::InputValue::as_str)
        .map(str::to_owned);
    let better = input
        .get("better")
        .and_then(sg_graphql::exec::InputValue::as_str)
        .map(str::to_owned);
    let mut email = input
        .get("email")
        .and_then(sg_graphql::exec::InputValue::as_str)
        .map(str::to_owned);
    let mut user_id = None;
    if let Some(uid) = actor.uid() {
        user_id = Some(uid);
        // `GetPrimaryEmail` errors that are not `ErrNoRows` propagate; the
        // primary email overwrites the manually supplied one.
        match sqlx::query_scalar::<_, String>(
            "SELECT email FROM user_emails WHERE user_id = $1 AND is_primary",
        )
        .bind(uid)
        .fetch_optional(pool)
        .await
        {
            Ok(Some(primary)) => email = Some(primary),
            Ok(None) => {}
            Err(error) => return SurveyMutationOutcome::Error(error.to_string()),
        }
    }
    match sqlx::query(
        "INSERT INTO survey_responses(user_id, email, score, other_use_case, better) \
         VALUES($1, $2, $3, $4, $5)",
    )
    .bind(user_id)
    .bind(email)
    .bind(score as i32)
    .bind(other_use_case)
    .bind(better)
    .execute(pool)
    .await
    {
        Ok(_) => SurveyMutationOutcome::Empty,
        Err(error) => SurveyMutationOutcome::Error(error.to_string()),
    }
}

/// `relay.MarshalID("SurveyResponse", int64)`.
fn marshal_relay_id(kind: &str, id: i64) -> String {
    use base64::Engine as _;
    let spec = serde_json::to_string(&id).expect("int json");
    base64::engine::general_purpose::URL_SAFE.encode(format!("{kind}:{spec}"))
}
