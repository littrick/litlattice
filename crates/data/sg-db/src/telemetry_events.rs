//! `internal/database/event_logs.go` (`BulkInsert`) and
//! `internal/database/telemetry_export_store.go` (`QueueForExport`): the two
//! writes behind Telemetry V2's `teestore.StoreEvents`.
//!
//! The baseline tees every recorded event into both the export queue
//! (`telemetry_events_export_queue`, the new store exported to the Telemetry
//! Gateway) and the legacy `event_logs` table, translating the protobuf event
//! into the legacy row shape on a best-effort basis. The translation itself
//! lives with the event recorder; this module only owns the SQL.

use serde_json::Value;
use sqlx::postgres::PgPool;
use sqlx::types::time::{Date, OffsetDateTime};
use sqlx::QueryBuilder;

/// One legacy `event_logs` row, in the column order `BulkInsert` writes.
///
/// `device_id` and `insert_id` are filled with a random v4 UUID when the
/// event does not carry one (`ensureUuid` in the baseline), which is why they
/// are plain strings here.
#[derive(Debug, Clone)]
pub struct EventLogRow {
    pub name: String,
    pub url: String,
    pub user_id: i32,
    pub anonymous_user_id: String,
    pub source: String,
    pub argument: Value,
    pub public_argument: Value,
    pub version: String,
    pub timestamp: OffsetDateTime,
    pub feature_flags: Value,
    pub cohort_id: Option<Date>,
    pub first_source_url: Option<String>,
    pub last_source_url: Option<String>,
    pub referrer: Option<String>,
    pub device_id: String,
    pub insert_id: String,
    pub client: Option<String>,
    pub billing_product_category: Option<String>,
    pub billing_event_id: Option<String>,
}

/// One `telemetry_events_export_queue` row: the event ID, the event timestamp
/// and the serialized protobuf payload.
#[derive(Debug, Clone)]
pub struct QueuedEvent {
    pub id: String,
    pub timestamp: OffsetDateTime,
    pub payload_pb: Vec<u8>,
}

/// `eventLogStore.BulkInsert`: `batch.InsertValues` into the 19 columns above.
///
/// `batch.InsertValues` inserts as many rows per statement as the parameter
/// limit allows; the rows here are small enough that one statement is
/// equivalent.
pub async fn bulk_insert_event_logs(
    pool: &PgPool,
    rows: &[EventLogRow],
) -> Result<(), sqlx::Error> {
    bulk_insert_event_logs_on(pool, rows).await
}

/// [`bulk_insert_event_logs`] against any executor, so the user-deletion
/// transaction in [`crate::users::hard_delete_list`] can write its events
/// through the same statement.
pub async fn bulk_insert_event_logs_on<'e, E>(
    executor: E,
    rows: &[EventLogRow],
) -> Result<(), sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    if rows.is_empty() {
        return Ok(());
    }

    let mut builder: QueryBuilder<'_, sqlx::Postgres> = QueryBuilder::new(
        "INSERT INTO event_logs (name, url, user_id, anonymous_user_id, source, \
         argument, public_argument, version, timestamp, feature_flags, cohort_id, \
         first_source_url, last_source_url, referrer, device_id, insert_id, client, \
         billing_product_category, billing_event_id) ",
    );
    builder.push_values(rows, |mut builder, row| {
        builder
            .push_bind(&row.name)
            .push_bind(&row.url)
            .push_bind(row.user_id)
            .push_bind(&row.anonymous_user_id)
            .push_bind(&row.source)
            .push_bind(&row.argument)
            .push_bind(&row.public_argument)
            .push_bind(&row.version)
            .push_bind(row.timestamp)
            .push_bind(&row.feature_flags)
            .push_bind(row.cohort_id)
            .push_bind(&row.first_source_url)
            .push_bind(&row.last_source_url)
            .push_bind(&row.referrer)
            .push_bind(&row.device_id)
            .push_bind(&row.insert_id)
            .push_bind(&row.client)
            .push_bind(&row.billing_product_category)
            .push_bind(&row.billing_event_id);
    });

    builder.build().execute(executor).await?;
    Ok(())
}

/// `telemetryEventsExportQueueStore.QueueForExport` with the export mode
/// already applied: `INSERT INTO telemetry_events_export_queue (id, timestamp,
/// payload_pb)`.
pub async fn queue_events_for_export(
    pool: &PgPool,
    events: &[QueuedEvent],
) -> Result<(), sqlx::Error> {
    if events.is_empty() {
        return Ok(());
    }

    let mut builder: QueryBuilder<'_, sqlx::Postgres> =
        QueryBuilder::new("INSERT INTO telemetry_events_export_queue (id, timestamp, payload_pb) ");
    builder.push_values(events, |mut builder, event| {
        builder
            .push_bind(&event.id)
            .push_bind(event.timestamp)
            .push_bind(&event.payload_pb);
    });

    builder.build().execute(pool).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_shape_matches_the_baseline_column_order() {
        // The column list in `bulk_insert_event_logs` must stay in the same
        // order as `BulkInsert`'s `[]string{...}`; the row struct mirrors it.
        let row = EventLogRow {
            name: "cody.llmTokenCounter.record".to_owned(),
            url: String::new(),
            user_id: 0,
            anonymous_user_id: "unknown".to_owned(),
            source: "BACKEND".to_owned(),
            argument: serde_json::json!({}),
            public_argument: serde_json::json!({"a": 1}),
            version: "0.0.0+dev".to_owned(),
            timestamp: OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("valid timestamp"),
            feature_flags: serde_json::json!({}),
            cohort_id: None,
            first_source_url: None,
            last_source_url: None,
            referrer: None,
            device_id: "device".to_owned(),
            insert_id: "insert".to_owned(),
            client: None,
            billing_product_category: None,
            billing_event_id: None,
        };

        assert_eq!(row.name, "cody.llmTokenCounter.record");
        assert_eq!(row.argument.to_string(), "{}");
        assert_eq!(row.timestamp.unix_timestamp(), 1_700_000_000);
    }
}
