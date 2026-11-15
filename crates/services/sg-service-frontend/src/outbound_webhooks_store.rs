#![forbid(unsafe_code)]

//! Production Postgres adapter for
//! [`crate::outbound_webhooks_graphql::OutboundWebhooksGraphqlDatabase`]
//! (M9.27b).
//!
//! Bridges the synchronous GraphQL seam to [`sg_db::outbound_webhooks`] with
//! `tokio::task::block_in_place` (the same pattern as the GitHub Apps
//! adapter). `url`/`secret` stay raw in the database; `decrypt_url` runs
//! `encryption.MaybeDecrypt` with `keyring.Default().OutboundWebhookKey`
//! (always `None` in the baseline, so encrypted rows fail exactly like Go).
//!
//! No table, column or Redis key is added.

use std::future::Future;
use std::sync::Arc;

use sg_graphql::exec::Actor;
use sqlx::PgPool;

use crate::outbound_webhooks_graphql::{
    FilterEventType, LogListOpts, OutboundWebhookJobData, OutboundWebhookLogData,
    OutboundWebhooksGraphqlDatabase, ScopedEventTypeData, WebhookData,
};

fn block_on<F: Future>(future: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("outbound webhooks blocking runtime")
            .block_on(future),
    }
}

pub struct OutboundWebhooksPgDatabase {
    pool: PgPool,
    keyring: Arc<sg_keyring::Keyring>,
}

impl OutboundWebhooksPgDatabase {
    pub fn new(pool: PgPool, keyring: Arc<sg_keyring::Keyring>) -> Self {
        Self { pool, keyring }
    }

    fn map_row(row: sg_db::outbound_webhooks::OutboundWebhook) -> WebhookData {
        WebhookData {
            id: row.id,
            url: row.url,
            encryption_key_id: row.encryption_key_id,
            event_types: row
                .event_types
                .into_iter()
                .map(|event_type| ScopedEventTypeData {
                    event_type: event_type.event_type,
                    scope: event_type.scope,
                })
                .collect(),
        }
    }

    fn db_filters(filters: &[FilterEventType]) -> Vec<sg_db::outbound_webhooks::FilterEventType> {
        filters
            .iter()
            .map(|filter| sg_db::outbound_webhooks::FilterEventType {
                event_type: filter.event_type.clone(),
                scope: filter.scope.clone(),
            })
            .collect()
    }

    fn db_event_types(
        event_types: &[ScopedEventTypeData],
    ) -> Vec<sg_db::outbound_webhooks::ScopedEventType> {
        event_types
            .iter()
            .map(|event_type| sg_db::outbound_webhooks::ScopedEventType {
                event_type: event_type.event_type.clone(),
                scope: event_type.scope.clone(),
            })
            .collect()
    }

    fn map_log(row: sg_db::outbound_webhooks::OutboundWebhookLog) -> OutboundWebhookLogData {
        OutboundWebhookLogData {
            id: row.id,
            job_id: row.job_id,
            sent_at: row.sent_at,
            status_code: row.status_code,
            encryption_key_id: row.encryption_key_id,
            request: row.request,
            response: row.response,
            error: row.error,
        }
    }

    fn map_job(row: sg_db::outbound_webhooks::OutboundWebhookJob) -> OutboundWebhookJobData {
        OutboundWebhookJobData {
            id: row.id,
            event_type: row.event_type,
            scope: row.scope,
            encryption_key_id: row.encryption_key_id,
            payload: row.payload,
        }
    }
}

impl OutboundWebhooksGraphqlDatabase for OutboundWebhooksPgDatabase {
    fn current_user_site_admin(&self, actor: Actor) -> Result<Option<bool>, String> {
        match actor {
            Actor::Internal => Ok(Some(true)),
            Actor::Anonymous => Ok(None),
            Actor::User { uid } => block_on(sg_db::users::get_site_admin(&self.pool, uid))
                .map_err(|err| err.to_string()),
        }
    }

    fn list_webhooks(
        &self,
        opts: &crate::outbound_webhooks_graphql::ListOpts,
    ) -> Result<Vec<WebhookData>, String> {
        let opts = sg_db::outbound_webhooks::OutboundWebhookListOpts {
            count: sg_db::outbound_webhooks::OutboundWebhookCountOpts {
                event_types: Self::db_filters(&opts.count.event_types),
            },
            limit: opts.limit,
            offset: opts.offset,
        };
        let rows = block_on(sg_db::outbound_webhooks::list(&self.pool, &opts))?;
        Ok(rows.into_iter().map(Self::map_row).collect())
    }

    fn count_webhooks(
        &self,
        opts: &crate::outbound_webhooks_graphql::CountOpts,
    ) -> Result<i64, String> {
        let opts = sg_db::outbound_webhooks::OutboundWebhookCountOpts {
            event_types: Self::db_filters(&opts.event_types),
        };
        block_on(sg_db::outbound_webhooks::count(&self.pool, &opts))
    }

    fn get_webhook(&self, id: i64) -> Result<Option<WebhookData>, String> {
        let row = block_on(sg_db::outbound_webhooks::get_by_id(&self.pool, id))?;
        Ok(row.map(Self::map_row))
    }

    fn create_webhook(
        &self,
        created_by: i32,
        updated_by: i32,
        url: &str,
        secret: &str,
        event_types: &[ScopedEventTypeData],
    ) -> Result<WebhookData, String> {
        let rows = Self::db_event_types(event_types);
        let row = block_on(sg_db::outbound_webhooks::create(
            &self.pool,
            created_by,
            updated_by,
            None,
            url.as_bytes(),
            secret.as_bytes(),
            &rows,
        ))?;
        Ok(Self::map_row(row))
    }

    fn update_webhook(
        &self,
        id: i64,
        updated_by: i32,
        url: &str,
        event_types: &[ScopedEventTypeData],
    ) -> Result<Option<WebhookData>, String> {
        let Some((secret, encryption_key_id)) =
            block_on(sg_db::outbound_webhooks::secret_for_update(&self.pool, id))?
        else {
            return Ok(None);
        };
        // `encryptFields`: the new URL encrypts under the nil key (key id ""),
        // while the preserved secret keeps its stored key id. Differing key
        // ids are rejected by the baseline.
        if !encryption_key_id.is_empty() {
            return Err("different key IDs returned when using the same key".to_owned());
        }
        let rows = Self::db_event_types(event_types);
        let row = block_on(sg_db::outbound_webhooks::update(
            &self.pool,
            id,
            updated_by,
            url.as_bytes(),
            &secret,
            None,
            &rows,
        ))?;
        Ok(row.map(Self::map_row))
    }

    fn delete_webhook(&self, id: i64) -> Result<(), String> {
        block_on(sg_db::outbound_webhooks::delete(&self.pool, id))
    }

    fn decrypt_url(&self, raw: &[u8], encryption_key_id: &str) -> Result<String, String> {
        let data = String::from_utf8_lossy(raw).into_owned();
        if encryption_key_id.is_empty()
            || encryption_key_id == sg_encryption::UNMIGRATED_ENCRYPTION_KEY_ID
            || data.is_empty()
        {
            return Ok(data);
        }
        let key = self.keyring.outbound_webhook_key();
        block_on(sg_encryption::maybe_decrypt(
            key.as_deref(),
            &data,
            encryption_key_id,
        ))
        .map_err(|err| err.to_string())
    }

    fn counts_for_outbound_webhook(&self, id: i64) -> Result<(i64, i64), String> {
        block_on(sg_db::outbound_webhooks::counts_for_outbound_webhook(
            &self.pool,
            id,
        ))
    }

    fn list_logs(
        &self,
        opts: &LogListOpts,
    ) -> Result<Vec<OutboundWebhookLogData>, String> {
        let opts = sg_db::outbound_webhooks::OutboundWebhookLogListOpts {
            limit: opts.limit,
            offset: opts.offset,
            only_errors: opts.only_errors,
            outbound_webhook_id: opts.outbound_webhook_id,
        };
        let rows = block_on(sg_db::outbound_webhooks::list_logs(&self.pool, &opts))?;
        Ok(rows.into_iter().map(Self::map_log).collect())
    }

    fn get_job(&self, id: i64) -> Result<Option<OutboundWebhookJobData>, String> {
        let row = block_on(sg_db::outbound_webhooks::get_job(&self.pool, id))?;
        Ok(row.map(Self::map_job))
    }
}
