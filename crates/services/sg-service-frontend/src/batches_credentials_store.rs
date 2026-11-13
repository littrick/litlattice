#![forbid(unsafe_code)]

//! Production Postgres adapter for
//! [`crate::batches_credentials::BatchesCredentialsDatabase`] (M9.30c).
//!
//! Reads and writes the existing `user_credentials` (domain `batches`) and
//! `batch_changes_site_credentials` tables, using the batch changes credential
//! keyring entry (`keyring.Default().BatchChangesCredentialKey`) through
//! `sg_encryption`. No table, column or index is added.

use std::sync::Arc;

use base64::Engine as _;
use rsa::pkcs1::EncodeRsaPrivateKey as _;
use rsa::traits::PublicKeyParts as _;
use sqlx::Row;

use sg_graphql::exec::Actor;
use time::OffsetDateTime;

use crate::batches_credentials::{
    BatchesCredentialsDatabase, CredentialRow, SshKeyPair,
};
use crate::batches_graphql_store::{block_on, PgBatchesGraphqlStore};

const USER_COLUMNS: &str = "id, user_id, external_service_type, external_service_id, \
     credential, encryption_key_id, github_app_id, created_at";

fn scan_user_row(row: &sqlx::postgres::PgRow) -> Result<CredentialRow, String> {
    Ok(CredentialRow {
        id: row.try_get(0).map_err(|e| e.to_string())?,
        user_id: row.try_get(1).map_err(|e| e.to_string())?,
        external_service_type: row.try_get(2).map_err(|e| e.to_string())?,
        external_service_id: row.try_get(3).map_err(|e| e.to_string())?,
        credential: row.try_get(4).map_err(|e| e.to_string())?,
        encryption_key_id: row.try_get(5).map_err(|e| e.to_string())?,
        github_app_id: row
            .try_get::<Option<i32>, _>(6)
            .map_err(|e| e.to_string())?
            .map(i64::from),
        created_at: row
            .try_get::<Option<OffsetDateTime>, _>(7)
            .map_err(|e| e.to_string())?
            .unwrap_or(OffsetDateTime::UNIX_EPOCH),
    })
}

const SITE_COLUMNS: &str = "id, external_service_type, external_service_id, credential, \
     encryption_key_id, github_app_id, created_at";

fn scan_site_row(row: &sqlx::postgres::PgRow) -> Result<CredentialRow, String> {
    Ok(CredentialRow {
        id: row.try_get(0).map_err(|e| e.to_string())?,
        user_id: 0,
        external_service_type: row.try_get(1).map_err(|e| e.to_string())?,
        external_service_id: row.try_get(2).map_err(|e| e.to_string())?,
        credential: row.try_get(3).map_err(|e| e.to_string())?,
        encryption_key_id: row.try_get(4).map_err(|e| e.to_string())?,
        github_app_id: row
            .try_get::<Option<i32>, _>(5)
            .map_err(|e| e.to_string())?
            .map(i64::from),
        created_at: row
            .try_get::<Option<OffsetDateTime>, _>(6)
            .map_err(|e| e.to_string())?
            .unwrap_or(OffsetDateTime::UNIX_EPOCH),
    })
}

enum UserAuthz {
    All,
    Same(i32),
}

impl PgBatchesGraphqlStore {
    /// `userCredentialsAuthzQueryConds` with `authz.enforceForSiteAdmins`
    /// defaulting to false.
    fn user_credential_authz(&self, actor: Actor) -> Result<UserAuthz, String> {
        if actor.is_internal() {
            return Ok(UserAuthz::All);
        }
        let Some(uid) = actor.uid() else {
            return Ok(UserAuthz::Same(0));
        };
        let site_admin =
            block_on(sg_db::users::get_site_admin(&self.pool, uid)).map_err(|e| e.to_string())?;
        if site_admin == Some(true) {
            Ok(UserAuthz::All)
        } else {
            Ok(UserAuthz::Same(uid))
        }
    }

    fn credential_key(&self) -> Option<Arc<dyn sg_encryption::Key>> {
        self.credential_keyring
            .as_ref()
            .and_then(|keyring| keyring.batch_changes_credential_key())
    }
}

impl BatchesCredentialsDatabase for PgBatchesGraphqlStore {
    fn list_user_credentials(&self, user_id: i32) -> Result<Vec<CredentialRow>, String> {
        let sql = format!(
            "SELECT {USER_COLUMNS} FROM user_credentials \
             WHERE domain = 'batches' AND user_id = $1 \
             ORDER BY created_at ASC, domain ASC, user_id ASC, external_service_id ASC"
        );
        let rows = block_on(sqlx::query(&sql).bind(user_id).fetch_all(&self.pool))
            .map_err(|e| e.to_string())?;
        rows.iter().map(scan_user_row).collect()
    }

    fn list_site_credentials(&self) -> Result<Vec<CredentialRow>, String> {
        let sql = format!(
            "SELECT {SITE_COLUMNS} FROM batch_changes_site_credentials \
             ORDER BY external_service_type ASC, external_service_id ASC"
        );
        let rows = block_on(sqlx::query(&sql).fetch_all(&self.pool))
            .map_err(|e| e.to_string())?;
        rows.iter().map(scan_site_row).collect()
    }

    fn get_user_credential(
        &self,
        id: i64,
        actor: Actor,
    ) -> Result<Option<CredentialRow>, String> {
        let authz = self.user_credential_authz(actor)?;
        let sql = format!(
            "SELECT {USER_COLUMNS} FROM user_credentials WHERE domain = 'batches' AND id = $1"
        );
        let row = block_on(sqlx::query(&sql).bind(id).fetch_optional(&self.pool))
            .map_err(|e| e.to_string())?;
        let Some(row) = row else { return Ok(None) };
        let parsed = scan_user_row(&row)?;
        match authz {
            UserAuthz::All => Ok(Some(parsed)),
            UserAuthz::Same(uid) if parsed.user_id == uid => Ok(Some(parsed)),
            UserAuthz::Same(_) => Ok(None),
        }
    }

    fn get_site_credential(&self, id: i64) -> Result<Option<CredentialRow>, String> {
        let sql = format!(
            "SELECT {SITE_COLUMNS} FROM batch_changes_site_credentials WHERE id = $1"
        );
        let row = block_on(sqlx::query(&sql).bind(id).fetch_optional(&self.pool))
            .map_err(|e| e.to_string())?;
        row.as_ref().map(scan_site_row).transpose()
    }

    fn get_site_credential_by_scope(
        &self,
        external_service_type: &str,
        external_service_id: &str,
    ) -> Result<Option<CredentialRow>, String> {
        let sql = format!(
            "SELECT {SITE_COLUMNS} FROM batch_changes_site_credentials \
             WHERE external_service_type = $1 AND external_service_id = $2"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(external_service_type)
                .bind(external_service_id)
                .fetch_optional(&self.pool),
        )
        .map_err(|e| e.to_string())?;
        row.as_ref().map(scan_site_row).transpose()
    }

    fn get_user_credential_by_scope(
        &self,
        user_id: i32,
        external_service_type: &str,
        external_service_id: &str,
    ) -> Result<Option<CredentialRow>, String> {
        let sql = format!(
            "SELECT {USER_COLUMNS} FROM user_credentials \
             WHERE domain = 'batches' AND user_id = $1 \
               AND external_service_type = $2 AND external_service_id = $3"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(user_id)
                .bind(external_service_type)
                .bind(external_service_id)
                .fetch_optional(&self.pool),
        )
        .map_err(|e| e.to_string())?;
        row.as_ref().map(scan_user_row).transpose()
    }

    fn create_user_credential(
        &self,
        user_id: i32,
        external_service_type: &str,
        external_service_id: &str,
        credential: &[u8],
        encryption_key_id: &str,
        github_app_id: Option<i64>,
    ) -> Result<CredentialRow, String> {
        let sql = format!(
            "INSERT INTO user_credentials \
                (domain, user_id, external_service_type, external_service_id, credential, \
                 encryption_key_id, github_app_id, created_at, updated_at, ssh_migration_applied) \
             VALUES ('batches', $1, $2, $3, $4, $5, $6, NOW(), NOW(), TRUE) \
             RETURNING {USER_COLUMNS}"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(user_id)
                .bind(external_service_type)
                .bind(external_service_id)
                .bind(credential.to_vec())
                .bind(encryption_key_id)
                .bind(github_app_id.map(|id| id as i32))
                .fetch_one(&self.pool),
        )
        .map_err(|e| e.to_string())?;
        scan_user_row(&row)
    }

    fn create_site_credential(
        &self,
        external_service_type: &str,
        external_service_id: &str,
        credential: &[u8],
        encryption_key_id: &str,
        github_app_id: Option<i64>,
    ) -> Result<CredentialRow, String> {
        let sql = format!(
            "INSERT INTO batch_changes_site_credentials \
                (external_service_type, external_service_id, credential, encryption_key_id, \
                 github_app_id, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5, NOW(), NOW()) \
             RETURNING {SITE_COLUMNS}"
        );
        let row = block_on(
            sqlx::query(&sql)
                .bind(external_service_type)
                .bind(external_service_id)
                .bind(credential.to_vec())
                .bind(encryption_key_id)
                .bind(github_app_id.map(|id| id as i32))
                .fetch_one(&self.pool),
        )
        .map_err(|e| e.to_string())?;
        scan_site_row(&row)
    }

    fn delete_user_credential(&self, id: i64, actor: Actor) -> Result<bool, String> {
        let authz = self.user_credential_authz(actor)?;
        let result = match authz {
            UserAuthz::All => block_on(
                sqlx::query("DELETE FROM user_credentials WHERE domain = 'batches' AND id = $1")
                    .bind(id)
                    .execute(&self.pool),
            ),
            UserAuthz::Same(uid) => block_on(
                sqlx::query(
                    "DELETE FROM user_credentials \
                     WHERE domain = 'batches' AND id = $1 AND user_id = $2",
                )
                .bind(id)
                .bind(uid)
                .execute(&self.pool),
            ),
        }
        .map_err(|e| e.to_string())?;
        Ok(result.rows_affected() > 0)
    }

    fn delete_site_credential(&self, id: i64) -> Result<bool, String> {
        let result = block_on(
            sqlx::query("DELETE FROM batch_changes_site_credentials WHERE id = $1")
                .bind(id)
                .execute(&self.pool),
        )
        .map_err(|e| e.to_string())?;
        Ok(result.rows_affected() > 0)
    }

    fn encrypt_credential(&self, plaintext: &str) -> Result<(Vec<u8>, String), String> {
        let key = self.credential_key();
        let (cipher, key_id) =
            block_on(sg_encryption::maybe_encrypt(key.as_deref(), plaintext))
                .map_err(|e| e.to_string())?;
        Ok((cipher.into_bytes(), key_id))
    }

    fn decrypt_credential(&self, ciphertext: &[u8], key_id: &str) -> Result<String, String> {
        let key = self.credential_key();
        let cipher = String::from_utf8_lossy(ciphertext);
        block_on(sg_encryption::maybe_decrypt(
            key.as_deref(),
            &cipher,
            key_id,
        ))
        .map_err(|e| e.to_string())
    }

    fn generate_ssh_keypair(&self) -> Result<SshKeyPair, String> {
        // `encryption.GenerateRSAKey`: 4096-bit RSA, UUID passphrase, OpenSSH
        // authorized_keys public key.
        let mut rng = rand::thread_rng();
        let private =
            rsa::RsaPrivateKey::new(&mut rng, 4096).map_err(|e| e.to_string())?;
        let public = private.to_public_key();
        let private_pem = private
            .to_pkcs1_pem(rsa::pkcs1::LineEnding::LF)
            .map_err(|e| e.to_string())?
            .to_string();
        let passphrase = uuid_like(&mut rng);
        let public_key = openssh_public_key(&public);
        Ok(SshKeyPair {
            private_key: private_pem,
            public_key,
            passphrase,
        })
    }
}

/// `uuid.NewRandom().String()` shape without pulling in the uuid crate.
fn uuid_like<R: rand::Rng + ?Sized>(rng: &mut R) -> String {
    let mut bytes = [0u8; 16];
    rng.fill(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-\
         {:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
    )
}

/// `ssh.NewPublicKey` + `ssh.MarshalAuthorizedKey`: `ssh-rsa <base64> \n`.
fn openssh_public_key(key: &rsa::RsaPublicKey) -> String {
    fn put_string(out: &mut Vec<u8>, bytes: &[u8]) {
        out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        out.extend_from_slice(bytes);
    }
    fn put_mpint(out: &mut Vec<u8>, bytes: &[u8]) {
        let mut bytes = bytes;
        if !bytes.is_empty() && bytes[0] & 0x80 != 0 {
            let mut prefixed = Vec::with_capacity(bytes.len() + 1);
            prefixed.push(0);
            prefixed.extend_from_slice(bytes);
            put_string(out, &prefixed);
        } else {
            put_string(out, bytes);
        }
        let _ = &mut bytes;
    }
    let mut blob = Vec::new();
    put_string(&mut blob, b"ssh-rsa");
    put_mpint(&mut blob, &key.e().to_bytes_be());
    put_mpint(&mut blob, &key.n().to_bytes_be());
    let encoded = base64::engine::general_purpose::STANDARD.encode(blob);
    format!("ssh-rsa {encoded}\n")
}
