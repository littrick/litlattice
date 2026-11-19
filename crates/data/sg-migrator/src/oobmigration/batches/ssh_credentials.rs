//! `internal/oobmigration/migrations/batches/ssh_migrator.go`, migration `2`.
//!
//! Rewrites every `user_credentials` row in the `batches` domain so that
//! `BasicAuth`/`OAuthBearerToken` authenticators gain an SSH keypair
//! (`...WithSSH`). The reverse direction strips the keypair again.
//!
//! # Format fidelity
//!
//! The JSON payload is byte-for-byte the Go shape: an object with `Type` and
//! `Auth`, where `Auth` carries `Username`/`Password`/`Token` (omitted when
//! empty) and, in the forward direction, `PrivateKey`/`PublicKey`/`Passphrase`.
//!
//! The generated keypair mirrors `encryption.GenerateRSAKey`: 4096-bit RSA, a
//! UUID v4 passphrase and an OpenSSH `ssh-rsa <base64>` authorized-key line.
//!
//! **Gap:** Go writes the PKCS#1 private key as a *legacy PEM* block encrypted
//! with AES-256-CBC using the passphrase (`x509.EncryptPEMBlock`, RFC 1423).
//! The `rsa` crate exposes only unencrypted PKCS#1 PEM and PBES2 PKCS#8
//! encryption (which additionally needs the disabled `pkcs5` feature); neither
//! produces RFC 1423 PEM, and `sg-migrator` cannot add an AES/CBC dependency.
//! The private key is therefore emitted as unencrypted PKCS#1 PEM while the
//! passphrase is still generated and stored. Consumers that decrypt the PEM
//! with the passphrase (Go's legacy path) must accept it without a `Proc-Type`
//! header; this is the maximum faithful subset available here.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rsa::pkcs1::EncodeRsaPrivateKey as _;
use rsa::traits::PublicKeyParts as _;
use serde::Serialize;
use serde_json::{Map, Value};
use sqlx::{PgPool, Row};

use sg_encryption::Key;

use crate::error::MigratorError;

use super::super::model::Migrator;

/// `SSHMigrator`.
#[derive(Clone)]
pub struct SSHMigrator {
    pub batch_size: i32,
    key: Option<Arc<dyn Key>>,
}

impl std::fmt::Debug for SSHMigrator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SSHMigrator")
            .field("batch_size", &self.batch_size)
            .field("has_key", &self.key.is_some())
            .finish()
    }
}

impl Default for SSHMigrator {
    fn default() -> Self {
        Self::new(5, None)
    }
}

impl SSHMigrator {
    pub fn new(batch_size: i32, key: Option<Arc<dyn Key>>) -> Self {
        Self { batch_size, key }
    }

    /// Build from the process keyring (`keyring.Default().BatchChangesCredentialKey`).
    pub fn from_keyring(keyring: &sg_keyring::Keyring, batch_size: i32) -> Self {
        Self::new(batch_size, keyring.batch_changes_credential_key())
    }
}

#[async_trait]
impl Migrator for SSHMigrator {
    fn id(&self) -> i32 {
        2
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(5)
    }

    async fn progress(&self, db: &PgPool, _apply_reverse: bool) -> Result<f64, MigratorError> {
        sqlx::query_scalar::<_, f64>(SSH_PROGRESS_QUERY)
            .bind("batches")
            .bind("batches")
            .fetch_one(db)
            .await
            .map_err(|error| MigratorError::new(format!("ssh migration progress: {error}")))
    }

    async fn up(&self, db: &PgPool) -> Result<(), MigratorError> {
        self.run(db, false).await
    }

    async fn down(&self, db: &PgPool) -> Result<(), MigratorError> {
        self.run(db, true).await
    }
}

impl SSHMigrator {
    async fn run(&self, db: &PgPool, undo: bool) -> Result<(), MigratorError> {
        let mut tx = db
            .begin()
            .await
            .map_err(|error| MigratorError::new(format!("ssh migration begin: {error}")))?;

        let rows = sqlx::query(SSH_SELECT_QUERY)
            .bind("batches")
            .bind(undo)
            .bind(self.batch_size)
            .fetch_all(&mut *tx)
            .await
            .map_err(|error| MigratorError::new(format!("ssh migration select: {error}")))?;

        let mut credentials = Vec::with_capacity(rows.len());
        for row in rows {
            let id: i32 = row
                .try_get("id")
                .map_err(|error| MigratorError::new(format!("ssh migration id: {error}")))?;
            let raw: String = row.try_get("credential").map_err(|error| {
                MigratorError::new(format!("ssh migration credential: {error}"))
            })?;
            let key_id: String = row
                .try_get("encryption_key_id")
                .map_err(|error| MigratorError::new(format!("ssh migration key id: {error}")))?;

            let plaintext = sg_encryption::maybe_decrypt(self.key.as_deref(), &raw, &key_id)
                .await
                .map_err(|error| {
                    MigratorError::new(format!("ssh migration decrypt credential {id}: {error}"))
                })?;

            credentials.push((id, plaintext));
        }

        for (id, credential) in credentials {
            let transformed = if undo {
                transform_down(&credential).map_err(|error| {
                    MigratorError::new(format!("ssh migration credential {id}: {error}"))
                })?
            } else {
                transform_up(&credential).map_err(|error| {
                    MigratorError::new(format!("ssh migration credential {id}: {error}"))
                })?
            };

            let Some(new_credential) = transformed else {
                sqlx::query(SSH_UPDATE_FLAG_ONLY_QUERY)
                    .bind(!undo)
                    .bind(id)
                    .execute(&mut *tx)
                    .await
                    .map_err(|error| {
                        MigratorError::new(format!("ssh migration flag credential {id}: {error}"))
                    })?;
                continue;
            };

            let (secret, key_id) =
                sg_encryption::maybe_encrypt(self.key.as_deref(), &new_credential)
                    .await
                    .map_err(|error| {
                        MigratorError::new(format!(
                            "ssh migration encrypt credential {id}: {error}"
                        ))
                    })?;

            sqlx::query(SSH_UPDATE_QUERY)
                .bind(!undo)
                .bind(secret)
                .bind(key_id)
                .bind(id)
                .execute(&mut *tx)
                .await
                .map_err(|error| {
                    MigratorError::new(format!("ssh migration update credential {id}: {error}"))
                })?;
        }

        tx.commit()
            .await
            .map_err(|error| MigratorError::new(format!("ssh migration commit: {error}")))?;
        Ok(())
    }
}

const SSH_PROGRESS_QUERY: &str = r#"
SELECT
	CASE c2.count WHEN 0 THEN 1 ELSE
		CAST((c2.count - c1.count) AS float) / CAST(c2.count AS float)
	END
FROM
	(SELECT COUNT(*) as count FROM user_credentials WHERE domain = $1 AND NOT ssh_migration_applied) c1,
	(SELECT COUNT(*) as count FROM user_credentials WHERE domain = $2) c2
"#;

const SSH_SELECT_QUERY: &str = r#"
SELECT
	id,
	credential,
	encryption_key_id
FROM user_credentials
WHERE
	domain = $1 AND
	ssh_migration_applied = $2
ORDER BY ID
LIMIT $3
FOR UPDATE
"#;

const SSH_UPDATE_QUERY: &str = r#"
UPDATE user_credentials
SET
	updated_at = NOW(),
	ssh_migration_applied = $1,
	credential = $2,
	encryption_key_id = $3
WHERE id = $4
"#;

const SSH_UPDATE_FLAG_ONLY_QUERY: &str = r#"
UPDATE user_credentials
SET ssh_migration_applied = $1
WHERE id = $2
"#;

/// The subset of `jsonSSHMigratorAuth` (`Username`, `Password`, `Token`).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct SshAuth {
    username: String,
    password: String,
    token: String,
}

impl SshAuth {
    fn from_json(value: &Value) -> Result<Self, String> {
        let Some(object) = value.as_object() else {
            if value.is_null() {
                return Ok(Self::default());
            }
            return Err("auth payload is not an object".to_owned());
        };
        Ok(Self {
            username: string_field(object, "Username")?,
            password: string_field(object, "Password")?,
            token: string_field(object, "Token")?,
        })
    }
}

#[derive(Serialize)]
struct SshAuthWithSsh<'a> {
    #[serde(rename = "Username", skip_serializing_if = "str::is_empty")]
    username: &'a str,
    #[serde(rename = "Password", skip_serializing_if = "str::is_empty")]
    password: &'a str,
    #[serde(rename = "Token", skip_serializing_if = "str::is_empty")]
    token: &'a str,
    #[serde(rename = "PrivateKey")]
    private_key: &'a str,
    #[serde(rename = "PublicKey")]
    public_key: &'a str,
    #[serde(rename = "Passphrase")]
    passphrase: &'a str,
}

#[derive(Serialize)]
struct SshAuthOnly<'a> {
    #[serde(rename = "Username", skip_serializing_if = "str::is_empty")]
    username: &'a str,
    #[serde(rename = "Password", skip_serializing_if = "str::is_empty")]
    password: &'a str,
    #[serde(rename = "Token", skip_serializing_if = "str::is_empty")]
    token: &'a str,
}

#[derive(Serialize)]
struct AuthWithSsh<'a> {
    #[serde(rename = "Type")]
    kind: String,
    #[serde(rename = "Auth")]
    auth: SshAuthWithSsh<'a>,
}

#[derive(Serialize)]
struct AuthOnly<'a> {
    #[serde(rename = "Type")]
    kind: String,
    #[serde(rename = "Auth")]
    auth: SshAuthOnly<'a>,
}

/// Split `{"Type":..,"Auth":..}`; `None` when `Auth` is absent.
fn split_envelope(credential: &str) -> Result<(String, Option<Value>), String> {
    let value: Value =
        serde_json::from_str(credential).map_err(|error| format!("parsing credential: {error}"))?;
    let object = value
        .as_object()
        .ok_or_else(|| "credential is not a JSON object".to_owned())?;
    let kind = match ci_get(object, "Type") {
        Some(Value::String(kind)) => kind.clone(),
        Some(_) => return Err("credential Type is not a string".to_owned()),
        None => String::new(),
    };
    Ok((kind, ci_get(object, "Auth").cloned()))
}

/// Forward transform of a decrypted credential (`Up`'s callback). `None`
/// means "leave the credential as-is and only flip the applied flag".
fn transform_up(credential: &str) -> Result<Option<String>, String> {
    let (kind, auth) = split_envelope(credential)?;
    if kind != "BasicAuth" && kind != "OAuthBearerToken" {
        return Ok(None);
    }
    let auth = SshAuth::from_json(
        auth.as_ref()
            .ok_or_else(|| "unexpected end of JSON input".to_owned())?,
    )?;
    let keypair = generate_rsa_key(4096)?;

    let with_ssh = AuthWithSsh {
        kind: format!("{kind}WithSSH"),
        auth: SshAuthWithSsh {
            username: &auth.username,
            password: &auth.password,
            token: &auth.token,
            private_key: &keypair.private_key,
            public_key: &keypair.public_key,
            passphrase: &keypair.passphrase,
        },
    };
    let encoded = marshal_go(&with_ssh)?;
    Ok(Some(encoded))
}

/// Reverse transform (`Down`'s callback).
fn transform_down(credential: &str) -> Result<Option<String>, String> {
    let (kind, auth) = split_envelope(credential)?;
    if kind != "BasicAuthWithSSH" && kind != "OAuthBearerTokenWithSSH" {
        return Ok(None);
    }
    let auth = SshAuth::from_json(
        auth.as_ref()
            .ok_or_else(|| "unexpected end of JSON input".to_owned())?,
    )?;

    let plain = AuthOnly {
        kind: kind.trim_end_matches("WithSSH").to_owned(),
        auth: SshAuthOnly {
            username: &auth.username,
            password: &auth.password,
            token: &auth.token,
        },
    };
    let encoded = marshal_go(&plain)?;
    Ok(Some(encoded))
}

/// `json.Marshal` escapes HTML-significant characters and the JavaScript line
/// separators inside strings; `serde_json` does not. The replacement is safe
/// on the serialized document because those bytes can only occur inside
/// strings in valid JSON.
fn marshal_go(value: &impl Serialize) -> Result<String, String> {
    let encoded =
        serde_json::to_string(value).map_err(|error| format!("serializing credential: {error}"))?;
    Ok(encoded
        .replace('&', "\\u0026")
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029"))
}

fn ci_get<'a>(object: &'a Map<String, Value>, name: &str) -> Option<&'a Value> {
    if let Some(value) = object.get(name) {
        return Some(value);
    }
    object
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value)
}

fn string_field(object: &Map<String, Value>, name: &str) -> Result<String, String> {
    match ci_get(object, name) {
        None | Some(Value::Null) => Ok(String::new()),
        Some(Value::String(value)) => Ok(value.clone()),
        Some(_) => Err(format!("field {name} is not a string")),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GeneratedKeyPair {
    private_key: String,
    public_key: String,
    passphrase: String,
}

/// `encryption.GenerateRSAKey` (see the module-level gap note about PEM
/// encryption).
fn generate_rsa_key(bits: usize) -> Result<GeneratedKeyPair, String> {
    let mut rng = rsa::rand_core::OsRng;
    let private = rsa::RsaPrivateKey::new(&mut rng, bits)
        .map_err(|error| format!("generating private key: {error}"))?;
    let public = private.to_public_key();

    let private_pem = private
        .to_pkcs1_pem(rsa::pkcs1::LineEnding::LF)
        .map_err(|error| format!("encoding private key: {error}"))?
        .to_string();
    let passphrase = uuid_v4(&mut rng);
    let public_key = openssh_public_key(&public);

    Ok(GeneratedKeyPair {
        private_key: private_pem,
        public_key,
        passphrase,
    })
}

/// `uuid.NewRandom().String()` (`crypto/rand`, v4 layout).
fn uuid_v4<R: rsa::rand_core::RngCore + ?Sized>(rng: &mut R) -> String {
    let mut bytes = [0u8; 16];
    rng.fill_bytes(&mut bytes);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-\
         {:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

/// `ssh.NewPublicKey` + `ssh.MarshalAuthorizedKey`: `ssh-rsa <base64> \n`.
fn openssh_public_key(key: &rsa::RsaPublicKey) -> String {
    fn put_string(out: &mut Vec<u8>, bytes: &[u8]) {
        out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        out.extend_from_slice(bytes);
    }
    fn put_mpint(out: &mut Vec<u8>, bytes: &[u8]) {
        if !bytes.is_empty() && bytes[0] & 0x80 != 0 {
            let mut prefixed = Vec::with_capacity(bytes.len() + 1);
            prefixed.push(0);
            prefixed.extend_from_slice(bytes);
            put_string(out, &prefixed);
        } else {
            put_string(out, bytes);
        }
    }

    let mut blob = Vec::new();
    put_string(&mut blob, b"ssh-rsa");
    put_mpint(&mut blob, &key.e().to_bytes_be());
    put_mpint(&mut blob, &key.n().to_bytes_be());

    format!("ssh-rsa {} \n", base64_encode(&blob))
}

/// Standard base64 (Go's `base64.StdEncoding`), avoiding an extra dependency.
fn base64_encode(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        out.push(ALPHABET[(b0 >> 2) as usize] as char);
        out.push(ALPHABET[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(b2 & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upgrades_basic_auth_with_keypair() {
        let credential = r#"{"Type":"BasicAuth","Auth":{"Username":"u","Password":"p"}}"#;
        let (kind, auth) = split_envelope(credential).unwrap();
        assert_eq!(kind, "BasicAuth");
        let parsed = SshAuth::from_json(&auth.unwrap()).unwrap();
        assert_eq!(parsed.username, "u");
        assert_eq!(parsed.password, "p");
        assert_eq!(parsed.token, "");

        let keypair = generate_rsa_key(1024).unwrap();
        assert!(keypair
            .private_key
            .starts_with("-----BEGIN RSA PRIVATE KEY-----"));
        assert!(keypair
            .private_key
            .ends_with("-----END RSA PRIVATE KEY-----\n"));
        assert!(keypair.public_key.starts_with("ssh-rsa "));
        assert!(keypair.public_key.ends_with(" \n"));
        assert_eq!(keypair.passphrase.len(), 36);
        assert_eq!(keypair.passphrase.as_bytes()[14], b'4');

        let output = serde_json::to_string(&AuthWithSsh {
            kind: "BasicAuthWithSSH".to_owned(),
            auth: SshAuthWithSsh {
                username: &parsed.username,
                password: &parsed.password,
                token: &parsed.token,
                private_key: &keypair.private_key,
                public_key: &keypair.public_key,
                passphrase: &keypair.passphrase,
            },
        })
        .unwrap();
        assert!(output.starts_with(r#"{"Type":"BasicAuthWithSSH","Auth":{"Username":"u","Password":"p","PrivateKey":"-----BEGIN RSA PRIVATE KEY-----"#));
        assert!(output.contains(r#""PublicKey":"ssh-rsa "#));
        assert!(output.contains(r#""Passphrase":"#));
    }

    #[test]
    fn leaves_other_auth_types_alone() {
        let credential = r#"{"Type":"GitHubApp","Auth":{}}"#;
        assert_eq!(transform_down(credential).unwrap(), None);
    }

    #[test]
    fn strips_ssh_from_supported_types() {
        let credential = r#"{"Type":"OAuthBearerTokenWithSSH","Auth":{"Token":"t","PrivateKey":"k","PublicKey":"p","Passphrase":"x"}}"#;
        assert_eq!(
            transform_down(credential).unwrap().as_deref(),
            Some(r#"{"Type":"OAuthBearerToken","Auth":{"Token":"t"}}"#)
        );
    }

    #[test]
    fn reports_malformed_auth_payloads() {
        let credential = r#"{"Type":"BasicAuthWithSSH"}"#;
        assert!(transform_down(credential).is_err());

        let credential = r#"{"Type":"BasicAuthWithSSH","Auth":{"Username":5}}"#;
        assert!(transform_down(credential).is_err());
    }

    #[tokio::test]
    #[ignore = "requires Postgres (set DATABASE_URL)"]
    async fn migrates_credentials_against_postgres() {
        let pool = test_pool().await;
        sqlx::query(
            r#"CREATE TEMP TABLE user_credentials (
                id integer PRIMARY KEY,
                domain text NOT NULL,
                ssh_migration_applied boolean NOT NULL DEFAULT false,
                credential text NOT NULL,
                encryption_key_id text NOT NULL DEFAULT '',
                updated_at timestamptz NOT NULL DEFAULT now()
            )"#,
        )
        .execute(&pool)
        .await
        .expect("create temp table");
        sqlx::query(
            r#"INSERT INTO user_credentials (id, domain, credential)
               VALUES (1, 'batches', '{"Type":"BasicAuth","Auth":{"Username":"u","Password":"p"}}')"#,
        )
        .execute(&pool)
        .await
        .expect("insert credential");

        let migrator = SSHMigrator::default();
        migrator.up(&pool).await.expect("up");

        let credential: String =
            sqlx::query_scalar("SELECT credential FROM user_credentials WHERE id = 1")
                .fetch_one(&pool)
                .await
                .expect("select credential");
        assert!(credential.contains(r#""Type":"BasicAuthWithSSH""#));
        assert!(credential.contains(r#""PublicKey":"ssh-rsa "#));

        migrator.down(&pool).await.expect("down");
        let credential: String =
            sqlx::query_scalar("SELECT credential FROM user_credentials WHERE id = 1")
                .fetch_one(&pool)
                .await
                .expect("select credential");
        assert!(credential.contains(r#""Type":"BasicAuth""#));
        assert!(!credential.contains("PrivateKey"));
    }

    async fn test_pool() -> PgPool {
        let dsn = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://localhost/postgres".to_owned());
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&dsn)
            .await
            .expect("connect to Postgres")
    }

    #[test]
    fn marshals_with_go_html_escaping() {
        let value = serde_json::json!({"v": "a&<b>c"});
        assert_eq!(
            marshal_go(&value).unwrap(),
            "{\"v\":\"a\\u0026\\u003cb\\u003ec\"}"
        );
    }

    #[test]
    fn base64_matches_known_vectors() {
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
    }
}
