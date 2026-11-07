//! Redis-backed session store compatible with Sourcegraph's
//! `cmd/frontend/internal/auth/session` + `boj/redistore`.
//!
//! Key layout: `session_<id>` (prefix `session_`), value is the gob-encoded
//! `map[interface{}]interface{}` decoded by [`crate::gob`], TTL is
//! `session.Options.MaxAge` (30 days) via `SETEX` (`redistore.save`).
//!
//! `session.SetData(key, value)` stores `json.Marshal(value)` bytes; the only
//! keys written by the baseline are `actor` (a `*sessionInfo` JSON document),
//! `oauthState`/`oidcState` (strings), `oidc@0` (OpenID Connect state) and
//! `soap@0` (sourcegraphoperator state).

use serde::{Deserialize, Serialize};

use crate::gob;

/// Cookie name and Redis key prefix (`session.go` constants).
pub const COOKIE_NAME: &str = "sgs";
pub const KEY_PREFIX: &str = "session_";
/// `redistore.RediStore.Options.MaxAge` = 30 days.
pub const TTL_SECONDS: u64 = 86_400 * 30;
/// `redistore.RediStore.DefaultMaxAge` = 20 minutes (used only when the
/// session's own MaxAge is 0, which the session package never sets).
#[allow(dead_code)]
pub const DEFAULT_MAX_AGE_SECONDS: u64 = 60 * 20;

/// `actor.Actor` as serialized into the session JSON. `FromSessionCookie` is
/// `json:"-"` and is deliberately absent.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Actor {
    #[serde(rename = "UID", default, skip_serializing_if = "is_zero_i32")]
    pub uid: i32,
    #[serde(
        rename = "AnonymousUID",
        default,
        skip_serializing_if = "String::is_empty"
    )]
    pub anonymous_uid: String,
    #[serde(rename = "Internal", default, skip_serializing_if = "is_false")]
    pub internal: bool,
    #[serde(
        rename = "SourcegraphOperator",
        default,
        skip_serializing_if = "is_false"
    )]
    pub sourcegraph_operator: bool,
}

fn is_zero_i32(v: &i32) -> bool {
    *v == 0
}

fn is_false(v: &bool) -> bool {
    !*v
}

/// `session.sessionInfo`, the JSON document stored under the `actor` key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionInfo {
    pub actor: Option<Actor>,
    #[serde(rename = "lastActive")]
    pub last_active: GoTime,
    /// `time.Duration` marshals as int64 nanoseconds.
    #[serde(rename = "expiryPeriod")]
    pub expiry_period: i64,
    #[serde(rename = "userCreatedAt")]
    pub user_created_at: GoTime,
}

/// `time.Time` JSON representation: RFC 3339 with Go's `RFC3339Nano` rules
/// (fraction trimmed of trailing zeros, offset preserved, `Z` for UTC).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoTime(pub chrono::DateTime<chrono::FixedOffset>);

impl GoTime {
    #[allow(dead_code)]
    pub fn now_local() -> Self {
        GoTime(chrono::Local::now().fixed_offset())
    }

    pub fn format_go(&self) -> String {
        let t = &self.0;
        let mut out = t.format("%Y-%m-%dT%H:%M:%S").to_string();
        let nanos = t.timestamp_subsec_nanos();
        if nanos > 0 {
            let mut frac = format!(".{nanos:09}");
            while frac.ends_with('0') {
                frac.pop();
            }
            out.push_str(&frac);
        }
        let offset = t.offset().local_minus_utc();
        if offset == 0 {
            out.push('Z');
        } else {
            let sign = if offset < 0 { '-' } else { '+' };
            let a = offset.abs();
            out.push_str(&format!("{sign}{:02}:{:02}", a / 3600, (a % 3600) / 60));
        }
        out
    }
}

impl Serialize for GoTime {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.format_go())
    }
}

impl<'de> Deserialize<'de> for GoTime {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        let t = chrono::DateTime::parse_from_rfc3339(&s).map_err(serde::de::Error::custom)?;
        Ok(GoTime(t))
    }
}

/// A loaded session: cookie timestamp, ID and the raw gob values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    pub timestamp: i64,
    pub id: String,
    /// Insertion order as decoded from gob (Go map order is randomized).
    pub values: Vec<(String, Vec<u8>)>,
}

impl Session {
    /// `session.GetData`: the raw `json.Marshal`ed bytes at `key`, if present.
    pub fn get_data(&self, key: &str) -> Option<&[u8]> {
        self.values
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_slice())
    }

    #[allow(dead_code)]
    pub fn actor_info(&self) -> Option<serde_json::Result<SessionInfo>> {
        self.get_data("actor")
            .map(|raw| serde_json::from_slice::<SessionInfo>(raw))
    }

    /// Serialize the values as the Redis payload.
    pub fn encode_redis(&self) -> Vec<u8> {
        let refs: Vec<(&str, &[u8])> = self
            .values
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_slice()))
            .collect();
        gob::encode_session_map(&refs)
    }

    pub fn from_redis(id: String, timestamp: i64, payload: &[u8]) -> gob::Result<Self> {
        Ok(Session {
            timestamp,
            id,
            values: gob::decode_session_map(payload)?,
        })
    }
}

/// redis `SETEX session_<id> <ttl> <payload>`, as `redistore.save` does.
pub async fn save(
    conn: &mut redis::aio::MultiplexedConnection,
    id: &str,
    payload: &[u8],
    ttl_seconds: u64,
) -> redis::RedisResult<()> {
    redis::cmd("SETEX")
        .arg(format!("{KEY_PREFIX}{id}"))
        .arg(ttl_seconds)
        .arg(payload)
        .query_async::<()>(conn)
        .await
}

/// redis `GET session_<id>`; `None` when the key does not exist
/// (`redistore.load` returns "no data" and the session is treated as new).
pub async fn load(
    conn: &mut redis::aio::MultiplexedConnection,
    id: &str,
) -> redis::RedisResult<Option<Vec<u8>>> {
    redis::cmd("GET")
        .arg(format!("{KEY_PREFIX}{id}"))
        .query_async(conn)
        .await
}

/// `redis DEL session_<id>`, as `redistore.delete` does.
pub async fn delete(
    conn: &mut redis::aio::MultiplexedConnection,
    id: &str,
) -> redis::RedisResult<()> {
    redis::cmd("DEL")
        .arg(format!("{KEY_PREFIX}{id}"))
        .query_async::<()>(conn)
        .await
}

/// `securecookie.GenerateRandomKey(32)` + `base32.StdEncoding` with the
/// `=` padding trimmed: a 52-character uppercase ID.
pub fn generate_id() -> String {
    let mut raw = [0u8; 32];
    rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut raw);
    base32_encode_no_pad(&raw)
}

const BASE32_ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

pub fn base32_encode_no_pad(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(5) * 8);
    let mut acc: u32 = 0;
    let mut bits: u32 = 0;
    for b in data {
        acc = (acc << 8) | *b as u32;
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(BASE32_ALPHABET[((acc >> bits) & 0x1f) as usize] as char);
        }
    }
    if bits > 0 {
        out.push(BASE32_ALPHABET[((acc << (5 - bits)) & 0x1f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base32_matches_go() {
        // base32.StdEncoding.EncodeToString(32 zero bytes) trimmed of "=".
        assert_eq!(
            base32_encode_no_pad(&[0u8; 32]),
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        );
        assert_eq!(base32_encode_no_pad(b"abc"), "MFRGG");
        assert_eq!(generate_id().len(), 52);
    }

    #[test]
    fn actor_json_matches_go() {
        let info = SessionInfo {
            actor: Some(Actor {
                uid: 1,
                ..Default::default()
            }),
            last_active: GoTime(
                chrono::DateTime::parse_from_rfc3339("2026-11-07T15:19:15.703398646Z").unwrap(),
            ),
            expiry_period: 7_776_000_000_000_000,
            user_created_at: GoTime(
                chrono::DateTime::parse_from_rfc3339("2026-11-07T13:56:25.553432Z").unwrap(),
            ),
        };
        // Exact bytes observed in the baseline Redis payload.
        let want = r#"{"actor":{"UID":1},"lastActive":"2026-11-07T15:19:15.703398646Z","expiryPeriod":7776000000000000,"userCreatedAt":"2026-11-07T13:56:25.553432Z"}"#;
        assert_eq!(serde_json::to_string(&info).unwrap(), want);
        let parsed: SessionInfo = serde_json::from_str(want).unwrap();
        assert_eq!(parsed, info);
    }

    #[test]
    fn go_time_formatting() {
        let t: GoTime = serde_json::from_str("\"2024-03-01T10:34:56.123000000Z\"").unwrap();
        assert_eq!(t.format_go(), "2024-03-01T10:34:56.123Z");
        let t: GoTime = serde_json::from_str("\"2024-03-01T10:34:56Z\"").unwrap();
        assert_eq!(t.format_go(), "2024-03-01T10:34:56Z");
        let t: GoTime = serde_json::from_str("\"2024-03-01T12:34:56.123456+02:00\"").unwrap();
        assert_eq!(t.format_go(), "2024-03-01T12:34:56.123456+02:00");
    }
}
