//! The `sessionInfo` JSON document and the Redis session payload.
//!
//! `session.SetData` JSON-marshals the value and stores the bytes in the
//! Redis map; the only key the authentication path reads is `actor`. The
//! baseline's `sessionInfo` is:
//!
//! ```go
//! type sessionInfo struct {
//!     Actor         *actor.Actor  `json:"actor"`
//!     LastActive    time.Time     `json:"lastActive"`
//!     ExpiryPeriod  time.Duration `json:"expiryPeriod"`
//!     UserCreatedAt time.Time     `json:"userCreatedAt"`
//! }
//! ```
//!
//! `time.Duration` marshals as int64 nanoseconds and `time.Time` uses Go's
//! `RFC3339Nano` JSON form.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::gob;

/// Cookie name (`session.go:47`).
pub const COOKIE_NAME: &str = "sgs";
/// Redis key prefix (`rstore.SetKeyPrefix("session_")`).
pub const KEY_PREFIX: &str = "session_";
/// `redistore.RediStore.Options.MaxAge` = 30 days, also the `SETEX` TTL.
pub const TTL_SECONDS: i64 = 86_400 * 30;

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

impl Actor {
    /// `actor.Actor.IsAuthenticated`: `UID != 0`.
    pub fn is_authenticated(&self) -> bool {
        self.uid != 0
    }
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GoTime(pub OffsetDateTime);

impl GoTime {
    pub fn now() -> Self {
        GoTime(OffsetDateTime::now_utc())
    }

    /// Go's `time.Time.IsZero` (`0001-01-01T00:00:00Z`).
    pub fn is_zero(&self) -> bool {
        let t = self.0;
        t.year() == 1
            && t.month() as u8 == 1
            && t.day() == 1
            && t.hour() == 0
            && t.minute() == 0
            && t.second() == 0
            && t.nanosecond() == 0
    }

    /// `time.Time.Equal`: compare instants, not offsets.
    pub fn same_instant(&self, other: &GoTime) -> bool {
        self.0.unix_timestamp_nanos() == other.0.unix_timestamp_nanos()
    }

    pub fn format_go(&self) -> String {
        let t = self.0;
        let mut out = format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
            t.year(),
            t.month() as u8,
            t.day(),
            t.hour(),
            t.minute(),
            t.second()
        );
        let nanos = t.nanosecond();
        if nanos > 0 {
            let mut frac = format!(".{nanos:09}");
            while frac.ends_with('0') {
                frac.pop();
            }
            out.push_str(&frac);
        }
        let offset = t.offset().whole_seconds();
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
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.format_go())
    }
}

impl<'de> Deserialize<'de> for GoTime {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        let t = OffsetDateTime::parse(&s, &Rfc3339).map_err(serde::de::Error::custom)?;
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

    /// The `actor` JSON document, if the session has one.
    pub fn actor_json(&self) -> Option<&[u8]> {
        self.get_data("actor")
    }

    /// Parse the `actor` document. `Ok(None)` means the stored JSON was
    /// `null` (`SetActor(w, r, nil, ...)` stores a nil `*sessionInfo`, which
    /// `json.Unmarshal` leaves as a nil pointer).
    pub fn actor_info(&self) -> Option<serde_json::Result<Option<SessionInfo>>> {
        let raw = self.actor_json()?;
        Some(serde_json::from_slice::<Option<SessionInfo>>(raw))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actor_json_matches_go() {
        let info = SessionInfo {
            actor: Some(Actor {
                uid: 1,
                ..Default::default()
            }),
            last_active: GoTime(
                OffsetDateTime::parse("2026-11-07T15:19:15.703398646Z", &Rfc3339).unwrap(),
            ),
            expiry_period: 7_776_000_000_000_000,
            user_created_at: GoTime(
                OffsetDateTime::parse("2026-11-07T13:56:25.553432Z", &Rfc3339).unwrap(),
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

    #[test]
    fn zero_time_and_instant_equality() {
        let zero: GoTime = serde_json::from_str("\"0001-01-01T00:00:00Z\"").unwrap();
        assert!(zero.is_zero());
        let a: GoTime = serde_json::from_str("\"2024-01-01T00:00:00Z\"").unwrap();
        let b: GoTime = serde_json::from_str("\"2024-01-01T02:00:00+02:00\"").unwrap();
        assert!(a.same_instant(&b));
        // `OffsetDateTime` equality is instant-based, matching `time.Time.Equal`.
        assert_eq!(a, b);
        assert!(!a.is_zero());
    }
}
