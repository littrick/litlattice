//! gorilla/securecookie v1.1.1 compatible codec for Sourcegraph's `sgs`
//! session cookie.
//!
//! Sourcegraph configures `securecookie.CodecsFromPairs([]byte(SRC_SESSION_COOKIE_KEY))`
//! (no block key, so the value is authenticated but *not* encrypted) and the
//! default `GobEncoder`, which serializes the session ID string with
//! `encoding/gob`.
//!
//! Cookie grammar (`SecureCookie.Encode`, securecookie.go:259):
//!
//! ```text
//! b64url( "<unix seconds>|<b64url(gob(value))>|<HMAC-SHA256(key, name|ts|b64value)>" )
//! ```
//!
//! The MAC input is `name|ts|b64value` (the name is stripped from the wire
//! value). Both base64 layers are padded `base64.URLEncoding` (Go's
//! `URLEncoding`) and a base64 layer is absent from the MAC input and the
//! wire: the MAC bytes are appended raw. Decoding rejects values whose
//! timestamp is older than 30 days, which is the `maxAge` `securecookie.New`
//! sets and Sourcegraph never overrides.

use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::gob;

type HmacSha256 = Hmac<Sha256>;

/// `securecookie.New` default max age (seconds).
pub const MAX_AGE: i64 = 86_400 * 30;

#[derive(Debug)]
pub enum CookieError {
    Base64(base64::DecodeError),
    Malformed,
    BadMac,
    BadTimestamp(String),
    Expired { timestamp: i64, now: i64 },
    Gob(gob::GobError),
}

impl std::fmt::Display for CookieError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CookieError::Base64(e) => write!(f, "securecookie: base64 decode failed: {e}"),
            CookieError::Malformed => write!(f, "securecookie: malformed value"),
            CookieError::BadMac => write!(f, "securecookie: the value is not valid"),
            CookieError::BadTimestamp(s) => write!(f, "securecookie: invalid timestamp {s:?}"),
            CookieError::Expired { timestamp, now } => write!(
                f,
                "securecookie: timestamp {timestamp} is older than max age (now {now})"
            ),
            CookieError::Gob(e) => write!(f, "securecookie: {e}"),
        }
    }
}

impl std::error::Error for CookieError {}

pub type Result<T> = std::result::Result<T, CookieError>;

fn b64_encode(raw: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE.encode(raw)
}

fn b64_decode(s: &str) -> Result<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE
        .decode(s)
        .map_err(CookieError::Base64)
}

fn mac(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut h = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    h.update(msg);
    h.finalize().into_bytes().to_vec()
}

/// A decoded `sgs` cookie value.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedCookie {
    pub timestamp: i64,
    /// gob-encoded payload (a Go string for the session ID).
    pub value_gob: Vec<u8>,
}

/// Encode a cookie value, mirroring `securecookie.Encode(name, value)` with the
/// default gob serializer. `timestamp` is Unix seconds.
pub fn encode(name: &str, key: &[u8], timestamp: i64, value_gob: &[u8]) -> String {
    let b64_value = b64_encode(value_gob);
    let signed = format!("{name}|{timestamp}|{b64_value}");
    let tag = mac(key, signed.as_bytes());
    let mut inner = signed.into_bytes();
    inner.push(b'|');
    inner.extend_from_slice(&tag);
    // The name is stripped from what goes on the wire.
    let wire = &inner[name.len() + 1..];
    b64_encode(wire)
}

/// Encode a session ID cookie (gob string payload), as `redistore.Save` does.
pub fn encode_session_id(name: &str, key: &[u8], timestamp: i64, id: &str) -> String {
    encode(name, key, timestamp, &gob::encode_string(id))
}

/// Decode and verify a cookie value. `now` is Unix seconds.
pub fn decode(name: &str, key: &[u8], cookie: &str, now: i64) -> Result<DecodedCookie> {
    let raw = b64_decode(cookie)?;
    // Go: bytes.SplitN(b, "|", 3) -> [ts, b64value, mac]
    let mut parts = raw.splitn(3, |b| *b == b'|');
    let (ts_s, b64_value, tag) = match (parts.next(), parts.next(), parts.next()) {
        (Some(a), Some(b), Some(c)) => (a, b, c),
        _ => return Err(CookieError::Malformed),
    };
    let mut signed = Vec::with_capacity(name.len() + 1 + ts_s.len() + 1 + b64_value.len());
    signed.extend_from_slice(name.as_bytes());
    signed.push(b'|');
    signed.extend_from_slice(ts_s);
    signed.push(b'|');
    signed.extend_from_slice(b64_value);
    let expected = mac(key, &signed);
    if !constant_time_eq(&expected, tag) {
        return Err(CookieError::BadMac);
    }
    let ts_str = std::str::from_utf8(ts_s).map_err(|_| CookieError::Malformed)?;
    let timestamp: i64 = ts_str
        .parse()
        .map_err(|_| CookieError::BadTimestamp(ts_str.to_string()))?;
    // securecookie: maxAge = 30 days, minAge = 0. The check happens when the
    // timestamp is decoded, before the payload.
    if timestamp < now - MAX_AGE {
        return Err(CookieError::Expired { timestamp, now });
    }
    let b64_str = std::str::from_utf8(b64_value).map_err(|_| CookieError::Malformed)?;
    Ok(DecodedCookie {
        timestamp,
        value_gob: b64_decode(b64_str)?,
    })
}

/// Decode a session-ID cookie and return `(timestamp, id)`.
pub fn decode_session_id(name: &str, key: &[u8], cookie: &str, now: i64) -> Result<(i64, String)> {
    let decoded = decode(name, key, cookie, now)?;
    let id = gob::decode_string(&decoded.value_gob).map_err(CookieError::Gob)?;
    Ok((decoded.timestamp, id))
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_symmetry_and_hmac_rejection() {
        let ts = 1_760_000_000;
        let cookie = encode_session_id("sgs", b"", ts, "OUYT");
        let (got_ts, got_id) = decode_session_id("sgs", b"", &cookie, ts + 10).unwrap();
        assert_eq!((got_ts, got_id.as_str()), (ts, "OUYT"));
        // Re-encoding with the recovered timestamp is byte-identical.
        assert_eq!(encode_session_id("sgs", b"", got_ts, &got_id), cookie);
        assert!(decode_session_id("sgs", b"wrong", &cookie, ts + 10).is_err());
        assert!(decode_session_id("sgs", b"", &cookie, ts + MAX_AGE + 1).is_err());
    }

    #[test]
    fn decode_fixtures_if_present() {
        for name in ["cookie-empty-key.json", "cookie-test-key.json"] {
            let path = format!(
                "{}/../../../testdata/sessions/{name}",
                env!("CARGO_MANIFEST_DIR")
            );
            let Ok(raw) = std::fs::read_to_string(path) else {
                continue; // fixtures are produced by the PoC generator
            };
            let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
            let cookie = v["cookie"].as_str().unwrap();
            let ts = v["timestamp"].as_i64().unwrap();
            let id = v["id"].as_str().unwrap();
            let key = v["key"].as_str().unwrap();
            let (got_ts, got_id) = decode_session_id("sgs", key.as_bytes(), cookie, ts).unwrap();
            assert_eq!((got_ts, got_id.clone()), (ts, id.to_string()));
            assert_eq!(
                encode_session_id("sgs", key.as_bytes(), got_ts, &got_id),
                cookie
            );
        }
    }
}
