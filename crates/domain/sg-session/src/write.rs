//! The session **write** path: `session.SetData` / `SetActor` /
//! `deleteSession` / `SetSignOutCookie` against the `redistore` semantics of
//! `boj/redistore` v1.0.0 and gorilla/sessions v1.2.1.
//!
//! The baseline builds the store with
//!
//! ```go
//! Options:       {Path: "/", HttpOnly: true, MaxAge: 86400 * 30, SameSite: Lax|None, Secure: https}
//! DefaultMaxAge: 60 * 20
//! maxLength:     4096
//! serializer:    GobSerializer
//! keyPrefix:     "session_"
//! ```
//!
//! and `RediStore.Save` has three observable branches:
//!
//! * `MaxAge <= 0` → `DEL session_<id>` and an already-expired `sgs` cookie;
//! * otherwise, generate a base32 ID only when the session has none, gob
//!   serialize the `map[interface{}]interface{}`, reject payloads over 4096
//!   bytes, `SETEX session_<id> <MaxAge or 1200> <gob>` and set the
//!   securecookie-encoded ID cookie.
//!
//! gorilla's `Registry.Get` caches the session **by cookie name** in the
//! request context, so every `newSessionStore().Get(r, "sgs")` in one request
//! returns the same `*Session`. That is what makes the baseline's sign-out
//! flow emit duplicate `DEL`s and duplicate expired cookies: `deleteSession`
//! sets `Options.MaxAge = -1` on the cached session, and the subsequent
//! `SetActor(nil)` and SSO `SetData` calls all take the delete branch too.
//! [`SessionManager`] reproduces that by loading once and keeping the session
//! (and its `MaxAge`) in memory.
//!
//! On a load error (bad cookie or Redis failure) redistore returns the
//! session together with the error; the ID decoded from the cookie is kept,
//! so `Save` (delete branch) still knows which key to `DEL`, and the error is
//! sticky for every later `Get` on the same request (the registry caches it).
//! [`SessionManager`] tracks that with `load_error`.

use std::fmt::Write as _;

use time::OffsetDateTime;

use crate::auth::SessionStore;
use crate::gob;
use crate::securecookie;
use crate::session::{Actor, GoTime, SessionInfo, COOKIE_NAME};
use crate::TTL_SECONDS;

/// `session.SignOutCookie`.
pub const SIGN_OUT_COOKIE: &str = "sg-signout";
/// `redistore.DefaultMaxAge` (20 minutes).
pub const DEFAULT_MAX_AGE: i64 = 60 * 20;
/// `rstore.SetMaxLength(4096)`.
pub const MAX_LENGTH: usize = 4096;
/// `defaultExpiryPeriod` (90 days) as `time.Duration` nanoseconds.
pub const DEFAULT_EXPIRY_PERIOD_NANOS: i64 = 90 * 24 * 3_600 * 1_000_000_000;
/// The store's `Options.MaxAge` (30 days).
pub const COOKIE_MAX_AGE: i64 = TTL_SECONDS;

/// `session.SetData`'s Redis error and log-error texts.
const ERR_TOO_BIG: &str = "SessionStore: the value to store is too big";

/// A `Set-Cookie` header as `net/http.Cookie.String` renders it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetCookie {
    pub name: String,
    pub value: String,
    /// Empty means "omit the attribute" (Go's zero `Path`).
    pub path: String,
    pub domain: String,
    /// `Cookie.Expires`; `None` omits the attribute.
    pub expires_unix: Option<i64>,
    /// `> 0` → `Max-Age=<v>`; `< 0` → `Max-Age=0`; `0` → omitted.
    pub max_age: i64,
    pub http_only: bool,
    pub secure: bool,
    /// `Some("Lax" | "None")`; `None` is `http.SameSiteDefaultMode`.
    pub same_site: Option<&'static str>,
}

impl SetCookie {
    /// `http.Cookie.String` (cookie.go), including the attribute order.
    pub fn to_header(&self) -> String {
        let mut out = String::with_capacity(96);
        let _ = write!(out, "{}={}", self.name, self.value);
        if !self.path.is_empty() {
            let _ = write!(out, "; Path={}", self.path);
        }
        if !self.domain.is_empty() {
            let _ = write!(out, "; Domain={}", self.domain);
        }
        if let Some(expires) = self.expires_unix {
            let _ = write!(out, "; Expires={}", http_date(expires));
        }
        if self.max_age > 0 {
            let _ = write!(out, "; Max-Age={}", self.max_age);
        } else if self.max_age < 0 {
            out.push_str("; Max-Age=0");
        }
        if self.http_only {
            out.push_str("; HttpOnly");
        }
        if self.secure {
            out.push_str("; Secure");
        }
        if let Some(same_site) = self.same_site {
            let _ = write!(out, "; SameSite={same_site}");
        }
        out
    }
}

/// `http.TimeFormat` (RFC 7231 IMF-fixdate) for a Unix timestamp.
pub fn http_date(unix_seconds: i64) -> String {
    const WEEKDAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let days = unix_seconds.div_euclid(86_400);
    let secs = unix_seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    // 1970-01-01 was a Thursday.
    let weekday = WEEKDAYS[((days + 4).rem_euclid(7)) as usize];
    format!(
        "{weekday}, {day:02} {} {year:04} {:02}:{:02}:{:02} GMT",
        MONTHS[(month - 1) as usize],
        secs / 3_600,
        (secs % 3_600) / 60,
        secs % 60
    )
}

/// Howard Hinnant's `civil_from_days` (days since the Unix epoch → date).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

/// A source of new session IDs. `redistore.Save` uses 32 random bytes from
/// `crypto/rand`; production uses [`OsSessionIdSource`] and the differential
/// harness injects a fixed source.
pub trait SessionIdSource: Send + Sync {
    fn generate(&self) -> String;
}

/// 32 bytes from the OS CSPRNG, standard base32, padding trimmed
/// (`strings.TrimRight(base32.StdEncoding.EncodeToString(securecookie.GenerateRandomKey(32)), "=")`).
pub struct OsSessionIdSource;

impl SessionIdSource for OsSessionIdSource {
    fn generate(&self) -> String {
        use std::io::Read as _;
        let mut bytes = [0u8; 32];
        let mut file = std::fs::File::open("/dev/urandom").expect("open /dev/urandom");
        file.read_exact(&mut bytes).expect("read /dev/urandom");
        base32_encode(&bytes)
    }
}

/// Deterministic source for tests.
#[derive(Debug, Clone)]
pub struct FixedSessionIdSource(pub String);

impl SessionIdSource for FixedSessionIdSource {
    fn generate(&self) -> String {
        self.0.clone()
    }
}

/// Standard base32 (`base32.StdEncoding`) without the trailing `=`.
pub fn base32_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut out = String::with_capacity((bytes.len() * 8).div_ceil(5));
    let mut buffer: u32 = 0;
    let mut bits: u32 = 0;
    for &byte in bytes {
        buffer = (buffer << 8) | u32::from(byte);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(ALPHABET[((buffer >> bits) & 0x1f) as usize] as char);
        }
    }
    if bits > 0 {
        out.push(ALPHABET[((buffer << (5 - bits)) & 0x1f) as usize] as char);
    }
    out
}

/// The store options `newSessionStore` installs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteOptions {
    pub secure: bool,
    pub max_age: i64,
}

impl WriteOptions {
    /// `setSessionSecureOptions`: `SameSite=Lax` over HTTP, `SameSite=None`
    /// over HTTPS.
    pub fn new(secure: bool) -> Self {
        Self {
            secure,
            max_age: COOKIE_MAX_AGE,
        }
    }

    fn same_site(&self) -> Option<&'static str> {
        Some(if self.secure { "None" } else { "Lax" })
    }
}

/// The in-memory session (`sessions.Session`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritableSession {
    pub id: String,
    pub values: Vec<(String, Vec<u8>)>,
    /// `session.Options.MaxAge`.
    pub max_age: i64,
}

impl WritableSession {
    fn new(max_age: i64) -> Self {
        Self {
            id: String::new(),
            values: Vec::new(),
            max_age,
        }
    }

    /// `session.Values[key]`; missing is `None`.
    pub fn get_data(&self, key: &str) -> Option<&[u8]> {
        self.values
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_slice())
    }

    /// `session.Values[key] = value` (Go maps replace in place).
    pub fn set_data(&mut self, key: &str, value: &[u8]) {
        if let Some((_, slot)) = self.values.iter_mut().find(|(k, _)| k == key) {
            *slot = value.to_vec();
        } else {
            self.values.push((key.to_owned(), value.to_vec()));
        }
    }
}

/// `redistore.New`: decode the cookie and `GET session_<id>`.
///
/// Returns the session object even on failure (redistore does), together with
/// the error text that every later `Get` on the request would repeat.
pub async fn load_session(
    store: &dyn SessionStore,
    cookie_key: &[u8],
    cookie: Option<&str>,
    now: OffsetDateTime,
    options: &WriteOptions,
) -> (WritableSession, Option<String>) {
    let mut session = WritableSession::new(options.max_age);
    let Some(cookie) = cookie else {
        return (session, None);
    };

    let (_, id) = match securecookie::decode_session_id(
        COOKIE_NAME,
        cookie_key,
        cookie,
        now.unix_timestamp(),
    ) {
        Ok(decoded) => decoded,
        Err(err) => return (session, Some(err.to_string())),
    };
    session.id = id;

    match store.load(&session.id).await {
        Ok(None) => (session, None),
        Err(err) => (session, Some(err.to_string())),
        Ok(Some(payload)) => match gob::decode_session_map(&payload) {
            Ok(values) => {
                session.values = values;
                (session, None)
            }
            Err(err) => (session, Some(err.to_string())),
        },
    }
}

/// `RediStore.Save` with the `sgs` cookie. `session.max_age` decides the
/// branch; a successful save returns the cookie the baseline would set.
pub async fn save_session(
    store: &dyn SessionStore,
    ids: &dyn SessionIdSource,
    cookie_key: &[u8],
    session: &mut WritableSession,
    options: &WriteOptions,
    now: OffsetDateTime,
) -> Result<SetCookie, String> {
    if session.max_age <= 0 {
        store
            .delete(&session.id)
            .await
            .map_err(|err| err.to_string())?;
        return Ok(expired_session_cookie(options));
    }

    if session.id.is_empty() {
        session.id = ids.generate();
    }
    let refs: Vec<(&str, &[u8])> = session
        .values
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_slice()))
        .collect();
    let payload = gob::encode_session_map(&refs);
    if payload.len() > MAX_LENGTH {
        return Err(ERR_TOO_BIG.to_owned());
    }
    let ttl = if session.max_age == 0 {
        DEFAULT_MAX_AGE
    } else {
        session.max_age
    };
    store
        .save(&session.id, payload, ttl)
        .await
        .map_err(|err| err.to_string())?;

    let value =
        securecookie::encode_session_id(COOKIE_NAME, cookie_key, now.unix_timestamp(), &session.id);
    Ok(SetCookie {
        name: COOKIE_NAME.to_owned(),
        value,
        path: "/".to_owned(),
        domain: String::new(),
        expires_unix: Some(now.unix_timestamp() + session.max_age),
        max_age: session.max_age,
        http_only: true,
        secure: options.secure,
        same_site: options.same_site(),
    })
}

fn expired_session_cookie(options: &WriteOptions) -> SetCookie {
    SetCookie {
        name: COOKIE_NAME.to_owned(),
        value: String::new(),
        path: "/".to_owned(),
        domain: String::new(),
        // `sessions.NewCookie` with a negative MaxAge uses `time.Unix(1, 0)`.
        expires_unix: Some(1),
        max_age: -1,
        http_only: true,
        secure: options.secure,
        same_site: options.same_site(),
    }
}

/// `time.ParseDuration`. Returns nanoseconds.
pub fn parse_go_duration(input: &str) -> Option<i64> {
    let (negative, mut rest) = match input.as_bytes().first()? {
        b'-' => (true, &input[1..]),
        b'+' => (false, &input[1..]),
        _ => (false, input),
    };
    if rest == "0" {
        return Some(0);
    }
    if rest.is_empty() {
        return None;
    }

    let mut total: i128 = 0;
    while !rest.is_empty() {
        let bytes = rest.as_bytes();
        let mut i = 0;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        if i == 0 {
            return None;
        }
        let mut whole: i128 = rest[..i].parse().ok()?;
        let mut frac: i128 = 0;
        let mut scale: u32 = 0;
        if i < bytes.len() && bytes[i] == b'.' {
            i += 1;
            let frac_start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                frac = frac
                    .checked_mul(10)?
                    .checked_add(i128::from(bytes[i] - b'0'))?;
                scale += 1;
                i += 1;
            }
            if i == frac_start {
                return None;
            }
        }
        let unit = &rest[i..];
        let (unit_nanos, length): (i128, usize) = if unit.starts_with("ns") {
            (1, 2)
        } else if unit.starts_with("us") {
            (1_000, 2)
        } else if unit.starts_with("µs") || unit.starts_with("μs") {
            (1_000, "µs".len())
        } else if unit.starts_with("ms") {
            (1_000_000, 2)
        } else if unit.starts_with('s') {
            (1_000_000_000, 1)
        } else if unit.starts_with('m') {
            (60_000_000_000, 1)
        } else if unit.starts_with('h') {
            (3_600_000_000_000, 1)
        } else {
            return None;
        };
        whole = whole.checked_mul(unit_nanos)?;
        if scale > 0 {
            let denominator = 10i128.checked_pow(scale)?;
            whole = whole.checked_add(frac.checked_mul(unit_nanos)? / denominator)?;
        }
        total = total.checked_add(whole)?;
        rest = &unit[length..];
    }

    let total = total.min(i128::from(i64::MAX));
    Some(if negative {
        -(total as i64)
    } else {
        total as i64
    })
}

/// `SetActor`'s expiry resolution: an explicit non-zero period wins; otherwise
/// `siteConfig.authSessionExpiry`; otherwise 90 days.
pub fn resolve_expiry_period(expiry_period: i64, configured: Option<&str>) -> i64 {
    if expiry_period != 0 {
        return expiry_period;
    }
    configured
        .and_then(parse_go_duration)
        .unwrap_or(DEFAULT_EXPIRY_PERIOD_NANOS)
}

/// The `actor` session value `SetActor` stores: JSON of a nil `*sessionInfo`
/// is `null`.
pub fn session_info_json(
    actor: Option<&Actor>,
    expiry_period: i64,
    configured_expiry: Option<&str>,
    now: OffsetDateTime,
    user_created_at: GoTime,
) -> Vec<u8> {
    let Some(actor) = actor else {
        return b"null".to_vec();
    };
    let info = SessionInfo {
        actor: Some(actor.clone()),
        last_active: GoTime(now),
        expiry_period: resolve_expiry_period(expiry_period, configured_expiry),
        user_created_at,
    };
    serde_json::to_vec(&info).expect("sessionInfo serializes")
}

/// One request's view of the `sgs` session.
///
/// `newSessionStore().Get` is called once per operation in the baseline but
/// gorilla's registry caches by cookie name, so this type loads once and
/// reuses the session, its `MaxAge` and any load error for the whole request.
pub struct SessionManager<'a> {
    store: &'a dyn SessionStore,
    ids: &'a dyn SessionIdSource,
    cookie_key: &'a [u8],
    options: WriteOptions,
    now: OffsetDateTime,
    request_cookie: Option<String>,
    has_sign_out_cookie: bool,
    session: Option<WritableSession>,
    load_error: Option<String>,
    /// `Set-Cookie` headers in the order `http.SetCookie` wrote them.
    pub cookies: Vec<SetCookie>,
}

impl<'a> SessionManager<'a> {
    pub fn new(
        store: &'a dyn SessionStore,
        ids: &'a dyn SessionIdSource,
        cookie_key: &'a [u8],
        options: WriteOptions,
        now: OffsetDateTime,
        request_cookie: Option<&str>,
        has_sign_out_cookie: bool,
    ) -> Self {
        Self {
            store,
            ids,
            cookie_key,
            options,
            now,
            request_cookie: request_cookie.map(str::to_owned),
            has_sign_out_cookie,
            session: None,
            load_error: None,
            cookies: Vec::new(),
        }
    }

    async fn ensure_loaded(&mut self) {
        if self.session.is_some() {
            return;
        }
        let (session, error) = load_session(
            self.store,
            self.cookie_key,
            self.request_cookie.as_deref(),
            self.now,
            &self.options,
        )
        .await;
        self.session = Some(session);
        self.load_error = error;
    }

    fn has_session_cookie(&self) -> bool {
        self.request_cookie.is_some()
    }

    /// `deleteSession`. A missing cookie is a no-op; otherwise the cached
    /// session is marked expired and `Session.Save` runs, setting the
    /// failsafe cookie when the store failed.
    pub async fn delete_session(&mut self) -> Result<(), String> {
        if !self.has_session_cookie() {
            return Ok(());
        }
        self.ensure_loaded().await;
        let session = self.session.as_mut().expect("loaded");
        session.max_age = -1;

        if let Some(error) = self.load_error.clone() {
            self.cookies.push(expired_session_cookie(&self.options));
            return Err(format!("deleting session: {error}"));
        }

        let saved = save_session(
            self.store,
            self.ids,
            self.cookie_key,
            session,
            &self.options,
            self.now,
        )
        .await;
        match saved {
            Ok(cookie) => {
                self.cookies.push(cookie);
                Ok(())
            }
            Err(error) => {
                // Failsafe: expire the client cookie even if the store failed.
                self.cookies.push(expired_session_cookie(&self.options));
                Err(format!("deleting session: {error}"))
            }
        }
    }

    /// `session.GetData`: the raw JSON bytes, or `Ok(None)` when the key is
    /// missing (the baseline leaves the destination untouched).
    pub async fn get_data(&mut self, key: &str) -> Result<Option<Vec<u8>>, String> {
        self.ensure_loaded().await;
        if let Some(error) = &self.load_error {
            return Err(format!("getting session: {error}"));
        }
        Ok(self
            .session
            .as_ref()
            .expect("loaded")
            .get_data(key)
            .map(<[u8]>::to_vec))
    }

    /// `session.SetData` (the value is already JSON-marshalled).
    pub async fn set_data(&mut self, key: &str, value: &[u8]) -> Result<(), String> {
        self.ensure_loaded().await;
        if let Some(error) = &self.load_error {
            return Err(format!("getting session: {error}"));
        }
        let session = self.session.as_mut().expect("loaded");
        session.set_data(key, value);
        let saved = save_session(
            self.store,
            self.ids,
            self.cookie_key,
            session,
            &self.options,
            self.now,
        )
        .await;
        match saved {
            Ok(cookie) => {
                self.cookies.push(cookie);
                Ok(())
            }
            Err(error) => Err(format!("saving session: {error}")),
        }
    }

    /// `session.SetActor`. Only a non-nil actor resolves the expiry and
    /// removes `sg-signout`.
    pub async fn set_actor(
        &mut self,
        actor: Option<&Actor>,
        expiry_period: i64,
        configured_expiry: Option<&str>,
        user_created_at: GoTime,
    ) -> Result<(), String> {
        if actor.is_some() && self.has_sign_out_cookie {
            self.cookies.push(SetCookie {
                name: SIGN_OUT_COOKIE.to_owned(),
                value: String::new(),
                path: String::new(),
                domain: String::new(),
                expires_unix: None,
                max_age: -1,
                http_only: false,
                secure: false,
                same_site: None,
            });
        }
        let value = session_info_json(
            actor,
            expiry_period,
            configured_expiry,
            self.now,
            user_created_at,
        );
        self.set_data("actor", &value).await
    }

    /// `session.SetSignOutCookie`.
    pub fn set_sign_out_cookie(&mut self) {
        self.cookies.push(SetCookie {
            name: SIGN_OUT_COOKIE.to_owned(),
            value: "true".to_owned(),
            path: "/".to_owned(),
            domain: String::new(),
            expires_unix: None,
            max_age: 0,
            http_only: false,
            secure: true,
            same_site: None,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::SessionStoreError;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::Mutex;

    const ID: &str = "OUYT6S23AHN2ZYTKWKMU7YFJT4DGO22FMINRI7TGSVV63QG2BWGA";
    const KEY: &[u8] = b"m9iv-session-key";

    #[derive(Default)]
    struct RecordingStore {
        data: Mutex<HashMap<String, Vec<u8>>>,
        ops: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl SessionStore for RecordingStore {
        async fn load(&self, id: &str) -> Result<Option<Vec<u8>>, SessionStoreError> {
            Ok(self.data.lock().unwrap().get(id).cloned())
        }

        async fn save(
            &self,
            id: &str,
            payload: Vec<u8>,
            ttl_seconds: i64,
        ) -> Result<(), SessionStoreError> {
            self.ops
                .lock()
                .unwrap()
                .push(format!("setex {id} {ttl_seconds}"));
            self.data.lock().unwrap().insert(id.to_owned(), payload);
            Ok(())
        }

        async fn delete(&self, id: &str) -> Result<(), SessionStoreError> {
            self.ops.lock().unwrap().push(format!("del {id}"));
            self.data.lock().unwrap().remove(id);
            Ok(())
        }
    }

    fn now() -> OffsetDateTime {
        OffsetDateTime::from_unix_timestamp(1_760_000_000).unwrap()
    }

    #[test]
    fn base32_and_http_date_match_go() {
        assert_eq!(base32_encode(b""), "");
        assert_eq!(base32_encode(b"f"), "MY");
        assert_eq!(base32_encode(b"foobar"), "MZXW6YTBOI");
        assert_eq!(http_date(1), "Thu, 01 Jan 1970 00:00:01 GMT");
        assert_eq!(http_date(784_111_777), "Sun, 06 Nov 1994 08:49:37 GMT");
    }

    #[test]
    fn duration_parser_matches_time_parse_duration() {
        assert_eq!(parse_go_duration("0"), Some(0));
        assert_eq!(parse_go_duration("24h"), Some(86_400_000_000_000));
        assert_eq!(parse_go_duration("1h30m"), Some(5_400_000_000_000));
        assert_eq!(parse_go_duration("1.5h"), Some(5_400_000_000_000));
        assert_eq!(parse_go_duration("-2m"), Some(-120_000_000_000));
        assert_eq!(parse_go_duration(""), None);
        assert_eq!(parse_go_duration("1"), None);
        assert_eq!(parse_go_duration("1x"), None);
    }

    #[test]
    fn expiry_resolution_matches_set_actor() {
        assert_eq!(resolve_expiry_period(42, Some("1h")), 42);
        assert_eq!(resolve_expiry_period(0, Some("24h")), 86_400_000_000_000);
        assert_eq!(
            resolve_expiry_period(0, Some("bogus")),
            DEFAULT_EXPIRY_PERIOD_NANOS
        );
        assert_eq!(resolve_expiry_period(0, None), DEFAULT_EXPIRY_PERIOD_NANOS);
    }

    #[tokio::test]
    async fn new_session_is_setex_then_cookie() {
        let store = RecordingStore::default();
        let ids = FixedSessionIdSource("GENERATED".to_owned());
        let options = WriteOptions::new(false);
        let mut manager = SessionManager::new(&store, &ids, KEY, options, now(), None, false);

        manager.set_sign_out_cookie();
        manager
            .set_actor(None, 0, None, GoTime(now()))
            .await
            .expect("saved");

        assert_eq!(
            manager
                .cookies
                .iter()
                .map(SetCookie::to_header)
                .collect::<Vec<_>>(),
            vec![
                "sg-signout=true; Path=/; Secure".to_owned(),
                format!(
                    "sgs={}; Path=/; Expires={}; Max-Age=2592000; HttpOnly; SameSite=Lax",
                    securecookie::encode_session_id(
                        "sgs",
                        KEY,
                        now().unix_timestamp(),
                        "GENERATED"
                    ),
                    http_date(now().unix_timestamp() + COOKIE_MAX_AGE)
                ),
            ]
        );
        assert_eq!(
            store.ops.lock().unwrap().as_slice(),
            ["setex GENERATED 2592000"]
        );
        let payload = store
            .data
            .lock()
            .unwrap()
            .get("GENERATED")
            .cloned()
            .expect("payload");
        assert_eq!(
            gob::decode_session_map(&payload).unwrap(),
            vec![("actor".to_owned(), b"null".to_vec())]
        );
    }

    #[tokio::test]
    async fn delete_session_dels_and_expires_the_cookie() {
        let store = RecordingStore::default();
        let ids = FixedSessionIdSource("UNUSED".to_owned());
        let options = WriteOptions::new(false);
        let mut session = WritableSession::new(options.max_age);
        session.id = ID.to_owned();
        session.set_data("actor", b"null");
        store
            .data
            .lock()
            .unwrap()
            .insert(ID.to_owned(), session.encode_for_test());
        let cookie = securecookie::encode_session_id("sgs", KEY, now().unix_timestamp(), ID);
        let mut manager =
            SessionManager::new(&store, &ids, KEY, options, now(), Some(&cookie), false);

        manager.delete_session().await.expect("deleted");
        // The cached session now carries MaxAge = -1, so SetActor's save also
        // deletes, exactly like the baseline's shared registry session.
        manager
            .set_actor(None, 0, None, GoTime(now()))
            .await
            .expect("saved");

        assert_eq!(
            store.ops.lock().unwrap().as_slice(),
            [format!("del {ID}"), format!("del {ID}")]
        );
        assert_eq!(
            manager.cookies.iter().map(SetCookie::to_header).collect::<Vec<_>>(),
            vec![
                "sgs=; Path=/; Expires=Thu, 01 Jan 1970 00:00:01 GMT; Max-Age=0; HttpOnly; SameSite=Lax"
                    .to_owned(),
                "sgs=; Path=/; Expires=Thu, 01 Jan 1970 00:00:01 GMT; Max-Age=0; HttpOnly; SameSite=Lax"
                    .to_owned(),
            ]
        );
    }

    #[tokio::test]
    async fn oversized_payload_is_rejected_before_setex() {
        let store = RecordingStore::default();
        let ids = FixedSessionIdSource("GENERATED".to_owned());
        let options = WriteOptions::new(false);
        let mut manager = SessionManager::new(&store, &ids, KEY, options, now(), None, false);
        manager
            .set_data("blob", "x".repeat(5000).as_bytes())
            .await
            .expect_err("too big");
        assert!(store.ops.lock().unwrap().is_empty());
        assert!(manager.cookies.is_empty());
    }

    #[test]
    fn sign_out_cookie_removal_only_for_an_actor() {
        let store = RecordingStore::default();
        let ids = FixedSessionIdSource("GENERATED".to_owned());
        let options = WriteOptions::new(false);
        let configured = Some("24h");
        let runtime = tokio::runtime::Runtime::new().unwrap();

        let mut manager = SessionManager::new(&store, &ids, KEY, options, now(), None, true);
        runtime
            .block_on(manager.set_actor(None, 0, configured, GoTime(now())))
            .unwrap();
        assert_eq!(manager.cookies.len(), 1);

        let mut manager = SessionManager::new(&store, &ids, KEY, options, now(), None, true);
        runtime
            .block_on(manager.set_actor(
                Some(&Actor {
                    uid: 1,
                    ..Default::default()
                }),
                0,
                configured,
                GoTime(now()),
            ))
            .unwrap();
        let headers: Vec<String> = manager.cookies.iter().map(SetCookie::to_header).collect();
        assert_eq!(headers[0], "sg-signout=; Max-Age=0");
        assert!(headers[1].starts_with("sgs="));
        let payload = store
            .data
            .lock()
            .unwrap()
            .get("GENERATED")
            .cloned()
            .unwrap();
        let values = gob::decode_session_map(&payload).unwrap();
        let actor: SessionInfo = serde_json::from_slice(&values[0].1).unwrap();
        assert_eq!(actor.actor.unwrap().uid, 1);
        assert_eq!(actor.expiry_period, 86_400_000_000_000);
    }

    impl WritableSession {
        fn encode_for_test(&self) -> Vec<u8> {
            let refs: Vec<(&str, &[u8])> = self
                .values
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_slice()))
                .collect();
            gob::encode_session_map(&refs)
        }
    }
}
