//! P4: Sourcegraph session cookie + Redis gob PoC.
//!
//! Two compatibility surfaces are exercised:
//!
//! 1. `gorilla/securecookie` v1.1.1 cookie `sgs` (HMAC-SHA256, gob payload);
//! 2. `boj/redistore` GobSerializer payload in Redis `session_<id>`
//!    (`map[interface{}]interface{}` with string keys and []byte values).

mod gob;
mod securecookie;
mod session;

use base64::Engine;
use serde_json::{json, Value};
use std::process::ExitCode;

const B64: base64::engine::general_purpose::GeneralPurpose =
    base64::engine::general_purpose::STANDARD;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        usage();
        return Err("missing subcommand".into());
    }
    let cmd = args.remove(0);
    let opts = Opts::parse(&args)?;
    match cmd.as_str() {
        "decode-cookie" => cmd_decode_cookie(&opts),
        "encode-cookie" => cmd_encode_cookie(&opts),
        "decode-gob-string" => cmd_decode_gob_string(&opts),
        "decode-map" => cmd_decode_map(&opts),
        "encode-map" => cmd_encode_map(&opts),
        "parse-actor" => cmd_parse_actor(&opts),
        "gen-id" => cmd_gen_id(&opts),
        "redis-get" => cmd_redis_get(&opts),
        "redis-set" => cmd_redis_set(&opts),
        "redis-del" => cmd_redis_del(&opts),
        other => {
            usage();
            Err(format!("unknown subcommand {other:?}"))
        }
    }
}

fn usage() {
    eprintln!(
        "usage: p4-session <subcommand> [args] [--key K] [--ts N] [--now N] [--redis ADDR]\n\
         \n\
         decode-cookie <cookie>             verify and decode sgs cookie\n\
         encode-cookie <id>                 build sgs cookie for a session ID\n\
         decode-gob-string <hex|->          decode a gob string (hex, '-' = stdin)\n\
         decode-map <hex|->                 decode a session gob map\n\
         encode-map <json|->                encode a session gob map from JSON\n\
         parse-actor <json|->               decode a session actor JSON document\n\
         gen-id                             generate a session ID (base32, 52 chars)\n\
         redis-get <cookie>                 decode cookie, load session_<id> from Redis\n\
         redis-set <id> <json|->            write session_<id> payload (JSON values)\n\
         redis-del <id>                     delete session_<id>\n\
         \n\
         --key K     SRC_SESSION_COOKIE_KEY (default: env or empty)\n\
         --ts N      unix seconds for encoding (default: now)\n\
         --now N     unix seconds used for max-age checks (default: now)\n\
         --ttl N     Redis TTL in seconds (default: {})\n\
         --redis A   Redis address (default: 127.0.0.1:6379)",
        session::TTL_SECONDS
    );
}

struct Opts {
    positional: Vec<String>,
    key: Vec<u8>,
    ts: Option<i64>,
    now: Option<i64>,
    ttl: Option<u64>,
    redis: String,
}

impl Opts {
    fn parse(args: &[String]) -> Result<Self, String> {
        let mut o = Opts {
            positional: Vec::new(),
            key: std::env::var("SRC_SESSION_COOKIE_KEY")
                .unwrap_or_default()
                .into_bytes(),
            ts: None,
            now: None,
            ttl: None,
            redis: "127.0.0.1:6379".into(),
        };
        let mut it = args.iter();
        while let Some(a) = it.next() {
            match a.as_str() {
                "--key" => o.key = it.next().ok_or("--key needs a value")?.clone().into_bytes(),
                "--ts" => o.ts = Some(parse_i64(it.next(), "--ts")?),
                "--now" => o.now = Some(parse_i64(it.next(), "--now")?),
                "--ttl" => o.ttl = Some(parse_i64(it.next(), "--ttl")? as u64),
                "--redis" => o.redis = it.next().ok_or("--redis needs a value")?.clone(),
                other => o.positional.push(other.to_string()),
            }
        }
        Ok(o)
    }

    fn arg(&self, i: usize, what: &str) -> Result<&str, String> {
        self.positional
            .get(i)
            .map(String::as_str)
            .ok_or_else(|| format!("missing {what}"))
    }

    fn now(&self) -> i64 {
        self.now.unwrap_or_else(now_secs)
    }
}

fn parse_i64(v: Option<&String>, flag: &str) -> Result<i64, String> {
    v.ok_or_else(|| format!("{flag} needs a value"))?
        .parse()
        .map_err(|_| format!("{flag}: not an integer"))
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn print_json(v: &Value) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string_pretty(v).map_err(|e| e.to_string())?
    );
    Ok(())
}

fn read_hex_or_stdin(arg: &str) -> Result<Vec<u8>, String> {
    let s = if arg == "-" {
        let mut s = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut s).map_err(|e| e.to_string())?;
        s
    } else {
        arg.to_string()
    };
    let s: String = s.chars().filter(|c| !c.is_whitespace()).collect();
    if s.len() % 2 != 0 {
        return Err("hex input has odd length".into());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| e.to_string()))
        .collect()
}

fn read_json(arg: &str) -> Result<Value, String> {
    let s = if arg == "-" {
        let mut s = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut s).map_err(|e| e.to_string())?;
        s
    } else {
        arg.to_string()
    };
    serde_json::from_str(&s).map_err(|e| e.to_string())
}

fn parse_entries(v: &Value) -> Result<Vec<(String, Vec<u8>)>, String> {
    // Either {"entries":[{"key":..,"base64":..}]} or {"actor":"<base64>", ...}.
    if let Some(entries) = v.get("entries").and_then(Value::as_array) {
        let mut out = Vec::with_capacity(entries.len());
        for e in entries {
            let key = e
                .get("key")
                .and_then(Value::as_str)
                .ok_or("entry missing key")?
                .to_string();
            let b64 = e
                .get("base64")
                .and_then(Value::as_str)
                .ok_or("entry missing base64")?;
            out.push((
                key,
                B64.decode(b64).map_err(|e| format!("bad base64: {e}"))?,
            ));
        }
        return Ok(out);
    }
    let obj = v.as_object().ok_or("expected a JSON object")?;
    let mut out = Vec::with_capacity(obj.len());
    for (k, val) in obj {
        let b64 = val.as_str().ok_or("values must be base64 strings")?;
        out.push((
            k.clone(),
            B64.decode(b64).map_err(|e| format!("bad base64: {e}"))?,
        ));
    }
    Ok(out)
}

fn entries_json(entries: &[(String, Vec<u8>)]) -> Value {
    json!({
        "entries": entries.iter().map(|(k, v)| json!({
            "key": k,
            "base64": B64.encode(v),
            "len": v.len(),
        })).collect::<Vec<_>>()
    })
}

fn cmd_decode_cookie(o: &Opts) -> Result<(), String> {
    let cookie = o.arg(0, "cookie")?;
    let (ts, id) = securecookie::decode_session_id(session::COOKIE_NAME, &o.key, cookie, o.now())
        .map_err(|e| e.to_string())?;
    print_json(&json!({
        "name": session::COOKIE_NAME,
        "timestamp": ts,
        "id": id,
        "id_len": id.len(),
    }))
}

fn cmd_encode_cookie(o: &Opts) -> Result<(), String> {
    let id = o.arg(0, "id")?;
    let ts = o.ts.unwrap_or_else(now_secs);
    println!(
        "{}",
        securecookie::encode_session_id(session::COOKIE_NAME, &o.key, ts, id)
    );
    Ok(())
}

fn cmd_decode_gob_string(o: &Opts) -> Result<(), String> {
    let bytes = read_hex_or_stdin(o.arg(0, "hex")?)?;
    let s = gob::decode_string(&bytes).map_err(|e| e.to_string())?;
    print_json(&json!({"value": s, "len": s.len()}))
}

fn cmd_decode_map(o: &Opts) -> Result<(), String> {
    let bytes = read_hex_or_stdin(o.arg(0, "hex")?)?;
    let entries = gob::decode_session_map(&bytes).map_err(|e| e.to_string())?;
    print_json(&json!({
        "entries": entries.iter().map(|(k, v)| json!({
            "key": k,
            "base64": B64.encode(v),
            "len": v.len(),
            "utf8": std::str::from_utf8(v).ok(),
        })).collect::<Vec<_>>()
    }))
}

fn cmd_encode_map(o: &Opts) -> Result<(), String> {
    let v = read_json(o.arg(0, "json")?)?;
    let entries = parse_entries(&v)?;
    let refs: Vec<(&str, &[u8])> = entries
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_slice()))
        .collect();
    let bytes = gob::encode_session_map(&refs);
    println!(
        "{}",
        bytes
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    );
    Ok(())
}

fn cmd_parse_actor(o: &Opts) -> Result<(), String> {
    let v = read_json(o.arg(0, "json")?)?;
    let raw = if let Some(s) = v.as_str() {
        B64.decode(s).map_err(|e| format!("bad base64: {e}"))?
    } else {
        serde_json::to_vec(&v).map_err(|e| e.to_string())?
    };
    let info: session::SessionInfo =
        serde_json::from_slice(&raw).map_err(|e| format!("parse sessionInfo: {e}"))?;
    // Prove the JSON round-trips byte-identically (Go struct field order).
    let reencoded = serde_json::to_vec(&info).map_err(|e| e.to_string())?;
    print_json(&json!({
        "actor": info.actor,
        "lastActive": info.last_active.format_go(),
        "expiryPeriod": info.expiry_period,
        "userCreatedAt": info.user_created_at.format_go(),
        "reencode_equal": reencoded == raw,
    }))
}

fn cmd_gen_id(_o: &Opts) -> Result<(), String> {
    print_json(&json!({"id": session::generate_id()}))
}

async fn redis_conn(addr: &str) -> Result<redis::aio::MultiplexedConnection, String> {
    let client = redis::Client::open(format!("redis://{addr}")).map_err(|e| e.to_string())?;
    client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| e.to_string())
}

fn cmd_redis_get(o: &Opts) -> Result<(), String> {
    let cookie = o.arg(0, "cookie")?.to_string();
    let addr = o.redis.clone();
    let key = o.key.clone();
    let now = o.now();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    rt.block_on(async move {
        let (ts, id) = securecookie::decode_session_id(session::COOKIE_NAME, &key, &cookie, now)
            .map_err(|e| e.to_string())?;
        let mut conn = redis_conn(&addr).await?;
        let payload = session::load(&mut conn, &id).await.map_err(|e| e.to_string())?;
        let (found, entries, raw_entries) = match payload {
            None => (false, Vec::new(), Value::Null),
            Some(raw) => {
                let s = session::Session::from_redis(id.clone(), ts, &raw)
                    .map_err(|e| format!("gob decode: {e}"))?;
                let j = entries_json(&s.values);
                (true, s.values, j)
            }
        };
        let actor = entries
            .iter()
            .find(|(k, _)| k == "actor")
            .map(|(_, raw)| {
                serde_json::from_slice::<session::SessionInfo>(raw)
                    .map(|info| {
                        json!({
                            "actor": info.actor,
                            "lastActive": info.last_active.format_go(),
                            "expiryPeriod": info.expiry_period,
                            "userCreatedAt": info.user_created_at.format_go(),
                        })
                    })
                    .map_err(|e| format!("actor JSON: {e}"))
            })
            .transpose()?;
        print_json(&json!({
            "cookie_timestamp": ts,
            "id": id,
            "redis_found": found,
            "entries": raw_entries,
            "actor": actor,
        }))
    })
}

fn cmd_redis_set(o: &Opts) -> Result<(), String> {
    let id = o.arg(0, "id")?.to_string();
    let v = read_json(o.arg(1, "json")?)?;
    let addr = o.redis.clone();
    let ttl = o.ttl.unwrap_or(session::TTL_SECONDS);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    rt.block_on(async move {
        let values = parse_entries(&v)?;
        let store = session::Session {
            timestamp: now_secs(),
            id: id.clone(),
            values,
        };
        let payload = store.encode_redis();
        let mut conn = redis_conn(&addr).await?;
        session::save(&mut conn, &id, &payload, ttl)
            .await
            .map_err(|e| e.to_string())?;
        print_json(&json!({
            "key": format!("{}{}", session::KEY_PREFIX, id),
            "ttl": ttl,
            "bytes": payload.len(),
            "gob_hex": payload.iter().map(|b| format!("{b:02x}")).collect::<String>(),
        }))
    })
}

fn cmd_redis_del(o: &Opts) -> Result<(), String> {
    let id = o.arg(0, "id")?.to_string();
    let addr = o.redis.clone();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    rt.block_on(async move {
        let mut conn = redis_conn(&addr).await?;
        session::delete(&mut conn, &id)
            .await
            .map_err(|e| e.to_string())?;
        print_json(&json!({"deleted": format!("{}{}", session::KEY_PREFIX, id)}))
    })
}
