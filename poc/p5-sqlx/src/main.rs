//! P5: sqlx <-> Postgres type mapping roundtrip against the real Sourcegraph
//! schema (read-only for existing tables; a TEMP table is used for writes).
//!
//! DATABASE_URL=postgres://sourcegraph:sourcegraph@127.0.0.1:5432/sourcegraph

use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::Row;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sourcegraph:sourcegraph@127.0.0.1:5432/sourcegraph".into());
    let pool = PgPoolOptions::new().max_connections(1).connect(&url).await?;

    let mut out = json!({});

    // 1. citext (repo.name) is a USER-DEFINED type; decode without a cast?
    match sqlx::query_scalar::<_, String>("select username from users order by id limit 1")
        .fetch_one(&pool)
        .await
    {
        Ok(v) => out["citext_raw"] = json!({"ok": true, "value": v}),
        Err(e) => out["citext_raw"] = json!({"ok": false, "error": e.to_string()}),
    }

    // 2. Explicit cast to text always works.
    let name: String = sqlx::query_scalar("select username::text from users order by id limit 1")
        .fetch_one(&pool)
        .await?;
    out["citext_cast"] = json!(name);

    // 3. Read representative columns of a real table.
    let user = sqlx::query(
        "select id, username::text as username, display_name, created_at, updated_at,
                deleted_at, site_admin
         from users order by id limit 1",
    )
    .fetch_one(&pool)
    .await?;
    out["user"] = json!({
        "id": user.try_get::<i32, _>("id")?,
        "username": user.try_get::<String, _>("username")?,
        "display_name": user.try_get::<Option<String>, _>("display_name")?,
        "created_at": user.try_get::<DateTime<Utc>, _>("created_at")?.to_rfc3339(),
        "updated_at": user.try_get::<DateTime<Utc>, _>("updated_at")?.to_rfc3339(),
        "deleted_at": user.try_get::<Option<DateTime<Utc>>, _>("deleted_at")?.map(|t| t.to_rfc3339()),
        "site_admin": user.try_get::<bool, _>("site_admin")?,
    });

    // 4. TEMP table roundtrip for types not well represented in `repo`.
    sqlx::query(
        "create temp table p5_types (
            ts timestamptz, js jsonb, arr text[], bin bytea,
            num numeric, big bigint, txt text
         )",
    )
    .execute(&pool)
    .await?;

    let ts: DateTime<Utc> = "2024-03-01T10:34:56.123456Z".parse()?;
    let js = json!({"b": 2, "a": "x", "nested": {"k": [1, null, "é"]}});
    let arr: Vec<Option<&str>> = vec![Some("a"), Some("b,c"), None];
    let bin: Vec<u8> = vec![0x00, 0xff, 0x10];
    let num: BigDecimal = "12345678901234567890.123456789".parse()?;
    let big: i64 = 9_007_199_254_740_993;
    let txt: Option<String> = None;

    eprintln!("num before bind: {num}");
    sqlx::query(
        "insert into p5_types (ts, js, arr, bin, num, big, txt)
         values ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(ts)
    .bind(&js)
    .bind(&arr)
    .bind(&bin)
    .bind(&num)
    .bind(big)
    .bind(&txt)
    .execute(&pool)
    .await?;

    // Insert a second row with an explicit zone offset to prove instant semantics.
    sqlx::query(
        "insert into p5_types (ts) values ('2024-03-01 12:34:56.123456+02'::timestamptz)",
    )
    .execute(&pool)
    .await?;

    type Row = (
        DateTime<Utc>,
        Option<Value>,
        Option<Vec<Option<String>>>,
        Option<Vec<u8>>,
        Option<BigDecimal>,
        Option<i64>,
        Option<String>,
    );
    let rows = sqlx::query_as::<_, Row>(
        "select ts, js, arr, bin, num, big, txt from p5_types order by ts",
    )
    .fetch_all(&pool)
    .await?;
    out["roundtrip"] = json!(rows
        .iter()
        .map(|(ts, js, arr, bin, num, big, txt)| json!({
            "ts": ts.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            "js": js,
            "arr": arr,
            "bin_hex": bin.as_deref().map(hex),
            "num": num.as_ref().map(|n| n.to_string()),
            "big": big,
            "txt": txt,
        }))
        .collect::<Vec<_>>());

    // 5. SQL commenter: sqlx has no hook; the comment must be part of the SQL.
    let one: i32 = sqlx::query_scalar(
        "/*{\"app\":\"sourcegraph\",\"controller\":\"p5\",\"action\":\"probe\",\"route\":\"/p5\"}*/ select 1",
    )
    .fetch_one(&pool)
    .await?;
    out["commenter_select"] = json!(one);

    // 6. Session parameters that affect decoding/comparison.
    let tz: String = sqlx::query_scalar("show timezone").fetch_one(&pool).await?;
    let datestyle: String = sqlx::query_scalar("show datestyle").fetch_one(&pool).await?;
    out["session"] = json!({"timezone": tz, "datestyle": datestyle});

    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
}
