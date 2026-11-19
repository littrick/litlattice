#![forbid(unsafe_code)]

//! `internal/database/postgresdsn`: build one DSN per schema from
//! `PG*`/`CODEINTEL_PG*`/`CODEINSIGHTS_PG*` environment variables.

/// Build the DSN for `schema` (`frontend` uses the unprefixed `PG*` set).
pub fn dsn_for_schema(schema: &str) -> String {
    dsn_for_schema_with(schema, |name| std::env::var(name).ok())
}

/// Testable half: `getenv` returns `None` for an unset variable.
pub fn dsn_for_schema_with(schema: &str, getenv: impl Fn(&str) -> Option<String>) -> String {
    let prefix = match schema {
        "frontend" => String::new(),
        other => format!("{}_", other.to_ascii_uppercase()),
    };
    let env = |name: &str| getenv(&format!("{prefix}{name}")).unwrap_or_default();

    if let Some(dsn) = non_empty(env("PGDATASOURCE")) {
        return dsn;
    }

    let username = match non_empty(env("PGUSER")) {
        Some(user) => user,
        None => std::env::var("USER")
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "postgres".to_owned()),
    };
    let password = non_empty(env("PGPASSWORD"));

    let mut host = non_empty(env("PGHOST")).unwrap_or_else(|| "127.0.0.1:5432".to_owned());
    if let Some(port) = non_empty(dequote(&env("PGPORT"))) {
        let hostname = host.split(':').next().unwrap_or(&host).to_owned();
        host = format!("{hostname}:{port}");
    }

    let database = non_empty(env("PGDATABASE")).unwrap_or_default();
    let mut query: Vec<(String, String)> = Vec::new();
    if let Some(sslmode) = non_empty(env("PGSSLMODE")) {
        query.push(("sslmode".to_owned(), sslmode));
    }
    if let Some(tz) = non_empty(env("PGTZ")) {
        query.push(("timezone".to_owned(), tz));
    }
    query.sort();

    let mut out = String::from("postgres://");
    out.push_str(&percent_encode(&username));
    if let Some(password) = password {
        out.push(':');
        out.push_str(&percent_encode(&password));
    }
    out.push('@');
    out.push_str(&host);
    out.push('/');
    out.push_str(&percent_encode_path(&database));
    for (index, (key, value)) in query.iter().enumerate() {
        out.push(if index == 0 { '?' } else { '&' });
        out.push_str(&percent_encode(key));
        out.push('=');
        out.push_str(&percent_encode(value));
    }
    out
}

fn non_empty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

/// Go `dequote`: strip matching single or double quotes around non-short values.
fn dequote(value: &str) -> String {
    let bytes = value.as_bytes();
    if bytes.len() > 2 && bytes[0] == bytes[bytes.len() - 1] && (bytes[0] == b'\'' || bytes[0] == b'"') {
        value[1..value.len() - 1].to_owned()
    } else {
        value.to_owned()
    }
}

/// `url.QueryEscape` for the userinfo component.
fn percent_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char)
            }
            _ => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

fn percent_encode_path(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn dsn(schema: &str, pairs: &[(&str, &str)]) -> String {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        dsn_for_schema_with(schema, |name| map.get(name).cloned())
    }

    #[test]
    fn datasource_wins() {
        assert_eq!(
            dsn("frontend", &[("PGDATASOURCE", "postgres://x/y?sslmode=disable")]),
            "postgres://x/y?sslmode=disable"
        );
        assert_eq!(
            dsn("codeintel", &[("CODEINTEL_PGDATASOURCE", "postgres://c/d")]),
            "postgres://c/d"
        );
    }

    #[test]
    fn builds_components_and_query() {
        assert_eq!(
            dsn(
                "frontend",
                &[
                    ("PGHOST", "db.example"),
                    ("PGPORT", "6543"),
                    ("PGUSER", "sg"),
                    ("PGPASSWORD", "p@ss"),
                    ("PGDATABASE", "sourcegraph"),
                    ("PGSSLMODE", "require"),
                ]
            ),
            "postgres://sg:p%40ss@db.example:6543/sourcegraph?sslmode=require"
        );
    }

    #[test]
    fn codeintel_prefix_is_applied() {
        assert_eq!(
            dsn(
                "codeintel",
                &[
                    ("CODEINTEL_PGHOST", "127.0.0.1"),
                    ("CODEINTEL_PGPORT", "'5555'"),
                    ("CODEINTEL_PGUSER", "ci"),
                    ("CODEINTEL_PGDATABASE", "sourcegraph-codeintel"),
                ]
            ),
            "postgres://ci@127.0.0.1:5555/sourcegraph-codeintel"
        );
    }

    #[test]
    fn default_host_and_user() {
        let value = dsn("frontend", &[]);
        assert!(value.starts_with("postgres://"), "{value}");
        assert!(value.contains("@127.0.0.1:5432/"), "{value}");
    }
}
