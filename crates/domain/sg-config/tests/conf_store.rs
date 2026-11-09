//! M9.8ee differential harness: drives `sg_config::store::ConfigStore` through
//! the same sequence as the Go `internal/conf` harness and prints one
//! normalized line per case. The Go side prints the identical format.

use sg_config::store::{ConfigStore, StoreError};
use sg_config::{RawUnified, ServiceConnections, SiteConfiguration, Unified};

fn connections(git: &[&str]) -> ServiceConnections {
    ServiceConnections {
        git_servers: git.iter().map(|value| (*value).to_owned()).collect(),
        postgres_dsn: "pg1".to_owned(),
        ..ServiceConnections::default()
    }
}

fn print_line(
    name: &str,
    initialized: bool,
    changed: bool,
    error: Option<&StoreError>,
    store: &ConfigStore,
) {
    let error_class = match error {
        None => "none",
        Some(StoreError::EmptySite) => "empty",
        Some(StoreError::Parse(_)) => "parse",
    };

    let (providers, git, postgres, raw_id, raw_site) = if initialized {
        let unified = store.last_valid().expect("initialized");
        let providers = unified
            .site
            .raw()
            .get("auth.providers")
            .and_then(serde_json::Value::as_array)
            .map_or(0, Vec::len);
        let connections = &unified.service_connections;
        let raw = store.raw();
        (
            providers.to_string(),
            connections.git_servers.join(","),
            connections.postgres_dsn.clone(),
            raw.id.to_string(),
            format!("{:?}", raw.site),
        )
    } else {
        (
            "-".to_owned(),
            "-".to_owned(),
            "-".to_owned(),
            "-".to_owned(),
            "-".to_owned(),
        )
    };

    println!(
        "cs {name} changed={changed} err={error_class} providers={providers} git={git} pg={postgres} raw_id={raw_id} raw_site={raw_site}"
    );
}

#[test]
fn conf_store_harness() {
    let store = ConfigStore::new();
    let site_a = r#"{"auth.providers":[{"type":"builtin"}]}"#;
    let site_b = r#"{"auth.providers":[{"type":"builtin"}],"externalURL":"https://ext.example"}"#;

    // 1. Empty site before initialization: rejected, nothing recorded.
    let (result, error) = store.maybe_update(&RawUnified {
        site: String::new(),
        ..RawUnified::default()
    });
    print_line(
        "empty-first",
        false,
        result.changed,
        error.as_ref().err(),
        &store,
    );

    // 2. First valid configuration: initializes the store.
    let (result, error) = store.maybe_update(&RawUnified {
        id: 7,
        site: site_a.to_owned(),
        service_connections: connections(&["g1", "g2"]),
    });
    print_line("first", true, result.changed, error.as_ref().err(), &store);

    // 3. Same site/connections with a different ID: Equal ignores the ID.
    let (result, error) = store.maybe_update(&RawUnified {
        id: 8,
        site: site_a.to_owned(),
        service_connections: connections(&["g1", "g2"]),
    });
    print_line(
        "same-ignoring-id",
        true,
        result.changed,
        error.as_ref().err(),
        &store,
    );

    // 4. Site change.
    let (result, error) = store.maybe_update(&RawUnified {
        id: 9,
        site: site_b.to_owned(),
        service_connections: connections(&["g1", "g2"]),
    });
    print_line(
        "site-change",
        true,
        result.changed,
        error.as_ref().err(),
        &store,
    );

    // 5. Service connection change with the same site.
    let (result, error) = store.maybe_update(&RawUnified {
        id: 10,
        site: site_b.to_owned(),
        service_connections: connections(&["g3"]),
    });
    print_line(
        "connections-change",
        true,
        result.changed,
        error.as_ref().err(),
        &store,
    );

    // 6. Parse failure: Raw advances before parsing, LastValid does not.
    let (result, error) = store.maybe_update(&RawUnified {
        id: 11,
        site: "{".to_owned(),
        service_connections: connections(&["g3"]),
    });
    print_line(
        "parse-fail",
        true,
        result.changed,
        error.as_ref().err(),
        &store,
    );

    // 7. Repeating the same invalid raw value is Equal and error-free.
    let (result, error) = store.maybe_update(&RawUnified {
        id: 11,
        site: "{".to_owned(),
        service_connections: connections(&["g3"]),
    });
    print_line(
        "same-invalid",
        true,
        result.changed,
        error.as_ref().err(),
        &store,
    );

    // 8. An empty site after a failure does not clear Raw.
    let (result, error) = store.maybe_update(&RawUnified {
        site: String::new(),
        ..RawUnified::default()
    });
    print_line(
        "empty-after-fail",
        true,
        result.changed,
        error.as_ref().err(),
        &store,
    );

    // 9. Mock takes precedence for LastValid/Raw.
    store.mock(Unified {
        site: SiteConfiguration::parse(r#"{"externalURL":"https://mock.example"}"#)
            .expect("mock site"),
        service_connections: ServiceConnections {
            postgres_dsn: "mock".to_owned(),
            ..ServiceConnections::default()
        },
    });
    print_line("mock", true, false, None, &store);
}
