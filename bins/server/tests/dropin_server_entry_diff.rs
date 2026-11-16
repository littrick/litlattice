//! M9.18c differential harness: early `server` exit codes.
//!
//! Runs the real `server` binary (no listeners are bound: both scenarios exit
//! before `launch_stages`) and emits the same JSON as the Go oracle
//! `cmd/server/shared/zz_m918c_server_entry_oracle_test.go`.
//!
//! Gated on `SG_M918C_EXIT_HARNESS=1` plus `M918C_CORPUS`/`M918C_RUST_EXIT_OUT`.

use std::path::PathBuf;
use std::process::Command;

use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct ExitCase {
    name: String,
    scenario: String,
    expected: i32,
}

#[derive(Deserialize)]
struct Corpus {
    exit_codes: Vec<ExitCase>,
}

#[derive(Serialize)]
struct ExitOut {
    name: String,
    code: i32,
}

#[derive(Serialize)]
struct Output {
    exit_codes: Vec<ExitOut>,
}

fn temp_dir(prefix: &str) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "{prefix}_{}_{}_{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).expect("temp dir");
    path
}

fn base_env(command: &mut Command, home: &PathBuf) {
    command.env_clear();
    command.env("HOME", home);
    command.env("PATH", "/usr/local/bin:/usr/bin:/bin");
    // Keep the run independent from the ambient deployment.
    command.env_remove("SOURCEGRAPH_5_1_DB_MIGRATION");
    command.env_remove("SRC_PROF_SERVICES");
    command.env_remove("PGHOST");
}

fn write_stub(dir: &std::path::Path, name: &str) {
    let path = dir.join(name);
    std::fs::write(&path, "#!/bin/sh\nexit 0\n").expect("stub");
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).expect("chmod stub");
}

fn run_case(scenario: &str) -> i32 {
    let root = temp_dir("m918c_server");
    let home = root.join("home");
    std::fs::create_dir_all(&home).expect("home");

    let mut command = Command::new(env!("CARGO_BIN_EXE_server"));
    base_env(&mut command, &home);

    match scenario {
        "config-dir-under-regular-file" => {
            // `os.MkdirAll(CONFIG_DIR)` fails -> `log.Fatalf` -> exit 1.
            let blocker = root.join("blocker");
            std::fs::write(&blocker, b"not a directory").expect("blocker");
            command.env("CONFIG_DIR", blocker.join("conf"));
            command.env("DATA_DIR", root.join("data"));
            command.env("PGHOST", "");
        }
        "local-postgres-without-migration-flag" => {
            // Local Postgres without the marker -> reindex required; without
            // `SOURCEGRAPH_5_1_DB_MIGRATION=true` -> os.Exit(101).
            //
            // Go `shared.Main` provisions the local database
            // (`maybePostgresProcFile`, `shared.go:200-241`) before `run`
            // evaluates the 5.1 gate, and only an *upgraded* install (existing
            // data directory) reaches the gate without a marker. Model that:
            // pre-create the data/marker layout so provisioning takes the
            // existing-database path (no fresh init, hence no 5.1 marker), and
            // stub the root-only local tools so provisioning succeeds here.
            let data = root.join("data");
            std::fs::create_dir_all(data.join("postgresql")).expect("postgres data dir");
            let markers = data.join("postgresql-markers");
            std::fs::create_dir_all(&markers).expect("markers dir");
            for database in ["sourcegraph", "sourcegraph-codeintel", "sourcegraph-codeinsights"] {
                std::fs::write(markers.join(database), b"").expect("marker");
            }
            let stubs = root.join("stubs");
            std::fs::create_dir_all(&stubs).expect("stubs");
            for tool in ["mkdir", "chown", "su-exec"] {
                write_stub(&stubs, tool);
            }
            command.env(
                "PATH",
                format!("{}:/usr/local/bin:/usr/bin:/bin", stubs.display()),
            );
            command.env("CONFIG_DIR", root.join("config"));
            command.env("DATA_DIR", data);
            command.env("PGHOST", "");
        }
        other => panic!("unknown scenario {other}"),
    }

    let output = command.output().expect("run server");
    output.status.code().expect("server exited via code")
}

#[test]
fn dump_server_exit_codes() {
    if std::env::var("SG_M918C_EXIT_HARNESS").as_deref() != Ok("1") {
        return;
    }
    let corpus_path = std::env::var("M918C_CORPUS").expect("M918C_CORPUS");
    let output_path = std::env::var("M918C_RUST_EXIT_OUT").expect("M918C_RUST_EXIT_OUT");
    let corpus: Corpus =
        serde_json::from_slice(&std::fs::read(corpus_path).expect("corpus")).expect("corpus json");

    let mut codes = Vec::new();
    for case in &corpus.exit_codes {
        let code = run_case(&case.scenario);
        assert_eq!(
            code, case.expected,
            "{} expected exit {} but got {}",
            case.name, case.expected, code
        );
        codes.push(ExitOut {
            name: case.name.clone(),
            code,
        });
    }

    let encoded = serde_json::to_vec(&Output { exit_codes: codes }).expect("serialize");
    std::fs::write(output_path, encoded).expect("write output");
}
