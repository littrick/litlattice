#![forbid(unsafe_code)]

//! Procfile parsing and the `goreman` supervisor lifecycle used by
//! `cmd/server` (`cmd/server/internal/goreman`).
//!
//! [`parse_procfile`] and [`simulate`] are pure: the differential harness feeds
//! them recorded child/signal events. [`run_staged`] is the real supervisor:
//! each entry runs as `/bin/sh -c "exec <command>"` in its own process group,
//! the first SIGINT/SIGTERM/SIGHUP is forwarded as SIGINT to every group, a
//! second signal (or the 10 second grace period) escalates to SIGKILL, and a
//! child that exits on its own shuts the group down unless
//! `IGNORE_PROCESS_DEATH` is set.

use std::process::Stdio;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::task::JoinSet;

/// Goreman's shutdown grace period (`proc.go`).
pub const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiedAction {
    Shutdown,
    Ignore,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcfileEntry {
    pub name: String,
    pub command: String,
}

impl ProcfileEntry {
    /// `/bin/sh -c` argument used by `proc.Start`.
    pub fn shell_command(&self) -> String {
        format!("exec {}", self.command)
    }
}

/// Parse Procfile lines. Blank lines and `#` comments are skipped; a line
/// without `:`, or with an empty name/command, is ignored.
pub fn parse_procfile(lines: &[String]) -> Vec<ProcfileEntry> {
    let mut entries = Vec::new();
    for line in lines {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((name, command)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim();
        let command = command.trim();
        if name.is_empty() || command.is_empty() {
            continue;
        }
        entries.push(ProcfileEntry {
            name: name.to_owned(),
            command: command.to_owned(),
        });
    }
    entries
}

/// Recorded events for [`simulate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SimEvent {
    Signal,
    ChildDied(String),
    ChildExited(String),
    GraceElapsed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleOutcome {
    pub actions: Vec<String>,
    /// Names in the order their process groups received SIGINT.
    pub sigint_targets: Vec<String>,
    pub sigkill_targets: Vec<String>,
    /// `goreman.Start` returns `nil`, so the process always exits 0.
    pub exit_code: i32,
}

/// Deterministic model of `goreman.Start` for one Procfile group.
pub fn simulate(
    entries: &[ProcfileEntry],
    events: &[SimEvent],
    died_action: DiedAction,
) -> LifecycleOutcome {
    let mut running: Vec<String> = entries.iter().map(|entry| entry.name.clone()).collect();
    let mut outcome = LifecycleOutcome {
        actions: Vec::new(),
        sigint_targets: Vec::new(),
        sigkill_targets: Vec::new(),
        exit_code: 0,
    };
    let mut stopping = false;
    let mut done = false;

    for event in events {
        if done {
            break;
        }
        match event {
            SimEvent::Signal => {
                if !stopping {
                    outcome.actions.push("SIGINT".to_owned());
                    outcome.sigint_targets = running.clone();
                    stopping = true;
                } else {
                    outcome.actions.push("SIGKILL".to_owned());
                    outcome.sigkill_targets = running.clone();
                    done = true;
                }
            }
            SimEvent::GraceElapsed => {
                if stopping {
                    outcome.actions.push("SIGKILL".to_owned());
                    outcome.sigkill_targets = running.clone();
                }
                done = true;
            }
            SimEvent::ChildDied(name) | SimEvent::ChildExited(name) => {
                if stopping {
                    running.retain(|running| running != name);
                    if running.is_empty() {
                        done = true;
                    }
                } else if died_action == DiedAction::Shutdown {
                    outcome.actions.push(format!("{name} died. Shutting down..."));
                    outcome.sigint_targets = running.clone();
                    stopping = true;
                } else {
                    outcome.actions.push(format!("{name} died."));
                    running.retain(|running| running != name);
                    if running.is_empty() {
                        done = true;
                    }
                }
            }
        }
    }

    outcome
}

/// `HH:MM:SS` with the same shape as `log.go`'s `time.Now().Format("15:04:05")`
/// (UTC here; the timestamp is informational, only the name prefix is part of
/// the supervisor contract).
fn now_hhmmss() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
        % 86_400;
    format!(
        "{:02}:{:02}:{:02}",
        seconds / 3600,
        (seconds % 3600) / 60,
        seconds % 60
    )
}

/// `createLogger`/`clogger`: print every child line as
/// `HH:MM:SS <right-aligned name> | <line>`.
///
/// The baseline flushes partial lines after 2ms; here a line is emitted when
/// its newline arrives. All service loggers write newline-terminated records.
fn spawn_log_reader<R>(stream: R, name: String, width: usize)
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut lines = BufReader::new(stream).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    println!("{} {:>width$} | {}", now_hhmmss(), name, line, width = width);
                }
                Ok(None) | Err(_) => return,
            }
        }
    });
}

fn signal_group(pid: u32, signal: &str) {
    // Negative pid addresses the process group created by `process_group(0)`.
    let _ = std::process::Command::new("kill")
        .arg("-s")
        .arg(signal)
        .arg("--")
        .arg(format!("-{pid}"))
        .status();
}

struct Supervisor {
    died_action: DiedAction,
    /// The final environment after `SetDefaultEnv` was applied. The baseline
    /// mutates the process environment, so every child inherits these
    /// defaults; the Rust port keeps them in an [`EnvMap`](crate::env::EnvMap)
    /// and must export them explicitly.
    env: Vec<(String, String)>,
    tasks: JoinSet<String>,
    pids: Vec<(String, u32)>,
    /// `goreman`'s `maxProcNameLength`: the widest process name read from the
    /// Procfiles so far. Log prefixes right-align names to this width.
    max_name_len: usize,
}

impl Supervisor {
    fn new(died_action: DiedAction, env: Vec<(String, String)>) -> Self {
        Self {
            died_action,
            env,
            tasks: JoinSet::new(),
            pids: Vec::new(),
            max_name_len: 0,
        }
    }

    fn start(&mut self, entries: &[ProcfileEntry]) -> bool {
        // `readProcfile` records the widest name before any process starts, so
        // every line of this group uses the same width (`log.go`).
        self.max_name_len = entries
            .iter()
            .map(|entry| entry.name.len())
            .chain(std::iter::once(self.max_name_len))
            .max()
            .unwrap_or(0);
        let name_width = self.max_name_len;

        let mut all_spawned = true;
        for entry in entries {
            let mut command = tokio::process::Command::new("/bin/sh");
            command.arg("-c").arg(entry.shell_command());
            command.envs(self.env.iter().map(|(key, value)| (key, value)));
            // `spawnProc` sets `cmd.Stdout`/`cmd.Stderr` to a logger that
            // prefixes every line with the process name.
            command.stdout(Stdio::piped()).stderr(Stdio::piped());
            #[cfg(unix)]
            {
                command.process_group(0);
            }
            match command.spawn() {
                Ok(mut child) => {
                    let pid = child.id().unwrap_or(0);
                    if let Some(stdout) = child.stdout.take() {
                        spawn_log_reader(stdout, entry.name.clone(), name_width);
                    }
                    if let Some(stderr) = child.stderr.take() {
                        spawn_log_reader(stderr, entry.name.clone(), name_width);
                    }
                    self.pids.push((entry.name.clone(), pid));
                    let name = entry.name.clone();
                    self.tasks.spawn(async move {
                        let _ = child.wait().await;
                        name
                    });
                }
                Err(error) => {
                    eprintln!("{}: failed to start: {error}", entry.name);
                    all_spawned = false;
                }
            }
        }
        if !all_spawned && self.died_action == DiedAction::Shutdown && !self.pids.is_empty() {
            self.signal_all("INT");
            true
        } else {
            all_spawned
        }
    }

    fn signal_all(&mut self, signal: &str) {
        for (_, pid) in &self.pids {
            signal_group(*pid, signal);
        }
    }

    async fn shutdown(&mut self) {
        self.signal_all("INT");
        let deadline = tokio::time::Instant::now() + SHUTDOWN_GRACE;
        loop {
            tokio::select! {
                _ = termination_signal() => {
                    self.signal_all("KILL");
                    while self.tasks.join_next().await.is_some() {}
                    return;
                }
                _ = tokio::time::sleep_until(deadline) => {
                    self.signal_all("KILL");
                    while self.tasks.join_next().await.is_some() {}
                    return;
                }
                joined = self.tasks.join_next() => {
                    match joined {
                        Some(Ok(_)) => {
                            if self.tasks.is_empty() {
                                return;
                            }
                        }
                        Some(Err(_)) => {}
                        None => return,
                    }
                }
            }
        }
    }

    async fn wait(mut self) -> i32 {
        let mut stopping = false;
        let mut deadline: Option<tokio::time::Instant> = None;
        loop {
            let sleep_until = deadline.unwrap_or_else(|| tokio::time::Instant::now());
            tokio::select! {
                _ = termination_signal() => {
                    if !stopping {
                        stopping = true;
                        self.signal_all("INT");
                        deadline = Some(tokio::time::Instant::now() + SHUTDOWN_GRACE);
                    } else {
                        self.signal_all("KILL");
                        while self.tasks.join_next().await.is_some() {}
                        return 0;
                    }
                }
                _ = tokio::time::sleep_until(sleep_until), if deadline.is_some() => {
                    self.signal_all("KILL");
                    while self.tasks.join_next().await.is_some() {}
                    return 0;
                }
                joined = self.tasks.join_next() => {
                    match joined {
                        Some(Ok(name)) => {
                            if stopping {
                                if self.tasks.is_empty() {
                                    return 0;
                                }
                            } else if self.died_action == DiedAction::Shutdown {
                                eprintln!("{name} died. Shutting down...");
                                self.shutdown().await;
                                return 0;
                            } else {
                                eprintln!("{name} died.");
                                if self.tasks.is_empty() {
                                    return 0;
                                }
                            }
                        }
                        Some(Err(error)) => {
                            eprintln!("supervisor task failed: {error}");
                            self.shutdown().await;
                            return 0;
                        }
                        None => return 0,
                    }
                }
            }
        }
    }
}

async fn termination_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut interrupt = signal(SignalKind::interrupt()).expect("install SIGINT handler");
    let mut terminate = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let mut hangup = signal(SignalKind::hangup()).expect("install SIGHUP handler");
    tokio::select! {
        _ = interrupt.recv() => {}
        _ = terminate.recv() => {}
        _ = hangup.recv() => {}
    }
}

/// Wait up to `DB_STARTUP_TIMEOUT`'s default (10s) for the local Postgres to
/// accept connections, using `pg_isready` against `PGHOST`/`PGPORT`.
///
/// Mirrors the startup window the Go baseline gets from the migrator's retry
/// loop; see the comment in [`run_staged`].
fn wait_for_postgres_ready(env: &[(String, String)]) {
    let value = |key: &str| {
        env.iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
            .filter(|value| !value.is_empty())
    };
    let host = value("PGHOST").unwrap_or("127.0.0.1");
    let port = value("PGPORT").unwrap_or("5432");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let ready = std::process::Command::new("pg_isready")
            .args(["-h", host, "-p", port, "-q"])
            .status()
            .map(|status| status.success())
            .unwrap_or(false);
        if ready || std::time::Instant::now() >= deadline {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The release version stamped in `/version.txt`, or `None` for a dev image.
///
/// Go's `internal/version` reads the same file; `version.IsDev` is an exact
/// comparison against `0.0.0+dev`.
fn release_version_stamp() -> Option<String> {
    release_version_stamp_from(&std::fs::read_to_string("/version.txt").ok()?)
}

fn release_version_stamp_from(raw: &str) -> Option<String> {
    if raw.is_empty() || raw == "0.0.0+dev" {
        return None;
    }
    Some(raw.to_owned())
}

/// Re-stamp `versions.version` rows written by earlier images of this project.
///
/// Those images pinned `/version.txt` to `0.0.0+dev`, so both the Go migrator
/// and the frontend recorded that version. Go's upgrade policy
/// (`upgradestore.IsValidUpgrade`) rejects `0.0.0+dev` -> release as an
/// unsupported jump. The schema is identical (the same baseline migrations
/// ran), so rewrite the marker to the image's release version before the
/// migrator validates it. No-op for dev builds and on first boots (the table
/// does not exist yet, and psql's failure is ignored).
fn reconcile_prerelease_version(env: &[(String, String)]) {
    let Some(version) = release_version_stamp() else {
        return;
    };
    let value = |key: &str| {
        env.iter()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
            .filter(|value| !value.is_empty())
    };
    let host = value("PGHOST").unwrap_or("127.0.0.1");
    let port = value("PGPORT").unwrap_or("5432");
    let user = value("PGUSER").unwrap_or("postgres");
    let database = value("PGDATABASE").unwrap_or("sourcegraph");
    let quoted = version.replace('\'', "''");
    // `first_version` matters too: `oobmigration.ValidateOutOfBandMigrationRunner`
    // compares it against the deprecation points of unfinished out-of-band
    // migrations. A dev `first_version` would require every pre-5.5 migration
    // to be at 100%, which a fresh instance never had to do.
    let sql = format!(
        "UPDATE versions SET version = '{quoted}', first_version = '{quoted}', \
         updated_at = now() \
         WHERE version = '0.0.0+dev' OR first_version = '0.0.0+dev'"
    );
    let mut command = std::process::Command::new("psql");
    command.args([
        "-h", host, "-p", port, "-U", user, "-d", database, "-Atc", &sql,
    ]);
    if let Some(password) = value("PGPASSWORD") {
        command.env("PGPASSWORD", password);
    }
    // Best effort: missing database/table on a fresh volume is expected.
    let _ = command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

/// Run the two `shared.run` process groups exactly like the baseline's
/// `errgroup`: start Postgres, run migrations when enabled, then start the
/// application group and supervise both.
pub async fn run_staged<M: crate::migrator::Migrator>(
    postgres: &[ProcfileEntry],
    application: &[ProcfileEntry],
    schemas: Option<&[String]>,
    migrator: &mut M,
    died_action: DiedAction,
    env: &crate::env::EnvMap,
) -> i32 {
    let env: Vec<(String, String)> = env
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let mut supervisor = Supervisor::new(died_action, env.clone());
    if !postgres.is_empty() {
        println!("Starting postgres processes");
    }
    supervisor.start(postgres);

    // The Go baseline relies on the migrator's own startup retry
    // (`openDBWithStartupWait`, `DB_STARTUP_TIMEOUT` = 10s) after starting
    // Postgres (`shared.go:250-258`). PostgreSQL 15+ reports "the database
    // system is not yet accepting connections (57P03)" during crash recovery,
    // which that retry does not match, so give the local database the same
    // window before invoking the migrator. On timeout the migrator reports the
    // real error, as in the baseline.
    if !postgres.is_empty() && schemas.is_some() {
        wait_for_postgres_ready(&env);
        reconcile_prerelease_version(&env);
    }

    if let Some(schemas) = schemas {
        println!("Starting migrator");
        for schema in schemas {
            if let Err(error) = migrator.up(schema, &env) {
                eprintln!("failed to run migrator for {schema}: {error}");
                supervisor.shutdown().await;
                return 1;
            }
        }
        println!("Migrated postgres schemas.");
    }

    if !application.is_empty() {
        println!("Starting all processes");
    }
    supervisor.start(application);
    supervisor.wait().await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn parses_procfile_like_goreman() {
        let entries = parse_procfile(&lines(&[
            "# comment",
            "",
            "no-colon",
            "empty:",
            ": nope",
            "frontend: env X=1 frontend",
            "  worker : worker  ",
        ]));
        assert_eq!(
            entries,
            vec![
                ProcfileEntry {
                    name: "frontend".to_owned(),
                    command: "env X=1 frontend".to_owned()
                },
                ProcfileEntry {
                    name: "worker".to_owned(),
                    command: "worker".to_owned()
                },
            ]
        );
    }

    #[test]
    fn child_death_shuts_down_unless_ignored() {
        let entries = parse_procfile(&lines(&["a: a", "b: b"]));
        let outcome = simulate(
            &entries,
            &[SimEvent::ChildDied("a".to_owned())],
            DiedAction::Shutdown,
        );
        assert_eq!(outcome.sigint_targets, vec!["a", "b"]);
        assert_eq!(
            outcome.actions,
            vec!["a died. Shutting down...".to_owned()]
        );

        let outcome = simulate(
            &entries,
            &[SimEvent::ChildDied("a".to_owned())],
            DiedAction::Ignore,
        );
        assert!(outcome.sigint_targets.is_empty());
        assert_eq!(outcome.actions, vec!["a died.".to_owned()]);
        assert_eq!(outcome.exit_code, 0);
    }

    #[test]
    fn release_version_stamp_skips_dev() {
        assert_eq!(release_version_stamp_from("0.0.0+dev"), None);
        assert_eq!(release_version_stamp_from(""), None);
        assert_eq!(
            release_version_stamp_from("5.5.0").as_deref(),
            Some("5.5.0")
        );
    }

    #[test]
    fn second_signal_escalates() {
        let entries = parse_procfile(&lines(&["a: a", "b: b"]));
        let outcome = simulate(
            &entries,
            &[SimEvent::Signal, SimEvent::Signal],
            DiedAction::Shutdown,
        );
        assert_eq!(outcome.sigint_targets, vec!["a", "b"]);
        assert_eq!(outcome.sigkill_targets, vec!["a", "b"]);
        assert_eq!(outcome.exit_code, 0);
    }
}
