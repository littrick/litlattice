#![forbid(unsafe_code)]

//! Prometheus/Alertmanager subprocesses (`cmd.go`).
//!
//! The Go baseline starts both children with [`streamexec`] and forwards their
//! combined output line-by-line to the wrapper's logger. We do not need the
//! line-splitting behavior: the wrapper's own stdout/stderr are captured by the
//! container runtime anyway, so the children simply inherit them. That keeps
//! the child output attributable to the right process without an extra relay
//! task per child.
//!
//! [`streamexec`]: https://pkg.go.dev/go.bobheadxi.dev/streamline/streamexec

use std::io;
use std::path::Path;
use std::process::{ExitStatus, Stdio};

use tokio::process::{Child, Command};

/// `NewPrometheusCmd`: run `/prometheus.sh` with wrapper args propagated.
///
/// The listen-address flag always comes first, followed verbatim by the
/// wrapper's own arguments (Go's `os.Args[1:]`), which Prometheus consumes.
pub fn prometheus_command(port: &str, args: &[String]) -> Command {
    let mut command = Command::new("/prometheus.sh");
    command
        .arg(format!("--web.listen-address=0.0.0.0:{port}"))
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    command
}

/// `NewAlertmanagerCmd`.
///
/// Matching Go, no listen-address flag is passed: Alertmanager listens on its
/// own default (`9093`). The reverse proxy dials the configured
/// `ALERTMANAGER_INTERNAL_PORT`, so that variable must remain at Alertmanager's
/// default (or the child must be configured separately) for the proxy to reach
/// it.
pub fn alertmanager_command(config_path: &Path, enable_cluster: bool) -> Command {
    let mut command = Command::new("/alertmanager.sh");
    command
        .arg(format!("--config.file={}", config_path.display()))
        .arg("--web.route-prefix=/alertmanager")
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    // Clustering is enabled by default in Alertmanager but can fail to start in
    // some environments; disable it unless explicitly requested.
    // See https://github.com/sourcegraph/sourcegraph/issues/13079.
    if !enable_cluster {
        command.arg("--cluster.listen-address=");
    }

    command
}

/// A spawned Prometheus/Alertmanager child.
///
/// Dropping the handle does *not* kill the child, matching Go's `exec.Cmd`
/// (the wrapper exits the container on shutdown rather than reaping children).
pub struct ChildProcess {
    name: String,
    pid: Option<u32>,
    child: Child,
}

impl ChildProcess {
    /// The child's process id, available after a successful [`spawn`].
    pub fn pid(&self) -> Option<u32> {
        self.pid
    }

    /// Human-readable label used in logs (`"prometheus"`, `"alertmanager"`).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Await the child's exit, returning its status.
    pub async fn wait(&mut self) -> io::Result<ExitStatus> {
        self.child.wait().await
    }

    /// Await the child's exit and return its exit code.
    ///
    /// Returns `None` when the child was terminated by a signal, mirroring
    /// Go's `ProcessState.ExitCode()` (which reports `-1` in that case; callers
    /// can decide how to map it).
    pub async fn exit_code(&mut self) -> io::Result<Option<i32>> {
        Ok(self.wait().await?.code())
    }

    /// Request immediate termination (`SIGKILL`), like `Cmd.Process.Kill`.
    pub fn kill(&mut self) -> io::Result<()> {
        self.child.start_kill()
    }
}

/// `runCmd`: start a prepared command and return a handle for it.
///
/// The command is logged before spawning exactly like Go's `runCmd`
/// (`commandLog.Info("running cmd")`); stdout/stderr were configured to be
/// inherited by the command builders above.
pub async fn spawn(mut command: Command, name: impl Into<String>) -> io::Result<ChildProcess> {
    let name = name.into();
    let program = command
        .as_std()
        .get_program()
        .to_string_lossy()
        .into_owned();
    let args: Vec<String> = command
        .as_std()
        .get_args()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    tracing::info!(command = %program, args = ?args, "running cmd");

    let child = command.spawn()?;
    let pid = child.id();
    tracing::info!(%name, pid = pid.unwrap_or_default(), "started command");
    Ok(ChildProcess { name, pid, child })
}

/// Await a child's exit, log it, and return its exit code.
///
/// This is the `runCmd` equivalent the service can spawn on a task; the
/// returned code matches Go's mapping (`ExitCode()` or `1` for non-`ExitError`
/// failures). A child killed by a signal yields `-1`.
pub async fn supervise(mut process: ChildProcess) -> io::Result<i32> {
    let name = process.name.clone();
    let status = process.wait().await?;
    let code = status.code().unwrap_or(-1);
    tracing::info!(%name, ?status, code, "command exited");
    Ok(code)
}

#[cfg(test)]
mod tests {
    use super::{alertmanager_command, prometheus_command};
    use std::ffi::OsStr;
    use std::path::Path;

    fn args_of(command: &tokio::process::Command) -> Vec<String> {
        command
            .as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn prometheus_command_prepends_listen_address() {
        let extra = vec!["--config.file=/etc/prometheus.yml".to_owned()];
        let command = prometheus_command("9092", &extra);

        assert_eq!(command.as_std().get_program(), OsStr::new("/prometheus.sh"));
        assert_eq!(
            args_of(&command),
            vec![
                "--web.listen-address=0.0.0.0:9092".to_owned(),
                "--config.file=/etc/prometheus.yml".to_owned(),
            ]
        );
    }

    #[test]
    fn prometheus_command_forwards_arguments_verbatim() {
        let extra: Vec<String> = vec![];
        let command = prometheus_command("1234", &extra);
        assert_eq!(
            args_of(&command),
            vec!["--web.listen-address=0.0.0.0:1234".to_owned()]
        );
    }

    #[test]
    fn alertmanager_command_disables_clustering_by_default() {
        let command = alertmanager_command(Path::new("/cfg/alertmanager.yml"), false);

        assert_eq!(
            command.as_std().get_program(),
            OsStr::new("/alertmanager.sh")
        );
        assert_eq!(
            args_of(&command),
            vec![
                "--config.file=/cfg/alertmanager.yml".to_owned(),
                "--web.route-prefix=/alertmanager".to_owned(),
                "--cluster.listen-address=".to_owned(),
            ]
        );
    }

    #[test]
    fn alertmanager_command_keeps_clustering_when_enabled() {
        let command = alertmanager_command(Path::new("/cfg/alertmanager.yml"), true);

        assert_eq!(
            args_of(&command),
            vec![
                "--config.file=/cfg/alertmanager.yml".to_owned(),
                "--web.route-prefix=/alertmanager".to_owned(),
            ]
        );
    }

    #[test]
    fn command_does_not_pass_an_alertmanager_listen_address() {
        // Go does not pass `--web.listen-address` to Alertmanager; the proxy
        // must rely on its default 9093.
        let command = alertmanager_command(Path::new("/cfg/alertmanager.yml"), false);
        assert!(!args_of(&command)
            .iter()
            .any(|arg| arg.starts_with("--web.listen-address")));
    }
}
