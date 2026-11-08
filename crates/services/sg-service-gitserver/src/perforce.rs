//! Port of `cmd/gitserver/internal/perforce` and the Perforce RPCs from
//! `cmd/gitserver/internal/server_grpc.go`.
//!
//! Every operation shells out to the `p4` CLI with a fresh scratch directory,
//! `HOME=<repos_dir>/.p4home`, `P4CLIENTPATH` set to the scratch directory and
//! `P4PORT`/`P4USER`/`P4PASSWD` taken from the request. The handlers only see
//! the error text, so the formatting of `exit status N`, the
//! `(output follows)` suffix and `specifyCommandInErrorMessage` are part of the
//! contract.
//!
//! Delegated deviations (documented in `docs/modules/02_gitserver.md`):
//! - JSON decode failures come from `serde_json`, not Go's `encoding/json`.
//! - A command that hits its 10s deadline reports the timeout without the
//!   partial output (the baseline includes whatever `CombinedOutput` captured).
//! - stdout/stderr are concatenated instead of interleaved.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use crate::fs;
use crate::vcssyncer;

/// The per-command timeout the baseline applies with `context.WithTimeout`
/// (`P4Test`, `P4Trust`, `P4UserIsSuperUser`, `P4Depots`). The other commands
/// inherit the RPC context and have no deadline of their own.
const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);

/// `PerforceConnectionDetails`.
#[derive(Debug, Clone, Default)]
pub struct ConnectionDetails {
    pub p4port: String,
    pub p4user: String,
    pub p4passwd: String,
}

/// `internal/perforce.User`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PerforceUser {
    pub username: String,
    pub email: String,
}

/// `internal/perforce.Protect`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PerforceProtect {
    pub level: String,
    pub entity_type: String,
    pub entity_name: String,
    pub match_: String,
    pub is_exclusion: bool,
    pub host: String,
}

/// `internal/perforce.ChangelistState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangelistState {
    Submitted,
    Pending,
    Shelved,
    Closed,
}

impl ChangelistState {
    /// `ChangelistState.ToProto`.
    pub fn as_proto(self) -> i32 {
        use sg_proto::gitserver::perforce_changelist::PerforceChangelistState;
        let state = match self {
            Self::Submitted => PerforceChangelistState::Submitted,
            Self::Pending => PerforceChangelistState::Pending,
            Self::Shelved => PerforceChangelistState::Shelved,
            Self::Closed => PerforceChangelistState::Closed,
        };
        state as i32
    }
}

/// `internal/perforce.Changelist`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Changelist {
    pub id: String,
    pub creation_date_seconds: i64,
    pub state: ChangelistState,
    pub author: String,
    pub title: String,
    pub message: String,
}

/// A failure of one of the Perforce operations. The text is already formatted
/// like the baseline's error chain.
#[derive(Debug)]
pub enum PerforceError {
    /// An ordinary error; the handler maps it to the RPC's baseline code.
    Message(String),
    /// The command reached its 10s deadline (`context.DeadlineExceeded` made
    /// observable for `P4Test`'s single retry).
    Deadline(String),
    /// `ErrIsNotSuperUser`, compared by identity in the baseline handler.
    NotSuperUser,
}

impl PerforceError {
    /// The text a handler reports (`err.Error()`).
    pub fn message(&self) -> &str {
        match self {
            Self::Message(message) | Self::Deadline(message) => message,
            Self::NotSuperUser => "the user does not have super access",
        }
    }

    fn is_deadline(&self) -> bool {
        matches!(self, Self::Deadline(_))
    }

    pub fn is_not_super_user(&self) -> bool {
        matches!(self, Self::NotSuperUser)
    }
}

/// The outcome of a `p4` invocation that did not succeed.
enum ExecError {
    /// The process ran and exited with a non-zero status; `output` is the
    /// combined stdout+stderr.
    Exited {
        output: Vec<u8>,
        status: std::process::ExitStatus,
    },
    /// The 10s deadline expired; the partial output is not retained.
    TimedOut,
    /// `exec.Command`/`Start` failed.
    Spawn(std::io::Error),
}

/// `perforce.NewBaseCommand` + `CombinedOutput`.
pub struct PerforceService {
    repos_dir: Arc<PathBuf>,
}

impl PerforceService {
    pub fn new(repos_dir: PathBuf) -> Self {
        Self {
            repos_dir: Arc::new(repos_dir),
        }
    }

    /// `fs.P4HomeDir`: `<repos_dir>/.p4home`, created on demand.
    fn p4_home_dir(&self) -> Result<PathBuf, String> {
        let p4_home = self.repos_dir.join(fs::P4_HOME_NAME);
        std::fs::create_dir_all(&p4_home).map_err(|err| {
            format!(
                "ensuring p4Home exists: {:?}: {err}",
                p4_home.display().to_string()
            )
        })?;
        Ok(p4_home)
    }

    /// `fs.P4HomeDir` + `fs.TempDir` with the baseline's per-call wrappers.
    fn scratch(&self, prefix: &str, command: &str) -> Result<(PathBuf, PathBuf), PerforceError> {
        let p4_home = self
            .p4_home_dir()
            .map_err(|err| PerforceError::Message(format!("failed to create p4home dir: {err}")))?;
        let dir = fs::temp_dir(&self.repos_dir, prefix).map_err(|err| {
            PerforceError::Message(format!(
                "could not create temp dir to invoke '{command}': {err}"
            ))
        })?;
        Ok((p4_home, dir))
    }

    /// `IsDepotPathCloneable`.
    pub async fn is_depot_path_cloneable(
        &self,
        conn: &ConnectionDetails,
        depot_path: &str,
    ) -> Result<(), PerforceError> {
        self.test_with_trust(conn).await.map_err(|err| {
            PerforceError::Message(go_wrap("checking perforce credentials", err.message()))
        })?;

        let depot = depot_path
            .trim_start_matches('/')
            .split('/')
            .next()
            .unwrap_or_default();

        let depots = self.depots(conn, depot).await?;
        if depots.is_empty() {
            if !depot.is_empty() {
                return Err(PerforceError::Message(format!(
                    "the user {} does not have access to the depot {} on the server {}",
                    conn.p4user, depot, conn.p4port
                )));
            }
            return Err(PerforceError::Message(format!(
                "the user {} does not have access to any depots on the server {}",
                conn.p4user, conn.p4port
            )));
        }
        Ok(())
    }

    /// `P4TestWithTrust`.
    pub async fn test_with_trust(&self, conn: &ConnectionDetails) -> Result<(), PerforceError> {
        let err = match self.test(conn).await {
            Ok(()) => return Ok(()),
            Err(err) => err,
        };
        if err
            .message()
            .contains("To allow connection use the 'p4 trust' command.")
        {
            self.trust(conn)
                .await
                .map_err(|err| PerforceError::Message(go_wrap("trust", err.message())))?;
            return self.test(conn).await.map_err(|err| {
                PerforceError::Message(go_wrap("testing connection after trust", err.message()))
            });
        }
        Err(err)
    }

    /// `P4Test`: `p4 login -s`, retried once on deadline exceeded.
    async fn test(&self, conn: &ConnectionDetails) -> Result<(), PerforceError> {
        let err = match self.test_once(conn).await {
            Ok(()) => return Ok(()),
            Err(err) => err,
        };
        if err.is_deadline() {
            return self.test_once(conn).await;
        }
        Err(err)
    }

    async fn test_once(&self, conn: &ConnectionDetails) -> Result<(), PerforceError> {
        let (p4_home, dir) = self.scratch("p4-login-", "p4 login")?;
        let result = self
            .exec(
                conn,
                &["login", "-s"],
                None,
                &p4_home,
                &dir,
                true,
                Some(COMMAND_TIMEOUT),
            )
            .await;
        let _ = std::fs::remove_dir_all(&dir);
        match result {
            Ok(_) => Ok(()),
            // `errors.Wrap(ctxerr, "p4 login context error")` + the output
            // suffix, which also runs `specifyCommandInErrorMessage`.
            Err(ExecError::TimedOut) => Err(PerforceError::Deadline(output_suffix(
                "p4 login context error: context deadline exceeded",
                &[],
                Some(&["login", "-s"]),
            ))),
            Err(ExecError::Exited { output, status }) => Err(PerforceError::Message(
                output_suffix(&exit_status(&status), &output, Some(&["login", "-s"])),
            )),
            Err(ExecError::Spawn(err)) => Err(PerforceError::Message(spawn_error(&err))),
        }
    }

    /// `P4Trust`: `p4 trust -y -f`.
    async fn trust(&self, conn: &ConnectionDetails) -> Result<(), PerforceError> {
        let (p4_home, dir) = self.scratch("p4-trust-", "p4 trust")?;
        let result = self
            .exec(
                conn,
                &["trust", "-y", "-f"],
                None,
                &p4_home,
                &dir,
                false,
                Some(COMMAND_TIMEOUT),
            )
            .await;
        let _ = std::fs::remove_dir_all(&dir);
        match result {
            Ok(_) => Ok(()),
            Err(ExecError::TimedOut) => Err(PerforceError::Deadline(output_suffix(
                "context deadline exceeded",
                &[],
                None,
            ))),
            Err(ExecError::Exited { output, status }) => Err(PerforceError::Message(
                output_suffix(&exit_status(&status), &output, None),
            )),
            Err(ExecError::Spawn(err)) => Err(PerforceError::Message(spawn_error(&err))),
        }
    }

    /// `P4UserIsSuperUser`: `p4 protects -u <user>`.
    pub async fn user_is_super_user(&self, conn: &ConnectionDetails) -> Result<(), PerforceError> {
        let (p4_home, dir) = self.scratch("p4-protects-", "p4 protects")?;
        let result = self
            .exec(
                conn,
                &["protects", "-u", &conn.p4user],
                None,
                &p4_home,
                &dir,
                true,
                Some(COMMAND_TIMEOUT),
            )
            .await;
        let _ = std::fs::remove_dir_all(&dir);
        match result {
            Ok(_) => Ok(()),
            Err(ExecError::TimedOut) => Err(PerforceError::Deadline(
                "context deadline exceeded".to_owned(),
            )),
            Err(ExecError::Exited { output, status }) => {
                let message = exit_status(&status);
                // The baseline checks `err.Error()`, which is `exit status N`
                // for a non-zero exit, so the permission text never matches
                // here; keep the same behavior.
                if message.contains("You don't have permission for this operation.") {
                    return Err(PerforceError::NotSuperUser);
                }
                Err(PerforceError::Message(output_suffix(
                    &message, &output, None,
                )))
            }
            Err(ExecError::Spawn(err)) => Err(PerforceError::Message(spawn_error(&err))),
        }
    }

    /// `P4Depots`: `p4 -Mj -ztag depots [-e <filter>]`.
    async fn depots(
        &self,
        conn: &ConnectionDetails,
        name_filter: &str,
    ) -> Result<Vec<PerforceDepot>, PerforceError> {
        let (p4_home, dir) = self.scratch("p4-depots-", "p4 depots")?;
        let mut args = vec!["-Mj", "-ztag", "depots"];
        if !name_filter.is_empty() {
            args.extend(["-e", name_filter]);
        }
        let result = self
            .exec(
                conn,
                &args,
                None,
                &p4_home,
                &dir,
                true,
                Some(COMMAND_TIMEOUT),
            )
            .await;
        let _ = std::fs::remove_dir_all(&dir);
        let output = match result {
            Ok(output) => output,
            Err(ExecError::TimedOut) => {
                return Err(PerforceError::Message(output_suffix(
                    "p4 depots context error: context deadline exceeded",
                    &[],
                    Some(&args),
                )))
            }
            Err(ExecError::Exited { output, status }) => {
                // `errors.Wrapf` puts the wrap message before the cause.
                return Err(PerforceError::Message(output_wrap(
                    "failed to run command \"p4 depots\"",
                    &output,
                    Some(&args),
                    &exit_status(&status),
                )));
            }
            Err(ExecError::Spawn(err)) => return Err(PerforceError::Message(spawn_error(&err))),
        };

        let mut depots = Vec::new();
        for line in lines(&output) {
            let depot: PerforceDepot = serde_json::from_slice(line).map_err(|err| {
                PerforceError::Message(format!("malformed output from p4 depots: {err}"))
            })?;
            depots.push(depot);
        }
        Ok(depots)
    }

    /// `P4Users`: `p4 -Mj -ztag users`.
    pub async fn users(
        &self,
        conn: &ConnectionDetails,
    ) -> Result<Vec<PerforceUser>, PerforceError> {
        let (p4_home, dir) = self.scratch("p4-users-", "p4 users")?;
        let args = ["-Mj", "-ztag", "users"];
        let result = self
            .exec(conn, &args, None, &p4_home, &dir, true, None)
            .await;
        let _ = std::fs::remove_dir_all(&dir);
        let output = match result {
            Ok(output) => output,
            Err(ExecError::TimedOut) => {
                return Err(PerforceError::Message(output_suffix(
                    "p4 users context error: context deadline exceeded",
                    &[],
                    Some(&args),
                )))
            }
            Err(ExecError::Exited { output, status }) => {
                return Err(PerforceError::Message(output_wrap(
                    "failed to run command \"p4 users\"",
                    &output,
                    Some(&args),
                    &exit_status(&status),
                )));
            }
            Err(ExecError::Spawn(err)) => return Err(PerforceError::Message(spawn_error(&err))),
        };

        let mut users = Vec::new();
        for line in lines(&output) {
            let user: PerforceJsonUser = serde_json::from_slice(line).map_err(|err| {
                PerforceError::Message(format!("malformed output from p4 users: {err}"))
            })?;
            users.push(PerforceUser {
                username: user.user,
                email: user.email,
            });
        }
        Ok(users)
    }

    /// `P4ProtectsForUser`: `p4 -Mj -ztag protects -u <username>`.
    pub async fn protects_for_user(
        &self,
        conn: &ConnectionDetails,
        username: &str,
    ) -> Result<Vec<PerforceProtect>, PerforceError> {
        let (p4_home, dir) = self.scratch("p4-protects-", "p4 protects")?;
        let args = ["-Mj", "-ztag", "protects", "-u", username];
        let result = self
            .exec(conn, &args, None, &p4_home, &dir, true, None)
            .await;
        let _ = std::fs::remove_dir_all(&dir);
        let output = match result {
            Ok(output) => output,
            Err(ExecError::TimedOut) => {
                return Err(PerforceError::Message(output_suffix(
                    "p4 protects context error: context deadline exceeded",
                    &[],
                    Some(&args),
                )))
            }
            Err(ExecError::Exited { output, status }) => {
                return Err(PerforceError::Message(output_wrap(
                    "failed to run command \"p4 protects\"",
                    &output,
                    Some(&args),
                    &exit_status(&status),
                )));
            }
            Err(ExecError::Spawn(err)) => return Err(PerforceError::Message(spawn_error(&err))),
        };

        if output.is_empty() {
            return Ok(Vec::new());
        }
        if std::env::var_os("SRC_GITSERVER_P4_BROKER_ENABLED").is_some() {
            parse_broker_protects(&output)
        } else {
            parse_protects(&output)
        }
    }

    /// `P4ProtectsForDepot`: `p4 -Mj -ztag protects -a <depot>`.
    pub async fn protects_for_depot(
        &self,
        conn: &ConnectionDetails,
        depot: &str,
    ) -> Result<Vec<PerforceProtect>, PerforceError> {
        let (p4_home, dir) = self.scratch("p4-protects-", "p4 protects")?;
        let args = ["-Mj", "-ztag", "protects", "-a", depot];
        let result = self
            .exec(conn, &args, None, &p4_home, &dir, true, None)
            .await;
        let _ = std::fs::remove_dir_all(&dir);
        let output = match result {
            Ok(output) => output,
            Err(ExecError::TimedOut) => {
                return Err(PerforceError::Message(output_suffix(
                    "p4 protects context error: context deadline exceeded",
                    &[],
                    Some(&args),
                )))
            }
            Err(ExecError::Exited { output, status }) => {
                return Err(PerforceError::Message(output_wrap(
                    "failed to run command \"p4 protects\"",
                    &output,
                    Some(&args),
                    &exit_status(&status),
                )));
            }
            Err(ExecError::Spawn(err)) => return Err(PerforceError::Message(spawn_error(&err))),
        };

        if output.is_empty() {
            return Ok(Vec::new());
        }
        parse_protects(&output)
    }

    /// `P4GroupMembers`: `p4 -Mj -ztag group -o <group>`.
    pub async fn group_members(
        &self,
        conn: &ConnectionDetails,
        group: &str,
    ) -> Result<Vec<String>, PerforceError> {
        let (p4_home, dir) = self.scratch("p4-group-", "p4 group")?;
        let args = ["-Mj", "-ztag", "group", "-o", group];
        let result = self
            .exec(conn, &args, None, &p4_home, &dir, true, None)
            .await;
        let _ = std::fs::remove_dir_all(&dir);
        let output = match result {
            Ok(output) => output,
            Err(ExecError::TimedOut) => {
                return Err(PerforceError::Message(output_suffix(
                    "p4 group context error: context deadline exceeded",
                    &[],
                    Some(&args),
                )))
            }
            Err(ExecError::Exited { output, status }) => {
                return Err(PerforceError::Message(output_wrap(
                    "failed to run command \"p4 group\"",
                    &output,
                    Some(&args),
                    &exit_status(&status),
                )));
            }
            Err(ExecError::Spawn(err)) => return Err(PerforceError::Message(spawn_error(&err))),
        };

        if output.is_empty() {
            return Ok(Vec::new());
        }
        parse_group_members(&output)
    }

    /// `GetChangelistByID`: `p4 -Mj -z tag changes -r -m 1 -l -e <id>`.
    pub async fn changelist_by_id(
        &self,
        conn: &ConnectionDetails,
        changelist_id: &str,
    ) -> Result<Changelist, PerforceError> {
        let (p4_home, dir) = self.scratch("p4-changelist-", "p4 changes")?;
        let args = [
            "-Mj",
            "-z",
            "tag",
            "changes",
            "-r",
            "-m",
            "1",
            "-l",
            "-e",
            changelist_id,
        ];
        let result = self
            .exec(conn, &args, None, &p4_home, &dir, true, None)
            .await;
        let _ = std::fs::remove_dir_all(&dir);
        let output = match result {
            Ok(output) => output,
            Err(ExecError::TimedOut) => {
                return Err(PerforceError::Message(output_suffix(
                    "p4 changes context error: context deadline exceeded",
                    &[],
                    Some(&args),
                )))
            }
            Err(ExecError::Exited { output, status }) => {
                return Err(PerforceError::Message(output_wrap(
                    "failed to run command \"p4 changes\"",
                    &output,
                    Some(&args),
                    &exit_status(&status),
                )));
            }
            Err(ExecError::Spawn(err)) => return Err(PerforceError::Message(spawn_error(&err))),
        };

        let output = trim_space(&output);
        if output.is_empty() {
            return Err(PerforceError::Message(format!(
                "invalid changelist {changelist_id}"
            )));
        }
        parse_changelist_output(output).map_err(|err| {
            PerforceError::Message(go_wrap("unable to parse change output", err.message()))
        })
    }

    /// `perforce.NewBaseCommand` + `CombinedOutput`.
    async fn exec(
        &self,
        conn: &ConnectionDetails,
        args: &[&str],
        client: Option<&str>,
        p4_home: &Path,
        dir: &Path,
        auth: bool,
        timeout: Option<Duration>,
    ) -> Result<Vec<u8>, ExecError> {
        let mut command = tokio::process::Command::new("p4");
        command
            .args(args)
            .current_dir(dir)
            .env("P4PORT", &conn.p4port)
            .env("P4CLIENTPATH", dir)
            .env("HOME", p4_home)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            // `exec.CommandContext` kills the process when the context ends.
            .kill_on_drop(true);
        // `WithAuthentication`: the baseline appends P4USER/P4PASSWD only for
        // commands that opt in (`p4 trust` inherits them instead).
        if auth {
            command.env("P4USER", &conn.p4user);
            command.env("P4PASSWD", &conn.p4passwd);
        }
        if let Some(client) = client {
            command.env("P4CLIENT", client);
        }

        let output = match timeout {
            Some(timeout) => match tokio::time::timeout(timeout, command.output()).await {
                Ok(Ok(output)) => output,
                Ok(Err(err)) => return Err(ExecError::Spawn(err)),
                Err(_) => return Err(ExecError::TimedOut),
            },
            None => match command.output().await {
                Ok(output) => output,
                Err(err) => return Err(ExecError::Spawn(err)),
            },
        };

        let mut combined = output.stdout;
        combined.extend_from_slice(&output.stderr);
        if output.status.success() {
            Ok(combined)
        } else {
            Err(ExecError::Exited {
                output: combined,
                status: output.status,
            })
        }
    }
}

/// `perforceDepot` (`p4 -Mj -ztag depots`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
struct PerforceDepot {
    #[serde(default)]
    desc: String,
    #[serde(default)]
    map: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    time: String,
    #[serde(default, rename = "type")]
    type_: String,
}

/// `perforceSJONUser` (`p4 -Mj -ztag users`).
#[derive(Debug, Default, serde::Deserialize)]
struct PerforceJsonUser {
    #[serde(default, rename = "Email")]
    email: String,
    #[serde(default, rename = "User")]
    user: String,
}

/// `perforceJSONProtect` (`p4 -Mj -ztag protects`).
#[derive(Debug, Default, serde::Deserialize)]
struct PerforceJsonProtect {
    #[serde(default, rename = "depotFile")]
    depot_file: String,
    #[serde(default)]
    host: String,
    #[serde(default)]
    perm: String,
    #[serde(default, rename = "isgroup")]
    is_group: Option<String>,
    #[serde(default)]
    unmap: Option<String>,
    #[serde(default)]
    user: String,
}

/// `parseP4Protects`.
fn parse_protects(output: &[u8]) -> Result<Vec<PerforceProtect>, PerforceError> {
    let mut protects = Vec::new();
    for line in lines(output) {
        let line = trim_space(line);
        let parsed: PerforceJsonProtect = serde_json::from_slice(line).map_err(|err| {
            PerforceError::Message(format!("failed to unmarshal protect line: {err}"))
        })?;
        if parsed.depot_file.is_empty() {
            return Err(PerforceError::Message(
                "not a valid protects response".to_owned(),
            ));
        }
        let entity_type = if parsed.is_group.is_some() {
            "group"
        } else {
            "user"
        };
        protects.push(PerforceProtect {
            host: parsed.host,
            entity_type: entity_type.to_owned(),
            entity_name: parsed.user,
            match_: parsed.depot_file,
            is_exclusion: parsed.unmap.is_some(),
            level: parsed.perm,
        });
    }
    Ok(protects)
}

/// `parseP4BrokerProtects`: the broker wraps the real `p4 protects` JSON in
/// `{"data": "<base64>"}`.
fn parse_broker_protects(output: &[u8]) -> Result<Vec<PerforceProtect>, PerforceError> {
    use base64::Engine as _;

    #[derive(serde::Deserialize)]
    struct BrokerResponse {
        #[serde(default)]
        data: String,
    }
    let broker: BrokerResponse = serde_json::from_slice(output).map_err(|err| {
        PerforceError::Message(format!("failed to unmarshal protect line: {err}"))
    })?;
    if broker.data.is_empty() {
        return Err(PerforceError::Message(
            "not a valid protects response".to_owned(),
        ));
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(broker.data.as_bytes())
        .map_err(|err| {
            PerforceError::Message(format!("failed to unmarshal protect line: {err}"))
        })?;
    parse_protects(&decoded)
}

/// `parseP4GroupMembers`: `Users0`, `Users1`, ... until the first missing key.
fn parse_group_members(output: &[u8]) -> Result<Vec<String>, PerforceError> {
    let group: serde_json::Value = serde_json::from_slice(output)
        .map_err(|err| PerforceError::Message(format!("malformed output from p4 group: {err}")))?;
    let object = group
        .as_object()
        .ok_or_else(|| PerforceError::Message("malformed output from p4 group".to_owned()))?;
    let mut users = Vec::new();
    let mut index = 0;
    while let Some(user) = object.get(&format!("Users{index}")) {
        index += 1;
        if let Some(username) = user.as_str() {
            users.push(username.to_owned());
        }
    }
    Ok(users)
}

/// `changelistJson` (`p4 -Mj -z tag changes`).
#[derive(Debug, Default, serde::Deserialize)]
struct ChangelistJson {
    #[serde(default)]
    change: String,
    #[serde(default)]
    client: String,
    #[serde(default)]
    desc: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    time: String,
    #[serde(default)]
    user: String,
}

/// `parseChangelistOutput`.
fn parse_changelist_output(output: &[u8]) -> Result<Changelist, PerforceError> {
    let json: ChangelistJson = serde_json::from_slice(output).map_err(|err| {
        PerforceError::Message(format!("unable to unmarshal change output: {err}"))
    })?;
    let state = parse_changelist_state(&json.status).map_err(|err| {
        PerforceError::Message(format!(
            "unable to parse changelist state: {}",
            err.message()
        ))
    })?;
    let seconds: i64 = json.time.parse().map_err(|_| {
        PerforceError::Message(format!(
            "invalid time: {}: strconv.Atoi: parsing {:?}: invalid syntax",
            json.time, json.time
        ))
    })?;
    Ok(Changelist {
        id: json.change,
        creation_date_seconds: seconds,
        state,
        author: json.user,
        title: json.client,
        message: String::from_utf8_lossy(trim_space(json.desc.as_bytes())).into_owned(),
    })
}

/// `parseChangelistState`.
fn parse_changelist_state(state: &str) -> Result<ChangelistState, PerforceError> {
    match state.trim().to_lowercase().as_str() {
        "submitted" => Ok(ChangelistState::Submitted),
        "pending" => Ok(ChangelistState::Pending),
        "shelved" => Ok(ChangelistState::Shelved),
        "closed" => Ok(ChangelistState::Closed),
        _ => Err(PerforceError::Message(format!(
            "invalid Perforce changelist state: {state}"
        ))),
    }
}

/// `specifyCommandInErrorMessage`: replace the first `this operation` in `p4`
/// output with the command line.
fn specify_command_message(message: &str, args: &[&str]) -> String {
    if !message.contains("this operation") {
        return message.to_owned();
    }
    let command = std::iter::once("p4")
        .chain(args.iter().copied())
        .collect::<Vec<_>>()
        .join(" ");
    message.replacen("this operation", &format!("`{command}`"), 1)
}

/// `errors.Errorf("%s (output follows)\n\n%s", err, out)`.
fn output_suffix(error: &str, output: &[u8], args: Option<&[&str]>) -> String {
    if output.is_empty() {
        return error.to_owned();
    }
    let mut output = String::from_utf8_lossy(output).into_owned();
    if let Some(args) = args {
        output = specify_command_message(&output, args);
    }
    format!("{error} (output follows)\n\n{output}")
}

/// `errors.Wrap(err, prefix)`: cockroachdb's formatter renders the chain with
/// `%v` on the cause, which collapses runs of `\n` and strips leading and
/// trailing newlines from the cause text. (Verified against the baseline:
/// `Wrap(New("a\n\nb\n"), "outer")` prints `outer: a\nb`.)
fn go_wrap(prefix: &str, cause: &str) -> String {
    let cause = cause.trim_matches('\n');
    let mut normalized = String::with_capacity(cause.len());
    let mut previous_newline = false;
    for character in cause.chars() {
        if character == '\n' {
            if previous_newline {
                continue;
            }
            previous_newline = true;
        } else {
            previous_newline = false;
        }
        normalized.push(character);
    }
    format!("{prefix}: {normalized}")
}

/// `errors.Wrapf` with a `failed to run command "p4 X" ...` message: the wrap
/// message precedes the cause.
fn output_wrap(wrapper: &str, output: &[u8], args: Option<&[&str]>, cause: &str) -> String {
    let mut output = String::from_utf8_lossy(output).into_owned();
    if let Some(args) = args {
        output = specify_command_message(&output, args);
    }
    // The baseline's wrapper format string is a raw Go string literal, so
    // these are two literal backslashes + `n`, not newlines.
    format!("{wrapper} (output follows)\\n\\n{output}: {cause}")
}

/// `vcssyncer::exit_status` (`exec.ExitError.Error()`).
fn exit_status(status: &std::process::ExitStatus) -> String {
    vcssyncer::exit_status(status)
}

/// Go `exec.Error` text for a failed spawn.
fn spawn_error(err: &std::io::Error) -> String {
    if err.kind() == std::io::ErrorKind::NotFound {
        return "exec: \"p4\": executable file not found in $PATH".to_owned();
    }
    format!("exec: \"p4\": {err}")
}

/// `bytes.TrimSpace`.
fn trim_space(bytes: &[u8]) -> &[u8] {
    let is_space = |byte: u8| matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c);
    let mut start = 0;
    let mut end = bytes.len();
    while start < end && is_space(bytes[start]) {
        start += 1;
    }
    while end > start && is_space(bytes[end - 1]) {
        end -= 1;
    }
    &bytes[start..end]
}

/// `byteutils.NewLineReader`: split on `\n`, no `\r` stripping.
fn lines(bytes: &[u8]) -> Vec<&[u8]> {
    let mut out = Vec::new();
    let mut start = 0;
    while start < bytes.len() {
        match bytes[start..].iter().position(|byte| *byte == b'\n') {
            Some(index) => {
                out.push(&bytes[start..start + index]);
                start += index + 1;
            }
            None => {
                out.push(&bytes[start..]);
                break;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_depots_users_and_protects() {
        let depots = lines(
            br#"{"name":"depot1","type":"local","time":"123"}
{"name":"depot2","type":"stream"}"#,
        );
        assert_eq!(depots.len(), 2);

        let users = lines(
            br#"{"User":"alice","Email":"a@example.com"}
{"User":"bob","Email":"b@example.com"}"#,
        );
        assert_eq!(users.len(), 2);

        let protects = parse_protects(
            br#"{"depotFile":"//depot/...","host":"*","perm":"read","user":"alice"}
{"depotFile":"//secret/...","perm":"write","user":"group1","isgroup":"1","unmap":"1"}"#,
        )
        .expect("protects");
        assert_eq!(
            protects[0],
            PerforceProtect {
                level: "read".to_owned(),
                entity_type: "user".to_owned(),
                entity_name: "alice".to_owned(),
                match_: "//depot/...".to_owned(),
                is_exclusion: false,
                host: "*".to_owned(),
            }
        );
        assert_eq!(protects[1].entity_type, "group");
        assert!(protects[1].is_exclusion);
    }

    #[test]
    fn parses_group_members_and_stops_at_the_first_gap() {
        let members = parse_group_members(
            br#"{"Group":"g","Users0":"alice","Users1":"bob","Users3":"carol"}"#,
        )
        .expect("group");
        assert_eq!(members, vec!["alice", "bob"]);
        // Non-string members are skipped but still advance the index.
        let members = parse_group_members(br#"{"Users0":42,"Users1":"bob"}"#).expect("group");
        assert_eq!(members, vec!["bob"]);
    }

    #[test]
    fn parses_broker_wrapped_protects() {
        use base64::Engine as _;
        let inner = br#"{"depotFile":"//depot/...","perm":"read","user":"alice"}"#;
        let wrapper = format!(
            r#"{{"data":"{}"}}"#,
            base64::engine::general_purpose::STANDARD.encode(inner)
        );
        let protects = parse_broker_protects(wrapper.as_bytes()).expect("broker protects");
        assert_eq!(protects.len(), 1);
        assert_eq!(protects[0].entity_name, "alice");
        // Missing data is the baseline's "not a valid protects response".
        assert_eq!(
            parse_broker_protects(br#"{"data":""}"#)
                .expect_err("empty")
                .message(),
            "not a valid protects response"
        );
    }

    #[test]
    fn parses_changelists_and_states() {
        let changelist = parse_changelist_output(
            br#"{"change":"1234","client":"alice-client","desc":"  hello\nworld  ","status":"Submitted","time":"1234567890","user":"alice"}"#,
        )
        .expect("changelist");
        assert_eq!(changelist.id, "1234");
        assert_eq!(changelist.state, ChangelistState::Submitted);
        assert_eq!(changelist.author, "alice");
        assert_eq!(changelist.title, "alice-client");
        assert_eq!(changelist.message, "hello\nworld");
        assert_eq!(changelist.creation_date_seconds, 1_234_567_890);

        assert_eq!(
            parse_changelist_state(" PENDING ").expect("pending"),
            ChangelistState::Pending
        );
        assert_eq!(
            parse_changelist_state("closed").expect("closed"),
            ChangelistState::Closed
        );
        assert!(parse_changelist_state("help").is_err());
    }

    #[test]
    fn formats_command_failures_like_the_baseline() {
        // The `this operation` replacement includes the `p4` argv[0].
        assert_eq!(
            specify_command_message(
                "You don't have permission for this operation.",
                &["login", "-s"]
            ),
            "You don't have permission for `p4 login -s`."
        );
        // Without the marker the output is unchanged.
        assert_eq!(
            specify_command_message("plain output", &["login", "-s"]),
            "plain output"
        );
        // `errors.Errorf` ordering: cause first.
        assert_eq!(
            output_suffix("exit status 1", b"boom\n", None),
            "exit status 1 (output follows)\n\nboom\n"
        );
        // `errors.Wrapf` ordering: wrapper first, cause last. The raw Go
        // format string contributes literal backslash-n characters.
        assert_eq!(
            output_wrap(
                "failed to run command \"p4 users\"",
                b"boom\n",
                None,
                "exit status 1"
            ),
            "failed to run command \"p4 users\" (output follows)\\n\\nboom\n: exit status 1"
        );
    }

    #[test]
    fn wraps_errors_like_cockroach() {
        // Values captured from `errors.Wrap(errors.New(s), "outer").Error()`.
        assert_eq!(go_wrap("outer", "a\n\nb\n"), "outer: a\nb");
        assert_eq!(go_wrap("outer", "a\n\n\nb"), "outer: a\nb");
        assert_eq!(go_wrap("outer", "a\nb"), "outer: a\nb");
        assert_eq!(go_wrap("outer", "a\n \nb"), "outer: a\n \nb");
        assert_eq!(go_wrap("outer", "a\r\n\r\nb"), "outer: a\r\n\r\nb");
        assert_eq!(go_wrap("outer", "\n\na"), "outer: a");
        assert_eq!(go_wrap("outer", "a\n\n"), "outer: a");
        assert_eq!(go_wrap("outer", "\n\n"), "outer: ");
        assert_eq!(
            go_wrap("trust", "exit status 1 (output follows)\n\nboom\n"),
            "trust: exit status 1 (output follows)\nboom"
        );
    }

    #[test]
    fn trims_space_like_go() {
        assert_eq!(trim_space(b" \t\r\nhello \n"), b"hello");
        assert_eq!(trim_space(b"   "), b"");
        assert_eq!(trim_space(b""), b"");
    }
}
