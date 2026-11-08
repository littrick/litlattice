#![forbid(unsafe_code)]

//! `internal/goroutine`: the background-routine framework.
//!
//! Two of the baseline's `BackgroundRoutine` implementations are ported: the
//! periodic routine (9.8b) and the HTTP server (`internal/httpserver`, 9.8j).
//! The DB-queue framework (`internal/workerutil`) follows with the queue job
//! groups.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub mod metrics_server;
pub mod periodic;
pub mod recorder;

use periodic::PeriodicRoutine;
use recorder::Recordable;

/// `goroutine.BackgroundRoutine`: something with a name that runs until the
/// process context is cancelled.
#[derive(Clone)]
pub enum BackgroundRoutine {
    Periodic(Arc<PeriodicRoutine>),
    Server(Arc<dyn ServerRoutine>),
}

/// `internal/httpserver`'s `BackgroundRoutine`: `Start` blocks while serving,
/// `Stop` is modelled by cancelling the shared context.
#[async_trait]
pub trait ServerRoutine: Send + Sync {
    /// `Name()`; the baseline's server names are constants.
    fn name(&self) -> &'static str;

    /// `Start()`; returns when `shutdown` is cancelled (the equivalent of
    /// `Stop` calling `http.Server.Shutdown`).
    async fn start(&self, shutdown: CancellationToken) -> Result<(), String>;
}

impl From<PeriodicRoutine> for BackgroundRoutine {
    fn from(routine: PeriodicRoutine) -> Self {
        Self::Periodic(Arc::new(routine))
    }
}

impl From<Arc<PeriodicRoutine>> for BackgroundRoutine {
    fn from(routine: Arc<PeriodicRoutine>) -> Self {
        Self::Periodic(routine)
    }
}

impl From<Arc<dyn ServerRoutine>> for BackgroundRoutine {
    fn from(server: Arc<dyn ServerRoutine>) -> Self {
        Self::Server(server)
    }
}

impl BackgroundRoutine {
    /// `BackgroundRoutine.Name`.
    pub fn name(&self) -> &str {
        match self {
            Self::Periodic(routine) => routine.name(),
            Self::Server(server) => server.name(),
        }
    }

    /// The period of a periodic routine; zero for servers.
    pub fn interval(&self) -> Duration {
        match self {
            Self::Periodic(routine) => routine.interval(),
            Self::Server(_) => Duration::ZERO,
        }
    }

    /// The description of a periodic routine; empty for servers (which the
    /// baseline's recorder never sees, because they are not `Recordable`).
    pub fn description(&self) -> &str {
        match self {
            Self::Periodic(routine) => routine.description(),
            Self::Server(_) => "",
        }
    }

    /// The first-run delay of a periodic routine; zero for servers.
    pub fn initial_delay(&self) -> Duration {
        match self {
            Self::Periodic(routine) => routine.initial_delay(),
            Self::Server(_) => Duration::ZERO,
        }
    }

    /// `SetJobName`: recorder metadata, only periodic routines implement it.
    pub fn set_job_name(&self, job_name: impl Into<String>) {
        if let Self::Periodic(routine) = self {
            routine.set_job_name(job_name);
        }
    }

    /// The `recorder.Recordable` view; `None` for servers, which the baseline
    /// never registers with the background-job-logger.
    pub fn recordable(self: &Arc<Self>) -> Option<Arc<dyn Recordable>> {
        match self.as_ref() {
            Self::Periodic(routine) => Some(Arc::clone(routine) as Arc<dyn Recordable>),
            Self::Server(_) => None,
        }
    }

    /// `Start` on the given tracker. A server that fails to start exits the
    /// process, exactly like `httpserver.server.Start`'s `os.Exit(1)`.
    pub fn spawn(self: &Arc<Self>, tracker: &TaskTracker, shutdown: CancellationToken) {
        match self.as_ref() {
            Self::Periodic(routine) => Arc::clone(routine).spawn(tracker, shutdown),
            Self::Server(server) => {
                let server = Arc::clone(server);
                tracker.spawn(async move {
                    if let Err(err) = server.start(shutdown).await {
                        tracing::error!(
                            routine = %server.name(),
                            error = %err,
                            "Failed to start server"
                        );
                        std::process::exit(1);
                    }
                });
            }
        }
    }
}
