#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::{LockedEnv, ReadyHandle, Service, ServiceContext};
use sg_env::EnvReader;

/// How long a cancelled service gets to return before its task is aborted.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy)]
pub struct RunOptions {
    /// Install SIGINT/SIGTERM handlers that cancel all services. Combined
    /// binaries that own signal handling pass `false`.
    pub handle_signals: bool,
}

impl Default for RunOptions {
    fn default() -> Self {
        Self {
            handle_signals: true,
        }
    }
}

/// Run the baseline `svcmain` lifecycle and return the process exit code.
///
/// `0` — every service stopped on its own after a termination signal;
/// `1` — configuration was invalid or a service failed to start.
pub async fn run(services: Vec<Box<dyn Service>>) -> i32 {
    run_with_options(services, RunOptions::default()).await
}

pub async fn run_with_options(mut services: Vec<Box<dyn Service>>, options: RunOptions) -> i32 {
    // Configure every service before the environment is locked.
    let mut env = EnvReader::new();
    for service in services.iter_mut() {
        if let Err(err) = service.configure(&mut env) {
            tracing::error!(
                service = service.name(),
                error = %err,
                "failed to configure service"
            );
            return 1;
        }
    }

    // Validate every configuration (baseline: fatal "invalid configuration").
    for service in services.iter() {
        if let Err(err) = service.validate() {
            tracing::error!(
                service = service.name(),
                error = %err,
                "invalid configuration"
            );
            return 1;
        }
    }

    env.lock();
    sg_env::handle_help_flag(&env);

    let env = Arc::new(LockedEnv::new(env));
    let shutdown = CancellationToken::new();

    let mut tasks: JoinSet<(&'static str, Result<(), crate::ServiceError>)> = JoinSet::new();
    let mut ready_handles = Vec::with_capacity(services.len());

    for service in services {
        let ready = ReadyHandle::new();
        ready_handles.push(ready.clone());
        let ctx = ServiceContext {
            service_name: service.name(),
            shutdown: shutdown.clone(),
            ready: ready.clone(),
            env: env.clone(),
        };
        tasks.spawn(async move {
            let name = service.name();
            let result = service.start(ctx).await;
            // The framework always marks a stopped service ready, mirroring
            // `defer ready()` in the baseline.
            ready.mark_ready();
            if let Err(err) = &result {
                tracing::error!(service = name, error = %err, "service failed to start");
            }
            (name, result)
        });
    }

    // Aggregate readiness for the (single) debug server.
    let all_ready = ReadyHandle::new();
    let readiness = tokio::spawn({
        let all_ready = all_ready.clone();
        async move {
            for handle in ready_handles {
                handle.wait_ready().await;
            }
            all_ready.mark_ready();
            tracing::info!("all services ready");
        }
    });

    let signal = termination_signal();
    tokio::pin!(signal);

    let mut exit_code = 0;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = &mut signal, if options.handle_signals => {
                tracing::info!("received termination signal, shutting down");
                shutdown.cancel();
                break;
            }
            joined = tasks.join_next() => {
                match joined {
                    Some(Ok((name, Ok(())))) => {
                        tracing::info!(service = name, "service stopped");
                    }
                    Some(Ok((name, Err(err)))) => {
                        tracing::error!(service = name, error = %err, "service failed");
                        exit_code = 1;
                        shutdown.cancel();
                        break;
                    }
                    Some(Err(err)) => {
                        tracing::error!(error = %err, "service task panicked");
                        exit_code = 1;
                        shutdown.cancel();
                        break;
                    }
                    // All services have stopped.
                    None => break,
                }
            }
        }
    }

    let drain = async { while tasks.join_next().await.is_some() {} };
    if tokio::time::timeout(SHUTDOWN_GRACE, drain).await.is_err() {
        tracing::warn!(
            grace_seconds = SHUTDOWN_GRACE.as_secs(),
            "services did not stop in time, aborting"
        );
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    readiness.abort();

    exit_code
}

async fn termination_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(sigterm) => sigterm,
            Err(err) => {
                tracing::warn!(error = %err, "cannot install SIGTERM handler");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
