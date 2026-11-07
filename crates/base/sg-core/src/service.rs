#![forbid(unsafe_code)]

use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::{ConfigError, LockedEnv, ReadyHandle, ServiceError};
use sg_env::EnvReader;

/// A service that takes part in the shared lifecycle.
///
/// `configure` and `validate` run for every service before the environment is
/// locked; `start` runs afterwards and blocks until the service stops.
#[async_trait]
pub trait Service: Send {
    fn name(&self) -> &'static str;

    fn configure(&mut self, env: &mut EnvReader) -> Result<(), ConfigError>;

    fn validate(&self) -> Result<(), ConfigError> {
        Ok(())
    }

    async fn start(self: Box<Self>, ctx: ServiceContext) -> Result<(), ServiceError>;
}

pub struct ServiceContext {
    pub service_name: &'static str,
    /// Cancelled when the process receives a termination signal or a sibling
    /// service fails. Services must stop accepting work and return promptly.
    pub shutdown: CancellationToken,
    /// Mark ready once the service can serve traffic.
    pub ready: ReadyHandle,
    pub env: Arc<LockedEnv>,
}
