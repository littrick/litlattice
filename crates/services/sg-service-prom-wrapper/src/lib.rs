#![forbid(unsafe_code)]

//! Rust `prom-wrapper` process, drop-in for
//! `docker-images/prometheus/cmd/prom-wrapper`.

pub mod alertmanager;
pub mod amconfig;
pub mod config;
pub mod process;
pub mod proxy;
pub mod receivers;
pub mod server;
pub mod silence;
pub mod status;
pub mod subscriber;

use async_trait::async_trait;
use sg_core::{ConfigError, EnvReader, Service, ServiceContext, ServiceError};

pub fn service_name() -> &'static str {
    "prom-wrapper"
}

#[derive(Debug, Default)]
pub struct PromWrapperService {
    config: Option<Config>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub export_port: String,
    pub prometheus_port: String,
    pub alertmanager_port: String,
    pub alertmanager_config_path: String,
    pub alertmanager_enable_cluster: bool,
    pub disable_alertmanager: bool,
    pub disable_sourcegraph_config: bool,
}

impl Config {
    pub fn load(env: &mut EnvReader) -> Self {
        Self {
            export_port: env.get(
                "EXPORT_PORT",
                "9090",
                "port used for the external reverse proxy",
            ),
            prometheus_port: env.get(
                "PROMETHEUS_INTERNAL_PORT",
                "9092",
                "internal Prometheus port",
            ),
            alertmanager_port: env.get(
                "ALERTMANAGER_INTERNAL_PORT",
                "9093",
                "internal Alertmanager port",
            ),
            alertmanager_config_path: env.get(
                "ALERTMANAGER_CONFIG_PATH",
                "/sg_config_prometheus/alertmanager.yml",
                "path to alertmanager configuration",
            ),
            alertmanager_enable_cluster: env.get_bool(
                "ALERTMANAGER_ENABLE_CLUSTER",
                false,
                "enable alertmanager clustering",
            ),
            disable_alertmanager: std::env::var("DISABLE_ALERTMANAGER")
                .is_ok_and(|value| value == "true"),
            disable_sourcegraph_config: std::env::var("DISABLE_SOURCEGRAPH_CONFIG")
                .is_ok_and(|value| value == "true"),
        }
    }
}

impl PromWrapperService {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Service for PromWrapperService {
    fn name(&self) -> &'static str {
        service_name()
    }

    fn configure(&mut self, env: &mut EnvReader) -> Result<(), ConfigError> {
        self.config = Some(Config::load(env));
        Ok(())
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.config.is_none() {
            return Err(ConfigError::new(
                "prom-wrapper configuration was not loaded",
            ));
        }
        Ok(())
    }

    async fn start(self: Box<Self>, ctx: ServiceContext) -> Result<(), ServiceError> {
        use std::sync::Arc;
        use std::time::Duration;

        let config = self
            .config
            .clone()
            .ok_or_else(|| ServiceError::new("prom-wrapper configuration was not loaded"))?;

        let client = Arc::new(alertmanager::AlertmanagerClient::new(
            format!("http://127.0.0.1:{}", config.alertmanager_port),
            config.alertmanager_config_path.clone(),
        ));

        // Go propagates `os.Args[1:]` to Prometheus.
        let prometheus_args: Vec<String> = std::env::args().skip(1).collect();
        let prometheus = process::spawn(
            process::prometheus_command(&config.prometheus_port, &prometheus_args),
            "prometheus",
        )
        .await
        .map_err(|error| ServiceError::new(format!("starting prometheus: {error}")))?;
        let mut prometheus_task = tokio::spawn(process::supervise(prometheus));

        let mut alertmanager_task = None;
        if !config.disable_alertmanager {
            let alertmanager = process::spawn(
                process::alertmanager_command(
                    std::path::Path::new(&config.alertmanager_config_path),
                    config.alertmanager_enable_cluster,
                ),
                "alertmanager",
            )
            .await
            .map_err(|error| ServiceError::new(format!("starting alertmanager: {error}")))?;
            // Go waits up to 30s for Alertmanager. Stay responsive to
            // shutdown and to a Prometheus crash while waiting.
            let ready = client.wait_until_ready(Duration::from_secs(30));
            tokio::pin!(ready);
            tokio::select! {
                result = &mut ready => result.map_err(ServiceError::new)?,
                _ = ctx.shutdown.cancelled() => return Ok(()),
                code = &mut prometheus_task => {
                    let code = match code {
                        Ok(Ok(code)) => code,
                        Ok(Err(error)) => return Err(ServiceError::new(format!("prometheus failed: {error}"))),
                        Err(error) => return Err(ServiceError::new(format!("prometheus task failed: {error}"))),
                    };
                    return Err(ServiceError::new(format!("prometheus exited with code {code}")));
                }
            }
            alertmanager_task = Some(tokio::spawn(process::supervise(alertmanager)));
        }

        let subscriber = if !config.disable_alertmanager && !config.disable_sourcegraph_config {
            let subscriber = Arc::new(subscriber::SiteConfigSubscriber::new(Arc::clone(&client)));
            let running = Arc::clone(&subscriber);
            let shutdown = ctx.shutdown.clone();
            tokio::spawn(async move { running.run(shutdown).await });
            Some(subscriber)
        } else {
            None
        };

        let status = Arc::new(status::AlertsStatusReporter::new(Arc::clone(&client)));
        let state = Arc::new(server::ServerState {
            prometheus_port: config.prometheus_port.clone(),
            alertmanager_port: config.alertmanager_port.clone(),
            subscriber,
            status,
            alertmanager: client,
            disable_alertmanager: config.disable_alertmanager,
            disable_sourcegraph_config: config.disable_sourcegraph_config,
        });

        let listener = sg_http::listen(&format!(":{}", config.export_port))
            .await
            .map_err(|error| ServiceError::new(format!("bind :{}: {error}", config.export_port)))?;
        tracing::info!(addr = %format!(":{}", config.export_port), "serving reverse proxy");
        ctx.ready.mark_ready();

        let serving = sg_http::serve_http(listener, server::handler(state), ctx.shutdown.clone());
        tokio::pin!(serving);

        let child_exit = async {
            match alertmanager_task {
                Some(alertmanager) => {
                    tokio::select! {
                        code = prometheus_task => ("prometheus", code),
                        code = alertmanager => ("alertmanager", code),
                    }
                }
                None => ("prometheus", prometheus_task.await),
            }
        };

        tokio::select! {
            result = &mut serving => result
                .map_err(|error| ServiceError::new(format!("serving reverse proxy: {error}"))),
            (name, result) = child_exit => {
                let code = match result {
                    Ok(Ok(code)) => code,
                    Ok(Err(error)) => return Err(ServiceError::new(format!("{name} failed: {error}"))),
                    Err(error) => return Err(ServiceError::new(format!("{name} task failed: {error}"))),
                };
                Err(ServiceError::new(format!("{name} exited with code {code}")))
            }
            _ = ctx.shutdown.cancelled() => Ok(()),
        }
    }
}
