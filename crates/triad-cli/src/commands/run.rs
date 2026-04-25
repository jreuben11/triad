use anyhow::Result;
use clap::Args;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use triad_core::config::TriadConfig;
use triad_runner::{
    admin::{AdminServer, AdminState, PatternControl},
    engine::PatternEngine,
    runner::Runner,
};

#[derive(Args)]
pub struct RunArgs {
    #[arg(short, long, default_value = "triad.yaml", env = "TRIAD_CONFIG")]
    pub config: PathBuf,
}

pub async fn run(args: RunArgs) -> Result<()> {
    let config_path = args.config.to_string_lossy().into_owned();
    let config = TriadConfig::load(&config_path)?;

    let admin_port = config.admin.port;
    let kafka_brokers = config.backends.kafka.brokers.join(",");
    let redis_url = config.backends.redis.url.clone();

    let cancel = CancellationToken::new();
    let (control_tx, control_rx) = mpsc::channel::<PatternControl>(64);

    let engine = PatternEngine::new(cancel).with_control_rx(control_rx);

    // Load a second copy for the live-reloadable shared config (TriadConfig is not Clone).
    let shared_config = Arc::new(tokio::sync::RwLock::new(TriadConfig::load(&config_path)?));
    let state = AdminState::new()
        .with_kafka_brokers(kafka_brokers)
        .with_redis_url(redis_url)
        .with_control_tx(control_tx)
        .with_config_path(config_path)
        .with_shared_config(shared_config)
        .with_mode("standalone");

    let admin = AdminServer::new(admin_port, state);
    let mut runner = Runner::new(config, engine, admin);
    runner.run().await?;
    Ok(())
}
