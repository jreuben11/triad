use anyhow::Result;
use clap::Args;
use std::path::PathBuf;
use triad_core::config::TriadConfig;

#[derive(Args)]
pub struct RunArgs {
    #[arg(short, long, default_value = "triad.yaml", env = "TRIAD_CONFIG")]
    pub config: PathBuf,
}

// TODO: Replace stub with Runner::new(config, engine, admin).run().await once
// feat/triad-runner-engine is merged to main. The Runner installs the SIGTERM
// handler via ShutdownCoordinator::install_signal_handler() inside Runner::run().
pub async fn run(args: RunArgs) -> Result<()> {
    let config_path = args.config.to_string_lossy();
    TriadConfig::load(&config_path)?;
    anyhow::bail!(
        "triad run requires feat/triad-runner-engine to be merged. \
         Config at '{}' loaded successfully.",
        args.config.display()
    )
}
