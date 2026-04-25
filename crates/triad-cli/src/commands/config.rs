use anyhow::Result;
use clap::{Args, Subcommand};
use std::path::PathBuf;
use triad_core::config::TriadConfig;

use crate::commands::admin::AdminClient;

#[derive(Subcommand)]
pub enum ConfigCommand {
    /// Validate triad.yaml and print startup check results
    Validate(ValidateArgs),
    /// Reload the running server's configuration from triad.yaml
    Reload,
}

#[derive(Args)]
pub struct ValidateArgs {
    #[arg(short, long, default_value = "triad.yaml", env = "TRIAD_CONFIG")]
    pub config: PathBuf,
}

pub async fn config(cmd: ConfigCommand) -> Result<()> {
    match cmd {
        ConfigCommand::Validate(args) => validate_config(args),
        ConfigCommand::Reload => reload().await,
    }
}

pub fn validate_config(args: ValidateArgs) -> Result<()> {
    let config_path = args.config.to_string_lossy();
    let cfg = TriadConfig::load(&config_path)?;
    cfg.validate()?;
    println!("Config OK: {}", args.config.display());
    Ok(())
}

async fn reload() -> Result<()> {
    let client = AdminClient::from_env();
    client.post("/config/reload").await?;
    println!("Config reloaded.");
    Ok(())
}
