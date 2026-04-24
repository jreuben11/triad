use anyhow::Result;
use clap::{Args, Subcommand};
use std::path::PathBuf;
use triad_core::config::TriadConfig;

#[derive(Subcommand)]
pub enum ConfigCommand {
    /// Validate triad.yaml and print startup check results
    Validate(ValidateArgs),
}

#[derive(Args)]
pub struct ValidateArgs {
    #[arg(short, long, default_value = "triad.yaml", env = "TRIAD_CONFIG")]
    pub config: PathBuf,
}

pub fn config(cmd: ConfigCommand) -> Result<()> {
    match cmd {
        ConfigCommand::Validate(args) => validate(args),
    }
}

fn validate(args: ValidateArgs) -> Result<()> {
    let config_path = args.config.to_string_lossy();
    let cfg = TriadConfig::load(&config_path)?;
    cfg.validate()?;
    println!("Config OK: {}", args.config.display());
    Ok(())
}
