use anyhow::{Context, Result};
use clap::Args;
use triad_core::config::TriadConfig;
use triad_runner::backends::run_migrations;

#[derive(Args)]
pub struct MigrateArgs {
    /// Path to triad.yaml (or set TRIAD_CONFIG env var)
    #[arg(short, long, default_value = "triad.yaml", env = "TRIAD_CONFIG")]
    pub config: std::path::PathBuf,

    /// Override the Postgres URL (or set DATABASE_URL env var)
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: Option<String>,
}

pub async fn migrate(args: MigrateArgs) -> Result<()> {
    let pg_url = if let Some(url) = args.database_url {
        url
    } else {
        let cfg = TriadConfig::load(&args.config.to_string_lossy())
            .with_context(|| format!("failed to load config from {:?}", args.config))?;
        cfg.backends.postgres.url
    };

    println!("Applying migrations to {pg_url} ...");
    run_migrations(&pg_url).await.context("migration failed")?;
    println!("Migrations applied successfully.");
    Ok(())
}
