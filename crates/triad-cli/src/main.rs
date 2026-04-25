use anyhow::Result;
use clap::{Parser, Subcommand};

mod commands;

#[derive(Parser)]
#[command(
    name = "triad",
    version,
    about = "Triad integration runner and admin client"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Start the Triad runner as a foreground process
    Run(commands::run::RunArgs),
    /// Print server health, mode, uptime, and active pattern count
    Status,
    /// Manage pattern modules
    #[command(subcommand)]
    Pattern(commands::admin::PatternCommand),
    /// Inspect checkpoint offsets
    #[command(subcommand)]
    Checkpoint(commands::admin::CheckpointCommand),
    /// Inspect and manage DLQ topics
    #[command(subcommand)]
    Dlq(commands::admin::DlqCommand),
    /// Show Kafka consumer group lag per topic/partition
    Lag,
    /// Reload a running pipeline
    #[command(subcommand)]
    Pipeline(commands::admin::PipelineCommand),
    /// Configuration utilities
    #[command(subcommand)]
    Config(commands::config::ConfigCommand),
    /// Apply database migrations against the configured Postgres instance
    Migrate(commands::migrate::MigrateArgs),
    /// Print version, OS/arch, and config schema version
    Version,
    /// Launch the terminal dashboard (requires triad-tui binary in PATH)
    Tui(commands::tui::TuiArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Run(args) => commands::run::run(args).await,
        Command::Status => commands::admin::status().await,
        Command::Pattern(cmd) => commands::admin::pattern(cmd).await,
        Command::Checkpoint(cmd) => commands::admin::checkpoint(cmd).await,
        Command::Dlq(cmd) => commands::admin::dlq(cmd).await,
        Command::Lag => commands::admin::lag().await,
        Command::Pipeline(cmd) => commands::admin::pipeline(cmd).await,
        Command::Config(cmd) => commands::config::config(cmd),
        Command::Migrate(args) => commands::migrate::migrate(args).await,
        Command::Version => commands::version::version(),
        Command::Tui(args) => commands::tui::run_tui(args).await,
    }
}
