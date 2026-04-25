use anyhow::{Context, Result};
use clap::Subcommand;
use serde::de::DeserializeOwned;
use serde_json::Value;

// ── Admin HTTP client ────────────────────────────────────────────────────────

pub struct AdminClient {
    base_url: String,
    client: reqwest::Client,
}

impl AdminClient {
    pub fn from_env() -> Self {
        let base_url = std::env::var("TRIAD_ADMIN_URL")
            .unwrap_or_else(|_| "http://localhost:8080".to_string());
        Self {
            base_url,
            client: reqwest::Client::new(),
        }
    }

    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        self.client
            .get(&url)
            .send()
            .await
            .context("failed to connect to admin server")?
            .error_for_status()
            .context("admin server returned error status")?
            .json::<T>()
            .await
            .context("failed to parse response")
    }

    pub async fn post(&self, path: &str) -> Result<()> {
        let url = format!("{}{}", self.base_url, path);
        self.client
            .post(&url)
            .send()
            .await
            .context("failed to connect to admin server")?
            .error_for_status()
            .context("admin server returned error status")?;
        Ok(())
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        let url = format!("{}{}", self.base_url, path);
        self.client
            .delete(&url)
            .send()
            .await
            .context("failed to connect to admin server")?
            .error_for_status()
            .context("admin server returned error status")?;
        Ok(())
    }
}

// ── Subcommand trees ─────────────────────────────────────────────────────────

#[derive(Subcommand)]
pub enum PatternCommand {
    /// List all pattern modules and their state
    List,
    /// Pause a pattern module
    Pause {
        /// Pattern module name
        name: String,
    },
    /// Resume a paused pattern module
    Resume {
        /// Pattern module name
        name: String,
    },
}

#[derive(Subcommand)]
pub enum CheckpointCommand {
    /// List checkpoint offsets for all pipelines
    List,
}

#[derive(Subcommand)]
pub enum DlqCommand {
    /// List all DLQ topics with message counts
    List,
    /// List messages in a specific DLQ topic (triad.dlq.<topic>)
    Messages {
        /// Source topic name
        topic: String,
    },
    /// Replay messages from a DLQ topic back to the source topic
    Replay {
        /// Source topic name
        topic: String,
    },
    /// Discard all DLQ messages for a topic (destructive)
    #[command(alias = "purge")]
    Drop {
        /// Source topic name
        topic: String,
    },
}

#[derive(Subcommand)]
pub enum PipelineCommand {
    /// Reload a running pipeline's configuration
    Reload {
        /// Pipeline name
        name: String,
    },
}

#[derive(Subcommand)]
pub enum SagaCommand {
    /// List in-flight sagas with step and state
    List,
    /// Print full saga state including step history
    Inspect {
        /// Saga ID
        id: String,
    },
    /// Trigger compensation for a running saga
    Cancel {
        /// Saga ID
        id: String,
    },
}

// ── Command handlers ─────────────────────────────────────────────────────────

pub async fn status() -> Result<()> {
    let client = AdminClient::from_env();
    let health: Value = client.get("/health/ready").await?;
    let patterns: Value = client.get("/patterns").await?;
    println!("Health:\n{}", serde_json::to_string_pretty(&health)?);
    println!("\nPatterns:\n{}", serde_json::to_string_pretty(&patterns)?);
    Ok(())
}

pub async fn pattern(cmd: PatternCommand) -> Result<()> {
    let client = AdminClient::from_env();
    match cmd {
        PatternCommand::List => {
            let result: Value = client.get("/patterns").await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        PatternCommand::Pause { name } => {
            client.post(&format!("/patterns/{name}/pause")).await?;
            println!("Pattern '{name}' paused.");
        }
        PatternCommand::Resume { name } => {
            client.post(&format!("/patterns/{name}/resume")).await?;
            println!("Pattern '{name}' resumed.");
        }
    }
    Ok(())
}

pub async fn checkpoint(cmd: CheckpointCommand) -> Result<()> {
    let client = AdminClient::from_env();
    match cmd {
        CheckpointCommand::List => {
            let result: Value = client.get("/checkpoints").await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
    }
    Ok(())
}

pub async fn dlq(cmd: DlqCommand) -> Result<()> {
    let client = AdminClient::from_env();
    match cmd {
        DlqCommand::List => {
            let result: Value = client.get("/dlq").await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        DlqCommand::Messages { topic } => {
            let result: Value = client.get(&format!("/dlq/{topic}")).await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        DlqCommand::Replay { topic } => {
            client.post(&format!("/dlq/{topic}/replay")).await?;
            println!("DLQ replay triggered for topic '{topic}'.");
        }
        DlqCommand::Drop { topic } => {
            client.delete(&format!("/dlq/{topic}")).await?;
            println!("DLQ dropped for topic '{topic}'.");
        }
    }
    Ok(())
}

pub async fn pipeline(cmd: PipelineCommand) -> Result<()> {
    let client = AdminClient::from_env();
    match cmd {
        PipelineCommand::Reload { name } => {
            client.post(&format!("/pipelines/{name}/reload")).await?;
            println!("Pipeline '{name}' reload triggered.");
        }
    }
    Ok(())
}

pub async fn saga(cmd: SagaCommand) -> Result<()> {
    let client = AdminClient::from_env();
    match cmd {
        SagaCommand::List => {
            let result: Value = client.get("/saga").await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        SagaCommand::Inspect { id } => {
            let result: Value = client.get(&format!("/saga/{id}")).await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        SagaCommand::Cancel { id } => {
            client.post(&format!("/saga/{id}/cancel")).await?;
            println!("Saga '{id}' cancel triggered.");
        }
    }
    Ok(())
}

pub async fn lag() -> Result<()> {
    let client = AdminClient::from_env();
    let result: Value = client.get("/lag").await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
