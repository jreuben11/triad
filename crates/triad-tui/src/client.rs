use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Default, Clone)]
pub struct AppData {
    pub connecting: bool,
    pub live: Option<LiveData>,
    pub ready: Option<ReadyData>,
    pub patterns: Vec<PatternInfo>,
    pub lag: Vec<LagInfo>,
    pub checkpoints: Vec<CheckpointInfo>,
    pub sagas: Vec<SagaInfo>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LiveData {
    pub status: String,
    pub uptime_seconds: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BackendStatus {
    pub status: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReadyData {
    #[allow(dead_code)]
    pub status: String,
    pub backends: HashMap<String, BackendStatus>,
    #[allow(dead_code)]
    pub cold_start_complete: bool,
    #[allow(dead_code)]
    pub drain_mode: bool,
    pub leader: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PatternInfo {
    pub name: String,
    pub pattern_type: String,
    pub status: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LagInfo {
    #[allow(dead_code)]
    pub pattern_name: String,
    pub topic: String,
    #[allow(dead_code)]
    pub partition: i32,
    pub lag_messages: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CheckpointInfo {
    pub pattern_name: String,
    pub pipeline_name: String,
    #[allow(dead_code)]
    pub owner_instance_id: String,
    pub version: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SagaInfo {
    pub saga_id: String,
    pub saga_name: String,
    pub current_step: i32,
    pub status: String,
}

pub struct AdminClient {
    base_url: String,
    client: reqwest::Client,
}

impl AdminClient {
    pub fn new(base_url: String) -> Self {
        Self {
            base_url,
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(3))
                .build()
                .unwrap_or_default(),
        }
    }

    pub async fn fetch_all(&self) -> AppData {
        let live = self.get::<LiveData>("/health/live").await.ok();
        if live.is_none() {
            return AppData {
                connecting: true,
                ..Default::default()
            };
        }

        let ready = self.get::<ReadyData>("/health/ready").await.ok();
        let patterns = self
            .get::<Vec<PatternInfo>>("/patterns")
            .await
            .unwrap_or_default();
        let lag = self.get::<Vec<LagInfo>>("/lag").await.unwrap_or_default();
        let checkpoints = self
            .get::<Vec<CheckpointInfo>>("/checkpoints")
            .await
            .unwrap_or_default();
        let sagas = self.get::<Vec<SagaInfo>>("/saga").await.unwrap_or_default();

        AppData {
            connecting: false,
            live,
            ready,
            patterns,
            lag,
            checkpoints,
            sagas,
        }
    }

    pub async fn call(&self, method: &str, path: &str) -> anyhow::Result<()> {
        let url = format!("{}{}", self.base_url, path);
        let resp = match method {
            "DELETE" => self.client.delete(&url).send().await?,
            "POST" => self.client.post(&url).send().await?,
            _ => self.client.get(&url).send().await?,
        };
        let _ = resp;
        Ok(())
    }

    async fn get<T: for<'de> serde::Deserialize<'de>>(&self, path: &str) -> anyhow::Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self.client.get(&url).send().await?;
        Ok(resp.json::<T>().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_app_data_default_is_connecting_false() {
        let data = AppData::default();
        assert!(!data.connecting);
        assert!(data.live.is_none());
        assert!(data.patterns.is_empty());
    }

    #[test]
    fn test_admin_client_new() {
        let client = AdminClient::new("http://localhost:8080".to_string());
        assert_eq!(client.base_url, "http://localhost:8080");
    }
}
