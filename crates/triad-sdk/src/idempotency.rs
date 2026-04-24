use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, instrument};
use uuid::Uuid;

/// Errors from idempotency operations.
#[derive(Error, Debug)]
pub enum IdempotencyError {
    #[error("backend: {0}")]
    Backend(String),
    #[error("serialise: {0}")]
    Serialise(#[from] serde_json::Error),
}

/// An opaque idempotency key, optionally scoped by a caller-supplied prefix.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IdempotencyKey(pub String);

impl IdempotencyKey {
    /// Generate a new random key with an optional scope prefix.
    ///
    /// Example: `IdempotencyKey::generate("payments")` → `"payments:<uuid>"`
    pub fn generate(scope: &str) -> Self {
        let id = Uuid::new_v4();
        if scope.is_empty() {
            Self(id.to_string())
        } else {
            Self(format!("{scope}:{id}"))
        }
    }

    /// Wrap an existing key string supplied by the client (e.g. from a request header).
    pub fn wrap(s: &str) -> Self {
        Self(s.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A completed-request record cached alongside the idempotency key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdempotencyRecord {
    pub key: String,
    pub status_code: u16,
    pub body: serde_json::Value,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl IdempotencyRecord {
    pub fn new(key: &IdempotencyKey, status_code: u16, body: serde_json::Value) -> Self {
        Self {
            key: key.0.clone(),
            status_code,
            body,
            created_at: chrono::Utc::now(),
        }
    }
}

/// Narrow trait for idempotency storage — mockable in tests.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait IdempotencyStore: Send + Sync {
    /// Look up an existing record. Returns `None` if not found.
    async fn get(&self, key: &str) -> Result<Option<IdempotencyRecord>, IdempotencyError>;

    /// Store a record with the given TTL using SET NX semantics (only if absent).
    /// Returns `true` if the record was stored (new key), `false` if it already existed.
    async fn set_nx(
        &self,
        key: &str,
        record: &IdempotencyRecord,
        ttl: Duration,
    ) -> Result<bool, IdempotencyError>;
}

/// Look up whether a request has already been processed.
///
/// Returns `Some(record)` on a duplicate request, `None` on first occurrence.
#[instrument(skip(store), fields(idempotency_key = %key))]
pub async fn lookup(
    store: &dyn IdempotencyStore,
    key: &IdempotencyKey,
) -> Result<Option<IdempotencyRecord>, IdempotencyError> {
    debug!("checking idempotency key");
    store.get(key.as_str()).await
}

/// Persist the result of a completed request so future duplicates short-circuit.
///
/// Returns `true` if this was the first write (key was new), `false` if a
/// concurrent request already stored it (race was lost — caller should re-read).
#[instrument(skip(store, record), fields(idempotency_key = %key))]
pub async fn store_result(
    store: &dyn IdempotencyStore,
    key: &IdempotencyKey,
    record: &IdempotencyRecord,
    ttl: Duration,
) -> Result<bool, IdempotencyError> {
    debug!("storing idempotency record");
    store.set_nx(key.as_str(), record, ttl).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use mockall::predicate::*;
    use rstest::rstest;

    #[rstest]
    #[case("payments", true)]
    #[case("", false)]
    fn test_generate_key_prefix(#[case] scope: &str, #[case] has_prefix: bool) {
        let key = IdempotencyKey::generate(scope);
        if has_prefix {
            assert!(key.as_str().starts_with(&format!("{scope}:")));
        } else {
            assert!(Uuid::parse_str(key.as_str()).is_ok());
        }
    }

    #[test]
    fn test_key_from_str_display() {
        let key = IdempotencyKey::wrap("my-key-123");
        assert_eq!(key.to_string(), "my-key-123");
        assert_eq!(key.as_str(), "my-key-123");
    }

    #[test]
    fn test_key_equality() {
        let a = IdempotencyKey::wrap("k");
        let b = IdempotencyKey::wrap("k");
        assert_eq!(a, b);
    }

    #[test]
    fn test_generate_keys_are_unique() {
        let a = IdempotencyKey::generate("scope");
        let b = IdempotencyKey::generate("scope");
        assert_ne!(a, b);
    }

    #[test]
    fn test_record_new_fields() {
        let key = IdempotencyKey::wrap("test-key");
        let body = serde_json::json!({"ok": true});
        let rec = IdempotencyRecord::new(&key, 200, body.clone());
        assert_eq!(rec.status_code, 200);
        assert_eq!(rec.key, "test-key");
        assert_eq!(rec.body, body);
    }

    #[tokio::test]
    async fn test_lookup_returns_none_when_not_found() {
        let mut mock = MockIdempotencyStore::new();
        mock.expect_get()
            .with(eq("missing-key"))
            .returning(|_| Ok(None));

        let key = IdempotencyKey::wrap("missing-key");
        let result = lookup(&mock, &key).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_lookup_returns_record_when_found() {
        let mut mock = MockIdempotencyStore::new();
        mock.expect_get().with(eq("existing-key")).returning(|k| {
            Ok(Some(IdempotencyRecord {
                key: k.to_string(),
                status_code: 201,
                body: serde_json::json!({"id": 42}),
                created_at: chrono::Utc::now(),
            }))
        });

        let key = IdempotencyKey::wrap("existing-key");
        let rec = lookup(&mock, &key).await.unwrap().unwrap();
        assert_eq!(rec.status_code, 201);
    }

    #[tokio::test]
    async fn test_store_result_returns_true_on_new_key() {
        let mut mock = MockIdempotencyStore::new();
        mock.expect_set_nx().returning(|_, _, _| Ok(true));

        let key = IdempotencyKey::wrap("new-key");
        let rec = IdempotencyRecord::new(&key, 200, serde_json::json!({}));
        let stored = store_result(&mock, &key, &rec, Duration::from_secs(3600))
            .await
            .unwrap();
        assert!(stored);
    }

    #[tokio::test]
    async fn test_store_result_returns_false_on_duplicate() {
        let mut mock = MockIdempotencyStore::new();
        mock.expect_set_nx().returning(|_, _, _| Ok(false));

        let key = IdempotencyKey::wrap("dup-key");
        let rec = IdempotencyRecord::new(&key, 200, serde_json::json!({}));
        let stored = store_result(&mock, &key, &rec, Duration::from_secs(3600))
            .await
            .unwrap();
        assert!(!stored);
    }
}
