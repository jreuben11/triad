use pyo3::prelude::*;
use triad_sdk::idempotency::IdempotencyKey;

// ── PyIdempotencyKey ─────────────────────────────────────────────────────────

/// An opaque idempotency key, optionally scoped by a caller-supplied prefix.
///
/// Usage:
///
/// ```python
/// key = IdempotencyKey.generate("payments")
/// assert str(key).startswith("payments:")
///
/// key2 = IdempotencyKey.wrap("my-request-id")
/// assert str(key2) == "my-request-id"
/// ```
#[pyclass(skip_from_py_object)]
#[derive(Clone)]
pub struct PyIdempotencyKey {
    inner: IdempotencyKey,
}

#[pymethods]
impl PyIdempotencyKey {
    /// Generate a new random key with an optional scope prefix.
    ///
    /// Example: `IdempotencyKey.generate("payments")` → `"payments:<uuid>"`
    #[staticmethod]
    fn generate(scope: String) -> Self {
        Self {
            inner: IdempotencyKey::generate(&scope),
        }
    }

    /// Wrap an existing key string (e.g. from a request header).
    #[staticmethod]
    fn wrap(key: String) -> Self {
        Self {
            inner: IdempotencyKey::wrap(&key),
        }
    }

    fn __str__(&self) -> String {
        self.inner.to_string()
    }

    fn __repr__(&self) -> String {
        format!("IdempotencyKey({:?})", self.inner.as_str())
    }

    fn __eq__(&self, other: &PyIdempotencyKey) -> bool {
        self.inner == other.inner
    }

    fn __hash__(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.inner.hash(&mut hasher);
        hasher.finish()
    }
}

// ── PyIdempotencyRecord ──────────────────────────────────────────────────────

/// A completed-request record cached alongside an idempotency key.
///
/// Usage:
///
/// ```python
/// record = IdempotencyRecord(key, status_code=200, body={"id": 42})
/// print(record.key, record.status_code, record.body)
/// ```
#[pyclass(get_all)]
pub struct PyIdempotencyRecord {
    pub key: String,
    pub status_code: u16,
    pub body: String, // JSON string
    pub created_at: String,
}

#[pymethods]
impl PyIdempotencyRecord {
    /// Create a new record.
    ///
    /// Args:
    ///     key: the IdempotencyKey this record is associated with
    ///     status_code: HTTP status code of the original response
    ///     body: response body as a Python dict or JSON-serialisable value
    #[new]
    fn new(
        key: &PyIdempotencyKey,
        status_code: u16,
        body: Py<PyAny>,
        py: Python<'_>,
    ) -> PyResult<Self> {
        let json_mod = py.import("json")?;
        let body_str: String = json_mod
            .call_method1("dumps", (body.bind(py),))?
            .extract()?;
        Ok(Self {
            key: key.inner.to_string(),
            status_code,
            body: body_str,
            created_at: chrono::Utc::now().to_rfc3339(),
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "IdempotencyRecord(key={:?}, status_code={})",
            self.key, self.status_code
        )
    }
}

// ── PyIdempotencyStore ───────────────────────────────────────────────────────

/// A Python-facing in-memory idempotency store.
///
/// Useful for testing. For production, use a Redis- or Postgres-backed store.
///
/// Usage:
///
/// ```python
/// store = IdempotencyStore()
/// stored = store.set_nx("my-key", '{"result": "ok"}', ttl_secs=3600)
/// assert stored  # True for new key
/// body = store.get("my-key")
/// assert body == '{"result": "ok"}'
/// ```
#[pyclass]
pub struct PyIdempotencyStore {
    map: std::sync::Mutex<std::collections::HashMap<String, PyIdempotencyRecord>>,
}

#[pymethods]
impl PyIdempotencyStore {
    #[new]
    fn new() -> Self {
        Self {
            map: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Look up a record by key string. Returns the JSON body or None.
    fn get(&self, key: String) -> Option<String> {
        let guard = self.map.lock().unwrap();
        guard.get(&key).map(|r| r.body.clone())
    }

    /// Store a record under a key (NX semantics).
    ///
    /// Returns True if stored (new key), False if already existed.
    fn set_nx(&self, key: String, body: String, _ttl_secs: u64) -> bool {
        let mut guard = self.map.lock().unwrap();
        if guard.contains_key(&key) {
            return false;
        }
        guard.insert(
            key.clone(),
            PyIdempotencyRecord {
                key,
                status_code: 200,
                body,
                created_at: chrono::Utc::now().to_rfc3339(),
            },
        );
        true
    }

    /// Number of keys currently in the store.
    fn __len__(&self) -> usize {
        self.map.lock().unwrap().len()
    }
}

// ── Module-level functions ───────────────────────────────────────────────────

/// Look up whether a request has already been processed (using in-memory store).
///
/// Returns the JSON body string if a duplicate, None if first occurrence.
#[pyfunction]
pub fn py_lookup(store: &PyIdempotencyStore, key: String) -> Option<String> {
    store.get(key)
}

/// Store the result of a completed request (using in-memory store).
///
/// Returns True if stored (new key), False if already existed.
#[pyfunction]
#[pyo3(signature = (store, key, body, ttl_secs=None))]
pub fn py_store_result(
    store: &PyIdempotencyStore,
    key: String,
    body: String,
    ttl_secs: Option<u64>,
) -> bool {
    store.set_nx(key, body, ttl_secs.unwrap_or(3600))
}
