use std::sync::Arc;

use pyo3::prelude::*;
use triad_core::config::OutboxPatternConfig;
use triad_sdk::patterns::OutboxPublisher;

use crate::{get_rt, instance::PyTransaction};

/// SDK facade for inserting events into `triad_outbox` inside a caller's transaction.
///
/// Usage:
///
/// ```python
/// publisher = OutboxPublisher(table="triad_outbox", kafka_topic="orders.events")
/// async with instance.transaction() as tx:
///     event_id = await publisher.publish(tx, "order", "ord-1", "order.created", {"total": 100})
/// ```
#[pyclass]
pub struct PyOutboxPublisher {
    inner: Arc<OutboxPublisher>,
}

#[pymethods]
impl PyOutboxPublisher {
    /// Create a new publisher.
    ///
    /// Args:
    ///     table: the outbox table name (e.g. "triad_outbox")
    ///     kafka_topic: the Kafka topic events will be published to
    ///     name: optional pattern name for tracing (default "py-outbox")
    #[new]
    #[pyo3(signature = (table, kafka_topic, name = None))]
    fn new(table: String, kafka_topic: String, name: Option<String>) -> Self {
        Self {
            inner: Arc::new(OutboxPublisher::new(OutboxPatternConfig {
                name: name.unwrap_or_else(|| "py-outbox".to_string()),
                table,
                kafka_topic,
                outbox_retention: None,
            })),
        }
    }

    /// Insert an event into the outbox within an existing transaction.
    ///
    /// Args:
    ///     tx: a PyTransaction obtained from `instance.transaction()`
    ///     aggregate_type: e.g. "order", "payment"
    ///     aggregate_id: the domain entity ID
    ///     event_type: dot-namespaced (e.g. "order.created")
    ///     payload: a Python dict or any JSON-serialisable object
    ///
    /// Returns:
    ///     The event UUID as a string.
    async fn publish(
        slf: Py<Self>,
        tx: Py<PyTransaction>,
        aggregate_type: String,
        aggregate_id: String,
        event_type: String,
        payload: Py<PyAny>,
    ) -> PyResult<String> {
        // Serialise payload dict → JSON string while holding the GIL
        let payload_json = Python::attach(|py| -> PyResult<String> {
            let json_mod = py.import("json")?;
            json_mod
                .call_method1("dumps", (payload.bind(py),))?
                .extract()
        })?;

        let publisher = Python::attach(|py| Arc::clone(&slf.borrow(py).inner));
        let tx_arc = Python::attach(|py| Arc::clone(&tx.borrow(py).tx));

        let (send, recv) = futures::channel::oneshot::channel::<Result<String, String>>();
        get_rt().spawn(async move {
            let payload_val: serde_json::Value = match serde_json::from_str(&payload_json) {
                Ok(v) => v,
                Err(e) => {
                    let _ = send.send(Err(format!("JSON parse error: {e}")));
                    return;
                }
            };
            let mut guard = tx_arc.lock().await;
            let result = match guard.as_mut() {
                Some(sqlx_tx) => publisher
                    .publish(
                        sqlx_tx,
                        &aggregate_type,
                        &aggregate_id,
                        &event_type,
                        &payload_val,
                    )
                    .await
                    .map(|id| id.0.to_string())
                    .map_err(|e| e.to_string()),
                None => Err("transaction already consumed".to_string()),
            };
            let _ = send.send(result);
        });

        recv.await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)
    }
}
