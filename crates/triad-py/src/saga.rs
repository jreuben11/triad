use pyo3::prelude::*;
use triad_core::config::SagaStepConfig;

// ── PySagaStepConfig ─────────────────────────────────────────────────────────

/// A single saga step configuration.
#[pyclass(get_all, skip_from_py_object)]
#[derive(Clone)]
pub struct PySagaStepConfig {
    pub name: String,
    pub command_topic: String,
    pub reply_topic: String,
    pub compensation: Option<String>,
    pub timeout: Option<String>,
}

#[pymethods]
impl PySagaStepConfig {
    fn __repr__(&self) -> String {
        format!(
            "SagaStepConfig(name={:?}, command_topic={:?}, reply_topic={:?})",
            self.name, self.command_topic, self.reply_topic
        )
    }
}

// ── PySagaConfig ─────────────────────────────────────────────────────────────

/// A complete saga pattern configuration.
#[pyclass(get_all)]
pub struct PySagaConfig {
    pub name: String,
    pub trigger_topic: String,
    pub trigger_event: String,
    pub timeout: String,
    pub steps: Vec<PySagaStepConfig>,
}

#[pymethods]
impl PySagaConfig {
    fn __repr__(&self) -> String {
        format!(
            "SagaConfig(name={:?}, steps={})",
            self.name,
            self.steps.len()
        )
    }
}

// ── PySagaBuilder ─────────────────────────────────────────────────────────────
//
// SagaBuilder in triad-sdk uses a consuming fluent style (`self` not `&mut self`).
// To bridge that to Python (which needs `&mut self` / `PyRefMut`), we store the
// internal config directly and re-implement the same fluent API.

/// Fluent builder for assembling a SagaConfig.
///
/// Usage:
///
/// ```python
/// config = (SagaBuilder("order-saga", "orders.commands", "OrderCreate", "30s")
///     .step("reserve-inventory", "inventory.commands", "inventory.replies")
///     .with_compensation("inventory.compensate")
///     .step("charge-payment", "payments.commands", "payments.replies")
///     .build())
///
/// assert config.name == "order-saga"
/// assert len(config.steps) == 2
/// assert config.steps[0].compensation == "inventory.compensate"
/// ```
#[pyclass]
pub struct PySagaBuilder {
    name: String,
    trigger_topic: String,
    trigger_event: String,
    timeout: String,
    steps: Vec<SagaStepConfig>,
}

#[pymethods]
impl PySagaBuilder {
    /// Create a new builder.
    ///
    /// Args:
    ///     name: saga name (must be unique in the pipeline)
    ///     trigger_topic: Kafka topic to listen for the trigger event
    ///     trigger_event: event type that starts the saga
    ///     timeout: overall saga timeout as a duration string (e.g. "30s", "5m")
    #[new]
    fn new(name: String, trigger_topic: String, trigger_event: String, timeout: String) -> Self {
        Self {
            name,
            trigger_topic,
            trigger_event,
            timeout,
            steps: Vec::new(),
        }
    }

    /// Add a step to the saga. Steps execute in the order added.
    ///
    /// Args:
    ///     name: step name
    ///     command_topic: Kafka topic to send the command on
    ///     reply_topic: Kafka topic to receive the reply from
    ///
    /// Returns self for chaining.
    fn step(
        mut slf: PyRefMut<'_, Self>,
        name: String,
        command_topic: String,
        reply_topic: String,
    ) -> Py<Self> {
        slf.steps.push(SagaStepConfig {
            name,
            command_topic,
            reply_topic,
            compensation: None,
            timeout: None,
        });
        slf.into()
    }

    /// Set the compensation topic for the most recently added step.
    ///
    /// Must be called immediately after `step()`.
    fn with_compensation(mut slf: PyRefMut<'_, Self>, compensation: String) -> Py<Self> {
        if let Some(last) = slf.steps.last_mut() {
            last.compensation = Some(compensation);
        }
        slf.into()
    }

    /// Set a per-step timeout for the most recently added step.
    ///
    /// Must be called immediately after `step()`.
    fn with_step_timeout(mut slf: PyRefMut<'_, Self>, timeout: String) -> Py<Self> {
        if let Some(last) = slf.steps.last_mut() {
            last.timeout = Some(timeout);
        }
        slf.into()
    }

    /// Consume the builder and return the completed SagaConfig.
    fn build(slf: PyRef<'_, Self>) -> PySagaConfig {
        PySagaConfig {
            name: slf.name.clone(),
            trigger_topic: slf.trigger_topic.clone(),
            trigger_event: slf.trigger_event.clone(),
            timeout: slf.timeout.clone(),
            steps: slf
                .steps
                .iter()
                .map(|s| PySagaStepConfig {
                    name: s.name.clone(),
                    command_topic: s.command_topic.clone(),
                    reply_topic: s.reply_topic.clone(),
                    compensation: s.compensation.clone(),
                    timeout: s.timeout.clone(),
                })
                .collect(),
        }
    }
}
