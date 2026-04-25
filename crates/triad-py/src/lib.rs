use pyo3::prelude::*;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

mod aggregate;
mod flags;
mod idempotency;
mod instance;
mod outbox;
mod saga;

use aggregate::PyAggregateRoot;
use flags::PyFlagEvaluator;
use idempotency::{PyIdempotencyKey, PyIdempotencyRecord, PyIdempotencyStore};
use instance::{PyTransaction, PyTransactionCM, PyTriadInstance};
use outbox::PyOutboxPublisher;
use saga::{PySagaBuilder, PySagaConfig, PySagaStepConfig};

/// Global Tokio runtime for driving async Rust operations from Python's asyncio loop.
///
/// Python's asyncio loop (via `experimental-async`) polls futures. We bridge tokio
/// by spawning tasks on this runtime and using oneshot channels to shuttle results
/// back to the asyncio-polled future.
static RT: OnceLock<Runtime> = OnceLock::new();

pub(crate) fn get_rt() -> &'static Runtime {
    RT.get().expect(
        "tokio runtime not initialized — the triad._triad module must be imported before use",
    )
}

#[pymodule]
fn _triad(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Initialize the tokio runtime once when the module is first imported.
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("failed to create tokio runtime for triad-py")
    });

    // Instance + Transaction
    m.add_class::<PyTriadInstance>()?;
    m.add_class::<PyTransactionCM>()?;
    m.add_class::<PyTransaction>()?;

    // Outbox
    m.add_class::<PyOutboxPublisher>()?;

    // Feature flags
    m.add_class::<PyFlagEvaluator>()?;

    // Saga
    m.add_class::<PySagaBuilder>()?;
    m.add_class::<PySagaConfig>()?;
    m.add_class::<PySagaStepConfig>()?;

    // Idempotency
    m.add_class::<PyIdempotencyKey>()?;
    m.add_class::<PyIdempotencyRecord>()?;
    m.add_class::<PyIdempotencyStore>()?;
    m.add_function(wrap_pyfunction!(idempotency::py_lookup, m)?)?;
    m.add_function(wrap_pyfunction!(idempotency::py_store_result, m)?)?;

    // Aggregate
    m.add_class::<PyAggregateRoot>()?;

    // Package metadata
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    Ok(())
}
