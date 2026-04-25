use pyo3::prelude::*;
use pyo3::types::PyAnyMethods;

/// Python-facing aggregate root that works with Python aggregate objects.
///
/// Unlike the Rust `AggregateRoot<A>` which is generic over `Aggregate`,
/// this wraps a Python object implementing the `Aggregate` ABC and works
/// with Python dicts for events.
///
/// Usage:
///
/// ```python
/// from triad import AggregateRoot, Aggregate
///
/// class Counter(Aggregate):
///     @classmethod
///     def aggregate_type(cls): return "counter"
///
///     def __init__(self):
///         self.count = 0
///
///     def apply(self, event: dict) -> None:
///         if event["type"] == "Incremented":
///             self.count += event["by"]
///
/// root = AggregateRoot(Counter, "c1")
/// root.apply_new({"type": "Incremented", "by": 5})
/// events = root.take_pending_events()
/// assert len(events) == 1
///
/// # Rehydrate from stored events
/// root2 = AggregateRoot.rehydrate(Counter, "c1", [{"type": "Incremented", "by": 3}])
/// assert root2.state.count == 3
/// ```
#[pyclass]
pub struct PyAggregateRoot {
    /// Aggregate ID string.
    id: String,
    /// The Python aggregate object (implements Aggregate ABC).
    state: Py<PyAny>,
    /// Pending (not-yet-persisted) events as JSON strings.
    pending_events: Vec<String>,
    /// Number of events applied since creation.
    version: i64,
}

#[pymethods]
impl PyAggregateRoot {
    /// Create a new aggregate root in its default (empty) state.
    ///
    /// Args:
    ///     aggregate_cls: the Python class (must implement `Aggregate` ABC)
    ///     aggregate_id: unique ID for this aggregate instance
    #[new]
    fn new(aggregate_cls: &Bound<'_, PyAny>, aggregate_id: String) -> PyResult<Self> {
        let state = aggregate_cls.call0()?;
        Ok(Self {
            id: aggregate_id,
            state: state.unbind(),
            pending_events: Vec::new(),
            version: 0,
        })
    }

    /// Reconstruct aggregate state by replaying stored events.
    ///
    /// Args:
    ///     aggregate_cls: the Python class (must implement `Aggregate` ABC)
    ///     aggregate_id: unique ID for this aggregate instance
    ///     events: iterable of event dicts to replay
    ///
    /// Returns a root with all events applied and no pending events.
    #[staticmethod]
    fn rehydrate(
        aggregate_cls: &Bound<'_, PyAny>,
        aggregate_id: String,
        events: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let state = aggregate_cls.call0()?;
        let mut version: i64 = 0;

        for event in events.try_iter()? {
            let event = event?;
            state.call_method1("apply", (&event,))?;
            version += 1;
        }

        // Update version attribute if aggregate supports it
        if state.hasattr("version")? {
            state.setattr("version", version)?;
        }

        Ok(Self {
            id: aggregate_id,
            state: state.unbind(),
            pending_events: Vec::new(),
            version,
        })
    }

    /// Apply a new (not-yet-persisted) event and stage it for persistence.
    ///
    /// Calls `aggregate.apply(event)` and adds the event to the pending queue.
    fn apply_new(mut slf: PyRefMut<'_, Self>, event: Py<PyAny>) -> PyResult<()> {
        let py = slf.py();

        // Serialise event to JSON for internal storage
        let json_mod = py.import("json")?;
        let event_json: String = json_mod
            .call_method1("dumps", (event.bind(py),))?
            .extract()?;

        // Call aggregate.apply(event)
        slf.state
            .bind(py)
            .call_method1("apply", (event.bind(py),))?;

        slf.version += 1;

        // Update version attribute on the aggregate object if it has one
        let version = slf.version;
        slf.state.bind(py).setattr("version", version).ok();

        slf.pending_events.push(event_json);
        Ok(())
    }

    /// Return the staged events and clear the internal queue.
    ///
    /// Each event is returned as a Python dict.
    fn take_pending_events(mut slf: PyRefMut<'_, Self>) -> PyResult<Vec<Py<PyAny>>> {
        let py = slf.py();
        let json_mod = py.import("json")?;
        let events: Vec<Py<PyAny>> = slf
            .pending_events
            .drain(..)
            .map(|json_str| -> PyResult<Py<PyAny>> {
                let obj = json_mod.call_method1("loads", (json_str.as_str(),))?;
                Ok(obj.unbind())
            })
            .collect::<PyResult<_>>()?;
        Ok(events)
    }

    /// The aggregate ID.
    #[getter]
    fn id(slf: PyRef<'_, Self>) -> String {
        slf.id.clone()
    }

    /// The current state object (the Python aggregate instance).
    #[getter]
    fn state(slf: PyRef<'_, Self>) -> Py<PyAny> {
        slf.state.clone_ref(slf.py())
    }

    /// Number of events applied (including pending).
    #[getter]
    fn version(slf: PyRef<'_, Self>) -> i64 {
        slf.version
    }

    /// Number of pending (unstored) events.
    fn pending_count(slf: PyRef<'_, Self>) -> usize {
        slf.pending_events.len()
    }

    fn __repr__(slf: PyRef<'_, Self>) -> String {
        format!(
            "AggregateRoot(id={:?}, version={}, pending={})",
            slf.id,
            slf.version,
            slf.pending_events.len()
        )
    }
}
