"""Tests for PyAggregateRoot and Aggregate ABC."""
import pytest

from triad import Aggregate, AggregateRoot

# ── Test aggregate implementation ──────────────────────────────────────────────

class Counter(Aggregate):
    @classmethod
    def aggregate_type(cls) -> str:
        return "counter"

    def __init__(self):
        self.count = 0
        self.version = 0

    def apply(self, event: dict) -> None:
        if event.get("type") == "Incremented":
            self.count += event.get("by", 0)
        elif event.get("type") == "Decremented":
            self.count -= event.get("by", 0)
        elif event.get("type") == "Reset":
            self.count = 0


# ── Aggregate ABC ──────────────────────────────────────────────────────────────

def test_aggregate_abc_is_abstract():
    with pytest.raises(TypeError):
        Aggregate()  # Cannot instantiate abstract class


def test_aggregate_type_method():
    assert Counter.aggregate_type() == "counter"


# ── AggregateRoot ──────────────────────────────────────────────────────────────

def test_aggregate_root_new():
    root = AggregateRoot(Counter, "c1")
    assert root.id == "c1"
    assert root.version == 0
    assert root.pending_count() == 0


def test_aggregate_root_apply_new():
    root = AggregateRoot(Counter, "c1")
    root.apply_new({"type": "Incremented", "by": 5})
    assert root.version == 1
    assert root.pending_count() == 1
    assert root.state.count == 5


def test_aggregate_root_take_pending_events():
    root = AggregateRoot(Counter, "c1")
    root.apply_new({"type": "Incremented", "by": 3})
    root.apply_new({"type": "Incremented", "by": 7})
    events = root.take_pending_events()
    assert len(events) == 2
    assert events[0]["type"] == "Incremented"
    assert events[0]["by"] == 3
    # After taking, queue is empty
    assert root.pending_count() == 0
    assert root.take_pending_events() == []


def test_aggregate_root_apply_multiple():
    root = AggregateRoot(Counter, "c1")
    root.apply_new({"type": "Incremented", "by": 10})
    root.apply_new({"type": "Decremented", "by": 3})
    root.apply_new({"type": "Reset"})
    assert root.state.count == 0
    assert root.version == 3


def test_aggregate_root_rehydrate():
    events = [
        {"type": "Incremented", "by": 10},
        {"type": "Incremented", "by": 5},
        {"type": "Decremented", "by": 2},
    ]
    root = AggregateRoot.rehydrate(Counter, "c1", events)
    assert root.state.count == 13
    assert root.version == 3
    # Rehydrated events are NOT pending
    assert root.pending_count() == 0
    assert root.take_pending_events() == []


def test_aggregate_root_rehydrate_empty():
    root = AggregateRoot.rehydrate(Counter, "c1", [])
    assert root.state.count == 0
    assert root.version == 0


def test_aggregate_root_repr():
    root = AggregateRoot(Counter, "c1")
    r = repr(root)
    assert "c1" in r


def test_aggregate_root_state_getter():
    root = AggregateRoot(Counter, "c1")
    root.apply_new({"type": "Incremented", "by": 42})
    assert isinstance(root.state, Counter)
    assert root.state.count == 42
