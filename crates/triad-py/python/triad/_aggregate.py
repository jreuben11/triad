"""Abstract base class for event-sourced aggregates.

Implement this class for your domain aggregate and use it with AggregateRoot.

Example::

    from triad import Aggregate, AggregateRoot

    class Counter(Aggregate):
        @classmethod
        def aggregate_type(cls) -> str:
            return "counter"

        def __init__(self):
            self.count = 0
            self.version = 0

        def apply(self, event: dict) -> None:
            if event["type"] == "Incremented":
                self.count += event["by"]
            elif event["type"] == "Reset":
                self.count = 0

    root = AggregateRoot(Counter, "c1")
    root.apply_new({"type": "Incremented", "by": 5})
    events = root.take_pending_events()
"""

from abc import ABC, abstractmethod
from typing import Any


class Aggregate(ABC):
    """Abstract base for event-sourced domain aggregates.

    Subclass this and implement:
    - ``aggregate_type()``: a stable string identifier for storage
    - ``apply(event: dict)``: apply one event to mutate state
    """

    version: int = 0

    @classmethod
    @abstractmethod
    def aggregate_type(cls) -> str:
        """Return the stable string identifier for this aggregate type."""
        ...

    @abstractmethod
    def apply(self, event: dict[str, Any]) -> None:
        """Apply a single event, mutating internal state."""
        ...
