"""Tests for PyOutboxPublisher (sync construction only — no real DB needed)."""
import sys
sys.path.insert(0, "crates/triad-py/python")

import pytest
from triad import OutboxPublisher


def test_outbox_publisher_creation():
    publisher = OutboxPublisher(table="triad_outbox", kafka_topic="orders.events")
    assert publisher is not None


def test_outbox_publisher_with_name():
    publisher = OutboxPublisher(
        table="triad_outbox",
        kafka_topic="orders.events",
        name="my-publisher",
    )
    assert publisher is not None


def test_outbox_publisher_is_importable():
    from triad import OutboxPublisher as OP
    assert OP is not None
