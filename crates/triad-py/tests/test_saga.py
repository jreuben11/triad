"""Tests for PySagaBuilder and related classes."""
import pytest
from triad import SagaBuilder, SagaConfig, SagaStepConfig


def test_saga_builder_no_steps():
    config = SagaBuilder("order-saga", "orders.commands", "OrderCreate", "30s").build()
    assert config.name == "order-saga"
    assert config.trigger_topic == "orders.commands"
    assert config.trigger_event == "OrderCreate"
    assert config.timeout == "30s"
    assert len(config.steps) == 0


def test_saga_builder_with_steps():
    config = (
        SagaBuilder("saga", "topic", "event", "5m")
        .step("step1", "cmd1", "reply1")
        .step("step2", "cmd2", "reply2")
        .build()
    )
    assert len(config.steps) == 2
    assert config.steps[0].name == "step1"
    assert config.steps[1].name == "step2"


def test_saga_builder_with_compensation():
    config = (
        SagaBuilder("saga", "topic", "event", "5m")
        .step("step1", "cmd1", "reply1")
        .with_compensation("compensate1")
        .step("step2", "cmd2", "reply2")
        .build()
    )
    assert config.steps[0].compensation == "compensate1"
    assert config.steps[1].compensation is None


def test_saga_builder_with_step_timeout():
    config = (
        SagaBuilder("saga", "topic", "event", "5m")
        .step("step1", "cmd1", "reply1")
        .with_step_timeout("10s")
        .build()
    )
    assert config.steps[0].timeout == "10s"


def test_saga_builder_full_chain():
    config = (
        SagaBuilder("order-saga", "orders.commands", "OrderCreate", "30s")
        .step("reserve-inventory", "inventory.commands", "inventory.replies")
        .with_compensation("inventory.compensate")
        .step("charge-payment", "payments.commands", "payments.replies")
        .with_compensation("payments.compensate")
        .build()
    )
    assert config.name == "order-saga"
    assert len(config.steps) == 2
    assert config.steps[0].name == "reserve-inventory"
    assert config.steps[0].compensation == "inventory.compensate"
    assert config.steps[1].name == "charge-payment"
    assert config.steps[1].compensation == "payments.compensate"


def test_saga_config_repr():
    config = SagaBuilder("my-saga", "t", "e", "30s").build()
    r = repr(config)
    assert "my-saga" in r


def test_saga_step_repr():
    config = (
        SagaBuilder("s", "t", "e", "30s").step("step1", "cmd", "reply").build()
    )
    r = repr(config.steps[0])
    assert "step1" in r
