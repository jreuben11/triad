"""Tests for PyFlagEvaluator (in-memory mode only — no real DB needed)."""
import pytest
from triad import FlagEvaluator


@pytest.mark.asyncio
async def test_flag_evaluator_enabled():
    ev = FlagEvaluator.from_flags({"my-feature": True})
    assert await ev.is_enabled("my-feature") is True


@pytest.mark.asyncio
async def test_flag_evaluator_disabled():
    ev = FlagEvaluator.from_flags({"my-feature": False})
    assert await ev.is_enabled("my-feature") is False


@pytest.mark.asyncio
async def test_flag_evaluator_missing_flag():
    ev = FlagEvaluator.from_flags({})
    assert await ev.is_enabled("nonexistent") is False


@pytest.mark.asyncio
async def test_flag_evaluator_multiple_flags():
    ev = FlagEvaluator.from_flags({
        "feature-a": True,
        "feature-b": False,
        "feature-c": True,
    })
    assert await ev.is_enabled("feature-a") is True
    assert await ev.is_enabled("feature-b") is False
    assert await ev.is_enabled("feature-c") is True


def test_flag_evaluator_sync_creation():
    """FlagEvaluator.from_flags should be a sync method."""
    ev = FlagEvaluator.from_flags({"x": True})
    assert ev is not None
