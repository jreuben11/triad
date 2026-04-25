"""Tests for PyTriadInstance (sync parts only — no real DB needed)."""
import sys
sys.path.insert(0, "crates/triad-py/python")

import pytest
from triad import TriadInstance, TransactionCM


def test_triad_instance_is_importable():
    """TriadInstance class is accessible."""
    assert TriadInstance is not None


def test_transaction_cm_is_importable():
    """TransactionCM class is accessible."""
    assert TransactionCM is not None


def test_triad_module_has_version():
    """The module version is set."""
    from triad import __version__
    assert isinstance(__version__, str)
    assert "." in __version__
