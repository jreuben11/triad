"""Tests for PyIdempotencyKey, PyIdempotencyRecord, and PyIdempotencyStore."""
from triad import (
    IdempotencyKey,
    IdempotencyRecord,
    IdempotencyStore,
    lookup,
    store_result,
)

# ── IdempotencyKey ─────────────────────────────────────────────────────────────

def test_idempotency_key_generate_with_scope():
    key = IdempotencyKey.generate("payments")
    assert str(key).startswith("payments:")


def test_idempotency_key_generate_empty_scope():
    key = IdempotencyKey.generate("")
    s = str(key)
    # UUID format: 8-4-4-4-12
    assert len(s) == 36
    assert s.count("-") == 4


def test_idempotency_key_wrap():
    key = IdempotencyKey.wrap("my-request-id")
    assert str(key) == "my-request-id"


def test_idempotency_key_equality():
    a = IdempotencyKey.wrap("same-key")
    b = IdempotencyKey.wrap("same-key")
    assert a == b


def test_idempotency_key_inequality():
    a = IdempotencyKey.wrap("key-a")
    b = IdempotencyKey.wrap("key-b")
    assert a != b


def test_idempotency_keys_unique():
    a = IdempotencyKey.generate("scope")
    b = IdempotencyKey.generate("scope")
    assert str(a) != str(b)


def test_idempotency_key_repr():
    key = IdempotencyKey.wrap("test-key")
    r = repr(key)
    assert "test-key" in r


def test_idempotency_key_hash():
    a = IdempotencyKey.wrap("k")
    b = IdempotencyKey.wrap("k")
    assert hash(a) == hash(b)


# ── IdempotencyRecord ──────────────────────────────────────────────────────────

def test_idempotency_record_new():
    key = IdempotencyKey.wrap("test-key")
    record = IdempotencyRecord(key, 200, {"id": 42})
    assert record.key == "test-key"
    assert record.status_code == 200
    assert '"id": 42' in record.body or '"id":42' in record.body


def test_idempotency_record_repr():
    key = IdempotencyKey.wrap("test-key")
    record = IdempotencyRecord(key, 201, {})
    r = repr(record)
    assert "201" in r


# ── IdempotencyStore ───────────────────────────────────────────────────────────

def test_idempotency_store_get_missing():
    store = IdempotencyStore()
    result = store.get("nonexistent")
    assert result is None


def test_idempotency_store_set_nx_new_key():
    store = IdempotencyStore()
    stored = store.set_nx("new-key", '{"ok": true}', 3600)
    assert stored is True


def test_idempotency_store_set_nx_existing_key():
    store = IdempotencyStore()
    store.set_nx("key", '{"v": 1}', 3600)
    stored = store.set_nx("key", '{"v": 2}', 3600)
    assert stored is False


def test_idempotency_store_get_after_set():
    store = IdempotencyStore()
    store.set_nx("key", '{"result": "ok"}', 3600)
    body = store.get("key")
    assert body == '{"result": "ok"}'


def test_idempotency_store_len():
    store = IdempotencyStore()
    assert len(store) == 0
    store.set_nx("k1", "{}", 3600)
    store.set_nx("k2", "{}", 3600)
    assert len(store) == 2


# ── Module-level functions ─────────────────────────────────────────────────────

def test_lookup_function():
    store = IdempotencyStore()
    assert lookup(store, "missing") is None


def test_store_result_function():
    store = IdempotencyStore()
    stored = store_result(store, "key", '{"x": 1}')
    assert stored is True
    stored2 = store_result(store, "key", '{"x": 2}')
    assert stored2 is False
