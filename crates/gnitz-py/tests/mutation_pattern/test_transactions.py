"""Atomic write-batch transactions (FLAG_PUSH_TXN) through the binary binding.

Covers the client-facing `with client.transaction()` context manager: atomic
multi-table commit, rollback on exception, deferred FK / unique-secondary
semantics, cumulative Error-mode PK existence, and shape rejections.

`test_sql_transactions.py` drives the same core buffer through SQL. What is
tested here and not there is the raw surface only this binding exposes: the
per-frame family mode (`"error"`) and the frame ordering within a bundle.
"""

import pytest
import gnitz

_KV_COLS = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]


def _kv_table(client, sn, name):
    """(pk U64 PK, val I64) table. Returns (tid, schema)."""
    return client.create_table(sn, name, _KV_COLS), gnitz.Schema(_KV_COLS)


def _batch(schema, rows):
    b = gnitz.ZSetBatch(schema)
    for pk, val in rows:
        b.append(pk=pk, val=val)
    return b


def _scan(client, tid):
    """Sorted `(pk, val)`, asserting every base row is at the clamped weight 1 —
    so a bundle applied twice fails here rather than reading as correct."""
    rows = list(client.scan(tid))
    assert all(r.weight == 1 for r in rows), f"unclamped weight in {tid}"
    return sorted((r.pk, r.val) for r in rows)


# ── core: atomic multi-table commit, rollback, empty ──────────────────────────


def test_two_table_atomic_commit(client, schema_name):
    """Both tables' rows are visible after a committed transaction."""
    a_tid, a_sch = _kv_table(client, schema_name, "a")
    b_tid, b_sch = _kv_table(client, schema_name, "b")
    with client.transaction() as txn:
        txn.push(a_tid, _batch(a_sch, [(1, 10), (2, 20)]))
        txn.push(b_tid, _batch(b_sch, [(5, 50)]))
    assert _scan(client, a_tid) == [(1, 10), (2, 20)]
    assert _scan(client, b_tid) == [(5, 50)]


def test_transaction_rollback_on_exception(client, schema_name):
    """An exception inside the with-block discards the whole bundle."""
    a_tid, a_sch = _kv_table(client, schema_name, "a")
    b_tid, b_sch = _kv_table(client, schema_name, "b")
    with pytest.raises(RuntimeError):
        with client.transaction() as txn:
            txn.push(a_tid, _batch(a_sch, [(1, 10)]))
            txn.push(b_tid, _batch(b_sch, [(2, 20)]))
            raise RuntimeError("boom")
    assert _scan(client, a_tid) == []
    assert _scan(client, b_tid) == []


def test_transaction_delete_and_insert(client, schema_name):
    """A txn can mix delete + insert across tables atomically."""
    a_tid, a_sch = _kv_table(client, schema_name, "a")
    client.push(a_tid, _batch(a_sch, [(1, 10), (2, 20)]))
    with client.transaction() as txn:
        txn.delete(a_tid, a_sch, [1])
        txn.push(a_tid, _batch(a_sch, [(3, 30)]))
    assert _scan(client, a_tid) == [(2, 20), (3, 30)]


def test_empty_transaction_noop(client, schema_name):
    """Committing an empty buffer is a no-op that raises nothing."""
    a_tid, _ = _kv_table(client, schema_name, "a")
    with client.transaction():
        pass
    assert _scan(client, a_tid) == []


# ── uniqueness — PK (Error mode, cumulative in frame order) ───────────────────


def test_txn_error_mode_duplicate_pk_aborts_whole_bundle(client, schema_name):
    """An Error-mode insert of a committed PK aborts the whole txn; the
    sibling table is left unmodified."""
    a_tid, a_sch = _kv_table(client, schema_name, "a")
    b_tid, b_sch = _kv_table(client, schema_name, "b")
    client.push(a_tid, _batch(a_sch, [(1, 10)]))
    with pytest.raises(gnitz.GnitzError):
        with client.transaction() as txn:
            txn.push(b_tid, _batch(b_sch, [(9, 90)]))
            txn.push(a_tid, _batch(a_sch, [(1, 99)]), "error")
    assert _scan(client, a_tid) == [(1, 10)]
    assert _scan(client, b_tid) == []


def test_txn_replace_idiom_delete_then_error_insert(client, schema_name):
    """Delete a committed key (Update family) then Error-insert it (Error
    family) in one txn: the replace idiom passes."""
    a_tid, a_sch = _kv_table(client, schema_name, "a")
    client.push(a_tid, _batch(a_sch, [(1, 10)]))
    with client.transaction() as txn:
        txn.delete(a_tid, a_sch, [1])
        txn.push(a_tid, _batch(a_sch, [(1, 20)]), "error")
    assert _scan(client, a_tid) == [(1, 20)]


def test_txn_pk_error_update_insert_then_error_insert_rejects(client, schema_name):
    """(Update-insert k, Error-insert k) frame order rejects — the Error family
    sees k inserted by the prefix (order-sensitive U-PK)."""
    a_tid, a_sch = _kv_table(client, schema_name, "a")
    with pytest.raises(gnitz.GnitzError):
        with client.transaction() as txn:
            txn.push(a_tid, _batch(a_sch, [(1, 10)]))
            txn.push(a_tid, _batch(a_sch, [(1, 20)]), "error")
    assert _scan(client, a_tid) == []


def test_txn_pk_blind_delete_then_error_insert_uncommitted_passes(client, schema_name):
    """Delete-then-Error-insert of an uncommitted key passes (the prefix marks
    it Deleted, so it does not 'exist')."""
    a_tid, a_sch = _kv_table(client, schema_name, "a")
    with client.transaction() as txn:
        txn.delete(a_tid, a_sch, [5])
        txn.push(a_tid, _batch(a_sch, [(5, 50)]), "error")
    assert _scan(client, a_tid) == [(5, 50)]


# ── foreign keys (deferred, post-transaction) ─────────────────────────────────


@pytest.fixture
def fk(client, schema_name):
    """`parent(id)` and `child(id, pid REFERENCES parent(id))`; yields
    `(p_tid, p_sch, c_tid, c_sch)`."""
    client.execute_sql("CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)",
                       schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE child (id BIGINT NOT NULL PRIMARY KEY, "
        "pid BIGINT NOT NULL REFERENCES parent(id))",
        schema_name=schema_name)
    p_tid, p_sch = client.resolve_table(schema_name, "parent")
    c_tid, c_sch = client.resolve_table(schema_name, "child")
    return p_tid, p_sch, c_tid, c_sch


def _p_batch(schema, ids):
    b = gnitz.ZSetBatch(schema)
    for i in ids:
        b.append(id=i)
    return b


def _c_batch(schema, rows):
    b = gnitz.ZSetBatch(schema)
    for cid, pid in rows:
        b.append(id=cid, pid=pid)
    return b


@pytest.mark.parametrize("child_first", [False, True], ids=["parent-first", "child-first"])
def test_txn_fk_parent_child_insert_both_orders(client, fk, child_first):
    """parent+child inserted in one txn passes regardless of frame order — the
    FK is a post-transaction survivor check, not an in-order one."""
    p_tid, p_sch, c_tid, c_sch = fk
    with client.transaction() as txn:
        if child_first:
            txn.push(c_tid, _c_batch(c_sch, [(1, 100)]))
            txn.push(p_tid, _p_batch(p_sch, [100]))
        else:
            txn.push(p_tid, _p_batch(p_sch, [100]))
            txn.push(c_tid, _c_batch(c_sch, [(1, 100)]))
    assert sorted(r.id for r in client.scan(p_tid)) == [100]
    assert sorted((r.id, r.pid) for r in client.scan(c_tid)) == [(1, 100)]


def test_txn_fk_child_referencing_absent_parent_fails(client, fk):
    """A child insert referencing a parent not present (committed or bundled)
    fails the whole txn."""
    _p_tid, _p_sch, c_tid, c_sch = fk
    with pytest.raises(gnitz.GnitzError):
        with client.transaction() as txn:
            txn.push(c_tid, _c_batch(c_sch, [(1, 999)]))
    assert sorted(r.id for r in client.scan(c_tid)) == []


def test_txn_fk_restrict_parent_delete_with_committed_child_fails(client, fk):
    """Deleting a parent whose committed child still references it fails (no
    child family in the bundle)."""
    p_tid, p_sch, c_tid, c_sch = fk
    client.push(p_tid, _p_batch(p_sch, [100]))
    client.push(c_tid, _c_batch(c_sch, [(1, 100)]))
    with pytest.raises(gnitz.GnitzError):
        with client.transaction() as txn:
            txn.delete(p_tid, p_sch, [100])
    assert sorted(r.id for r in client.scan(p_tid)) == [100]


def test_txn_fk_restrict_parent_and_child_delete_passes(client, fk):
    """Deleting a parent AND its child together in one txn passes."""
    p_tid, p_sch, c_tid, c_sch = fk
    client.push(p_tid, _p_batch(p_sch, [100]))
    client.push(c_tid, _c_batch(c_sch, [(1, 100)]))
    with client.transaction() as txn:
        txn.delete(c_tid, c_sch, [1])
        txn.delete(p_tid, p_sch, [100])
    assert sorted(r.id for r in client.scan(p_tid)) == []
    assert sorted(r.id for r in client.scan(c_tid)) == []


def test_txn_fk_restrict_parent_delete_partial_child_delete_fails(client, fk):
    """Parent-delete + only SOME of its children deleted fails (a committed
    child still references it)."""
    p_tid, p_sch, c_tid, c_sch = fk
    client.push(p_tid, _p_batch(p_sch, [100]))
    client.push(c_tid, _c_batch(c_sch, [(1, 100), (2, 100)]))
    with pytest.raises(gnitz.GnitzError):
        with client.transaction() as txn:
            txn.delete(c_tid, c_sch, [1])  # only child 1, child 2 still refs 100
            txn.delete(p_tid, p_sch, [100])
    assert sorted(r.id for r in client.scan(p_tid)) == [100]


def test_txn_fk_parent_delete_reinsert_plus_child_passes(client, fk):
    """Parent delete-then-reinsert + a new child referencing it passes."""
    p_tid, p_sch, c_tid, c_sch = fk
    client.push(p_tid, _p_batch(p_sch, [100]))
    with client.transaction() as txn:
        txn.delete(p_tid, p_sch, [100])
        txn.push(p_tid, _p_batch(p_sch, [100]))
        txn.push(c_tid, _c_batch(c_sch, [(1, 100)]))
    assert sorted(r.id for r in client.scan(p_tid)) == [100]
    assert sorted((r.id, r.pid) for r in client.scan(c_tid)) == [(1, 100)]


def test_txn_fk_parent_delete_with_child_repoint_passes(client, fk):
    """Parent-delete + Update-mode re-pointing of its committed child to another
    parent passes (the child no longer references the deleted parent)."""
    p_tid, p_sch, c_tid, c_sch = fk
    client.push(p_tid, _p_batch(p_sch, [100, 200]))
    client.push(c_tid, _c_batch(c_sch, [(1, 100)]))
    with client.transaction() as txn:
        txn.delete(p_tid, p_sch, [100])
        txn.push(c_tid, _c_batch(c_sch, [(1, 200)]))  # re-point child 1 → 200
    assert sorted(r.id for r in client.scan(p_tid)) == [200]
    assert sorted((r.id, r.pid) for r in client.scan(c_tid)) == [(1, 200)]


# ── unique secondary index (deferred, order-free) ─────────────────────────────


@pytest.fixture
def uniq(client, schema_name):
    """`t(id BIGINT PK, u BIGINT UNIQUE)`; yields `(tid, schema)`."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, u BIGINT NOT NULL UNIQUE)",
        schema_name=schema_name)
    return client.resolve_table(schema_name, "t")


def _u_batch(schema, rows):
    b = gnitz.ZSetBatch(schema)
    for i, u in rows:
        b.append(id=i, u=u)
    return b


def test_txn_unique_secondary_in_bundle_duplicate_rejected(client, uniq):
    """Two families inserting the same unique value on different PKs is a
    genuine in-bundle duplicate (two live holders) — rejected."""
    tid, sch = uniq
    with pytest.raises(gnitz.GnitzError):
        with client.transaction() as txn:
            txn.push(tid, _u_batch(sch, [(1, 5)]))
            txn.push(tid, _u_batch(sch, [(2, 5)]), "error")
    assert sorted(r.id for r in client.scan(tid)) == []


def test_txn_unique_secondary_retire_then_take_passes(client, uniq):
    """A unique value retired from its committed holder by one family and
    taken by another passes (deferred single-holder post-state)."""
    tid, sch = uniq
    client.push(tid, _u_batch(sch, [(1, 5)]))
    with client.transaction() as txn:
        txn.delete(tid, sch, [1])
        txn.push(tid, _u_batch(sch, [(2, 5)]))
    assert sorted((r.id, r.u) for r in client.scan(tid)) == [(2, 5)]


def test_txn_unique_secondary_value_shift_passes(client, uniq):
    """A value shift across bundle-created rows — insert (1,5),(2,6) then upsert
    (2,5),(1,7) — folds to the single-holder post-state {1→7, 2→5}, which the
    raw-row `seen` set would wrongly reject."""
    tid, sch = uniq
    with client.transaction() as txn:
        txn.push(tid, _u_batch(sch, [(1, 5), (2, 6)]))
        txn.push(tid, _u_batch(sch, [(2, 5), (1, 7)]))
    assert sorted((r.id, r.u) for r in client.scan(tid)) == [(1, 7), (2, 5)]


def test_txn_unique_secondary_fold_valid_intermediate_collision_passes(client, uniq):
    """`[I(a,v), I(a,w), I(b,v)]` folds to `{a→w, b→v}` (single-holder) and
    passes, though `a`'s discarded intermediate `v` collides raw-row with `b`."""
    tid, sch = uniq
    with client.transaction() as txn:
        txn.push(tid, _u_batch(sch, [(1, 5)]))
        txn.push(tid, _u_batch(sch, [(1, 6)]))
        txn.push(tid, _u_batch(sch, [(2, 5)]))
    assert sorted((r.id, r.u) for r in client.scan(tid)) == [(1, 6), (2, 5)]


# ── shape rejections ─────────────────────────────────────────────────────────


def test_txn_system_table_rejected(client, schema_name):
    """A system-table tid (below FIRST_USER_TABLE_ID) is rejected."""
    _, a_sch = _kv_table(client, schema_name, "a")
    with pytest.raises(gnitz.GnitzError):
        with client.transaction() as txn:
            txn.push(gnitz.TABLE_TAB, _batch(a_sch, [(1, 10)]))
