"""E2E tests for atomic write-batch transactions (FLAG_PUSH_TXN).

Covers the client-facing `with client.transaction()` context manager: atomic
multi-table commit, rollback on exception, deferred FK / unique-secondary
semantics, cumulative Error-mode PK existence, and shape rejections.
"""

import random
import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _kv_table(client, sn, name, unique_pk=True):
    """(pk U64 PK, val I64) table. Returns (tid, schema)."""
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols)
    tid = client.create_table(sn, name, cols, unique_pk=unique_pk)
    return tid, schema


def _batch(schema, rows, weight=1):
    b = gnitz.ZSetBatch(schema)
    for pk, val in rows:
        b.append(pk=pk, val=val, weight=weight)
    return b


def _scan(client, tid):
    return sorted((row.pk, row.val) for row in client.scan(tid) if row.weight > 0)


# ---------------------------------------------------------------------------
# Core: atomic multi-table commit, rollback, empty
# ---------------------------------------------------------------------------


def test_two_table_atomic_commit(client):
    """Both tables' rows are visible after a committed transaction."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        a_tid, a_sch = _kv_table(client, sn, "a")
        b_tid, b_sch = _kv_table(client, sn, "b")
        with client.transaction() as txn:
            txn.push(a_tid, _batch(a_sch, [(1, 10), (2, 20)]))
            txn.push(b_tid, _batch(b_sch, [(5, 50)]))
        assert _scan(client, a_tid) == [(1, 10), (2, 20)]
        assert _scan(client, b_tid) == [(5, 50)]
    finally:
        client.drop_schema(sn)


def test_transaction_rollback_on_exception(client):
    """An exception inside the with-block discards the whole bundle."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        a_tid, a_sch = _kv_table(client, sn, "a")
        b_tid, b_sch = _kv_table(client, sn, "b")
        with pytest.raises(RuntimeError):
            with client.transaction() as txn:
                txn.push(a_tid, _batch(a_sch, [(1, 10)]))
                txn.push(b_tid, _batch(b_sch, [(2, 20)]))
                raise RuntimeError("boom")
        assert _scan(client, a_tid) == []
        assert _scan(client, b_tid) == []
    finally:
        client.drop_schema(sn)


def test_transaction_delete_and_insert(client):
    """A txn can mix delete + insert across tables atomically."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        a_tid, a_sch = _kv_table(client, sn, "a")
        client.push(a_tid, _batch(a_sch, [(1, 10), (2, 20)]))
        with client.transaction() as txn:
            txn.delete(a_tid, a_sch, [1])
            txn.push(a_tid, _batch(a_sch, [(3, 30)]))
        assert _scan(client, a_tid) == [(2, 20), (3, 30)]
    finally:
        client.drop_schema(sn)


def test_empty_transaction_noop(client):
    """Committing an empty buffer is a no-op that raises nothing."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        a_tid, a_sch = _kv_table(client, sn, "a")
        with client.transaction():
            pass
        assert _scan(client, a_tid) == []
    finally:
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Uniqueness — PK (Error mode, cumulative in frame order)
# ---------------------------------------------------------------------------


def test_txn_error_mode_duplicate_pk_aborts_whole_bundle(client):
    """An Error-mode insert of a committed PK aborts the whole txn; the
    sibling table is left unmodified."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        a_tid, a_sch = _kv_table(client, sn, "a")
        b_tid, b_sch = _kv_table(client, sn, "b")
        client.push(a_tid, _batch(a_sch, [(1, 10)]))
        with pytest.raises(gnitz.GnitzError):
            with client.transaction() as txn:
                txn.push(b_tid, _batch(b_sch, [(9, 90)]))
                txn.push(a_tid, _batch(a_sch, [(1, 99)]), "error")
        assert _scan(client, a_tid) == [(1, 10)]
        assert _scan(client, b_tid) == []
    finally:
        client.drop_schema(sn)


def test_txn_replace_idiom_delete_then_error_insert(client):
    """Delete a committed key (Update family) then Error-insert it (Error
    family) in one txn: the replace idiom passes."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        a_tid, a_sch = _kv_table(client, sn, "a")
        client.push(a_tid, _batch(a_sch, [(1, 10)]))
        with client.transaction() as txn:
            txn.delete(a_tid, a_sch, [1])
            txn.push(a_tid, _batch(a_sch, [(1, 20)]), "error")
        assert _scan(client, a_tid) == [(1, 20)]
    finally:
        client.drop_schema(sn)


def test_txn_pk_error_update_insert_then_error_insert_rejects(client):
    """(Update-insert k, Error-insert k) frame order rejects — the Error family
    sees k inserted by the prefix (order-sensitive U-PK)."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        a_tid, a_sch = _kv_table(client, sn, "a")
        with pytest.raises(gnitz.GnitzError):
            with client.transaction() as txn:
                txn.push(a_tid, _batch(a_sch, [(1, 10)]))
                txn.push(a_tid, _batch(a_sch, [(1, 20)]), "error")
        assert _scan(client, a_tid) == []
    finally:
        client.drop_schema(sn)


def test_txn_pk_blind_delete_then_error_insert_uncommitted_passes(client):
    """Delete-then-Error-insert of an uncommitted key passes (the prefix marks
    it Deleted, so it does not 'exist')."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        a_tid, a_sch = _kv_table(client, sn, "a")
        with client.transaction() as txn:
            txn.delete(a_tid, a_sch, [5])
            txn.push(a_tid, _batch(a_sch, [(5, 50)]), "error")
        assert _scan(client, a_tid) == [(5, 50)]
    finally:
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Foreign keys (deferred, post-transaction)
# ---------------------------------------------------------------------------


def _fk_tables(client, sn):
    """parent(id BIGINT PK), child(id BIGINT PK, pid BIGINT REFERENCES parent(id))."""
    client.execute_sql("CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE child (id BIGINT NOT NULL PRIMARY KEY, pid BIGINT NOT NULL REFERENCES parent(id))",
        schema_name=sn,
    )
    p_tid, p_sch = client.resolve_table(sn, "parent")
    c_tid, c_sch = client.resolve_table(sn, "child")
    return p_tid, p_sch, c_tid, c_sch


def _p_batch(schema, ids, weight=1):
    b = gnitz.ZSetBatch(schema)
    for i in ids:
        b.append(id=i, weight=weight)
    return b


def _c_batch(schema, rows, weight=1):
    b = gnitz.ZSetBatch(schema)
    for cid, pid in rows:
        b.append(id=cid, pid=pid, weight=weight)
    return b


def test_txn_fk_parent_child_insert_both_orders(client):
    """parent+child inserted in one txn passes regardless of frame order."""
    for child_first in (False, True):
        sn = "x" + _uid()
        client.create_schema(sn)
        try:
            p_tid, p_sch, c_tid, c_sch = _fk_tables(client, sn)
            with client.transaction() as txn:
                if child_first:
                    txn.push(c_tid, _c_batch(c_sch, [(1, 100)]))
                    txn.push(p_tid, _p_batch(p_sch, [100]))
                else:
                    txn.push(p_tid, _p_batch(p_sch, [100]))
                    txn.push(c_tid, _c_batch(c_sch, [(1, 100)]))
            assert sorted(r.id for r in client.scan(p_tid) if r.weight > 0) == [100]
        finally:
            client.drop_schema(sn)


def test_txn_fk_child_referencing_absent_parent_fails(client):
    """A child insert referencing a parent not present (committed or bundled)
    fails the whole txn."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        p_tid, p_sch, c_tid, c_sch = _fk_tables(client, sn)
        with pytest.raises(gnitz.GnitzError):
            with client.transaction() as txn:
                txn.push(c_tid, _c_batch(c_sch, [(1, 999)]))
        assert sorted(r.id for r in client.scan(c_tid) if r.weight > 0) == []
    finally:
        client.drop_schema(sn)


def test_txn_fk_restrict_parent_delete_with_committed_child_fails(client):
    """Deleting a parent whose committed child still references it fails (no
    child family in the bundle)."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        p_tid, p_sch, c_tid, c_sch = _fk_tables(client, sn)
        client.push(p_tid, _p_batch(p_sch, [100]))
        client.push(c_tid, _c_batch(c_sch, [(1, 100)]))
        with pytest.raises(gnitz.GnitzError):
            with client.transaction() as txn:
                txn.delete(p_tid, p_sch, [100])
        # parent still present
        assert sorted(r.id for r in client.scan(p_tid) if r.weight > 0) == [100]
    finally:
        client.drop_schema(sn)


def test_txn_fk_restrict_parent_and_child_delete_passes(client):
    """Deleting a parent AND its child together in one txn passes."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        p_tid, p_sch, c_tid, c_sch = _fk_tables(client, sn)
        client.push(p_tid, _p_batch(p_sch, [100]))
        client.push(c_tid, _c_batch(c_sch, [(1, 100)]))
        with client.transaction() as txn:
            txn.delete(c_tid, c_sch, [1])
            txn.delete(p_tid, p_sch, [100])
        assert sorted(r.id for r in client.scan(p_tid) if r.weight > 0) == []
        assert sorted(r.id for r in client.scan(c_tid) if r.weight > 0) == []
    finally:
        client.drop_schema(sn)


def test_txn_fk_restrict_parent_delete_partial_child_delete_fails(client):
    """Parent-delete + only SOME of its children deleted fails (a committed
    child still references it)."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        p_tid, p_sch, c_tid, c_sch = _fk_tables(client, sn)
        client.push(p_tid, _p_batch(p_sch, [100]))
        client.push(c_tid, _c_batch(c_sch, [(1, 100), (2, 100)]))
        with pytest.raises(gnitz.GnitzError):
            with client.transaction() as txn:
                txn.delete(c_tid, c_sch, [1])  # only child 1, child 2 still refs 100
                txn.delete(p_tid, p_sch, [100])
        assert sorted(r.id for r in client.scan(p_tid) if r.weight > 0) == [100]
    finally:
        client.drop_schema(sn)


def test_txn_fk_parent_delete_reinsert_plus_child_passes(client):
    """Parent delete-then-reinsert + a new child referencing it passes."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        p_tid, p_sch, c_tid, c_sch = _fk_tables(client, sn)
        client.push(p_tid, _p_batch(p_sch, [100]))
        with client.transaction() as txn:
            txn.delete(p_tid, p_sch, [100])
            txn.push(p_tid, _p_batch(p_sch, [100]))
            txn.push(c_tid, _c_batch(c_sch, [(1, 100)]))
        assert sorted(r.id for r in client.scan(p_tid) if r.weight > 0) == [100]
        assert sorted((r.id, r.pid) for r in client.scan(c_tid) if r.weight > 0) == [(1, 100)]
    finally:
        client.drop_schema(sn)


def test_txn_fk_parent_delete_with_child_repoint_passes(client):
    """Parent-delete + Update-mode re-pointing of its committed child to another
    parent passes (the child no longer references the deleted parent)."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        p_tid, p_sch, c_tid, c_sch = _fk_tables(client, sn)
        client.push(p_tid, _p_batch(p_sch, [100, 200]))
        client.push(c_tid, _c_batch(c_sch, [(1, 100)]))
        with client.transaction() as txn:
            txn.delete(p_tid, p_sch, [100])
            txn.push(c_tid, _c_batch(c_sch, [(1, 200)]))  # re-point child 1 → 200
        assert sorted(r.id for r in client.scan(p_tid) if r.weight > 0) == [200]
        assert sorted((r.id, r.pid) for r in client.scan(c_tid) if r.weight > 0) == [(1, 200)]
    finally:
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Unique secondary index (deferred, order-free)
# ---------------------------------------------------------------------------


def _uniq_table(client, sn):
    """t(id BIGINT PK, u BIGINT UNIQUE)."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, u BIGINT NOT NULL UNIQUE)",
        schema_name=sn,
    )
    tid, sch = client.resolve_table(sn, "t")
    return tid, sch


def _u_batch(schema, rows, weight=1):
    b = gnitz.ZSetBatch(schema)
    for i, u in rows:
        b.append(id=i, u=u, weight=weight)
    return b


def test_txn_unique_secondary_in_bundle_duplicate_rejected(client):
    """Two families inserting the same unique value on different PKs is a
    genuine in-bundle duplicate (two live holders) — rejected."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        tid, sch = _uniq_table(client, sn)
        with pytest.raises(gnitz.GnitzError):
            with client.transaction() as txn:
                txn.push(tid, _u_batch(sch, [(1, 5)]))
                txn.push(tid, _u_batch(sch, [(2, 5)]), "error")
        assert sorted(r.id for r in client.scan(tid) if r.weight > 0) == []
    finally:
        client.drop_schema(sn)


def test_txn_unique_secondary_retire_then_take_passes(client):
    """A unique value retired from its committed holder by one family and
    taken by another passes (deferred single-holder post-state)."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        tid, sch = _uniq_table(client, sn)
        client.push(tid, _u_batch(sch, [(1, 5)]))
        with client.transaction() as txn:
            txn.delete(tid, sch, [1])
            txn.push(tid, _u_batch(sch, [(2, 5)]))
        assert sorted((r.id, r.u) for r in client.scan(tid) if r.weight > 0) == [(2, 5)]
    finally:
        client.drop_schema(sn)


def test_txn_unique_secondary_value_shift_passes(client):
    """A value shift across bundle-created rows — insert (1,5),(2,6) then upsert
    (2,5),(1,7) — folds to the single-holder post-state {1→7, 2→5}, which the
    raw-row `seen` set would wrongly reject."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        tid, sch = _uniq_table(client, sn)
        with client.transaction() as txn:
            txn.push(tid, _u_batch(sch, [(1, 5), (2, 6)]))
            txn.push(tid, _u_batch(sch, [(2, 5), (1, 7)]))
        assert sorted((r.id, r.u) for r in client.scan(tid) if r.weight > 0) == [(1, 7), (2, 5)]
    finally:
        client.drop_schema(sn)


def test_txn_unique_secondary_fold_valid_intermediate_collision_passes(client):
    """`[I(a,v), I(a,w), I(b,v)]` folds to `{a→w, b→v}` (single-holder) and
    passes, though `a`'s discarded intermediate `v` collides raw-row with `b`."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        tid, sch = _uniq_table(client, sn)
        with client.transaction() as txn:
            txn.push(tid, _u_batch(sch, [(1, 5)]))
            txn.push(tid, _u_batch(sch, [(1, 6)]))
            txn.push(tid, _u_batch(sch, [(2, 5)]))
        assert sorted((r.id, r.u) for r in client.scan(tid) if r.weight > 0) == [(1, 6), (2, 5)]
    finally:
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Shape rejections
# ---------------------------------------------------------------------------


def test_txn_non_unique_pk_table_rejected(client):
    """A non-unique_pk table family is rejected."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        tid, sch = _kv_table(client, sn, "a", unique_pk=False)
        with pytest.raises(gnitz.GnitzError):
            with client.transaction() as txn:
                txn.push(tid, _batch(sch, [(1, 10)]))
    finally:
        client.drop_schema(sn)


def test_txn_system_table_rejected(client):
    """A system-table tid (below FIRST_USER_TABLE_ID) is rejected."""
    sn = "x" + _uid()
    client.create_schema(sn)
    try:
        _, a_sch = _kv_table(client, sn, "a")
        with pytest.raises(gnitz.GnitzError):
            with client.transaction() as txn:
                txn.push(gnitz.TABLE_TAB, _batch(a_sch, [(1, 10)]))
    finally:
        client.drop_schema(sn)
