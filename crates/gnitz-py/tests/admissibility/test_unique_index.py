"""A UNIQUE index: what `CREATE` refuses over rows already present, and what the
index refuses once it exists.

At create time the master validates uniqueness globally, across every worker,
before it broadcasts the index. It is the only uniqueness check on that path —
the per-worker backfill runs none — so without it every duplicate, within one
partition or across two, would be silently accepted and the index would
thereafter enforce a constraint the table never satisfied.

Once the index exists, the value a row holds is a claim on it, and a batch is
admitted only if every claim it leaves standing is held once. That is what makes
an upsert that vacates a value and a fresh PK that takes it admissible in one
batch, and two survivors claiming one value a rejection — including when the
claim is forged by a retraction the pusher never held.
"""

import pytest
import gnitz
from _serverproc import NEEDS_MULTI

_T = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)"
_T_NULLABLE = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT)"
_RAW_COLS = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
             gnitz.ColumnDef("val", gnitz.TypeCode.I64)]

# `metadata.rs` is the single source of both messages, so pinning them costs
# nothing and stops an unrelated failure satisfying a bare `raises`.
_CREATE_DUP = "contains duplicate values"
_VIOLATION = "[Uu]nique index violation"


def _insert(client, sn, rows, chunk=500):
    """Multi-row INSERT into `t` of same-width value tuples, split into
    at-most-`chunk`-row statements; a literal 'NULL' passes through."""
    for i in range(0, len(rows), chunk):
        values = ", ".join(f"({', '.join(str(v) for v in r)})" for r in rows[i:i + chunk])
        client.execute_sql(f"INSERT INTO t VALUES {values}", schema_name=sn)


def _has_index(client, sn, table="t"):
    """True if any live IdxTab row names `table` as its owner."""
    batch = client.scan(gnitz.IDX_TAB)
    if batch.schema is None:
        return False
    tid, _ = client.resolve_table(sn, table)
    # Hoisted: every `.scalars`/`.weights` read rebuilds the whole list, so
    # reading one inside the row loop is quadratic in the catalog size.
    owners = batch.scalars("owner_id")
    return any(w > 0 and o == tid for w, o in zip(batch.weights, owners))


def _raw_table(client, sn, cols=_RAW_COLS):
    """Raw `t` + a SQL unique index on `val`. Returns `(tid, schema)`."""
    tid = client.create_table(sn, "t", cols)
    client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
    return tid, gnitz.Schema(cols)


def _rows(client, tid):
    return sorted((r.pk, r.val) for r in client.scan(tid))


# ── CREATE over rows already present ─────────────────────────────────────────

def test_preexisting_duplicate_rejected_and_the_cluster_survives(client, schema_name):
    """50 rows share one value across consecutive PKs. Routing is a hash of the
    key, so at any worker count that is both a within-partition repeat (by
    pigeonhole) and a cross-partition one — the duplicate no single worker's
    backfill can see. The rejection must name the table and column, no index may
    exist afterwards, and every worker must still answer.
    """
    client.execute_sql(_T, schema_name=schema_name)
    _insert(client, schema_name, [(pk, 42) for pk in range(1, 51)])

    with pytest.raises(gnitz.GnitzError, match=_CREATE_DUP) as exc:
        client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=schema_name)
    # The pre-flight is the only producer of this message, and naming the
    # qualified table and the column is what tells the author what to change.
    assert f"{schema_name}.t" in str(exc.value), exc.value
    assert "val" in str(exc.value), exc.value

    tid, _ = client.resolve_table(schema_name, "t")
    result = client.scan(tid)
    assert result.schema is not None
    assert len(result.pks) == 50, "all workers must answer the scan"
    assert not _has_index(client, schema_name)
    # No phantom constraint leaked out of the failed DDL: the write path still
    # accepts another duplicate.
    client.execute_sql("INSERT INTO t VALUES (51, 42)", schema_name=schema_name)


def test_one_planted_duplicate_among_many_is_found(client, schema_name):
    """A single duplicate pair among hundreds of distinct values, on far-apart
    PKs, is brought adjacent only by the sorted merge."""
    client.execute_sql(_T, schema_name=schema_name)
    rows = [(pk, pk * 10) for pk in range(1, 301)]
    rows.append((1000, 1500))  # duplicates the val of pk=150
    _insert(client, schema_name, rows)
    with pytest.raises(gnitz.GnitzError, match=_CREATE_DUP):
        client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=schema_name)
    assert not _has_index(client, schema_name)


def test_create_at_scale_passes_then_enforces_then_rejects(client, schema_name):
    """A few thousand distinct values across all workers pass the pre-flight and
    the index enforces immediately (the merge's seed must be complete, never
    truncated). Planting one duplicate and re-creating must then reject, and the
    cluster must stay alive through both verdicts."""
    n = 3000
    client.execute_sql(_T, schema_name=schema_name)
    _insert(client, schema_name, [(pk, pk) for pk in range(1, n + 1)])

    client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=schema_name)
    assert _has_index(client, schema_name)
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        client.execute_sql("INSERT INTO t VALUES (900001, 500)", schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (900002, 0)", schema_name=schema_name)
    client.execute_sql(f"DROP INDEX {schema_name}__t__idx_val", schema_name=schema_name)

    client.execute_sql(f"INSERT INTO t VALUES ({n + 1}, 1)", schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError, match=_CREATE_DUP):
        client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=schema_name)

    tid, _ = client.resolve_table(schema_name, "t")
    result = client.scan(tid)
    assert result.schema is not None
    # n distinct + the fresh value + the planted duplicate; the rejected
    # insert of a held value landed nothing.
    assert len(result.pks) == n + 2, "all workers must answer post-failure"
    assert not _has_index(client, schema_name)


def test_preexisting_nulls_allowed(client, schema_name):
    """SQL UNIQUE permits many NULLs: a NULL key never enters any worker's key
    stream, so a NULL-heavy column passes and stays NULL-insertable, while a
    duplicate non-NULL value is still refused."""
    client.execute_sql(_T_NULLABLE, schema_name=schema_name)
    _insert(client, schema_name,
            [(pk, "NULL") for pk in range(1, 9)] + [(pk, pk) for pk in range(100, 108)])
    client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=schema_name)
    assert _has_index(client, schema_name)
    client.execute_sql("INSERT INTO t VALUES (200, NULL)", schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        client.execute_sql("INSERT INTO t VALUES (201, 100)", schema_name=schema_name)


def test_non_unique_index_on_a_duplicate_column_succeeds(client, schema_name):
    """The pre-flight is gated on is_unique: a NON-unique index over a column of
    duplicates must be created without validation."""
    client.execute_sql(_T, schema_name=schema_name)
    _insert(client, schema_name, [(1, 42), (2, 42)])
    assert client.execute_sql(
        "CREATE INDEX ON t(val)", schema_name=schema_name)[0]["type"] == "IndexCreated"
    assert _has_index(client, schema_name)


def test_empty_table_succeeds_and_enforces(client, schema_name):
    client.execute_sql(_T, schema_name=schema_name)
    client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=schema_name)
    assert _has_index(client, schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        client.execute_sql("INSERT INTO t VALUES (1000000, 42)", schema_name=schema_name)


@pytest.mark.parametrize("rows,ok", [
    ([(1, -5), (2, 300), (3, -5)], False),
    ([(1, -1), (2, -2), (3, -3), (4, 0)], True),
], ids=["equal-negatives-collide", "distinct-negatives-pass"])
def test_signed_values_round_trip_through_the_merge(client, schema_name, rows, ok):
    """The merge's key order is not monotonic in signed value, so what the
    verdict needs is only `equal value => equal key`. After a passing create the
    post-create filter must reject a re-insert of one of them — no sign
    confusion in the native-key round trip."""
    client.execute_sql(_T, schema_name=schema_name)
    _insert(client, schema_name, rows)
    if not ok:
        with pytest.raises(gnitz.GnitzError, match=_CREATE_DUP):
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=schema_name)
        assert not _has_index(client, schema_name)
        return
    client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        client.execute_sql("INSERT INTO t VALUES (10, -2)", schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (11, -9)", schema_name=schema_name)


def test_pk_column_unique_index_short_circuits(client, schema_name):
    """A unique index on the sole PK column is trivially satisfiable (PK
    uniqueness is enforced on every ingest path), so the create succeeds without
    scanning — including when a pushed weight >= 2 has collapsed to one live
    instance, which is the short-circuit's premise."""
    tid = client.create_table(schema_name, "t", _RAW_COLS)
    schema = gnitz.Schema(_RAW_COLS)
    b = gnitz.ZSetBatch(schema)
    b.append(pk=1, val=10, _weight=2)
    client.push(tid, b)
    assert [(r.pk, r.weight) for r in client.scan(tid)] == [(1, 1)]
    b = gnitz.ZSetBatch(schema)
    b.append(pk=1, val=10, _weight=-1)
    client.push(tid, b)
    assert list(client.scan(tid)) == []

    client.execute_sql("CREATE UNIQUE INDEX ON t(pk)", schema_name=schema_name)
    assert _has_index(client, schema_name)


@pytest.mark.parametrize("rows,ok", [
    ("(1, 1, 10), (1, 2, 20), (3, 1, 30)", False),   # `a` repeats across rows
    ("(1, 1, 10), (2, 2, 20), (3, 1, 30)", True),
], ids=["duplicate-member", "distinct-member"])
def test_compound_pk_member_is_not_trivially_unique(client, schema_name, rows, ok):
    """A member of a compound PK is not the PK, so it takes the full-path scan."""
    client.execute_sql(
        "CREATE TABLE t (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL,"
        " payload BIGINT, PRIMARY KEY (a, b))", schema_name=schema_name)
    client.execute_sql(f"INSERT INTO t VALUES {rows}", schema_name=schema_name)
    if not ok:
        with pytest.raises(gnitz.GnitzError, match=_CREATE_DUP):
            client.execute_sql("CREATE UNIQUE INDEX ON t(a)", schema_name=schema_name)
        assert not _has_index(client, schema_name)
        return
    client.execute_sql("CREATE UNIQUE INDEX ON t(a)", schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        client.execute_sql("INSERT INTO t VALUES (2, 9, 90)", schema_name=schema_name)


def test_concurrent_inserts_during_create(client, server, schema_name):
    """A steady INSERT stream into the owner table concurrent with CREATE UNIQUE
    INDEX. All streamed values are distinct, so — whatever the interleaving — the
    catalog write lock orders every INSERT strictly before or after the
    pre-flight+backfill snapshot, the index must be created and enforce, no row
    may be lost from it, and no worker may wedge."""
    import threading

    with gnitz.connect(server) as writer:
        client.execute_sql(_T, schema_name=schema_name)
        # Seed so the pre-flight scan has data on every worker.
        _insert(client, schema_name, [(i, i) for i in range(1, 201)])

        stop, errors = threading.Event(), []

        def insert_stream():
            v = 1000
            try:
                while not stop.is_set() and v < 5000:
                    writer.execute_sql(f"INSERT INTO t VALUES ({v}, {v})", schema_name=schema_name)
                    v += 1
            except Exception as e:  # noqa: BLE001 — surfaced via `errors`
                errors.append(e)

        th = threading.Thread(target=insert_stream)
        th.start()
        try:
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=schema_name)
        finally:
            stop.set()
            th.join(timeout=60)
        assert not th.is_alive(), "insert stream did not stop"
        assert not errors, f"streaming inserts errored: {errors}"
        assert _has_index(client, schema_name)

        # The index enforces a seeded value, and a brand-new one still inserts.
        with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
            client.execute_sql("INSERT INTO t VALUES (888888, 1)", schema_name=schema_name)
        client.execute_sql("INSERT INTO t VALUES (888888, 888888)", schema_name=schema_name)


def test_multi_frame_key_train(unique_preflight_frame_server):
    """With frames shrunk to 7 keys, every worker streams a multi-frame
    continuation train; the merge must stay exact across frame boundaries — on
    both verdicts, wherever the boundaries happen to fall."""
    srv, sn = unique_preflight_frame_server, "frames"
    srv.create_schema(sn)
    try:
        srv.execute_sql(_T, schema_name=sn)
        _insert(srv, sn, [(pk, pk * 7) for pk in range(1, 201)])
        srv.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
        assert _has_index(srv, sn)
        srv.execute_sql(f"DROP INDEX {sn}__t__idx_val", schema_name=sn)

        # One duplicate pair on far-apart PKs: the equal keys are adjacent in
        # the merged stream however the frames are cut.
        srv.execute_sql("INSERT INTO t VALUES (1000, 700)", schema_name=sn)
        with pytest.raises(gnitz.GnitzError, match=_CREATE_DUP):
            srv.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
        assert not _has_index(srv, sn)
    finally:
        srv.drop_schema(sn)


def test_worker_fault_mid_preflight(unique_preflight_fault_server):
    """An injected worker fault during the pre-flight scan must surface as a
    client error with no index created, no filter seeded, and every worker
    drained (not wedged): the table stays fully usable, and the PK
    short-circuit — which never fans out — still succeeds."""
    srv, sn = unique_preflight_fault_server, "fault"
    srv.create_schema(sn)
    try:
        srv.execute_sql(_T, schema_name=sn)
        _insert(srv, sn, [(pk, pk) for pk in range(1, 33)])
        with pytest.raises(gnitz.GnitzError):
            srv.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
        assert not _has_index(srv, sn)
        # No filter was seeded and no index exists, so a duplicate value is
        # accepted — no partial constraint leaked out of the failed DDL.
        srv.execute_sql("INSERT INTO t VALUES (100, 1)", schema_name=sn)
        # All workers answer a full scan: nobody is wedged on a half-drained
        # pre-flight train.
        tid, _ = srv.resolve_table(sn, "t")
        assert len(list(srv.scan(tid))) == 33
        # The PK short-circuit returns before any fan-out, so it succeeds even
        # while every worker's scan path is faulted.
        srv.execute_sql("CREATE UNIQUE INDEX ON t(pk)", schema_name=sn)
        assert _has_index(srv, sn)
    finally:
        srv.drop_schema(sn)


@NEEDS_MULTI
@pytest.mark.parametrize("planted", [False, True], ids=["all-distinct", "duplicate"])
def test_preflight_single_sources_a_replicated_owner(client, schema_name, planted):
    """A replicated owner is the case where the global merge sees each value once
    per worker. The fan-out must take one worker's full copy: without that, W
    identical streams make every value look like its own duplicate and a
    genuinely unique table is refused. Single-sourcing must not blunt the check
    either — a real duplicate is still adjacent within the one stream.
    """
    client.execute_sql(_T + " WITH (replicated = true)", schema_name=schema_name)
    rows = [(pk, pk * 100) for pk in range(1, 6)]
    if planted:
        rows.append((6, 500))
    _insert(client, schema_name, rows)

    if planted:
        with pytest.raises(gnitz.GnitzError, match=_CREATE_DUP):
            client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=schema_name)
        assert not _has_index(client, schema_name)
        # No phantom constraint: another duplicate is still accepted.
        client.execute_sql("INSERT INTO t VALUES (7, 500)", schema_name=schema_name)
        return

    client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=schema_name)
    assert _has_index(client, schema_name)
    # The constraint is live, and the read still single-sources one copy.
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        client.execute_sql("INSERT INTO t VALUES (6, 100)", schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (6, 600)", schema_name=schema_name)
    tid, _ = client.resolve_table(schema_name, "t")
    assert sorted(r.val for r in client.scan(tid)) == [100, 200, 300, 400, 500, 600]


@pytest.mark.parametrize("planted", [False, True], ids=["all-distinct", "buried-duplicate"])
def test_preflight_spill_is_bounded_and_exact(unique_preflight_spill_server, planted):
    """With a 256-byte sort budget, a partition far past it drives the external
    merge sort (many spill runs, then a k-way merge over them). All-distinct data
    creates and then enforces; a duplicate buried among the runs, reachable only
    once the merge brings the two spans adjacent, is still caught — and leaves no
    phantom constraint. Unlike the debug seams, this budget is honoured in every
    build, so it bites a release server too."""
    srv, sn, n = unique_preflight_spill_server, "spill", 2000
    srv.create_schema(sn)
    try:
        srv.execute_sql(_T, schema_name=sn)
        # A wide PK spread → hundreds of spans per worker, far past the
        # 32-span budget → many spilled runs.
        rows = [(i * 7 + 1, i) for i in range(n)]
        if planted:
            # Repeats the first row's val on a far-away PK, appended last.
            rows.append((n * 10 + 3, 0))
        _insert(srv, sn, rows)

        if planted:
            with pytest.raises(gnitz.GnitzError, match=_CREATE_DUP):
                srv.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
            assert not _has_index(srv, sn)
            srv.execute_sql(f"INSERT INTO t VALUES ({n * 10 + 4}, 0)", schema_name=sn)
            return

        srv.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
        assert _has_index(srv, sn)
        with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
            srv.execute_sql(f"INSERT INTO t VALUES ({n * 10}, 5)", schema_name=sn)
        srv.execute_sql(f"INSERT INTO t VALUES ({n * 10 + 1}, {n + 1})", schema_name=sn)
    finally:
        srv.drop_schema(sn)


# ── Enforcement once the index exists ────────────────────────────────────────

@pytest.mark.parametrize("ddl", [
    _T + "; CREATE UNIQUE INDEX ON t(val)",
    "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT UNIQUE)",
], ids=["create-index", "column-constraint"])
def test_every_declaration_entry_point_enforces(client, schema_name, ddl):
    """`CREATE UNIQUE INDEX` and a column-level `UNIQUE` register the same
    index — the column form once discarded it silently."""
    for stmt in ddl.split("; "):
        client.execute_sql(stmt, schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=schema_name)
    # A second holder is refused whichever partition it would land on.
    for pk in (2, 1000000):
        with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
            client.execute_sql(f"INSERT INTO t VALUES ({pk}, 42)", schema_name=schema_name)
    # And so is a pair colliding inside one statement.
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        client.execute_sql("INSERT INTO t VALUES (3, 7), (4, 7)", schema_name=schema_name)


@pytest.fixture
def indexed(client, schema_name):
    """`t(pk, val)` with a unique index on `val`."""
    client.execute_sql(_T, schema_name=schema_name)
    client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=schema_name)
    return schema_name


_UPSERT = " ON CONFLICT (pk) DO UPDATE SET val = EXCLUDED.val"

_CLAIMS = [
    # A row rewriting its own value collides only with its own committed entry,
    # which the write retracts.
    ("keeps its own value", "INSERT INTO t VALUES (1, 42)" + _UPSERT, [(1, 42), (2, 99)]),
    # Moving to a free value vacates the old one.
    ("moves to a free value", "INSERT INTO t VALUES (1, 7)" + _UPSERT, [(1, 7), (2, 99)]),
    # Taking a value another live PK holds is the violation.
    ("takes a held value", "INSERT INTO t VALUES (1, 99)" + _UPSERT, None),
    ("update takes a held value", "UPDATE t SET val = 99 WHERE pk = 1", None),
    # Two rows claiming one *new* value: absent from committed storage, so the
    # occupancy probe answers "free" and only the in-batch check can catch it.
    ("two upserts claim one new value",
     "INSERT INTO t VALUES (1, 55), (2, 55)" + _UPSERT, None),
]


@pytest.mark.parametrize("stmt,final", [c[1:] for c in _CLAIMS], ids=[c[0] for c in _CLAIMS])
def test_a_claim_is_admitted_only_if_it_ends_up_held_once(client, indexed, stmt, final):
    client.execute_sql("INSERT INTO t VALUES (1, 42), (2, 99)", schema_name=indexed)
    tid, _ = client.resolve_table(indexed, "t")
    if final is None:
        with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
            client.execute_sql(stmt, schema_name=indexed)
        assert _rows(client, tid) == [(1, 42), (2, 99)], "a rejected write applies nothing"
        return
    client.execute_sql(stmt, schema_name=indexed)
    assert _rows(client, tid) == final


def test_a_value_freed_in_an_earlier_statement_is_reusable(client, indexed):
    """A DELETE releases the value it held; multiple NULLs never claim one."""
    client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=indexed)
    client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=indexed)
    client.execute_sql("INSERT INTO t VALUES (2, 42)", schema_name=indexed)
    tid, _ = client.resolve_table(indexed, "t")
    assert _rows(client, tid) == [(2, 42)]


def test_nulls_never_collide(client, schema_name):
    client.execute_sql(_T_NULLABLE, schema_name=schema_name)
    client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, NULL), (2, NULL)", schema_name=schema_name)
    tid, _ = client.resolve_table(schema_name, "t")
    assert _rows(client, tid) == [(1, None), (2, None)]


def test_two_indices_on_one_table_are_both_enforced(client, schema_name):
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
        schema_name=schema_name)
    client.execute_sql("CREATE UNIQUE INDEX ON t(a)", schema_name=schema_name)
    client.execute_sql("CREATE UNIQUE INDEX ON t(b)", schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 10, 20), (2, 11, 21)", schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        client.execute_sql("INSERT INTO t VALUES (3, 10, 22)", schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        client.execute_sql("INSERT INTO t VALUES (4, 12, 20)", schema_name=schema_name)


def test_anticorrelated_multi_source_index_merge(client, indexed):
    """The duplicate check seeks the indexed value in the secondary index, whose
    PK is compound `(value, src_pk)`. With values inserted one per push and
    falling as the PK rises, the check merges N runs. If that merge ordered by
    the raw u128 PK — `(src_pk, value)` — the seek for a smaller value would land
    on a larger value's run, report "absent", and let a duplicate through."""
    n = 6
    vals = [(n - i) * 100 for i in range(n)]  # 600, 500, ..., 100
    for i, val in enumerate(vals):
        client.execute_sql(f"INSERT INTO t VALUES ({i + 1}, {val})", schema_name=indexed)
    # Including the smallest values, which a u128 merge would mask.
    for j, val in enumerate(vals):
        with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
            client.execute_sql(f"INSERT INTO t VALUES ({1000 + j}, {val})", schema_name=indexed)


def test_a_concurrent_second_write_cannot_slip_past_a_buffered_first(client, server, indexed):
    """The second insert arrives before any scan forces a flush, so both writes
    would sit buffered without the drain — the TOCTOU window."""
    with gnitz.connect(server) as other:
        client.execute_sql("INSERT INTO t VALUES (1, 42)", schema_name=indexed)
        with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
            other.execute_sql("INSERT INTO t VALUES (2, 42)", schema_name=indexed)


# ── The fold a whole batch produces, not its rows one at a time ──────────────

@pytest.mark.parametrize("committed,batch,final", [
    # The batch folds to one live row, so only one claim stands.
    ([], [(1, 10, 1), (1, 10, 1)], [(1, 10)]),
    # Only 20 is written, so the superseded 10 must not be probed against P9.
    ([(9, 10)], [(1, 10, 1), (1, 20, 1)], [(1, 20), (9, 10)]),
    # P2 is gone after the fold, so only P1 claims 10.
    ([], [(1, 10, 1), (2, 10, 1), (2, 10, -1)], [(1, 10)]),
    # A fresh PK takes a value vacated by an upsert of a different PK.
    ([(1, 10)], [(1, 99, 1), (2, 10, 1)], [(1, 99), (2, 10)]),
    # The retraction carries filler payload, as `delete` ships: the freed value
    # is not on the wire at all — the committed row's own payload is retracted.
    ([(1, 5)], [(1, 0, -1), (2, 5, 1)], [(2, 5)]),
    # A committed holder releases a value while a fresh PK takes it, and the
    # forged retraction of an absent PK changes nothing.
    ([(2, 6)], [(2, 7, 1), (4, 6, 1), (3, 6, -1)], [(2, 7), (4, 6)]),
    # A value moved to a fresh PK by an explicit retraction of its holder.
    ([(1, 5)], [(1, 5, -1), (2, 5, 1)], [(2, 5)]),
], ids=["repeated-identical-row", "superseded-value", "claimant-removed-again",
        "vacated-by-upsert", "vacated-by-filler-retraction",
        "forged-retraction-freed-by-upserted-holder", "explicit-transfer"])
def test_a_push_is_validated_against_its_fold(client, schema_name, committed, batch, final):
    tid, schema = _raw_table(client, schema_name)
    if committed:
        b = gnitz.ZSetBatch(schema)
        for pk, val in committed:
            b.append(pk=pk, val=val)
        client.push(tid, b)
    b = gnitz.ZSetBatch(schema)
    for pk, val, w in batch:
        b.append(pk=pk, val=val, _weight=w)
    client.push(tid, b)
    assert _rows(client, tid) == final


@pytest.mark.parametrize("committed,batch", [
    # Two survivors claiming one value is still a violation.
    ([], [(1, 10, 1), (2, 10, 1)]),
    # A fresh PK insertion of a still-held value, with nothing freeing it.
    ([(1, 5)], [(3, 5, 1)]),
    # The exemption keys on (holder PK, value): a retraction naming a non-holder
    # must not let a real duplicate through.
    ([(1, 5)], [(3, 5, -1), (2, 5, 1)]),
], ids=["two-survivors", "genuine-duplicate", "forged-retraction"])
def test_a_push_whose_fold_leaves_two_claims_is_rejected(client, schema_name, committed, batch):
    tid, schema = _raw_table(client, schema_name)
    before = []
    if committed:
        b = gnitz.ZSetBatch(schema)
        for pk, val in committed:
            b.append(pk=pk, val=val)
        client.push(tid, b)
        before = sorted(committed)
    b = gnitz.ZSetBatch(schema)
    for pk, val, w in batch:
        b.append(pk=pk, val=val, _weight=w)
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        client.push(tid, b)
    assert _rows(client, tid) == before


def test_bulk_colliding_fresh_pks_rejected(client, schema_name):
    """A re-run bulk load: 2000 fresh PKs re-claiming 2000 committed values,
    none of which the batch frees. Every span comes back occupied by a holder
    the bundle does not retire, so the whole push is rejected — and one
    occupancy probe decides all 2000."""
    tid, schema = _raw_table(client, schema_name)
    n = 2000
    b = gnitz.ZSetBatch(schema)
    for i in range(n):
        b.append(pk=i, val=i)
    client.push(tid, b)

    b = gnitz.ZSetBatch(schema)
    for i in range(n):
        b.append(pk=n + i, val=i)  # fresh PKs, committed values
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        client.push(tid, b)
    assert len(list(client.scan(tid))) == n


@pytest.mark.parametrize("stmt,final", [
    # Every occupied span resolves to a holder the same bundle retires.
    ("UPDATE t SET val = val + 1", [(1, 2), (2, 3), (3, 4)]),
    # Two rows exchange their values in one batch of same-PK upserts.
    ("UPDATE t SET val = 4 - val", [(1, 3), (2, 2), (3, 1)]),
], ids=["shift", "swap"])
def test_a_bulk_transfer_within_one_statement_is_admitted(client, schema_name, stmt, final):
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT UNIQUE)",
        schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 1), (2, 2), (3, 3)", schema_name=schema_name)
    client.execute_sql(stmt, schema_name=schema_name)
    tid, _ = client.resolve_table(schema_name, "t")
    assert _rows(client, tid) == final


# ── A wide PK behind the index ───────────────────────────────────────────────

@pytest.fixture
def wide_pk(client, schema_name):
    """`t(a, b, c)` PK with a unique index on a separate `val`."""
    client.execute_sql(
        "CREATE TABLE t (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL,"
        " c BIGINT UNSIGNED NOT NULL, val BIGINT UNSIGNED NOT NULL, PRIMARY KEY (a, b, c))",
        schema_name=schema_name)
    client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=schema_name)
    return schema_name


def test_duplicate_value_on_distinct_wide_pks_rejected(client, wide_pk):
    client.execute_sql("INSERT INTO t VALUES (1, 1, 1, 42)", schema_name=wide_pk)
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        client.execute_sql("INSERT INTO t VALUES (2, 2, 2, 42)", schema_name=wide_pk)
    res = client.execute_sql("SELECT a, val FROM t", schema_name=wide_pk)
    assert sorted(tuple(r) for r in res[0]["rows"]) == [(1, 42)]


def test_prefix_colliding_wide_pks_are_distinct_rows(client, wide_pk):
    """`(7,7,100)` and `(7,7,200)` share their first 16 bytes. Moving the second
    row's value onto the first must be rejected: a 16-byte-truncated holder
    compare would misread the collision as the row's own entry. A row rewriting
    its own unchanged value is admitted, since it retracts that entry itself."""
    client.execute_sql("INSERT INTO t VALUES (7, 7, 100, 10), (7, 7, 200, 42)",
                       schema_name=wide_pk)
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        client.execute_sql("UPDATE t SET val = 42 WHERE a = 7 AND b = 7 AND c = 100",
                           schema_name=wide_pk)
    client.execute_sql("UPDATE t SET val = 10 WHERE a = 7 AND b = 7 AND c = 100",
                       schema_name=wide_pk)
    res = client.execute_sql("SELECT c, val FROM t", schema_name=wide_pk)
    assert sorted(tuple(r) for r in res[0]["rows"]) == [(100, 10), (200, 42)]


# ── Resolving the holder a probe reports ─────────────────────────────────────

# NULL-valued rows spread over a PK range wide enough to reach every worker.
_NULL_PK_LO, _NULL_PK_HI = 10_000, 10_048

# `(column type, the value whose order-preserving image is all-zero for it, that
# value's unsigned native image)`. The last is what the seek API takes: its key
# values are `u128`, the zero-extended cell the engine reads out of a row, so a
# signed column's negative value arrives as its two's-complement image.
_ZERO_IMAGE_COLUMNS = [
    ("BIGINT UNSIGNED", 0, 0),
    ("INT", -2147483648, 1 << 31),
]

_HOLDER_PK = 999983


class TestUniqueHolderFromProbe:
    """A NULL indexed cell must not claim the all-zero key image, and the holder
    a probe reports must be the one that currently holds the value."""

    def _setup(self, client, sn, col_type):
        client.execute_sql(
            f"CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a {col_type})", schema_name=sn)
        client.execute_sql("CREATE UNIQUE INDEX ON t(a)", schema_name=sn)

    def _fill_nulls(self, client, sn):
        _insert(client, sn, [(pk, "NULL") for pk in range(_NULL_PK_LO, _NULL_PK_HI)])

    def _holders_of(self, client, sn, value):
        """Committed PKs whose `a` equals `value`, read by a full scan (a SELECT
        by the indexed column would itself be the thing under test)."""
        res = client.execute_sql("SELECT pk, a FROM t", schema_name=sn)
        return sorted(r.pk for r in res[0]["rows"] if r.a == value)

    def _seek_pk(self, client, tid, value):
        """PKs a direct single-column index seek returns for `value`."""
        res = client.seek_by_index(tid, [1], [value])
        if res.schema is None:
            return []
        return sorted(pk for pk, w in zip(res.pks, res.weights) if w > 0)

    @pytest.mark.parametrize("col_type,value,seek_key", _ZERO_IMAGE_COLUMNS)
    def test_the_zero_image_holder_survives_a_flood_of_nulls(
            self, client, schema_name, col_type, value, seek_key):
        """`value` is the one whose order-preserving image is all-zero for this
        column type — the image a NULL indexed cell would fold onto if anything
        keyed by it treated NULL as a value. The index is NULL-distinct (a NULL
        row has no entry at all), so both a direct seek and an UPDATE/DELETE by
        that value must reach exactly the holder's one row, however many NULL
        rows were written after it."""
        self._setup(client, schema_name, col_type)
        tid, _ = client.resolve_table(schema_name, "t")
        client.execute_sql(f"INSERT INTO t VALUES ({_HOLDER_PK}, {value})",
                           schema_name=schema_name)
        self._fill_nulls(client, schema_name)

        assert self._seek_pk(client, tid, seek_key) == [_HOLDER_PK]

        res = client.execute_sql(f"UPDATE t SET a = {value} WHERE a = {value}",
                                 schema_name=schema_name)
        assert (res[0]["type"], res[0]["count"]) == ("RowsAffected", 1)
        res = client.execute_sql(f"DELETE FROM t WHERE a = {value}", schema_name=schema_name)
        assert (res[0]["type"], res[0]["count"]) == ("RowsAffected", 1)
        assert self._holders_of(client, schema_name, value) == []

    def test_the_constraint_holds_under_the_same_null_flood(self, client, schema_name):
        """A bundle that moves a second committed row's value AND claims the
        holder's value for a fresh PK must be rejected — the second row is what
        makes the bundle touch a committed PK, so the check reaches the holder
        rather than stopping at the claim."""
        other, fresh = _NULL_PK_HI + 1, _NULL_PK_HI + 2
        self._setup(client, schema_name, "BIGINT UNSIGNED")
        client.execute_sql(f"INSERT INTO t VALUES ({_HOLDER_PK}, 0)", schema_name=schema_name)
        client.execute_sql(f"INSERT INTO t VALUES ({other}, 77)", schema_name=schema_name)
        self._fill_nulls(client, schema_name)

        with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
            client.execute_sql(
                f"INSERT INTO t VALUES ({other}, 88), ({fresh}, 0)" + _UPSERT.replace("val", "a"),
                schema_name=schema_name)
        assert self._holders_of(client, schema_name, 0) == [_HOLDER_PK]

    def test_a_value_moved_across_the_drop_index_window(self, client, schema_name):
        """The value is deleted and re-inserted under a different PK while no
        index exists at all. Nothing rewrites the moved row through the index
        before the re-create, so both a direct seek and a bundle claiming the
        value must name the NEW holder, not the pre-DROP one."""
        other, fresh, moved = 555, 556, 31337
        self._setup(client, schema_name, "BIGINT NOT NULL")
        tid, _ = client.resolve_table(schema_name, "t")
        client.execute_sql(f"INSERT INTO t VALUES ({_HOLDER_PK}, 7)", schema_name=schema_name)
        client.execute_sql(f"INSERT INTO t VALUES ({other}, 77)", schema_name=schema_name)
        client.execute_sql(f"DROP INDEX {schema_name}__t__idx_a", schema_name=schema_name)

        client.execute_sql(f"DELETE FROM t WHERE pk = {_HOLDER_PK}", schema_name=schema_name)
        client.execute_sql(f"INSERT INTO t VALUES ({moved}, 7)", schema_name=schema_name)
        client.execute_sql("CREATE UNIQUE INDEX ON t(a)", schema_name=schema_name)

        assert self._seek_pk(client, tid, 7) == [moved]
        with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
            client.execute_sql(
                f"INSERT INTO t VALUES ({other}, 88), ({fresh}, 7)" + _UPSERT.replace("val", "a"),
                schema_name=schema_name)
        assert self._holders_of(client, schema_name, 7) == [moved]

    def test_composite_unique_holder_split(self, client, schema_name):
        """A composite span is wider than one column, so the `[span || holder PK]`
        split must land at the whole span's width."""
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY,"
            " a BIGINT NOT NULL, b BIGINT NOT NULL)", schema_name=schema_name)
        client.execute_sql("CREATE UNIQUE INDEX ON t(a, b)", schema_name=schema_name)
        client.execute_sql("INSERT INTO t VALUES (1, 1, 1), (2, 2, 2)", schema_name=schema_name)

        with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
            client.execute_sql(
                f"INSERT INTO t VALUES (2, 3, 3), ({_HOLDER_PK}, 1, 1)"
                " ON CONFLICT (pk) DO UPDATE SET a = EXCLUDED.a, b = EXCLUDED.b",
                schema_name=schema_name)
        res = client.execute_sql("SELECT pk, a, b FROM t", schema_name=schema_name)
        assert sorted(r.pk for r in res[0]["rows"] if (r.a, r.b) == (1, 1)) == [1]

    def test_replicated_owner_holder_is_deduped_and_decisive(self, client, schema_name):
        """Every worker holds a replicated table's whole index, so every worker
        answers for the same span. The `W` identical answers must collapse to one
        holder, and that holder must decide the verdict: the same bundle shape is
        rejected when the holder keeps the value and accepted when it releases it
        in the same bundle."""
        upsert = _UPSERT.replace("val", "a")
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)"
            " WITH (replicated = true)", schema_name=schema_name)
        client.execute_sql("CREATE UNIQUE INDEX ON t(a)", schema_name=schema_name)
        client.execute_sql("INSERT INTO t VALUES (1, 42), (2, 77)", schema_name=schema_name)

        # pk=1 keeps 42 → the fresh claim collides.
        with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
            client.execute_sql(
                f"INSERT INTO t VALUES (2, 88), ({_HOLDER_PK}, 42)" + upsert,
                schema_name=schema_name)
        res = client.execute_sql("SELECT pk, a FROM t", schema_name=schema_name)
        assert sorted(r.pk for r in res[0]["rows"] if r.a == 42) == [1]

        # Same bundle, but the holder releases 42 in it → accepted.
        client.execute_sql(
            f"INSERT INTO t VALUES (1, 99), ({_HOLDER_PK}, 42)" + upsert,
            schema_name=schema_name)
        res = client.execute_sql("SELECT pk, a FROM t", schema_name=schema_name)
        rows = {r.pk: r.a for r in res[0]["rows"]}
        assert rows[1] == 99 and rows[_HOLDER_PK] == 42


# ── Composite UNIQUE (a, b) ──────────────────────────────────────────────────

@pytest.fixture
def ab(client, schema_name):
    """`t(pk, a, b)` with no index yet."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
        schema_name=schema_name)
    return schema_name


def test_composite_preflight_rejects_a_cross_partition_duplicate(client, ab):
    """A pre-existing duplicate `(a, b)` spread across a wide PK range is
    invisible to any single worker's backfill but caught by the master composite
    pre-flight, and leaves no phantom constraint."""
    spread = [1, 7, 13, 1000, 99999, 123456, 777777, 8888888, 73501234, 901234567]
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(f"({pk}, 5, 9)" for pk in spread), schema_name=ab)
    with pytest.raises(gnitz.GnitzError, match=_CREATE_DUP):
        client.execute_sql("CREATE UNIQUE INDEX ON t(a, b)", schema_name=ab)
    assert not _has_index(client, ab)
    client.execute_sql("INSERT INTO t VALUES (2, 5, 9)", schema_name=ab)


def test_composite_distinguishes_the_trailing_column(client, ab):
    """Rows sharing the leading column `a` with distinct trailing `b` are all
    distinct composites — the regression a u128 leading-column truncation would
    have falsely rejected. Once created, only a full `(a, b)` repeat collides,
    including one committed on a different worker; a composite seek then returns
    exactly one row."""
    spread = [1, 1000, 99999, 8888888, 901234567]
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(f"({pk}, 5, {i})" for i, pk in enumerate(spread)),
        schema_name=ab)
    client.execute_sql("CREATE UNIQUE INDEX ON t(a, b)", schema_name=ab)

    # Re-claiming each committed (a, b) on a fresh PK — likely a different
    # worker than the holder — must be rejected every time.
    for i in range(len(spread)):
        with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
            client.execute_sql(f"INSERT INTO t VALUES ({2000 + i}, 5, {i})", schema_name=ab)
    # A distinct trailing column, and a distinct leading one, are both admitted.
    client.execute_sql("INSERT INTO t VALUES (11, 5, 999)", schema_name=ab)
    client.execute_sql("INSERT INTO t VALUES (12, 6, 0)", schema_name=ab)

    res = client.execute_sql("SELECT pk FROM t WHERE a = 5 AND b = 0", schema_name=ab)
    assert sorted(r.pk for r in res[0]["rows"]) == [1]


def test_composite_null_distinctness(client, schema_name):
    """A row NULL in ANY indexed column is not indexed and never collides, while
    a fully non-null `(a, b)` still enforces."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT, b BIGINT)",
        schema_name=schema_name)
    client.execute_sql("CREATE UNIQUE INDEX ON t(a, b)", schema_name=schema_name)
    client.execute_sql(
        "INSERT INTO t VALUES (1, NULL, 1), (2, NULL, 1), (3, 5, NULL), (4, 5, NULL),"
        " (5, NULL, NULL), (6, NULL, NULL), (7, 5, 1)", schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        client.execute_sql("INSERT INTO t VALUES (8, 5, 1)", schema_name=schema_name)


def test_composite_over_the_pk_columns_short_circuits(client, schema_name):
    """UNIQUE over exactly the PK columns, in any order, can never collide — so
    it is created without a scan and admits every row the PK already permits."""
    client.execute_sql(
        "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT NOT NULL,"
        " PRIMARY KEY (a, b))", schema_name=schema_name)
    # Duplicate `v` across distinct (a, b) PKs — a per-column unique would choke.
    client.execute_sql("INSERT INTO t VALUES (1, 2, 100), (1, 3, 100)", schema_name=schema_name)
    client.execute_sql("CREATE UNIQUE INDEX ON t(b, a)", schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (2, 3, 100)", schema_name=schema_name)


def test_composite_table_constraint_entry_point_and_bulk_shift(client, schema_name):
    """`CREATE TABLE ... UNIQUE (a, b)` creates and enforces the composite index,
    and a dense `b = b + 1` shift under it resolves every occupied span to a
    holder the same bundle retires."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL,"
        " b BIGINT NOT NULL, UNIQUE (a, b))", schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 7, 1), (2, 7, 2), (3, 7, 3)",
                       schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        client.execute_sql("INSERT INTO t VALUES (4, 7, 1)", schema_name=schema_name)

    client.execute_sql("UPDATE t SET b = b + 1", schema_name=schema_name)
    tid, _ = client.resolve_table(schema_name, "t")
    assert sorted((r.pk, r.a, r.b) for r in client.scan(tid)) == [(1, 7, 2), (2, 7, 3), (3, 7, 4)]
