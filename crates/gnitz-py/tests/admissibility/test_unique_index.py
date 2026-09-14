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

import threading

import pytest
import gnitz
from _read import bag, rows as read_rows, scanned
from _serverproc import NEEDS_MULTI, join_or_fail
from _sql import insert

_T = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)"
_T_NULLABLE = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT)"
_TAB = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT, b BIGINT)"
_AB_PK = ("CREATE TABLE t (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL,"
          " payload BIGINT, PRIMARY KEY (a, b))")
_RAW_COLS = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
             gnitz.ColumnDef("val", gnitz.TypeCode.I64)]

# `metadata.rs` is the single source of both messages, so pinning them costs
# nothing and stops an unrelated failure satisfying a bare `raises`.
_CREATE_DUP = "contains duplicate values"
_VIOLATION = "[Uu]nique index violation"


def _has_index(client, sn, table="t"):
    """True if any live IdxTab row names `table` as its owner."""
    batch = client.scan(gnitz.IDX_TAB)
    tid, _ = client.resolve_table(sn, table)
    return any(r._weight > 0 and r.owner_id == tid for r in batch)


def _raw_table(client, sn):
    """Raw `t` + a SQL unique index on `val`. Returns `(tid, schema)`."""
    tid = client.create_table(sn, "t", _RAW_COLS)
    client.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
    return tid, gnitz.Schema(_RAW_COLS)


# ── CREATE over rows already present ─────────────────────────────────────────

# A PK spread wide enough that routing puts the rows on different workers.
_SPREAD = [1, 7, 13, 1000, 99999, 123456, 777777, 8888888, 73501234, 901234567]

# `(ddl, index columns, seeded rows, a row claiming a seeded value, a row
# claiming a fresh one)`. Without a fresh row the seed already holds a
# duplicate: the create is refused and the claiming row then lands, since no
# constraint survived. Otherwise the create passes, the fresh row lands, and the
# claiming row — where one can be formed — is the violation.
_PREFLIGHT = {
    # One value over 50 consecutive PKs: at any worker count both a
    # within-partition repeat (by pigeonhole) and a cross-partition one — the
    # duplicate no single worker's backfill can see.
    "one value over many rows": (_T, "val", [(pk, 42) for pk in range(1, 51)], (51, 42), None),
    # A single pair on far-apart PKs, brought adjacent only by the sorted merge.
    "one pair among many": (
        _T, "val", [(pk, pk * 10) for pk in range(1, 301)] + [(1000, 1500)], (1001, 1500), None),
    "all distinct": (_T, "val", [(pk, pk) for pk in range(1, 301)], (900001, 150), (900002, 0)),
    # A NULL key never enters any worker's key stream.
    "nulls": (
        _T_NULLABLE, "val", [(pk, None) for pk in range(1, 9)] + [(pk, pk) for pk in range(100, 108)],
        (201, 100), (200, None)),
    # The merge's key order is not monotonic in signed value; the verdict needs
    # only `equal value => equal key`, with no sign confusion in the round trip.
    "equal negatives": (_T, "val", [(1, -5), (2, 300), (3, -5)], (4, -5), None),
    "distinct negatives": (_T, "val", [(1, -1), (2, -2), (3, -3), (4, 0)], (10, -2), (11, -9)),
    # A compound-PK member is not the PK, so it takes the full-path scan.
    "a repeated compound-key member": (_AB_PK, "a", [(1, 1, 10), (1, 2, 20), (3, 1, 30)], (1, 9, 90), None),
    "distinct compound-key members": (_AB_PK, "a", [(1, 1, 10), (2, 2, 20), (3, 1, 30)], (2, 9, 90), (4, 9, 90)),
    # The member's key is its bytes copied out of the PK region verbatim, so
    # values straddling 2^63 are where a lost sign flip or byte-order slip would
    # misorder or collide them; the claim lands on the widest.
    "compound-key members across the midpoint": (
        _AB_PK, "a", [(0, 1, 10), (2**63 - 1, 1, 20), (2**63, 1, 30), (2**64 - 1, 1, 40)],
        (2**64 - 1, 9, 90), (5, 9, 90)),
    # UNIQUE over exactly the PK columns, in any order, can never collide, so it
    # is created without a scan over a payload that repeats.
    "the whole compound key": (
        "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT NOT NULL, PRIMARY KEY (a, b))",
        "b, a", [(1, 2, 100), (1, 3, 100)], None, (2, 3, 100)),
    "a composite pair across partitions": (_TAB, "a, b", [(pk, 5, 9) for pk in _SPREAD], (2, 5, 9), None),
    # Rows sharing the leading column are distinct composites — what a
    # leading-column truncation would refuse.
    "composites sharing a leading column": (
        _TAB, "a, b", [(pk, 5, i) for i, pk in enumerate(_SPREAD)], (2000, 5, 3), (11, 5, 999)),
    # A row NULL in any indexed column is not indexed and never collides.
    "composites with NULLs": (
        _TAB, "a, b",
        [(1, None, 1), (2, None, 1), (3, 5, None), (4, 5, None), (5, None, None), (6, None, None), (7, 5, 1)],
        (8, 5, 1), (9, None, 1)),
    # The global merge sees a replicated owner's every value once per worker, so
    # the fan-out takes one worker's copy — which must not blunt a real
    # duplicate inside it.
    "replicated, all distinct": pytest.param(
        _T + " WITH (replicated = true)", "val", [(pk, pk * 100) for pk in range(1, 6)],
        (6, 100), (7, 600), marks=NEEDS_MULTI),
    "replicated, one pair": pytest.param(
        _T + " WITH (replicated = true)", "val", [(pk, pk * 100) for pk in range(1, 6)] + [(6, 500)],
        (7, 500), None, marks=NEEDS_MULTI),
}


@pytest.mark.parametrize("ddl,cols,seed,claim,fresh", _PREFLIGHT.values(), ids=_PREFLIGHT.keys())
def test_create_over_present_rows_admits_exactly_a_duplicate_free_seed(
        client, schema_name, ddl, cols, seed, claim, fresh):
    sn = schema_name
    client.execute_sql(ddl, schema_name=sn)
    insert(client, sn, "t", seed)
    create = f"CREATE UNIQUE INDEX ON t({cols})"
    if fresh is None:
        with pytest.raises(gnitz.GnitzError, match=_CREATE_DUP) as exc:
            client.execute_sql(create, schema_name=sn)
        # The qualified table is what tells the author what to change.
        assert f"{sn}.t" in str(exc.value), exc.value
        assert not _has_index(client, sn)
        insert(client, sn, "t", [claim])
        landed = claim
    else:
        client.execute_sql(create, schema_name=sn)
        insert(client, sn, "t", [fresh])
        if claim is not None:
            with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
                insert(client, sn, "t", [claim])
        landed = fresh
    # Every worker still answers, and nothing a verdict refused landed.
    assert bag(scanned(client, sn, "t")) == dict.fromkeys(seed + [landed], 1)


def test_concurrent_inserts_during_create(client, server, schema_name):
    """A steady INSERT stream into the owner table concurrent with CREATE UNIQUE
    INDEX. All streamed values are distinct, so — whatever the interleaving — the
    catalog write lock orders every INSERT strictly before or after the
    pre-flight+backfill snapshot, the index must be created and enforce, no row
    may be lost from it, and no worker may wedge."""
    with gnitz.connect(server) as writer:
        client.execute_sql(_T, schema_name=schema_name)
        # Seed so the pre-flight scan has data on every worker.
        insert(client, schema_name, "t", [(i, i) for i in range(1, 201)])

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
            join_or_fail("insert stream did not stop", th)
        assert not errors, f"streaming inserts errored: {errors}"

        # The index enforces a seeded value, and a brand-new one still inserts.
        with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
            client.execute_sql("INSERT INTO t VALUES (888888, 1)", schema_name=schema_name)
        client.execute_sql("INSERT INTO t VALUES (888888, 888888)", schema_name=schema_name)


def test_multi_frame_key_train(unique_preflight_frame_server):
    """With frames shrunk to 7 keys, every worker streams a multi-frame
    continuation train; the merge must stay exact across frame boundaries — on
    both verdicts, wherever the boundaries happen to fall."""
    srv, sn = unique_preflight_frame_server, "public"
    srv.execute_sql(_T, schema_name=sn)
    insert(srv, sn, "t", [(pk, pk * 7) for pk in range(1, 201)])
    srv.execute_sql("CREATE UNIQUE INDEX ix ON t(val)", schema_name=sn)
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        insert(srv, sn, "t", [(201, 7)])
    srv.execute_sql("DROP INDEX ix", schema_name=sn)

    # One duplicate pair on far-apart PKs: the equal keys are adjacent in the
    # merged stream however the frames are cut.
    insert(srv, sn, "t", [(1000, 700)])
    with pytest.raises(gnitz.GnitzError, match=_CREATE_DUP):
        srv.execute_sql("CREATE UNIQUE INDEX ix ON t(val)", schema_name=sn)
    assert not _has_index(srv, sn)


def test_unique_index_over_a_long_text_table_past_one_frame(reply_frame_budget_server):
    """The pre-flight warms its cold filters from a whole-table scan, so a table
    with a long TEXT column reaches that scan's 16 KiB frame budget on rows the
    user never asked to read. Every frame carries a heap compacted to its own
    rows, so the DDL completes and the index it builds enforces uniqueness."""
    srv, sn, n = reply_frame_budget_server, "public", 800
    srv.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, u BIGINT NOT NULL, body TEXT NOT NULL)",
        schema_name=sn)
    # 200 bytes of heap per row: several frame budgets on every worker.
    insert(srv, sn, "t", [(i, i, f"row-{i}-" + "z" * 200) for i in range(n)])

    srv.execute_sql("CREATE UNIQUE INDEX ON t(u)", schema_name=sn)
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        srv.execute_sql("INSERT INTO t VALUES (99999, 7, 'dup')", schema_name=sn)
    srv.execute_sql(f"INSERT INTO t VALUES (99999, {n}, 'new')", schema_name=sn)
    assert bag(read_rows(srv, sn, "SELECT pk, u FROM t WHERE u = 17")) == {(17, 17): 1}


def test_worker_fault_mid_preflight(unique_preflight_fault_server):
    """An injected worker fault during the pre-flight scan must surface as a
    client error with no index created, no filter seeded, and every worker
    drained (not wedged): the table stays fully usable, and the PK
    short-circuit — which never fans out — still succeeds."""
    srv, sn = unique_preflight_fault_server, "public"
    srv.execute_sql(_T, schema_name=sn)
    seed = [(pk, pk) for pk in range(1, 33)]
    insert(srv, sn, "t", seed)
    with pytest.raises(gnitz.GnitzError):
        srv.execute_sql("CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
    assert not _has_index(srv, sn)
    # No filter was seeded and no index exists, so a duplicate value is
    # accepted — no partial constraint leaked out of the failed DDL.
    insert(srv, sn, "t", [(100, 1)])
    # All workers answer a full scan: nobody is wedged on a half-drained
    # pre-flight train.
    assert bag(scanned(srv, sn, "t")) == dict.fromkeys(seed + [(100, 1)], 1)
    # The PK short-circuit returns before any fan-out, so it succeeds even
    # while every worker's scan path is faulted.
    srv.execute_sql("CREATE UNIQUE INDEX ON t(pk)", schema_name=sn)
    assert _has_index(srv, sn)


def test_preflight_spill_is_bounded_and_exact(unique_preflight_spill_server):
    """With a 256-byte sort budget, a partition far past it drives the external
    merge sort (many spill runs, then a k-way merge over them). All-distinct data
    creates and then enforces; a duplicate buried among the runs, reachable only
    once the merge brings the two spans adjacent, is still caught. Unlike the
    debug seams, this budget is honoured in every build, so it bites a release
    server too."""
    srv, sn, n = unique_preflight_spill_server, "public", 2000
    srv.execute_sql(_T, schema_name=sn)
    # A wide PK spread → hundreds of spans per worker, far past the 32-span
    # budget → many spilled runs.
    insert(srv, sn, "t", [(i * 7 + 1, i) for i in range(n)])
    srv.execute_sql("CREATE UNIQUE INDEX ix ON t(val)", schema_name=sn)
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        insert(srv, sn, "t", [(n * 10, 5)])
    srv.execute_sql("DROP INDEX ix", schema_name=sn)

    # Repeats the first row's val on a far-away PK.
    insert(srv, sn, "t", [(n * 10 + 3, 0)])
    with pytest.raises(gnitz.GnitzError, match=_CREATE_DUP):
        srv.execute_sql("CREATE UNIQUE INDEX ix ON t(val)", schema_name=sn)
    assert not _has_index(srv, sn)


# ── Enforcement once the index exists ────────────────────────────────────────

@pytest.mark.parametrize("ddl", [
    _T + "; CREATE UNIQUE INDEX ON t(val)",
    "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT UNIQUE)",
    _T + "; ALTER TABLE t ADD CONSTRAINT UNIQUE (val)",
], ids=["create-index", "column-constraint", "add-constraint"])
def test_every_declaration_entry_point_enforces(client, schema_name, ddl):
    """`CREATE UNIQUE INDEX`, a column-level `UNIQUE` and an unnamed `ADD
    CONSTRAINT ... UNIQUE` register the same index — the column form once
    discarded it silently."""
    client.execute_sql(ddl, schema_name=schema_name)
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
    client.execute_sql(_T + "; CREATE UNIQUE INDEX ON t(val)", schema_name=schema_name)
    return schema_name


_UPSERT = " ON CONFLICT (pk) DO UPDATE SET val = EXCLUDED.val"

_CLAIMS = [
    # A row rewriting its own value collides only with its own committed entry,
    # which the write retracts.
    ("keeps its own value", "INSERT INTO t VALUES (1, 42)" + _UPSERT, [(1, 42), (2, 99)]),
    # Moving to a free value vacates the old one.
    ("moves to a free value", "INSERT INTO t VALUES (1, 7)" + _UPSERT, [(1, 7), (2, 99)]),
    # A DELETE releases the value it held for a later statement.
    ("takes a value a DELETE freed",
     "DELETE FROM t WHERE pk = 1; INSERT INTO t VALUES (3, 42)", [(2, 99), (3, 42)]),
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
    committed = [(1, 42), (2, 99)]
    insert(client, indexed, "t", committed)
    if final is None:
        with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
            client.execute_sql(stmt, schema_name=indexed)
        final = committed
    else:
        client.execute_sql(stmt, schema_name=indexed)
    assert bag(scanned(client, indexed, "t")) == dict.fromkeys(final, 1)


def test_two_indices_on_one_table_are_both_enforced(client, schema_name):
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL); "
        "CREATE UNIQUE INDEX ON t(a); CREATE UNIQUE INDEX ON t(b)", schema_name=schema_name)
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


# ── The fold a whole write produces, not its rows one at a time ──────────────

# `(committed rows, frames of (pk, val, weight), rows after — or None where the
# fold leaves a value held twice and nothing applies)`.
_FOLDS = {
    # The batch folds to one live row, so only one claim stands.
    "repeated identical row": ([], [[(1, 10, 1), (1, 10, 1)]], [(1, 10)]),
    # Only 20 is written, so the superseded 10 must not be probed against P9.
    "superseded value": ([(9, 10)], [[(1, 10, 1), (1, 20, 1)]], [(1, 20), (9, 10)]),
    # P2 is gone after the fold, so only P1 claims 10.
    "claimant removed again": ([], [[(1, 10, 1), (2, 10, 1), (2, 10, -1)]], [(1, 10)]),
    # A fresh PK takes a value vacated by an upsert of a different PK.
    "vacated by upsert": ([(1, 10)], [[(1, 99, 1), (2, 10, 1)]], [(1, 99), (2, 10)]),
    # The retraction carries filler payload, as `delete` ships: the freed value
    # is not on the wire at all — the committed row's own payload is retracted.
    "vacated by filler retraction": ([(1, 5)], [[(1, 0, -1), (2, 5, 1)]], [(2, 5)]),
    # A committed holder releases a value while a fresh PK takes it, and the
    # forged retraction of an absent PK changes nothing.
    "forged retraction beside an upserted holder": (
        [(2, 6)], [[(2, 7, 1), (4, 6, 1), (3, 6, -1)]], [(2, 7), (4, 6)]),
    # A value moved to a fresh PK by an explicit retraction of its holder.
    "explicit transfer": ([(1, 5)], [[(1, 5, -1), (2, 5, 1)]], [(2, 5)]),
    "two survivors": ([], [[(1, 10, 1), (2, 10, 1)]], None),
    # A fresh PK insertion of a still-held value, with nothing freeing it.
    "genuine duplicate": ([(1, 5)], [[(3, 5, 1)]], None),
    # The exemption keys on (holder PK, value): a retraction naming a non-holder
    # must not let a real duplicate through.
    "forged retraction": ([(1, 5)], [[(3, 5, -1), (2, 5, 1)]], None),
    "two holders across frames": ([], [[(1, 5, 1)], [(2, 5, 1)]], None),
    # A value retired from its committed holder in one frame is free for the next.
    "retire then take": ([(1, 5)], [[(1, 0, -1)], [(2, 5, 1)]], [(2, 5)]),
    # Values shifting across rows the bundle itself creates.
    "value shift": ([], [[(1, 5, 1), (2, 6, 1)], [(2, 5, 1), (1, 7, 1)]], [(1, 7), (2, 5)]),
    # A claim a later frame supersedes frees its value for a third.
    "superseded claim": ([], [[(1, 5, 1)], [(1, 6, 1)], [(2, 5, 1)]], [(1, 6), (2, 5)]),
}


@pytest.mark.parametrize("via,committed,frames,final", [
    pytest.param(via, *case, id=f"{via}-{name}")
    for name, case in _FOLDS.items()
    for via in ("push", "transaction") if via == "transaction" or len(case[1]) == 1
])
def test_a_write_is_validated_against_its_fold(client, schema_name, via, committed, frames, final):
    """The rows of one push, and the frames of one transaction, fold together
    before any claim is checked: only the holders the whole write leaves count."""
    tid, schema = _raw_table(client, schema_name)
    if committed:
        client.push(tid, gnitz.ZSetBatch(schema).extend([{"pk": p, "val": v} for p, v in committed]))
    batches = [gnitz.ZSetBatch(schema).extend([{"pk": p, "val": v, "_weight": w} for p, v, w in frame])
               for frame in frames]

    def run():
        if via == "push":
            client.push(tid, batches[0])
        else:
            with client.transaction() as txn:
                for b in batches:
                    txn.push(tid, b)

    if final is None:
        with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
            run()
        final = committed
    else:
        run()
    assert bag(client.scan(tid)) == dict.fromkeys(final, 1)


def test_bulk_colliding_fresh_pks_rejected(client, schema_name):
    """A re-run bulk load: 2000 fresh PKs re-claiming 2000 committed values,
    none of which the batch frees. Every span comes back occupied by a holder
    the bundle does not retire, so the whole push is rejected — and one
    occupancy probe decides all 2000."""
    tid, schema = _raw_table(client, schema_name)
    n = 2000
    committed = [(i, i) for i in range(n)]
    client.push(tid, gnitz.ZSetBatch(schema).extend([{"pk": p, "val": v} for p, v in committed]))
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        client.push(tid, gnitz.ZSetBatch(schema).extend([{"pk": n + i, "val": i} for i in range(n)]))
    assert bag(client.scan(tid)) == dict.fromkeys(committed, 1)


_UNIQUE_A = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT UNIQUE)"


@pytest.mark.parametrize("ddl,seed,stmt,final", [
    # Every occupied span resolves to a holder the same bundle retires.
    (_UNIQUE_A, [(1, 1), (2, 2), (3, 3)], "UPDATE t SET a = a + 1", [(1, 2), (2, 3), (3, 4)]),
    # Two rows exchange their values in one batch of same-PK upserts.
    (_UNIQUE_A, [(1, 1), (2, 2), (3, 3)], "UPDATE t SET a = 4 - a", [(1, 3), (2, 2), (3, 1)]),
    # A composite span, declared as a table constraint.
    ("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL, UNIQUE (a, b))",
     [(1, 7, 1), (2, 7, 2), (3, 7, 3)], "UPDATE t SET b = b + 1", [(1, 7, 2), (2, 7, 3), (3, 7, 4)]),
], ids=["shift", "swap", "composite-shift"])
def test_a_bulk_transfer_within_one_statement_is_admitted(client, schema_name, ddl, seed, stmt, final):
    client.execute_sql(ddl, schema_name=schema_name)
    insert(client, schema_name, "t", seed)
    client.execute_sql(stmt, schema_name=schema_name)
    assert bag(scanned(client, schema_name, "t")) == dict.fromkeys(final, 1)


def test_a_wide_pk_behind_the_index_is_compared_whole(client, schema_name):
    """`(7,7,100)` and `(7,7,200)` share their first 16 bytes. A value held
    under one wide PK is refused under another, and moving the second row's
    value onto the first is refused too — a 16-byte-truncated holder compare
    would misread that collision as the row's own entry. A row rewriting its own
    unchanged value is admitted, since it retracts that entry itself."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL,"
        " c BIGINT UNSIGNED NOT NULL, val BIGINT UNSIGNED NOT NULL, PRIMARY KEY (a, b, c)); "
        "CREATE UNIQUE INDEX ON t(val)", schema_name=sn)
    insert(client, sn, "t", [(7, 7, 100, 10), (7, 7, 200, 42)])
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        client.execute_sql("INSERT INTO t VALUES (2, 2, 2, 42)", schema_name=sn)
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        client.execute_sql("UPDATE t SET val = 42 WHERE a = 7 AND b = 7 AND c = 100", schema_name=sn)
    client.execute_sql("UPDATE t SET val = 10 WHERE a = 7 AND b = 7 AND c = 100", schema_name=sn)
    assert bag(scanned(client, sn, "t")) == {(7, 7, 100, 10): 1, (7, 7, 200, 42): 1}


# ── Resolving the holder a probe reports ─────────────────────────────────────
#
# A NULL indexed cell must not claim the all-zero key image, and the holder a
# probe reports must be the one that currently holds the value.

# NULL-valued rows spread over a PK range wide enough to reach every worker.
_NULLS = [(pk, None) for pk in range(10_000, 10_048)]
_HOLDER_PK = 999983
_UPSERT_A = " ON CONFLICT (pk) DO UPDATE SET a = EXCLUDED.a"


def _holder_table(client, sn, col_type):
    client.execute_sql(
        f"CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a {col_type}); CREATE UNIQUE INDEX ix ON t(a)",
        schema_name=sn)
    return client.resolve_table(sn, "t")[0]


# `(column type, the value whose order-preserving image is all-zero for it, that
# value's unsigned native image)`. The last is what the seek API takes: its key
# values are `u128`, the zero-extended cell the engine reads out of a row, so a
# signed column's negative value arrives as its two's-complement image.
@pytest.mark.parametrize("col_type,value,seek_key", [
    ("BIGINT UNSIGNED", 0, 0),
    ("INT", -2147483648, 1 << 31),
])
def test_the_zero_image_holder_survives_a_flood_of_nulls(client, schema_name, col_type, value, seek_key):
    """The index is NULL-distinct (a NULL row has no entry at all), so both a
    direct seek and an UPDATE/DELETE by the zero-image value must reach exactly
    the holder's one row, however many NULL rows were written after it."""
    sn = schema_name
    tid = _holder_table(client, sn, col_type)
    insert(client, sn, "t", [(_HOLDER_PK, value)])
    insert(client, sn, "t", _NULLS)

    assert bag(client.seek_by_index(tid, [1], [seek_key])) == {(_HOLDER_PK, value): 1}
    for stmt in (f"UPDATE t SET a = {value} WHERE a = {value}", f"DELETE FROM t WHERE a = {value}"):
        res = client.execute_sql(stmt, schema_name=sn)
        assert (res[0]["type"], res[0]["count"]) == ("RowsAffected", 1), stmt
    assert bag(scanned(client, sn, "t")) == dict.fromkeys(_NULLS, 1)


def test_the_constraint_holds_under_the_same_null_flood(client, schema_name):
    """A bundle that moves a second committed row's value AND claims the
    holder's value for a fresh PK must be rejected — the second row is what
    makes the bundle touch a committed PK, so the check reaches the holder
    rather than stopping at the claim."""
    sn, other, fresh = schema_name, 1, 2
    _holder_table(client, sn, "BIGINT UNSIGNED")
    committed = [(_HOLDER_PK, 0), (other, 77)] + _NULLS
    insert(client, sn, "t", committed)
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        client.execute_sql(f"INSERT INTO t VALUES ({other}, 88), ({fresh}, 0)" + _UPSERT_A, schema_name=sn)
    assert bag(scanned(client, sn, "t")) == dict.fromkeys(committed, 1)


def test_a_value_moved_across_the_drop_index_window(client, schema_name):
    """The value is deleted and re-inserted under a different PK while no
    index exists at all. Nothing rewrites the moved row through the index
    before the re-create, so both a direct seek and a bundle claiming the
    value must name the NEW holder, not the pre-DROP one."""
    sn, other, fresh, moved = schema_name, 555, 556, 31337
    tid = _holder_table(client, sn, "BIGINT NOT NULL")
    insert(client, sn, "t", [(_HOLDER_PK, 7), (other, 77)])
    client.execute_sql(
        f"DROP INDEX ix; DELETE FROM t WHERE pk = {_HOLDER_PK}; INSERT INTO t VALUES ({moved}, 7); "
        "CREATE UNIQUE INDEX ix ON t(a)", schema_name=sn)

    assert bag(client.seek_by_index(tid, [1], [7])) == {(moved, 7): 1}
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        client.execute_sql(f"INSERT INTO t VALUES ({other}, 88), ({fresh}, 7)" + _UPSERT_A, schema_name=sn)
    assert bag(scanned(client, sn, "t")) == {(moved, 7): 1, (other, 77): 1}


def test_composite_unique_holder_split(client, schema_name):
    """A composite span is wider than one column, so the `[span || holder PK]`
    split must land at the whole span's width."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL); "
        "CREATE UNIQUE INDEX ON t(a, b)", schema_name=sn)
    insert(client, sn, "t", [(1, 1, 1), (2, 2, 2)])
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        client.execute_sql(
            f"INSERT INTO t VALUES (2, 3, 3), ({_HOLDER_PK}, 1, 1)"
            " ON CONFLICT (pk) DO UPDATE SET a = EXCLUDED.a, b = EXCLUDED.b", schema_name=sn)
    assert bag(scanned(client, sn, "t")) == {(1, 1, 1): 1, (2, 2, 2): 1}


def test_replicated_owner_holder_is_deduped_and_decisive(client, schema_name):
    """Every worker holds a replicated table's whole index, so every worker
    answers for the same span. The `W` identical answers must collapse to one
    holder, and that holder must decide the verdict: the same bundle shape is
    rejected when the holder keeps the value and accepted when it releases it
    in the same bundle."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL) WITH (replicated = true); "
        "CREATE UNIQUE INDEX ON t(a)", schema_name=sn)
    insert(client, sn, "t", [(1, 42), (2, 77)])

    # pk=1 keeps 42 → the fresh claim collides.
    with pytest.raises(gnitz.GnitzError, match=_VIOLATION):
        client.execute_sql(f"INSERT INTO t VALUES (2, 88), ({_HOLDER_PK}, 42)" + _UPSERT_A, schema_name=sn)
    assert bag(scanned(client, sn, "t")) == {(1, 42): 1, (2, 77): 1}

    # Same bundle, but the holder releases 42 in it → accepted.
    client.execute_sql(f"INSERT INTO t VALUES (1, 99), ({_HOLDER_PK}, 42)" + _UPSERT_A, schema_name=sn)
    assert bag(scanned(client, sn, "t")) == {(1, 99): 1, (2, 77): 1, (_HOLDER_PK, 42): 1}
