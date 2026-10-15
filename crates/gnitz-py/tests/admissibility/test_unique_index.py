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

import contextlib

import pytest
import gnitz
from _catalog import family
from _read import bag, scanned
from _schemas import KV
from _serverproc import NEEDS_MULTI, TINY_SCAN_CHUNKS
from _sql import insert

_T = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)"
_T_NULLABLE = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT)"
_TAB = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT, b BIGINT)"
_AB_PK = ("CREATE TABLE t (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL,"
          " payload BIGINT, PRIMARY KEY (a, b))")

_T_INT = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val INT NOT NULL)"

_CREATE_DUP = "contains duplicate values"


def _violation(cols="val"):
    """The refusal of a write claiming a value the index over `cols` holds."""
    return pytest.raises(gnitz.GnitzIntegrityError, match=f"Unique index violation on '[^']*' column '{cols}'")


def _indices(client):
    """The bag of index ids `t` owns."""
    return family(client, "indices", "index_id", where=f"owner_id = {client.resolve_table('t')[0]}")


def _raw_table(client):
    """Raw `t` over `KV` + a SQL unique index on `val`. Returns its tid."""
    tid = client.create_table("t", KV)
    client.execute_sql("CREATE UNIQUE INDEX ON t(val)")
    return tid


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
    # A 4-byte key: the spans sort at their own width, across zero.
    "INT, one pair among many": (
        _T_INT, "val", [(pk, pk - 150) for pk in range(1, 301)] + [(1000, -7)], (1001, -7), None),
    "INT, all distinct": (_T_INT, "val", [(pk, pk - 150) for pk in range(1, 301)], (900001, -100), (900002, 500)),
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
        client, ddl, cols, seed, claim, fresh):
    client.execute_sql(ddl)
    insert(client, "t", seed)
    create = f"CREATE UNIQUE INDEX ON t({cols})"
    if fresh is None:
        # The qualified table is what tells the author what to change.
        with pytest.raises(gnitz.GnitzIntegrityError, match=f"{client.schema}.t.*{_CREATE_DUP}"):
            client.execute_sql(create)
        assert _indices(client) == {}
        insert(client, "t", [claim])
        landed = claim
    else:
        client.execute_sql(create)
        insert(client, "t", [fresh])
        if claim is not None:
            with _violation(cols):
                insert(client, "t", [claim])
        landed = fresh
    # Every worker still answers, and nothing a verdict refused landed.
    assert bag(scanned(client, "t")) == dict.fromkeys(seed + [landed], 1)


_n = 2000

# `(server environment, seeded rows — each value held once, a row claiming a
# seeded value, a row repeating one on a far-away PK)`.
_MERGES = {
    # With 3-row scan chunks, every worker streams its spans in 3-key frames, a
    # multi-frame continuation train.
    "multi-frame key train": (
        TINY_SCAN_CHUNKS, [(pk, pk * 7) for pk in range(1, 201)], (201, 7), (1000, 700)),
    # With a 256-byte sort budget, a partition far past it drives the external
    # merge sort: a wide PK spread makes hundreds of spans per worker, so many
    # spilled runs and a k-way merge over them. The budget is honoured in every
    # build, so this bites a release server too.
    "external sort spill": (
        {"GNITZ_KEY_SPANS_SPILL_BYTES": "256"}, [(i * 7 + 1, i) for i in range(_n)],
        (_n * 10, 5), (_n * 10 + 3, 0)),
}


@pytest.mark.parametrize("env,seed,claim,repeat", _MERGES.values(), ids=_MERGES.keys())
def test_the_preflight_merge_stays_exact_across_its_own_boundaries(own_server, env, seed, claim, repeat):
    """The merge must stay exact wherever its frame or run boundaries happen to
    fall, on both verdicts: an all-distinct seed creates and then enforces, and
    one duplicate pair on far-apart PKs — adjacent only in the merged stream —
    is still caught."""
    with gnitz.connect(own_server.start(extra_env=env).target) as srv:
        srv.execute_sql(_T)
        insert(srv, "t", seed)
        srv.execute_sql("CREATE UNIQUE INDEX ix ON t(val)")
        with _violation():
            insert(srv, "t", [claim])
        srv.execute_sql("DROP INDEX ix")

        insert(srv, "t", [repeat])
        with pytest.raises(gnitz.GnitzIntegrityError, match=_CREATE_DUP):
            srv.execute_sql("CREATE UNIQUE INDEX ix ON t(val)")
        assert _indices(srv) == {}


def test_worker_fault_mid_preflight(own_server):
    """An injected worker fault during the pre-flight scan must surface as a
    client error with no index created, no filter seeded, and every worker
    drained (not wedged): the table stays fully usable, and the PK
    short-circuit — which never fans out — still succeeds."""
    with gnitz.connect(own_server.start(extra_env={"GNITZ_INJECT_KEY_SPANS_ERROR": "1"}).target) as srv:
        srv.execute_sql(_T)
        seed = [(pk, pk) for pk in range(1, 33)]
        insert(srv, "t", seed)
        with pytest.raises(gnitz.GnitzRefusedError, match="injected key-spans fault"):
            srv.execute_sql("CREATE UNIQUE INDEX ON t(val)")
        assert _indices(srv) == {}
        # No filter was seeded and no index exists, so a duplicate value is
        # accepted — no partial constraint leaked out of the failed DDL.
        insert(srv, "t", [(100, 1)])
        # All workers answer a full scan: nobody is wedged on a half-drained
        # pre-flight train.
        assert bag(scanned(srv, "t")) == dict.fromkeys(seed + [(100, 1)], 1)
        # The PK-covering short-circuit returns before any fan-out, so it
        # succeeds even while every worker's scan path is faulted — for the PK
        # exactly, and for any column list containing it.
        srv.execute_sql("CREATE UNIQUE INDEX ON t(pk); CREATE UNIQUE INDEX ON t(pk, val)")
        assert len(_indices(srv)) == 2


# ── Enforcement once the index exists ────────────────────────────────────────

_T_IX = _T + "; CREATE UNIQUE INDEX ON t(val)"
_UPSERT = " ON CONFLICT (pk) DO UPDATE SET val = EXCLUDED.val"
_UNIQUE_A = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT UNIQUE)"
_TWO_IX = ("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL); "
           "CREATE UNIQUE INDEX ON t(a); CREATE UNIQUE INDEX ON t(b)")
_WIDE_PK = ("CREATE TABLE t (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL,"
            " c BIGINT UNSIGNED NOT NULL, val BIGINT UNSIGNED NOT NULL, PRIMARY KEY (a, b, c)); "
            "CREATE UNIQUE INDEX ON t(val)")
# `(7,7,100)` and `(7,7,200)` share their first 16 bytes, so a 16-byte-truncated
# holder compare would misread a collision between them as the row's own entry.
_WIDE_ROWS = [(7, 7, 100, 10), (7, 7, 200, 42)]
_HOLDER = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a {}); CREATE UNIQUE INDEX ix ON t(a)"
_HOLDER_PK = 999983
_UPSERT_A = " ON CONFLICT (pk) DO UPDATE SET a = EXCLUDED.a"
# NULL-valued rows spread over a PK range wide enough to reach every worker.
_NULLS = [(pk, None) for pk in range(10_000, 10_048)]
_REPLICATED = ("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL) WITH (replicated = true); "
               "CREATE UNIQUE INDEX ON t(a)")

# `(ddl, committed rows, statement, rows after — or the columns of the index
# that refuses it, every row then kept)`.
_CLAIMS = {
    # A row rewriting its own value collides only with its own committed entry,
    # which the write retracts.
    "keeps its own value": (_T_IX, [(1, 42), (2, 99)], "INSERT INTO t VALUES (1, 42)" + _UPSERT, [(1, 42), (2, 99)]),
    # Moving to a free value vacates the old one.
    "moves to a free value": (_T_IX, [(1, 42), (2, 99)], "INSERT INTO t VALUES (1, 7)" + _UPSERT, [(1, 7), (2, 99)]),
    # A DELETE releases the value it held for a later statement.
    "takes a value a DELETE freed": (
        _T_IX, [(1, 42), (2, 99)], "DELETE FROM t WHERE pk = 1; INSERT INTO t VALUES (3, 42)", [(2, 99), (3, 42)]),
    # Taking a value another live PK holds is the violation, whichever partition
    # the new holder lands on.
    "takes a held value": (_T_IX, [(1, 42), (2, 99)], "INSERT INTO t VALUES (1, 99)" + _UPSERT, "val"),
    "a fresh PK takes a held value": (_T_IX, [(1, 42), (2, 99)], "INSERT INTO t VALUES (1000000, 42)", "val"),
    "update takes a held value": (_T_IX, [(1, 42), (2, 99)], "UPDATE t SET val = 99 WHERE pk = 1", "val"),
    # Two rows claiming one *new* value: absent from committed storage, so the
    # occupancy probe answers "free" and only the in-batch check can catch it.
    "two upserts claim one new value": (
        _T_IX, [(1, 42), (2, 99)], "INSERT INTO t VALUES (1, 55), (2, 55)" + _UPSERT, "val"),
    "two inserts claim one new value": (_T_IX, [(1, 42)], "INSERT INTO t VALUES (3, 7), (4, 7)", "val"),
    # A column-level `UNIQUE` registers the same index `CREATE UNIQUE INDEX` does.
    "a column-level declaration": (_UNIQUE_A, [(1, 42)], "INSERT INTO t VALUES (2, 42)", "a"),
    # Each of two indices refuses for itself.
    "the first of two indices": (_TWO_IX, [(1, 10, 20), (2, 11, 21)], "INSERT INTO t VALUES (3, 10, 22)", "a"),
    "the second of two indices": (_TWO_IX, [(1, 10, 20), (2, 11, 21)], "INSERT INTO t VALUES (4, 12, 20)", "b"),
    # A bulk transfer: every occupied span resolves to a holder the same bundle
    # retires.
    "a shift": (_UNIQUE_A, [(1, 1), (2, 2), (3, 3)], "UPDATE t SET a = a + 1", [(1, 2), (2, 3), (3, 4)]),
    "a swap": (_UNIQUE_A, [(1, 1), (2, 2), (3, 3)], "UPDATE t SET a = 4 - a", [(1, 3), (2, 2), (3, 1)]),
    "a composite shift": (
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL, UNIQUE (a, b))",
        [(1, 7, 1), (2, 7, 2), (3, 7, 3)], "UPDATE t SET b = b + 1", [(1, 7, 2), (2, 7, 3), (3, 7, 4)]),
    # The holder a probe reports is compared at the PK's full width.
    "a wide PK takes a held value": (_WIDE_PK, _WIDE_ROWS, "INSERT INTO t VALUES (2, 2, 2, 42)", "val"),
    "a wide PK takes its neighbour's value": (
        _WIDE_PK, _WIDE_ROWS, "UPDATE t SET val = 42 WHERE a = 7 AND b = 7 AND c = 100", "val"),
    "a wide PK keeps its own value": (
        _WIDE_PK, _WIDE_ROWS, "UPDATE t SET val = 10 WHERE a = 7 AND b = 7 AND c = 100", _WIDE_ROWS),
    # The `[span || holder PK]` split lands at the whole composite span's width.
    "a composite span's holder": (
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL); "
        "CREATE UNIQUE INDEX ON t(a, b)",
        [(1, 1, 1), (2, 2, 2)],
        f"INSERT INTO t VALUES (2, 3, 3), ({_HOLDER_PK}, 1, 1) ON CONFLICT (pk) DO UPDATE SET a = EXCLUDED.a, "
        "b = EXCLUDED.b", "a, b"),
    # A bundle that moves a second committed row's value and claims the
    # holder's for a fresh PK: the second row is what makes it touch a committed
    # PK, so the check reaches the holder — past a flood of NULLs, none of which
    # may claim the zero value's all-zero key image.
    "the holder past a NULL flood": (
        _HOLDER.format("BIGINT UNSIGNED"), [(_HOLDER_PK, 0), (1, 77)] + _NULLS,
        "INSERT INTO t VALUES (1, 88), (2, 0)" + _UPSERT_A, "a"),
    # Every worker holds a replicated table's whole index and answers for the
    # same span; the `W` identical answers collapse to one holder, which decides
    # the verdict.
    "a replicated holder keeps its value": pytest.param(
        _REPLICATED, [(1, 42), (2, 77)], f"INSERT INTO t VALUES (2, 88), ({_HOLDER_PK}, 42)" + _UPSERT_A, "a",
        marks=NEEDS_MULTI),
    "a replicated holder releases its value": pytest.param(
        _REPLICATED, [(1, 42), (2, 77)], f"INSERT INTO t VALUES (1, 99), ({_HOLDER_PK}, 42)" + _UPSERT_A,
        [(1, 99), (2, 77), (_HOLDER_PK, 42)], marks=NEEDS_MULTI),
}


@pytest.mark.parametrize("ddl,committed,stmt,after", _CLAIMS.values(), ids=_CLAIMS.keys())
def test_a_claim_is_admitted_only_if_it_ends_up_held_once(client, ddl, committed, stmt, after):
    client.execute_sql(ddl)
    insert(client, "t", committed)
    refused = isinstance(after, str)
    with _violation(after) if refused else contextlib.nullcontext():
        client.execute_sql(stmt)
    assert bag(scanned(client, "t")) == dict.fromkeys(committed if refused else after, 1)


def test_anticorrelated_multi_source_index_merge(client):
    """The duplicate check seeks the indexed value in the secondary index, whose
    PK is compound `(value, src_pk)`. With values inserted one per push and
    falling as the PK rises, the check merges N runs. If that merge ordered by
    the raw u128 PK — `(src_pk, value)` — the seek for a smaller value would land
    on a larger value's run, report "absent", and let a duplicate through."""
    client.execute_sql(_T_IX)
    committed = [(i + 1, (6 - i) * 100) for i in range(6)]  # values 600, 500, ..., 100
    for row in committed:
        insert(client, "t", [row])
    # Including the smallest values, which a u128 merge would mask.
    for j, (_, val) in enumerate(committed):
        with _violation():
            insert(client, "t", [(1000 + j, val)])
    assert bag(scanned(client, "t")) == dict.fromkeys(committed, 1)


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
def test_a_write_is_validated_against_its_fold(client, via, committed, frames, final):
    """The rows of one push, and the frames of one transaction, fold together
    before any claim is checked: only the holders the whole write leaves count."""
    tid = _raw_table(client)
    if committed:
        client.push(tid, gnitz.ZSetBatch(KV).extend([{"pk": p, "val": v} for p, v in committed]))
    batches = [gnitz.ZSetBatch(KV).extend([{"pk": p, "val": v, "_weight": w} for p, v, w in frame])
               for frame in frames]

    with _violation() if final is None else contextlib.nullcontext():
        if via == "push":
            client.push(tid, batches[0])
        else:
            with client.transaction() as txn:
                for b in batches:
                    txn.push(tid, b)
    assert bag(client.scan(tid, KV)) == dict.fromkeys(final or committed, 1)


def test_bulk_colliding_fresh_pks_rejected(client):
    """A re-run bulk load: 2000 fresh PKs re-claiming 2000 committed values,
    none of which the batch frees. Every span comes back occupied by a holder
    the bundle does not retire, so the whole push is rejected — and one
    occupancy probe decides all 2000."""
    tid = _raw_table(client)
    n = 2000
    committed = [(i, i) for i in range(n)]
    client.push(tid, gnitz.ZSetBatch(KV).extend([{"pk": p, "val": v} for p, v in committed]))
    with _violation():
        client.push(tid, gnitz.ZSetBatch(KV).extend([{"pk": n + i, "val": i} for i in range(n)]))
    assert bag(client.scan(tid, KV)) == dict.fromkeys(committed, 1)


# ── Resolving the holder a probe reports ─────────────────────────────────────
#
# A NULL indexed cell must not claim the all-zero key image, and the holder a
# probe reports must be the one that currently holds the value.

# `(column type, the value whose order-preserving image is all-zero for it)`.
@pytest.mark.parametrize("col_type,value", [
    ("BIGINT UNSIGNED", 0),
    ("INT", -2147483648),
])
def test_the_zero_image_holder_survives_a_flood_of_nulls(client, col_type, value):
    """The index is NULL-distinct (a NULL row has no entry at all), so both a
    direct seek and an UPDATE/DELETE by the zero-image value must reach exactly
    the holder's one row, however many NULL rows were written after it."""
    client.execute_sql(_HOLDER.format(col_type))
    insert(client, "t", [(_HOLDER_PK, value)])
    insert(client, "t", _NULLS)

    tid, schema = client.resolve_table("t")
    assert bag(client.seek_by_index(tid, schema, [1], [value])) == {(_HOLDER_PK, value): 1}
    for stmt in (f"UPDATE t SET a = {value} WHERE a = {value}", f"DELETE FROM t WHERE a = {value}"):
        res = client.execute_sql(stmt)
        assert (res[0]["type"], res[0]["count"]) == ("RowsAffected", 1), stmt
    assert bag(scanned(client, "t")) == dict.fromkeys(_NULLS, 1)


def test_a_value_moved_across_the_drop_index_window(client):
    """The value is deleted and re-inserted under a different PK while no
    index exists at all. Nothing rewrites the moved row through the index
    before the re-create, so both a direct seek and a bundle claiming the
    value must name the NEW holder, not the pre-DROP one."""
    other, fresh, moved = 555, 556, 31337
    client.execute_sql(_HOLDER.format("BIGINT NOT NULL"))
    insert(client, "t", [(_HOLDER_PK, 7), (other, 77)])
    client.execute_sql(
        f"DROP INDEX ix; DELETE FROM t WHERE pk = {_HOLDER_PK}; INSERT INTO t VALUES ({moved}, 7); "
        "CREATE UNIQUE INDEX ix ON t(a)")

    tid, schema = client.resolve_table("t")
    assert bag(client.seek_by_index(tid, schema, [1], [7])) == {(moved, 7): 1}
    with _violation("a"):
        client.execute_sql(f"INSERT INTO t VALUES ({other}, 88), ({fresh}, 7)" + _UPSERT_A)
    assert bag(scanned(client, "t")) == {(moved, 7): 1, (other, 77): 1}
