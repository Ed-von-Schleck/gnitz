"""Capacity-bounded views: `CREATE VIEW … WITH (capacity = '…')`.

The claim under test is that capacity is invisible to correctness. Every test
here runs a bounded view beside an unbounded twin over the same churn and
requires the two to agree through every read verb — full scan, point seek, and
the ad-hoc `SELECT … WHERE / ORDER BY / LIMIT / aggregate` path — while the
bounded one's registered shard bytes stay under its cap.

These tests need the store to reach the *disk* regime, which at the production
32 MiB RAM-tier ceiling would take megabytes of churn. `GNITZ_RAM_TIER_BYTES`
shrinks that ceiling for the server they start, so a few thousand rows spill,
compact, and dehydrate.
"""
import os
import struct

import pytest
import gnitz
from _serverproc import ServerProc
from _uid import uid as _uid

# A view's output store spills when its RAM tier crosses this ceiling — which
# only happens at a memtable fold or a checkpoint's ephemeral round, not at every
# tick. So the checkpoint threshold is squeezed too: together they give a few
# thousand rows the many spills the capacity sweep needs (it budgets itself to
# one push-down per spill, so dehydration takes several).
_SWEEP_ENV = {"GNITZ_RAM_TIER_BYTES": "1024", "GNITZ_CHECKPOINT_BYTES": str(32 * 1024)}

# Shard header words (storage/repr/layout.rs). A skeleton shard stamps the high
# bit of OFF_FILE_NPC.
_OFF_FILE_NPC = 32
_SHARD_FLAG_SKELETON = 1 << 63




@pytest.fixture
def bounded_server(server_dirs):
    """A server whose stores spill to disk almost immediately, so the capacity
    sweep actually runs."""
    data_dir, sock_path = server_dirs
    proc = ServerProc(data_dir, sock_path, extra_env=dict(_SWEEP_ENV))
    proc.start()
    try:
        yield proc
    finally:
        proc.stop()


@pytest.fixture
def bounded_client(bounded_server):
    with gnitz.connect(bounded_server.sock_path) as conn:
        yield conn


# ── on-disk inspection ───────────────────────────────────────────────────────


def _shard_files(data_dir, view_id):
    """Every `.db` shard under any child directory of `view_id`'s relation dir,
    on every worker. The directory name carries the id, so no catalog read is
    needed."""
    out = []
    needle = f"_{view_id}"
    for root, _dirs, files in os.walk(data_dir):
        parts = root.split(os.sep)
        if not any(p.endswith(needle) for p in parts):
            continue
        for f in files:
            if f.endswith(".db"):
                out.append(os.path.join(root, f))
    return out


def _is_skeleton(path):
    with open(path, "rb") as fh:
        fh.seek(_OFF_FILE_NPC)
        return bool(struct.unpack("<Q", fh.read(8))[0] & _SHARD_FLAG_SKELETON)


def _view_dir_bytes(data_dir, view_id):
    return sum(os.path.getsize(p) for p in _shard_files(data_dir, view_id))


def _any_skeleton(data_dir, view_id):
    return any(_is_skeleton(p) for p in _shard_files(data_dir, view_id))


# ── data helpers ─────────────────────────────────────────────────────────────


def _rows(client, vid):
    """Every live row of a relation as a sorted list of value tuples, weights
    included — the Z-set, not just the row set."""
    return sorted(tuple(r) + (r.weight,) for r in client.scan(vid))


def _seek_rows(client, vid, pk):
    return sorted(tuple(r) + (r.weight,) for r in client.seek(vid, pk))


def _sql_rows(client, sn, sql):
    res = client.execute_sql(sql, schema_name=sn)
    return sorted(tuple(r) for r in res)


def _churn(client, sn, lo, hi):
    """Insert keys `[lo, hi]` into `t` and `u`, then update and delete a slice of
    the range, so the views see inserts, retract/insert pairs and pure
    retractions. Each statement is its own settle, so each drives a spill."""
    for a in range(lo, hi + 1, 100):
        b = min(a + 99, hi)
        client.execute_sql(
            "INSERT INTO t VALUES " + ",".join(f"({i}, {i * 3}, 'body-{i:0>20}')" for i in range(a, b + 1)),
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO u VALUES " + ",".join(f"({i}, {i}, {i * 7})" for i in range(a, b + 1)),
            schema_name=sn,
        )
    span = hi - lo + 1
    client.execute_sql(f"UPDATE t SET v = v + 1 WHERE id >= {lo} AND id < {lo + span // 4}", schema_name=sn)
    client.execute_sql(f"UPDATE u SET w = w + 2 WHERE id >= {lo} AND id < {lo + span // 4}", schema_name=sn)
    client.execute_sql(f"DELETE FROM t WHERE id > {hi - span // 8}", schema_name=sn)
    client.execute_sql(f"DELETE FROM u WHERE id > {hi - span // 16}", schema_name=sn)


def _drain(client, sn, base, rounds=30):
    """Trailing writes so the sweep can catch up. It budgets itself to one
    push-down per spill, so whatever was written since the last few spills is
    still hydrated when ingest stops — the residue is bounded by the recent write
    burst, not by the store. These rounds give it the spills to sink that burst.
    Returns the first key past the range it used."""
    for k in range(rounds):
        lo = base + k * 20
        client.execute_sql(
            "INSERT INTO t VALUES " + ",".join(f"({i}, {i * 3}, 'tail-{i:0>20}')" for i in range(lo, lo + 20)),
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO u VALUES " + ",".join(f"({i}, {i}, {i * 7})" for i in range(lo, lo + 20)),
            schema_name=sn,
        )
    return base + rounds * 20


def _setup(client, sn, view_sql, *, capacity="1 KB"):
    """`t`/`u` plus a bounded view `b` and its unbounded twin `p`, both over the
    same body. Returns `(bounded_vid, plain_vid)`."""
    client.create_schema(sn)
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL, body TEXT NOT NULL)",
        schema_name=sn,
    )
    client.execute_sql(
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, tid BIGINT NOT NULL, w BIGINT NOT NULL)",
        schema_name=sn,
    )
    client.execute_sql(f"CREATE VIEW b WITH (capacity = '{capacity}') AS {view_sql}", schema_name=sn)
    client.execute_sql(f"CREATE VIEW p AS {view_sql}", schema_name=sn)
    bid, _ = client.resolve_table(sn, "b")
    pid, _ = client.resolve_table(sn, "p")
    return bid, pid


# ── parity ───────────────────────────────────────────────────────────────────


LINEAR_BODY = "SELECT id, v, body FROM t WHERE v > 10"
JOIN_BODY = "SELECT t.id, t.body, u.w FROM t JOIN u ON t.id = u.tid"


@pytest.mark.parametrize("body", [LINEAR_BODY, JOIN_BODY], ids=["linear", "join"])
def test_bounded_view_matches_its_unbounded_twin(bounded_client, bounded_server, body):
    """Full scans, point seeks and the ad-hoc scan-spec path agree at every step
    of a churn stream, and the bounded store really did dehydrate."""
    sn = "s" + _uid()
    bid, pid = _setup(bounded_client, sn, body)

    for lo, hi in ((1, 600), (601, 1600)):
        _churn(bounded_client, sn, lo, hi)
        assert _rows(bounded_client, bid) == _rows(bounded_client, pid), f"full scan through {hi}"

    # Point seeks, on keys spread across the whole range — the low ones are the
    # coldest by write recency, so they are the ones most likely dehydrated.
    keys = sorted({r[0] for r in bounded_client.scan(pid)})
    for pk in keys[:5] + keys[len(keys) // 2 : len(keys) // 2 + 5] + keys[-5:]:
        assert _seek_rows(bounded_client, bid, pk) == _seek_rows(bounded_client, pid, pk), f"seek {pk}"
    # A key the view does not hold.
    assert _seek_rows(bounded_client, bid, 10**9) == _seek_rows(bounded_client, pid, 10**9)

    # The ScanSpec path: predicate, ORDER BY / LIMIT, and an aggregate fold.
    for tmpl in [
        "SELECT id FROM {v} WHERE id > 100 AND id < 300",
        "SELECT id, w FROM {v} ORDER BY id LIMIT 25" if body is JOIN_BODY else "SELECT id, v FROM {v} ORDER BY id LIMIT 25",
        "SELECT COUNT(*) FROM {v}",
        "SELECT id FROM {v} WHERE id IN (3, 17, 251, 999999)",
    ]:
        assert _sql_rows(bounded_client, sn, tmpl.format(v="b")) == _sql_rows(
            bounded_client, sn, tmpl.format(v="p")
        ), tmpl

    assert _any_skeleton(bounded_server.data_dir, bid), "the bounded store must have dehydrated"
    assert not _any_skeleton(bounded_server.data_dir, pid), "the twin must never dehydrate"


def test_all_null_and_all_zero_payloads_survive_dehydration(bounded_client, bounded_server):
    """Both payload-comparator arms, end to end: a view whose projected payload is
    all-NULL for some rows (the `Generic` arm's `Equal` case against a skeleton
    row) and one whose payload is all-zero non-nullable integers (the
    `FixedIntNonnull` arm's)."""
    sn = "s" + _uid()
    bounded_client.create_schema(sn)
    bounded_client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, z BIGINT NOT NULL, s TEXT)",
        schema_name=sn,
    )
    bounded_client.execute_sql(
        "CREATE VIEW b_nul WITH (capacity = '1 KB') AS SELECT id, s FROM t", schema_name=sn
    )
    bounded_client.execute_sql("CREATE VIEW p_nul AS SELECT id, s FROM t", schema_name=sn)
    bounded_client.execute_sql(
        "CREATE VIEW b_zero WITH (capacity = '1 KB') AS SELECT id, z FROM t", schema_name=sn
    )
    bounded_client.execute_sql("CREATE VIEW p_zero AS SELECT id, z FROM t", schema_name=sn)

    for lo in range(1, 1201, 200):
        hi = min(lo + 199, 1200)
        vals = []
        for i in range(lo, hi + 1):
            # Every third row is all-NULL / all-zero in the projected payload.
            s = "NULL" if i % 3 == 0 else f"'text-{i:0>20}'"
            vals.append(f"({i}, {0 if i % 3 == 0 else i}, {s})")
        bounded_client.execute_sql("INSERT INTO t VALUES " + ",".join(vals), schema_name=sn)

    for b, p in [("b_nul", "p_nul"), ("b_zero", "p_zero")]:
        bid, _ = bounded_client.resolve_table(sn, b)
        pid, _ = bounded_client.resolve_table(sn, p)
        assert _rows(bounded_client, bid) == _rows(bounded_client, pid), b
        assert _any_skeleton(bounded_server.data_dir, bid), f"{b} must have dehydrated"


def test_a_single_tenant_uuid_pk_dehydrates_gradually(bounded_client, bounded_server):
    """The degenerate PK shape: `PRIMARY KEY (a UUID, b UUID)` with one distinct
    `a`, so every row's leading sixteen OPK bytes are identical.

    A guard partition keyed on a truncated prefix cannot cut such a store — it is
    one guard end to end, the byte target stops bounding it, and the first sweep
    dehydrates the whole view at once, so a read recomputes everything from the
    source and the view stops being materialized. Keyed on the whole PK the store
    partitions on the trailing column like any other, so the sweep leaves
    hydrated guards behind while it evicts the coldest.

    Reads are checked against the unbounded twin first: correctness is never a
    function of what is resident, whichever way the partition falls.
    """
    sn = "s" + _uid()
    bounded_client.create_schema(sn)
    bounded_client.execute_sql(
        "CREATE TABLE t (a UUID NOT NULL, b UUID NOT NULL, body TEXT NOT NULL, PRIMARY KEY (a, b))",
        schema_name=sn,
    )
    body = "SELECT a, b, body FROM t"
    bounded_client.execute_sql(f"CREATE VIEW bu WITH (capacity = '1 KB') AS {body}", schema_name=sn)
    bounded_client.execute_sql(f"CREATE VIEW pu AS {body}", schema_name=sn)
    bid, _ = bounded_client.resolve_table(sn, "bu")
    pid, _ = bounded_client.resolve_table(sn, "pu")

    # One tenant, so `a` is constant; `b` varies only in its low bytes, which are
    # the OPK bytes past 16. Inserted in batches so the view spills repeatedly.
    tenant = "11111111-1111-1111-1111-111111111111"
    n = 1200
    for lo in range(0, n, 100):
        vals = ",".join(
            f"('{tenant}', '00000000-0000-0000-0000-{i:012x}', 'body-{i:0>20}')" for i in range(lo, lo + 100)
        )
        bounded_client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)

    assert _rows(bounded_client, bid) == _rows(bounded_client, pid), "full scan"
    # Point lookups on the coldest, middle and newest keys, plus the ScanSpec
    # and aggregate paths. The wire `seek` verb takes a compound PK as its
    # native byte image, which would be a second spelling of the OPK encoding
    # here, so these go through SQL — the path a reader actually uses.
    for i in [0, n // 2, n - 1]:
        b = f"00000000-0000-0000-0000-{i:012x}"
        q = f"SELECT b, body FROM {{v}} WHERE a = '{tenant}' AND b = '{b}'"
        got = _sql_rows(bounded_client, sn, q.format(v="bu"))
        assert got == _sql_rows(bounded_client, sn, q.format(v="pu")), f"point lookup {b}"
        assert len(got) == 1, f"point lookup {b}: {got}"
    for q in ["SELECT COUNT(*) FROM {v}", "SELECT b FROM {v} ORDER BY b LIMIT 25"]:
        assert _sql_rows(bounded_client, sn, q.format(v="bu")) == _sql_rows(
            bounded_client, sn, q.format(v="pu")
        ), q

    # The sweep evicted something, and it did so a guard at a time: an
    # all-or-nothing store leaves not one hydrated shard behind.
    shards = _shard_files(bounded_server.data_dir, bid)
    assert any(_is_skeleton(p) for p in shards), "the bounded store must have dehydrated"
    assert any(not _is_skeleton(p) for p in shards), (
        "every shard is a skeleton — the store was one unsplittable guard, so the "
        "sweep had no smaller unit than the whole view"
    )


# ── capacity semantics ───────────────────────────────────────────────────────


def test_a_slack_capacity_never_dehydrates(bounded_client, bounded_server):
    sn = "s" + _uid()
    bid, pid = _setup(bounded_client, sn, LINEAR_BODY, capacity="1 GB")
    _churn(bounded_client, sn, 1, 1600)
    assert _rows(bounded_client, bid) == _rows(bounded_client, pid)
    assert not _any_skeleton(bounded_server.data_dir, bid), "1 GB is never reached"


def test_a_tight_capacity_holds_the_registered_bytes_down(bounded_server):
    """The bound is on the *registered* shard set — the shards the manifest names.
    A running store's directory additionally holds every compaction input
    superseded since the last checkpoint, which only the post-publish drain
    unlinks. So each figure here is taken after a graceful shutdown *and* a
    reboot, whose `gc_orphans` leaves exactly the registered set on disk.

    Sustained ingest does not let the bounded store climb back: the sweep runs at
    every spill, so its share of the twin's size does not grow with the data.
    """
    sn = "s" + _uid()

    def churn_and_measure(lo, hi):
        with gnitz.connect(bounded_server.sock_path) as c:
            if lo == 1:
                bid, pid = _setup(c, sn, LINEAR_BODY, capacity="1 KB")
            else:
                bid, _ = c.resolve_table(sn, "b")
                pid, _ = c.resolve_table(sn, "p")
            _churn(c, sn, lo, hi)
            _drain(c, sn, hi + 1, rounds=max(30, (hi - lo) // 100))
            assert _rows(c, bid) == _rows(c, pid), f"parity through {hi}"
        assert bounded_server.stop_graceful() == 0
        bounded_server.start()
        # Read both views before measuring: `start` returns as soon as the socket
        # exists, and a read drains whatever the boot still owes (a resume's
        # un-checkpointed tail, or a rebuild's backfill), so the directory is not
        # sampled mid-flight.
        with gnitz.connect(bounded_server.sock_path) as c:
            assert _rows(c, bid) == _rows(c, pid), "parity across the reboot"
        return _view_dir_bytes(bounded_server.data_dir, bid), _view_dir_bytes(bounded_server.data_dir, pid)

    # A quarter is generous headroom: both phases measure ~0.2 here, and the
    # residue is dominated by the recent write burst rather than by the store, so
    # the second (3x the data) does not come out worse than the first.
    bounded, twin = churn_and_measure(1, 4000)
    assert bounded < twin / 4, f"bounded {bounded} vs unbounded twin {twin}"

    grown, grown_twin = churn_and_measure(6001, 16000)
    assert grown < grown_twin / 4, f"bounded {grown} vs unbounded twin {grown_twin}"


# ── boot and backfill ────────────────────────────────────────────────────────


def test_a_bounded_view_survives_a_restart(bounded_client, bounded_server):
    """The store resumes with its skeleton shards, reads still agree with the
    twin, and the capacity is re-enforced once the view ticks again."""
    sn = "s" + _uid()
    bid, pid = _setup(bounded_client, sn, LINEAR_BODY)
    _churn(bounded_client, sn, 1, 1600)
    before = _rows(bounded_client, bid)
    assert before == _rows(bounded_client, pid)
    assert _any_skeleton(bounded_server.data_dir, bid)

    bounded_client.close()
    # Graceful: the final checkpoint publishes the view's skeleton shards, so the
    # boot resumes the store rather than rebuilding it.
    assert bounded_server.stop_graceful() == 0
    bounded_server.start()

    with gnitz.connect(bounded_server.sock_path) as c2:
        bid2, _ = c2.resolve_table(sn, "b")
        pid2, _ = c2.resolve_table(sn, "p")
        # The very first read after boot arrives before any post-boot tick, so
        # it exercises the lazily-recompiled plan cache.
        assert _rows(c2, bid2) == before
        assert _rows(c2, bid2) == _rows(c2, pid2)

        _churn(c2, sn, 1601, 4000)
        assert _rows(c2, bid2) == _rows(c2, pid2)
    assert bounded_server.stop_graceful() == 0
    assert _view_dir_bytes(bounded_server.data_dir, bid2) < _view_dir_bytes(bounded_server.data_dir, pid2)
    bounded_server.start()


def test_backfill_over_prepopulated_tables_matches_fresh_data(bounded_client, bounded_server):
    """Creating a bounded view over tables that already hold rows equals creating
    it first and then inserting them. The backfill re-derives the store hydrated
    and the sweep re-dehydrates as it spills."""
    fresh_sn, back_sn = "s" + _uid(), "s" + _uid()
    fresh_bid, fresh_pid = _setup(bounded_client, fresh_sn, LINEAR_BODY)
    _churn(bounded_client, fresh_sn, 1, 1600)

    # Same data, views created afterwards.
    bounded_client.create_schema(back_sn)
    bounded_client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL, body TEXT NOT NULL)",
        schema_name=back_sn,
    )
    bounded_client.execute_sql(
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, tid BIGINT NOT NULL, w BIGINT NOT NULL)",
        schema_name=back_sn,
    )
    _churn(bounded_client, back_sn, 1, 1600)
    bounded_client.execute_sql(
        f"CREATE VIEW b WITH (capacity = '1 KB') AS {LINEAR_BODY}", schema_name=back_sn
    )
    back_bid, _ = bounded_client.resolve_table(back_sn, "b")

    assert _rows(bounded_client, back_bid) == _rows(bounded_client, fresh_bid)
    assert _rows(bounded_client, back_bid) == _rows(bounded_client, fresh_pid)


# ── leaf rule ────────────────────────────────────────────────────────────────


def test_nothing_can_be_created_over_a_bounded_view(bounded_client):
    sn = "s" + _uid()
    bid, _pid = _setup(bounded_client, sn, LINEAR_BODY)
    _churn(bounded_client, sn, 1, 200)

    for body in [
        "SELECT id, v FROM b",
        "SELECT b.id, u.w FROM b JOIN u ON b.id = u.tid",
        "SELECT id FROM t WHERE id IN (SELECT id FROM b)",
        "WITH x AS (SELECT id, v FROM b) SELECT id FROM x",
    ]:
        with pytest.raises(Exception) as e:
            bounded_client.execute_sql(f"CREATE VIEW over AS {body}", schema_name=sn)
        assert "capacity-bounded" in str(e.value), body

    # Dropping it works — it is never a dependency of anything.
    bounded_client.execute_sql("DROP VIEW b", schema_name=sn)
