"""Capacity-bounded views: `CREATE VIEW … WITH (capacity = '…')`.

The claim under test is that capacity is invisible to correctness. Every test
here runs a bounded view beside an unbounded twin over the same churn and
requires the two to agree through every read verb — full scan, point seek, and
the ad-hoc `SELECT … WHERE / ORDER BY / LIMIT / aggregate` path — while the
bounded one's registered shard bytes stay near what it declared.

Every comparison goes through `bag`, which sums weights per row. That is not
just the house style here: a skeleton row carries a key's *summed* weight where
the hydrated twin holds the physical entries, so per-key weights are the only
observable both sides are obliged to agree on.

These tests need the store to reach the *disk* regime, which at the production
32 MiB RAM-tier ceiling would take megabytes of churn. `sweeping_server` shrinks
that ceiling, so a few thousand rows spill, compact, and dehydrate.

What a `WITH (capacity = …)` clause *refuses* — the eligible-body list, the leaf
rule and the option grammar — is planner-only, and lives beside the rule in
`crates/gnitz-sql/tests/plan_view_rejections.rs`.
"""
import os
import struct

import pytest
import gnitz
from _feedviews import JOIN, LINEAR, _base_tables, _churn
from _read import bag, rows
from _uid import uid as _uid

# Shard header words (storage/repr/layout.rs). A skeleton shard stamps the high
# bit of OFF_FILE_NPC.
_OFF_FILE_NPC = 32
_SHARD_FLAG_SKELETON = 1 << 63

# A skeleton row is the PK plus one summed weight, so a store that has swept
# everything it can still holds this much per live key. `pk_stride` is 8 for a
# single BIGINT PK.
_SKELETON_ROW_BYTES = 8 + 8


# ── on-disk inspection ───────────────────────────────────────────────────────


def _shard_files(data_dir, view_id):
    """Every `.db` shard of `view_id`'s *output* store, on every worker.

    `w{k}of{n}` is the output store's child directory (storage/lsm/child_dir.rs);
    `scratch_*_w{k}` holds the operator traces, which the capacity deliberately
    does not bound. Matching the grammar rather than the whole relation directory
    is what makes the byte figures below the quantity the contract names.
    """
    out = []
    needle = f"_{view_id}"
    for root, _dirs, files in os.walk(data_dir):
        parts = root.split(os.sep)
        if not any(p.endswith(needle) for p in parts):
            continue
        if not any(p.startswith("w") and "of" in p for p in parts):
            continue
        out += [os.path.join(root, f) for f in files if f.endswith(".db")]
    assert out, f"no output shard of view {view_id} under {data_dir}"
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


def _drain(client, sn, base, rounds=30):
    """Trailing writes so the sweep can catch up. It budgets itself to one
    push-down per spill, so whatever was written since the last few spills is
    still hydrated when ingest stops — the residue is bounded by the recent write
    burst, not by the store, which is why this count is constant rather than a
    share of the data."""
    for k in range(rounds):
        lo = base + k * 20
        client.execute_sql(
            "INSERT INTO t VALUES " + ",".join(f"({i}, {i * 3}, 'tail-{i:0>20}')" for i in range(lo, lo + 20)),
            schema_name=sn,
        )


def _setup(client, sn, view_sql, *, capacity="1 KB"):
    """`t`/`u` plus a bounded view `b` and its unbounded twin `p`, both over the
    same body. Returns `(bounded_vid, plain_vid)`."""
    _base_tables(client, sn)
    client.execute_sql(f"CREATE VIEW b WITH (capacity = '{capacity}') AS {view_sql}", schema_name=sn)
    client.execute_sql(f"CREATE VIEW p AS {view_sql}", schema_name=sn)
    bid, _ = client.resolve_table(sn, "b")
    pid, _ = client.resolve_table(sn, "p")
    return bid, pid


# ── parity ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "body,capacity,sweeps",
    [
        (LINEAR, "1 KB", True),
        (JOIN, "1 KB", True),
        # A capacity nothing reaches: the read path branches on whether a store
        # *holds* a skeleton row, so a bounded view under its cap must read
        # exactly like any other relation.
        (LINEAR, "1 GB", False),
    ],
    ids=["linear", "join", "slack"],
)
def test_bounded_view_matches_its_unbounded_twin(sweeping_client, sweeping_server, body, capacity, sweeps):
    """Full scans, point seeks and the ad-hoc scan-spec path agree at every step
    of a churn stream, and the store dehydrated exactly when its cap said so."""
    sn = "s" + _uid()
    bid, pid = _setup(sweeping_client, sn, body, capacity=capacity)

    for lo, hi in ((1, 400), (401, 1200)):
        _churn(sweeping_client, sn, lo, hi, chunk=100)
        live = bag(sweeping_client.scan(pid))
        assert bag(sweeping_client.scan(bid)) == live, f"full scan through {hi}"

    # Point seeks, on keys spread across the whole range — the low ones are the
    # coldest by write recency, so they are the ones most likely dehydrated.
    keys = sorted({k[0] for k in live})
    for pk in keys[:5] + keys[len(keys) // 2 : len(keys) // 2 + 5] + keys[-5:]:
        assert bag(sweeping_client.seek(bid, pk)) == bag(sweeping_client.seek(pid, pk)), f"seek {pk}"
    # A key the view does not hold.
    assert bag(sweeping_client.seek(bid, 10**9)) == {}

    # The ScanSpec path: predicate, ORDER BY / LIMIT, and an aggregate fold.
    payload = "w" if body is JOIN else "v"
    for tmpl in [
        "SELECT id FROM {v} WHERE id > 100 AND id < 300",
        "SELECT id, " + payload + " FROM {v} ORDER BY id LIMIT 25",
        "SELECT COUNT(*) AS n FROM {v}",
        "SELECT id FROM {v} WHERE id IN (3, 17, 251, 999999)",
    ]:
        want = bag(rows(sweeping_client, sn, tmpl.format(v="p")))
        assert want, tmpl
        assert bag(rows(sweeping_client, sn, tmpl.format(v="b"))) == want, tmpl

    assert _any_skeleton(sweeping_server.data_dir, bid) == sweeps
    assert not _any_skeleton(sweeping_server.data_dir, pid), "the twin must never dehydrate"


def test_a_pk_group_with_several_payloads_folds_to_one_skeleton_weight(sweeping_client, sweeping_server):
    """The output PK of a join is the left input's, so several right-side matches
    put several payload rows under one key — and a projection that drops the
    discriminating column collapses them into one element at weight N.

    Both are what a skeleton row's *summed* weight is for, and both are the one
    deliberate exception to (PK, payload) identity: a cursor holding a skeleton
    run folds a whole PK group to that row regardless of payload. A comparator
    that kept payload identity there would answer a hydrated group beside the
    skeleton that already summed it, doubling every key the sweep had touched.
    """
    sn = "s" + _uid()
    _base_tables(sweeping_client, sn)
    # Four `u` rows per `t.id`, so every output key carries a group of four.
    fan, keys = 4, 400
    spread = "SELECT t.id, u.w FROM t JOIN u ON t.id = u.tid"
    folded = "SELECT t.id, t.v FROM t JOIN u ON t.id = u.tid"
    for name, cap, sql in [("b_spread", "1 KB", spread), ("p_spread", None, spread),
                           ("b_folded", "1 KB", folded), ("p_folded", None, folded)]:
        with_ = f" WITH (capacity = '{cap}')" if cap else ""
        sweeping_client.execute_sql(f"CREATE VIEW {name}{with_} AS {sql}", schema_name=sn)

    for lo in range(1, keys + 1, 100):
        hi = min(lo + 99, keys)
        sweeping_client.execute_sql(
            "INSERT INTO t VALUES " + ",".join(f"({i}, {i * 3}, 'body-{i:0>20}')" for i in range(lo, hi + 1)),
            schema_name=sn,
        )
        sweeping_client.execute_sql(
            "INSERT INTO u VALUES "
            + ",".join(f"({i * fan + j}, {i}, {j})" for i in range(lo, hi + 1) for j in range(fan)),
            schema_name=sn,
        )

    ids = {}
    for name in ("b_spread", "p_spread", "b_folded", "p_folded"):
        ids[name], _ = sweeping_client.resolve_table(sn, name)
    spread_want = bag(sweeping_client.scan(ids["p_spread"]))
    folded_want = bag(sweeping_client.scan(ids["p_folded"]))
    assert set(spread_want.values()) == {1} and len(spread_want) == keys * fan
    assert set(folded_want.values()) == {fan} and len(folded_want) == keys

    assert _any_skeleton(sweeping_server.data_dir, ids["b_spread"]), "the group view must have dehydrated"
    assert _any_skeleton(sweeping_server.data_dir, ids["b_folded"]), "the weight view must have dehydrated"
    assert bag(sweeping_client.scan(ids["b_spread"])) == spread_want
    assert bag(sweeping_client.scan(ids["b_folded"])) == folded_want
    # And through a seek, which opens the skeleton run directly.
    for pk in (1, keys // 2, keys):
        assert bag(sweeping_client.seek(ids["b_folded"], pk)) == \
            bag(sweeping_client.seek(ids["p_folded"], pk)), f"seek {pk}"


def test_all_null_and_all_zero_payloads_survive_dehydration(sweeping_client, sweeping_server):
    """A row whose whole projected payload is NULL, and one whose whole payload
    is zero non-nullable integers, both come back from a dehydrated store with
    their weights — the two shapes whose byte image a comparator could confuse
    with a skeleton row's absent payload."""
    sn = "s" + _uid()
    sweeping_client.create_schema(sn)
    sweeping_client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, z BIGINT NOT NULL, s TEXT)",
        schema_name=sn,
    )
    for name, cap, cols in [("b_nul", True, "id, s"), ("p_nul", False, "id, s"),
                            ("b_zero", True, "id, z"), ("p_zero", False, "id, z")]:
        with_ = " WITH (capacity = '1 KB')" if cap else ""
        sweeping_client.execute_sql(f"CREATE VIEW {name}{with_} AS SELECT {cols} FROM t", schema_name=sn)

    for lo in range(1, 1201, 200):
        # Every third row is all-NULL / all-zero in the projected payload.
        vals = [f"({i}, {0 if i % 3 == 0 else i}, {'NULL' if i % 3 == 0 else repr(f'text-{i:0>20}')})"
                for i in range(lo, min(lo + 199, 1200) + 1)]
        sweeping_client.execute_sql("INSERT INTO t VALUES " + ",".join(vals), schema_name=sn)

    for b, p in [("b_nul", "p_nul"), ("b_zero", "p_zero")]:
        bid, _ = sweeping_client.resolve_table(sn, b)
        pid, _ = sweeping_client.resolve_table(sn, p)
        assert bag(sweeping_client.scan(bid)) == bag(sweeping_client.scan(pid)), b
        assert _any_skeleton(sweeping_server.data_dir, bid), f"{b} must have dehydrated"


def test_a_single_tenant_uuid_pk_dehydrates_gradually(sweeping_client, sweeping_server):
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
    sweeping_client.create_schema(sn)
    sweeping_client.execute_sql(
        "CREATE TABLE t (a UUID NOT NULL, b UUID NOT NULL, body TEXT NOT NULL, PRIMARY KEY (a, b))",
        schema_name=sn,
    )
    body = "SELECT a, b, body FROM t"
    sweeping_client.execute_sql(f"CREATE VIEW bu WITH (capacity = '1 KB') AS {body}", schema_name=sn)
    sweeping_client.execute_sql(f"CREATE VIEW pu AS {body}", schema_name=sn)
    bid, _ = sweeping_client.resolve_table(sn, "bu")
    pid, _ = sweeping_client.resolve_table(sn, "pu")

    # One tenant, so `a` is constant; `b` varies only in its low bytes, which are
    # the OPK bytes past 16. Inserted in batches so the view spills repeatedly.
    tenant = "11111111-1111-1111-1111-111111111111"
    n = 1200
    for lo in range(0, n, 100):
        vals = ",".join(
            f"('{tenant}', '00000000-0000-0000-0000-{i:012x}', 'body-{i:0>20}')" for i in range(lo, lo + 100)
        )
        sweeping_client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)

    assert bag(sweeping_client.scan(bid)) == bag(sweeping_client.scan(pid)), "full scan"
    # Point lookups on the coldest, middle and newest keys, plus the ScanSpec
    # and aggregate paths. The wire `seek` verb takes a compound PK as its
    # native byte image, which would be a second spelling of the OPK encoding
    # here, so these go through SQL — the path a reader actually uses.
    for i in [0, n // 2, n - 1]:
        b = f"00000000-0000-0000-0000-{i:012x}"
        q = f"SELECT b, body FROM {{v}} WHERE a = '{tenant}' AND b = '{b}'"
        got = bag(rows(sweeping_client, sn, q.format(v="bu")))
        assert len(got) == 1 and set(got.values()) == {1}, f"point lookup {b}: {got}"
        assert got == bag(rows(sweeping_client, sn, q.format(v="pu"))), f"point lookup {b}"
    for q in ["SELECT COUNT(*) AS n FROM {v}", "SELECT b FROM {v} ORDER BY b LIMIT 25"]:
        want = bag(rows(sweeping_client, sn, q.format(v="pu")))
        assert want and bag(rows(sweeping_client, sn, q.format(v="bu"))) == want, q

    # The sweep evicted something, and it did so a guard at a time: an
    # all-or-nothing store leaves not one hydrated shard behind.
    shards = _shard_files(sweeping_server.data_dir, bid)
    assert any(_is_skeleton(p) for p in shards), "the bounded store must have dehydrated"
    assert any(not _is_skeleton(p) for p in shards), (
        "every shard is a skeleton — the store was one unsplittable guard, so the "
        "sweep had no smaller unit than the whole view"
    )


# ── capacity semantics ───────────────────────────────────────────────────────


def test_a_tight_capacity_holds_the_registered_bytes_down(sweeping_server):
    """The bound is on the *registered* shard set — the shards the manifest names.
    A running store's directory additionally holds every compaction input
    superseded since the last checkpoint, which only the post-publish drain
    unlinks. So each figure here is taken after a graceful shutdown *and* a
    reboot, whose `gc_orphans` leaves exactly the registered set on disk.

    The figure is compared against what the declaration promises, not against the
    twin: the twin's size is a function of its payload width, so a ratio against
    it pins the skeleton compression of this row shape rather than the bound. A
    bounded store converges to `max(cap, skeleton floor)` per worker, and the
    floor is `live keys × (pk_stride + weight)`. Sustained ingest does not let it
    climb back: the sweep runs at every spill, so the residue stays what the
    recent write burst leaves rather than growing with the data.
    """
    sn = "s" + _uid()
    cap = 1024

    def churn_and_measure(lo, hi):
        with gnitz.connect(sweeping_server.sock_path) as c:
            if lo == 1:
                bid, pid = _setup(c, sn, LINEAR, capacity="1 KB")
            else:
                bid, _ = c.resolve_table(sn, "b")
                pid, _ = c.resolve_table(sn, "p")
            _churn(c, sn, lo, hi, chunk=100)
            _drain(c, sn, hi + 1)
            live = bag(c.scan(pid))
            assert bag(c.scan(bid)) == live, f"parity through {hi}"
        sweeping_server.restart(graceful=True)
        # Read both views before measuring: a read drains whatever the boot still
        # owes (a resume's un-checkpointed tail, or a rebuild's backfill), so the
        # directory is not sampled mid-flight.
        with gnitz.connect(sweeping_server.sock_path) as c:
            assert bag(c.scan(bid)) == bag(c.scan(pid)), "parity across the reboot"
        return _view_dir_bytes(sweeping_server.data_dir, bid), len(live)

    # Twice the floor is the headroom: a shard carries a header and the sweep
    # stops one guard above the fixpoint, so the converged store sits above the
    # bare row arithmetic without tracking the data. Measured at roughly half the
    # floor in both phases — the same share despite the second carrying four
    # times the data, which is the claim.
    for lo, hi in ((1, 1200), (2001, 5200)):
        bounded, keys = churn_and_measure(lo, hi)
        floor = keys * _SKELETON_ROW_BYTES
        assert bounded <= 2 * max(cap, floor), (
            f"through {hi}: {bounded} bytes over {keys} keys, against a declared "
            f"{cap} and a skeleton floor of {floor}"
        )


# ── boot and backfill ────────────────────────────────────────────────────────


def test_a_bounded_view_survives_a_restart(sweeping_client, sweeping_server):
    """The store resumes with its skeleton shards, reads still agree with the
    twin, and the capacity is re-enforced once the view ticks again."""
    sn = "s" + _uid()
    bid, pid = _setup(sweeping_client, sn, LINEAR)
    _churn(sweeping_client, sn, 1, 1200, chunk=100)
    before = bag(sweeping_client.scan(bid))
    assert before == bag(sweeping_client.scan(pid))
    assert _any_skeleton(sweeping_server.data_dir, bid)

    sweeping_client.close()
    # Graceful: the final checkpoint publishes the view's skeleton shards, so the
    # boot resumes the store rather than rebuilding it.
    sweeping_server.restart(graceful=True)

    with gnitz.connect(sweeping_server.sock_path) as c2:
        bid2, _ = c2.resolve_table(sn, "b")
        pid2, _ = c2.resolve_table(sn, "p")
        # The very first read after boot arrives before any post-boot tick, so
        # it exercises the lazily-recompiled plan cache.
        assert bag(c2.scan(bid2)) == before
        assert bag(c2.scan(bid2)) == bag(c2.scan(pid2))

        _churn(c2, sn, 1201, 3000, chunk=100)
        assert bag(c2.scan(bid2)) == bag(c2.scan(pid2))
    assert sweeping_server.stop_graceful() == 0
    assert _view_dir_bytes(sweeping_server.data_dir, bid2) < _view_dir_bytes(sweeping_server.data_dir, pid2)


def test_backfill_over_prepopulated_tables_matches_fresh_data(sweeping_client, sweeping_server):
    """Creating a bounded view over tables that already hold rows equals creating
    it first and then inserting them. The backfill re-derives the store hydrated
    and the sweep re-dehydrates as it spills."""
    fresh_sn, back_sn = "s" + _uid(), "s" + _uid()
    fresh_bid, fresh_pid = _setup(sweeping_client, fresh_sn, LINEAR)
    _churn(sweeping_client, fresh_sn, 1, 1200, chunk=100)

    # Same data, view created afterwards.
    _base_tables(sweeping_client, back_sn)
    _churn(sweeping_client, back_sn, 1, 1200, chunk=100)
    sweeping_client.execute_sql(
        f"CREATE VIEW b WITH (capacity = '1 KB') AS {LINEAR}", schema_name=back_sn
    )
    back_bid, _ = sweeping_client.resolve_table(back_sn, "b")

    want = bag(sweeping_client.scan(fresh_pid))
    assert bag(sweeping_client.scan(fresh_bid)) == want
    assert bag(sweeping_client.scan(back_bid)) == want
    assert _any_skeleton(sweeping_server.data_dir, back_bid), "the backfilled store must sweep too"
