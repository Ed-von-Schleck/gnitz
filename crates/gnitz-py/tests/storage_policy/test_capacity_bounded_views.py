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

These tests need the store to reach the *disk* regime, which `SWEEP_ENV`
reaches on a few thousand rows by shrinking the RAM-tier ceiling. All but the
restart test share one such server.

What a `WITH (capacity = …)` clause *refuses*: the eligible-body rule is the
engine's, tested in `crates/gnitz-sql/tests/engine/views.rs`. The leaf rule and
the option grammar are the planner's, in `plan_view_rejections.rs`.
"""
import pytest
import gnitz
from _feedviews import JOIN, LINEAR, base_tables, churn
from _read import bag, rows
from _serverproc import disk_usage
from _sql import insert

# An inner equi-join whose right input is a filtered derived table, which fuses
# into the join's own circuit rather than cutting a segment.
DERIVED_JOIN = "SELECT t.id, t.body, d.w FROM t JOIN (SELECT tid, w FROM u WHERE w > 70) d ON t.id = d.tid"
# A join whose projection computes a column, and an `EXISTS` semi-join.
COMPUTED_JOIN = "SELECT t.id, t.body, t.v + u.w AS z FROM t JOIN u ON t.id = u.tid"
EXISTS_JOIN = "SELECT id, body FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.tid = t.id)"
# A key between payload columns: a dehydrated key is recomputed and read back in
# the view's declared column order.
MID_KEY = "SELECT body, id, v FROM t WHERE v > 10"

# A skeleton row is the PK plus one summed weight, so a store that has swept
# everything it can still holds this much per live key. `pk_stride` is 8 for a
# single BIGINT PK.
_SKELETON_ROW_BYTES = 8 + 8


def _skeletal(data_dir, *relations):
    """Which of `relations` hold skeleton rows in their output store. Nothing on
    the wire reports dehydration, so this is the one way to prove a sweep
    happened — and that one did not."""
    _, stores = disk_usage(data_dir)
    return {s["relation"] for s in stores if s["store"] == "rows" and s["skeletons"]} & set(relations)


def _twin(client, name, body, capacity="1 KB"):
    """A bounded view `b_{name}` and its unbounded twin `p_{name}` over one body.
    Returns `((bounded_vid, schema), (plain_vid, schema))`."""
    client.execute_sql(f"CREATE VIEW b_{name} WITH (capacity = '{capacity}') AS {body}")
    client.execute_sql(f"CREATE VIEW p_{name} AS {body}")
    return client.resolve_table(f"b_{name}"), client.resolve_table(f"p_{name}")


# ── parity ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "body,capacity",
    [
        (LINEAR, "1 KB"),
        (JOIN, "1 KB"),
        (DERIVED_JOIN, "1 KB"),
        (COMPUTED_JOIN, "1 KB"),
        (EXISTS_JOIN, "1 KB"),
        (MID_KEY, "1 KB"),
        # A capacity nothing reaches: a bounded view under its cap must read
        # exactly like any other relation.
        (LINEAR, "1 GB"),
    ],
    ids=["linear", "join", "derived_join", "computed_join", "exists_join", "mid_key", "slack"],
)
def test_bounded_view_matches_its_unbounded_twin(swept, swept_client, body, capacity):
    """Full scans, point seeks and the ad-hoc scan-spec path agree at every step
    of a churn stream, a bounded view backfilled over the churned tables agrees
    too, and each store dehydrated exactly when its cap said so."""
    c = swept_client
    base_tables(c)
    bid, pid = _twin(c, "v", body, capacity)

    for lo, hi in ((1, 400), (401, 1200)):
        churn(c, lo, hi, chunk=100)
        live = bag(c.scan(*pid))
        assert bag(c.scan(*bid)) == live, f"full scan through {hi}"

    # Created over tables that already hold every row: the backfill derives the
    # store hydrated and the sweep dehydrates it as it spills.
    c.execute_sql(f"CREATE VIEW late WITH (capacity = '{capacity}') AS {body}")
    late = c.resolve_table("late")
    assert bag(c.scan(*late)) == live, "backfill"

    # Point seeks, on keys spread across the whole range — the low ones are the
    # coldest by write recency, so they are the ones most likely dehydrated.
    keys = sorted(r.id for r in c.scan(*pid))
    for pk in keys[:5] + keys[len(keys) // 2 : len(keys) // 2 + 5] + keys[-5:]:
        assert bag(c.seek(*bid, pk)) == bag(c.seek(*pid, pk)), f"seek {pk}"
    # A key the view does not hold.
    assert bag(c.seek(*bid, 10**9)) == {}

    # The scan_spec path: predicate, ORDER BY / LIMIT, and an aggregate fold.
    # A bag key is positional, so a column out of its declared order fails it.
    for tmpl in [
        "SELECT * FROM {v}",
        "SELECT id FROM {v} WHERE id > 100 AND id < 300",
        "SELECT id, body FROM {v} ORDER BY id LIMIT 25",
        "SELECT COUNT(*) AS n FROM {v}",
        "SELECT id FROM {v} WHERE id IN (3, 17, 251, 999999)",
    ]:
        want = rows(c, tmpl.format(v="p_v"))
        got = rows(c, tmpl.format(v="b_v"))
        assert want and got[0]._fields == want[0]._fields, tmpl
        assert bag(got) == bag(want), tmpl

    bounded = {bid[0], late[0]}
    assert _skeletal(swept.data_dir, *bounded, pid[0]) == (bounded if capacity == "1 KB" else set())


def test_a_pk_group_with_several_payloads_folds_to_one_skeleton_weight(swept, swept_client):
    """The output PK of a join is the left input's, so several right-side matches
    put several payload rows under one key — and a projection that drops the
    discriminating column collapses them into one element at weight N.

    Both are what a skeleton row's *summed* weight is for, and both are the one
    deliberate exception to (PK, payload) identity: a cursor holding a skeleton
    run folds a whole PK group to that row regardless of payload. A comparator
    that kept payload identity there would answer a hydrated group beside the
    skeleton that already summed it, doubling every key the sweep had touched.
    """
    c = swept_client
    base_tables(c)
    # Enough keys that each worker's share of the folded view, one row per key,
    # outgrows the capacity at eight workers too.
    fan, keys = 4, 1600
    b_spread, p_spread = _twin(c, "spread", "SELECT t.id, u.w FROM t JOIN u ON t.id = u.tid")
    b_folded, p_folded = _twin(c, "folded", "SELECT t.id, t.v FROM t JOIN u ON t.id = u.tid")

    # `fan` `u` rows per `t.id`, so every output key carries a group of `fan`.
    for lo in range(1, keys + 1, 100):
        ids = range(lo, lo + 100)
        insert(c, "t", [(i, i * 3, f"body-{i:0>20}") for i in ids])
        insert(c, "u", [(i * fan + j, i, j) for i in ids for j in range(fan)])

    spread_want = bag(c.scan(*p_spread))
    folded_want = bag(c.scan(*p_folded))
    assert set(spread_want.values()) == {1} and len(spread_want) == keys * fan
    assert set(folded_want.values()) == {fan} and len(folded_want) == keys

    views = (b_spread[0], p_spread[0], b_folded[0], p_folded[0])
    assert _skeletal(swept.data_dir, *views) == {b_spread[0], b_folded[0]}, "both bounded views dehydrated"
    assert bag(c.scan(*b_spread)) == spread_want
    assert bag(c.scan(*b_folded)) == folded_want
    # And through a seek, which opens the skeleton run directly.
    for pk in (1, keys // 2, keys):
        assert bag(c.seek(*b_folded, pk)) == bag(c.seek(*p_folded, pk)), f"seek {pk}"


def test_a_compound_uuid_pk_with_one_leading_value_reads_like_its_twin(swept, swept_client):
    """`PRIMARY KEY (a UUID, b UUID)` with one distinct `a`: every row's leading
    sixteen key bytes are identical, and the whole key is 32 bytes wide. Point
    lookups, the aggregate path and ORDER BY / LIMIT agree with the unbounded
    twin once the store has dehydrated."""
    c = swept_client
    c.execute_sql("CREATE TABLE t (a UUID NOT NULL, b UUID NOT NULL, body TEXT NOT NULL, PRIMARY KEY (a, b))")
    bid, pid = _twin(c, "u", "SELECT a, b, body FROM t")

    tenant = "11111111-1111-1111-1111-111111111111"
    n = 1200
    for lo in range(0, n, 100):
        insert(c, "t", [(tenant, f"00000000-0000-0000-0000-{i:012x}", f"body-{i:0>20}") for i in range(lo, lo + 100)])

    assert _skeletal(swept.data_dir, bid[0], pid[0]) == {bid[0]}, "the bounded store alone dehydrated"
    assert bag(c.scan(*bid)) == bag(c.scan(*pid)), "full scan"
    # Point lookups go through SQL — the path a reader actually uses.
    for i in [0, n // 2, n - 1]:
        q = f"SELECT b, body FROM {{v}} WHERE a = '{tenant}' AND b = '00000000-0000-0000-0000-{i:012x}'"
        got = bag(rows(c, q.format(v="b_u")))
        assert len(got) == 1 and set(got.values()) == {1}, f"point lookup {i}: {got}"
        assert got == bag(rows(c, q.format(v="p_u"))), f"point lookup {i}"
    for q in ["SELECT COUNT(*) AS n FROM {v}", "SELECT b FROM {v} ORDER BY b LIMIT 25"]:
        want = bag(rows(c, q.format(v="p_u")))
        assert want and bag(rows(c, q.format(v="b_u"))) == want, q


def test_a_bounded_join_over_a_stream_reads_like_its_twin(swept, swept_client):
    """A bounded join recomputes a skeleton row from its own traces, not from its
    sources' stores, so it may read a stream, whose store holds nothing.

    No checkpoint writes a view a stream reaches, so its rows reach a shard only
    by outgrowing the store's memtable: hence 4 KB bodies, which give each
    worker's share of the 1,200 rows more than a memtable holds at eight workers."""
    c = swept_client
    c.execute_sql(
        "CREATE TABLE s (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL, body TEXT NOT NULL) "
        "WITH (stream = true); "
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, tid BIGINT NOT NULL, w BIGINT NOT NULL)",
    )
    n = 1200
    insert(c, "u", [(i, i, i * 7) for i in range(1, n + 1)])
    bid, pid = _twin(c, "sj", "SELECT s.id, s.body, u.w FROM s JOIN u ON s.id = u.tid")

    for lo in range(1, n + 1, 100):
        insert(c, "s", [(i, i * 3, f"body-{i:0>4000}") for i in range(lo, lo + 100)])

    live = bag(c.scan(*pid))
    assert live == {(i, f"body-{i:0>4000}", i * 7): 1 for i in range(1, n + 1)}
    assert _skeletal(swept.data_dir, bid[0], pid[0]) == {bid[0]}, "the bounded store alone dehydrated"
    assert bag(c.scan(*bid)) == live, "full scan"
    for pk in (1, 2, n // 2, n - 1, n):
        assert bag(c.seek(*bid, pk)) == bag(c.seek(*pid, pk)), f"seek {pk}"


# ── capacity across restarts ─────────────────────────────────────────────────


def test_a_tight_capacity_holds_the_registered_bytes_down_across_restarts(sweeping):
    """The bound is on the *registered* shard set. A running store's directory
    also holds compaction inputs superseded since the last checkpoint, so each
    figure is taken after a graceful restart, which leaves exactly the registered
    set on disk.

    The figure is compared against what the declaration promises, not against the
    twin, whose size is a function of its payload width. A bounded store
    converges to `max(cap, skeleton floor)` per worker, where the floor is
    `live keys × (pk_stride + weight)`, and sustained ingest does not let it climb
    back: the second phase carries four times the data at the same share.

    The same restarts carry the resume: the first read after boot answers what the
    store held before it, and churn after a reboot keeps parity.
    """
    with gnitz.connect(sweeping.target) as c:
        base_tables(c)
        bid, pid = _twin(c, "v", LINEAR)

    for lo, hi in ((1, 1200), (2001, 5200)):
        with gnitz.connect(sweeping.target) as c:
            churn(c, lo, hi, chunk=100)
            live = bag(c.scan(*pid))
            assert bag(c.scan(*bid)) == live, f"parity through {hi}"
        assert _skeletal(sweeping.data_dir, bid[0], pid[0]) == {bid[0]}

        sweeping.restart(graceful=True)
        # Reading drains whatever the boot still owes, so the directory is not
        # sampled mid-flight.
        with gnitz.connect(sweeping.target) as c:
            assert bag(c.scan(*bid)) == live, f"first read after the reboot through {hi}"
            assert bag(c.scan(*pid)) == live, f"twin after the reboot through {hi}"

        # Twice the floor is the headroom: a shard carries a header and the sweep
        # stops one guard above the fixpoint.
        report, stores = disk_usage(sweeping.data_dir)
        shards = [s for s in stores if s["relation"] == bid[0] and s["store"] == "rows"]
        assert shards, report
        bounded = sum(s["bytes"] for s in shards)
        floor = len(live) * _SKELETON_ROW_BYTES
        assert bounded <= 2 * max(1024, floor), (
            f"through {hi}: {bounded} bytes over {len(live)} keys, against a declared "
            f"1024 and a skeleton floor of {floor}"
        )
