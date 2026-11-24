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
from _read import bag, reads_alike, scanned
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

# The ad-hoc scan-spec path: predicate, ORDER BY / LIMIT, and an aggregate fold.
_READS = [
    "SELECT * FROM {v}",
    "SELECT id FROM {v} WHERE id > 100 AND id < 300",
    "SELECT id, body FROM {v} ORDER BY id LIMIT 25",
    "SELECT COUNT(*) AS n FROM {v}",
    "SELECT id FROM {v} WHERE id IN (3, 17, 251, 999999)",
]


def _rows_shards(data_dir, relation):
    """The shard lines of `relation`'s output store."""
    _, stores = disk_usage(data_dir)
    return [s for s in stores if s["relation"] == relation and s["store"] == "rows"]


def _skeletal(data_dir, client, *names):
    """Which of the relations `names` hold skeleton rows in their output store.
    Nothing on the wire reports dehydration, so this is the one way to prove a
    sweep happened — and that one did not."""
    return {n for n in names if any(s["skeletons"] for s in _rows_shards(data_dir, client.resolve_table(n)[0]))}


def _twin(client, name, body, opts="capacity = '1 KB'"):
    """A bounded view `b_{name}` and its unbounded twin `p_{name}` over one body."""
    client.execute_sql(f"CREATE VIEW b_{name} WITH ({opts}) AS {body}")
    client.execute_sql(f"CREATE VIEW p_{name} AS {body}")


# ── parity ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "body,opts",
    [
        (LINEAR, "capacity = '1 KB'"),
        (JOIN, "capacity = '1 KB'"),
        (DERIVED_JOIN, "capacity = '1 KB'"),
        (COMPUTED_JOIN, "capacity = '1 KB'"),
        (EXISTS_JOIN, "capacity = '1 KB'"),
        (MID_KEY, "capacity = '1 KB'"),
        # A skeleton row beside compressed, hydrated rows of one store.
        (LINEAR, "capacity = '1 KB', compress = true"),
        # A capacity nothing reaches: a bounded view under its cap must read
        # exactly like any other relation.
        (LINEAR, "capacity = '1 GB'"),
    ],
    ids=["linear", "join", "derived_join", "computed_join", "exists_join", "mid_key", "compressed", "slack"],
)
def test_bounded_view_matches_its_unbounded_twin(swept, swept_client, body, opts):
    """Full scans and point seeks agree at every step of a churn stream; at the
    end they, the ad-hoc scan-spec path, and a bounded view backfilled over the
    churned tables agree too, and each store dehydrated exactly when its cap
    said so. A bag key is positional, so a column read out of its declared order
    fails it."""
    c = swept_client
    base_tables(c)
    _twin(c, "v", body, opts)

    for lo, hi in ((1, 400), (401, 1200)):
        churn(c, lo, hi, chunk=100)
        reads_alike(c, "b_v", "p_v")

    # Created over tables that already hold every row: the backfill derives the
    # store hydrated and the sweep dehydrates it as it spills.
    c.execute_sql(f"CREATE VIEW late WITH ({opts}) AS {body}")
    for name in ("b_v", "late"):
        reads_alike(c, name, "p_v", _READS)
    # A key the view does not hold.
    assert bag(c.seek(*c.resolve_table("b_v"), 10**9)) == {}

    swept_names = {"b_v", "late"} if "1 KB" in opts else set()
    assert _skeletal(swept.data_dir, c, "b_v", "late", "p_v") == swept_names


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
    _twin(c, "spread", "SELECT t.id, u.w FROM t JOIN u ON t.id = u.tid")
    _twin(c, "folded", "SELECT t.id, t.v FROM t JOIN u ON t.id = u.tid")

    # `fan` `u` rows per `t.id`, so every output key carries a group of `fan`.
    for lo in range(1, keys + 1, 100):
        ids = range(lo, lo + 100)
        insert(c, "t", [(i, i * 3, f"body-{i:0>20}") for i in ids])
        insert(c, "u", [(i * fan + j, i, j) for i in ids for j in range(fan)])

    spread_want = bag(scanned(c, "p_spread"))
    folded_want = bag(scanned(c, "p_folded"))
    assert set(spread_want.values()) == {1} and len(spread_want) == keys * fan
    assert set(folded_want.values()) == {fan} and len(folded_want) == keys

    views = ("b_spread", "p_spread", "b_folded", "p_folded")
    assert _skeletal(swept.data_dir, c, *views) == {"b_spread", "b_folded"}, "both bounded views dehydrated"
    reads_alike(c, "b_spread", "p_spread")
    reads_alike(c, "b_folded", "p_folded")


def test_a_compound_uuid_pk_with_one_leading_value_reads_like_its_twin(swept, swept_client):
    """`PRIMARY KEY (a UUID, b UUID)` with one distinct `a`: every row's leading
    sixteen key bytes are identical, and the whole key is 32 bytes wide. Point
    lookups — through SQL, the path a reader of a compound key uses — the
    aggregate path and ORDER BY / LIMIT agree with the unbounded twin once the
    store has dehydrated."""
    c = swept_client
    c.execute_sql("CREATE TABLE t (a UUID NOT NULL, b UUID NOT NULL, body TEXT NOT NULL, PRIMARY KEY (a, b))")
    _twin(c, "u", "SELECT a, b, body FROM t")

    tenant = "11111111-1111-1111-1111-111111111111"
    n = 1200
    for lo in range(0, n, 100):
        insert(c, "t", [(tenant, f"00000000-0000-0000-0000-{i:012x}", f"body-{i:0>20}") for i in range(lo, lo + 100)])

    assert _skeletal(swept.data_dir, c, "b_u", "p_u") == {"b_u"}, "the bounded store alone dehydrated"
    lookups = [
        f"SELECT b, body FROM {{v}} WHERE a = '{tenant}' AND b = '00000000-0000-0000-0000-{i:012x}'"
        for i in (0, n // 2, n - 1)
    ]
    reads_alike(c, "b_u", "p_u", [*lookups, "SELECT COUNT(*) AS n FROM {v}", "SELECT b FROM {v} ORDER BY b LIMIT 25"], key=None)


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
    _twin(c, "sj", "SELECT s.id, s.body, u.w FROM s JOIN u ON s.id = u.tid")

    for lo in range(1, n + 1, 100):
        insert(c, "s", [(i, i * 3, f"body-{i:0>4000}") for i in range(lo, lo + 100)])

    assert bag(scanned(c, "p_sj")) == {(i, f"body-{i:0>4000}", i * 7): 1 for i in range(1, n + 1)}
    assert _skeletal(swept.data_dir, c, "b_sj", "p_sj") == {"b_sj"}, "the bounded store alone dehydrated"
    reads_alike(c, "b_sj", "p_sj")


# ── capacity across restarts ─────────────────────────────────────────────────


def test_a_tight_capacity_holds_the_registered_bytes_down_across_restarts(sweeping):
    """The bound is on the *registered* shard set. A running store's directory
    also holds compaction inputs superseded since the last checkpoint, so each
    figure is taken after a graceful restart, which leaves exactly the registered
    set on disk, and after the first read has drained whatever the boot owed.

    The figure is compared against what the declaration promises, not against the
    twin, whose size is a function of its payload width. A bounded store
    converges to `max(cap, skeleton floor)` per worker, where the floor is
    `live keys × (pk_stride + weight)`, and sustained ingest does not let it climb
    back: the second phase carries four times the data at the same share.

    The same restarts carry the resume, after a crash and after a graceful stop:
    the first read after boot answers what the store held before it, and churn
    after a reboot keeps parity.
    """
    with gnitz.connect(sweeping.target) as c:
        base_tables(c)
        _twin(c, "v", LINEAR)

    for lo, hi in ((1, 1200), (2001, 5200)):
        with gnitz.connect(sweeping.target) as c:
            churn(c, lo, hi, chunk=100)
            reads_alike(c, "b_v", "p_v")
            assert _skeletal(sweeping.data_dir, c, "b_v", "p_v") == {"b_v"}
            live = bag(scanned(c, "p_v"))
            bid = c.resolve_table("b_v")[0]

        for graceful in (False, True):
            sweeping.restart(graceful=graceful)
            with gnitz.connect(sweeping.target) as c:
                assert bag(scanned(c, "b_v")) == live, f"first read after the reboot through {hi}"
                reads_alike(c, "b_v", "p_v")

        # Twice the floor is the headroom: a shard carries a header and the sweep
        # stops one guard above the fixpoint.
        shards = _rows_shards(sweeping.data_dir, bid)
        assert shards, disk_usage(sweeping.data_dir)[0]
        bounded = sum(s["bytes"] for s in shards)
        cap = 1024 * sweeping.workers
        floor = len(live) * _SKELETON_ROW_BYTES
        assert bounded <= 2 * max(cap, floor), (
            f"through {hi}: {bounded} bytes over {len(live)} keys, against a declared "
            f"{cap} across {sweeping.workers} workers and a skeleton floor of {floor}"
        )
