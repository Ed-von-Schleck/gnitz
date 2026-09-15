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

These tests need the store to reach the *disk* regime, which `sweeping_server`
reaches on a few thousand rows by shrinking the RAM-tier ceiling.

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

# A skeleton shard stamps the high bit of its header's npc word. Nothing on the
# wire reports dehydration, so this is the one way to prove a sweep happened.
_OFF_FILE_NPC = 32
_SHARD_FLAG_SKELETON = 1 << 63

# An inner equi-join whose right input is a filtered derived table, which fuses
# into the join's own circuit rather than cutting a segment.
DERIVED_JOIN = "SELECT t.id, t.body, d.w FROM t JOIN (SELECT tid, w FROM u WHERE w > 70) d ON t.id = d.tid"

# A skeleton row is the PK plus one summed weight, so a store that has swept
# everything it can still holds this much per live key. `pk_stride` is 8 for a
# single BIGINT PK.
_SKELETON_ROW_BYTES = 8 + 8


def _shard_files(data_dir, view_id):
    """Every `.db` shard of `view_id`'s *output* store (`w{k}of{n}`), on every
    worker — not its operator traces (`scratch_*`), which the capacity does not
    bound."""
    out = []
    for root, _dirs, files in os.walk(data_dir):
        rel, child = os.path.split(root)
        if rel.endswith(f"_{view_id}") and child.startswith("w") and "of" in child:
            out += [os.path.join(root, f) for f in files if f.endswith(".db")]
    assert out, f"no output shard of view {view_id} under {data_dir}"
    return out


def _any_skeleton(data_dir, view_id):
    def is_skeleton(path):
        with open(path, "rb") as fh:
            fh.seek(_OFF_FILE_NPC)
            return bool(struct.unpack("<Q", fh.read(8))[0] & _SHARD_FLAG_SKELETON)

    return any(is_skeleton(p) for p in _shard_files(data_dir, view_id))


def _twin(client, sn, name, body, capacity="1 KB"):
    """A bounded view `b_{name}` and its unbounded twin `p_{name}` over one body.
    Returns `(bounded_vid, plain_vid)`."""
    client.execute_sql(f"CREATE VIEW b_{name} WITH (capacity = '{capacity}') AS {body}", schema_name=sn)
    client.execute_sql(f"CREATE VIEW p_{name} AS {body}", schema_name=sn)
    return client.resolve_table(sn, f"b_{name}")[0], client.resolve_table(sn, f"p_{name}")[0]


# ── parity ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "body,capacity,sweeps",
    [
        (LINEAR, "1 KB", True),
        (JOIN, "1 KB", True),
        (DERIVED_JOIN, "1 KB", True),
        # A capacity nothing reaches: a bounded view under its cap must read
        # exactly like any other relation.
        (LINEAR, "1 GB", False),
    ],
    ids=["linear", "join", "derived_join", "slack"],
)
def test_bounded_view_matches_its_unbounded_twin(sweeping_client, sweeping_server, body, capacity, sweeps):
    """Full scans, point seeks and the ad-hoc scan-spec path agree at every step
    of a churn stream, a bounded view backfilled over the churned tables agrees
    too, and each store dehydrated exactly when its cap said so."""
    c, sn = sweeping_client, "s" + _uid()
    c.create_schema(sn)
    _base_tables(c, sn)
    bid, pid = _twin(c, sn, "v", body, capacity)

    for lo, hi in ((1, 400), (401, 1200)):
        _churn(c, sn, lo, hi, chunk=100)
        live = bag(c.scan(pid))
        assert bag(c.scan(bid)) == live, f"full scan through {hi}"

    # Created over tables that already hold every row: the backfill derives the
    # store hydrated and the sweep dehydrates it as it spills.
    c.execute_sql(f"CREATE VIEW late WITH (capacity = '{capacity}') AS {body}", schema_name=sn)
    late, _ = c.resolve_table(sn, "late")
    assert bag(c.scan(late)) == live, "backfill"

    # Point seeks, on keys spread across the whole range — the low ones are the
    # coldest by write recency, so they are the ones most likely dehydrated.
    keys = sorted({k[0] for k in live})
    for pk in keys[:5] + keys[len(keys) // 2 : len(keys) // 2 + 5] + keys[-5:]:
        assert bag(c.seek(bid, pk)) == bag(c.seek(pid, pk)), f"seek {pk}"
    # A key the view does not hold.
    assert bag(c.seek(bid, 10**9)) == {}

    # The ScanSpec path: predicate, ORDER BY / LIMIT, and an aggregate fold.
    for tmpl in [
        "SELECT id FROM {v} WHERE id > 100 AND id < 300",
        "SELECT id, body FROM {v} ORDER BY id LIMIT 25",
        "SELECT COUNT(*) AS n FROM {v}",
        "SELECT id FROM {v} WHERE id IN (3, 17, 251, 999999)",
    ]:
        want = bag(rows(c, sn, tmpl.format(v="p_v")))
        assert want, tmpl
        assert bag(rows(c, sn, tmpl.format(v="b_v"))) == want, tmpl

    data_dir = sweeping_server.data_dir
    assert _any_skeleton(data_dir, bid) == sweeps
    assert _any_skeleton(data_dir, late) == sweeps
    assert not _any_skeleton(data_dir, pid), "the twin must never dehydrate"


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
    c, sn = sweeping_client, "s" + _uid()
    c.create_schema(sn)
    _base_tables(c, sn)
    fan, keys = 4, 400
    b_spread, p_spread = _twin(c, sn, "spread", "SELECT t.id, u.w FROM t JOIN u ON t.id = u.tid")
    b_folded, p_folded = _twin(c, sn, "folded", "SELECT t.id, t.v FROM t JOIN u ON t.id = u.tid")

    # `fan` `u` rows per `t.id`, so every output key carries a group of `fan`.
    for lo in range(1, keys + 1, 100):
        ids = range(lo, lo + 100)
        c.execute_sql("INSERT INTO t VALUES " + ",".join(f"({i}, {i * 3}, 'body-{i:0>20}')" for i in ids),
                      schema_name=sn)
        c.execute_sql("INSERT INTO u VALUES " + ",".join(f"({i * fan + j}, {i}, {j})" for i in ids for j in range(fan)),
                      schema_name=sn)

    spread_want = bag(c.scan(p_spread))
    folded_want = bag(c.scan(p_folded))
    assert set(spread_want.values()) == {1} and len(spread_want) == keys * fan
    assert set(folded_want.values()) == {fan} and len(folded_want) == keys

    assert _any_skeleton(sweeping_server.data_dir, b_spread), "the group view must have dehydrated"
    assert _any_skeleton(sweeping_server.data_dir, b_folded), "the weight view must have dehydrated"
    assert bag(c.scan(b_spread)) == spread_want
    assert bag(c.scan(b_folded)) == folded_want
    # And through a seek, which opens the skeleton run directly.
    for pk in (1, keys // 2, keys):
        assert bag(c.seek(b_folded, pk)) == bag(c.seek(p_folded, pk)), f"seek {pk}"


def test_a_compound_uuid_pk_with_one_leading_value_reads_like_its_twin(sweeping_client, sweeping_server):
    """`PRIMARY KEY (a UUID, b UUID)` with one distinct `a`: every row's leading
    sixteen key bytes are identical, and the whole key is 32 bytes wide. Point
    lookups, the aggregate path and ORDER BY / LIMIT agree with the unbounded
    twin once the store has dehydrated."""
    c, sn = sweeping_client, "s" + _uid()
    c.create_schema(sn)
    c.execute_sql(
        "CREATE TABLE t (a UUID NOT NULL, b UUID NOT NULL, body TEXT NOT NULL, PRIMARY KEY (a, b))",
        schema_name=sn,
    )
    bid, pid = _twin(c, sn, "u", "SELECT a, b, body FROM t")

    tenant = "11111111-1111-1111-1111-111111111111"
    n = 1200
    for lo in range(0, n, 100):
        vals = ",".join(
            f"('{tenant}', '00000000-0000-0000-0000-{i:012x}', 'body-{i:0>20}')" for i in range(lo, lo + 100)
        )
        c.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)

    assert _any_skeleton(sweeping_server.data_dir, bid), "the bounded store must have dehydrated"
    assert bag(c.scan(bid)) == bag(c.scan(pid)), "full scan"
    # The wire `seek` verb takes a compound PK as its native byte image, so
    # point lookups go through SQL — the path a reader actually uses.
    for i in [0, n // 2, n - 1]:
        q = f"SELECT b, body FROM {{v}} WHERE a = '{tenant}' AND b = '00000000-0000-0000-0000-{i:012x}'"
        got = bag(rows(c, sn, q.format(v="b_u")))
        assert len(got) == 1 and set(got.values()) == {1}, f"point lookup {i}: {got}"
        assert got == bag(rows(c, sn, q.format(v="p_u"))), f"point lookup {i}"
    for q in ["SELECT COUNT(*) AS n FROM {v}", "SELECT b FROM {v} ORDER BY b LIMIT 25"]:
        want = bag(rows(c, sn, q.format(v="p_u")))
        assert want and bag(rows(c, sn, q.format(v="b_u"))) == want, q


# ── capacity across restarts ─────────────────────────────────────────────────


def test_a_tight_capacity_holds_the_registered_bytes_down_across_restarts(sweeping_server):
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
    sn = "s" + _uid()
    with gnitz.connect(sweeping_server.sock_path) as c:
        c.create_schema(sn)
        _base_tables(c, sn)
        bid, pid = _twin(c, sn, "v", LINEAR)

    for lo, hi in ((1, 1200), (2001, 5200)):
        with gnitz.connect(sweeping_server.sock_path) as c:
            _churn(c, sn, lo, hi, chunk=100)
            live = bag(c.scan(pid))
            assert bag(c.scan(bid)) == live, f"parity through {hi}"
        assert _any_skeleton(sweeping_server.data_dir, bid)

        sweeping_server.restart(graceful=True)
        # Reading drains whatever the boot still owes, so the directory is not
        # sampled mid-flight.
        with gnitz.connect(sweeping_server.sock_path) as c:
            assert bag(c.scan(bid)) == live, f"first read after the reboot through {hi}"
            assert bag(c.scan(pid)) == live, f"twin after the reboot through {hi}"

        # Twice the floor is the headroom: a shard carries a header and the sweep
        # stops one guard above the fixpoint.
        bounded = sum(os.path.getsize(p) for p in _shard_files(sweeping_server.data_dir, bid))
        floor = len(live) * _SKELETON_ROW_BYTES
        assert bounded <= 2 * max(1024, floor), (
            f"through {hi}: {bounded} bytes over {len(live)} keys, against a declared "
            f"1024 and a skeleton floor of {floor}"
        )
