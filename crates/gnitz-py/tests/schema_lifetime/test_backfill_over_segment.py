"""E2E: a linear final over a hidden segment must backfill.

A CREATE VIEW whose final (or an intermediate) circuit is **linear** — no `Join`,
no `ExchangeShard` — but whose delta source is an in-bundle hidden segment
carrying one (a grouped or joined CTE / derived table) once silently lost ALL
pre-existing base data on the data-before-view path: the view was filled inline
at registration, from a sibling segment that was still empty. Every view is now
populated by one dependency-ordered distributed driver, so a source segment is
already filled when the view reading it takes its turn.

Each test asserts **data-before-view == view-before-data** (weights, not just row
presence) and that a post-create insert *adds to* the backfilled rows and a
delete of a backfilled row retracts it weight-correctly. Two structural guards
pin that ordering costs no exchange: neither a linear view over a grouped CTE nor
one over a linear CTE carries an `ExchangeShard`, while the grouped CTE's own
segment keeps the one its reduce needs.

Run at GNITZ_WORKERS=4 (the exchange/fanout paths only engage at W>1):
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest \
        tests/schema_lifetime/test_backfill_over_segment.py -v --tb=short
"""

from gnitz import Opcode, CIRCUIT_NODES_TAB, VIEW_TAB
from _uid import uid as _uid




def _cleanup(client, sn, tables=None, views=None):
    for name in (views or []):
        try:
            client.execute_sql(f"DROP VIEW {name}", schema_name=sn)
        except Exception:
            pass
    for name in (tables or []):
        try:
            client.execute_sql(f"DROP TABLE {name}", schema_name=sn)
        except Exception:
            pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _weights(client, sn, view, cols):
    """row-tuple over `cols` → net weight."""
    vid = client.resolve_table(sn, view)[0]
    m = {}
    for r in client.scan(vid):
        d = r._asdict()
        key = tuple(d[c] for c in cols)
        m[key] = m.get(key, 0) + r.weight
    return m


def _sole_segment_vid(client, owner_vid):
    """The vid of the one internal chain segment `owner_vid` owns.

    Read off VIEW_TAB's ownership column — the same thing the engine's drop
    cascade keys on — so nothing here assumes how the ids were allocated.
    """
    segs = [r["view_id"] for r in client.scan(VIEW_TAB)
            if r["owner_view_id"] == owner_vid]
    assert len(segs) == 1, f"expected exactly one segment, got {segs}"
    return segs[0]


def _has_exchange_shard(client, vid):
    """True iff any `ExchangeShard` circuit node belongs to `vid`."""
    return any(
        r["view_id"] == vid and r["opcode"] == Opcode.ExchangeShard
        for r in client.scan(CIRCUIT_NODES_TAB)
    )


def test_bfseg_linear_over_grouped_cte(client):
    """Linear final over a grouped CTE — the canonical repro. Both orderings
    must agree, and the backfilled rows survive a later insert and a delete."""
    view = (
        "CREATE VIEW s AS WITH c AS (SELECT id, SUM(v) AS sv FROM t GROUP BY id) "
        "SELECT id FROM c WHERE sv > 10"
    )
    # data-before-view
    sn_d = "bfsegd" + _uid()
    client.create_schema(sn_d)
    # view-before-data
    sn_v = "bfsegv" + _uid()
    client.create_schema(sn_v)
    try:
        for sn, view_first in ((sn_d, False), (sn_v, True)):
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
                schema_name=sn,
            )
            if view_first:
                client.execute_sql(view, schema_name=sn)
                client.execute_sql("INSERT INTO t VALUES (1, 20), (2, 5)", schema_name=sn)
            else:
                client.execute_sql("INSERT INTO t VALUES (1, 20), (2, 5)", schema_name=sn)
                client.execute_sql(view, schema_name=sn)

        # The backfill (data-before-view) must equal the steady state (view-first).
        got_d = _weights(client, sn_d, "s", ["id"])
        got_v = _weights(client, sn_v, "s", ["id"])
        assert got_d == {(1,): 1}, f"data-before-view lost backfill: {got_d}"
        assert got_d == got_v, f"backfill != steady state: {got_d} vs {got_v}"

        # The ordering costs no exchange: the final is a plain filter/projection
        # and carries none, while the grouped CTE's own segment keeps the one its
        # reduce needs. An `ExchangeShard` here would be a cluster-wide IPC
        # barrier on every epoch, paid solely to order the backfill.
        vid = client.resolve_table(sn_d, "s")[0]
        seg_vid = _sole_segment_vid(client, vid)
        assert not _has_exchange_shard(client, vid), \
            "a linear view over a grouped CTE must not be sharded to order its backfill"
        assert _has_exchange_shard(client, seg_vid), \
            "the grouped CTE segment's own reduce exchange must survive"

        # A post-create insert ADDS to the backfilled rows (does not replace them).
        client.execute_sql("INSERT INTO t VALUES (3, 30)", schema_name=sn_d)
        assert _weights(client, sn_d, "s", ["id"]) == {(1,): 1, (3,): 1}

        # Deleting a backfilled row retracts it weight-correctly.
        client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn_d)
        assert _weights(client, sn_d, "s", ["id"]) == {(3,): 1}
    finally:
        _cleanup(client, sn_d, tables=["t"], views=["s"])
        _cleanup(client, sn_v, tables=["t"], views=["s"])


def test_bfseg_linear_over_join_cte(client):
    """Linear final over a join CTE — the join body seeds via its `Join` node."""
    view = (
        "CREATE VIEW s AS WITH c AS (SELECT a.id AS id, b.x AS x FROM a JOIN b ON a.k = b.id) "
        "SELECT id FROM c WHERE x > 0"
    )
    sn_d = "bfsegjd" + _uid()
    client.create_schema(sn_d)
    sn_v = "bfsegjv" + _uid()
    client.create_schema(sn_v)
    try:
        for sn, view_first in ((sn_d, False), (sn_v, True)):
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)",
                schema_name=sn,
            )
            ins_a = "INSERT INTO a VALUES (1, 10), (2, 20), (3, 10)"
            ins_b = "INSERT INTO b VALUES (10, 5), (20, 0)"  # b(20).x = 0 excluded by x > 0
            if view_first:
                client.execute_sql(view, schema_name=sn)
                client.execute_sql(ins_a, schema_name=sn)
                client.execute_sql(ins_b, schema_name=sn)
            else:
                client.execute_sql(ins_a, schema_name=sn)
                client.execute_sql(ins_b, schema_name=sn)
                client.execute_sql(view, schema_name=sn)
        # a1→b10(x=5>0)✓ id=1; a3→b10(x=5>0)✓ id=3; a2→b20(x=0)✗.
        got_d = _weights(client, sn_d, "s", ["id"])
        got_v = _weights(client, sn_v, "s", ["id"])
        assert got_d == {(1,): 1, (3,): 1}, f"data-before-view lost backfill: {got_d}"
        assert got_d == got_v, f"backfill != steady state: {got_d} vs {got_v}"

        client.execute_sql("INSERT INTO a VALUES (4, 20)", schema_name=sn_d)  # b20.x=0 ✗
        client.execute_sql("INSERT INTO b VALUES (30, 9)", schema_name=sn_d)
        client.execute_sql("INSERT INTO a VALUES (5, 30)", schema_name=sn_d)  # b30.x=9>0 ✓
        assert _weights(client, sn_d, "s", ["id"]) == {(1,): 1, (3,): 1, (5,): 1}
    finally:
        _cleanup(client, sn_d, tables=["a", "b"], views=["s"])
        _cleanup(client, sn_v, tables=["a", "b"], views=["s"])


def test_bfseg_linear_hidden_between_two_seeding(client):
    """A linear hidden segment between two exchanging ones — the
    `compile_hidden_body` linear-arm coverage: a grouped CTE `g`, a linear CTE
    `l` over it (the middle segment, which shards nothing itself), and a grouped
    final over `l`."""
    view = (
        "CREATE VIEW s AS "
        "WITH g AS (SELECT id, SUM(v) AS sv FROM t GROUP BY id), "
        "     l AS (SELECT id, sv FROM g WHERE sv > 10) "
        "SELECT sv, COUNT(*) AS c FROM l GROUP BY sv"
    )
    sn_d = "bfseg3d" + _uid()
    client.create_schema(sn_d)
    sn_v = "bfseg3v" + _uid()
    client.create_schema(sn_v)
    try:
        for sn, view_first in ((sn_d, False), (sn_v, True)):
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
                schema_name=sn,
            )
            ins = "INSERT INTO t VALUES (1, 20), (2, 5), (3, 20), (4, 30)"
            if view_first:
                client.execute_sql(view, schema_name=sn)
                client.execute_sql(ins, schema_name=sn)
            else:
                client.execute_sql(ins, schema_name=sn)
                client.execute_sql(view, schema_name=sn)
        # per-id sv: 1→20, 2→5(dropped), 3→20, 4→30. sv>10 keeps {20,20,30}.
        # GROUP BY sv: sv=20 count 2, sv=30 count 1.
        got_d = _weights(client, sn_d, "s", ["sv", "c"])
        got_v = _weights(client, sn_v, "s", ["sv", "c"])
        assert got_d == {(20, 2): 1, (30, 1): 1}, f"data-before-view lost backfill: {got_d}"
        assert got_d == got_v, f"backfill != steady state: {got_d} vs {got_v}"

        client.execute_sql("INSERT INTO t VALUES (5, 30)", schema_name=sn_d)
        assert _weights(client, sn_d, "s", ["sv", "c"]) == {(20, 2): 1, (30, 2): 1}
    finally:
        _cleanup(client, sn_d, tables=["t"], views=["s"])
        _cleanup(client, sn_v, tables=["t"], views=["s"])


def test_bfseg_linear_over_preexisting_grouped_view(client):
    """A view over a grouped view created by a *separate, earlier* DDL — no
    shared bundle, so the source is already fully populated when the second
    CREATE runs. This is the shape the retired inline backfill served, and the
    one driver must still return its rows."""
    sn = "bfsegpre" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, v BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO t VALUES (1, 7, 20), (2, 7, 5), (3, 9, 4)", schema_name=sn
        )
        client.execute_sql(
            "CREATE VIEW g AS SELECT g, SUM(v) AS sv FROM t GROUP BY g", schema_name=sn
        )
        assert _weights(client, sn, "g", ["g", "sv"]) == {(7, 25): 1, (9, 4): 1}

        # Separate DDL, over a source that is already full.
        client.execute_sql("CREATE VIEW s AS SELECT g FROM g WHERE sv > 10", schema_name=sn)
        assert _weights(client, sn, "s", ["g"]) == {(7,): 1}

        # And it stays incremental over the pre-existing view.
        client.execute_sql("INSERT INTO t VALUES (4, 9, 30)", schema_name=sn)
        assert _weights(client, sn, "s", ["g"]) == {(7,): 1, (9,): 1}
    finally:
        _cleanup(client, sn, tables=["t"], views=["s", "g"])


def test_bfseg_linear_over_linear_cte_not_oversharded(client):
    """Negative-cost guard: a linear view over a *linear* CTE compiles with NO
    `ExchangeShard` node — a filter/projection neither re-keys nor redistributes
    its source, whatever the source is."""
    sn = "bfsegneg" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name=sn,
        )
        # `c` is a linear CTE (WHERE only, no join/group) → a linear hidden
        # segment that does not seed; `s` is a linear final over it.
        client.execute_sql(
            "CREATE VIEW s AS WITH c AS (SELECT id, v FROM t WHERE v > 0) "
            "SELECT id FROM c WHERE v > 5",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 3), (2, 8)", schema_name=sn)
        vid = client.resolve_table(sn, "s")[0]
        assert not _has_exchange_shard(client, vid), \
            "a linear view over a linear CTE must not be sharded"
        # And it still backfills / maintains correctly (v>5 keeps id=2).
        assert _weights(client, sn, "s", ["id"]) == {(2,): 1}
    finally:
        _cleanup(client, sn, tables=["t"], views=["s"])
