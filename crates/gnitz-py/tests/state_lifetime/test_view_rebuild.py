"""A view holds the same Z-set after a crash restart as before it.

One claim, one sweep. Every body shape below is built on one server, snapshotted
as a weight-multiset, crash-restarted and compared — so a rebuild that doubles a
shared base, triples a diamond, under-fills a two-base join or truncates a
chunked backfill names itself, and nineteen shapes cost one restart rather than
nineteen.

The comparison is `oracle.scan_multiset`: weights summed per projected tuple,
ghosts dropped, floats canonicalized to bits. Row presence is not the observable
— every historical regression here was a *weight*: an exchange closure re-derived
once per (view, source) pair came back exactly doubled, a broadcast replayed per
worker came back W-fold, and a per-key prefix is invisible to any comparison that
keys by PK.

Where a live value is anchored below (`want`), it is because that shape's
pre-restart value is what a regression moved; the rest are pinned live by
`relational_shape/` and `schema_lifetime/test_view_creation_order.py`, so the
before/after equality is the fact this file adds.

Each shape owns its own schema and its own base tables. That isolation is
load-bearing for `bare_join`: neither of its bases roots any other exchange view,
which is what once left an `ExchangeShard`-only classifier with an empty base set
and the join a deterministic per-key prefix at W>1.
"""

from collections import Counter

import pytest
import gnitz
import _oracle as oracle
from _serverproc import NUM_WORKERS


def _values(rows):
    return ", ".join("(" + ", ".join(str(c) for c in r) + ")" for r in rows)


def _shape(ddl, rows, views, want=None):
    return {"ddl": ddl, "rows": rows, "views": views, "want": want or {}}


# ── the shapes ──────────────────────────────────────────────────────────────
# Row counts stay small on purpose: ~24 rows over 4 workers is ~6 per partition,
# which is enough for unequal partitions (the chunked cut's pad rounds) and for
# every aggregate below to be non-trivial, and small enough that the whole sweep
# is one cheap server.

_SB = [(pk, pk % 6, pk + 1) for pk in range(24)]
_GG = [(pk, pk % 2, (pk % 2) + 1) for pk in range(20)]
_GP = [(pk, pk % 5) for pk in range(40)]
_PG = [(pk, pk % 3, pk + 1) for pk in range(24)]
_SIB = [(pk, pk % 4, pk) for pk in range(24)]
_NEST = [(pk, pk * 10) for pk in range(12)]
_JA = [(i, i % 5, i * 10) for i in range(24)]
_JB = [(k, k * 100) for k in range(5)]
_DIA = [(i, i % 4, i + 1) for i in range(24)]
_CT = [(i, 0) for i in range(1, 5)]
_CU = [(i, 0) for i in range(10, 13)]
_RA = [(i, i) for i in range(8)]
_MM = [(pk, pk % 6, (pk * 7) % 50) for pk in range(24)]
_SA = [(i, i % 20) for i in range(40)]
_SB_SET = [(i, (i % 20) + 10) for i in range(40)]
_DT = [(pk, pk % 7) for pk in range(42)]
_CB = [(a, b, a * 100 + b) for a in range(1, 9) for b in range(1, 5)]
_FT = [(i, i % 4, (100 if i <= 18 else 5000)) for i in range(1, 25)]


def _grouped(rows, key, *vals):
    """`Counter({(key, *aggregates): 1})` — the reference a GROUP BY view must
    equal, where each `vals` entry is a row -> number to sum over the group."""
    acc = {}
    for r in rows:
        acc.setdefault(key(r), [0] * len(vals))
        for i, v in enumerate(vals):
            acc[key(r)][i] += v(r)
    return Counter({(k, *tuple(v)): 1 for k, v in acc.items()})


_SHAPES = {
    # A base feeding several exchange views must be driven once, not once per
    # view — the doubling this `want` is here to catch lands as weight 2 on the
    # same group row, which only a weight comparison sees.
    "shared_base_groupby": _shape(
        ["CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
         "g BIGINT NOT NULL, val BIGINT NOT NULL)",
         "CREATE VIEW vx1 AS SELECT g, SUM(val) AS s FROM t GROUP BY g",
         "CREATE VIEW vx2 AS SELECT g, SUM(pk) AS s FROM t GROUP BY g"],
        [f"INSERT INTO t VALUES {_values(_SB)}"],
        {"vx1": ["g", "s"], "vx2": ["g", "s"]},
        {"vx1": _grouped(_SB, lambda r: r[1], lambda r: r[2]),
         "vx2": _grouped(_SB, lambda r: r[1], lambda r: r[0])}),

    # An exchange view over another: driving the base fills the chain
    # transitively, and driving the intermediate as a source too would
    # double-fill the top view in a hashmap-order-dependent way. The decoys
    # perturb that order.
    "groupby_over_groupby": _shape(
        ["CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, "
         "grp BIGINT NOT NULL, val BIGINT NOT NULL)",
         "CREATE VIEW decoy1 AS SELECT grp, COUNT(*) AS c FROM a GROUP BY grp",
         "CREATE VIEW v AS SELECT grp, SUM(val) AS s FROM a GROUP BY grp",
         "CREATE VIEW decoy2 AS SELECT s, COUNT(*) AS c FROM v GROUP BY s",
         "CREATE VIEW g AS SELECT s, COUNT(*) AS c FROM v GROUP BY s"],
        [f"INSERT INTO a VALUES {_values(_GG)}"],
        {"v": ["grp", "s"], "g": ["s", "c"]},
        {"v": Counter({(0, 10): 1, (1, 20): 1}),
         "g": Counter({(10, 1): 1, (20, 1): 1})}),

    # `n` is cascade-reachable through `x`, so the worker non-exchange pass must
    # skip it: the `want` on `n` is weight exactly 1 per row, which is what a
    # regressed inline open-time backfill doubles.
    "groupby_over_projection": _shape(
        ["CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
         "CREATE VIEW n AS SELECT pk, val + 1 AS p1 FROM a",
         "CREATE VIEW x AS SELECT p1, COUNT(*) AS c FROM n GROUP BY p1"],
        [f"INSERT INTO a VALUES {_values(_GP)}"],
        {"n": ["pk", "p1"], "x": ["p1", "c"]},
        {"n": Counter({(pk, v + 1): 1 for pk, v in _GP}),
         "x": Counter({(v + 1, 8): 1 for v in range(5)})}),

    "projection_over_groupby": _shape(
        ["CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
         "g BIGINT NOT NULL, val BIGINT NOT NULL)",
         "CREATE VIEW vx AS SELECT g, SUM(val) AS s FROM t GROUP BY g",
         "CREATE VIEW vn AS SELECT g, s + 1 AS s1 FROM vx"],
        [f"INSERT INTO t VALUES {_values(_PG)}"],
        {"vn": ["g", "s1"]}),

    "projection_sibling_of_groupby": _shape(
        ["CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
         "g BIGINT NOT NULL, val BIGINT NOT NULL)",
         "CREATE VIEW vn AS SELECT pk, val + 1 AS plus1 FROM t",
         "CREATE VIEW vx AS SELECT g, SUM(val) AS s FROM t GROUP BY g"],
        [f"INSERT INTO t VALUES {_values(_SIB)}"],
        {"vn": ["pk", "plus1"], "vx": ["g", "s"]},
        {"vn": Counter({(pk, v + 1): 1 for pk, _, v in _SIB})}),

    # Both views are cascade-unreachable, so the depth sort must fill the inner
    # one first or the outer reads an empty source.
    "nested_projections": _shape(
        ["CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
         "CREATE VIEW v1 AS SELECT pk, val + 1 AS a FROM t",
         "CREATE VIEW v2 AS SELECT pk, a + 1 AS b FROM v1"],
        [f"INSERT INTO t VALUES {_values(_NEST)}"],
        {"v1": ["pk", "a"], "v2": ["pk", "b"]},
        {"v1": Counter({(pk, v + 1): 1 for pk, v in _NEST}),
         "v2": Counter({(pk, v + 2): 1 for pk, v in _NEST})}),

    "joins_sharing_a_base": _shape(
        ["CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, "
         "k BIGINT NOT NULL, av BIGINT NOT NULL)",
         "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, bv BIGINT NOT NULL)",
         "CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, cv BIGINT NOT NULL)",
         "CREATE VIEW x AS SELECT a.id AS aid, a.av AS av, b.bv AS bv "
         "FROM a JOIN b ON a.k = b.id",
         "CREATE VIEW y AS SELECT a.id AS aid, a.av AS av, c.cv AS cv "
         "FROM a JOIN c ON a.k = c.id"],
        [f"INSERT INTO a VALUES {_values(_JA)}",
         f"INSERT INTO b VALUES {_values(_JB)}",
         f"INSERT INTO c VALUES {_values(_JB)}"],
        {"x": ["aid", "av", "bv"], "y": ["aid", "av", "cv"]}),

    # The minimal two-base equi-join, on bases nothing else reads: an
    # `ExchangeShard`-only classifier drove nothing for it and it came back a
    # deterministic per-key prefix at W>1, so the live `want` is the anchor.
    "bare_join": _shape(
        ["CREATE TABLE ja (id BIGINT NOT NULL PRIMARY KEY, "
         "k BIGINT NOT NULL, av BIGINT NOT NULL)",
         "CREATE TABLE jb (id BIGINT NOT NULL PRIMARY KEY, bv BIGINT NOT NULL)",
         "CREATE VIEW jx AS SELECT ja.id AS aid, ja.av AS av, jb.bv AS bv "
         "FROM ja JOIN jb ON ja.k = jb.id"],
        [f"INSERT INTO ja VALUES {_values(_JA)}",
         f"INSERT INTO jb VALUES {_values(_JB)}"],
        {"jx": ["aid", "av", "bv"]},
        {"jx": Counter({(i, av, k * 100): 1 for i, k, av in _JA})}),

    # `x` reaches `a` by two paths; within one drive it is evaluated once per
    # incoming edge, which is correct multi-input behaviour and not a re-run.
    "diamond": _shape(
        ["CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, "
         "g BIGINT NOT NULL, val BIGINT NOT NULL)",
         "CREATE VIEW v1 AS SELECT g, SUM(val) AS s FROM a GROUP BY g",
         "CREATE VIEW x AS SELECT a.id AS id, a.g AS g, v1.s AS s "
         "FROM a JOIN v1 ON a.g = v1.g"],
        [f"INSERT INTO a VALUES {_values(_DIA)}"],
        {"x": ["id", "g", "s"]}),

    # A keyless step broadcasts its delta instead of exchanging it, so a
    # broadcast replayed per worker returns a W-fold duplication.
    "cross_join": _shape(
        ["CREATE TABLE ct (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
         "CREATE TABLE cu (id BIGINT NOT NULL PRIMARY KEY, w BIGINT NOT NULL)",
         "CREATE VIEW cv AS SELECT ct.id AS tid, cu.id AS uid "
         "FROM ct CROSS JOIN cu"],
        [f"INSERT INTO ct VALUES {_values(_CT)}",
         f"INSERT INTO cu VALUES {_values(_CU)}"],
        {"cv": ["tid", "uid"]},
        {"cv": Counter({(t, u): 1 for t, _ in _CT for u, _ in _CU})}),

    "range_join": _shape(
        ["CREATE TABLE ra (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)",
         "CREATE TABLE rb (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)",
         "CREATE VIEW rv AS SELECT ra.x AS x, rb.y AS y "
         "FROM ra JOIN rb ON ra.x < rb.y"],
        [f"INSERT INTO ra VALUES {_values(_RA)}",
         f"INSERT INTO rb VALUES {_values(_RA)}"],
        {"rv": ["x", "y"]},
        {"rv": Counter({(x, y): 1 for _, x in _RA for _, y in _RA if x < y})}),

    # The DELETE removes group 0's MIN holder, so the live trace carries a
    # MIN-retraction recompute before the snapshot. AVG is one exact division,
    # so it is bit-stable across a rebuild and stays in the projection.
    "groupby_min_max_avg": _shape(
        ["CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
         "g BIGINT NOT NULL, a BIGINT NOT NULL)",
         "CREATE VIEW v AS SELECT g, MIN(a) AS lo, MAX(a) AS hi, AVG(a) AS av, "
         "COUNT(*) AS c FROM t GROUP BY g"],
        [f"INSERT INTO t VALUES {_values(_MM)}",
         "DELETE FROM t WHERE pk = 0"],
        {"v": ["g", "lo", "hi", "av", "c"]}),

    # Four set-ops over one overlapping pair: UNION ALL's weight-2 duplicates,
    # the deduplicating ops' boundary state, and EXCEPT/INTERSECT's distinct and
    # positive_part integrals all rebuild together. The DELETE removes both
    # carriers of b.val=15, so 15 re-enters EXCEPT and leaves INTERSECT.
    "set_ops": _shape(
        ["CREATE TABLE sa (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
         "CREATE TABLE sb (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
         "CREATE VIEW v_all AS SELECT val FROM sa UNION ALL SELECT val FROM sb",
         "CREATE VIEW v_union AS SELECT val FROM sa UNION SELECT val FROM sb",
         "CREATE VIEW v_isect AS SELECT val FROM sa INTERSECT SELECT val FROM sb",
         "CREATE VIEW v_exc AS SELECT val FROM sa EXCEPT SELECT val FROM sb"],
        [f"INSERT INTO sa VALUES {_values(_SA)}",
         f"INSERT INTO sb VALUES {_values(_SB_SET)}",
         "DELETE FROM sb WHERE pk IN (5, 25)"],
        {"v_all": ["val"], "v_union": ["val"],
         "v_isect": ["val"], "v_exc": ["val"]}),

    # Every surviving value keeps at least one carrier, so the deduplicated set
    # is stable across the DELETE and the rebuild alike.
    "distinct": _shape(
        ["CREATE TABLE dt (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL)",
         "CREATE VIEW dv AS SELECT DISTINCT g FROM dt"],
        [f"INSERT INTO dt VALUES {_values(_DT)}",
         "DELETE FROM dt WHERE pk = 0"],
        {"dv": ["g"]},
        {"dv": Counter({(g,): 1 for g in range(7)})}),

    # A linear view over a CLUSTER BY proper prefix sits on the worker owning the
    # source's distribution prefix. That placement is folded at registration and
    # written to no wire format, so boot re-derives it; re-deriving the full-PK
    # default instead addresses the resumed rows by a key naming a different
    # worker, and the view comes back a per-key prefix of itself.
    "cluster_by_prefix": _shape(
        ["CREATE TABLE cbt (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, "
         "v BIGINT NOT NULL, PRIMARY KEY (a, b)) CLUSTER BY a",
         "CREATE VIEW cbv AS SELECT a, b, v FROM cbt WHERE v > 0"],
        [f"INSERT INTO cbt VALUES {_values(_CB)}"],
        {"cbv": ["a", "b", "v"]},
        {"cbv": Counter({r: 1 for r in _CB})}),

    # A contiguous low-id range fails the predicate on every worker's first
    # chunks, so the relayed pre-phase payload for those rounds is empty.
    # Inferring "done" from an empty relay truncates the view; the pad bit tracks
    # the raw drain instead, so the later passing rows still arrive.
    "filtered_groupby": _shape(
        ["CREATE TABLE ft (id BIGINT NOT NULL PRIMARY KEY, "
         "category BIGINT NOT NULL, amount BIGINT NOT NULL)",
         "CREATE VIEW fv AS SELECT category, COUNT(*) AS cnt, SUM(amount) AS total "
         "FROM ft WHERE amount > 1000 GROUP BY category"],
        [f"INSERT INTO ft VALUES {_values(_FT)}"],
        {"fv": ["category", "cnt", "total"]},
        {"fv": _grouped([r for r in _FT if r[2] > 1000],
                        lambda r: r[1], lambda r: 1, lambda r: r[2])}),

    # A global (ungrouped) aggregate's ground row: the seed must re-fire at boot
    # and exactly one row must survive, on the partition the constant key routes
    # to rather than on every worker.
    "global_agg_never_populated": _shape(
        ["CREATE TABLE gt (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
         "CREATE VIEW gv AS SELECT COUNT(*) AS cnt, SUM(a) AS total FROM gt"],
        [],
        {"gv": ["cnt", "total"]},
        {"gv": Counter({(0, None): 1})}),

    "global_agg_emptied": _shape(
        ["CREATE TABLE gt (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
         "CREATE VIEW gv AS SELECT COUNT(*) AS cnt, MIN(a) AS lo FROM gt"],
        ["INSERT INTO gt VALUES (1, 5), (2, 8)", "DELETE FROM gt"],
        {"gv": ["cnt", "lo"]},
        {"gv": Counter({(0, None): 1})}),

    # The replicated source takes the `i_am_owner` disjunct rather than the
    # partition route, and must still seed exactly one ground row.
    "global_agg_replicated": _shape(
        ["CREATE TABLE gt (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL) "
         "WITH (replicated = true)",
         "CREATE VIEW gv AS SELECT COUNT(*) AS cnt, SUM(a) AS total FROM gt"],
        [],
        {"gv": ["cnt", "total"]},
        {"gv": Counter({(0, None): 1})}),
}


def _build(conn):
    for name, sh in _SHAPES.items():
        conn.create_schema(name)
        for stmt in sh["ddl"] + sh["rows"]:
            conn.execute_sql(stmt, schema_name=name)


def _snapshot(conn):
    """Every shape's every view, as `{"shape.view": weight-multiset}`."""
    out = {}
    for name, sh in _SHAPES.items():
        for view, cols in sh["views"].items():
            vid, _ = conn.resolve_table(name, view)
            out[f"{name}.{view}"] = oracle.scan_multiset(conn, vid, cols)
    return out


# How the pre-crash state is cut. `tail` leaves every row in the SAL; `flushed`
# runs checkpoints throughout the build so most rows are already on shards when
# the crash lands; `chunked` shrinks the backfill chunk so a handful of rows per
# worker still spans many chunked rounds with unequal partitions, exercising the
# lockstep padding and the pad-bit termination.
_MULTI = max(2, NUM_WORKERS)
_CUTS = {
    "tail": ({}, None),
    "flushed": ({"GNITZ_CHECKPOINT_BYTES": "16384", "GNITZ_LOG_LEVEL": "normal"}, None),
    "chunked": ({"GNITZ_SCAN_CHUNK_ROWS": "3"}, _MULTI),
}


@pytest.mark.parametrize("cut", list(_CUTS))
def test_every_view_shape_comes_back_with_the_same_zset(cut, own_server):
    """Build every shape, snapshot it, SIGKILL, and compare. One list of
    divergences, asserted once, so a single broken shape names itself instead of
    hiding the other eighteen."""
    env, workers = _CUTS[cut]
    own_server.extra_env.update(env)
    own_server.start(workers=workers)
    armed = own_server.sal_checkpoints()

    with gnitz.connect(own_server.sock_path) as conn:
        _build(conn)
        before = _snapshot(conn)

    if cut == "flushed":
        assert own_server.sal_checkpoints() > armed, (
            "no checkpoint fired during the build, so the base is still SAL-only "
            "and this cut is the `tail` cut under another name")

    empty = sorted(k for k, v in before.items() if not v)
    assert not empty, f"an empty view compares equal to anything: {empty}"
    live = {f"{n}.{v}": w for n, sh in _SHAPES.items()
            for v, w in sh["want"].items()}
    assert {k: before[k] for k in live} == live, "the pre-crash value is wrong"

    own_server.restart()
    with gnitz.connect(own_server.sock_path) as conn:
        after = _snapshot(conn)

    diverged = {k: (before[k], after[k]) for k in before if before[k] != after[k]}
    assert not diverged, "views diverged across the rebuild:\n" + "\n".join(
        f"  {k}: before={b} after={a}" for k, (b, a) in diverged.items())


def test_a_recovered_view_still_ticks_from_every_source(own_server):
    """A rebuilt view is a starting state, not a final one. Both sources of a
    two-base view must drive it after the boundary, and a further insert into a
    `CLUSTER BY` source must land in the store the resume loaded rather than in
    one addressed by a re-derived full-PK key."""
    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        conn.create_schema("tick")
        conn.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="tick")
        conn.execute_sql(
            "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="tick")
        conn.execute_sql(
            "CREATE VIEW ab AS SELECT val FROM a UNION ALL SELECT val FROM b",
            schema_name="tick")
        conn.execute_sql(
            "CREATE TABLE cbt (a BIGINT UNSIGNED NOT NULL, b BIGINT UNSIGNED NOT NULL, "
            "v BIGINT NOT NULL, PRIMARY KEY (a, b)) CLUSTER BY a", schema_name="tick")
        conn.execute_sql("CREATE VIEW cbv AS SELECT a, b, v FROM cbt WHERE v > 0",
                         schema_name="tick")
        conn.execute_sql("INSERT INTO a VALUES (1, 10)", schema_name="tick")
        conn.execute_sql("INSERT INTO b VALUES (2, 10)", schema_name="tick")
        conn.execute_sql(f"INSERT INTO cbt VALUES {_values(_CB)}", schema_name="tick")

    own_server.restart()
    with gnitz.connect(own_server.sock_path) as conn:
        ab, _ = conn.resolve_table("tick", "ab")
        cbv, _ = conn.resolve_table("tick", "cbv")
        # Both branches carry val=10, so UNION ALL holds it at weight 2 — the one
        # multiplicity a row-set comparison of this view could not see.
        assert oracle.scan_multiset(conn, ab, ["val"]) == Counter({(10,): 2})

        conn.execute_sql("INSERT INTO a VALUES (3, 30)", schema_name="tick")
        conn.execute_sql("INSERT INTO b VALUES (4, 40)", schema_name="tick")
        assert oracle.scan_multiset(conn, ab, ["val"]) == Counter(
            {(10,): 2, (30,): 1, (40,): 1}), "the view must tick from both sources"

        more = [(a, 9, a * 100 + 9) for a in range(1, 9)]
        conn.execute_sql(f"INSERT INTO cbt VALUES {_values(more)}", schema_name="tick")
        assert oracle.scan_multiset(conn, cbv, ["a", "b", "v"]) == Counter(
            {r: 1 for r in _CB + more}), "the post-restart insert must reach the view"


def test_a_backfill_that_reclaims_the_sal_every_round_still_rebuilds_exactly(own_server):
    """The worst-case relay — a pure range join, which broadcasts because it has
    no eq prefix to scatter on — with the reclaim seam forcing a checkpoint and
    SAL reset on every backfill round. Workers must re-epoch inline as they
    consume each relay and the master must reset its write side at the next round
    barrier; if any of that is wrong the backfill deadlocks or the view is short.
    """
    own_server.extra_env.update({"GNITZ_SCAN_CHUNK_ROWS": "3",
                                 "GNITZ_INJECT_BACKFILL_RELAY_SPACE_LOW": "1"})
    own_server.start(workers=_MULTI)
    sh = _SHAPES["range_join"]
    with gnitz.connect(own_server.sock_path) as conn:
        conn.create_schema("rr")
        for stmt in sh["ddl"] + sh["rows"]:
            conn.execute_sql(stmt, schema_name="rr")

    # Both boots run under the seam: the restart's backfill is what has to
    # survive it.
    own_server.restart()
    with gnitz.connect(own_server.sock_path) as conn:
        vid, _ = conn.resolve_table("rr", "rv")
        assert oracle.scan_multiset(conn, vid, ["x", "y"]) == sh["want"]["rv"]


def test_a_min_max_view_decodes_back_to_the_same_value_after_a_restart(own_server):
    """MIN/MAX is the one aggregate whose value lives in a secondary index rather
    than the output row: the AVI stores an order-preserving image, and a resumed
    view must decode back to the value it emitted. One column per arm the image
    has — a signed narrow int (sign-flipped), a full-width unsigned (identity),
    and both float widths (`total_cmp` order, F32 widening to an F64 output).

    MIN/MAX select a stored value rather than folding one, so the float columns
    must come back bit-identical; a tolerance window here would pass a decode
    that lost the low mantissa bits, which is exactly what the image round-trip
    could break."""
    own_server.start()
    conn = gnitz.connect(own_server.sock_path)
    conn.create_schema("mmr")
    conn.execute_sql(
        "CREATE TABLE t ("
        "  pk BIGINT NOT NULL PRIMARY KEY,"
        "  grp BIGINT NOT NULL,"
        "  i32 INT NOT NULL,"
        "  u64 BIGINT UNSIGNED NOT NULL,"
        "  f32 REAL NOT NULL,"
        "  f64 DOUBLE NOT NULL"
        ")",
        schema_name="mmr")
    conn.execute_sql(
        "CREATE VIEW v AS SELECT grp,"
        "  MIN(i32) AS i32_lo, MAX(i32) AS i32_hi,"
        "  MIN(u64) AS u64_lo, MAX(u64) AS u64_hi,"
        "  MIN(f32) AS f32_lo, MAX(f32) AS f32_hi,"
        "  MIN(f64) AS f64_lo, MAX(f64) AS f64_hi"
        " FROM t GROUP BY grp",
        schema_name="mmr")
    # Values chosen so a byte-swap, a missing sign flip or a signed read of the
    # unsigned column each move the extremum: the U64 high-bit value is the max
    # only under unsigned order, and the negative I32 the min only under signed.
    conn.execute_sql(
        "INSERT INTO t VALUES"
        "  (1, 7, -2147483648, 1, -2.25, -2.25),"
        "  (2, 7, 256, 9223372036854775809, 1.5, 1.5),"
        "  (3, 7, 100000, 256, 4.0, 4.0)",
        schema_name="mmr")

    cols = ["grp", "i32_lo", "i32_hi", "u64_lo", "u64_hi",
            "f32_lo", "f32_hi", "f64_lo", "f64_hi"]
    want = Counter({(7, -2147483648, 100000, 1, 9223372036854775809,
                     -2.25, 4.0, -2.25, 4.0): 1})
    vid, _ = conn.resolve_table("mmr", "v")
    oracle.assert_view_matches(conn, vid, cols, want, "pre-restart")
    conn.close()

    own_server.restart()
    conn = gnitz.connect(own_server.sock_path)
    vid, _ = conn.resolve_table("mmr", "v")
    oracle.assert_view_matches(conn, vid, cols, want, "post-restart")

    # A retraction after the restart forces the resumed view back through the
    # index: the extremum recedes to the next stored value, not to the delta's.
    conn.execute_sql("DELETE FROM t WHERE pk = 1", schema_name="mmr")
    oracle.assert_view_matches(conn, vid, cols, Counter(
        {(7, 256, 100000, 256, 9223372036854775809, 1.5, 4.0, 1.5, 4.0): 1}),
        "after the extremum was retracted")
    conn.close()
