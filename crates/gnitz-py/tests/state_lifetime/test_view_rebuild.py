"""A view holds the same Z-set after a crash restart as before it.

One claim, one sweep. Every body shape below is built on one server, snapshotted
as a weight-multiset, crash-restarted and compared — so a rebuild that doubles a
shared base, triples a diamond, under-fills a two-base join or truncates a
chunked backfill names itself, and every shape costs one restart rather than one
each.

The comparison is `_read.bag`: weights summed per projected tuple, ghosts
dropped. Row presence is not the observable — every historical regression here
was a *weight*: an exchange closure re-derived once per (view, source) pair came
back exactly doubled, a broadcast replayed per worker came back W-fold, and a
per-key prefix is invisible to any comparison that keys by PK.

The shapes are `_shapes.SHAPES`, each in a schema of its own; every view is
anchored to its expected bag before the restart, so the before/after equality is
the fact this file adds.
"""

import pytest
import gnitz
from _read import bag, scanned
from _serverproc import MULTI, TINY_SCAN_CHUNKS
from _shapes import SHAPES, views_ddl
from _sql import values


def _build(conn):
    """Views before rows, so every row reaches its views through a tick."""
    for shape, (tables, rows, views) in SHAPES.items():
        conn.schema = shape
        conn.create_schema(shape)
        conn.execute_sql("; ".join(filter(None, (tables, views_ddl(views), rows))))


def _snapshot(conn):
    """Every shape's every view, as `{"shape.view": weight-multiset}`."""
    out = {}
    for shape, (_, _, views) in SHAPES.items():
        conn.schema = shape
        for view, (_, cols, _) in views.items():
            out[f"{shape}.{view}"] = bag(scanned(conn, view), *cols)
    return out


# cut -> (server environment, workers the build runs at, workers it reboots at).
# `tail` leaves every row in the SAL; `flushed` checkpoints throughout the build,
# so most rows are on shards when the crash lands; `chunked` shrinks the backfill
# chunk so a handful of rows per worker still spans many rounds with unequal
# partitions; `relayout` reboots the tail at a different count, where every view
# is rebuilt over re-cut slots — and a global aggregate's ground row, which
# lives on the worker its constant key routes to, must move with its owner
# without leaving a second copy behind. `GNITZ_LOG_LEVEL=normal` is what puts the
# "SAL checkpoint epoch=" line in the log — the server defaults to quiet — which
# is how `flushed` proves it is not `tail` under another name.
_CUTS = {
    "tail": ({}, MULTI, MULTI),
    "flushed": ({"GNITZ_CHECKPOINT_BYTES": "65536", "GNITZ_LOG_LEVEL": "normal"}, MULTI, MULTI),
    "chunked": (TINY_SCAN_CHUNKS, MULTI, MULTI),
    "relayout": ({}, 4, 2),
}


@pytest.mark.parametrize("cut", list(_CUTS))
def test_every_view_shape_comes_back_with_the_same_zset(cut, own_server):
    """Build every shape, snapshot it, SIGKILL, and compare. One list of
    divergences, asserted once, so a single broken shape names itself instead of
    hiding the others.

    Every cut runs multi-worker: the regressions above are all unreachable at one
    worker, so a cut at the ambient count passes vacuously under `WORKERS=1`."""
    env, built_at, rebooted_at = _CUTS[cut]
    own_server.extra_env.update(env)
    own_server.start(workers=built_at)
    armed = own_server.sal_checkpoints()

    with gnitz.connect(own_server.target) as conn:
        _build(conn)
        before = _snapshot(conn)

    if cut == "flushed":
        assert own_server.sal_checkpoints() > armed, (
            "no checkpoint fired during the build, so the base is still SAL-only "
            "and this cut is the `tail` cut under another name")

    live = {f"{shape}.{view}": want for shape, (_, _, views) in SHAPES.items()
            for view, (_, _, want) in views.items()}
    assert before == live, "the pre-crash value is wrong"

    own_server.restart(workers=rebooted_at)
    with gnitz.connect(own_server.target) as conn:
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
    tables, rows, views = SHAPES["cluster_by_prefix"]
    with gnitz.connect(own_server.target) as conn:
        conn.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
            "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
            "CREATE VIEW ab AS SELECT val FROM a UNION ALL SELECT val FROM b; "
            f"{tables}; {views_ddl(views)}; {rows}; "
            "INSERT INTO a VALUES (1, 10); INSERT INTO b VALUES (2, 10)")

    own_server.restart()
    with gnitz.connect(own_server.target) as conn:
        ab = conn.resolve_table("ab")
        cbv = conn.resolve_table("cbv")
        # Both branches carry val=10, so UNION ALL holds it at weight 2 — the one
        # multiplicity a row-set comparison of this view could not see.
        assert bag(conn.scan(*ab), "val") == {(10,): 2}

        conn.execute_sql("INSERT INTO a VALUES (3, 30)")
        conn.execute_sql("INSERT INTO b VALUES (4, 40)")
        assert bag(conn.scan(*ab), "val") == {(10,): 2, (30,): 1, (40,): 1}, \
            "the view must tick from both sources"

        more = [(a, 9, a * 100 + 9) for a in range(1, 9)]
        conn.execute_sql(f"INSERT INTO cbt VALUES {values(more)}")
        assert bag(conn.scan(*cbv), "a", "b", "v") == views["cbv"][2] | dict.fromkeys(more, 1), \
            "the post-restart insert must reach the view"


def test_a_min_max_view_decodes_back_to_the_same_value_after_a_restart(own_server):
    """MIN/MAX is the one aggregate whose value lives in a secondary index rather
    than the output row: the AVI stores an order-preserving image, and a resumed
    view must decode back to the value it emitted. One column per arm the image
    has — a signed narrow int (sign-flipped), a full-width unsigned (identity),
    and both float widths (`total_cmp` order).

    MIN/MAX select a stored value rather than folding one, so the float columns
    must come back bit-identical; a tolerance window here would pass a decode
    that lost the low mantissa bits, which is exactly what the image round-trip
    could break."""
    own_server.start()
    conn = gnitz.connect(own_server.target)
    conn.execute_sql(
        "CREATE TABLE t ("
        "  pk BIGINT NOT NULL PRIMARY KEY,"
        "  grp BIGINT NOT NULL,"
        "  i32 INT NOT NULL,"
        "  u64 BIGINT UNSIGNED NOT NULL,"
        "  f32 REAL NOT NULL,"
        "  f64 DOUBLE NOT NULL"
        ")")
    conn.execute_sql(
        "CREATE VIEW v AS SELECT grp,"
        "  MIN(i32) AS i32_lo, MAX(i32) AS i32_hi,"
        "  MIN(u64) AS u64_lo, MAX(u64) AS u64_hi,"
        "  MIN(f32) AS f32_lo, MAX(f32) AS f32_hi,"
        "  MIN(f64) AS f64_lo, MAX(f64) AS f64_hi"
        " FROM t GROUP BY grp")
    # Values chosen so a byte-swap, a missing sign flip or a signed read of the
    # unsigned column each move the extremum: the U64 high-bit value is the max
    # only under unsigned order, and the negative I32 the min only under signed.
    conn.execute_sql(
        "INSERT INTO t VALUES"
        "  (1, 7, -2147483648, 1, -2.25, -2.25),"
        "  (2, 7, 256, 9223372036854775809, 1.5, 1.5),"
        "  (3, 7, 100000, 256, 4.0, 4.0)")

    cols = ["grp", "i32_lo", "i32_hi", "u64_lo", "u64_hi",
            "f32_lo", "f32_hi", "f64_lo", "f64_hi"]
    want = {(7, -2147483648, 100000, 1, 9223372036854775809, -2.25, 4.0, -2.25, 4.0): 1}
    v = conn.resolve_table("v")
    assert bag(conn.scan(*v), *cols) == want, "pre-restart"
    conn.close()

    own_server.restart()
    conn = gnitz.connect(own_server.target)
    v = conn.resolve_table("v")
    assert bag(conn.scan(*v), *cols) == want, "post-restart"

    # A retraction after the restart forces the resumed view back through the
    # index: the extremum recedes to the next stored value, not to the delta's.
    conn.execute_sql("DELETE FROM t WHERE pk = 1")
    assert bag(conn.scan(*v), *cols) == {
        (7, 256, 100000, 256, 9223372036854775809, 1.5, 4.0, 1.5, 4.0): 1}
    conn.close()
