"""A view holds the same Z-set after a restart as before it, and keeps ticking
from it.

One claim, one sweep. Every body shape below is built on one server, snapshotted
as a weight-multiset, restarted and compared — so a rebuild that doubles a
shared base, triples a diamond, under-fills a two-base join or truncates a
chunked backfill names itself, and every shape costs one restart rather than one
each. A weight is the observable: an exchange closure re-derived once per
(view, source) pair comes back exactly doubled, a broadcast replayed per worker
W-fold, and a per-key prefix is invisible to any comparison that keys by PK.

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
        conn.execute_sql(f'CREATE SCHEMA "{shape}"')
        conn.execute_sql("; ".join(filter(None, (tables, views_ddl(views), rows))))


def _snapshot(conn):
    """Every shape's every view, as `{"shape.view": weight-multiset}`."""
    out = {}
    for shape, (_, _, views) in SHAPES.items():
        conn.schema = shape
        for view, (_, cols, _) in views.items():
            out[f"{shape}.{view}"] = bag(scanned(conn, view), *cols)
    return out


# cut -> (server environment, workers the build runs at, workers it reboots at,
# whether it stops gracefully). `graceful` checkpoints the whole build, so every
# view resumes; `tail` leaves every row in the SAL; `flushed` checkpoints
# throughout the build, so most rows are on shards when the crash lands;
# `chunked` shrinks the backfill chunk so a handful of rows per worker still
# spans many rounds with unequal partitions; `relayout` reboots the tail at a
# different count, where every view is rebuilt over re-cut slots — and a global
# aggregate's ground row, which lives on the worker its constant key routes to,
# must move with its owner without leaving a second copy behind.
_CUTS = {
    "graceful": ({}, MULTI, MULTI, True),
    "tail": ({}, MULTI, MULTI, False),
    "flushed": ({"GNITZ_CHECKPOINT_BYTES": "65536"}, MULTI, MULTI, False),
    "chunked": (TINY_SCAN_CHUNKS, MULTI, MULTI, False),
    "relayout": ({}, 4, 2, False),
}


@pytest.mark.parametrize("cut", list(_CUTS))
def test_every_view_shape_comes_back_with_the_same_zset(cut, own_server):
    """Build every shape, snapshot it, restart, and compare. One list of
    divergences, asserted once, so a single broken shape names itself instead of
    hiding the others.

    Every cut runs multi-worker: the regressions above are all unreachable at one
    worker, so a cut at the ambient count passes vacuously under `WORKERS=1`."""
    env, built_at, rebooted_at, graceful = _CUTS[cut]
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

    own_server.restart(graceful=graceful, workers=rebooted_at)
    if graceful:
        assert own_server.rebuilt_view_count() == 0, "a checkpointed view must resume"
    with gnitz.connect(own_server.target) as conn:
        after = _snapshot(conn)

    diverged = {k: (before[k], after[k]) for k in before if before[k] != after[k]}
    assert not diverged, "views diverged across the restart:\n" + "\n".join(
        f"  {k}: before={b} after={a}" for k, (b, a) in diverged.items())


_CB_TABLES, _CB_ROWS, _CB_VIEWS = SHAPES["cluster_by_prefix"]
_CB_BODY, _, _CB = _CB_VIEWS["cbv"]
_CB_MORE = [(a, 9, a * 100 + 9) for a in range(1, 9)]

# name -> (tables, rows, the body of its view `v`, the view's value, and the
# writes after the restart, each with the value it leaves).
_TICKS = {
    # Both branches carry val=10, so UNION ALL holds it at weight 2; both sources
    # must drive the view after the boundary.
    "union_of_two_sources": (
        "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
        "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        "INSERT INTO a VALUES (1, 10); INSERT INTO b VALUES (2, 10)",
        "SELECT val FROM a UNION ALL SELECT val FROM b",
        {(10,): 2},
        [("INSERT INTO a VALUES (3, 30); INSERT INTO b VALUES (4, 40)",
          {(10,): 2, (30,): 1, (40,): 1})]),
    # An insert must land in the store the boot opened, not in one addressed by a
    # re-derived full-PK key.
    "cluster_by_prefix": (
        _CB_TABLES, _CB_ROWS, _CB_BODY, _CB,
        [(f"INSERT INTO cbt VALUES {values(_CB_MORE)}", _CB | dict.fromkeys(_CB_MORE, 1))]),
    # MIN/MAX keeps its value in an index of order-preserving images, one column
    # per arm the image has — a signed narrow int (sign-flipped), a full-width
    # unsigned (identity), both float widths (`total_cmp` order) — and must decode
    # back bit-identical. The U64 high-bit value is the max only under unsigned
    # order, the negative I32 the min only under signed. A retraction forces the
    # view back through the index: the extremum recedes to the next stored value.
    "min_max_images": (
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, i32 INT NOT NULL, "
        "u64 BIGINT UNSIGNED NOT NULL, f32 REAL NOT NULL, f64 DOUBLE NOT NULL)",
        "INSERT INTO t VALUES (1, 7, -2147483648, 1, -2.25, -2.25), "
        "(2, 7, 256, 9223372036854775809, 1.5, 1.5), (3, 7, 100000, 256, 4.0, 4.0)",
        "SELECT grp, MIN(i32) AS i32_lo, MAX(i32) AS i32_hi, MIN(u64) AS u64_lo, MAX(u64) AS u64_hi, "
        "MIN(f32) AS f32_lo, MAX(f32) AS f32_hi, MIN(f64) AS f64_lo, MAX(f64) AS f64_hi FROM t GROUP BY grp",
        {(7, -2147483648, 100000, 1, 9223372036854775809, -2.25, 4.0, -2.25, 4.0): 1},
        [("DELETE FROM t WHERE pk = 1",
          {(7, 256, 100000, 256, 9223372036854775809, 1.5, 4.0, 1.5, 4.0): 1})]),
    # The index below the cut must promote the next row into the window.
    "top_n": (
        "CREATE TABLE s (id BIGINT NOT NULL PRIMARY KEY, score BIGINT)",
        "INSERT INTO s VALUES (1, 10), (2, 30), (3, 20)",
        "SELECT id, score FROM s ORDER BY score DESC LIMIT 2",
        {(2, 30): 1, (3, 20): 1},
        [("DELETE FROM s WHERE id = 2", {(3, 20): 1, (1, 10): 1})]),
    # A window's index of every input row is its whole history: a new row ranks
    # behind the rows written before it, and a retraction moves the rows behind it.
    "window": (
        "CREATE TABLE s (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, score BIGINT NOT NULL)",
        "INSERT INTO s VALUES (1, 0, 10), (2, 0, 30), (3, 0, 20), (4, 1, 5)",
        "SELECT id, RANK() OVER w AS r, SUM(score) OVER w AS s FROM s "
        "WINDOW w AS (PARTITION BY grp ORDER BY score)",
        {(1, 1, 10): 1, (2, 3, 60): 1, (3, 2, 30): 1, (4, 1, 5): 1},
        [("INSERT INTO s VALUES (5, 0, 25)",
          {(1, 1, 10): 1, (2, 4, 85): 1, (3, 2, 30): 1, (4, 1, 5): 1, (5, 3, 55): 1}),
         ("DELETE FROM s WHERE id = 1",
          {(2, 3, 75): 1, (3, 1, 20): 1, (4, 1, 5): 1, (5, 2, 45): 1})]),
    # Exactly one worker owns a global aggregate's row: emptying the input leaves
    # `n = 0` once — not no row, and not one per worker.
    "global_count": (
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        f"INSERT INTO t VALUES {values((k, k) for k in range(24))}",
        "SELECT COUNT(*) AS n FROM t",
        {(24,): 1},
        [("DELETE FROM t WHERE pk >= 0", {(0,): 1})]),
}


@pytest.mark.parametrize("graceful", [True, False], ids=["stop", "kill"])
def test_a_recovered_view_keeps_ticking(own_server, graceful):
    """A recovered view is a starting state, not a final one: each view below
    holds state a later write reads, and must keep ticking from it whether the
    boot resumed it from its checkpoint or rebuilt it from the base."""
    own_server.start(workers=MULTI)
    with gnitz.connect(own_server.target) as conn:
        for name, (tables, rws, body, want, _) in _TICKS.items():
            conn.execute_sql(f'CREATE SCHEMA "{name}"')
            conn.schema = name
            conn.execute_sql(f"{tables}; CREATE VIEW v AS {body}; {rws}")
            assert bag(scanned(conn, "v")) == want, f"{name}: before the restart"

    own_server.restart(graceful=graceful)
    assert own_server.rebuilt_view_count() == (0 if graceful else len(_TICKS))
    with gnitz.connect(own_server.target) as conn:
        for name, (_, _, _, want, writes) in _TICKS.items():
            conn.schema = name
            assert bag(scanned(conn, "v")) == want, f"{name}: after the restart"
            for sql, want in writes:
                conn.execute_sql(sql)
                assert bag(scanned(conn, "v")) == want, f"{name}: after {sql}"
