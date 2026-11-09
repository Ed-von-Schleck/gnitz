"""A view created over a base table whose rows sit in shards holds every column
it projects.

A backfill drains a base table without the payload columns no operator of the
view reads, so the rows reaching the view's store must still carry every column
the view keeps, whether the body ends in its scan, a filter, a projection or an
aggregate. The table's rows reach the disk tier in several batches, rewritten
and deleted across them, so its drain merges shards in which one key's rows
tie, and the strings are long enough to live on the heap.
"""

from collections import Counter

import pytest
from _read import bag, scanned
from _sql import insert, lit

_COLS = "(id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, s TEXT NOT NULL, n BIGINT, g BIGINT NOT NULL)"


def _row(i, gen):
    return (i, i * 3 + gen, f"a string long enough for the heap #{i:05d}/{gen}", None if i % 4 == 0 else i - gen, i % 7)


def _live():
    """The table's rows after the batches below: rows 0..300 inserted, every
    third rewritten, every tenth deleted."""
    rows = {i: _row(i, 0) for i in range(300)}
    rows.update({i: _row(i, 1) for i in range(0, 300, 3)})
    for i in range(0, 300, 10):
        del rows[i]
    return list(rows.values())


def _fill(c):
    for lo in range(0, 300, 60):
        insert(c, "t", [_row(i, 0) for i in range(lo, lo + 60)])
    for i in range(0, 300, 3):
        _, a, s, n, _ = _row(i, 1)
        c.execute_sql(f"UPDATE t SET a = {a}, s = {lit(s)}, n = {lit(n)} WHERE id = {i}")
    c.execute_sql("DELETE FROM t WHERE id % 10 = 0")


def _grouped(rows, *aggs):
    out = {}
    for r in rows:
        out.setdefault(r[4], []).append(r)
    return Counter((g, *(f(rs) for f in aggs)) for g, rs in out.items())


_LIVE = _live()
_VIEWS = {
    "v_star": ("SELECT * FROM t", Counter(_LIVE)),
    "v_where": ("SELECT * FROM t WHERE a > 400", Counter(r for r in _LIVE if r[1] > 400)),
    "v_text": ("SELECT id, s FROM t WHERE n IS NOT NULL", Counter((r[0], r[2]) for r in _LIVE if r[3] is not None)),
    "v_nulls": ("SELECT id, n FROM t", Counter((r[0], r[3]) for r in _LIVE)),
    "v_sum": ("SELECT g, COUNT(*) AS c, SUM(a) AS sa FROM t GROUP BY g",
              _grouped(_LIVE, len, lambda rs: sum(r[1] for r in rs))),
    "v_extremes": ("SELECT g, MIN(a) AS lo, MAX(a) AS hi FROM t GROUP BY g",
                   _grouped(_LIVE, lambda rs: min(r[1] for r in rs), lambda rs: max(r[1] for r in rs))),
}


@pytest.mark.parametrize("placement", ["", " WITH (replicated = true)"], ids=["partitioned", "replicated"])
def test_a_view_backfilled_from_shards_keeps_every_column_it_projects(swept_client, placement):
    c = swept_client
    c.execute_sql(f"CREATE TABLE t {_COLS}{placement}")
    _fill(c)
    for name, (body, _) in _VIEWS.items():
        c.execute_sql(f"CREATE VIEW {name} AS {body}")
    for name, (_, want) in _VIEWS.items():
        assert bag(scanned(c, name)) == want, name


def test_a_view_backfilled_from_shards_older_than_its_columns_reads_them_as_null(swept_client):
    """Rows written before an ADD COLUMN sit in shards of the narrower schema,
    and a drain widens them: a column the view reads comes back NULL on them,
    whatever the drain leaves out beside it."""
    c = swept_client
    c.execute_sql("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, p TEXT NOT NULL, q BIGINT NOT NULL)")
    old = [(i, f"a string long enough for the heap #{i:05d}", i % 7) for i in range(300)]
    for lo in range(0, 300, 100):
        insert(c, "a", old[lo:lo + 100])
    c.execute_sql("ALTER TABLE a ADD COLUMN w BIGINT")
    c.execute_sql("ALTER TABLE a ADD COLUMN v TEXT")
    new = [(i, f"a string long enough for the heap #{i:05d}", i % 7, i, f"another heap string #{i}")
           for i in range(300, 400)]
    insert(c, "a", new)
    c.execute_sql("UPDATE a SET w = id * 2 WHERE id % 5 = 0")
    live = [(i, p, q, i * 2 if i % 5 == 0 else w, v) for i, p, q, w, v in [(*r, None, None) for r in old] + new]

    c.execute_sql("CREATE VIEW v_counted AS SELECT q, COUNT(*) AS n, COUNT(w) AS nw, SUM(w) AS sw FROM a GROUP BY q")
    c.execute_sql("CREATE VIEW v_unset AS SELECT id, v FROM a WHERE w IS NULL")
    want = {}
    for _, _, q, w, _ in live:
        n, nw, sw = want.get(q, (0, 0, None))
        want[q] = (n + 1, nw + (w is not None), sw if w is None else (sw or 0) + w)
    assert bag(scanned(c, "v_counted"), "q", "n", "nw", "sw") == Counter((q, *t) for q, t in want.items())
    assert bag(scanned(c, "v_unset"), "id", "v") == Counter((r[0], r[4]) for r in live if r[3] is None)
