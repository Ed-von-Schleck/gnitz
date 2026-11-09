"""A view created, a SELECT run, or an index filled over a base table whose rows
sit in shards sees every column it reads.

A backfill drains a base table without the payload columns no operator of the
view reads, so the rows reaching the view's store must still carry every column
the view keeps, whether the body ends in its scan, a filter, a projection or an
aggregate. An ad-hoc read and an index fill drain the same way. The table's
rows reach the disk tier in several batches, rewritten
and deleted across them, so its drain merges shards in which one key's rows
tie, and the strings are long enough to live on the heap.
"""

from collections import Counter

import pytest
from _read import bag, rows, scanned
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
_KEYS = range(1, 300, 7)
# A view bounded by a key list gathers its keys' rows, and one bounded by a
# narrow range of the indexed `a` walks the index; the others drain the table.
_VIEWS = {
    "v_star": ("SELECT * FROM t", Counter(_LIVE)),
    "v_where": ("SELECT * FROM t WHERE a > 400", Counter(r for r in _LIVE if r[1] > 400)),
    "v_text": ("SELECT id, s FROM t WHERE n IS NOT NULL", Counter((r[0], r[2]) for r in _LIVE if r[3] is not None)),
    "v_nulls": ("SELECT id, n FROM t", Counter((r[0], r[3]) for r in _LIVE)),
    "v_sum": ("SELECT g, COUNT(*) AS c, SUM(a) AS sa FROM t GROUP BY g",
              _grouped(_LIVE, len, lambda rs: sum(r[1] for r in rs))),
    "v_extremes": ("SELECT g, MIN(a) AS lo, MAX(a) AS hi FROM t GROUP BY g",
                   _grouped(_LIVE, lambda rs: min(r[1] for r in rs), lambda rs: max(r[1] for r in rs))),
    "v_keys": (f"SELECT id, n FROM t WHERE id IN ({', '.join(map(str, _KEYS))})",
               Counter((r[0], r[3]) for r in _LIVE if r[0] in _KEYS)),
    "v_band": ("SELECT id, s FROM t WHERE a BETWEEN 30 AND 60",
               Counter((r[0], r[2]) for r in _LIVE if 30 <= r[1] <= 60)),
    "v_band_sum": ("SELECT g, SUM(n) AS sn FROM t WHERE a BETWEEN 30 AND 60 GROUP BY g",
                   _grouped([r for r in _LIVE if 30 <= r[1] <= 60],
                            lambda rs: sum(r[3] for r in rs if r[3] is not None)
                            if any(r[3] is not None for r in rs) else None)),
}


@pytest.mark.parametrize("placement", ["", " WITH (replicated = true)"], ids=["partitioned", "replicated"])
def test_a_view_backfilled_from_shards_keeps_every_column_it_projects(swept_client, placement):
    c = swept_client
    c.execute_sql(f"CREATE TABLE t {_COLS}{placement}")
    c.execute_sql("CREATE INDEX ON t(a)")
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


# An ad-hoc SELECT drains a base table without the payload columns neither its
# predicate, its walk nor its sink reads, as a backfill does.
_READS = {
    "SELECT * FROM t": Counter(_LIVE),
    "SELECT id, s FROM t WHERE n IS NULL": Counter((r[0], r[2]) for r in _LIVE if r[3] is None),
    "SELECT id FROM t WHERE n IS NOT NULL AND a > 400":
        Counter((r[0],) for r in _LIVE if r[3] is not None and r[1] > 400),
    "SELECT id, a * 2 AS a2 FROM t WHERE g = 3": Counter((r[0], r[1] * 2) for r in _LIVE if r[4] == 3),
    "SELECT g, COUNT(*) AS c, SUM(a) AS sa FROM t GROUP BY g": _grouped(_LIVE, len, lambda rs: sum(r[1] for r in rs)),
    "SELECT g, COUNT(n) AS c FROM t GROUP BY g": _grouped(_LIVE, lambda rs: sum(r[3] is not None for r in rs)),
    "SELECT COUNT(*) AS c FROM t": Counter([(len(_LIVE),)]),
    "SELECT id, s FROM t ORDER BY a DESC LIMIT 5":
        Counter((r[0], r[2]) for r in sorted(_LIVE, key=lambda r: -r[1])[:5]),
}


@pytest.mark.parametrize("placement", ["", " WITH (replicated = true)"], ids=["partitioned", "replicated"])
def test_a_read_drained_from_shards_sees_every_column_it_reads(swept_client, placement):
    c = swept_client
    c.execute_sql(f"CREATE TABLE t {_COLS}{placement}")
    _fill(c)
    for q, want in _READS.items():
        assert bag(rows(c, q)) == want, q


def test_an_index_filled_from_shards_holds_every_entry(swept_client):
    """An index fill drains its base table with only the PK and the key columns.
    A range over nearly every row walks the whole table and applies the range
    itself, so the walked column must survive that drain too."""
    c = swept_client
    c.execute_sql(f"CREATE TABLE t {_COLS}")
    _fill(c)
    c.execute_sql("CREATE INDEX ON t (a)")
    c.execute_sql("CREATE INDEX ON t (n)")
    for q, want in [
        ("SELECT id, s FROM t WHERE a BETWEEN 400 AND 410", Counter((r[0], r[2]) for r in _LIVE if 400 <= r[1] <= 410)),
        ("SELECT id FROM t WHERE a >= 0", Counter((r[0],) for r in _LIVE)),
        ("SELECT id, g FROM t WHERE n = 17", Counter((r[0], r[4]) for r in _LIVE if r[3] == 17)),
        ("SELECT id FROM t WHERE n >= -1000", Counter((r[0],) for r in _LIVE if r[3] is not None)),
    ]:
        assert bag(rows(c, q)) == want, q
