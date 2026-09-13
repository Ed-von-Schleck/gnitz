"""The base tables, view bodies and churn the storage-policy suites share, and
the weight-exact comparison over the fed ones.

`storage_policy/test_delta_feed.py` drives the feed by hand — bootstrap, poll,
apply — and `client_surface/test_mirror.py` drives the same views through a
mirroring client, which does that for it. Both need the same base tables, the
same churn (inserts, an UPDATE and a DELETE, so a round carries retractions and
not just insertions), the same view bodies, and the same `{row → net weight}`
comparison. Keeping one copy here is what makes "the mirror agrees with the
feed" a comparison of two mechanisms rather than of two setups.
`storage_policy/test_capacity_bounded_views.py` runs the same bodies bounded, so
"capacity is invisible" is a claim about the shapes the other two already cover.
"""

# Big enough that nothing built on these helpers falls off the window; a test
# about retention sets its own.
FEED = "32 MB"

LINEAR = "SELECT id, v, body FROM t WHERE v > 10"
JOIN = "SELECT t.id, t.body, u.w FROM t JOIN u ON t.id = u.tid"
GROUPBY = "SELECT tid, COUNT(*) AS n, SUM(w) AS total FROM u GROUP BY tid"
SETOP = "SELECT id FROM t EXCEPT SELECT tid FROM u"


def _key(row):
    """A row as a name-keyed tuple, so a view row and a delta row compare
    directly: the delta schema is a *reordering* of the view's columns (`_tick`,
    then the PK, then the payload), not the view's own order.

    The stamp column drops out — it is which round carried the row, not part of
    it. No view column can collide with the name: a user identifier may not start
    with `_`.
    """
    return tuple(sorted((f, v) for f, v in row._asdict().items() if f != "_tick"))


def _zset(rows):
    """`{row → net weight}` over a result, dropping anything that cancels."""
    out = {}
    for r in rows:
        k = _key(r)
        out[k] = out.get(k, 0) + r.weight
    return {k: w for k, w in out.items() if w != 0}


def _mk_feed(client, sn, name, body, feed=FEED):
    client.execute_sql(f"CREATE VIEW {name} WITH (delta = '{feed}') AS {body}", schema_name=sn)


def _base_tables(client, sn):
    client.create_schema(sn)
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL, body TEXT NOT NULL)",
        schema_name=sn,
    )
    client.execute_sql(
        "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, tid BIGINT NOT NULL, w BIGINT NOT NULL)",
        schema_name=sn,
    )


def _churn(client, sn, lo, hi, chunk=None):
    """Inserts, then an UPDATE and a DELETE over part of the range — so the feed
    sees fresh keys, `enforce_unique_pk`'s retract/insert pair, and pure
    retractions. Without the last two the retraction half of the design goes
    untested.

    `chunk` splits the inserts into that many rows per statement. Each statement
    is its own settle, so a caller that needs many spills — the capacity sweep
    budgets itself to one push-down per spill — asks for small ones.
    """
    span = hi - lo + 1
    step = chunk or span
    for a in range(lo, hi + 1, step):
        b = min(a + step - 1, hi)
        client.execute_sql(
            "INSERT INTO t VALUES " + ",".join(f"({i}, {i * 3}, 'body-{i:0>20}')" for i in range(a, b + 1)),
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO u VALUES " + ",".join(f"({i}, {i}, {i * 7})" for i in range(a, b + 1)),
            schema_name=sn,
        )
    client.execute_sql(f"UPDATE t SET v = v + 1 WHERE id >= {lo} AND id < {lo + span // 3}", schema_name=sn)
    # `u` loses the longer tail, so `t.id` is never a subset of `u.tid` and the
    # SETOP body (`t EXCEPT u`) holds rows. The other way round it is empty, and
    # a copy-versus-view comparison over it then agrees about nothing.
    client.execute_sql(f"DELETE FROM t WHERE id > {hi - span // 5}", schema_name=sn)
    client.execute_sql(f"DELETE FROM u WHERE id > {hi - span // 4}", schema_name=sn)


# The delta store is in neither checkpoint round and nothing flushes it, so it
# reaches disk only through its own memtable fold (~192 KiB) and then the RAM
# tier above. Reaching the sweep therefore takes real volume, not just a small
# ceiling — this is the byte width of one delta row, wide enough that a few
# thousand rows cross the fold several times.
_WIDE = "x" * 200


def _flood(client, sn, lo_key, rows, chunk=500):
    """Push `rows` wide rows into `t` starting at `lo_key`, in `chunk`-sized
    statements. Each statement is its own settle, so each is its own tick round —
    which is what gives the delta store a stream of distinct `_tick` values to
    stratify by."""
    for lo in range(lo_key, lo_key + rows, chunk):
        hi = min(lo + chunk - 1, lo_key + rows - 1)
        client.execute_sql(
            "INSERT INTO t VALUES " + ",".join(f"({i}, {i * 3}, '{_WIDE}-{i}')" for i in range(lo, hi + 1)),
            schema_name=sn,
        )
