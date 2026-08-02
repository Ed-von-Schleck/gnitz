"""E2E for keyed reads that route to the one partition their key hashes into.

A read that names its keys (a PK point or prefix-confined range, a `pk IN (…)`
set, an FK dereference, a secondary-index range gather) opens only the partitions
those keys reach instead of merging every local partition. The failure mode is
silent under-reporting — a key routed to no partition, or to the wrong one, comes
back as "no such row" — so every case here asserts the FULL result set over keys
that span every worker.

Run with GNITZ_WORKERS=4 (routing is a no-op at one partition slice):
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_keyed_read_routing.py -v
"""
import random

# Enough distinct keys that, at 256 partitions and any worker count, the set
# spans many partitions on every worker.
NROWS = 400


def _uid():
    return str(random.randint(100000, 999999))


def _rows(client, sn, q):
    res = client.execute_sql(q, schema_name=sn)[0]
    assert res["type"] == "Rows", f"expected Rows, got {res['type']}: {res}"
    return list(res["rows"])


def _cleanup(client, sn, *names):
    for name in names:
        for kind in ("VIEW", "TABLE"):
            try:
                client.execute_sql(f"DROP {kind} {name}", schema_name=sn)
            except Exception:
                pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _insert(client, sn, table, rows, chunk=200):
    for i in range(0, len(rows), chunk):
        vals = ",".join("(" + ",".join(str(v) for v in r) + ")" for r in rows[i:i + chunk])
        client.execute_sql(f"INSERT INTO {table} VALUES {vals}", schema_name=sn)


def _fixture(client, sn, indexed=False):
    """`t (id PK, v)` with `v = id * 10`, `NROWS` rows spanning every partition."""
    client.create_schema(sn)
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=sn)
    _insert(client, sn, "t", [(i, i * 10) for i in range(NROWS)])
    if indexed:
        client.execute_sql("CREATE INDEX ON t (v)", schema_name=sn)


# ---------------------------------------------------------------------------
# PK-keyed reads
# ---------------------------------------------------------------------------


def test_pk_point_select_finds_every_key(client):
    """`WHERE id = k` compiles to a point PK range, which routes to one
    partition. Every key must still come back, from whichever worker owns it."""
    sn = "krr" + _uid()
    try:
        _fixture(client, sn)
        for i in range(NROWS):
            rows = _rows(client, sn, f"SELECT id, v FROM t WHERE id = {i}")
            assert [(r.id, r.v) for r in rows] == [(i, i * 10)], f"id {i}"
        # An absent key is an empty result, not an error.
        assert _rows(client, sn, f"SELECT id FROM t WHERE id = {NROWS + 7}") == []
    finally:
        _cleanup(client, sn, "t")


def test_pk_range_spanning_partitions_loses_no_row(client):
    """A multi-key PK range on a full-PK-hashed table is NOT confinable, so it
    must keep the merged cursor over every partition."""
    sn = "krr" + _uid()
    try:
        _fixture(client, sn)
        rows = _rows(client, sn, f"SELECT id FROM t WHERE id >= 100 AND id < {NROWS}")
        assert sorted(r.id for r in rows) == list(range(100, NROWS))
    finally:
        _cleanup(client, sn, "t")


def test_pk_in_list_gathers_every_named_key(client):
    """`id IN (…)` is the PkSet gather: the request is broadcast, each worker
    answers only the keys it owns, and the union must be the whole list."""
    sn = "krr" + _uid()
    try:
        _fixture(client, sn)
        wanted = list(range(0, NROWS, 3))
        in_list = ",".join(str(i) for i in wanted)
        rows = _rows(client, sn, f"SELECT id, v FROM t WHERE id IN ({in_list})")
        assert sorted((r.id, r.v) for r in rows) == [(i, i * 10) for i in wanted]
    finally:
        _cleanup(client, sn, "t")


def test_update_where_pk_in_list_touches_every_row(client):
    """The DML committed-row fetch reads through the same PkSet gather."""
    sn = "krr" + _uid()
    try:
        _fixture(client, sn)
        wanted = list(range(0, NROWS, 7))
        in_list = ",".join(str(i) for i in wanted)
        client.execute_sql(f"UPDATE t SET v = v + 1 WHERE id IN ({in_list})", schema_name=sn)
        rows = _rows(client, sn, "SELECT id, v FROM t")
        got = dict((r.id, r.v) for r in rows)
        assert len(got) == NROWS
        bumped = set(wanted)
        for i in range(NROWS):
            assert got[i] == i * 10 + (1 if i in bumped else 0), f"id {i}"
    finally:
        _cleanup(client, sn, "t")


# ---------------------------------------------------------------------------
# Secondary-index gathers
# ---------------------------------------------------------------------------


def test_indexed_point_lookup_finds_every_key(client):
    """An index range is broadcast to every worker and each gathers its own
    source rows: the per-PK probe routes, so a key must resolve on exactly the
    worker that owns it."""
    sn = "krr" + _uid()
    try:
        _fixture(client, sn, indexed=True)
        for i in range(0, NROWS, 5):
            rows = _rows(client, sn, f"SELECT id, v FROM t WHERE v = {i * 10}")
            assert [(r.id, r.v) for r in rows] == [(i, i * 10)], f"v {i * 10}"
    finally:
        _cleanup(client, sn, "t")


def test_indexed_between_spans_partitions(client):
    """A BETWEEN over the indexed column selects PKs from many partitions; the
    gather must return every one, at weight 1."""
    sn = "krr" + _uid()
    try:
        _fixture(client, sn, indexed=True)
        for lo, hi in [(0, 90), (1000, 1990), (500, 2500)]:
            rows = _rows(client, sn, f"SELECT id, v FROM t WHERE v BETWEEN {lo} AND {hi}")
            want = [(i, i * 10) for i in range(NROWS) if lo <= i * 10 <= hi]
            assert sorted((r.id, r.v) for r in rows) == want, f"[{lo}, {hi}]"
    finally:
        _cleanup(client, sn, "t")


def test_view_over_indexed_range_backfills_exactly(client):
    """A CREATE VIEW over a populated source runs the same chunked gather the
    ad-hoc read does; its backfill must equal the base's matching contents."""
    sn = "krr" + _uid()
    try:
        _fixture(client, sn, indexed=True)
        client.execute_sql(
            "CREATE VIEW vsel AS SELECT id, v FROM t WHERE v BETWEEN 1000 AND 1990",
            schema_name=sn)
        rows = _rows(client, sn, "SELECT id, v FROM vsel")
        want = [(i, i * 10) for i in range(NROWS) if 1000 <= i * 10 <= 1990]
        assert sorted((r.id, r.v) for r in rows) == want
    finally:
        _cleanup(client, sn, "vsel", "t")


# ---------------------------------------------------------------------------
# FK dereference
# ---------------------------------------------------------------------------


def test_fk_bulk_insert_across_partitions_validates(client):
    """FK validation dereferences each parent key through the routed gather. A
    bulk INSERT whose parents span every partition must be accepted whole."""
    sn = "krr" + _uid()
    try:
        client.create_schema(sn)
        client.execute_sql(
            "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name=sn)
        _insert(client, sn, "parent", [(i, i) for i in range(NROWS)])
        client.execute_sql(
            "CREATE TABLE child ("
            "  id BIGINT NOT NULL PRIMARY KEY,"
            "  pid BIGINT NOT NULL REFERENCES parent(id)"
            ")",
            schema_name=sn)
        _insert(client, sn, "child", [(i, i) for i in range(NROWS)])
        rows = _rows(client, sn, "SELECT id, pid FROM child")
        assert sorted((r.id, r.pid) for r in rows) == [(i, i) for i in range(NROWS)]

        # A parent key no partition holds is still rejected.
        try:
            client.execute_sql(
                f"INSERT INTO child VALUES ({NROWS}, {NROWS + 99})", schema_name=sn)
            raise AssertionError("an absent parent key must be rejected")
        except AssertionError:
            raise
        except Exception:
            pass
    finally:
        _cleanup(client, sn, "child", "parent")
