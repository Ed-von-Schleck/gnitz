"""E2E tests for REPLICATED tables.

A replicated table keeps a full copy on every worker: writes broadcast to every
worker's ingest + SAL, reads single-source one copy, and a join against a
partitioned fact runs locally on every worker with no exchange on either side.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_replicated.py -v --tb=short
"""
import os
import random
import shutil
import signal
import subprocess
import tempfile
import time

import pytest
import gnitz
from _serverproc import server_preexec

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))
_NEEDS_MULTI = pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="replication only matters with GNITZ_WORKERS >= 2"
)


def _uid():
    return str(random.randint(100000, 999999))


def _positive(rows):
    return [r for r in rows if r.weight > 0]


def _select_pks(client, sql, sn):
    """Sorted list of positive-weight PKs from a SQL SELECT — duplicates kept, so
    a ×W read inflation (the same row returned once per worker) shows up as
    repeated PKs, not a deduped set."""
    res = client.execute_sql(sql, schema_name=sn)
    assert res[0]["type"] == "Rows"
    b = res[0]["rows"].batch
    if b is None:
        return []
    return sorted(b.pks[i] for i in range(len(b.pks)) if b.weights[i] > 0)


# ---------------------------------------------------------------------------
# Writes broadcast + reads single-source
# ---------------------------------------------------------------------------

def test_replicated_scan_returns_one_copy(client):
    """SELECT * over a replicated table returns exactly the inserted rows — one
    copy, not one per worker. Without single-source reads it would return N×."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, name BIGINT NOT NULL) "
            "WITH (replicated = true)",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO dim VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)",
            schema_name=sn,
        )
        tid = client.resolve_table(sn, "dim")[0]
        rows = _positive(client.scan(tid))
        assert len(rows) == 5, f"expected one copy (5 rows), got {len(rows)}"
        assert sorted(r["id"] for r in rows) == [1, 2, 3, 4, 5]
        assert {r["id"]: r["name"] for r in rows} == {1: 100, 2: 200, 3: 300, 4: 400, 5: 500}
    finally:
        client.drop_schema(sn)


def test_replicated_seek_point_lookup(client):
    """SEEK already unicasts to one worker, which holds the full copy: one row."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, name BIGINT NOT NULL) "
            "WITH (replicated = true)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO dim VALUES (1, 100), (2, 200), (3, 300)", schema_name=sn)
        tid = client.resolve_table(sn, "dim")[0]
        rows = _positive(client.seek(tid, pk=2))
        assert len(rows) == 1, f"point lookup must return one row, got {len(rows)}"
        assert rows[0]["id"] == 2 and rows[0]["name"] == 200
    finally:
        client.drop_schema(sn)


def test_replicated_count_returns_single_value(client):
    """COUNT(*) GROUP BY over a replicated table returns the single-copy count.
    A sharded reduce over a replicated input would N-fold-multiply it; the
    planner builds the shard-free `reduce_multi_local`, and the replicated view
    output is single-sourced on read (so neither the value nor the row set is
    multiplied)."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, "
            "amount BIGINT NOT NULL) WITH (replicated = true)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT grp, COUNT(*) AS cnt, SUM(amount) AS total "
            "FROM dim GROUP BY grp",
            schema_name=sn,
        )
        vid = client.resolve_table(sn, "v")[0]
        client.execute_sql(
            "INSERT INTO dim VALUES (1, 10, 100), (2, 10, 200), (3, 20, 300)",
            schema_name=sn,
        )
        rows = _positive(client.scan(vid))
        assert len(rows) == 2, f"expected one copy of 2 groups, got {len(rows)}"
        by_grp = {r["grp"]: r for r in rows}
        assert by_grp[10]["cnt"] == 2, f"COUNT must be single-copy (2), got {by_grp[10]['cnt']}"
        assert by_grp[10]["total"] == 300  # 100 + 200, not N×
        assert by_grp[20]["cnt"] == 1
        assert by_grp[20]["total"] == 300
    finally:
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Joins: replicated dim is local on every worker
# ---------------------------------------------------------------------------

@_NEEDS_MULTI
def test_replicated_dim_join_fact_not_distributed_by_key(client):
    """A partitioned fact joined to a replicated dim is correct under W=4 even
    when the fact is NOT distributed by the join key (its PK is `fact_id`, the
    join key is the non-PK `dim_ref`) — the case hash co-partitioning cannot
    serve. A wrongly elided or wrongly fired exchange would drop join rows and
    fail the multiset assertion loudly."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE dim (dim_id BIGINT NOT NULL PRIMARY KEY, name BIGINT NOT NULL) "
            "WITH (replicated = true)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE fact (fact_id BIGINT NOT NULL PRIMARY KEY, dim_ref BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW j AS SELECT fact.fact_id AS fid, dim.name AS nm "
            "FROM fact JOIN dim ON fact.dim_ref = dim.dim_id",
            schema_name=sn,
        )
        jid = client.resolve_table(sn, "j")[0]

        client.execute_sql(
            "INSERT INTO dim VALUES (1, 100), (2, 200), (3, 300), (4, 400)",
            schema_name=sn,
        )
        # 40 facts spread across workers by fact_id; each references a dim row.
        vals = ", ".join(f"({i}, {(i % 4) + 1})" for i in range(1, 41))
        client.execute_sql(f"INSERT INTO fact VALUES {vals}", schema_name=sn)

        rows = _positive(client.scan(jid))
        assert len(rows) == 40, f"every fact must join its dim once; got {len(rows)}"
        got = {r["fid"]: r["nm"] for r in rows}
        assert set(got) == set(range(1, 41))
        for i in range(1, 41):
            assert got[i] == ((i % 4) + 1) * 100, f"fact {i} joined the wrong dim"
    finally:
        client.drop_schema(sn)


@_NEEDS_MULTI
def test_replicated_star_join_two_dims(client):
    """One partitioned fact joined to two replicated dims (as a nested join —
    gnitz builds one JOIN per view) stays local on every worker. The inner view
    `fact ⋈ d1` is locally partitioned (fact's distribution); joining it to the
    replicated `d2` is again local, and the result is correct under W=4."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE d1 (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL) "
            "WITH (replicated = true)", schema_name=sn)
        client.execute_sql(
            "CREATE TABLE d2 (id BIGINT NOT NULL PRIMARY KEY, b BIGINT NOT NULL) "
            "WITH (replicated = true)", schema_name=sn)
        client.execute_sql(
            "CREATE TABLE fact (fid BIGINT NOT NULL PRIMARY KEY, "
            "r1 BIGINT NOT NULL, r2 BIGINT NOT NULL)", schema_name=sn)
        # Inner: fact ⋈ d1, carrying r2 through for the outer join key.
        client.execute_sql(
            "CREATE VIEW j1 AS SELECT fact.fid AS fid, fact.r2 AS r2, d1.a AS a "
            "FROM fact JOIN d1 ON fact.r1 = d1.id", schema_name=sn)
        # Outer: (fact ⋈ d1) ⋈ d2.
        client.execute_sql(
            "CREATE VIEW j AS SELECT j1.fid AS fid, j1.a AS a, d2.b AS b "
            "FROM j1 JOIN d2 ON j1.r2 = d2.id", schema_name=sn)
        jid = client.resolve_table(sn, "j")[0]

        client.execute_sql("INSERT INTO d1 VALUES (1, 11), (2, 22)", schema_name=sn)
        client.execute_sql("INSERT INTO d2 VALUES (1, 1000), (2, 2000)", schema_name=sn)
        vals = ", ".join(f"({i}, {(i % 2) + 1}, {((i + 1) % 2) + 1})" for i in range(1, 33))
        client.execute_sql(f"INSERT INTO fact VALUES {vals}", schema_name=sn)

        rows = _positive(client.scan(jid))
        assert len(rows) == 32, f"every fact must join both dims; got {len(rows)}"
        for r in rows:
            i = r["fid"]
            assert r["a"] == ((i % 2) + 1) * 11
            assert r["b"] == (((i + 1) % 2) + 1) * 1000
    finally:
        client.drop_schema(sn)


def test_replicated_join_replicated_single_source(client):
    """A view joining two replicated tables is itself replicated — every worker
    computes the full join — so its read must single-source (no N× on read)."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL) "
            "WITH (replicated = true)", schema_name=sn)
        client.execute_sql(
            "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL) "
            "WITH (replicated = true)", schema_name=sn)
        client.execute_sql(
            "CREATE VIEW j AS SELECT a.id AS id, a.x AS x, b.y AS y "
            "FROM a JOIN b ON a.id = b.id", schema_name=sn)
        jid = client.resolve_table(sn, "j")[0]
        client.execute_sql("INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)", schema_name=sn)
        client.execute_sql("INSERT INTO b VALUES (1, 11), (2, 22), (3, 33)", schema_name=sn)

        rows = _positive(client.scan(jid))
        assert len(rows) == 3, f"replicated⋈replicated must read one copy (3 rows), got {len(rows)}"
        got = {r["id"]: (r["x"], r["y"]) for r in rows}
        assert got == {1: (10, 11), 2: (20, 22), 3: (30, 33)}
    finally:
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Mutations broadcast to every copy: delete, update, late incremental delta
# ---------------------------------------------------------------------------

def test_replicated_delete_broadcasts(client):
    """A DELETE on a replicated table broadcasts the retraction to every worker's
    copy. The row is gone on a single-source read, and a join against the dim
    drops exactly its facts on every worker — a copy that missed the retraction
    would keep joining the deleted dim."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE dim (dim_id BIGINT NOT NULL PRIMARY KEY, name BIGINT NOT NULL) "
            "WITH (replicated = true)", schema_name=sn)
        client.execute_sql(
            "CREATE TABLE fact (fact_id BIGINT NOT NULL PRIMARY KEY, dim_ref BIGINT NOT NULL)",
            schema_name=sn)
        client.execute_sql(
            "CREATE VIEW j AS SELECT fact.fact_id AS fid, dim.name AS nm "
            "FROM fact JOIN dim ON fact.dim_ref = dim.dim_id", schema_name=sn)
        tid = client.resolve_table(sn, "dim")[0]
        jid = client.resolve_table(sn, "j")[0]

        client.execute_sql(
            "INSERT INTO dim VALUES (1, 100), (2, 200), (3, 300), (4, 400)", schema_name=sn)
        vals = ", ".join(f"({i}, {(i % 4) + 1})" for i in range(1, 41))
        client.execute_sql(f"INSERT INTO fact VALUES {vals}", schema_name=sn)

        client.execute_sql("DELETE FROM dim WHERE dim_id = 3", schema_name=sn)

        # Single-source read: one copy, the deleted row gone.
        drows = _positive(client.scan(tid))
        assert sorted(r["dim_id"] for r in drows) == [1, 2, 4], \
            f"deleted row must be gone, one copy; got {sorted(r['dim_id'] for r in drows)}"

        # Join drops exactly the facts referencing the deleted dim, on every worker.
        survivors = sorted(i for i in range(1, 41) if (i % 4) + 1 != 3)
        jrows = _positive(client.scan(jid))
        assert sorted(r["fid"] for r in jrows) == survivors, \
            "join must drop the deleted dim's facts on every worker"
    finally:
        client.drop_schema(sn)


def test_replicated_update_broadcasts(client):
    """A SQL UPDATE on a replicated table (retract old + insert new) broadcasts
    both halves to every worker. The new payload shows on a single-source read,
    and a join reflects it on every worker — a copy that missed either half would
    still join the stale value."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE dim (dim_id BIGINT NOT NULL PRIMARY KEY, name BIGINT NOT NULL) "
            "WITH (replicated = true)", schema_name=sn)
        client.execute_sql(
            "CREATE TABLE fact (fact_id BIGINT NOT NULL PRIMARY KEY, dim_ref BIGINT NOT NULL)",
            schema_name=sn)
        client.execute_sql(
            "CREATE VIEW j AS SELECT fact.fact_id AS fid, dim.name AS nm "
            "FROM fact JOIN dim ON fact.dim_ref = dim.dim_id", schema_name=sn)
        tid = client.resolve_table(sn, "dim")[0]
        jid = client.resolve_table(sn, "j")[0]

        client.execute_sql(
            "INSERT INTO dim VALUES (1, 100), (2, 200), (3, 300), (4, 400)", schema_name=sn)
        vals = ", ".join(f"({i}, {(i % 4) + 1})" for i in range(1, 41))
        client.execute_sql(f"INSERT INTO fact VALUES {vals}", schema_name=sn)

        client.execute_sql("UPDATE dim SET name = 999 WHERE dim_id = 2", schema_name=sn)

        # Single-source read: one copy, the updated row carries the new payload.
        drows = {r["dim_id"]: r["name"] for r in _positive(client.scan(tid))}
        assert drows == {1: 100, 2: 999, 3: 300, 4: 400}, f"updated copy diverged: {drows}"

        # Join reflects the new value for every fact referencing dim 2, on every worker.
        jrows = _positive(client.scan(jid))
        assert len(jrows) == 40, f"every fact still joins; got {len(jrows)}"
        for r in jrows:
            i = r["fid"]
            expected = 999 if (i % 4) + 1 == 2 else ((i % 4) + 1) * 100
            assert r["nm"] == expected, f"fact {i} join did not reflect the update"
    finally:
        client.drop_schema(sn)


@_NEEDS_MULTI
def test_replicated_dim_delta_rejoins_existing_facts(client):
    """A dim row inserted AFTER the join view and facts already exist must
    broadcast and re-join the existing facts on EVERY worker — the symmetric DBSP
    join term (dim delta ⋈ fact trace) realized on each worker's local copy. Facts
    referencing a not-yet-present dim produce no row until the dim arrives; once it
    does, every such fact (wherever it landed) gains its row."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE dim (dim_id BIGINT NOT NULL PRIMARY KEY, name BIGINT NOT NULL) "
            "WITH (replicated = true)", schema_name=sn)
        client.execute_sql(
            "CREATE TABLE fact (fact_id BIGINT NOT NULL PRIMARY KEY, dim_ref BIGINT NOT NULL)",
            schema_name=sn)
        client.execute_sql(
            "CREATE VIEW j AS SELECT fact.fact_id AS fid, dim.name AS nm "
            "FROM fact JOIN dim ON fact.dim_ref = dim.dim_id", schema_name=sn)
        jid = client.resolve_table(sn, "j")[0]

        # Only dims 1,2 present; facts reference 1..4 across all workers.
        client.execute_sql("INSERT INTO dim VALUES (1, 100), (2, 200)", schema_name=sn)
        vals = ", ".join(f"({i}, {(i % 4) + 1})" for i in range(1, 41))
        client.execute_sql(f"INSERT INTO fact VALUES {vals}", schema_name=sn)

        early = {i for i in range(1, 41) if (i % 4) + 1 in (1, 2)}
        rows = _positive(client.scan(jid))
        assert {r["fid"] for r in rows} == early, "only facts whose dim already exists join"

        # Dims 3,4 arrive late → broadcast delta re-joins the waiting facts on
        # every worker.
        client.execute_sql("INSERT INTO dim VALUES (3, 300), (4, 400)", schema_name=sn)
        rows = _positive(client.scan(jid))
        assert len(rows) == 40, f"late dim delta must re-join all facts; got {len(rows)}"
        got = {r["fid"]: r["nm"] for r in rows}
        for i in range(1, 41):
            assert got[i] == ((i % 4) + 1) * 100
    finally:
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Uniqueness, conflict, FK, validation
# ---------------------------------------------------------------------------

def test_replicated_duplicate_pk_rejected(client):
    """A duplicate PK insert into a replicated table is rejected before the ACK
    (the single-worker preflight answers from the worker's full copy)."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, name BIGINT NOT NULL) "
            "WITH (replicated = true)", schema_name=sn)
        client.execute_sql("INSERT INTO dim VALUES (1, 100)", schema_name=sn)
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql("INSERT INTO dim VALUES (1, 999)", schema_name=sn)
        assert "duplicate key" in str(exc.value).lower()
        # The original row is intact and single-copy.
        tid = client.resolve_table(sn, "dim")[0]
        rows = _positive(client.scan(tid))
        assert len(rows) == 1 and rows[0]["name"] == 100
    finally:
        client.drop_schema(sn)


def test_replicated_upsert_one_row(client):
    """An UPSERT of an existing PK replaces the payload exactly once on every
    worker; the copies stay identical, so the single-source read sees one row."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, name BIGINT NOT NULL) "
            "WITH (replicated = true)", schema_name=sn)
        tid = client.resolve_table(sn, "dim")[0]
        schema = gnitz.Schema([
            gnitz.ColumnDef("id", gnitz.TypeCode.I64, primary_key=True),
            gnitz.ColumnDef("name", gnitz.TypeCode.I64),
        ])
        # Raw push uses Update (upsert) mode; second push of pk=1 replaces.
        b1 = gnitz.ZSetBatch(schema)
        b1.append(id=1, name=100)
        client.push(tid, b1)
        b2 = gnitz.ZSetBatch(schema)
        b2.append(id=1, name=200)
        client.push(tid, b2)

        rows = _positive(client.scan(tid))
        assert len(rows) == 1, f"upsert must leave one row, got {len(rows)}"
        assert rows[0]["name"] == 200
    finally:
        client.drop_schema(sn)


def test_fk_against_replicated_parent(client):
    """A FK referencing a replicated parent: valid child rows insert, an orphan
    is rejected. The broadcast existence check is correct against a replicated
    parent (every worker holds the full copy)."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY, label BIGINT NOT NULL) "
            "WITH (replicated = true)", schema_name=sn)
        client.execute_sql(
            "CREATE TABLE child (cid BIGINT NOT NULL PRIMARY KEY, "
            "pid BIGINT NOT NULL REFERENCES parent(id))", schema_name=sn)
        client.execute_sql("INSERT INTO parent VALUES (1, 10), (2, 20)", schema_name=sn)
        # Valid children referencing existing parents.
        client.execute_sql(
            "INSERT INTO child VALUES (100, 1), (101, 2), (102, 1)", schema_name=sn)
        cid = client.resolve_table(sn, "child")[0]
        assert len(_positive(client.scan(cid))) == 3
        # Orphan child: parent 99 does not exist → rejected.
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql("INSERT INTO child VALUES (103, 99)", schema_name=sn)
    finally:
        client.drop_schema(sn)


def test_replicated_plus_cluster_by_rejected(client):
    """REPLICATED and CLUSTER BY are mutually exclusive and rejected at DDL."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        # sqlparser parses WITH before CLUSTER BY, and CLUSTER BY takes bare
        # columns (no parens). This is the parseable combination that reaches the
        # planner's mutual-exclusion check.
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql(
                "CREATE TABLE bad (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL) "
                "WITH (replicated = true) CLUSTER BY id",
                schema_name=sn,
            )
        msg = str(exc.value).lower()
        assert "replicated" in msg and "cluster by" in msg
    finally:
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Secondary indexes on a replicated owner: reads single-source, CREATE UNIQUE
# INDEX pre-flight single-sources
#
# Every worker holds an identical full copy of a replicated table and its
# secondary indexes. Any read that broadcasts-and-merges (indexed SELECT, range
# scan) would return each row once per worker (×W), and the CREATE UNIQUE INDEX
# pre-flight would see every value W times and reject a genuinely unique table
# as duplicate. These paths must single-source worker 0. Masked at W=1 (single
# copy); the suite runs W=4.
# ---------------------------------------------------------------------------


@_NEEDS_MULTI
def test_replicated_indexed_select_returns_one_copy(client):
    """SELECT on a NON-unique secondary-indexed column of a replicated table must
    return each matching row once, not once per worker. The seek broadcasts and
    merges every worker's matches; on a replicated owner all W copies match, so
    without single-sourcing the client sees ×W duplicates."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, cust BIGINT NOT NULL) "
            "WITH (replicated = true)", schema_name=sn)
        # cust=42 held by two distinct rows; cust=99 by one.
        client.execute_sql(
            "INSERT INTO dim VALUES (1, 42), (2, 42), (3, 99)", schema_name=sn)
        client.execute_sql("CREATE INDEX ON dim(cust)", schema_name=sn)

        assert _select_pks(client, "SELECT * FROM dim WHERE cust = 42", sn) == [1, 2]
        assert _select_pks(client, "SELECT * FROM dim WHERE cust = 99", sn) == [3]
        assert _select_pks(client, "SELECT * FROM dim WHERE cust = 7", sn) == []
    finally:
        client.drop_schema(sn)


@_NEEDS_MULTI
def test_replicated_indexed_range_returns_one_copy(client):
    """Ordered range scan over a secondary index of a replicated table must return
    each row once, not ×W. Same broadcast-and-merge path as the point seek."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL) "
            "WITH (replicated = true)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO dim VALUES (1, 0), (2, 10), (3, 20), (4, 30)", schema_name=sn)
        client.execute_sql("CREATE INDEX ON dim(x)", schema_name=sn)

        assert _select_pks(client, "SELECT * FROM dim WHERE x BETWEEN 10 AND 20", sn) == [2, 3]
        assert _select_pks(client, "SELECT * FROM dim WHERE x > 10", sn) == [3, 4]
        assert _select_pks(client, "SELECT * FROM dim WHERE x < 20", sn) == [1, 2]
    finally:
        client.drop_schema(sn)


@_NEEDS_MULTI
def test_replicated_unique_indexed_point_select(client):
    """SELECT on a single-column UNIQUE-indexed column of a replicated table
    returns the one holder (the unicast-to-one-worker fast path stays correct
    when routed straight to worker 0's full copy)."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL) "
            "WITH (replicated = true)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO dim VALUES (1, 100), (2, 200), (3, 300)", schema_name=sn)
        client.execute_sql("CREATE UNIQUE INDEX ON dim(val)", schema_name=sn)

        assert _select_pks(client, "SELECT * FROM dim WHERE val = 200", sn) == [2]
        assert _select_pks(client, "SELECT * FROM dim WHERE val = 999", sn) == []
    finally:
        client.drop_schema(sn)


@_NEEDS_MULTI
def test_create_unique_index_on_populated_replicated_table(client):
    """CREATE UNIQUE INDEX on a populated replicated table with all-distinct
    values must SUCCEED. The pre-flight fans to every worker; on a replicated
    owner each worker streams the same full copy, so without single-sourcing the
    merge sees every value W times and rejects the unique table as duplicate.
    After creation the constraint enforces on later inserts."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL) "
            "WITH (replicated = true)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO dim VALUES (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)",
            schema_name=sn)

        # The bug: this raises a false duplicate error under W >= 2.
        client.execute_sql("CREATE UNIQUE INDEX ON dim(val)", schema_name=sn)

        # Enforcement is live: a colliding value is rejected, a fresh one inserts.
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql("INSERT INTO dim VALUES (6, 100)", schema_name=sn)
        client.execute_sql("INSERT INTO dim VALUES (6, 600)", schema_name=sn)

        # Single-source read: one copy of every row.
        tid = client.resolve_table(sn, "dim")[0]
        rows = _positive(client.scan(tid))
        assert sorted(r["val"] for r in rows) == [100, 200, 300, 400, 500, 600]
    finally:
        client.drop_schema(sn)


def test_create_unique_index_on_replicated_rejects_real_duplicate(client):
    """A GENUINE duplicate on a replicated table must still fail CREATE UNIQUE
    INDEX. Two rows share the indexed value before the CREATE; single-sourcing
    the pre-flight to one worker's full copy still catches it (adjacent equal
    spans within the one stream), so the index is not created and no phantom
    constraint is left behind."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL) "
            "WITH (replicated = true)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO dim VALUES (1, 42), (2, 42), (3, 99)", schema_name=sn)

        with pytest.raises(gnitz.GnitzError):
            client.execute_sql("CREATE UNIQUE INDEX ON dim(val)", schema_name=sn)

        # No phantom constraint: another duplicate value is still accepted.
        client.execute_sql("INSERT INTO dim VALUES (4, 42)", schema_name=sn)
        tid = client.resolve_table(sn, "dim")[0]
        assert len(_positive(client.scan(tid))) == 4
    finally:
        client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Recovery: the full copy survives a reboot on EVERY worker
# ---------------------------------------------------------------------------

def _server_binary():
    return os.environ.get(
        "GNITZ_SERVER_BIN",
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../gnitz-server")),
    )


def _start(data_dir, sock_path, workers):
    binary = _server_binary()
    if not os.path.isfile(binary):
        pytest.skip(f"Server binary not found: {binary}")
    proc = subprocess.Popen(
        [binary, data_dir, sock_path, f"--workers={workers}"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        start_new_session=True, preexec_fn=server_preexec,
    )
    deadline = time.time() + 10.0
    while time.time() < deadline:
        if os.path.exists(sock_path):
            return proc
        time.sleep(0.05)
    proc.kill(); proc.communicate()
    raise RuntimeError("server did not start")


def _stop(proc):
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except ProcessLookupError:
        pass
    proc.wait()


@_NEEDS_MULTI
def test_replicated_full_copy_survives_reboot_on_every_worker(client):
    """After a reboot under W=4 the replicated copy must live on EVERY worker,
    not just worker 0 (the `trim_worker_partitions` regression would drop it on
    every worker whose range excludes partition 0). The probe is a join created
    AFTER the reboot against a partitioned fact spread across all workers: a
    fact whose worker lost the dim copy would produce no join row, so a missing
    copy on any worker shows up as missing join rows."""
    workers = max(2, _NUM_WORKERS)
    tmpdir = tempfile.mkdtemp(dir=os.path.expanduser("~/git/gnitz/tmp"), prefix="gnitz_repl_")
    data_dir = os.path.join(tmpdir, "data")
    sock = os.path.join(tmpdir, "gnitz.sock")
    proc = None
    try:
        # Phase 1: create + populate the replicated dim, then crash.
        proc = _start(data_dir, sock, workers)
        c = gnitz.connect(sock)
        c.create_schema("repl")
        c.execute_sql(
            "CREATE TABLE dim (dim_id BIGINT NOT NULL PRIMARY KEY, name BIGINT NOT NULL) "
            "WITH (replicated = true)", schema_name="repl")
        c.execute_sql(
            "INSERT INTO dim VALUES (1, 100), (2, 200), (3, 300), (4, 400)", schema_name="repl")
        c.close()

        # Phase 2: reboot (SIGKILL + restart on the same data dir).
        _stop(proc)
        if os.path.exists(sock):
            os.unlink(sock)
        proc = _start(data_dir, sock, workers)
        c = gnitz.connect(sock)

        # The dim is replayed; build a fresh fact + join AFTER the reboot so the
        # join probes the dim copy on whichever worker each fact lands on.
        c.execute_sql(
            "CREATE TABLE fact (fact_id BIGINT NOT NULL PRIMARY KEY, dim_ref BIGINT NOT NULL)",
            schema_name="repl")
        c.execute_sql(
            "CREATE VIEW j AS SELECT fact.fact_id AS fid, dim.name AS nm "
            "FROM fact JOIN dim ON fact.dim_ref = dim.dim_id", schema_name="repl")
        jid = c.resolve_table("repl", "j")[0]
        vals = ", ".join(f"({i}, {(i % 4) + 1})" for i in range(1, 41))
        c.execute_sql(f"INSERT INTO fact VALUES {vals}", schema_name="repl")

        rows = [r for r in c.scan(jid) if r.weight > 0]
        assert len(rows) == 40, (
            f"join after reboot lost rows ({len(rows)}/40): the replicated dim copy "
            f"did not survive on every worker")
        got = {r["fid"]: r["nm"] for r in rows}
        for i in range(1, 41):
            assert got[i] == ((i % 4) + 1) * 100
        c.close()
    finally:
        if proc is not None:
            _stop(proc)
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Exchange-shaped views over all-replicated sources run correct-local
#
# A view whose sources are all replicated is read from worker 0 only, but the
# set-op / SELECT DISTINCT / range-band-join circuits scatter their output across
# every worker. At GNITZ_WORKERS >= 2 that silently drops the rows whose hash
# missed worker 0's partition and inflates the survivors' weights (broadcast in ×
# scatter out). Every case below asserts per-row WEIGHTS, not just presence — the
# bug inflates weights, so a presence-only check would pass on the ALL variants.
# Masked at W=1 (no scatter); the suite always runs W=4.
# ---------------------------------------------------------------------------


def _wmap(rows, *cols):
    """{col-value(s) -> net weight} over positive-net rows, summing any duplicate
    identity (a duplicate summing above its expected weight is exactly the ×W
    inflation these tests pin down). Single col -> scalar key; else a tuple."""
    out = {}
    for r in rows:
        key = r[cols[0]] if len(cols) == 1 else tuple(r[c] for c in cols)
        out[key] = out.get(key, 0) + r.weight
    return {k: w for k, w in out.items() if w != 0}


def _mk_repl_ab(client, sn, a_extra="val BIGINT NOT NULL", b_extra="val BIGINT NOT NULL"):
    """Two replicated single-PK tables `a`/`b` for the set-op and join cases."""
    client.execute_sql(
        f"CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, {a_extra}) "
        "WITH (replicated = true)", schema_name=sn)
    client.execute_sql(
        f"CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, {b_extra}) "
        "WITH (replicated = true)", schema_name=sn)


def test_replicated_union_all_and_distinct(client):
    """UNION ALL / UNION over two replicated tables. The overlap rows (3,30),(4,40)
    carry weight 2 under UNION ALL (Σw=8) and 1 under UNION DISTINCT — both would be
    wrong under the scatter bug (dropped rows and/or ×W weights)."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        _mk_repl_ab(client, sn)
        client.execute_sql(
            "CREATE VIEW v_all AS SELECT * FROM a UNION ALL SELECT * FROM b", schema_name=sn)
        client.execute_sql(
            "CREATE VIEW v_dist AS SELECT * FROM a UNION SELECT * FROM b", schema_name=sn)
        v_all = client.resolve_table(sn, "v_all")[0]
        v_dist = client.resolve_table(sn, "v_dist")[0]

        client.execute_sql("INSERT INTO a VALUES (1,10),(2,20),(3,30),(4,40)", schema_name=sn)
        client.execute_sql("INSERT INTO b VALUES (3,30),(4,40),(5,50),(6,60)", schema_name=sn)

        all_rows = list(client.scan(v_all))
        assert _wmap(all_rows, "pk", "val") == {
            (1, 10): 1, (2, 20): 1, (3, 30): 2, (4, 40): 2, (5, 50): 1, (6, 60): 1,
        }
        assert sum(r.weight for r in all_rows if r.weight > 0) == 8
        assert _wmap(client.scan(v_dist), "pk", "val") == {
            (1, 10): 1, (2, 20): 1, (3, 30): 1, (4, 40): 1, (5, 50): 1, (6, 60): 1,
        }
    finally:
        client.drop_schema(sn)


def test_replicated_intersect_except(client):
    """INTERSECT / EXCEPT (DISTINCT and ALL) over two replicated tables. Each row is
    unique per source (unique_pk), so ALL and DISTINCT coincide at weight 1 — but they
    compile to different clamp circuits, so both are exercised."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        _mk_repl_ab(client, sn)
        client.execute_sql(
            "CREATE VIEW v_int AS SELECT * FROM a INTERSECT SELECT * FROM b", schema_name=sn)
        client.execute_sql(
            "CREATE VIEW v_exc AS SELECT * FROM a EXCEPT SELECT * FROM b", schema_name=sn)
        client.execute_sql(
            "CREATE VIEW v_int_all AS SELECT * FROM a INTERSECT ALL SELECT * FROM b", schema_name=sn)
        client.execute_sql(
            "CREATE VIEW v_exc_all AS SELECT * FROM a EXCEPT ALL SELECT * FROM b", schema_name=sn)
        vid = {n: client.resolve_table(sn, n)[0]
               for n in ("v_int", "v_exc", "v_int_all", "v_exc_all")}

        client.execute_sql("INSERT INTO a VALUES (1,10),(2,20),(3,30),(4,40)", schema_name=sn)
        client.execute_sql("INSERT INTO b VALUES (3,30),(4,40),(5,50),(6,60)", schema_name=sn)

        assert _wmap(client.scan(vid["v_int"]), "pk", "val") == {(3, 30): 1, (4, 40): 1}
        assert _wmap(client.scan(vid["v_int_all"]), "pk", "val") == {(3, 30): 1, (4, 40): 1}
        assert _wmap(client.scan(vid["v_exc"]), "pk", "val") == {(1, 10): 1, (2, 20): 1}
        assert _wmap(client.scan(vid["v_exc_all"]), "pk", "val") == {(1, 10): 1, (2, 20): 1}
    finally:
        client.drop_schema(sn)


def test_replicated_select_distinct(client):
    """SELECT DISTINCT over one replicated source. The content-hash PK scatters the
    output; under the bug worker 0 keeps only its hash slice, losing distinct values."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL) "
            "WITH (replicated = true)", schema_name=sn)
        client.execute_sql("CREATE VIEW v AS SELECT DISTINCT val FROM t", schema_name=sn)
        vid = client.resolve_table(sn, "v")[0]

        client.execute_sql("INSERT INTO t VALUES (1,5),(2,5),(3,7),(4,7),(5,9)", schema_name=sn)

        assert _wmap(client.scan(vid), "val") == {5: 1, 7: 1, 9: 1}
    finally:
        client.drop_schema(sn)


def test_replicated_self_union_all(client):
    """`a UNION ALL a` over one replicated table: every identical row keeps weight 2
    (branch-id disambiguated), NOT 2×W. Pins that the local path is not itself
    inflating the broadcast delta."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE s (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL) "
            "WITH (replicated = true)", schema_name=sn)
        client.execute_sql(
            "CREATE VIEW v AS SELECT * FROM s UNION ALL SELECT * FROM s", schema_name=sn)
        vid = client.resolve_table(sn, "v")[0]

        client.execute_sql("INSERT INTO s VALUES (1,10),(2,20),(3,30)", schema_name=sn)

        assert _wmap(client.scan(vid), "pk", "val") == {(1, 10): 2, (2, 20): 2, (3, 30): 2}
    finally:
        client.drop_schema(sn)


def test_replicated_band_join_inner_and_left(client):
    """Band join (`a.k = b.k AND a.lo <= b.t`, n_eq=1) over two replicated tables,
    INNER and LEFT. Band output rides the mandatory output exchange; the bug drops
    pairs and inflates weights. Every expected pair once (weight 1)."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        _mk_repl_ab(
            client, sn,
            a_extra="k BIGINT NOT NULL, lo BIGINT NOT NULL",
            b_extra="k BIGINT NOT NULL, t BIGINT NOT NULL")
        client.execute_sql(
            "CREATE VIEW vin AS SELECT a.pk AS aid, b.pk AS bid "
            "FROM a JOIN b ON a.k = b.k AND a.lo <= b.t", schema_name=sn)
        client.execute_sql(
            "CREATE VIEW vleft AS SELECT a.pk AS aid, b.pk AS bid "
            "FROM a LEFT JOIN b ON a.k = b.k AND a.lo <= b.t", schema_name=sn)
        vin = client.resolve_table(sn, "vin")[0]
        vleft = client.resolve_table(sn, "vleft")[0]

        # a4 (k=3) matches no b -> LEFT null-fills it.
        client.execute_sql(
            "INSERT INTO a VALUES (1,1,10),(2,1,50),(3,2,5),(4,3,1)", schema_name=sn)
        client.execute_sql(
            "INSERT INTO b VALUES (1,1,40),(2,1,60),(3,2,100)", schema_name=sn)

        # a1(lo10): b1(t40),b2(t60); a2(lo50): b2(t60); a3(k2,lo5): b3(t100).
        assert _wmap(client.scan(vin), "aid", "bid") == {
            (1, 1): 1, (1, 2): 1, (2, 2): 1, (3, 3): 1,
        }
        assert _wmap(client.scan(vleft), "aid", "bid") == {
            (1, 1): 1, (1, 2): 1, (2, 2): 1, (3, 3): 1, (4, None): 1,
        }
    finally:
        client.drop_schema(sn)


def test_replicated_pure_range_join_inner_and_left(client):
    """Pure-range join (`a.x < b.y`, n_eq=0) over two replicated tables. The broadcast
    input is normally trimmed by a PartitionFilter to the owning worker's slice; under
    an all-replicated local run that filter must be gone (Part B) or it discards rows.
    Includes a LEFT with a NULL range key, exercising the second (NULL-branch) filter."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, x BIGINT) "
            "WITH (replicated = true)", schema_name=sn)
        client.execute_sql(
            "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL) "
            "WITH (replicated = true)", schema_name=sn)
        client.execute_sql(
            "CREATE VIEW vin AS SELECT a.pk AS aid, b.pk AS bid "
            "FROM a JOIN b ON a.x < b.y", schema_name=sn)
        client.execute_sql(
            "CREATE VIEW vleft AS SELECT a.pk AS aid, b.pk AS bid "
            "FROM a LEFT JOIN b ON a.x < b.y", schema_name=sn)
        vin = client.resolve_table(sn, "vin")[0]
        vleft = client.resolve_table(sn, "vleft")[0]

        # a4 has a NULL range key -> never matches (3VL) -> LEFT null-fills via the
        # separate NULL-branch. a3 (x=50) exceeds every b.y -> null-fills too.
        client.execute_sql("INSERT INTO a VALUES (1,10),(2,30),(3,50),(4,NULL)", schema_name=sn)
        client.execute_sql("INSERT INTO b VALUES (1,20),(2,40)", schema_name=sn)

        # a1(10)<20,<40; a2(30)<40; a3(50) none; a4(NULL) none.
        assert _wmap(client.scan(vin), "aid", "bid") == {(1, 1): 1, (1, 2): 1, (2, 2): 1}
        assert _wmap(client.scan(vleft), "aid", "bid") == {
            (1, 1): 1, (1, 2): 1, (2, 2): 1, (3, None): 1, (4, None): 1,
        }
    finally:
        client.drop_schema(sn)


def test_replicated_pure_range_exists(client):
    """Pure-range EXISTS/NOT EXISTS (`b.y < a.x` -> `a.x > MIN(b.y)`) over two
    replicated tables. Carries the same broadcast-trim PartitionFilters as the
    pure-range LEFT join (Part B). A NULL outer range key exercises the NOT EXISTS
    NULL-branch filter."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT, v BIGINT NOT NULL) "
            "WITH (replicated = true)", schema_name=sn)
        client.execute_sql(
            "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL) "
            "WITH (replicated = true)", schema_name=sn)
        client.execute_sql(
            "CREATE VIEW semi AS SELECT v FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.y < a.x)",
            schema_name=sn)
        client.execute_sql(
            "CREATE VIEW anti AS SELECT v FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.y < a.x)",
            schema_name=sn)
        semi = client.resolve_table(sn, "semi")[0]
        anti = client.resolve_table(sn, "anti")[0]

        # MIN(b.y)=15. a2(x=20)>15 -> exists; a1(x=10),a3(x=5) -> not; a4(x=NULL) -> not.
        client.execute_sql(
            "INSERT INTO a VALUES (1,10,100),(2,20,200),(3,5,300),(4,NULL,400)", schema_name=sn)
        client.execute_sql("INSERT INTO b VALUES (1,15)", schema_name=sn)

        assert _wmap(client.scan(semi), "v") == {200: 1}
        assert _wmap(client.scan(anti), "v") == {100: 1, 300: 1, 400: 1}
    finally:
        client.drop_schema(sn)


def test_replicated_equi_in_subquery(client):
    """Equi `IN (SELECT ...)` over two replicated tables. Already local via the
    join-shard co-partition skip; pins that the all-replicated short-circuit (Part A)
    keeps it correct — the semi-join emits each outer row once at weight 1."""
    sn = "r" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL) "
            "WITH (replicated = true)", schema_name=sn)
        client.execute_sql(
            "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL) "
            "WITH (replicated = true)", schema_name=sn)
        client.execute_sql(
            "CREATE VIEW vin AS SELECT v FROM a WHERE k IN (SELECT k FROM b)", schema_name=sn)
        vin = client.resolve_table(sn, "vin")[0]

        # b holds k in {1,3}. a rows with k in {1,3} pass; k=2 does not.
        client.execute_sql(
            "INSERT INTO a VALUES (1,1,100),(2,2,200),(3,3,300),(4,1,400)", schema_name=sn)
        client.execute_sql("INSERT INTO b VALUES (1,1),(2,3)", schema_name=sn)

        assert _wmap(client.scan(vin), "v") == {100: 1, 300: 1, 400: 1}
    finally:
        client.drop_schema(sn)
