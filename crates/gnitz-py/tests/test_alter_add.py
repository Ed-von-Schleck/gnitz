"""End-to-end ALTER TABLE ADD COLUMN tests.

Run with GNITZ_WORKERS=4 so the region-count growth must reach every partition:
the descriptor swap widens each partition's resident runs and re-opens its
shards, and a pre-ALTER shard is read back through the per-shard NULL pad on
every worker, not just one.

    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_alter_add.py -v --tb=short
"""
import threading

import pytest
import gnitz
from _uid import uid as _uid




def _rows(client, sn, sql):
    res = client.execute_sql(sql, schema_name=sn)
    assert res[0]["type"] == "Rows", f"expected Rows, got {res[0]['type']}"
    # `rows` is a lazy ScanResult (iterable, not indexable) — materialize it so
    # tests can index and re-scan it.
    return list(res[0]["rows"])


# ── Basic semantics ─────────────────────────────────────────────────────────


def test_add_column_reads_null_for_pre_alter_rows(client):
    """The core contract: existing rows read the appended column as NULL, and it
    is writable from then on — through inserts, updates, scans and point seeks."""
    sn = "aadd" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
            schema_name=sn,
        )
        for k in range(1, 21):
            client.execute_sql(f"INSERT INTO t VALUES ({k}, {k * 10})", schema_name=sn)

        client.execute_sql("ALTER TABLE t ADD COLUMN c BIGINT", schema_name=sn)

        # `SELECT *` includes the new column; every pre-ALTER row reads NULL.
        rows = _rows(client, sn, "SELECT * FROM t")
        assert len(rows) == 20
        assert "c" in rows[0]._fields
        assert all(r["c"] is None for r in rows)

        # A new INSERT supplies it; an explicit NULL leaves it unset. (gnitz has
        # no partial column lists, so the NULL is spelled out.)
        client.execute_sql("INSERT INTO t VALUES (21, 210, 2100)", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (22, 220, NULL)", schema_name=sn)

        # UPDATE sets it on a pre-ALTER row (retract + insert of a padded row —
        # the retraction has to match the stored row, whose `c` is NULL).
        client.execute_sql("UPDATE t SET c = 55 WHERE id = 1", schema_name=sn)
        # …and DELETE removes one.
        client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=sn)

        got = {r["id"]: r["c"] for r in _rows(client, sn, "SELECT * FROM t")}
        assert 2 not in got
        assert got[1] == 55
        assert got[21] == 2100
        assert got[22] is None
        assert got[3] is None
        assert len(got) == 21

        # Point seek on a padded row and on a post-ALTER row.
        assert _rows(client, sn, "SELECT c FROM t WHERE id = 3")[0]["c"] is None
        assert _rows(client, sn, "SELECT c FROM t WHERE id = 21")[0]["c"] == 2100

        # The new column is filterable and projectable by name.
        hits = _rows(client, sn, "SELECT id FROM t WHERE c = 55")
        assert [r["id"] for r in hits] == [1]
    finally:
        client.drop_schema(sn)


def test_add_string_column(client):
    """A STRING column appended over existing rows: old rows read NULL (no heap
    read for a cell the shard has no bytes for), new inserts store heap values."""
    sn = "aadd" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
            schema_name=sn,
        )
        for k in range(1, 6):
            client.execute_sql(f"INSERT INTO t VALUES ({k}, {k})", schema_name=sn)

        client.execute_sql("ALTER TABLE t ADD COLUMN s TEXT", schema_name=sn)
        assert all(r["s"] is None for r in _rows(client, sn, "SELECT * FROM t"))

        # Both German-string forms: inline (<= 12 bytes) and heap-resident.
        client.execute_sql("INSERT INTO t VALUES (6, 6, 'short')", schema_name=sn)
        client.execute_sql(
            "INSERT INTO t VALUES (7, 7, 'a string comfortably past the inline prefix')",
            schema_name=sn,
        )
        client.execute_sql("UPDATE t SET s = 'set later' WHERE id = 1", schema_name=sn)

        got = {r["id"]: r["s"] for r in _rows(client, sn, "SELECT * FROM t")}
        assert got[1] == "set later"
        assert got[2] is None
        assert got[6] == "short"
        assert got[7] == "a string comfortably past the inline prefix"
    finally:
        client.drop_schema(sn)


def test_add_column_after_drop_column_reuses_the_name(client):
    """Re-ADD of a previously DROPped name is a **new** physical column starting
    NULL; the hidden old column keeps its own values and stays unnameable."""
    sn = "aadd" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 10, 100)", schema_name=sn)
        client.execute_sql("ALTER TABLE t DROP COLUMN a", schema_name=sn)
        client.execute_sql("ALTER TABLE t ADD COLUMN a BIGINT", schema_name=sn)

        rows = _rows(client, sn, "SELECT * FROM t")
        assert len(rows) == 1
        # One visible `a`, and it is the new (NULL) one — not the hidden slot's 10.
        assert rows[0]["a"] is None
        assert rows[0]["b"] == 100

        client.execute_sql("INSERT INTO t VALUES (2, 200, 20)", schema_name=sn)
        got = {r["id"]: (r["b"], r["a"]) for r in _rows(client, sn, "SELECT * FROM t")}
        assert got == {1: (100, None), 2: (200, 20)}
    finally:
        client.drop_schema(sn)


def test_two_serialized_add_columns(client):
    """Two ADD COLUMNs in sequence both succeed and add two columns — the second
    reads the already-widened count and picks the next index. (Two *concurrent*
    appends that both resolved the table at width N pick the same packed column
    id, and the loser fails the per-PK net bound engine-side.)"""
    sn = "aadd" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
        client.execute_sql("ALTER TABLE t ADD COLUMN c1 BIGINT", schema_name=sn)
        client.execute_sql("ALTER TABLE t ADD COLUMN c2 BIGINT", schema_name=sn)

        rows = _rows(client, sn, "SELECT * FROM t")
        assert rows[0]._fields == ("id", "a", "c1", "c2")
        assert (rows[0]["c1"], rows[0]["c2"]) == (None, None)

        client.execute_sql("INSERT INTO t VALUES (2, 20, 21, 22)", schema_name=sn)
        got = {r["id"]: (r["c1"], r["c2"]) for r in _rows(client, sn, "SELECT * FROM t")}
        assert got == {1: (None, None), 2: (21, 22)}
    finally:
        client.drop_schema(sn)


def test_add_column_reaches_every_worker(client):
    """Enough rows to spread across all 4 workers: the swap has
    to widen every partition's resident runs, or a scan would read the appended
    column off a narrow run."""
    sn = "aadd" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
            schema_name=sn,
        )
        n = 1000
        for k in range(n):
            client.execute_sql(f"INSERT INTO t VALUES ({k}, {k})", schema_name=sn)

        client.execute_sql("ALTER TABLE t ADD COLUMN c BIGINT", schema_name=sn)

        rows = _rows(client, sn, "SELECT * FROM t")
        assert len(rows) == n
        assert all(r["c"] is None for r in rows)
        # Weights, not just presence: a mis-widened run would consolidate a padded
        # row against a real one and change the row count under a re-scan.
        assert len({r["id"] for r in rows}) == n
    finally:
        client.drop_schema(sn)


# ── Secondary indexes and dependent views ───────────────────────────────────


def test_add_column_with_secondary_index(client):
    """An index schema folds every indexed column into its PK, so it has zero
    payload columns and no locator that a trailing append moves: no index swap,
    no rebuild, and seeks keep working at the wider width."""
    sn = "aadd" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
            schema_name=sn,
        )
        for k in range(1, 11):
            client.execute_sql(f"INSERT INTO t VALUES ({k}, {k * 10})", schema_name=sn)
        client.execute_sql("CREATE INDEX ON t (a)", schema_name=sn)

        client.execute_sql("ALTER TABLE t ADD COLUMN c BIGINT", schema_name=sn)

        # Point seek through the index, on a pre-ALTER row.
        hit = _rows(client, sn, "SELECT id, a, c FROM t WHERE a = 30")
        assert len(hit) == 1
        assert (hit[0]["id"], hit[0]["a"], hit[0]["c"]) == (3, 30, None)

        # Range seek through the index, spanning pre- and post-ALTER rows.
        client.execute_sql("INSERT INTO t VALUES (11, 110, 1)", schema_name=sn)
        span = _rows(client, sn, "SELECT id, c FROM t WHERE a >= 90 ORDER BY a")
        assert [(r["id"], r["c"]) for r in span] == [(9, None), (10, None), (11, 1)]
    finally:
        client.drop_schema(sn)


def test_add_column_rejected_with_dependent_view(client):
    """RESTRICT: a compiled circuit's ScanDelta register schema is baked from the
    base descriptor, so the ALTER is refused while a view scans the table — and
    the view keeps serving afterwards."""
    sn = "aadd" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW v AS SELECT id, a FROM t WHERE a > 5", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)

        with pytest.raises(Exception):
            client.execute_sql("ALTER TABLE t ADD COLUMN c BIGINT", schema_name=sn)

        # The rejection left nothing behind: no new column, and the view still
        # maintains incrementally.
        rows = _rows(client, sn, "SELECT * FROM t")
        assert rows[0]._fields == ("id", "a")
        client.execute_sql("INSERT INTO t VALUES (2, 20)", schema_name=sn)
        assert {r["id"] for r in _rows(client, sn, "SELECT * FROM v")} == {1, 2}

        # Dropping the view lifts the RESTRICT.
        client.execute_sql("DROP VIEW v", schema_name=sn)
        client.execute_sql("ALTER TABLE t ADD COLUMN c BIGINT", schema_name=sn)
        assert all(r["c"] is None for r in _rows(client, sn, "SELECT * FROM t"))
    finally:
        client.drop_schema(sn)


# ── Rejections ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "clause",
    [
        "ADD COLUMN c BIGINT NOT NULL",
        "ADD COLUMN c SERIAL",
        "ADD COLUMN c BIGINT DEFAULT 0",
        "ADD COLUMN c BIGINT PRIMARY KEY",
        "ADD COLUMN c BIGINT UNIQUE",
        "ADD COLUMN c BIGINT REFERENCES p (id)",
        "ADD COLUMN c BIGINT CHECK (c > 0)",
        "ADD COLUMN c BIGINT COLLATE utf8",
        "ADD COLUMN IF NOT EXISTS c BIGINT",
        "ADD COLUMN c BIGINT FIRST",
        "ADD COLUMN c BIGINT AFTER a",
    ],
)
def test_add_column_rejected_clauses(client, clause):
    """Everything that would need a value for the existing rows, a second catalog
    object, or a physical move is refused before anything reaches the engine."""
    sn = "aadd" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE p (id BIGINT NOT NULL PRIMARY KEY)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
            schema_name=sn,
        )
        with pytest.raises(Exception):
            client.execute_sql(f"ALTER TABLE t {clause}", schema_name=sn)
        # Nothing partial landed.
        assert _rows(client, sn, "SELECT * FROM t") == []
        client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
        assert _rows(client, sn, "SELECT * FROM t")[0]._fields == ("id", "a")
    finally:
        client.drop_schema(sn)


def test_add_duplicate_column_name_rejected(client):
    sn = "aadd" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
            schema_name=sn,
        )
        with pytest.raises(Exception):
            client.execute_sql("ALTER TABLE t ADD COLUMN a BIGINT", schema_name=sn)
        with pytest.raises(Exception):
            client.execute_sql("ALTER TABLE t ADD COLUMN id BIGINT", schema_name=sn)
    finally:
        client.drop_schema(sn)


def test_add_column_on_view_rejected(client):
    sn = "aadd" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW v AS SELECT id, a FROM t", schema_name=sn)
        with pytest.raises(Exception):
            client.execute_sql("ALTER TABLE v ADD COLUMN c BIGINT", schema_name=sn)
    finally:
        client.drop_schema(sn)


# ── Concurrency ─────────────────────────────────────────────────────────────


def test_add_column_concurrent_with_inserts_and_scans(client, server):
    """An ALTER against live traffic. Every acknowledged INSERT must be readable
    at the value it supplied — never truncated to NULL by a worker whose stashed
    DdlSync had not drained — and every concurrent SELECT must return the
    pre-ALTER or the post-ALTER column set, never a torn row."""
    sn = "aadd" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
            schema_name=sn,
        )
        for k in range(200):
            client.execute_sql(f"INSERT INTO t VALUES ({k}, {k})", schema_name=sn)

        stop = threading.Event()
        scan_widths = set()
        acked = set()
        acked_lock = threading.Lock()

        def scanner():
            conn = gnitz.connect(server)
            try:
                while not stop.is_set():
                    try:
                        res = conn.execute_sql("SELECT * FROM t", schema_name=sn)
                        rows = list(res[0]["rows"])
                        if rows:
                            scan_widths.add(len(rows[0]._fields))
                    except Exception:
                        # A clean rejection across the width change is allowed;
                        # a torn row is not, and would show as a third width.
                        pass
            finally:
                conn.close()

        def inserter():
            conn = gnitz.connect(server)
            try:
                for k in range(1000, 1100):
                    # The width flips under us mid-run, so try the pre-ALTER shape
                    # then the post-ALTER one. A rejection is fine; an *accepted*
                    # INSERT is an ACK, and `a` must read back as `k` — the guard
                    # this test exists for is a wider batch silently truncated
                    # against a worker's stale schema and ACKed anyway.
                    for sql in (
                        f"INSERT INTO t VALUES ({k}, {k})",
                        f"INSERT INTO t VALUES ({k}, {k}, {k})",
                    ):
                        try:
                            conn.execute_sql(sql, schema_name=sn)
                        except Exception:
                            continue
                        with acked_lock:
                            acked.add(k)
                        break
            finally:
                conn.close()

        threads = [threading.Thread(target=scanner), threading.Thread(target=inserter)]
        for th in threads:
            th.start()
        try:
            client.execute_sql("ALTER TABLE t ADD COLUMN c BIGINT", schema_name=sn)
        finally:
            stop.set()
            for th in threads:
                th.join()

        # Only the two legitimate column sets were ever observed.
        assert scan_widths <= {2, 3}, f"torn scan widths: {scan_widths}"

        rows = _rows(client, sn, "SELECT * FROM t")
        assert rows[0]._fields == ("id", "a", "c")
        got = {r["id"]: (r["a"], r["c"]) for r in rows}
        # The pre-ALTER rows are intact and padded to NULL.
        for k in range(200):
            assert got[k] == (k, None)
        # Every acknowledged concurrent INSERT is present at the value it
        # supplied — nothing was accepted and then silently truncated.
        for k in acked:
            assert k in got, f"acknowledged INSERT of id={k} is missing"
            assert got[k][0] == k, f"acknowledged INSERT of id={k} read back {got[k][0]}"
        assert len(got) == 200 + len(acked)
    finally:
        client.drop_schema(sn)


# ── Durability ──────────────────────────────────────────────────────────────


def _setup(conn, sn):
    conn.create_schema(sn)
    conn.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
        schema_name=sn,
    )


def test_add_column_then_crash_replays_pre_alter_sal_tail(own_server):
    """The ALTER and the pushes it postdates are all in the un-checkpointed SAL
    tail. On recovery the master applies the catalog tail pre-fork, so the table
    is already at its final width when the workers replay pushes embedded at the
    *old* width — each of which must be widened with a NULL tail exactly once."""
    sn = "aadd_crash"
    own_server.start()
    conn = gnitz.connect(own_server.sock_path)
    _setup(conn, sn)
    for k in range(1, 11):
        conn.execute_sql(f"INSERT INTO t VALUES ({k}, {k * 10})", schema_name=sn)
    conn.execute_sql("ALTER TABLE t ADD COLUMN c BIGINT", schema_name=sn)
    conn.execute_sql("INSERT INTO t VALUES (11, 110, 1100)", schema_name=sn)
    before = sorted(
        (r["id"], r["a"], r["c"])
        for r in list(conn.execute_sql("SELECT * FROM t", schema_name=sn)[0]["rows"])
    )
    conn.close()

    # SIGKILL: no checkpoint, so everything is recovered from the SAL.
    own_server.stop()
    own_server.start()

    conn = gnitz.connect(own_server.sock_path)
    after = sorted(
        (r["id"], r["a"], r["c"])
        for r in list(conn.execute_sql("SELECT * FROM t", schema_name=sn)[0]["rows"])
    )
    assert after == before
    assert after[0] == (1, 10, None)
    assert after[-1] == (11, 110, 1100)
    conn.close()


def test_add_column_survives_checkpoint_and_restart(own_server):
    """A graceful stop checkpoints, so the pre-ALTER rows land in shards written
    at the *old* arity. The next boot opens them under the wider schema and pads
    the appended column to NULL; rewriting them (compaction / a later flush)
    materializes it at full width and the pad decays."""
    sn = "aadd_ckpt"
    own_server.start()
    conn = gnitz.connect(own_server.sock_path)
    _setup(conn, sn)
    for k in range(1, 21):
        conn.execute_sql(f"INSERT INTO t VALUES ({k}, {k * 10})", schema_name=sn)
    conn.close()

    # Checkpoint the narrow rows to disk, then ALTER against those shards.
    own_server.stop_graceful()
    own_server.start()

    conn = gnitz.connect(own_server.sock_path)
    conn.execute_sql("ALTER TABLE t ADD COLUMN c BIGINT", schema_name=sn)
    rows = list(conn.execute_sql("SELECT * FROM t", schema_name=sn)[0]["rows"])
    assert len(rows) == 20
    assert all(r["c"] is None for r in rows)

    # Write across the padded shards: a new row, and an UPDATE of a padded one
    # (whose retraction must match the stored NULL, not a non-null zero).
    conn.execute_sql("INSERT INTO t VALUES (21, 210, 2100)", schema_name=sn)
    conn.execute_sql("UPDATE t SET c = 7 WHERE id = 5", schema_name=sn)
    conn.close()

    # Checkpoint again — this rewrites the padded shards at the new width — and
    # restart, so the reads below come off full-width files.
    own_server.stop_graceful()
    own_server.start()

    conn = gnitz.connect(own_server.sock_path)
    got = {
        r["id"]: r["c"]
        for r in list(conn.execute_sql("SELECT * FROM t", schema_name=sn)[0]["rows"])
    }
    assert len(got) == 21
    assert got[5] == 7
    assert got[21] == 2100
    assert got[1] is None
    assert got[20] is None
    conn.close()
