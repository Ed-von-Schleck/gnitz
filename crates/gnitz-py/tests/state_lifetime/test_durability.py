"""An ACKed operation survives SIGKILL.

The SAL durability rule is that an ACK implies `fdatasync` iff the operation
wrote something a restart must recover, so everything acknowledged below must
come back from the SAL tail with no checkpoint in between. What varies here is
the *operation* — DML verb, DDL kind, id allocation, column encoding — against
one fixed boundary; a test that varies the boundary instead lives in one of this
directory's other files.

Assertions are weight bags. A crash's failure modes are a lost row, a row
replayed twice, and a retraction applied twice; only the middle one is invisible
to a row-set or `len()` comparison, and it is the one a replay bug produces.
"""

import pytest
import gnitz
from _read import bag, rows, scanned

_COLS = ("pk", "val", "s", "n")

# 24 rows so every partition of a 4-worker run holds several, plus the payload
# shapes whose durable form differs: an inline German string, an out-of-line one
# past the 12-byte threshold, the empty string, and NULL in a nullable column.
_ROWS = [(i, i * 10, f"r{i}", i if i % 2 else None) for i in range(24)]
_ROWS += [(100, 1000, "A" * 200, 5), (101, 1010, "B" * 250, None), (102, 1020, "", 7)]


def _row_sql(rows):
    return ", ".join(
        "({}, {}, '{}', {})".format(pk, val, s, "NULL" if n is None else n)
        for pk, val, s, n in rows)


def test_every_dml_verb_survives_two_crashes(own_server):
    """INSERT, UPDATE, DELETE and delete-then-reinsert, the last three applied
    repeatedly to one key, then two consecutive crashes with no clean shutdown
    between them. A retraction replayed twice nets the row to weight 0 and a
    push replayed twice to weight 2, so the bag is the observable."""
    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        conn.create_schema("dur")
        conn.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL, "
            "s VARCHAR(500) NOT NULL, n BIGINT)", schema_name="dur")
        tid, _ = conn.resolve_table("dur", "t")
        conn.execute_sql(f"INSERT INTO t VALUES {_row_sql(_ROWS)}", schema_name="dur")
        for v in (200, 300, 400):
            conn.execute_sql(f"UPDATE t SET val = {v} WHERE pk = 1", schema_name="dur")
        conn.execute_sql("DELETE FROM t WHERE pk = 3", schema_name="dur")
        conn.execute_sql("INSERT INTO t VALUES (3, 999, 'again', NULL)",
                         schema_name="dur")
        conn.execute_sql("DELETE FROM t WHERE pk = 5", schema_name="dur")

    want = {r for r in _ROWS if r[0] not in (1, 3, 5)}
    want |= {(1, 400, "r1", 1), (3, 999, "again", None)}
    want = {r: 1 for r in want}

    own_server.restart()
    with gnitz.connect(own_server.sock_path) as conn:
        assert conn.resolve_table("dur", "t")[0] == tid, "table_id moved across a crash"
        assert bag(scanned(conn, "dur", "t"), *_COLS) == want
        conn.execute_sql("INSERT INTO t VALUES (300, 3000, 'post', 1)",
                         schema_name="dur")

    own_server.restart()
    with gnitz.connect(own_server.sock_path) as conn:
        assert bag(scanned(conn, "dur", "t"), *_COLS) == want | {
            (300, 3000, "post", 1): 1}, "a second crash with no clean stop between"


def test_every_ddl_kind_survives_a_crash(own_server):
    """CREATE TABLE and VIEW, a rapid batch of CREATEs whose fdatasync is
    deferred to end-of-cycle, DROP TABLE and DROP VIEW, and DDL interleaved with
    the DML it brackets. Every id must be the one it was, every drop must stay
    dropped, and the view must still be maintained afterwards.

    The same 13 relations pin the id allocators, whose durable home is a system
    table: recovered from the stale shard rather than the SAL, the next CREATE
    re-issues an id already in use."""
    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        sid = conn.create_schema("dur")
        conn.execute_sql(
            "CREATE TABLE t1 (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="dur")
        conn.execute_sql("INSERT INTO t1 VALUES (1, 10)", schema_name="dur")
        conn.execute_sql(
            "CREATE TABLE t2 (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="dur")
        conn.execute_sql("INSERT INTO t2 VALUES (1, 20)", schema_name="dur")
        conn.execute_sql("CREATE VIEW v AS SELECT pk, val * 2 AS doubled FROM t1",
                         schema_name="dur")
        conn.execute_sql("INSERT INTO t1 VALUES (2, 30)", schema_name="dur")

        # Rapid-fire CREATEs: one SAL broadcast each, one fdatasync for the lot.
        for i in range(10):
            conn.execute_sql(
                f"CREATE TABLE b{i} (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
                schema_name="dur")
        ids = {n: conn.resolve_table("dur", n)[0]
               for n in ["t1", "t2", "v"] + [f"b{i}" for i in range(10)]}

        conn.execute_sql(
            "CREATE TABLE gone (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name="dur")
        conn.execute_sql("INSERT INTO gone VALUES (1, 1)", schema_name="dur")
        conn.execute_sql("DROP TABLE dur.gone", schema_name="dur")
        conn.execute_sql("CREATE VIEW vgone AS SELECT pk, val FROM t1",
                         schema_name="dur")
        conn.execute_sql("DROP VIEW dur.vgone", schema_name="dur")

    own_server.restart()
    with gnitz.connect(own_server.sock_path) as conn:
        assert {n: conn.resolve_table("dur", n)[0] for n in ids} == ids
        sid2 = conn.create_schema("dur2")
        conn.execute_sql("CREATE TABLE fresh (pk BIGINT NOT NULL PRIMARY KEY)",
                         schema_name="dur2")
        assert conn.resolve_table("dur2", "fresh")[0] > max(ids.values()), \
            "table_id reissued across the crash"
        assert sid2 > sid, "schema_id reissued across the crash"

        for dropped in ("gone", "vgone"):
            with pytest.raises(gnitz.GnitzNotFoundError):
                conn.resolve_table("dur", dropped)

        assert bag(scanned(conn, "dur", "t1"), "pk", "val") == {(1, 10): 1, (2, 30): 1}
        assert bag(scanned(conn, "dur", "t2"), "pk", "val") == {(1, 20): 1}

        # Every re-created relation is functional, and the view still ticks.
        conn.execute_sql("INSERT INTO b7 VALUES (1, 42)", schema_name="dur")
        assert bag(scanned(conn, "dur", "b7"), "pk", "v") == {(1, 42): 1}
        conn.execute_sql("INSERT INTO t1 VALUES (3, 50)", schema_name="dur")
        assert bag(scanned(conn, "dur", "v"), "pk", "doubled") == {
            (1, 20): 1, (2, 60): 1, (3, 100): 1}


def test_an_acked_create_index_survives_a_crash(own_server):
    """An FK auto-index is applied as an un-pinned local ingest that bumps
    `sys_indices`' current_lsn once per non-PK FK column, while the CREATE TABLE
    consumes a single zone LSN — so the IDX_TAB recovery watermark drifts ahead
    of the zone allocator. A checkpoint persists the drifted counter, and a later
    CREATE INDEX whose zone LSN sits at or below it is deduped away by recovery's
    `msg.lsn <= flushed` check: the index vanishes despite the ACK.

    Read through a seek rather than the catalog: the contract is that the
    acknowledged index still serves, and a scan of IDX_TAB pins a column ordinal
    any schema edit moves."""
    # A tiny threshold so the DDL broadcasts below make the first INSERT's
    # committer cycle run a checkpoint, persisting the drifted watermark.
    own_server.start(extra_env={"GNITZ_CHECKPOINT_BYTES": "1024"})
    with gnitz.connect(own_server.sock_path) as conn:
        conn.create_schema("idxcrash")
        conn.execute_sql(
            "CREATE TABLE p (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
            schema_name="idxcrash")
        # Three children × four non-PK FK columns drift the watermark well past
        # the zone allocator.
        for t in ("c1", "c2", "c3"):
            conn.execute_sql(
                f"CREATE TABLE {t} (cid BIGINT NOT NULL PRIMARY KEY,"
                "  f1 BIGINT NOT NULL REFERENCES p(id),"
                "  f2 BIGINT NOT NULL REFERENCES p(id),"
                "  f3 BIGINT NOT NULL REFERENCES p(id),"
                "  f4 BIGINT NOT NULL REFERENCES p(id),"
                "  val BIGINT NOT NULL)", schema_name="idxcrash")
        conn.execute_sql("INSERT INTO p VALUES (1, 100)", schema_name="idxcrash")
        conn.execute_sql("INSERT INTO c1 VALUES (1, 1, 1, 1, 1, 77)",
                         schema_name="idxcrash")

        conn.execute_sql("CREATE INDEX ON c1(val)", schema_name="idxcrash")
        c1, _ = conn.resolve_table("idxcrash", "c1")
        VAL_COL = 5  # cid, f1..f4, val
        assert sorted(conn.seek_by_index(c1, [VAL_COL], [77]).pks) == [1], \
            "index missing pre-crash"

    # SIGKILL before any further checkpoint: the CREATE INDEX lives only in the
    # SAL, guarded by the watermark dedup.
    own_server.restart()
    with gnitz.connect(own_server.sock_path) as conn:
        c1_again, _ = conn.resolve_table("idxcrash", "c1")
        assert c1_again == c1
        assert sorted(conn.seek_by_index(c1_again, [VAL_COL], [77]).pks) == [1], \
            "acknowledged CREATE INDEX lost after crash recovery"


# ── ALTER TABLE ADD COLUMN across the boundary ──────────────────────────────
# `schema_lifetime/test_alter_add.py` owns the live semantics and delegates the
# replay paths here: the pre-ALTER SAL tail carries pushes embedded at the *old*
# width, and a checkpointed shard was written at it.


def _alter_add_base(conn, sn, rows):
    conn.create_schema(sn)
    conn.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
        schema_name=sn)
    conn.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(f"({k}, {k * 10})" for k in rows),
        schema_name=sn)


def _wide(conn, sn):
    """The widened table as a weight bag over `(id, a, c)`. A pk-keyed dict would
    read the doubled row a replayed push produces as correct — the one failure
    mode these two tests exist for."""
    return bag(rows(conn, sn, "SELECT * FROM t"), "id", "a", "c")


def test_add_column_then_crash_replays_the_pre_alter_sal_tail(own_server):
    """The ALTER and the pushes it postdates are all in the un-checkpointed tail.
    On recovery the master applies the catalog tail pre-fork, so the table is
    already at its final width when the workers replay pushes embedded at the old
    width — each of which must be widened with a NULL tail exactly once."""
    sn = "aadd_crash"
    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        _alter_add_base(conn, sn, range(1, 11))
        conn.execute_sql("ALTER TABLE t ADD COLUMN c BIGINT", schema_name=sn)
        conn.execute_sql("INSERT INTO t VALUES (11, 110, 1100)", schema_name=sn)
        before = _wide(conn, sn)
        assert before == {(k, k * 10, None): 1 for k in range(1, 11)} | {
            (11, 110, 1100): 1}

    own_server.restart()
    with gnitz.connect(own_server.sock_path) as conn:
        assert _wide(conn, sn) == before


def test_add_column_over_shards_written_at_the_old_arity(own_server):
    """A graceful stop checkpoints, so the pre-ALTER rows land in shards written
    at the *old* arity. The next boot opens them under the wider schema and pads
    the appended column to NULL; rewriting them materializes it at full width and
    the pad decays.

    Both stops go through `restart(graceful=True)`, which asserts the master
    exited 0 — a failed shutdown checkpoint would leave the rows in the SAL and
    the replay path, not the pad path, would answer every assertion below."""
    sn = "aadd_ckpt"
    own_server.start()
    with gnitz.connect(own_server.sock_path) as conn:
        _alter_add_base(conn, sn, range(1, 21))

    # Checkpoint the narrow rows to disk, then ALTER against those shards.
    own_server.restart(graceful=True)
    with gnitz.connect(own_server.sock_path) as conn:
        conn.execute_sql("ALTER TABLE t ADD COLUMN c BIGINT", schema_name=sn)
        assert _wide(conn, sn) == {(k, k * 10, None): 1 for k in range(1, 21)}
        # Write across the padded shards: a new row, and an UPDATE of a padded
        # one, whose retraction must match the stored NULL rather than a zero.
        conn.execute_sql("INSERT INTO t VALUES (21, 210, 2100)", schema_name=sn)
        conn.execute_sql("UPDATE t SET c = 7 WHERE id = 5", schema_name=sn)

    # Checkpoint again — rewriting the padded shards at the new width — so the
    # reads below come off full-width files.
    own_server.restart(graceful=True)
    with gnitz.connect(own_server.sock_path) as conn:
        want = {(k, k * 10, None): 1 for k in range(1, 21) if k != 5}
        want |= {(5, 50, 7): 1, (21, 210, 2100): 1}
        assert _wide(conn, sn) == want
