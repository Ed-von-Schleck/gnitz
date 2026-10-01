"""A DDL landing while other connections read and write.

What must hold is what those connections were told: every ACKed write reads back
at the values it supplied, every read issued across the DDL is exact, and a
write the DDL overtook is refused rather than committed against the new layout.
"""

import threading

import gnitz
import pytest
from _read import bag, rows, scanned
from _schemas import KV
from _serverproc import START_TIMEOUT, join_or_fail, spawn


def test_every_insert_acked_across_an_add_column_reads_back(client, server):
    """An inserter runs across `ALTER TABLE ADD COLUMN`, trying the pre-ALTER
    shape and then the post-ALTER one. A rejection is allowed; an accepted INSERT
    is an ACK and must read back at the values it supplied, never re-framed
    against the other width. It keeps going for a fixed count after the ALTER
    returns, so the post-ALTER width is always exercised."""
    client.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)")
    acked = {}  # id -> the `c` its accepted INSERT supplied
    started, altered = threading.Event(), threading.Event()

    def inserter():
        try:
            with gnitz.connect(server, schema=client.schema) as conn:
                k = after = 0
                while after < 20:
                    after += altered.is_set()
                    for sql, c in ((f"INSERT INTO t VALUES ({k}, {k})", None),
                                   (f"INSERT INTO t VALUES ({k}, {k}, {k})", k)):
                        try:
                            conn.execute_sql(sql)
                        except gnitz.GnitzRefusedError:
                            continue
                        acked[k] = c
                        break
                    started.set()
                    k += 1
        finally:
            started.set()

    th = spawn(inserter)
    try:
        assert started.wait(START_TIMEOUT), "the inserter never issued a statement"
        client.execute_sql("ALTER TABLE t ADD COLUMN c BIGINT")
    finally:
        altered.set()
        join_or_fail("the inserter hung", th)
    assert bag(rows(client, "SELECT * FROM t"), "id", "a", "c") == \
        {(k, k, c): 1 for k, c in acked.items()}


def test_a_push_decoded_before_an_alter_is_refused(own_server):
    """A push matches its schema record against the catalog's and decodes its
    batch before it takes the catalog read lock, and the lock is
    writer-preferring, so an ALTER queued in between is applied first and the
    push resumes holding a batch laid out for the old width. The re-check under
    the lock must refuse it, not commit it.

    `GNITZ_INJECT_PUSH_HOLD_FOR_DDL` parks the server's first matched push
    between its decode and the lock, holding no lock, until a DDL replaces its
    target's schema record. The ALTER is issued once the pusher is at its push."""
    target = own_server.start(extra_env={"GNITZ_INJECT_PUSH_HOLD_FOR_DDL": "1"}).target
    client = gnitz.connect(target)
    tid = client.create_table("t", KV)
    pushing = threading.Event()

    def pusher():
        with gnitz.connect(target) as conn:
            batch = gnitz.ZSetBatch(KV).append(pk=1, val=7)
            pushing.set()
            with pytest.raises(gnitz.GnitzRefusedError):
                conn.push(tid, batch)

    th = spawn(pusher)
    try:
        assert pushing.wait(START_TIMEOUT), "the pusher never reached its push"
        client.execute_sql("ALTER TABLE t ADD COLUMN c BIGINT")
    finally:
        join_or_fail("the held push never returned", th)
    assert bag(scanned(client, "t")) == {}


@pytest.mark.parametrize("body, row", [
    ("SELECT a.id AS aid, b.bv AS bv FROM a JOIN b ON a.k = b.id", lambda i: (i, (i % 5) * 100)),
    ("SELECT a.id AS aid, a.av + 1 AS bv FROM a", lambda i: (i, i * 10 + 1)),
], ids=["exchange", "linear"])
def test_a_view_created_under_traffic_holds_every_row(client, server, body, row):
    """While `CREATE VIEW` runs, one connection inserts into its source and another
    runs an exchange-bearing ad-hoc GROUP BY. Every read must be exact, and the
    finished view must hold the snapshot rows and every insert at weight 1,
    whichever side of the DDL each insert landed on. An exchange body and a
    partition-local one both run, because only the first shuffles its backfill."""
    for sql in ("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, av BIGINT NOT NULL)",
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, bv BIGINT NOT NULL)",
                "INSERT INTO b VALUES " + ", ".join(f"({j}, {j * 100})" for j in range(5)),
                "INSERT INTO a VALUES " + ", ".join(f"({i}, {i % 5}, {i * 10})" for i in range(30))):
        client.execute_sql(sql)
    reading, writing, created = threading.Event(), threading.Event(), threading.Event()

    def reader():
        try:
            with gnitz.connect(server, schema=client.schema) as c:
                while True:
                    last = created.is_set()
                    got = bag(rows(c, "SELECT bv, COUNT(*) AS n FROM b GROUP BY bv"), "bv", "n")
                    assert got == {(j * 100, 1): 1 for j in range(5)}, got
                    reading.set()
                    if last:
                        return
        finally:
            reading.set()

    def inserter():
        try:
            with gnitz.connect(server, schema=client.schema) as c:
                for i in range(30, 60):
                    c.execute_sql(f"INSERT INTO a VALUES ({i}, {i % 5}, {i * 10})")
                    writing.set()
        finally:
            writing.set()

    threads = [spawn(reader), spawn(inserter)]
    try:
        assert reading.wait(START_TIMEOUT) and writing.wait(START_TIMEOUT), \
            "a concurrent connection never issued a statement"
        client.execute_sql(f"CREATE VIEW v AS {body}")
    finally:
        created.set()
        join_or_fail("a concurrent connection hung", *threads)
    assert bag(scanned(client, "v"), "aid", "bv") == {row(i): 1 for i in range(60)}


def test_a_drop_does_not_race_a_worker_still_creating_the_table(own_server):
    """The master sweeps a dropped table's directory only behind a checkpoint's
    worker-ACK barrier, so a worker still inside that table's CREATE finishes
    against a directory that exists instead of aborting on ENOENT.

    `GNITZ_INJECT_TABLE_CREATE_DELAY_MS` makes every worker sleep between creating
    the table directory and its partition subdirectories, and a DDL's ACK waits on
    no worker, so each DROP lands inside a lagging CREATE. Every worker answers the
    closing scan only after applying the racing groups, so one that aborted fails
    it by ordering rather than by timing."""
    client = gnitz.connect(own_server.start(extra_env={"GNITZ_INJECT_TABLE_CREATE_DELAY_MS": "50"}).target)
    for i in range(3):
        client.create_table(f"t{i}", KV)
        client.drop_table(f"t{i}")
    tid = client.create_table("t", KV)
    client.push(tid, gnitz.ZSetBatch(KV).append(pk=1, val=1))
    assert bag(client.scan(tid, KV)) == {(1, 1): 1}
