"""A DDL landing while other connections read and write.

What must hold is what those connections were told: every ACKed write reads back
at the values it supplied, every read issued across the DDL is exact, and a
write the DDL overtook is refused rather than committed against the new layout.
"""

import gnitz
import pytest
from _read import bag, rows, scanned
from _schemas import KV, kv
from _serverproc import START_TIMEOUT, across, await_log, join_or_fail, spawn
from _sql import values


def test_every_insert_acked_across_an_add_column_reads_back(client, peer):
    """An inserter runs across `ALTER TABLE ADD COLUMN`, trying the pre-ALTER
    shape and, once that is refused, the post-ALTER one, which must then be
    accepted. Every INSERT is an ACK and must read back at the values it
    supplied, never re-framed against the other width."""
    client.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)")
    conn = peer()
    acked = {}  # id -> the `c` its INSERT supplied

    def insert(k):
        try:
            conn.execute_sql(f"INSERT INTO t VALUES ({k}, {k})")
            acked[k] = None
        except gnitz.GnitzRefusedError:
            conn.execute_sql(f"INSERT INTO t VALUES ({k}, {k}, {k})")
            acked[k] = k

    across(lambda: client.execute_sql("ALTER TABLE t ADD COLUMN c BIGINT"), insert)
    assert bag(rows(client, "SELECT * FROM t"), "id", "a", "c") == \
        {(k, k, c): 1 for k, c in acked.items()}


def test_every_insert_acked_across_a_create_unique_index_reads_back(client, peer):
    """An inserter of distinct values runs across `CREATE UNIQUE INDEX`. The
    catalog write lock orders every INSERT strictly before or after the
    pre-flight and backfill snapshot, so every one is acked, the index is
    created, and it enforces each value an ACK wrote — the backfilled ones and
    those inserted after it."""
    seeded = range(1, 201)
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL); "
        f"INSERT INTO t VALUES {values((i, i) for i in seeded)}")
    conn = peer()
    n = across(lambda: client.execute_sql("CREATE UNIQUE INDEX ON t(v)"),
               lambda k: conn.execute_sql(f"INSERT INTO t VALUES ({1000 + k}, {1000 + k})"))
    acked = [*seeded, *range(1000, 1000 + n)]
    assert bag(scanned(client, "t")) == {(v, v): 1 for v in acked}
    for v in acked:
        with pytest.raises(gnitz.GnitzIntegrityError, match="Unique index violation"):
            client.execute_sql(f"INSERT INTO t VALUES ({-v}, {v})")
    client.execute_sql("INSERT INTO t VALUES (888888, 888888)")


def test_a_push_decoded_before_an_alter_is_refused(own_server):
    """A push matches its schema record against the catalog's and decodes its
    batch before it takes the catalog read lock, and the lock is
    writer-preferring, so an ALTER queued in between is applied first and the
    push resumes holding a batch laid out for the old width. The re-check under
    the lock must refuse it, not commit it.

    `GNITZ_INJECT_PUSH_HOLD_FOR_DDL` parks the server's first matched push
    between its decode and the lock, holding no lock, until a DDL replaces its
    target's schema record. The ALTER is issued once that push is parked."""
    target = own_server.start(extra_env={"GNITZ_INJECT_PUSH_HOLD_FOR_DDL": "1"}).target
    client = gnitz.connect(target)
    tid = client.create_table("t", KV)
    with gnitz.connect(target) as pusher:
        held = spawn(pusher.push, tid, kv((1, 7)))
        await_log(own_server.proc, own_server.log_text, "push hold: parked", START_TIMEOUT)
        client.execute_sql("ALTER TABLE t ADD COLUMN c BIGINT")
        with pytest.raises(gnitz.GnitzRefusedError):
            join_or_fail("the held push never returned", held)
    assert bag(scanned(client, "t")) == {}


@pytest.mark.parametrize("body, row", [
    ("SELECT a.id AS aid, b.bv AS bv FROM a JOIN b ON a.k = b.id", lambda i: (i, (i % 5) * 100)),
    ("SELECT a.id AS aid, a.av + 1 AS bv FROM a", lambda i: (i, i * 10 + 1)),
], ids=["exchange", "linear"])
def test_a_view_created_under_traffic_holds_every_row(client, peer, body, row):
    """While `CREATE VIEW` runs, another connection inserts into its source and
    runs an exchange-bearing ad-hoc GROUP BY after each insert. Every read must
    be exact, and the finished view must hold the snapshot rows and every insert
    at weight 1, whichever side of the DDL each insert landed on. An exchange
    body and a partition-local one both run, because only the first shuffles its
    backfill."""
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, av BIGINT NOT NULL); "
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, bv BIGINT NOT NULL); "
        f"INSERT INTO b VALUES {values((j, j * 100) for j in range(5))}; "
        f"INSERT INTO a VALUES {values((i, i % 5, i * 10) for i in range(30))}")
    conn = peer()

    def step(k):
        i = 30 + k
        conn.execute_sql(f"INSERT INTO a VALUES ({i}, {i % 5}, {i * 10})")
        got = bag(rows(conn, "SELECT bv, COUNT(*) AS n FROM b GROUP BY bv"), "bv", "n")
        assert got == {(j * 100, 1): 1 for j in range(5)}, got

    n = across(lambda: client.execute_sql(f"CREATE VIEW v AS {body}"), step)
    assert bag(scanned(client, "v"), "aid", "bv") == {row(i): 1 for i in range(30 + n)}
