"""A DDL landing while other connections write.

What must hold is what those connections were told: every ACKed write reads back
at the values it supplied, and a write the DDL overtook is refused rather than
committed against the new layout.
"""

import gnitz
import pytest
from _read import bag, scanned
from _schemas import KV, kv
from _serverproc import across
from _sql import values

# A write laid out for `t`'s pre-ALTER width.
_OLD_WIDTH = {
    "sql": lambda conn, tid, k: conn.execute_sql(f"INSERT INTO t VALUES ({k}, {k})"),
    "push": lambda conn, tid, k: conn.push(tid, kv((k, k))),
}


@pytest.mark.parametrize("via", _OLD_WIDTH)
def test_every_write_acked_across_an_add_column_reads_back(client, peer, via):
    """A writer laying out the pre-ALTER width runs across `ALTER TABLE ADD
    COLUMN`, falling back to the post-ALTER INSERT once refused. A write is judged
    against the catalog it commits under, so every ACK reads back at the values
    it supplied — an old-width one with the new column NULL — and a refusal
    wrote nothing; once the ALTER has returned the old width is refused for
    good."""
    tid = client.create_table("t", KV)
    conn = peer()
    acked = {}  # pk -> the `c` its write supplied

    def write(k):
        try:
            _OLD_WIDTH[via](conn, tid, k)
            acked[k] = None
        except gnitz.GnitzRefusedError:
            conn.execute_sql(f"INSERT INTO t VALUES ({k}, {k}, {k})")
            acked[k] = k

    n = across(lambda: client.execute_sql("ALTER TABLE t ADD COLUMN c BIGINT"), write)
    with pytest.raises(gnitz.GnitzRefusedError):
        _OLD_WIDTH[via](conn, tid, n)
    assert bag(scanned(client, "t")) == {(k, k, c): 1 for k, c in acked.items()}


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


def test_a_bundle_into_a_table_dropped_before_commit_is_refused_whole(client, peer):
    """The commit is validated against the catalog it reaches, not the one the
    transaction began under: the refusal names the dropped relation, and the
    bundle's write to a live table is discarded with it."""
    keep, gone = client.create_table("keep", KV), client.create_table("gone", KV)
    with pytest.raises(gnitz.GnitzNotFoundError, match=rf"\b{gone}\b"):
        with client.transaction() as txn:
            txn.push(keep, kv((1, 1)))
            txn.push(gone, kv((1, 1)))
            peer().execute_sql("DROP TABLE gone")
    assert bag(scanned(client, "keep")) == {}
