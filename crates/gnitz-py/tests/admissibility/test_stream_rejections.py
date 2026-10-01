"""What a stream refuses on the client bindings.

A stream holds no rows and nothing it ingests is recovered, so no binary read
verb serves one — zero rows would make it indistinguishable from an empty table
— and no transaction may write one. The SQL statements a stream refuses are
pinned in the planner's own tests.
"""

import pytest
import gnitz
from _read import bag


@pytest.fixture(scope="module")
def conn(module_client):
    """A stream `s` and an ordinary table `t`; no test here changes either."""
    module_client.execute_sql(
        "CREATE TABLE s (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, kind BIGINT NOT NULL, "
        "amount BIGINT NOT NULL) WITH (stream = true); "
        "CREATE TABLE t (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, kind BIGINT NOT NULL)",
    )
    return module_client


def test_no_binary_read_verb_serves_a_stream(conn):
    """Each verb is wired to the guard on its own."""
    tid, schema = conn.resolve_table("s")
    for verb in (
        lambda: conn.scan(tid, schema),
        lambda: conn.seek(tid, schema, 1),
        lambda: conn.seek_by_index(tid, schema, [1], [0]),
        lambda: conn.scan_many([(tid, schema)]),
        lambda: conn.delta_bootstrap(tid, schema),
    ):
        with pytest.raises(gnitz.GnitzRefusedError, match="stream"):
            verb()


def test_a_transaction_writing_a_stream_is_refused_whole(conn):
    """A transaction promises all-or-nothing recovery and stream data is never
    recovered, so a transaction touching a stream is refused — its writes to the
    ordinary table included, rather than lost after a partial commit."""
    t_tid, t_schema = conn.resolve_table("t")
    s_tid, s_schema = conn.resolve_table("s")
    with pytest.raises(gnitz.GnitzRefusedError, match="stream"):
        with conn.transaction() as txn:
            txn.push(t_tid, gnitz.ZSetBatch(t_schema).extend([{"id": 1, "kind": 1}]))
            txn.push(s_tid, gnitz.ZSetBatch(s_schema).extend([{"id": 2, "kind": 2, "amount": 2}]))
    assert bag(conn.scan(t_tid, t_schema)) == {}, "the table's write must be refused with it"
