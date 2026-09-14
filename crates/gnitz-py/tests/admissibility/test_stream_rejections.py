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
def streamed(module_schema):
    """`(conn, schema)` holding a stream `s` and an ordinary table `t`; no test
    here changes either."""
    conn, sn = module_schema
    conn.execute_sql(
        "CREATE TABLE s (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, kind BIGINT NOT NULL, "
        "amount BIGINT NOT NULL) WITH (stream = true); "
        "CREATE TABLE t (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, kind BIGINT NOT NULL)",
        schema_name=sn,
    )
    return conn, sn


def test_no_binary_read_verb_serves_a_stream(streamed):
    """Each verb is wired to the guard on its own."""
    conn, sn = streamed
    tid, schema = conn.resolve_table(sn, "s")
    for verb in (
        lambda: conn.scan(tid),
        lambda: conn.seek(tid, 1),
        lambda: conn.seek_by_index(tid, schema, [1], [0]),
        lambda: conn.scan_many([tid]),
        lambda: conn.delta_bootstrap(tid, schema),
    ):
        with pytest.raises(gnitz.GnitzError, match="stream"):
            verb()


def test_a_transaction_writing_a_stream_is_refused_whole(streamed):
    """A transaction promises all-or-nothing recovery and stream data is never
    recovered, so a transaction touching a stream is refused — its writes to the
    ordinary table included, rather than lost after a partial commit."""
    conn, sn = streamed
    t_tid, t_schema = conn.resolve_table(sn, "t")
    s_tid, s_schema = conn.resolve_table(sn, "s")
    with pytest.raises(gnitz.GnitzError, match="stream"):
        with conn.transaction() as txn:
            txn.push(t_tid, gnitz.ZSetBatch(t_schema).extend([{"id": 1, "kind": 1}]))
            txn.push(s_tid, gnitz.ZSetBatch(s_schema).extend([{"id": 2, "kind": 2, "amount": 2}]))
    assert bag(conn.scan(t_tid)) == {}, "the table's write must be refused with it"
