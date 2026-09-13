"""What a stream refuses.

A stream holds no rows and nothing it ingests is recovered, so every statement
family that would read its store, mutate a stored row, hang a durable structure
off it, or promise its data back after a restart is refused — each naming the
stream, so the message says which rule fired rather than leaving a generic
parse failure to stand in.

The option-form cases are the other half of the same contract: a key the engine
never reads must not be accepted, since silently discarding it produces an
ordinary durable table with the opposite INSERT semantics.
"""

import pytest
import gnitz

_STREAM = (
    "CREATE TABLE s (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, kind BIGINT NOT NULL, "
    "amount BIGINT NOT NULL) WITH (stream = true)"
)


@pytest.fixture(scope="module")
def streamed(module_schema):
    """`(conn, schema)` holding a stream `s` and an ordinary table `t` — for the
    rejection cases only, so none of them can leave a mark on it."""
    conn, sn = module_schema
    conn.execute_sql(_STREAM, schema_name=sn)
    conn.execute_sql(
        "CREATE TABLE t (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, kind BIGINT NOT NULL)",
        schema_name=sn,
    )
    return conn, sn


# `(id, sql, message needle)`.
_REJECTED = [
    # A stored row to mutate is exactly what a stream does not have.
    ("update", "UPDATE s SET amount = 1 WHERE id = 1", "stream"),
    ("delete", "DELETE FROM s WHERE id = 1", "stream"),
    # Both ON CONFLICT actions resolve the incoming row against existing rows
    # with a client-side seek, and there is no store to seek.
    ("on-conflict-nothing", "INSERT INTO s VALUES (1, 2, 3) ON CONFLICT (id) DO NOTHING", "stream"),
    ("on-conflict-update", "INSERT INTO s VALUES (1, 2, 3) ON CONFLICT (id) DO UPDATE SET amount = 1", "stream"),
    # A durable structure hung off a relation whose rows are never recovered.
    ("create-index", "CREATE INDEX s_kind ON s (kind)", "stream"),
    ("create-unique-index", "CREATE UNIQUE INDEX s_kind_u ON s (kind)", "stream"),
    ("add-unique-constraint", "ALTER TABLE s ADD CONSTRAINT s_u UNIQUE (kind)", "stream"),
    # An inline UNIQUE is refused by the planner's stream rule, which names the
    # column; the engine's index owner check is the backstop, and rejects the
    # whole CREATE bundle.
    ("inline-unique", "CREATE TABLE su (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, k BIGINT NOT NULL UNIQUE) "
     "WITH (stream = true)", "stream"),
    ("add-column", "ALTER TABLE s ADD COLUMN extra BIGINT", "stream"),
    ("drop-column", "ALTER TABLE s DROP COLUMN kind", "stream"),
    ("rename-column", "ALTER TABLE s RENAME COLUMN kind TO k", "stream"),
    # A bounded view recomputes an evicted key from its source, which for a
    # stream is empty.
    ("bounded-view", "CREATE VIEW b WITH (capacity = '4 MB') AS SELECT id, amount FROM s", "stream"),
    # A FOREIGN KEY to a stream, and one from a stream.
    ("fk-to-stream", "CREATE TABLE child (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, "
     "ref BIGINT UNSIGNED NOT NULL REFERENCES s(id))", "stream"),
    ("fk-from-stream", "CREATE TABLE fs (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, "
     "ref BIGINT UNSIGNED NOT NULL REFERENCES t(id)) WITH (stream = true)", "stream"),
    # SERIAL draws from a durable sequence, making every push a catalog write.
    ("serial", "CREATE TABLE ss (id BIGSERIAL PRIMARY KEY, v BIGINT NOT NULL) WITH (stream = true)", "stream"),
    # A non-`WITH` option form carries keys nothing reads, so accepting it would
    # silently produce an ordinary durable table — and, on CREATE VIEW, an
    # unbounded view where a bounded one was asked for.
    ("options-table", "CREATE TABLE o (id BIGINT UNSIGNED NOT NULL PRIMARY KEY) OPTIONS(stream = true)", "OPTIONS"),
    ("options-view", "CREATE VIEW bv OPTIONS(capacity = '4 MB') AS SELECT id FROM t", "OPTIONS"),
    # A misspelled key is named back rather than discarded.
    ("misspelled-key", "CREATE TABLE o (id BIGINT UNSIGNED NOT NULL PRIMARY KEY) WITH (streem = true)", "streem"),
]


@pytest.mark.parametrize("sql,needle", [c[1:] for c in _REJECTED], ids=[c[0] for c in _REJECTED])
def test_a_stream_refuses(streamed, sql, needle):
    conn, sn = streamed
    with pytest.raises(gnitz.GnitzError, match=needle):
        conn.execute_sql(sql, schema_name=sn)


def test_a_stream_is_not_readable_by_sql_or_any_binary_read_verb(streamed):
    """Zero rows would make a live stream indistinguishable from an empty table,
    so every read path errors instead — each binary verb on its own."""
    conn, sn = streamed
    tid, schema = conn.resolve_table(sn, "s")

    for sql in (
        "SELECT * FROM s",
        "SELECT kind FROM s WHERE amount > 0",
        "SELECT COUNT(*) FROM s",
        "WITH c AS (SELECT id FROM s) SELECT id FROM c",
    ):
        with pytest.raises(gnitz.GnitzError, match="stream"):
            conn.execute_sql(sql, schema_name=sn)

    for verb in (
        lambda: list(conn.scan(tid)),
        lambda: list(conn.seek(tid, 1)),
        lambda: list(conn.seek_by_index(tid, [1], [0])),
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
    tb = gnitz.ZSetBatch(t_schema)
    tb.append(id=1, kind=1, _weight=1)
    sb = gnitz.ZSetBatch(s_schema)
    sb.append(id=2, kind=2, amount=2, _weight=1)

    with pytest.raises(gnitz.GnitzError, match="stream"):
        with conn.transaction() as txn:
            txn.push(t_tid, tb)
            txn.push(s_tid, sb)

    assert [r._weight for r in conn.scan(t_tid)] == [], "the table's write must be refused with it"
