"""E2E for relation resolution: one master-served descriptor per relation per
statement, addressed by qualified name.

The resolve contract itself — round-trip counts, absent verdicts, kind, index
list exactness, the cross-connection rename/drop/recreate shapes — is pinned by
`crates/gnitz-sql/tests/relation_resolve.rs`, which drives the same client
directly and can assert on request counts. What lives here is what only a
multi-worker end-to-end run can show: that a freshly resolved descriptor is
still the one the *planner* and the *engine* act on, with the fan-out in play.

Run with GNITZ_WORKERS=4 (a resolve is master-local, but every read and push it
feeds fans out):
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_relation_resolve.py -v
"""
import random

import gnitz
import pytest


def _uid():
    return str(random.randint(100000, 999999))


def _rows(conn, sn, q):
    res = conn.execute_sql(q, schema_name=sn)[0]
    assert res["type"] == "Rows", f"expected Rows, got {res['type']}: {res}"
    return list(res["rows"])


def _tuples(conn, sn, q, *cols):
    """`_rows` projected to a sorted list of plain tuples, so a test compares
    whole result sets without depending on `Row`'s repr or on row order."""
    return sorted(tuple(getattr(r, c) for c in cols) for r in _rows(conn, sn, q))


def _drop_schema(conn, sn):
    try:
        conn.execute_sql(f"DROP SCHEMA {sn} CASCADE", schema_name=sn)
    except Exception:
        pass


@pytest.fixture
def two_clients(_srv):
    """`(a, b, schema_name)` — two independent connections to one server, with a
    fresh schema. B never runs any of A's DDL: it is the idle observer."""
    sn = "rr" + _uid()
    with gnitz.connect(_srv.target) as a, gnitz.connect(_srv.target) as b:
        a.create_schema(sn)
        try:
            yield a, b, sn
        finally:
            _drop_schema(a, sn)


def test_drop_index_is_seen_by_the_next_statement(two_clients):
    """A drops the index B's last plan used; B's very next statement must be
    planned against the *absence* of that index.

    A UUID equality makes the two outcomes distinguishable, because it has no
    index-free plan at all: the predicate VM has no register for a 128-bit
    column, so the WHERE only becomes servable when an index bound can consume
    it byte-exactly (the `exact` arm).

      * fresh (now empty) index list → the planner's own
        "128-bit columns cannot be used in expressions";
      * stale index list → an `exact` bound against a dropped index, which the
        engine answers with "No index on cols … for table …".

    Both are errors, so asserting on *which* error is what actually pins
    freshness — a bare `raises` would pass either way.
    """
    a, b, sn = two_clients
    a.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, wide UUID NOT NULL)",
        schema_name=sn,
    )
    u1 = "11111111-1111-1111-1111-111111111111"
    u2 = "22222222-2222-2222-2222-222222222222"
    a.execute_sql(f"INSERT INTO t VALUES (1, '{u1}'), (2, '{u2}')", schema_name=sn)
    a.execute_sql("CREATE INDEX ix_wide ON t(wide)", schema_name=sn)

    # B plans against the index.
    assert _tuples(b, sn, f"SELECT id FROM t WHERE wide = '{u1}'", "id") == [(1,)]

    a.execute_sql("DROP INDEX ix_wide", schema_name=sn)

    with pytest.raises(Exception) as ei:
        _rows(b, sn, f"SELECT id FROM t WHERE wide = '{u1}'")
    msg = str(ei.value)
    assert "128-bit columns cannot be used in expressions" in msg, (
        f"B planned against a stale index list: {msg}"
    )
    assert "No index on cols" not in msg

    # The relation itself is unaffected — a read that needs no index still works.
    assert _tuples(b, sn, "SELECT id FROM t", "id") == [(1,), (2,)]

    # And recreating the index makes the bounded read servable again, at once.
    a.execute_sql("CREATE INDEX ix_wide ON t(wide)", schema_name=sn)
    assert _tuples(b, sn, f"SELECT id FROM t WHERE wide = '{u2}'", "id") == [(2,)]


def test_dropped_column_is_invisible_to_an_idle_client(two_clients):
    """A logically drops a column; B's next wildcard SELECT stops projecting it,
    and B's INSERT still works against the remaining columns."""
    a, b, sn = two_clients
    a.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, gone BIGINT NOT NULL, keep BIGINT NOT NULL)",
        schema_name=sn,
    )
    a.execute_sql("INSERT INTO t VALUES (1, 2, 3)", schema_name=sn)
    assert _tuples(b, sn, "SELECT * FROM t", "id", "gone", "keep") == [(1, 2, 3)]

    a.execute_sql("ALTER TABLE t DROP COLUMN gone", schema_name=sn)

    assert _tuples(b, sn, "SELECT * FROM t", "id", "keep") == [(1, 3)]
    b.execute_sql("INSERT INTO t (id, keep) VALUES (2, 30)", schema_name=sn)
    assert _tuples(a, sn, "SELECT * FROM t", "id", "keep") == [(1, 3), (2, 30)]


def test_serial_and_fk_survive_the_resolve(client):
    """`is_serial` rides the schema block and the FK target rides the descriptor
    blob, so an INSERT into a SERIAL table still auto-assigns and an FK is still
    enforced when the schema came from a resolve rather than a COL_TAB scan."""
    sn = "rr" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE p (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn
        )
        client.execute_sql("INSERT INTO p VALUES (7, 70)", schema_name=sn)
        client.execute_sql(
            "CREATE TABLE c (id SERIAL PRIMARY KEY, parent BIGINT NOT NULL REFERENCES p(id))",
            schema_name=sn,
        )

        # SERIAL: the PK is omitted and auto-assigned.
        client.execute_sql("INSERT INTO c (parent) VALUES (7)", schema_name=sn)
        assert _tuples(client, sn, "SELECT parent FROM c", "parent") == [(7,)]

        # FK: a missing parent is still rejected.
        with pytest.raises(Exception):
            client.execute_sql("INSERT INTO c (parent) VALUES (999)", schema_name=sn)

        # And the FK column cannot be dropped while it carries the constraint —
        # the guard reads `fk_table_id` off the resolved schema.
        with pytest.raises(Exception):
            client.execute_sql("ALTER TABLE c DROP COLUMN parent", schema_name=sn)
    finally:
        _drop_schema(client, sn)


def test_view_resolves_and_stays_read_only(two_clients):
    """A view resolves through the same by-name path as a table, reports its kind,
    and is still rejected as a write target."""
    a, b, sn = two_clients
    a.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
    a.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20)", schema_name=sn)
    a.execute_sql("CREATE VIEW vw AS SELECT id, v FROM t WHERE v > 15", schema_name=sn)

    assert _tuples(b, sn, "SELECT * FROM vw", "id", "v") == [(2, 20)]
    with pytest.raises(Exception, match="is a view"):
        b.execute_sql("INSERT INTO vw VALUES (3, 30)", schema_name=sn)

    # A's new row flows into the view B reads next.
    a.execute_sql("INSERT INTO t VALUES (3, 30)", schema_name=sn)
    assert _tuples(b, sn, "SELECT * FROM vw", "id", "v") == [(2, 20), (3, 30)]
