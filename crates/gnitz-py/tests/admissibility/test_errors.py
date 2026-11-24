"""What the catalog and the wire refuse from a client binding: a pushed schema
that disagrees with its target, a write aimed at a view or a system table, a
base table under a live view, and a relation that is not there.

The engine — not the SQL planner — has to hold the writes, because a binary
push or delete is a shipped client binding no planner stands in front of.
"""

import re
from decimal import Decimal

import pytest

import gnitz
from _read import bag, scanned
from _schemas import KV, kv


def test_a_push_whose_schema_disagrees_is_refused(client):
    """A push laying out a U64 PK as I64, or a DECIMAL at another scale, is
    refused rather than reinterpreted at rest: a scale-2 `1.50` on a scale-4
    column would read back as `0.0150`. Which mismatches the gate distinguishes
    is pinned in Rust; the same row in the table's own schema lands."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT UNSIGNED NOT NULL PRIMARY KEY, d DECIMAL(10,4) NOT NULL)")
    tid, schema = client.resolve_table("t")
    pk, d = schema.columns
    row = {"pk": 1, "d": Decimal("1.50")}
    for cols in ([gnitz.ColumnDef("pk", gnitz.TypeCode.I64), d],
                 [pk, gnitz.ColumnDef("d", gnitz.TypeCode.DECIMAL, scale=2)]):
        with pytest.raises(gnitz.GnitzRefusedError, match="Schema mismatch"):
            client.push(tid, gnitz.ZSetBatch(gnitz.Schema(cols, [0])).extend([row]))
    assert bag(client.scan(tid, schema)) == {}

    client.push(tid, gnitz.ZSetBatch(schema).extend([row]))
    assert bag(client.scan(tid, schema)) == {(1, Decimal("1.5000")): 1}


@pytest.mark.parametrize("target,noun", [("v", "view"), ("_system.tables", "system table")],
                         ids=["view", "system table"])
def test_a_write_to_no_base_table_is_refused_and_changes_nothing(client, target, noun):
    """A view's tid lives in the same id space as a base table's, and a system
    family's rows are written by DDL alone, so a raw push, delete or transaction
    addressed to either would commit rows nothing derived. SQL refuses both
    targets while planning; the raw API reaches only the server's guard. It is one guard on
    every write, the empty-batch arm included — so a client bug producing an
    empty batch fails instead of being masked by the no-op ACK a base table
    still gives. The writes re-push and delete a row the target holds."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
        "CREATE VIEW v AS SELECT * FROM t WHERE val > 50; "
        "INSERT INTO t VALUES (1, 100), (2, 10)")
    tid, schema = client.resolve_table(target)
    held = scanned(client, target)
    before = bag(held)
    row = held[0]._asdict()
    key = row[schema.columns[schema.pk_indices[0]].name]

    def in_a_transaction():
        with client.transaction() as txn:
            txn.push(tid, gnitz.ZSetBatch(schema).extend([row]))

    name = target if "." in target else f"{client.schema}.{target}"
    for write in (lambda: client.push(tid, gnitz.ZSetBatch(schema).extend([row])),
                  lambda: client.delete(tid, schema, [key]),
                  lambda: client.push(tid, gnitz.ZSetBatch(schema)),
                  in_a_transaction):
        with pytest.raises(gnitz.GnitzRefusedError, match=f"{noun} {re.escape(name)} is not writable"):
            write()
    assert bag(scanned(client, target)) == before

    t_tid, t_schema = client.resolve_table("t")
    assert client.push(t_tid, gnitz.ZSetBatch(t_schema)) == 0


def test_an_absent_relation_is_a_miss_not_a_writability_failure(client):
    """A genuinely absent tid reports 'not found' rather than 'not writable',
    and an absent schema raises the same class, so a caller branches on absence
    without matching prose. Every SQL verb's miss is pinned in Rust."""
    with pytest.raises(gnitz.GnitzNotFoundError, match="not found"):
        client.push(99999999, kv((1, 0)))

    absent = "schema 'nonexistent_schema_xyz' not found"
    with pytest.raises(gnitz.GnitzNotFoundError, match=absent):
        client.execute_sql("DROP SCHEMA nonexistent_schema_xyz")
    client.schema = "nonexistent_schema_xyz"
    with pytest.raises(gnitz.GnitzNotFoundError, match=absent):
        client.create_table("t", KV)


def test_a_live_view_blocks_dropping_its_base_even_from_another_schema(client, other_client):
    """The dependency graph the engine derives from a view's circuit gates DROP
    TABLE and the schema cascade that would reach the table, whichever schema
    the view lives in. The rejected drops leave the table and the view it feeds
    intact, and dropping the view lifts the gate."""
    tid = other_client.create_table("t", KV)
    client.execute_sql(f"CREATE VIEW v AS SELECT * FROM {other_client.schema}.t")
    other_client.push(tid, kv((1, 10)))

    for drop in ("DROP TABLE t", f"DROP SCHEMA {other_client.schema} CASCADE"):
        with pytest.raises(gnitz.GnitzRefusedError, match=rf"dependent views \({client.schema}\.v\)"):
            other_client.execute_sql(drop)
    other_client.push(tid, kv((2, 20)))
    assert bag(scanned(client, "v")) == {(1, 10): 1, (2, 20): 1}

    client.execute_sql("DROP VIEW v")
    other_client.execute_sql("DROP TABLE t")


@pytest.mark.parametrize("tables,body,base", [
    ("t", "SELECT x.a AS xa, y.b AS yb FROM t x JOIN t y ON x.b = y.id", "t"),
    ("tuw", "SELECT t.a AS ta, w.b AS wb FROM t JOIN u ON t.b = u.id JOIN w ON u.b = w.id", "u"),
    ("tu", "WITH agg AS (SELECT a, SUM(b) AS s FROM t GROUP BY a) "
           "SELECT u.b AS ub, agg.s AS s FROM agg JOIN u ON agg.a = u.id", "t"),
], ids=["self-join-pass-through", "join-segment", "reduce-segment"])
def test_a_generated_relation_holds_its_base_until_the_view_drops(client, tables, body, base):
    """Whatever the lowering generates — a collision pass-through, a join
    segment, a reduce segment — is a node of the dependency graph like any view:
    it holds a reference on its source, so the base refuses to drop under the
    live view, and `DROP VIEW` retires the whole bundle, so the base is free
    straight after. An orphaned generated relation would keep refusing. In the
    two segment cases `base` is reached only through the generated relation."""
    client.execute_sql(
        "; ".join(f"CREATE TABLE {n} (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, "
                  "b BIGINT NOT NULL)" for n in tables)
        + f"; CREATE VIEW v AS {body}")
    # A generated relation surfaces as the user view that owns it.
    with pytest.raises(gnitz.GnitzRefusedError, match=rf"dependent views \({client.schema}\.v\)"):
        client.execute_sql(f"DROP TABLE {base}")
    client.execute_sql(f"DROP VIEW v; DROP TABLE {base}")
