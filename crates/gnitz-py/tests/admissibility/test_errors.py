"""What the catalog and the wire refuse from a client binding: a pushed schema
that disagrees with its target, a write aimed at a view, a system relation, a
base table under a live view, a reserved name, and a relation that is not
there.

The engine — not the SQL planner — has to hold these, because every path below
is a shipped client binding that validates nothing on the way in.
"""

from decimal import Decimal

import pytest
import gnitz
from _read import bag
from _schemas import KV
from _uid import uid

_NOT_WRITABLE = "is not writable"


def test_a_push_whose_pk_type_disagrees_is_rejected(client):
    """A push whose schema lays out a U64 PK as I64 is refused, not reinterpreted
    at rest; which mismatches the gate distinguishes is pinned in Rust."""
    tid = client.create_table("t", KV)
    signed_pk = gnitz.Schema([gnitz.ColumnDef("pk", gnitz.TypeCode.I64), KV.columns[1]], [0])
    with pytest.raises(gnitz.GnitzRefusedError, match="Schema mismatch"):
        client.push(tid, gnitz.ZSetBatch(signed_pk).extend([{"pk": 1, "val": 42}]))
    assert bag(client.scan(tid, KV)) == {}


def test_a_push_whose_decimal_scale_disagrees_is_rejected(client):
    """A DECIMAL's stored integer means a different number at every scale, so a
    push encoded at scale 2 onto a scale-4 column would read `1.50` back as
    `0.0150`. The gate compares the scale; the same push at scale 4 lands."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, d DECIMAL(10,4) NOT NULL)")
    tid, t_schema = client.resolve_table("t")

    def at_scale(scale):
        schema = gnitz.Schema([gnitz.ColumnDef("pk", gnitz.TypeCode.I64),
                               gnitz.ColumnDef("d", gnitz.TypeCode.DECIMAL, scale=scale)], [0])
        return gnitz.ZSetBatch(schema).extend([{"pk": 1, "d": Decimal("1.50")}])

    with pytest.raises(gnitz.GnitzRefusedError, match="Schema mismatch"):
        client.push(tid, at_scale(2))
    assert bag(client.scan(tid, t_schema)) == {}

    client.push(tid, at_scale(4))
    assert bag(client.scan(tid, t_schema), "pk", "d") == {(1, Decimal("1.5000")): 1}


def test_writes_to_a_view_are_rejected_and_change_nothing(client):
    """A view's tid lives in the same id space as a base table's, so a raw push
    or delete addressed to one would commit rows its circuit never produced. SQL
    refuses a view target in the binder; the raw API reaches only the server's
    guard. It is one guard on
    every write, the empty-batch arm included — so a client bug producing an
    empty batch fails instead of being masked by the no-op ACK a base table
    still gives."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
        "CREATE VIEW v AS SELECT * FROM t WHERE val > 50; "
        "INSERT INTO t VALUES (1, 100), (2, 10)")
    tid, t_schema = client.resolve_table("t")
    vid, v_schema = client.resolve_table("v")
    batch = gnitz.ZSetBatch(v_schema).extend([{"pk": 999, "val": 999}])

    for write in (lambda: client.push(vid, batch),
                  lambda: client.delete(vid, v_schema, [1]),
                  lambda: client.push(vid, gnitz.ZSetBatch(v_schema))):
        with pytest.raises(gnitz.GnitzRefusedError, match=_NOT_WRITABLE):
            write()
    assert bag(client.scan(vid, v_schema)) == {(1, 100): 1}

    # The same empty batch against the base table is the ordinary no-op ACK.
    assert client.push(tid, gnitz.ZSetBatch(t_schema)) == 0


def test_an_absent_relation_is_a_miss_not_a_writability_failure(client):
    """A genuinely absent tid reports 'not found' rather than 'not writable',
    and a name the client itself cannot resolve raises a catchable class, so a
    caller branches on absence without matching prose."""
    batch = gnitz.ZSetBatch(gnitz.Schema([KV.columns[0]], [0])).extend([{"pk": 1}])
    with pytest.raises(gnitz.GnitzNotFoundError, match="not found"):
        client.push(99999999, batch)
    with pytest.raises(gnitz.GnitzNotFoundError):
        client.scan(99999999, KV)

    with pytest.raises(gnitz.GnitzNotFoundError, match=f"{client.schema}.nope"):
        client.drop_table("nope")

    # SQL reports the same class for a read and a write.
    for sql in ("SELECT * FROM nope", "INSERT INTO nope VALUES (1)"):
        with pytest.raises(gnitz.GnitzNotFoundError):
            client.execute_sql(sql)

    with pytest.raises(gnitz.GnitzRefusedError):
        client.drop_schema("nonexistent_schema_xyz")
    client.schema = "nonexistent_schema_xyz"
    with pytest.raises(gnitz.GnitzRefusedError):
        client.create_table("t", KV)


def test_create_schema_rejects_a_leading_underscore(client):
    """`_` is the system prefix; `create_schema` is the one naming surface with
    no SQL planner in front of the engine's rule."""
    with pytest.raises(gnitz.GnitzRefusedError, match="cannot start with '_'"):
        client.create_schema("_reserved")


def test_system_relations_survive_a_rejected_drop(client):
    """Neither a system table nor the system schema may be dropped, and the
    rejected cascade must not have retracted a single member on its way to the
    row it was rejected on."""
    def sys_ids():
        return {r["table_id"] for r in client.scan(gnitz.TABLE_TAB, gnitz.sys_schema(gnitz.TABLE_TAB))
                if r["table_id"] < gnitz.FIRST_USER_TABLE_ID}

    before = sys_ids()
    assert before, "bootstrap registered no system relations"

    with pytest.raises(gnitz.GnitzRefusedError):
        client.drop_schema("_system")
    assert sys_ids() == before

    client.schema = "_system"
    with pytest.raises(gnitz.GnitzRefusedError):
        client.drop_table("_tables")
    assert sys_ids() == before


def test_a_live_view_blocks_dropping_its_base_even_from_another_schema(client, server):
    """The dependency graph the engine derives from a view's circuit gates DROP
    TABLE and the schema cascade that would reach the table, whichever schema
    the view lives in. The rejected drops leave the table intact, and dropping
    the view lifts both."""
    with gnitz.connect(server, schema="s" + uid()) as other:
        other.create_schema(other.schema)
        tid = other.create_table("t", KV)
        client.create_view("v", "t", source_schema=other.schema)

        for drop in (lambda: other.drop_table("t"), lambda: other.drop_schema(other.schema)):
            with pytest.raises(gnitz.GnitzRefusedError, match="dependen"):
                drop()
        assert other.resolve_table("t")[0] == tid

        client.drop_view("v")
        other.drop_schema(other.schema)


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
    with pytest.raises(gnitz.GnitzRefusedError, match="dependen"):
        client.execute_sql(f"DROP TABLE {base}")
    client.execute_sql(f"DROP VIEW v; DROP TABLE {base}")
