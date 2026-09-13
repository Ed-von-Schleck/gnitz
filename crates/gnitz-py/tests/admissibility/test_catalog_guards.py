"""What the catalog refuses: a schema the shared rule set rejects, a system-range
relation, and a base table under a live view.

The engine — not the SQL planner — has to hold these, because every path below
is a shipped client binding that validates nothing on the way in.
"""

import pytest
import gnitz
from _uid import uid


def _bad_schemas():
    """Column lists the shared rule set rejects, and why."""
    too_many = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True)]
    too_many += [gnitz.ColumnDef(f"c{i}", gnitz.TypeCode.I64) for i in range(65)]
    string_pk = [
        gnitz.ColumnDef("name", gnitz.TypeCode.STRING, primary_key=True),
        gnitz.ColumnDef("val", gnitz.TypeCode.I64),
    ]
    return [(too_many, "66-columns"), (string_pk, "string-pk")]


@pytest.mark.parametrize("cols", [c for c, _ in _bad_schemas()],
                         ids=[i for _, i in _bad_schemas()])
def test_create_table_applies_the_shared_schema_rules(client, cols):
    """`create_table` resolves its argument to a Schema, so the rule set every
    other schema surface applies rejects it client-side — before any id is
    allocated, which is why no schema need exist to be refused."""
    with pytest.raises(ValueError):
        client.create_table("no_such_schema", "t", cols)


def test_create_table_nonexistent_schema(client):
    """A well-formed schema in a schema that does not exist is the engine's
    rejection, not the client's."""
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    with pytest.raises(gnitz.GnitzError):
        client.create_table("nosuch_schema_xyz", "t", cols)


def test_system_relations_survive_a_rejected_drop(client):
    """Neither a system table nor the system schema may be dropped, and the
    rejected cascade must not have retracted a single member on its way to the
    row it was rejected on."""
    def sys_ids():
        return {r["table_id"] for r in client.scan(gnitz.TABLE_TAB)
                if r["table_id"] < gnitz.FIRST_USER_TABLE_ID}

    before = sys_ids()
    assert before, "bootstrap registered no system relations"

    with pytest.raises(gnitz.GnitzError):
        client.drop_table("_system", "_tables")
    assert sys_ids() == before

    with pytest.raises(gnitz.GnitzError):
        client.drop_schema("_system")
    assert sys_ids() == before


def test_a_live_view_blocks_dropping_its_base_even_from_another_schema(client, schema_name):
    """The dependency graph the engine derives from a view's circuit gates DROP
    TABLE and the schema cascade that would reach the table, whichever schema
    the view lives in. The rejected drops leave the table intact, and dropping
    the view lifts both."""
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    other = "s" + uid()
    client.create_schema(other)
    tid = client.create_table(other, "t", cols)
    client.create_view(schema_name, "v", tid, gnitz.Schema(cols))

    with pytest.raises(gnitz.GnitzError):
        client.drop_table(other, "t")
    with pytest.raises(gnitz.GnitzError):
        client.drop_schema(other)
    assert client.resolve_table(other, "t")[0] == tid

    client.drop_view(schema_name, "v")
    client.drop_schema(other)


@pytest.mark.parametrize("body,base", [
    ("SELECT x.a AS xa, y.b AS yb FROM t x JOIN t y ON x.b = y.id", "t"),
    ("SELECT t.a AS ta, w.b AS wb FROM t JOIN u ON t.b = u.id JOIN w ON u.b = w.id", "u"),
    ("WITH agg AS (SELECT a, SUM(b) AS s FROM t GROUP BY a) "
     "SELECT u.b AS ub, agg.s AS s FROM agg JOIN u ON agg.a = u.id", "t"),
], ids=["self-join-pass-through", "join-segment", "reduce-segment"])
def test_a_generated_relation_holds_its_base_until_the_view_drops(client, schema_name, body, base):
    """Whatever the lowering generates — a collision pass-through, a join
    segment, a reduce segment — is a node of the dependency graph like any view:
    it holds a reference on its source, so the base refuses to drop under the
    live view, and `DROP VIEW` retires the whole bundle, so the base is free
    straight after. An orphaned generated relation would keep refusing. In the
    two segment cases `base` is reached only through the generated relation."""
    client.execute_sql(
        "; ".join(f"CREATE TABLE {n} (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, "
                  "b BIGINT NOT NULL)" for n in ("t", "u", "w"))
        + f"; CREATE VIEW v AS {body}", schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError, match="dependen"):
        client.execute_sql(f"DROP TABLE {base}", schema_name=schema_name)
    client.execute_sql(f"DROP VIEW v; DROP TABLE {base}", schema_name=schema_name)
