"""What the catalog refuses: a schema the shared rule set rejects, a system-range
relation, and a base table under a live view.

The engine — not the SQL planner — has to hold these, because every path below
is a shipped client binding that validates nothing on the way in.
"""

import pytest
import gnitz


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


def test_drop_base_under_live_view_rejected_then_ordered_drop_succeeds(client, schema_name):
    """The dependency graph the engine derives from a view's circuit gates DROP
    TABLE; the rejected drop must leave the table intact."""
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    tid = client.create_table(schema_name, "t", cols)
    client.create_view(schema_name, "v", tid, gnitz.Schema(cols))

    with pytest.raises(gnitz.GnitzError):
        client.drop_table(schema_name, "t")
    assert client.resolve_table(schema_name, "t")[0] == tid

    client.drop_view(schema_name, "v")
    client.drop_table(schema_name, "t")
