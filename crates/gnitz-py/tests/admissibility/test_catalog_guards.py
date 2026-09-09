"""What the catalog refuses: a schema the shared rule set rejects, a system-range
relation, and a base table under a live view.

The engine — not the SQL planner — has to hold these, because every path below
is a shipped client binding that validates nothing on the way in.
"""

import pytest
import gnitz
from _uid import uid as _uid


class TestCreateTableRejections:

    def test_create_table_too_many_columns(self, client):
        """66 columns exceeds MAX_COLUMNS (65). `create_table` resolves its
        argument to a Schema, so the shared rule set rejects it client-side —
        the same ValueError `Schema(cols)` raises for the same list."""
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True)]
        for i in range(65):
            cols.append(gnitz.ColumnDef(f"c{i}", gnitz.TypeCode.I64))
        try:
            with pytest.raises(ValueError):
                client.create_table(sn, "t" + _uid(), cols)
        finally:
            client.drop_schema(sn)

    def test_create_table_string_pk_rejected(self, client):
        """STRING is not a valid primary key type — rejected client-side by the
        same schema validation every other schema surface applies."""
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [
            gnitz.ColumnDef("name", gnitz.TypeCode.STRING, primary_key=True),
            gnitz.ColumnDef("val",  gnitz.TypeCode.I64),
        ]
        try:
            with pytest.raises(ValueError):
                client.create_table(sn, "t" + _uid(), cols)
        finally:
            client.drop_schema(sn)

    def test_create_table_nonexistent_schema(self, client):
        """Creating a table in a nonexistent schema raises GnitzError."""
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        with pytest.raises(gnitz.GnitzError):
            client.create_table("nosuch_" + _uid(), "t", cols)



# ===========================================================================

class TestSystemRelationGuards:
    """The engine — not the SQL planner — must reject a mutation of a
    system-range relation. Both paths below are shipped client bindings that
    validate nothing on the way in."""

    def _sys_relation_ids(self, client):
        """Every system-range relation bootstrap registered under `_system`."""
        return {r["table_id"] for r in client.scan(gnitz.TABLE_TAB)
                if r["table_id"] < gnitz.FIRST_USER_TABLE_ID}

    def test_drop_system_table_rejected(self, client):
        before = self._sys_relation_ids(client)
        assert before, "bootstrap registered no system relations"

        with pytest.raises(gnitz.GnitzError):
            client.drop_table("_system", "_tables")

        assert self._sys_relation_ids(client) == before

    def test_drop_system_schema_rejected(self, client):
        before = self._sys_relation_ids(client)

        with pytest.raises(gnitz.GnitzError):
            client.drop_schema("_system")

        # The cascade must not have retracted a single member on its way to the
        # SCHEMA_TAB row it would have been rejected on.
        assert self._sys_relation_ids(client) == before


class TestViewDependencyGraph:
    """The dependency graph the engine derives from a view's circuit: it gates
    DROP TABLE and it drives which views tick on a source delta."""

    def test_drop_base_under_live_view_rejected_then_ordered_drop_succeeds(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        tid = client.create_table(sn, "t", cols)
        client.create_view(sn, "v", tid, gnitz.Schema(cols))

        with pytest.raises(gnitz.GnitzError):
            client.drop_table(sn, "t")
        # The rejected drop left the table intact.
        assert client.resolve_table(sn, "t")[0] == tid

        client.drop_view(sn, "v")
        client.drop_table(sn, "t")
        client.drop_schema(sn)
