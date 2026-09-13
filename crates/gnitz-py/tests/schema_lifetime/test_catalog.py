"""The catalog's object lifecycle: a table's declared shape coming back
unchanged, a dropped relation's id retiring while its siblings keep serving,
and a dropped index vanishing from the next statement another connection plans.

The verdicts the catalog refuses live in `admissibility/test_errors.py`.
"""

import gnitz
import pytest
from _read import access, bag, rows

_KV = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
       gnitz.ColumnDef("val", gnitz.TypeCode.I64)]


def _push(client, tid, *pairs):
    batch = gnitz.ZSetBatch(gnitz.Schema(_KV))
    for pk, val in pairs:
        batch.append(pk=pk, val=val)
    client.push(tid, batch)


def test_create_table_hands_back_the_declared_columns_up_to_the_cap(client, schema_name):
    """`create_table` does not silently rename, retype or re-key a column: each
    comes back as declared, with the PK at the index the caller put it rather
    than at column 0, at the full `MAX_COLUMNS` width. The cap is located rather
    than named, so moving it moves this test with it.

    The per-type value round trip is `value_domain/test_value_round_trip.py`'s.
    """
    cols = [gnitz.ColumnDef("val", gnitz.TypeCode.I64),
            gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True)]
    cols += [gnitz.ColumnDef(f"c_{tc.name.lower()}", tc, is_nullable=True)
             for tc in (gnitz.TypeCode.U8, gnitz.TypeCode.I8, gnitz.TypeCode.U16,
                        gnitz.TypeCode.I16, gnitz.TypeCode.U32, gnitz.TypeCode.I32,
                        gnitz.TypeCode.F32, gnitz.TypeCode.U64, gnitz.TypeCode.F64,
                        gnitz.TypeCode.STRING)]
    cols += [gnitz.ColumnDef(f"pad{i}", gnitz.TypeCode.I64, is_nullable=True)
             for i in range(gnitz.MAX_COLUMNS - len(cols))]
    tid = client.create_table(schema_name, "t", cols)

    resolved, schema = client.resolve_table(schema_name, "t")
    assert resolved == tid
    assert [(c.name, c.type_code) for c in schema.columns] == [(c.name, c.type_code) for c in cols]
    assert schema.pk_indices == [1]


def test_a_dropped_relation_stops_answering_while_its_siblings_serve(client, schema_name):
    """Two views over one source both receive every push; dropping one retires
    exactly that id — it stops answering scans — and leaves the other
    maintained. A dropped populated table's id retires the same way."""
    tid = client.create_table(schema_name, "src", _KV)
    schema = gnitz.Schema(_KV)
    va = client.create_view(schema_name, "va", tid, schema)
    vb = client.create_view(schema_name, "vb", tid, schema)

    _push(client, tid, (1, 10))
    for vid in (va, vb):
        assert bag(client.scan(vid), "pk", "val") == {(1, 10): 1}

    client.drop_view(schema_name, "va")
    with pytest.raises(gnitz.GnitzError):
        client.scan(va)
    _push(client, tid, (2, 20))
    assert bag(client.scan(vb), "pk", "val") == {(1, 10): 1, (2, 20): 1}

    client.drop_view(schema_name, "vb")
    client.drop_table(schema_name, "src")
    with pytest.raises(gnitz.GnitzError):
        client.scan(tid)


def test_drop_index_is_seen_by_the_next_statement_on_another_connection(
        client, schema_name, server):
    """A drops the index B's last plan used; B's very next statement is planned
    against its absence, and against the recreated index at once.

    Asserted through EXPLAIN's access line: a plan against a stale index list
    and one against a fresh one answer the same rows, so only the chosen walk
    tells them apart.
    """
    sn = schema_name
    q = "SELECT id FROM t WHERE v = 20"
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL); "
        "INSERT INTO t VALUES (1, 10), (2, 20); "
        "CREATE INDEX ix ON t (v)", schema_name=sn)

    with gnitz.connect(server) as b:
        for ddl, indexed in ((None, True), ("DROP INDEX ix", False), ("CREATE INDEX ix ON t (v)", True)):
            if ddl:
                client.execute_sql(ddl, schema_name=sn)
            plan = access(b, sn, q)
            assert ("index" in plan) == indexed, (ddl, plan)
            assert bag(rows(b, sn, q), "id") == {(2,): 1}, ddl
