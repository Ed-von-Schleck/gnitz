"""The catalog's object lifecycle: a table's declared shape coming back
unchanged, the foreign key a column registers, a dropped relation's id retiring
while its siblings keep serving.

The verdicts the catalog refuses live in `admissibility/test_errors.py`.
"""

import gnitz
import pytest
from _catalog import family
from _read import bag, rows
from _schemas import KV, kv


def test_create_table_hands_back_the_declared_columns_up_to_the_cap(client):
    """`create_table` does not silently rename, retype or re-key a column: each
    comes back as declared, with the PK at the index the caller put it rather
    than at column 0, at the full `MAX_COLUMNS` width. The cap is located rather
    than named, so moving it moves this test with it.

    The per-type value round trip is `value_domain/test_value_round_trip.py`'s.
    """
    cols = [gnitz.ColumnDef("val", gnitz.TypeCode.I64),
            gnitz.ColumnDef("pk", gnitz.TypeCode.U64)]
    cols += [gnitz.ColumnDef(f"c_{tc.name.lower()}", tc, is_nullable=True)
             for tc in (gnitz.TypeCode.U8, gnitz.TypeCode.I8, gnitz.TypeCode.U16,
                        gnitz.TypeCode.I16, gnitz.TypeCode.U32, gnitz.TypeCode.I32,
                        gnitz.TypeCode.F32, gnitz.TypeCode.U64, gnitz.TypeCode.F64,
                        gnitz.TypeCode.STRING)]
    cols += [gnitz.ColumnDef(f"pad{i}", gnitz.TypeCode.I64, is_nullable=True)
             for i in range(gnitz.MAX_COLUMNS - len(cols))]
    tid = client.create_table("t", gnitz.Schema(cols, [1]))

    resolved, schema = client.resolve_table("t")
    assert resolved == tid
    assert [(c.name, c.type_code) for c in schema.columns] == [(c.name, c.type_code) for c in cols]
    assert schema.pk_indices == [1]


def test_only_a_base_table_registers_an_fk_and_a_self_reference_names_itself(client):
    client.execute_sql(
        "CREATE TABLE tree (id BIGINT NOT NULL PRIMARY KEY, parent_id BIGINT REFERENCES tree(id)); "
        "CREATE VIEW tree_v AS SELECT id, parent_id FROM tree")
    tid, _ = client.resolve_table("tree")
    vid, _ = client.resolve_table("tree_v")
    assert family(client, "columns", "owner_id", "col_idx", "fk_table_id",
                  where=f"owner_id IN ({tid}, {vid})") == \
        {(tid, 0, 0): 1, (tid, 1, tid): 1, (vid, 0, 0): 1, (vid, 1, 0): 1}


def test_a_dropped_relation_stops_answering_while_its_siblings_serve(client):
    """Two views over one source both receive every push; dropping one retires
    exactly that id — it stops answering scans — and leaves the other
    maintained. A dropped populated table's id retires the same way."""
    tid = client.create_table("src", KV)
    client.execute_sql("CREATE VIEW va AS SELECT * FROM src; CREATE VIEW vb AS SELECT * FROM src")
    va, vb = (client.resolve_table(v)[0] for v in ("va", "vb"))

    client.push(tid, kv((1, 10)))
    for vid in (va, vb):
        assert bag(client.scan(vid, KV), "pk", "val") == {(1, 10): 1}

    client.execute_sql("DROP VIEW va")
    with pytest.raises(gnitz.GnitzNotFoundError):
        client.scan(va, KV)
    client.push(tid, kv((2, 20)))
    assert bag(client.scan(vb, KV), "pk", "val") == {(1, 10): 1, (2, 20): 1}

    client.execute_sql("DROP VIEW vb; DROP TABLE src")
    with pytest.raises(gnitz.GnitzNotFoundError):
        client.scan(tid, KV)



def test_a_select_run_again_reads_a_relation_another_connection_replaced(client, peer):
    """The same SELECT text, run often enough on one connection to be kept,
    answers from the relation its name names now: another connection dropping
    it and creating one of another shape under that name, or dropping it for
    good, is what the next run reads."""
    other = peer()
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL); "
        "INSERT INTO t VALUES (1, 10), (2, 20); "
        "CREATE VIEW v AS SELECT id, a FROM t WHERE a > 10")
    for _ in range(3):
        assert bag(rows(client, "SELECT * FROM t"), "id", "a") == {(1, 10): 1, (2, 20): 1}
        assert bag(rows(client, "SELECT * FROM v LIMIT 5"), "id", "a") == {(2, 20): 1}

    other.execute_sql(
        "DROP VIEW v; DROP TABLE t; "
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL, a BIGINT NOT NULL); "
        "INSERT INTO t VALUES (7, 'x', 70); "
        "CREATE VIEW v AS SELECT id, s FROM t")
    for _ in range(3):
        assert bag(rows(client, "SELECT * FROM t"), "id", "s", "a") == {(7, "x", 70): 1}
        assert bag(rows(client, "SELECT * FROM v LIMIT 5"), "id", "s") == {(7, "x"): 1}

    other.execute_sql("DROP VIEW v; DROP TABLE t")
    for _ in range(2):
        with pytest.raises(gnitz.GnitzNotFoundError, match="relation 't' not found"):
            client.execute_sql("SELECT * FROM t")
        with pytest.raises(gnitz.GnitzNotFoundError, match="relation 'v' not found"):
            client.execute_sql("SELECT * FROM v LIMIT 5")
