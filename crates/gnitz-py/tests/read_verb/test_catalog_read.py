"""The system families read ad hoc: each is a relation of `_system`, and a
`SELECT` over one takes every sink a table's does."""

import gnitz
from _catalog import family, schema_id
from _read import bag, rows, scanned


def _three_tables(client):
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL); "
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, y BIGINT); "
        "CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY)")
    return schema_id(client), {n: client.resolve_table(n)[0] for n in "abc"}


def test_a_family_reads_as_the_scan_of_its_id(client):
    """The SQL name and the family's id are one relation: every row once, in
    any spelling of the schema."""
    _three_tables(client)
    want = bag(client.scan(gnitz.TABLE_TAB, gnitz.sys_schema(gnitz.TABLE_TAB)), "table_id", "name")
    assert bag(rows(client, "SELECT table_id, name FROM _system.tables")) == want
    assert bag(rows(client, "SELECT table_id, name FROM _SYSTEM.Tables")) == want
    assert bag(scanned(client, "_system.tables"), "table_id", "name") == want
    assert set(want.values()) == {1}
    assert {name for (tid, name) in want if tid < gnitz.FIRST_USER_TABLE_ID} == {
        "schemas", "tables", "views", "columns", "indices", "circuits", "sequences"}


def test_a_family_is_projected_and_filtered(client):
    sid, ids = _three_tables(client)
    assert family(client, "tables", "name", where=f"schema_id = {sid}") == {
        ("a",): 1, ("b",): 1, ("c",): 1}
    assert family(client, "tables", "table_id", where=f"schema_id = {sid} AND name = 'b'") == {
        (ids["b"],): 1}
    assert family(client, "columns", "col_idx", "name", where=f"owner_id = {ids['b']}") == {
        (0, "id"): 1, (1, "x"): 1, (2, "y"): 1}
    assert family(client, "schemas", "name", where=f"schema_id = {sid}") == {(client.schema,): 1}


def test_a_family_is_counted_grouped_and_ordered(client):
    sid, ids = _three_tables(client)
    owners = ", ".join(str(i) for i in ids.values())
    assert bag(rows(client, f"SELECT COUNT(*) AS n FROM _system.tables WHERE schema_id = {sid}")) == {(3,): 1}
    assert bag(rows(
        client,
        f"SELECT owner_id, COUNT(*) AS n FROM _system.columns WHERE owner_id IN ({owners}) GROUP BY owner_id",
    )) == {(ids["a"], 2): 1, (ids["b"], 3): 1, (ids["c"], 1): 1}
    top = rows(
        client,
        f"SELECT name FROM _system.tables WHERE schema_id = {sid} ORDER BY name DESC LIMIT 2")
    assert [r.name for r in top] == ["c", "b"]


def test_sequences_is_readable(client):
    """No view may scan it, but it reads like the others."""
    client.execute_sql("CREATE TABLE s (id BIGSERIAL PRIMARY KEY, v BIGINT NOT NULL)")
    client.execute_sql("INSERT INTO s (v) VALUES (1), (2)")
    got = rows(client, "SELECT * FROM _system.sequences")
    assert got and all(r._weight == 1 for r in got)
