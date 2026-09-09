"""Hidden key slots: a view's visible schema is exactly what its SELECT projects.

Every emitter that fabricates a synthetic key (`_join_pk`, `_pair_pk`, `_set_pk`,
`_distinct_pk`, `_group_pk`) marks that column hidden, and so is a simple view's
auto-prepended unprojected source PK. A hidden column is physical — it still
keys, routes, sorts and consolidates the view — but is excluded from wildcard
expansion, name resolution and client rows.

That exclusion is the contract, so the assertions here are over the presented
field *names*, never a column count; and the weights alongside them are what
shows the key is still doing its physical job.
"""

import pytest
from _read import bag, scanned


def _sources(client, sn):
    """Three sources and their rows — one pair for the join/set shapes, one TEXT
    grouping key for the shape whose group key cannot be a PK column."""
    client.execute_sql(
        "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, av BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, bv BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE TABLE cat (pk BIGINT NOT NULL PRIMARY KEY, category TEXT NOT NULL, "
        "amount BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql("INSERT INTO a VALUES (1, 7, 100), (2, 5, 100)", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (1, 7, 200), (2, 9, 200)", schema_name=sn)
    client.execute_sql(
        "INSERT INTO cat VALUES (1, 'x', 10), (2, 'x', 20), (3, 'y', 30)", schema_name=sn)


_SHAPES = [
    pytest.param(
        "SELECT a.av AS av, b.bv AS bv FROM a JOIN b ON a.k = b.k",
        {"av", "bv"}, ("av", "bv"), {(100, 200): 1}, id="equi-join"),
    pytest.param(
        # Three (a, b) pairs satisfy a.k < b.k and all project to one tuple, so
        # the pair key is what keeps them three elements rather than one.
        "SELECT a.av AS av, b.bv AS bv FROM a JOIN b ON a.k < b.k",
        {"av", "bv"}, ("av", "bv"), {(100, 200): 3}, id="range-join"),
    pytest.param(
        "SELECT av FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k)",
        {"av"}, ("av",), {(100,): 1}, id="exists"),
    pytest.param(
        "SELECT av AS val FROM a UNION SELECT bv AS val FROM b",
        {"val"}, ("val",), {(100,): 1, (200,): 1}, id="union"),
    pytest.param(
        "SELECT DISTINCT av FROM a",
        {"av"}, ("av",), {(100,): 1}, id="distinct"),
    pytest.param(
        "SELECT category, COUNT(*) AS cnt FROM cat GROUP BY category",
        {"category", "cnt"}, ("category", "cnt"), {("x", 2): 1, ("y", 1): 1}, id="group-by-text"),
    pytest.param(
        # No synthetic key: the source PK the projection drops rides hidden, and
        # is what keeps two rows sharing `av` two elements.
        "SELECT av FROM a",
        {"av"}, ("av",), {(100,): 2}, id="unprojected-source-pk"),
]


@pytest.mark.parametrize("body, fields, cols, want", _SHAPES)
def test_a_key_slot_is_absent_from_every_client_row(client, schema_name, body, fields, cols, want):
    """For each emitter that fabricates or inherits a key, the presented fields
    are exactly the projected names, and the weights show the key still keying."""
    sn = schema_name
    _sources(client, sn)
    client.execute_sql(f"CREATE VIEW v AS {body}", schema_name=sn)

    rows = scanned(client, sn, "v")
    assert bag(rows, *cols) == want
    for r in rows:
        assert set(r._fields) == fields, r._fields


def test_include_hidden_surfaces_the_key_at_its_physical_slot(client, schema_name):
    """The debugging escape hatch: `include_hidden=True` presents the synthetic
    key in its physical position — first, ahead of the payload — carrying the
    decoded join-key value, not the raw ordered bytes."""
    sn = schema_name
    _sources(client, sn)
    client.execute_sql(
        "CREATE VIEW jv AS SELECT a.av AS av, b.bv AS bv FROM a JOIN b ON a.k = b.k",
        schema_name=sn)
    vid = client.resolve_table(sn, "jv")[0]

    raw = list(client.scan(vid, include_hidden=True))
    assert len(raw) == 1, raw
    assert raw[0]._fields[0] == "_join_pk", raw[0]._fields
    assert (raw[0]["_join_pk"], raw[0]["av"], raw[0]["bv"]) == (7, 100, 200)


def test_a_hidden_key_never_re_enters_through_a_downstream_view(client, schema_name):
    """`SELECT *` over a hidden-keyed view expands to the visible columns only,
    however many layers deep, and naming a hidden column outright fails rather
    than resolving."""
    sn = schema_name
    _sources(client, sn)
    client.execute_sql(
        "CREATE VIEW l1 AS SELECT a.av AS av, b.bv AS bv FROM a JOIN b ON a.k = b.k",
        schema_name=sn)
    client.execute_sql("CREATE VIEW l2 AS SELECT * FROM l1", schema_name=sn)
    client.execute_sql("CREATE VIEW l3 AS SELECT * FROM l2", schema_name=sn)
    client.execute_sql("CREATE VIEW sv AS SELECT av FROM a", schema_name=sn)

    rows = scanned(client, sn, "l3")
    assert bag(rows, "av", "bv") == {(100, 200): 1}
    for r in rows:
        assert set(r._fields) == {"av", "bv"}, r._fields

    # `sv` drops `a`'s PK, which rides hidden; a downstream view cannot name it.
    with pytest.raises(Exception) as ei:
        client.execute_sql("CREATE VIEW ds AS SELECT pk FROM sv", schema_name=sn)
    assert "pk" in str(ei.value).lower() or "not found" in str(ei.value).lower(), str(ei.value)


def test_a_set_op_over_two_identical_join_views_dedups_on_content(client, schema_name):
    """Downstream identity is the projected content: two structurally identical
    join views must reach UNION ALL as one tuple at weight 2 and collapse under
    UNION to weight 1, rather than staying distinct on their upstream key."""
    sn = schema_name
    _sources(client, sn)
    for name in ("jv1", "jv2"):
        client.execute_sql(
            f"CREATE VIEW {name} AS SELECT a.av AS av, b.bv AS bv FROM a JOIN b ON a.k = b.k",
            schema_name=sn)
    client.execute_sql("CREATE VIEW ua AS SELECT * FROM jv1 UNION ALL SELECT * FROM jv2",
                       schema_name=sn)
    client.execute_sql("CREATE VIEW ud AS SELECT * FROM jv1 UNION SELECT * FROM jv2",
                       schema_name=sn)

    assert bag(scanned(client, sn, "ua"), "av", "bv") == {(100, 200): 2}
    assert bag(scanned(client, sn, "ud"), "av", "bv") == {(100, 200): 1}
