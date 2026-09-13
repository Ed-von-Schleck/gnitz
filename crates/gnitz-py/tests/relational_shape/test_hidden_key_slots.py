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
from _read import bag, scanned

# view -> (body, its visible columns, its bag before and after `a(2)` leaves).
_SHAPES = {
    "equi_join": ("SELECT a.av AS av, b.bv AS bv FROM a JOIN b ON a.k = b.k",
                  ("av", "bv"), {(100, 200): 1}, {(100, 200): 1}),
    # Three pairs satisfy a.k < b.k and all project to one tuple, so the pair key
    # is what keeps them three elements rather than one.
    "range_join": ("SELECT a.av AS av, b.bv AS bv FROM a JOIN b ON a.k < b.k",
                   ("av", "bv"), {(100, 200): 3}, {(100, 200): 1}),
    "exists": ("SELECT av FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k)",
               ("av",), {(100,): 1}, {(100,): 1}),
    "union": ("SELECT av AS val FROM a UNION SELECT bv AS val FROM b",
              ("val",), {(100,): 1, (200,): 1}, {(100,): 1, (200,): 1}),
    "distinct": ("SELECT DISTINCT av FROM a", ("av",), {(100,): 1}, {(100,): 1}),
    "group_by_text": ("SELECT cat, COUNT(*) AS cnt FROM a GROUP BY cat",
                      ("cat", "cnt"), {("x", 1): 1, ("y", 1): 1}, {("x", 1): 1}),
    # No synthetic key: the source PK the projection drops rides hidden, and is
    # what keeps two rows sharing `av` two elements.
    "unprojected_source_pk": ("SELECT av FROM a", ("av",), {(100,): 2}, {(100,): 1}),
    # A wildcard modifier decides the column list before any expression lowers;
    # a dropped or renamed name must be absent from the row, not merely unread.
    "wildcard_except_rename": ("SELECT * EXCEPT (cat) RENAME (av AS years) FROM a",
                               ("pk", "k", "years"), {(1, 7, 100): 1, (2, 5, 100): 1}, {(1, 7, 100): 1}),
    "wildcard_exclude": ("SELECT * EXCLUDE (k, cat) FROM a",
                         ("pk", "av"), {(1, 100): 1, (2, 100): 1}, {(1, 100): 1}),
    # A hidden key never re-enters through a downstream wildcard, however deep.
    "stacked": ("SELECT * FROM over_jv1", ("av", "bv"), {(100, 200): 1}, {(100, 200): 1}),
    # Downstream identity is the projected content: two structurally identical
    # join views meet UNION ALL as one tuple at weight 2 and UNION at weight 1.
    "union_all_of_twins": ("SELECT * FROM jv1 UNION ALL SELECT * FROM jv2",
                           ("av", "bv"), {(100, 200): 2}, {(100, 200): 2}),
    "union_of_twins": ("SELECT * FROM jv1 UNION SELECT * FROM jv2",
                       ("av", "bv"), {(100, 200): 1}, {(100, 200): 1}),
}


def test_a_key_slot_is_absent_from_every_client_row(client, schema_name):
    """For each emitter that fabricates or inherits a key, the presented fields
    are exactly the projected names, and the weights show the key still keying
    through a retraction. `include_hidden=True` is the debugging escape hatch: it
    presents the synthetic key first, carrying the decoded join-key value."""
    sn = schema_name
    join = "SELECT a.av AS av, b.bv AS bv FROM a JOIN b ON a.k = b.k"
    client.execute_sql(
        "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, av BIGINT NOT NULL, "
        "cat TEXT NOT NULL); "
        "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, bv BIGINT NOT NULL); "
        f"CREATE VIEW jv1 AS {join}; CREATE VIEW jv2 AS {join}; "
        "CREATE VIEW over_jv1 AS SELECT * FROM jv1; "
        + "; ".join(f"CREATE VIEW {name} AS {body}" for name, (body, *_) in _SHAPES.items()) + "; "
        "INSERT INTO a VALUES (1, 7, 100, 'x'), (2, 5, 100, 'y'); "
        "INSERT INTO b VALUES (1, 7, 200), (2, 9, 200)", schema_name=sn)

    for step in ("before", "after"):
        if step == "after":
            client.execute_sql("DELETE FROM a WHERE pk = 2", schema_name=sn)
        for name, (_, cols, before, after) in _SHAPES.items():
            rows = scanned(client, sn, name)
            assert bag(rows, *cols) == (before if step == "before" else after), (step, name)
            assert all(set(r._fields) == set(cols) for r in rows), (step, name, rows)

    raw = list(client.scan(client.resolve_table(sn, "jv1")[0], include_hidden=True))
    assert [r._fields[0] for r in raw] == ["_join_pk"], raw
    assert bag(raw, "_join_pk", "av", "bv") == {(7, 100, 200): 1}
