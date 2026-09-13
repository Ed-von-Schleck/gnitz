"""ALTER TABLE ADD COLUMN: the appended column reads NULL for every row that
predates it, and is writable from then on.

Run at GNITZ_WORKERS=4 — the widening has to reach every partition, or one left
at the old width consolidates a padded row against a real one.

The clause refusals are `gnitz-sql/tests/engine_ddl.rs`'s; the crash and
checkpoint replays are `state_lifetime/test_durability.py`'s; the ALTER racing
live traffic is `interleaving/test_ddl_under_traffic.py`'s; the RESTRICT a
dependent view imposes is `test_alter.py`'s.
"""

from _read import access, bag, rows
from _serverproc import NEEDS_MULTI


@NEEDS_MULTI
def test_add_column_pads_every_pre_alter_row_with_null(client, schema_name):
    """Two serialized ADD COLUMNs — a BIGINT and a TEXT — over enough rows to
    land on every partition: the columns append in order, existing rows read
    both as NULL, and both are writable through INSERT, an UPDATE of a pre-ALTER
    row (whose retraction has to match the stored, padded row), DELETE, a point
    seek and a filter. Strings cover both German-string forms, inline and heap.

    The secondary index on `a` keeps *serving* seeks at the wider width —
    asserted through EXPLAIN's access line, since a plan that lost the index
    answers the same rows by full scan.

    Asserted as weighted bags: a partition left at the old width moves a weight,
    not the row set.
    """
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL); "
        "INSERT INTO t VALUES " + ", ".join(f"({k}, {k * 10})" for k in range(1, 41)) + "; "
        "CREATE INDEX ia ON t (a); "
        "ALTER TABLE t ADD COLUMN c BIGINT; "
        "ALTER TABLE t ADD COLUMN s TEXT", schema_name=sn)

    got = rows(client, sn, "SELECT * FROM t")
    assert got[0]._fields == ("id", "a", "c", "s")
    assert bag(got, "id", "a", "c", "s") == {(k, k * 10, None, None): 1 for k in range(1, 41)}

    long = "a string comfortably past the inline prefix"
    client.execute_sql(
        f"INSERT INTO t VALUES (41, 410, 4100, 'short'), (42, 420, NULL, '{long}'); "
        "UPDATE t SET c = 55, s = 'set later' WHERE id = 1; "
        "DELETE FROM t WHERE id = 2", schema_name=sn)

    want = {(k, k * 10, None, None): 1 for k in range(3, 41)}
    want |= {(1, 10, 55, "set later"): 1, (41, 410, 4100, "short"): 1, (42, 420, None, long): 1}
    assert bag(rows(client, sn, "SELECT * FROM t"), "id", "a", "c", "s") == want

    assert bag(rows(client, sn, "SELECT c FROM t WHERE id = 3"), "c") == {(None,): 1}
    assert bag(rows(client, sn, "SELECT id FROM t WHERE c = 55"), "id") == {(1,): 1}

    for q, want in (("SELECT id, c FROM t WHERE a = 30", {(3, None): 1}),
                    ("SELECT id, c FROM t WHERE a >= 390",
                     {(39, None): 1, (40, None): 1, (41, 4100): 1, (42, None): 1})):
        plan = access(client, sn, q)
        assert "index" in plan, plan
        assert bag(rows(client, sn, q), "id", "c") == want, q
