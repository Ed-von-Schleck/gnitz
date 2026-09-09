"""Retraction reaching a view: what a negative-weight write does to a filter, a
join, and a chain of views over both.

Views are defined in SQL, so the engine's operators are reached through the
planner. Every assertion is on weights, not row presence: a view runs no
`enforce_unique_pk`, so a delta applied twice keeps the row set and doubles the
weights, and a retraction that never landed leaves the row at weight 1 beside
its replacement.
"""

import pytest
import gnitz

_CREATE_T = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)"


def _weights(client, tid, *cols):
    """Live rows as `(*cols, weight)`, sorted."""
    return sorted(tuple(getattr(r, c) for c in cols) + (r.weight,)
                  for r in client.scan(tid))


def _retract(client, tid, schema, **row):
    """Push `row` at weight -1 through the table's own schema.

    The schema comes from `resolve_table`, never hand-built: a BIGINT PK is
    stored as signed I64, and a U64-typed batch would encode a different
    order-preserving PK image and never consolidate against the original.
    """
    batch = gnitz.ZSetBatch(schema)
    batch.append(_weight=-1, **row)
    client.push(tid, batch)


# ── retraction through a filter ───────────────────────────────────────────────


@pytest.fixture
def filtered(client, schema_name):
    """`t` under `v = SELECT * FROM t WHERE val > 50`; `(tid, schema, vid)`."""
    client.execute_sql(_CREATE_T, schema_name=schema_name)
    client.execute_sql("CREATE VIEW v AS SELECT * FROM t WHERE val > 50",
                       schema_name=schema_name)
    tid, schema = client.resolve_table(schema_name, "t")
    vid, _ = client.resolve_table(schema_name, "v")
    return tid, schema, vid


@pytest.mark.parametrize("val,in_view", [(100, True), (10, False)],
                         ids=["inside-the-filter", "outside-the-filter"])
def test_retraction_propagates_through_a_filter(client, schema_name, filtered, val, in_view):
    """A retraction of a row the filter passed removes it; one the filter never
    passed leaves the view untouched rather than driving it negative."""
    tid, schema, vid = filtered
    client.execute_sql(f"INSERT INTO t VALUES (1, {val})", schema_name=schema_name)
    assert _weights(client, vid, "pk", "val") == ([(1, val, 1)] if in_view else [])

    _retract(client, tid, schema, pk=1, val=val)
    assert _weights(client, vid, "pk", "val") == []


def test_update_crossing_the_filter_boundary(client, schema_name, filtered):
    """An UPDATE is a retraction plus a re-insert, so a row moving across the
    predicate enters the view at weight 1 — not weight 2 beside its old self."""
    tid, _schema, vid = filtered
    client.execute_sql("INSERT INTO t VALUES (1, 100), (2, 30)", schema_name=schema_name)
    assert _weights(client, vid, "pk", "val") == [(1, 100, 1)]

    client.execute_sql("UPDATE t SET val = 200 WHERE pk = 2", schema_name=schema_name)
    assert _weights(client, vid, "pk", "val") == [(1, 100, 1), (2, 200, 1)]

    # And back out again: the row leaves rather than lingering at weight 0.
    client.execute_sql("UPDATE t SET val = 1 WHERE pk = 2", schema_name=schema_name)
    assert _weights(client, vid, "pk", "val") == [(1, 100, 1)]


# ── retraction through a join ─────────────────────────────────────────────────


@pytest.fixture
def joined(client, schema_name):
    """`a ⋈ b` on the PK; yields the two tables' `(tid, schema)` and the vid."""
    client.execute_sql("CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                       schema_name=schema_name)
    client.execute_sql("CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, label BIGINT NOT NULL)",
                       schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.pk, a.val, b.label FROM a JOIN b ON a.pk = b.pk",
        schema_name=schema_name)
    return (client.resolve_table(schema_name, "a"),
            client.resolve_table(schema_name, "b"),
            client.resolve_table(schema_name, "v")[0])


@pytest.mark.parametrize("side", ["left", "right"])
def test_retracting_either_operand_removes_the_join_output(client, schema_name, joined, side):
    """Join output weight is the product of the input weights, so retracting
    either side cancels the pair exactly once — from the delta on one side and
    from the other side's integral."""
    (a_tid, a_schema), (b_tid, b_schema), vid = joined
    client.execute_sql("INSERT INTO a VALUES (1, 100)", schema_name=schema_name)
    client.execute_sql("INSERT INTO b VALUES (1, 999)", schema_name=schema_name)
    assert _weights(client, vid, "pk", "val", "label") == [(1, 100, 999, 1)]

    if side == "left":
        _retract(client, a_tid, a_schema, pk=1, val=100)
    else:
        _retract(client, b_tid, b_schema, pk=1, label=999)
    assert _weights(client, vid, "pk", "val", "label") == []


def test_retraction_cascades_through_a_view_chain(client, schema_name):
    """`t → v1 (filter) → v2 (passthrough)`: an insert reaches the far end of the
    chain and its retraction unwinds the whole chain, each at weight 1."""
    client.execute_sql(_CREATE_T, schema_name=schema_name)
    client.execute_sql("CREATE VIEW v1 AS SELECT * FROM t WHERE val > 10",
                       schema_name=schema_name)
    tid, t_schema = client.resolve_table(schema_name, "t")
    v1_id, v1_schema = client.resolve_table(schema_name, "v1")
    v2_id = client.create_view(schema_name, "v2", v1_id, v1_schema)

    client.execute_sql("INSERT INTO t VALUES (1, 100)", schema_name=schema_name)
    assert _weights(client, v1_id, "pk", "val") == [(1, 100, 1)]
    assert _weights(client, v2_id, "pk", "val") == [(1, 100, 1)]

    _retract(client, tid, t_schema, pk=1, val=100)
    assert _weights(client, v1_id, "pk", "val") == []
    assert _weights(client, v2_id, "pk", "val") == []


# ── NULL through the operators ────────────────────────────────────────────────


def test_null_excluded_by_a_filter(client, schema_name):
    """`IS NOT NULL` drops the null-valued row and keeps the rest at weight 1."""
    client.execute_sql("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT)",
                       schema_name=schema_name)
    client.execute_sql("CREATE VIEW v AS SELECT * FROM t WHERE val IS NOT NULL",
                       schema_name=schema_name)
    vid, _ = client.resolve_table(schema_name, "v")

    client.execute_sql("INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30)",
                       schema_name=schema_name)
    assert _weights(client, vid, "pk", "val") == [(1, 10, 1), (3, 30, 1)]


def test_null_payload_survives_a_join(client, schema_name):
    """A nullable payload column on the preserved side reaches the join output
    still NULL — the null bitmap is re-indexed, not dropped."""
    client.execute_sql("CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT)",
                       schema_name=schema_name)
    client.execute_sql("CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, score BIGINT NOT NULL)",
                       schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.pk, a.val, b.score FROM a JOIN b ON a.pk = b.pk",
        schema_name=schema_name)
    vid, _ = client.resolve_table(schema_name, "v")

    client.execute_sql("INSERT INTO a VALUES (1, NULL)", schema_name=schema_name)
    client.execute_sql("INSERT INTO b VALUES (1, 42)", schema_name=schema_name)
    assert _weights(client, vid, "pk", "val", "score") == [(1, None, 42, 1)]


def test_count_counts_null_rows_and_sum_skips_them(client, schema_name):
    """`COUNT(*)` sums row weight over the group including the NULL-valued row;
    `SUM` folds only the non-NULL inputs."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, val BIGINT)",
        schema_name=schema_name)
    client.execute_sql("CREATE VIEW v AS SELECT grp, COUNT(*) AS cnt, SUM(val) AS total "
                       "FROM t GROUP BY grp", schema_name=schema_name)
    vid, _ = client.resolve_table(schema_name, "v")

    client.execute_sql("INSERT INTO t VALUES (1, 1, 10), (2, 1, 20), (3, 1, NULL)",
                       schema_name=schema_name)
    # One group row, at weight 1: a reduce that failed to retract the old
    # aggregate would leave the superseded (cnt, total) here beside this one.
    assert _weights(client, vid, "grp", "cnt", "total") == [(1, 3, 30, 1)]


# ── a grouped write reaching a join ───────────────────────────────────────────


def test_a_batched_insert_joins_to_exactly_its_matches(client, schema_name):
    """Both sides written by one multi-row INSERT each: the join emits one row
    per key at weight 1, and carries both payloads."""
    n = 200
    client.execute_sql("CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, a_val BIGINT NOT NULL)",
                       schema_name=schema_name)
    client.execute_sql("CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, b_val BIGINT NOT NULL)",
                       schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.pk, a.a_val, b.b_val FROM a JOIN b ON a.pk = b.pk",
        schema_name=schema_name)
    vid, _ = client.resolve_table(schema_name, "v")

    client.execute_sql("INSERT INTO a VALUES " + ",".join(f"({i}, {i * 10})" for i in range(1, n + 1)),
                       schema_name=schema_name)
    client.execute_sql("INSERT INTO b VALUES " + ",".join(f"({i}, {i * 100})" for i in range(1, n + 1)),
                       schema_name=schema_name)

    assert _weights(client, vid, "pk", "a_val", "b_val") == [
        (i, i * 10, i * 100, 1) for i in range(1, n + 1)
    ]
