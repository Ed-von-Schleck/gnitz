"""Creating views around data that is already there, across every worker.

A view created after the rows must backfill to the same value as one created
before them, a view over a view must backfill in dependency order, two views
over one table must each get the whole source, and a schema whose views are
already ticking must keep its own answers while another schema's DDL lands.
"""

import pytest
from _serverproc import NEEDS_MULTI
from _uid import uid as _uid
import _oracle as oracle

pytestmark = NEEDS_MULTI

_T = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)"


def test_workers_ddl_create_table(client, schema_name):
    """Create table while workers running; push rows; scan → all rows present."""
    client.execute_sql(_T, schema_name=schema_name)
    n = 50
    client.execute_sql(
        "INSERT INTO t VALUES " + ",".join(f"({i}, {i * 10})" for i in range(1, n + 1)),
        schema_name=schema_name)

    tid, _ = client.resolve_table(schema_name, "t")
    assert sorted((r.pk, r.val) for r in client.scan(tid)) == [
        (i, i * 10) for i in range(1, n + 1)]


def test_workers_view_cascade(client, schema_name):
    """V1 = filter view, V2 = passthrough of V1; push rows; verify V2 has matching rows."""
    client.execute_sql(_T, schema_name=schema_name)
    client.execute_sql("CREATE VIEW v1 AS SELECT * FROM t WHERE val > 10", schema_name=schema_name)
    v1_id, v1_schema = client.resolve_table(schema_name, "v1")
    v2_id = client.create_view(schema_name, "v2", v1_id, v1_schema)

    client.execute_sql("INSERT INTO t VALUES (1, 5), (2, 50), (3, 100)", schema_name=schema_name)

    assert sorted(r.pk for r in client.scan(v1_id)) == [2, 3]
    assert sorted(r.pk for r in client.scan(v2_id)) == [2, 3]


def test_workers_view_ddl_then_push(client, schema_name):
    """Push rows THEN create view; push more rows; scan view → all rows via backfill."""
    client.execute_sql(_T, schema_name=schema_name)
    # Before the view: reaches it only through the backfill.
    client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=schema_name)
    client.execute_sql("CREATE VIEW v AS SELECT * FROM t", schema_name=schema_name)
    # After the view: reaches it as an ordinary delta.
    client.execute_sql("INSERT INTO t VALUES (2, 20), (3, 30)", schema_name=schema_name)

    vid, _ = client.resolve_table(schema_name, "v")
    assert sorted(r.pk for r in client.scan(vid)) == [1, 2, 3]


def test_workers_view_on_view_backfill(client, schema_name):
    """Insert rows, then create v1 then v2 (view on view); both get backfilled."""
    client.execute_sql(_T, schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 5), (2, 30), (3, 100)", schema_name=schema_name)

    client.execute_sql("CREATE VIEW v1 AS SELECT * FROM t WHERE val > 10", schema_name=schema_name)
    client.execute_sql("CREATE VIEW v2 AS SELECT * FROM v1 WHERE val > 50", schema_name=schema_name)

    v1_id, _ = client.resolve_table(schema_name, "v1")
    v2_id, _ = client.resolve_table(schema_name, "v2")
    assert sorted(r.pk for r in client.scan(v1_id)) == [2, 3]
    assert sorted(r.pk for r in client.scan(v2_id)) == [3]


def test_workers_multiple_views_same_table(client, schema_name):
    """Two views on same table; both get pushed data."""
    client.execute_sql(_T, schema_name=schema_name)
    client.execute_sql("CREATE VIEW v1 AS SELECT * FROM t WHERE val > 10", schema_name=schema_name)
    client.execute_sql("CREATE VIEW v2 AS SELECT * FROM t WHERE val > 50", schema_name=schema_name)

    client.execute_sql("INSERT INTO t VALUES (1, 5), (2, 30), (3, 100)", schema_name=schema_name)

    v1_id, _ = client.resolve_table(schema_name, "v1")
    v2_id, _ = client.resolve_table(schema_name, "v2")
    assert sorted(r.pk for r in client.scan(v1_id)) == [2, 3]
    assert sorted(r.pk for r in client.scan(v2_id)) == [3]


# ── A second schema's DDL landing while the first one's views tick ───────────

_NDIM = 10
_NFACT = 40


def _dim_of(pk):
    """The dim row fact `pk` references — every fact matches one."""
    return ((pk - 1) % _NDIM) + 1


@pytest.fixture
def second_schema(client):
    """A second schema beside the `schema_name` one, dropped whole at teardown."""
    sn = "s" + _uid()
    client.create_schema(sn)
    yield sn
    client.drop_schema(sn)


def test_a_second_schemas_ddl_does_not_disturb_a_ticking_schema(
        client, schema_name, second_schema):
    """Each schema's exchange relay stays bound to its own operand schema.

    The two facts differ in width (4 columns against 3) and the two view chains
    in depth, so a relay that labelled its batches with the wrong side's schema
    would cross the two and produce wrong aggregates rather than none. Schema A
    is asserted once before B exists and again after B is fully live, with a
    further insert in between — that last insert is what makes A tick while B's
    views are already running.
    """
    a, b = schema_name, second_schema

    # A: filter -> join -> SUM by region, over a 4-column fact.
    client.execute_sql(
        "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, dim_pk BIGINT NOT NULL, "
        "amount BIGINT NOT NULL, category BIGINT NOT NULL)", schema_name=a)
    client.execute_sql(
        "CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, region BIGINT NOT NULL)",
        schema_name=a)
    client.execute_sql("CREATE VIEW v_filter AS SELECT * FROM fact WHERE amount > 0",
                       schema_name=a)
    client.execute_sql(
        "CREATE VIEW v_joined AS SELECT v_filter.pk, v_filter.amount, dim.region "
        "FROM v_filter INNER JOIN dim ON v_filter.dim_pk = dim.pk", schema_name=a)
    client.execute_sql(
        "CREATE VIEW v_agg AS SELECT region, SUM(amount) AS total FROM v_joined GROUP BY region",
        schema_name=a)
    client.execute_sql(
        "INSERT INTO dim VALUES " + ", ".join(f"({i}, {i % 5})" for i in range(1, _NDIM + 1)),
        schema_name=a)
    # amount = pk, so every fact passes the filter and the sums are exact.
    client.execute_sql(
        "INSERT INTO fact VALUES " + ", ".join(
            f"({pk}, {_dim_of(pk)}, {pk}, {pk % 6})" for pk in range(1, _NFACT + 1)),
        schema_name=a)

    def want_a(hi):
        totals = {}
        for pk in range(1, hi + 1):
            region = _dim_of(pk) % 5
            totals[region] = totals.get(region, 0) + pk
        return {(region, total): 1 for region, total in totals.items()}

    a_agg, _ = client.resolve_table(a, "v_agg")
    oracle.assert_view_matches(client, a_agg, ["region", "total"], want_a(_NFACT), "A before B")

    # B: a five-view chain over a 3-column fact, created while A is live.
    client.execute_sql(
        "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, dim_pk BIGINT NOT NULL, "
        "amount BIGINT NOT NULL)", schema_name=b)
    client.execute_sql(
        "CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, label BIGINT NOT NULL)",
        schema_name=b)
    client.execute_sql("CREATE VIEW v1 AS SELECT * FROM fact WHERE amount > 50", schema_name=b)
    client.execute_sql(
        "CREATE VIEW v2 AS SELECT v1.pk, v1.amount, dim.label "
        "FROM v1 INNER JOIN dim ON v1.dim_pk = dim.pk", schema_name=b)
    client.execute_sql(
        "CREATE VIEW v3 AS SELECT label, SUM(amount) AS total, COUNT(*) AS cnt "
        "FROM v2 GROUP BY label", schema_name=b)
    client.execute_sql("CREATE VIEW v4 AS SELECT * FROM v3 WHERE total > 10", schema_name=b)
    client.execute_sql("CREATE VIEW v5 AS SELECT DISTINCT label FROM v4", schema_name=b)
    client.execute_sql(
        "INSERT INTO dim VALUES " + ", ".join(f"({i}, {i % 5})" for i in range(1, _NDIM + 1)),
        schema_name=b)
    client.execute_sql(
        "INSERT INTO fact VALUES " + ", ".join(
            f"({pk}, {_dim_of(pk)}, {pk * 7})" for pk in range(1, _NFACT + 1)),
        schema_name=b)

    by_label = {}
    for pk in range(1, _NFACT + 1):
        if pk * 7 <= 50:
            continue
        label = _dim_of(pk) % 5
        total, cnt = by_label.get(label, (0, 0))
        by_label[label] = (total + pk * 7, cnt + 1)
    b3 = {(label, total, cnt): 1 for label, (total, cnt) in by_label.items()}
    b5 = {(label,): 1 for label, (total, _) in by_label.items() if total > 10}

    oracle.assert_view_matches(
        client, client.resolve_table(b, "v3")[0], ["label", "total", "cnt"], b3, "B v3")
    oracle.assert_view_matches(
        client, client.resolve_table(b, "v5")[0], ["label"], b5, "B v5")

    # A ticks while B's chain is live: the relay must still be A's own.
    client.execute_sql(
        "INSERT INTO fact VALUES " + ", ".join(
            f"({pk}, {_dim_of(pk)}, {pk}, {pk % 6})"
            for pk in range(_NFACT + 1, _NFACT + 21)),
        schema_name=a)
    oracle.assert_view_matches(
        client, a_agg, ["region", "total"], want_a(_NFACT + 20), "A after B")
