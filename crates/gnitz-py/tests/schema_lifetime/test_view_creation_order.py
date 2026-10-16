"""`CREATE VIEW` over base tables whose rows have not ticked yet.

A push applies to the store at once, and the DDL drains every reachable base
before `VIEW_TAB` registers the view, so the backfill sees a full store. What
this pins is *where that drain sits*: moved after registration, the pending rows
would reach the view twice — through the backfill and through the deferred tick.
So every assertion is a weighted bag, where that failure is doubled weights over
the right row set.
"""

import pytest
from _read import bag, scanned
from _serverproc import NEEDS_MULTI
from _shapes import SHAPES, grouped, views_ddl
from _sql import values


@pytest.mark.parametrize("shape", list(SHAPES))
def test_a_view_created_over_pending_rows_holds_them_once(client, shape):
    """One statement bundle: the tables, their rows, then the views — a chain of
    separate DDLs where a shape has several, each backfilling from what the
    previous one filled."""
    tables, rows, views = SHAPES[shape]
    client.execute_sql("; ".join(filter(None, (tables, rows, views_ddl(views)))))
    for name, (_, cols, want) in views.items():
        assert bag(scanned(client, name), *cols) == want, name


# ── another schema's DDL landing while this one's views tick ────────────────

_NDIM = 10
_NFACT = 40
_DIM_ROWS = f"INSERT INTO dim VALUES {values((i, i % 5) for i in range(1, _NDIM + 1))}"


def _dim_of(pk):
    """The dim row fact `pk` references — every fact matches one."""
    return ((pk - 1) % _NDIM) + 1


def _region(pk):
    return _dim_of(pk) % 5


@NEEDS_MULTI
def test_a_second_schemas_ddl_does_not_disturb_a_ticking_schema(client, other_client):
    """Each schema's exchange rounds stay bound to their own operand schema, and
    one table name in two schemas addresses two relations.

    The two facts differ in width (4 columns against 3) and the two view chains
    in depth, so an exchange that labelled its batches with the wrong side's schema
    would cross the two and produce wrong aggregates rather than none. Schema A
    is asserted once before B holds a relation and again after B is fully live,
    with a further insert in between — that last insert is what makes A tick
    while B's views are already running. Exchange rounds only exist at W>1.
    """
    a, b = client, other_client

    def fact_rows(lo, hi):
        # amount = pk, so every fact passes A's filter and the sums are exact.
        return f"INSERT INTO fact VALUES {values((pk, _dim_of(pk), pk, pk % 6) for pk in range(lo, hi))}"

    def want_a(hi):
        return grouped(range(1, hi), _region, lambda pk: pk)

    # A: filter -> join -> SUM by region, over a 4-column fact.
    a.execute_sql(
        "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, dim_pk BIGINT NOT NULL, "
        "amount BIGINT NOT NULL, category BIGINT NOT NULL); "
        "CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, region BIGINT NOT NULL); "
        "CREATE VIEW v_filter AS SELECT * FROM fact WHERE amount > 0; "
        "CREATE VIEW v_joined AS SELECT v_filter.pk, v_filter.amount, dim.region "
        "FROM v_filter INNER JOIN dim ON v_filter.dim_pk = dim.pk; "
        "CREATE VIEW v_agg AS SELECT region, SUM(amount) AS total FROM v_joined GROUP BY region; "
        f"{_DIM_ROWS}; {fact_rows(1, _NFACT + 1)}")
    assert bag(scanned(a, "v_agg")) == want_a(_NFACT + 1), "A before B"

    # B: a five-view chain over a 3-column fact, created while A is live. `v4`'s
    # threshold excludes the smallest label group, so the DISTINCT below it sees
    # fewer labels than `v3` produced.
    b.execute_sql(
        "CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, dim_pk BIGINT NOT NULL, "
        "amount BIGINT NOT NULL); "
        "CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, label BIGINT NOT NULL); "
        "CREATE VIEW v1 AS SELECT * FROM fact WHERE amount > 50; "
        "CREATE VIEW v2 AS SELECT v1.pk, v1.amount, dim.label "
        "FROM v1 INNER JOIN dim ON v1.dim_pk = dim.pk; "
        "CREATE VIEW v3 AS SELECT label, SUM(amount) AS total, COUNT(*) AS cnt "
        "FROM v2 GROUP BY label; "
        "CREATE VIEW v4 AS SELECT * FROM v3 WHERE total > 1000; "
        "CREATE VIEW v5 AS SELECT DISTINCT label FROM v4; "
        f"{_DIM_ROWS}; "
        f"INSERT INTO fact VALUES {values((pk, _dim_of(pk), pk * 7) for pk in range(1, _NFACT + 1))}")

    b3 = grouped([pk for pk in range(1, _NFACT + 1) if pk * 7 > 50], _region, lambda pk: pk * 7, lambda pk: 1)
    assert bag(scanned(b, "v3")) == b3
    assert bag(scanned(b, "v5")) == {(label,): 1 for label, total, _ in b3 if total > 1000}

    # A ticks while B's chain is live: its exchange rounds must still be A's own.
    a.execute_sql(fact_rows(_NFACT + 1, _NFACT + 21))
    assert bag(scanned(a, "v_agg")) == want_a(_NFACT + 21), "A after B"
