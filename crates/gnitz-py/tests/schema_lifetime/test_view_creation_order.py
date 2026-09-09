"""`CREATE VIEW` over base tables that already hold data.

A view created after the rows must come back identical to the same view created
before them — for every shape and every ordering of the source rows relative to
the CREATE:

  * pending     — INSERT then CREATE VIEW (rows still un-ticked at CREATE)
  * committed   — INSERT, force a tick, then CREATE VIEW (rows committed)
  * view_first  — CREATE VIEW then INSERT (the steady-state control)

The two orderings that look alike are not: a push applies to the store at once
and the DDL drains every reachable base before `VIEW_TAB` registers the view, so
both present a full store to the backfill. What `pending` pins is *where that
drain sits* — moved after registration, it would double-count the pending rows
while `committed` stayed green.

Assertions are weighted bags throughout. The failures this guards are doubled
weights over the right row set: a pending source driven by both a catalog-layer
backfill and the deferred ticks, and a committed source a single-process driver
could not shuffle.

Run at GNITZ_WORKERS=4 — the exchange/fanout paths only engage at W>1.
"""

import pytest
from gnitz import CIRCUIT_NODES_TAB, Opcode, VIEW_TAB
from _oracle import assert_view_matches
from _read import bag, scanned
from _serverproc import NEEDS_MULTI
from _uid import uid as _uid


def _values(rows):
    return ", ".join("(" + ", ".join(str(c) for c in r) + ")" for r in rows)


# ── shape builders ──────────────────────────────────────────────────────────
# Each returns the view DDL (one statement, or several for a chain), the base
# tables whose tick the `committed` ordering forces, an insert() closure, the
# expected weighted bag of the last view, and the columns to read it by.

_A_DDL = ("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, "
          "k BIGINT NOT NULL, av BIGINT NOT NULL)")
_A_ROWS = [(i, i % 5, i * 10) for i in range(30)]


def _create_a(client, sn):
    client.execute_sql(_A_DDL, schema_name=sn)


def _insert_a(client, sn):
    client.execute_sql(f"INSERT INTO a VALUES {_values(_A_ROWS)}", schema_name=sn)


def _shape_proj(client, sn):
    _create_a(client, sn)
    return dict(view=["CREATE VIEW v AS SELECT id, av + 1 AS p FROM a"],
                bases=["a"], insert=lambda: _insert_a(client, sn),
                cols=["id", "p"],
                want={(i, i * 10 + 1): 1 for i in range(30)})


def _shape_grp(client, sn):
    _create_a(client, sn)
    return dict(view=["CREATE VIEW v AS SELECT k, COUNT(*) AS c FROM a GROUP BY k"],
                bases=["a"], insert=lambda: _insert_a(client, sn),
                cols=["k", "c"], want={(k, 6): 1 for k in range(5)})


def _shape_distinct(client, sn):
    _create_a(client, sn)
    return dict(view=["CREATE VIEW v AS SELECT DISTINCT k FROM a"],
                bases=["a"], insert=lambda: _insert_a(client, sn),
                cols=["k"], want={(k,): 1 for k in range(5)})


def _shape_joi(client, sn):
    _create_a(client, sn)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, bv BIGINT NOT NULL)", schema_name=sn)

    def insert():
        _insert_a(client, sn)
        client.execute_sql(
            f"INSERT INTO b VALUES {_values([(j, j * 100) for j in range(5)])}", schema_name=sn)

    return dict(view=["CREATE VIEW v AS SELECT a.id AS aid, b.bv AS bv "
                      "FROM a JOIN b ON a.k = b.id"],
                bases=["a", "b"], insert=insert, cols=["aid", "bv"],
                want={(i, (i % 5) * 100): 1 for i in range(30)})


def _shape_setop(client, sn):
    client.execute_sql("CREATE TABLE s1 (id BIGINT NOT NULL PRIMARY KEY)", schema_name=sn)
    client.execute_sql("CREATE TABLE s2 (id BIGINT NOT NULL PRIMARY KEY)", schema_name=sn)

    def insert():
        client.execute_sql(
            f"INSERT INTO s1 VALUES {_values([(i,) for i in range(30)])}", schema_name=sn)
        client.execute_sql(
            f"INSERT INTO s2 VALUES {_values([(i,) for i in range(20, 40)])}", schema_name=sn)

    return dict(view=["CREATE VIEW v AS SELECT id FROM s1 UNION SELECT id FROM s2"],
                bases=["s1", "s2"], insert=insert, cols=["id"],
                want={(i,): 1 for i in range(40)})


def _shape_rangejoin(client, sn):
    client.execute_sql(
        "CREATE TABLE ra (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE rb (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)", schema_name=sn)

    def insert():
        client.execute_sql(
            f"INSERT INTO ra VALUES {_values([(i, i) for i in range(10)])}", schema_name=sn)
        client.execute_sql(
            f"INSERT INTO rb VALUES {_values([(j, j) for j in range(10)])}", schema_name=sn)

    return dict(view=["CREATE VIEW v AS SELECT ra.x AS x, rb.y AS y "
                      "FROM ra JOIN rb ON ra.x < rb.y"],
                bases=["ra", "rb"], insert=insert, cols=["x", "y"],
                want={(i, j): 1 for i in range(10) for j in range(10) if i < j})


def _shape_viewonview(client, sn):
    """A chain: `v` reads a filter view, which reads the base. The driver has to
    populate them in dependency order, or `v` backfills from an empty source."""
    _create_a(client, sn)
    return dict(view=["CREATE VIEW mid AS SELECT id, k, av FROM a WHERE av > 100",
                      "CREATE VIEW v AS SELECT id, av FROM mid WHERE k = 3"],
                bases=["a"], insert=lambda: _insert_a(client, sn),
                cols=["id", "av"],
                want={(i, i * 10): 1 for i in range(30) if i * 10 > 100 and i % 5 == 3})


def _shape_grouped_cte(client, sn):
    """A linear final over a *grouped* CTE — the final circuit is linear (no
    Join, no ExchangeShard) but its delta source is an in-bundle hidden segment
    that carries one, so the segment must be filled before the final reads it."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
    return dict(view=["CREATE VIEW v AS WITH c AS "
                      "(SELECT id, SUM(v) AS sv FROM t GROUP BY id) "
                      "SELECT id FROM c WHERE sv > 10"],
                bases=["t"],
                insert=lambda: client.execute_sql(
                    "INSERT INTO t VALUES (1, 20), (2, 5), (3, 30)", schema_name=sn),
                cols=["id"], want={(1,): 1, (3,): 1})


def _shape_join_cte(client, sn):
    """The same, over a *join* CTE — the hidden segment seeds via its Join."""
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)", schema_name=sn)

    def insert():
        client.execute_sql("INSERT INTO a VALUES (1, 10), (2, 20), (3, 10)", schema_name=sn)
        client.execute_sql("INSERT INTO b VALUES (10, 5), (20, 0)", schema_name=sn)

    return dict(view=["CREATE VIEW v AS WITH c AS "
                      "(SELECT a.id AS id, b.x AS x FROM a JOIN b ON a.k = b.id) "
                      "SELECT id FROM c WHERE x > 0"],
                bases=["a", "b"], insert=insert, cols=["id"], want={(1,): 1, (3,): 1})


def _shape_linear_between(client, sn):
    """A linear hidden segment between two exchanging ones: a grouped CTE `g`, a
    linear CTE `l` over it, and a grouped final over `l`."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
    return dict(view=["CREATE VIEW v AS "
                      "WITH g AS (SELECT id, SUM(v) AS sv FROM t GROUP BY id), "
                      "     l AS (SELECT id, sv FROM g WHERE sv > 10) "
                      "SELECT sv, COUNT(*) AS c FROM l GROUP BY sv"],
                bases=["t"],
                insert=lambda: client.execute_sql(
                    "INSERT INTO t VALUES (1, 20), (2, 5), (3, 20), (4, 30)", schema_name=sn),
                cols=["sv", "c"], want={(20, 2): 1, (30, 1): 1})


_SHAPES = {
    "proj": _shape_proj,
    "grp": _shape_grp,
    "distinct": _shape_distinct,
    "joi": _shape_joi,
    "setop": _shape_setop,
    "rangejoin": _shape_rangejoin,
    "viewonview": _shape_viewonview,
    "grouped_cte": _shape_grouped_cte,
    "join_cte": _shape_join_cte,
    "linear_between": _shape_linear_between,
}


@pytest.mark.parametrize("shape", list(_SHAPES))
@pytest.mark.parametrize("ordering", ["pending", "committed", "view_first"])
def test_a_view_holds_the_same_zset_whenever_it_was_created(
        client, schema_name, shape, ordering):
    """Every shape × every ordering comes back as the exact expected bag."""
    spec = _SHAPES[shape](client, schema_name)

    def create():
        for stmt in spec["view"]:
            client.execute_sql(stmt, schema_name=schema_name)

    if ordering == "view_first":
        create()
        spec["insert"]()
    else:
        spec["insert"]()
        if ordering == "committed":
            # A scan drains pending deltas, so the rows are committed before the
            # view exists; otherwise they would be picked up as still-pending on
            # the next tick and mask the committed path.
            for b in spec["bases"]:
                scanned(client, schema_name, b)
        create()

    assert_view_matches(client, client.resolve_table(schema_name, "v")[0],
                        spec["cols"], spec["want"], f"{shape}/{ordering}")


def test_a_backfilled_view_stays_incremental(client, schema_name):
    """The backfilled rows are a starting state, not a final one: a later insert
    adds to them and a delete of a backfilled row retracts it weight-correctly.
    """
    spec = _shape_grouped_cte(client, schema_name)
    spec["insert"]()
    for stmt in spec["view"]:
        client.execute_sql(stmt, schema_name=schema_name)
    assert bag(scanned(client, schema_name, "v"), "id") == {(1,): 1, (3,): 1}

    client.execute_sql("INSERT INTO t VALUES (4, 40)", schema_name=schema_name)
    assert bag(scanned(client, schema_name, "v"), "id") == {(1,): 1, (3,): 1, (4,): 1}

    client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=schema_name)
    assert bag(scanned(client, schema_name, "v"), "id") == {(3,): 1, (4,): 1}


def test_a_view_over_a_separately_created_grouped_view_backfills(client, schema_name):
    """No shared bundle: the source view was registered and filled by an earlier
    DDL, so it is already complete when the second CREATE runs."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, v BIGINT NOT NULL)",
        schema_name=schema_name)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 7, 20), (2, 7, 5), (3, 9, 4)", schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW g AS SELECT g, SUM(v) AS sv FROM t GROUP BY g", schema_name=schema_name)
    assert bag(scanned(client, schema_name, "g"), "g", "sv") == {(7, 25): 1, (9, 4): 1}

    client.execute_sql("CREATE VIEW s AS SELECT g FROM g WHERE sv > 10", schema_name=schema_name)
    assert bag(scanned(client, schema_name, "s"), "g") == {(7,): 1}

    client.execute_sql("INSERT INTO t VALUES (4, 9, 30)", schema_name=schema_name)
    assert bag(scanned(client, schema_name, "s"), "g") == {(7,): 1, (9,): 1}


# ── the ordering costs no exchange ──────────────────────────────────────────


def _sole_segment_vid(client, owner_vid):
    """The vid of the one internal chain segment `owner_vid` owns, read off
    VIEW_TAB's ownership column — the same thing the engine's drop cascade keys
    on — so nothing here assumes how the ids were allocated."""
    segs = [r["view_id"] for r in client.scan(VIEW_TAB) if r["owner_view_id"] == owner_vid]
    assert len(segs) == 1, f"expected exactly one segment, got {segs}"
    return segs[0]


def _has_exchange_shard(client, vid):
    return any(r["view_id"] == vid and r["opcode"] == Opcode.ExchangeShard
               for r in client.scan(CIRCUIT_NODES_TAB))


@NEEDS_MULTI
@pytest.mark.parametrize("source", ["grouped_cte", "linear_cte"])
def test_a_linear_final_is_not_sharded_to_order_its_backfill(client, schema_name, source):
    """A filter/projection neither re-keys nor redistributes its source, whatever
    the source is, so the final carries no `ExchangeShard` — while a grouped
    CTE's own segment keeps the one its reduce needs.

    A cost claim, not a value one: an `ExchangeShard` here would be a
    cluster-wide IPC barrier on every epoch, paid solely to order the backfill.
    """
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=schema_name)
    cte = ("SELECT id, SUM(v) AS sv FROM t GROUP BY id" if source == "grouped_cte"
           else "SELECT id, v AS sv FROM t WHERE v > 0")
    client.execute_sql(
        f"CREATE VIEW s AS WITH c AS ({cte}) SELECT id FROM c WHERE sv > 10",
        schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 20), (2, 5)", schema_name=schema_name)

    vid = client.resolve_table(schema_name, "s")[0]
    assert not _has_exchange_shard(client, vid), \
        "a linear final must not be sharded to order its backfill"
    if source == "grouped_cte":
        assert _has_exchange_shard(client, _sole_segment_vid(client, vid)), \
            "the grouped CTE segment's own reduce exchange must survive"
    assert bag(scanned(client, schema_name, "s"), "id") == {(1,): 1}


# ── another schema's DDL landing while this one's views tick ────────────────

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


@NEEDS_MULTI
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
    assert_view_matches(client, a_agg, ["region", "total"], want_a(_NFACT), "A before B")

    # B: a five-view chain over a 3-column fact, created while A is live. `v4`'s
    # threshold excludes the smallest label group, so the DISTINCT below it sees
    # fewer labels than `v3` produced.
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
    client.execute_sql("CREATE VIEW v4 AS SELECT * FROM v3 WHERE total > 1000", schema_name=b)
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
    b5 = {(label,): 1 for label, (total, _) in by_label.items() if total > 1000}
    assert 0 < len(b5) < len(b3), "v4's threshold must exclude some but not all labels"

    assert_view_matches(client, client.resolve_table(b, "v3")[0],
                        ["label", "total", "cnt"], b3, "B v3")
    assert_view_matches(client, client.resolve_table(b, "v5")[0], ["label"], b5, "B v5")

    # A ticks while B's chain is live: the relay must still be A's own.
    client.execute_sql(
        "INSERT INTO fact VALUES " + ", ".join(
            f"({pk}, {_dim_of(pk)}, {pk}, {pk % 6})"
            for pk in range(_NFACT + 1, _NFACT + 21)),
        schema_name=a)
    assert_view_matches(client, a_agg, ["region", "total"], want_a(_NFACT + 20), "A after B")


def test_the_backfill_streams_long_strings_across_chunk_boundaries(tiny_ddl_chunk_server):
    """The backfill streams the source in `scan_chunk_rows`-sized chunks, so a
    view over a populated table must equal it row for row whatever the chunk
    size. Strings above the inline threshold are what make the chunking visible:
    each chunk relocates its payload into a fresh blob arena, so a value that
    survives within a chunk can still be lost or aliased across one.

    At the 65 536-row default a test table is a single chunk and pins nothing;
    the 3-row fixture makes 10 rows span four.
    """
    c = tiny_ddl_chunk_server
    sn = "s" + _uid()
    c.create_schema(sn)
    try:
        c.execute_sql(
            "CREATE TABLE base (id BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL)",
            schema_name=sn)
        want = {(i, f"row_{i:02d}_" + "x" * 40): 1 for i in range(10)}
        c.execute_sql(
            "INSERT INTO base VALUES " + ", ".join(f"({i}, '{s}')" for i, s in sorted(want)),
            schema_name=sn)

        # Created after the data, so the whole table arrives through the chunked
        # backfill rather than tick by tick.
        c.execute_sql("CREATE VIEW v AS SELECT id, name FROM base", schema_name=sn)
        assert bag(scanned(c, sn, "v"), "id", "name") == want

        # Still incremental over the backfilled rows.
        c.execute_sql("INSERT INTO base VALUES (10, '" + "row_10_" + "y" * 40 + "')",
                      schema_name=sn)
        want[(10, "row_10_" + "y" * 40)] = 1
        assert bag(scanned(c, sn, "v"), "id", "name") == want

        c.execute_sql("DELETE FROM base WHERE id = 3", schema_name=sn)
        del want[(3, "row_03_" + "x" * 40)]
        assert bag(scanned(c, sn, "v"), "id", "name") == want
    finally:
        c.drop_schema(sn)
