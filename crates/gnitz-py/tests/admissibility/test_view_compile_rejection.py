"""A view definition past a cap is refused naming the limit — never created and
silently empty.

The expression-register cap and the column-width caps are decided while
planning, before anything is shipped. What only an end-to-end run can show is
both sides of that boundary at once: every size the planner admits is served
whole by the engine, rather than created, resolvable, and empty forever — the
same observable as a correct view over a source that happens to match nothing.

The sweep assertion is threshold-free, so it cannot rot when register allocation
or companion-spec injection changes: across a range spanning the limit, every N
below the first refusal returns exactly the correct rows, and that refusal names
a cap. Each range below spans the limit measured today, so each sweep contains
both outcomes and neither can pass vacuously. No test pins the limit itself.
"""

import pytest
from _caps import conjunct_ladder, first_rejected
from _read import bag, scanned
from _sql import insert


@pytest.fixture(scope="module")
def caps(module_client):
    """A connection holding every base relation the sweeps read, seeded so each
    correct answer is non-empty (a zero-row answer would make the invariant
    unfalsifiable). Each sweep drops the view it creates, and no DML below
    changes a row, so every test sees the same data."""
    sql = module_client.execute_sql

    sql("CREATE TABLE hg (pk BIGINT NOT NULL PRIMARY KEY, cat BIGINT NOT NULL)")
    insert(module_client, "hg", [(1, 1), (2, 1), (3, 2)])

    sql("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)")
    insert(module_client, "t", [(1, 10), (2, 20), (3, 30)])

    sql("CREATE TABLE ts (pk BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)")
    insert(module_client, "ts", [(i, f"a{i}") for i in range(5)])

    # 16 NULLABLE BIGINTs: a nullable SUM finalizes as `sum / (cnt != 0)`, five
    # registers each, so this is the register cap reached by ordinary SQL rather
    # than by a hand-written predicate.
    sql("CREATE TABLE wn (pk BIGINT NOT NULL PRIMARY KEY, "
        + ", ".join(f"c{i} BIGINT" for i in range(16)) + ")")
    insert(module_client, "wn", [(pk, *range(16)) for pk in (1, 2)])

    # 64 NOT NULL BIGINTs + pk = 65 columns: the reduce-output width cap.
    sql("CREATE TABLE w (pk BIGINT NOT NULL PRIMARY KEY, "
        + ", ".join(f"c{i} BIGINT NOT NULL" for i in range(64)) + ")")
    insert(module_client, "w", [(1, *range(64)), (2, *range(100, 164))])
    return module_client


def _strings(n):
    """An IN list of `n` strings, the first five of which `ts` holds."""
    return ", ".join(f"'a{i}'" for i in range(n))


# `name: (range spanning the limit, body for n, the correct answer's rows for n)`.
_SWEEPS = {
    # N equality conjuncts in HAVING; the answer is the single cat=1 group.
    "having_conjunct": (
        range(10, 16), lambda n: "SELECT cat FROM hg GROUP BY cat HAVING " + conjunct_ladder("cat", n),
        lambda n: [(1,)]),
    # An N-item string IN inside a view's WHERE.
    "in_list": (
        range(30, 37), lambda n: f"SELECT pk, s FROM ts WHERE s IN ({_strings(n)})",
        lambda n: [(i, f"a{i}") for i in range(5)]),
    # N computed projection items: one shared column load, then a constant and
    # an add apiece.
    "computed_projection": (
        range(29, 35), lambda n: "SELECT pk, " + ", ".join(f"a+{i} AS c{i}" for i in range(n)) + " FROM t",
        lambda n: [(pk, *(a + i for i in range(n))) for pk, a in [(1, 10), (2, 20), (3, 30)]]),
    # N nullable SUMs grouped by pk — the register cap reached by plain SQL.
    "nullable_sum": (
        range(10, 17), lambda n: "SELECT pk, " + ", ".join(f"SUM(c{i})" for i in range(n)) + " FROM wn GROUP BY pk",
        lambda n: [(pk, *range(n)) for pk in (1, 2)]),
    # N SUMs over a 65-column NOT NULL table: the reduce output width, which is
    # not an expression program at all, so no amount of expression validation
    # would have caught it.
    "wide_grouped_aggregate": (
        range(61, 65), lambda n: "SELECT pk, " + ", ".join(f"SUM(c{i})" for i in range(n)) + " FROM w GROUP BY pk",
        lambda n: [(1, *range(n)), (2, *range(100, 100 + n))]),
    # N group-by columns plus one aggregate. The two seeded rows differ in every
    # column, so each N yields two groups.
    "wide_group_key": (
        range(60, 65), lambda n: "SELECT " + ", ".join(f"c{i}" for i in range(n)) + ", SUM(c63) FROM w GROUP BY "
        + ", ".join(f"c{i}" for i in range(n)),
        lambda n: [(*range(n), 63), (*range(100, 100 + n), 163)]),
    # N ungrouped SUMs, each gated on the shared COUNT(*): the finalize's
    # register cap.
    "ungrouped_wide_aggregate": (
        range(28, 34), lambda n: "SELECT " + ", ".join(f"SUM(c{i})" for i in range(n)) + " FROM w",
        lambda n: [tuple(2 * i + 100 for i in range(n))]),
}


@pytest.mark.parametrize("ns,sql_for,want", _SWEEPS.values(), ids=_SWEEPS.keys())
def test_a_view_never_compiles_to_silently_empty(caps, ns, sql_for, want):
    """Across a range spanning the limit, `CREATE VIEW` creates a view holding
    exactly the correct rows, each once, until it errors naming a cap."""
    def create(n):
        caps.execute_sql(f"CREATE VIEW v AS {sql_for(n)}")
        got = bag(scanned(caps, "v"))
        caps.execute_sql("DROP VIEW v")
        assert got == dict.fromkeys(want(n), 1), f"n={n}"

    first_rejected(create, ns)


# `name: (table, range spanning the limit, an identity UPDATE for n, the rows it
# matches)`.
_RESIDUALS = {
    "conjuncts": ("hg", range(12, 20), lambda n: f"UPDATE hg SET cat = 1 WHERE {conjunct_ladder('cat', n)}", 2),
    "in_list": ("ts", range(28, 40), lambda n: f"UPDATE ts SET s = s WHERE s IN ({_strings(n)})", 5),
}


@pytest.mark.parametrize("table,ns,stmt_for,count", _RESIDUALS.values(), ids=_RESIDUALS.keys())
def test_a_dml_residual_crosses_the_same_cap_and_says_so(caps, table, ns, stmt_for, count):
    """A residual is compiled by the client and shipped as a blob, not built
    into a circuit, so it is a second path to the one cap: every size below the
    boundary updates exactly the rows it matches, and the first one past it
    names the limit."""
    before = bag(scanned(caps, table))

    def update(n):
        assert caps.execute_sql(stmt_for(n))[0]["count"] == count, f"n={n}"
        assert bag(scanned(caps, table)) == before, f"n={n}"

    first_rejected(update, ns)
