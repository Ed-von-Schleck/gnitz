"""A view definition past a cap is refused naming the limit — never created and
silently empty.

The expression-register cap and the column-width caps are decided while
planning, before anything is shipped. What only an end-to-end run can show is
both sides of that boundary at once: every size the planner admits is served
whole by the engine, rather than created, resolvable, and empty forever — the
same observable as a correct view over a source that happens to match nothing.

The sweep assertion is threshold-free, so it cannot rot when register allocation
or companion-spec injection changes: for each N in a range spanning the limit,
`CREATE VIEW` either errors naming a cap or returns exactly the correct rows.
Each range below spans the limit measured today, so each sweep contains both
outcomes and neither can pass vacuously. No test pins the limit itself.
"""

import pytest
import gnitz
from _caps import conjunct_ladder, first_rejected, names_a_cap
from _read import bag


@pytest.fixture(scope="module")
def caps(module_schema):
    """A schema with every base relation the sweeps read, seeded so each correct
    answer is non-empty (a zero-row answer would make the invariant
    unfalsifiable). Each sweep drops the view it creates, and no DML below
    changes a row, so every test sees the same data."""
    conn, sn = module_schema

    def sql(q):
        conn.execute_sql(q, schema_name=sn)

    sql("CREATE TABLE hg (pk BIGINT NOT NULL PRIMARY KEY, cat BIGINT NOT NULL)")
    sql("INSERT INTO hg VALUES (1, 1), (2, 1), (3, 2)")

    sql("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)")
    sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")

    sql("CREATE TABLE ts (pk BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)")
    sql("INSERT INTO ts VALUES " + ", ".join(f"({i}, 'a{i}')" for i in range(5)))

    # 16 NULLABLE BIGINTs: a nullable SUM finalizes as `sum / (cnt != 0)`, five
    # registers each, so this is the register cap reached by ordinary SQL rather
    # than by a hand-written predicate.
    sql("CREATE TABLE wn (pk BIGINT NOT NULL PRIMARY KEY, "
        + ", ".join(f"c{i} BIGINT" for i in range(16)) + ")")
    vals = ", ".join(str(i) for i in range(16))
    sql(f"INSERT INTO wn VALUES (1, {vals}), (2, {vals})")

    # 64 NOT NULL BIGINTs + pk = 65 columns: the reduce-output width cap.
    sql("CREATE TABLE w (pk BIGINT NOT NULL PRIMARY KEY, "
        + ", ".join(f"c{i} BIGINT NOT NULL" for i in range(64)) + ")")
    row1 = ", ".join(str(i) for i in range(64))
    row2 = ", ".join(str(i + 100) for i in range(64))
    sql(f"INSERT INTO w VALUES (1, {row1}), (2, {row2})")
    return sn


# `(id, range spanning the limit, body for n, rows the correct answer holds)`.
_SWEEPS = [
    # N equality conjuncts in HAVING; the answer is the single cat=1 group.
    ("having_conjunct", range(10, 16),
     lambda n: "SELECT cat FROM hg GROUP BY cat HAVING " + conjunct_ladder("cat", n), 1),
    # An N-item string IN inside a view's WHERE.
    ("in_list", range(30, 37),
     lambda n: "SELECT pk, s FROM ts WHERE s IN (" + ", ".join(f"'a{i}'" for i in range(n)) + ")", 5),
    # N computed projection items: one shared column load, then a constant and
    # an add apiece.
    ("computed_projection", range(29, 35),
     lambda n: "SELECT pk, " + ", ".join(f"a+{i} AS c{i}" for i in range(n)) + " FROM t", 3),
    # N nullable SUMs grouped by pk — the register cap reached by plain SQL.
    ("nullable_sum", range(10, 17),
     lambda n: "SELECT pk, " + ", ".join(f"SUM(c{i})" for i in range(n)) + " FROM wn GROUP BY pk", 2),
    # N SUMs over a 65-column NOT NULL table: the reduce output width, which is
    # not an expression program at all, so no amount of expression validation
    # would have caught it.
    ("wide_grouped_aggregate", range(61, 65),
     lambda n: "SELECT pk, " + ", ".join(f"SUM(c{i})" for i in range(n)) + " FROM w GROUP BY pk", 2),
    # N group-by columns plus one aggregate. The two seeded rows differ in every
    # column, so each N yields two groups.
    ("wide_group_key", range(60, 65),
     lambda n: "SELECT " + ", ".join(f"c{i}" for i in range(n)) + ", SUM(c63) FROM w GROUP BY "
               + ", ".join(f"c{i}" for i in range(n)), 2),
    # N ungrouped SUMs. This breaks one aggregate earlier than the grouped form
    # because the two-phase combine reduce appends a COUNT-of-partials existence
    # gate the declared reduce schema does not model.
    ("ungrouped_wide_aggregate", range(60, 65),
     lambda n: "SELECT " + ", ".join(f"SUM(c{i})" for i in range(n)) + " FROM w", 1),
]


@pytest.mark.parametrize("ns,sql_for,expected_rows", [s[1:] for s in _SWEEPS],
                         ids=[s[0] for s in _SWEEPS])
def test_a_view_never_compiles_to_silently_empty(client, caps, ns, sql_for, expected_rows):
    """Across a range spanning the limit, `CREATE VIEW` either errors naming a
    cap, or creates a view holding exactly the correct rows, each once."""
    outcomes = set()
    for n in ns:
        try:
            client.execute_sql(f"CREATE VIEW v AS {sql_for(n)}", schema_name=caps)
        except gnitz.GnitzError as e:
            assert names_a_cap(e), f"n={n}: rejection must name a limit, got: {e}"
            outcomes.add("error")
            continue
        weights = list(bag(client.scan(client.resolve_table(caps, "v")[0])).values())
        client.drop_view(caps, "v")
        assert weights == [1] * expected_rows, f"n={n}: expected {expected_rows} rows at weight 1, got {weights}"
        outcomes.add("ok")
    assert outcomes == {"ok", "error"}, f"the sweep must span the limit — outcomes were {outcomes}"


def test_a_dml_residual_crosses_the_same_cap_and_says_so(client, caps):
    """A residual is compiled by the client and shipped as a blob, not built
    into a circuit, so it is a second path to the one cap. Both shapes are
    located rather than pinned: every size below the boundary is served, and the
    first one past it names the limit."""
    sn = caps

    def update(n):
        res = client.execute_sql(
            f"UPDATE hg SET cat = 1 WHERE {conjunct_ladder('cat', n)}", schema_name=sn)
        assert res[0]["count"] == 2, f"n={n}: both cat=1 rows satisfy every conjunct"

    def delete_in(n):
        items = ", ".join(f"'x{i}'" for i in range(n))
        client.execute_sql(f"DELETE FROM ts WHERE s IN ({items})", schema_name=sn)

    first_rejected(update, range(12, 20))
    first_rejected(delete_in, range(28, 40))
