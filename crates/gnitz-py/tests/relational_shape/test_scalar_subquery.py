"""Scalar aggregate subquery view bodies — correlated and uncorrelated — and the
`ANY` / `ALL` quantified comparisons that lower onto the same machinery.

A scalar subquery becomes a reduce whose one row per correlation group (or one
row for the whole relation) is joined back onto the outer row and substituted
for the subquery wherever it appeared. The two coordinates that decide the
compiled shape are therefore:

  * **correlated** — a grouped reduce keyed on the correlation columns, joined
    back as a LEFT join so an outer row with no inner group survives;
  * **uncorrelated** — a global reduce whose single ground row is broadcast and
    joined as an INNER join, so an empty source's ground *value* is what decides
    whether the outer row survives at all.

The ground row is where the two aggregate families part: COUNT's is a real 0,
every other aggregate's is NULL, and a comparison against NULL is UNKNOWN. A
lowering that keyed the join on the raw reduce column instead of the finalized
one reads that NULL as 0 and admits exactly the rows the ground row must
exclude.

Every assertion is a weighted bag: a flip that emits the new value without
retracting the old leaves both present, which a value lookup would not see.

Run with GNITZ_WORKERS=4 — a global reduce funnels, a grouped one exchanges.
"""
import pytest
import gnitz
from _read import bag, scanned

_A = ("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
      "v BIGINT NOT NULL)")
_B = ("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
      "w BIGINT NOT NULL)")


def _ab(client, sn):
    client.execute_sql(_A, schema_name=sn)
    client.execute_sql(_B, schema_name=sn)


# ---------------------------------------------------------------------------
# Correlated: a grouped reduce joined back onto the outer row
# ---------------------------------------------------------------------------


def test_a_projected_correlated_aggregate_reads_its_groups_ground_value(client, schema_name):
    """Projected into the SELECT list, a correlated aggregate reads its group's
    value, and an outer row with no inner group survives with the aggregate's
    ground value: 0 for COUNT, NULL for SUM / MIN / MAX / AVG. Each transition
    retracts the previous row, so no id ever carries two values at once."""
    sn = schema_name
    _ab(client, sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.id, "
        "(SELECT COUNT(*) FROM b WHERE b.k = a.k) AS c, "
        "(SELECT SUM(w) FROM b WHERE b.k = a.k) AS s, "
        "(SELECT MIN(w) FROM b WHERE b.k = a.k) AS mn, "
        "(SELECT MAX(w) FROM b WHERE b.k = a.k) AS mx, "
        "(SELECT AVG(w) FROM b WHERE b.k = a.k) AS av FROM a", schema_name=sn)
    outs = ("id", "c", "s", "mn", "mx", "av")

    client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), *outs) == {
        (1, 0, None, None, None, None): 1,
        (2, 0, None, None, None, None): 1,
    }, "no inner rows: COUNT reads 0 and every other aggregate NULL"

    client.execute_sql("INSERT INTO b VALUES (1, 10, 4), (2, 10, 10)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), *outs) == {
        (1, 2, 14, 4, 10, 7.0): 1,
        (2, 0, None, None, None, None): 1,
    }

    client.execute_sql("DELETE FROM b WHERE id IN (1, 2)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), *outs) == {
        (1, 0, None, None, None, None): 1,
        (2, 0, None, None, None, None): 1,
    }


def test_a_correlated_minimum_re_derives_from_history_when_it_is_retracted(
        client, schema_name):
    """MIN is not linear, so retracting the current extremum needs the next value
    out of the group's history rather than the delta alone. Under the scalar
    chain that retraction must still reach the outer row: the projected value
    steps 3 → 8 → NULL as the group empties."""
    sn = schema_name
    _ab(client, sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.id, (SELECT MIN(w) FROM b WHERE b.k = a.k) AS mn FROM a",
        schema_name=sn)

    client.execute_sql("INSERT INTO a VALUES (1, 10, 0)", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (1, 10, 3), (2, 10, 8)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id", "mn") == {(1, 3): 1}

    client.execute_sql("DELETE FROM b WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id", "mn") == {(1, 8): 1}

    client.execute_sql("DELETE FROM b WHERE id = 2", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id", "mn") == {(1, None): 1}


def test_an_inner_local_conjunct_filters_the_group_before_it_folds(client, schema_name):
    """A WHERE conjunct referencing only the inner relation is not a correlation
    key: it filters the reduce's input, so a row failing it never contributes to
    the group's value."""
    sn = schema_name
    _ab(client, sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.id, "
        "(SELECT COUNT(*) FROM b WHERE b.k = a.k AND b.w > 5) AS c FROM a", schema_name=sn)

    client.execute_sql("INSERT INTO a VALUES (1, 10, 0)", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (1, 10, 3), (2, 10, 8), (3, 10, 20)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id", "c") == {(1, 2): 1}, "only w > 5 is counted"


def test_a_correlated_aggregate_is_a_column_in_every_expression_position(
        client, schema_name):
    """Substituted for the subquery, the aggregate is an ordinary column: it
    compares in a WHERE, feeds a CASE and combines under OR. The comparison in
    the WHERE moves with the group as the group changes."""
    sn = schema_name
    _ab(client, sn)
    client.execute_sql(
        "CREATE VIEW cnt AS SELECT a.v FROM a "
        "WHERE (SELECT COUNT(*) FROM b WHERE b.k = a.k) >= 2", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW mixed AS SELECT a.id, "
        "CASE WHEN (SELECT COUNT(*) FROM b WHERE b.k = a.k) > 0 THEN 1 ELSE 0 END AS hit "
        "FROM a WHERE a.v > 0 OR (SELECT SUM(w) FROM b WHERE b.k = a.k) > 100",
        schema_name=sn)

    client.execute_sql("INSERT INTO a VALUES (1, 10, 5), (2, 20, 0)", schema_name=sn)
    assert bag(scanned(client, sn, "cnt"), "v") == {}
    # id 2 has v = 0 and an empty group, so `NULL > 100` is UNKNOWN: excluded.
    assert bag(scanned(client, sn, "mixed"), "id", "hit") == {(1, 0): 1}

    client.execute_sql("INSERT INTO b VALUES (1, 10, 5), (2, 10, 6)", schema_name=sn)
    assert bag(scanned(client, sn, "cnt"), "v") == {(5,): 1}
    assert bag(scanned(client, sn, "mixed"), "id", "hit") == {(1, 1): 1}


def test_the_inner_relation_may_be_the_outer_one(client, schema_name):
    """Outer and inner name the same base table through two aliases. The
    collision rule gives the inner reduce its own source id, so the circuit still
    sees one delta per source per epoch."""
    sn = schema_name
    _ab(client, sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a1.id FROM a a1 "
        "WHERE a1.v >= (SELECT SUM(a2.v) FROM a a2 WHERE a2.k = a1.k)", schema_name=sn)

    # k = 10 sums to 105, so neither of its rows reaches it; k = 20's lone row is
    # its own sum and does.
    client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 10, 5), (3, 20, 7)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id") == {(3,): 1}


def test_two_subqueries_stack_in_one_body(client, schema_name):
    """Each subquery gets its own reduce and its own synthetic output name, so
    two of them in one projection do not collide."""
    sn = schema_name
    _ab(client, sn)
    client.execute_sql(
        "CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, x BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.id, "
        "(SELECT SUM(w) FROM b WHERE b.k = a.k) AS sb, "
        "(SELECT SUM(x) FROM c WHERE c.k = a.k) AS sc FROM a", schema_name=sn)

    client.execute_sql("INSERT INTO a VALUES (1, 10, 100)", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (1, 10, 7)", schema_name=sn)
    client.execute_sql("INSERT INTO c VALUES (1, 10, 9)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id", "sb", "sc") == {(1, 7, 9): 1}


# ---------------------------------------------------------------------------
# Uncorrelated: a global reduce broadcast as one ground row
# ---------------------------------------------------------------------------


def test_an_uncorrelated_extremum_over_an_empty_source_is_null_not_zero(
        client, schema_name):
    """A companion-free MIN/MAX keys the join on the raw reduce column, whose
    ground row over an empty source is NULL — zero bytes under a set null bit.
    Declaring that column NOT NULL would suppress the lowering's NULL gate and
    let `map_reindex` OPK-encode it as the real key 0, so the discriminator is a
    predicate that is *true* at 0: `v < MAX` needs a negative v, `v = MIN` a v of
    exactly 0. A genuine zero extremum must then match where the NULL did not."""
    sn = schema_name
    _ab(client, sn)
    client.execute_sql(
        "CREATE VIEW lt AS SELECT a.id FROM a WHERE a.v < (SELECT MAX(w) FROM b)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW eq AS SELECT a.id FROM a WHERE a.v = (SELECT MIN(w) FROM b)",
        schema_name=sn)

    client.execute_sql("INSERT INTO a VALUES (1, 1, -7), (2, 2, 0)", schema_name=sn)
    assert bag(scanned(client, sn, "lt"), "id") == {}, "a NULL MAX must not compare as 0"
    assert bag(scanned(client, sn, "eq"), "id") == {}, "a NULL MIN must not compare as 0"

    client.execute_sql("INSERT INTO b VALUES (1, 1, 0)", schema_name=sn)
    assert bag(scanned(client, sn, "lt"), "id") == {(1,): 1}, "a genuine 0 MAX admits v = -7"
    assert bag(scanned(client, sn, "eq"), "id") == {(2,): 1}, "a genuine 0 MIN admits v = 0"

    # The threshold moves with the extremum.
    client.execute_sql("INSERT INTO b VALUES (2, 1, 25)", schema_name=sn)
    assert bag(scanned(client, sn, "lt"), "id") == {(1,): 1, (2,): 1}
    assert bag(scanned(client, sn, "eq"), "id") == {(2,): 1}, "MIN is still 0"

    client.execute_sql("DELETE FROM b", schema_name=sn)
    assert bag(scanned(client, sn, "lt"), "id") == {}
    assert bag(scanned(client, sn, "eq"), "id") == {}


def test_an_uncorrelated_count_grounds_at_a_real_zero(client, schema_name):
    """COUNT is the one aggregate whose empty-source value is a value rather than
    NULL, so the equi join against it matches an outer 0 over an empty source —
    the opposite verdict to every other aggregate's ground row."""
    sn = schema_name
    _ab(client, sn)
    client.execute_sql(
        "CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT c.id FROM c WHERE c.v = (SELECT COUNT(*) FROM b)",
        schema_name=sn)

    client.execute_sql("INSERT INTO c VALUES (1, 0), (2, 1), (3, 2)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id") == {(1,): 1}, "COUNT over an empty b is 0"

    client.execute_sql("INSERT INTO b VALUES (1, 1, 5)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id") == {(2,): 1}


def test_an_uncorrelated_sum_joins_on_its_finalized_value(client, schema_name):
    """SUM over a nullable column carries a raw accumulator and a non-NULL count;
    the join must key on the finalized `sum / (cnt != 0)`, since the raw column
    reads 0 for an empty, fully-retracted or all-NULL group and would admit
    `x = 0` and `x < 0`. Pinned in both join positions — equi and pure range —
    because each keys the ground row itself."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE d (id BIGINT NOT NULL PRIMARY KEY, y BIGINT)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW eq AS SELECT t.id FROM t WHERE t.x = (SELECT SUM(y) FROM d)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW lt AS SELECT t.id FROM t WHERE t.x < (SELECT SUM(y) FROM d)",
        schema_name=sn)

    # x = 0 and x = -5 are the rows a raw 0 would wrongly admit.
    client.execute_sql("INSERT INTO t VALUES (1, 0), (2, 5), (3, -5)", schema_name=sn)
    assert bag(scanned(client, sn, "eq"), "id") == {}, "an empty SUM is NULL, not 0"
    assert bag(scanned(client, sn, "lt"), "id") == {}, "an empty SUM bounds nothing"

    client.execute_sql("INSERT INTO d VALUES (1, 5)", schema_name=sn)
    assert bag(scanned(client, sn, "eq"), "id") == {(2,): 1}
    assert bag(scanned(client, sn, "lt"), "id") == {(1,): 1, (3,): 1}

    client.execute_sql("INSERT INTO d VALUES (2, NULL)", schema_name=sn)
    assert bag(scanned(client, sn, "eq"), "id") == {(2,): 1}, "a NULL contributes nothing"
    assert bag(scanned(client, sn, "lt"), "id") == {(1,): 1, (3,): 1}

    client.execute_sql("DELETE FROM d WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "eq"), "id") == {}, "only a NULL left: SUM is NULL again"
    assert bag(scanned(client, sn, "lt"), "id") == {}


def test_a_count_that_can_never_be_null_folds_its_null_tests_away(client, schema_name):
    """COUNT has no NULL in its range, so `COALESCE(count, x)` folds to the count
    — never to `x`, not even for a group with no rows — and `IS NOT NULL` over it
    folds to TRUE. The fold applies to the correlated and the uncorrelated form
    alike; the uncorrelated one then compiles to no reduce segment at all."""
    sn = schema_name
    _ab(client, sn)
    client.execute_sql(
        "CREATE VIEW co AS SELECT a.id, "
        "COALESCE((SELECT COUNT(*) FROM b WHERE b.k = a.k), 5) AS c FROM a", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW nn AS SELECT a.id FROM a "
        "WHERE (SELECT COUNT(*) FROM b WHERE b.k = a.k) IS NOT NULL", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW gn AS SELECT a.id FROM a WHERE (SELECT COUNT(*) FROM b) IS NOT NULL",
        schema_name=sn)

    client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (1, 10, 5)", schema_name=sn)
    assert bag(scanned(client, sn, "co"), "id", "c") == {(1, 1): 1, (2, 0): 1}, \
        "the empty group reads 0, never the COALESCE default"
    assert bag(scanned(client, sn, "nn"), "id") == {(1,): 1, (2,): 1}
    assert bag(scanned(client, sn, "gn"), "id") == {(1,): 1, (2,): 1}


def test_correlated_and_uncorrelated_subqueries_compose_in_one_body(client, schema_name):
    """A projected correlated COUNT and an uncorrelated WHERE comparison in the
    same body: a LEFT join onto the grouped reduce and an INNER join onto the
    global one, in one chain."""
    sn = schema_name
    _ab(client, sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.k = a.k) AS c "
        "FROM a WHERE a.v < (SELECT MAX(w) FROM b)", schema_name=sn)

    client.execute_sql("INSERT INTO a VALUES (1, 10, 5), (2, 20, 50)", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (1, 10, 8), (2, 10, 30)", schema_name=sn)
    # Global MAX is 30: id 1 (v = 5) passes and counts its two k = 10 rows.
    assert bag(scanned(client, sn, "v"), "id", "c") == {(1, 2): 1}


def test_a_backfill_reaches_the_same_value_as_the_incremental_order(client, schema_name):
    """Seeded by a backfill scan rather than by deltas, both the correlated and
    the uncorrelated shape reach the value the incremental order produces."""
    sn = schema_name
    _ab(client, sn)
    client.execute_sql("INSERT INTO a VALUES (1, 10, 10), (2, 20, 20), (3, 30, 30)",
                       schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (1, 10, 5), (2, 10, 25)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW corr AS SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.k = a.k) AS c FROM a",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW unco AS SELECT a.id FROM a WHERE a.v < (SELECT MAX(w) FROM b)",
        schema_name=sn)

    assert bag(scanned(client, sn, "corr"), "id", "c") == {(1, 2): 1, (2, 0): 1, (3, 0): 1}
    assert bag(scanned(client, sn, "unco"), "id") == {(1,): 1, (2,): 1}


# ---------------------------------------------------------------------------
# ANY / ALL: the same machinery under a quantifier
# ---------------------------------------------------------------------------


def test_eq_any_and_ne_all_are_in_and_not_in(client, schema_name):
    """`x = ANY (...)` is set membership and `x <> ALL (...)` its complement, so
    both lower onto the semi/anti shape rather than onto an extremum."""
    sn = schema_name
    _ab(client, sn)
    client.execute_sql(
        "CREATE VIEW any_eq AS SELECT a.v FROM a WHERE a.k = ANY (SELECT k FROM b)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW all_ne AS SELECT a.v FROM a WHERE a.k <> ALL (SELECT k FROM b)",
        schema_name=sn)

    client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (1, 20, 5)", schema_name=sn)
    assert bag(scanned(client, sn, "any_eq"), "v") == {(200,): 1}
    assert bag(scanned(client, sn, "all_ne"), "v") == {(100,): 1}


def test_an_ordering_quantifier_becomes_an_extremum_and_an_existence_test(
        client, schema_name):
    """`v < ANY (group)` is `v < MAX(group)` and `v >= ALL (group)` is
    `v >= MAX(group)`, but the two disagree on the empty group — ANY(∅) is FALSE
    and ALL(∅) TRUE — so an existence conjunct rides alongside the comparison.
    That conjunct is what makes the empty group a *definite* FALSE rather than
    UNKNOWN, so negating an ANY over it yields TRUE."""
    sn = schema_name
    _ab(client, sn)
    for name, pred in (
        ("any_lt", "a.v < ANY (SELECT w FROM b WHERE b.k = a.k)"),
        ("all_ge", "a.v >= ALL (SELECT w FROM b WHERE b.k = a.k)"),
        ("not_any", "NOT (a.v < ANY (SELECT w FROM b WHERE b.k = a.k))"),
    ):
        client.execute_sql(f"CREATE VIEW {name} AS SELECT a.id FROM a WHERE {pred}",
                           schema_name=sn)

    client.execute_sql(
        "INSERT INTO a VALUES (1, 10, 5), (2, 10, 50), (3, 20, 1), (4, 10, 25)", schema_name=sn)
    # Every group is empty: ANY is FALSE, ALL and NOT-ANY are TRUE.
    assert bag(scanned(client, sn, "any_lt"), "id") == {}
    assert bag(scanned(client, sn, "all_ge"), "id") == {(1,): 1, (2,): 1, (3,): 1, (4,): 1}
    assert bag(scanned(client, sn, "not_any"), "id") == {(1,): 1, (2,): 1, (3,): 1, (4,): 1}

    # k = 10 now maxes at 20; k = 20 stays empty.
    client.execute_sql("INSERT INTO b VALUES (1, 10, 8), (2, 10, 20)", schema_name=sn)
    assert bag(scanned(client, sn, "any_lt"), "id") == {(1,): 1}, "only v = 5 is under MAX 20"
    assert bag(scanned(client, sn, "all_ge"), "id") == {(2,): 1, (3,): 1, (4,): 1}, \
        "the empty k = 20 group is TRUE"
    assert bag(scanned(client, sn, "not_any"), "id") == {(2,): 1, (3,): 1, (4,): 1}


def test_an_uncorrelated_ordering_quantifier_reads_the_global_extremum(
        client, schema_name):
    """`v < ANY (SELECT w FROM b)` with no correlation is `v < MAX(w)` over the
    whole relation, and an empty relation is still a definite FALSE."""
    sn = schema_name
    _ab(client, sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.id FROM a WHERE a.v < ANY (SELECT w FROM b)", schema_name=sn)

    client.execute_sql("INSERT INTO a VALUES (1, 1, 10), (2, 2, 20), (3, 3, 30)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id") == {}
    client.execute_sql("INSERT INTO b VALUES (1, 1, 25)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id") == {(1,): 1, (2,): 1}


# ---------------------------------------------------------------------------
# What the lowering refuses
# ---------------------------------------------------------------------------

_REFUSED = [
    # The subquery must fold to exactly one row per outer row.
    "SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.k=a.k GROUP BY b.w) FROM a",
    "SELECT a.id, (SELECT b.w FROM b WHERE b.k=a.k) FROM a",
    # The correlation must be an equality: a range one has no group key.
    "SELECT a.id, (SELECT COUNT(*) FROM b WHERE b.k < a.k) FROM a",
    # An uncorrelated scalar is admissible only in a comparison, and only in one
    # the join can key on.
    "SELECT a.id, (SELECT COUNT(*) FROM b) FROM a",
    "SELECT a.id FROM a WHERE a.v <> (SELECT COUNT(*) FROM b)",
    # The inner relation is one relation, not a derivation.
    "SELECT a.id, (SELECT COUNT(*) FROM b JOIN a a2 ON b.k=a2.k WHERE b.k=a.k) FROM a",
    # A JOIN in the outer FROM leaves no slot to join the reduce back onto.
    "SELECT a.id FROM a JOIN b ON a.k=b.k WHERE a.v < (SELECT MAX(w) FROM b b2)",
    # Only the quantifier forms that reduce to one extremum are implemented.
    "SELECT a.v FROM a WHERE a.k = ALL (SELECT k FROM b)",
    "SELECT a.v FROM a WHERE a.k <> ANY (SELECT k FROM b)",
    "SELECT a.v FROM a WHERE a.v < ALL (SELECT w FROM b)",
]


@pytest.mark.parametrize("body", _REFUSED, ids=range(len(_REFUSED)))
def test_a_subquery_the_lowering_cannot_express_is_refused(client, schema_name, body):
    """Each of these would need a shape the rewrite does not build, so it must
    error rather than compile to a body answering a different question. A refused
    CREATE leaves no partial chain behind — the same name is free afterwards."""
    sn = schema_name
    _ab(client, sn)
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql(f"CREATE VIEW bad AS {body}", schema_name=sn)
    client.execute_sql("CREATE VIEW bad AS SELECT id FROM a", schema_name=sn)
