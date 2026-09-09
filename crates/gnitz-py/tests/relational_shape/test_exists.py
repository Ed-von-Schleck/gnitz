"""`[NOT] EXISTS` and `x [NOT] IN (SELECT ...)` view bodies.

There is no anti-join operator: a semi-join emits the preserved row at
`w_A · [S > 0]` and an anti-join at `w_A · [S = 0]`, where `S` is the summed
weight of the inner rows it matches. Output weight therefore depends on match
*existence*, not on weight arithmetic, and the failure a row-set check cannot
see is a second inner match doubling an outer row. Every assertion here is a
weighted bag.

Two lowerings share the file. A subquery that is a top-level `AND` conjunct of
the WHERE becomes the semi/anti shape directly; one in any other boolean
position — under OR, under NOT, inside CASE, or projected as a column — is
rewritten to a 0/1 **mark** that ordinary expression evaluation consumes.

Run with GNITZ_WORKERS=4: the correlation key is an exchange key.
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


def _semi_anti(client, sn, correlation, source="a"):
    """The `semi` / `anti` view pair over `source`, both selecting `v` and
    correlating the inner `b` by `correlation`."""
    for name, polarity in (("semi", "EXISTS"), ("anti", "NOT EXISTS")):
        client.execute_sql(
            f"CREATE VIEW {name} AS SELECT v FROM {source} "
            f"WHERE {polarity} (SELECT 1 FROM b WHERE {correlation})",
            schema_name=sn)


# ---------------------------------------------------------------------------
# The semi/anti shape: a top-level AND conjunct
# ---------------------------------------------------------------------------


def test_a_second_match_never_doubles_the_preserved_row(client, schema_name):
    """The first inner match moves an outer row from the anti output to the semi
    one; a second match for the same key leaves it at weight 1, because the
    output weight is `w_A · [S > 0]` and not `w_A · S`. Retracting down to zero
    matches flips it back."""
    sn = schema_name
    _ab(client, sn)
    _semi_anti(client, sn, "b.k = a.k")

    client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
    assert bag(scanned(client, sn, "semi"), "v") == {}
    assert bag(scanned(client, sn, "anti"), "v") == {(100,): 1, (200,): 1}

    client.execute_sql("INSERT INTO b VALUES (1, 10, 7)", schema_name=sn)
    assert bag(scanned(client, sn, "semi"), "v") == {(100,): 1}
    assert bag(scanned(client, sn, "anti"), "v") == {(200,): 1}

    client.execute_sql("INSERT INTO b VALUES (2, 10, 8)", schema_name=sn)
    assert bag(scanned(client, sn, "semi"), "v") == {(100,): 1}, \
        "a second match must not double the row"
    assert bag(scanned(client, sn, "anti"), "v") == {(200,): 1}

    client.execute_sql("DELETE FROM b WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "semi"), "v") == {(100,): 1}, \
        "one of two matches gone: still matched"

    client.execute_sql("DELETE FROM b WHERE id = 2", schema_name=sn)
    assert bag(scanned(client, sn, "semi"), "v") == {}
    assert bag(scanned(client, sn, "anti"), "v") == {(100,): 1, (200,): 1}


def test_the_preserved_side_keeps_its_bag_multiplicity(client, schema_name):
    """A weight-2 outer element stays weight-2 in both outputs. The preserved
    side is a `UNION ALL` view whose two base rows carry one (k, v), so it is
    genuinely bag-valued; clamping the match *witness* to 1 rather than the
    result of the weight-exact subtraction would leak a weight-1 null-fill, and
    multiplying by the two matches would give 4."""
    sn = schema_name
    for name in ("a1", "a2"):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
            "v BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(_B, schema_name=sn)
    client.execute_sql(
        "CREATE VIEW bagv AS SELECT k, v FROM a1 UNION ALL SELECT k, v FROM a2", schema_name=sn)
    _semi_anti(client, sn, "b.k = bagv.k", source="bagv")

    client.execute_sql("INSERT INTO a1 VALUES (1, 10, 100)", schema_name=sn)
    client.execute_sql("INSERT INTO a2 VALUES (1, 10, 100)", schema_name=sn)
    assert bag(scanned(client, sn, "anti"), "v") == {(100,): 2}
    assert bag(scanned(client, sn, "semi"), "v") == {}

    client.execute_sql("INSERT INTO b VALUES (1, 10, 5), (2, 10, 6)", schema_name=sn)
    assert bag(scanned(client, sn, "semi"), "v") == {(100,): 2}, \
        "two matches must not multiply the weight"
    assert bag(scanned(client, sn, "anti"), "v") == {}

    client.execute_sql("DELETE FROM b WHERE id IN (1, 2)", schema_name=sn)
    assert bag(scanned(client, sn, "semi"), "v") == {}
    assert bag(scanned(client, sn, "anti"), "v") == {(100,): 2}


def test_in_and_not_in_lower_to_the_same_shape(client, schema_name):
    """`k IN (SELECT k FROM b)` is the semi-join and `NOT IN` the anti-join, over
    the same correlation an explicit EXISTS spells out."""
    sn = schema_name
    _ab(client, sn)
    client.execute_sql(
        "CREATE VIEW vin AS SELECT v FROM a WHERE k IN (SELECT k FROM b)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW vnotin AS SELECT v FROM a WHERE k NOT IN (SELECT k FROM b)", schema_name=sn)

    client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (1, 20, 5)", schema_name=sn)
    assert bag(scanned(client, sn, "vin"), "v") == {(200,): 1}
    assert bag(scanned(client, sn, "vnotin"), "v") == {(100,): 1}

    client.execute_sql("DELETE FROM b WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "vin"), "v") == {}
    assert bag(scanned(client, sn, "vnotin"), "v") == {(100,): 1, (200,): 1}


def test_a_backfill_reaches_the_same_value_as_the_incremental_order(client, schema_name):
    """Created over populated tables, the views are seeded by a backfill scan
    rather than by deltas, and must agree with what the incremental order
    produces — including the duplicate inner match that must not double."""
    sn = schema_name
    _ab(client, sn)
    client.execute_sql(
        "INSERT INTO a VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (1, 10, 5), (2, 10, 6), (3, 30, 7)", schema_name=sn)
    _semi_anti(client, sn, "b.k = a.k")

    assert bag(scanned(client, sn, "semi"), "v") == {(100,): 1, (300,): 1}
    assert bag(scanned(client, sn, "anti"), "v") == {(200,): 1}


def test_a_null_correlation_key_matches_nothing_on_either_side(client, schema_name):
    """A NULL outer key is excluded from EXISTS and included in NOT EXISTS; a
    NULL inner key is a match for nothing, not even for a NULL outer key."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT, v BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT, w BIGINT NOT NULL)",
        schema_name=sn)
    _semi_anti(client, sn, "b.k = a.k")

    client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, NULL, 200)", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (1, NULL, 5)", schema_name=sn)
    assert bag(scanned(client, sn, "semi"), "v") == {}
    assert bag(scanned(client, sn, "anti"), "v") == {(100,): 1, (200,): 1}

    client.execute_sql("INSERT INTO b VALUES (2, 10, 6)", schema_name=sn)
    assert bag(scanned(client, sn, "semi"), "v") == {(100,): 1}
    assert bag(scanned(client, sn, "anti"), "v") == {(200,): 1}


def test_local_conjuncts_filter_each_side_before_the_match(client, schema_name):
    """An outer-local conjunct filters A ahead of everything; an inner-local one
    pre-filters B, so a row failing it is not a match at all rather than a match
    filtered afterwards."""
    sn = schema_name
    _ab(client, sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT v FROM a "
        "WHERE v > 100 AND EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND b.w > 5)",
        schema_name=sn)

    client.execute_sql(
        "INSERT INTO a VALUES (1, 10, 100), (2, 10, 200), (3, 20, 300)", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (1, 10, 5)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "v") == {}, "w = 5 fails the inner pre-filter"

    client.execute_sql("INSERT INTO b VALUES (2, 10, 6)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "v") == {(200,): 1}, \
        "only k = 10 rows with v > 100 survive"


def test_a_compound_correlation_key_matches_on_every_column(client, schema_name):
    """Two correlation columns, the first of them cross-width (INT against
    BIGINT): a row agreeing on one column alone is not a match."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k1 INT NOT NULL, "
        "k2 BIGINT NOT NULL, v BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k1 BIGINT NOT NULL, "
        "k2 BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT v FROM a WHERE EXISTS "
        "(SELECT 1 FROM b WHERE b.k1 = a.k1 AND b.k2 = a.k2)", schema_name=sn)

    client.execute_sql("INSERT INTO a VALUES (1, 7, 70, 100), (2, 7, 71, 200)", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (1, 7, 70)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "v") == {(100,): 1}


def test_select_star_takes_the_identity_projection(client, schema_name):
    """`SELECT *` emits no output map: the view carries every outer column
    verbatim under the synthetic PK. Pinned for all three lowerings — the equi
    shape (no output exchange), the band shape (one), and the mark path, where
    the mark itself is consumed by the WHERE and never materialized."""
    sn = schema_name
    _ab(client, sn)
    for name, pred in (
        ("star_eq", "EXISTS (SELECT 1 FROM b WHERE b.k = a.k)"),
        ("star_bd", "EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND b.w < a.v)"),
        ("star_mk", "v = 999 OR EXISTS (SELECT 1 FROM b WHERE b.k = a.k)"),
    ):
        client.execute_sql(f"CREATE VIEW {name} AS SELECT * FROM a WHERE {pred}", schema_name=sn)

    client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (1, 10, 50)", schema_name=sn)
    for name in ("star_eq", "star_bd", "star_mk"):
        assert bag(scanned(client, sn, name), "id", "k", "v") == {(1, 10, 100): 1}, name


def test_the_sources_may_themselves_be_views(client, schema_name):
    """Both sides are views, and an EXISTS view is itself a source for a further
    one — the semi/anti shape composes wherever a relation is admissible."""
    sn = schema_name
    _ab(client, sn)
    client.execute_sql("CREATE VIEW av AS SELECT * FROM a WHERE v > 0", schema_name=sn)
    client.execute_sql("CREATE VIEW bv AS SELECT * FROM b WHERE w > 0", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW semi AS SELECT v FROM av "
        "WHERE EXISTS (SELECT 1 FROM bv WHERE bv.k = av.k)", schema_name=sn)
    client.execute_sql("CREATE VIEW over AS SELECT v FROM semi WHERE v >= 200", schema_name=sn)

    client.execute_sql(
        "INSERT INTO a VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300)", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (1, 20, 5), (2, 30, 6)", schema_name=sn)
    assert bag(scanned(client, sn, "semi"), "v") == {(200,): 1, (300,): 1}
    assert bag(scanned(client, sn, "over"), "v") == {(200,): 1, (300,): 1}


def test_a_band_correlation_flips_as_the_threshold_crosses(client, schema_name):
    """An equality prefix plus a range conjunct (`b.k = a.k AND b.t < a.t`): a
    row matches once any inner row of its key falls below its threshold, and a
    second such row leaves the weight at 1."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
        "t BIGINT NOT NULL, v BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
        "t BIGINT NOT NULL)", schema_name=sn)
    _semi_anti(client, sn, "b.k = a.k AND b.t < a.t")

    client.execute_sql(
        "INSERT INTO a VALUES (1, 10, 50, 100), (2, 10, 5, 200), (3, 20, 50, 300)",
        schema_name=sn)
    assert bag(scanned(client, sn, "anti"), "v") == {(100,): 1, (200,): 1, (300,): 1}

    # (k=10, t=40): under a1's t=50, not under a2's t=5, wrong key for a3.
    client.execute_sql("INSERT INTO b VALUES (1, 10, 40)", schema_name=sn)
    assert bag(scanned(client, sn, "semi"), "v") == {(100,): 1}
    assert bag(scanned(client, sn, "anti"), "v") == {(200,): 1, (300,): 1}

    client.execute_sql("INSERT INTO b VALUES (2, 10, 45)", schema_name=sn)
    assert bag(scanned(client, sn, "semi"), "v") == {(100,): 1}, \
        "a second band match must not double"

    client.execute_sql("DELETE FROM b WHERE id IN (1, 2)", schema_name=sn)
    assert bag(scanned(client, sn, "semi"), "v") == {}
    assert bag(scanned(client, sn, "anti"), "v") == {(100,): 1, (200,): 1, (300,): 1}


def test_a_pure_range_correlation_collapses_to_one_threshold_row(client, schema_name):
    """With no equality conjunct, `EXISTS (b.y < a.x)` is `a.x > MIN(b.y)`: the
    whole inner side is one extremum row. Retracting the current minimum
    re-derives the threshold from the remaining history, an all-NULL inner side
    behaves as an empty one, and a NULL outer range key is never matched."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT, v BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT)", schema_name=sn)
    _semi_anti(client, sn, "b.y < a.x")

    client.execute_sql(
        "INSERT INTO a VALUES (1, 10, 100), (2, 20, 200), (3, NULL, 300)", schema_name=sn)
    assert bag(scanned(client, sn, "semi"), "v") == {}, "an empty inner side matches nothing"
    assert bag(scanned(client, sn, "anti"), "v") == {(100,): 1, (200,): 1, (300,): 1}

    client.execute_sql("INSERT INTO b VALUES (1, NULL)", schema_name=sn)
    assert bag(scanned(client, sn, "semi"), "v") == {}, "an all-NULL inner side is an empty one"
    assert bag(scanned(client, sn, "anti"), "v") == {(100,): 1, (200,): 1, (300,): 1}

    client.execute_sql("INSERT INTO b VALUES (2, 15)", schema_name=sn)
    assert bag(scanned(client, sn, "semi"), "v") == {(200,): 1}, "only x = 20 sits above y = 15"
    assert bag(scanned(client, sn, "anti"), "v") == {(100,): 1, (300,): 1}

    client.execute_sql("INSERT INTO b VALUES (3, 5)", schema_name=sn)
    assert bag(scanned(client, sn, "semi"), "v") == {(100,): 1, (200,): 1}, \
        "two candidates, weights stay 1"
    assert bag(scanned(client, sn, "anti"), "v") == {(300,): 1}, \
        "the NULL outer key is never matched"

    client.execute_sql("DELETE FROM b WHERE id = 3", schema_name=sn)
    assert bag(scanned(client, sn, "semi"), "v") == {(200,): 1}, "threshold recomputed to 15"
    assert bag(scanned(client, sn, "anti"), "v") == {(100,): 1, (300,): 1}


# ---------------------------------------------------------------------------
# The mark rewrite: one subquery in an arbitrary boolean position
# ---------------------------------------------------------------------------


def test_the_mark_is_an_ordinary_boolean_wherever_it_appears(client, schema_name):
    """One subquery outside the top-level AND becomes a 0/1 mark the expression
    compiler consumes like any other operand — under OR, under NOT, and as a
    `NOT IN` over non-nullable columns. Membership flips incrementally in every
    position."""
    sn = schema_name
    _ab(client, sn)
    for name, pred in (
        ("mk_or", "EXISTS (SELECT 1 FROM b WHERE b.k = a.k) OR v = 100"),
        ("mk_not", "NOT (EXISTS (SELECT 1 FROM b WHERE b.k = a.k) AND v > 150)"),
        ("mk_notin", "v = 100 OR k NOT IN (SELECT k FROM b)"),
    ):
        client.execute_sql(f"CREATE VIEW {name} AS SELECT id FROM a WHERE {pred}", schema_name=sn)

    client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
    assert bag(scanned(client, sn, "mk_or"), "id") == {(1,): 1}, \
        "no match: only v = 100 survives the OR"
    assert bag(scanned(client, sn, "mk_not"), "id") == {(1,): 1, (2,): 1}, "NOT (0 AND …) is TRUE"
    assert bag(scanned(client, sn, "mk_notin"), "id") == {(1,): 1, (2,): 1}, "empty b: NOT IN holds"

    client.execute_sql("INSERT INTO b VALUES (1, 20, 7)", schema_name=sn)
    assert bag(scanned(client, sn, "mk_or"), "id") == {(1,): 1, (2,): 1}, "id 2 now kept via EXISTS"
    assert bag(scanned(client, sn, "mk_not"), "id") == {(1,): 1}, "id 2 matches and v > 150"
    assert bag(scanned(client, sn, "mk_notin"), "id") == {(1,): 1}, "id 2's k is in b"

    client.execute_sql("DELETE FROM b WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "mk_or"), "id") == {(1,): 1}
    assert bag(scanned(client, sn, "mk_not"), "id") == {(1,): 1, (2,): 1}
    assert bag(scanned(client, sn, "mk_notin"), "id") == {(1,): 1, (2,): 1}


def test_a_projected_mark_retracts_its_old_value_when_it_flips(client, schema_name):
    """The mark can be an output column rather than a predicate — bare as
    `EXISTS(...) AS flag`, folded through a searched CASE, and through a simple
    `CASE <operand> WHEN` whose operand is cloned into every branch. A flip must
    retract the old row: the failure is both values of the mark present at once
    for one id, which the bag shows as two entries under that id."""
    sn = schema_name
    _ab(client, sn)
    client.execute_sql(
        "CREATE VIEW mk AS SELECT id, EXISTS (SELECT 1 FROM b WHERE b.k = a.k) AS flag FROM a",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW cs AS SELECT id, "
        "CASE WHEN EXISTS (SELECT 1 FROM b WHERE b.k = a.k) THEN v ELSE 0 END AS flag FROM a",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW op AS SELECT id, "
        "CASE EXISTS (SELECT 1 FROM b WHERE b.k = a.k) "
        "WHEN 1 THEN 10 WHEN 0 THEN 20 END AS flag FROM a", schema_name=sn)

    def flags(name):
        return bag(scanned(client, sn, name), "id", "flag")

    client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
    assert flags("mk") == {(1, 0): 1, (2, 0): 1}
    assert flags("cs") == {(1, 0): 1, (2, 0): 1}
    assert flags("op") == {(1, 20): 1, (2, 20): 1}

    client.execute_sql("INSERT INTO b VALUES (1, 10, 7)", schema_name=sn)
    assert flags("mk") == {(1, 1): 1, (2, 0): 1}
    assert flags("cs") == {(1, 100): 1, (2, 0): 1}
    assert flags("op") == {(1, 10): 1, (2, 20): 1}

    # A second match for the same key changes nothing: the mark is already 1.
    client.execute_sql("INSERT INTO b VALUES (2, 10, 8)", schema_name=sn)
    assert flags("mk") == {(1, 1): 1, (2, 0): 1}
    assert flags("op") == {(1, 10): 1, (2, 20): 1}

    client.execute_sql("DELETE FROM b WHERE id IN (1, 2)", schema_name=sn)
    assert flags("mk") == {(1, 0): 1, (2, 0): 1}
    assert flags("cs") == {(1, 0): 1, (2, 0): 1}
    assert flags("op") == {(1, 20): 1, (2, 20): 1}


def test_band_and_pure_range_correlations_reach_the_mark_position_too(client, schema_name):
    """The mark rewrite is not restricted to the equi correlation: an eq-prefix
    band and a bare range each produce a mark under an OR."""
    sn = schema_name
    _ab(client, sn)
    client.execute_sql(
        "CREATE VIEW band AS SELECT id FROM a "
        "WHERE v = 100 OR EXISTS (SELECT 1 FROM b WHERE b.k = a.k AND b.w < a.v)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW rng AS SELECT id FROM a "
        "WHERE v = 100 OR EXISTS (SELECT 1 FROM b WHERE b.w < a.v)", schema_name=sn)

    client.execute_sql("INSERT INTO a VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
    assert bag(scanned(client, sn, "band"), "id") == {(1,): 1}
    assert bag(scanned(client, sn, "rng"), "id") == {(1,): 1}

    # k = 20, w = 50: the band matches id 2 alone (the key is its own), while
    # the bare range matches both rows. id 1 is already kept by the OR.
    client.execute_sql("INSERT INTO b VALUES (1, 20, 50)", schema_name=sn)
    assert bag(scanned(client, sn, "band"), "id") == {(1,): 1, (2,): 1}
    assert bag(scanned(client, sn, "rng"), "id") == {(1,): 1, (2,): 1}


def test_what_the_subquery_lowerings_refuse(client, schema_name):
    """Each refusal here is a body that would otherwise answer a different
    question than the one asked: a `NOT IN` over a nullable operand (SQL's
    three-valued result is not the anti-join), any nullable `IN` reaching a mark
    position, and a second subquery in one body, which has no mark slot. The
    positive top-level `AND` conjunct over the same nullable column still
    compiles — it is the shape whose semantics the anti-join does implement."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT, v BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, w BIGINT NOT NULL)",
        schema_name=sn)

    with pytest.raises(gnitz.GnitzError, match="NOT NULL"):
        client.execute_sql(
            "CREATE VIEW n1 AS SELECT * FROM a WHERE k NOT IN (SELECT k FROM b)", schema_name=sn)
    for i, body in enumerate((
        "SELECT id FROM a WHERE v = 1 OR k IN (SELECT k FROM b)",
        "SELECT id FROM a WHERE NOT (k IN (SELECT k FROM b))",
        "SELECT id, k IN (SELECT k FROM b) AS f FROM a",
    )):
        with pytest.raises(gnitz.GnitzError):
            client.execute_sql(f"CREATE VIEW m{i} AS {body}", schema_name=sn)
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql(
            "CREATE VIEW two AS SELECT id FROM a WHERE "
            "EXISTS (SELECT 1 FROM b WHERE b.k = a.k) OR "
            "EXISTS (SELECT 1 FROM b WHERE b.w = a.v)", schema_name=sn)

    client.execute_sql(
        "CREATE VIEW ok AS SELECT id FROM a WHERE k IN (SELECT k FROM b)", schema_name=sn)
    client.execute_sql("INSERT INTO a VALUES (1, 10, 1), (2, NULL, 2)", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (1, 10, 5)", schema_name=sn)
    assert bag(scanned(client, sn, "ok"), "id") == {(1,): 1}
