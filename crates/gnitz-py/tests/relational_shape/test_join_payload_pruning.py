"""What a join carries: the pruned reindex payload.

A source column no join view ever reads is dropped from the reindex payload and
from the join trace. That has to be *behaviourally invisible* — same rows, same
weights as an unpruned view — so every test here drives it through a case where
it could stop being invisible, and asserts weights rather than row presence.

Three of those cases are where the prune could change the answer:

  * dropping a column makes two preserved rows COINCIDE, so an outer join's
    null-fill `ν` becomes bag-valued and the `positive_part` has to subtract the
    raw matched multiplicity rather than a clamped witness;
  * a NULLable join key must stay in the payload, because NULL and a real 0 (or
    the empty string) pack to the same synthetic key, and a matched real-keyed
    row would then cancel the NULL-keyed row's unmatched weight;
  * range, band and cross joins pack their output PK out of the payload, so each
    side's PK columns are pinned there even when the SELECT names none of them.
"""
import pytest
from _read import bag, scanned


def test_an_inner_join_drops_the_keys_and_the_columns_nothing_reads(client, schema_name):
    """The SELECT names neither join key and three of the five payload columns.
    A view backfilled over the already-populated tables prunes identically, so
    both paths agree."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE fact (pk BIGINT PRIMARY KEY, k BIGINT NOT NULL, a BIGINT NOT NULL, "
        "b BIGINT NOT NULL, c BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE dim (pk BIGINT PRIMARY KEY, name BIGINT NOT NULL, extra BIGINT NOT NULL)",
        schema_name=sn)
    body = "SELECT fact.a, dim.name FROM fact JOIN dim ON fact.k = dim.pk"
    client.execute_sql(f"CREATE VIEW maintained AS {body}", schema_name=sn)

    client.execute_sql("INSERT INTO dim VALUES (10, 111, 999), (20, 222, 888)", schema_name=sn)
    client.execute_sql(
        "INSERT INTO fact VALUES (1, 10, 5, 6, 7), (2, 20, 8, 9, 1), (3, 10, 50, 60, 70)",
        schema_name=sn)
    client.execute_sql(f"CREATE VIEW backfilled AS {body}", schema_name=sn)

    want = {(5, 111): 1, (8, 222): 1, (50, 111): 1}
    for name in ("maintained", "backfilled"):
        assert bag(scanned(client, sn, name), "a", "name") == want, name


def test_preserved_rows_that_coincide_under_the_prune_null_fill_at_their_bag_weight(
        client, schema_name):
    """Two preserved rows agreeing on the key and every KEPT column differ only
    in a dropped one, so after the prune they are one identity at weight 2. A
    single matching right row joins both, and the null-fill has to cancel
    exactly: `positive_part(2 - 2) = 0`. Clamping the witness to 1 instead would
    leak a weight-1 null-fill beside the matched rows."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE l (pk BIGINT PRIMARY KEY, k BIGINT NOT NULL, kept BIGINT NOT NULL, "
        "dropped BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE r (pk BIGINT PRIMARY KEY, k BIGINT NOT NULL, rval BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT l.kept, r.rval FROM l LEFT JOIN r ON l.k = r.k", schema_name=sn)

    client.execute_sql("INSERT INTO l VALUES (1, 7, 9, 100), (2, 7, 9, 200)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "kept", "rval") == {(9, None): 2}

    client.execute_sql("INSERT INTO r VALUES (10, 7, 555)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "kept", "rval") == {(9, 555): 2}

    client.execute_sql("DELETE FROM r WHERE pk = 10", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "kept", "rval") == {(9, None): 2}


@pytest.mark.parametrize("key_type,collider", [("BIGINT", "0"), ("TEXT", "''")])
def test_a_nullable_join_key_stays_in_the_payload(client, schema_name, key_type, collider):
    """A NULL key and a real one that packs to the same synthetic key: 0 for an
    integer, the empty string for a TEXT (both hash to the empty content). The
    key column and its null bit have to be retained, or the real-keyed row's
    matched multiplicity would coarsen with the NULL-keyed row and cancel the
    null-fill it is owed — silently, since both project to the same `x`."""
    sn = schema_name
    client.execute_sql(
        f"CREATE TABLE l (pk BIGINT PRIMARY KEY, k {key_type}, x BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        f"CREATE TABLE r (pk BIGINT PRIMARY KEY, k {key_type} NOT NULL, rval BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT l.x, r.rval FROM l LEFT JOIN r ON l.k = r.k", schema_name=sn)
    client.execute_sql(f"INSERT INTO l VALUES (1, NULL, 5), (2, {collider}, 5)", schema_name=sn)
    # Two right rows on the colliding key, so the real-keyed left row matches twice.
    client.execute_sql(f"INSERT INTO r VALUES (10, {collider}, 100), (11, {collider}, 200)",
                       schema_name=sn)

    assert bag(scanned(client, sn, "v"), "x", "rval") == \
        {(5, None): 1, (5, 100): 1, (5, 200): 1}


# ── the shapes that pack their output PK out of the payload ────────────────


def _ra_rb(client, sn, *, late_pk=False):
    """`ra` and `rb`, whose PKs no test below projects. With `late_pk` the PK is
    declared last, so the pinned PK is not source column 0."""
    ra_cols = ("nm BIGINT NOT NULL, k BIGINT NOT NULL, lo BIGINT NOT NULL, x BIGINT NOT NULL, "
               "id BIGINT PRIMARY KEY") if late_pk else (
        "id BIGINT PRIMARY KEY, k BIGINT NOT NULL, lo BIGINT NOT NULL, x BIGINT NOT NULL, "
        "nm BIGINT NOT NULL")
    client.execute_sql(f"CREATE TABLE ra ({ra_cols})", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE rb (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, hi BIGINT NOT NULL, "
        "y BIGINT NOT NULL, junk BIGINT NOT NULL)", schema_name=sn)


def _fill_ra_rb(client, sn, *, late_pk=False):
    rows = "(7, 10, 1, 70, 1), (8, 20, 9, 80, 2)" if late_pk \
        else "(1, 10, 1, 70, 7), (2, 20, 9, 80, 8)"
    client.execute_sql(f"INSERT INTO ra VALUES {rows}", schema_name=sn)
    client.execute_sql("INSERT INTO rb VALUES (5, 10, 5, 500, 0), (6, 30, 5, 600, 0)",
                       schema_name=sn)


@pytest.mark.parametrize("on,want", [
    # band: ra(1) alone shares rb(5)'s k and beats its hi.
    ("JOIN rb ON ra.k = rb.k AND ra.lo < rb.hi", {(70, 500): 1}),
    ("LEFT JOIN rb ON ra.k = rb.k AND ra.lo < rb.hi", {(70, 500): 1, (80, None): 1}),
    # pure range: ra(1)'s lo beats both rb rows' hi, ra(2)'s beats neither.
    ("JOIN rb ON ra.lo < rb.hi", {(70, 500): 1, (70, 600): 1}),
    ("LEFT JOIN rb ON ra.lo < rb.hi", {(70, 500): 1, (70, 600): 1, (80, None): 1}),
    ("CROSS JOIN rb", {(70, 500): 1, (70, 600): 1, (80, 500): 1, (80, 600): 1}),
], ids=["band", "band_left", "range", "range_left", "cross"])
def test_a_pair_pk_shape_pins_both_sides_pks_in_the_payload(client, schema_name, on, want):
    """These shapes key their output on the two source PKs, so those columns are
    part of the keep demand however narrow the SELECT is — while the join keys
    the SELECT does not read are still dropped."""
    sn = schema_name
    _ra_rb(client, sn)
    client.execute_sql(f"CREATE VIEW v AS SELECT ra.x, rb.y FROM ra {on}", schema_name=sn)
    _fill_ra_rb(client, sn)

    assert bag(scanned(client, sn, "v"), "x", "y") == want


def test_a_pinned_pk_need_not_be_the_first_source_column(client, schema_name):
    """The pinned PK is pulled to the front of the kept payload, so a PK declared
    last must still key the pure-range threshold re-key and the null-fill."""
    sn = schema_name
    _ra_rb(client, sn, late_pk=True)
    client.execute_sql("CREATE VIEW v AS SELECT ra.nm, rb.y FROM ra LEFT JOIN rb ON ra.lo < rb.hi",
                       schema_name=sn)
    _fill_ra_rb(client, sn, late_pk=True)

    assert bag(scanned(client, sn, "v"), "nm", "y") == \
        {(7, 500): 1, (7, 600): 1, (8, None): 1}


# ── correlations, which read nothing of the inner side but the match ───────


def test_a_correlation_keeps_no_inner_payload_at_all(client, schema_name):
    """EXISTS / NOT EXISTS / IN read only whether the inner matched, so every
    inner column but the correlation key is dead. Weight is the contract: two
    outer rows that coincide on the projected column report weight 2, and the
    three inner rows on one key must be absorbed by the clamp rather than
    multiplied through."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE o (pk BIGINT PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL, "
        "w BIGINT NOT NULL, x BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE i (pk BIGINT PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL, "
        "j1 BIGINT NOT NULL, j2 BIGINT NOT NULL, j3 TEXT NOT NULL)", schema_name=sn)
    # o(1) and o(2) share every column, so a correct semi-join reports weight 2.
    client.execute_sql(
        "INSERT INTO o VALUES (1, 10, 5, 100, 7), (2, 10, 5, 100, 7), (3, 20, 50, 200, 9)",
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO i VALUES (1, 10, 1, 0, 0, 'aaa'), (2, 10, 2, 0, 0, 'bbb'), "
        "(3, 10, 3, 0, 0, 'ccc'), (4, 30, 400, 0, 0, 'ddd')", schema_name=sn)

    for name, where, want in (
        ("ex", "EXISTS (SELECT 1 FROM i WHERE i.k = o.k)", {(7,): 2}),
        ("nex", "NOT EXISTS (SELECT 1 FROM i WHERE i.k = o.k)", {(9,): 1}),
        ("inlist", "o.k IN (SELECT i.k FROM i)", {(7,): 2}),
    ):
        client.execute_sql(f"CREATE VIEW {name} AS SELECT o.x FROM o WHERE {where}",
                           schema_name=sn)
        assert bag(scanned(client, sn, name), "x") == want, name


def test_a_not_exists_pins_its_outers_nullable_correlation_key(client, schema_name):
    """NOT EXISTS builds a `ν` over the OUTER side, so the same rule-3 collision
    applies there: a NULL correlation key and a real 0 pack to the same synthetic
    PK, and the matched row's multiplicity would cancel the NULL-keyed row's
    unmatched weight. Both outer rows carry the same `x`, so only the retained
    key column keeps them apart."""
    sn = schema_name
    client.execute_sql("CREATE TABLE o (pk BIGINT PRIMARY KEY, k BIGINT, x BIGINT NOT NULL)",
                       schema_name=sn)
    client.execute_sql("CREATE TABLE i (pk BIGINT PRIMARY KEY, k BIGINT NOT NULL)",
                       schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT o.x FROM o WHERE NOT EXISTS (SELECT 1 FROM i WHERE i.k = o.k)",
        schema_name=sn)
    client.execute_sql("INSERT INTO o VALUES (1, NULL, 5), (2, 0, 5)", schema_name=sn)
    client.execute_sql("INSERT INTO i VALUES (10, 0), (11, 0)", schema_name=sn)

    # The NULL-keyed row matches nothing, so exactly one row survives.
    assert bag(scanned(client, sn, "v"), "x") == {(5,): 1}


@pytest.mark.parametrize("corr,want", [
    ("i.k = o.k", [1, 2]),
    ("i.k = o.k AND i.v < o.v", [1, 2]),
    ("i.v < o.v", [1]),
])
def test_a_mark_joins_where_columns_are_part_of_the_keep_demand(client, schema_name, corr, want):
    """A mark join applies its WHERE per branch, *after* the mark, so the WHERE's
    columns must survive the prune even though the prefilter's need not. `o.w` is
    read only by that WHERE, `o.k`/`o.v` only by the correlation, and neither is
    projected — over all three correlation shapes."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE o (pk BIGINT PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL, "
        "w BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql(
        "CREATE TABLE i (pk BIGINT PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        f"CREATE VIEW v AS SELECT o.pk FROM o WHERE NOT (EXISTS "
        f"(SELECT 1 FROM i WHERE {corr}) AND o.w > 150)", schema_name=sn)
    client.execute_sql("INSERT INTO o VALUES (1, 10, 5, 100), (2, 20, 50, 200), (3, 10, 500, 300)",
                       schema_name=sn)
    client.execute_sql("INSERT INTO i VALUES (1, 10, 1), (2, 30, 400)", schema_name=sn)

    assert bag(scanned(client, sn, "v"), "pk") == {(pk,): 1 for pk in want}
