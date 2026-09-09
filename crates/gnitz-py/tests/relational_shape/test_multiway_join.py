"""N-way joins: the left-deep chain of hidden two-way segments.

`a JOIN b JOIN c` is decomposed into `(a ⋈ b) ⋈ c`, where the inner step becomes
a hidden join view — a first-class relation whose registered schema already
carries its `_join_pk`, so the outer step resolves its columns like any other
relation and needs no rule for the synthetic key. A provenance map carries the
original aliases forward, so a later ON, WHERE or projection naming `a.x`
resolves to the accumulated segment's physical column. Writing the segments by
hand as CTEs or derived tables produces the same chain.

Each segment projects only the columns something above it still reads. That is
invisible in the result by construction, so the tests here drive it through the
cases where it could stop being invisible: a column only a later step reads, a
chain wide enough that pruning is what keeps it under the column cap, and an
outer step whose null-fill sits inside a hidden segment.
"""
import pytest
from _read import bag, scanned


def _abc(client, sn, names=("a", "b", "c")):
    """`(id PK, k, v)` tables — the chain links `x.k` to the next relation's PK."""
    for name in names:
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
            schema_name=sn)


# A hand-written segment against the direct form. The three spellings of the
# segment itself (CTE, derived table, inline) are one bound tree, stated once in
# the cut-rule suite, so only one of them needs to appear here.
_THREE_WAY = {
    "nested_cte":
        "WITH h0 AS (SELECT a.id AS aid, b.k AS bk FROM a JOIN b ON a.k = b.id) "
        "SELECT h0.aid AS aid, c.v AS cv FROM h0 JOIN c ON h0.bk = c.id",
    "direct":
        "SELECT a.id AS aid, c.v AS cv FROM a JOIN b ON a.k = b.id JOIN c ON b.k = c.id",
}


@pytest.mark.parametrize("body", list(_THREE_WAY.values()), ids=list(_THREE_WAY))
def test_every_spelling_of_a_three_way_join_is_one_left_deep_chain(client, schema_name, body):
    """A hand-written segment and the direct form compile to the same chain: a delta at the head flows through both steps, and
    retracting the bridge row in the middle relation retracts everything the
    chain derived from it. A view created over the populated tables backfills to
    the value the maintained one holds."""
    sn = schema_name
    _abc(client, sn)
    client.execute_sql(f"CREATE VIEW v AS {body}", schema_name=sn)

    client.execute_sql("INSERT INTO b VALUES (10, 500, 0)", schema_name=sn)
    client.execute_sql("INSERT INTO c VALUES (500, 0, 99)", schema_name=sn)
    client.execute_sql("INSERT INTO a VALUES (1, 10, 7)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "aid", "cv") == {(1, 99): 1}

    client.execute_sql("INSERT INTO a VALUES (2, 10, 8)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "aid", "cv") == {(1, 99): 1, (2, 99): 1}

    client.execute_sql("DELETE FROM b WHERE id = 10", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "aid", "cv") == {}

    client.execute_sql("INSERT INTO b VALUES (10, 500, 0)", schema_name=sn)
    client.execute_sql(f"CREATE VIEW late AS {body}", schema_name=sn)
    assert bag(scanned(client, sn, "late"), "aid", "cv") == \
        bag(scanned(client, sn, "v"), "aid", "cv") == {(1, 99): 1, (2, 99): 1}


def test_a_later_step_reaches_every_relation_below_it(client, schema_name):
    """The second ON keys on `a.v`, which sits deep in the accumulator rather
    than in the relation just joined — provenance is what resolves it. The final
    projection pulls one column from each of the three relations, and a WHERE
    over the whole chain folds into the last segment's residual."""
    sn = schema_name
    _abc(client, sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.v AS av, b.v AS bv, c.v AS cv "
        "FROM a JOIN b ON a.k = b.id JOIN c ON a.v = c.id WHERE c.v > 50", schema_name=sn)

    client.execute_sql("INSERT INTO b VALUES (10, 0, 11)", schema_name=sn)
    client.execute_sql("INSERT INTO c VALUES (700, 0, 88), (800, 0, 10)", schema_name=sn)
    # a(1) chains to b(10) and, by a.v, to c(700) whose v passes the WHERE;
    # a(2) reaches c(800), which the WHERE drops.
    client.execute_sql("INSERT INTO a VALUES (1, 10, 700), (2, 10, 800)", schema_name=sn)

    assert bag(scanned(client, sn, "v"), "av", "bv", "cv") == {(700, 11, 88): 1}


def test_the_intermediate_join_pk_never_accumulates(client, schema_name):
    """`SELECT *` marks every column live, so nothing is pruned — and the
    physical schema still carries exactly ONE `_join_pk`, the final segment's.
    Each step's key replaces the accumulator's rather than being appended to it."""
    sn = schema_name
    _abc(client, sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT * FROM a JOIN b ON a.k = b.id JOIN c ON b.k = c.id",
        schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (10, 500, 0)", schema_name=sn)
    client.execute_sql("INSERT INTO c VALUES (500, 0, 99)", schema_name=sn)
    client.execute_sql("INSERT INTO a VALUES (1, 10, 7)", schema_name=sn)

    raw = list(client.scan(client.resolve_table(sn, "v")[0], include_hidden=True))
    assert len(raw) == 1 and raw[0].weight == 1, raw
    names = list(raw[0]._asdict())
    assert names.count("_join_pk") == 1, names


def test_a_cte_alias_list_names_the_visible_columns_only(client, schema_name):
    """A JOIN body's synthetic PK is hidden, so a positional alias list on the
    CTE that wraps it lines up with the two *visible* projected columns and skips
    the key. A downstream ON and projection then resolve them by the new names."""
    sn = schema_name
    _abc(client, sn)
    client.execute_sql(
        "CREATE VIEW v AS WITH h0(x, y) AS "
        "(SELECT a.id AS aid, b.k AS bk FROM a JOIN b ON a.k = b.id) "
        "SELECT h0.x AS x FROM h0 JOIN c ON h0.y = c.id", schema_name=sn)
    client.execute_sql("INSERT INTO a VALUES (1, 10, 1)", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (10, 20, 2)", schema_name=sn)
    client.execute_sql("INSERT INTO c VALUES (20, 0, 3)", schema_name=sn)

    assert bag(scanned(client, sn, "v"), "x") == {(1,): 1}


def test_an_outer_step_null_fills_wherever_it_sits_in_the_chain(client, schema_name):
    """A chain's steps are ordered syntactically, so `a LEFT JOIN b JOIN c` is
    `(a LEFT JOIN b) JOIN c`: a preserved `a` row with no `b` still reaches `c`
    through an `a` column. At the last step the null-fill is the chain's own
    output, and a later delta that first satisfies a preserved row retracts its
    null-fill and emits the match."""
    sn = schema_name
    _abc(client, sn)
    client.execute_sql(
        "CREATE VIEW first_step AS SELECT a.id AS aid, b.id AS bid, c.v AS cv "
        "FROM a LEFT JOIN b ON a.k = b.id JOIN c ON a.v = c.id", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW last_step AS SELECT a.id AS aid, c.id AS cid "
        "FROM a JOIN b ON a.k = b.id LEFT JOIN c ON b.k = c.id", schema_name=sn)

    client.execute_sql("INSERT INTO b VALUES (10, 999, 0)", schema_name=sn)
    client.execute_sql("INSERT INTO c VALUES (700, 0, 88)", schema_name=sn)
    # a(1) matches b; a(2) does not, but both reach c(700) by a.v.
    client.execute_sql("INSERT INTO a VALUES (1, 10, 700), (2, 99, 700)", schema_name=sn)
    assert bag(scanned(client, sn, "first_step"), "aid", "bid", "cv") == \
        {(1, 10, 88): 1, (2, None, 88): 1}
    # Only a(1) reaches the last step, and b(10).k = 999 has no c yet.
    assert bag(scanned(client, sn, "last_step"), "aid", "cid") == {(1, None): 1}

    client.execute_sql("INSERT INTO c VALUES (999, 0, 0)", schema_name=sn)
    assert bag(scanned(client, sn, "last_step"), "aid", "cid") == {(1, 999): 1}, \
        "the null-fill is retracted and the match emitted in its place"


def test_a_null_filled_row_carries_a_null_key_into_the_next_inner_step(client, schema_name):
    """A FULL step mid-chain preserves rows of both sides. The right-only
    null-fill's `a` columns are NULL, so the key the next INNER step reads from
    them is NULL and matches nothing — which is what SQL requires and what makes
    an outer step composable with an inner one."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, dead BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql("CREATE TABLE b (id BIGINT PRIMARY KEY, bv BIGINT NOT NULL)",
                       schema_name=sn)
    client.execute_sql("CREATE TABLE c (id BIGINT PRIMARY KEY, cv BIGINT NOT NULL)",
                       schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.k AS ak, b.bv AS bbv, c.cv AS ccv "
        "FROM a FULL JOIN b ON a.k = b.id JOIN c ON a.k = c.id", schema_name=sn)

    client.execute_sql("INSERT INTO b VALUES (10, 500), (99, 600)", schema_name=sn)
    client.execute_sql("INSERT INTO c VALUES (10, 88), (5, 77)", schema_name=sn)
    client.execute_sql("INSERT INTO a VALUES (1, 10, 111), (2, 5, 222)", schema_name=sn)
    # a(1) matches b(10) and c(10); a(2) has no b so bbv null-fills, and c(5) matches.
    # b(99)'s right-only null-fill has a NULL a.k, so the inner step to c drops it.
    assert bag(scanned(client, sn, "v"), "ak", "bbv", "ccv") == \
        {(10, 500, 88): 1, (5, None, 77): 1}

    client.execute_sql("DELETE FROM a WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "ak", "bbv", "ccv") == {(5, None, 77): 1}


def test_an_update_to_a_mid_chain_key_reroutes_the_row(client, schema_name):
    """Re-keying the head relation moves the row onto a different path through
    the chain: the old chain's output is retracted and the new one emitted, in
    one epoch."""
    sn = schema_name
    _abc(client, sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.id AS aid, c.v AS cv "
        "FROM a JOIN b ON a.k = b.id JOIN c ON b.k = c.id", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (10, 500, 0), (20, 600, 0)", schema_name=sn)
    client.execute_sql("INSERT INTO c VALUES (500, 0, 99), (600, 0, 88)", schema_name=sn)
    client.execute_sql("INSERT INTO a VALUES (1, 10, 7)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "aid", "cv") == {(1, 99): 1}

    client.execute_sql("UPDATE a SET k = 20 WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "aid", "cv") == {(1, 88): 1}


def test_a_range_step_inside_a_chain(client, schema_name):
    """A chain step need not be an equijoin: a `<` in the second ON compiles to
    the range shape and still composes with the equi segment below it."""
    sn = schema_name
    _abc(client, sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.id AS aid, c.id AS cid "
        "FROM a JOIN b ON a.k = b.id JOIN c ON a.v < c.k", schema_name=sn)
    client.execute_sql("INSERT INTO b VALUES (10, 0, 0)", schema_name=sn)
    client.execute_sql("INSERT INTO c VALUES (100, 50, 0), (200, 5, 0)", schema_name=sn)
    client.execute_sql("INSERT INTO a VALUES (1, 10, 10)", schema_name=sn)

    assert bag(scanned(client, sn, "v"), "aid", "cid") == {(1, 100): 1}


def test_an_eight_way_chain_compiles_and_maintains(client, schema_name):
    """Seven hidden segments stacked: the decomposition is not depth-limited, and
    the accumulator stays narrow enough at every step to compile."""
    sn = schema_name
    n = 8
    _abc(client, sn, [f"t{i}" for i in range(n)])
    joins = " ".join(f"JOIN t{i} ON t{i - 1}.k = t{i}.id" for i in range(1, n))
    client.execute_sql(
        f"CREATE VIEW v AS SELECT t0.id AS x, t{n - 1}.v AS y FROM t0 {joins}", schema_name=sn)
    # One chain: every t(i).k is 1, which is t(i+1)'s id.
    for i in range(n):
        client.execute_sql(f"INSERT INTO t{i} VALUES (1, {1 if i < n - 1 else 0}, {100 + i})",
                           schema_name=sn)

    assert bag(scanned(client, sn, "v"), "x", "y") == {(1, 100 + n - 1): 1}


def test_a_column_only_a_later_step_reads_survives_the_intermediate(client, schema_name):
    """The keep demand is what anything ABOVE a segment reads, not what the final
    SELECT names: `a.v` keys the second step and `a.p`/`b.q` feed the first
    step's residual, none of them projected. A column nothing reads is dropped,
    which an UPDATE to it must therefore leave the result alone."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (id BIGINT PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL, "
        "p BIGINT NOT NULL, dead BIGINT NOT NULL)", schema_name=sn)
    client.execute_sql("CREATE TABLE b (id BIGINT PRIMARY KEY, q BIGINT NOT NULL)",
                       schema_name=sn)
    client.execute_sql("CREATE TABLE c (id BIGINT PRIMARY KEY, cv BIGINT NOT NULL)",
                       schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.id AS aid, c.cv AS ccv "
        "FROM a JOIN b ON a.k = b.id AND a.p > b.q JOIN c ON a.v = c.id", schema_name=sn)

    client.execute_sql("INSERT INTO b VALUES (10, 4)", schema_name=sn)
    client.execute_sql("INSERT INTO c VALUES (700, 88), (800, 77)", schema_name=sn)
    # a(1) passes the residual (p=5 > q=4) and reaches c(700); a(2) fails it (p=3).
    client.execute_sql(
        "INSERT INTO a VALUES (1, 10, 700, 5, 999), (2, 10, 800, 3, 0)", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "aid", "ccv") == {(1, 88): 1}

    client.execute_sql("UPDATE a SET dead = 12345 WHERE id = 1", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "aid", "ccv") == {(1, 88): 1}

    client.execute_sql("DELETE FROM c WHERE id = 700", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "aid", "ccv") == {}


def test_pruning_is_what_keeps_a_wide_chain_under_the_column_cap(client, schema_name):
    """Four 18-column tables and a two-column SELECT. Carrying every source
    column forward would put the last segment at 1 + 55 + 18 = 74 columns, over
    the 65-column cap, and the view would be rejected; pruned to its live columns
    the accumulator is three wide and the chain compiles."""
    sn = schema_name
    pads = ", ".join(f"p{i} BIGINT NOT NULL" for i in range(16))
    for table, key in (("a", "k1"), ("b", "k2"), ("c", "k3"), ("d", "val")):
        client.execute_sql(
            f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, {key} BIGINT NOT NULL, {pads})",
            schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.id AS aid, d.val AS dval "
        "FROM a JOIN b ON a.k1 = b.id JOIN c ON b.k2 = c.id JOIN d ON c.k3 = d.id",
        schema_name=sn)

    padvals = ", ".join("0" for _ in range(16))
    for table, row in (("d", "30, 42"), ("c", "20, 30"), ("b", "10, 20"), ("a", "1, 10")):
        client.execute_sql(f"INSERT INTO {table} VALUES ({row}, {padvals})", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "aid", "dval") == {(1, 42): 1}

    client.execute_sql("DELETE FROM c WHERE id = 20", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "aid", "dval") == {}
    client.execute_sql(f"INSERT INTO c VALUES (20, 30, {padvals})", schema_name=sn)
    client.execute_sql("UPDATE d SET val = 99 WHERE id = 30", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "aid", "dval") == {(1, 99): 1}
