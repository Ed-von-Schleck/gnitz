"""Maintained top-N views: `CREATE VIEW ... ORDER BY ... LIMIT n [OFFSET m]` (one
global window) and `QUALIFY ROW_NUMBER() OVER (PARTITION BY ...) <= n` (one
window per partition).

The operator keeps an ordered index of every input row, not just the ones in the
window, so retracting a member promotes the next row from that index. What the
assertions state is the maintained *state* after each write, weights included: a
window is a cut at a position in a weighted order, so an element at weight 2
fills two slots and the cut can fall inside it.

Run with GNITZ_WORKERS=4 — the global window is two-phase (a local window per
worker, exchanged and cut once), and the guarantee is that which rows are
selected is a function of the data alone.
"""

import pytest
from _read import bag, scanned


def _scores(client, sn, score="BIGINT"):
    client.execute_sql(
        "CREATE TABLE scores (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, "
        f"score {score}, name VARCHAR(30) NOT NULL)", schema_name=sn)


def test_a_global_window_promotes_from_its_ordered_index(client, schema_name):
    """The index holds the rows below the cut too, so deleting a member promotes
    the next one rather than shrinking the window; an update moves a row into the
    window or out of it, and emptying the source empties the window."""
    sn = schema_name
    _scores(client, sn)
    client.execute_sql(
        "CREATE VIEW top2 AS SELECT id, score FROM scores ORDER BY score DESC LIMIT 2",
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO scores VALUES (1, 0, 10, 'a'), (2, 0, 30, 'b'), (3, 0, 20, 'c'), (4, 0, 5, 'd')",
        schema_name=sn)
    assert bag(scanned(client, sn, "top2"), "id", "score") == {(2, 30): 1, (3, 20): 1}

    client.execute_sql("DELETE FROM scores WHERE id = 2", schema_name=sn)
    assert bag(scanned(client, sn, "top2"), "id", "score") == {(3, 20): 1, (1, 10): 1}

    client.execute_sql("UPDATE scores SET score = 25 WHERE id = 4", schema_name=sn)
    assert bag(scanned(client, sn, "top2"), "id", "score") == {(4, 25): 1, (3, 20): 1}

    client.execute_sql("UPDATE scores SET score = 1 WHERE id = 3", schema_name=sn)
    assert bag(scanned(client, sn, "top2"), "id", "score") == {(4, 25): 1, (1, 10): 1}

    client.execute_sql("DELETE FROM scores", schema_name=sn)
    assert bag(scanned(client, sn, "top2"), "id", "score") == {}


def test_offset_ties_and_null_placement_all_cut_one_order(client, schema_name):
    """The window is a cut at a position in one total order, so OFFSET moves the
    cut, a second key orders ties within it, and NULLs occupy a defined end —
    last under ASC and first under DESC unless the body says otherwise."""
    sn = schema_name
    _scores(client, sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, score, name FROM scores "
        "ORDER BY score ASC, name DESC LIMIT 2 OFFSET 1", schema_name=sn)
    client.execute_sql(
        "INSERT INTO scores VALUES (1, 0, 5, 'x'), (2, 0, 5, 'y'), (3, 0, NULL, 'n'), "
        "(4, 0, 7, 'z'), (5, 0, 1, 'w')", schema_name=sn)
    # Order: 5(w,1), 2(y,5), 1(x,5), 4(z,7), 3(NULL) — skip one, keep two.
    assert bag(scanned(client, sn, "v"), "id") == {(1,): 1, (2,): 1}

    # Deleting the skipped row moves every later row up one position.
    client.execute_sql("DELETE FROM scores WHERE id = 5", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id") == {(1,): 1, (4,): 1}

    client.execute_sql(
        "CREATE VIEW nf AS SELECT id FROM scores ORDER BY score ASC NULLS FIRST LIMIT 1",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW nd AS SELECT id FROM scores ORDER BY score DESC LIMIT 1", schema_name=sn)
    assert bag(scanned(client, sn, "nf"), "id") == {(3,): 1}
    assert bag(scanned(client, sn, "nd"), "id") == {(3,): 1}


def test_a_sort_key_need_not_be_a_projected_column(client, schema_name):
    """An ORDER BY key that the SELECT does not project rides the view as a
    hidden column, so the ordering survives without widening the visible schema;
    a positional key names a projected one, and a string key orders as a string."""
    sn = schema_name
    _scores(client, sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, name FROM scores ORDER BY score * 2 DESC, 2 LIMIT 2",
        schema_name=sn)
    client.execute_sql(
        "CREATE VIEW s AS SELECT id FROM scores ORDER BY name DESC LIMIT 1", schema_name=sn)
    client.execute_sql(
        "INSERT INTO scores VALUES (1, 0, 10, 'b'), (2, 0, 10, 'a'), (3, 0, 20, 'c')",
        schema_name=sn)

    rows = scanned(client, sn, "v")
    assert bag(rows, "id", "name") == {(3, "c"): 1, (2, "a"): 1}
    for r in rows:
        assert set(r._fields) == {"id", "name"}, r._fields
    assert bag(scanned(client, sn, "s"), "id") == {(3,): 1}


def test_an_elements_weight_fills_that_many_slots(client, schema_name):
    """A window cuts a weighted order, so an element of weight w occupies w
    slots and the cut may fall inside one. A stream's PK is not unique, which is
    how an element of weight 2 is produced here."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE ev (id BIGINT NOT NULL PRIMARY KEY, score BIGINT NOT NULL) "
        "WITH (stream = true)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, score FROM ev ORDER BY score DESC LIMIT 3", schema_name=sn)

    client.execute_sql("INSERT INTO ev VALUES (1, 10), (2, 30), (3, 20), (2, 30)", schema_name=sn)
    # Slots: 30, 30, 20 — id 2 takes two of the three.
    assert bag(scanned(client, sn, "v"), "id") == {(2,): 2, (3,): 1}

    client.execute_sql("INSERT INTO ev VALUES (4, 40)", schema_name=sn)
    # Slots: 40, 30, 30 — id 3 is cut out and id 2 keeps both of its slots.
    assert bag(scanned(client, sn, "v"), "id") == {(2,): 2, (4,): 1}


def test_the_window_cuts_a_grouped_or_set_op_body(client, schema_name):
    """The ordered index sits over whatever the body emits, so it cuts groups of
    a GROUP BY and the output of a set operation exactly as it cuts base rows —
    and a group whose aggregate moves moves in the order."""
    sn = schema_name
    _scores(client, sn)
    client.execute_sql(
        "CREATE VIEW leaders AS SELECT grp, SUM(score) AS total FROM scores "
        "GROUP BY grp ORDER BY total DESC LIMIT 2", schema_name=sn)
    # QUALIFY reaches the same cut over a grouped body, with an aggregate as the
    # order key — the recognizer runs after the body binds, so it sees groups.
    client.execute_sql(
        "CREATE VIEW busiest AS SELECT grp, COUNT(*) AS n FROM scores GROUP BY grp "
        "QUALIFY ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, grp) <= 2", schema_name=sn)
    client.execute_sql(
        "INSERT INTO scores VALUES (1, 1, 10, 'a'), (2, 1, 12, 'b'), (3, 2, 15, 'c'), "
        "(4, 3, 30, 'd'), (5, 4, 1, 'e')", schema_name=sn)
    assert bag(scanned(client, sn, "leaders"), "grp", "total") == {(3, 30): 1, (1, 22): 1}
    assert bag(scanned(client, sn, "busiest"), "grp", "n") == {(1, 2): 1, (2, 1): 1}

    client.execute_sql("INSERT INTO scores VALUES (6, 4, 100, 'f')", schema_name=sn)
    assert bag(scanned(client, sn, "leaders"), "grp", "total") == {(4, 101): 1, (3, 30): 1}
    assert bag(scanned(client, sn, "busiest"), "grp", "n") == {(1, 2): 1, (4, 2): 1}

    client.execute_sql(
        "CREATE VIEW u AS SELECT id, score FROM scores WHERE grp = 1 "
        "UNION SELECT id, score FROM scores WHERE grp = 4 ORDER BY score LIMIT 2", schema_name=sn)
    assert bag(scanned(client, sn, "u"), "id") == {(5,): 1, (1,): 1}


def test_qualify_recognizes_every_spelling_of_one_window(client, schema_name):
    """`rn <= n`, `n > rn` and `rn = 1` are the three bounds the recognizer reads,
    so each takes the top-N rows of each partition; a partition emptied loses its
    rows and a new one gains them, without the window being rebuilt."""
    sn = schema_name
    # A window key must be provably NOT NULL — the desugar's rule, checked before
    # the top-N rewrite is chosen.
    _scores(client, sn, score="BIGINT NOT NULL")
    client.execute_sql(
        "CREATE VIEW le AS SELECT grp, id FROM scores "
        "QUALIFY ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score) <= 2", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW gt AS SELECT grp, id FROM scores "
        "QUALIFY 3 > ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW eq AS SELECT grp, id FROM scores "
        "QUALIFY ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score) = 1", schema_name=sn)

    client.execute_sql(
        "INSERT INTO scores VALUES (1, 1, 10, 'a'), (2, 1, 30, 'b'), (3, 1, 20, 'c'), "
        "(4, 2, 25, 'd')", schema_name=sn)
    want = {(1, 1): 1, (1, 3): 1, (2, 4): 1}
    assert bag(scanned(client, sn, "le"), "grp", "id") == want
    assert bag(scanned(client, sn, "gt"), "grp", "id") == want
    assert bag(scanned(client, sn, "eq"), "grp", "id") == {(1, 1): 1, (2, 4): 1}

    # Partition 2 empties and partition 3 appears in one epoch: a transaction is
    # one commit batch, where two autocommit statements would be two ticks and
    # the operator would never see the pair together.
    client.execute_sql(
        "BEGIN; DELETE FROM scores WHERE id = 4; "
        "INSERT INTO scores VALUES (5, 3, 1, 'e'); COMMIT", schema_name=sn)
    want = {(1, 1): 1, (1, 3): 1, (3, 5): 1}
    assert bag(scanned(client, sn, "le"), "grp", "id") == want
    assert bag(scanned(client, sn, "gt"), "grp", "id") == want
    assert bag(scanned(client, sn, "eq"), "grp", "id") == {(1, 1): 1, (3, 5): 1}


def test_a_projected_row_number_falls_back_to_the_desugar(client, schema_name):
    """`rn` in the SELECT list is a value the top-N operator cannot supply, so
    the band-join desugar answers instead — and the two routes must select the
    same rows."""
    sn = schema_name
    _scores(client, sn, score="BIGINT NOT NULL")
    client.execute_sql(
        "CREATE VIEW ranked AS SELECT grp, id, "
        "ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score DESC) AS rn "
        "FROM scores QUALIFY rn <= 2", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW cut AS SELECT grp, id FROM scores "
        "QUALIFY ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score DESC) <= 2", schema_name=sn)
    client.execute_sql(
        "INSERT INTO scores VALUES (1, 1, 10, 'a'), (2, 1, 30, 'b'), (3, 1, 20, 'c'), "
        "(4, 2, 25, 'd')", schema_name=sn)

    assert bag(scanned(client, sn, "ranked"), "grp", "id") == \
        bag(scanned(client, sn, "cut"), "grp", "id")
    assert bag(scanned(client, sn, "ranked"), "grp", "id", "rn") == \
        {(1, 2, 1): 1, (1, 3, 2): 1, (2, 4, 1): 1}


def test_a_partition_key_that_permutes_the_pk_shards_in_pk_order(client, schema_name):
    """PARTITION BY naming the whole PK in a different order is one row per PK.
    The output is keyed by the PK in pk-list order, so the operator has to shard
    by that order too — otherwise the rows land on a worker the multi-worker
    gather does not look for them on."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE pairs (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT NOT NULL, "
        "PRIMARY KEY (a, b))", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW p AS SELECT a, b, v FROM pairs "
        "QUALIFY ROW_NUMBER() OVER (PARTITION BY b, a ORDER BY v DESC) <= 1", schema_name=sn)
    client.execute_sql(
        "INSERT INTO pairs VALUES "
        + ", ".join(f"({a}, {b}, {a * 10 + b})" for a in range(4) for b in range(4)),
        schema_name=sn)

    # Every (a, b) is its own partition, so every row survives.
    assert bag(scanned(client, sn, "p"), "a", "b") == \
        {(a, b): 1 for a in range(4) for b in range(4)}

    client.execute_sql("DELETE FROM pairs WHERE a = 2", schema_name=sn)
    assert bag(scanned(client, sn, "p"), "a", "b") == \
        {(a, b): 1 for a in range(4) if a != 2 for b in range(4)}


@pytest.mark.parametrize("body, needle", [
    ("SELECT id FROM scores ORDER BY score", "ORDER BY without LIMIT"),
    ("SELECT id FROM scores LIMIT 3", "LIMIT without ORDER BY"),
    ("SELECT id FROM scores ORDER BY score LIMIT 0", "LIMIT 0"),
    # The body honours the ORDER BY / LIMIT pair, so a leftover OFFSET must be
    # refused rather than silently dropped.
    ("SELECT id FROM scores OFFSET 5", "OFFSET without"),
    ("SELECT DISTINCT grp FROM scores ORDER BY score LIMIT 1", "selected column"),
    ("SELECT id FROM scores UNION SELECT id FROM scores ORDER BY score LIMIT 1",
     "output column or position"),
])
def test_the_planner_names_what_it_refuses(client, schema_name, body, needle):
    """A window the operator cannot maintain is refused by name, so it cannot
    compile to a differently-shaped graph."""
    sn = schema_name
    _scores(client, sn)
    with pytest.raises(Exception) as ei:
        client.execute_sql(f"CREATE VIEW v AS {body}", schema_name=sn)
    assert needle in str(ei.value), (body, str(ei.value))


def test_a_bounded_top_n_view_is_refused(client, schema_name):
    """`capacity` sweeps rows to skeletons and recomputes them on read, which a
    window over an ordered index cannot answer, so the pair is refused."""
    sn = schema_name
    _scores(client, sn)
    with pytest.raises(Exception) as ei:
        client.execute_sql(
            "CREATE VIEW v WITH (capacity = '4 MB') AS SELECT id FROM scores "
            "ORDER BY score LIMIT 1", schema_name=sn)
    assert "capacity" in str(ei.value), str(ei.value)
