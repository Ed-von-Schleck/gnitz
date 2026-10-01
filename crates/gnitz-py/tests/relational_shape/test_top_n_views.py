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
from _read import bag, scanned

# view -> (body, the columns it is read by).
_WINDOWS = {
    "top2": ("SELECT id, score FROM scores ORDER BY score DESC LIMIT 2", ("id", "score")),
    # A second key orders ties inside the cut, OFFSET moves the cut, and NULLs sit
    # last under ASC and first under DESC unless the body says otherwise.
    "offset": ("SELECT id FROM scores ORDER BY ns ASC, name DESC LIMIT 2 OFFSET 1", ("id",)),
    "nulls_first": ("SELECT id FROM scores ORDER BY ns ASC NULLS FIRST LIMIT 1", ("id",)),
    "nulls_desc": ("SELECT id FROM scores ORDER BY ns DESC LIMIT 1", ("id",)),
    # An unprojected sort key rides hidden; a positional key names a projected one.
    "hidden_key": ("SELECT id, name FROM scores ORDER BY score * 2 DESC, 2 LIMIT 2", ("id", "name")),
    "by_name": ("SELECT id FROM scores ORDER BY name DESC LIMIT 1", ("id",)),
    # `dup` holds grp=1's rows at weight 2.
    "weighted": ("SELECT id, score FROM dup ORDER BY score DESC LIMIT 3", ("id", "score")),
    # A ROW_NUMBER bounded in QUALIFY numbers each copy of a weight-2 row, so the
    # cut takes one of the two.
    "dup_top": ("SELECT id, score FROM dup QUALIFY ROW_NUMBER() OVER (ORDER BY score DESC) <= 1", ("id", "score")),
    # The largest LIMIT the grammar admits, behind an OFFSET.
    "unbounded": ("SELECT id FROM scores ORDER BY score DESC LIMIT 9223372036854775807 OFFSET 1", ("id",)),
}

# (statement, {view: bag}) — each view's whole state after the statement.
_CHURN = [
    ("INSERT INTO scores VALUES (1, 1, 10, 5, 'x'), (2, 1, 30, 5, 'y'), (3, 1, 20, NULL, 'n'), "
     "(4, 2, 5, 7, 'z'), (5, 2, 25, 1, 'w')",
     {"top2": {(2, 30): 1, (5, 25): 1}, "offset": {(2,): 1, (1,): 1}, "nulls_first": {(3,): 1},
      "nulls_desc": {(3,): 1}, "hidden_key": {(2, "y"): 1, (5, "w"): 1}, "by_name": {(4,): 1},
      "weighted": {(2, 30): 2, (5, 25): 1}, "dup_top": {(2, 30): 1},
      "unbounded": {(5,): 1, (3,): 1, (1,): 1, (4,): 1}}),
    # A member leaves and the next row is promoted from the index.
    ("DELETE FROM scores WHERE id = 2",
     {"top2": {(5, 25): 1, (3, 20): 1}, "offset": {(1,): 1, (4,): 1}, "nulls_first": {(3,): 1},
      "nulls_desc": {(3,): 1}, "hidden_key": {(5, "w"): 1, (3, "n"): 1}, "by_name": {(4,): 1},
      "weighted": {(5, 25): 1, (3, 20): 2}, "dup_top": {(5, 25): 1},
      "unbounded": {(3,): 1, (1,): 1, (4,): 1}}),
    # Into the window, cutting through weight-2 element 3.
    ("UPDATE scores SET score = 28 WHERE id = 4",
     {"top2": {(4, 28): 1, (5, 25): 1}, "offset": {(1,): 1, (4,): 1}, "nulls_first": {(3,): 1},
      "nulls_desc": {(3,): 1}, "hidden_key": {(4, "z"): 1, (5, "w"): 1}, "by_name": {(4,): 1},
      "weighted": {(4, 28): 1, (5, 25): 1, (3, 20): 1}, "dup_top": {(4, 28): 1},
      "unbounded": {(5,): 1, (3,): 1, (1,): 1}}),
    # Out of the window.
    ("UPDATE scores SET score = 1 WHERE id = 5",
     {"top2": {(4, 28): 1, (3, 20): 1}, "offset": {(1,): 1, (4,): 1}, "nulls_first": {(3,): 1},
      "nulls_desc": {(3,): 1}, "hidden_key": {(4, "z"): 1, (3, "n"): 1}, "by_name": {(4,): 1},
      "weighted": {(4, 28): 1, (3, 20): 2}, "dup_top": {(4, 28): 1},
      "unbounded": {(3,): 1, (1,): 1, (5,): 1}}),
    ("INSERT INTO scores VALUES (6, 1, 40, 0, 'a')",
     {"top2": {(6, 40): 1, (4, 28): 1}, "offset": {(5,): 1, (1,): 1}, "nulls_first": {(3,): 1},
      "nulls_desc": {(3,): 1}, "hidden_key": {(6, "a"): 1, (4, "z"): 1}, "by_name": {(4,): 1},
      "weighted": {(6, 40): 2, (4, 28): 1}, "dup_top": {(6, 40): 1},
      "unbounded": {(4,): 1, (3,): 1, (1,): 1, (5,): 1}}),
    ("DELETE FROM scores", dict.fromkeys(_WINDOWS, {})),
]


def test_a_global_window_is_a_cut_in_one_weighted_order(client):
    """The index holds the rows below the cut too, so deleting a member promotes
    the next one rather than shrinking the window, an update moves a row into or
    out of it, and emptying the source empties it. The hidden sort key never
    widens the presented row."""
    client.execute_sql(
        "CREATE TABLE scores (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, "
        "score BIGINT NOT NULL, ns BIGINT, name TEXT NOT NULL); "
        "CREATE VIEW dup AS SELECT id, score FROM scores UNION ALL SELECT id, score FROM scores WHERE grp = 1; "
        + "; ".join(f"CREATE VIEW {name} AS {body}" for name, (body, _) in _WINDOWS.items()))

    for sql, want in _CHURN:
        client.execute_sql(sql)
        for name, (_, cols) in _WINDOWS.items():
            rows = scanned(client, name)
            assert bag(rows, *cols) == want[name], (sql, name)
            assert all(set(r._fields) == set(cols) for r in rows), (sql, name)


def test_the_window_cuts_a_grouped_or_set_op_body(client):
    """The ordered index sits over whatever the body emits, so it cuts groups of
    a GROUP BY and the output of a set operation exactly as it cuts base rows —
    and a group whose aggregate moves moves in the order. QUALIFY reaches the same
    cut over a grouped body with an aggregate as its order key."""
    client.execute_sql(
        "CREATE TABLE scores (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, score BIGINT NOT NULL); "
        "CREATE VIEW leaders AS SELECT grp, SUM(score) AS total FROM scores "
        "GROUP BY grp ORDER BY total DESC LIMIT 2; "
        "CREATE VIEW busiest AS SELECT grp, COUNT(*) AS n FROM scores GROUP BY grp "
        "QUALIFY ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, grp) <= 2; "
        "CREATE VIEW u AS SELECT id, score FROM scores WHERE grp = 1 "
        "UNION SELECT id, score FROM scores WHERE grp = 4 ORDER BY score LIMIT 2; "
        "INSERT INTO scores VALUES (1, 1, 10), (2, 1, 12), (3, 2, 15), (4, 3, 30), (5, 4, 1)")
    assert bag(scanned(client, "leaders"), "grp", "total") == {(3, 30): 1, (1, 22): 1}
    assert bag(scanned(client, "busiest"), "grp", "n") == {(1, 2): 1, (2, 1): 1}
    assert bag(scanned(client, "u"), "id") == {(5,): 1, (1,): 1}

    client.execute_sql("INSERT INTO scores VALUES (6, 4, 100)")
    assert bag(scanned(client, "leaders"), "grp", "total") == {(4, 101): 1, (3, 30): 1}
    assert bag(scanned(client, "busiest"), "grp", "n") == {(1, 2): 1, (4, 2): 1}
    assert bag(scanned(client, "u"), "id") == {(5,): 1, (1,): 1}


def test_qualify_cuts_each_partition(client):
    """`rn <= n`, `n > rn` and `rn = 1` are bounds the recognizer reads, so each
    takes the top rows of each partition, and a partition emptied loses its rows
    while a new one gains them. A *projected* `rn` is a value the top-N operator
    cannot supply, so the join-keyed desugar answers instead, and selects the same
    rows because both order by the written keys and then the unique key. A
    partition key naming the whole PK in another order is one row per PK, and the
    operator must shard by the PK's own order to find them."""
    rn = "ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score"
    client.execute_sql(
        "CREATE TABLE scores (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, score BIGINT NOT NULL); "
        f"CREATE VIEW le AS SELECT grp, id FROM scores QUALIFY {rn}) <= 2; "
        f"CREATE VIEW gt AS SELECT grp, id FROM scores QUALIFY 3 > {rn}); "
        f"CREATE VIEW eq AS SELECT grp, id FROM scores QUALIFY {rn}) = 1; "
        f"CREATE VIEW ranked AS SELECT grp, id, {rn} DESC) AS rn FROM scores QUALIFY rn <= 2; "
        f"CREATE VIEW cut AS SELECT grp, id FROM scores QUALIFY {rn} DESC) <= 2; "
        "CREATE TABLE pairs (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT NOT NULL, PRIMARY KEY (a, b)); "
        "CREATE VIEW p AS SELECT a, b, v FROM pairs QUALIFY ROW_NUMBER() OVER (PARTITION BY b, a ORDER BY v DESC) <= 1; "
        "INSERT INTO pairs VALUES " + ", ".join(f"({a}, {b}, {a * 10 + b})" for a in range(4) for b in range(4))
        + "; INSERT INTO scores VALUES (1, 1, 10), (2, 1, 30), (3, 1, 20), (4, 2, 25)")

    def expect(lowest_two, lowest, highest_two):
        for name in ("le", "gt"):
            assert bag(scanned(client, name), "grp", "id") == dict.fromkeys(lowest_two, 1), name
        assert bag(scanned(client, "eq"), "grp", "id") == dict.fromkeys(lowest, 1)
        assert bag(scanned(client, "ranked"), "grp", "id", "rn") == dict.fromkeys(highest_two, 1)
        assert bag(scanned(client, "cut"), "grp", "id") == dict.fromkeys(((g, i) for g, i, _ in highest_two), 1)

    expect([(1, 1), (1, 3), (2, 4)], [(1, 1), (2, 4)], [(1, 2, 1), (1, 3, 2), (2, 4, 1)])
    # Partition 2 empties and partition 3 appears in one commit batch, where two
    # autocommit statements would be two ticks.
    client.execute_sql("BEGIN; DELETE FROM scores WHERE id = 4; INSERT INTO scores VALUES (5, 3, 1); COMMIT")
    expect([(1, 1), (1, 3), (3, 5)], [(1, 1), (3, 5)], [(1, 2, 1), (1, 3, 2), (3, 5, 1)])

    assert bag(scanned(client, "p"), "a", "b") == {(a, b): 1 for a in range(4) for b in range(4)}
    client.execute_sql("DELETE FROM pairs WHERE a = 2")
    assert bag(scanned(client, "p"), "a", "b") == {(a, b): 1 for a in (0, 1, 3) for b in range(4)}


def test_a_cut_takes_any_slot_and_sits_beside_other_windows(client):
    """`rn = n` keeps slot `n` alone, so a partition shorter than `n` keeps
    nothing. With no ORDER BY the input's unique key orders the partition, and a
    partition holding that whole key leaves no order at all: every row is numbered
    1. Another window beside the bound is computed over the whole partition, not
    over the rows the cut keeps. Over a grouped body the unique key is the group
    key in GROUP BY order, and a bounded and a selected number pick the same
    group under ties."""
    by_count = "ROW_NUMBER() OVER (ORDER BY COUNT(*))"
    client.execute_sql(
        "CREATE TABLE scores (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, score BIGINT NOT NULL); "
        "CREATE VIEW first AS SELECT grp, id FROM scores QUALIFY ROW_NUMBER() OVER (PARTITION BY grp) = 1; "
        "CREATE VIEW second AS SELECT grp, id FROM scores "
        "QUALIFY ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score) = 2; "
        "CREATE VIEW with_sum AS SELECT grp, id, SUM(score) OVER (PARTITION BY grp) AS s FROM scores "
        "QUALIFY ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score DESC) <= 2; "
        "CREATE TABLE pairs (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT NOT NULL, PRIMARY KEY (a, b)); "
        "CREATE VIEW every AS SELECT a, b FROM pairs QUALIFY ROW_NUMBER() OVER (PARTITION BY b, a) <= 1; "
        f"CREATE VIEW g_cut AS SELECT a, b, COUNT(*) AS n FROM pairs GROUP BY b, a QUALIFY {by_count} <= 1; "
        f"CREATE VIEW g_sel AS SELECT a, b, COUNT(*) AS n, {by_count} AS rn FROM pairs GROUP BY b, a QUALIFY rn <= 1; "
        "INSERT INTO pairs VALUES " + ", ".join(f"({a}, {b}, {a * 10 + b})" for a in range(3) for b in range(3))
        + "; INSERT INTO scores VALUES (1, 1, 10), (2, 1, 30), (3, 1, 20), (4, 2, 25)")

    def expect(first, second, with_sum, pairs, lowest_group):
        assert bag(scanned(client, "first"), "grp", "id") == dict.fromkeys(first, 1)
        assert bag(scanned(client, "second"), "grp", "id") == dict.fromkeys(second, 1)
        assert bag(scanned(client, "with_sum"), "grp", "id", "s") == dict.fromkeys(with_sum, 1)
        assert bag(scanned(client, "every"), "a", "b") == dict.fromkeys(pairs, 1)
        assert bag(scanned(client, "g_cut"), "a", "b", "n") == {(*lowest_group, 1): 1}
        assert bag(scanned(client, "g_sel"), "a", "b", "n", "rn") == {(*lowest_group, 1, 1): 1}

    all_pairs = [(a, b) for a in range(3) for b in range(3)]
    expect([(1, 1), (2, 4)], [(1, 3)], [(1, 2, 60), (1, 3, 60), (2, 4, 25)], all_pairs, (0, 0))
    # Every group ties on its count, so the group key decides: `b` before `a`.
    client.execute_sql(
        "BEGIN; DELETE FROM scores WHERE id = 4; INSERT INTO scores VALUES (5, 3, 1), (6, 1, 5); COMMIT; "
        "DELETE FROM pairs WHERE a = 0 AND b = 0")
    expect([(1, 1), (3, 5)], [(1, 1)], [(1, 2, 65), (1, 3, 65), (3, 5, 1)], all_pairs[1:], (1, 0))


def test_a_cut_takes_null_keys(client):
    """A top-N's partition is a group key and its order any column, so both may
    be NULL: the NULL partition is one partition, and NULLs sort where the ORDER BY
    says — which a join-keyed window, refusing a nullable key, never had to honour."""
    client.execute_sql(
        "CREATE TABLE n (id BIGINT NOT NULL PRIMARY KEY, p BIGINT, o BIGINT); "
        "CREATE VIEW nf AS SELECT id FROM n QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o NULLS FIRST) <= 2; "
        "CREATE VIEW nl AS SELECT id FROM n "
        "QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o DESC NULLS LAST) <= 2; "
        "CREATE VIEW dflt AS SELECT id FROM n QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) = 1")

    def cut(rows, nulls_first, desc, limit):
        """Each partition's first `limit` ids under `o`, then `id`."""
        def key(r):
            _, _, o = r
            if o is None:
                return (0 if nulls_first else 2, 0, r[0])
            return (1, -o if desc else o, r[0])
        parts = {}
        for r in rows:
            parts.setdefault(r[1], []).append(r)
        return {(r[0],): 1 for part in parts.values() for r in sorted(part, key=key)[:limit]}

    rows = {}

    def write(sql, changes):
        client.execute_sql(sql)
        for i, r in changes.items():
            if r is None:
                del rows[i]
            else:
                rows[i] = (i, *r)
        state = list(rows.values())
        assert bag(scanned(client, "nf"), "id") == cut(state, True, False, 2), sql
        assert bag(scanned(client, "nl"), "id") == cut(state, False, True, 2), sql
        # Ascending defaults to NULLS LAST.
        assert bag(scanned(client, "dflt"), "id") == cut(state, False, False, 1), sql

    write("INSERT INTO n VALUES (1, 1, 5), (2, 1, NULL), (3, 1, 7), (4, NULL, 3), (5, NULL, NULL), (6, NULL, 9), "
          "(7, 2, NULL)",
          {1: (1, 5), 2: (1, None), 3: (1, 7), 4: (None, 3), 5: (None, None), 6: (None, 9), 7: (2, None)})
    write("UPDATE n SET o = NULL WHERE id = 3", {3: (1, None)})
    write("UPDATE n SET p = NULL WHERE id = 1", {1: (None, 5)})
    write("DELETE FROM n WHERE id = 5", {5: None})
    write("INSERT INTO n VALUES (8, 2, 4), (9, NULL, NULL)", {8: (2, 4), 9: (None, None)})
