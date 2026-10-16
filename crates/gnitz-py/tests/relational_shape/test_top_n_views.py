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
from collections import Counter

from _read import bag, scanned
from _shapes import groups, sorted_by, views_ddl
from _sql import churn, inserting

_SCORE = ("score", True, True)

# view -> (body, its columns, its source, its order as `sorted_by` keys, LIMIT, OFFSET).
_WINDOWS = {
    "top2": ("SELECT id, score FROM scores ORDER BY score DESC LIMIT 2", ("id", "score"), "scores", [_SCORE], 2, 0),
    # A second key orders ties inside the cut, OFFSET moves the cut, and NULLs sit
    # last under ASC and first under DESC unless the body says otherwise.
    "offset": ("SELECT id FROM scores ORDER BY ns ASC, name DESC LIMIT 2 OFFSET 1", ("id",), "scores",
               [("ns", False, False), ("name", True, True)], 2, 1),
    "nulls_first": ("SELECT id FROM scores ORDER BY ns ASC NULLS FIRST LIMIT 1", ("id",), "scores",
                    [("ns", False, True)], 1, 0),
    "nulls_desc": ("SELECT id FROM scores ORDER BY ns DESC LIMIT 1", ("id",), "scores", [("ns", True, True)], 1, 0),
    # An unprojected sort key rides hidden; a positional key names a projected one.
    "hidden_key": ("SELECT id, name FROM scores ORDER BY score * 2 DESC, 2 LIMIT 2", ("id", "name"), "scores",
                   [_SCORE, ("name", False, False)], 2, 0),
    "by_name": ("SELECT id FROM scores ORDER BY name DESC LIMIT 1", ("id",), "scores", [("name", True, True)], 1, 0),
    # `dup` holds grp=1's rows at weight 2, so an element fills two slots and the
    # cut can fall inside it.
    "weighted": ("SELECT id, score FROM dup ORDER BY score DESC LIMIT 3", ("id", "score"), "dup", [_SCORE], 3, 0),
    # A ROW_NUMBER bounded in QUALIFY numbers each copy of a weight-2 row, so the
    # cut takes one of the two.
    "dup_top": ("SELECT id, score FROM dup QUALIFY ROW_NUMBER() OVER (ORDER BY score DESC) <= 1",
                ("id", "score"), "dup", [_SCORE], 1, 0),
    # The largest LIMIT the grammar admits, behind an OFFSET.
    "unbounded": ("SELECT id FROM scores ORDER BY score DESC LIMIT 9223372036854775807 OFFSET 1", ("id",), "scores",
                  [_SCORE], 2**63 - 1, 1),
}

_SCORES = ("id", "grp", "score", "ns", "name")

# (statement, table, {id: (grp, score, ns, name)}). Every order key is unique
# among the rows it ranks, so each window's content is a function of the data.
_CHURN = [
    inserting("scores", {1: (1, 10, 5, "x"), 2: (1, 30, 5, "y"), 3: (1, 20, None, "n"),
                         4: (2, 5, 7, "z"), 5: (2, 25, 1, "w")}),
    # A member leaves and the next row is promoted from the index.
    ("DELETE FROM scores WHERE id = 2", "scores", {2: None}),
    # Into the window, cutting through weight-2 element 3.
    ("UPDATE scores SET score = 28 WHERE id = 4", "scores", {4: (2, 28, 7, "z")}),
    # Out of the window.
    ("UPDATE scores SET score = 1 WHERE id = 5", "scores", {5: (2, 1, 1, "w")}),
    inserting("scores", {6: (1, 40, 0, "a")}),
    # The last NULL takes a value: both NULL-placement windows move off it.
    ("UPDATE scores SET ns = 3 WHERE id = 3", "scores", {3: (1, 20, 3, "n")}),
    ("UPDATE scores SET name = 'zz' WHERE id = 1", "scores", {1: (1, 10, 5, "zz")}),
    ("DELETE FROM scores", "scores", {i: None for i in (1, 3, 4, 5, 6)}),
]


def test_a_global_window_is_a_cut_in_one_weighted_order(client):
    """The index holds the rows below the cut too, so deleting a member promotes
    the next one rather than shrinking the window, an update moves a row into or
    out of it, and emptying the source empties it. The hidden sort key never
    widens the presented row, so each view is read whole."""
    client.execute_sql(
        "CREATE TABLE scores (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, "
        "score BIGINT NOT NULL, ns BIGINT, name TEXT NOT NULL); "
        "CREATE VIEW dup AS SELECT id, score FROM scores UNION ALL SELECT id, score FROM scores WHERE grp = 1; "
        + views_ddl(_WINDOWS))

    state = {"scores": {}}
    for sql in churn(client, state, _CHURN):
        scores = [dict(zip(_SCORES, (i, *r))) for i, r in state["scores"].items()]
        source = {"scores": scores, "dup": scores + [r for r in scores if r["grp"] == 1]}
        for name, (_, cols, table, order, limit, offset) in _WINDOWS.items():
            assert bag(scanned(client, name)) == Counter(
                tuple(r[c] for c in cols) for r in sorted_by(source[table], order)[offset:offset + limit]), (sql, name)


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


def _numbered(rows, partition, order):
    """`{id: ROW_NUMBER() OVER (PARTITION BY partition ORDER BY order)}` over
    `rows` (dicts), the unique key `id` ordering what `order` leaves tied."""
    return {r["id"]: n for part in groups(rows, lambda r: r[partition]).values()
            for n, r in enumerate(sorted_by(part, order), 1)}


def test_qualify_cuts_each_partition(client):
    """`rn <= n`, `n > rn` and `rn = 1` are bounds the recognizer reads, so each
    takes the top rows of each partition, and `rn = n` keeps slot `n` alone, so a
    partition shorter than `n` keeps nothing; a partition emptied loses its rows
    while a new one gains them. A *projected* `rn` is a value the top-N operator
    cannot supply, so the window operator answers instead, and selects the same
    rows because both order by the written keys and then the unique key. With no
    ORDER BY the unique key orders the partition, and a partition key naming the
    whole PK in another order is one row per PK, which the operator must shard
    by the PK's own order to find. Another window beside the bound is computed
    over the whole partition, not over the rows the cut keeps. Over a grouped
    body the unique key is the group key in GROUP BY order, and a bounded and a
    selected number pick the same group under ties."""
    rn = "ROW_NUMBER() OVER (PARTITION BY grp ORDER BY score"
    by_count = "ROW_NUMBER() OVER (ORDER BY COUNT(*))"
    client.execute_sql(
        "CREATE TABLE scores (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, score BIGINT NOT NULL); "
        f"CREATE VIEW le AS SELECT grp, id FROM scores QUALIFY {rn}) <= 2; "
        f"CREATE VIEW gt AS SELECT grp, id FROM scores QUALIFY 3 > {rn}); "
        f"CREATE VIEW eq AS SELECT grp, id FROM scores QUALIFY {rn}) = 1; "
        f"CREATE VIEW second AS SELECT grp, id FROM scores QUALIFY {rn}) = 2; "
        f"CREATE VIEW ranked AS SELECT grp, id, {rn} DESC) AS rn FROM scores QUALIFY rn <= 2; "
        f"CREATE VIEW cut AS SELECT grp, id FROM scores QUALIFY {rn} DESC) <= 2; "
        "CREATE VIEW first AS SELECT grp, id FROM scores QUALIFY ROW_NUMBER() OVER (PARTITION BY grp) = 1; "
        "CREATE VIEW with_sum AS SELECT grp, id, SUM(score) OVER (PARTITION BY grp) AS s FROM scores "
        f"QUALIFY {rn} DESC) <= 2; "
        "CREATE TABLE pairs (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT NOT NULL, PRIMARY KEY (a, b)); "
        "CREATE VIEW p AS SELECT a, b FROM pairs QUALIFY ROW_NUMBER() OVER (PARTITION BY b, a ORDER BY v DESC) <= 1; "
        "CREATE VIEW every AS SELECT a, b FROM pairs QUALIFY ROW_NUMBER() OVER (PARTITION BY b, a) <= 1; "
        f"CREATE VIEW g_cut AS SELECT a, b, COUNT(*) AS n FROM pairs GROUP BY b, a QUALIFY {by_count} <= 1; "
        f"CREATE VIEW g_sel AS SELECT a, b, COUNT(*) AS n, {by_count} AS rn FROM pairs GROUP BY b, a QUALIFY rn <= 1")

    state = {"scores": {}, "pairs": {}}
    for sql in churn(client, state, [
        inserting("pairs", {(a, b): a * 10 + b for a in range(3) for b in range(3)}),
        inserting("scores", {1: (1, 10), 2: (1, 30), 3: (1, 20), 4: (2, 25)}),
        # Partition 2 empties and partition 3 appears in one commit batch, where two
        # autocommit statements would be two ticks.
        ("BEGIN; DELETE FROM scores WHERE id = 4; INSERT INTO scores VALUES (5, 3, 1), (6, 1, 5); COMMIT",
         "scores", {4: None, 5: (3, 1), 6: (1, 5)}),
        # Every group ties on its count, so the group key decides: `b` before `a`.
        ("DELETE FROM pairs WHERE a = 0 AND b = 0", "pairs", {(0, 0): None}),
    ]):
        rows = [dict(id=i, grp=g, score=sc) for i, (g, sc) in state["scores"].items()]
        asc = _numbered(rows, "grp", [("score", False, False)])
        desc = _numbered(rows, "grp", [("score", True, True)])
        by_id = _numbered(rows, "grp", [])
        sums = {g: sum(r["score"] for r in rs) for g, rs in groups(rows, lambda r: r["grp"]).items()}

        def kept(view, cols, numbers, keep):
            assert bag(scanned(client, view), *cols) == {
                tuple(r[c] for c in cols): 1 for r in rows if keep(numbers[r["id"]])}, (sql, view)

        for name in ("le", "gt"):
            kept(name, ("grp", "id"), asc, lambda n: n <= 2)
        kept("eq", ("grp", "id"), asc, lambda n: n == 1)
        kept("second", ("grp", "id"), asc, lambda n: n == 2)
        kept("cut", ("grp", "id"), desc, lambda n: n <= 2)
        kept("first", ("grp", "id"), by_id, lambda n: n == 1)
        assert bag(scanned(client, "ranked"), "grp", "id", "rn") == {
            (r["grp"], r["id"], desc[r["id"]]): 1 for r in rows if desc[r["id"]] <= 2}, sql
        assert bag(scanned(client, "with_sum"), "grp", "id", "s") == {
            (r["grp"], r["id"], sums[r["grp"]]): 1 for r in rows if desc[r["id"]] <= 2}, sql

        for name in ("p", "every"):
            assert bag(scanned(client, name), "a", "b") == dict.fromkeys(state["pairs"], 1), (sql, name)
        a, b = min(state["pairs"], key=lambda ab: ab[::-1])
        assert bag(scanned(client, "g_cut"), "a", "b", "n") == {(a, b, 1): 1}, sql
        assert bag(scanned(client, "g_sel"), "a", "b", "n", "rn") == {(a, b, 1, 1): 1}, sql


def test_a_cut_takes_null_keys(client):
    """A top-N's partition is a group key and its order any column, so both may
    be NULL: the NULL partition is one partition, and NULLs sort where the ORDER BY
    says."""
    client.execute_sql(
        "CREATE TABLE n (id BIGINT NOT NULL PRIMARY KEY, p BIGINT, o BIGINT); "
        "CREATE VIEW nf AS SELECT id FROM n QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o NULLS FIRST) <= 2; "
        "CREATE VIEW nl AS SELECT id FROM n "
        "QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o DESC NULLS LAST) <= 2; "
        "CREATE VIEW dflt AS SELECT id FROM n QUALIFY ROW_NUMBER() OVER (PARTITION BY p ORDER BY o) = 1")

    state = {"n": {}}
    for sql in churn(client, state, [
        inserting("n", {1: (1, 5), 2: (1, None), 3: (1, 7), 4: (None, 3), 5: (None, None), 6: (None, 9),
                        7: (2, None)}),
        ("UPDATE n SET o = NULL WHERE id = 3", "n", {3: (1, None)}),
        ("UPDATE n SET p = NULL WHERE id = 1", "n", {1: (None, 5)}),
        ("DELETE FROM n WHERE id = 5", "n", {5: None}),
        inserting("n", {8: (2, 4), 9: (None, None)}),
    ]):
        rows = [dict(id=i, p=p, o=o) for i, (p, o) in state["n"].items()]
        # Ascending defaults to NULLS LAST.
        for view, order, limit in (("nf", ("o", False, True), 2), ("nl", ("o", True, False), 2),
                                   ("dflt", ("o", False, False), 1)):
            numbers = _numbered(rows, "p", [order])
            assert bag(scanned(client, view), "id") == {
                (i,): 1 for i, n in numbers.items() if n <= limit}, (sql, view)
