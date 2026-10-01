"""The set-operation graph: UNION / INTERSECT / EXCEPT in both quantifiers,
`MINUS`, and SELECT DISTINCT.

A set op is join-free — a linear combination of `{union, negate}` and the
weight-clamp primitive over content-hashed leaves — so what it computes is a
weight, never a row set. An `ALL` branch overlap legitimately carries weight 2,
and a tuple that should net to 0 surviving at weight 1 is invisible to a row
count, so every assertion here is a weighted bag.

Leaves over one source, however many, are driven by one push in one epoch.

Run with GNITZ_WORKERS=4: each side is repartitioned by its content hash, so
both branches of a tuple land on one worker only if the hash is a pure function
of the logical value.
"""
from collections import Counter

from _read import bag, scanned
from _shapes import ONE_SOURCE_OPS, setop
from _sql import churn


# view -> (operator as written, the operator it computes). `MINUS` is Oracle's
# spelling of EXCEPT, quantifiers included.
_OPS = {
    "ua": ("UNION ALL", "UNION ALL"), "u": ("UNION", "UNION"),
    "ia": ("INTERSECT ALL", "INTERSECT ALL"), "i": ("INTERSECT", "INTERSECT"),
    "ea": ("EXCEPT ALL", "EXCEPT ALL"), "e": ("EXCEPT", "EXCEPT"),
    "ma": ("MINUS ALL", "EXCEPT ALL"), "m": ("MINUS", "EXCEPT"), "md": ("MINUS DISTINCT", "EXCEPT"),
}
# Unprojected, so identity is the whole (pk, val) row rather than the PK.
_STAR_OPS = {"sx": "EXCEPT", "si": "INTERSECT", "sua": "UNION ALL"}
# (statement, table, {pk: (val,)}); a `None` row deletes the pk.
_CHURN = [
    # A branch may arrive before the other has anything.
    ("INSERT INTO b VALUES (8, 40)", "b", {8: (40,)}),
    # Three rows carry 10, so a DISTINCT/ALL split shows.
    ("INSERT INTO a VALUES (1, 10), (2, 10), (3, 10), (4, 20)", "a",
     {1: (10,), 2: (10,), 3: (10,), 4: (20,)}),
    # 10 overlaps at unequal multiplicity and 30 is right-only (cR=2, cL=0), which
    # drives the clamp's pre-image integral net-negative. (1, 999) shares a's PK 1
    # and (4, 20) is a's row verbatim: under SELECT * two elements and one.
    ("INSERT INTO b VALUES (5, 10), (6, 30), (7, 30), (1, 999), (4, 20)", "b",
     {5: (10,), 6: (30,), 7: (30,), 1: (999,), 4: (20,)}),
    ("DELETE FROM b WHERE pk = 5", "b", {5: None}),
    # A retraction and an insertion under one PK: an exit and an entry together.
    ("UPDATE a SET val = 30 WHERE pk = 4", "a", {4: (30,)}),
    ("DELETE FROM a WHERE pk IN (1, 2, 3)", "a", {1: None, 2: None, 3: None}),
    # NULL is its own value: two coalesce under a deduplicating op, keep their
    # multiplicity under ALL, and never meet a genuine 0.
    ("INSERT INTO b VALUES (12, NULL), (13, NULL), (14, 0)", "b", {12: (None,), 13: (None,), 14: (0,)}),
    ("INSERT INTO a VALUES (15, 0), (16, 30)", "a", {15: (0,), 16: (30,)}),
    # One value driven back and forth across membership, through the tick where
    # its pre-image integral is -1 while the output stays clamped at 0.
    ("INSERT INTO a VALUES (9, 50)", "a", {9: (50,)}),
    ("INSERT INTO b VALUES (10, 50)", "b", {10: (50,)}),
    ("DELETE FROM a WHERE pk = 9", "a", {9: None}),
    ("DELETE FROM b WHERE pk = 10", "b", {10: None}),
    ("INSERT INTO a VALUES (11, 50)", "a", {11: (50,)}),
]


def test_every_operator_tracks_its_weight_algebra_through_churn(client):
    """Each operator equals its Z-set definition after every epoch, and `mc`
    associates `(a UNION b) MINUS c` left to right. The right branch is populated
    first, so every left delta meets a non-empty right trace — an incremental
    circuit's fixpoint cannot depend on which source ticked first. `tri`'s outer
    UNION ALL keeps a second copy of `a` beside the inner UNION."""
    client.execute_sql(
        "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
        "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT); "
        "CREATE TABLE c (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
        "INSERT INTO c VALUES (1, 30); "
        "CREATE VIEW av AS SELECT pk, val FROM a WHERE val > 15; "
        "CREATE VIEW aw AS SELECT pk, val FROM a WHERE val > 25; "
        + "; ".join(f"CREATE VIEW {n} AS SELECT val FROM a {op} SELECT val FROM b"
                    for n, (op, _) in _OPS.items()) + "; "
        + "; ".join(f"CREATE VIEW {n} AS SELECT * FROM a {op} SELECT * FROM b"
                    for n, op in _STAR_OPS.items()) + "; "
        + "; ".join(f"CREATE VIEW same_{n} AS SELECT val FROM a {op} SELECT val FROM a; "
                    f"CREATE VIEW split_{n} AS SELECT val FROM a WHERE val > 15 {op} "
                    f"SELECT val FROM a WHERE val < 40; "
                    f"CREATE VIEW view_{n} AS SELECT val FROM av {op} SELECT val FROM av; "
                    f"CREATE VIEW views_{n} AS SELECT val FROM av {op} SELECT val FROM aw"
                    for n, op in ONE_SOURCE_OPS.items()) + "; "
        "CREATE VIEW mc AS SELECT val FROM a UNION SELECT val FROM b MINUS SELECT val FROM c; "
        "CREATE VIEW tri AS SELECT val FROM a UNION SELECT val FROM b UNION ALL SELECT val FROM a")

    state = {"a": {}, "b": {}}
    for sql in churn(client, state, _CHURN):
        a, b = state["a"], state["b"]
        vals = Counter(a.values()), Counter(b.values())
        for name, (_, op) in _OPS.items():
            assert bag(scanned(client, name), "val") == setop(op, *vals), (sql, name)
        for name, op in _STAR_OPS.items():
            assert bag(scanned(client, name), "pk", "val") == setop(
                op, *(Counter((pk, *row) for pk, row in t.items()) for t in (a, b))), (sql, name)
        assert bag(scanned(client, "mc"), "val") == \
            setop("EXCEPT", Counter(setop("UNION", *vals)), Counter({(30,): 1})), sql
        assert bag(scanned(client, "tri"), "val") == \
            setop("UNION ALL", Counter(setop("UNION", *vals)), vals[0]), sql

        def of_a(keep):
            return Counter(row for row in a.values() if keep(*row))

        over15 = of_a(lambda v: v > 15)
        branches = {"same": (vals[0], vals[0]), "split": (over15, of_a(lambda v: v < 40)),
                    "view": (over15, over15), "views": (over15, of_a(lambda v: v > 25))}
        for n, op in ONE_SOURCE_OPS.items():
            for shape, (left, right) in branches.items():
                assert bag(scanned(client, f"{shape}_{n}"), "val") == \
                    setop(op, left, right), (sql, shape, n)


def test_a_distinct_tuple_lives_exactly_while_a_row_carries_it(client):
    """DISTINCT is the non-linear boundary operator (DBSP Prop 4.7): a projected
    tuple sits at weight 1 while its accumulated weight is positive, whatever
    number of rows carry it. Its identity is the whole tuple, so two rows sharing
    one component are two tuples."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL); "
        "CREATE VIEW v AS SELECT DISTINCT a, b FROM t")

    for sql, want in [
        # (1,1) carried twice; (1,2) shares `a` with it and (2,1) shares `b`.
        ("INSERT INTO t VALUES (1,1,1), (2,1,1), (3,1,2), (4,2,1), (5,3,3)",
         {(1, 1), (1, 2), (2, 1), (3, 3)}),
        ("DELETE FROM t WHERE pk = 1", {(1, 1), (1, 2), (2, 1), (3, 3)}),
        ("DELETE FROM t WHERE pk = 5", {(1, 1), (1, 2), (2, 1)}),
        # The last carrier moves: an exit and an entry boundary in one epoch.
        ("UPDATE t SET a = 4 WHERE pk = 2", {(1, 2), (2, 1), (4, 1)}),
        # Onto a tuple that already exists: a second carrier, not a second row.
        ("UPDATE t SET a = 2, b = 1 WHERE pk = 3", {(2, 1), (4, 1)}),
        ("DELETE FROM t WHERE pk = 4", {(2, 1), (4, 1)}),
        ("INSERT INTO t VALUES (6, 3, 3)", {(2, 1), (4, 1), (3, 3)}),
    ]:
        client.execute_sql(sql)
        assert bag(scanned(client, "v"), "a", "b") == dict.fromkeys(want, 1), sql
