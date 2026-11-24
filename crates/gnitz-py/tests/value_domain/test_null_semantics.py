"""NULL as a value: which bit records it, and what it refuses to equal.

A NULL payload cell is **zero-filled and the null bitmap is the sole truth**, so
a test only proves something read that bitmap if the zero would have answered
differently. Every case here therefore pairs a NULL against the type's own zero —
the integer `0`, the empty string — rather than against an arbitrary value: a
predicate over a NULL that a zero would also fail proves nothing.

The bitmap is indexed by *payload* position, which is the column's index minus
the number of PK columns before it. For a PK declared as a prefix that is the
same as "minus the PK count", so a prefix table cannot tell a correct
implementation from one that subtracts a constant. Only a key interleaved with
the payload separates them.

The connectives themselves are one per-row program whatever the column type —
the width is consumed entirely by the load that fills the register, and
`AND`/`OR`/`NOT` never see a column — so they are stated once over the full 3×3
pattern table in `scalar_expression/test_three_valued_logic.py`. Each width's own
load and bit — a NULL beside its type's zero in every column type — is asserted
over the type catalogue in `test_value_round_trip.py`.
"""

from collections import Counter

import pytest
from _read import bag, scanned
from _sql import insert, values


# ---------------------------------------------------------------------------
# The null bit, per position
# ---------------------------------------------------------------------------


def test_the_null_bit_follows_payload_position_around_an_interleaved_key(
        client):
    """The PK columns sit *between* the payload columns, so a bitmap index that
    subtracted the PK count rather than the PK columns *before* each one maps
    `p1` to a negative and `p3` past the end. Writing all eight NULL patterns at
    once shows a mis-indexed bit as a NULL that moved to a neighbouring column
    rather than as a missing row.
    """
    client.execute_sql(
        "CREATE TABLE t (p1 BIGINT, a BIGINT UNSIGNED NOT NULL, p2 BIGINT, "
        "b BIGINT UNSIGNED NOT NULL, p3 BIGINT, PRIMARY KEY (a, b))")
    _, schema = client.resolve_table("t")
    assert schema.pk_indices == [1, 3]

    # All eight NULL patterns over (p1, p2, p3), each under its own key.
    def p(i, b):
        return None if i >> b & 1 else (b + 1) * 10
    seed = [(p(i, 0), i, p(i, 1), i, p(i, 2)) for i in range(8)]
    insert(client, "t", seed)
    assert bag(scanned(client, "t")) == dict.fromkeys(seed, 1)

    # A projection that reorders the payload rebuilds the bitmap, so the bits
    # must be re-indexed rather than copied across.
    client.execute_sql(
        "CREATE VIEW v AS SELECT b, a, p3, p2, p1 FROM t")
    assert bag(scanned(client, "v")) == {(r[3], r[1], r[4], r[2], r[0]): 1 for r in seed}


# ---------------------------------------------------------------------------
# A NULL key equals nothing, including another NULL
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("key_sql,zero,x,lead", [("BIGINT", 0, 7, False), ("TEXT", "", "x", False),
                                                  ("BIGINT", 0, 7, True)], ids=["int", "text", "composite"])
def test_a_null_key_matches_nothing_and_fills_once_beside_its_types_zero(client, key_sql, zero, x, lead):
    """A NULL key matches neither its type's zero nor another NULL. A NULL key
    reindexed to a synthetic key of zero would collide with the real zero, and a
    string key routes by the hash of its content, which for the empty string is
    what a NULL's absent content hashes to: a wrong *match* rather than a
    missing one, which a "NULL matched nothing" assertion alone cannot see.

    The preserved rows whose key is NULL and whose key is the zero project the
    same `x`, so only the retained key column and its null bit keep them apart
    — through a LEFT JOIN's null-fill and a NOT EXISTS's alike. The zero-keyed
    row matches twice, so a merge would cancel the NULL row's unmatched weight,
    and the weights are the assertion because a double-emit holds the same rows.
    A matched row's NULL payload crosses the inner join still NULL, beside the
    fills it would otherwise be mistaken for. The composite arm pairs the key
    with a NOT NULL lead column, so the row sharing `k` under another `a` fills.

    The key is `r.k`, a plain column the exchange builds a key for; the
    boundary-crossing case below joins the key region itself — two routes to the
    same collision. Emptying `r` turns every row into its fill."""
    on = ("l.a = r.a AND " if lead else "") + "l.k = r.k"
    left = [(1, 1, None, 5), (2, 1, zero, 5), (3, 2, zero, 6), (4, 1, x, 7)]
    right = [(10, 1, zero, 100), (11, 1, zero, 200), (12, 1, None, 900), (13, 1, x, None)]
    client.execute_sql(
        f"CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, k {key_sql}, x BIGINT NOT NULL); "
        f"CREATE TABLE r (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, k {key_sql}, name BIGINT); "
        f"CREATE VIEW j AS SELECT l.x, r.name FROM l JOIN r ON {on}; "
        f"CREATE VIEW lj AS SELECT l.x, r.name FROM l LEFT JOIN r ON {on}; "
        f"CREATE VIEW ne AS SELECT l.x FROM l WHERE NOT EXISTS (SELECT 1 FROM r WHERE {on}); "
        f"INSERT INTO r VALUES {values(right)}; INSERT INTO l VALUES {values(left)}")

    def expect():
        hits = [(lx, [nm for _, ra, rk, nm in right if lk is not None and lk == rk and (ra == la or not lead)])
                for _, la, lk, lx in left]
        assert bag(scanned(client, "j")) == Counter((lx, nm) for lx, ns in hits for nm in ns)
        assert bag(scanned(client, "lj")) == Counter((lx, nm) for lx, ns in hits for nm in ns or [None])
        assert bag(scanned(client, "ne")) == Counter((lx,) for lx, ns in hits if not ns)

    expect()
    client.execute_sql("DELETE FROM r")
    right = []
    expect()


def test_a_null_group_key_and_a_null_extremum_are_not_zero(client):
    """A NULL group key must not merge with the 0 group — in the grouping, in
    each group's extremes, or in a HAVING null test. A NULL → 0 transition of a
    MIN changes only the null bit, so the reduce's `old @ -1` / `new @ +1` pair
    must still differ: were the trace compared null-blind the stale NULL row
    would survive and surface on the next transition, which retracts it."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, g BIGINT, v BIGINT); "
        "CREATE VIEW gv AS SELECT g, COUNT(*) AS c, MIN(v) AS lo, MAX(v) AS hi FROM t GROUP BY g; "
        "CREATE VIEW gn AS SELECT g, COUNT(*) AS c FROM t GROUP BY g HAVING g IS NULL; "
        "CREATE VIEW m7 AS SELECT MIN(v) AS m FROM t WHERE g = 7; "
        "INSERT INTO t VALUES (1, NULL, 100), (2, 0, 7), (3, NULL, 50), (4, 0, 9), (5, 7, NULL)")

    def expect(seven, null_group=(2, 50, 100), zero_group=(2, 7, 9)):
        assert bag(scanned(client, "gv")) == {
            (None, *null_group): 1, (0, *zero_group): 1, (7, 1, seven, seven): 1}
        assert bag(scanned(client, "gn")) == {(None, null_group[0]): 1}
        assert bag(scanned(client, "m7")) == {(seven,): 1}

    expect(None)
    # Lower the NULL group's MIN and raise the zero group's MAX: neither moves the other.
    client.execute_sql("INSERT INTO t VALUES (6, NULL, 10), (7, 0, 999)")
    grown = {"null_group": (3, 10, 100), "zero_group": (3, 7, 999)}
    expect(None, **grown)
    client.execute_sql("UPDATE t SET v = 0 WHERE pk = 5")
    expect(0, **grown)
    client.execute_sql("UPDATE t SET v = 7 WHERE pk = 5")
    expect(7, **grown)


@pytest.mark.parametrize("kind", ["JOIN", "LEFT JOIN"])
def test_a_key_crossing_the_null_boundary_retracts_and_readmits_exactly(client, kind):
    """A key toggling NULL ↔ real moves the row between the match branch and the
    bypass branch, which must be exact complements at every tick. A stale verdict
    leaves the old row at weight 1 beside the new one.
    """
    client.execute_sql(
        "CREATE TABLE l (id BIGINT NOT NULL PRIMARY KEY, fk BIGINT); "
        "CREATE TABLE r (k BIGINT NOT NULL PRIMARY KEY, name BIGINT); "
        f"CREATE VIEW v AS SELECT l.id AS id, r.name AS name FROM l {kind} r ON l.fk = r.k; "
        "INSERT INTO r VALUES (5, 905)")
    unmatched = {(1, None): 1} if kind == "LEFT JOIN" else {}

    for stmt, want in (("INSERT INTO l VALUES (1, NULL)", unmatched),
                       ("UPDATE l SET fk = 5 WHERE id = 1", {(1, 905): 1}),
                       ("UPDATE l SET fk = NULL WHERE id = 1", unmatched),
                       ("DELETE FROM l WHERE id = 1", {})):
        client.execute_sql(stmt)
        assert bag(scanned(client, "v")) == want, stmt
