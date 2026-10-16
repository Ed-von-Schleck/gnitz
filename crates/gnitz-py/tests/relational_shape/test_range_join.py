"""The inner non-equi join: pure range (`n_eq == 0`) and band (`n_eq >= 1`).

Both sides reindex onto `[eq keys…, range key]`. A band join scatters its delta
by the eq PREFIX, so equal eq-values co-partition and the range walk runs
worker-local; a pure range join has no prefix, so its delta is broadcast to every
worker and walked against that worker's owned trace. Either way the output is
re-keyed onto the source-PK pair `(a.pk, b.pk)` and exchanged by it, so the view
is PK-partitioned like every other view.

Every assertion is a weighted bag of the pair identity: a broadcast that
double-counts shows up as weight W, which a row set cannot see. The outer
orientations are test_outer_join.py's.

Run with GNITZ_WORKERS=4: the broadcast and the eq-prefix scatter both collapse
to nothing at one worker.
"""
import operator
from collections import Counter

from _read import bag, scanned
from _shapes import RANGE_OPS, known
from _sql import churn, inserting, values

_PURE = {n: (op, known(f)) for n, (op, f) in RANGE_OPS.items()}
_EQ, _LE = known(operator.eq), known(operator.le)

# (statement, table, {pk: (k, k2, x-or-y)}); a `None` row deletes the pk.
_CHURN = [
    # The thin side first, then a delta wider than it in one epoch: residues mod
    # 23 put `x == y` pairs in the data, separating the inclusive operators from
    # the strict ones.
    inserting("b", {i: (i % 8, i % 2, i * 5 % 23) for i in range(1, 21)}),
    inserting("a", {i: (i % 8, i % 2, i * 7 % 23) for i in range(1, 61)}),
    # Narrow deltas on each side in turn: the other bilinear term.
    inserting("a", {100: (1, 1, 11)}),
    inserting("b", {100: (1, 1, 12)}),
    # A NULL in the equality key or the range column, on either side.
    inserting("a", {101: (None, 1, 5), 102: (1, 1, None)}),
    inserting("b", {101: (None, 1, 20), 102: (1, 1, None)}),
    # A pair's `+1` and `-1` come from opposite terms on different workers and
    # must cancel through the output exchange: the range column moves out, then
    # the equality key moves to another group.
    ("UPDATE a SET x = 22 WHERE id = 100", "a", {100: (1, 1, 22)}),
    ("UPDATE a SET k = 3 WHERE id = 100", "a", {100: (3, 1, 22)}),
    ("DELETE FROM b WHERE id = 100", "b", {100: None}),
    ("DELETE FROM a WHERE id <= 30", "a", {i: None for i in range(1, 31)}),
]


def test_a_range_join_is_the_product_its_predicate_admits_through_churn(client):
    """`ON a.x OP b.y` is the cross product filtered by the operator, each pair
    once, for every operator; a band matches only inside an equality group, on
    every equality column (`band2` is `n_eq = 2`); and a GROUP BY over a range
    join reads its output deltas like any other. Checked after each epoch of a
    churn on both sides."""
    pair = "SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON"
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT, k2 BIGINT NOT NULL, x BIGINT); "
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT, k2 BIGINT NOT NULL, y BIGINT); "
        + "; ".join(f"CREATE VIEW pure_{n} AS {pair} a.x {op} b.y" for n, (op, _) in _PURE.items())
        + f"; CREATE VIEW band AS {pair} a.k = b.k AND a.x <= b.y; "
        f"CREATE VIEW band2 AS {pair} a.k = b.k AND a.k2 = b.k2 AND a.x <= b.y; "
        "CREATE VIEW per_a AS SELECT aid, COUNT(*) AS n FROM pure_lt GROUP BY aid")

    state = {"a": {}, "b": {}}
    for sql in churn(client, state, _CHURN):

        def pairs(admits):
            return [(ai, bi) for ai, ar in state["a"].items() for bi, br in state["b"].items()
                    if admits(ar, br)]

        for n, (_, f) in _PURE.items():
            assert bag(scanned(client, f"pure_{n}"), "aid", "bid") == \
                dict.fromkeys(pairs(lambda ar, br: f(ar[2], br[2])), 1), (sql, n)
        assert bag(scanned(client, "band"), "aid", "bid") == dict.fromkeys(
            pairs(lambda ar, br: _EQ(ar[0], br[0]) and _LE(ar[2], br[2])), 1), sql
        assert bag(scanned(client, "band2"), "aid", "bid") == dict.fromkeys(
            pairs(lambda ar, br: _EQ(ar[0], br[0]) and ar[1] == br[1] and _LE(ar[2], br[2])), 1), sql
        assert bag(scanned(client, "per_a"), "aid", "n") == dict.fromkeys(
            Counter(ai for ai, _ in pairs(lambda ar, br: _PURE["lt"][1](ar[2], br[2]))).items(), 1), sql


def test_the_pair_pk_is_the_source_keys_at_their_own_width(client):
    """The output key is the source-PK pair. When the range column *is* the PK
    on both sides no co-partition shortcut applies, and the pair key is the pair
    of range values; compound source keys make a four-column, 32-byte pair key,
    whose hidden slots carry exactly the source key columns."""
    client.execute_sql(
        "CREATE TABLE ra (x BIGINT NOT NULL PRIMARY KEY); "
        "CREATE TABLE rb (y BIGINT NOT NULL PRIMARY KEY); "
        "CREATE VIEW on_pk AS SELECT ra.x AS aid, rb.y AS bid FROM ra JOIN rb ON ra.x < rb.y; "
        "CREATE TABLE pa (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, x BIGINT NOT NULL, PRIMARY KEY (k1, k2)); "
        "CREATE TABLE pb (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, y BIGINT NOT NULL, PRIMARY KEY (k1, k2)); "
        "CREATE VIEW wide AS SELECT pa.k1 AS ak1, pa.k2 AS ak2, pb.k1 AS bk1, pb.k2 AS bk2 "
        "FROM pa JOIN pb ON pa.x < pb.y; "
        f"INSERT INTO ra VALUES {values((x,) for x in range(1, 16))}; "
        f"INSERT INTO rb VALUES {values((y,) for y in range(5, 20))}; "
        f"INSERT INTO pa VALUES {values((i, i * 2, i * 7 % 19) for i in range(1, 13))}; "
        f"INSERT INTO pb VALUES {values((i, i * 3, i * 5 % 19) for i in range(1, 13))}")
    assert bag(scanned(client, "on_pk"), "aid", "bid") == {
        (x, y): 1 for x in range(1, 16) for y in range(5, 20) if x < y}

    def expect(b_ids):
        want = {(i, i * 2, j, j * 3): 1 for i in range(1, 13) for j in b_ids if i * 7 % 19 < j * 5 % 19}
        assert bag(scanned(client, "wide"), "ak1", "ak2", "bk1", "bk2") == want
        assert bag(scanned(client, "wide", hidden=True),
                   "_pair_pk_0", "_pair_pk_1", "_pair_pk_2", "_pair_pk_3") == want

    expect(range(1, 13))
    client.execute_sql("DELETE FROM pb WHERE k1 = 3 AND k2 = 9")
    expect([j for j in range(1, 13) if j != 3])
