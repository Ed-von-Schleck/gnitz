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


def _cmp(op):
    """`op` under SQL 3VL: a comparison against NULL admits nothing."""
    return lambda l, r: l is not None and r is not None and op(l, r)


_PURE = {"lt": ("<", _cmp(operator.lt)), "le": ("<=", _cmp(operator.le)),
         "gt": (">", _cmp(operator.gt)), "ge": (">=", _cmp(operator.ge))}
_EQ, _LE = _cmp(operator.eq), _cmp(operator.le)

# (statement, table, {pk: (k, k2, x-or-y)}); a `None` row deletes the pk.
_CHURN = [
    # The thin side first, then a delta wider than it in one epoch: residues mod
    # 23 put `x == y` pairs in the data, separating the inclusive operators from
    # the strict ones.
    ("INSERT INTO b VALUES " + ", ".join(f"({i}, {i % 8}, {i % 2}, {i * 5 % 23})" for i in range(1, 21)),
     "b", {i: (i % 8, i % 2, i * 5 % 23) for i in range(1, 21)}),
    ("INSERT INTO a VALUES " + ", ".join(f"({i}, {i % 8}, {i % 2}, {i * 7 % 23})" for i in range(1, 61)),
     "a", {i: (i % 8, i % 2, i * 7 % 23) for i in range(1, 61)}),
    # Narrow deltas on each side in turn: the other bilinear term.
    ("INSERT INTO a VALUES (100, 1, 1, 11)", "a", {100: (1, 1, 11)}),
    ("INSERT INTO b VALUES (100, 1, 1, 12)", "b", {100: (1, 1, 12)}),
    # A NULL in the equality key or the range column, on either side.
    ("INSERT INTO a VALUES (101, NULL, 1, 5), (102, 1, 1, NULL)",
     "a", {101: (None, 1, 5), 102: (1, 1, None)}),
    ("INSERT INTO b VALUES (101, NULL, 1, 20), (102, 1, 1, NULL)",
     "b", {101: (None, 1, 20), 102: (1, 1, None)}),
    # A pair's `+1` and `-1` come from opposite terms on different workers and
    # must cancel through the output exchange: the range column moves out, then
    # the equality key moves to another group.
    ("UPDATE a SET x = 22 WHERE id = 100", "a", {100: (1, 1, 22)}),
    ("UPDATE a SET k = 3 WHERE id = 100", "a", {100: (3, 1, 22)}),
    ("DELETE FROM b WHERE id = 100", "b", {100: None}),
    ("DELETE FROM a WHERE id <= 30", "a", {i: None for i in range(1, 31)}),
]


def test_a_range_join_is_the_product_its_predicate_admits_through_churn(client, schema_name):
    """`ON a.x OP b.y` is the cross product filtered by the operator, each pair
    once, for every operator; a band matches only inside an equality group, on
    every equality column (`band2` is `n_eq = 2`); and a GROUP BY over a range
    join reads its output deltas like any other. Checked after each epoch of a
    churn on both sides."""
    sn = schema_name
    pair = "SELECT a.id AS aid, b.id AS bid FROM a JOIN b ON"
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT, k2 BIGINT NOT NULL, x BIGINT); "
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT, k2 BIGINT NOT NULL, y BIGINT); "
        + "; ".join(f"CREATE VIEW pure_{n} AS {pair} a.x {op} b.y" for n, (op, _) in _PURE.items())
        + f"; CREATE VIEW band AS {pair} a.k = b.k AND a.x <= b.y; "
        f"CREATE VIEW band2 AS {pair} a.k = b.k AND a.k2 = b.k2 AND a.x <= b.y; "
        "CREATE VIEW per_a AS SELECT aid, COUNT(*) AS n FROM pure_lt GROUP BY aid",
        schema_name=sn)

    state = {"a": {}, "b": {}}
    for sql, table, changes in _CHURN:
        client.execute_sql(sql, schema_name=sn)
        for pk, row in changes.items():
            if row is None:
                del state[table][pk]
            else:
                state[table][pk] = row

        def pairs(admits):
            return [(ai, bi) for ai, ar in state["a"].items() for bi, br in state["b"].items()
                    if admits(ar, br)]

        for n, (_, f) in _PURE.items():
            assert bag(scanned(client, sn, f"pure_{n}"), "aid", "bid") == \
                dict.fromkeys(pairs(lambda ar, br: f(ar[2], br[2])), 1), (sql, n)
        assert bag(scanned(client, sn, "band"), "aid", "bid") == dict.fromkeys(
            pairs(lambda ar, br: _EQ(ar[0], br[0]) and _LE(ar[2], br[2])), 1), sql
        assert bag(scanned(client, sn, "band2"), "aid", "bid") == dict.fromkeys(
            pairs(lambda ar, br: _EQ(ar[0], br[0]) and ar[1] == br[1] and _LE(ar[2], br[2])), 1), sql
        assert bag(scanned(client, sn, "per_a"), "aid", "n") == dict.fromkeys(
            Counter(ai for ai, _ in pairs(lambda ar, br: _PURE["lt"][1](ar[2], br[2]))).items(), 1), sql


def test_a_band_null_fill_over_a_join_keyed_input_counts_each_row_once(client, schema_name):
    """A band LEFT JOIN over a join-keyed input whose two rows differ only in the
    range value: each null-fills exactly while nothing matches it."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE p (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, g BIGINT NOT NULL, x BIGINT NOT NULL); "
        "CREATE TABLE q (k BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL); "
        "CREATE TABLE r (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, y BIGINT NOT NULL); "
        "CREATE VIEW band_over_join AS SELECT d.v AS v, r.id AS rid "
        "FROM (SELECT p.g AS g, p.x AS x, q.v AS v FROM p JOIN q ON p.k = q.k) d "
        "LEFT JOIN r ON d.g = r.g AND d.x <= r.y",
        schema_name=sn)
    churn = [
        ("INSERT INTO q VALUES (1, 7), (2, 8)", "q", {1: (7,), 2: (8,)}),
        # p(1) and p(2) share `k` and so `v`, and differ only in `x`.
        ("INSERT INTO p VALUES (1, 1, 5, 10), (2, 1, 5, 100), (3, 2, 5, 40)",
         "p", {1: (1, 5, 10), 2: (1, 5, 100), 3: (2, 5, 40)}),
        # p(1) matches both, p(2) neither.
        ("INSERT INTO r VALUES (1, 5, 50), (2, 5, 60)", "r", {1: (5, 50), 2: (5, 60)}),
        ("UPDATE p SET x = 55 WHERE id = 2", "p", {2: (1, 5, 55)}),
        ("DELETE FROM r WHERE id = 2", "r", {2: None}),
        ("DELETE FROM q WHERE k = 1", "q", {1: None}),
    ]
    state = {"p": {}, "q": {}, "r": {}}
    for sql, table, changes in churn:
        client.execute_sql(sql, schema_name=sn)
        for pk, row in changes.items():
            if row is None:
                del state[table][pk]
            else:
                state[table][pk] = row
        want = Counter()
        for k, g, x in state["p"].values():
            if k not in state["q"]:
                continue
            v = state["q"][k][0]
            matched = [ri for ri, (rg, y) in state["r"].items() if rg == g and x <= y]
            want.update([(v, ri) for ri in matched] or [(v, None)])
        assert bag(scanned(client, sn, "band_over_join"), "v", "rid") == want, sql


def test_the_pair_pk_is_the_source_keys_at_their_own_width(client, schema_name):
    """The output key is the source-PK pair. When the range column *is* the PK
    on both sides no co-partition shortcut applies, and the pair key is the pair
    of range values; compound source keys make a four-column, 32-byte pair key,
    whose hidden slots carry exactly the source key columns."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE ra (x BIGINT NOT NULL PRIMARY KEY); "
        "CREATE TABLE rb (y BIGINT NOT NULL PRIMARY KEY); "
        "CREATE VIEW on_pk AS SELECT ra.x AS aid, rb.y AS bid FROM ra JOIN rb ON ra.x < rb.y; "
        "CREATE TABLE pa (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, x BIGINT NOT NULL, PRIMARY KEY (k1, k2)); "
        "CREATE TABLE pb (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, y BIGINT NOT NULL, PRIMARY KEY (k1, k2)); "
        "CREATE VIEW wide AS SELECT pa.k1 AS ak1, pa.k2 AS ak2, pb.k1 AS bk1, pb.k2 AS bk2 "
        "FROM pa JOIN pb ON pa.x < pb.y; "
        f"INSERT INTO ra VALUES {', '.join(f'({x})' for x in range(1, 16))}; "
        f"INSERT INTO rb VALUES {', '.join(f'({y})' for y in range(5, 20))}; "
        f"INSERT INTO pa VALUES {', '.join(f'({i}, {i * 2}, {i * 7 % 19})' for i in range(1, 13))}; "
        f"INSERT INTO pb VALUES {', '.join(f'({i}, {i * 3}, {i * 5 % 19})' for i in range(1, 13))}",
        schema_name=sn)
    assert bag(scanned(client, sn, "on_pk"), "aid", "bid") == {
        (x, y): 1 for x in range(1, 16) for y in range(5, 20) if x < y}

    def expect(b_ids):
        want = {(i, i * 2, j, j * 3): 1 for i in range(1, 13) for j in b_ids if i * 7 % 19 < j * 5 % 19}
        assert bag(scanned(client, sn, "wide"), "ak1", "ak2", "bk1", "bk2") == want
        wide = client.resolve_table(sn, "wide")[0]
        assert bag(client.scan(wide).including_hidden(),
                   "_pair_pk_0", "_pair_pk_1", "_pair_pk_2", "_pair_pk_3") == want

    expect(range(1, 13))
    client.execute_sql("DELETE FROM pb WHERE k1 = 3 AND k2 = 9", schema_name=sn)
    expect([j for j in range(1, 13) if j != 3])
