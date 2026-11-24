"""Replicated placement: a whole copy of the relation on every worker.

Writes broadcast to every worker's copy, a read of a replicated relation is
served by one worker, and a mixed view's exchange never relays a replicated
source's delta: every worker already holds it, so each keeps its own share.

At one worker there is a single copy, so the whole module needs W > 1.
"""

import operator
from collections import Counter

import gnitz
import pytest
from _serverproc import NEEDS_MULTI
from _read import access, bag, reads_alike, rows, scanned
from _shapes import ONE_SOURCE_OPS, agg, groups, known, setop, views_ddl
from _sql import churn, insert, inserting

pytestmark = NEEDS_MULTI

_REPL = " WITH (replicated = true)"


# ── Reads return one copy, whatever the verb ─────────────────────────────────

# (id, cust, val); ids past 40 pad the table so that an indexed predicate
# selecting a few of the first 40 is selective enough for the worker to walk the
# index the plan names rather than scan.
_DIM = [(i, i % 7, i * 10) for i in range(1, 41)] + \
       [(i, 1000 + i % 7, 1_000_000 + i) for i in range(41, 401)]


@pytest.fixture(scope="module")
def dim(module_client):
    """A connection holding a replicated `dim` with both index kinds, and `dimv`,
    a passthrough view over it. Read-only."""
    conn = module_client
    conn.execute_sql(
        "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, cust BIGINT NOT NULL, "
        "val BIGINT NOT NULL)" + _REPL)
    insert(conn, "dim", _DIM)
    conn.execute_sql("CREATE INDEX ON dim(cust); CREATE UNIQUE INDEX ON dim(val); "
                     "CREATE VIEW dimv AS SELECT * FROM dim")
    return conn


def test_a_scan_of_a_replicated_table_returns_one_copy(dim):
    for rel in ("dim", "dimv"):
        assert bag(scanned(dim, rel), "id", "cust", "val") == dict.fromkeys(_DIM, 1), rel


@pytest.mark.parametrize("where,walk,keep", [
    pytest.param("cust = 0", "index range on (cust)", lambda i, c, v: c == 0, id="index-point"),
    pytest.param("val = 100", "index range on (val)", lambda i, c, v: v == 100, id="unique-index-point"),
    pytest.param("val BETWEEN 100 AND 200", "index range on (val)", lambda i, c, v: 100 <= v <= 200,
                 id="index-range"),
    pytest.param("val < 60", "index range on (val)", lambda i, c, v: v < 60, id="index-open-range"),
    pytest.param("cust = 1 AND val < 200", "index range on (cust)", lambda i, c, v: c == 1 and v < 200,
                 id="index-and-residual"),
    pytest.param("id = 23", "pk point lookup", lambda i, c, v: i == 23, id="pk-point"),
    pytest.param("id IN (1, 9, 17, 25, 33)", "pk set gather (5 keys)",
                 lambda i, c, v: i in (1, 9, 17, 25, 33), id="pk-in-list"),
    pytest.param("id BETWEEN 5 AND 12", "pk range walk", lambda i, c, v: 5 <= i <= 12, id="pk-range"),
])
def test_a_read_of_a_replicated_table_returns_one_copy(dim, where, walk, keep):
    """Every worker's copy of the table, and of a view over it, matches every
    predicate, so a read that gathered from all of them returns each row at
    weight W. The PK-keyed reads carry the opposite hazard: confining one to the
    worker its key would hash to under partitioning is correct only by accident
    of which copy answers. `dimv` has no index of its own, so it is read by the
    PK walks or a filtered scan."""
    q = "SELECT * FROM {} WHERE " + where
    assert access(dim, q.format("dim")) == walk
    want = {r: 1 for r in _DIM if keep(*r)}
    for rel in ("dim", "dimv"):
        assert bag(rows(dim, q.format(rel)), "id", "cust", "val") == want, rel


def test_an_aggregate_over_a_replicated_stream_counts_one_copy(client):
    """Every worker ingests the whole stream, so a view that combined the copies
    would count each row W times. A replicated table's aggregates are among the
    shapes below."""
    client.execute_sql(
        "CREATE TABLE src (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, "
        "amount BIGINT NOT NULL) WITH (stream = true, replicated = true); "
        "CREATE VIEW per_grp AS SELECT grp, COUNT(*) AS cnt, SUM(amount) AS total "
        "FROM src GROUP BY grp; "
        "CREATE VIEW overall AS SELECT COUNT(*) AS cnt, SUM(amount) AS total FROM src; "
        "CREATE VIEW top2 AS SELECT id, amount FROM src ORDER BY amount DESC LIMIT 2")

    assert bag(scanned(client, "per_grp"), "grp", "cnt", "total") == {}
    assert bag(scanned(client, "overall"), "cnt", "total") == {(0, None): 1}
    assert bag(scanned(client, "top2"), "id", "amount") == {}
    client.execute_sql("INSERT INTO src VALUES (1, 10, 100), (2, 10, 200), (3, 20, 350)")
    assert bag(scanned(client, "per_grp"), "grp", "cnt", "total") == {
        (10, 2, 300): 1, (20, 1, 350): 1}
    assert bag(scanned(client, "overall"), "cnt", "total") == {(3, 650): 1}
    assert bag(scanned(client, "top2"), "id", "amount") == {(3, 350): 1, (2, 200): 1}
    # Displacing a top-2 member retracts it once, not once per copy.
    client.execute_sql("INSERT INTO src VALUES (4, 20, 500)")
    assert bag(scanned(client, "per_grp"), "grp", "cnt", "total") == {
        (10, 2, 300): 1, (20, 2, 850): 1}
    assert bag(scanned(client, "overall"), "cnt", "total") == {(4, 1150): 1}
    assert bag(scanned(client, "top2"), "id", "amount") == {(4, 500): 1, (3, 350): 1}


# ── Writes reach every copy, and a view behind an exchange sits on its key's owner

# view over `agg` -> its body
_OVER_AGG = {
    "again": ("SELECT grp, SUM(n) AS n FROM agg GROUP BY grp",),
    "tagged": ("SELECT agg.grp AS grp, agg.s AS s, label.tag AS tag "
               "FROM agg JOIN label ON agg.grp = label.grp",),
}


def test_a_replicated_join_absorbs_every_write_once(client):
    """Replicated `dim` and `dim2`, a partitioned `fact`, and views over
    `fact ⋈ dim` checked after every write.

    A scan of a replicated table is served by one worker, whose copy is fine even
    when another's is not; the joins are the observable, because facts spread
    over every worker and each joins against its own worker's copy. The dims
    arrive in two rounds, so later dims re-join facts already in the trace, and
    every half of a write — an UPDATE's retraction and insertion, a DELETE, a raw
    push's upsert — must land on every copy exactly once.

    `j` is keyed by its join key, the dim id, but its rows sit on their fact's
    worker, so a seek of it must gather from every worker. `agg` groups the join,
    exchanging every row to its group's owner, so a read bounded to one group asks
    one worker, and `again` and `tagged` over it, grouped again or joined on that
    key, move nothing; a group anywhere but on its owner would split across
    workers into rows at partial counts. `band_n` is the same over a band join,
    whose output the planner shards.
    """
    client.execute_sql(
        "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL)" + _REPL + "; "
        "CREATE TABLE dim2 (id BIGINT NOT NULL PRIMARY KEY, b BIGINT NOT NULL)" + _REPL + "; "
        "CREATE TABLE fact (id BIGINT NOT NULL PRIMARY KEY, r1 BIGINT NOT NULL, "
        "r2 BIGINT NOT NULL, x BIGINT NOT NULL); "
        "CREATE TABLE label (grp BIGINT NOT NULL PRIMARY KEY, tag BIGINT NOT NULL); "
        "CREATE VIEW j AS SELECT fact.id AS fid, fact.r2 AS r2, dim.grp AS grp "
        "FROM fact JOIN dim ON fact.r1 = dim.id; "
        "CREATE VIEW star AS SELECT j.fid AS fid, j.grp AS grp, dim2.b AS b "
        "FROM j JOIN dim2 ON j.r2 = dim2.id; "
        "CREATE VIEW agg AS SELECT dim.grp AS grp, COUNT(*) AS n, SUM(fact.x) AS s "
        "FROM fact JOIN dim ON fact.r1 = dim.id GROUP BY dim.grp; "
        + views_ddl(_OVER_AGG) + "; "
        "CREATE VIEW band AS SELECT fact.id AS f, dim.id AS d "
        "FROM fact JOIN dim ON fact.r1 = dim.id AND fact.x <= dim.grp; "
        "CREATE VIEW band_n AS SELECT d, COUNT(*) AS n FROM band GROUP BY d")
    dim_id, dim_schema = client.resolve_table("dim")
    jid, j_schema = client.resolve_table("j")
    agg_id, agg_schema = client.resolve_table("agg")
    assert access(client, "SELECT grp, n, s FROM agg WHERE grp = 0") == "pk point lookup"

    facts = {i: (i % 9 + 1, i % 2 + 1, i) for i in range(1, 61)}   # (r1, r2, x)
    steps = [
        inserting("dim2", {1: 1000, 2: 2000}),
        inserting("dim", {d: d % 5 for d in range(1, 7)}),
        inserting("label", {g: 100 + g for g in range(4)}),
        inserting("fact", facts),
        inserting("dim", {d: d % 5 for d in range(7, 10)}),
        ("UPDATE dim SET grp = 3 WHERE id IN (1, 6)", "dim", {1: 3, 6: 3}),
        ("UPDATE fact SET x = x + 7 WHERE id <= 12", "fact",
         {i: (*facts[i][:2], facts[i][2] + 7) for i in range(1, 13)}),
        ("DELETE FROM dim WHERE id = 4", "dim", {4: None}),
        (lambda c: c.push(dim_id, gnitz.ZSetBatch(dim_schema).extend([{"id": 2, "grp": 4}])),
         "dim", {2: 4}),
        ("DELETE FROM fact WHERE id > 45", "fact", dict.fromkeys(range(46, 61))),
        ("DELETE FROM label WHERE grp = 3", "label", {3: None}),
        ("DELETE FROM dim2 WHERE id = 1", "dim2", {1: None}),
    ]
    state = {"dim": {}, "dim2": {}, "fact": {}, "label": {}}
    for sql in churn(client, state, steps):
        dims, dims2, labels = state["dim"], state["dim2"], state["label"]
        live = [(r1, r2, x) for r1, r2, x in state["fact"].values() if r1 in dims]

        # joined row -> the dim id it joined on, the key `j` is sought by
        joined = {(f, r2, dims[r1]): r1 for f, (r1, r2, _) in state["fact"].items() if r1 in dims}
        assert bag(scanned(client, "dim"), "id", "grp") == dict.fromkeys(dims.items(), 1), sql
        assert bag(scanned(client, "j"), "fid", "r2", "grp") == dict.fromkeys(joined, 1), sql
        assert bag(scanned(client, "star"), "fid", "grp", "b") == {
            (f, g, dims2[r2]): 1 for f, r2, g in joined if r2 in dims2}, sql
        for k in range(1, 10):
            assert bag(client.seek(jid, j_schema, pk=k), "fid", "r2", "grp") == {
                row: 1 for row, r1 in joined.items() if r1 == k}, (sql, k)

        by_grp = groups(((dims[r1], x) for r1, _, x in live), lambda row: row[0])
        want = {g: (len(rs), sum(x for _, x in rs)) for g, rs in by_grp.items()}
        assert bag(scanned(client, "agg"), "grp", "n", "s") == {
            (g, n, s): 1 for g, (n, s) in want.items()}, sql
        for g in range(6):
            one = {(g, *want[g]): 1} if g in want else {}
            assert bag(rows(client, f"SELECT grp, n, s FROM agg WHERE grp = {g}")) == one, (sql, g)
            assert bag(client.seek(agg_id, agg_schema, pk=g), "grp", "n", "s") == one, (sql, g)
        assert bag(scanned(client, "again"), "grp", "n") == {
            (g, n): 1 for g, (n, _) in want.items()}, sql
        assert bag(scanned(client, "tagged"), "grp", "s", "tag") == {
            (g, s, labels[g]): 1 for g, (_, s) in want.items() if g in labels}, sql
        banded = Counter(r1 for r1, _, x in live if x <= dims[r1])
        assert bag(scanned(client, "band_n"), "d", "n") == dict.fromkeys(banded.items(), 1), sql

    # A view of the same body created now is a pure backfill of the same rows.
    client.execute_sql(views_ddl({f"{n}2": v for n, v in _OVER_AGG.items()}))
    for n in _OVER_AGG:
        reads_alike(client, f"{n}2", n, key="grp")


# ── Every shape over every replicated placement ──────────────────────────────

_PAIR = "SELECT a.pk AS l, b.pk AS r FROM a"

# view -> (body, columns read back)
_VIEWS = {
    **{f"s_{n}": (f"SELECT k, x FROM a {op} SELECT k, y FROM b", ("k", "x")) for n, op in ONE_SOURCE_OPS.items()},
    "dist": ("SELECT DISTINCT k FROM a", ("k",)),
    "twice": ("SELECT k, x FROM a UNION ALL SELECT k, x FROM a", ("k", "x")),
    "eq": (f"{_PAIR} JOIN b ON a.k = b.k", ("l", "r")),
    "eq_left": (f"{_PAIR} LEFT JOIN b ON a.k = b.k", ("l", "r")),
    "eq_left_pk": (f"{_PAIR} LEFT JOIN b ON a.k = b.pk", ("l", "r")),
    "band": (f"{_PAIR} JOIN b ON a.k = b.k AND a.x <= b.y", ("l", "r")),
    "band_left": (f"{_PAIR} LEFT JOIN b ON a.k = b.k AND a.x <= b.y", ("l", "r")),
    "rng": (f"{_PAIR} JOIN b ON a.x < b.y", ("l", "r")),
    "rng_left": (f"{_PAIR} LEFT JOIN b ON a.x < b.y", ("l", "r")),
    "product": ("SELECT a.pk AS l, b.pk AS r FROM a, b", ("l", "r")),
    "exists_lt": ("SELECT pk FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.y < a.x)", ("pk",)),
    "not_exists_lt": ("SELECT pk FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.y < a.x)", ("pk",)),
    "in_b": ("SELECT pk FROM a WHERE k IN (SELECT k FROM b)", ("pk",)),
    "not_in_b": ("SELECT pk FROM a WHERE k NOT IN (SELECT k FROM b)", ("pk",)),
    "in_pk": ("SELECT pk FROM a WHERE k IN (SELECT pk FROM b)", ("pk",)),
    "not_in_pk": ("SELECT pk FROM a WHERE k NOT IN (SELECT pk FROM b)", ("pk",)),
    "lt_max": ("SELECT pk FROM a WHERE x < (SELECT MAX(y) FROM b)", ("pk",)),
    "n_b": ("SELECT pk, (SELECT COUNT(*) FROM b) AS n FROM a", ("pk", "n")),
    "per_k_a": ("SELECT k, COUNT(*) AS n, SUM(x) AS s FROM a GROUP BY k", ("k", "n", "s")),
    "overall": ("SELECT COUNT(*) AS n, SUM(x) AS s FROM a", ("n", "s")),
    "top2": ("SELECT pk, x FROM a ORDER BY pk DESC LIMIT 2", ("pk", "x")),
    "per_k": ("SELECT k, COUNT(*) AS n FROM s_ua GROUP BY k", ("k", "n")),
    "c_and_ua": ("SELECT k, x FROM c UNION ALL SELECT k, x FROM s_ua", ("k", "x")),
}

# (k, x) rows of `a` repeat, some of them also in `b`, and `x` is sometimes
# NULL; `c` is always partitioned.
_A = {p: (p % 4, None if p % 7 == 0 else p % 6) for p in range(1, 17)}
_B = {p: (p % 3, p % 5) for p in range(1, 13)}
_C = {p: (p % 4, p % 6) for p in range(1, 7)}

# (statement, table, changes): `a` meets a `b` never populated first, and one
# emptied again last; the backfilled twins see every write after it.
_CHURN = [
    inserting("a", _A), inserting("c", _C), inserting("b", _B),
    # Every shape again, created over populated sources: a pure backfill.
    (lambda c: c.execute_sql(views_ddl({f"{n}_bf": v for n, v in _VIEWS.items()})), {}),
    ("DELETE FROM a WHERE pk IN (5, 10, 15)", "a", dict.fromkeys((5, 10, 15))),
    ("UPDATE b SET y = y + 3 WHERE pk IN (1, 5, 9)",
     "b", {p: (_B[p][0], _B[p][1] + 3) for p in (1, 5, 9)}),
    ("DELETE FROM b WHERE pk > 8", "b", dict.fromkeys(range(9, 13))),
    ("DELETE FROM b", "b", dict.fromkeys(range(1, 9))),
]


_lt = known(operator.lt)


def _want(a, b, c):
    """Every view of `_VIEWS` over the rows `a`, `b`, `c` as `{pk: (k, x)}`."""
    ca, cb = Counter(a.values()), Counter(b.values())

    def pairs(on):
        return {(l, r): 1 for l, (ak, x) in a.items() for r, (bk, y) in b.items()
                if on(ak, x, bk, y)}

    def left(on):
        matched = pairs(on)
        return {**matched, **{(l, None): 1 for l in a if not any(m[0] == l for m in matched)}}

    def where(keep):
        return {(p,): 1 for p, (k, x) in a.items() if keep(k, x)}

    def band(ak, x, bk, y):
        return ak == bk and x is not None and x <= y

    def rng(ak, x, bk, y):
        return _lt(x, y)

    b_keys = {bk for bk, _ in b.values()}
    top = max((y for _, y in b.values()), default=None)
    return {
        **{f"s_{n}": setop(op, ca, cb) for n, op in ONE_SOURCE_OPS.items()},
        "dist": dict.fromkeys(((k,) for k, _ in a.values()), 1),
        "twice": ca + ca,
        "eq": pairs(lambda ak, x, bk, y: ak == bk),
        "eq_left": left(lambda ak, x, bk, y: ak == bk),
        "eq_left_pk": {(l, ak if ak in b else None): 1 for l, (ak, _) in a.items()},
        "band": pairs(band), "band_left": left(band),
        "rng": pairs(rng), "rng_left": left(rng),
        "product": pairs(lambda *_: True),
        "exists_lt": where(lambda k, x: any(_lt(y, x) for _, y in b.values())),
        "not_exists_lt": where(lambda k, x: not any(_lt(y, x) for _, y in b.values())),
        "in_b": where(lambda k, x: k in b_keys),
        "not_in_b": where(lambda k, x: k not in b_keys),
        "in_pk": where(lambda k, x: k in b),
        "not_in_pk": where(lambda k, x: k not in b),
        "lt_max": where(lambda k, x: _lt(x, top)),
        "n_b": {(p, len(b)): 1 for p in a},
        "per_k_a": {(k, len(rs), agg("SUM", [x for _, x in rs])): 1
                    for k, rs in groups(a.values(), lambda r: r[0]).items()},
        "overall": {(len(a), agg("SUM", [x for _, x in a.values()])): 1},
        "top2": {(p, a[p][1]): 1 for p in sorted(a)[-2:]},
        "per_k": dict.fromkeys(Counter(k for k, _ in (ca + cb).elements()).items(), 1),
        "c_and_ua": Counter(c.values()) + ca + cb,
    }


@pytest.mark.parametrize("a_repl,b_repl", [(True, True), (False, True), (True, False)],
                         ids=["both-replicated", "b-replicated", "a-replicated"])
def test_every_shape_keeps_its_weights_over_replicated_sources(client,
                                                               a_repl, b_repl):
    """Each view equals its Z-set definition after every write, whichever of its
    sources are replicated, whether it absorbed them by tick or by backfill.

    A view over only replicated sources is computed whole on every worker and
    read from one; a mixed view must take each replicated row once, not once per
    worker — a relayed replicated delta lands in the join trace or the union W
    times, and a replicated preserved side of an outer, semi or anti join clamped
    against one worker's slice of its partner null-fills or keeps a row once per
    worker. An uncorrelated scalar's one row must reach each outer row once, over
    a `b` never populated and one emptied again too, where it is the ground row.
    `c_and_ua` unions a partitioned table with `s_ua`, replicated when both its
    sources are.
    """
    client.execute_sql(
        f"CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, x BIGINT)"
        f"{_REPL if a_repl else ''}; "
        f"CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, y BIGINT NOT NULL)"
        f"{_REPL if b_repl else ''}; "
        "CREATE TABLE c (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, x BIGINT); "
        + views_ddl(_VIEWS))

    state = {"a": {}, "b": {}, "c": {}}
    twins = False
    for sql in churn(client, state, _CHURN):
        twins |= callable(sql)
        want = _want(state["a"], state["b"], state["c"])
        for name, (_, cols) in _VIEWS.items():
            for view in (name, f"{name}_bf") if twins else (name,):
                assert bag(scanned(client, view), *cols) == want[name], (sql, view)
