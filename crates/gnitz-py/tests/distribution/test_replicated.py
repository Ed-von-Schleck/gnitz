"""Replicated placement: a whole copy of the relation on every worker.

Writes broadcast to every worker's copy, a read of a replicated relation is
served by one worker, and a mixed view's exchange relays a replicated source's
delta from one worker only.

Everything here is asserted by WEIGHT, never by row presence. The failures this
placement admits leave the row *set* right and only the weights wrong — a read
gathered from all W copies, a delta relayed once per worker and consolidated into
one row at weight W — so a presence check passes under them. At one worker there
is a single copy and none of it can happen, so the whole module needs W > 1.
"""

from collections import Counter

import gnitz
import pytest
from _serverproc import NEEDS_MULTI
from _read import bag, rows, scanned
from _sql import insert, values

pytestmark = NEEDS_MULTI

_REPL = " WITH (replicated = true)"


def _apply(state, changes):
    """`changes` as `{pk: row}` onto `state`; a `None` row deletes the pk."""
    for pk, row in changes.items():
        if row is None:
            del state[pk]
        else:
            state[pk] = row


# ── Reads return one copy, whatever the verb ─────────────────────────────────

# (id, cust, val); ids past 40 pad the table so that an indexed predicate
# selecting a few of the first 40 clears the worker's selectivity gate and
# walks the index rather than falling back to a scan.
_DIM = [(i, i % 7, i * 10) for i in range(1, 41)] + \
       [(i, 1000 + i % 7, 1_000_000 + i) for i in range(41, 401)]


@pytest.fixture(scope="module")
def dim(module_schema):
    """A replicated `dim` carrying both index kinds. Read-only."""
    conn, sn = module_schema
    conn.execute_sql(
        "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, cust BIGINT NOT NULL, "
        "val BIGINT NOT NULL)" + _REPL, schema_name=sn)
    insert(conn, sn, "dim", _DIM)
    conn.execute_sql("CREATE INDEX ON dim(cust); CREATE UNIQUE INDEX ON dim(val)",
                     schema_name=sn)
    return sn


@pytest.mark.parametrize("where,keep", [
    pytest.param(None, lambda i, c, v: True, id="scan"),
    pytest.param("cust = 0", lambda i, c, v: c == 0, id="index-point"),
    pytest.param("cust = 99", lambda i, c, v: False, id="index-miss"),
    pytest.param("val = 100", lambda i, c, v: v == 100, id="unique-index-point"),
    pytest.param("val = 7", lambda i, c, v: False, id="unique-index-miss"),
    pytest.param("val BETWEEN 100 AND 200", lambda i, c, v: 100 <= v <= 200, id="index-range"),
    pytest.param("val < 60", lambda i, c, v: v < 60, id="index-open-range"),
    pytest.param("cust = 1 AND val < 200", lambda i, c, v: c == 1 and v < 200,
                 id="index-and-residual"),
    pytest.param("id = 23", lambda i, c, v: i == 23, id="pk-point"),
    pytest.param("id IN (1, 9, 17, 25, 33)", lambda i, c, v: i in (1, 9, 17, 25, 33),
                 id="pk-in-list"),
    pytest.param("id BETWEEN 5 AND 12", lambda i, c, v: 5 <= i <= 12, id="pk-range"),
])
def test_a_read_of_a_replicated_table_returns_one_copy(client, dim, where, keep):
    """Every worker's copy matches every predicate, so a read that gathered from
    all of them returns each row at weight W. The PK-keyed reads carry the
    opposite hazard: confining one to the worker its key would hash to under
    partitioning is correct only by accident of which copy answers."""
    got = (scanned(client, dim, "dim") if where is None
           else rows(client, dim, f"SELECT * FROM dim WHERE {where}"))
    assert bag(got, "id", "cust", "val") == {r: 1 for r in _DIM if keep(*r)}


@pytest.mark.parametrize("options", ["replicated = true", "stream = true, replicated = true"],
                         ids=["table", "stream"])
def test_an_aggregate_over_a_replicated_source_counts_one_copy(client, schema_name, options):
    """Every worker reduces its own whole copy, so a reduce that exchanged or
    combined across workers would multiply COUNT and SUM by W, and a global
    aggregate over the empty source would ground once per worker instead of once.
    A replicated stream holds no copy to read but must be placed the same way."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE src (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, "
        f"amount BIGINT NOT NULL) WITH ({options}); "
        "CREATE VIEW per_grp AS SELECT grp, COUNT(*) AS cnt, SUM(amount) AS total "
        "FROM src GROUP BY grp; "
        "CREATE VIEW overall AS SELECT COUNT(*) AS cnt, SUM(amount) AS total FROM src",
        schema_name=sn)

    assert bag(scanned(client, sn, "per_grp"), "grp", "cnt", "total") == {}
    assert bag(scanned(client, sn, "overall"), "cnt", "total") == {(0, None): 1}
    client.execute_sql("INSERT INTO src VALUES (1, 10, 100), (2, 10, 200), (3, 20, 350)",
                       schema_name=sn)
    assert bag(scanned(client, sn, "per_grp"), "grp", "cnt", "total") == {
        (10, 2, 300): 1, (20, 1, 350): 1}
    assert bag(scanned(client, sn, "overall"), "cnt", "total") == {(3, 650): 1}


# ── Writes reach every copy ──────────────────────────────────────────────────

# fact id -> (dim id, dim2 id)
_FACTS = {i: (i % 4 + 1, i % 2 + 1) for i in range(1, 41)}


def test_every_write_reaches_every_copy(client, schema_name):
    """Replicated `dim` and `dim2`, a partitioned `fact`, and the star
    `(fact ⋈ dim) ⋈ dim2` over them, checked after every write.

    A scan of a replicated table is served by one worker, whose copy is fine even
    when another's is not; the joins are the observable, because facts spread
    over every worker and each joins against its own worker's copy. The dims
    arrive in two rounds, so dims 3 and 4 re-join facts already in the trace, and
    every half of a write — an UPDATE's retraction and insertion, a DELETE, a raw
    push's upsert — must land on every copy exactly once.

    `j` is keyed by its join key, the dim id, but its rows sit on their fact's
    worker, so a seek of it must gather from every worker.
    """
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, nm BIGINT NOT NULL)" + _REPL + "; "
        "CREATE TABLE dim2 (id BIGINT NOT NULL PRIMARY KEY, b BIGINT NOT NULL)" + _REPL + "; "
        "CREATE TABLE fact (id BIGINT NOT NULL PRIMARY KEY, r1 BIGINT NOT NULL, "
        "r2 BIGINT NOT NULL); "
        "CREATE VIEW j AS SELECT fact.id AS fid, fact.r2 AS r2, dim.nm AS nm "
        "FROM fact JOIN dim ON fact.r1 = dim.id; "
        "CREATE VIEW star AS SELECT j.fid AS fid, j.nm AS nm, dim2.b AS b "
        "FROM j JOIN dim2 ON j.r2 = dim2.id", schema_name=sn)
    dim_id, dim_schema = client.resolve_table(sn, "dim")
    jid = client.resolve_table(sn, "j")[0]

    # (statement, table, changes); a `None` statement pushes the changes raw.
    steps = [
        ("INSERT INTO dim2 VALUES (1, 1000), (2, 2000)", "dim2", {1: 1000, 2: 2000}),
        ("INSERT INTO dim VALUES (1, 100), (2, 200)", "dim", {1: 100, 2: 200}),
        (f"INSERT INTO fact VALUES {values((i, *r) for i, r in _FACTS.items())}",
         "fact", _FACTS),
        ("INSERT INTO dim VALUES (3, 300), (4, 400)", "dim", {3: 300, 4: 400}),
        ("UPDATE dim SET nm = 999 WHERE id = 2", "dim", {2: 999}),
        ("DELETE FROM dim WHERE id = 3", "dim", {3: None}),
        (None, "dim", {1: 555}),
        ("DELETE FROM dim2 WHERE id = 1", "dim2", {1: None}),
    ]
    state = {"dim": {}, "dim2": {}, "fact": {}}
    for sql, table, changes in steps:
        if sql is None:
            batch = gnitz.ZSetBatch(dim_schema)
            for pk, nm in changes.items():
                batch.append(id=pk, nm=nm)
            client.push(dim_id, batch)
        else:
            client.execute_sql(sql, schema_name=sn)
        _apply(state[table], changes)
        dims, dims2 = state["dim"], state["dim2"]

        # joined row -> the dim id it joined on, the key `j` is sought by
        joined = {(f, r2, dims[r1]): r1 for f, (r1, r2) in state["fact"].items() if r1 in dims}
        assert bag(scanned(client, sn, "dim"), "id", "nm") == dict.fromkeys(dims.items(), 1), sql
        assert bag(scanned(client, sn, "j"), "fid", "r2", "nm") == dict.fromkeys(joined, 1), sql
        assert bag(scanned(client, sn, "star"), "fid", "nm", "b") == {
            (f, nm, dims2[r2]): 1 for f, r2, nm in joined if r2 in dims2}, sql
        for k in range(1, 5):
            assert bag(client.seek(jid, pk=k), "fid", "r2", "nm") == {
                row: 1 for row, r1 in joined.items() if r1 == k}, (sql, k)


# ── Every shape over every replicated placement ──────────────────────────────

_SETOPS = {"ua": "UNION ALL", "u": "UNION", "ia": "INTERSECT ALL", "i": "INTERSECT",
           "ea": "EXCEPT ALL", "e": "EXCEPT"}
_PAIR = "SELECT a.pk AS l, b.pk AS r FROM a"

# view -> (columns read back, body)
_VIEWS = {
    **{n: (("k", "x"), f"SELECT k, x FROM a {op} SELECT k, y FROM b") for n, op in _SETOPS.items()},
    "dist": (("k",), "SELECT DISTINCT k FROM a"),
    "twice": (("k", "x"), "SELECT k, x FROM a UNION ALL SELECT k, x FROM a"),
    "eq": (("l", "r"), f"{_PAIR} JOIN b ON a.k = b.k"),
    "eq_left": (("l", "r"), f"{_PAIR} LEFT JOIN b ON a.k = b.k"),
    "eq_left_pk": (("l", "r"), f"{_PAIR} LEFT JOIN b ON a.k = b.pk"),
    "band": (("l", "r"), f"{_PAIR} JOIN b ON a.k = b.k AND a.x <= b.y"),
    "band_left": (("l", "r"), f"{_PAIR} LEFT JOIN b ON a.k = b.k AND a.x <= b.y"),
    "rng": (("l", "r"), f"{_PAIR} JOIN b ON a.x < b.y"),
    "rng_left": (("l", "r"), f"{_PAIR} LEFT JOIN b ON a.x < b.y"),
    "product": (("l", "r"), "SELECT a.pk AS l, b.pk AS r FROM a, b"),
    "exists_lt": (("pk",), "SELECT pk FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.y < a.x)"),
    "not_exists_lt": (("pk",),
                      "SELECT pk FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.y < a.x)"),
    "in_b": (("pk",), "SELECT pk FROM a WHERE k IN (SELECT k FROM b)"),
    "not_in_b": (("pk",), "SELECT pk FROM a WHERE k NOT IN (SELECT k FROM b)"),
    "in_pk": (("pk",), "SELECT pk FROM a WHERE k IN (SELECT pk FROM b)"),
    "not_in_pk": (("pk",), "SELECT pk FROM a WHERE k NOT IN (SELECT pk FROM b)"),
    "per_k": (("k", "n"), "SELECT k, COUNT(*) AS n FROM ua GROUP BY k"),
    "c_and_ua": (("k", "x"), "SELECT k, x FROM c UNION ALL SELECT k, x FROM ua"),
}

# (k, x) rows of `a` repeat, some of them also in `b`, and `x` is sometimes
# NULL; `c` is always partitioned.
_A = {p: (p % 4, None if p % 7 == 0 else p % 6) for p in range(1, 17)}
_B = {p: (p % 3, p % 5) for p in range(1, 13)}
_C = {p: (p % 4, p % 6) for p in range(1, 7)}

# (statement, table, changes)
_CHURN = [
    (f"INSERT INTO b VALUES {values((p, *r) for p, r in _B.items())}", "b", _B),
    (f"INSERT INTO c VALUES {values((p, *r) for p, r in _C.items())}", "c", _C),
    (f"INSERT INTO a VALUES {values((p, *r) for p, r in _A.items())}", "a", _A),
    ("DELETE FROM a WHERE pk IN (5, 10, 15)", "a", dict.fromkeys((5, 10, 15))),
    ("UPDATE b SET y = y + 3 WHERE pk IN (1, 5, 9)",
     "b", {p: (_B[p][0], _B[p][1] + 3) for p in (1, 5, 9)}),
    ("DELETE FROM b WHERE pk > 8", "b", dict.fromkeys(range(9, 13))),
]


def _lt(l, r):
    return l is not None and r is not None and l < r


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
    return {
        "ua": ca + cb, "u": dict.fromkeys(ca | cb, 1),
        "ia": ca & cb, "i": dict.fromkeys(ca & cb, 1),
        "ea": ca - cb, "e": dict.fromkeys(ca.keys() - cb.keys(), 1),
        "dist": dict.fromkeys(((k,) for k, _ in a.values()), 1),
        "twice": ca + ca,
        "eq": pairs(lambda ak, x, bk, y: ak == bk),
        "eq_left": left(lambda ak, x, bk, y: ak == bk),
        "eq_left_pk": {**{(l, ak): 1 for l, (ak, _) in a.items() if ak in b},
                       **{(l, None): 1 for l, (ak, _) in a.items() if ak not in b}},
        "band": pairs(band), "band_left": left(band),
        "rng": pairs(rng), "rng_left": left(rng),
        "product": pairs(lambda *_: True),
        "exists_lt": where(lambda k, x: any(_lt(y, x) for _, y in b.values())),
        "not_exists_lt": where(lambda k, x: not any(_lt(y, x) for _, y in b.values())),
        "in_b": where(lambda k, x: k in b_keys),
        "not_in_b": where(lambda k, x: k not in b_keys),
        "in_pk": where(lambda k, x: k in b),
        "not_in_pk": where(lambda k, x: k not in b),
        "per_k": dict.fromkeys(Counter(k for k, _ in (ca + cb).elements()).items(), 1),
        "c_and_ua": Counter(c.values()) + ca + cb,
    }


@pytest.mark.parametrize("a_repl,b_repl", [(True, True), (False, True), (True, False)],
                         ids=["both-replicated", "b-replicated", "a-replicated"])
def test_every_shape_keeps_its_weights_over_replicated_sources(client, schema_name,
                                                               a_repl, b_repl):
    """Each view equals its Z-set definition after every epoch, whichever of its
    sources are replicated.

    A view over only replicated sources is itself replicated: every worker
    computes all of it and the read is served by one, so a set-op, DISTINCT or
    range-join circuit that still scattered its output would lose the rows hashed
    away from that worker and inflate the rest. A mixed view does exchange, and
    relaying the replicated side's delta from every worker instead of one would
    put W copies of each of its rows in the join trace or the union, where
    consolidation sums them into one row at weight W. A replicated preserved side
    of an outer, semi or anti join cannot stay local against a partitioned
    partner either: every worker would clamp its whole copy against its own slice
    of the partner, null-filling or keeping a row once per worker. `c_and_ua`
    unions a partitioned table with `ua`, which is replicated when both its
    sources are.
    """
    sn = schema_name
    client.execute_sql(
        f"CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, x BIGINT)"
        f"{_REPL if a_repl else ''}; "
        f"CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, y BIGINT NOT NULL)"
        f"{_REPL if b_repl else ''}; "
        "CREATE TABLE c (pk BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, x BIGINT); "
        + "; ".join(f"CREATE VIEW {n} AS {body}" for n, (_, body) in _VIEWS.items()),
        schema_name=sn)

    state = {"a": {}, "b": {}, "c": {}}
    for sql, table, changes in _CHURN:
        client.execute_sql(sql, schema_name=sn)
        _apply(state[table], changes)
        want = _want(state["a"], state["b"], state["c"])
        for name, (cols, _) in _VIEWS.items():
            assert bag(scanned(client, sn, name), *cols) == want[name], (sql, name)
