"""Replicated placement: a whole copy of the relation on every worker.

Writes broadcast to every worker's ingest and SAL, reads single-source one copy,
and a join against a partitioned fact runs locally on every worker with no
exchange on either side.

Everything here is asserted by WEIGHT, never by row presence. The failures this
placement admits leave the row *set* right and only the weights wrong — a read
gathered from all W copies, a delta relayed once per worker and consolidated into
one row at weight W — so a presence check passes under the bug either way. At one
worker there is a single copy and none of it can happen, which is what the
`NEEDS_MULTI` marks record rather than paper over.
"""

import pytest
import gnitz
from _serverproc import NEEDS_MULTI
from _read import bag, scanned

_REPL = " WITH (replicated = true)"


def _select(client, sn, sql):
    """Positive-weight PKs of a direct SELECT, duplicates kept.

    A ×W read inflation returns the same row once per worker, so it shows up as
    repeated PKs here; a set or a dict comparison would hide it.
    """
    res = client.execute_sql(sql, schema_name=sn)
    assert res[0]["type"] == "Rows", f"expected Rows, got {res[0]['type']}"
    b = res[0]["rows"]
    if b.schema is None:
        return []
    return sorted(pk for pk, w in zip(b.pks, b.weights) if w > 0)


# ── Reads single-source one copy, whatever the verb ──────────────────────────

_NDIM = 40


@pytest.fixture
def dim_indexed(client, schema_name):
    """A replicated `dim` carrying both index kinds over 40 rows.

    One table serves every read verb below, and the two indexes coexisting on a
    replicated owner is itself a case none of the single-index spellings reached.
    """
    client.execute_sql(
        "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, cust BIGINT NOT NULL, "
        "val BIGINT NOT NULL)" + _REPL, schema_name=schema_name)
    client.execute_sql(
        "INSERT INTO dim VALUES " + ",".join(
            f"({i}, {i % 7}, {i * 10})" for i in range(1, _NDIM + 1)),
        schema_name=schema_name)
    client.execute_sql("CREATE INDEX ON dim(cust)", schema_name=schema_name)
    client.execute_sql("CREATE UNIQUE INDEX ON dim(val)", schema_name=schema_name)
    return schema_name


_ONE_COPY_READS = [
    ("non-unique-index-point", "cust = 0", [i for i in range(1, _NDIM + 1) if i % 7 == 0]),
    ("non-unique-index-miss", "cust = 99", []),
    ("unique-index-point", "val = 100", [10]),
    ("unique-index-miss", "val = 7", []),
    ("index-range", "val BETWEEN 100 AND 200", list(range(10, 21))),
    ("index-open-range", "val > 350", list(range(36, _NDIM + 1))),
    ("pk-in-list", "id IN (1, 9, 17, 25, 33)", [1, 9, 17, 25, 33]),
    ("pk-point", "id = 23", [23]),
    ("unindexed-predicate", "cust = 1 AND val < 200", [1, 8, 15]),
]


@NEEDS_MULTI
@pytest.mark.parametrize("where,want", [c[1:] for c in _ONE_COPY_READS],
                         ids=[c[0] for c in _ONE_COPY_READS])
def test_a_read_of_a_replicated_table_returns_one_copy(client, dim_indexed, where, want):
    """Every read verb single-sources one worker's copy.

    A seek, an index range and a bounded PK read all broadcast and merge; on a
    replicated owner every worker matches, so without single-sourcing the client
    sees each row W times. The PK-keyed reads carry the opposite hazard: worker 0
    holds one child covering the whole table, so confining the read to the keys
    that worker would own under partitioning drops most of the answer.
    """
    assert _select(client, dim_indexed, f"SELECT * FROM dim WHERE {where}") == want


@NEEDS_MULTI
def test_a_scan_of_a_replicated_table_returns_one_copy(client, schema_name):
    """The unbounded read, weight-exact: five rows at weight 1, not W."""
    client.execute_sql(
        "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, name BIGINT NOT NULL)" + _REPL,
        schema_name=schema_name)
    client.execute_sql(
        "INSERT INTO dim VALUES (1,100),(2,200),(3,300),(4,400),(5,500)",
        schema_name=schema_name)
    tid, _ = client.resolve_table(schema_name, "dim")
    assert bag(client.scan(tid), "id", "name") == {
        (i, i * 100): 1 for i in range(1, 6)}


@NEEDS_MULTI
@pytest.mark.parametrize("options", ["replicated = true", "stream = true, replicated = true"],
                         ids=["table", "stream"])
def test_an_aggregate_over_a_replicated_source_is_not_w_folded(client, schema_name, options):
    """A reduce over a replicated input must not shard.

    A sharded reduce would fold each of the W copies in, multiplying COUNT and
    SUM by the worker count; the planner builds the shard-free local reduce and
    the replicated output is single-sourced on read. The two groups carry
    different totals, so swapping them fails too. A replicated stream must be
    known as replicated just the same, though it holds no copy to read.
    """
    client.execute_sql(
        "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, "
        f"amount BIGINT NOT NULL) WITH ({options})", schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW v AS SELECT grp, COUNT(*) AS cnt, SUM(amount) AS total "
        "FROM dim GROUP BY grp", schema_name=schema_name)
    client.execute_sql(
        "INSERT INTO dim VALUES (1, 10, 100), (2, 10, 200), (3, 20, 350)",
        schema_name=schema_name)

    vid, _ = client.resolve_table(schema_name, "v")
    assert bag(client.scan(vid), "grp", "cnt", "total") == {(10, 2, 300): 1, (20, 1, 350): 1}


@NEEDS_MULTI
def test_a_replicated_source_grounds_exactly_once(client, schema_name):
    """A global aggregate's ground row is where a backfill over an *empty*
    replicated source can go wrong in both directions: drop the owner disjunct
    and it emits none, forget the disjunct is exclusive and it emits one per
    worker. The linear view alongside is the control — the same empty backfill
    must produce no row at all there."""
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)" + _REPL,
        schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW v AS SELECT COUNT(*) AS cnt, SUM(a) AS total FROM t",
        schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW f AS SELECT pk, a * 2 AS d FROM t WHERE a > 0", schema_name=schema_name)
    vid, _ = client.resolve_table(schema_name, "v")
    fid, _ = client.resolve_table(schema_name, "f")

    assert bag(client.scan(vid), "cnt", "total") == {(0, None): 1}, "empty"
    assert bag(client.scan(fid), "pk", "d") == {}, "empty"
    client.execute_sql("INSERT INTO t VALUES (1, 5), (2, 7)", schema_name=schema_name)
    assert bag(client.scan(vid), "cnt", "total") == {(2, 12): 1}, "filled"
    assert bag(client.scan(fid), "pk", "d") == {(1, 10): 1, (2, 14): 1}, "filled"


# ── Writes reach every copy ──────────────────────────────────────────────────

_NFACT = 40
_DIMS = {i: i * 100 for i in range(1, 5)}


def _fact_dim(i):
    """The dim row fact `i` references."""
    return (i % 4) + 1


def _joined(dim_state):
    """The join's expected weight-multiset given the dim rows still live."""
    return {(i, dim_state[_fact_dim(i)]): 1
            for i in range(1, _NFACT + 1) if _fact_dim(i) in dim_state}


@pytest.fixture
def dim_fact_join(client, schema_name):
    """A replicated `dim`, a partitioned `fact`, and the join over them — DDL only.

    The join is the observable, because a scan of the replicated table alone is
    single-sourced to one worker whose copy is fine even when another worker's is
    not. Facts spread over every worker by their own PK and the join runs locally
    against each worker's copy, so a copy that missed a write shows up as missing
    or mis-valued join rows.
    """
    client.execute_sql(
        "CREATE TABLE dim (dim_id BIGINT NOT NULL PRIMARY KEY, name BIGINT NOT NULL)" + _REPL,
        schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE fact (fact_id BIGINT NOT NULL PRIMARY KEY, dim_ref BIGINT NOT NULL)",
        schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW j AS SELECT fact.fact_id AS fid, dim.name AS nm "
        "FROM fact JOIN dim ON fact.dim_ref = dim.dim_id", schema_name=schema_name)
    return (schema_name,
            client.resolve_table(schema_name, "dim")[0],
            client.resolve_table(schema_name, "j")[0])


def _insert_facts(client, sn):
    client.execute_sql(
        "INSERT INTO fact VALUES " + ", ".join(
            f"({i}, {_fact_dim(i)})" for i in range(1, _NFACT + 1)), schema_name=sn)


@NEEDS_MULTI
@pytest.mark.parametrize("mutation,dim_after", [
    ("DELETE FROM dim WHERE dim_id = 3", {k: v for k, v in _DIMS.items() if k != 3}),
    ("UPDATE dim SET name = 999 WHERE dim_id = 2", {**_DIMS, 2: 999}),
], ids=["delete", "update"])
def test_a_mutation_reaches_every_copy(client, dim_fact_join, mutation, dim_after):
    """Both halves of a write broadcast to every worker's copy.

    A DELETE is a retraction and an UPDATE is a retraction plus an insertion; a
    copy that missed either half keeps joining the stale row. The join is checked
    weight-exact, so a retraction applied twice — or to no copy at all — fails
    where a comparison of the surviving fact ids would not.
    """
    sn, tid, jid = dim_fact_join
    client.execute_sql(
        "INSERT INTO dim VALUES " + ", ".join(f"({k}, {v})" for k, v in _DIMS.items()),
        schema_name=sn)
    _insert_facts(client, sn)

    client.execute_sql(mutation, schema_name=sn)

    assert bag(client.scan(tid), "dim_id", "name") == {
        (k, v): 1 for k, v in dim_after.items()}
    assert bag(client.scan(jid), "fid", "nm") == _joined(dim_after)


@NEEDS_MULTI
def test_a_late_dim_delta_rejoins_the_waiting_facts(client, dim_fact_join):
    """A dim row arriving after the facts must broadcast and re-join on EVERY
    worker — the symmetric DBSP term (dim delta ⋈ fact trace) realized against
    each worker's own copy. Facts whose dim is absent produce nothing until it
    lands; once it does, every such fact gains its row exactly once, wherever it
    happens to live.
    """
    sn, _, jid = dim_fact_join
    early = {k: v for k, v in _DIMS.items() if k in (1, 2)}
    client.execute_sql(
        "INSERT INTO dim VALUES " + ", ".join(f"({k}, {v})" for k, v in early.items()),
        schema_name=sn)
    _insert_facts(client, sn)
    assert bag(client.scan(jid), "fid", "nm") == _joined(early), "before the late dim"

    client.execute_sql(
        "INSERT INTO dim VALUES " + ", ".join(
            f"({k}, {v})" for k, v in _DIMS.items() if k not in early), schema_name=sn)
    assert bag(client.scan(jid), "fid", "nm") == _joined(_DIMS), "after the late dim"


@NEEDS_MULTI
def test_an_upsert_replaces_the_row_on_every_copy(client, schema_name):
    """A raw push is an upsert: the second push of a key replaces the payload
    once per copy, so the copies stay identical and the read still sees one row.
    """
    client.execute_sql(
        "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, name BIGINT NOT NULL)" + _REPL,
        schema_name=schema_name)
    tid, schema = client.resolve_table(schema_name, "dim")
    for name in (100, 200):
        batch = gnitz.ZSetBatch(schema)
        batch.append(id=1, name=name)
        client.push(tid, batch)

    assert bag(client.scan(tid), "id", "name") == {(1, 200): 1}


# ── Joins: a replicated side is local on every worker ────────────────────────

@NEEDS_MULTI
def test_a_star_join_of_two_replicated_dims_stays_local(client, schema_name):
    """One partitioned fact joined to two replicated dims, as the nested pair
    gnitz builds (one JOIN per view). The inner view is partitioned by the fact's
    own distribution; joining that to a second replicated dim is local again, and
    neither hop may duplicate a row.
    """
    client.execute_sql(
        "CREATE TABLE d1 (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)" + _REPL,
        schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE d2 (id BIGINT NOT NULL PRIMARY KEY, b BIGINT NOT NULL)" + _REPL,
        schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE fact (fid BIGINT NOT NULL PRIMARY KEY, r1 BIGINT NOT NULL, "
        "r2 BIGINT NOT NULL)", schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW j1 AS SELECT fact.fid AS fid, fact.r2 AS r2, d1.a AS a "
        "FROM fact JOIN d1 ON fact.r1 = d1.id", schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW j AS SELECT j1.fid AS fid, j1.a AS a, d2.b AS b "
        "FROM j1 JOIN d2 ON j1.r2 = d2.id", schema_name=schema_name)

    client.execute_sql("INSERT INTO d1 VALUES (1, 11), (2, 22)", schema_name=schema_name)
    client.execute_sql("INSERT INTO d2 VALUES (1, 1000), (2, 2000)", schema_name=schema_name)
    client.execute_sql(
        "INSERT INTO fact VALUES " + ", ".join(
            f"({i}, {(i % 2) + 1}, {((i + 1) % 2) + 1})" for i in range(1, 33)),
        schema_name=schema_name)

    jid, _ = client.resolve_table(schema_name, "j")
    assert bag(client.scan(jid), "fid", "a", "b") == {
        (i, ((i % 2) + 1) * 11, (((i + 1) % 2) + 1) * 1000): 1 for i in range(1, 33)}


@NEEDS_MULTI
def test_a_join_of_two_replicated_tables_reads_one_copy(client, schema_name):
    """A view over two replicated sources is itself replicated — every worker
    computes the whole join — so its read must single-source like a table's."""
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)" + _REPL,
        schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)" + _REPL,
        schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW j AS SELECT a.id AS id, a.x AS x, b.y AS y "
        "FROM a JOIN b ON a.id = b.id", schema_name=schema_name)
    client.execute_sql("INSERT INTO a VALUES (1,10),(2,20),(3,30)", schema_name=schema_name)
    client.execute_sql("INSERT INTO b VALUES (1,11),(2,22),(3,33)", schema_name=schema_name)

    jid, _ = client.resolve_table(schema_name, "j")
    assert bag(client.scan(jid), "id", "x", "y") == {(1, 10, 11): 1, (2, 20, 22): 1, (3, 30, 33): 1}


# ── Exchange-shaped views whose sources are all replicated ───────────────────
#
# A view over only replicated sources is read from one worker, but the set-op,
# DISTINCT and range/band-join circuits scatter their output across every worker.
# At more than one worker that silently drops the rows whose hash missed the
# reading worker's partition and inflates the survivors (broadcast in × scatter
# out). Every case below is weight-exact for that reason.


def _mk_repl_ab(client, sn, a_extra="val BIGINT NOT NULL", b_extra="val BIGINT NOT NULL"):
    """Two replicated single-PK tables `a`/`b` for the set-op and join cases."""
    client.execute_sql(
        f"CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, {a_extra})" + _REPL, schema_name=sn)
    client.execute_sql(
        f"CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, {b_extra})" + _REPL, schema_name=sn)


@NEEDS_MULTI
def test_replicated_set_ops_keep_their_weights(client, schema_name):
    """Every set-op spelling over one pair of replicated tables.

    `a` and `b` overlap on two rows, so UNION ALL carries them at weight 2 where
    UNION DISTINCT clamps to 1. INTERSECT and EXCEPT coincide between their ALL
    and DISTINCT forms here — each row is unique per source — but compile to
    different clamp circuits, so both are exercised. The scatter bug drops rows
    and inflates weights, and both show up here.
    """
    _mk_repl_ab(client, schema_name)
    bodies = {
        "v_all": "SELECT * FROM a UNION ALL SELECT * FROM b",
        "v_dist": "SELECT * FROM a UNION SELECT * FROM b",
        "v_int": "SELECT * FROM a INTERSECT SELECT * FROM b",
        "v_int_all": "SELECT * FROM a INTERSECT ALL SELECT * FROM b",
        "v_exc": "SELECT * FROM a EXCEPT SELECT * FROM b",
        "v_exc_all": "SELECT * FROM a EXCEPT ALL SELECT * FROM b",
    }
    for name, body in bodies.items():
        client.execute_sql(f"CREATE VIEW {name} AS {body}", schema_name=schema_name)
    client.execute_sql("INSERT INTO a VALUES (1,10),(2,20),(3,30),(4,40)", schema_name=schema_name)
    client.execute_sql("INSERT INTO b VALUES (3,30),(4,40),(5,50),(6,60)", schema_name=schema_name)

    every = {(1, 10): 1, (2, 20): 1, (3, 30): 1, (4, 40): 1, (5, 50): 1, (6, 60): 1}
    want = {
        "v_all": {**every, (3, 30): 2, (4, 40): 2},
        "v_dist": every,
        "v_int": {(3, 30): 1, (4, 40): 1},
        "v_int_all": {(3, 30): 1, (4, 40): 1},
        "v_exc": {(1, 10): 1, (2, 20): 1},
        "v_exc_all": {(1, 10): 1, (2, 20): 1},
    }
    for name, expected in want.items():
        vid, _ = client.resolve_table(schema_name, name)
        assert bag(client.scan(vid), "pk", "val") == expected, name


@NEEDS_MULTI
def test_replicated_distinct_and_self_union(client, schema_name):
    """DISTINCT over one replicated source, and `s UNION ALL s` over another.

    DISTINCT keys its output by a content hash, which scatters it — under the bug
    the reading worker keeps only its hash slice and distinct values vanish. The
    self-union is the companion: identical rows must come out at weight 2
    (branch-id disambiguated), not 2×W, which pins that the local path does not
    itself inflate the broadcast delta.
    """
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)" + _REPL,
        schema_name=schema_name)
    client.execute_sql("CREATE VIEW v_dist AS SELECT DISTINCT val FROM t", schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW v_self AS SELECT * FROM t UNION ALL SELECT * FROM t",
        schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1,5),(2,5),(3,7),(4,7),(5,9)",
                       schema_name=schema_name)

    assert bag(scanned(client, schema_name, "v_dist"), "val") == {(5,): 1, (7,): 1, (9,): 1}
    assert bag(scanned(client, schema_name, "v_self"), "id", "val") == {
        (1, 5): 2, (2, 5): 2, (3, 7): 2, (4, 7): 2, (5, 9): 2}


@NEEDS_MULTI
def test_replicated_band_join_inner_and_left(client, schema_name):
    """Band join (`a.k = b.k AND a.lo <= b.t`, one equality) over two replicated
    tables, INNER and LEFT. Band output rides the mandatory output exchange, so
    the scatter bug drops pairs and inflates the rest. `a4` matches nothing, so
    LEFT null-fills it.
    """
    _mk_repl_ab(client, schema_name,
                a_extra="k BIGINT NOT NULL, lo BIGINT NOT NULL",
                b_extra="k BIGINT NOT NULL, t BIGINT NOT NULL")
    client.execute_sql(
        "CREATE VIEW vin AS SELECT a.pk AS aid, b.pk AS bid "
        "FROM a JOIN b ON a.k = b.k AND a.lo <= b.t", schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW vleft AS SELECT a.pk AS aid, b.pk AS bid "
        "FROM a LEFT JOIN b ON a.k = b.k AND a.lo <= b.t", schema_name=schema_name)
    client.execute_sql("INSERT INTO a VALUES (1,1,10),(2,1,50),(3,2,5),(4,3,1)",
                       schema_name=schema_name)
    client.execute_sql("INSERT INTO b VALUES (1,1,40),(2,1,60),(3,2,100)",
                       schema_name=schema_name)

    # a1(lo10): b1(t40), b2(t60); a2(lo50): b2(t60); a3(k2, lo5): b3(t100).
    matched = {(1, 1): 1, (1, 2): 1, (2, 2): 1, (3, 3): 1}
    assert bag(scanned(client, schema_name, "vin"), "aid", "bid") == matched, "inner"
    assert bag(scanned(client, schema_name, "vleft"), "aid", "bid") == {**matched, (4, None): 1}


@NEEDS_MULTI
def test_replicated_pure_range_join_inner_and_left(client, schema_name):
    """Pure-range join (`a.x < b.y`, no equality) over two replicated tables.

    A broadcast input is normally trimmed to the owning worker's slice by a
    worker filter; under an all-replicated local run that filter must be absent
    or it discards rows. The LEFT side carries a NULL range key, which exercises
    the separate NULL branch and its own filter.
    """
    client.execute_sql(
        "CREATE TABLE a (pk BIGINT NOT NULL PRIMARY KEY, x BIGINT)" + _REPL,
        schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE b (pk BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)" + _REPL,
        schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW vin AS SELECT a.pk AS aid, b.pk AS bid FROM a JOIN b ON a.x < b.y",
        schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW vleft AS SELECT a.pk AS aid, b.pk AS bid FROM a LEFT JOIN b ON a.x < b.y",
        schema_name=schema_name)
    client.execute_sql("INSERT INTO a VALUES (1,10),(2,30),(3,50),(4,NULL)",
                       schema_name=schema_name)
    client.execute_sql("INSERT INTO b VALUES (1,20),(2,40)", schema_name=schema_name)

    # a1(10) < 20 and < 40; a2(30) < 40; a3(50) exceeds every y; a4 is NULL.
    matched = {(1, 1): 1, (1, 2): 1, (2, 2): 1}
    assert bag(scanned(client, schema_name, "vin"), "aid", "bid") == matched, "inner"
    assert bag(scanned(client, schema_name, "vleft"), "aid", "bid") == {
        **matched, (3, None): 1, (4, None): 1}


@NEEDS_MULTI
def test_replicated_keyless_join_sides(client, schema_name):
    """The keyless (cross) product with a replicated side. Its delta relays
    single-sourced and the trace filter partitions it like a keyed side, so the
    product carries no W× duplication — for one replicated side, and for two,
    where the view itself is replicated and runs correct-local."""
    for name in ("r", "r2"):
        client.execute_sql(
            f"CREATE TABLE {name} (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)" + _REPL,
            schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE k (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=schema_name)
    for name, body in (("rk", "SELECT r.id AS a, k.id AS b FROM r CROSS JOIN k"),
                       ("kr", "SELECT k.id AS a, r.id AS b FROM k CROSS JOIN r"),
                       ("rr", "SELECT r.id AS a, r2.id AS b FROM r, r2")):
        client.execute_sql(f"CREATE VIEW {name} AS {body}", schema_name=schema_name)
    ids_r, ids_r2, ids_k = list(range(1, 6)), list(range(50, 53)), list(range(10, 17))
    for name, ids in (("r", ids_r), ("r2", ids_r2), ("k", ids_k)):
        client.execute_sql(
            f"INSERT INTO {name} VALUES " + ",".join(f"({i},0)" for i in ids),
            schema_name=schema_name)

    def check(ctx, ids_r):
        for name, left, right in (("rk", ids_r, ids_k), ("kr", ids_k, ids_r),
                                  ("rr", ids_r, ids_r2)):
            assert bag(scanned(client, schema_name, name), "a", "b") == {
                (x, y): 1 for x in left for y in right}, f"{name} {ctx}"

    check("initial", ids_r)
    client.execute_sql("DELETE FROM r WHERE id = 3", schema_name=schema_name)
    check("after delete", [i for i in ids_r if i != 3])


@NEEDS_MULTI
def test_replicated_subquery_predicates(client, schema_name):
    """Pure-range EXISTS/NOT EXISTS and an equi `IN`, all over replicated pairs.

    The range pair lowers to a MIN threshold and carries the same broadcast-trim
    filters as the pure-range LEFT join, with a NULL outer key exercising the
    NOT EXISTS null branch. The equi `IN` is already local through the join-shard
    co-partition skip; it pins that the all-replicated short-circuit keeps the
    semi-join emitting each outer row once, at weight 1.
    """
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT, k BIGINT NOT NULL, "
        "v BIGINT NOT NULL)" + _REPL, schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL, "
        "k BIGINT NOT NULL)" + _REPL, schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW semi AS SELECT v FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.y < a.x)",
        schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW anti AS SELECT v FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.y < a.x)",
        schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW inlist AS SELECT v FROM a WHERE k IN (SELECT k FROM b)",
        schema_name=schema_name)

    # MIN(b.y) = 15, and b holds k in {1, 3}.
    client.execute_sql(
        "INSERT INTO a VALUES (1,10,1,100),(2,20,2,200),(3,5,3,300),(4,NULL,1,400)",
        schema_name=schema_name)
    client.execute_sql("INSERT INTO b VALUES (1,15,1),(2,15,3)", schema_name=schema_name)

    assert bag(scanned(client, schema_name, "semi"), "v") == {(200,): 1}, "exists"
    assert bag(scanned(client, schema_name, "anti"), "v") == {(100,): 1, (300,): 1, (400,): 1}
    assert bag(scanned(client, schema_name, "inlist"), "v") == {(100,): 1, (300,): 1, (400,): 1}


# ── Mixed views: one replicated source and one partitioned source ────────────
#
# A mixed view is neither read single-sourced nor run under the all-replicated
# local intercept: its circuit really does exchange. The replicated side's delta
# is identical on every worker, so relaying all W payloads routes each of its
# rows to the hash owner W times and consolidation sums the byte-identical
# entries into one row at weight W. The relay must take exactly one worker's
# payload for a replicated source — the same single-sourcing the read applies.
#
# Under the bug the row set is right and only the weights are wrong, so every
# case here is weight-exact.


def _mk_mixed(client, sn, fact_extra="val BIGINT NOT NULL", dim_extra="val BIGINT NOT NULL"):
    """One partitioned `fact` and one replicated `dim`."""
    client.execute_sql(
        f"CREATE TABLE fact (pk BIGINT NOT NULL PRIMARY KEY, {fact_extra})", schema_name=sn)
    client.execute_sql(
        f"CREATE TABLE dim (pk BIGINT NOT NULL PRIMARY KEY, {dim_extra})" + _REPL,
        schema_name=sn)


@NEEDS_MULTI
def test_mixed_union_all_weights_reach_a_downstream_aggregate(client, schema_name):
    """`fact UNION ALL dim`, and a GROUP BY over it.

    The union's own rows must all weigh 1 — the dim rows come out at W without
    the relay collapse. The aggregate is what makes that escape into a
    user-visible column: COUNT(*) would report W for a group holding one row.
    """
    _mk_mixed(client, schema_name)
    client.execute_sql(
        "CREATE VIEW u AS SELECT * FROM fact UNION ALL SELECT * FROM dim",
        schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW g AS SELECT val, COUNT(*) AS cnt FROM u GROUP BY val",
        schema_name=schema_name)
    client.execute_sql("INSERT INTO fact VALUES (1,10),(2,20)", schema_name=schema_name)
    client.execute_sql("INSERT INTO dim VALUES (3,30),(4,40)", schema_name=schema_name)

    assert bag(scanned(client, schema_name, "u"), "pk", "val") == {
        (1, 10): 1, (2, 20): 1, (3, 30): 1, (4, 40): 1}
    assert bag(scanned(client, schema_name, "g"), "val", "cnt") == {
        (10, 1): 1, (20, 1): 1, (30, 1): 1, (40, 1): 1}


@NEEDS_MULTI
def test_mixed_except_all_with_the_replicated_side_on_the_left(client, schema_name):
    """`dim EXCEPT ALL fact` with the replicated side on the left.

    The shared row must cancel to nothing. At weight W on the left it would
    survive at W−1; the mirror orientation cancels either way, since
    `positive_part(1 − W) = 0`, so this is the orientation that pins the left
    side's weight.
    """
    _mk_mixed(client, schema_name)
    client.execute_sql(
        "CREATE VIEW v AS SELECT * FROM dim EXCEPT ALL SELECT * FROM fact",
        schema_name=schema_name)
    client.execute_sql("INSERT INTO dim VALUES (1,10),(2,20),(3,30)", schema_name=schema_name)
    client.execute_sql("INSERT INTO fact VALUES (3,30),(4,40)", schema_name=schema_name)

    vid, _ = client.resolve_table(schema_name, "v")
    assert bag(client.scan(vid), "pk", "val") == {(1, 10): 1, (2, 20): 1}


@NEEDS_MULTI
def test_mixed_band_join_weights(client, schema_name):
    """Mixed band join (`f.k = d.k AND f.lo <= d.t`, one equality).

    The join relays its raw INPUT delta and scatters it by the equality prefix,
    so a replicated input relayed W times integrates into the trace W times and
    EVERY output row — not only the replicated branch's — comes out at weight W.
    """
    _mk_mixed(client, schema_name,
              fact_extra="k BIGINT NOT NULL, lo BIGINT NOT NULL",
              dim_extra="k BIGINT NOT NULL, t BIGINT NOT NULL")
    client.execute_sql(
        "CREATE VIEW v AS SELECT fact.pk AS fid, dim.pk AS did "
        "FROM fact JOIN dim ON fact.k = dim.k AND fact.lo <= dim.t", schema_name=schema_name)
    client.execute_sql("INSERT INTO fact VALUES (1,1,10),(2,1,50),(3,2,5),(4,3,1)",
                       schema_name=schema_name)
    client.execute_sql("INSERT INTO dim VALUES (1,1,40),(2,1,60),(3,2,100)",
                       schema_name=schema_name)

    # f1(lo10): d1(t40), d2(t60); f2(lo50): d2(t60); f3(k2, lo5): d3(t100); f4: none.
    vid, _ = client.resolve_table(schema_name, "v")
    assert bag(client.scan(vid), "fid", "did") == {(1, 1): 1, (1, 2): 1, (2, 2): 1, (3, 3): 1}


@NEEDS_MULTI
def test_mixed_pure_range_left_join_weights(client, schema_name):
    """Mixed pure-range LEFT join (`f.x < d.y`, no equality) — the input relay's
    other destination arm, a broadcast rather than an equality-prefix scatter,
    plus the null-fill pipeline. Matched rows weigh 1; the null-filled rows ride
    the replicated threshold reduce and weigh 1 either way, so they pin that the
    collapse disturbs nothing.
    """
    _mk_mixed(client, schema_name, fact_extra="x BIGINT", dim_extra="y BIGINT NOT NULL")
    client.execute_sql(
        "CREATE VIEW v AS SELECT fact.pk AS fid, dim.pk AS did "
        "FROM fact LEFT JOIN dim ON fact.x < dim.y", schema_name=schema_name)
    client.execute_sql("INSERT INTO fact VALUES (1,10),(2,30),(3,50),(4,NULL)",
                       schema_name=schema_name)
    client.execute_sql("INSERT INTO dim VALUES (1,20),(2,40)", schema_name=schema_name)

    vid, _ = client.resolve_table(schema_name, "v")
    assert bag(client.scan(vid), "fid", "did") == {
        (1, 1): 1, (1, 2): 1, (2, 2): 1, (3, None): 1, (4, None): 1}


@NEEDS_MULTI
def test_mixed_union_all_over_a_replicated_view_keeps_multiplicity(client, schema_name):
    """The replicated property is transitive, so a view over only replicated
    sources is itself replicated and its relay must collapse too. The overlapping
    row carries a genuine weight of 2 — the one case here above 1 — which pins
    that the collapse drops duplicate payloads rather than clamping weights.
    """
    _mk_mixed(client, schema_name)
    client.execute_sql(
        "CREATE TABLE dim2 (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)" + _REPL,
        schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW rv AS SELECT * FROM dim UNION ALL SELECT * FROM dim2",
        schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW v AS SELECT * FROM fact UNION ALL SELECT * FROM rv",
        schema_name=schema_name)
    client.execute_sql("INSERT INTO fact VALUES (5,50)", schema_name=schema_name)
    client.execute_sql("INSERT INTO dim VALUES (1,10),(2,20)", schema_name=schema_name)
    client.execute_sql("INSERT INTO dim2 VALUES (2,20),(3,30)", schema_name=schema_name)

    vid, _ = client.resolve_table(schema_name, "v")
    assert bag(client.scan(vid), "pk", "val") == {(5, 50): 1, (1, 10): 1, (2, 20): 2, (3, 30): 1}


@NEEDS_MULTI
def test_a_bounded_read_of_a_mixed_view_broadcasts(client, schema_name):
    """A view with some — not all — replicated sources is built single-partition,
    so its rows are not keyed by the PK hash and a bounded read of it must
    broadcast. Confining it to one worker would answer from that worker alone and
    come up short.
    """
    client.execute_sql(
        "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, name BIGINT NOT NULL)" + _REPL,
        schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE fact (id BIGINT NOT NULL PRIMARY KEY, dim_id BIGINT NOT NULL, "
        "amount BIGINT NOT NULL)", schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW j AS SELECT f.id AS fid, d.name AS name, f.amount AS amount "
        "FROM fact f JOIN dim d ON f.dim_id = d.id", schema_name=schema_name)
    client.execute_sql("INSERT INTO dim VALUES (1,100),(2,200),(3,300)", schema_name=schema_name)
    client.execute_sql(
        "INSERT INTO fact VALUES " + ",".join(
            f"({i}, {i % 3 + 1}, {i * 10})" for i in range(60)), schema_name=schema_name)

    res = client.execute_sql("SELECT fid, amount FROM j WHERE fid < 20",
                             schema_name=schema_name)[0]["rows"]
    assert sorted((r.fid, r.amount) for r in res) == [(i, i * 10) for i in range(20)]
    res = client.execute_sql("SELECT amount FROM j WHERE fid = 47",
                             schema_name=schema_name)[0]["rows"]
    assert [r.amount for r in res] == [470]


@NEEDS_MULTI
def test_a_replicated_table_has_no_size_cap(client, schema_name):
    """A replicated table may grow far past dimension scale.

    Replication is meant for small dimensions, but no row or byte cap is
    enforced: the cost of a large replicated copy — W× ingest, W× storage, scans
    served by a single worker — is the user's to bound, not the engine's to
    refuse. That is a decision, not an oversight, so it is pinned here. If a cap
    is ever introduced this fails, and the trade gets made deliberately.
    """
    client.execute_sql(
        "CREATE TABLE dim (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)" + _REPL,
        schema_name=schema_name)
    tid, schema = client.resolve_table(schema_name, "dim")
    n = 20_000
    batch = gnitz.ZSetBatch(schema)
    for i in range(n):
        batch.append(id=i, v=i * 7)
    client.push(tid, batch)

    res = client.execute_sql(f"SELECT v FROM dim WHERE id = {n - 1}",
                             schema_name=schema_name)[0]["rows"]
    assert [r.v for r in res] == [(n - 1) * 7]
    assert len(client.scan(tid)) == n, "every replicated row must be readable"
