"""The parameterized bounded read (`ReadSpec`) — the ad-hoc SELECT path with no
view behind it: server-side predicate and projection, the bound pushed into the
walk, and the keyed gathers that route to the partitions their keys reach.

A read that names its keys (a PK point or prefix-confined range, a `pk IN (…)`
set, an FK dereference) opens only those partitions rather than merging every
one. The failure mode is silent: a key routed to no partition comes back as "no
such row", and one routed to two comes back at weight 2. So every case here
asserts the full result set *with its weights* over keys that span every worker
— a PK list would see the first failure only by luck and the second not at all.

What this path refuses is in `admissibility/test_sql_rejections.py`.
"""

import gnitz
import pytest
from _read import bag, rows

# Enough distinct keys that the set spans every worker at any worker count.
NROWS = 400


def _insert(client, sn, table, values, chunk=200):
    for i in range(0, len(values), chunk):
        vals = ",".join("(" + ",".join(str(v) for v in r) + ")" for r in values[i:i + chunk])
        client.execute_sql(f"INSERT INTO {table} VALUES {vals}", schema_name=sn)


@pytest.fixture
def kv(client, schema_name):
    """`t (id PK, v)` with `v = id * 10` over `NROWS` rows spanning every
    partition. Read-only for every case that takes it directly."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=sn)
    _insert(client, sn, "t", [(i, i * 10) for i in range(NROWS)])
    return sn


# ---------------------------------------------------------------------------
# Server-side predicate and projection
# ---------------------------------------------------------------------------


def test_the_projection_is_the_clients_to_shape(client, schema_name):
    """The read path hidden-prepends the physical PK and keeps the SELECT list
    as written — so a column may follow the PK, repeat under two names, or be
    computed, and the presented shape is the SELECT list alone."""
    sn = schema_name
    client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, a BIGINT)", schema_name=sn)
    _insert(client, sn, "t", [(5, 100), (6, 200)])
    assert bag(rows(client, sn, "SELECT a, id FROM t WHERE id = 5")) == {(100, 5): 1}
    assert bag(rows(client, sn, "SELECT id AS x, id AS y FROM t WHERE id = 5")) == {(5, 5): 1}
    assert bag(rows(client, sn, "SELECT a + 1 AS ap1 FROM t WHERE id = 5")) == {(101,): 1}


def test_a_string_predicate_runs_server_side(client, schema_name):
    """`WHERE name = 'x'` — a string comparison — is evaluated server-side, and
    a non-indexed column degrades the bound to a full cursor rather than being
    refused."""
    sn = schema_name
    client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT)", schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, 'alice')", schema_name=sn)
    assert bag(rows(client, sn, "SELECT id FROM t WHERE name = 'alice'")) == {(1,): 1, (3,): 1}


# ---------------------------------------------------------------------------
# PK bounds and keyed gathers
# ---------------------------------------------------------------------------


def test_a_pk_point_finds_every_key(client, kv):
    """`WHERE id = k` compiles to a one-key PK range, which routes to a single
    partition. Every key must still come back exactly once, from whichever
    worker owns it."""
    for i in range(NROWS):
        assert bag(rows(client, kv, f"SELECT id, v FROM t WHERE id = {i}")) == {(i, i * 10): 1}
    assert rows(client, kv, f"SELECT id FROM t WHERE id = {NROWS + 7}") == []


@pytest.mark.parametrize("predicate,keep", [
    ("id >= 100", lambda i: i >= 100),
    ("id > 5 AND id < 9", lambda i: 5 < i < 9),
    ("id > 10 AND id < 5", lambda i: False),          # provably empty
])
def test_a_pk_range_spans_every_partition(client, kv, predicate, keep):
    """A multi-key PK range on a full-PK-hashed table is not confinable, so it
    keeps the merged cursor over every partition and must lose no row."""
    assert bag(rows(client, kv, f"SELECT id FROM t WHERE {predicate}")) == \
        {(i,): 1 for i in range(NROWS) if keep(i)}


def test_a_signed_pk_range_orders_across_zero(client, schema_name):
    """The PK's OPK encoding sign-flips, so a range spanning zero is contiguous
    rather than inverted."""
    sn = schema_name
    client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)", schema_name=sn)
    client.execute_sql("INSERT INTO t VALUES (-5, 1), (-1, 2), (0, 3), (3, 4)", schema_name=sn)
    assert bag(rows(client, sn, "SELECT id FROM t WHERE id > -2")) == \
        {(-1,): 1, (0,): 1, (3,): 1}


def test_a_pk_set_gathers_every_named_key(client, kv):
    """`id IN (…)` is the PkSet gather: broadcast, each worker answering only the
    keys it owns. The union must be the whole list at weight 1 — a key two
    workers both claim shows up here as weight 2 — and absent keys miss silently
    rather than erroring."""
    wanted = list(range(0, NROWS, 3))
    in_list = ",".join(str(i) for i in wanted + [NROWS + 99])
    assert bag(rows(client, kv, f"SELECT id, v FROM t WHERE id IN ({in_list})")) == \
        {(i, i * 10): 1 for i in wanted}


def test_the_dml_committed_row_fetch_reads_through_the_same_gather(client, kv):
    """An UPDATE bounded by a PK set reads its committed rows back through the
    PkSet gather, so a key the gather drops is a row that silently keeps its old
    value."""
    wanted = set(range(0, NROWS, 7))
    in_list = ",".join(str(i) for i in sorted(wanted))
    client.execute_sql(f"UPDATE t SET v = v + 1 WHERE id IN ({in_list})", schema_name=kv)
    assert bag(rows(client, kv, "SELECT id, v FROM t")) == \
        {(i, i * 10 + (1 if i in wanted else 0)): 1 for i in range(NROWS)}


def test_an_fk_dereference_routes_to_every_parent(client, schema_name):
    """FK validation dereferences each parent key through the routed gather, so
    a bulk INSERT whose parents span every partition is accepted whole — and a
    parent key no partition holds is still rejected."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=sn)
    _insert(client, sn, "parent", [(i, i) for i in range(NROWS)])
    client.execute_sql(
        "CREATE TABLE child (id BIGINT NOT NULL PRIMARY KEY,"
        " pid BIGINT NOT NULL REFERENCES parent(id))", schema_name=sn)
    _insert(client, sn, "child", [(i, i) for i in range(NROWS)])
    assert bag(rows(client, sn, "SELECT id, pid FROM child")) == \
        {(i, i): 1 for i in range(NROWS)}

    with pytest.raises(gnitz.GnitzError):
        client.execute_sql(
            f"INSERT INTO child VALUES ({NROWS}, {NROWS + 99})", schema_name=sn)


# ---------------------------------------------------------------------------
# Single-partition confinement
#
# A PK range that provably lands on one worker is unicast rather than broadcast.
# Every case here fails as a SHORT result — rows answered by the wrong worker
# are simply absent — never as a wrong value.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("lead", ["a = {a}", "a IN ({a})"])
def test_a_full_point_on_a_compound_pk_confines(client, schema_name, lead):
    """The range is one key wide, so it unicasts to the worker owning that key's
    partition. `a IN (x)` is `a = x`, so both spellings must confine alike."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT, PRIMARY KEY (a, b))",
        schema_name=sn)
    _insert(client, sn, "t", [(a, b, a * 100 + b) for a in range(6) for b in range(6)])
    for a, b in [(0, 0), (3, 4), (5, 5)]:
        got = bag(rows(client, sn, f"SELECT v FROM t WHERE {lead.format(a=a)} AND b = {b}"))
        assert got == {(a * 100 + b,): 1}, f"({a}, {b})"


def test_a_cluster_by_prefix_range_returns_the_whole_group(client, schema_name):
    """With `CLUSTER BY a` every row sharing `a` lands in one partition, so a
    bound pinning `a` and ranging `b` is confined — and must still return the
    whole group, not the one key the range starts at."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT, "
        "PRIMARY KEY (a, b)) CLUSTER BY a", schema_name=sn)
    _insert(client, sn, "t", [(a, b, a * 100 + b) for a in range(4) for b in range(10)])
    assert bag(rows(client, sn, "SELECT b FROM t WHERE a = 2 AND b BETWEEN 3 AND 7")) == \
        {(b,): 1 for b in range(3, 8)}
    assert bag(rows(client, sn, "SELECT b FROM t WHERE a = 2 AND b >= 0")) == \
        {(b,): 1 for b in range(10)}


def test_a_point_on_a_partitioned_views_key_confines(client, schema_name):
    """A view over a non-replicated source has a hash-partitioned output store,
    so a point on its key is confinable — and must find its row."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT, v BIGINT)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW agg AS SELECT g, SUM(v) AS total FROM t GROUP BY g", schema_name=sn)
    _insert(client, sn, "t", [(i, i % 5, i) for i in range(40)])
    for g in range(5):
        assert bag(rows(client, sn, f"SELECT total FROM agg WHERE g = {g}")) == \
            {(sum(i for i in range(40) if i % 5 == g),): 1}


def test_a_provably_empty_range_still_grounds_a_fold(client, kv):
    """A provably-empty PK range skips the fan-out for a rows sink — but a fold
    still owes its ground row, so COUNT(*) must answer 0, not nothing."""
    assert bag(rows(client, kv, "SELECT COUNT(*) AS c FROM t WHERE id > 10 AND id < 5")) == \
        {(0,): 1}


# ---------------------------------------------------------------------------
# An indexed predicate through the read path
# ---------------------------------------------------------------------------


def test_an_indexed_predicate_gathers_from_every_owner(client, schema_name):
    """An index range is broadcast and each worker gathers its own source rows,
    so a per-PK probe must resolve on exactly the worker owning it. Both a point
    and a band are checked over keys that scatter across every partition."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=sn)
    _insert(client, sn, "t", [(i, i * 10) for i in range(NROWS)])
    client.execute_sql("CREATE INDEX ON t (v)", schema_name=sn)
    for i in range(0, NROWS, 5):
        assert bag(rows(client, sn, f"SELECT id, v FROM t WHERE v = {i * 10}")) == \
            {(i, i * 10): 1}
    for lo, hi in [(0, 90), (1000, 1990), (500, 2500)]:
        assert bag(rows(client, sn, f"SELECT id, v FROM t WHERE v BETWEEN {lo} AND {hi}")) == \
            {(i, i * 10): 1 for i in range(NROWS) if lo <= i * 10 <= hi}


def test_a_nonselective_bound_degrades_but_keeps_the_conjunct(client, schema_name):
    """An indexed `WHERE flag = 1` whose bound the selectivity gate may degrade
    to a full cursor still returns only matching rows — the conjunct stays in
    the server predicate rather than being consumed by a bound that was dropped."""
    sn = schema_name
    client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, flag BIGINT)", schema_name=sn)
    client.execute_sql("CREATE INDEX ix ON t (flag)", schema_name=sn)
    _insert(client, sn, "t", [(i, i % 2) for i in range(40)])
    assert bag(rows(client, sn, "SELECT id FROM t WHERE flag = 1")) == \
        {(i,): 1 for i in range(40) if i % 2 == 1}


def test_an_all_empty_broadcast_keeps_the_column_metadata(client, schema_name):
    """An indexed read matching on no worker: every worker's reply frame carries
    neither rows nor a schema block, so the master forwards none of them and the
    client sees only the terminal frame. The result must still present the column
    metadata — which the client authored and shipped with the request, since a
    read-spec reply never carries a schema block back.

    The unprojected source PK rides along as a hidden leading column and is not
    part of the presented shape."""
    sn = schema_name
    client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, flag BIGINT)", schema_name=sn)
    client.execute_sql("CREATE INDEX ix ON t (flag)", schema_name=sn)
    _insert(client, sn, "t", [(i, i % 4) for i in range(40)])

    assert bag(rows(client, sn, "SELECT id, flag FROM t WHERE flag = 2")) == \
        {(i, 2): 1 for i in range(40) if i % 4 == 2}

    res = client.execute_sql("SELECT id, flag FROM t WHERE flag = 999", schema_name=sn)[0]
    assert res["type"] == "Rows", res
    miss = res["rows"]
    assert len(miss) == 0
    assert [(c.name, c.type_code) for c in miss.schema.columns if not c.is_hidden] == \
        [("id", gnitz.TypeCode.I64), ("flag", gnitz.TypeCode.I64)]


def test_a_predicate_matching_nothing_is_a_well_formed_empty_result(client, kv):
    assert rows(client, kv, "SELECT v, id FROM t WHERE id = 999999") == []


# ---------------------------------------------------------------------------
# Bodies the direct path serves without a circuit
# ---------------------------------------------------------------------------


def test_a_view_read_reflects_the_preceding_push(client, schema_name):
    """A read of a view drains pending ticks first, so it reflects an
    immediately-preceding insert."""
    sn = schema_name
    client.execute_sql("CREATE TABLE t (id BIGINT PRIMARY KEY, v BIGINT)", schema_name=sn)
    client.execute_sql("CREATE VIEW pos AS SELECT id, v FROM t WHERE v > 0", schema_name=sn)
    client.execute_sql("INSERT INTO t VALUES (1, 10), (2, -5), (3, 20)", schema_name=sn)
    assert bag(rows(client, sn, "SELECT id FROM pos")) == {(1,): 1, (3,): 1}


def test_a_cte_expands_into_the_direct_path(client, schema_name):
    """A CTE over one relation is a macro expanded into the body, so every
    single-relation shape reads via the direct path — an identity, a narrowing
    or computed projection, a WHERE in the CTE conjoined with the outer one, a
    chain, and a fold over it — and reads exactly what the flat query reads,
    weights included."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        schema_name=sn)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)", schema_name=sn)
    client.execute_sql(
        "CREATE VIEW v_hi AS SELECT pk, val FROM t WHERE val >= 30", schema_name=sn)

    for cte, flat in [
        ("WITH x AS (SELECT * FROM t) SELECT pk FROM x WHERE val > 30",
         "SELECT pk FROM t WHERE val > 30"),
        ("WITH y AS (SELECT * FROM v_hi) SELECT pk FROM y WHERE val = 50",
         "SELECT pk FROM v_hi WHERE val = 50"),
        ("WITH x AS (SELECT pk, val AS q FROM t) SELECT pk, q FROM x ORDER BY q DESC",
         "SELECT pk, val AS q FROM t ORDER BY val DESC"),
        ("WITH x AS (SELECT pk, val * 2 AS d FROM t WHERE val > 10) SELECT d FROM x WHERE d < 100 ORDER BY d",
         "SELECT val * 2 AS d FROM t WHERE val > 10 AND val * 2 < 100 ORDER BY d"),
        ("WITH x(i, j) AS (SELECT pk, val FROM t), y AS (SELECT j FROM x WHERE i > 3) SELECT SUM(j) AS s FROM y",
         "SELECT SUM(val) AS s FROM t WHERE pk > 3"),
        ("WITH x AS (SELECT val + 1 AS q FROM t) SELECT q, COUNT(*) AS n FROM x GROUP BY q ORDER BY q",
         "SELECT val + 1 AS q, COUNT(*) AS n FROM t GROUP BY val + 1 ORDER BY q"),
        ("WITH x AS (SELECT * EXCEPT (val) FROM t) SELECT * FROM x ORDER BY pk",
         "SELECT pk FROM t ORDER BY pk"),
    ]:
        got = [(tuple(r), r.weight) for r in rows(client, sn, cte)]
        want = [(tuple(r), r.weight) for r in rows(client, sn, flat)]
        assert got == want and got, f"{cte!r}: {got} != {want}"

    # A CTE exposes only what it projects, whatever the source holds.
    with pytest.raises(gnitz.GnitzError, match="column 'val' not found"):
        client.execute_sql("WITH x AS (SELECT pk FROM t) SELECT val FROM x", schema_name=sn)


def test_a_from_less_select_answers_one_constant_row(client, schema_name):
    """The probe a driver or health check sends: it reads nothing and answers
    one row under the computed-column names, with LIMIT honored."""
    sn = schema_name
    assert bag(rows(client, sn, "SELECT 1")) == {(1,): 1}
    assert bag(rows(client, sn, "SELECT 1 + 2 AS three, 'x' AS s, 2.5 AS f")) == \
        {(3, "x", 2.5): 1}
    assert rows(client, sn, "SELECT 1 AS a LIMIT 0") == []
    with pytest.raises(gnitz.GnitzError, match="column 'x' not found"):
        client.execute_sql("SELECT x", schema_name=sn)
