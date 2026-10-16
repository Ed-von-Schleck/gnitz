"""The parameterized bounded read (`ReadSpec`) — the ad-hoc SELECT path with no
view behind it: server-side predicate and projection, the bound pushed into the
walk, and the keyed gathers that route to the partitions their keys reach.

A read that names its keys (a PK point or prefix-confined range, a `pk IN (…)`
set) opens only those partitions rather than merging every one. The failure mode
is silent: a key routed to no partition comes back as "no such row", and one
routed to two comes back at weight 2. So every case here asserts the full result
set *with its weights* over keys that span every worker — a PK list would see
the first failure only by luck and the second not at all.

What this path refuses is pinned in the planner's `gnitz-sql/src/suites/plan_read.rs`.
"""

import os
import signal

import gnitz
import pytest
from _read import access, bag, reply, rows
from _serverproc import TINY_REPLY_FRAMES, join_or_fail, spawn
from _sql import insert

# Enough rows that an indexed point clears the worker's selectivity gate at any
# worker count, and far more distinct keys than workers.
NROWS = 400
# Consecutive keys from 0 reach every worker well before this many, at any legal
# worker count.
SPAN = 96


@pytest.fixture(scope="module")
def kv(module_client):
    """A connection holding `t (id PK, v)` with `v = id * 10` over `NROWS` rows
    and an index on `v`. Read-only: every case that takes it is a SELECT."""
    conn = module_client
    conn.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
    insert(conn, "t", [(i, i * 10) for i in range(NROWS)])
    conn.execute_sql("CREATE INDEX ON t (v)")
    return conn


# ---------------------------------------------------------------------------
# Server-side predicate and projection
# ---------------------------------------------------------------------------


def test_the_projection_is_the_clients_to_shape(kv):
    """The reply keeps the SELECT list as written, carrying the physical PK at
    its SELECT slot or hidden in front when unselected — so a column may precede
    the PK, repeat under two names, or be computed, and the presented shape is
    the SELECT list alone."""
    assert bag(rows(kv, "SELECT v, id FROM t WHERE id = 5")) == {(50, 5): 1}
    assert bag(rows(kv, "SELECT id AS x, id AS y FROM t WHERE id = 5")) == {(5, 5): 1}
    assert bag(rows(kv, "SELECT v + 1 AS vp1 FROM t WHERE id = 5")) == {(51,): 1}


# ---------------------------------------------------------------------------
# PK bounds and keyed gathers
# ---------------------------------------------------------------------------


def test_a_pk_point_finds_every_key(kv):
    """`WHERE id = k` is a PK point lookup, sent to the one worker owning the key.
    Every key must still come back exactly once, from whichever worker owns
    it."""
    for i in range(SPAN):
        assert bag(rows(kv, f"SELECT id, v FROM t WHERE id = {i}")) == {(i, i * 10): 1}
    assert rows(kv, f"SELECT id FROM t WHERE id = {NROWS + 7}") == []


@pytest.mark.parametrize("predicate,keep", [
    ("id >= 100", lambda i: i >= 100),
    ("id > 5 AND id < 9", lambda i: 5 < i < 9),
])
def test_a_pk_range_spans_every_partition(kv, predicate, keep):
    """A multi-key PK range on a full-PK-hashed table is not confinable, so it
    keeps the merged cursor over every partition and must lose no row."""
    assert bag(rows(kv, f"SELECT id FROM t WHERE {predicate}")) == \
        {(i,): 1 for i in range(NROWS) if keep(i)}


def test_a_pk_set_gathers_every_named_key(kv):
    """`id IN (…)` is the PkSet gather: each key is sent only to the worker owning
    it. The union must be the whole list at weight 1 — a key two
    workers both claim shows up here as weight 2, as does a key the list names
    twice — and absent keys miss silently rather than erroring."""
    wanted = list(range(0, NROWS, 3))
    in_list = ",".join(str(i) for i in wanted + wanted[:3] + [NROWS + 99])
    assert bag(rows(kv, f"SELECT id, v FROM t WHERE id IN ({in_list})")) == \
        {(i, i * 10): 1 for i in wanted}


def test_a_provably_empty_pk_range_is_answered_by_one_worker(own_server):
    """With every worker but worker 0 stopped, only a read no other worker is asked
    for can complete. A provably-empty range skips the fan-out for a rows sink —
    but a fold still owes its ground row, so COUNT(*) must answer 0, not nothing."""
    def read(conn):
        for q, want in [("SELECT id FROM t WHERE id = -1", {}),
                        ("SELECT id FROM t WHERE id > 10 AND id < 5", {}),
                        ("SELECT COUNT(*) AS c FROM t WHERE id > 10 AND id < 5", {(0,): 1}),
                        ("SELECT COUNT(*) AS c FROM t WHERE id < 0", {(0,): 1})]:
            assert bag(rows(conn, q)) == want, q

    with gnitz.connect(own_server.start(workers=4).target) as conn:
        conn.execute_sql("CREATE TABLE t (id BIGINT UNSIGNED NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
        insert(conn, "t", [(i, i) for i in range(SPAN)])
        pids = own_server.worker_pids()
        assert sorted(pids) == [0, 1, 2, 3], pids
        stopped = [pid for w, pid in pids.items() if w != 0]
        for pid in stopped:
            os.kill(pid, signal.SIGSTOP)
        try:
            join_or_fail("a provably-empty read waited on a stopped worker", spawn(read, conn))
        finally:
            for pid in stopped:
                os.kill(pid, signal.SIGCONT)


# ---------------------------------------------------------------------------
# Single-partition confinement
#
# A PK range that provably lands on one worker is unicast rather than broadcast.
# Every case here fails as a SHORT result — rows answered by the wrong worker
# are simply absent — never as a wrong value.
# ---------------------------------------------------------------------------


def test_a_full_point_on_a_compound_pk_confines(client):
    """The range is one key wide, so it unicasts to the worker owning that key's
    partition."""
    client.execute_sql(
        "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT, PRIMARY KEY (a, b))")
    insert(client, "t", [(a, b, a * 100 + b) for a in range(6) for b in range(6)])
    for a, b in [(0, 0), (3, 4), (5, 5)]:
        got = bag(rows(client, f"SELECT v FROM t WHERE a = {a} AND b = {b}"))
        assert got == {(a * 100 + b,): 1}, f"({a}, {b})"


def test_a_cluster_by_prefix_range_returns_the_whole_group(client):
    """With `CLUSTER BY a` every row sharing `a` lands in one partition, so a
    bound pinning `a` and ranging `b` is confined — and must still return the
    whole group, not the one key the range starts at."""
    client.execute_sql(
        "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT, "
        "PRIMARY KEY (a, b)) CLUSTER BY a")
    insert(client, "t", [(a, b, a * 100 + b) for a in range(4) for b in range(10)])
    assert bag(rows(client, "SELECT b FROM t WHERE a = 2 AND b BETWEEN 3 AND 7")) == \
        {(b,): 1 for b in range(3, 8)}
    assert bag(rows(client, "SELECT b FROM t WHERE a = 2 AND b >= 0")) == \
        {(b,): 1 for b in range(10)}


def test_a_point_on_a_partitioned_views_key_confines(client):
    """A view over a non-replicated source has a hash-partitioned output store,
    so a point on its key is confinable — and must find its row. `g` is NOT NULL,
    so the view is keyed on it rather than on a hidden key a WHERE cannot bound."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, g BIGINT NOT NULL, v BIGINT); "
        "CREATE VIEW agg AS SELECT g, SUM(v) AS total FROM t GROUP BY g")
    insert(client, "t", [(i, i % 5, i) for i in range(40)])
    for g in range(5):
        q = f"SELECT total FROM agg WHERE g = {g}"
        assert access(client, q) == "pk point lookup"
        assert bag(rows(client, q)) == {(sum(i for i in range(40) if i % 5 == g),): 1}


# ---------------------------------------------------------------------------
# An indexed predicate through the read path
# ---------------------------------------------------------------------------


def test_an_indexed_predicate_gathers_from_every_owner(kv):
    """An index range is broadcast and each worker gathers its own source rows,
    so a per-PK probe must resolve on exactly the worker owning it. Both a point
    — through SQL and through the binary `seek_by_index` — and a narrow band are
    checked over keys that scatter across every partition."""
    tid, schema = kv.resolve_table("t")
    for i in range(SPAN):
        want = {(i, i * 10): 1}
        assert bag(rows(kv, f"SELECT id, v FROM t WHERE v = {i * 10}")) == want
        assert bag(kv.seek_by_index(tid, schema, [1], [i * 10])) == want
    # An absent value is an empty answer, not an error.
    assert bag(kv.seek_by_index(tid, schema, [1], [7])) == {}
    for lo, hi in [(0, 90), (1000, 1090), (3900, 3990)]:
        assert bag(rows(kv, f"SELECT id, v FROM t WHERE v BETWEEN {lo} AND {hi}")) == \
            {(i, i * 10): 1 for i in range(NROWS) if lo <= i * 10 <= hi}


def test_a_nonselective_index_walk_returns_only_its_range(kv):
    """An indexed range covering nearly the whole table is served by a full
    scan, and still returns only the rows inside the range."""
    assert bag(rows(kv, "SELECT id FROM t WHERE v >= 10")) == \
        {(i,): 1 for i in range(1, NROWS)}


def test_an_empty_indexed_read_keeps_the_column_metadata(kv):
    """An indexed read matching on no worker still presents the column metadata
    in SELECT order, and no hidden column is part of the presented shape."""
    miss = reply(kv.execute_sql("SELECT id, v FROM t WHERE v = 7"))
    assert len(miss) == 0
    assert miss._fields == ("id", "v")
    assert [(c.name, c.type_code) for c in miss.schema.columns if not c.is_hidden] == \
        [("id", gnitz.TypeCode.I64), ("v", gnitz.TypeCode.I64)]


# ---------------------------------------------------------------------------
# A reply train that spans more than one frame
# ---------------------------------------------------------------------------


def test_a_reply_spanning_several_frames_returns_the_full_set(own_server):
    """A per-worker reply that spans several 16 KiB frames still returns every
    row, with payload intact across the chunk boundaries and net weights after a
    retraction."""
    with gnitz.connect(own_server.start(extra_env=TINY_REPLY_FRAMES).target) as client:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, y BIGINT NOT NULL)")
        # ~32 KB of matching rows per worker per value at 4 workers: every worker's
        # train is several frames past the 16 KiB budget.
        n = 4_000
        insert(client, "t", [(i, i % 2, i * 7) for i in range(n)])

        # Full-row values, so payload integrity through the boundaries is asserted
        # rather than just the key set.
        assert bag(rows(client, "SELECT * FROM t WHERE x = 0")) == \
            {(i, 0, i * 7): 1 for i in range(0, n, 2)}
        client.execute_sql("DELETE FROM t WHERE pk IN (1, 3, 5)")
        assert bag(rows(client, "SELECT pk FROM t WHERE x = 1")) == \
            {(i,): 1 for i in range(7, n, 2)}


# ---------------------------------------------------------------------------
# A SELECT that reads nothing
# ---------------------------------------------------------------------------


def test_a_from_less_select_answers_one_constant_row(client):
    """The probe a driver or health check sends: it reads nothing and answers
    one row under the computed-column names, with LIMIT honored."""
    assert bag(rows(client, "SELECT 1")) == {(1,): 1}
    assert bag(rows(client, "SELECT 1 + 2 AS three, 'x' AS s, 2.5 AS f")) == \
        {(3, "x", 2.5): 1}
    assert rows(client, "SELECT 1 AS a LIMIT 0") == []
