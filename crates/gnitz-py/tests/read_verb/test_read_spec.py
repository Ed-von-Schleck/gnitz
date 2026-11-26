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
from _read import access, bag, ordered, plan_line, reply, rows
from _serverproc import TINY_REPLY_FRAMES, join_or_fail, spawn
from _sql import insert

# Enough rows that an indexed point clears the worker's selectivity gate at any
# worker count, and far more distinct keys than workers.
NROWS = 400
# Consecutive keys from 0 reach every worker well before this many, at any legal
# worker count.
SPAN = 96


@pytest.fixture(scope="module")
def indexed(module_client):
    """A connection holding `t (id PK, v)` with `v = id * 10` over `NROWS` rows
    and an index on `v`. Read-only: every case that takes it is a read."""
    conn = module_client
    conn.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)")
    insert(conn, "t", [(i, i * 10) for i in range(NROWS)])
    conn.execute_sql("CREATE INDEX ON t (v)")
    return conn


# ---------------------------------------------------------------------------
# Server-side predicate and projection
# ---------------------------------------------------------------------------


def test_the_projection_is_the_clients_to_shape(indexed):
    """The reply keeps the SELECT list as written, carrying the physical PK at
    its SELECT slot or hidden in front when unselected — so a column may precede
    the PK, repeat under two names, or be computed, and the presented shape is
    the SELECT list alone."""
    for q, fields, want in [("SELECT v, id FROM t WHERE id = 5", ("v", "id"), (50, 5)),
                            ("SELECT id AS x, id AS y FROM t WHERE id = 5", ("x", "y"), (5, 5)),
                            ("SELECT v + 1 AS vp1 FROM t WHERE id = 5", ("vp1",), (51,))]:
        got = reply(indexed.execute_sql(q))
        assert got._fields == fields, q
        assert bag(got) == {want: 1}, q
    assert plan_line(indexed, "SELECT id FROM t WHERE v + id > 7", "predicate") == "server-side"


def test_every_read_reports_the_lsn_it_was_read_at(indexed):
    """A read answered by one server read carries that read's watermark, whichever
    verb asks and whichever access path serves it."""
    tid, schema = indexed.resolve_table("t")
    lsn = indexed.scan(tid, schema).lsn
    assert lsn is not None
    assert indexed.seek(tid, schema, 3).lsn == lsn
    assert indexed.seek_by_index(tid, schema, [1], [30]).lsn == lsn
    assert indexed.scan_many([(tid, schema)])[0].lsn == lsn
    for q in ["SELECT * FROM t WHERE v = 30",
              "SELECT * FROM t WHERE id = 3",
              "SELECT * FROM t",
              "SELECT COUNT(*) AS n FROM t"]:
        assert reply(indexed.execute_sql(q)).lsn == lsn, q


# ---------------------------------------------------------------------------
# PK bounds and keyed gathers
# ---------------------------------------------------------------------------


def test_a_point_finds_every_key_from_its_owner(indexed):
    """A PK point is sent to the one worker owning the key; an index point is
    broadcast and each worker gathers its own source rows. Through SQL and the
    binary verbs alike, every key comes back exactly once over keys that scatter
    across every partition, and an absent one is an empty answer."""
    tid, schema = indexed.resolve_table("t")
    assert access(indexed, "SELECT id, v FROM t WHERE id = 0") == "pk point lookup"
    assert access(indexed, "SELECT id, v FROM t WHERE v = 0") == "index range on (v)"
    for i in range(SPAN):
        want = {(i, i * 10): 1}
        for q in (f"SELECT id, v FROM t WHERE id = {i}", f"SELECT id, v FROM t WHERE v = {i * 10}"):
            assert bag(rows(indexed, q)) == want, q
        assert bag(indexed.seek(tid, schema, i)) == want, i
        assert bag(indexed.seek_by_index(tid, schema, [1], [i * 10])) == want, i
    assert rows(indexed, f"SELECT id FROM t WHERE id = {NROWS + 7}") == []
    assert list(indexed.seek(tid, schema, NROWS + 7)) == []
    assert list(indexed.seek_by_index(tid, schema, [1], [7])) == []


@pytest.mark.parametrize("predicate,walk,keep", [
    # A multi-key PK range on a full-PK-hashed table is not confinable, so it
    # keeps the merged cursor over every partition.
    ("id >= 100", "pk range walk", lambda i: i >= 100),
    ("id > 5 AND id < 9", "pk range walk", lambda i: 5 < i < 9),
    ("v BETWEEN 0 AND 90", "index range on (v)", lambda i: i <= 9),
    ("v BETWEEN 1000 AND 1090", "index range on (v)", lambda i: 100 <= i <= 109),
    ("v BETWEEN 3900 AND 3990", "index range on (v)", lambda i: 390 <= i),
    # Nearly the whole table: each worker trades the walk for a filtered scan.
    ("v >= 10", "index range on (v)", lambda i: i >= 1),
])
def test_a_range_loses_no_row_of_any_partition(indexed, predicate, walk, keep):
    q = f"SELECT id, v FROM t WHERE {predicate}"
    assert access(indexed, q) == walk
    assert bag(rows(indexed, q)) == {(i, i * 10): 1 for i in range(NROWS) if keep(i)}


def test_a_pk_set_gathers_every_named_key(indexed):
    """`id IN (…)` is the PkSet gather: each key is sent only to the worker owning
    it. The union must be the whole list at weight 1 — a key two
    workers both claim shows up here as weight 2, as does a key the list names
    twice — and absent keys miss silently rather than erroring."""
    wanted = list(range(0, NROWS, 3))
    in_list = ",".join(str(i) for i in wanted + wanted[:3] + [NROWS + 99])
    q = f"SELECT id, v FROM t WHERE id IN ({in_list})"
    assert access(indexed, q) == f"pk set gather ({len(wanted) + 1} keys)"
    assert bag(rows(indexed, q)) == {(i, i * 10): 1 for i in wanted}


def test_an_index_set_gathers_every_listed_key(indexed):
    """`v IN (…)` on an indexed column walks the index at each listed key on
    every worker, each gathering its own source rows. Every listed key comes back
    once at weight 1 — a repeated key or two workers claiming one show up as
    weight 2 — absent keys miss silently, and the set serves a cut and a fold as
    the scan it replaces would."""
    wanted = list(range(0, NROWS, 7))
    in_list = ",".join(str(i * 10) for i in wanted + wanted[:3]) + ",7,-10"
    q = f"SELECT id, v FROM t WHERE v IN ({in_list})"
    assert access(indexed, q) == f"index set on (v) ({len(wanted) + 2} keys)"
    assert plan_line(indexed, q, "predicate") == "none"
    assert bag(rows(indexed, q)) == {(i, i * 10): 1 for i in wanted}
    top = sorted(wanted, reverse=True)[1:4]
    got = ordered(rows(indexed, f"{q} ORDER BY v DESC LIMIT 3 OFFSET 1"), "id", "v")
    assert got == [((i, i * 10), 1) for i in top]
    total = rows(indexed, f"SELECT COUNT(*) AS n, SUM(v) AS s FROM t WHERE v IN ({in_list})")
    assert bag(total) == {(len(wanted), sum(i * 10 for i in wanted)): 1}
    # The other conjuncts on the column narrow the list; the rest filter it.
    narrowed = f"SELECT id FROM t WHERE v IN ({in_list}) AND v > 100 AND id % 2 = 0"
    assert bag(rows(indexed, narrowed)) == {(i,): 1 for i in wanted if i > 10 and i % 2 == 0}


def test_an_index_set_reads_what_a_scan_does(client):
    """An `IN` list through an index — on a 128-bit column, which no expression
    reads, and after a pinned prefix of a compound index — reads the rows the
    same list reads through a scan of an unindexed copy, over a non-unique column
    and across updates and deletes that move rows between keys. The copy holds
    each UUID as the number it was made from."""
    def uuid(k):
        return f"'00000000-0000-0000-0000-{k:012x}'"
    client.execute_sql(
        "CREATE TABLE ix (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, k BIGINT, u UUID); "
        "CREATE TABLE plain (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, k BIGINT, u BIGINT)")
    client.execute_sql("CREATE INDEX ix_u ON ix (u); CREATE INDEX ix_gk ON ix (g, k)")
    rows_of = [(i, i % 3, i % 11, i % 13) for i in range(300)]
    client.execute_sql("INSERT INTO ix VALUES " + ",".join(f"({i}, {g}, {k}, {uuid(u)})" for i, g, k, u in rows_of))
    client.execute_sql("INSERT INTO plain VALUES " + ",".join(f"({i}, {g}, {k}, {u})" for i, g, k, u in rows_of))

    def agree(ix_where, plain_where, walk):
        q = "SELECT id, g, k FROM {} WHERE {}"
        assert access(client, q.format("ix", ix_where)) == walk, ix_where
        got = bag(rows(client, q.format("ix", ix_where)))
        assert got and got == bag(rows(client, q.format("plain", plain_where))), ix_where

    keys = (0, 4, 12, 99)
    for step in range(3):
        agree(f"u IN ({','.join(uuid(k) for k in keys)})", f"u IN ({','.join(map(str, keys))})",
              "index set on (u) (4 keys)")
        agree("g = 2 AND k IN (1, 5, 10, 40)", "g = 2 AND k IN (1, 5, 10, 40)", "index set on (g, k) (4 keys)")
        for t, four, pair in (("ix", uuid(4), f"{uuid(0)}, {uuid(12)}"), ("plain", "4", "0, 12")):
            client.execute_sql(f"UPDATE {t} SET u = {four}, k = 5 WHERE id % 7 = {step}")
            client.execute_sql(f"DELETE FROM {t} WHERE id % 17 = {step}")
            # A write's read through the set, in a transaction and out of one.
            client.execute_sql(f"DELETE FROM {t} WHERE u IN ({pair}) AND id % 5 = {step}")
            client.execute_sql(f"BEGIN; UPDATE {t} SET k = 1 WHERE u IN ({pair}) AND id % 4 = {step}; COMMIT")


def test_an_empty_indexed_read_keeps_the_column_metadata(indexed):
    """An indexed read matching on no worker still presents the column metadata
    in SELECT order, and no hidden column is part of the presented shape."""
    miss = reply(indexed.execute_sql("SELECT id, v FROM t WHERE v = 7"))
    assert len(miss) == 0
    assert miss._fields == ("id", "v")
    assert [(c.name, c.type_code) for c in miss.schema.columns if not c.is_hidden] == \
        [("id", gnitz.TypeCode.I64), ("v", gnitz.TypeCode.I64)]


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


@pytest.mark.parametrize("cluster", ["", " CLUSTER BY a"], ids=["full-pk", "cluster-by-a"])
def test_a_compound_key_read_returns_its_rows(client, cluster):
    """A full point is one key wide, so it unicasts to the worker owning that
    key, through SQL and a tuple-keyed seek alike. Under `CLUSTER BY a` every
    row sharing `a` lands in one partition, so a bound pinning `a` and ranging
    `b` is confined too — and must still return the whole group, not the one
    key the range starts at."""
    client.execute_sql(
        "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT, "
        f"PRIMARY KEY (a, b)){cluster}")
    insert(client, "t", [(a, b, a * 100 + b) for a in range(6) for b in range(10)])
    tid, schema = client.resolve_table("t")
    for a, b in [(0, 0), (3, 4), (5, 9)]:
        want = {(a, b, a * 100 + b): 1}
        assert bag(rows(client, f"SELECT * FROM t WHERE a = {a} AND b = {b}")) == want
        assert bag(client.seek(tid, schema, (a, b))) == want
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
    """The probe a driver or health check sends: it reads nothing, so it carries
    no LSN, and answers one row under the computed-column names, with LIMIT
    honored."""
    assert bag(rows(client, "SELECT 1")) == {(1,): 1}
    got = reply(client.execute_sql("SELECT 1 + 2 AS three, 'x' AS s, 2.5 AS f"))
    assert got.lsn is None
    assert got._fields == ("three", "s", "f")
    assert bag(got) == {(3, "x", 2.5): 1}
    assert rows(client, "SELECT 1 AS a LIMIT 0") == []
