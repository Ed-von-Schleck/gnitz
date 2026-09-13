"""Where the exchange runs, and what must survive it.

Two placements of the same query can differ in whether the engine exchanges at
all: a reduce or join keyed on the source's own PK is already co-partitioned and
the exchange is elided, where the same query keyed on a payload column cannot be.
The elision is a runtime decision with no client-visible artifact, so what is
assertable — and what these tests hold — is that it never changes the answer.

Past that, a summed partial must survive the merge of every worker's
contribution, and a PK range spanning the whole U64 space must round-trip
whatever the hash does with it.
"""

import ctypes

import pytest
from _serverproc import NEEDS_MULTI
from _read import bag

pytestmark = NEEDS_MULTI

_N = 50

# Each pair is (co-partitioned view, its exchanging twin). The twin keys on a
# payload column holding the same value as the PK, so the two must agree row for
# row and weight for weight while only one of them can elide its exchange.
_ELISION_PAIRS = [
    pytest.param(
        ["CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, "
         "val BIGINT NOT NULL)"],
        ["INSERT INTO t VALUES " + ",".join(f"({i}, {i}, {i * 10})" for i in range(1, _N + 1))],
        "SELECT id AS k, SUM(val) AS total FROM t GROUP BY id",
        "SELECT grp AS k, SUM(val) AS total FROM t GROUP BY grp",
        ["k", "total"],
        {(i, i * 10): 1 for i in range(1, _N + 1)},
        id="reduce-on-pk",
    ),
    pytest.param(
        ["CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, x BIGINT NOT NULL)",
         "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, y BIGINT NOT NULL)"],
        ["INSERT INTO a VALUES " + ",".join(f"({i}, {i}, {i * 2})" for i in range(1, _N + 1)),
         "INSERT INTO b VALUES " + ",".join(f"({i}, {i}, {i * 3})" for i in range(1, _N + 1))],
        "SELECT a.id AS k, a.x AS x, b.y AS y FROM a JOIN b ON a.id = b.id",
        "SELECT a.k AS k, a.x AS x, b.y AS y FROM a JOIN b ON a.k = b.k",
        ["k", "x", "y"],
        {(i, i * 2, i * 3): 1 for i in range(1, _N + 1)},
        id="join-on-pk",
    ),
]


@pytest.mark.parametrize("ddl,rows,co_body,exchanged_body,cols,want", _ELISION_PAIRS)
def test_eliding_the_exchange_does_not_change_the_answer(
        client, schema_name, ddl, rows, co_body, exchanged_body, cols, want):
    """A co-partitioned view and the twin that cannot co-partition agree exactly.

    The twin is the control: it keys on a payload column carrying the same value
    as the PK, so it must exchange where the co-partitioned one may skip. Both are
    checked against the same independently computed weight-multiset, so a skip
    that dropped a group, kept a per-worker partial, or doubled a weight fails on
    the co-partitioned side while the control still passes.
    """
    for stmt in ddl:
        client.execute_sql(stmt, schema_name=schema_name)
    client.execute_sql(f"CREATE VIEW v_co AS {co_body}", schema_name=schema_name)
    client.execute_sql(f"CREATE VIEW v_ex AS {exchanged_body}", schema_name=schema_name)
    for stmt in rows:
        client.execute_sql(stmt, schema_name=schema_name)

    for name in ("v_co", "v_ex"):
        vid, _ = client.resolve_table(schema_name, name)
        assert bag(client.scan(vid), *cols) == want, name


def test_a_summed_partial_survives_the_merge(client, schema_name):
    """One group fed by every worker: the reduce's per-worker partials are merged
    into a single row at weight 1.

    All 100 rows share a group, so at any worker count every worker holds a
    partial. An exchange output wrongly marked consolidated leaves the partials
    unmerged — several rows for the one group, or a retraction that never
    cancels — which the weight-multiset catches and a `totals[grp]` lookup, which
    keeps only the last row it sees, would not.
    """
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, grp BIGINT NOT NULL, "
        "val BIGINT NOT NULL)", schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW v AS SELECT grp, SUM(val) AS total FROM t GROUP BY grp",
        schema_name=schema_name)
    client.execute_sql(
        "INSERT INTO t VALUES " + ",".join(f"({i}, 1, {i})" for i in range(1, 101)),
        schema_name=schema_name)

    vid, _ = client.resolve_table(schema_name, "v")
    assert bag(client.scan(vid), "grp", "total") == {(1, sum(range(1, 101))): 1}


def test_a_wide_u64_pk_range_round_trips(client, schema_name):
    """PKs drawn from the low, middle and high thirds of the U64 space all come
    back exactly once.

    The high keys have their top bit set, so they exercise the OPK sign flip and
    the XXH3 routing over the full width rather than the small integers every
    other test uses.
    """
    pks = ([i for i in range(1, 33)]
           + [(1 << 32) + i for i in range(32)]
           + [(1 << 62) + i for i in range(32)])
    client.execute_sql(
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        schema_name=schema_name)
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(
            f"({ctypes.c_int64(pk).value}, {pk & 0xFFFF})" for pk in pks),
        schema_name=schema_name)

    tid, _ = client.resolve_table(schema_name, "t")
    rows = list(client.scan(tid))
    assert all(r.weight == 1 for r in rows), "a key routed to two workers comes back twice"
    assert sorted(ctypes.c_uint64(r.pk).value for r in rows) == sorted(pks)
