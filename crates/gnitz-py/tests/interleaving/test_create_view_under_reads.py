"""CREATE VIEW while ad-hoc reads and pushes are in flight.

A CREATE VIEW parks the single-threaded reactor (`drain_tick_blocking` and
`fan_out_backfill` are synchronous futex loops) under the catalog write lock.
An ad-hoc read holds the catalog READ lock for one whole atomic fan-out with no
mid-flight release, so the writer-preferring lock orders each read entirely
before or after the DDL window — never interleaved, never wedged.
"""

import threading

import gnitz
import pytest
from _read import bag, scanned
from _serverproc import HANG_TIMEOUT, NEEDS_MULTI, join_or_fail

pytestmark = NEEDS_MULTI

# Each thread's work outlasts the CREATE VIEW by two orders of magnitude (400
# reads is ~200 ms against a ~3 ms DDL), which is what puts the DDL inside the
# concurrent window by construction rather than by a sampled counter that a
# descheduled thread can zero.
_ITERATIONS = 400

_GROUPS = "SELECT g, COUNT(*) AS n FROM t GROUP BY g"


def _zset(res):
    """`{(g, n) → net weight}` over a one-statement Rows result."""
    assert res[0]["type"] == "Rows", res[0]["type"]
    out = {}
    for r in res[0]["rows"]:
        k = (r.g, r.n)
        out[k] = out.get(k, 0) + r.weight
    return {k: w for k, w in out.items() if w != 0}


def test_create_view_under_concurrent_adhoc_reads(client, server, schema_name):
    """Reads and pushes issued throughout a CREATE VIEW must all succeed, the
    DDL must complete, and the view it built must agree with the same query run
    ad-hoc — at equal weights, since a backfill that double-applied a source
    batch keeps the group set and doubles every count."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL)",
        schema_name=schema_name)
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(f"({i}, {i % 5})" for i in range(1, 201)),
        schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE other (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
        schema_name=schema_name)

    errors = []
    reading = threading.Event()

    def hammer_reads():
        try:
            with gnitz.connect(server) as c:
                for _ in range(_ITERATIONS):
                    res = c.execute_sql(_GROUPS, schema_name=schema_name)
                    assert res[0]["type"] == "Rows"
                    n = sum(r.n for r in res[0]["rows"])
                    assert n == 200, f"ad-hoc read must see all 200 rows, saw {n}"
                    reading.set()
        except Exception as e:  # noqa: BLE001
            errors.append(("read", repr(e)))
            reading.set()

    def hammer_pushes():
        try:
            with gnitz.connect(server) as c:
                for i in range(_ITERATIONS):
                    c.execute_sql(f"INSERT INTO other VALUES ({i}, {i})",
                                  schema_name=schema_name)
        except Exception as e:  # noqa: BLE001
            errors.append(("push", repr(e)))

    threads = [threading.Thread(target=hammer_reads), threading.Thread(target=hammer_pushes)]
    for t in threads:
        t.start()

    # The DDL goes in once the readers are provably in their loop, so it lands
    # inside the concurrent window rather than ahead of it.
    assert reading.wait(timeout=HANG_TIMEOUT), "the read thread never issued a read"
    client.execute_sql(
        f"CREATE VIEW mid AS {_GROUPS}", schema_name=schema_name)

    join_or_fail("deadlock: a concurrent worker never completed", *threads)
    assert not errors, f"concurrent work failed: {errors}"

    view = _zset(client.execute_sql("SELECT * FROM mid", schema_name=schema_name))
    assert view == _zset(client.execute_sql(_GROUPS, schema_name=schema_name))
    assert set(view.values()) == {1}, f"view holds a non-unit weight: {view}"


@pytest.mark.parametrize("body, cols, want", [
    ("SELECT a.id AS aid, b.bv AS bv FROM a JOIN b ON a.k = b.id", ("aid", "bv"),
     lambda n: {(i, (i % 5) * 100): 1 for i in range(n)}),
    ("SELECT a.id AS aid, a.av + 1 AS bv FROM a", ("aid", "bv"),
     lambda n: {(i, i * 10 + 1): 1 for i in range(n)}),
], ids=["exchange", "linear"])
def test_a_view_created_over_a_table_being_written_holds_every_row(
        client, schema_name, server, body, cols, want):
    """Rows land in the sources from a second connection WHILE the CREATE VIEW
    runs. The finished view must equal brute force over the whole base — the
    snapshot rows plus every post-create delta — at weight 1 each.

    The write lock serialises the interleave, so the result must be correct
    whichever order the two land in; both an exchange body and a partition-local
    one are run, because only the first pays a shuffle for its backfill.
    """
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
        "av BIGINT NOT NULL)", schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, bv BIGINT NOT NULL)",
        schema_name=schema_name)
    client.execute_sql(
        "INSERT INTO b VALUES " + ", ".join(f"({j}, {j * 100})" for j in range(5)),
        schema_name=schema_name)
    # Snapshot rows present before the CREATE.
    client.execute_sql(
        "INSERT INTO a VALUES " + ", ".join(f"({i}, {i % 5}, {i * 10})" for i in range(30)),
        schema_name=schema_name)

    errors = []
    writing = threading.Event()

    def inserter():
        try:
            with gnitz.connect(server) as c2:
                for i in range(30, 60):
                    c2.execute_sql(
                        f"INSERT INTO a VALUES ({i}, {i % 5}, {i * 10})",
                        schema_name=schema_name)
                    writing.set()
        except Exception as e:  # noqa: BLE001
            errors.append(repr(e))
            writing.set()

    t = threading.Thread(target=inserter)
    t.start()
    try:
        # The DDL goes in once the writer is provably in its loop.
        assert writing.wait(timeout=HANG_TIMEOUT), "the write thread never issued a write"
        client.execute_sql(f"CREATE VIEW v AS {body}", schema_name=schema_name)
    finally:
        join_or_fail("the inserter hung — possible wedge", t)
    assert not errors, f"concurrent inserter failed: {errors}"

    assert bag(scanned(client, schema_name, "v"), *cols) == want(60)
