"""CREATE VIEW while ad-hoc reads and pushes are in flight.

A CREATE VIEW parks the single-threaded reactor (`drain_tick_blocking` and
`fan_out_backfill` are synchronous futex loops) under the catalog write lock.
An ad-hoc read holds the catalog READ lock for one whole atomic fan-out with no
mid-flight release, so the writer-preferring lock orders each read entirely
before or after the DDL window — never interleaved, never wedged.
"""

import threading

import gnitz
from _serverproc import HANG_TIMEOUT, NEEDS_MULTI

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

    for t in threads:
        t.join(timeout=HANG_TIMEOUT)
        assert not t.is_alive(), "deadlock: a concurrent worker never completed"
    assert not errors, f"concurrent work failed: {errors}"

    view = _zset(client.execute_sql("SELECT * FROM mid", schema_name=schema_name))
    assert view == _zset(client.execute_sql(_GROUPS, schema_name=schema_name))
    assert set(view.values()) == {1}, f"view holds a non-unit weight: {view}"
