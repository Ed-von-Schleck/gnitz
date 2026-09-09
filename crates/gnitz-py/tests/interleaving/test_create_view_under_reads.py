"""CREATE VIEW must not wedge under concurrent ad-hoc reads and pushes.
"""

import os
import threading
import time

import pytest
import gnitz
from _uid import uid as _uid




_NEEDS_MULTI = pytest.mark.skipif(
    int(os.environ.get("GNITZ_WORKERS", "1")) < 2,
    reason="the read/DDL concurrency path only exercises exchange/fanout at W >= 2",
)



@_NEEDS_MULTI
def test_create_view_under_concurrent_adhoc_reads(client, server):
    """CREATE VIEW must not wedge under concurrent ad-hoc reads and pushes.

    A CREATE VIEW parks the single-threaded reactor (drain_tick_blocking +
    fan_out_backfill are synchronous futex loops) under the catalog write lock.
    Ad-hoc reads take only the catalog READ lock for the whole of one atomic
    fan-out (no mid-flight release), so the writer-preferring catalog_rwlock
    serialises each read entirely before or after the DDL window — never
    interleaved, never wedged. Reads before/during/after the CREATE VIEW must all
    succeed, ingestion must continue, and the post-DDL read must agree with the
    view built mid-flight.
    """
    sn = "s" + _uid()
    client.create_schema(sn)
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL)", schema_name=sn
    )
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(f"({i}, {i % 5})" for i in range(1, 201)), schema_name=sn
    )
    client.execute_sql(
        "CREATE TABLE other (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn
    )

    errors = []
    ddl_done = threading.Event()
    q_count = [0]

    def hammer_reads():
        try:
            with gnitz.connect(server) as c:
                while not ddl_done.is_set() and q_count[0] < 400:
                    res = c.execute_sql("SELECT g, COUNT(*) AS n FROM t GROUP BY g", schema_name=sn)
                    assert res[0]["type"] == "Rows"
                    n = sum(r.n for r in res[0]["rows"])
                    assert n == 200, f"ad-hoc read must see all 200 rows, saw {n}"
                    q_count[0] += 1
        except Exception as e:  # noqa: BLE001
            errors.append(("read", repr(e)))

    def hammer_pushes():
        try:
            with gnitz.connect(server) as c:
                i = 0
                while not ddl_done.is_set() and i < 400:
                    c.execute_sql(f"INSERT INTO other VALUES ({i}, {i})", schema_name=sn)
                    i += 1
        except Exception as e:  # noqa: BLE001
            errors.append(("push", repr(e)))

    tq = threading.Thread(target=hammer_reads)
    tp = threading.Thread(target=hammer_pushes)
    tq.start()
    tp.start()

    time.sleep(0.05)
    q_before = q_count[0]
    t0 = time.time()
    client.execute_sql("CREATE VIEW mid AS SELECT g, COUNT(*) AS n FROM t GROUP BY g", schema_name=sn)
    ddl_secs = time.time() - t0
    q_across = q_count[0] - q_before
    ddl_done.set()

    tq.join(timeout=120)
    tp.join(timeout=120)
    assert not tq.is_alive() and not tp.is_alive(), "deadlock: a concurrent worker never completed"
    assert not errors, f"concurrent work failed: {errors}"
    assert ddl_secs < 60, f"CREATE VIEW took {ddl_secs:.1f}s -- wedged behind the reads"
    assert q_across > 0, (
        "no ad-hoc read overlapped the CREATE VIEW -- the test proved nothing about read/DDL concurrency"
    )

    # The mid-flight DDL produced a correct view, and ad-hoc reads still agree.
    def _rows(sql):
        res = client.execute_sql(sql, schema_name=sn)
        out = []
        for r in res[0]["rows"]:
            if r.weight <= 0:
                continue
            d = r._asdict()
            out.append((tuple(d[k] for k in sorted(d)), r.weight))
        return sorted(out)

    assert _rows("SELECT g, COUNT(*) AS n FROM t GROUP BY g") == _rows("SELECT * FROM mid"), (
        "post-DDL ad-hoc read must agree with the view built mid-flight"
    )
