"""A DDL issued while a tick is in flight waits for that tick, and what it changes
is in force on every worker by the next statement.

`GNITZ_INJECT_TICK_HOLD_FOR_DDL` holds the server's first tick in flight — its
group written, the tick gate held, its ACKs outstanding — until a DDL is waiting
on the tick gate, so each DDL here provably lands on a mid-epoch tick. The seam
is one-shot, so each case gets its own server.
"""

import gnitz
import pytest
from _read import bag, scanned

PARKED_ROWS, PARKED_GROUPS = 20_000, 8
# The GROUP BY the parked tick maintains: every source row, counted once.
_PARKED_VIEW = {(a, PARKED_ROWS // PARKED_GROUPS): 1 for a in range(PARKED_GROUPS)}
_T = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, c BIGINT NOT NULL)"


@pytest.mark.parametrize("before, ddl, after, read", [
    ([_T, "INSERT INTO t VALUES (1, 100), (2, 200)"],
     "CREATE INDEX ix ON t (c)", ["DELETE FROM t WHERE c = 100"],
     ("t", ("pk", "c"), {(2, 200): 1})),
    ([], _T, ["BEGIN", "INSERT INTO t VALUES " + ", ".join(f"({i}, {i * 10})" for i in range(64)), "COMMIT"],
     ("t", ("pk", "c"), {(i, i * 10): 1 for i in range(64)})),
    ([], "CREATE VIEW v2 AS SELECT a, COUNT(*) AS n FROM src GROUP BY a", [],
     ("v2", ("a", "n"), _PARKED_VIEW)),
], ids=["index-then-indexed-delete", "table-then-transactional-insert", "view-over-the-parked-source"])
def test_a_ddl_during_an_in_flight_tick(own_server, before, ddl, after, read):
    """The indexed DELETE needs the index on every worker; the transaction's COMMIT
    reports Ok even when a worker dropped its partition's share, so only the rows
    show it; and a view created over the parked source must neither miss nor
    double-count the parked rows. The view the parked tick was maintaining still
    holds every source row once."""
    c = gnitz.connect(own_server.start(extra_env={"GNITZ_INJECT_TICK_HOLD_FOR_DDL": "1"}).target)
    for sql in [*before,
                "CREATE TABLE src (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
                "CREATE VIEW v AS SELECT a, COUNT(*) AS n FROM src GROUP BY a"]:
        c.execute_sql(sql)
    # The GROUP BY seeds an exchange, and a push crossing the tick-coalesce
    # threshold fires its tick before the push ACKs.
    tid, schema = c.resolve_table("src")
    c.push(tid, gnitz.ZSetBatch(schema).extend([{"pk": i, "a": i % PARKED_GROUPS} for i in range(PARKED_ROWS)]))

    for sql in [ddl, *after]:
        c.execute_sql(sql)

    name, cols, want = read
    assert bag(scanned(c, name), *cols) == want
    assert bag(scanned(c, "v"), "a", "n") == _PARKED_VIEW
