"""A DDL landing on a tick still in flight waits for it, and what it changes is in
force on every worker by the next statement.

Each case arms a one-shot seam that holds the tick in flight, so it gets its own
server.
"""

import gnitz
import pytest
from _read import bag, scanned
from _schemas import KV, kv
from _serverproc import MULTI
from _sql import values

# A push of `PARKED_ROWS` crosses the pinned threshold and fires its tick before
# the push ACKs.
COALESCE_ROWS, PARKED_ROWS, PARKED_GROUPS = 100, 200, 8
# The GROUP BY the parked tick maintains: every source row, counted once.
_PARKED_VIEW = {(g, PARKED_ROWS // PARKED_GROUPS): 1 for g in range(PARKED_GROUPS)}
_T = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, c BIGINT NOT NULL)"


@pytest.mark.parametrize("before, stmts, read", [
    ([_T, "INSERT INTO t VALUES (1, 100), (2, 200)"],
     ["CREATE INDEX ix ON t (c)", "DELETE FROM t WHERE c = 100"],
     ("t", {(2, 200): 1})),
    ([], [_T, "BEGIN", f"INSERT INTO t VALUES {values((i, i * 10) for i in range(64))}", "COMMIT"],
     ("t", {(i, i * 10): 1 for i in range(64)})),
    ([], ["CREATE VIEW v2 AS SELECT val, COUNT(*) AS n FROM src GROUP BY val"],
     ("v2", _PARKED_VIEW)),
], ids=["index-then-indexed-delete", "table-then-transactional-insert", "view-over-the-parked-source"])
def test_a_ddl_during_an_in_flight_tick(own_server, before, stmts, read):
    """The indexed DELETE needs the index on every worker; the transaction's rows
    need the table on every worker, so a worker missing it shows in the rows; and
    a view created over the parked source must neither miss nor double-count the
    parked rows. The view the parked tick was maintaining still holds every source
    row once.

    `GNITZ_INJECT_TICK_HOLD_FOR_DDL` holds the server's first tick in flight — its
    group written, the apply lock held, its ACKs outstanding — until a task is
    waiting on that lock. Nothing here asks for a checkpoint, the lock's other
    taker, and the first of `stmts` is the only DDL issued after the push, so the
    hold's logged release proves it landed on a mid-epoch tick."""
    own_server.start(workers=MULTI, extra_env={
        "GNITZ_INJECT_TICK_HOLD_FOR_DDL": "1", "GNITZ_TICK_COALESCE_ROWS": str(COALESCE_ROWS)})
    with gnitz.connect(own_server.target) as c:
        if before:
            c.execute_sql("; ".join(before))
        src = c.create_table("src", KV)
        c.execute_sql("CREATE VIEW v AS SELECT val, COUNT(*) AS n FROM src GROUP BY val")
        c.push(src, kv(*((i, i % PARKED_GROUPS) for i in range(PARKED_ROWS))))

        for sql in stmts:
            c.execute_sql(sql)

        assert "tick hold: released by its event" in own_server.log_text()
        name, want = read
        assert bag(scanned(c, name)) == want
        assert bag(scanned(c, "v")) == _PARKED_VIEW
