"""A DDL landing on worker work still in flight waits for it, and what it changes
is in force on every worker by the next statement.

Each case arms a one-shot seam that holds that work in flight, so it gets its own
server.
"""

import gnitz
import pytest
from _read import bag, scanned
from _schemas import KV, kv
from _sql import values

# A push of `PARKED_ROWS` crosses the pinned threshold and fires its tick.
COALESCE_ROWS, PARKED_ROWS, PARKED_GROUPS = 100, 200, 8
# The GROUP BY the parked tick maintains: every source row, counted once.
_PARKED_VIEW = {(a, PARKED_ROWS // PARKED_GROUPS): 1 for a in range(PARKED_GROUPS)}
_T = "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, c BIGINT NOT NULL)"


@pytest.mark.parametrize("before, ddl, after, read", [
    ([_T, "INSERT INTO t VALUES (1, 100), (2, 200)"],
     "CREATE INDEX ix ON t (c)", ["DELETE FROM t WHERE c = 100"],
     ("t", ("pk", "c"), {(2, 200): 1})),
    ([], _T, ["BEGIN", f"INSERT INTO t VALUES {values((i, i * 10) for i in range(64))}", "COMMIT"],
     ("t", ("pk", "c"), {(i, i * 10): 1 for i in range(64)})),
    ([], "CREATE VIEW v2 AS SELECT a, COUNT(*) AS n FROM src GROUP BY a", [],
     ("v2", ("a", "n"), _PARKED_VIEW)),
], ids=["index-then-indexed-delete", "table-then-transactional-insert", "view-over-the-parked-source"])
def test_a_ddl_during_an_in_flight_tick(own_server, before, ddl, after, read):
    """The indexed DELETE needs the index on every worker; the transaction's COMMIT
    reports Ok even when a worker dropped its partition's share, so only the rows
    show it; and a view created over the parked source must neither miss nor
    double-count the parked rows. The view the parked tick was maintaining still
    holds every source row once.

    `GNITZ_INJECT_TICK_HOLD_FOR_DDL` holds the server's first tick in flight — its
    group written, the tick gate held, its ACKs outstanding — until a DDL is
    waiting on the tick gate. `ddl` is the only DDL issued after the push, so the
    hold's logged release proves it landed on a mid-epoch tick."""
    c = gnitz.connect(own_server.start(extra_env={
        "GNITZ_INJECT_TICK_HOLD_FOR_DDL": "1", "GNITZ_TICK_COALESCE_ROWS": str(COALESCE_ROWS)}).target)
    c.execute_sql("; ".join([*before,
                             "CREATE TABLE src (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
                             "CREATE VIEW v AS SELECT a, COUNT(*) AS n FROM src GROUP BY a"]))
    # The GROUP BY seeds an exchange, and a push crossing the tick-coalesce
    # threshold fires its tick before the push ACKs.
    tid, schema = c.resolve_table("src")
    c.push(tid, gnitz.ZSetBatch(schema).extend([{"pk": i, "a": i % PARKED_GROUPS} for i in range(PARKED_ROWS)]))

    for sql in [ddl, *after]:
        c.execute_sql(sql)

    assert "tick hold: released by its event" in own_server.log_text()
    name, cols, want = read
    assert bag(scanned(c, name), *cols) == want
    assert bag(scanned(c, "v"), "a", "n") == _PARKED_VIEW


def test_a_drop_does_not_race_a_worker_still_creating_the_table(own_server):
    """The master sweeps a dropped table's directory only behind a checkpoint's
    worker-ACK barrier, so a worker still inside that table's CREATE finishes
    against a directory that exists instead of aborting on ENOENT.

    `GNITZ_INJECT_TABLE_CREATE_DELAY_MS` makes every worker sleep between creating
    the table directory and its partition subdirectories, and a DDL's ACK waits on
    no worker, so each DROP lands inside a lagging CREATE. Every worker answers the
    closing scan only after applying the racing groups, so one that aborted fails
    it by ordering rather than by timing."""
    client = gnitz.connect(own_server.start(extra_env={"GNITZ_INJECT_TABLE_CREATE_DELAY_MS": "50"}).target)
    for i in range(3):
        client.create_table(f"t{i}", KV)
        client.drop_table(f"t{i}")
    tid = client.create_table("t", KV)
    client.push(tid, kv((1, 1)))
    assert bag(client.scan(tid, KV)) == {(1, 1): 1}
