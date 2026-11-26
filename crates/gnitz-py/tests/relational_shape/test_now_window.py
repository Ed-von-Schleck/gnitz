"""A view body comparing a column against NOW(): a join with `_system.clock`,
which the server moves forward, so rows leave and enter the view as time passes.

Every expectation is a bag, weights included, and every timing is an eventual
state polled under a deadline: the clock steps every `_RES_MS`, and the margins
below are seconds wide.
"""
import time
from datetime import datetime, timezone

import gnitz
from _read import bag, rows
from _serverproc import MULTI

_RES_MS = 50
_ENV = {"GNITZ_CLOCK_RESOLUTION_MS": str(_RES_MS)}
_W = 10  # the window, seconds
_DEADLINE = 30.0


def _ts(t):
    """`t` (Unix seconds) as a TIMESTAMP literal."""
    return f"TIMESTAMP '{datetime.fromtimestamp(t, timezone.utc):%Y-%m-%d %H:%M:%S.%f}'"


def _settles(client, view, want):
    """Poll `view` until it reads `want`, weight for weight."""
    deadline = time.monotonic() + _DEADLINE
    while True:
        got = bag(rows(client, f"SELECT id FROM {view}"), "id")
        if got == want:
            return
        assert time.monotonic() < deadline, f"{view}: {got} never became {want}"
        time.sleep(_RES_MS / 1000)


def _ids(*ids):
    return {(i,): 1 for i in ids}


def _boot(server):
    server.start(workers=MULTI, extra_env=_ENV)
    return gnitz.connect(server.target)


def _seed(client, t0):
    client.execute_sql("CREATE TABLE ev (id BIGINT PRIMARY KEY, ts TIMESTAMP, v BIGINT)")
    client.execute_sql(f"CREATE VIEW recent AS SELECT id, v FROM ev WHERE ts > NOW() - {_W * 1_000_000}")
    client.execute_sql("CREATE VIEW due AS SELECT id FROM ev WHERE ts <= NOW()")
    # A window bounded on both sides: a view over a view, both reading the clock.
    client.execute_sql(f"CREATE VIEW open_ev AS SELECT id, ts FROM ev WHERE ts > NOW() - {_W * 1_000_000}")
    client.execute_sql("CREATE VIEW band AS SELECT id FROM open_ev WHERE ts <= NOW()")
    # 1 expired before it arrives, 2 expiring 1.5 s from now, 3 inside the window
    # for all of it, 4 not due until 1.5 s from now.
    stamps = {1: t0 - 2 * _W, 2: t0 - _W + 1.5, 3: t0, 4: t0 + 1.5}
    client.execute_sql(
        "INSERT INTO ev VALUES " + ", ".join(f"({i}, {_ts(t)}, {i})" for i, t in stamps.items())
    )


def test_rows_leave_and_enter_as_the_clock_moves(own_server):
    with _boot(own_server) as c:
        t0 = time.time()
        _seed(c, t0)
        _settles(c, "recent", _ids(2, 3, 4))
        _settles(c, "due", _ids(1, 2, 3))
        _settles(c, "band", _ids(2, 3))
        # Row 2 expires, row 4 comes due; row 3 stays for seconds yet.
        _settles(c, "recent", _ids(3, 4))
        _settles(c, "due", _ids(1, 2, 3, 4))
        _settles(c, "band", _ids(3, 4))

        # Rows written below the window's edge: a late arrival never shows, and
        # its delete, like an expired row's, leaves the view as it is.
        c.execute_sql(f"INSERT INTO ev VALUES (5, {_ts(t0 - 3 * _W)}, 5)")
        c.execute_sql("DELETE FROM ev WHERE id IN (1, 2)")
        c.execute_sql("DELETE FROM ev WHERE id = 3")
        _settles(c, "recent", _ids(4))
        _settles(c, "due", _ids(4, 5))
        _settles(c, "band", _ids(4))

        # A view created now fills from the table and the clock as they stand,
        # and reads as the one maintained through every step.
        c.execute_sql(f"CREATE VIEW recent_twin AS SELECT id, v FROM ev WHERE ts > NOW() - {_W * 1_000_000}")
        _settles(c, "recent_twin", _ids(4))

        clock = rows(c, "SELECT * FROM _system.clock")
        assert len(clock) == 1 and clock[0]._weight == 1
        assert abs(clock[0].now.replace(tzinfo=timezone.utc).timestamp() - time.time()) < 5


def test_a_view_over_the_clock_is_rebuilt_at_boot(own_server):
    with _boot(own_server) as c:
        t0 = time.time()
        _seed(c, t0)
        _settles(c, "recent", _ids(2, 3, 4))
    own_server.stop_graceful()
    # Booted past row 2's expiry: the rebuilt view reads the clock as it stands.
    time.sleep(2)
    with _boot(own_server) as c:
        _settles(c, "recent", _ids(3, 4))
        _settles(c, "due", _ids(1, 2, 3, 4))
        _settles(c, "band", _ids(3, 4))


def test_a_view_reads_the_clock_at_its_creation_and_boot(own_server):
    # A resolution no test outlasts: only the creation and the boot step it.
    own_server.extra_env = {"GNITZ_CLOCK_RESOLUTION_MS": "600000"}
    own_server.start(workers=MULTI)
    t0 = time.time()
    # 1 expired before it arrives, 2 expiring 1.5 s from now, 3 inside the window.
    stamps = {1: t0 - 2 * _W, 2: t0 - _W + 1.5, 3: t0}
    with gnitz.connect(own_server.target) as c:
        c.execute_sql("CREATE TABLE ev (id BIGINT PRIMARY KEY, ts TIMESTAMP, v BIGINT)")
        c.execute_sql("INSERT INTO ev VALUES " + ", ".join(f"({i}, {_ts(t)}, {i})" for i, t in stamps.items()))
        # The first view over the clock, over rows already there: read once,
        # with no step of the clock but the one its creation made.
        c.execute_sql(f"CREATE VIEW recent AS SELECT id, v FROM ev WHERE ts > NOW() - {_W * 1_000_000}")
        assert bag(rows(c, "SELECT id FROM recent"), "id") == _ids(2, 3)
    own_server.stop_graceful()
    # Booted past row 2's expiry, the rebuilt view read once: no step but the
    # boot's has moved the clock past the time the last process left in it.
    time.sleep(2)
    with gnitz.connect(own_server.start().target) as c:
        assert bag(rows(c, "SELECT id FROM recent"), "id") == _ids(3)
