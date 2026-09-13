"""The server refuses a `--workers` outside `1..=MAX_WORKERS` at argv parse time.

`parse_workers` itself is unit-tested over the whole valid range and every
rejected spelling (`gnitz-server/src/tests/main.rs`). What only a spawn can show
is that its `Err` is wired to an exit before the socket is ever bound.
"""


def test_out_of_range_workers_exits_before_binding(own_server):
    assert own_server.start_expecting_exit(workers=10_000) != 0
