"""The server refuses a `--workers` outside `1..=MAX_WORKERS` at argv parse time.

The refusal itself is unit-tested (`gnitz-server/src/tests/main.rs`). What only a
spawn can show is that it ends the process as a usage error, where a count that
got past argv would die later in boot with a different exit and no such message.
"""


def test_out_of_range_workers_is_a_usage_error(own_server):
    assert own_server.start_expecting_exit(workers=10_000) == 1
    assert "--workers" in own_server.log_text()
