"""Strings that repeat read back from shards exactly as they were written.

A shard stores a string column whose values repeat as a dictionary, or as one
cell, over a heap holding each long value once. The store is driven into the
disk regime — a RAM tier of a KiB or so spills, folds and compacts on nearly every
push — and read back three ways: from the running server, after a graceful
restart, where every row comes off a shard, and after an update and a delete
have been folded through a compaction of those shards.

The columns cover every image a string column takes: one value in every row, a
few inline values, a few hundred long ones (a two-byte dictionary), values that
never repeat, and a nullable column of repeats.
"""

import gnitz
from _read import bag, reads_alike, scanned
from _serverproc import disk_usage

_ROWS = 6_000
_COLS = ("id", "constant", "inline", "long", "unique_", "maybe")


def _row(i, tag=""):
    return (
        i,
        "one long value every row of the table holds",
        f"t{i % 7}",
        f"https://example.com/a/path/long/enough/for/the/heap/{i % 300:04d}{tag}",
        f"row {i:06d} holds a value no other row holds",
        None if i % 5 == 0 else f"a repeated nullable value, number {i % 3}",
    )


def _push(conn, rows):
    tid, schema = conn.resolve_table("t")
    conn.push(tid, gnitz.ZSetBatch(schema).extend(dict(zip(_COLS, r)) for r in rows))


def test_repeated_strings_survive_the_shards(sweeping):
    want = {_row(i): 1 for i in range(_ROWS)}
    with gnitz.connect(sweeping.target) as conn:
        conn.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, constant TEXT NOT NULL, inline TEXT NOT NULL, "
            "long TEXT NOT NULL, unique_ TEXT NOT NULL, maybe TEXT)"
        )
        conn.execute_sql("CREATE VIEW v AS SELECT * FROM t WHERE id >= 0")
        for lo in range(0, _ROWS, 500):
            _push(conn, [_row(i) for i in range(lo, lo + 500)])
        assert bag(scanned(conn, "t")) == want
        reads_alike(conn, "v", "t")

    sweeping.restart(graceful=True)
    # The premise: the shards the restart reads carry every image. The report
    # reads the files alone, beside the server now running on them.
    report, stores = disk_usage(sweeping.data_dir)
    user = [s for s in stores if s["store"] == "rows" and s["relation"] >= gnitz.FIRST_USER_TABLE_ID]
    assert user and all(" dict " in s["regions"] for s in user), report
    with gnitz.connect(sweeping.target) as conn:
        assert bag(scanned(conn, "t")) == want, "after the restart"
        reads_alike(conn, "v", "t")

        # An upsert retracts a stored row by its full content, so it must find
        # the stored strings equal to the ones pushed.
        _push(conn, [_row(i, tag="-changed") for i in range(0, _ROWS, 3)])
        conn.execute_sql("DELETE FROM t WHERE id >= 5000")
        want = {_row(i, tag="-changed" if i % 3 == 0 else ""): 1 for i in range(5000)}
        assert bag(scanned(conn, "t")) == want, "after the churn"
        reads_alike(conn, "v", "t")
