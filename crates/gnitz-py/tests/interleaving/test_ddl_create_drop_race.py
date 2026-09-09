"""A DROP must not race a lagging worker still applying the CREATE of the same
table.
"""

import gnitz
from _uid import uid as _uid


def test_create_then_drop_no_worker_race(race_server):
    """The master defers a dropped table's `remove_dir_all` to the next
    checkpoint, so a worker still inside the CREATE finishes against a directory
    that still exists instead of aborting on ENOENT.

    The `race_server` seam (`GNITZ_INJECT_TABLE_CREATE_DELAY_MS`) makes every
    worker sleep between creating the table directory and its partition
    subdirectories, so each CREATE→DROP cycle reaches the window by ordering
    rather than by timing. A worker that aborted would take the server with it,
    which the follow-up DDL is what detects — it would raise "connection closed".
    """
    client = race_server
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    for i in range(3):
        client.create_table(sn, f"t{i}", cols)
        client.drop_table(sn, f"t{i}")
    client.create_schema("s" + _uid())
