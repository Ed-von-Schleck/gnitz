"""A DROP must not race a lagging worker still applying the CREATE of the same
table.
"""

import gnitz
from _uid import uid as _uid


def test_create_then_drop_no_worker_race(race_server):
    """
    Regression: master DROP `remove_dir_all` must not race a lagging worker
    still applying the CREATE of the same table.

    With the debug-only `GNITZ_INJECT_TABLE_CREATE_DELAY_MS` seam active
    (see the `race_server` fixture), each worker sleeps between creating the
    table dir and its own child subdir.  Pre-fix, the master removed the dir
    on DROP while a worker was mid-create → worker ENOENT abort → the next
    request fails with "connection closed".  Post-fix the master defers
    removal to the next checkpoint, so the worker finishes safely.

    Asserts the server survives a handful of immediate CREATE→DROP cycles
    (the follow-up create_schema would raise "connection closed" if a worker
    had aborted).
    """
    client = race_server
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    for i in range(6):
        client.create_table(sn, f"t{i}", cols)
        client.drop_table(sn, f"t{i}")
    # If any worker aborted above, the server is dead and this raises.
    sn2 = "s" + _uid()
    client.create_schema(sn2)
    client.drop_schema(sn2)
    client.drop_schema(sn)
