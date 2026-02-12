import sys
import os
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from gnitz.core import zset, types, values as db_values


def mk_payload(s):
    """
    GnitzDB insert logic: payload should exclude the Primary Key.
    Moved to module level to avoid RPython closure errors.
    """
    return [db_values.StringValue(s)]


def entry_point(argv):
    print "--- GnitzDB Integrated Lifecycle Test ---"
    db_dir = "translate_test_db"

    # RPython: os.system is not supported. Simple directory check.
    if not os.path.exists(db_dir):
        os.mkdir(db_dir)

    # Table Schema: [PK(i64), Payload(String)]
    layout = types.TableSchema(
        [types.ColumnDefinition(types.TYPE_I64), types.ColumnDefinition(types.TYPE_STRING)],
        0,
    )

    db = zset.PersistentTable(
        db_dir,
        "test",
        layout,
        table_id=1,
        cache_size=1048576,
        read_only=False,
        validate_checksums=False,
    )

    try:
        print "[Step 1] Creating Shard A..."
        db.insert(100, mk_payload("base_state"))
        db.flush()

        print "[Step 2] Modifying in MemTable..."
        db.insert(100, mk_payload("base_state"))
        db.insert(100, mk_payload("base_state"))

        print "[Step 3] Verifying Algebraic Summation..."
        # 1 (from Shard) + 2 (from MemTable) = 3
        w_100 = db.get_weight(100, mk_payload("base_state"))
        if w_100 != 3:
            print "ERR: Entity 100 weight mismatch. Expected 3, got %d" % w_100
            return 1

        print "  [OK] Integrated lifecycle verified."

    finally:
        db.close()

    return 0


def target(driver, args):
    """
    Required RPython entry point for the translation toolchain.
    """
    return entry_point, None


if __name__ == "__main__":
    entry_point(sys.argv)
