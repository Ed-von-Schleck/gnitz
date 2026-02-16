# translate_test.py

import sys
import os
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from gnitz.core import zset, types, values as db_values

def mk_payload(s):
    return [db_values.TaggedValue.make_string(s)]

def entry_point(argv):
    print "--- GnitzDB Integrated Lifecycle Test ---"
    
    try:
        db_dir = "translate_test_db"

        if not os.path.exists(db_dir):
            os.mkdir(db_dir)
        else:
            print "[INFO] DB Directory exists. Appending to existing state."

        # Schema: PK(0)=i64, Column(1)=String
        layout = types.TableSchema(
            [types.ColumnDefinition(types.TYPE_I64), types.ColumnDefinition(types.TYPE_STRING)],
            0,
        )

        print "[Step 0] Initializing Engine..."
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
            w_100 = db.get_weight(100, mk_payload("base_state"))
            if w_100 != 3:
                # We use intmask/r_int64 for weights, but print works normally
                print "ERR: Entity 100 weight mismatch. Expected 3, got %d" % int(w_100)
                return 1

            print "  [OK] Integrated lifecycle verified."

        finally:
            print "[Step 4] Closing Database..."
            db.close()

    except Exception as e:
        os.write(2, "FATAL ERROR in entry_point: " + str(e) + "\n")
        return 1

    return 0

def target(driver, args):
    return entry_point, None

if __name__ == "__main__":
    entry_point(sys.argv)
