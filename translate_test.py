import sys
import os
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from gnitz.core import zset, types, values as db_values

def mk_payload(s):
    return [db_values.StringValue(s)]

def entry_point(argv):
    print "--- GnitzDB Integrated Lifecycle Test ---"
    
    # 1. Wrap the ENTIRE logic in a try/except block to catch RPython exceptions
    try:
        db_dir = "translate_test_db"

        if not os.path.exists(db_dir):
            os.mkdir(db_dir)
        else:
            # OPTIONAL: For testing, you might want to clean the directory 
            # to ensure a fresh start and avoid recovery errors.
            print "[INFO] DB Directory exists. Appending to existing state."

        layout = types.TableSchema(
            [types.ColumnDefinition(types.TYPE_I64), types.ColumnDefinition(types.TYPE_STRING)],
            0,
        )

        # 2. Move PersistentTable instantiation INSIDE the try block
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
                print "ERR: Entity 100 weight mismatch. Expected 3, got %d" % w_100
                return 1

            print "  [OK] Integrated lifecycle verified."

        finally:
            print "[Step 4] Closing Database..."
            db.close()

    except Exception as e:
        # 3. Catch, Print, and Return Error Code
        # We write to stderr (file descriptor 2) to ensure visibility
        os.write(2, "FATAL ERROR in entry_point: " + str(e) + "\n")
        return 1

    return 0

def target(driver, args):
    return entry_point, None

if __name__ == "__main__":
    entry_point(sys.argv)
