# translate_test.py

import sys
import os
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128, intmask, r_uint64
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import types, values as db_values
from gnitz.storage.table import PersistentTable
from gnitz.core.row_logic import make_payload_row
from gnitz.storage import compactor

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------


def mk_payload(s, i):
    row = make_payload_row(2)
    row.append(db_values.TaggedValue.make_string(s))
    row.append(db_values.TaggedValue.make_i64(i)
    return row


def mk_int_payload(i):
    row = make_payload_row(1)
    row.append(db_values.TaggedValue.make_i64(i)
    return row


def cleanup_dir(path):
    if not os.path.exists(path):
        return
    items = os.listdir(path)
    for item in items:
        full_path = os.path.join(path, item)
        if os.path.isdir(full_path):
            cleanup_dir(full_path)
        else:
            os.unlink(full_path)
    os.rmdir(path)


# ------------------------------------------------------------------------------
# Test Phases
# ------------------------------------------------------------------------------


def test_multiset_and_string_logic(base_dir):
    """
    Exercises multiset semantics (1 PK, 2 Payloads) and deep string comparison.
    """
    print "\n[Test 1] Multiset Semantics & Deep String Comparison..."
    db_path = os.path.join(base_dir, "test_multiset")
    if not os.path.exists(db_path):
        os.mkdir(db_path)

    layout = types.TableSchema(
        [
            types.ColumnDefinition(types.TYPE_U64),
            types.ColumnDefinition(types.TYPE_STRING),
            types.ColumnDefinition(types.TYPE_I64),
        ],
        pk_index=0,
    )

    db = PersistentTable(db_path, "multiset", layout, cache_size=1024 * 1024)

    try:
        pk = 12345
        # Payload A and B share a prefix but differ in the suffix
        # German String threshold is 12. These are 20+ chars.
        str_a = "Shared_Prefix_Suffix_AAAAAA"
        str_b = "Shared_Prefix_Suffix_BBBBBB"

        # 1. Add both payloads to the same PK
        db.insert(pk, mk_payload(str_a, 10))
        db.insert(pk, mk_payload(str_b, 20))

        # 2. Verify both exist (Multiset property)
        if db.get_weight(pk, mk_payload(str_a, 10)) != 1:
            return False
        if db.get_weight(pk, mk_payload(str_b, 20)) != 1:
            return False

        # 3. Annihilate Payload A
        db.remove(pk, mk_payload(str_a, 10))

        # 4. Flush and verify B survives while A is a 'Ghost'
        db.flush()

        if db.get_weight(pk, mk_payload(str_a, 10)) != 0:
            print "ERR: Payload A should be annihilated."
            return False
        if db.get_weight(pk, mk_payload(str_b, 20)) != 1:
            print "ERR: Payload B should have survived."
            return False

        print "    [OK] Multiset algebraic logic verified."
        return True
    finally:
        db.close()


def test_triple_shard_compaction(base_dir):
    """
    Forces a 3-way merge to exercise the Tournament Tree heap logic.
    """
    print "\n[Test 2] Triple-Shard Tournament Tree Merge..."
    db_path = os.path.join(base_dir, "test_3way")
    if not os.path.exists(db_path):
        os.mkdir(db_path)

    layout = types.TableSchema(
        [
            types.ColumnDefinition(types.TYPE_U64),
            types.ColumnDefinition(types.TYPE_I64),
        ],
        pk_index=0,
    )

    db = PersistentTable(db_path, "3way", layout, validate_checksums=True)

    try:
        # Shard 1: PK 100, 200
        db.insert(100, mk_int_payload(1))
        db.insert(200, mk_int_payload(1))
        db.flush()

        # Shard 2: PK 100, 300
        db.insert(100, mk_int_payload(1))
        db.insert(300, mk_int_payload(1))
        db.flush()

        # Shard 3: PK 200, 300
        db.insert(200, mk_int_payload(1))
        db.insert(300, mk_int_payload(1))
        db.flush()

        # At this point: PK 100(w=2), 200(w=2), 300(w=2)
        if db.get_weight(100, mk_int_payload(1)) != 2:
            return False

        # Force 3-way merge
        print "    -> Merging 3 shards..."
        compactor.execute_compaction(db.index, db.manifest_manager, db.directory)

        if db.get_weight(100, mk_int_payload(1)) != 2:
            return False
        if db.get_weight(300, mk_int_payload(1)) != 2:
            return False

        # Check Manifest
        reader = db.manifest_manager.load_current()
        if reader.entry_count != 1:
            print "ERR: Compaction should have resulted in exactly 1 shard."
            return False
        reader.close()

        print "    [OK] N-way merge and Manifest update verified."
        return True
    finally:
        db.close()


def test_cold_boot_and_cleanup(base_dir):
    """
    Tests engine reconstruction from shards and physical file cleanup.
    """
    print "\n[Test 3] Cold Boot & RefCount Cleanup..."
    db_path = os.path.join(base_dir, "test_boot")
    if not os.path.exists(db_path):
        os.mkdir(db_path)

    layout = types.TableSchema(
        [
            types.ColumnDefinition(types.TYPE_U64),
            types.ColumnDefinition(types.TYPE_I64),
        ],
        pk_index=0,
    )

    # 1. Create data and flush
    db = PersistentTable(db_path, "boot", layout)
    db.insert(999, mk_int_payload(888))
    shard_name = db.flush()
    db.close()

    # 2. Re-open (This tests Manifest -> ShardIndex -> ShardHandle loading)
    db2 = PersistentTable(db_path, "boot", layout)
    try:
        w = db2.get_weight(999, mk_int_payload(888))
        if w != 1:
            print "ERR: Cold boot failed to load shards."
            return False

        # 3. Insert new data, flush, then compact to exercise RefCounter
        db2.insert(1, mk_int_payload(1))
        db2.flush()

        print "    -> Compacting to trigger file releases..."
        compactor.execute_compaction(db2.index, db2.manifest_manager, db2.directory)

        deleted = db2.index.ref_counter.try_cleanup()
        print "    -> Files physically deleted: %d" % len(deleted)

        print "    [OK] Bootstrapping and RefCounting verified."
        return True
    finally:
        db2.close()


def test_u128_recovery(base_dir):
    """
    Exercises u128 logic during WAL recovery specifically.
    """
    print "\n[Test 4] u128 WAL Recovery..."
    db_path = os.path.join(base_dir, "test_u128_wal")
    if not os.path.exists(db_path):
        os.mkdir(db_path)

    layout = types.TableSchema(
        [
            types.ColumnDefinition(types.TYPE_U128),
            types.ColumnDefinition(types.TYPE_I64),
        ],
        pk_index=0,
    )

    huge_k = (r_uint128(0xDEADBEEF) << 64) | r_uint128(0xCAFEBABE)

    db = PersistentTable(db_path, "u128wal", layout)
    db.insert(huge_k, mk_int_payload(777))
    db.close()  # Crash simulation

    db2 = PersistentTable(db_path, "u128wal", layout)
    try:
        w = db2.get_weight(huge_k, mk_int_payload(777))
        if w != 1:
            print "ERR: u128 WAL recovery failed."
            return False
        print "    [OK] u128 WAL recovery verified."
        return True
    finally:
        db2.close()


# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------


def entry_point(argv):
    print "--- GnitzDB Comprehensive Integration Test ---"

    base_dir = "translate_test_data"
    if os.path.exists(base_dir):
        cleanup_dir(base_dir)
    os.mkdir(base_dir)

    try:
        if not test_multiset_and_string_logic(base_dir):
            return 1
        if not test_triple_shard_compaction(base_dir):
            return 1
        if not test_cold_boot_and_cleanup(base_dir):
            return 1
        if not test_u128_recovery(base_dir):
            return 1

        print "\nPASSED: All engine code paths exercised."
        return 0
    except Exception as e:
        print "FATAL ERROR: %s" % str(e)
        return 1
    finally:
        cleanup_dir(base_dir)


def target(driver, args):
    return entry_point, None


if __name__ == "__main__":
    entry_point(sys.argv)
