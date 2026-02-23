import os
import shutil
import unittest

from rpython.rlib.rarithmetic import r_int64, r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import types
from gnitz.storage import wal_layout
from gnitz.storage.table import PersistentTable
from tests.row_helpers import create_test_row


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _u128_schema():
    """PK=u128, Col=String."""
    return types.TableSchema(
        [
            types.ColumnDefinition(types.TYPE_U128),
            types.ColumnDefinition(types.TYPE_STRING),
        ],
        pk_index=0,
    )


def _u64_schema():
    """PK=u64, Col=String."""
    return types.TableSchema(
        [
            types.ColumnDefinition(types.TYPE_U64),
            types.ColumnDefinition(types.TYPE_STRING),
        ],
        pk_index=0,
    )


def _make_key(hi, lo):
    """Construct a u128 from two 64-bit halves."""
    return (r_uint128(hi) << 64) | r_uint128(lo)


class _TableTestBase(unittest.TestCase):
    """Mixin that manages a temporary directory and PersistentTable lifecycle."""

    _schema_factory = staticmethod(_u128_schema)

    def setUp(self):
        self.test_dir = "test_zset_highlevel_env"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        self.layout = self._schema_factory()
        self.db = PersistentTable(self.test_dir, "test", self.layout)

    def tearDown(self):
        if hasattr(self, "db") and not self.db.is_closed:
            self.db.close()
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def _row(self, s):
        return create_test_row(self.layout, [s])


# ===========================================================================
# Original tests (preserved exactly)
# ===========================================================================


class TestZSetHighLevel(_TableTestBase):
    def test_u128_persistence_and_recovery(self):
        """Exercises the full stack with 128-bit keys and German Strings."""
        key = _make_key(0xAAAA, 0xBBBB)
        long_str = "this_is_a_long_string_that_must_survive_crash_recovery"

        p = self._row(long_str)

        # 1. Ingest and Crash (Close without flush)
        self.db.insert(key, p)
        self.db.close()

        # 2. Recovery - ensure the WAL replay reconstructs state correctly
        self.db = PersistentTable(self.test_dir, "test", self.layout)
        self.assertEqual(self.db.get_weight(key, p), 1)

    def test_cross_shard_summation(self):
        """Verifies Z-Set algebraic identity across persistent layers."""
        p = self._row("common")
        key = r_uint128(10)

        self.db.insert(key, p)
        self.db.flush()

        self.db.insert(key, p)
        self.db.flush()

        self.db.insert(key, p)

        self.assertEqual(self.db.get_weight(key, p), 3)


# ===========================================================================
# DIAGNOSTIC GROUP 1 — WAL round-trip for u128 primary keys
#
# The suspected bug: wal_layout.read_u128 uses  r_uint64(p[1]) << 64
# which is a 64-bit shift on a 64-bit type → always 0, silently dropping
# the high word of the recovered primary key.
# ===========================================================================


class TestWALLayoutU128RoundTrip(unittest.TestCase):
    """
    Low-level tests that exercise wal_layout.write_u128 / read_u128 directly,
    without going through PersistentTable.  If these fail the bug is confirmed
    to live in wal_layout; if they pass the bug is higher up the stack.
    """

    def _buf(self, size=32):
        buf = lltype.malloc(rffi.CCHARP.TO, size, flavor="raw")
        for i in range(size):
            buf[i] = "\x00"
        return buf

    def tearDown(self):
        # buffers freed per-test via try/finally in each test
        pass

    def test_zero_roundtrip(self):
        """u128(0) should survive write→read unchanged."""
        buf = self._buf()
        try:
            wal_layout.write_u128(buf, 0, r_uint128(0))
            result = wal_layout.read_u128(buf, 0)
            self.assertEqual(result, r_uint128(0))
        finally:
            lltype.free(buf, flavor="raw")

    def test_low_word_only_roundtrip(self):
        """A value that fits in 64 bits should survive (smoke test)."""
        val = r_uint128(0xBBBB)
        buf = self._buf()
        try:
            wal_layout.write_u128(buf, 0, val)
            result = wal_layout.read_u128(buf, 0)
            self.assertEqual(result, val)
        finally:
            lltype.free(buf, flavor="raw")

    def test_high_word_only_roundtrip(self):
        """
        A value stored entirely in the high 64 bits must survive.
        If read_u128 does  r_uint64(p[1]) << 64  the shift overflows the
        64-bit type and the result is 0 — this test will catch that.
        """
        val = _make_key(0xAAAA, 0)
        buf = self._buf()
        try:
            wal_layout.write_u128(buf, 0, val)
            result = wal_layout.read_u128(buf, 0)
            self.assertEqual(
                result,
                val,
                "High-word lost: got 0x%x, expected 0x%x" % (result, val),
            )
        finally:
            lltype.free(buf, flavor="raw")

    def test_full_u128_roundtrip(self):
        """Both high and low words must be preserved."""
        val = _make_key(0xAAAABBBBCCCCDDDD, 0x1111222233334444)
        buf = self._buf()
        try:
            wal_layout.write_u128(buf, 0, val)
            result = wal_layout.read_u128(buf, 0)
            self.assertEqual(
                result,
                val,
                "Full u128 lost: got 0x%x, expected 0x%x" % (result, val),
            )
        finally:
            lltype.free(buf, flavor="raw")

    def test_max_u128_roundtrip(self):
        """All-ones u128 — maximum possible value."""
        val = _make_key(0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF)
        buf = self._buf()
        try:
            wal_layout.write_u128(buf, 0, val)
            result = wal_layout.read_u128(buf, 0)
            self.assertEqual(result, val)
        finally:
            lltype.free(buf, flavor="raw")

    def test_high_word_distinguishes_two_keys(self):
        """
        Two keys that differ only in their high word must not be equal after
        the round-trip.  This directly tests the case from
        test_u128_persistence_and_recovery where the key has a non-zero hi.
        """
        val_a = _make_key(0xAAAA, 0xBBBB)
        val_b = _make_key(0x0000, 0xBBBB)  # same low word, different high

        buf = self._buf()
        try:
            wal_layout.write_u128(buf, 0, val_a)
            result = wal_layout.read_u128(buf, 0)
            self.assertNotEqual(
                result,
                val_b,
                "High word collapsed: key with hi=0xAAAA read back as hi=0",
            )
            self.assertEqual(result, val_a)
        finally:
            lltype.free(buf, flavor="raw")


# ===========================================================================
# DIAGNOSTIC GROUP 2 — WAL recovery path, step by step
#
# Isolates exactly where the u128 recovery breaks down.
# ===========================================================================


class TestWALRecoveryU128StepByStep(_TableTestBase):

    # --- 2a. Does the memtable hold the correct weight before any close? ---

    def test_get_weight_before_close_hi_zero(self):
        """
        Baseline: u128 key whose high word is 0 (behaves like u64).
        get_weight must return 1 in the same session.
        """
        key = r_uint128(0xBBBB)  # hi=0, lo=0xBBBB
        p = self._row("baseline")
        self.db.insert(key, p)
        self.assertEqual(self.db.get_weight(key, p), 1)

    def test_get_weight_before_close_hi_nonzero(self):
        """
        u128 key with a non-zero high word.
        If the memtable stores PKs correctly this must return 1 in the same
        session (no WAL involved yet).
        """
        key = _make_key(0xAAAA, 0xBBBB)
        p = self._row("hi_nonzero")
        self.db.insert(key, p)
        self.assertEqual(self.db.get_weight(key, p), 1)

    # --- 2b. Does flushing (no WAL recovery) preserve u128 keys? ---

    def test_get_weight_after_flush_hi_zero(self):
        """After flush, u128 key with hi=0 should still have weight 1."""
        key = r_uint128(0xBBBB)
        p = self._row("flush_baseline")
        self.db.insert(key, p)
        self.db.flush()
        self.assertEqual(self.db.get_weight(key, p), 1)

    def test_get_weight_after_flush_hi_nonzero(self):
        """After flush, u128 key with nonzero hi must survive in the shard."""
        key = _make_key(0xAAAA, 0xBBBB)
        p = self._row("flush_hi_nonzero")
        self.db.insert(key, p)
        self.db.flush()
        self.assertEqual(self.db.get_weight(key, p), 1)

    # --- 2c. Does WAL recovery work for u64-keyed tables? ---

    def test_recovery_u64_schema(self):
        """
        Sanity check: WAL recovery works when the PK is TYPE_U64.
        If this fails the bug is general; if it passes the bug is u128-specific.
        """
        layout_u64 = _u64_schema()
        db = PersistentTable(self.test_dir + "_u64", "test64", layout_u64)
        try:
            key = r_uint128(42)
            p = create_test_row(layout_u64, ["hello"])
            db.insert(key, p)
            db.close()

            db = PersistentTable(self.test_dir + "_u64", "test64", layout_u64)
            self.assertEqual(db.get_weight(key, p), 1)
        finally:
            db.close()
            shutil.rmtree(self.test_dir + "_u64", ignore_errors=True)

    # --- 2d. WAL recovery with a key whose high bits are all zero ---

    def test_recovery_u128_key_hi_zero(self):
        """
        WAL recovery for a u128 key that fits in 64 bits (hi=0).
        If read_u128 drops the high word the result is still correct here
        because the high word is 0 — so this is not a sufficient test alone.
        """
        key = r_uint128(0xBBBB)  # hi=0
        p = self._row("recover_hi_zero")
        self.db.insert(key, p)
        self.db.close()

        self.db = PersistentTable(self.test_dir, "test", self.layout)
        self.assertEqual(self.db.get_weight(key, p), 1)

    # --- 2e. WAL recovery with a key whose high bits are non-zero ---

    def test_recovery_u128_key_hi_nonzero(self):
        """
        WAL recovery for a key that only differs from 0xBBBB in its high word.
        If the high word is dropped on recovery, get_weight will look up
        key=0xBBBB and return 0 (no such key).
        """
        key = _make_key(0xAAAA, 0xBBBB)
        p = self._row("recover_hi_nonzero")
        self.db.insert(key, p)
        self.db.close()

        self.db = PersistentTable(self.test_dir, "test", self.layout)
        self.assertEqual(self.db.get_weight(key, p), 1)

    def test_recovery_does_not_confuse_two_u128_keys(self):
        """
        Two keys that share the same low word but differ in the high word
        must remain distinct after WAL recovery.
        """
        key_a = _make_key(0xAAAA, 0xBBBB)
        key_b = _make_key(0x0000, 0xBBBB)
        p_a = self._row("payload_a")
        p_b = self._row("payload_b")

        self.db.insert(key_a, p_a)
        self.db.insert(key_b, p_b)
        self.db.close()

        self.db = PersistentTable(self.test_dir, "test", self.layout)
        self.assertEqual(self.db.get_weight(key_a, p_a), 1)
        self.assertEqual(self.db.get_weight(key_b, p_b), 1)
        # Cross-check: key_a's row should not be found under key_b
        self.assertEqual(self.db.get_weight(key_b, p_a), 0)

    def test_recovery_weight_accumulation(self):
        """Multiple inserts into the WAL before close must all be recovered."""
        key = _make_key(0xDEAD, 0xBEEF)
        p = self._row("accumulate")
        self.db.insert(key, p)
        self.db.insert(key, p)
        self.db.insert(key, p)
        self.db.close()

        self.db = PersistentTable(self.test_dir, "test", self.layout)
        self.assertEqual(self.db.get_weight(key, p), 3)

    def test_recovery_delete_then_insert(self):
        """A delete followed by an insert should net to weight 0 after recovery."""
        key = _make_key(0xCAFE, 0xBABE)
        p = self._row("del_then_ins")
        self.db.insert(key, p)
        self.db.delete(key, p)
        self.db.close()

        self.db = PersistentTable(self.test_dir, "test", self.layout)
        # Ghost Property: net weight 0 → record should not be present
        self.assertEqual(self.db.get_weight(key, p), 0)

    def test_recovery_long_string_preserved(self):
        """Verifies that long (heap-allocated) German Strings survive WAL recovery."""
        key = _make_key(0xAAAA, 0xBBBB)
        long_str = "this_is_a_long_string_that_must_survive_crash_recovery"
        p = self._row(long_str)
        self.db.insert(key, p)
        self.db.close()

        self.db = PersistentTable(self.test_dir, "test", self.layout)
        self.assertEqual(self.db.get_weight(key, p), 1)


# ===========================================================================
# DIAGNOSTIC GROUP 3 — RefCounter and shard lifecycle
#
# Isolates exactly why the shard survives after the test releases its
# manually-acquired reference.
# ===========================================================================


class TestRefCounterDiagnostics(_TableTestBase):

    # --- 3a. How many references does a fresh shard have? ---

    def test_shard_ref_count_after_flush(self):
        """
        After PersistentTable.flush(), the shard should appear in the
        ref_counter with exactly one reference (held by the index).
        Exposes the number so we know whether cleanup can succeed.
        """
        p = self._row("data")
        key = r_uint128(1)
        self.db.insert(key, p)
        shard1 = self.db.flush()

        # The table's index acquired the file; ref_count should be 1.
        rc = self.db.ref_counter
        self.assertIn(shard1, rc.handles, "shard not tracked by RefCounter")
        handle = rc.handles[shard1]
        self.assertEqual(
            handle.ref_count,
            1,
            "Expected ref_count=1 right after flush, got %d" % handle.ref_count,
        )

    def test_can_delete_returns_false_while_in_index(self):
        """
        can_delete() must return False while the table's index holds the shard.
        """
        p = self._row("data")
        key = r_uint128(2)
        self.db.insert(key, p)
        shard1 = self.db.flush()

        rc = self.db.ref_counter
        self.assertFalse(
            rc.can_delete(shard1),
            "can_delete returned True while shard is still in the index",
        )

    # --- 3b. Does try_cleanup succeed when genuinely no references exist? ---

    def test_cleanup_without_any_references(self):
        """
        If the ref_counter has no entry for a file, try_cleanup must be
        able to delete it (no shared lock held).  Creates a dummy file and
        marks it for deletion without ever acquiring a reference.
        """
        dummy = os.path.join(self.test_dir, "dummy_shard.db")
        with open(dummy, "wb") as f:
            f.write(b"\x00" * 8)

        rc = self.db.ref_counter
        rc.mark_for_deletion(dummy)
        deleted = rc.try_cleanup()

        self.assertIn(dummy, deleted)
        self.assertFalse(os.path.exists(dummy))

    # --- 3c. Does a manual acquire + release sequence affect ref_count? ---

    def test_manual_acquire_increments_ref_count(self):
        """
        Manually calling acquire() on an already-tracked shard must
        increment its ref_count from 1 to 2.
        """
        p = self._row("data")
        self.db.insert(r_uint128(10), p)
        shard1 = self.db.flush()

        rc = self.db.ref_counter
        before = rc.handles[shard1].ref_count  # should be 1
        rc.acquire(shard1)
        after = rc.handles[shard1].ref_count
        self.assertEqual(after, before + 1)
        # Clean up the extra reference so tearDown doesn't error
        rc.release(shard1)

    def test_manual_release_decrements_ref_count(self):
        """
        After acquiring and then releasing, the ref_count must be back
        to 1 (the index's reference remains).
        """
        p = self._row("data")
        self.db.insert(r_uint128(11), p)
        shard1 = self.db.flush()

        rc = self.db.ref_counter
        rc.acquire(shard1)  # ref_count → 2
        rc.release(shard1)  # ref_count → 1
        self.assertEqual(rc.handles[shard1].ref_count, 1)

    # --- 3d. The exact scenario from the failing test ---

    def test_cleanup_fails_while_index_holds_shard(self):
        """
        Even after the manual reference is released, the index still
        holds the shard.  try_cleanup MUST NOT delete it.
        This documents the current (expected) behaviour: the shard is
        protected by the index's own reference.
        """
        p = self._row("protected")
        key = r_uint128(1)
        self.db.insert(key, p)
        shard1 = self.db.flush()

        rc = self.db.ref_counter
        rc.acquire(shard1)        # ref_count → 2
        rc.mark_for_deletion(shard1)
        rc.try_cleanup()          # ref_count still 2 → cannot delete

        self.assertTrue(os.path.exists(shard1), "Shard deleted while still referenced (×2)")

        rc.release(shard1)        # ref_count → 1  (index still holds it)
        rc.try_cleanup()

        # The index's reference is still active, so the shard must survive.
        self.assertTrue(os.path.exists(shard1), "Shard deleted while index still holds a reference")

    def test_cleanup_succeeds_after_table_close(self):
        """
        After the table is fully closed (index releases all handles),
        try_cleanup should be able to physically remove the marked shard.
        Uses a separate RefCounter instance to simulate an external observer.
        """
        p = self._row("closeable")
        key = r_uint128(99)
        self.db.insert(key, p)
        shard1 = self.db.flush()

        # Close the table — this calls index.close_all() which releases
        # the shard from the ref_counter.
        self.db.close()

        # Now try to clean up with a fresh RefCounter (external observer).
        from gnitz.storage import refcount as rc_mod
        external_rc = rc_mod.RefCounter()
        external_rc.mark_for_deletion(shard1)
        deleted = external_rc.try_cleanup()

        self.assertIn(
            shard1,
            deleted,
            "Expected shard to be deleted after table.close() releases all locks",
        )
        self.assertFalse(os.path.exists(shard1))

    # --- 3e. Full SWMR lifecycle: acquire → compaction → release → delete ---

    def test_swmr_refcounting_and_cleanup_original(self):
        """
        Verify that an active external reader protects a shard from physical
        deletion until the reader releases its reference.

        The key insight: flush() only ADDS shards to the index; the index's
        own reference is only released when replace_handles() is called (i.e.
        by the compactor).  Without that call the index permanently holds a
        shared lock and try_cleanup() can never acquire the exclusive lock
        needed to unlink the file.

        Correct lifecycle:
        1. flush() → shard enters index (ref_count=1).
        2. External reader acquire() → ref_count=2.
        3. replace_handles() → index releases its lock (ref_count=1).
        4. mark_for_deletion + try_cleanup → blocked (ref_count=1).
        5. release() → ref_count=0, handle removed.
        6. try_cleanup → exclusive lock acquired, file unlinked.
        """
        from gnitz.storage import index as index_mod

        p = self._row("data")
        key = r_uint128(1)
        self.db.insert(key, p)
        shard1 = self.db.flush()

        # Step 2: external reader acquires the shard (ref_count → 2).
        self.db.ref_counter.acquire(shard1)

        # Step 3: simulate compaction — flush a second shard then call
        # replace_handles so the index drops its reference to shard1.
        self.db.insert(r_uint128(2), p)
        shard2 = self.db.flush()
        last_handle = self.db.index.handles[-1]
        replacement = index_mod.ShardHandle(
            shard2,
            self.layout,
            min_lsn=last_handle.min_lsn,
            max_lsn=last_handle.lsn,
        )
        self.db.index.replace_handles([shard1], replacement)

        # Step 4: mark for deletion; blocked by external reader (ref_count=1).
        self.db.ref_counter.mark_for_deletion(shard1)
        self.db.ref_counter.try_cleanup()
        self.assertTrue(os.path.exists(shard1))

        # Steps 5 & 6: external reader releases; file must now be deleted.
        self.db.ref_counter.release(shard1)
        self.db.ref_counter.try_cleanup()
        self.assertFalse(os.path.exists(shard1))


# ===========================================================================
# DIAGNOSTIC GROUP 4 — u128 key handling in the shard (flush path)
#
# Rules out the shard reader as a source of the u128 key loss.
# ===========================================================================


class TestShardU128KeyHandling(_TableTestBase):

    def test_shard_cursor_reads_correct_u128_key(self):
        """
        After flushing, iterating via a cursor must yield the original u128
        key with both high and low words intact.
        """
        key = _make_key(0xAAAA, 0xBBBB)
        p = self._row("cursor_check")
        self.db.insert(key, p)
        self.db.flush()

        cursor = self.db.create_cursor()
        self.assertTrue(cursor.is_valid(), "Cursor should be valid after flush")
        recovered_key = cursor.key()
        self.assertEqual(
            recovered_key,
            key,
            "Shard cursor key mismatch: got 0x%x, expected 0x%x"
            % (recovered_key, key),
        )

    def test_memtable_cursor_reads_correct_u128_key(self):
        """
        Before flushing, the MemTable cursor must also preserve both words.
        """
        key = _make_key(0xDEAD, 0xBEEF)
        p = self._row("memtable_cursor")
        self.db.insert(key, p)

        cursor = self.db.create_cursor()
        self.assertTrue(cursor.is_valid())
        recovered_key = cursor.key()
        self.assertEqual(
            recovered_key,
            key,
            "MemTable cursor key mismatch: got 0x%x, expected 0x%x"
            % (recovered_key, key),
        )

    def test_shard_binary_search_finds_u128_key(self):
        """
        find_lower_bound must locate the correct row when both high and low
        words of the key are non-zero.
        """
        from gnitz.storage import shard_table

        key = _make_key(0xAAAA, 0xBBBB)
        p = self._row("binary_search")
        self.db.insert(key, p)
        shard_path = self.db.flush()

        view = shard_table.TableShardView(shard_path, self.layout)
        try:
            idx = view.find_lower_bound(key)
            self.assertLess(idx, view.count, "Key not found by binary search")
            found_key = view.get_pk_u128(idx)
            self.assertEqual(
                found_key,
                key,
                "Binary search found wrong key: 0x%x vs 0x%x" % (found_key, key),
            )
        finally:
            view.close()


if __name__ == "__main__":
    unittest.main()
