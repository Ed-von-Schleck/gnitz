import unittest
import os
import shutil
from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128

from gnitz.core import types, values as db_values
from gnitz.storage.table import PersistentTable
from gnitz.storage import errors, shard_table, buffer

class TestZSetPersistence(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_zset_integrated_db"
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)
        
        # Complex Schema: PK(u128), Col1(i8), Col2(String), Col3(f64)
        self.layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U128), 
            types.ColumnDefinition(types.TYPE_I8),
            types.ColumnDefinition(types.TYPE_STRING),
            types.ColumnDefinition(types.TYPE_F64)
        ], 0)
        self.db = PersistentTable(
            self.test_dir, 
            "test", 
            self.layout,
            table_id=1,
            cache_size=1048576,
            read_only=False,
            validate_checksums=False
        )

    def tearDown(self):
        if hasattr(self, 'db') and not self.db.is_closed:
            self.db.close()
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)

    def _p(self, i8, s, f64):
        return [
            db_values.TaggedValue.make_int(i8), 
            db_values.TaggedValue.make_string(s), 
            db_values.TaggedValue.make_float(f64)
        ]

    def test_u128_key_alignment_and_persistence(self):
        """Verify 128-bit key alignment and retrieval through Shards."""
        # Key that exercises high 64 bits and triggers Overflow if not handled correctly
        key = (r_uint128(0xAAAABBBBCCCCDDDD) << 64) | r_uint128(0x1111222233334444)
        payload = self._p(7, "u128_test", 3.14)
        self.db.insert(key, payload)
        self.db.flush()
        self.assertEqual(self.db.get_weight(key, payload), 1)

    def test_empty_flush_logic(self):
        """Verify that a MemTable that sums to zero does not produce a shard."""
        p = self._p(1, "empty", 0.0)
        self.db.insert(999, p)
        self.db.remove(999, p)
        shard_path = self.db.flush()
        self.assertFalse(os.path.exists(shard_path))
        reader = self.db.manifest_manager.load_current()
        self.assertEqual(reader.entry_count, 0)
        reader.close()

    def test_shard_checksum_corruption(self):
        """Verify that region-scoped checksums catch disk corruption."""
        p = self._p(1, "corrupt_me", 1.1)
        self.db.insert(100, p)
        shard_path = self.db.flush()
        self.db.close()
        
        # Locate the Weight Region (index 1) to corrupt it
        view = shard_table.TableShardView(shard_path, self.layout, validate_checksums=False)
        w_offset = view.get_region_offset(1)
        view.close()
        
        with open(shard_path, "r+b") as f:
            f.seek(w_offset)
            f.write(b'\xFF\xFF\xFF\xFF')
            
        # Error is raised on instantiation because PK/Weight regions are validated eagerly
        with self.assertRaises(errors.CorruptShardError):
            self.db = PersistentTable(
                self.test_dir, 
                "test", 
                self.layout, 
                table_id=1,
                cache_size=1048576,
                read_only=False,
                validate_checksums=True
            )

    def test_memory_bounds_safety(self):
        """Verify MappedBuffer prevents out-of-bounds access."""
        from rpython.rtyper.lltypesystem import rffi, lltype
        raw = lltype.malloc(rffi.CCHARP.TO, 10, flavor='raw')
        try:
            for i in range(10): raw[i] = '\x00' # Initialize to avoid UninitializedMemoryAccess
            buf = buffer.MappedBuffer(raw, 10)
            buf.read_u8(9)
            with self.assertRaises(errors.BoundsError):
                buf.read_i64(5) 
        finally:
            lltype.free(raw, flavor='raw')

    def test_deduplication_and_ghost_reclamation(self):
        """Verify string deduplication and that zero-weight blobs are not persisted."""
        long_s = "this_is_a_very_long_string_shared_by_many_rows"
        p = self._p(10, long_s, 1.0)
        for i in range(10): self.db.insert(i, p)
        
        ghost_s = "this_string_should_never_appear_in_the_shard_blob_heap"
        p_ghost = self._p(11, ghost_s, 2.0)
        self.db.insert(100, p_ghost)
        self.db.remove(100, p_ghost)
        
        shard_path = self.db.flush()
        view = shard_table.TableShardView(shard_path, self.layout, validate_checksums=False)
        try:
            self.assertLess(view.blob_buf.size, len(long_s) * 2)
            from rpython.rtyper.lltypesystem import rffi
            found_ghost = False
            for i in range(view.blob_buf.size - len(ghost_s)):
                if rffi.charpsize2str(rffi.ptradd(view.blob_buf.ptr, i), len(ghost_s)) == ghost_s:
                    found_ghost = True
            self.assertFalse(found_ghost)
        finally:
            view.close()

    def test_cross_shard_weight_summation(self):
        """Verify get_weight sums correctly across MemTable and multiple shards."""
        p = self._p(1, "shared", 0.5)
        self.db.insert(50, p)
        self.db.flush()
        self.db.insert(50, p)
        self.db.flush()
        self.db.insert(50, p)
        self.db.insert(50, p)
        self.assertEqual(self.db.get_weight(50, p), 4)

    def test_swmr_refcounting_and_cleanup(self):
        """Verify that active readers protect shards from physical deletion."""
        p = self._p(1, "data", 1.0)
        self.db.insert(1, p)
        shard1 = self.db.flush()
        self.db.ref_counter.acquire(shard1)
        self.db.insert(2, p)
        self.db.flush()
        self.db._trigger_compaction()
        self.assertTrue(os.path.exists(shard1))
        self.db.ref_counter.release(shard1)
        self.db.ref_counter.try_cleanup()
        self.assertFalse(os.path.exists(shard1))

if __name__ == '__main__':
    unittest.main()
