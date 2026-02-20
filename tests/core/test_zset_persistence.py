import unittest
import os
import shutil
from rpython.rlib.rarithmetic import r_int64, r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128

from gnitz.core import types, errors
from gnitz.storage.table import PersistentTable
from gnitz.storage import shard_table, buffer
from tests.row_helpers import create_test_row

class TestZSetPersistence(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_zset_integrated_db"
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir)
        
        # Complex Schema: PK(u128) [Idx 0], Col1(i8) [Idx 1], Col2(String) [Idx 2], Col3(f64) [Idx 3]
        self.layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U128), 
            types.ColumnDefinition(types.TYPE_I8),
            types.ColumnDefinition(types.TYPE_STRING),
            types.ColumnDefinition(types.TYPE_F64)
        ], 0)
        
        # Note: PersistentTable must be updated to accept these arguments 
        # and to include the ref_counter attribute.
        self.db = PersistentTable(
            self.test_dir, 
            "test", 
            self.layout,
            table_id=1,
            validate_checksums=False
        )

    def tearDown(self):
        if hasattr(self, 'db') and not self.db.is_closed:
            self.db.close()
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def _p(self, i8, s, f64):
        """Helper to create a PayloadRow matching the test schema."""
        return create_test_row(self.layout, [i8, s, f64])

    def test_u128_key_alignment_and_persistence(self):
        """Verify 128-bit key alignment and retrieval through Shards."""
        # Key that exercises high 64 bits
        key = (r_uint128(0xAAAABBBBCCCCDDDD) << 64) | r_uint128(0x1111222233334444)
        payload = self._p(7, "u128_test", 3.14)
        self.db.insert(key, payload)
        self.db.flush()
        # Requires get_weight to be implemented in PersistentTable
        self.assertEqual(self.db.get_weight(key, payload), 1)

    def test_empty_flush_logic(self):
        """Verify that a MemTable that sums to zero does not produce a shard."""
        p = self._p(1, "empty", 0.0)
        key = r_uint128(999)
        self.db.insert(key, p)
        self.db.delete(key, p)
        shard_path = self.db.flush()
        
        # A shard with count=0 should be created
        if os.path.exists(shard_path):
            view = shard_table.TableShardView(shard_path, self.layout, validate_checksums=False)
            self.assertEqual(view.count, 0)
            view.close()

    def test_shard_checksum_corruption(self):
        """Verify that region-scoped checksums catch disk corruption."""
        p = self._p(1, "corrupt_me", 1.1)
        self.db.insert(r_uint128(100), p)
        shard_path = self.db.flush()
        self.db.close()
        
        # Locate the Weight Region (index 1) to corrupt it
        view = shard_table.TableShardView(shard_path, self.layout, validate_checksums=False)
        w_offset = view.get_region_offset(1)
        view.close()
        
        with open(shard_path, "r+b") as f:
            f.seek(w_offset)
            f.write(b'\xFF\xFF\xFF\xFF')
            
        # Should raise CorruptShardError if validate_checksums is True
        with self.assertRaises(errors.CorruptShardError):
            self.db = PersistentTable(
                self.test_dir, 
                "test", 
                self.layout, 
                table_id=1,
                validate_checksums=True
            )

    def test_memory_bounds_safety(self):
        """Verify MappedBuffer prevents out-of-bounds access."""
        from rpython.rtyper.lltypesystem import rffi, lltype
        size = 10
        raw = lltype.malloc(rffi.CCHARP.TO, size, flavor='raw')
        try:
            for i in range(size): 
                raw[i] = '\x00'
            buf = buffer.MappedBuffer(raw, size)
            buf.read_u8(9)
            with self.assertRaises(errors.BoundsError):
                # Reading 8 bytes at offset 5 (reaches index 12) exceeds size 10
                buf.read_i64(5) 
        finally:
            lltype.free(raw, flavor='raw')

    def test_deduplication_and_ghost_reclamation(self):
        """Verify string deduplication and that zero-weight blobs are not persisted."""
        long_s = "this_is_a_very_long_string_shared_by_many_rows"
        p = self._p(10, long_s, 1.0)
        for i in range(10): 
            self.db.insert(r_uint128(i), p)
        
        ghost_s = "this_string_should_never_appear_in_the_shard_blob_heap"
        p_ghost = self._p(11, ghost_s, 2.0)
        key_ghost = r_uint128(100)
        self.db.insert(key_ghost, p_ghost)
        self.db.delete(key_ghost, p_ghost)
        
        shard_path = self.db.flush()
        view = shard_table.TableShardView(shard_path, self.layout, validate_checksums=False)
        try:
            # Check for deduplication
            self.assertLess(view.blob_buf.size, len(long_s) * 2)
            
            # Check for ghost reclamation
            from rpython.rtyper.lltypesystem import rffi
            found_ghost = False
            for i in range(view.blob_buf.size - len(ghost_s) + 1):
                if rffi.charpsize2str(rffi.ptradd(view.blob_buf.ptr, i), len(ghost_s)) == ghost_s:
                    found_ghost = True
                    break
            self.assertFalse(found_ghost)
        finally:
            view.close()

    def test_cross_shard_weight_summation(self):
        """Verify get_weight sums correctly across MemTable and multiple shards."""
        p = self._p(1, "shared", 0.5)
        key = r_uint128(50)
        self.db.insert(key, p)
        self.db.flush()
        self.db.insert(key, p)
        self.db.flush()
        self.db.insert(key, p)
        self.db.insert(key, p)
        self.assertEqual(self.db.get_weight(key, p), 4)

    def test_swmr_refcounting_and_cleanup(self):
        """Verify that active readers protect shards from physical deletion."""
        p = self._p(1, "data", 1.0)
        key = r_uint128(1)
        self.db.insert(key, p)
        shard1 = self.db.flush()
        
        # Acquire reference
        self.db.ref_counter.acquire(shard1)
        
        self.db.insert(r_uint128(2), p)
        self.db.flush()
        
        # Mark for deletion
        self.db.ref_counter.mark_for_deletion(shard1)
        self.db.ref_counter.try_cleanup()
        
        # Shard must remain due to active reference
        self.assertTrue(os.path.exists(shard1))
        
        self.db.ref_counter.release(shard1)
        self.db.ref_counter.try_cleanup()
        
        # Shard should be physically deleted now
        self.assertFalse(os.path.exists(shard1))

if __name__ == '__main__':
    unittest.main()
