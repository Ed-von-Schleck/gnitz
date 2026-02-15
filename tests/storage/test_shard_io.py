import unittest
import os
import shutil
from gnitz.core import types, values as db_values
from gnitz.storage import writer_table, shard_table, errors

class TestShardIO(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_shard_io_env"
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir)
        # Schema: PK(0), I64(1), String(2)
        self.layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U64),
            types.ColumnDefinition(types.TYPE_I64),
            types.ColumnDefinition(types.TYPE_STRING)
        ], 0)
        self.fn = os.path.join(self.test_dir, "test.db")

    def tearDown(self):
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)

    def test_n_partition_roundtrip(self):
        """Verifies writing and reading all columnar regions."""
        writer = writer_table.TableShardWriter(self.layout)
        writer.add_row_from_values(10, 1, [db_values.wrap(100), db_values.wrap("alpha")])
        writer.add_row_from_values(20, -1, [db_values.wrap(200), db_values.wrap("beta")])
        writer.finalize(self.fn)

        view = shard_table.TableShardView(self.fn, self.layout)
        try:
            self.assertEqual(view.count, 2)
            # Verify PKs
            self.assertEqual(view.get_pk_u64(0), 10)
            self.assertEqual(view.get_pk_u64(1), 20)
            # Verify Weights
            self.assertEqual(view.get_weight(0), 1)
            self.assertEqual(view.get_weight(1), -1)
            # Verify I64 Column (Index 1)
            self.assertEqual(view.read_field_i64(0, 1), 100)
            self.assertEqual(view.read_field_i64(1, 1), 200)
            # Verify String Column (Index 2)
            self.assertTrue(view.string_field_equals(0, 2, "alpha"))
            self.assertTrue(view.string_field_equals(1, 2, "beta"))
        finally:
            view.close()

    def test_atomic_swap(self):
        """Verifies that finalizing a shard safely overwrites existing files."""
        writer1 = writer_table.TableShardWriter(self.layout)
        writer1.add_row_from_values(1, 1, [db_values.wrap(111), db_values.wrap("one")])
        writer1.finalize(self.fn)

        writer2 = writer_table.TableShardWriter(self.layout)
        writer2.add_row_from_values(2, 1, [db_values.wrap(222), db_values.wrap("two")])
        writer2.finalize(self.fn)

        view = shard_table.TableShardView(self.fn, self.layout)
        self.assertEqual(view.get_pk_u64(0), 2)
        view.close()

    def test_integrity_and_corruption(self):
        """Verifies that checksums catch Region B (blob) corruption lazily."""
        long_str = "this_is_a_long_string_to_force_blob_region_usage"
        writer = writer_table.TableShardWriter(self.layout)
        writer.add_row_from_values(99, 1, [db_values.wrap(999), db_values.wrap(long_str)])
        writer.finalize(self.fn)

        # Open to find blob region offset
        view = shard_table.TableShardView(self.fn, self.layout)
        # PK=0, W=1, Col1=2, Blobs=3 (Blob is last region)
        blob_offset = view.get_region_offset(3)
        view.close()

        # Corrupt the blob region
        with open(self.fn, "r+b") as f:
            f.seek(blob_offset)
            f.write(b"\xFF\xFF\xFF")

        # Open with checksum validation enabled
        view = shard_table.TableShardView(self.fn, self.layout, validate_checksums=True)
        # Reading PK/Weight should work (eagerly validated, but not corrupted)
        self.assertEqual(view.get_pk_u64(0), 99)
        
        # Accessing the string should trigger CorruptShardError lazily
        with self.assertRaises(errors.CorruptShardError):
            view.string_field_equals(0, 2, long_str)
        view.close()

if __name__ == '__main__':
    unittest.main()
