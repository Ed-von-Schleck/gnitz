import unittest
import os
import shutil
from gnitz.core import types, values as db_values
from gnitz.storage import writer_table, shard_table, errors

class TestShardColumnar(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_shard_col_env"
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir)
        self.layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U64),
            types.ColumnDefinition(types.TYPE_I64),
            types.ColumnDefinition(types.TYPE_STRING)
        ], 0)
        self.fn = os.path.join(self.test_dir, "test.db")

    def tearDown(self):
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)

    def test_columnar_pointer_access(self):
        """Verifies that directory offsets correctly point to columnar regions."""
        writer = writer_table.TableShardWriter(self.layout)
        writer.add_row_from_values(10, 1, [db_values.TaggedValue.make_int(100), db_values.TaggedValue.make_string("alpha")])
        writer.finalize(self.fn)

        view = shard_table.TableShardView(self.fn, self.layout)
        try:
            self.assertEqual(view.get_pk_u64(0), 10)
            self.assertEqual(view.read_field_i64(0, 1), 100)
            self.assertTrue(view.string_field_equals(0, 2, "alpha"))
        finally:
            view.close()

    def test_lazy_checksum_validation(self):
        """Verifies the Ghost Property: Column checksums are only checked on access."""
        long_str = "payload_that_lives_in_blob_region_b"
        writer = writer_table.TableShardWriter(self.layout)
        writer.add_row_from_values(99, 1, [db_values.TaggedValue.make_int(999), db_values.TaggedValue.make_string(long_str)])
        writer.finalize(self.fn)

        view = shard_table.TableShardView(self.fn, self.layout)
        blob_off = view.get_region_offset(3) # Region 3 = Blob Heap
        view.close()

        with open(self.fn, "r+b") as f:
            f.seek(blob_off); f.write(b"\xFF\xFF")

        view = shard_table.TableShardView(self.fn, self.layout, validate_checksums=True)
        # PK/Weight are eagerly validated (and uncorrupted), so this passes:
        self.assertEqual(view.get_pk_u64(0), 99)
        # Accessing the corrupted blob region triggers the error:
        with self.assertRaises(errors.CorruptShardError):
            view.string_field_equals(0, 2, long_str)
        view.close()
