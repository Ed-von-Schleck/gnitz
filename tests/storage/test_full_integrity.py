import unittest
import os
import shutil
import struct
from gnitz.storage import writer_table, shard_table, errors, layout
from gnitz.core import types, values as db_values

class TestFullIntegrity(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_integrity_env"
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir)
        self.layout = types.ComponentLayout([types.TYPE_STRING])
        self.shard_path = os.path.join(self.test_dir, "corruptible.db")

    def tearDown(self):
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)

    def test_deferred_blob_corruption(self):
        long_string = "this_is_a_very_long_string_that_will_be_in_region_b"
        writer = writer_table.TableShardWriter(self.layout)
        writer.add_row_from_values(1, 1, [db_values.wrap(x) for x in [long_string]])
        writer.finalize(self.shard_path)

        # Open shard to find dynamic region offset
        view = shard_table.TableShardView(self.shard_path, self.layout)
        # Region 0: PK, 1: W, 2: Col1, 3: Blobs
        b_offset = view.get_region_offset(3)
        view.close()

        # Corrupt Region B manually inside the file
        with open(self.shard_path, "r+b") as f:
            f.seek(b_offset)
            f.write(b'\xFF\xFF\xFF')

        # Fixed: Explicitly enable checksums to test integrity validation
        view = shard_table.TableShardView(self.shard_path, self.layout, validate_checksums=True)
        # Fixed: use get_pk_u64 instead of get_primary_key
        self.assertEqual(view.get_pk_u64(0), 1)

        # Triggers lazy Region B validation
        with self.assertRaises(errors.CorruptShardError):
            view.string_field_equals(0, 1, long_string)
            
        view.close()
