import unittest
import os
import shutil
import struct
from gnitz.storage import writer_ecs, shard_ecs, errors, layout
from gnitz.core import types

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
        """
        Verifies that corruption in the Blob region is only detected 
        when the blob data is actually accessed.
        """
        long_string = "this_is_a_very_long_string_that_will_be_in_region_b"
        writer = writer_ecs.ECSShardWriter(self.layout)
        writer.add_entity(1, long_string)
        writer.finalize(self.shard_path)

        # Read the offset of Region B from the header (stored at layout.OFF_REG_B_ECS)
        with open(self.shard_path, "rb") as f:
            f.seek(layout.OFF_REG_B_ECS)
            b_offset = struct.unpack("<Q", f.read(8))[0]

        # Corrupt Region B manually inside the file
        with open(self.shard_path, "r+b") as f:
            f.seek(int(b_offset))
            f.write(b'\xFF\xFF\xFF')

        # Open shard. Header, E, and W regions are still valid.
        view = shard_ecs.ECSShardView(self.shard_path, self.layout)
        
        # Access Entity ID. Should NOT fail.
        eid = view.get_entity_id(0)
        self.assertEqual(eid, 1)

        # Access the long string (triggers lazy Region B validation). NOW it should fail.
        with self.assertRaises(errors.CorruptShardError):
            view.string_field_equals(0, 0, long_string)
            
        view.close()

if __name__ == '__main__':
    unittest.main()
