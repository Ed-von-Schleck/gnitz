import unittest
import os
from gnitz.storage import manifest, errors

class TestManifestIO(unittest.TestCase):
    def setUp(self):
        self.test_file = "test_manifest_io.db"
    
    def tearDown(self):
        if os.path.exists(self.test_file):
            os.unlink(self.test_file)
    
    def test_writer_empty_manifest(self):
        writer = manifest.ManifestWriter(self.test_file)
        writer.finalize()
        
        reader = manifest.ManifestReader(self.test_file)
        try:
            self.assertEqual(reader.get_entry_count(), 0)
        finally:
            reader.close()
    
    def test_writer_single_entry(self):
        writer = manifest.ManifestWriter(self.test_file)
        writer.add_entry_obj(manifest.ManifestEntry(1, "shard.db", 100, 200, 5, 10))
        writer.finalize()
        
        reader = manifest.ManifestReader(self.test_file)
        try:
            self.assertEqual(reader.get_entry_count(), 1)
            entry = list(reader.iterate_entries())[0]
            self.assertEqual(entry.table_id, 1)
            self.assertEqual(entry.shard_filename, "shard.db")
        finally:
            reader.close()

if __name__ == '__main__':
    unittest.main()
