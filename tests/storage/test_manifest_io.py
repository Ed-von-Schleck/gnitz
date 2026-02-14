import unittest
import os
from gnitz.storage import manifest

class TestManifestIO(unittest.TestCase):
    def setUp(self):
        self.test_file = "test_manifest_io.db"
    
    def tearDown(self):
        if os.path.exists(self.test_file): os.unlink(self.test_file)
    
    def test_writer_empty_manifest(self):
        writer = manifest.ManifestWriter(self.test_file)
        writer.finalize()
        
        reader = manifest.ManifestReader(self.test_file)
        try:
            self.assertEqual(reader.entry_count, 0)
        finally:
            reader.close()
    
    def test_writer_single_entry(self):
        writer = manifest.ManifestWriter(self.test_file)
        # Using add_entry_obj for robustness test
        e = manifest.ManifestEntry(1, "shard.db", 100, 200, 5, 10)
        writer.add_entry_obj(e)
        writer.finalize()
        
        reader = manifest.ManifestReader(self.test_file)
        try:
            self.assertEqual(reader.entry_count, 1)
            # Use next() on iterator for robustness
            entry = next(reader.iterate_entries())
            self.assertEqual(entry.table_id, 1)
            self.assertEqual(entry.shard_filename, "shard.db")
            self.assertEqual(entry.get_min_key(), 100)
        finally:
            reader.close()

if __name__ == '__main__':
    unittest.main()
