import unittest
import os
from gnitz.storage import manifest

class TestManifestVersioning(unittest.TestCase):
    def setUp(self):
        self.manifest_path = "test_manifest_versioned.db"
        self.manager = manifest.ManifestManager(self.manifest_path)
    
    def tearDown(self):
        if os.path.exists(self.manifest_path): os.unlink(self.manifest_path)
    
    def test_create_initial_manifest(self):
        entries = [manifest.ManifestEntry(1, "shard_001.db", 0, 100, 0, 5)]
        self.manager.publish_new_version(entries)
        
        reader = self.manager.load_current()
        try:
            self.assertEqual(reader.entry_count, 1)
            entries = list(reader.iterate_entries())
            self.assertEqual(entries[0].shard_filename, "shard_001.db")
        finally:
            reader.close()
    
    def test_atomic_update_add_shard(self):
        # Initial version
        self.manager.publish_new_version([manifest.ManifestEntry(1, "shard_001.db", 0, 100, 0, 5)])
        
        # Load and append
        reader = self.manager.load_current()
        current = list(reader.iterate_entries())
        reader.close()
        
        current.append(manifest.ManifestEntry(1, "shard_002.db", 101, 200, 5, 10))
        self.manager.publish_new_version(current)
        
        # Verify
        reader = self.manager.load_current()
        try:
            self.assertEqual(reader.entry_count, 2)
            filenames = [e.shard_filename for e in reader.iterate_entries()]
            self.assertIn("shard_001.db", filenames)
            self.assertIn("shard_002.db", filenames)
        finally:
            reader.close()

if __name__ == '__main__':
    unittest.main()
