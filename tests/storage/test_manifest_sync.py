import unittest
import os
import shutil
from gnitz.storage import manifest
from rpython.rlib.rarithmetic import r_uint64

class TestManifestSync(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_manifest_sync_env"
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir)
        self.manifest_path = os.path.join(self.test_dir, "sync.manifest")
        self.mgr = manifest.ManifestManager(self.manifest_path)

    def tearDown(self):
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)

    def test_reader_detects_writer_updates(self):
        # 1. Writer publishes version 1
        self.mgr.publish_new_version([], global_max_lsn=r_uint64(100))
        
        # 2. Reader opens manifest
        reader = self.mgr.load_current()
        self.assertEqual(reader.global_max_lsn, 100)
        self.assertFalse(reader.has_changed(), "Should not report change on fresh open")
        
        # 3. Writer updates manifest to version 2 (Atomic rename changes Inode)
        self.mgr.publish_new_version([], global_max_lsn=r_uint64(200))
        
        # 4. Reader should now detect change
        self.assertTrue(reader.has_changed(), "Reader failed to detect atomic rename")
        
        # 5. Reader reloads state
        reader.reload()
        self.assertEqual(reader.global_max_lsn, 200)
        self.assertFalse(reader.has_changed(), "Should be stable after reload")
        
        reader.close()

if __name__ == '__main__':
    unittest.main()
