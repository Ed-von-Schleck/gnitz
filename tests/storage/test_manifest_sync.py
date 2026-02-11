import unittest
import os
import shutil
from gnitz.storage import manifest, writer_table
from gnitz.core import types
from rpython.rlib.rarithmetic import r_uint64

class TestManifestSync(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_manifest_sync_env"
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir)
        self.manifest_path = os.path.join(self.test_dir, "sync.manifest")
        self.layout = types.ComponentLayout([types.TYPE_I64])
        self.mgr = manifest.ManifestManager(self.manifest_path)

    def tearDown(self):
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)

    def test_reader_detects_writer_updates(self):
        """
        Simulates two handles to the same manifest (Writer and Reader).
        Verifies the Reader can detect updates via has_changed().
        """
        # 1. Writer publishes version 1
        self.mgr.publish_new_version([], global_max_lsn=r_uint64(100))
        
        # 2. Reader opens manifest
        reader = self.mgr.load_current()
        self.assertEqual(reader.global_max_lsn, 100)
        self.assertFalse(reader.has_changed())
        
        # 3. Writer updates manifest to version 2
        self.mgr.publish_new_version([], global_max_lsn=r_uint64(200))
        
        # 4. Reader should now see it has changed
        self.assertTrue(reader.has_changed())
        
        # 5. Reader reloads
        reader.reload()
        self.assertEqual(reader.global_max_lsn, 200)
        self.assertFalse(reader.has_changed())
        
        reader.close()

if __name__ == '__main__':
    unittest.main()
