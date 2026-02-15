import unittest
import os
import shutil
from gnitz.storage import manifest, refcount, errors, index, writer_table
from gnitz.core import types
from rpython.rlib.rarithmetic import r_uint64

class TestManifestState(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_manifest_state_env"
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir)
        self.manifest_path = os.path.join(self.test_dir, "sync.manifest")
        self.mgr = manifest.ManifestManager(self.manifest_path)
        self.rc = refcount.RefCounter()

    def tearDown(self):
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)

    def test_swmr_sync_detection(self):
        """Verifies reader detects atomic renames via Inode/MTime changes."""
        self.mgr.publish_new_version([], global_max_lsn=r_uint64(100))
        reader = self.mgr.load_current()
        self.assertEqual(reader.global_max_lsn, 100)
        
        # Simulate background update by another process
        self.mgr.publish_new_version([], global_max_lsn=r_uint64(200))
        
        self.assertTrue(reader.has_changed())
        reader.reload()
        self.assertEqual(reader.global_max_lsn, 200)
        reader.close()

    def test_refcounter_lifecycle(self):
        """Tests acquire, release, and deferred cleanup logic."""
        fn = os.path.join(self.test_dir, "ref_test.db")
        with open(fn, 'w') as f: f.write("data")
        
        self.rc.acquire(fn)
        self.assertFalse(self.rc.can_delete(fn))
        
        self.rc.mark_for_deletion(fn)
        # Cleanup should fail because ref is still held
        self.rc.try_cleanup()
        self.assertTrue(os.path.exists(fn))
        
        self.rc.release(fn)
        self.assertTrue(self.rc.can_delete(fn))
        self.rc.try_cleanup()
        self.assertFalse(os.path.exists(fn))

    def test_deferred_index_cleanup(self):
        """Tests interaction between ShardHandles in an Index and the RefCounter."""
        # Setup dummy shard
        layout = types.TableSchema([types.ColumnDefinition(types.TYPE_I64)], 0)
        fn = os.path.join(self.test_dir, "index_ref.db")
        
        # Create a valid shard file
        w = writer_table.TableShardWriter(layout)
        w.add_row_from_values(1, 1, [])
        w.finalize(fn)

        # Initialize Handle (min_lsn=0, max_lsn=1)
        h = index.ShardHandle(fn, layout, 0, 1)
        
        # Initialize Index and add handle (should auto-acquire ref)
        idx = index.ShardIndex(1, layout, self.rc)
        idx.add_handle(h)
        
        self.assertFalse(self.rc.can_delete(fn))
        self.rc.mark_for_deletion(fn)
        
        # Closing the index must release the handles and allow physical deletion
        idx.close_all()
        self.rc.try_cleanup()
        self.assertFalse(os.path.exists(fn))

if __name__ == '__main__':
    unittest.main()
