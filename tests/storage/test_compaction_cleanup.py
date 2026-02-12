import unittest
import os
from gnitz.storage import refcount, compactor, spine, writer_table
from gnitz.core import types
from rpython.rtyper.lltypesystem import rffi, lltype

class TestCompactionCleanup(unittest.TestCase):
    def setUp(self):
        self.layout = types.TableSchema([types.ColumnDefinition(types.TYPE_I64)], 0)
        self.rc = refcount.RefCounter()
        self.files_to_clean = []
        self.shard1 = "cleanup_test_s1.db"
        self.shard2 = "cleanup_test_s2.db"
        self._create_shard(self.shard1, [10], [1])
        self._create_shard(self.shard2, [10], [1])
        self.files_to_clean.extend([self.shard1, self.shard2])

    def tearDown(self):
        if hasattr(self, 'spine_obj'):
            self.spine_obj.close_all()
        for f in self.files_to_clean:
            if os.path.exists(f): os.unlink(f)

    def _create_shard(self, filename, entities, weights):
        w = writer_table.TableShardWriter(self.layout)
        for e, wt in zip(entities, weights):
            w.add_row(e, wt, lltype.nullptr(rffi.CCHARP.TO), lltype.nullptr(rffi.CCHARP.TO))
        w.finalize(filename)

    def test_deferred_cleanup(self):
        h1 = spine.ShardHandle(self.shard1, self.layout, 0, validate_checksums=False)
        h2 = spine.ShardHandle(self.shard2, self.layout, 0, validate_checksums=False)
        self.spine_obj = spine.Spine([h1, h2], self.rc)
        
        self.assertFalse(self.rc.can_delete(self.shard1))
        self.rc.mark_for_deletion(self.shard1)
        self.spine_obj.close_all()
        self.assertFalse(os.path.exists(self.shard1))

if __name__ == '__main__':
    unittest.main()
