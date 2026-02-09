import unittest
import os
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import memtable, spine, engine, writer_ecs
from gnitz.core import types, values as db_values

class TestEngineSummation(unittest.TestCase):
    def setUp(self):
        self.layout = types.ComponentLayout([types.TYPE_I64])
        self.files = []

    def tearDown(self):
        for f in self.files:
            if os.path.exists(f): os.unlink(f)

    def test_raw_weight_summation(self):
        fn1 = "test_sum_1.db"
        w1 = writer_ecs.ECSShardWriter(self.layout)
        w1._add_entity_weighted(1, 1, 100)
        w1.finalize(fn1)
        self.files.append(fn1)
        
        fn2 = "test_sum_2.db"
        w2 = writer_ecs.ECSShardWriter(self.layout)
        w2._add_entity_weighted(1, 1, 100)
        w2.finalize(fn2)
        self.files.append(fn2)
        
        mgr = memtable.MemTableManager(self.layout, 1024)
        h1 = spine.ShardHandle(fn1, self.layout, 1)
        h2 = spine.ShardHandle(fn2, self.layout, 2)
        sp = spine.Spine([h1, h2])
        db = engine.Engine(mgr, sp)
        
        scratch = lltype.malloc(rffi.CCHARP.TO, self.layout.stride, flavor='raw')
        try:
            for i in range(self.layout.stride): scratch[i] = '\x00'
            rffi.cast(rffi.LONGLONGP, scratch)[0] = 100
            self.assertEqual(db.get_effective_weight_raw(1, scratch, lltype.nullptr(rffi.CCHARP.TO)), 2)
            db.mem_manager.put(1, -1, [db_values.IntValue(100)])
            self.assertEqual(db.get_effective_weight_raw(1, scratch, lltype.nullptr(rffi.CCHARP.TO)), 1)
        finally:
            lltype.free(scratch, flavor='raw')
        db.close()

if __name__ == '__main__':
    unittest.main()
