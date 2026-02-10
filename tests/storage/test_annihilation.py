import unittest
from rpython.rtyper.lltypesystem import rffi
from gnitz.storage import memtable
from gnitz.core import types, values as db_values

class TestAnnihilation(unittest.TestCase):
    def test_active_annihilation(self):
        # 1. Setup Table with one I64 column
        layout = types.ComponentLayout([types.TYPE_I64])
        mgr = memtable.MemTableManager(layout, 1024*1024)
        
        # 2. Insert record (weight +1)
        p = [db_values.IntValue(100)]
        mgr.put(1, 1, p)
        
        # 3. Verify something is in the SkipList
        base = mgr.active_table.arena.base_ptr
        head = mgr.active_table.head_off
        has_node = False
        for i in range(16):
            if rffi.cast(rffi.UINTP, rffi.ptradd(base, head + 12 + i*4))[0] != 0:
                has_node = True
        self.assertTrue(has_node, "SkipList should have a node after insert")
        
        # 4. Insert annihilation (weight -1)
        mgr.put(1, -1, p)
        
        # 5. Verify SkipList is empty (Head links point to 0)
        for i in range(16):
            next_ptr = rffi.cast(rffi.UINTP, rffi.ptradd(base, head + 12 + i*4))[0]
            self.assertEqual(next_ptr, 0, "Node should be unlinked after annihilation at level %d" % i)
            
        mgr.close()

if __name__ == '__main__':
    unittest.main()
