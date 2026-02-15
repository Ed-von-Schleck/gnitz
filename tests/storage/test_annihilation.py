import unittest
from gnitz.storage import memtable, memtable_node
from gnitz.core import types, values as db_values

class TestAnnihilation(unittest.TestCase):
    def test_active_annihilation(self):
        layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U64), # PK
            types.ColumnDefinition(types.TYPE_I64)  # Val
        ], 0)
        
        table = memtable.MemTable(layout, 1024*1024)
        base = table.arena.base_ptr
        head = table.head_off
        
        p = [db_values.IntValue(100)]
        key = 1
        
        table.upsert(key, 1, p)
        
        def is_skip_list_empty():
            for i in range(16): # MAX_HEIGHT
                if memtable_node.node_get_next_off(base, head, i) != 0:
                    return False
            return True

        self.assertFalse(is_skip_list_empty())
        
        # Annihilate
        table.upsert(key, -1, p)
        self.assertTrue(is_skip_list_empty())
        table.free()
