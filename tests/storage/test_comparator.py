import os
import unittest
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import types, values, strings
from gnitz.storage import comparator, arena, writer_table, shard_table

class TestComparator(unittest.TestCase):
    def setUp(self):
        self.schema = types.TableSchema([
            types.ColumnDefinition(types.TYPE_I64),    # PK
            types.ColumnDefinition(types.TYPE_STRING), # Payload 1
            types.ColumnDefinition(types.TYPE_F64)     # Payload 2
        ], 0)
        self.arena = arena.Arena(1024)

    def tearDown(self):
        self.arena.free()

    def _pack(self, s_val, f_val):
        ptr = lltype.malloc(rffi.CCHARP.TO, self.schema.stride, flavor='raw')
        # Simple packing for test
        strings.pack_string(rffi.ptradd(ptr, self.schema.get_column_offset(1)), s_val, 0)
        rffi.cast(rffi.DOUBLEP, rffi.ptradd(ptr, self.schema.get_column_offset(2)))[0] = f_val
        return ptr

    def test_equality(self):
        p1 = self._pack("hello", 1.23)
        p2 = self._pack("hello", 1.23)
        
        res = comparator.compare_payloads(self.schema, p1, lltype.nullptr(rffi.CCHARP.TO), 
                                         p2, lltype.nullptr(rffi.CCHARP.TO))
        self.assertEqual(res, 0)
        
        lltype.free(p1, flavor='raw'); lltype.free(p2, flavor='raw')

    def test_inequality_primitive(self):
        p1 = self._pack("hello", 1.23)
        p2 = self._pack("hello", 4.56)
        
        res = comparator.compare_payloads(self.schema, p1, None, p2, None)
        self.assertNotEqual(res, 0)
        
        lltype.free(p1, flavor='raw'); lltype.free(p2, flavor='raw')

    def test_ordering_string(self):
        p1 = self._pack("apple", 1.0)
        p2 = self._pack("banana", 1.0)
        
        res = comparator.compare_payloads(self.schema, p1, None, p2, None)
        self.assertEqual(res, -1) # "apple" < "banana"
        
        lltype.free(p1, flavor='raw'); lltype.free(p2, flavor='raw')
        
    def test_soa_to_soa_equality(self):
        # Create two shards with identical data
        fn1, fn2 = "c1.db", "c2.db"
        for fn in [fn1, fn2]:
            w = writer_table.TableShardWriter(self.schema)
            w._add_row_weighted(10, 1, "test", 1.5)
            w.finalize(fn)
        
        v1 = shard_table.TableShardView(fn1, self.schema)
        v2 = shard_table.TableShardView(fn2, self.schema)
        
        res = comparator.compare_soa_rows(self.schema, v1, 0, v2, 0)
        self.assertEqual(res, 0)
        
        v1.close(); v2.close()
        os.unlink(fn1); os.unlink(fn2)

if __name__ == '__main__':
    unittest.main()
