import unittest
from gnitz.core import types, zset, values as db_values

class TestZSetAlgebra(unittest.TestCase):
    """Tests the in-memory Z-Set multiset logic and DBSP algebraic properties."""
    
    def setUp(self):
        self.schema = types.TableSchema([
            types.ColumnDefinition(types.TYPE_I64), # PK
            types.ColumnDefinition(types.TYPE_STRING),
            types.ColumnDefinition(types.TYPE_F64)
        ], 0)
    
    def _p(self, s, f):
        return [db_values.StringValue(s), db_values.FloatValue(f)]
    
    def test_multiset_coalescing(self):
        zs = zset.ZSet(self.schema)
        p1 = self._p("data", 1.5)
        p2 = self._p("data", 1.5)
        p3 = self._p("other", 2.0)
        
        zs.upsert(1, 1, p1)
        zs.upsert(1, 2, p2) 
        zs.upsert(1, 1, p3) 
        
        self.assertEqual(zs.get_weight(1), 4) 
        
        nonzero = list(zs.iter_nonzero())
        self.assertEqual(len(nonzero), 2)
        
    def test_annihilation_visibility(self):
        zs = zset.ZSet(self.schema)
        p = self._p("ghost", 0.0)
        zs.upsert(1, 1, p)
        zs.upsert(1, -1, p)
        
        self.assertEqual(zs.get_weight(1), 0)
        self.assertEqual(len(list(zs.iter_nonzero())), 0)
        self.assertIsNone(zs.get_payload(1))

    def test_iter_positive(self):
        zs = zset.ZSet(self.schema)
        zs.upsert(1, 5, self._p("pos", 1.0))
        zs.upsert(2, -2, self._p("neg", 2.0))
        
        pos = list(zs.iter_positive())
        self.assertEqual(len(pos), 1)
        self.assertEqual(pos[0][0], 1)
        self.assertEqual(pos[0][1], 5)

if __name__ == '__main__':
    unittest.main()
