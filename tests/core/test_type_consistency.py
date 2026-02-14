import unittest
import os
from rpython.rlib.rarithmetic import r_uint64
from gnitz.storage import manifest, shard_table, writer_table
from gnitz.core import types, values as db_values

class TestTypeConsistency(unittest.TestCase):
    def setUp(self):
        self.m_fn, self.s_fn = "test_type.manifest", "test_type.shard"
        self.layout = types.ComponentLayout([types.TYPE_I64])

    def tearDown(self):
        for f in [self.m_fn, self.s_fn]:
            if os.path.exists(f): os.unlink(f)

    def test_manifest_header_unsigned(self):
        large_lsn = r_uint64(0xFFFFFFFFFFFFFFFE)
        # atomic write via convenience writer
        writer = manifest.ManifestWriter(self.m_fn, large_lsn)
        writer.finalize()
        
        reader = manifest.ManifestReader(self.m_fn)
        try:
            self.assertEqual(reader.global_max_lsn, large_lsn)
        finally:
            reader.close()

if __name__ == '__main__':
    unittest.main()
