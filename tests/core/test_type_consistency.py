import unittest
import os
from rpython.rlib.rarithmetic import r_uint64
from gnitz.storage import manifest, shard_ecs, writer_ecs
from gnitz.core import types

class TestTypeConsistency(unittest.TestCase):
    def setUp(self):
        self.m_fn = "test_type.manifest"
        self.s_fn = "test_type.shard"
        self.layout = types.ComponentLayout([types.TYPE_I64])

    def tearDown(self):
        for f in [self.m_fn, self.s_fn]:
            if os.path.exists(f): os.unlink(f)

    def test_manifest_header_unsigned(self):
        # Explicitly wrap in r_uint64 to ensure unsigned 64-bit behavior in CPython tests
        large_lsn = r_uint64(0xFFFFFFFFFFFFFFFE)
        m_writer = manifest.ManifestWriter(self.m_fn, large_lsn)
        m_writer.finalize()
        m_reader = manifest.ManifestReader(self.m_fn)
        self.assertEqual(m_reader.global_max_lsn, large_lsn)
        m_reader.close()

    def test_shard_header_consistency(self):
        writer = writer_ecs.ECSShardWriter(self.layout)
        writer.add_entity(1, 100)
        writer.finalize(self.s_fn)
        view = shard_ecs.ECSShardView(self.s_fn, self.layout)
        self.assertEqual(view.count, 1)
        # Fixed: use get_pk_u64 instead of get_entity_id
        self.assertEqual(view.get_pk_u64(0), 1)
        view.close()
