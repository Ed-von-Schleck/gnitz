import unittest
import os
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import types
from gnitz.storage import writer_ecs, shard_ecs, layout, errors

class TestShardChecksums(unittest.TestCase):
    def setUp(self):
        self.fn = "test_shard_checksums.db"
        self.layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])

    def tearDown(self):
        if os.path.exists(self.fn):
            os.unlink(self.fn)

    def test_write_and_validate_checksums(self):
        writer = writer_ecs.ECSShardWriter(self.layout)
        writer.add_entity(10, 100, "test")
        writer.add_entity(20, 200, "data")
        writer.finalize(self.fn)
        
        view = shard_ecs.ECSShardView(self.fn, self.layout, validate_checksums=True)
        self.assertEqual(view.count, 2)
        view.close()

    def test_checksums_stored_in_header(self):
        writer = writer_ecs.ECSShardWriter(self.layout)
        writer.add_entity(10, 100, "test")
        writer.finalize(self.fn)
        
        with open(self.fn, 'rb') as f:
            header = f.read(64)
        
        checksum_e_bytes = header[16:24]
        checksum_w_bytes = header[24:32]
        
        checksum_e = 0
        for i in range(8):
            checksum_e |= ord(checksum_e_bytes[i]) << (i * 8)
        
        checksum_w = 0
        for i in range(8):
            checksum_w |= ord(checksum_w_bytes[i]) << (i * 8)
        
        self.assertNotEqual(checksum_e, 0)
        self.assertNotEqual(checksum_w, 0)

    def test_corrupt_region_e_detection(self):
        writer = writer_ecs.ECSShardWriter(self.layout)
        writer.add_entity(10, 100, "test")
        writer.finalize(self.fn)
        
        with open(self.fn, 'r+b') as f:
            # Read Region E offset from Header
            f.seek(layout.OFF_REG_E_ECS)
            off_e_bytes = f.read(8)
            off_e = 0
            for i in range(8):
                off_e |= ord(off_e_bytes[i]) << (i * 8)

            # Corrupt first byte of Region E
            f.seek(off_e)
            byte_val = ord(f.read(1))
            f.seek(off_e)
            f.write(chr(byte_val ^ 0xFF))
        
        with self.assertRaises(errors.CorruptShardError):
            view = shard_ecs.ECSShardView(self.fn, self.layout, validate_checksums=True)

    def test_corrupt_region_w_detection(self):
        writer = writer_ecs.ECSShardWriter(self.layout)
        writer.add_entity(10, 100, "test")
        writer.finalize(self.fn)
        
        with open(self.fn, 'r+b') as f:
            # Read Region W offset from Header
            f.seek(layout.OFF_REG_W_ECS)
            off_w_bytes = f.read(8)
            off_w = 0
            for i in range(8):
                off_w |= ord(off_w_bytes[i]) << (i * 8)
            
            # Corrupt first byte of Region W
            f.seek(off_w)
            byte_val = ord(f.read(1))
            f.seek(off_w)
            f.write(chr(byte_val ^ 0xFF))
        
        with self.assertRaises(errors.CorruptShardError):
            view = shard_ecs.ECSShardView(self.fn, self.layout, validate_checksums=True)

    def test_skip_validation_option(self):
        writer = writer_ecs.ECSShardWriter(self.layout)
        writer.add_entity(10, 100, "test")
        writer.finalize(self.fn)
        
        with open(self.fn, 'r+b') as f:
            # Read Region E offset from Header
            f.seek(layout.OFF_REG_E_ECS)
            off_e_bytes = f.read(8)
            off_e = 0
            for i in range(8):
                off_e |= ord(off_e_bytes[i]) << (i * 8)

            f.seek(off_e)
            byte_val = ord(f.read(1))
            f.seek(off_e)
            f.write(chr(byte_val ^ 0xFF))
        
        view = shard_ecs.ECSShardView(self.fn, self.layout, validate_checksums=False)
        self.assertEqual(view.count, 1)
        view.close()

    def test_multiple_entities_checksum(self):
        writer = writer_ecs.ECSShardWriter(self.layout)
        for i in range(100):
            writer.add_entity(i, i * 10, "entity_%d" % i)
        writer.finalize(self.fn)
        
        view = shard_ecs.ECSShardView(self.fn, self.layout, validate_checksums=True)
        self.assertEqual(view.count, 100)
        view.close()

    def test_empty_shard_checksum(self):
        writer = writer_ecs.ECSShardWriter(self.layout)
        writer.finalize(self.fn)
        
        view = shard_ecs.ECSShardView(self.fn, self.layout, validate_checksums=True)
        self.assertEqual(view.count, 0)
        view.close()

if __name__ == '__main__':
    unittest.main()
