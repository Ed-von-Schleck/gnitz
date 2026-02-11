import unittest
import os
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import types
from gnitz.storage import writer_table, shard_table, layout, errors

class TestShardChecksums(unittest.TestCase):
    def setUp(self):
        self.fn = "test_shard_checksums.db"
        self.layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_I64), 
            types.ColumnDefinition(types.TYPE_STRING)
        ], 0)

    def tearDown(self):
        if os.path.exists(self.fn):
            os.unlink(self.fn)

    def test_write_and_validate_checksums(self):
        writer = writer_table.TableShardWriter(self.layout)
        writer.add_row_values(10, 100, "test")
        writer.add_row_values(20, 200, "data")
        writer.finalize(self.fn)
        
        view = shard_table.TableShardView(self.fn, self.layout, validate_checksums=True)
        self.assertEqual(view.count, 2)
        view.close()

    def test_corrupt_region_e_detection(self):
        writer = writer_table.TableShardWriter(self.layout)
        writer.add_row_values(10, 100, "test")
        writer.finalize(self.fn)
        
        view = shard_table.TableShardView(self.fn, self.layout, validate_checksums=False)
        off_e = view.get_region_offset(0) # Region 0 is PK (Entity)
        view.close()
        
        with open(self.fn, 'r+b') as f:
            f.seek(off_e)
            byte_val = ord(f.read(1))
            f.seek(off_e)
            f.write(chr(byte_val ^ 0xFF))
        
        with self.assertRaises(errors.CorruptShardError):
            view = shard_table.TableShardView(self.fn, self.layout, validate_checksums=True)

    def test_corrupt_region_w_detection(self):
        writer = writer_table.TableShardWriter(self.layout)
        writer.add_row_values(10, 100, "test")
        writer.finalize(self.fn)
        
        view = shard_table.TableShardView(self.fn, self.layout, validate_checksums=False)
        off_w = view.get_region_offset(1) # Region 1 is Weights
        view.close()
        
        with open(self.fn, 'r+b') as f:
            f.seek(off_w)
            byte_val = ord(f.read(1))
            f.seek(off_w)
            f.write(chr(byte_val ^ 0xFF))
        
        with self.assertRaises(errors.CorruptShardError):
            view = shard_table.TableShardView(self.fn, self.layout, validate_checksums=True)

    def test_skip_validation_option(self):
        writer = writer_table.TableShardWriter(self.layout)
        writer.add_row_values(10, 100, "test")
        writer.finalize(self.fn)
        
        view = shard_table.TableShardView(self.fn, self.layout, validate_checksums=False)
        off_e = view.get_region_offset(0)
        view.close()
        
        with open(self.fn, 'r+b') as f:
            f.seek(off_e)
            byte_val = ord(f.read(1))
            f.seek(off_e)
            f.write(chr(byte_val ^ 0xFF))
        
        view = shard_table.TableShardView(self.fn, self.layout, validate_checksums=False)
        self.assertEqual(view.count, 1)
        view.close()

if __name__ == '__main__':
    unittest.main()
