# tests/storage/test_shard_registry.py
import unittest
import os
from gnitz.core import types, errors
from gnitz.storage import writer_table, shard_table
from tests.row_helpers import create_test_row

class TestShardChecksums(unittest.TestCase):
    def setUp(self):
        self.fn = "test_shard_checksums.db"
        # Schema: PK (0) -> U64, String (1) -> TYPE_STRING
        self.layout = types.TableSchema([
            types.ColumnDefinition(types.TYPE_U64),
            types.ColumnDefinition(types.TYPE_STRING)
        ], 0)

    def tearDown(self):
        if os.path.exists(self.fn):
            os.unlink(self.fn)

    def test_write_and_validate_checksums(self):
        writer = writer_table.TableShardWriter(self.layout)
        # Use create_test_row to wrap the payload values in a PayloadRow
        writer.add_row_from_values(10, 1, create_test_row(self.layout, ["test"]))
        writer.add_row_from_values(20, 1, create_test_row(self.layout, ["data"]))
        writer.finalize(self.fn)
        
        view = shard_table.TableShardView(self.fn, self.layout, validate_checksums=True)
        self.assertEqual(view.count, 2)
        view.close()

    def test_corrupt_region_e_detection(self):
        writer = writer_table.TableShardWriter(self.layout)
        writer.add_row_from_values(10, 1, create_test_row(self.layout, ["test"]))
        writer.finalize(self.fn)
        
        view = shard_table.TableShardView(self.fn, self.layout, validate_checksums=False)
        off_e = view.get_region_offset(0)
        view.close()
        
        # Manually corrupt the PK region (Region 0)
        with open(self.fn, 'r+b') as f:
            f.seek(off_e)
            val = ord(f.read(1))
            f.seek(off_e)
            f.write(chr(val ^ 0xFF))
            
        with self.assertRaises(errors.CorruptShardError):
            shard_table.TableShardView(self.fn, self.layout, validate_checksums=True)

    def test_corrupt_region_w_detection(self):
        writer = writer_table.TableShardWriter(self.layout)
        writer.add_row_from_values(10, 1, create_test_row(self.layout, ["test"]))
        writer.finalize(self.fn)
        
        view = shard_table.TableShardView(self.fn, self.layout, validate_checksums=False)
        off_w = view.get_region_offset(1)
        view.close()
        
        # Manually corrupt the Weight region (Region 1)
        with open(self.fn, 'r+b') as f:
            f.seek(off_w)
            val = ord(f.read(1))
            f.seek(off_w)
            f.write(chr(val ^ 0xFF))
            
        with self.assertRaises(errors.CorruptShardError):
            shard_table.TableShardView(self.fn, self.layout, validate_checksums=True)

    def test_skip_validation_option(self):
        writer = writer_table.TableShardWriter(self.layout)
        writer.add_row_from_values(10, 1, create_test_row(self.layout, ["test"]))
        writer.finalize(self.fn)
        
        view = shard_table.TableShardView(self.fn, self.layout, validate_checksums=False)
        off_e = view.get_region_offset(0)
        view.close()
        
        with open(self.fn, 'r+b') as f:
            f.seek(off_e)
            val = ord(f.read(1))
            f.seek(off_e)
            f.write(chr(val ^ 0xFF))
            
        # Shard should open successfully if validate_checksums is False
        view = shard_table.TableShardView(self.fn, self.layout, validate_checksums=False)
        self.assertEqual(view.count, 1)
        view.close()
