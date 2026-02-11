import unittest
import os
from gnitz.core import types
from gnitz.storage import writer_table, shard_table, layout, errors
from rpython.rtyper.lltypesystem import rffi, lltype

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
        # For simplicity, we create empty payloads for the string col in this test
        writer.add_row(10, 1, lltype.nullptr(rffi.CCHARP.TO), lltype.nullptr(rffi.CCHARP.TO))
        writer.finalize(self.fn)
        
        view = shard_table.TableShardView(self.fn, self.layout, validate_checksums=True)
        self.assertEqual(view.count, 1)
        view.close()

if __name__ == '__main__':
    unittest.main()
