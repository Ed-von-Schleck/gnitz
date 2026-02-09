import unittest
import os
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import wal_format, errors
from gnitz.core import types, strings as string_logic

class TestWALFormat(unittest.TestCase):
    def setUp(self):
        self.layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
        self.tmp = "test_wal_format.bin"

    def tearDown(self):
        if os.path.exists(self.tmp): os.unlink(self.tmp)

    def _create_packed(self, value, label):
        stride = self.layout.stride
        buf = lltype.malloc(rffi.CCHARP.TO, stride, flavor='raw')
        try:
            for i in range(stride): buf[i] = '\x00'
            rffi.cast(rffi.LONGLONGP, buf)[0] = rffi.cast(rffi.LONGLONG, value)
            string_logic.pack_string(rffi.ptradd(buf, 8), label, 0)
            return rffi.charpsize2str(buf, stride)
        finally:
            lltype.free(buf, flavor='raw')

    def test_write_and_decode_roundtrip(self):
        recs = [
            (10, 1, self._create_packed(42, "one")),
            (20, -1, self._create_packed(99, "two"))
        ]
        fd = os.open(self.tmp, os.O_WRONLY | os.O_CREAT, 0o644)
        wal_format.write_wal_block(fd, 100, 1, recs, self.layout)
        os.close(fd)
        
        with open(self.tmp, 'rb') as f:
            data = f.read()
            
        lsn, cid, decoded = wal_format.decode_wal_block(data, self.layout)
        self.assertEqual(lsn, 100)
        self.assertEqual(len(decoded), 2)
        self.assertEqual(decoded[0][0], 10)
        self.assertEqual(decoded[1][1], -1)

if __name__ == '__main__':
    unittest.main()
