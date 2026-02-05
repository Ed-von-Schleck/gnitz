import unittest
from gnitz.storage.writer import ShardWriter, encode_varint

class TestWriter(unittest.TestCase):
    def test_varint_encoding(self):
        self.assertEqual("".join(encode_varint(127)), "\x7f")
        self.assertEqual("".join(encode_varint(128)), "\x80\x01")
        self.assertEqual("".join(encode_varint(0)), "\x00")

    def test_alignment_logic(self):
        sw = ShardWriter()
        self.assertEqual(sw._align(64), 0)
        self.assertEqual(sw._align(65), 63)
        self.assertEqual(sw._align(127), 1)

    def test_entry_accumulation(self):
        sw = ShardWriter()
        sw.add_entry("k", "v", 1)
        self.assertEqual(sw.count, 1)
        self.assertEqual(len(sw.weights), 1)

if __name__ == '__main__':
    unittest.main()
