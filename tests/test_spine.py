import unittest
import os
from gnitz.storage.writer import ShardWriter
from gnitz.storage.spine import ShardHandle, Spine

class TestSpine(unittest.TestCase):
    def setUp(self):
        self.files = ["s1.db", "s2.db", "s3.db"]
        # Shard 1: a - c
        sw = ShardWriter()
        sw.add_entry("a", "v1", 1); sw.add_entry("c", "v2", 1)
        sw.finalize(self.files[0])
        # Shard 2: e - g
        sw = ShardWriter()
        sw.add_entry("e", "v3", 1); sw.add_entry("g", "v4", 1)
        sw.finalize(self.files[1])
        # Shard 3: x - z
        sw = ShardWriter()
        sw.add_entry("x", "v5", 1); sw.add_entry("z", "v6", 1)
        sw.finalize(self.files[2])

        self.h1 = ShardHandle(self.files[0])
        self.h2 = ShardHandle(self.files[1])
        self.h3 = ShardHandle(self.files[2])
        self.spine = Spine([self.h1, self.h2, self.h3])

    def tearDown(self):
        self.spine.close_all()
        for f in self.files:
            if os.path.exists(f): os.unlink(f)

    def test_binary_search_hits(self):
        # Exact match on min
        self.assertEqual(self.spine.lookup_candidate_index("a"), 0)
        self.assertEqual(self.spine.lookup_candidate_index("e"), 1)
        # Inside range
        self.assertEqual(self.spine.lookup_candidate_index("b"), 0)
        self.assertEqual(self.spine.lookup_candidate_index("f"), 1)
        # Exact match on max
        self.assertEqual(self.spine.lookup_candidate_index("c"), 0)
        self.assertEqual(self.spine.lookup_candidate_index("g"), 1)

    def test_binary_search_misses(self):
        # Before first shard
        self.assertEqual(self.spine.lookup_candidate_index("0"), -1)
        # In the gap between s1 and s2 (d)
        self.assertEqual(self.spine.lookup_candidate_index("d"), -1)
        # After last shard
        self.assertEqual(self.spine.lookup_candidate_index("zzz"), -1)

if __name__ == '__main__':
    unittest.main()
