import unittest
import os
import shutil
from gnitz.storage import wal, errors
from gnitz.core import types

class TestWALConcurrency(unittest.TestCase):
    def setUp(self):
        self.test_dir = "test_wal_lock_env"
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)
        os.makedirs(self.test_dir)
        self.layout = types.ComponentLayout([types.TYPE_I64])
        self.wal_path = os.path.join(self.test_dir, "locked.wal")

    def tearDown(self):
        if os.path.exists(self.test_dir): shutil.rmtree(self.test_dir)

    def test_single_writer_lock(self):
        """Verifies that two WALWriters cannot open the same file simultaneously."""
        writer1 = wal.WALWriter(self.wal_path, self.layout)
        
        # Second writer should fail to acquire the lock
        with self.assertRaises(errors.StorageError):
            wal.WALWriter(self.wal_path, self.layout)
            
        writer1.close()
        
        # After closing, a new writer should be able to take the lock
        writer2 = wal.WALWriter(self.wal_path, self.layout)
        writer2.close()

if __name__ == '__main__':
    unittest.main()
