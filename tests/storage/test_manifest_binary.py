import unittest
import os
from rpython.rlib import rposix
from rpython.rlib.rarithmetic import r_uint64
from gnitz.storage import manifest, errors

class TestManifestBinary(unittest.TestCase):
    def setUp(self):
        self.test_file = "test_manifest_binary.db"
    
    def tearDown(self):
        if os.path.exists(self.test_file):
            os.unlink(self.test_file)
    
    def test_header_serialization(self):
        """Verifies the 64-byte manifest header structure."""
        fd = rposix.open(self.test_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            manifest._write_manifest_header(fd, 42, r_uint64(999))
        finally:
            rposix.close(fd)
        
        fd = rposix.open(self.test_file, os.O_RDONLY, 0)
        try:
            version, entry_count, max_lsn = manifest._read_manifest_header(fd)
            self.assertEqual(version, manifest.VERSION)
            self.assertEqual(entry_count, 42)
            self.assertEqual(max_lsn, 999)
        finally:
            rposix.close(fd)

    def test_entry_serialization(self):
        """Verifies the 184-byte entry structure and PK lo/hi splitting."""
        # Test with a key that exercises both lo and hi 64 bits
        large_key = (r_uint64(0xAAAA) << 64) | r_uint64(0xBBBB)
        entry = manifest.ManifestEntry(123, "shard_001.db", large_key, large_key + 1, 5, 10)
        
        fd = rposix.open(self.test_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            manifest._write_manifest_entry(fd, entry)
        finally:
            rposix.close(fd)
        
        fd = rposix.open(self.test_file, os.O_RDONLY, 0)
        try:
            read_entry = manifest._read_manifest_entry(fd)
            self.assertIsNotNone(read_entry)
            self.assertEqual(read_entry.table_id, 123)
            self.assertEqual(read_entry.get_min_key(), large_key)
            self.assertEqual(read_entry.shard_filename, "shard_001.db")
            self.assertEqual(read_entry.min_lsn, 5)
        finally:
            rposix.close(fd)

    def test_empty_manifest_io(self):
        """Ensures readers handle valid manifests with zero entries."""
        mgr = manifest.ManifestManager(self.test_file)
        mgr.publish_new_version([], r_uint64(0))
        
        reader = manifest.ManifestReader(self.test_file)
        try:
            self.assertEqual(reader.entry_count, 0)
            entries = list(reader.iterate_entries())
            self.assertEqual(len(entries), 0)
        finally:
            reader.close()

if __name__ == '__main__':
    unittest.main()
