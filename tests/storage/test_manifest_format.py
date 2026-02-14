import unittest
import os
from rpython.rlib import rposix
from rpython.rlib.rarithmetic import r_uint64
from gnitz.storage import manifest, errors

class TestManifestFormat(unittest.TestCase):
    def setUp(self):
        self.test_file = "test_manifest.db"
    
    def tearDown(self):
        if os.path.exists(self.test_file):
            os.unlink(self.test_file)
    
    def test_write_read_header(self):
        fd = rposix.open(self.test_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            # FIXED: Added max_lsn argument (100)
            manifest._write_manifest_header(fd, 42, r_uint64(100))
        finally:
            rposix.close(fd)
        
        fd = rposix.open(self.test_file, os.O_RDONLY, 0)
        try:
            version, entry_count, max_lsn = manifest._read_manifest_header(fd)
            self.assertEqual(version, manifest.VERSION)
            self.assertEqual(entry_count, 42)
            self.assertEqual(max_lsn, 100)
        finally:
            rposix.close(fd)
    
    def test_write_read_entry(self):
        entry = manifest.ManifestEntry(123, "test_shard_001.db", 1000, 2000, 5, 10)
        
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
            self.assertEqual(read_entry.get_min_key(), 1000)
            self.assertEqual(read_entry.get_max_key(), 2000)
        finally:
            rposix.close(fd)

    def test_round_trip_multiple_entries(self):
        entries = [
            manifest.ManifestEntry(1, "shard_a.db", 0, 100, 0, 5),
            manifest.ManifestEntry(2, "shard_b.db", 101, 200, 5, 10),
        ]
        
        fd = rposix.open(self.test_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            # FIXED: Added max_lsn argument (500)
            manifest._write_manifest_header(fd, len(entries), r_uint64(500))
            for entry in entries:
                manifest._write_manifest_entry(fd, entry)
        finally:
            rposix.close(fd)
        
        fd = rposix.open(self.test_file, os.O_RDONLY, 0)
        try:
            version, entry_count, max_lsn = manifest._read_manifest_header(fd)
            self.assertEqual(entry_count, 2)
            self.assertEqual(max_lsn, 500)
            
            read_entries = []
            for i in range(entry_count):
                read_entries.append(manifest._read_manifest_entry(fd))
            
            self.assertEqual(read_entries[0].table_id, 1)
            self.assertEqual(read_entries[1].table_id, 2)
            self.assertEqual(read_entries[0].shard_filename, "shard_a.db")
        finally:
            rposix.close(fd)

if __name__ == '__main__':
    unittest.main()
