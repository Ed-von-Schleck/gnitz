import unittest
import os
from rpython.rlib import rposix
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
            manifest._write_manifest_header(fd, 42)
        finally:
            rposix.close(fd)
        
        fd = rposix.open(self.test_file, os.O_RDONLY, 0)
        try:
            version, entry_count, max_lsn = manifest._read_manifest_header(fd)
            self.assertEqual(version, manifest.VERSION)
            self.assertEqual(entry_count, 42)
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
        finally:
            rposix.close(fd)

    def test_round_trip_multiple_entries(self):
        entries = [
            manifest.ManifestEntry(1, "shard_a.db", 0, 100, 0, 5),
            manifest.ManifestEntry(2, "shard_b.db", 101, 200, 5, 10),
        ]
        
        fd = rposix.open(self.test_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            manifest._write_manifest_header(fd, len(entries))
            for entry in entries:
                manifest._write_manifest_entry(fd, entry)
        finally:
            rposix.close(fd)
        
        fd = rposix.open(self.test_file, os.O_RDONLY, 0)
        try:
            version, entry_count, max_lsn = manifest._read_manifest_header(fd)
            read_entries = []
            for i in range(entry_count):
                read_entries.append(manifest._read_manifest_entry(fd))
            
            for i in range(len(entries)):
                self.assertEqual(read_entries[i].table_id, entries[i].table_id)
        finally:
            rposix.close(fd)

if __name__ == '__main__':
    unittest.main()
