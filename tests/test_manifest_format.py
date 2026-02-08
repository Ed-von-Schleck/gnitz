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
    
    def test_validate_magic_number(self):
        fd = rposix.open(self.test_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            bad_header = '\x00' * manifest.HEADER_SIZE
            os.write(fd, bad_header)
        finally:
            rposix.close(fd)
        
        fd = rposix.open(self.test_file, os.O_RDONLY, 0)
        try:
            with self.assertRaises(errors.CorruptShardError):
                manifest._read_manifest_header(fd)
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
            self.assertEqual(read_entry.component_id, 123)
        finally:
            rposix.close(fd)
    
    def test_entry_with_long_filename(self):
        long_filename = "a" * 200
        entry = manifest.ManifestEntry(1, long_filename, 0, 100, 0, 1)
        
        fd = rposix.open(self.test_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            manifest._write_manifest_entry(fd, entry)
        finally:
            rposix.close(fd)
        
        fd = rposix.open(self.test_file, os.O_RDONLY, 0)
        try:
            read_entry = manifest._read_manifest_entry(fd)
            self.assertEqual(len(read_entry.shard_filename), manifest.FILENAME_MAX_LEN - 1)
        finally:
            rposix.close(fd)
    
    def test_empty_manifest(self):
        fd = rposix.open(self.test_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            manifest._write_manifest_header(fd, 0)
        finally:
            rposix.close(fd)
        
        fd = rposix.open(self.test_file, os.O_RDONLY, 0)
        try:
            version, entry_count, max_lsn = manifest._read_manifest_header(fd)
            self.assertEqual(entry_count, 0)
            read_entry = manifest._read_manifest_entry(fd)
            self.assertIsNone(read_entry)
        finally:
            rposix.close(fd)
    
    def test_round_trip_multiple_entries(self):
        entries = [
            manifest.ManifestEntry(1, "shard_a.db", 0, 100, 0, 5),
            manifest.ManifestEntry(2, "shard_b.db", 101, 200, 5, 10),
            manifest.ManifestEntry(1, "shard_c.db", 201, 300, 10, 15),
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
            # Fixed ValueError: unpack 3 values
            version, entry_count, max_lsn = manifest._read_manifest_header(fd)
            self.assertEqual(entry_count, 3)
            
            read_entries = []
            for i in range(entry_count):
                entry = manifest._read_manifest_entry(fd)
                self.assertIsNotNone(entry)
                read_entries.append(entry)
            
            for i in range(len(entries)):
                self.assertEqual(read_entries[i].component_id, entries[i].component_id)
        finally:
            rposix.close(fd)

if __name__ == '__main__':
    unittest.main()
