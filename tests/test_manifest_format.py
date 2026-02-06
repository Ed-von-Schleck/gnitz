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
        """Test writing and reading manifest header."""
        # Write header
        fd = rposix.open(self.test_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            manifest._write_manifest_header(fd, 42)
        finally:
            rposix.close(fd)
        
        # Read header back
        fd = rposix.open(self.test_file, os.O_RDONLY, 0)
        try:
            version, entry_count = manifest._read_manifest_header(fd)
            self.assertEqual(version, manifest.VERSION)
            self.assertEqual(entry_count, 42)
        finally:
            rposix.close(fd)
    
    def test_validate_magic_number(self):
        """Test that invalid magic number raises error."""
        # Write invalid header
        fd = rposix.open(self.test_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            # Write wrong magic number
            bad_header = '\x00' * manifest.HEADER_SIZE
            os.write(fd, bad_header)
        finally:
            rposix.close(fd)
        
        # Try to read - should fail
        fd = rposix.open(self.test_file, os.O_RDONLY, 0)
        try:
            with self.assertRaises(errors.CorruptShardError):
                manifest._read_manifest_header(fd)
        finally:
            rposix.close(fd)
    
    def test_write_read_entry(self):
        """Test writing and reading a single manifest entry."""
        entry = manifest.ManifestEntry(
            component_id=123,
            shard_filename="test_shard_001.db",
            min_eid=1000,
            max_eid=2000,
            min_lsn=5,
            max_lsn=10
        )
        
        # Write entry
        fd = rposix.open(self.test_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            manifest._write_manifest_entry(fd, entry)
        finally:
            rposix.close(fd)
        
        # Read entry back
        fd = rposix.open(self.test_file, os.O_RDONLY, 0)
        try:
            read_entry = manifest._read_manifest_entry(fd)
            self.assertIsNotNone(read_entry)
            self.assertEqual(read_entry.component_id, 123)
            self.assertEqual(read_entry.shard_filename, "test_shard_001.db")
            self.assertEqual(read_entry.min_entity_id, 1000)
            self.assertEqual(read_entry.max_entity_id, 2000)
            self.assertEqual(read_entry.min_lsn, 5)
            self.assertEqual(read_entry.max_lsn, 10)
        finally:
            rposix.close(fd)
    
    def test_entry_with_long_filename(self):
        """Test that long filenames are truncated properly."""
        long_filename = "a" * 200  # Longer than FILENAME_MAX_LEN
        entry = manifest.ManifestEntry(
            component_id=1,
            shard_filename=long_filename,
            min_eid=0,
            max_eid=100,
            min_lsn=0,
            max_lsn=1
        )
        
        # Write and read back
        fd = rposix.open(self.test_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            manifest._write_manifest_entry(fd, entry)
        finally:
            rposix.close(fd)
        
        fd = rposix.open(self.test_file, os.O_RDONLY, 0)
        try:
            read_entry = manifest._read_manifest_entry(fd)
            # Should be truncated to FILENAME_MAX_LEN - 1
            self.assertEqual(len(read_entry.shard_filename), manifest.FILENAME_MAX_LEN - 1)
            self.assertEqual(read_entry.shard_filename, "a" * (manifest.FILENAME_MAX_LEN - 1))
        finally:
            rposix.close(fd)
    
    def test_empty_manifest(self):
        """Test creating and reading an empty manifest (zero entries)."""
        # Write header with 0 entries
        fd = rposix.open(self.test_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            manifest._write_manifest_header(fd, 0)
        finally:
            rposix.close(fd)
        
        # Read back
        fd = rposix.open(self.test_file, os.O_RDONLY, 0)
        try:
            version, entry_count = manifest._read_manifest_header(fd)
            self.assertEqual(entry_count, 0)
            
            # Try to read entry - should get None (EOF)
            read_entry = manifest._read_manifest_entry(fd)
            self.assertIsNone(read_entry)
        finally:
            rposix.close(fd)
    
    def test_round_trip_multiple_entries(self):
        """Test writing and reading multiple entries."""
        entries = [
            manifest.ManifestEntry(1, "shard_a.db", 0, 100, 0, 5),
            manifest.ManifestEntry(2, "shard_b.db", 101, 200, 5, 10),
            manifest.ManifestEntry(1, "shard_c.db", 201, 300, 10, 15),
        ]
        
        # Write header and entries
        fd = rposix.open(self.test_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            manifest._write_manifest_header(fd, len(entries))
            for entry in entries:
                manifest._write_manifest_entry(fd, entry)
        finally:
            rposix.close(fd)
        
        # Read back
        fd = rposix.open(self.test_file, os.O_RDONLY, 0)
        try:
            version, entry_count = manifest._read_manifest_header(fd)
            self.assertEqual(entry_count, 3)
            
            # Read all entries
            read_entries = []
            for i in range(entry_count):
                entry = manifest._read_manifest_entry(fd)
                self.assertIsNotNone(entry)
                read_entries.append(entry)
            
            # Verify entries match
            for i in range(len(entries)):
                self.assertEqual(read_entries[i].component_id, entries[i].component_id)
                self.assertEqual(read_entries[i].shard_filename, entries[i].shard_filename)
                self.assertEqual(read_entries[i].min_entity_id, entries[i].min_entity_id)
                self.assertEqual(read_entries[i].max_entity_id, entries[i].max_entity_id)
                self.assertEqual(read_entries[i].min_lsn, entries[i].min_lsn)
                self.assertEqual(read_entries[i].max_lsn, entries[i].max_lsn)
            
            # Next read should return None (EOF)
            self.assertIsNone(manifest._read_manifest_entry(fd))
        finally:
            rposix.close(fd)

if __name__ == '__main__':
    unittest.main()
