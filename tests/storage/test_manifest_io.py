import unittest
import os
from gnitz.storage import manifest, errors

class TestManifestIO(unittest.TestCase):
    def setUp(self):
        self.test_file = "test_manifest_io.db"
    
    def tearDown(self):
        if os.path.exists(self.test_file):
            os.unlink(self.test_file)
    
    def test_writer_empty_manifest(self):
        """Test creating an empty manifest."""
        writer = manifest.ManifestWriter(self.test_file)
        writer.finalize()
        
        # Read it back
        reader = manifest.ManifestReader(self.test_file)
        try:
            self.assertEqual(reader.get_entry_count(), 0)
        finally:
            reader.close()
    
    def test_writer_single_entry(self):
        """Test creating manifest with a single entry."""
        writer = manifest.ManifestWriter(self.test_file)
        writer.add_entry(
            component_id=1,
            shard_filename="test_shard.db",
            min_eid=100,
            max_eid=200,
            min_lsn=5,
            max_lsn=10
        )
        writer.finalize()
        
        # Read it back
        reader = manifest.ManifestReader(self.test_file)
        try:
            self.assertEqual(reader.get_entry_count(), 1)
            entries = list(reader.iterate_entries())
            entry = entries[0]
            self.assertEqual(entry.component_id, 1)
            self.assertEqual(entry.shard_filename, "test_shard.db")
            self.assertEqual(entry.min_entity_id, 100)
            self.assertEqual(entry.max_entity_id, 200)
            self.assertEqual(entry.min_lsn, 5)
            self.assertEqual(entry.max_lsn, 10)
        finally:
            reader.close()
    
    def test_writer_multiple_entries(self):
        """Test creating manifest with multiple entries."""
        writer = manifest.ManifestWriter(self.test_file)
        
        # Add entries for different components
        writer.add_entry(1, "shard_001.db", 0, 100, 0, 5)
        writer.add_entry(2, "shard_002.db", 0, 50, 0, 3)
        writer.add_entry(1, "shard_003.db", 101, 200, 5, 10)
        
        writer.finalize()
        
        # Read back and verify
        reader = manifest.ManifestReader(self.test_file)
        try:
            self.assertEqual(reader.get_entry_count(), 3)
            entries = list(reader.iterate_entries())
            
            entry0 = entries[0]
            self.assertEqual(entry0.component_id, 1)
            self.assertEqual(entry0.shard_filename, "shard_001.db")
            
            entry1 = entries[1]
            self.assertEqual(entry1.component_id, 2)
            self.assertEqual(entry1.shard_filename, "shard_002.db")
            
            entry2 = entries[2]
            self.assertEqual(entry2.component_id, 1)
            self.assertEqual(entry2.shard_filename, "shard_003.db")
        finally:
            reader.close()
    
    def test_reader_iterate_entries(self):
        """Test iteration over all entries."""
        # Create manifest with entries
        writer = manifest.ManifestWriter(self.test_file)
        expected_entries = [
            (1, "a.db", 0, 10, 0, 1),
            (2, "b.db", 0, 20, 0, 2),
            (3, "c.db", 0, 30, 0, 3),
        ]
        
        for comp_id, fname, min_e, max_e, min_l, max_l in expected_entries:
            writer.add_entry(comp_id, fname, min_e, max_e, min_l, max_l)
        
        writer.finalize()
        
        # Iterate and verify
        reader = manifest.ManifestReader(self.test_file)
        try:
            entries = list(reader.iterate_entries())
            self.assertEqual(len(entries), 3)
            
            for i, entry in enumerate(entries):
                exp = expected_entries[i]
                self.assertEqual(entry.component_id, exp[0])
                self.assertEqual(entry.shard_filename, exp[1])
                self.assertEqual(entry.min_entity_id, exp[2])
                self.assertEqual(entry.max_entity_id, exp[3])
                self.assertEqual(entry.min_lsn, exp[4])
                self.assertEqual(entry.max_lsn, exp[5])
        finally:
            reader.close()
    
    def test_writer_cannot_add_after_finalize(self):
        """Test that adding entries after finalize raises error."""
        writer = manifest.ManifestWriter(self.test_file)
        writer.add_entry(1, "test.db", 0, 100, 0, 1)
        writer.finalize()
        
        # Try to add after finalize
        with self.assertRaises(errors.StorageError):
            writer.add_entry(2, "another.db", 0, 50, 0, 1)
    
    def test_reader_nonexistent_file(self):
        """Test that reading non-existent file raises error."""
        with self.assertRaises(OSError):
            manifest.ManifestReader("nonexistent.db")
    
    def test_reader_corrupt_file(self):
        """Test that corrupt manifest raises error."""
        # Create file with invalid magic number
        with open(self.test_file, 'wb') as f:
            f.write(b'\x00' * manifest.HEADER_SIZE)
        
        with self.assertRaises(errors.CorruptShardError):
            manifest.ManifestReader(self.test_file)

if __name__ == '__main__':
    unittest.main()
