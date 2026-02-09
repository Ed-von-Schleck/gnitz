import unittest
import os
from gnitz.storage import manifest, errors

class TestManifestVersioning(unittest.TestCase):
    def setUp(self):
        self.manifest_path = "test_manifest_versioned.db"
        self.temp_path = self.manifest_path + ".tmp"
        self.manager = manifest.ManifestManager(self.manifest_path)
    
    def tearDown(self):
        if os.path.exists(self.manifest_path):
            os.unlink(self.manifest_path)
        if os.path.exists(self.temp_path):
            os.unlink(self.temp_path)
    
    def test_initial_manifest_does_not_exist(self):
        """Test that initially no manifest exists."""
        self.assertFalse(self.manager.exists())
    
    def test_create_initial_manifest(self):
        """Test creating an initial manifest."""
        entries = [
            manifest.ManifestEntry(1, "shard_001.db", 0, 100, 0, 5),
        ]
        
        # Use publish_new_version to create initial
        self.manager.publish_new_version(entries)
        
        # Verify it exists
        self.assertTrue(self.manager.exists())
        
        # Verify contents
        reader = self.manager.load_current()
        try:
            self.assertEqual(reader.get_entry_count(), 1)
            entries = list(reader.iterate_entries())
            self.assertEqual(entries[0].shard_filename, "shard_001.db")
        finally:
            reader.close()
    
    def test_atomic_update_add_shard(self):
        """Test atomically adding a shard to the manifest."""
        # Create initial manifest with 1 shard
        initial_entries = [
            manifest.ManifestEntry(1, "shard_001.db", 0, 100, 0, 5),
        ]
        self.manager.publish_new_version(initial_entries)
        
        # Read current state
        reader = self.manager.load_current()
        current_entries = list(reader.iterate_entries())
        reader.close()
        
        # Add a new shard
        new_entry = manifest.ManifestEntry(1, "shard_002.db", 101, 200, 5, 10)
        current_entries.append(new_entry)
        
        # Publish new version
        self.manager.publish_new_version(current_entries)
        
        # Verify new version
        reader = self.manager.load_current()
        try:
            self.assertEqual(reader.get_entry_count(), 2)
            entries = list(reader.iterate_entries())
            self.assertEqual(entries[0].shard_filename, "shard_001.db")
            self.assertEqual(entries[1].shard_filename, "shard_002.db")
        finally:
            reader.close()
    
    def test_atomic_update_remove_shard(self):
        """Test atomically removing a shard from the manifest."""
        # Create initial manifest with 3 shards
        initial_entries = [
            manifest.ManifestEntry(1, "shard_001.db", 0, 100, 0, 5),
            manifest.ManifestEntry(1, "shard_002.db", 101, 200, 5, 10),
            manifest.ManifestEntry(1, "shard_003.db", 201, 300, 10, 15),
        ]
        self.manager.publish_new_version(initial_entries)
        
        # Read current and remove middle shard
        reader = self.manager.load_current()
        current_entries = list(reader.iterate_entries())
        reader.close()
        
        # Remove shard_002.db
        filtered_entries = [e for e in current_entries if e.shard_filename != "shard_002.db"]
        
        self.manager.publish_new_version(filtered_entries)
        
        # Verify
        reader = self.manager.load_current()
        try:
            self.assertEqual(reader.get_entry_count(), 2)
            entries = list(reader.iterate_entries())
            self.assertEqual(entries[0].shard_filename, "shard_001.db")
            self.assertEqual(entries[1].shard_filename, "shard_003.db")
        finally:
            reader.close()
    
    def test_multiple_sequential_updates(self):
        """Test multiple sequential updates."""
        # Start with empty manifest
        self.manager.publish_new_version([])
        
        # Update 1: Add shard
        entries = [manifest.ManifestEntry(1, "shard_001.db", 0, 100, 0, 5)]
        self.manager.publish_new_version(entries)
        
        reader = self.manager.load_current()
        self.assertEqual(reader.get_entry_count(), 1)
        reader.close()
        
        # Update 2: Add another shard
        entries.append(manifest.ManifestEntry(1, "shard_002.db", 101, 200, 5, 10))
        self.manager.publish_new_version(entries)
        
        reader = self.manager.load_current()
        self.assertEqual(reader.get_entry_count(), 2)
        reader.close()
        
        # Update 3: Add third shard
        entries.append(manifest.ManifestEntry(1, "shard_003.db", 201, 300, 10, 15))
        self.manager.publish_new_version(entries)
        
        reader = self.manager.load_current()
        self.assertEqual(reader.get_entry_count(), 3)
        reader.close()
    
    def test_old_manifest_not_corrupted_during_update(self):
        """Test that the old manifest remains valid during update process."""
        initial_entries = [
            manifest.ManifestEntry(1, "shard_001.db", 0, 100, 0, 5),
        ]
        self.manager.publish_new_version(initial_entries)
        
        # Read the manifest
        reader1 = self.manager.load_current()
        self.assertEqual(reader1.get_entry_count(), 1)
        
        # While reader1 is still open, publish new version
        new_entries = [
            manifest.ManifestEntry(1, "shard_001.db", 0, 100, 0, 5),
            manifest.ManifestEntry(1, "shard_002.db", 101, 200, 5, 10),
        ]
        self.manager.publish_new_version(new_entries)
        
        # Old reader should still work (reading old file descriptor)
        entries = list(reader1.iterate_entries())
        self.assertEqual(entries[0].shard_filename, "shard_001.db")
        reader1.close()
        
        # New reader should see new version
        reader2 = self.manager.load_current()
        try:
            self.assertEqual(reader2.get_entry_count(), 2)
        finally:
            reader2.close()
    
    def test_incomplete_tmp_file_does_not_affect_current(self):
        """Test that an incomplete .tmp file doesn't affect the current manifest."""
        # Create initial manifest
        initial_entries = [manifest.ManifestEntry(1, "shard_001.db", 0, 100, 0, 5)]
        self.manager.publish_new_version(initial_entries)
        
        # Manually create a corrupt .tmp file
        with open(self.temp_path, 'wb') as f:
            f.write(b'\x00' * 10)  # Incomplete/corrupt data
        
        # Current manifest should still be readable
        reader = self.manager.load_current()
        try:
            self.assertEqual(reader.get_entry_count(), 1)
            entries = list(reader.iterate_entries())
            self.assertEqual(entries[0].shard_filename, "shard_001.db")
        finally:
            reader.close()
    
    def test_manual_entry_filtering(self):
        """Test manually creating new version by filtering and adding entries."""
        # Create initial manifest
        initial_entries = [
            manifest.ManifestEntry(1, "shard_001.db", 0, 100, 0, 5),
            manifest.ManifestEntry(1, "shard_002.db", 101, 200, 5, 10),
            manifest.ManifestEntry(1, "shard_003.db", 201, 300, 10, 15),
        ]
        self.manager.publish_new_version(initial_entries)
        
        # Load and filter manually
        reader = self.manager.load_current()
        current_entries = list(reader.iterate_entries())
        reader.close()
        
        # Remove shard_002.db
        new_entries = []
        for e in current_entries:
            if e.shard_filename != "shard_002.db":
                new_entries.append(e)
        
        # Add new shards
        new_entries.append(manifest.ManifestEntry(1, "shard_004.db", 301, 400, 15, 20))
        new_entries.append(manifest.ManifestEntry(1, "shard_005.db", 401, 500, 20, 25))
        
        # Verify the new entries list has correct count
        self.assertEqual(len(new_entries), 4)  # 3 - 1 + 2 = 4
        
        # Verify filenames
        filenames = [e.shard_filename for e in new_entries]
        self.assertIn("shard_001.db", filenames)
        self.assertNotIn("shard_002.db", filenames)
        self.assertIn("shard_003.db", filenames)
        self.assertIn("shard_004.db", filenames)
        self.assertIn("shard_005.db", filenames)
        
        # Publish this new version
        self.manager.publish_new_version(new_entries)
        
        # Verify
        reader = self.manager.load_current()
        try:
            self.assertEqual(reader.get_entry_count(), 4)
        finally:
            reader.close()

if __name__ == '__main__':
    unittest.main()
