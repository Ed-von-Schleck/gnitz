import unittest
import os
from gnitz.storage.refcount import RefCounter
from gnitz.storage import errors

class TestRefCounter(unittest.TestCase):
    def setUp(self):
        self.refcount = RefCounter()
        self.test_files = []
    
    def tearDown(self):
        # Clean up any test files that still exist
        for fn in self.test_files:
            if os.path.exists(fn):
                os.unlink(fn)
    
    def _create_test_file(self, filename):
        """Helper to create a temporary test file."""
        with open(filename, 'w') as f:
            f.write("test")
        self.test_files.append(filename)
    
    def test_acquire_release_single(self):
        """Test basic acquire and release."""
        filename = "test_ref_single.db"
        
        # Initial state - can delete
        self.assertTrue(self.refcount.can_delete(filename))
        
        # Acquire reference
        self.refcount.acquire(filename)
        self.assertFalse(self.refcount.can_delete(filename))
        
        # Release reference
        self.refcount.release(filename)
        self.assertTrue(self.refcount.can_delete(filename))
    
    def test_multiple_acquires(self):
        """Test multiple acquires of the same file."""
        filename = "test_ref_multiple.db"
        
        # Acquire 3 times
        self.refcount.acquire(filename)
        self.refcount.acquire(filename)
        self.refcount.acquire(filename)
        
        self.assertFalse(self.refcount.can_delete(filename))
        
        # Release once
        self.refcount.release(filename)
        self.assertFalse(self.refcount.can_delete(filename))
        
        # Release again
        self.refcount.release(filename)
        self.assertFalse(self.refcount.can_delete(filename))
        
        # Release final time
        self.refcount.release(filename)
        self.assertTrue(self.refcount.can_delete(filename))
    
    def test_can_delete_with_active_references(self):
        """Test that files with active references cannot be deleted."""
        filename = "test_ref_active.db"
        
        self.refcount.acquire(filename)
        self.refcount.acquire(filename)
        
        self.assertFalse(self.refcount.can_delete(filename))
    
    def test_can_delete_with_zero_references(self):
        """Test that files with zero references can be deleted."""
        filename = "test_ref_zero.db"
        
        # Never acquired - should be deletable
        self.assertTrue(self.refcount.can_delete(filename))
        
        # Acquire and release - should be deletable again
        self.refcount.acquire(filename)
        self.refcount.release(filename)
        self.assertTrue(self.refcount.can_delete(filename))
    
    def test_mark_for_deletion_and_cleanup(self):
        """Test marking a file for deletion and cleanup process."""
        filename = "test_ref_cleanup.db"
        self._create_test_file(filename)
        
        # Mark for deletion while no references
        self.refcount.mark_for_deletion(filename)
        
        # Cleanup should delete the file
        deleted = self.refcount.try_cleanup()
        self.assertEqual(len(deleted), 1)
        self.assertEqual(deleted[0], filename)
        self.assertFalse(os.path.exists(filename))
    
    def test_cleanup_with_active_references(self):
        """Test that cleanup doesn't delete files with active references."""
        filename = "test_ref_cleanup_active.db"
        self._create_test_file(filename)
        
        # Acquire reference
        self.refcount.acquire(filename)
        
        # Mark for deletion
        self.refcount.mark_for_deletion(filename)
        
        # Cleanup should NOT delete (still has refs)
        deleted = self.refcount.try_cleanup()
        self.assertEqual(len(deleted), 0)
        self.assertTrue(os.path.exists(filename))
        
        # Release reference
        self.refcount.release(filename)
        
        # Now cleanup should delete
        deleted = self.refcount.try_cleanup()
        self.assertEqual(len(deleted), 1)
        self.assertFalse(os.path.exists(filename))
    
    def test_cleanup_multiple_files(self):
        """Test cleanup of multiple files."""
        file1 = "test_ref_multi1.db"
        file2 = "test_ref_multi2.db"
        file3 = "test_ref_multi3.db"
        
        self._create_test_file(file1)
        self._create_test_file(file2)
        self._create_test_file(file3)
        
        # file1: no refs
        # file2: has refs
        # file3: no refs
        
        self.refcount.acquire(file2)
        
        self.refcount.mark_for_deletion(file1)
        self.refcount.mark_for_deletion(file2)
        self.refcount.mark_for_deletion(file3)
        
        # Cleanup should delete file1 and file3, but not file2
        deleted = self.refcount.try_cleanup()
        self.assertEqual(len(deleted), 2)
        self.assertIn(file1, deleted)
        self.assertIn(file3, deleted)
        
        self.assertFalse(os.path.exists(file1))
        self.assertTrue(os.path.exists(file2))
        self.assertFalse(os.path.exists(file3))
        
        # Release file2 and cleanup again
        self.refcount.release(file2)
        deleted = self.refcount.try_cleanup()
        self.assertEqual(len(deleted), 1)
        self.assertEqual(deleted[0], file2)
        self.assertFalse(os.path.exists(file2))
    
    def test_release_without_acquire_raises_error(self):
        """Test that releasing a file without acquiring raises error."""
        filename = "test_ref_invalid.db"
        
        with self.assertRaises(errors.StorageError):
            self.refcount.release(filename)
    
    def test_release_too_many_times_raises_error(self):
        """Test that releasing more times than acquired raises error."""
        filename = "test_ref_overrelease.db"
        
        self.refcount.acquire(filename)
        self.refcount.release(filename)
        
        # Try to release again
        with self.assertRaises(errors.StorageError):
            self.refcount.release(filename)
    
    def test_mark_for_deletion_multiple_times(self):
        """Test that marking the same file multiple times doesn't duplicate entries."""
        filename = "test_ref_duplicate_mark.db"
        self._create_test_file(filename)
        
        self.refcount.mark_for_deletion(filename)
        self.refcount.mark_for_deletion(filename)
        self.refcount.mark_for_deletion(filename)
        
        # Should only be marked once in internal list (verified via cleanup behavior)
        
        # Cleanup should only try to delete once
        deleted = self.refcount.try_cleanup()
        self.assertEqual(len(deleted), 1)
    
    def test_cleanup_nonexistent_file(self):
        """Test that cleanup handles nonexistent files gracefully."""
        filename = "test_ref_nonexistent.db"
        
        # Mark a file that doesn't exist
        self.refcount.mark_for_deletion(filename)
        
        # Cleanup should handle gracefully
        deleted = self.refcount.try_cleanup()
        self.assertEqual(len(deleted), 1)  # Still reports as "deleted"
    
    def test_interleaved_acquire_release_delete(self):
        """Test complex interleaved operations."""
        filename = "test_ref_interleaved.db"
        self._create_test_file(filename)
        
        # Acquire twice
        self.refcount.acquire(filename)
        self.refcount.acquire(filename)
        
        # Mark for deletion
        self.refcount.mark_for_deletion(filename)
        
        # Try cleanup - should not delete yet
        deleted = self.refcount.try_cleanup()
        self.assertEqual(len(deleted), 0)
        self.assertTrue(os.path.exists(filename))
        
        # Release once
        self.refcount.release(filename)
        
        # Try cleanup - still should not delete
        deleted = self.refcount.try_cleanup()
        self.assertEqual(len(deleted), 0)
        self.assertTrue(os.path.exists(filename))
        
        # Release final time
        self.refcount.release(filename)
        
        # Now cleanup should succeed
        deleted = self.refcount.try_cleanup()
        self.assertEqual(len(deleted), 1)
        self.assertFalse(os.path.exists(filename))

if __name__ == '__main__':
    unittest.main()
