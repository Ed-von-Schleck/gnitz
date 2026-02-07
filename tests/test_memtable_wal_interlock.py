import unittest
import os
from gnitz.storage import memtable, wal, wal_format
from gnitz.core import types


class TestMemTableWALInterlock(unittest.TestCase):
    def setUp(self):
        # Simple layout: [Value (I64), Label (String)]
        self.layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
        self.test_wal = "test_interlock.log"
        
        # Clean up any existing files
        if os.path.exists(self.test_wal):
            os.unlink(self.test_wal)
    
    def tearDown(self):
        # Clean up test file
        if os.path.exists(self.test_wal):
            os.unlink(self.test_wal)
    
    def test_memtable_without_wal(self):
        """Test that MemTable works without WAL (backward compatibility)."""
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024)
        
        # Should work without WAL writer
        mgr.put(1, 1, 100, "test")
        mgr.put(2, 1, 200, "another")
        
        mgr.close()
    
    def test_memtable_with_wal_integration(self):
        """Test that MemTable writes to WAL before updating memory."""
        # Create WAL writer
        wal_writer = wal.WALWriter(self.test_wal, self.layout)
        
        # Create MemTable with WAL
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024, wal_writer, component_id=1)
        
        # Insert data - should write to WAL first
        mgr.put(10, 1, 100, "first")
        mgr.put(20, 1, 200, "second")
        
        # Close to flush WAL
        mgr.close()
        wal_writer.close()
        
        # Verify WAL contains the data
        self.assertTrue(os.path.exists(self.test_wal))
        
        # Read WAL back
        reader = wal.WALReader(self.test_wal, self.layout)
        
        # Should have 2 blocks (one per put)
        block1 = reader.read_next_block()
        self.assertIsNotNone(block1)
        lsn1, comp_id1, records1 = block1
        self.assertEqual(comp_id1, 1)
        self.assertEqual(len(records1), 1)
        entity_id1, weight1, _ = records1[0]
        self.assertEqual(entity_id1, 10)
        self.assertEqual(weight1, 1)
        
        block2 = reader.read_next_block()
        self.assertIsNotNone(block2)
        lsn2, comp_id2, records2 = block2
        self.assertEqual(comp_id2, 1)
        entity_id2, weight2, _ = records2[0]
        self.assertEqual(entity_id2, 20)
        self.assertEqual(weight2, 1)
        
        reader.close()
    
    def test_wal_write_before_memory_update(self):
        """Test that WAL is written BEFORE MemTable is updated."""
        wal_writer = wal.WALWriter(self.test_wal, self.layout)
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024, wal_writer, component_id=1)
        
        # Insert one record
        mgr.put(100, 1, 999, "durable")
        
        # WAL should have content immediately (before close)
        # This simulates crash - MemTable in memory, but WAL on disk
        file_size = os.path.getsize(self.test_wal)
        self.assertGreater(file_size, 0)
        
        mgr.close()
        wal_writer.close()
    
    def test_algebraic_consistency(self):
        """Test that WAL weights match MemTable weights."""
        wal_writer = wal.WALWriter(self.test_wal, self.layout)
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024, wal_writer, component_id=1)
        
        # Insert with weight +1
        mgr.put(50, 1, 500, "add")
        
        # Insert with weight -1 (deletion)
        mgr.put(60, -1, 600, "delete")
        
        # Insert with weight +2
        mgr.put(70, 2, 700, "double")
        
        mgr.close()
        wal_writer.close()
        
        # Read WAL and verify weights
        reader = wal.WALReader(self.test_wal, self.layout)
        
        blocks = list(reader.iterate_blocks())
        self.assertEqual(len(blocks), 3)
        
        # Verify weights in WAL
        _, _, records1 = blocks[0]
        _, weight1, _ = records1[0]
        self.assertEqual(weight1, 1)
        
        _, _, records2 = blocks[1]
        _, weight2, _ = records2[0]
        self.assertEqual(weight2, -1)
        
        _, _, records3 = blocks[2]
        _, weight3, _ = records3[0]
        self.assertEqual(weight3, 2)
        
        reader.close()
    
    def test_crash_scenario_simulation(self):
        """Test crash scenario: WAL exists but MemTable was not flushed."""
        # Phase 1: Write to WAL (simulating crash before flush)
        wal_writer = wal.WALWriter(self.test_wal, self.layout)
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024, wal_writer, component_id=1)
        
        mgr.put(1, 1, 111, "pre_crash")
        mgr.put(2, 1, 222, "also_pre_crash")
        
        # Simulate crash: close WAL but don't flush MemTable
        wal_writer.close()
        # DON'T call mgr.flush_and_rotate()
        mgr.close()  # Just free memory
        
        # Phase 2: Recovery - WAL should contain the data
        reader = wal.WALReader(self.test_wal, self.layout)
        
        blocks = list(reader.iterate_blocks())
        self.assertEqual(len(blocks), 2)
        
        # Both records should be in WAL
        _, _, records1 = blocks[0]
        entity_id1, _, _ = records1[0]
        self.assertEqual(entity_id1, 1)
        
        _, _, records2 = blocks[1]
        entity_id2, _, _ = records2[0]
        self.assertEqual(entity_id2, 2)
        
        reader.close()
    
    def test_multiple_puts_single_entity(self):
        """Test multiple puts to same entity (algebraic accumulation)."""
        wal_writer = wal.WALWriter(self.test_wal, self.layout)
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024, wal_writer, component_id=1)
        
        # Put same entity 3 times with different weights
        mgr.put(100, 1, 10, "v1")
        mgr.put(100, 1, 20, "v2")
        mgr.put(100, -1, 30, "v3")
        
        mgr.close()
        wal_writer.close()
        
        # WAL should have 3 separate blocks
        reader = wal.WALReader(self.test_wal, self.layout)
        blocks = list(reader.iterate_blocks())
        self.assertEqual(len(blocks), 3)
        
        # All should have entity_id 100
        for _, _, records in blocks:
            entity_id, _, _ = records[0]
            self.assertEqual(entity_id, 100)
        
        reader.close()
    
    def test_component_id_propagation(self):
        """Test that component_id is correctly written to WAL."""
        wal_writer = wal.WALWriter(self.test_wal, self.layout)
        
        # Create MemTable with specific component_id
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024, wal_writer, component_id=42)
        
        mgr.put(1, 1, 100, "test")
        mgr.close()
        wal_writer.close()
        
        # Verify component_id in WAL
        reader = wal.WALReader(self.test_wal, self.layout)
        lsn, component_id, records = reader.read_next_block()
        
        self.assertEqual(component_id, 42)
        reader.close()
    
    def test_lsn_increments(self):
        """Test that LSN increments with each put."""
        wal_writer = wal.WALWriter(self.test_wal, self.layout)
        mgr = memtable.MemTableManager(self.layout, 1024 * 1024, wal_writer, component_id=1)
        
        # Multiple puts
        for i in range(5):
            mgr.put(i, 1, i * 10, "rec_%d" % i)
        
        mgr.close()
        wal_writer.close()
        
        # Read WAL and verify LSN sequence
        reader = wal.WALReader(self.test_wal, self.layout)
        lsns = []
        for lsn, _, _ in reader.iterate_blocks():
            lsns.append(lsn)
        
        # LSNs should be monotonically increasing
        for i in range(len(lsns) - 1):
            self.assertLess(lsns[i], lsns[i + 1])
        
        reader.close()


if __name__ == '__main__':
    unittest.main()
