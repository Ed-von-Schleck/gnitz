import os
from gnitz.storage import engine, memtable, spine, wal, manifest, shard_registry
from gnitz.core import types

def test_truncation_lifecycle():
    layout = types.ComponentLayout([types.TYPE_I64])
    m_name = "trunc.manifest"
    w_name = "trunc.wal"
    s_name = "trunc_shard.db"
    
    for f in [m_name, w_name, s_name]:
        if os.path.exists(f): os.unlink(f)

    try:
        # 1. Start DB and write some data
        w_writer = wal.WALWriter(w_name, layout)
        mgr = memtable.MemTableManager(layout, 1024, wal_writer=w_writer)
        m_mgr = manifest.ManifestManager(m_name)
        db = engine.Engine(mgr, spine.Spine([]), m_mgr)
        
        db.mem_manager.put(1, 1, 100) # LSN 1
        db.mem_manager.put(2, 1, 200) # LSN 2
        
        # 2. Flush to shard (Checkpoints LSN 1 & 2)
        db.flush_and_rotate(s_name)
        
        # 3. Write data that stays in MemTable (LSN 3)
        db.mem_manager.put(3, 1, 300)
        
        initial_wal_size = os.path.getsize(w_name)
        
        # 4. Truncate
        # This should remove LSN 1 and 2 from WAL because they are in the shard
        db.checkpoint(w_name)
        
        truncated_wal_size = os.path.getsize(w_name)
        assert truncated_wal_size < initial_wal_size
        
        # 5. Verify WAL only contains LSN 3
        reader = wal.WALReader(w_name, layout)
        blocks = list(reader.iterate_blocks())
        assert len(blocks) == 1
        assert blocks[0][0] == 3
        reader.close()
        
        # 6. Verify Restart Monotonicity
        db.close()
        
        db_new = engine.Engine(
            memtable.MemTableManager(layout, 1024),
            spine.Spine.from_manifest(m_name, 1, layout),
            m_mgr,
            recover_wal_filename=w_name
        )
        
        # Should have recovered LSN 3 from WAL
        assert db_new.get_effective_weight(3) == 1
        # Monotonicity check: Next LSN should be 4
        assert db_new.current_lsn == 4
        
        db_new.close()

    finally:
        for f in [m_name, w_name, s_name]:
            if os.path.exists(f): os.unlink(f)

if __name__ == "__main__":
    test_truncation_lifecycle()
    print("Truncation and Monotonicity Test Passed")
