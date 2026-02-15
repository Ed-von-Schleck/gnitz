import os
import shutil
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import engine, memtable, spine, wal, manifest, shard_registry
from gnitz.core import types, values as db_values

def test_engine_recovery_cycle():
    layout = types.TableSchema([types.ColumnDefinition(types.TYPE_I64)], 0)
    test_dir = "test_engine_lifecycle_env"
    if os.path.exists(test_dir): shutil.rmtree(test_dir)
    os.makedirs(test_dir)
    
    m_path = os.path.join(test_dir, "test.manifest")
    w_path = os.path.join(test_dir, "test.wal")
    s_path = os.path.join(test_dir, "shard1.db")

    # 1. Simulate active state
    w_writer = wal.WALWriter(w_path, layout)
    m_mgr = manifest.ManifestManager(m_path)
    db = engine.Engine(memtable.MemTableManager(layout, 1024, wal_writer=w_writer), spine.Spine([]), m_mgr)
    
    db.mem_manager.put(1, 1, [db_values.IntValue(100)]) # LSN 1
    db.mem_manager.put(2, 1, [db_values.IntValue(200)]) # LSN 2
    
    # 2. Checkpoint: Flush to shard and truncate WAL
    db.flush_and_rotate(s_path) 
    db.mem_manager.wal_writer.truncate_before_lsn(3)
    
    # 3. Add un-flushed data
    db.mem_manager.put(3, 1, [db_values.IntValue(300)]) # LSN 3
    db.close()
    
    # 4. Restart: Recovery should load Shard 1 AND replay WAL Block LSN 3
    new_spine = spine.spine_from_manifest(m_path, 1, layout)
    db_new = engine.Engine(memtable.MemTableManager(layout, 1024), new_spine, manifest.ManifestManager(m_path), recover_wal_filename=w_path)
    
    # Logic verification: Total LSN should be 4
    if db_new.current_lsn != 4: raise ValueError("LSN monotonicity failure")
    
    db_new.close()
    shutil.rmtree(test_dir)

if __name__ == "__main__":
    test_engine_recovery_cycle()
    print("Engine Lifecycle Test Passed")
