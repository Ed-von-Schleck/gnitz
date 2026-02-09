import os
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import engine, memtable, spine, wal, manifest, shard_registry
from gnitz.core import types, values as db_values

def get_weight_helper(engine_inst, eid, vals):
    layout = engine_inst.layout
    scratch = lltype.malloc(rffi.CCHARP.TO, layout.stride, flavor='raw')
    try:
        for i in range(layout.stride): scratch[i] = '\x00'
        engine_inst.mem_manager.active_table._pack_values_to_buf(scratch, vals)
        return engine_inst.get_effective_weight_raw(eid, scratch, engine_inst.mem_manager.active_table.blob_arena.base_ptr)
    finally:
        lltype.free(scratch, flavor='raw')

def test_truncation_lifecycle():
    layout = types.ComponentLayout([types.TYPE_I64])
    m_name, w_name, s_name = "trunc.manifest", "trunc.wal", "trunc_shard.db"
    for f in [m_name, w_name, s_name]:
        if os.path.exists(f): os.unlink(f)

    try:
        w_writer = wal.WALWriter(w_name, layout)
        m_mgr = manifest.ManifestManager(m_name)
        db = engine.Engine(memtable.MemTableManager(layout, 1024, wal_writer=w_writer), spine.Spine([]), m_mgr)
        
        db.mem_manager.put(1, 1, [db_values.IntValue(100)]) # LSN 1
        db.mem_manager.put(2, 1, [db_values.IntValue(200)]) # LSN 2
        
        db.flush_and_rotate(s_name) # Persists LSN 1, 2. Manifest global_max_lsn becomes 2.
        db.mem_manager.put(3, 1, [db_values.IntValue(300)]) # LSN 3 (MemTable)
        
        init_size = os.path.getsize(w_name)
        db.mem_manager.wal_writer.truncate_before_lsn(3) # Keep LSN 3
        
        assert os.path.getsize(w_name) < init_size
        reader = wal.WALReader(w_name, layout)
        blocks = list(reader.iterate_blocks())
        assert len(blocks) == 1 and blocks[0][0] == 3
        reader.close()
        db.close()
        
        # Restart and verify recovery
        db_new = engine.Engine(memtable.MemTableManager(layout, 1024), spine.Spine.from_manifest(m_name, 1, layout), manifest.ManifestManager(m_name), recover_wal_filename=w_name)
        assert get_weight_helper(db_new, 3, [db_values.IntValue(300)]) == 1
        assert db_new.current_lsn == 4
        db_new.close()
    finally:
        for f in [m_name, w_name, s_name]:
            if os.path.exists(f): os.unlink(f)

if __name__ == "__main__":
    test_truncation_lifecycle()
    print "Truncation and Monotonicity Test Passed"
