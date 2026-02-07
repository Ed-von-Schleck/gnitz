import os
from gnitz.storage import engine, memtable, spine, wal, manifest, shard_registry, refcount
from gnitz.core import types

def test_wal_recovery():
    """
    Test that the Engine can recover state from a WAL file after a 'crash'.
    """
    wal_filename = "recovery_test.wal"
    manifest_filename = "recovery_test.manifest"
    
    # Clean up previous run
    if os.path.exists(wal_filename): os.unlink(wal_filename)
    if os.path.exists(manifest_filename): os.unlink(manifest_filename)

    layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_I64]) # Value 1, Value 2

    try:
        # --- 1. Simulate "Crash" Scenario ---
        print "[TEST] Creating WAL data..."
        
        # We manually use a WALWriter to simulate a process that wrote to WAL
        # but crashed before it could flush to disk (create a shard).
        writer = wal.WALWriter(wal_filename, layout)
        
        # Batch 1: LSN 100
        # Entity 1: Weight 1, Values (10, 20)
        c_data_1 = _pack_simple(layout, [10, 20])
        writer.append_block(100, 1, [(1, 1, c_data_1)])
        
        # Batch 2: LSN 101
        # Entity 2: Weight 1, Values (30, 40)
        c_data_2 = _pack_simple(layout, [30, 40])
        writer.append_block(101, 1, [(2, 1, c_data_2)])
        
        # Batch 3: LSN 102
        # Entity 1: Weight 1, Values (50, 60) -> Net weight for Entity 1 becomes 2
        # Last-Write-Wins means values should be (50, 60)
        c_data_3 = _pack_simple(layout, [50, 60])
        writer.append_block(102, 1, [(1, 1, c_data_3)])
        
        writer.close()
        
        print "[TEST] Crash simulated. WAL contains un-flushed data."

        # --- 2. Start Engine and Recover ---
        print "[TEST] Starting Engine with recovery..."
        
        mgr = memtable.MemTableManager(layout, 1024 * 1024)
        m_mgr = manifest.ManifestManager(manifest_filename) # Empty manifest
        reg = shard_registry.ShardRegistry()
        sp = spine.Spine([])
        
        # Initialize Engine
        # In a real app, we might pass recovery_wal_filename to __init__,
        # but here we call recover_from_wal explicitly for clarity.
        db = engine.Engine(mgr, sp, m_mgr, reg, component_id=1, current_lsn=1)
        
        # Perform Recovery
        db.recover_from_wal(wal_filename)
        
        # --- 3. Verify State ---
        print "[TEST] Verifying recovered state..."
        
        # Entity 1: Should have weight 2 (1 + 1)
        w1 = db.get_effective_weight(1)
        if w1 != 2:
            print "FAILURE: Entity 1 weight mismatch. Expected 2, got %d" % w1
            return 1
            
        # Entity 1: Should have values (50, 60) due to LWW
        # Check first field (index 0)
        v1 = db.read_component_i64(1, 0)
        if v1 != 50:
            print "FAILURE: Entity 1 value mismatch. Expected 50, got %d" % v1
            return 1

        # Entity 2: Should have weight 1
        w2 = db.get_effective_weight(2)
        if w2 != 1:
            print "FAILURE: Entity 2 weight mismatch. Expected 1, got %d" % w2
            return 1
            
        # Verify LSN advanced
        # We replayed up to LSN 102, so next LSN should be 103
        if db.current_lsn != 103:
             print "FAILURE: Current LSN mismatch. Expected 103, got %d" % db.current_lsn
             return 1

        print "[TEST] Recovery successful!"
        
        # --- 4. Verify New Writes Work ---
        # Writes after recovery should append to the WAL (if we attached a writer)
        # In this test harness, we didn't attach a writer to the Engine's MemManager
        # because we are testing the *Read* path. 
        
        db.close()
        
    finally:
        if os.path.exists(wal_filename): os.unlink(wal_filename)
        if os.path.exists(manifest_filename): os.unlink(manifest_filename)

    return 0

def _pack_simple(layout, values):
    """Helper to pack int64s into bytes for WAL setup."""
    from rpython.rtyper.lltypesystem import rffi, lltype
    stride = layout.stride
    buf = lltype.malloc(rffi.CCHARP.TO, stride, flavor='raw')
    try:
        # Zero out
        for i in range(stride): buf[i] = '\x00'
        # Pack
        for i in range(len(values)):
            val = values[i]
            off = layout.field_offsets[i]
            rffi.cast(rffi.LONGLONGP, rffi.ptradd(buf, off))[0] = rffi.cast(rffi.LONGLONG, val)
        
        return rffi.charpsize2str(buf, stride)
    finally:
        lltype.free(buf, flavor='raw')

if __name__ == "__main__":
    test_wal_recovery()
