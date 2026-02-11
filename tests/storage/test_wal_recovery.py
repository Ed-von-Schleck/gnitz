import os
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import engine, memtable, spine, wal, manifest, shard_registry, refcount
from gnitz.core import types, values as db_values

def get_weight_helper(engine_inst, pk, vals):
    layout = engine_inst.layout
    scratch = lltype.malloc(rffi.CCHARP.TO, layout.stride, flavor='raw')
    try:
        for i in range(layout.stride): scratch[i] = '\x00'
        engine_inst.mem_manager.active_table._pack_values_to_buf(scratch, vals)
        return engine_inst.get_effective_weight_raw(pk, scratch, engine_inst.mem_manager.active_table.blob_arena.base_ptr)
    finally:
        lltype.free(scratch, flavor='raw')

def test_wal_recovery():
    wal_fn = "recovery_test.wal"
    m_fn = "recovery_test.manifest"
    if os.path.exists(wal_fn): os.unlink(wal_fn)
    if os.path.exists(m_fn): os.unlink(m_fn)

    layout = types.ComponentLayout([types.TYPE_I64, types.TYPE_I64])
    try:
        writer = wal.WALWriter(wal_fn, layout)
        writer.append_block(100, 1, [(1, 1, _pack_simple(layout, [10, 20]))])
        writer.append_block(101, 1, [(2, 1, _pack_simple(layout, [30, 40]))])
        writer.append_block(102, 1, [(1, 1, _pack_simple(layout, [10, 20]))]) # Add weight to Entity 1
        writer.close()
        
        mgr = memtable.MemTableManager(layout, 1024 * 1024)
        db = engine.Engine(mgr, spine.Spine([]), manifest.ManifestManager(m_fn), shard_registry.ShardRegistry(), table_id=1)
        db.recover_from_wal(wal_fn)
        
        # Verify Entity 1 has weight 2
        chk1 = [db_values.IntValue(10), db_values.IntValue(20)]
        if get_weight_helper(db, 1, chk1) != 2: return 1
        if db.current_lsn != 103: return 1

        print "Recovery successful!"
        db.close()
        return 0
    finally:
        if os.path.exists(wal_fn): os.unlink(wal_fn)
        if os.path.exists(m_fn): os.unlink(m_fn)

def _pack_simple(layout, values):
    stride = layout.stride
    buf = lltype.malloc(rffi.CCHARP.TO, stride, flavor='raw')
    try:
        for i in range(stride): buf[i] = '\x00'
        for i, val in enumerate(values):
            rffi.cast(rffi.LONGLONGP, rffi.ptradd(buf, layout.field_offsets[i]))[0] = rffi.cast(rffi.LONGLONG, val)
        return rffi.charpsize2str(buf, stride)
    finally:
        lltype.free(buf, flavor='raw')

if __name__ == "__main__":
    import sys
    sys.exit(test_wal_recovery())
