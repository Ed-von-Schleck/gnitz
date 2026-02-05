import sys
import os
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import memtable, spine, engine, writer_ecs, layout, shard_ecs
from gnitz.core import types, strings

def entry_point(argv):
    print "--- GnitzDB ECS Integration Test ---"
    
    layout_obj = types.ComponentLayout([types.TYPE_I64, types.TYPE_STRING])
    fn = "ecs_engine_test.db"
    if os.path.exists(fn): os.unlink(fn)
    
    try:
        mgr = memtable.MemTableManager(layout_obj, 1024 * 1024)
        sp = spine.Spine([]) 
        db = engine.Engine(mgr, sp)
        
        print "[1/5] MemTable Upsert..."
        db.mem_manager.put(1, 1, 100, "mem_only")
        
        if db.get_effective_weight(1) != 1: return 1
        if db.read_component_i64(1, 0) != 100: return 1
            
        print "[2/5] Flushing to Disk..."
        db.mem_manager.flush_and_rotate(fn)
        
        sp.close_all()
        handle = spine.ShardHandle(fn, layout_obj)
        sp = spine.Spine([handle])
        db = engine.Engine(mgr, sp)
        
        print "[3/5] Spine Read..."
        if db.get_effective_weight(1) != 1: return 1
        if db.read_component_i64(1, 0) != 100: return 1

        print "[4/5] Annihilation Test..."
        db.mem_manager.put(1, -1, 100, "mem_only") 
        if db.get_effective_weight(1) != 0: return 1
            
        print "Full ECS Engine Validation Complete."
    finally:
        if os.path.exists(fn): os.unlink(fn)

    return 0

def target(driver, args):
    return entry_point, None

if __name__ == '__main__':
    entry_point(sys.argv)
