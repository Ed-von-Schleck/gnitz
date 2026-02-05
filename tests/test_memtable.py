import os
import pytest
from gnitz.storage.memtable import MemTable, MemTableManager
from gnitz.storage.shard import ShardView

def test_memtable_upsert_coalesce():
    mt = MemTable(1024 * 1024)
    mt.upsert("user:1", "Alice", 1)
    mt.upsert("user:1", "Alice", 1)
    
    head = mt.head
    first_off = head.get_next_offset(0)
    first_node = mt._get_node_at(first_off)
    
    assert first_node.get_key() == "user:1"
    assert first_node.get_weight() == 2
    mt.free()

def test_memtable_sorting():
    mt = MemTable(1024 * 1024)
    mt.upsert("z", "last", 1)
    mt.upsert("a", "first", 1)
    mt.upsert("m", "middle", 1)
    
    keys = []
    curr_off = mt.head.get_next_offset(0)
    while curr_off != 0:
        node = mt._get_node_at(curr_off)
        keys.append(node.get_key())
        curr_off = node.get_next_offset(0)
        
    assert keys == ["a", "m", "z"]
    mt.free()

def test_manager_flow():
    mgr = MemTableManager(2048)
    mgr.put("k1", "v1", 1)
    
    filename = "managed_flush.db"
    try:
        mgr.flush_and_rotate(filename)
        view = ShardView(filename)
        assert view.count == 1
        assert view.materialize_key(0) == "k1"
        view.close()
    finally:
        mgr.close()
        if os.path.exists(filename):
            os.unlink(filename)
