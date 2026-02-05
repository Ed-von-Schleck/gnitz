from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage.memtable import node_get_weight, node_compare_key, node_get_next_off

class Engine(object):
    """
    Unified point-query engine for DBSP.
    Sums weights from active MemTable and immutable Spine.
    """
    def __init__(self, mem_manager, spine):
        self.mem_manager = mem_manager
        self.spine = spine

    def get_weight(self, key):
        # 1. Search MemTable (Optimized SkipList search)
        base = self.mem_manager.active_table.arena.base_ptr
        curr_off = self.mem_manager.active_table.head_off
        
        mem_weight = 0
        # Search from top of skip towers
        for i in range(15, -1, -1):
            next_off = node_get_next_off(base, curr_off, i)
            while next_off != 0:
                cmp_res = node_compare_key(base, next_off, key)
                if cmp_res < 0:
                    curr_off = next_off
                    next_off = node_get_next_off(base, curr_off, i)
                elif cmp_res == 0:
                    mem_weight = node_get_weight(base, next_off)
                    break
                else:
                    break
            if mem_weight != 0:
                break

        # 2. Search Spine
        spine_weight = self.spine.get_weight_for_key(key)
        
        return mem_weight + spine_weight

    def close(self):
        self.mem_manager.close()
        self.spine.close_all()
