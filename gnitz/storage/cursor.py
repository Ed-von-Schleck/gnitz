# gnitz/storage/cursor.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_ulonglonglong as r_uint128, r_uint64
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.objectmodel import newlist_hint
from gnitz.core import types
from gnitz.core.store import AbstractCursor
from gnitz.storage import tournament_tree, comparator
from gnitz.core import comparator as core_comparator
from gnitz.storage.memtable_node import node_get_key, node_get_weight, node_get_next_off

NULL_PTR = lltype.nullptr(rffi.CCHARP.TO)


# ---------------------------------------------------------------------------
# MemTableRef — mutable indirection enabling cursor stability across flushes
# ---------------------------------------------------------------------------


class MemTableRef(object):
    """
    A one-field mutable wrapper around a live MemTable instance.
    """

    def __init__(self, memtable):
        self.current = memtable


# ---------------------------------------------------------------------------
# BaseCursor
# ---------------------------------------------------------------------------


class BaseCursor(AbstractCursor):
    def __init__(self):
        pass

    def seek(self, target_key):
        raise NotImplementedError

    def advance(self):
        raise NotImplementedError

    def key(self):
        raise NotImplementedError

    def weight(self):
        raise NotImplementedError

    def is_valid(self):
        raise NotImplementedError

    def is_exhausted(self):
        raise NotImplementedError

    def peek_key(self):
        raise NotImplementedError

    def get_accessor(self):
        raise NotImplementedError

    def close(self):
        pass


# ---------------------------------------------------------------------------
# MemTableCursor
# ---------------------------------------------------------------------------


class MemTableCursor(BaseCursor):
    """
    Cursor over a MemTable's SkipList.
    """

    _immutable_fields_ = ["mem_ref", "schema", "key_size", "accessor"]

    def __init__(self, memtable):
        BaseCursor.__init__(self)
        self.mem_ref = MemTableRef(memtable)
        self.schema = memtable.schema
        self.key_size = memtable.key_size
        self.current_node_off = node_get_next_off(
            memtable.arena.base_ptr, memtable.head_off, 0
        )
        self.accessor = comparator.PackedNodeAccessor(
            self.schema, memtable.blob_arena.base_ptr
        )

    def _get_memtable(self):
        return self.mem_ref.current

    def seek(self, target_key):
        self.current_node_off = self._get_memtable().lower_bound_node(target_key)

    def advance(self):
        if self.current_node_off != 0:
            base = self._get_memtable().arena.base_ptr
            self.current_node_off = node_get_next_off(base, self.current_node_off, 0)

    def is_valid(self):
        return self.current_node_off != 0

    def is_exhausted(self):
        return not self.is_valid()

    def peek_key(self):
        return self.key()

    def key(self):
        if self.current_node_off == 0:
            return r_uint128(-1)
        return node_get_key(
            self._get_memtable().arena.base_ptr, self.current_node_off, self.key_size
        )

    def weight(self):
        if self.current_node_off == 0:
            return r_int64(0)
        return node_get_weight(
            self._get_memtable().arena.base_ptr, self.current_node_off
        )

    def get_accessor(self):
        memtable = self._get_memtable()
        self.accessor.blob_base_ptr = memtable.blob_arena.base_ptr
        self.accessor.set_row(memtable.arena.base_ptr, self.current_node_off)
        return self.accessor


# ---------------------------------------------------------------------------
# ShardCursor
# ---------------------------------------------------------------------------


class ShardCursor(BaseCursor):
    _immutable_fields_ = ["view", "schema", "is_u128", "accessor"]

    def __init__(self, shard_view):
        BaseCursor.__init__(self)
        self.view = shard_view
        self.schema = shard_view.schema
        self.is_u128 = self.schema.get_pk_column().field_type.code == types.TYPE_U128.code
        self.position = 0
        self.accessor = comparator.SoAAccessor(self.schema)
        self._skip_ghosts()

    def get_accessor(self):
        return self.accessor

    def _skip_ghosts(self):
        while self.position < self.view.count:
            if self.view.get_weight(self.position) != 0:
                self.accessor.set_row(self.view, self.position)
                return
            self.position += 1

    def seek(self, target_key):
        self.position = self.view.find_lower_bound(target_key)
        self._skip_ghosts()

    def advance(self):
        if not self.is_valid():
            return
        self.position += 1
        self._skip_ghosts()

    def key(self):
        if not self.is_valid():
            return r_uint128(-1)
        if self.is_u128:
            return self.view.get_pk_u128(self.position)
        return r_uint128(self.view.get_pk_u64(self.position))

    def weight(self):
        if not self.is_valid():
            return r_int64(0)
        return self.view.get_weight(self.position)

    def is_valid(self):
        return self.position < self.view.count

    def is_exhausted(self):
        return not self.is_valid()

    def peek_key(self):
        return self.key()


def _copy_cursors(cursors):
    res = newlist_hint(len(cursors))
    for c in cursors:
        res.append(c)
    return res


# ---------------------------------------------------------------------------
# UnifiedCursor
# ---------------------------------------------------------------------------


class UnifiedCursor(AbstractCursor):
    """
    N-way merge cursor over one or more sub-cursors (MemTable + shards).
    """

    _immutable_fields_ = ["schema", "is_single_source", "tree"]

    def __init__(self, schema, cursors):
        self.schema = schema
        self.cursors = cursors
        self.num_cursors = len(cursors)
        self.is_single_source = self.num_cursors == 1

        if not self.is_single_source:
            self.tree = tournament_tree.TournamentTree(_copy_cursors(self.cursors), schema)
        else:
            self.tree = None

        # Appendix A: Split u128 into lo/hi components for alignment safety.
        self._current_key_lo = r_uint64(0)
        self._current_key_hi = r_uint64(0)
        self._current_weight = r_int64(0)

        self._current_accessor = None
        self._valid = False
        self._find_next_non_ghost()

    def _find_next_non_ghost(self):
        if self.is_single_source:
            cursor = self.cursors[0]
            if cursor.is_valid():
                k = cursor.key()
                self._current_key_lo = r_uint64(k)
                self._current_key_hi = r_uint64(k >> 64)
                self._current_weight = cursor.weight()
                self._current_accessor = cursor.get_accessor()
                self._valid = True
            else:
                self._valid = False
            return

        while not self.tree.is_exhausted():
            min_key = self.tree.get_min_key()

            if min_key == r_uint128(-1):
                break

            indices = self.tree.get_all_indices_at_min()
            
            net_weight = r_int64(0)
            idx = 0
            num_candidates = len(indices)
            while idx < num_candidates:
                c_idx = indices[idx]
                net_weight += self.cursors[c_idx].weight()
                idx += 1

            if net_weight != r_int64(0):
                self._current_key_lo = r_uint64(min_key)
                self._current_key_hi = r_uint64(min_key >> 64)
                self._current_weight = net_weight
                self._current_accessor = self.cursors[indices[0]].get_accessor()
                self._valid = True
                return
            else:
                idx = 0
                while idx < num_candidates:
                    self.tree.advance_cursor_by_index(indices[idx])
                    idx += 1

        self._valid = False

    def seek(self, target_key):
        for c in self.cursors:
            c.seek(target_key)
        if not self.is_single_source:
            self.tree.rebuild()
        self._find_next_non_ghost()

    def advance(self):
        if not self._valid:
            return
        if self.is_single_source:
            self.cursors[0].advance()
            self._find_next_non_ghost()
            return

        indices = self.tree.get_all_indices_at_min()
        idx = 0
        num_to_advance = len(indices)
        while idx < num_to_advance:
            self.tree.advance_cursor_by_index(indices[idx])
            idx += 1
            
        self._find_next_non_ghost()

    def key(self):
        return (r_uint128(self._current_key_hi) << 64) | r_uint128(self._current_key_lo)

    def weight(self):
        return self._current_weight

    def is_valid(self):
        return self._valid

    def get_accessor(self):
        return self._current_accessor

    def close(self):
        if self.tree is not None:
            self.tree.close()
        for c in self.cursors:
            c.close()
