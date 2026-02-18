# gnitz/storage/cursor.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.objectmodel import newlist_hint
from gnitz.core import types
from gnitz.storage import comparator, tournament_tree
from gnitz.storage.memtable_node import node_get_key, node_get_weight, node_get_next_off

NULL_PTR = lltype.nullptr(rffi.CCHARP.TO)


class BaseCursor(object):
    """Base class to allow homogeneous lists in RPython."""

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

    def get_row_accessor(self):
        raise NotImplementedError


class MemTableCursor(BaseCursor):
    _immutable_fields_ = ["memtable", "schema", "is_u128", "key_size", "accessor"]

    def __init__(self, memtable):
        BaseCursor.__init__(self)
        self.memtable = memtable
        self.schema = memtable.schema
        self.key_size = memtable.key_size
        self.is_u128 = self.key_size == 16
        self.current_node_off = 0
        self.exhausted = True
        self.accessor = comparator.PackedNodeAccessor(
            self.schema, memtable.blob_arena.base_ptr
        )

    def get_row_accessor(self):
        return self.accessor

    def seek(self, target_key):
        self.current_node_off = self.memtable._lower_bound_node(target_key)
        self.exhausted = self.current_node_off == 0
        if not self.exhausted:
            self.accessor.set_row(self.memtable.arena.base_ptr, self.current_node_off)

    def advance(self):
        if self.current_node_off != 0:
            base = self.memtable.arena.base_ptr
            self.current_node_off = node_get_next_off(base, self.current_node_off, 0)
        self.exhausted = self.current_node_off == 0
        if not self.exhausted:
            self.accessor.set_row(self.memtable.arena.base_ptr, self.current_node_off)

    def key(self):
        if self.exhausted:
            return r_uint128(-1)
        return node_get_key(
            self.memtable.arena.base_ptr, self.current_node_off, self.key_size
        )

    def weight(self):
        if self.exhausted:
            return r_int64(0)
        return node_get_weight(self.memtable.arena.base_ptr, self.current_node_off)

    def is_valid(self):
        return not self.exhausted

    def is_exhausted(self):
        return self.exhausted

    def peek_key(self):
        return self.key()


class ShardCursor(BaseCursor):
    _immutable_fields_ = ["view", "schema", "is_u128", "accessor"]

    def __init__(self, shard_view):
        BaseCursor.__init__(self)
        self.view = shard_view
        self.schema = shard_view.schema
        self.is_u128 = self.schema.get_pk_column().field_type == types.TYPE_U128
        self.position = 0
        self.exhausted = False
        self.accessor = comparator.SoAAccessor(self.schema)
        self._skip_ghosts()

    def get_row_accessor(self):
        return self.accessor

    def _skip_ghosts(self):
        while self.position < self.view.count:
            if self.view.get_weight(self.position) != 0:
                self.exhausted = False
                self.accessor.set_row(self.view, self.position)
                return
            self.position += 1
        self.exhausted = True

    def seek(self, target_key):
        self.position = self.view.find_lower_bound(target_key)
        self._skip_ghosts()

    def advance(self):
        if self.exhausted:
            return
        self.position += 1
        self._skip_ghosts()

    def key(self):
        if self.exhausted:
            return r_uint128(-1)
        if self.is_u128:
            return self.view.get_pk_u128(self.position)
        return r_uint128(self.view.get_pk_u64(self.position))

    def weight(self):
        if self.exhausted:
            return r_int64(0)
        return self.view.get_weight(self.position)

    def is_valid(self):
        return not self.exhausted

    def is_exhausted(self):
        return self.exhausted

    def peek_key(self):
        return self.key()


def _copy_cursors(cursors):
    return [c for c in cursors]


class UnifiedCursor(object):
    """
    The TRACE READER.
    Unified view over multiple shards and memtables. 
    Optimized for in-place tree reconstruction to eliminate malloc-churn.
    """

    _immutable_fields_ = ["schema", "is_single_source"]

    def __init__(self, schema, cursors):
        self.schema = schema
        self.cursors = cursors
        self.num_cursors = len(cursors)
        self.is_single_source = self.num_cursors == 1

        if not self.is_single_source:
            self.tree = tournament_tree.TournamentTree(_copy_cursors(self.cursors))
        else:
            self.tree = None

        self._current_key = r_uint128(0)
        self._current_weight = r_int64(0)
        self._current_accessor = None
        self._valid = False
        self._find_next_non_ghost()

    def _find_next_non_ghost(self):
        if self.is_single_source:
            cursor = self.cursors[0]
            if cursor.is_valid():
                self._current_key = cursor.key()
                self._current_weight = cursor.weight()
                self._current_accessor = cursor.get_row_accessor()
                self._valid = True
            else:
                self._valid = False
            return

        while not self.tree.is_exhausted():
            min_key = self.tree.get_min_key()
            candidates = self.tree.get_all_cursors_at_min()

            # 1. Find lexicographical minimum payload in group
            best_cursor = candidates[0]
            best_acc = best_cursor.get_row_accessor()

            idx = 1
            num_candidates = len(candidates)
            while idx < num_candidates:
                curr = candidates[idx]
                curr_acc = curr.get_row_accessor()
                if comparator.compare_rows(self.schema, curr_acc, best_acc) < 0:
                    best_cursor = curr
                    best_acc = curr_acc
                idx += 1

            # 2. Sum weight for this payload group
            net_weight = r_int64(0)
            to_advance_indices = newlist_hint(num_candidates)

            idx = 0
            while idx < num_candidates:
                curr = candidates[idx]
                curr_acc = curr.get_row_accessor()
                if comparator.compare_rows(self.schema, curr_acc, best_acc) == 0:
                    net_weight += curr.weight()
                    to_advance_indices.append(self._get_cursor_index(curr))
                idx += 1

            if net_weight != 0:
                self._current_key = min_key
                self._current_weight = net_weight
                self._current_accessor = best_acc
                self._valid = True
                return
            else:
                for c_idx in to_advance_indices:
                    self.tree.advance_cursor_by_index(c_idx)

        self._valid = False

    def _get_cursor_index(self, target):
        """Helper to map a cursor instance back to its original index."""
        for i in range(self.num_cursors):
            if self.cursors[i] is target:
                return i
        return -1

    def seek(self, target_key):
        """Monotonic seek across all sources using in-place heap rebuild."""
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

        target_accessor = self._current_accessor
        candidates = self.tree.get_all_cursors_at_min()

        to_advance_indices = newlist_hint(len(candidates))
        for c in candidates:
            if comparator.compare_rows(self.schema, c.get_row_accessor(), target_accessor) == 0:
                to_advance_indices.append(self._get_cursor_index(c))

        for c_idx in to_advance_indices:
            self.tree.advance_cursor_by_index(c_idx)

        self._find_next_non_ghost()

    def key(self):
        return self._current_key

    def weight(self):
        return self._current_weight

    def is_valid(self):
        return self._valid

    def get_accessor(self):
        return self._current_accessor

    def close(self):
        if self.tree:
            self.tree.close()
            self.tree = None
