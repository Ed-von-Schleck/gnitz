# gnitz/storage/cursor.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.objectmodel import newlist_hint
from gnitz.core import types
from gnitz.storage import tournament_tree, comparator
from gnitz.core import comparator as core_comparator
from gnitz.storage.memtable_node import node_get_key, node_get_weight, node_get_next_off
from gnitz.backend.cursor import AbstractCursor

NULL_PTR = lltype.nullptr(rffi.CCHARP.TO)


class BaseCursor(AbstractCursor):
    """
    Common base for all storage-layer cursors to satisfy RPython unification.
    Inherits from the backend interface to maintain VM compatibility.
    """

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

    def get_row_accessor(self):
        raise NotImplementedError

    def close(self):
        pass


class MemTableCursor(BaseCursor):
    """
    SkipList cursor for the MemTable.
    Implements the AbstractCursor interface for the DBSP VM.
    """

    _immutable_fields_ = ["memtable", "schema", "key_size", "accessor"]

    def __init__(self, memtable):
        BaseCursor.__init__(self)
        self.memtable = memtable
        self.schema = memtable.schema
        self.key_size = memtable.key_size
        # Position at the first real node (level-0 successor of the sentinel head).
        self.current_node_off = node_get_next_off(
            memtable.arena.base_ptr, memtable.head_off, 0
        )
        # Pre-allocate accessor for zero-allocation iteration.
        self.accessor = comparator.PackedNodeAccessor(
            self.schema, memtable.blob_arena.base_ptr
        )

    def seek(self, target_key):
        """Finds the first SkipList node >= target_key."""
        self.current_node_off = self.memtable.lower_bound_node(target_key)

    def advance(self):
        """Moves to the next node in the SkipList."""
        if self.current_node_off != 0:
            base = self.memtable.arena.base_ptr
            self.current_node_off = node_get_next_off(base, self.current_node_off, 0)

    def is_valid(self):
        """Returns True if the cursor is pointing at a valid node."""
        return self.current_node_off != 0

    def is_exhausted(self):
        return not self.is_valid()

    def peek_key(self):
        return self.key()

    def key(self):
        """Returns the Primary Key of the current node or MAX_UINT128 if invalid."""
        if self.current_node_off == 0:
            return r_uint128(-1)  # Sentinel for Tournament Tree merges
        return node_get_key(
            self.memtable.arena.base_ptr, self.current_node_off, self.key_size
        )

    def weight(self):
        """Returns the algebraic weight of the current node."""
        if self.current_node_off == 0:
            return r_int64(0)
        return node_get_weight(self.memtable.arena.base_ptr, self.current_node_off)

    def get_accessor(self):
        """Sets the state of the pre-allocated accessor and returns it."""
        self.accessor.set_row(self.memtable.arena.base_ptr, self.current_node_off)
        return self.accessor

    def get_row_accessor(self):
        return self.get_accessor()


class ShardCursor(BaseCursor):
    """
    Columnar cursor for Persistent Shards.
    Automatically skips 'Ghost' records (weight == 0).
    """

    _immutable_fields_ = ["view", "schema", "is_u128", "accessor"]

    def __init__(self, shard_view):
        BaseCursor.__init__(self)
        self.view = shard_view
        self.schema = shard_view.schema
        self.is_u128 = self.schema.get_pk_column().field_type.code == types.TYPE_U128.code
        self.position = 0
        self.accessor = comparator.SoAAccessor(self.schema)
        self._skip_ghosts()

    def get_row_accessor(self):
        return self.accessor

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
    """Helper for Tournament Tree initialization to satisfy RPython list constraints."""
    res = newlist_hint(len(cursors))
    for c in cursors:
        res.append(c)
    return res


class UnifiedCursor(AbstractCursor):
    """
    Authoritative cursor providing a merged view over multiple shards and memtables.
    Handles algebraic weight summation and payload grouping.
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
                self._current_accessor = cursor.get_accessor()
                self._valid = True
            else:
                self._valid = False
            return

        while not self.tree.is_exhausted():
            min_key = self.tree.get_min_key()
            indices = self.tree.get_all_indices_at_min()

            # 1. Find lexicographical minimum payload in group
            best_idx = indices[0]
            best_cursor = self.cursors[best_idx]
            best_acc = best_cursor.get_accessor()

            idx = 1
            num_candidates = len(indices)
            while idx < num_candidates:
                curr = self.cursors[indices[idx]]
                curr_acc = curr.get_accessor()
                if core_comparator.compare_rows(self.schema, curr_acc, best_acc) < 0:
                    best_cursor = curr
                    best_acc = curr_acc
                idx += 1

            # 2. Sum weight for this payload group
            net_weight = r_int64(0)
            to_advance = newlist_hint(num_candidates)

            idx = 0
            while idx < num_candidates:
                c_idx = indices[idx]
                curr = self.cursors[c_idx]
                curr_acc = curr.get_accessor()
                if core_comparator.compare_rows(self.schema, curr_acc, best_acc) == 0:
                    net_weight += curr.weight()
                    to_advance.append(c_idx)
                idx += 1

            if net_weight != 0:
                self._current_key = min_key
                self._current_weight = net_weight
                self._current_accessor = best_acc
                self._valid = True
                return
            else:
                # Ghost detected: advance past this specific payload group and continue searching
                for c_idx in to_advance:
                    self.tree.advance_cursor_by_index(c_idx)

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

        target_accessor = self._current_accessor
        indices = self.tree.get_all_indices_at_min()

        to_advance = newlist_hint(len(indices))
        for c_idx in indices:
            cursor = self.cursors[c_idx]
            if (
                core_comparator.compare_rows(
                    self.schema, cursor.get_accessor(), target_accessor
                )
                == 0
            ):
                to_advance.append(c_idx)

        for c_idx in to_advance:
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
        for c in self.cursors:
            c.close()
