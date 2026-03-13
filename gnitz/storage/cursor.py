# gnitz/storage/cursor.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_ulonglonglong as r_uint128, r_uint64, intmask
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.objectmodel import newlist_hint
from gnitz.core import types
from gnitz.core.store import AbstractCursor
from gnitz.storage import tournament_tree, comparator
from gnitz.core import comparator as core_comparator
from gnitz.core.batch import ColumnarBatchAccessor

NULL_PTR = lltype.nullptr(rffi.CCHARP.TO)


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

    def get_accessor(self):
        raise NotImplementedError

    def close(self):
        pass


# ---------------------------------------------------------------------------
# SortedBatchCursor
# ---------------------------------------------------------------------------


class SortedBatchCursor(BaseCursor):
    """Sequential iterator over a sorted ArenaZSetBatch."""

    _immutable_fields_ = ["_batch", "accessor"]

    def __init__(self, batch):
        BaseCursor.__init__(self)
        self._batch = batch
        self._pos = 0
        self.accessor = ColumnarBatchAccessor(batch._schema)

    def seek(self, target_key):
        lo = 0
        hi = self._batch.length()
        while lo < hi:
            mid = (lo + hi) >> 1
            if self._batch.get_pk(mid) < target_key:
                lo = mid + 1
            else:
                hi = mid
        self._pos = lo

    def advance(self):
        self._pos += 1

    def is_valid(self):
        return self._pos < self._batch.length()

    def key(self):
        if self._pos >= self._batch.length():
            return r_uint128(-1)
        return self._batch.get_pk(self._pos)

    def weight(self):
        if self._pos >= self._batch.length():
            return r_int64(0)
        return self._batch.get_weight(self._pos)

    def get_accessor(self):
        self.accessor.bind(self._batch, self._pos)
        return self.accessor

    def peek_key(self):
        if self._pos < self._batch.length():
            return self._batch.get_pk(self._pos)
        return r_uint128(0)

    def is_exhausted(self):
        return self._pos >= self._batch.length()

    def bind_to(self, acc):
        """Bind an external ColumnarBatchAccessor to this cursor's current position."""
        acc.bind(self._batch, self._pos)

    def close(self):
        pass


# ---------------------------------------------------------------------------
# MemTableCursor
# ---------------------------------------------------------------------------


class MemTableCursor(BaseCursor):
    """
    Cursor over a consolidated snapshot of a MemTable's sorted runs.
    Creates an independent snapshot to avoid use-after-free when flush()
    frees runs while the cursor is live.
    """

    _immutable_fields_ = ["schema", "_snapshot", "_accessor"]

    def __init__(self, memtable):
        from gnitz.storage.memtable import _merge_runs_to_consolidated

        BaseCursor.__init__(self)
        self.schema = memtable.schema

        # Build list of all sorted runs (including sorted accumulator).
        num_runs = len(memtable.runs)
        has_acc = memtable._accumulator.length() > 0
        all_runs = newlist_hint(num_runs + 1)
        for ri in range(num_runs):
            all_runs.append(memtable.runs[ri])
        temp_sorted_acc = None
        if has_acc:
            temp_sorted_acc = memtable._accumulator.to_sorted()
            all_runs.append(temp_sorted_acc)

        self._snapshot = _merge_runs_to_consolidated(all_runs, memtable.schema)

        # Free temporary sorted accumulator if to_sorted() created a new batch
        if temp_sorted_acc is not None and temp_sorted_acc is not memtable._accumulator:
            temp_sorted_acc.free()

        self._pos = 0
        self._accessor = ColumnarBatchAccessor(memtable.schema)

    def seek(self, target_key):
        lo = 0
        hi = self._snapshot.length()
        while lo < hi:
            mid = (lo + hi) >> 1
            if self._snapshot.get_pk(mid) < target_key:
                lo = mid + 1
            else:
                hi = mid
        self._pos = lo

    def advance(self):
        self._pos += 1

    def is_valid(self):
        return self._pos < self._snapshot.length()

    def key(self):
        if self._pos >= self._snapshot.length():
            return r_uint128(-1)
        return self._snapshot.get_pk(self._pos)

    def weight(self):
        if self._pos >= self._snapshot.length():
            return r_int64(0)
        return self._snapshot.get_weight(self._pos)

    def get_accessor(self):
        self._accessor.bind(self._snapshot, self._pos)
        return self._accessor

    def close(self):
        self._snapshot.free()


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
                self._current_key_lo = r_uint64(intmask(k))
                self._current_key_hi = r_uint64(intmask(k >> 64))
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

            num_candidates = self.tree.get_all_indices_at_min()

            net_weight = r_int64(0)
            idx = 0
            while idx < num_candidates:
                c_idx = self.tree._min_indices[idx]
                net_weight += self.cursors[c_idx].weight()
                idx += 1

            if net_weight != r_int64(0):
                self._current_key_lo = r_uint64(intmask(min_key))
                self._current_key_hi = r_uint64(intmask(min_key >> 64))
                self._current_weight = net_weight
                self._current_accessor = self.cursors[self.tree._min_indices[0]].get_accessor()
                self._valid = True
                return
            else:
                idx = 0
                while idx < num_candidates:
                    self.tree.advance_cursor_by_index(self.tree._min_indices[idx])
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

        # Reuse cached indices from the last _find_next_non_ghost() call
        # instead of traversing the heap again.
        count = self.tree._min_count
        idx = 0
        while idx < count:
            self.tree.advance_cursor_by_index(self.tree._min_indices[idx])
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
