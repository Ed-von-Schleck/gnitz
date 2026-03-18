# gnitz/storage/flsm.py

import os
from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rlib.objectmodel import newlist_hint
from gnitz.storage.index import ShardHandle
from gnitz.storage.metadata import ManifestEntry
from gnitz.storage import manifest
from gnitz.storage import compactor

MAX_LEVELS = 3
L0_COMPACT_THRESHOLD = 4
GUARD_FILE_THRESHOLD = 4
LMAX_FILE_THRESHOLD = 1
LEVEL_SIZE_RATIO = 4
L1_TARGET_FILES = 16

MAX_UINT128 = (r_uint128(0xFFFFFFFFFFFFFFFF) << 64) | r_uint128(0xFFFFFFFFFFFFFFFF)


class LevelGuard(object):
    def __init__(self, guard_key_lo, guard_key_hi):
        self.guard_key_lo = r_uint64(guard_key_lo)
        self.guard_key_hi = r_uint64(guard_key_hi)
        self.handles = newlist_hint(4)

    def guard_key(self):
        return (r_uint128(self.guard_key_hi) << 64) | r_uint128(self.guard_key_lo)

    def needs_horizontal_compact(self, threshold):
        return len(self.handles) > threshold

    def add_handle(self, ref_counter, handle):
        ref_counter.acquire(handle.filename)
        self.handles.append(handle)

    def close_all(self, ref_counter):
        for h in self.handles:
            h.close()
            ref_counter.release(h.filename)
        self.handles = newlist_hint(0)


class FLSMLevel(object):
    def __init__(self, level_num):
        self.level_num = level_num
        self.guards = newlist_hint(8)

    def find_guard_idx(self, key):
        """Binary search: index of the guard whose range covers key. -1 if empty or key < first guard."""
        n = len(self.guards)
        if n == 0:
            return -1
        lo, hi = 0, n - 1
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if self.guards[mid].guard_key() <= key:
                lo = mid
            else:
                hi = mid - 1
        if self.guards[lo].guard_key() <= key:
            return lo
        return -1

    def find_guards_for_range(self, range_min, range_max):
        """Returns list of guard indices whose key range overlaps [range_min, range_max]."""
        n = len(self.guards)
        result = newlist_hint(4)
        for i in range(n):
            gk = self.guards[i].guard_key()
            if gk > range_max:
                break
            if i + 1 < n:
                next_gk = self.guards[i + 1].guard_key()
            else:
                next_gk = MAX_UINT128
            if next_gk > range_min:
                result.append(i)
        return result

    def insert_guard_sorted(self, guard):
        """Insert guard into sorted list (rebuild since RPython has no list.insert)."""
        gk = guard.guard_key()
        new_guards = newlist_hint(len(self.guards) + 1)
        inserted = False
        for g in self.guards:
            if not inserted and g.guard_key() > gk:
                new_guards.append(guard)
                inserted = True
            new_guards.append(g)
        if not inserted:
            new_guards.append(guard)
        self.guards = new_guards

    def total_file_count(self):
        n = 0
        for g in self.guards:
            n += len(g.handles)
        return n

    def close_all(self, ref_counter):
        for g in self.guards:
            g.close_all(ref_counter)
        self.guards = newlist_hint(0)


class FLSMIndex(object):
    def __init__(self, table_id, schema, ref_counter, output_dir, validate_checksums=False):
        self.table_id = table_id
        self.schema = schema
        self.ref_counter = ref_counter
        self.output_dir = output_dir
        self.validate_checksums = validate_checksums
        self.handles = newlist_hint(8)       # L0 flat pool
        self.levels = newlist_hint(MAX_LEVELS - 1)  # L1..Lmax
        self.needs_compaction = False
        self._compact_seq = 0

    # L0 management

    def add_handle(self, handle):
        self.ref_counter.acquire(handle.filename)
        self.handles.append(handle)
        self._sort_l0()
        self._update_flags()

    def add_l0_handle(self, handle):
        self.add_handle(handle)

    def _sort_l0(self):
        for i in range(1, len(self.handles)):
            h = self.handles[i]
            k = h.get_min_key()
            j = i - 1
            while j >= 0 and self.handles[j].get_min_key() > k:
                self.handles[j + 1] = self.handles[j]
                j -= 1
            self.handles[j + 1] = h

    def _update_flags(self):
        self.needs_compaction = len(self.handles) > L0_COMPACT_THRESHOLD

    def replace_handles(self, old_filenames, new_handle):
        """Replaces L0 handles after flat compaction (same interface as ShardIndex)."""
        new_list = newlist_hint(len(self.handles))
        for h in self.handles:
            superseded = False
            for old_fn in old_filenames:
                if h.filename == old_fn:
                    superseded = True
                    break
            if superseded:
                h.close()
                self.ref_counter.release(h.filename)
            else:
                new_list.append(h)
        self.ref_counter.acquire(new_handle.filename)
        new_list.append(new_handle)
        self.handles = new_list
        self._sort_l0()
        self._update_flags()

    # Level management

    def get_or_create_level(self, level_num):
        idx = level_num - 1
        while len(self.levels) <= idx:
            self.levels.append(FLSMLevel(len(self.levels) + 1))
        return self.levels[idx]

    # Read path: L0 + all levels

    def find_all_shards_and_indices(self, key):
        results = newlist_hint(8)
        for h in self.handles:
            if h.get_min_key() <= key <= h.get_max_key():
                if h.xor8_filter is not None and not h.xor8_filter.may_contain(key):
                    continue
                row_idx = h.view.find_row_index(key)
                if row_idx != -1:
                    results.append((h, row_idx))
        for level in self.levels:
            g_idx = level.find_guard_idx(key)
            if g_idx == -1:
                continue
            guard = level.guards[g_idx]
            for h in guard.handles:
                if h.get_min_key() <= key <= h.get_max_key():
                    if h.xor8_filter is not None and not h.xor8_filter.may_contain(key):
                        continue
                    row_idx = h.view.find_row_index(key)
                    if row_idx != -1:
                        results.append((h, row_idx))
        return results

    def all_handles_for_cursor(self):
        n = len(self.handles)
        for level in self.levels:
            n += level.total_file_count()
        result = newlist_hint(n)
        for h in self.handles:
            result.append(h)
        for level in self.levels:
            for guard in level.guards:
                for h in guard.handles:
                    result.append(h)
        return result

    # Metadata

    def get_metadata_list(self):
        result = newlist_hint(64)
        for h in self.handles:
            result.append(ManifestEntry(self.table_id, h.filename,
                h.get_min_key(), h.get_max_key(), h.min_lsn, h.lsn,
                level=0, guard_key_lo=r_uint64(0), guard_key_hi=r_uint64(0)))
        for level in self.levels:
            for guard in level.guards:
                for h in guard.handles:
                    result.append(ManifestEntry(self.table_id, h.filename,
                        h.get_min_key(), h.get_max_key(), h.min_lsn, h.lsn,
                        level=level.level_num,
                        guard_key_lo=guard.guard_key_lo,
                        guard_key_hi=guard.guard_key_hi))
        return result

    def max_lsn(self):
        result = r_uint64(0)
        for h in self.handles:
            if h.lsn > result:
                result = h.lsn
        for level in self.levels:
            for guard in level.guards:
                for h in guard.handles:
                    if h.lsn > result:
                        result = h.lsn
        return result

    def populate_from_reader(self, table_id, reader):
        """Load shard handles from a ManifestReader into this index."""
        for entry in reader.iterate_entries():
            if entry.table_id != table_id:
                continue
            handle = ShardHandle(entry.shard_filename, self.schema,
                                 entry.min_lsn, entry.max_lsn, self.validate_checksums)
            if entry.level == 0:
                self.add_l0_handle(handle)
            else:
                level = self.get_or_create_level(entry.level)
                gk_lo = entry.guard_key_lo
                gk_hi = entry.guard_key_hi
                gk = (r_uint128(gk_hi) << 64) | r_uint128(gk_lo)
                g_idx = level.find_guard_idx(gk)
                if g_idx == -1 or level.guards[g_idx].guard_key() != gk:
                    g = LevelGuard(gk_lo, gk_hi)
                    level.insert_guard_sorted(g)
                    g_idx = level.find_guard_idx(gk)
                level.guards[g_idx].add_handle(self.ref_counter, handle)

    def commit_l0_to_l1(self, l0_input_filenames, guard_outputs, max_lsn):
        """Clear compacted L0 handles and install output handles into L1 guards."""
        new_l0 = newlist_hint(len(self.handles))
        for h in self.handles:
            found = False
            for fn in l0_input_filenames:
                if h.filename == fn:
                    found = True
                    break
            if found:
                h.close()
                self.ref_counter.release(h.filename)
            else:
                new_l0.append(h)
        self.handles = new_l0

        l1 = self.get_or_create_level(1)
        i = 0
        while i < len(guard_outputs):
            gk_lo, gk_hi, filename = guard_outputs[i]
            new_handle = ShardHandle(filename, self.schema, r_uint64(0), max_lsn,
                                     self.validate_checksums)
            gk = (r_uint128(gk_hi) << 64) | r_uint128(gk_lo)
            g_idx = l1.find_guard_idx(gk)
            if g_idx == -1 or l1.guards[g_idx].guard_key() != gk:
                new_guard = LevelGuard(gk_lo, gk_hi)
                l1.insert_guard_sorted(new_guard)
                g_idx = l1.find_guard_idx(gk)
            l1.guards[g_idx].add_handle(self.ref_counter, new_handle)
            i += 1

        self._update_flags()

    def run_compact(self):
        """L0 overflow → tournament-tree merge → rows routed into L1 guards."""
        if not self.needs_compaction:
            return

        self._compact_seq += 1
        l0_max_lsn = r_uint64(0)
        l0_input_files = newlist_hint(len(self.handles))
        for h in self.handles:
            l0_input_files.append(h.filename)
            if h.lsn > l0_max_lsn:
                l0_max_lsn = h.lsn
        if l0_max_lsn > r_uint64(0):
            lsn_tag = intmask(l0_max_lsn)
        else:
            lsn_tag = self._compact_seq

        guard_outputs = newlist_hint(0)
        try:
            guard_outputs = compactor.compact_l0_to_l1(
                self, self.output_dir, self.schema, self.table_id, lsn_tag,
                self.validate_checksums,
            )
            self.commit_l0_to_l1(l0_input_files, guard_outputs, l0_max_lsn)
            for fn in l0_input_files:
                self.ref_counter.mark_for_deletion(fn)
            self.ref_counter.try_cleanup()
        except Exception as e:
            i = 0
            while i < len(guard_outputs):
                gk_lo_x, gk_hi_x, fn = guard_outputs[i]
                if os.path.exists(fn):
                    try:
                        os.unlink(fn)
                    except OSError:
                        pass
                i += 1
            raise e

    def close_all(self):
        for h in self.handles:
            h.close()
            self.ref_counter.release(h.filename)
        self.handles = newlist_hint(0)
        for level in self.levels:
            level.close_all(self.ref_counter)
        self.levels = newlist_hint(0)


def index_from_manifest(manifest_path, table_id, schema, ref_counter,
                        validate_checksums=False, output_dir=""):
    """Factory to initialize an FLSMIndex from a Manifest file."""
    idx = FLSMIndex(table_id, schema, ref_counter, output_dir, validate_checksums)
    reader = manifest.ManifestReader(manifest_path)
    try:
        idx.populate_from_reader(table_id, reader)
    finally:
        reader.close()
    return idx
