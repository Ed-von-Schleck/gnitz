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
        key_lo = r_uint64(intmask(key))
        key_hi = r_uint64(intmask(key >> 64))
        results = newlist_hint(8)
        for h in self.handles:
            if h.get_min_key() <= key <= h.get_max_key():
                if h.xor8_filter is not None and not h.xor8_filter.may_contain(key_lo, key_hi):
                    continue
                row_idx = h.view.find_row_index(key_lo, key_hi)
                if row_idx != -1:
                    results.append((h, row_idx))
        for level in self.levels:
            g_idx = level.find_guard_idx(key)
            if g_idx == -1:
                continue
            guard = level.guards[g_idx]
            for h in guard.handles:
                if h.get_min_key() <= key <= h.get_max_key():
                    if h.xor8_filter is not None and not h.xor8_filter.may_contain(key_lo, key_hi):
                        continue
                    row_idx = h.view.find_row_index(key_lo, key_hi)
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

    def compact_guards_if_needed(self):
        """
        Scans all level guards. Runs horizontal compaction on any guard exceeding its threshold.
        L2 (Lmax, level_num == MAX_LEVELS-1 == 2) uses LMAX_FILE_THRESHOLD=1.
        All other levels use GUARD_FILE_THRESHOLD=4.
        """
        for level in self.levels:
            if level.level_num == MAX_LEVELS - 1:
                threshold = LMAX_FILE_THRESHOLD
            else:
                threshold = GUARD_FILE_THRESHOLD
            g_idx = 0
            while g_idx < len(level.guards):
                guard = level.guards[g_idx]
                if guard.needs_horizontal_compact(threshold):
                    self._compact_one_guard(level, g_idx)
                g_idx += 1

    def _compact_one_guard(self, level, guard_idx):
        guard = level.guards[guard_idx]
        self._compact_seq += 1

        guard_max_lsn = r_uint64(0)
        for h in guard.handles:
            if h.lsn > guard_max_lsn:
                guard_max_lsn = h.lsn

        out_path = self.output_dir + "/hcomp_%d_L%d_G%d_%d.db" % (
            self.table_id, level.level_num,
            intmask(guard.guard_key_lo), self._compact_seq)

        try:
            compactor.compact_guard_horizontal(
                guard, out_path, self.schema, self.table_id, self.validate_checksums)
            new_handle = ShardHandle(out_path, self.schema, r_uint64(0), guard_max_lsn,
                                     self.validate_checksums)
            # Acquire new handle BEFORE releasing old ones — exception-safe ordering:
            # if acquire fails, guard.handles is still the valid original list.
            self.ref_counter.acquire(new_handle.filename)
            old_handles = guard.handles
            new_handles = newlist_hint(1)
            new_handles.append(new_handle)
            guard.handles = new_handles          # atomic swap: guard is valid from here on
            for h in old_handles:
                h.close()
                self.ref_counter.release(h.filename)
                self.ref_counter.mark_for_deletion(h.filename)
            self.ref_counter.try_cleanup()
        except Exception as e:
            if os.path.exists(out_path):
                try:
                    os.unlink(out_path)
                except OSError:
                    pass
            raise e

    def compact_guard_vertical_if_needed(self, src_level_num):
        if src_level_num < 1 or src_level_num >= MAX_LEVELS:
            return
        src_level = self.levels[src_level_num - 1]

        # Phase A: select worst guard
        worst_idx = -1
        worst_count = 0
        for i in range(len(src_level.guards)):
            n = len(src_level.guards[i].handles)
            if n > worst_count:
                worst_count = n
                worst_idx = i
        if worst_idx == -1 or worst_count <= 1:
            return   # Nothing to promote

        # Phase B: compute LSN tag + collect overlapping dest guards
        src_guard = src_level.guards[worst_idx]
        dest_level = self.get_or_create_level(src_level_num + 1)

        vert_max_lsn = r_uint64(0)
        for h in src_guard.handles:
            if h.lsn > vert_max_lsn:
                vert_max_lsn = h.lsn

        src_min = src_guard.guard_key()
        if worst_idx + 1 < len(src_level.guards):
            src_max_bound = src_level.guards[worst_idx + 1].guard_key() - r_uint128(1)
        else:
            src_max_bound = MAX_UINT128

        dest_guard_indices = dest_level.find_guards_for_range(src_min, src_max_bound)
        dest_guards = newlist_hint(len(dest_guard_indices))
        for di in dest_guard_indices:
            dg = dest_level.guards[di]
            dest_guards.append(dg)
            for h in dg.handles:
                if h.lsn > vert_max_lsn:
                    vert_max_lsn = h.lsn

        if vert_max_lsn > r_uint64(0):
            lsn_tag = intmask(vert_max_lsn)
        else:
            self._compact_seq += 1       # fallback when all inputs have lsn=0
            lsn_tag = self._compact_seq

        # Phase C: compact BEFORE mutating in-memory state (exception safety)
        results = newlist_hint(0)
        try:
            results = compactor.compact_guard_vertical(
                src_guard, dest_guards, self.output_dir, self.schema, self.table_id,
                src_level_num + 1, lsn_tag, self.validate_checksums)
        except Exception as e:
            i = 0
            while i < len(results):
                gk_lo_x, gk_hi_x, fn = results[i]
                if os.path.exists(fn):
                    try:
                        os.unlink(fn)
                    except OSError:
                        pass
                i += 1
            raise e

        # Phase D: tear down old handles and guards

        # Close src_guard handles
        for h in src_guard.handles:
            h.close()
            self.ref_counter.release(h.filename)
            self.ref_counter.mark_for_deletion(h.filename)

        # Close dest_guard handles AND clear the list (edge case: stale handle refs)
        for dg in dest_guards:
            for h in dg.handles:
                h.close()
                self.ref_counter.release(h.filename)
                self.ref_counter.mark_for_deletion(h.filename)
            dg.handles = newlist_hint(0)   # CRITICAL: prevent stale closed handles

        # Remove src_guard from src_level (promoted out)
        new_src_guards = newlist_hint(len(src_level.guards) - 1)
        for i in range(len(src_level.guards)):
            if i != worst_idx:
                new_src_guards.append(src_level.guards[i])
        src_level.guards = new_src_guards

        # Remove old dest_guards from dest_level
        replaced_lo = newlist_hint(len(dest_guards))
        replaced_hi = newlist_hint(len(dest_guards))
        for dg in dest_guards:
            replaced_lo.append(dg.guard_key_lo)
            replaced_hi.append(dg.guard_key_hi)
        new_dest_guards = newlist_hint(len(dest_level.guards))
        for g in dest_level.guards:
            is_replaced = False
            for ri in range(len(replaced_lo)):
                if g.guard_key_lo == replaced_lo[ri] and g.guard_key_hi == replaced_hi[ri]:
                    is_replaced = True
                    break
            if not is_replaced:
                new_dest_guards.append(g)
        dest_level.guards = new_dest_guards

        # Phase E: install outputs + Lazy Leveling enforcement
        i = 0
        while i < len(results):
            dest_gk_lo, dest_gk_hi, out_fn = results[i]
            new_handle = ShardHandle(out_fn, self.schema, r_uint64(0), vert_max_lsn,
                                     self.validate_checksums)
            dest_gk = (r_uint128(dest_gk_hi) << 64) | r_uint128(dest_gk_lo)
            g_idx = dest_level.find_guard_idx(dest_gk)
            if g_idx == -1 or dest_level.guards[g_idx].guard_key() != dest_gk:
                new_guard = LevelGuard(dest_gk_lo, dest_gk_hi)
                dest_level.insert_guard_sorted(new_guard)
                g_idx = dest_level.find_guard_idx(dest_gk)
            dest_level.guards[g_idx].add_handle(self.ref_counter, new_handle)
            i += 1

        self.ref_counter.try_cleanup()
        self._update_flags()

        # Lazy Leveling safety net: Z=1 at Lmax (each L2 guard holds exactly one file)
        if dest_level.level_num == MAX_LEVELS - 1:
            g_idx = 0
            while g_idx < len(dest_level.guards):
                if dest_level.guards[g_idx].needs_horizontal_compact(LMAX_FILE_THRESHOLD):
                    self._compact_one_guard(dest_level, g_idx)
                g_idx += 1

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

        # Phase 3: horizontal compaction within guards
        self.compact_guards_if_needed()

        # Phase 4: L1→L2 vertical if L1 is overfull
        if len(self.levels) > 0:
            l1 = self.levels[0]
            if l1.total_file_count() > L1_TARGET_FILES:
                self.compact_guard_vertical_if_needed(1)

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
