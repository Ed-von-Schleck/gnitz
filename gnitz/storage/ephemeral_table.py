# gnitz/storage/ephemeral_table.py

import errno
import os
from rpython.rlib import rposix
from rpython.rlib.rarithmetic import (
    r_int64,
    r_uint64,
    r_ulonglonglong as r_uint128,
    intmask,
)
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import types, errors
from gnitz.core import comparator as core_comparator
from gnitz.core.store import ZSetStore
from gnitz.core.keys import promote_to_index_key
from gnitz.core.batch import ColumnarBatchAccessor, pk_eq
from gnitz.storage import (
    index,
    flsm,
    memtable,
    mmap_posix,
    refcount,
    comparator as storage_comparator,
    cursor,
)


EPH_SHARD_PREFIX = "eph_shard_"


def _name_to_tid(name):
    """
    Standardizes name-to-TID hashing for internal state tables.
    """
    tid = 0
    for char in name:
        tid = (tid * 31 + ord(char)) & 0x7FFFFFFF
    if tid == 0:
        tid = 1
    return tid


class EphemeralTable(ZSetStore):
    """
    Scratch Z-Set storage for internal VM state or Secondary Indices.
    Bypasses the Write-Ahead Log to maximize throughput.
    Inherits directly from ZSetStore; PersistentTable inherits from this.
    """

    _immutable_fields_ = [
        "schema",
        "table_id",
        "directory",
        "name",
        "ref_counter",
    ]

    def __init__(
        self,
        directory,
        name,
        schema,
        table_id=0,
        memtable_arena_size=1 * 1024 * 1024,
        validate_checksums=False,
    ):
        self.directory = directory
        self.name = name
        self.schema = schema
        self.table_id = table_id
        self.validate_checksums = validate_checksums
        self.is_closed = False

        self.ref_counter = refcount.RefCounter()

        try:
            rposix.mkdir(directory, 0o755)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        mmap_posix.try_set_nocow_dir(directory)
        self._dirfd = rposix.open(directory, os.O_RDONLY, 0)

        self._erase_stale_shards()

        self.index = flsm.FLSMIndex(table_id, schema, self.ref_counter,
                                    directory, validate_checksums)
        self.memtable = memtable.MemTable(schema, memtable_arena_size)
        self._flush_seq = 0
        self._has_wal = True
        self.current_lsn = r_uint64(0)
        self._retract_acc = ColumnarBatchAccessor(schema)
        self._retract_soa = storage_comparator.SoAAccessor(schema)

    def _erase_stale_shards(self):
        """Delete any leftover ephemeral shard files from a previous run."""
        try:
            entries = os.listdir(self.directory)
        except OSError:
            return
        for name in entries:
            if name.startswith(EPH_SHARD_PREFIX):
                try:
                    os.unlink(self.directory + "/" + name)
                except OSError:
                    pass

    # -- ZSetStore Interface Implementation -----------------------------------

    def get_schema(self):
        return self.schema

    def create_child(self, name, schema):
        """Returns another EphemeralTable instance for recursive state."""
        # Avoid os.path.join (Appendix A §10 slicing proof failure)
        scratch_dir = self.directory + "/scratch_" + name
        tid = _name_to_tid(name)

        return EphemeralTable(
            scratch_dir,
            name,
            schema,
            table_id=tid,
            memtable_arena_size=self.memtable.max_bytes,
        )

    def _build_cursor(self):
        all_handles = self.index.all_handles_for_cursor()
        cs = newlist_hint(1 + len(all_handles))
        cs.append(cursor.MemTableCursor(self.memtable))
        for h in all_handles:
            cs.append(cursor.ShardCursor(h.view))
        return cursor.UnifiedCursor(self.schema, cs)

    def compact_if_needed(self):
        if self.index.needs_compaction:
            self._compact()

    def create_cursor(self):
        self.compact_if_needed()
        return self._build_cursor()

    def _compact(self):
        self.index.run_compact()
        self.index.ref_counter.try_cleanup()

    def _scan_shards_for_pk(self, key_lo, key_hi):
        """Walk shards for a PK via the shard index.

        Returns (net_weight_from_shards, view_or_None, first_row_idx).
        """
        total_w = r_int64(0)
        found_view = None
        found_idx = -1
        r_key = (r_uint128(key_hi) << 64) | r_uint128(key_lo)
        shard_matches = self.index.find_all_shards_and_indices(r_key)
        if shard_matches:
            for handle, row_idx in shard_matches:
                view = handle.view
                idx = row_idx
                while idx < view.count:
                    if not pk_eq(view.get_pk_lo(idx), view.get_pk_hi(idx), key_lo, key_hi):
                        break
                    total_w += view.get_weight(idx)
                    if found_view is None:
                        found_view = view
                        found_idx = idx
                    idx += 1
        return total_w, found_view, found_idx

    def has_pk(self, key_lo, key_hi):
        """
        Fast-path existence check for a Primary Key.
        Returns True if the net algebraic weight across MemTable and Shards is > 0.
        """
        total_w = r_int64(0)
        if self.memtable.may_contain_pk(key_lo, key_hi):
            mt_w, _, _ = self.memtable.lookup_pk(key_lo, key_hi)
            total_w = mt_w
        shard_w, _, _ = self._scan_shards_for_pk(key_lo, key_hi)
        total_w += shard_w
        return total_w > 0

    def retract_pk(self, key_lo, key_hi, out_batch):
        """If PK exists with positive net weight, append a retraction
        (weight=-1) of the current row to out_batch.

        Returns True if a retraction was emitted, False if PK was absent
        or had non-positive net weight.
        """
        total_w = r_int64(0)
        mt_batch = None
        mt_idx = -1
        if self.memtable.may_contain_pk(key_lo, key_hi):
            mt_w, mt_batch, mt_idx = self.memtable.lookup_pk(key_lo, key_hi)
            total_w = mt_w
        shard_w, shard_view, shard_idx = self._scan_shards_for_pk(key_lo, key_hi)
        total_w += shard_w
        if total_w <= r_int64(0):
            return False
        if mt_batch is not None:
            self._retract_acc.bind(mt_batch, mt_idx)
            out_batch.append_from_accessor(key_lo, key_hi, r_int64(-1), self._retract_acc)
        elif shard_view is not None:
            self._retract_soa.set_row(shard_view, shard_idx)
            out_batch.append_from_accessor(key_lo, key_hi, r_int64(-1), self._retract_soa)
        return True

    def get_weight(self, key_lo, key_hi, accessor):
        """
        Returns the net algebraic weight for a specific record.
        """
        total_w = r_int64(0)

        # 1. Check MemTable (Bloom-filtered)
        if self.memtable.may_contain_pk(key_lo, key_hi):
            total_w += self.memtable.find_weight_for_row(key_lo, key_hi, accessor)

        # 2. Check Columnar Shards via Index
        r_key = (r_uint128(key_hi) << 64) | r_uint128(key_lo)
        shard_matches = self.index.find_all_shards_and_indices(r_key)
        if shard_matches:
            soa = storage_comparator.SoAAccessor(self.schema)

            for handle, row_idx in shard_matches:
                view = handle.view
                idx = row_idx
                while idx < view.count:
                    if not pk_eq(view.get_pk_lo(idx), view.get_pk_hi(idx), key_lo, key_hi):
                        break

                    soa.set_row(view, idx)
                    if core_comparator.compare_rows(self.schema, soa, accessor) == 0:
                        total_w += view.get_weight(idx)
                    idx += 1

        return total_w

    def ingest_batch(self, batch):
        if self.is_closed:
            raise errors.StorageError("Table is closed")

        if batch.length() == 0:
            return

        try:
            self.memtable.upsert_batch(batch)
            if self.memtable.should_flush():
                self.flush()
        except errors.MemTableFullError:
            self.flush()
            self.memtable.upsert_batch(batch)

    def flush(self):
        """Transitions MemTable state to a temporary columnar shard."""
        if self.is_closed:
            raise errors.StorageError("Table is closed")

        self._flush_seq += 1
        shard_name = EPH_SHARD_PREFIX + "%d_%d_%d.db" % (self.table_id, os.getpid(), self._flush_seq)

        wrote = self.memtable.flush(self._dirfd, shard_name, self.table_id,
                                    durable=False)

        if not wrote:
            return ""

        shard_path = self.directory + "/" + shard_name
        h = index.ShardHandle(
            shard_path,
            self.schema,
            r_uint64(0),
            r_uint64(0),
            validate_checksums=self.validate_checksums,
        )

        if h.view.count > 0:
            self.index.add_handle(h)
        else:
            h.close()
            try:
                mmap_posix.unlinkat_c(self._dirfd, shard_name)
            except mmap_posix.MMapError:
                pass

        self.memtable.reset()

        return shard_path

    def close(self):
        if self.is_closed:
            return
        if self.memtable:
            self.memtable.free()
        if self.index:
            self.index.close_all()
        if self._dirfd != -1:
            rposix.close(self._dirfd)
            self._dirfd = -1
        self.is_closed = True

    # -- Internal / Index Specific APIs ---------------------------------------

    def ingest_projection(
        self, source_batch, source_col_idx, source_col_type, payload_accessor, is_unique
    ):
        """
        Hot-path projection kernel for secondary indices.
        Extracts index keys from source_batch and injects them into the local store.
        """
        n = source_batch.length()
        if n == 0:
            return

        acc = source_batch.get_accessor(0)

        for i in range(n):
            source_batch.bind_accessor(i, acc)

            if acc.is_null(source_col_idx):
                continue

            weight = source_batch.get_weight(i)
            if weight == r_int64(0):
                continue

            index_key = promote_to_index_key(acc, source_col_idx, source_col_type)
            index_key_lo = r_uint64(intmask(index_key))
            index_key_hi = r_uint64(intmask(index_key >> 64))

            if is_unique and weight > r_int64(0):
                if self.has_pk(index_key_lo, index_key_hi):
                    raise errors.LayoutError(
                        "Unique index violation on column index %d" % source_col_idx
                    )

            source_pk = source_batch.get_pk(i)
            # Alignment-safe u128 assignment (Appendix A §4)
            payload_accessor.pk_lo = r_uint64(intmask(source_pk))
            payload_accessor.pk_hi = r_uint64(intmask(source_pk >> 64))

            try:
                self.memtable.upsert_single(index_key_lo, index_key_hi, weight, payload_accessor)
            except errors.MemTableFullError:
                self.flush()
                self.memtable.upsert_single(index_key_lo, index_key_hi, weight, payload_accessor)

    def ingest_one(self, key_lo, key_hi, weight, accessor):
        """Cold-path injection for index backfills."""
        try:
            self.memtable.upsert_single(key_lo, key_hi, weight, accessor)
        except errors.MemTableFullError:
            self.flush()
            self.memtable.upsert_single(key_lo, key_hi, weight, accessor)
