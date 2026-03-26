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
from gnitz.core.comparator import RowAccessor
from gnitz.core.store import ZSetStore
from gnitz.core.keys import promote_to_index_key
from gnitz.core.batch import ColumnarBatchAccessor, pk_eq
from gnitz.storage import (
    engine_ffi,
    memtable,
    mmap_posix,
    cursor,
)
from gnitz.core import strings as string_logic


EPH_SHARD_PREFIX = "eph_shard_"

# Maximum number of shards to return from all_shard_ptrs / find_pk
_MAX_SHARDS = 4096
_MAX_PK_RESULTS = 256

NULL_PTR = lltype.nullptr(rffi.CCHARP.TO)


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


class _ShardPtrView(object):
    """Thin wrapper around a raw MappedShard pointer from the Rust ShardIndex.

    Presents the same ``_handle`` and ``count`` attributes that
    RustUnifiedCursor expects so that shard pointers obtained from
    gnitz_shard_index_all_shard_ptrs can be passed directly to
    gnitz_read_cursor_create.
    """

    _immutable_fields_ = ["_handle", "count"]

    def __init__(self, raw_ptr):
        self._handle = raw_ptr
        self.count = intmask(engine_ffi._shard_row_count(raw_ptr))


class ShardRowAccessor(RowAccessor):
    """Accesses a single row from a Rust MappedShard via the existing
    gnitz_shard_col_ptr / gnitz_shard_blob_ptr / etc. FFI functions.

    The caller must ensure the shard pointer remains valid for the
    lifetime of this accessor.
    """

    _immutable_fields_ = ["schema"]

    def __init__(self, schema):
        self.schema = schema
        self._shard_ptr = lltype.nullptr(rffi.VOIDP.TO)
        self._row_idx = 0

    def set_row(self, shard_ptr, row_idx):
        self._shard_ptr = shard_ptr
        self._row_idx = row_idx

    def is_null(self, col_idx):
        schema = self.schema
        if col_idx == schema.pk_index:
            return False
        if not schema.has_nullable:
            return False
        null_word = r_uint64(engine_ffi._shard_get_null_word(
            self._shard_ptr, rffi.cast(rffi.INT, self._row_idx)))
        payload_idx = col_idx if col_idx < schema.pk_index else col_idx - 1
        return bool(r_uint64(null_word) & (r_uint64(1) << payload_idx))

    def _get_ptr(self, col_idx):
        stride = self.schema.columns[col_idx].field_type.size
        return engine_ffi._shard_col_ptr(
            self._shard_ptr,
            rffi.cast(rffi.INT, self._row_idx),
            rffi.cast(rffi.INT, col_idx),
            rffi.cast(rffi.INT, stride),
        )

    def get_int(self, col_idx):
        ptr = self._get_ptr(col_idx)
        sz = self.schema.columns[col_idx].field_type.size
        if sz == 8:
            return rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.LONGLONGP, ptr)[0])
        elif sz == 4:
            return rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.UINTP, ptr)[0])
        elif sz == 2:
            return rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.USHORTP, ptr)[0])
        elif sz == 1:
            return rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.UCHARP, ptr)[0])
        return r_uint64(0)

    def get_int_signed(self, col_idx):
        ptr = self._get_ptr(col_idx)
        sz = self.schema.columns[col_idx].field_type.size
        if sz == 8:
            return rffi.cast(rffi.LONGLONG, rffi.cast(rffi.LONGLONGP, ptr)[0])
        elif sz == 4:
            return rffi.cast(rffi.LONGLONG, rffi.cast(rffi.INTP, ptr)[0])
        elif sz == 2:
            return rffi.cast(rffi.LONGLONG, rffi.cast(rffi.SHORTP, ptr)[0])
        elif sz == 1:
            return rffi.cast(rffi.LONGLONG, rffi.cast(rffi.SIGNEDCHARP, ptr)[0])
        return r_int64(0)

    def get_float(self, col_idx):
        ptr = self._get_ptr(col_idx)
        sz = self.schema.columns[col_idx].field_type.size
        if sz == 4:
            return float(rffi.cast(rffi.FLOATP, ptr)[0])
        return float(rffi.cast(rffi.DOUBLEP, ptr)[0])

    def get_u128(self, col_idx):
        ptr = self._get_ptr(col_idx)
        lo = rffi.cast(rffi.ULONGLONGP, ptr)
        hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))
        return (r_uint128(hi[0]) << 64) | r_uint128(lo[0])

    def get_u128_lo(self, col_idx):
        ptr = self._get_ptr(col_idx)
        lo = rffi.cast(rffi.ULONGLONGP, ptr)
        return r_uint64(lo[0])

    def get_u128_hi(self, col_idx):
        ptr = self._get_ptr(col_idx)
        hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))
        return r_uint64(hi[0])

    def get_str_struct(self, col_idx):
        ptr = self._get_ptr(col_idx)
        blob_ptr = engine_ffi._shard_blob_ptr(self._shard_ptr)

        if not ptr:
            return (0, 0, NULL_PTR, blob_ptr, None)

        u32_ptr = rffi.cast(rffi.UINTP, ptr)
        length = rffi.cast(lltype.Signed, u32_ptr[0])
        prefix = rffi.cast(lltype.Signed, u32_ptr[1])

        s = None
        if False:
            s = ""
        return (length, prefix, ptr, blob_ptr, s)

    def get_col_ptr(self, col_idx):
        return self._get_ptr(col_idx)


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

        try:
            rposix.mkdir(directory, 0o755)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        mmap_posix.try_set_nocow_dir(directory)
        self._dirfd = rposix.open(directory, os.O_RDONLY, 0)

        self._erase_stale_shards()

        # Create Rust ShardIndex handle
        self._schema_buf = engine_ffi.pack_schema(schema)
        dir_c = rffi.str2charp(directory)
        try:
            self._index_handle = engine_ffi._shard_index_create(
                rffi.cast(rffi.UINT, table_id),
                dir_c,
                rffi.cast(rffi.VOIDP, self._schema_buf),
            )
        finally:
            rffi.free_charp(dir_c)

        self.memtable = memtable.MemTable(schema, memtable_arena_size)
        self._flush_seq = 0
        self._has_wal = True
        self.current_lsn = r_uint64(0)
        self._retract_acc = ColumnarBatchAccessor(schema)
        self._retract_soa = ShardRowAccessor(schema)

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
        snapshot = self.memtable.get_consolidated_snapshot()
        # Get all shard pointers from the Rust ShardIndex
        out_ptrs = lltype.malloc(rffi.VOIDPP.TO, _MAX_SHARDS, flavor="raw")
        try:
            n_shards = intmask(engine_ffi._shard_index_all_shard_ptrs(
                self._index_handle,
                out_ptrs,
                rffi.cast(rffi.UINT, _MAX_SHARDS),
            ))
            shard_views = newlist_hint(n_shards)
            for i in range(n_shards):
                shard_views.append(_ShardPtrView(out_ptrs[i]))
        finally:
            lltype.free(out_ptrs, flavor="raw")
        return cursor.RustUnifiedCursor(self.schema, shard_views, [snapshot])

    def all_shard_views_for_cursor(self):
        """Return a list of _ShardPtrView wrappers for all shards in the index.

        Used by PartitionedTable.create_cursor() to gather shard pointers
        across multiple partitions.
        """
        out_ptrs = lltype.malloc(rffi.VOIDPP.TO, _MAX_SHARDS, flavor="raw")
        try:
            n_shards = intmask(engine_ffi._shard_index_all_shard_ptrs(
                self._index_handle,
                out_ptrs,
                rffi.cast(rffi.UINT, _MAX_SHARDS),
            ))
            result = newlist_hint(n_shards)
            for i in range(n_shards):
                result.append(_ShardPtrView(out_ptrs[i]))
        finally:
            lltype.free(out_ptrs, flavor="raw")
        return result

    def compact_if_needed(self):
        if intmask(engine_ffi._shard_index_needs_compaction(self._index_handle)):
            self._compact()

    def create_cursor(self):
        self.compact_if_needed()
        return self._build_cursor()

    def _compact(self):
        rc = intmask(engine_ffi._shard_index_compact(self._index_handle))
        if rc < 0:
            raise errors.StorageError("ShardIndex compact failed (error %d)" % rc)
        engine_ffi._shard_index_try_cleanup(self._index_handle)

    def _scan_shards_for_pk(self, key_lo, key_hi):
        """Walk shards for a PK via the Rust shard index.

        Returns (net_weight_from_shards, shard_ptr_or_None, first_row_idx).
        shard_ptr_or_None is a rffi.VOIDP pointing to the MappedShard.
        """
        total_w = r_int64(0)
        found_ptr = lltype.nullptr(rffi.VOIDP.TO)
        found_idx = -1

        out_shard_ptrs = lltype.malloc(rffi.VOIDPP.TO, _MAX_PK_RESULTS, flavor="raw")
        out_row_indices = lltype.malloc(rffi.INTP.TO, _MAX_PK_RESULTS, flavor="raw")
        try:
            n_matches = intmask(engine_ffi._shard_index_find_pk(
                self._index_handle,
                rffi.cast(rffi.ULONGLONG, key_lo),
                rffi.cast(rffi.ULONGLONG, key_hi),
                out_shard_ptrs,
                out_row_indices,
                rffi.cast(rffi.UINT, _MAX_PK_RESULTS),
            ))
            for mi in range(n_matches):
                shard_ptr = out_shard_ptrs[mi]
                row_idx = intmask(out_row_indices[mi])
                # Read row count from shard to iterate duplicates
                shard_count = intmask(engine_ffi._shard_row_count(shard_ptr))
                idx = row_idx
                while idx < shard_count:
                    s_pk_lo = r_uint64(engine_ffi._shard_get_pk_lo(
                        shard_ptr, rffi.cast(rffi.INT, idx)))
                    s_pk_hi = r_uint64(engine_ffi._shard_get_pk_hi(
                        shard_ptr, rffi.cast(rffi.INT, idx)))
                    if not pk_eq(s_pk_lo, s_pk_hi, key_lo, key_hi):
                        break
                    w = intmask(engine_ffi._shard_get_weight(
                        shard_ptr, rffi.cast(rffi.INT, idx)))
                    total_w += r_int64(w)
                    if not found_ptr:
                        found_ptr = shard_ptr
                        found_idx = idx
                    idx += 1
        finally:
            lltype.free(out_shard_ptrs, flavor="raw")
            lltype.free(out_row_indices, flavor="raw")

        return total_w, found_ptr, found_idx

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
        shard_w, shard_ptr, shard_idx = self._scan_shards_for_pk(key_lo, key_hi)
        total_w += shard_w
        if total_w <= r_int64(0):
            return False
        if mt_batch is not None:
            self._retract_acc.bind(mt_batch, mt_idx)
            out_batch.append_from_accessor(key_lo, key_hi, r_int64(-1), self._retract_acc)
        elif shard_ptr:
            self._retract_soa.set_row(shard_ptr, shard_idx)
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

        # 2. Check Columnar Shards via Rust ShardIndex
        out_shard_ptrs = lltype.malloc(rffi.VOIDPP.TO, _MAX_PK_RESULTS, flavor="raw")
        out_row_indices = lltype.malloc(rffi.INTP.TO, _MAX_PK_RESULTS, flavor="raw")
        try:
            n_matches = intmask(engine_ffi._shard_index_find_pk(
                self._index_handle,
                rffi.cast(rffi.ULONGLONG, key_lo),
                rffi.cast(rffi.ULONGLONG, key_hi),
                out_shard_ptrs,
                out_row_indices,
                rffi.cast(rffi.UINT, _MAX_PK_RESULTS),
            ))
            if n_matches > 0:
                soa = ShardRowAccessor(self.schema)
                for mi in range(n_matches):
                    shard_ptr = out_shard_ptrs[mi]
                    row_idx = intmask(out_row_indices[mi])
                    shard_count = intmask(engine_ffi._shard_row_count(shard_ptr))
                    idx = row_idx
                    while idx < shard_count:
                        s_pk_lo = r_uint64(engine_ffi._shard_get_pk_lo(
                            shard_ptr, rffi.cast(rffi.INT, idx)))
                        s_pk_hi = r_uint64(engine_ffi._shard_get_pk_hi(
                            shard_ptr, rffi.cast(rffi.INT, idx)))
                        if not pk_eq(s_pk_lo, s_pk_hi, key_lo, key_hi):
                            break
                        soa.set_row(shard_ptr, idx)
                        if core_comparator.compare_rows(self.schema, soa, accessor) == 0:
                            w = intmask(engine_ffi._shard_get_weight(
                                shard_ptr, rffi.cast(rffi.INT, idx)))
                            total_w += r_int64(w)
                        idx += 1
        finally:
            lltype.free(out_shard_ptrs, flavor="raw")
            lltype.free(out_row_indices, flavor="raw")

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

        # Check row count via a temporary shard open
        shard_count = self._check_shard_row_count(shard_path)
        if shard_count > 0:
            path_c = rffi.str2charp(shard_path)
            try:
                rc = intmask(engine_ffi._shard_index_add_shard(
                    self._index_handle,
                    path_c,
                    rffi.cast(rffi.ULONGLONG, r_uint64(0)),
                    rffi.cast(rffi.ULONGLONG, r_uint64(0)),
                ))
            finally:
                rffi.free_charp(path_c)
            if rc < 0:
                raise errors.StorageError("ShardIndex add_shard failed (error %d)" % rc)
        else:
            try:
                mmap_posix.unlinkat_c(self._dirfd, shard_name)
            except mmap_posix.MMapError:
                pass

        self.memtable.reset()

        return shard_path

    def _check_shard_row_count(self, shard_path):
        """Open a shard temporarily to check its row count, then close it."""
        fn_c = rffi.str2charp(shard_path)
        try:
            handle = engine_ffi._shard_open(
                fn_c,
                rffi.cast(rffi.VOIDP, self._schema_buf),
                rffi.cast(rffi.INT, 0),
            )
            if not handle:
                return 0
            count = intmask(engine_ffi._shard_row_count(handle))
            engine_ffi._shard_close(handle)
            return count
        finally:
            rffi.free_charp(fn_c)

    def close(self):
        if self.is_closed:
            return
        if self.memtable:
            self.memtable.free()
        if self._index_handle:
            engine_ffi._shard_index_close(self._index_handle)
            self._index_handle = lltype.nullptr(rffi.VOIDP.TO)
        if self._schema_buf:
            lltype.free(self._schema_buf, flavor="raw")
            self._schema_buf = lltype.nullptr(rffi.CCHARP.TO)
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
