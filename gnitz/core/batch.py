# gnitz/core/batch.py

from rpython.rlib import jit
from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core.values import PayloadRow, make_payload_row, _analyze_schema
from gnitz.core import serialize, strings as string_logic
from gnitz.core import comparator as core_comparator
from gnitz.storage import buffer
from gnitz.storage import comparator as storage_comparator

# ---------------------------------------------------------------------------
# Arena Constants
# ---------------------------------------------------------------------------
BATCH_REC_PK_OFFSET = 0
BATCH_REC_WEIGHT_OFFSET = 16
BATCH_REC_NULL_OFFSET = 24
BATCH_REC_PAYLOAD_BASE = 32
BATCH_HEADER_SIZE = 32


class BatchBlobAllocator(string_logic.BlobAllocator):
    """Strategy for writing variable-length data into the batch's blob arena."""

    def __init__(self, arena):
        self.arena = arena

    def allocate(self, string_data):
        length = len(string_data)
        dest = self.arena.alloc(length, alignment=1)
        for i in range(length):
            dest[i] = string_data[i]
        return r_uint64(
            rffi.cast(lltype.Signed, dest) - rffi.cast(lltype.Signed, self.arena.base_ptr)
        )

    def allocate_from_ptr(self, src_ptr, length):
        dest = self.arena.alloc(length, alignment=1)
        buffer.c_memmove(
            rffi.cast(rffi.VOIDP, dest),
            rffi.cast(rffi.VOIDP, src_ptr),
            rffi.cast(rffi.SIZE_T, length),
        )
        return r_uint64(
            rffi.cast(lltype.Signed, dest) - rffi.cast(lltype.Signed, self.arena.base_ptr)
        )


# ---------------------------------------------------------------------------
# Mergesort Support (Raw Indices)
# ---------------------------------------------------------------------------

def _mergesort_indices(indices, batch, lo, hi, scratch):
    if hi - lo <= 1:
        return
    mid = (lo + hi) >> 1
    _mergesort_indices(indices, batch, lo, mid, scratch)
    _mergesort_indices(indices, batch, mid, hi, scratch)
    _merge_indices(indices, batch, lo, mid, hi, scratch)


def _merge_indices(indices, batch, lo, mid, hi, scratch):
    for k in range(lo, mid):
        scratch[k] = indices[k]

    i = lo
    j = mid
    k = lo

    while i < mid and j < hi:
        if batch.compare_indices(scratch[i], indices[j]) <= 0:
            indices[k] = scratch[i]
            i += 1
        else:
            indices[k] = indices[j]
            j += 1
        k += 1

    while i < mid:
        indices[k] = scratch[i]
        i += 1
        k += 1


# ---------------------------------------------------------------------------
# ArenaZSetBatch
# ---------------------------------------------------------------------------

class ArenaZSetBatch(object):
    """
    A zero-allocation DBSP Z-Set batch stored in raw memory arenas.
    """

    _immutable_fields_ = ["_schema", "record_stride"]

    def __init__(self, schema, initial_capacity=1024):
        self._schema = schema
        raw_stride = BATCH_HEADER_SIZE + schema.memtable_stride
        self.record_stride = (raw_stride + 15) & ~15

        # If initial_capacity is 0, Buffer will safely initialize to nullptr.
        self.primary_arena = buffer.Buffer(initial_capacity * self.record_stride)
        self.blob_arena = buffer.Buffer(initial_capacity * 64)

        self.allocator = BatchBlobAllocator(self.blob_arena)

        # Pre-allocated accessors
        self._raw_accessor = storage_comparator.RawWALAccessor(schema)
        self._row_accessor = core_comparator.PayloadRowAccessor(schema)
        self._cmp_acc_a = storage_comparator.RawWALAccessor(schema)
        self._cmp_acc_b = storage_comparator.RawWALAccessor(schema)

        self._count = 0
        self._sorted = False

    @staticmethod
    def from_buffers(schema, primary_buf, blob_buf, count, is_sorted=True):
        """
        Zero-copy factory for IPC transport.
        Uses initial_capacity=0 to skip wasteful heap allocations.
        """
        batch = ArenaZSetBatch(schema, initial_capacity=0)
        
        # Buffer views are already prepared by ipc.py
        batch.primary_arena = primary_buf
        batch.blob_arena = blob_buf
        
        # Synchronize offsets so future appends work correctly.
        batch.primary_arena.offset = count * batch.record_stride
        
        batch._count = count
        batch._sorted = is_sorted
        
        # Re-initialize strategy with the correct buffer
        batch.allocator = BatchBlobAllocator(batch.blob_arena)
        
        return batch

    def length(self):
        return self._count

    def is_sorted(self):
        return self._sorted

    def is_empty(self):
        return self._count == 0

    def clear(self):
        self.primary_arena.offset = 0
        self.blob_arena.offset = 0
        self._count = 0
        self._sorted = False

    def free(self):
        # buffer.free() now respects is_owned flags correctly.
        self.primary_arena.free()
        self.blob_arena.free()

    def _get_rec_ptr(self, i):
        return rffi.ptradd(self.primary_arena.base_ptr, i * self.record_stride)

    def get_pk(self, i):
        assert 0 <= i < self._count
        ptr = self._get_rec_ptr(i)
        lo = rffi.cast(rffi.ULONGLONGP, ptr)[0]
        hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))[0]
        return (r_uint128(hi) << 64) | r_uint128(lo)

    def get_weight(self, i):
        assert 0 <= i < self._count
        ptr = self._get_rec_ptr(i)
        return rffi.cast(rffi.LONGLONGP, rffi.ptradd(ptr, BATCH_REC_WEIGHT_OFFSET))[0]

    def get_accessor(self, i):
        self.bind_raw_accessor(i, self._raw_accessor)
        return self._raw_accessor

    def bind_raw_accessor(self, i, out_accessor):
        assert 0 <= i < self._count
        ptr = self._get_rec_ptr(i)
        null_word = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, BATCH_REC_NULL_OFFSET))[0]
        payload_ptr = rffi.ptradd(ptr, BATCH_REC_PAYLOAD_BASE)
        out_accessor.set_pointers(payload_ptr, self.blob_arena.base_ptr, null_word)

    def get_row(self, i):
        assert 0 <= i < self._count
        ptr = self._get_rec_ptr(i)
        null_word = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, BATCH_REC_NULL_OFFSET))[0]
        payload_ptr = rffi.ptradd(ptr, BATCH_REC_PAYLOAD_BASE)
        return serialize.deserialize_row(
            self._schema, payload_ptr, self.blob_arena.base_ptr, null_word
        )

    def append(self, pk, weight, row):
        self._row_accessor.set_row(row)
        self.append_from_accessor(pk, weight, self._row_accessor)

    def append_from_accessor(self, pk, weight, accessor):
        dest = self.primary_arena.alloc(self.record_stride, alignment=16)

        pk_u128 = r_uint128(pk)
        rffi.cast(rffi.ULONGLONGP, dest)[0] = rffi.cast(rffi.ULONGLONG, r_uint64(pk_u128))
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(dest, 8))[0] = rffi.cast(
            rffi.ULONGLONG, r_uint64(pk_u128 >> 64)
        )

        rffi.cast(rffi.LONGLONGP, rffi.ptradd(dest, BATCH_REC_WEIGHT_OFFSET))[0] = weight

        null_word = r_uint64(0)
        if isinstance(accessor, storage_comparator.RawWALAccessor):
            null_word = accessor.null_word
        elif isinstance(accessor, core_comparator.PayloadRowAccessor):
            if accessor._row:
                null_word = accessor._row._null_word
        else:
            for i in range(len(self._schema.columns)):
                if i == self._schema.pk_index:
                    continue
                if accessor.is_null(i):
                    payload_idx = i if i < self._schema.pk_index else i - 1
                    null_word |= r_uint64(1) << payload_idx

        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(dest, BATCH_REC_NULL_OFFSET))[0] = null_word
        payload_dest = rffi.ptradd(dest, BATCH_REC_PAYLOAD_BASE)
        serialize.serialize_row(self._schema, accessor, payload_dest, self.allocator)

        self._count += 1
        self._sorted = False

    def compare_indices(self, idx_a, idx_b):
        ptr_a = self._get_rec_ptr(idx_a)
        ptr_b = self._get_rec_ptr(idx_b)

        ahi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr_a, 8))[0]
        bhi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr_b, 8))[0]
        if ahi < bhi: return -1
        if ahi > bhi: return 1

        alo = rffi.cast(rffi.ULONGLONGP, ptr_a)[0]
        blo = rffi.cast(rffi.ULONGLONGP, ptr_b)[0]
        if alo < blo: return -1
        if alo > blo: return 1

        na = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr_a, BATCH_REC_NULL_OFFSET))[0]
        nb = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr_b, BATCH_REC_NULL_OFFSET))[0]

        self._cmp_acc_a.set_pointers(
            rffi.ptradd(ptr_a, BATCH_REC_PAYLOAD_BASE), self.blob_arena.base_ptr, na
        )
        self._cmp_acc_b.set_pointers(
            rffi.ptradd(ptr_b, BATCH_REC_PAYLOAD_BASE), self.blob_arena.base_ptr, nb
        )

        return core_comparator.compare_rows(self._schema, self._cmp_acc_a, self._cmp_acc_b)

    def sort(self):
        if self._count <= 1 or self._sorted:
            self._sorted = True
            return

        indices = newlist_hint(self._count)
        scratch = newlist_hint(self._count)
        for i in range(self._count):
            indices.append(i)
            scratch.append(0)

        _mergesort_indices(indices, self, 0, self._count, scratch)

        # Sorting always produces an OWNED heap-allocated buffer.
        new_primary = buffer.Buffer(self.primary_arena.capacity)
        new_blob    = buffer.Buffer(self.blob_arena.capacity)
        new_alloc   = BatchBlobAllocator(new_blob)

        old_blob_base = self.blob_arena.base_ptr
        has_varlen = self._schema.has_varlen

        for i in range(self._count):
            old_idx  = indices[i]
            src_ptr  = self._get_rec_ptr(old_idx)
            dest_ptr = new_primary.alloc(self.record_stride, alignment=16)

            if not has_varlen:
                buffer.c_memmove(
                    rffi.cast(rffi.VOIDP, dest_ptr),
                    rffi.cast(rffi.VOIDP, src_ptr),
                    rffi.cast(rffi.SIZE_T, self.record_stride)
                )
            else:
                pk_lo   = rffi.cast(rffi.ULONGLONGP, src_ptr)[0]
                pk_hi   = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(src_ptr, 8))[0]
                weight  = rffi.cast(rffi.LONGLONGP,  rffi.ptradd(src_ptr, BATCH_REC_WEIGHT_OFFSET))[0]
                null_w  = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(src_ptr, BATCH_REC_NULL_OFFSET))[0]

                rffi.cast(rffi.ULONGLONGP, dest_ptr)[0] = pk_lo
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(dest_ptr, 8))[0] = pk_hi
                rffi.cast(rffi.LONGLONGP,  rffi.ptradd(dest_ptr, BATCH_REC_WEIGHT_OFFSET))[0] = weight
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(dest_ptr, BATCH_REC_NULL_OFFSET))[0] = null_w

                src_payload  = rffi.ptradd(src_ptr,  BATCH_REC_PAYLOAD_BASE)
                dest_payload = rffi.ptradd(dest_ptr, BATCH_REC_PAYLOAD_BASE)
                self._raw_accessor.set_pointers(src_payload, old_blob_base, null_w)
                serialize.serialize_row(self._schema, self._raw_accessor, dest_payload, new_alloc)

        self.primary_arena.free()
        self.blob_arena.free()

        self.primary_arena = new_primary
        self.blob_arena    = new_blob
        self.allocator     = BatchBlobAllocator(self.blob_arena)
        self._sorted       = True

    def consolidate(self):
        if self._count == 0:
            return
        if not self._sorted:
            self.sort()

        new_primary = buffer.Buffer(self.primary_arena.capacity)
        new_blob    = buffer.Buffer(self.blob_arena.capacity)
        new_alloc   = BatchBlobAllocator(new_blob)
        new_count   = 0

        acc_a = storage_comparator.RawWALAccessor(self._schema)
        acc_b = storage_comparator.RawWALAccessor(self._schema)
        has_varlen = self._schema.has_varlen

        i = 0
        while i < self._count:
            ptr_i    = self._get_rec_ptr(i)
            pk_i_lo  = rffi.cast(rffi.ULONGLONGP, ptr_i)[0]
            pk_i_hi  = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr_i, 8))[0]
            weight_acc = rffi.cast(rffi.LONGLONGP, rffi.ptradd(ptr_i, BATCH_REC_WEIGHT_OFFSET))[0]
            null_i   = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr_i, BATCH_REC_NULL_OFFSET))[0]

            acc_a.set_pointers(
                rffi.ptradd(ptr_i, BATCH_REC_PAYLOAD_BASE),
                self.blob_arena.base_ptr,
                null_i,
            )

            j = i + 1
            while j < self._count:
                ptr_j = self._get_rec_ptr(j)

                if rffi.cast(rffi.ULONGLONGP, ptr_j)[0] != pk_i_lo or \
                   rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr_j, 8))[0] != pk_i_hi:
                    break

                null_j = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr_j, BATCH_REC_NULL_OFFSET))[0]
                acc_b.set_pointers(
                    rffi.ptradd(ptr_j, BATCH_REC_PAYLOAD_BASE),
                    self.blob_arena.base_ptr,
                    null_j,
                )

                if core_comparator.compare_rows(self._schema, acc_a, acc_b) != 0:
                    break

                weight_acc += rffi.cast(
                    rffi.LONGLONGP, rffi.ptradd(ptr_j, BATCH_REC_WEIGHT_OFFSET)
                )[0]
                j += 1

            if weight_acc != 0:
                dest = new_primary.alloc(self.record_stride, alignment=16)

                if not has_varlen:
                    buffer.c_memmove(
                        rffi.cast(rffi.VOIDP, dest),
                        rffi.cast(rffi.VOIDP, ptr_i),
                        rffi.cast(rffi.SIZE_T, self.record_stride)
                    )
                    rffi.cast(rffi.LONGLONGP, rffi.ptradd(dest, BATCH_REC_WEIGHT_OFFSET))[0] = weight_acc
                else:
                    rffi.cast(rffi.ULONGLONGP, dest)[0] = pk_i_lo
                    rffi.cast(rffi.ULONGLONGP, rffi.ptradd(dest, 8))[0] = pk_i_hi
                    rffi.cast(rffi.LONGLONGP,  rffi.ptradd(dest, BATCH_REC_WEIGHT_OFFSET))[0] = weight_acc
                    rffi.cast(rffi.ULONGLONGP, rffi.ptradd(dest, BATCH_REC_NULL_OFFSET))[0] = null_i

                    payload_dest = rffi.ptradd(dest, BATCH_REC_PAYLOAD_BASE)
                    serialize.serialize_row(self._schema, acc_a, payload_dest, new_alloc)
                
                new_count += 1

            i = j

        self.primary_arena.free()
        self.blob_arena.free()

        self.primary_arena = new_primary
        self.blob_arena    = new_blob
        self.allocator     = BatchBlobAllocator(self.blob_arena)
        self._count        = new_count
        self._sorted       = True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

ZSetBatch = ArenaZSetBatch

def make_singleton_batch(schema, pk, weight, row):
    batch = ZSetBatch(schema)
    batch.append(pk, weight, row)
    batch._sorted = True
    return batch
