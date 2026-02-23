# gnitz/storage/memtable.py

from rpython.rlib.rrandom import Random
from rpython.rlib.rarithmetic import r_uint64, intmask, r_int64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import errors, serialize, types, strings as string_logic, comparator as core_comparator
from gnitz.storage import buffer, writer_table, comparator
from gnitz.storage.memtable_node import (
    node_get_next_off,
    node_set_next_off,
    node_get_weight,
    node_set_weight,
    node_get_key,
    node_get_payload_ptr,
    get_key_offset,
    node_get_hash,
    node_set_hash,
    HASH_SIZE,
)

MAX_HEIGHT = 16


class MemTableBlobAllocator(string_logic.BlobAllocator):
    """Allocates string tails in the MemTable's blob arena."""

    def __init__(self, arena):
        self.arena = arena

    def allocate(self, string_data):
        length = len(string_data)
        b_ptr = self.arena.alloc(length, alignment=8)
        for j in range(length):
            b_ptr[j] = string_data[j]
        off = rffi.cast(lltype.Signed, b_ptr) - rffi.cast(
            lltype.Signed, self.arena.base_ptr
        )
        return r_uint64(off)

    def allocate_from_ptr(self, src_ptr, length):
        """Zero-copy relocation of string data."""
        b_ptr = self.arena.alloc(length, alignment=8)
        if length > 0:
            buffer.c_memmove(
                rffi.cast(rffi.VOIDP, b_ptr),
                rffi.cast(rffi.VOIDP, src_ptr),
                rffi.cast(rffi.SIZE_T, length),
            )
        off = rffi.cast(lltype.Signed, b_ptr) - rffi.cast(
            lltype.Signed, self.arena.base_ptr
        )
        return r_uint64(off)


class MemTable(object):
    """
    Mutable, in-memory Z-Set storage.
    Optimized for rapid algebraic coalescing via SkipList + Payload Hashing.

    Entry point for mutations is upsert_batch().
    """

    _immutable_fields_ = [
        "schema",
        "arena",
        "blob_arena",
        "rng",
        "key_size",
        "head_off",
        "node_accessor",
    ]

    def __init__(self, schema, arena_size):
        self.schema = schema
        self.arena = buffer.Buffer(arena_size, growable=False)
        self.blob_arena = buffer.Buffer(arena_size, growable=False)
        self.rng = Random(1234)
        self._update_offsets = [0] * MAX_HEIGHT
        self.key_size = schema.get_pk_column().field_type.size

        # Node accessor for SkipList traversal comparisons
        self.node_accessor = comparator.PackedNodeAccessor(
            schema, self.blob_arena.base_ptr
        )

        # Reusable hash buffer to minimize mallocs
        self.hash_buf_cap = 1024
        self.hash_buf = lltype.malloc(rffi.CCHARP.TO, self.hash_buf_cap, flavor="raw")

        # SkipList Initialization
        self.arena.alloc(8, alignment=8)  # Padding
        head_key_off = get_key_offset(MAX_HEIGHT)
        h_sz = head_key_off + self.key_size + HASH_SIZE + self.schema.memtable_stride

        ptr = self.arena.alloc(h_sz, alignment=16)
        self.head_off = rffi.cast(lltype.Signed, ptr) - rffi.cast(
            lltype.Signed, self.arena.base_ptr
        )

        ptr[8] = chr(MAX_HEIGHT)
        for i in range(MAX_HEIGHT):
            node_set_next_off(self.arena.base_ptr, self.head_off, i, 0)

        # Sentinel Key: Max possible value
        key_ptr = rffi.ptradd(ptr, head_key_off)
        all_ones = r_uint64(-1)
        if self.key_size == 16:
            rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = all_ones
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(key_ptr, 8))[0] = all_ones
        else:
            rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = all_ones

    def lower_bound_node(self, key):
        """Standard SkipList search for first node where node.key >= key."""
        base = self.arena.base_ptr
        curr_off = self.head_off

        for i in range(MAX_HEIGHT - 1, -1, -1):
            next_off = node_get_next_off(base, curr_off, i)
            while next_off != 0:
                next_key = node_get_key(base, next_off, self.key_size)
                if next_key < key:
                    curr_off = next_off
                else:
                    break
                next_off = node_get_next_off(base, curr_off, i)

        return node_get_next_off(base, curr_off, 0)

    def _find_exact_values(self, key, target_hash, accessor):
        """
        Finds the exact SkipList node matching (Key, Hash, Payload).
        Updates self._update_offsets as a side-effect for subsequent insertions.
        """
        base = self.arena.base_ptr
        curr_off = self.head_off

        for i in range(MAX_HEIGHT - 1, -1, -1):
            next_off = node_get_next_off(base, curr_off, i)
            while next_off != 0:
                next_key = node_get_key(base, next_off, self.key_size)

                if next_key < key:
                    curr_off = next_off
                elif next_key == key:
                    stored_hash = node_get_hash(base, next_off, self.key_size)
                    if stored_hash < target_hash:
                        curr_off = next_off
                    elif stored_hash > target_hash:
                        break
                    else:
                        # Hash collision or exact match: perform full byte-level check
                        self.node_accessor.set_row(base, next_off)
                        if (
                            core_comparator.compare_rows(
                                self.schema, self.node_accessor, accessor
                            )
                            < 0
                        ):
                            curr_off = next_off
                        else:
                            break
                else:
                    break
                next_off = node_get_next_off(base, curr_off, i)
            self._update_offsets[i] = curr_off

        match_off = node_get_next_off(base, curr_off, 0)
        if match_off != 0:
            if node_get_key(base, match_off, self.key_size) == key:
                if node_get_hash(base, match_off, self.key_size) == target_hash:
                    self.node_accessor.set_row(base, match_off)
                    if (
                        core_comparator.compare_rows(
                            self.schema, self.node_accessor, accessor
                        )
                        == 0
                    ):
                        return match_off
        return 0

    def upsert_batch(self, batch):
        """
        Public entry point for Z-Set batch ingestion.
        """
        num_records = batch.length()
        for i in range(num_records):
            pk = batch.get_pk(i)
            w = batch.get_weight(i)
            acc = batch.get_accessor(i)

            # Note: We rely on the internal upsert logic for correctness.
            # accessor MUST implement RowAccessor (checked via type annotation).
            self._upsert_internal(pk, w, acc)

    def _upsert_internal(self, key, weight, accessor):
        """
        INTERNAL helper. Ingests a single record from any RowAccessor.
        Callers must provide a valid RowAccessor implementation (e.g. from Batch).
        """
        base = self.arena.base_ptr
        hash_val, self.hash_buf, self.hash_buf_cap = serialize.compute_hash(
            self.schema, accessor, self.hash_buf, self.hash_buf_cap
        )

        match_off = self._find_exact_values(key, hash_val, accessor)

        if match_off != 0:
            # Existing node: additive coalescing
            new_w = node_get_weight(base, match_off) + weight
            if new_w == 0:
                # Annihilation: unlink node
                h = ord(base[match_off + 8])
                for i in range(h):
                    pred_off = self._update_offsets[i]
                    node_set_next_off(
                        base, pred_off, i, node_get_next_off(base, match_off, i)
                    )
            else:
                node_set_weight(base, match_off, new_w)
            return

        # New node creation
        h = 1
        while h < MAX_HEIGHT and (self.rng.genrand32() & 1):
            h += 1

        key_off = get_key_offset(h)
        node_full_sz = key_off + self.key_size + HASH_SIZE + self.schema.memtable_stride

        self._ensure_capacity(node_full_sz, accessor)

        new_ptr = self.arena.alloc(node_full_sz, alignment=16)
        new_off = rffi.cast(lltype.Signed, new_ptr) - rffi.cast(lltype.Signed, base)

        node_set_weight(base, new_off, weight)
        new_ptr[8] = chr(h)
        key_ptr = rffi.ptradd(new_ptr, key_off)

        # Write PK
        pk_u128 = r_uint128(key)
        if self.key_size == 16:
            rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = rffi.cast(
                rffi.ULONGLONG, r_uint64(pk_u128)
            )
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(key_ptr, 8))[0] = rffi.cast(
                rffi.ULONGLONG, r_uint64(pk_u128 >> 64)
            )
        else:
            rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = rffi.cast(
                rffi.ULONGLONG, r_uint64(pk_u128)
            )

        node_set_hash(base, new_off, self.key_size, hash_val)

        # Serialize Payload
        payload_ptr = node_get_payload_ptr(base, new_off, self.key_size)
        serialize.serialize_row(
            self.schema, accessor, payload_ptr, MemTableBlobAllocator(self.blob_arena)
        )

        # Link into SkipList
        for i in range(h):
            pred = self._update_offsets[i]
            node_set_next_off(base, new_off, i, node_get_next_off(base, pred, i))
            node_set_next_off(base, pred, i, new_off)

    def _ensure_capacity(self, node_sz, accessor):
        blob_sz = serialize.get_heap_size(self.schema, accessor)
        if (
            self.arena.offset + node_sz > self.arena.size
            or self.blob_arena.offset + blob_sz > self.blob_arena.size
        ):
            raise errors.MemTableFullError()

    def flush(self, filename, table_id=0):
        sw = writer_table.TableShardWriter(self.schema, table_id)
        base = self.arena.base_ptr
        curr_off = node_get_next_off(base, self.head_off, 0)

        while curr_off != 0:
            w = node_get_weight(base, curr_off)
            if w != 0:
                sw.add_row(
                    node_get_key(base, curr_off, self.key_size),
                    w,
                    node_get_payload_ptr(base, curr_off, self.key_size),
                    self.blob_arena.base_ptr,
                )
            curr_off = node_get_next_off(base, curr_off, 0)
        sw.finalize(filename)

    def free(self):
        self.arena.free()
        self.blob_arena.free()
        if self.hash_buf != lltype.nullptr(rffi.CCHARP.TO):
            lltype.free(self.hash_buf, flavor="raw")
            self.hash_buf = lltype.nullptr(rffi.CCHARP.TO)
