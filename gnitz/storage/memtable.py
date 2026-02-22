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
from gnitz.core.comparator import PayloadRowAccessor

MAX_HEIGHT = 16


class MemTableBlobAllocator(string_logic.BlobAllocator):
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
        """Zero-copy allocation from raw pointer."""
        b_ptr = self.arena.alloc(length, alignment=8)
        buffer.c_memmove(
            rffi.cast(rffi.VOIDP, b_ptr),
            rffi.cast(rffi.VOIDP, src_ptr),
            rffi.cast(rffi.SIZE_T, length)
        )
        off = rffi.cast(lltype.Signed, b_ptr) - rffi.cast(
            lltype.Signed, self.arena.base_ptr
        )
        return r_uint64(off)


class MemTable(object):
    _immutable_fields_ = [
        "schema",
        "arena",
        "blob_arena",
        "rng",
        "key_size",
        "head_off",
        "node_accessor",
        "value_accessor",
        "raw_wal_accessor",
    ]

    def __init__(self, schema, arena_size):
        self.schema = schema
        self.arena = buffer.Buffer(arena_size, growable=False)
        self.blob_arena = buffer.Buffer(arena_size, growable=False)
        self.rng = Random(1234)
        self._update_offsets = [0] * MAX_HEIGHT
        self.key_size = schema.get_pk_column().field_type.size

        self.node_accessor = comparator.PackedNodeAccessor(
            schema, self.blob_arena.base_ptr
        )
        self.value_accessor = comparator.ValueAccessor(schema)
        self.raw_wal_accessor = comparator.RawWALAccessor(schema)

        self.hash_buf_cap = 1024
        self.hash_buf = lltype.malloc(rffi.CCHARP.TO, self.hash_buf_cap, flavor="raw")

        self.arena.alloc(8, alignment=8)

        head_key_off = get_key_offset(MAX_HEIGHT)
        h_sz = head_key_off + self.key_size + HASH_SIZE + self.schema.memtable_stride

        ptr = self.arena.alloc(h_sz, alignment=16)
        self.head_off = rffi.cast(lltype.Signed, ptr) - rffi.cast(
            lltype.Signed, self.arena.base_ptr
        )

        ptr[8] = chr(MAX_HEIGHT)
        for i in range(MAX_HEIGHT):
            node_set_next_off(self.arena.base_ptr, self.head_off, i, 0)

        key_ptr = rffi.ptradd(ptr, head_key_off)
        all_ones = r_uint64(-1)
        if self.key_size == 16:
            rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = rffi.cast(rffi.ULONGLONG, all_ones)
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(key_ptr, 8))[0] = rffi.cast(
                rffi.ULONGLONG, all_ones
            )
        else:
            rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = rffi.cast(rffi.ULONGLONG, all_ones)

    def lower_bound_node(self, key):
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

    def _find_exact_values_with_hash(self, key, target_hash, target_accessor):
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
                        self.node_accessor.set_row(base, next_off)
                        if (
                            core_comparator.compare_rows(
                                self.schema, self.node_accessor, target_accessor
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
                            self.schema, self.node_accessor, target_accessor
                        )
                        == 0
                    ):
                        return match_off
        return 0

    def _find_exact_values(self, key, row):
        self.value_accessor.set_row(row)
        hash_val, self.hash_buf, self.hash_buf_cap = serialize.compute_hash(
            self.schema, self.value_accessor, self.hash_buf, self.hash_buf_cap
        )
        return self._find_exact_values_with_hash(key, hash_val, self.value_accessor)

    def upsert(self, key, weight, row):
        base = self.arena.base_ptr
        self.value_accessor.set_row(row)
        hash_val, self.hash_buf, self.hash_buf_cap = serialize.compute_hash(
            self.schema, self.value_accessor, self.hash_buf, self.hash_buf_cap
        )
        match_off = self._find_exact_values_with_hash(key, hash_val, self.value_accessor)

        if match_off != 0:
            new_w = node_get_weight(base, match_off) + weight
            if new_w == 0:
                h = ord(base[match_off + 8])
                for i in range(h):
                    pred_off = self._update_offsets[i]
                    node_set_next_off(
                        base, pred_off, i, node_get_next_off(base, match_off, i)
                    )
            else:
                node_set_weight(base, match_off, new_w)
            return

        h = 1
        while h < MAX_HEIGHT and (self.rng.genrand32() & 1):
            h += 1

        key_off = get_key_offset(h)
        node_full_sz = key_off + self.key_size + HASH_SIZE + self.schema.memtable_stride
        
        # Unified capacity check
        self._ensure_capacity(node_full_sz, self.value_accessor)

        new_ptr = self.arena.alloc(node_full_sz, alignment=16)
        new_off = rffi.cast(lltype.Signed, new_ptr) - rffi.cast(lltype.Signed, base)

        node_set_weight(base, new_off, weight)
        new_ptr[8] = chr(h)
        key_ptr = rffi.ptradd(new_ptr, key_off)

        if self.key_size == 16:
            r_key = r_uint128(key)
            rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = rffi.cast(rffi.ULONGLONG, r_uint64(r_key))
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(key_ptr, 8))[0] = rffi.cast(
                rffi.ULONGLONG, r_uint64(r_key >> 64)
            )
        else:
            rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = rffi.cast(rffi.ULONGLONG, r_uint64(key))

        node_set_hash(base, new_off, self.key_size, hash_val)
        payload_ptr = node_get_payload_ptr(base, new_off, self.key_size)
        
        # Site 3 Eradicated: Unified Serialization
        serialize.serialize_row(
            self.schema, self.value_accessor, payload_ptr, MemTableBlobAllocator(self.blob_arena)
        )

        for i in range(h):
            pred = self._update_offsets[i]
            node_set_next_off(base, new_off, i, node_get_next_off(base, pred, i))
            node_set_next_off(base, pred, i, new_off)

    def upsert_raw(self, key, weight, raw_record):
        base = self.arena.base_ptr
        self.raw_wal_accessor.set_record(raw_record)
        hash_val, self.hash_buf, self.hash_buf_cap = serialize.compute_hash(
            self.schema, self.raw_wal_accessor, self.hash_buf, self.hash_buf_cap
        )
        match_off = self._find_exact_values_with_hash(
            key, hash_val, self.raw_wal_accessor
        )

        if match_off != 0:
            new_w = node_get_weight(base, match_off) + weight
            if new_w == 0:
                h = ord(base[match_off + 8])
                for i in range(h):
                    pred_off = self._update_offsets[i]
                    node_set_next_off(
                        base, pred_off, i, node_get_next_off(base, match_off, i)
                    )
            else:
                node_set_weight(base, match_off, new_w)
            return

        h = 1
        while h < MAX_HEIGHT and (self.rng.genrand32() & 1):
            h += 1

        key_off = get_key_offset(h)
        node_full_sz = key_off + self.key_size + HASH_SIZE + self.schema.memtable_stride
        
        # Site 4 Eradicated: Capacity check using raw accessor
        self._ensure_capacity(node_full_sz, self.raw_wal_accessor)

        new_ptr = self.arena.alloc(node_full_sz, alignment=16)
        new_off = rffi.cast(lltype.Signed, new_ptr) - rffi.cast(lltype.Signed, base)

        node_set_weight(base, new_off, weight)
        new_ptr[8] = chr(h)
        key_ptr = rffi.ptradd(new_ptr, key_off)

        if self.key_size == 16:
            r_key = r_uint128(key)
            rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = rffi.cast(rffi.ULONGLONG, r_uint64(r_key))
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(key_ptr, 8))[0] = rffi.cast(
                rffi.ULONGLONG, r_uint64(r_key >> 64)
            )
        else:
            rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = rffi.cast(rffi.ULONGLONG, r_uint64(key))

        node_set_hash(base, new_off, self.key_size, hash_val)
        payload_ptr = node_get_payload_ptr(base, new_off, self.key_size)

        # Site 5 Eradicated: Raw-to-Raw relocation using unified kernel
        serialize.serialize_row(
            self.schema, self.raw_wal_accessor, payload_ptr, MemTableBlobAllocator(self.blob_arena)
        )

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
        base, curr_off = self.arena.base_ptr, node_get_next_off(
            self.arena.base_ptr, self.head_off, 0
        )
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
