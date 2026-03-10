### Part 2 Execution Plan: SoA Unification & Vectorized Batching

#### Objective
Unify the system around a pure Structure-of-Arrays (SoA) layout to match `ArenaZSetBatch` and `wal_columnar.py`. Eliminate the millions of FFI boundary crossings during Compactions, Flushes, and MemTable inserts by utilizing C-level batched hashing.

---

### Step 1: C-Level Vectorized Batching (`xxh.py`)
**Goal:** Provide a C function that iterates over SoA column buffers (low and high bits separately) and computes hashes, neutralizing FFI overhead entirely.

**1. Update the C Snippet:**
Add the following SoA batch hasher to the `implementation_code` in `xxh.py`. Notice the strict branching to prevent segfaults when dealing with 64-bit keys (where `hi_ptr` might be null or unallocated).
```c
void gnitz_xxh3_batch_soa(const uint64_t* lo_ptr, const uint64_t* hi_ptr, size_t count, 
                          int is_u128, uint64_t seed_lo, uint64_t seed_hi, uint64_t* out_hashes) {
    for(size_t i = 0; i < count; i++) {
        if (is_u128) {
            uint64_t buf[2] = {lo_ptr[i] ^ seed_lo, hi_ptr[i] ^ seed_hi};
            out_hashes[i] = XXH3_64bits(buf, 16);
        } else {
            uint64_t buf[1] = {lo_ptr[i] ^ seed_lo}; // Ignore seed_hi for 64-bit to match existing math
            out_hashes[i] = XXH3_64bits(buf, 8);
        }
    }
}
```

**2. Add the RPython FFI Binding:**
```python
_xxh3_batch_soa = rffi.llexternal(
    "gnitz_xxh3_batch_soa",[rffi.ULONGLONGP, rffi.ULONGLONGP, rffi.SIZE_T, rffi.INT, rffi.ULONGLONG, rffi.ULONGLONG, rffi.ULONGLONGP],
    lltype.Void,
    compilation_info=eci,
    _nowrapper=True
)
```

---

### Step 2: The SoA Architecture Fix (`writer_table.py`)
**Goal:** Fix the Appendix C violation where `TableShardWriter` packed 128-bit keys contiguously (AoS). Split it into `pk_lo_buf` and `pk_hi_buf` to perfectly match `ArenaZSetBatch` and WAL layouts.

**1. Refactor Initialization and Row Appending:**
```python
class TableShardWriter(object):
    def __init__(self, schema, table_id=0):
        # ... [existing setup] ...
        self.is_u128 = schema.get_pk_column().field_type.size == 16
        
        # Replace self.pk_buf with strict SoA buffers
        self.pk_lo_buf = buffer.Buffer(8 * 1024, growable=True)
        self.pk_hi_buf = buffer.Buffer(8 * 1024, growable=True) # Always allocate to satisfy JIT/Monomorphism
        self.w_buf = buffer.Buffer(8 * 1024, growable=True)
        # ...

    def add_row_from_accessor(self, key, weight, accessor):
        if weight == 0:
            return
        self.count += 1
        
        self.pk_lo_buf.put_u64(r_uint64(key))
        if self.is_u128:
            self.pk_hi_buf.put_u64(r_uint64(key >> 64))
        else:
            self.pk_hi_buf.put_u64(r_uint64(0)) # Pad to keep lengths identical
            
        self.w_buf.put_i64(weight)
        self._append_from_accessor(accessor)
```

**2. Refactor `finalize()` to persist regions correctly:**
Adjust the directory region count and logic to write `pk_lo` and `pk_hi` separately.
```python
    def finalize(self, filename):
        # ...
        num_cols_with_pk_w = 3 + (len(self.col_bufs) - 1) # lo, hi, weight
        num_regions = num_cols_with_pk_w + 1 + 1  # + null + blob
        # ...
        region_list[0] = (self.pk_lo_buf.ptr, 0, self.pk_lo_buf.offset)
        region_list[1] = (self.pk_hi_buf.ptr, 0, self.pk_hi_buf.offset)
        region_list[2] = (self.w_buf.ptr, 0, self.w_buf.offset)
        region_list[3] = (self.null_buf.ptr, 0, self.null_buf.offset)
        
        # Offset reg_idx accordingly
        reg_idx = 4
        # ... [write col_bufs and b_buf] ...
        
        # Update XOR8 Builder call
        if self.count > 0:
            from gnitz.storage.xor8 import build_xor8
            xor8_filter = build_xor8(
                self.pk_lo_buf.ptr, 
                self.pk_hi_buf.ptr, 
                self.count, 
                self.is_u128
            )
```

---

### Step 3: The Perfectly Aligned Mega-Malloc (`xor8.py`)
**Goal:** Replace the 5 separate heap allocations during XOR8 construction with 1 contiguous, mathematically aligned block, and hash everything in C.

**1. Refactor `build_xor8`:**
```python
def align8(val):
    return (val + 7) & ~7

def build_xor8(pk_lo_ptr, pk_hi_ptr, num_keys, is_u128):
    if num_keys == 0:
        return None

    segment_length = intmask(1 + (num_keys * 123 + 99) // 100 // 3)
    if segment_length < 4:
        segment_length = 4
    total_size = 3 * segment_length

    # 1. Strict 8-byte alignment carving
    off_fp      = 0
    off_counts  = align8(total_size)
    off_keys_at = align8(off_counts + (total_size * 4))
    off_stack   = align8(off_keys_at + (total_size * 8))
    off_queue   = align8(off_stack + (num_keys * 16))
    off_hashes  = align8(off_queue + (total_size * 4))
    total_alloc = off_hashes + (num_keys * 8)

    # ONE allocation
    workspace = lltype.malloc(rffi.CCHARP.TO, total_alloc, flavor="raw")
    built = False

    try:
        # 2. Strict Pointer Casting
        fingerprints = workspace
        counts_p = rffi.cast(rffi.INTP, rffi.ptradd(workspace, off_counts))
        keys_at_p = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(workspace, off_keys_at))
        stack_p = rffi.cast(rffi.CCHARP, rffi.ptradd(workspace, off_stack))
        stack_hash_p = rffi.cast(rffi.ULONGLONGP, stack_p)
        stack_slot_p = rffi.cast(rffi.INTP, rffi.ptradd(stack_p, num_keys * 8))
        queue_p = rffi.cast(rffi.INTP, rffi.ptradd(workspace, off_queue))
        hashes_p = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(workspace, off_hashes))

        seed = r_uint128(1)
        attempt = 0
        while attempt < MAX_CONSTRUCTION_ATTEMPTS:
            # Re-zero the structural tracking buffers on retry
            for i in range(total_size):
                counts_p[i] = rffi.cast(rffi.INT, 0)
                keys_at_p[i] = rffi.cast(rffi.ULONGLONG, 0)

            # 3. Vectorized Hashing (Zero FFI Overhead)
            seed_lo = r_uint64(seed)
            seed_hi = r_uint64(seed >> 64)
            
            from gnitz.core import xxh
            xxh._xxh3_batch_soa(
                rffi.cast(rffi.ULONGLONGP, pk_lo_ptr),
                rffi.cast(rffi.ULONGLONGP, pk_hi_ptr) if is_u128 else lltype.nullptr(rffi.ULONGLONGP.TO),
                rffi.cast(rffi.SIZE_T, num_keys),
                rffi.cast(rffi.INT, 1 if is_u128 else 0),
                rffi.cast(rffi.ULONGLONG, seed_lo),
                rffi.cast(rffi.ULONGLONG, seed_hi),
                hashes_p
            )

            # 4. Standard Graph Peeling (Using hashes_p)
            for i in range(num_keys):
                h = r_uint64(hashes_p[i])
                h0, h1, h2 = _get_h0_h1_h2(h, segment_length)
                # ...[existing counts/keys_at XOR logic] ...
            
            # ... [existing queue/stack peeling logic] ...
            
            if stack_size == num_keys:
                built = True
                break
            seed += r_uint128(1)
            attempt += 1

        if not built: return None
        
        # ... [existing fingerprint finalization loop using stack_hash_p] ...

        # Note: Filter owns the ENTIRE workspace block by holding 'fingerprints'
        return Xor8Filter(fingerprints, segment_length, seed, total_size, owned=True)
    finally:
        if not built:
            lltype.free(workspace, flavor="raw")
```

---

### Step 4: Vectorized Bloom Inserts (`memtable.py` & `bloom.py`)
**Goal:** Process massive incoming batch hashes in C to saturate memory bandwidth, while protecting RPython string relocations by appending row data standardly.

**1. JIT-Friendly Bulk Update (`bloom.py`):**
```python
    @jit.unroll_safe
    def _apply_hash_probes(self, h_val, num_bits_u, bits):
        """Helper to force the JIT to unroll the hash probes, avoiding loop-explosion."""
        h1 = h_val
        h2 = (h_val >> 32) | r_uint64(1)
        for i in range(NUM_PROBES):
            pos = intmask((h1 + r_uint64(i) * h2) % num_bits_u)
            bit_mask = 1 << (pos & 7)
            bits[pos >> 3] = chr(ord(bits[pos >> 3]) | bit_mask)

    def add_hashes(self, hashes_ptr, count):
        num_bits_u = r_uint64(self.num_bits)
        bits = self.bits
        for i in range(count):
            self._apply_hash_probes(r_uint64(hashes_ptr[i]), num_bits_u, bits)
```

**2. Refactor `upsert_batch` (`memtable.py`):**
```python
    def upsert_batch(self, batch):
        self._check_capacity()
        count = batch.length()
        
        # 1. Safe Row-by-Row Append (Protects `blob_arena` string offsets)
        for i in range(count):
            self.batch.append_from_accessor(
                batch.get_pk(i), batch.get_weight(i), batch.get_accessor(i)
            )
            
        # 2. Vectorized C-Level Hashing
        is_u128 = self.schema.get_pk_column().field_type.size == 16
        with lltype.scoped_alloc(rffi.ULONGLONGP.TO, count) as hashes_buf:
            xxh._xxh3_batch_soa(
                rffi.cast(rffi.ULONGLONGP, batch.pk_lo_buf.ptr),
                rffi.cast(rffi.ULONGLONGP, batch.pk_hi_buf.ptr) if is_u128 else lltype.nullptr(rffi.ULONGLONGP.TO),
                rffi.cast(rffi.SIZE_T, count),
                rffi.cast(rffi.INT, 1 if is_u128 else 0),
                rffi.cast(rffi.ULONGLONG, 0), rffi.cast(rffi.ULONGLONG, 0),
                hashes_buf
            )
            # 3. Apply hashes to Bloom bitset
            self.bloom.add_hashes(hashes_buf, count)
```

---

### Quality Assurance & Verification Checklist for Part 2
*[x] **Strict Aliasing Protection:** Pointers inside the XOR8 Mega-Malloc are forced to align to 8-byte boundaries using `(val + 7) & ~7`. Casting them to `ULONGLONGP` or `INTP` is strictly safe on all CPU architectures (x86, ARM).
*   [x] **Segfault Prevention:** The `gnitz_xxh3_batch_soa` explicitly branches on `is_u128` to guarantee the C runtime never dereferences a 64-bit table's empty `pk_hi_buf`.
*   [x] **Appendix C Compliance:** `TableShardWriter` now writes strictly formatted `pk_lo_buf` and `pk_hi_buf` regions, allowing zero-copy `mmap` ingestion into `ArenaZSetBatch` without layout conversion.
*   [x] **String Relocation Safety:** `MemTable` retains its row-by-row `append_from_accessor` loop, ensuring that German String `heap_ptr` offsets are safely re-calculated against the new `blob_arena` base instead of blindly `memcpy`-ing dead offsets.
