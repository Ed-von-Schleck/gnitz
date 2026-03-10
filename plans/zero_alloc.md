### Part 1 Execution Plan: Zero-Allocation Foundations

#### Objective
Transform point-lookups (Bloom/XOR8) and row-hashing (Group-By) into zero-allocation, stateless operations. This eliminates the PyPy GC churn and C-level memory fragmentation caused by millions of `_scratch` and row-buffer `malloc` cycles.

---

### Step 1: C-Level Inline Hashing (`xxh.py`)
**Goal:** Expose XXH3's stack-based hashing to RPython to avoid heap allocations when hashing 128-bit primitive keys.

**1. Update the C Snippet:**
Add the following to the `implementation_code` string in `xxh.py`. This allocates a tiny 16-byte array purely on the CPU stack/registers, XORs the seed, and hashes it.
```c
unsigned long long gnitz_xxh3_128_inline(uint64_t lo, uint64_t hi, uint64_t seed_lo, uint64_t seed_hi) {
    uint64_t buf[2] = {lo ^ seed_lo, hi ^ seed_hi};
    return XXH3_64bits(buf, 16);
}
```

**2. Add the RPython FFI Binding:**
In `xxh.py`, map the function with `_nowrapper=True` so the JIT can inline the FFI boundary directly into the trace.
```python
_xxh3_128_inline = rffi.llexternal(
    "gnitz_xxh3_128_inline",[rffi.ULONGLONG, rffi.ULONGLONG, rffi.ULONGLONG, rffi.ULONGLONG],
    rffi.ULONGLONG,
    compilation_info=eci,
    _nowrapper=True
)

@jit.elidable
def hash_u128_inline(lo, hi, seed_lo=r_uint64(0), seed_hi=r_uint64(0)):
    """Zero-allocation 128-bit hasher."""
    res = _xxh3_128_inline(
        rffi.cast(rffi.ULONGLONG, lo),
        rffi.cast(rffi.ULONGLONG, hi),
        rffi.cast(rffi.ULONGLONG, seed_lo),
        rffi.cast(rffi.ULONGLONG, seed_hi)
    )
    return r_uint64(res)
```

---

### Step 2: Stateless Filter Lookups (`bloom.py`, `xor8.py`)
**Goal:** Remove the `self._scratch` buffers.

**1. `bloom.py` Modifications:**
*   **Remove** `self._scratch = lltype.malloc(...)` from `__init__`.
*   **Remove** `if self._scratch: lltype.free(...)` from `free()`.
*   **Rewrite `_hash_key`:**
```python
    @jit.elidable
    def _hash_key(self, key):
        lo = r_uint64(key)
        hi = r_uint64(key >> 64)
        return xxh.hash_u128_inline(lo, hi, r_uint64(0), r_uint64(0))
```

**2. `xor8.py` Modifications:**
*   **Remove** `self._scratch` logic from `Xor8Filter.__init__` and `free()`.
*   **Rewrite `_hash_key`:**
```python
    @jit.elidable
    def _hash_key(self, key):
        lo = r_uint64(key)
        hi = r_uint64(key >> 64)
        # Note: The C-function inherently XORs the seed, fulfilling the XOR8 requirement.
        return xxh.hash_u128_inline(lo, hi, self.seed_lo, self.seed_hi)
```
*(Validation: This preserves identical math to `_hash_with_seed`, so existing `.xor8` files on disk remain 100% valid).*

---

### Step 3: Reusable Row Hash Workspace (`serialize.py`)
**Goal:** Eliminate $O(1)$ `malloc`/`free` per row in the `compute_hash` function while retaining the exact binary padding structure necessary for hash determinism.

**1. Add a Fast `memset` and Workspace Class:**
At the top of `serialize.py`:
```python
from rpython.translator.tool.cbuild import ExternalCompilationInfo
# Use standard libc memset for blisteringly fast zero-padding
c_memset = rffi.llexternal('memset',[rffi.VOIDP, rffi.INT, rffi.SIZE_T], rffi.VOIDP, compilation_info=ExternalCompilationInfo(includes=['string.h']))

class HashWorkspace(object):
    def __init__(self, initial_cap=128):
        self.capacity = initial_cap
        self.buf = lltype.malloc(rffi.CCHARP.TO, initial_cap, flavor="raw")

    def ensure_capacity(self, sz):
        if sz > self.capacity:
            new_cap = max(self.capacity * 2, sz)
            lltype.free(self.buf, flavor="raw")
            self.buf = lltype.malloc(rffi.CCHARP.TO, new_cap, flavor="raw")
            self.capacity = new_cap

    def free(self):
        if self.buf:
            lltype.free(self.buf, flavor="raw")
            self.buf = lltype.nullptr(rffi.CCHARP.TO)
```

**2. Refactor `compute_hash` Signature and Logic:**
```python
@jit.unroll_safe
def compute_hash(schema, accessor, workspace):
    num_cols = len(schema.columns)
    sz = 0
    # 1. Calculate required size (Leave exact logic intact)
    # ... [existing size calc loop] ...

    # 2. Ensure capacity in the Reusable Workspace
    workspace.ensure_capacity(sz)

    # 3. CRITICAL: Zero out the buffer using fast C memset
    c_memset(rffi.cast(rffi.VOIDP, workspace.buf), 0, rffi.cast(rffi.SIZE_T, sz))

    # 4. Write data into the deterministic buffer
    ptr = workspace.buf
    # ... [existing packing loop] ...

    # 5. Hash & Return
    return xxh.compute_checksum(workspace.buf, sz)
```
*(Integration Note: The caller of `compute_hash` (e.g., in DBSP operators or write-ahead log serializers) must now instantiate a `HashWorkspace()` during initialization and pass it into this function).*

---

### Step 4: Zero-Copy String Hashing (`reduce.py`)
**Goal:** Prevent RPython from materializing short-lived strings (via `strings.resolve_string`) when extracting the 128-bit composite group key.

**1. Modify `_extract_group_key`:**
Replace the `TYPE_STRING` hashing branch with pure pointer logic:
```python
        if t == types.TYPE_STRING.code:
            length, _, struct_ptr, heap_ptr, py_string = accessor.get_str_struct(c_idx)
            
            if py_string is not None:
                # Unflushed python string
                with rffi.scoped_str2charp(py_string) as s_ptr:
                    h_lo ^= xxh.compute_checksum(s_ptr, length)
            else:
                # Flushed German String (Zero Allocation)
                if length <= strings.SHORT_STRING_THRESHOLD: # typically 12
                    # The prefix + tail are contiguous inside the struct starting at byte 4
                    target_ptr = rffi.ptradd(struct_ptr, 4)
                else:
                    # Long string: resolve pointer to the blob arena
                    u64_p = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(struct_ptr, 8))
                    heap_off = rffi.cast(lltype.Signed, u64_p[0])
                    target_ptr = rffi.ptradd(heap_ptr, heap_off)
                
                h_lo ^= xxh.compute_checksum(target_ptr, length)
```

---

### Quality Assurance & RPython Constraints Checklist

*   [x] **Monomorphism Compliance:** All `rffi` casts correctly use `rffi.ULONGLONG`. No mixing of `int` and `long` happens in the hot path.
*   [x] **GC Safety:** `gnitz_xxh3_128_inline` accepts primitives by value and returns a primitive. There are no invisible pointers the GC could lose track of.
*   [x] **Alignment Verification:** Pointer addition (`rffi.ptradd`) on `CCHARP` strings acts as standard byte addition. Reading `u64_p` from `struct_ptr + 8` is perfectly safe because German String structs are naturally 16-byte aligned.
*   [x] **Trace Unrolling:** The `@jit.elidable` on `_hash_key` and `@jit.unroll_safe` on `compute_hash` guarantee the loops will collapse down to linear execution tracks, resulting in tightly packed CPU assembly.

---

Note: later review of the plan brough up this point:

---


### 1. The Missing Item: `save_xor8` & `load_xor8` I/O Overhead
In my very first analysis, I identified that `save_xor8` and `load_xor8` were causing unnecessary allocations. While we successfully eliminated the `_scratch` buffer from `Xor8Filter` (removing the need for the "piggybacking" trick), the file I/O operations were left out of the final plans.

**The Correctness & Performance Pitfall:**
In `load_xor8`, you use `fp_data = rposix.read(fd, total_size)`. In RPython, `rposix.read` returns a newly allocated immutable string. For an 8MB XOR8 filter, this forces PyPy to allocate an 8MB string, copy it to the C-heap (`fingerprints[i] = fp_data[i]`), and immediately trigger a GC cycle to clean up the string. During database startup with thousands of shards, this will cause massive GC pauses.

**The Fix (Add to Part 1):**
You must bypass `rposix.read` for raw binary files and use the native C `read` directly into your pre-allocated C-buffer.

Add this to `mmap_posix.py`:
```python
_read_c = rffi.llexternal("read",[rffi.INT, rffi.VOIDP, rffi.SIZE_T], rffi.SSIZE_T, compilation_info=eci)

def read_into_ptr(fd, ptr, length):
    res = _read_c(rffi.cast(rffi.INT, fd), rffi.cast(rffi.VOIDP, ptr), rffi.cast(rffi.SIZE_T, length))
    return rffi.cast(lltype.Signed, res)
```
Update `load_xor8`:
```python
        # Zero-copy load directly into the C-heap!
        fingerprints = lltype.malloc(rffi.CCHARP.TO, total_size, flavor="raw")
        bytes_read = mmap_posix.read_into_ptr(fd, fingerprints, total_size)
        if bytes_read < total_size:
            lltype.free(fingerprints, flavor="raw")
            return None
```

