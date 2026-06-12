# Hugepage-aligned batch allocation

## Problem

Large batch data buffers in `storage/batch.rs` use `MADV_HUGEPAGE` to
request transparent hugepage backing, but the hint is applied to `Vec<u8>`
memory returned by the system allocator (glibc malloc — no custom
allocator is configured). Glibc guarantees only 16-byte alignment for heap
allocations; for a 2 MB buffer at an arbitrary 16-byte-aligned address the
entire region may fall within a single 2 MB-aligned window without
covering it completely, so the kernel promotes zero pages. The
`MADV_HUGEPAGE` hint is unreliable for this path.

There are four sites in `batch.rs` with this pattern:

| Site | Allocation | Wasted if misaligned |
|------|------------|----------------------|
| `with_schema` (`:298`) | `vec![0u8; total_size]` + madvise | all ≥ 2 MB batches |
| `reserve_rows` (`:590`) | `Vec::with_capacity` + set_len + madvise | all grows into ≥ 2 MB |
| `alloc_large_zeroed` (`:83`) | `vec![0u8; size]` + madvise | used by `write_to_batch` arena |
| `write_to_batch` blob (`:1728`) | pooled `Vec` + madvise on `as_mut_ptr()` | blob buffers ≥ 2 MB |

The W2M ring region (`bootstrap.rs:351`) and shard file mappings
(`shard_reader.rs:62`) come from `mmap` directly — those calls are correct
and are not changed by this plan.

## Goal

Replace the `vec![] + MADV_HUGEPAGE` pattern with:
1. A first attempt using `MAP_HUGETLB` — deterministic allocation from the
   hugepage pool on systems where `vm.nr_hugepages > 0`. Zero TLB
   pressure on first access; no subsequent kernel promotion needed.
2. A fallback that over-allocates via anonymous `mmap` to guarantee 2 MB
   alignment, then applies `MADV_HUGEPAGE` to the aligned region. Works on
   all systems regardless of hugepage pool configuration.

## New type: `HugeBuf`

Introduce `HugeBuf` in `storage/batch.rs` (or `sys.rs`) as the buffer
backing for `Batch.data` and the `write_to_batch` arena:

```rust
pub(crate) enum HugeBuf {
    /// Small allocation: backed by a recycled or fresh Vec<u8>.
    Small(Vec<u8>),
    /// Large allocation: backed by a direct mmap region.
    /// `cap` may exceed `len` because the aligned sub-region may be
    /// smaller than the over-allocated mapping.
    Mmap { ptr: std::ptr::NonNull<u8>, cap: usize, len: usize },
}

impl Drop for HugeBuf {
    fn drop(&mut self) {
        if let HugeBuf::Mmap { ptr, cap, .. } = self {
            unsafe { libc::munmap(ptr.as_ptr() as *mut libc::c_void, *cap); }
        }
        // Vec variant: Drop handles dealloc.
    }
}

impl std::ops::Deref for HugeBuf {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            HugeBuf::Small(v) => v.as_slice(),
            HugeBuf::Mmap { ptr, len, .. } =>
                unsafe { std::slice::from_raw_parts(ptr.as_ptr(), *len) },
        }
    }
}

impl std::ops::DerefMut for HugeBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        match self {
            HugeBuf::Small(v) => v.as_mut_slice(),
            HugeBuf::Mmap { ptr, len, .. } =>
                unsafe { std::slice::from_raw_parts_mut(ptr.as_ptr(), *len) },
        }
    }
}

impl HugeBuf {
    pub fn capacity(&self) -> usize {
        match self {
            HugeBuf::Small(v) => v.capacity(),
            HugeBuf::Mmap { cap, .. } => *cap,
        }
    }
    pub fn len(&self) -> usize {
        match self {
            HugeBuf::Small(v) => v.len(),
            HugeBuf::Mmap { len, .. } => *len,
        }
    }
    // Used by pool recycle path — only called on Small variants.
    pub fn into_vec(self) -> Option<Vec<u8>> {
        match self { HugeBuf::Small(v) => Some(v), HugeBuf::Mmap { .. } => None }
    }
}
```

`HugeBuf` is `Send` (the pointer is owned and not aliased).

## Allocator functions in `sys.rs`

```rust
const HUGEPAGE_SIZE: usize = 2 * 1024 * 1024;

/// Allocate `size` bytes with at least HUGEPAGE_SIZE alignment, zeroed.
/// Tries MAP_HUGETLB first; falls back to over-aligned anonymous mmap
/// with MADV_HUGEPAGE; panics if mmap fails entirely.
pub fn alloc_huge_zeroed(size: usize) -> (*mut u8, usize) {
    // Attempt 1: MAP_HUGETLB — deterministic, requires vm.nr_hugepages > 0.
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_HUGETLB,
            -1, 0,
        )
    };
    if ptr != libc::MAP_FAILED {
        return (ptr as *mut u8, size);
    }

    // Attempt 2: over-allocate to guarantee 2 MB alignment.
    // Allocate size + HUGEPAGE_SIZE extra bytes, align upward to the next
    // HUGEPAGE_SIZE boundary, unmap the leading and trailing waste.
    let oversize = size + HUGEPAGE_SIZE;
    let raw = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            oversize,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1, 0,
        )
    };
    assert!(raw != libc::MAP_FAILED, "alloc_huge_zeroed: mmap failed");
    let raw_addr = raw as usize;
    let aligned_addr = (raw_addr + HUGEPAGE_SIZE - 1) & !(HUGEPAGE_SIZE - 1);
    let lead = aligned_addr - raw_addr;
    let tail = oversize - lead - size;
    if lead > 0 {
        unsafe { libc::munmap(raw, lead); }
    }
    let aligned_ptr = aligned_addr as *mut u8;
    if tail > 0 {
        unsafe { libc::munmap(aligned_ptr.add(size) as *mut libc::c_void, tail); }
    }
    madvise_hugepage(aligned_ptr, size);
    (aligned_ptr, size)
}
```

The returned mapping is zero-filled by the OS (`MAP_ANONYMOUS`). The
caller owns it and must `munmap(ptr, len)` on drop — this is what
`HugeBuf::Mmap`'s `Drop` does.

## Changes to `batch.rs`

**Change `Batch.data` type from `Vec<u8>` to `HugeBuf`.**

All slice accesses (`self.data[off..off+n]`) already go through indexing
and work unchanged via `Deref`. Capacity checks (`self.data.capacity()`)
use `HugeBuf::capacity()`.

**Pool recycle:** `batch_pool` pools `Vec<u8>`. The `HugeBuf::into_vec()`
method returns `Some(v)` for `Small` variants, `None` for `Mmap`. The
recycle path checks:

```rust
pub(crate) fn recycle_buf(buf: HugeBuf) {
    if let Some(v) = buf.into_vec() {
        // existing Vec recycle logic
        recycle_vec(v);
    }
    // Mmap variant: Drop munmaps; no pool involvement.
}
```

**Replace the four allocation sites:**

```rust
// with_schema and alloc_large_zeroed (initial allocation):
let data: HugeBuf = if total_size >= HUGEPAGE_THRESHOLD {
    let (ptr, cap) = crate::sys::alloc_huge_zeroed(total_size);
    HugeBuf::Mmap {
        ptr: NonNull::new(ptr).unwrap(),
        cap,
        len: total_size,
    }
} else {
    HugeBuf::Small(/* existing pool/vec path */)
};

// reserve_rows (grow path) — same pattern, no zeroing needed:
let (ptr, cap) = crate::sys::alloc_huge_zeroed(new_total);  // mmap is zero by OS
// scatter-copy existing data, then drop old HugeBuf
```

**The blob buffer** in `write_to_batch` remains a `Vec<u8>` (blobs are
populated by `extend_from_slice` and don't have a fixed pre-computed size;
the pool recycles them). Remove the `madvise_hugepage` call on blob buffers
— the pool's cap limit (`MAX_RECYCLE_CAPACITY = 2MB`) already prevents
large blobs from being pooled, and the `Vec`-backed blob does not benefit
from an unaligned THP hint.

## Scope

This plan covers only `batch.rs`. The W2M ring (`bootstrap.rs:351`) and
shard file mappings (`shard_reader.rs:62`) already use `mmap` directly and
are not changed.

## Migration order

1. Add `HugeBuf` enum and `alloc_huge_zeroed` to `sys.rs`. No callers yet.
2. Change `Batch.data: Vec<u8>` → `Batch.data: HugeBuf`. Update all
   access sites (capacity check, recycle path). `make test` green.
3. Replace `alloc_large_zeroed` and the inline allocation in `with_schema`
   with `alloc_huge_zeroed`. `make test` green.
4. Replace the `reserve_rows` grow path. `make test` green.
5. Remove the `madvise_hugepage` call from the blob buffer in
   `write_to_batch`. `make test` green.
6. `make e2e` (polled).

## Testing

- Unit: allocate a `HugeBuf` at `HUGEPAGE_THRESHOLD` and verify the
  returned pointer is 2 MB-aligned (`ptr as usize % HUGEPAGE_SIZE == 0`).
- Unit: `Drop` correctness — allocate and immediately drop several
  `HugeBuf::Mmap` buffers; verify no address-space leak (check
  `/proc/self/maps` or use address sanitizer in debug builds).
- Unit: `reserve_rows` growth path — create a batch that crosses the
  `HUGEPAGE_THRESHOLD` during growth; verify data integrity post-grow.
- Existing `make test` suite covers Batch correctness end-to-end.
- `make e2e` for full integration.
