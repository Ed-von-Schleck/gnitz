# Appendix A: RPython Implementation Reference

**Target:** Systems developers.
**Context:** RPython is a statically analyzable subset of Python 2.7 that translates to C with a Meta-Tracing JIT.

## 1. The Translation Contract
*   **Global Type Inference:** The annotator analyzes all reachable code. Every variable must have a single, static type (Monomorphism) per control-flow merge point.
*   **Entry Point:** Do not use `__main__`. Define a target function:
    ```python
    def entry_point(argv): return 0 # Must return int
    def target(driver, args): return entry_point, None
    ```
*   **Dead Code:** Elimination occurs *after* annotation. Unreachable code that violates type constraints will still cause translation failure.

## 2. Language Subset & Restrictions
*   **Banned:** `eval`, `exec`, dynamic class/function creation, runtime metaprogramming, `**kwargs`, `set` (use `dict` keys), multiple inheritance (mostly), re-binding globals.
*   **Restricted:** Generators (simple iteration only), Recursion (avoid in hot paths), Exceptions (expensive in JIT).
*   **Globals:** Treated as immutable `const` after initialization. Do not mutate global module state; use a Singleton `State` class instance passed via `entry_point`.

## 3. Type System & Containers
*   **Lists:** Strictly homogeneous. `[1, "a"]` is illegal. Use a base class wrapper for heterogeneous data.
    *   **Resizability Poisoning:** If a list is created in a way that RPython considers "fixed-size," the entire listdef is marked as **must-not-resize (mr)**. Any subsequent `.append()` call on *any* instance of that list type will fail with `ListChangeUnallowed`.
    *   **Fixed-Size Triggers:**
        *   Literals: `[]`, `[x, y]`.
        *   Multiplication: `[None] * n`.
        *   Concatenation: `list_a + list_b` (if lengths are known at compile time).
    *   **The Solution:** Always use `rpython.rlib.objectmodel.newlist_hint(n)` to initialize lists destined for mutation or shared storage. 
    *   **Concatenation:** Instead of `a + b`, create a new list via `newlist_hint(len(a) + len(b))` and use explicit loops with `.append()`.
*   **Dicts:** Keys and values must be statically typed.
*   **Signatures:** Avoid `*args` unless the tuple layout is static. Prefer `List[T]` for variable arguments.
*   **None/Null:** Variables can be `Optional[T]`. However, mixing `T` and `None` in a list can confuse the annotator (Union types). Prefer the Null Object pattern over `None` in containers.

## 4. Integer Semantics (Critical)
*   **Semantics:** Python `int`/`long` translate to C `long` (machine word).
*   **Overflow:** Standard math uses C semantics (wraps or overflows).
    *   Use `rpython.rlib.rarithmetic.ovfcheck(x+y)` to catch overflow.
    *   Use `intmask(x)` to force truncation to machine word.
*   **Unsigned:** Use `r_uint` for bitwise ops/checksums.
*   **128-bit:** Natively supported via `r_ulonglonglong`. **Warning:** RPython types (`int`, `long`) are "frozen" descriptors. **Never** cast via `long(x)`; use specialized casting helpers.
*   **List Tracing Bug:** Storing `r_uint128` primitives directly in resizable lists can lead to alignment issues or SIGSEGV in the translated C code.
*   **Best Practice:** Split `u128` values into two `u64` lists (`keys_lo`, `keys_hi`) when storing them in resizable containers. Reconstruct the `u128` only at the point of computation/comparison.
*   **Prebuilt Long Trap:** Do not use literals > `sys.maxint` (e.g., `0xFFFFFF...`). Python 2 creates a `long` object, which cannot be frozen into the binary. Use `r_uint(-1)` or bit-shifts.
*   **FFI/Buffer Casting:** The `rffi.cast` utility cannot process `r_uint128` types directly. When writing 128-bit values to buffers or FFI pointers, you must explicitly truncate the value to `r_uint64` (e.g., `r_uint64(val)` or `r_uint64(val >> 64)`) before casting.
*   **128-bit Struct Alignment Bug:** Storing `r_uint128` primitives directly in resizable lists **or as persistent object attributes** (e.g., `self._current_key = r_uint128(0)`) can lead to strict-aliasing violations, alignment issues, or `SIGSEGV` in the translated C code. The RPython C backend does not consistently guarantee 16-byte alignment for these fields inside generated C structs.
*   **Best Practice:** Always split `u128` values into two `u64` components (`lo` and `hi`) when storing them as object state or inside containers. Reconstruct the `u128` dynamically (e.g., `(r_uint128(hi) << 64) | r_uint128(lo)`) only at the exact point of computation, return, or comparison.

## 5. Memory & FFI
*   **Allocations:** Visible to the annotator. Avoid object churn in loops.
*   **FFI:** Use `rpython.rtyper.lltypesystem.rffi`.
    *   **RAII:** Use `with rffi.scoped_str2charp(s) as ptr:` to prevent leaks.
    *   **Pointers:** Always validate `fd < 0` or `ptr` nullability.
    *   **Nullability:** `rffi.CCHARP` is a raw pointer. Always use `lltype.nullptr(rffi.CCHARP.TO)` for nulls. 
*   **I/O:** Avoid `os.read`/`write`. They introduce nullability ambiguity (`SomeString(can_be_None=True)`).
    *   Use `rpython.rlib.rposix.read`: Returns non-nullable string.
    *   Use `rposix_stat`: Returns strictly typed structs (fixed-width `st_ino`/`st_size`), preventing C-signedness warnings.
    *   **I/O:** Use `rpython.rlib.rposix.read` (returns non-nullable strings) rather than `os.read`.

## 6. JIT Optimization Guidelines
*   **Control Flow:** Keep hot loops simple (`while/for`). The JIT traces the interpreter loop; obscure control flow breaks the trace.
*   **Virtualizables:** Use `jit.promote()` on values that are effectively constant within a specific trace (e.g., Schema strides) to compile them as immediates.
*   **Unrolling:** Use `jit.unrolling_iterable` for loops over small, fixed lists (e.g., column schemas).
*   **Immutable Hinting:** Mark arrays/lists as immutable if they don't change after initialization to allow JIT constant folding.

## 7. Common Failure Modes
1.  **Monomorphism Violation:** `x = 1` then `x = "a"`.
2.  **FrozenDesc:** Calling `int()` or `long()` as functions on RPython primitives.
3.  **Prebuilt Long:** Using large integer literals directly.
4.  **Nullability Mismatch:** Passing a potentially `None` string (from `os`) to a function expecting strict `str` (like `rffi`).
5.  **List Mutation:** Calling `.append()` on a list previously hinted as immutable.
6.  **mr-Poisoning:** Using `[]` for a payload list, causing all future `.append` calls to crash.
7.  **u128-Alignment Crashes:** Storing raw 128-bit integers directly as class attributes or in resizable lists instead of splitting them into `lo/hi` `r_uint64` pairs, resulting in C-level struct alignment segfaults during property assignment or read-back.
8.  **Raw-Leak:** Malloc-ing a `dummy_ptr` in an accessor and never freeing it, corrupting the C heap.
9.  **`r_int64`-Long Overflow:** Storing a value in `[2**63, 2**64)` via `r_int64(x)` and later reading it back with `rffi.cast(rffi.ULONGLONG, ...)` causes `OverflowError: integer is out of bounds` in test mode. Use `r_uint64(x)` for the read-back and `rffi.cast(rffi.LONGLONG, r_uint64(x))` for the write-in.
10. **Slicing Proof Failure:** Using `s[:i]` or `s[i:]` where `i` is derived from arithmetic or `rfind`. The annotator fails with `AnnotatorError: slicing: not proven to have non-negative stop` because it cannot statically prove `i >= 0`.
    *   **Fix:** Avoid `os.path` (which slices internally) in favor of `rpython.rlib.rposix` functions. For manual slicing, use an explicit `assert i >= 0` immediately before the slice to guide the annotator's type inference.
11. **Signedness UnionError:** Occurs when a single function or method (e.g., `Buffer.put_i64`) is called in different places with both signed (`r_int64`) and unsigned (`r_uint64`) integers. RPython cannot unify these into a single type. **Fix:** Use `rffi.cast(rffi.LONGLONG, ...)` at the call site to ensure all inputs are consistently treated as signed machine words.
12. **Unsigned Logical Comparison:** Comparing unsigned bit patterns (retrieved via `get_int()`) for columns that are logically signed (`TYPE_I64`, etc.). This causes negative numbers to sort as larger than positive numbers. **Fix:** Always cast to `r_int64` (using 

# Appendix B: Coding practices

* Code should be formatted how the tool `black` would do it. If you change a file with bad formatting, fix the formatting carefully.
* Attributes, methods and functions starting with underscore (`_`) are considered private. They should not be used by code outside the class/module, not even by tests. If you encounter a test using a private method, fix it by either making it use a public attribute/method/function, or remove it entirely.
* When fixing a problem, don't leave code comments like `# FIXED: ...` or `# REMOVED: ...` or similar. Code comments are not for documenting change.
* Prefer module-level functions to staticmethods.
* This is a database. The best reaction to any unexpected error is usually to fail, and to fail as loudly as possible.

# Appendix C: AoS vs SoA Architecture Report

## Current Data Flow — Layer by Layer

### 1. ArenaZSetBatch (AoS)

The in-flight data format. Records are row-packed in a contiguous arena:

```
Record[i] at base + i * record_stride:
  [16: PK (u128)]
  [8:  Weight (i64)]
  [8:  Null bitmap (u64)]
  [memtable_stride: row-packed payload columns]
```

A secondary `blob_arena` holds string tails > 12 bytes (German String encoding). Column access is `payload_ptr + schema.column_offsets[col_idx]` — pointer arithmetic within the packed row.

Sorting (`to_sorted`) creates a permutation index, then copies entire records via `memmove` into a new batch. For varlen schemas, blobs must be re-serialized (offsets become invalid).

### 2. WAL (AoS)

Identical record layout to ArenaZSetBatch, streamed into a 32MB buffer. `WALWriter.append_batch()` iterates row-by-row, calling `serialize_row()` per record. Recovery reads records back via `RawWALAccessor`.

### 3. MemTable (AoS)

SkipList with each node holding a row-packed payload identical to the batch layout. `upsert_batch()` iterates the batch row-by-row, gets an accessor, and re-serializes into each SkipList node.

### 4. TableShardWriter — the AoS-to-SoA conversion

This is the single format conversion point. `add_row()` wraps the packed row pointer in a `RawWALAccessor`, then `_append_from_accessor()` iterates columns and appends each value to its own column buffer. Called during MemTable flush and compaction.

### 5. Columnar Shards (SoA)

On-disk format:
```
[Header 64B]
[Column Directory: N * 24B entries]
[PK column region]
[Weight column region]
[Col 1 region] [Col 2 region] ...
[Blob heap region]
```

`TableShardView` mmaps the file. Column access is `col_bufs[col_idx].ptr + row_idx * field_size` — pure columnar stride.

### 6. Cursors (row-by-row interface over both layouts)

All cursors present `key()`, `weight()`, `get_accessor()`, `advance()`:

- **MemTableCursor** → returns `PackedNodeAccessor` (reads from AoS node)
- **ShardCursor** → returns `SoAAccessor` (reads from columnar mmap)
- **UnifiedCursor** → tournament tree merge, returns whichever sub-cursor won

The consumer (DBSP ops) cannot tell which backing format it's reading from.

### 7. DBSP Operators (AoS batches, row-at-a-time)

All ops consume `RowAccessor` and write via `BatchWriter.append_from_accessor()` into AoS `ArenaZSetBatch`. Key patterns:

- **Filter/Negate/Union**: Zero-copy accessor forwarding (accessor reference passed directly to output writer, no field-by-field copy)
- **Map**: `MapRowBuilder` accumulates column values, then `commit_row()` serializes into AoS batch
- **Join**: `CompositeAccessor` virtually concatenates two accessors — no intermediate materialization
- **Reduce**: `ReduceAccessor` maps output columns to exemplar columns or aggregate value

### 8. VM Interpreter (row-at-a-time dispatch)

The register machine wraps each output register's batch in a `BatchWriter` and dispatches to op functions. All data flows through `RowAccessor`. The JIT specializes per-instruction and can unroll per-column loops via `@jit.unroll_safe`.

### 9. Compaction (SoA → AoS → SoA round-trip)

`compact_shards()` reads from `SoAAccessor`, copies columns byte-by-byte into a temporary packed row (`tmp_row`), then passes it to `TableShardWriter.add_row()` which immediately decomposes it back to columnar buffers. A gratuitous format conversion.

---

## Format Transition Map

```
Python PayloadRow (SoA-ish, single row)
        ↕ deserialize/serialize (slow path, only batch.get_row())
ArenaZSetBatch (AoS, arena)
    ├──→ WAL (AoS, disk)
    ├──→ MemTable SkipList (AoS, arena)
    │       └──→ TableShardWriter ──→ Columnar Shard (SoA, disk)
    │                                       ↑
    │                           Compaction (SoA→AoS→SoA round-trip)
    │
    └──→ DBSP Ops (AoS→AoS, via RowAccessor)
              ↑
         Cursor reads (SoA or AoS → RowAccessor abstraction)
```

---

## The Mismatch

The reviewer is correct: there are two distinct worlds with a clean but never-exploited boundary between them.

**AoS world** (hot path — all computation): ArenaZSetBatch, MemTable, WAL, all DBSP ops, VM interpreter. Every operator processes row-at-a-time through the RowAccessor interface.

**SoA world** (cold path — only persistence): Columnar shards on disk. Read back through cursors that present a row-at-a-time interface anyway.

The columnar shard format's main benefit — enabling vectorized column-at-a-time processing — is never used. The SoAAccessor reads one row at a time. The only actual benefit today is that mmap only pages in touched columns during a scan, reducing I/O amplification for partial-column reads (which the system doesn't do either, since all ops touch all columns through accessors).

---

## Option A: Unify on AoS (row-packed everywhere)

Replace columnar shards with row-packed shard files. The on-disk format becomes essentially the same as ArenaZSetBatch / WAL records, sorted by PK.

### Changes Required
- Replace `TableShardWriter` with a row-packed writer (trivial — just write sorted batch records to file)
- Replace `TableShardView` with mmap over row-packed records
- `ShardCursor` becomes pointer arithmetic (like `RawWALAccessor` over mmap)
- Remove `SoAAccessor`, column directory, per-column buffers
- Compaction becomes a direct merge-write — no format conversion at all

### Pros
- **Eliminates both format conversions** (AoS→SoA flush, SoA→AoS→SoA compaction)
- **Simplifies the codebase significantly**: `writer_table.py` shrinks to a trivial sorted writer, `compactor.py` loses the tmp_row ceremony, `shard_table.py` loses per-column buffer management
- **Faster MemTable flush**: no column decomposition, just write sorted records
- **Faster compaction**: direct record copy, no per-column iteration
- **Simpler WAL recovery**: WAL format and shard format are the same
- **Natural alignment with how data is actually consumed**: row-at-a-time through DBSP ops
- **MemTable flush can potentially skip re-serialization**: SkipList walk produces records already in the target format

### Cons
- **Read amplification**: reading a single column from disk requires paging in entire rows. With 10 columns, a scan touching 1 column reads 10x more data
- **Closes the door on future columnar optimizations**: if you later want vectorized column scans, SIMD filtering, or column pruning, you'd need to re-introduce SoA
- **Compression disadvantage**: columnar layouts compress better because similar values (same column) are adjacent. Row-packed data mixes types, hurting compression ratios
- **Large row penalty**: wide tables (many columns) waste I/O bandwidth on untouched columns during partial scans

### Verdict
Best fit if the system will remain row-oriented in its operator model and tables are narrow (few columns). The simplification is substantial.

---

## Option B: Unify on SoA (columnar everywhere)

Replace `ArenaZSetBatch` with a columnar in-memory batch. Each batch holds separate column arrays (PK array, weight array, per-column arrays). DBSP operators process data column-at-a-time or at least store intermediates columnar.

### Changes Required
- Rewrite `ArenaZSetBatch` as a columnar batch (one `Buffer` per column, similar to `TableShardWriter`'s internal structure)
- Rewrite `BatchWriter` to append to per-column buffers
- Rewrite or remove `serialize_row()` / `RawWALAccessor` — data is no longer row-packed
- Either: (a) rewrite DBSP ops to process column-at-a-time (vectorized), or (b) keep the RowAccessor interface but back it with columnar access (SoAAccessor already does this)
- MemTable needs rethinking: SkipList nodes can't hold row-packed payloads. Options: store column indices or keep a separate columnar buffer
- WAL needs rethinking: either write columnar mini-batches, or keep row-streaming and convert on recovery
- MemTable flush becomes trivial: columnar batch → columnar shard is a direct write
- Compaction: SoA → SoA, no conversion needed

### Pros
- **Eliminates both format conversions**
- **Enables future vectorized operations**: SIMD filtering, column pruning, late materialization
- **Better compression**: same-type values adjacent
- **Minimal I/O amplification**: only touched columns are read from disk (already true for shards)
- **Columnar MemTable flush**: column buffers write directly to shard column regions
- **Natural fit for analytical workloads**: column scans, aggregations, projections

### Cons
- **Massive rewrite**: ArenaZSetBatch is the most central data structure. Every DBSP op, every test, every serialization path touches it
- **Row construction becomes expensive**: assembling a full row (e.g., for output to a client) requires gathering from N column arrays. This is the opposite of the current zero-copy row forwarding
- **MemTable complexity**: SkipList-based sorted insertion doesn't naturally fit columnar layout. Options are all awkward: (a) store row indices + columnar buffers (loses cache locality during inserts), (b) use a different sorted structure (B+ tree on column arrays)
- **Sorting penalty**: sorting columnar data requires permuting all column arrays, not just swapping record pointers. N column arrays x M rows to permute
- **JIT interaction**: RPython's JIT optimizes the current accessor pattern well. Columnar vectorized loops would need different JIT annotations
- **Zero-copy accessor forwarding breaks**: filter currently forwards an accessor reference (zero-copy). With columnar batches, "forwarding a row" means copying from N column arrays into N output column arrays
- **WAL complexity**: row-streaming WAL is simple and crash-safe. Columnar WAL batches require careful framing

### Verdict
Maximum future potential, but the rewrite cost is extreme and the current operator model (row-at-a-time DBSP) doesn't benefit from it. Only justified if the roadmap includes vectorized query execution.

---

## Option C: Keep the split, fix the warts (status quo + cleanup)

Keep AoS for in-flight data and SoA for persistent shards, but fix the gratuitous conversion in compaction and tighten the boundary.

### Changes Required
1. **Fix compaction SoA→AoS→SoA round-trip**: add `TableShardWriter.add_row_from_accessor(key, weight, accessor)` that directly appends from any `RowAccessor` to column buffers, skipping the `tmp_row` intermediate. The `_append_from_accessor` method already exists — just expose it:
   ```python
   def add_row_from_accessor(self, key, weight, accessor):
       if weight == 0:
           return
       self.count += 1
       self.pk_buf.put_...(key)
       self.w_buf.put_i64(weight)
       self._append_from_accessor(accessor)
   ```
   Compaction calls `writer.add_row_from_accessor(min_key, net_weight, acc)` directly.

2. **Optional: add direct SoA column copy for compaction**: when all inputs are shards (no MemTable), copy column regions directly with `memmove` for non-overlapping key ranges, only falling back to per-column accessor copy for key ranges that need weight merging.

### Pros
- **Minimal change**: the fix is ~10 lines of code
- **Preserves optionality**: SoA shards remain available for future columnar optimizations
- **No DBSP op changes**: the hot path is untouched
- **Compaction gets faster**: eliminates the tmp_row allocation and byte-by-byte copy loop

### Cons
- **Doesn't address the fundamental mismatch**: the SoA shard format's columnar benefits are still unused
- **Two data layouts to maintain**: continued cognitive overhead for contributors
- **The AoS→SoA conversion during flush remains**: this is inherent to the split design and can't be eliminated without choosing one format

### Verdict
Low-risk, low-effort fix that removes the most egregious wart. Good as an immediate action regardless of longer-term direction.

---

## Option D: AoS with columnar projection shards (hybrid)

Store shards in AoS (row-packed, like Option A), but add an optional columnar projection index alongside each shard for columns that benefit from columnar access.

### Changes Required
- Primary shard format becomes row-packed (same as Option A)
- Optional: during flush or compaction, also emit column-specific projection files for selected columns (e.g., the PK column for binary search, or a column used in a filter predicate)
- `ShardCursor` reads from row-packed primary by default
- Projection files are used opportunistically (e.g., PK binary search, predicate pushdown)

### Pros
- **Primary path is simple**: row-packed, no format conversion
- **Selective columnar benefit**: only columns that actually benefit get columnar projections
- **Incremental adoption**: start with just PK projection (for seek), add more later
- **No DBSP op changes**

### Cons
- **More on-disk files to manage**: shard + projection files per shard
- **Projection maintenance**: must be kept in sync during compaction
- **Complexity without clear payoff**: if no operator actually does columnar processing, the projections are write-only overhead
- **Design complexity**: deciding which columns to project, when to use projections vs primary

### Verdict
Over-engineered for the current system. Only makes sense if specific columnar operations (predicate pushdown, column pruning) are on the roadmap.

---

## Recommendation

**Immediate**: Option C. Fix the compaction round-trip. It's a clear bug/inefficiency with a trivial fix.

**Short-term decision**: Option A vs staying with C.

The key question is: **will the operator model ever go columnar?**

- If the system stays DBSP row-at-a-time (which the JIT-based RPython architecture strongly favors), Option A is the right call. The codebase simplification is significant: `writer_table.py`, `shard_table.py`, `compactor.py`, and `layout.py` all get simpler. The SoA format is paying complexity rent for benefits that aren't collected.

- If vectorized columnar execution is planned, keep Option C and evolve toward Option B incrementally. But this would be a major architectural shift that fights the RPython JIT model (which optimizes row-at-a-time loops, not SIMD column operations).

Given the current architecture — RPython JIT, row-at-a-time DBSP operators, accessor-based polymorphism — **Option A (unify on AoS) is the strongest simplification**. The columnar shard format is premature infrastructure.
