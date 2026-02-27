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
