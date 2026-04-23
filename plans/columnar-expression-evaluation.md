# Columnar Expression Evaluation — IMPLEMENTED

## Context

`Plan::execute_map` has a layered pipeline:

1. System columns — batch memcpy
2. `ColMove` / `copy_column` — batch memcpy per column
3. `NullPerm::apply_column` — bulk bitwise permutation over the full null bitmap
4. `ComputeKernel::Interpreted` — **per-row interpreter loop**

Steps 1–3 are uniformly columnar: each operates on contiguous column slices with
no per-row dispatch. Step 4 breaks the pattern. For each of N rows,
`eval_with_emit` allocates a stack register file, iterates every opcode,
re-derives each column's base pointer from scratch, and writes EMIT results
via raw `EmitTarget` pointers into output column buffers. `op_filter` has the
same shape: a per-row `evaluate_predicate` call inside the contiguous-range
detection loop.

This plan makes step 4 match steps 1–3: a sequence of columnar passes over
register buffers, with pointer arithmetic hoisted out of inner loops and LLVM
free to auto-vectorize every arithmetic opcode.


## What to remove first

`FilterKernel::CompareConst`, `Plan::from_compare`, and the three `OP_*`
constants are dead code — no SQL compiler path calls `from_compare`. The variant
also has a correctness bug: it never checks the null bitmap, so null column
values pass predicates they should fail. Remove all three before beginning the
batch interpreter work. The batch interpreter handles the same pattern (a single
comparison against a constant) with correct null semantics and no special case.


## EvalScratch — the SoA register file

```rust
pub struct EvalScratch {
    /// Register buffers, register-major layout:
    ///   regs[reg * MORSEL + row]   ← always the compile-time constant MORSEL,
    ///                                not the variable morsel size m.
    /// One i64 per (register, row) pair. Capacity = num_regs * MORSEL.
    /// Each register region is 256 × 8 = 2048 bytes, so register boundaries
    /// are always 2048-byte aligned relative to the Vec base. The default
    /// allocator guarantees ≥16-byte alignment; LLVM's auto-vectorizer uses
    /// unaligned loads (VMOVDQU / VPMASKMOVQ) which are penalty-free on modern
    /// CPUs. No custom allocator is required.
    regs: Vec<i64>,
    /// Null bitmask, register-major layout:
    ///   null_bits[reg * null_words + word]
    /// One bit per (register, row) pair, packed 64 per u64.
    /// Absent (len == 0) when the program has no nullable inputs.
    null_bits: Vec<u64>,
    /// Filter output bitmask: one bit per row, 64 per u64.
    /// Used only by filter_batch.
    filter_bits: Vec<u64>,
}
```

`MORSEL = 256`. For 8 registers: `8 × 256 × 8 = 16 KB` for `regs`, 256 bytes
for `null_bits`. This fits in L1. Each opcode operates on a contiguous register
slice of length M (the morsel size, ≤ MORSEL); LLVM sees a fixed-stride loop
with no pointer aliasing and auto-vectorizes.

`EvalScratch` should be passed in by callers (`&mut EvalScratch`) rather than
owned by `Plan`. This keeps `Plan` immutable and is zero-overhead at runtime.
Each operator (`op_filter`, `execute_map`) owns or borrows a scratch from its
enclosing context. If a thread-local is desired, model it after `batch_pool`.

Alternatively, `EvalScratch` can live inside `Plan` as a `RefCell<EvalScratch>`
with one integer comparison overhead per call — acceptable given DBSP is
single-threaded per process, but the passed-in form is architecturally
preferable.

The scratch is resized (never shrunk) if a batch larger than the current
capacity arrives. In practice, morsel_size is bounded by MORSEL so this never
fires after the first call.


## Batch interpreter structure

### Outer morsel loop

```rust
fn eval_batch(prog: &ExprProgram, mb: &MemBatch, n: usize, scratch: &mut EvalScratch, pki: usize) {
    for morsel_start in (0..n).step_by(MORSEL) {
        let m = (morsel_start + MORSEL).min(n) - morsel_start;
        scratch.clear_null(m);   // zero only the live null_bits; skipped if no_nulls

        for instr in prog.code.chunks_exact(4) {
            let (op, dst) = (instr[0], instr[1] as usize);
            // a1/a2 are decoded as usize for register indices in most opcodes,
            // but LOAD_CONST reads them as raw i64 inside its match arm — do NOT
            // apply `as usize` here, because negative 32-bit constant halves
            // (e.g., -1i64) would become usize::MAX and corrupt reconstruction.
            let (a1_raw, a2_raw) = (instr[2], instr[3]);
            let r_dst = scratch.reg_mut(dst, m);
            // ... opcode dispatch (once per opcode per morsel, not per row)
            // For all non-LOAD_CONST opcodes: let (a1, a2) = (a1_raw as usize, a2_raw as usize);
        }

        // Pack result register into filter_bits, or copy EMIT registers to output.
    }
}
```

The opcode `match` executes once per opcode per morsel. The inner loop over
rows is a tight slice operation. The per-opcode, per-morsel structure is what
enables vectorization: LLVM sees `for i in 0..m { dst[i] = a[i] + b[i] }` with
no intervening control flow.

### LOAD opcodes: base hoisting and width dispatch

For `LOAD_PAYLOAD_INT` with physical column index `pi` and byte stride `s`:

```rust
EXPR_LOAD_PAYLOAD_INT => {
    // mb.col_data[pi] is &[u8] for the full column; slice from morsel_start.
    // MemBatch.col_data is Vec<&[u8]> — direct slice indexing, no unsafe needed.
    let base = &mb.col_data[pi][morsel_start * s ..];
    let dst = scratch.reg_mut(dst, m);
    // Guard: payload_col_type_codes is empty if set_payload_col_info was never
    // called (legacy mode). Mirror the existing per-row fallback: treat as 8-byte i64.
    if prog.payload_col_sizes.is_empty() {
        let base8 = &mb.col_data[pi][morsel_start * 8 .. (morsel_start + m) * 8];
        for (i, chunk) in base8.chunks_exact(8).enumerate() {
            dst[i] = i64::from_le_bytes(chunk.try_into().unwrap());
        }
        if !no_nulls { load_null_bits(scratch, dst_idx, pi, mb, morsel_start, m); }
        continue;
    }
    let is_signed = matches!(prog.payload_col_type_codes[pi], tc::I8 | tc::I16 | tc::I32 | tc::I64);
    // Pre-slice to m*s bytes so LLVM can prove loop bounds without per-iteration
    // checks — use chunks_exact rather than base[i*s..][..s] indexing.
    match s {
        8 => { let b = &mb.col_data[pi][morsel_start*8..(morsel_start+m)*8];
               for (i, c) in b.chunks_exact(8).enumerate() { dst[i] = i64::from_le_bytes(c.try_into().unwrap()); } }
        4 => if is_signed {
                 let b = &mb.col_data[pi][morsel_start*4..(morsel_start+m)*4];
                 for (i, c) in b.chunks_exact(4).enumerate() { dst[i] = i32::from_le_bytes(c.try_into().unwrap()) as i64; }
             } else {
                 let b = &mb.col_data[pi][morsel_start*4..(morsel_start+m)*4];
                 for (i, c) in b.chunks_exact(4).enumerate() { dst[i] = u32::from_le_bytes(c.try_into().unwrap()) as i64; }
             }
        2 => if is_signed {
                 let b = &mb.col_data[pi][morsel_start*2..(morsel_start+m)*2];
                 for (i, c) in b.chunks_exact(2).enumerate() { dst[i] = i16::from_le_bytes(c.try_into().unwrap()) as i64; }
             } else {
                 let b = &mb.col_data[pi][morsel_start*2..(morsel_start+m)*2];
                 for (i, c) in b.chunks_exact(2).enumerate() { dst[i] = u16::from_le_bytes(c.try_into().unwrap()) as i64; }
             }
        1 => if is_signed {
                 for i in 0..m { dst[i] = mb.col_data[pi][morsel_start + i] as i8 as i64; }
             } else {
                 for i in 0..m { dst[i] = mb.col_data[pi][morsel_start + i] as i64; }
             }
        _ => unreachable!()
    }
    if !scratch.no_nulls {
        load_null_bits(scratch, dst_idx, pi, mb, morsel_start, m);
    }
}
```

`ExprProgram` already carries `payload_col_type_codes` (same indexing as
`payload_col_sizes`). The `is_signed` check must use it — a U32 column whose
value exceeds `2^31 - 1` would become negative if widened with `i32::from_le_bytes`.

Two wins over the per-row interpreter:

1. **Base hoisted once per opcode** — the per-row interpreter calls
   `get_col_ptr(row, pi, size)` for every row, re-computing `col_data_base +
   row * stride` each time. Here the base is computed once.
2. **Width dispatched once per opcode** — the per-row interpreter branches on
   `payload_col_sizes[pi]` for every row. Here the branch executes once and
   each inner loop is a uniform widen operation that LLVM vectorizes
   (e.g., `VPMOVSXDQ` for 4→8 byte signed widening).

For the 8-byte case (BIGINT, the common type), the inner loop is a contiguous
i64 load — effectively a memcpy over aligned data that LLVM turns into AVX2
loads.

`LOAD_PK_INT` and `LOAD_CONST` follow the same structure: one pointer or value
derived once, then a fill loop over the morsel.

### Arithmetic and comparison

```rust
EXPR_INT_ADD => {
    let (a, b, d) = scratch.reg3(a1, a2, dst, m);
    for i in 0..m { d[i] = a[i].wrapping_add(b[i]); }
    if !scratch.no_nulls { null_or2(scratch, dst, a1, a2, m); }
}
```

`null_or2` is a word-level OR over the packed null bitmask:
```rust
fn null_or2(s: &mut EvalScratch, dst: usize, a: usize, b: usize, m: usize) {
    let words = (m + 63) / 64;
    let (na, nb, nd) = s.null_words3(a, b, dst, words);
    for w in 0..words { nd[w] = na[w] | nb[w]; }
}
```

This is one bitwise OR per 64 rows, not one per row. LLVM auto-vectorizes the
null propagation loop as well as the arithmetic loop.

Comparisons produce 0 or 1 per element — the same tight loop pattern. No
special case for result packing: the result lives in a register buffer until
`filter_bits` packing at morsel end.

### Division: replace-and-mask

Division requires null-on-zero-divisor semantics without per-element branching
in the main loop:

```rust
EXPR_INT_DIV => {
    let (num, den, d) = scratch.reg3(a1, a2, dst, m);
    // Pass 1: build zero-divisor mask; use a local variable — do NOT mutate den
    let mut zero_mask_words = [0u64; MORSEL / 64];
    for i in 0..m {
        if den[i] == 0 { zero_mask_words[i/64] |= 1u64 << (i%64); }
        let divisor = if den[i] == 0 { 1 } else { den[i] };
        d[i] = num[i].wrapping_div(divisor);
    }
    // Pass 2: OR null-from-operands and zero-divisor mask into dst null bits
    // no_nulls is always false when INT_DIV/INT_MOD/FLOAT_DIV are present
    // (these opcodes produce dynamic nulls regardless of schema nullability).
    null_or2(scratch, dst, a1, a2, m);
    let nd = scratch.null_words_mut(dst, (m+63)/64);
    for w in 0..(m+63)/64 { nd[w] |= zero_mask_words[w]; }
}
```

Do **not** mutate `den` to replace zeros with 1. `reg3` returns `den` as a
shared reference (`&[i64]`); mutating it would require unsafe and would corrupt
the register if the divisor operand is reused later in the same program (e.g.,
`A / B` and `A % B` in the same expression). Instead, select the divisor into a
local variable in the loop body so `den` is never touched.

Pass 1 has a data-dependent branch but runs only once per row. The conditional
select compiles to a `CMOV` — no mispredictions. The same structure applies to
`FLOAT_DIV` (test `rhs == 0.0`) and `INT_MOD`.

Note: `wrapping_div` returns `i64::MIN` for `i64::MIN / -1` — matching the
existing per-row interpreter's behavior exactly (see `expr.rs` line 445). If
SQL overflow-error semantics are ever required, this is the place to add the
`-1` divisor check alongside the zero-divisor check.

### SQL 3VL boolean operators

`BOOL_AND` and `BOOL_OR` require three-valued logic that cannot be reduced to
bitwise ops on values alone. They operate correctly only on a word-at-a-time
basis using the null bitmask:

```rust
EXPR_BOOL_AND => {
    let words = (m + 63) / 64;
    let d = scratch.reg_mut(dst, m);
    for w in 0..words {
        let v1 = val_word(scratch, a1, w);   // packed nonzero bits
        let v2 = val_word(scratch, a2, w);
        let n1 = null_word(scratch, a1, w);
        let n2 = null_word(scratch, a2, w);
        // FALSE AND anything = FALSE (non-null)
        let definite_false = (!n1 & !v1) | (!n2 & !v2);
        let is_null = !definite_false & (n1 | n2);
        let result_word = (v1 & v2) & !is_null;
        set_val_word(scratch, dst, w, result_word);
        set_null_word(scratch, dst, w, is_null);
        // Unpack the result bitmask back into regs[dst] so that EMIT and
        // downstream arithmetic read correct 0/1 values.
        let base = w * 64;
        let end = (base + 64).min(m);
        for i in base..end {
            d[i] = ((result_word >> (i - base)) & 1) as i64;
        }
    }
}
```

`val_word` packs 64 register values into a u64 bitmask (bit set iff value ≠ 0)
on demand — one call to `scratch.pack_val_bits(reg, m)` per word per BOOL op.
The packing loop is `for i in 0..64 { w |= (v[i] != 0) as u64 << i }` which
LLVM vectorizes to `VPCMPEQQ` + `VPMOVMSKB`. No caching is needed: under SSA
each register is written exactly once, so `pack_val_bits` on a source register
always sees the final, correct value. No dirty-bits tracking required.

The unpack step is required: `EMIT` and any arithmetic opcode that reads `dst`
will call `reg_slice(dst)` which reads from `regs`. Without the unpack, those
callers see uninitialized `i64` values.

### IS_NULL / IS_NOT_NULL

These opcodes retain logical column indices (intentionally — `resolve_column_indices` does not remap them, matching the existing `eval_predicate` behavior documented in `expr.rs:590-592`). In the batch interpreter they read from the batch's row-major null bitmap, not from EvalScratch:

```rust
EXPR_IS_NULL => {
    // a1 is a logical column index. PK column is never null (pki → false).
    let pi = if a1 < pki { a1 } else { a1 - 1 };
    let d = scratch.reg_mut(dst, m);
    for i in 0..m {
        let null_word = mb.get_null_word(morsel_start + i);
        d[i] = if a1 == pki { 0 } else { ((null_word >> pi) & 1) as i64 };
    }
    // Result is never itself null — clear the dst null bits.
    let words = (m + 63) / 64;
    let nd = scratch.null_words_mut(dst, words);
    for w in nd { *w = 0; }
}
```

`IS_NOT_NULL` is identical with the logic inverted (`1 - d[i]`). Note that `pki` must be threaded through to `eval_batch` as an explicit parameter; `ExprProgram` does not store it (the post-`resolve_column_indices` remapped opcodes no longer need it, but the unremapped ones do).

`no_nulls` cannot be true when any `IS_NULL`/`IS_NOT_NULL` opcode is present — not because the result is null, but because skipping null-bit initialization would leave the `EvalScratch` null words in an uncleared state for any opcode that does propagate nulls.

### String operations: two-pass prefix batching

German Strings store a big-endian 4-byte prefix at bytes 4–7 of the 16-byte
struct. For `STR_COL_EQ_CONST`:

```rust
// Pass 1: vectorized prefix comparison across all M rows
let const_pfx = prog.const_prefixes[str_idx];
// mb.col_data[pi] is &[u8] for the full column; slice from morsel_start.
// STR_COL opcodes use a logical column index (a1) that is NOT remapped by
// resolve_column_indices; the physical payload index pi = a1 − (a1 > pki) as usize.
let col_base = &mb.col_data[pi][morsel_start * 16 ..];
let mut ambiguous: SmallVec<[u16; 8]> = SmallVec::new();
for i in 0..m {
    dst[i] = 0;
    // Skip null rows using the batch's row-major null bitmap.
    // EvalScratch null_bits cannot be used here: string opcodes write no
    // register before this point, so no register-major null entry exists.
    let null_word = mb.get_null_word(morsel_start + i);
    if (null_word >> pi) & 1 != 0 { continue; }
    let col_pfx = u32::from_be_bytes(col_base[i*16+4..][..4].try_into().unwrap());
    if col_pfx == const_pfx { ambiguous.push(i as u16); }
}

// Pass 2: full comparison only for ambiguous rows (typically 0 or very few)
for &i in &ambiguous {
    let s = &col_base[i as usize * 16..][..16];
    dst[i as usize] = col_string_equals_const(s, mb.blob, ...) as i64;
    // No null check needed here: null rows were excluded from ambiguous above.
}
```

Pass 1 is a load + integer compare loop over 4-byte values spaced 16 bytes
apart — stride-16 access, vectorizable with gather instructions or scalar with
good prefetching. Pass 2 is uncommon: for a selective equality predicate, most
prefixes differ and the ambiguous set is empty or tiny. This eliminates tail
blob pointer dereferences for the vast majority of rows.

### Null elision

`Plan::from_predicate` and `Plan::from_map` do not receive a `SchemaDescriptor`
— only `pk_index: u32`. Compute `no_nulls` locally at the start of
`execute_map` / `filter_batch` where the schema is in scope, and pass it into
`eval_batch` / `EvalScratch::clear_null`:

```rust
// Inside execute_map, before the morsel loop:
let pki = self.pk_index as usize;  // stored in Plan by from_map/from_predicate
let no_nulls = prog.is_strictly_non_nullable(in_schema);
// no_nulls = true only when ALL hold:
// - No LOAD_PAYLOAD_INT/FLOAT references a nullable column.
// - No IS_NULL / IS_NOT_NULL instruction present.
// - No EXPR_INT_DIV / INT_MOD / FLOAT_DIV (dynamic null on zero divisor).
```

`Plan` must store `pk_index: u32` (set by `from_map` and `from_predicate`). `IS_NULL`,
`IS_NOT_NULL`, and `STR_COL_*` opcodes retain logical column indices after
`resolve_column_indices`, so `eval_batch` needs `pki` at runtime. Passing it as
`self.pk_index as usize` from the method body is the cleanest path; adding it to
`eval_batch`'s signature makes the dependency explicit.

This keeps `Plan` immutable at eval time and avoids a separate `init_null_elision` call.
If `no_nulls` is expensive to compute and the schema never changes between
calls, cache it in the `EvalScratch` after the first evaluation.

When `no_nulls` is true: `null_bits` is empty (zero allocation), every
`null_or2` / `load_null_bits` call is skipped, and the null-merge step at EMIT
is skipped. This eliminates all null tracking for the common case of non-nullable
schemas. The `no_nulls` check is a single branch per opcode, hoisted out of the
inner row loop.


## Filter integration

`Plan::filter_batch` evaluates the predicate program via `eval_batch` and
returns the `filter_bits` bitmask from scratch, borrowed for the duration of the
call. `op_filter` replaces its per-row predicate loop with a bitmask scan:

```rust
let bits = plan.filter_batch(mb, n, scratch);
let mut range_start: isize = -1;
let mut i = 0usize;
for &word in bits {
    match word {
        0 => {
            // All 64 fail. Flush any open range and move on.
            if range_start >= 0 {
                append_range(&mut output, range_start as usize, i);
                range_start = -1;
            }
        }
        u64::MAX => {
            // All 64 pass. Start or extend the current range.
            if range_start < 0 { range_start = i as isize; }
        }
        _ => {
            // Mixed word: iterate per bit, maintaining range_start state.
            // The predicate cost is already paid; this loop is cheap.
            for bit in 0..64usize {
                let row = i + bit;
                if row >= n { break; }
                if (word >> bit) & 1 != 0 {
                    if range_start < 0 { range_start = row as isize; }
                } else if range_start >= 0 {
                    append_range(&mut output, range_start as usize, row);
                    range_start = -1;
                }
            }
        }
    }
    i += 64;
}
if range_start >= 0 {
    append_range(&mut output, range_start as usize, n);
}
```

The `word == 0` and `word == u64::MAX` fast paths — which dominate when
predicates are selective or accept most rows — execute in a handful of
instructions for 64 rows. Fragmented pass/fail patterns degrade to per-bit
iteration but still avoid re-evaluating the predicate. The `range_start` state
variable tracks cross-word contiguous ranges exactly as the original per-row
loop does.

The blob pre-clone optimization in `op_filter` is preserved unchanged — it is
orthogonal to predicate evaluation.


## Map integration

`Plan::execute_map` calls `eval_batch` instead of the per-row `eval_with_emit`
loop. After `eval_batch` completes, each EMIT register buffer holds the computed
output for all N rows. EMIT becomes a morsel-aligned bulk copy:

```rust
// After eval_batch finishes:
for (emit_idx, &out_payload) in emit_payloads.iter().enumerate() {
    let reg = emit_regs[emit_idx];  // source register saved at from_map time
    let dst = output.col_data_mut(out_payload);
    for morsel_start in (0..n).step_by(MORSEL) {
        let m = (morsel_start + MORSEL).min(n) - morsel_start;
        let src = scratch.reg_slice(reg, m);
        let reg_nulls = scratch.null_words(reg, (m + 63) / 64);
        // For stride-8 output: copy each row, writing 0 for null rows.
        // The per-row interpreter writes 0 for nulls (eval_with_emit line:
        //   `write_unaligned(ptr, 0)` when null). We must match that exactly
        // rather than relying on the output buffer's initial zero-fill, because
        // the register may hold a non-zero intermediate (e.g. from div-by-zero
        // replace-and-mask) that would otherwise be copied into the output.
        let dst8 = &mut dst[morsel_start*8..(morsel_start+m)*8];
        for i in 0..m {
            let val = if (reg_nulls[i/64] >> (i%64)) & 1 != 0 { 0i64 } else { src[i] };
            dst8[i*8..(i+1)*8].copy_from_slice(&val.to_le_bytes());
        }
    }
}
```

This replaces the `EmitTarget` raw pointer pattern entirely. No unsafe emit
pointers, no per-row side effects during evaluation, no aliasing concerns. Each
output column gets one streaming write per morsel — pure sequential memory
access.

**Null merge for EMIT**: For each EMIT register `reg` with nullable output:
```rust
let null_words = (n + 63) / 64;
let reg_nulls = scratch.null_word_slice(reg, null_words);
let out_null_bmp = output.null_bmp_data_mut();
// null_bmp_data_mut() returns &mut [u8]; use .as_mut_ptr() to get *mut u8,
// then cast to *mut u64.  `as *mut u64` directly from a slice reference is
// not valid Rust — it does not compile.
let word_ptr = out_null_bmp.as_mut_ptr() as *mut u64;
for row in 0..n {
    let w = reg_nulls[row / 64];
    if (w >> (row % 64)) & 1 != 0 {
        unsafe { *word_ptr.add(row) |= 1u64 << out_payload; }
    }
}
```

The row loop is the correct approach and must not be replaced with a bulk
shift-and-OR. The two layouts are incompatible: `reg_nulls` is register-major
(one word = 64 rows, one bit per row) while the batch null bitmap is row-major
(one word = one row, one bit per payload column — as written by
`extend_null_bmp` and read by `get_null_word`). A scatter/transpose would be
needed to convert between them, which costs more than the loop.
When `no_nulls`, this entire block is skipped.

`execute_map` after the change:

```
system columns  →  batch memcpy           (unchanged)
col_moves       →  copy_column per col    (unchanged)
null_perm       →  apply_column           (unchanged)
compute         →  eval_batch             (NEW: columnar)
emit            →  bulk slice copy        (NEW: replaces EmitTarget)
null merge      →  word-level OR          (NEW: replaces per-row unsafe write)
```

Every step is now a columnar pass over contiguous slices.


## Program stripping

`Plan::from_map` already separates `COPY_COL` and `EMIT_NULL` instructions into
`col_moves` and `null_perm`. In the batch interpreter's outer loop, handle them
with a single `continue` rather than stripping the bytecode vector:

```rust
for instr in prog.code.chunks_exact(4) {
    match instr[0] {
        EXPR_COPY_COL | EXPR_EMIT_NULL => continue,  // handled by col_moves / null_perm
        ...
    }
}
```

This avoids maintaining two semantically different `ExprProgram` variants
(stripped vs. unstripped). The dispatch cost of two cheap `continue` arms is
negligible — the inner row loops dominate. Do not physically remove opcodes
from the stored code vector.

`EXPR_EMIT` opcodes are not stripped from the bytecode but their source
registers must be recorded during `from_map` for use during the bulk copy phase.
Collect them into `emit_regs: Vec<usize>` alongside the existing `emit_payloads`:

```rust
ComputeKernel::Interpreted { prog, emit_payloads, emit_regs }
```

`from_map` already iterates the instructions and pushes to `emit_payloads` when
`op == EXPR_EMIT`. Add: `emit_regs.push(a1 as usize)` at the same point. The
map integration section uses `emit_regs[emit_idx]` — `ExprProgram` never gains
an `emit_reg()` method; the data lives in the kernel struct. In the batch
interpreter's match, `EXPR_EMIT` should also be a `continue` (the actual emit is
handled by the bulk copy loop after `eval_batch` returns).

The `finalize_prog` passed to `reduce.rs` is a separate `ExprProgram` object —
do not apply any of the above to it. `emit_finalized_row` pre-scans raw bytecode
for `COPY_COL`, `EMIT`, and `EMIT_NULL` to classify output columns.


## Small-batch fallback

For N < 16, the batch interpreter's setup cost (scratch init, morsel bookkeeping)
exceeds the per-row interpreter's benefit. Add a threshold in `filter_batch` and
`execute_map`:

```rust
if n < BATCH_THRESHOLD {
    // Existing per-row path: eval_predicate / eval_with_emit
} else {
    // Batch path
}
```

`BATCH_THRESHOLD = 16` is a conservative starting point. Profile to tune.
The per-row paths are preserved unchanged and remain correct for all inputs.
The threshold can be raised as benchmarks confirm the batch path is faster.


## EvalScratch API

```rust
impl EvalScratch {
    fn ensure_capacity(&mut self, num_regs: usize, no_nulls: bool);
    fn reg_slice(&self, reg: usize, m: usize) -> &[i64];
    fn reg_mut(&mut self, reg: usize, m: usize) -> &mut [i64];
    fn reg3(&mut self, a: usize, b: usize, d: usize, m: usize)
        -> (&[i64], &[i64], &mut [i64]);   // split borrows
    fn null_words(&self, reg: usize, words: usize) -> &[u64];
    fn null_words_mut(&mut self, reg: usize, words: usize) -> &mut [u64];
    fn null_words3(&mut self, a: usize, b: usize, d: usize, words: usize)
        -> (&[u64], &[u64], &mut [u64]);
    fn clear_null(&mut self, m: usize);     // no-op if no_nulls
    fn pack_val_bits(&self, reg: usize, m: usize) -> u64;  // for BOOL ops
}
```

`reg3` returns three slices into the single backing `regs` Vec. `dst` is guaranteed
distinct from `a1` and `a2` because `ExprBuilder` is strictly SSA: every method
calls `alloc_reg()` to obtain a fresh, monotonically increasing register number
and returns it. No codepath through the SQL compiler emits an instruction where
`dst` equals an input register. The construction-time assert validates this
invariant and surfaces any future compiler regression immediately.

`a1` and `a2` may alias each other (e.g., `A + A`). This makes `split_at_mut`
awkward: producing three non-overlapping sub-slices requires `min`/`max` bounds
checks for the `a1 == a2` case. Use a small `unsafe` block instead — the
standard practice for Rust VM register files:

```rust
fn reg3(&mut self, a: usize, b: usize, d: usize, m: usize)
    -> (&[i64], &[i64], &mut [i64])
{
    unsafe {
        let p = self.regs.as_ptr();
        let q = self.regs.as_mut_ptr();
        (
            std::slice::from_raw_parts(p.add(a * MORSEL), m),
            std::slice::from_raw_parts(p.add(b * MORSEL), m),
            std::slice::from_raw_parts_mut(q.add(d * MORSEL), m),
        )
    }
}
```

This is safe because the construction-time assert (see below) guarantees `d != a`
and `d != b`, so the mutable slice never aliases either read slice. `a` and `b`
may alias; that is fine for reads.

Enforce this invariant at `ExprProgram::new` construction time, not only at
`reg3` call sites:
```rust
for instr in code.chunks_exact(4) {
    let (op, dst, a1, a2) = (instr[0], instr[1], instr[2], instr[3]);
    if is_binary_alu_op(op) {
        assert!(dst != a1 && dst != a2, "register aliasing: dst == src");
    }
}
```
A panic at construction time surfaces compiler bugs immediately and removes the
need for per-call checks inside the hot loop.


## Implementation order

1. **Remove `FilterKernel::CompareConst`**, `from_compare`, and the `OP_*`
   constants. Update tests. One commit.

2. **Add `EvalScratch`** with the API above. No behavior change yet — just the
   data structure and its methods. Unit-test `ensure_capacity`, `reg3`,
   `null_words3`, `pack_val_bits`. One commit.

3. **Implement `eval_batch`** for the non-string, non-division, non-boolean
   opcode subset: `LOAD_PAYLOAD_INT`, `LOAD_PK_INT`, `LOAD_CONST`, all integer
   and float arithmetic (except DIV/MOD), all comparison opcodes, `INT_TO_FLOAT`,
   `EMIT`. Run existing `scalar_func` tests against both paths (per-row and
   batch) for a correctness oracle. One commit.

4. **Add the remaining opcodes**: `INT_DIV`/`INT_MOD`/`FLOAT_DIV` with
   replace-and-mask, `BOOL_AND`/`BOOL_OR`/`BOOL_NOT`, `IS_NULL`/`IS_NOT_NULL`,
   `LOAD_PAYLOAD_FLOAT` with f32-widening. One commit per opcode family.

5. **Add string opcodes** (`STR_COL_EQ_CONST` etc.) with two-pass prefix
   batching. One commit.

6. **Wire filter path**: `Plan::filter_batch`, `op_filter` bitmask scan. Keep
   old per-row path behind threshold. Regression-test with full E2E suite.

7. **Wire map path**: Replace per-row `eval_with_emit` loop in `execute_map`
   with `eval_batch` + bulk EMIT copy. Remove `EmitTarget`. Run E2E. One commit.

8. **Skip `COPY_COL`/`EMIT_NULL` in batch interpreter**: Add `continue` arms
   in the outer opcode match. No bytecode vector mutation needed.

9. **Null elision**: Compute `no_nulls` inline at the top of `execute_map` /
   `filter_batch` (both have the schema in scope). Pass it into `eval_batch`.
   Unit-test both nullable and non-nullable schemas through the batch path.


## Correctness invariants

- Every opcode in `eval_batch` must produce identical results to the corresponding
  case in `eval_predicate`/`eval_with_emit` for all inputs, including null inputs,
  zero divisors, and float edge cases.
- The per-row paths are kept and used as correctness oracles during development.
  A table-driven test parameterized over `use_batch: bool` is the primary test
  vehicle.
- `no_nulls` must only be set when it is safe: if any LOAD instruction touches a
  nullable column, or `IS_NULL`/`IS_NOT_NULL` appears, or any of
  `EXPR_INT_DIV`/`EXPR_INT_MOD`/`EXPR_FLOAT_DIV` appear, `no_nulls` must be
  false (division by zero generates a dynamic null regardless of schema).
- The batch threshold must not affect results — only timing.
- Register aliasing (`dst == a1` or `dst == a2` in a binary ALU instruction) is
  checked at `ExprProgram::new` construction time with a debug assert, not
  deferred to `reg3` call sites.
