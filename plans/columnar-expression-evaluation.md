# Columnar Expression Evaluation — IMPLEMENTED

Status: shipped in `crates/gnitz-engine/src/expr/{batch.rs,plan.rs,program.rs}`.
Both the per-row interpreter (`eval_predicate` / `eval_with_emit`) and the
morsel-oriented evaluator (`eval_batch`) coexist; the per-row path is the
small-batch fallback and is still used by `reduce.rs` for per-row finalize.

## What this evaluator does

`Plan::execute_map` and `Plan::filter_batch` are layered pipelines of columnar
passes:

1. System columns (PK, weight)         — bulk slice copy
2. `ColMove` / `copy_column`           — per-column memcpy
3. `NullPerm::apply_column`            — bulk bitwise permutation
4. `ComputeKernel::Interpreted`        — `eval_batch` over morsels
5. EMIT                                — per-morsel streaming write per output column

Every step operates on contiguous slices; no per-row dispatch in step 4 either.

## EvalScratch — the SoA register file

```rust
pub(in crate::expr) struct EvalScratch {
    pub regs: Vec<i64>,            // regs[reg * MORSEL + row]
    pub null_bits: Vec<u64>,       // null_bits[reg * NULL_WORDS_PER_REG + word]; empty when no_nulls
    pub filter_bits: Vec<u64>,     // one bit per row, packed 64 per u64
    pub no_nulls: bool,            // cached per call by ensure_capacity
}
```

`MORSEL = 256`, `NULL_WORDS_PER_REG = MORSEL / 64 = 4`. With 8 registers the
register file is 16 KB (`regs`) + 256 B (`null_bits`) — fits in L1. Each opcode
operates on a contiguous register slice of length M (the current morsel size,
≤ MORSEL); LLVM sees a fixed-stride loop with no aliasing and auto-vectorizes.

The scratch lives behind `RefCell<EvalScratch>` on `Plan` (single-threaded
DBSP). `ensure_capacity(num_regs, no_nulls, n)` grows the vectors monotonically
and caches `no_nulls`. After the first call on a given Plan, capacity is
saturated and the call is a couple of length comparisons.

`reg3` (and the analogous `null_words3`) return two shared and one mutable
slice using `unsafe` raw-pointer slicing. SSA from `ExprBuilder` guarantees
`dst != a1 && dst != a2` for every binary ALU op; this is enforced at
construction time in `ExprProgram::new` (hard assert, not `debug_assert`) so
any compiler regression surfaces immediately. `a1` and `a2` may alias each
other (e.g. `A + A`); that's fine for two shared reads.

## Outer loop and opcode dispatch

```rust
pub(in crate::expr) fn eval_batch(
    prog: &ExprProgram,
    mb: &MemBatch,
    morsel_start: usize,
    m: usize,
    scratch: &mut EvalScratch,
) {
    for instr in prog.code.chunks_exact(4) {
        match instr[0] {
            EXPR_COPY_COL | EXPR_EMIT_NULL | EXPR_EMIT => {}  // handled outside
            EXPR_LOAD_PAYLOAD_INT => { /* base-hoisted width-dispatched fill */ }
            EXPR_INT_ADD => {
                let (ra, rb, rd) = scratch.reg3(a1, a2, dst, m);
                for i in 0..m { rd[i] = ra[i].wrapping_add(rb[i]); }
                null_or2(scratch, dst, a1, a2, m);
            }
            ...
        }
    }
}
```

The opcode match runs once per opcode per morsel. The inner loops are tight
slice operations LLVM turns into AVX/SVE. The outer driver (`filter_batch` /
`execute_map`) loops `morsel_start in (0..n).step_by(MORSEL)` and calls
`eval_batch` per morsel.

`COPY_COL`, `EMIT_NULL`, and `EMIT` are matched in the dispatch and ignored —
they are realized as columnar passes in the wrapper (`col_moves`, `null_perm`)
or after `eval_batch` returns (EMIT bulk copy). The bytecode vector is left
unmodified; two cheap match arms are cheaper than maintaining stripped vs.
unstripped program variants.

## LOAD opcodes: base hoisting and width dispatch

`EXPR_LOAD_PAYLOAD_INT` slices `mb.col_data(pi, col_size)` once, then dispatches
on `col_size ∈ {1,2,4,8}` (with `is_signed` selecting between `iN::from_le_bytes`
and `uN::from_le_bytes`). Inside each arm the inner loop is
`for (i, c) in chunk.chunks_exact(K).enumerate() { dst_reg[i] = ... }` —
constant stride, no aliasing, LLVM uses `VPMOVSXDQ` / `VPMOVZXDQ` and friends.

When `payload_col_sizes` is empty (legacy callers that never invoked
`set_payload_col_info`) the loader falls back to 8-byte `i64::from_le_bytes`.

`EXPR_LOAD_PAYLOAD_FLOAT` is the same shape with `col_size ∈ {4, 8}`; the
4-byte path widens `f32 → f64` and stores `f64::to_bits(...) as i64`.

Null bits are loaded via `fill_null_bits_pi` — a word-by-word transpose from
the batch's row-major null bitmap to the register-major scratch. The transpose
is the only place where the load is not a uniform contiguous read; see "Known
hotspots" below.

`EXPR_LOAD_PK_INT` and `EXPR_LOAD_CONST` fill the destination register with one
value (PK widened per row, or the reassembled i64 constant). Both clear the
destination null bits via `clear_null_reg`, which is a no-op under `no_nulls`.

`LOAD_CONST` reassembles the constant as
`let val: i64 = (a2 << 32) | (a1 & 0xFFFF_FFFF);`. **Do not** apply
`as usize` to `a1` / `a2` here — for negative 32-bit halves (e.g. `-1`), the
high bits would be discarded and the constant corrupted. Register-index opcodes
do `let ai = a1 as usize;` locally inside their arm.

## Arithmetic, comparison, type cast

All binary opcodes follow the same shape:

```rust
EXPR_INT_ADD => {
    let ai = a1 as usize; let bi = a2 as usize;
    {
        let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
        for i in 0..m { rd[i] = ra[i].wrapping_add(rb[i]); }
    }
    null_or2(scratch, dst, ai, bi, m);
}
```

`null_or2` is a word-level `na | nb → nd` over `ceil(m/64)` words (`null_or1`
is the unary analog). `null_or*` early-returns under `no_nulls`. One bitwise OR
per 64 rows replaces the per-row branch in `eval_predicate`.

Comparisons produce 0/1 in the destination register; result packing into
`filter_bits` happens once at morsel end inside `filter_batch`.

## Division: replace-and-mask

`EXPR_INT_DIV` / `EXPR_INT_MOD` / `EXPR_FLOAT_DIV` build a per-row
zero-divisor mask, replace the divisor with 1 (so the inner loop has no branch
that prevents vectorization), and OR the mask into the destination null bits
after `null_or2`:

```rust
EXPR_INT_DIV => {
    let mut zero_mask = [0u64; NULL_WORDS_PER_REG];
    {
        let (ra, rb, rd) = scratch.reg3(ai, bi, dst, m);
        for i in 0..m {
            if rb[i] == 0 { zero_mask[i / 64] |= 1u64 << (i % 64); }
            let divisor = if rb[i] == 0 { 1 } else { rb[i] };
            rd[i] = ra[i].wrapping_div(divisor);
        }
    }
    null_or2(scratch, dst, ai, bi, m);
    if !scratch.no_nulls {
        let base = dst * NULL_WORDS_PER_REG;
        for w in 0..m.div_ceil(64) { scratch.null_bits[base + w] |= zero_mask[w]; }
    }
}
```

`reg3` returns `rb` as `&[i64]`; the divisor swap is done in a local variable,
never by mutating `rb` (which is shared with `ra` if the program ever does
`A / B` and `A % B` over the same operand). `wrapping_div` returns `i64::MIN`
for `i64::MIN / -1`, matching the per-row interpreter exactly.

`is_strictly_non_nullable` returns `false` whenever any DIV/MOD opcode is
present, so `no_nulls` cannot be true here in practice — the `if !no_nulls`
guard is defensive.

## SQL 3VL boolean operators

`EXPR_BOOL_AND` / `EXPR_BOOL_OR` have two implementations:

- **`no_nulls == true`**: a straight `(a != 0) && (b != 0)` per-row loop in
  the value registers. No null bookkeeping at all.
- **`no_nulls == false`**: a word-at-a-time loop that packs each register's
  64 nonzero-bits into a `u64`, applies 3VL logic with bitwise ops
  (`definite_false = (!na & !va) | (!nb & !vb)`, etc.), writes one null word,
  then unpacks the result-bit word back into the value register so EMIT and
  downstream opcodes see correct 0/1 i64 values.

`EXPR_BOOL_NOT` is a unary `(reg == 0) as i64` loop with `null_or1` propagation.

## IS_NULL / IS_NOT_NULL

`a1` is a *resolved* payload byte (set by `resolve_column_indices`). PK is
encoded as `PAYLOAD_MAPPING_PK_SENTINEL`; the implementation branches on that
sentinel inline — no `pki` parameter needs to be threaded through `eval_batch`.

```rust
EXPR_IS_NULL => {
    let pi_byte = a1 as u8;
    scratch.clear_null_reg(dst, m);
    let base_d = dst * MORSEL;
    if pi_byte == PAYLOAD_MAPPING_PK_SENTINEL {
        for i in 0..m { scratch.regs[base_d + i] = 0; }
    } else {
        let pi = pi_byte as usize;
        let null_bmp = mb.null_bmp();
        for i in 0..m {
            let row_null = read_u64_le(null_bmp, (morsel_start + i) * 8);
            scratch.regs[base_d + i] = ((row_null >> pi) & 1) as i64;
        }
    }
}
```

The result is never itself null, so the dst null bits are cleared first.

## String comparisons

Both `STR_COL_*_CONST` and `STR_COL_*_COL` route through shared helpers
(`eval_str_col_vs_const`, `eval_str_col_vs_col`). For each row in the morsel:

1. `fill_null_bits_for_resolved` (SENTINEL → cleared, payload byte → loaded).
2. A single-pass loop over the morsel calling `compare_col_string_vs_const` /
   `compare_german_strings`. The 4-byte big-endian prefix early-out lives
   inside the compare function, so for selective predicates the dominant cost
   is the prefix compare and the dereference of the rest of the German String
   struct only happens for ambiguous rows.
3. A second pass zeroes the result for any row that the null bitmap marked
   null. This isolates the value-write loop from the null branch, which is
   what allows LLVM to vectorize the comparison loop.

No explicit two-pass ambiguous-row scatter (`SmallVec<[u16; 8]>`) is needed —
the prefix short-circuit inside `compare_col_string_vs_const` already
eliminates the blob dereference for the common case. If a future workload
shows that scatter still helps (large M with mostly-non-ambiguous rows), the
helper is small enough to be replaced.

`STR_COL_*` opcodes operate on logical-column operands that
`resolve_column_indices` rewrites to resolved payload bytes (both operands for
the `_COL` variants). PK is never a string — the helpers `debug_assert!` this.

## Filter integration

`Plan::filter_batch` runs `eval_batch` per morsel, packing the result register
into `scratch.filter_bits`:

```rust
for morsel_start in (0..n).step_by(MORSEL) {
    let m = MORSEL.min(n - morsel_start);
    eval_batch(prog, mb, morsel_start, m, &mut scratch);

    let base_r = result_reg * MORSEL;
    let base_null = result_reg * NULL_WORDS_PER_REG;
    for i in 0..m {
        let abs = morsel_start + i;
        let is_null = !no_nulls &&
            (scratch.null_bits[base_null + i / 64] >> (i % 64)) & 1 != 0;
        if !is_null && scratch.regs[base_r + i] != 0 {
            scratch.filter_bits[abs / 64] |= 1u64 << (abs % 64);
        }
    }
}
```

`op_filter` consumes `filter_bits` with a word-level scan that maintains a
`range_start` across words:

- `word == 0`: flush any open range.
- `word == u64::MAX` (or `word == (1<<chunk)-1` when this is the partial last
  word): start/extend the run without per-bit iteration.
- Mixed word: iterate the 64 bits, opening/closing ranges as the bitmap
  alternates. Predicate cost is already paid; this is a few branches per row
  on the cold path only.

The partial-last-word fast path is an addition over the original sketch — it
turns a fragmented-tail batch into the same all-pass run as a full word.

Below the threshold (`THRESHOLD = 16` rows), `filter_batch_bits` returns
`None` and `op_filter` falls back to the per-row `evaluate_predicate` loop.

The blob pre-clone optimization in `op_filter` is orthogonal and untouched.

## Map integration

After `eval_batch` writes EMIT registers, `execute_map` performs one bulk copy
per morsel per EMIT, using the per-emit stride stored in
`ComputeKernel::Interpreted::emit_strides` (one byte width per EMIT, parallel
to `emit_payloads` and `emit_regs`):

```rust
for morsel_start in (0..n).step_by(MORSEL) {
    let m = MORSEL.min(n - morsel_start);
    eval_batch(prog, &in_mb, morsel_start, m, &mut scratch);

    for ((&out_payload, &reg), &stride_u8) in
        emit_payloads.iter().zip(emit_regs.iter()).zip(emit_strides.iter())
    {
        let stride = stride_u8 as usize;
        let dst8 = output.col_data_mut(out_payload);
        let base_r = reg * MORSEL;
        let base_null_r = reg * NULL_WORDS_PER_REG;
        for i in 0..m {
            let row = morsel_start + i;
            let is_null = !no_nulls &&
                (scratch.null_bits[base_null_r + i / 64] >> (i % 64)) & 1 != 0;
            let val = if is_null { 0i64 } else { scratch.regs[base_r + i] };
            dst8[row * stride..(row + 1) * stride]
                .copy_from_slice(&val.to_le_bytes()[..stride]);
            if is_null {
                unsafe { *null_arr.add(row) |= 1u64 << out_payload; }
            }
        }
    }
}
```

The per-row 0-write for null rows is required: the register may hold a
non-zero intermediate (e.g. from div-by-zero replace-and-mask) that must be
masked to zero on the way out, matching `eval_with_emit`'s
`write_unaligned(ptr, 0)` for null rows.

The output null bitmap is row-major (one u64 per row, one bit per payload
column), so the merge is `*null_arr.add(row) |= 1u64 << out_payload` — a
gather-by-column from register-major scratch into row-major output. A
transpose to do this word-at-a-time would cost more than the loop.

## Plan structure

```rust
pub(crate) struct Plan {
    filter: FilterKernel,                // PassAll | Interpreted { prog, no_nulls }
    col_moves: Vec<ColMove>,
    null_perm: NullPerm,
    compute: ComputeKernel,              // None | Interpreted { prog, emit_payloads, emit_regs, emit_strides, no_nulls }
    scratch: RefCell<EvalScratch>,
}
```

`from_predicate` and `from_map` call `resolve_column_indices` once and then
`is_strictly_non_nullable(schema)` once, caching the boolean in the kernel.
`Plan` does **not** store `pk_index`: after `resolve_column_indices`, every
column-bearing opcode (`LOAD_*`, `IS_NULL`, `IS_NOT_NULL`, `COPY_COL`,
`STR_COL_*`) carries a resolved payload byte or SENTINEL in its operand, and
the SENTINEL branch is handled inline at each call site.

`from_map` splits the bytecode into three classes during one pass:

- `EXPR_COPY_COL` → `col_moves` (resolved src byte + dst payload + type code).
- `EXPR_EMIT_NULL` → `null_perm.constant` bit.
- everything else → `has_compute = true`; if also `EXPR_EMIT`, push
  `(src_reg, out_payload)` into `(emit_regs, emit_payloads)`.

`emit_strides[i] = out_schema.columns[out_schema.payload_col_idx(emit_payloads[i])].size()`.

## Correctness invariants

- `eval_batch` debug-asserts `prog.resolved` so a bypass caller cannot reach
  the inner loops with unresolved operands.
- `ExprProgram::new` hard-asserts SSA non-aliasing on binary ALU opcodes.
- Per-row paths (`eval_predicate`, `eval_with_emit`) are preserved and used as
  the small-batch fallback **and** by `reduce.rs` per-row finalize. They serve
  as the correctness oracle for `eval_batch` (see `batch_tests.rs`).
- `no_nulls` may be true only if **no** opcode can produce a null. The
  `is_strictly_non_nullable` predicate returns false for DIV/MOD, IS_NULL /
  IS_NOT_NULL, and nullable-column `LOAD_PAYLOAD_INT` / `LOAD_PAYLOAD_FLOAT`.
- `THRESHOLD = 16` selects the batch path and never affects results, only
  timing.

## Known issues / follow-ups

1. **Outer driver allocates a fresh `Vec<u64>` for filter bits per call.**
   `Plan::filter_batch` returns `scratch.filter_bits[..words].to_vec()` so that
   the `RefCell::borrow_mut` can be dropped before the caller scans the bits.
   The allocation is small (`ceil(n/64) * 8` bytes), but it's per call. Two
   ways out: (a) the caller borrows the `Ref`/`RefMut` for the scan duration,
   (b) move the scan inside `Plan::filter_batch` so the bitmap never escapes.
   Option (b) is the cleaner refactor — `op_filter` would pass an
   `append_range` closure and never see the bitmap.

2. **`fill_null_bits_pi` is a row-major → register-major transpose.** Each
   `LOAD_PAYLOAD_INT/FLOAT` reads up to 256 row-major null words and rotates
   each into one register-bit. For LOAD-heavy programs on nullable columns
   this is the dominant load-time cost. An alternative: load the whole morsel
   of null words into a small `[u64; MORSEL]` once at morsel start, then have
   each LOAD index it. Saves one read per row per LOAD when multiple loads
   target the same morsel, at the cost of 2 KB of additional scratch.

3. **EMIT inner loop with `[u8]::copy_from_slice(&val.to_le_bytes()[..stride])`
   uses a runtime stride.** For the common case `stride == 8`, an `i64`
   unaligned store is one instruction; for narrow strides (`stride == 4` for
   downcast TYPECAST → INT, etc.) the same pattern with `as i32`/`as i16`
   produces a single store. A `match stride { 8 => …, 4 => …, … }` at
   per-EMIT scope (not per-row) would let LLVM see the constant width and
   collapse the inner loop to a stride-8 / stride-4 streaming store.

4. **`BOOL_AND` / `BOOL_OR` with nulls pack values into a `u64` via a
   byte-by-byte extract loop.** This is the rare branch (selectivity
   notwithstanding, most workloads do not lean on 3VL boolean composition).
   If profile shows BOOL ops in a hot expression, the packing loop is a
   candidate for `VPCMPEQQ + VPMOVMSKB`.

5. **`eval_batch`'s match is ~600 lines.** Rust + LLVM should turn this into a
   jump table given the constants are dense, but the table will spill data
   cache lines from the inner-loop scratch reads. If the dispatch overhead
   becomes measurable, factor the arithmetic-and-comparison cluster into
   generic helpers parameterized over the per-element operation (one for
   integer binary, one for float binary, one for comparison), so the bulky
   match body shrinks.

## Why this design pays off

- **Base pointer hoisted once per opcode**, not recomputed per row.
- **Column-width branch made once per opcode**, not per row.
- **No `EmitTarget` raw-pointer write-into-output during evaluation** — EMIT
  is a deferred bulk copy after `eval_batch` returns. (The legacy
  `EmitTarget` / `eval_with_emit` path is retained only for per-row callers
  in `reduce.rs::emit_finalized_row`.)
- **Null propagation is word-level**, one bitwise OR per 64 rows.
- **Auto-vectorization** of every arithmetic/comparison inner loop — the
  SoA layout, constant stride, and `reg3`-enforced non-aliasing are
  exactly what LLVM needs.
- **Selective predicates flow through the filter scan in single-instruction
  fast paths** (`word == 0` or `word == u64::MAX`) instead of evaluating
  the predicate per row.
