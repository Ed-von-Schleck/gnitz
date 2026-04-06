# Columnar Expression Evaluation

## Context

This plan assumes Phase 1+2 has been completed: `ScalarFuncKind` has been
replaced by `Plan`, `NullPerm` handles the null bitmap columnarly for
COPY_COL and EMIT_NULL columns, `copy_column` is a shared function, and
`UniversalProjection`, `MapAnalysis`, and `ScalarFuncKind::Null` no longer
exist.

After Phase 1+2, the `Plan` type looks roughly like:

```rust
pub struct Plan {
    filter: FilterKernel,
    col_moves: Vec<ColMove>,
    null_perm: NullPerm,
    compute: ComputeKernel,
}

enum FilterKernel {
    CompareConst { col_idx: u32, pk_index: u32, op: CmpOp, val: i64, is_float: bool },
    Interpreted(ExprProgram),
}

enum ComputeKernel {
    None,
    Interpreted { prog: ExprProgram, emit_payloads: Vec<usize> },
}
```

The interpreter (`expr::eval_predicate`, `expr::eval_with_emit`) is the
fallback for both filter and map compute. It runs per-row: allocates a
register file on the stack, iterates all opcodes, reads column values by
row index. This is correct and preserved from the original code.

Phase 3 adds **typed kernels** — columnar alternatives to the interpreter
for recognized expression patterns. The interpreter remains as the fallback
for patterns that cannot be classified.


## Problem statement

The expression interpreter in `expr.rs` evaluates every opcode for every
row independently. For a filter predicate with K opcodes over N rows, the
interpreter dispatches K×N opcode matches. For a map with C computed output
columns, the interpreter runs the full program N times and writes to C
pre-allocated output columns via `EmitTarget` raw pointers.

For large batches (initial table load, large deltas), this is measurably
slower than operating on column buffers directly, because:

1. The column buffer reference (`mb.col_data[pi]`) is re-derived per row
   via `get_col_ptr(row, pi, 8)` instead of being hoisted once.
2. The opcode `match` dispatch runs N times per opcode instead of once.
3. LLVM cannot auto-vectorize the inner loop because it is buried inside
   the interpreter's control flow.

For typical DBSP incremental workloads, deltas are small (tens to hundreds
of rows), so the per-row overhead may not dominate. **Phase 3 should only
be pursued if profiling shows the interpreter is a bottleneck.**


## Prerequisite: profiling

Before implementing any of the designs below, run the following:

```bash
# Build release binary
make release-server

# Profile a workload with computed columns and complex filters
# Check if expr::eval_predicate or expr::eval_with_emit appear in the top
# functions. If they do not, this work is not worth pursuing.
```

Also measure with the E2E benchmark on a view that has a computed column
(e.g., `SELECT a + b, c * 2 FROM t WHERE a > 10`). If the filter and map
operators are <5% of total epoch time, the interpreter is not the
bottleneck and this work should be deferred.


## Design A: Typed filter kernels (lowest risk, smallest scope)

### Concept

Extend `FilterKernel` with typed variants for common predicates. Each
variant is a tight loop over a single column buffer — auto-vectorizable
by LLVM.

```rust
enum FilterKernel {
    CompareConst { col_idx: u32, pk_index: u32, op: CmpOp, val: i64, is_float: bool },
    // New:
    AndCompare(Box<FilterKernel>, Box<FilterKernel>),
    OrCompare(Box<FilterKernel>, Box<FilterKernel>),
    IsNull { col_idx: u32, pk_index: u32 },
    IsNotNull { col_idx: u32, pk_index: u32 },
    Interpreted(ExprProgram),
}
```

### How filter_batch changes

Currently `op_filter` calls `evaluate_predicate(batch, row)` per row in
an interleaved loop with range detection. With typed kernels, the filter
produces a boolean result per row first, then the range detection loop
scans that result:

```rust
fn filter_batch(&self, mb: &MemBatch, n: usize) -> FilterResult {
    match &self.filter {
        CompareConst { col_idx, pk_index, op, val, is_float } => {
            // Tight loop over column buffer: auto-vectorizable
            let mut result = vec![false; n];
            // ... one comparison per row, no interpreter dispatch
            FilterResult::Batch(result)
        }
        AndCompare(left, right) => {
            let l = left.filter_batch(mb, n);
            let r = right.filter_batch(mb, n);
            // Bitwise AND of results — trivially vectorizable
            FilterResult::Batch(and_results(l, r))
        }
        Interpreted(prog) => {
            // Fallback: per-row interpreter, same as today
            FilterResult::PerRow(prog)
        }
    }
}
```

### op_filter adaptation

```rust
match plan.filter_batch(&mb, n) {
    FilterResult::Batch(passes) => {
        // Scan boolean array for contiguous runs → append_batch
        for (i, &pass) in passes.iter().enumerate() { /* range detection */ }
    }
    FilterResult::PerRow(prog) => {
        // Existing interleaved loop — no allocation, no regression
        for i in 0..n {
            if eval_predicate(prog, &mb, i, pk_index) { /* range detection */ }
        }
    }
}
```

The `PerRow` path preserves the current behavior exactly — no `Vec<bool>`
allocation, no regression for complex expressions. The `Batch` path is
only used for recognized patterns.

### Allocation strategy for the Batch path

The `Vec<bool>` in `filter_batch` allocates per call. For small N (typical
DBSP deltas), this is negligible. For large N, two options:

1. **Scratch buffer inside Plan**: A `RefCell<Vec<bool>>` field, resized
   on demand. Zero allocation after the first call. Safe because GnitzDB
   is single-threaded per process.

2. **Packed bitmask**: `Vec<u64>` with one bit per row (N/64 words). For
   N=4096, that's 64 u64s = 512 bytes. More efficient but the range
   detection loop becomes a bit-scan. Standard approach:
   `for each word { while word != 0 { trailing_zeros → row index } }`.

### Classification

`FilterKernel::from_program(prog)` analyzes the bytecode to detect:

- 3-instruction pattern `LOAD_COL_INT, LOAD_CONST, CMP_*` → `CompareConst`
- Two classified sub-trees joined by `BOOL_AND` → `AndCompare`
- Two classified sub-trees joined by `BOOL_OR` → `OrCompare`
- `IS_NULL col` / `IS_NOT_NULL col` → `IsNull`/`IsNotNull`
- Anything else → `Interpreted`

This classifier runs once at Plan construction time. It reads the flat
bytecode array and pattern-matches instruction sequences. It does NOT
need to handle float predicates (those stay in `CompareConst` which
already has `is_float`) or string predicates (those fall back to
`Interpreted`).

### Risk assessment

- **Correctness**: Each kernel reproduces the exact same comparison as
  the interpreter for the same opcode. `CompareConst` with `is_float`
  reinterprets bits as f64 and compares — identical to `UniversalPredicate`
  which was already tested.
- **Null handling**: `CompareConst` must check the null bitmap for the
  source column. If null, the predicate fails (same as `ExprPredicate`
  where null propagation causes `!is_null && val != 0` to return false).
  For `AndCompare`/`OrCompare`, null rows fail both sides independently.
- **The `Interpreted` fallback is always available**: any expression
  that the classifier cannot handle falls through to the existing
  interpreter with zero behavior change.


## Design B: Typed map kernels (medium risk, medium scope)

### Concept

Extend `ComputeKernel` with typed variants for common arithmetic patterns
in computed columns. Each variant operates on column slices directly.

```rust
enum ComputeKernel {
    None,
    Typed(Vec<MapKernel>),
    Interpreted { prog: ExprProgram, emit_payloads: Vec<usize> },
    // Mixed: some columns typed, remainder interpreted
    Mixed {
        typed: Vec<MapKernel>,
        interpreted_prog: ExprProgram,
        interpreted_emit_payloads: Vec<usize>,
    },
}

enum MapKernel {
    AddColCol      { src_a_pi: usize, src_b_pi: usize, dst_pi: usize },
    AddColConst    { src_pi: usize, val: i64, dst_pi: usize },
    SubColCol      { src_a_pi: usize, src_b_pi: usize, dst_pi: usize },
    SubColConst    { src_pi: usize, val: i64, dst_pi: usize },
    MulColConst    { src_pi: usize, val: i64, dst_pi: usize },
    MulColCol      { src_a_pi: usize, src_b_pi: usize, dst_pi: usize },
    NegCol         { src_pi: usize, dst_pi: usize },
    IntToFloat     { src_pi: usize, dst_pi: usize },
    // Float variants
    FAddColCol     { src_a_pi: usize, src_b_pi: usize, dst_pi: usize },
    FAddColConst   { src_pi: usize, val: f64, dst_pi: usize },
    FMulColConst   { src_pi: usize, val: f64, dst_pi: usize },
    // ... extend as profiling shows need
}
```

### Execution

Each `MapKernel` operates on a pre-allocated output column buffer:

```rust
impl MapKernel {
    fn execute(&self, in_mb: &MemBatch, out: &mut OwnedBatch, n: usize) {
        match self {
            AddColCol { src_a_pi, src_b_pi, dst_pi } => {
                let col_a = in_mb.col_data[*src_a_pi];
                let col_b = in_mb.col_data[*src_b_pi];
                let dst = &mut out.col_data[*dst_pi];
                for i in 0..n {
                    let a = i64::from_le_bytes(col_a[i*8..i*8+8].try_into().unwrap());
                    let b = i64::from_le_bytes(col_b[i*8..i*8+8].try_into().unwrap());
                    dst[i*8..i*8+8].copy_from_slice(&a.wrapping_add(b).to_le_bytes());
                }
            }
            // ...
        }
    }
}
```

The inner loop reads two column slices and writes a third. No interpreter
dispatch, no register file, no null propagation in the hot path. LLVM
auto-vectorizes the wrapping_add loop.

### Null propagation for typed map kernels

For non-division arithmetic: output null = OR of input column null bits.
This is a static function of which source columns contribute. It can be
represented as additional entries in `NullPerm` at Plan construction time:

```rust
struct NullPerm {
    // Existing: bit moves for COPY_COL
    moves: Vec<(u8, u8)>,            // (src_bit, dst_bit)
    // New: OR-combos for typed compute kernels
    or_moves: Vec<(u8, u8, u8)>,     // (src_a_bit, src_b_bit, dst_bit)
    constant: u64,                    // EMIT_NULL bits
}
```

`NullPerm::apply` applies moves first, then or_moves:
```rust
fn apply(&self, in_null: u64) -> u64 {
    let mut out = self.constant;
    for &(src, dst) in &self.moves { out |= ((in_null >> src) & 1) << dst; }
    for &(a, b, dst) in &self.or_moves {
        out |= (((in_null >> a) | (in_null >> b)) & 1) << dst;
    }
    out
}
```

For `AddColConst` (one source column): use a regular `move` pair.
For `AddColCol` (two source columns): use an `or_move` triple.

This keeps the null bitmap computation fully columnar for typed kernels.
Only the `Interpreted` fallback retains a per-row null update.

### Division: the special case

`INT_DIV` and `INT_MOD` produce NULL when the divisor is zero. This is
data-dependent — it cannot be represented in NullPerm. Two options:

1. **Never classify division as a typed kernel** — fall back to
   `Interpreted`. Since division in SQL SELECT expressions is uncommon
   compared to addition/multiplication, this covers 90%+ of cases.

2. **Handle division in a special kernel** that scans the divisor column
   first, produces a null bitmask for zero-divisor rows, then runs the
   division only on non-zero rows. More complex, marginal benefit.

Recommend option 1 for simplicity.

### Classification

The classifier reads the EMIT opcodes in the bytecode and traces backward
to determine each EMIT column's expression tree:

- `EMIT r2` where `r2 = INT_ADD(LOAD_COL_INT col_a, LOAD_COL_INT col_b)`
  → `AddColCol { src_a_pi, src_b_pi, dst_pi }`
- `EMIT r2` where `r2 = INT_MUL(LOAD_COL_INT col, LOAD_CONST val)`
  → `MulColConst { src_pi, val, dst_pi }`

If ALL EMIT columns are classifiable: `ComputeKernel::Typed(kernels)`.
If NONE are: `ComputeKernel::Interpreted { prog, emit_payloads }`.
If some are: `ComputeKernel::Mixed { typed, interpreted_prog, interpreted_emit_payloads }`.

The Mixed case is the most complex. It requires splitting the ExprProgram
into two parts: the classified EMITs (handled by typed kernels) and the
remaining EMITs (handled by the interpreter). The interpreter must skip
the classified EMITs. This is doable by setting a flag per EMIT, but adds
bookkeeping. Whether Mixed is worth implementing depends on how common
partially-classifiable programs are in practice.

### Risk assessment

- **Correctness**: Each kernel must match the interpreter's behavior
  exactly for the same opcode. Wrapping arithmetic (wrapping_add etc.)
  matches the interpreter. Float bit-reinterpretation must match
  `float_to_bits`/`bits_to_float`.
- **The Mixed case is the riskiest part**: splitting a program into typed
  and interpreted EMITs requires careful bookkeeping. Consider deferring
  Mixed until there is evidence it is needed — if a program has any
  unclassifiable EMIT, fall back to fully Interpreted.
- **String operations are never typed**: `STR_COL_EQ_CONST` etc. involve
  variable-length blob data. These always fall back to Interpreted.


## Design C: Batch register file (highest scope, highest reward)

### Concept

Instead of running the interpreter per-row, invert the loop order:
run each opcode across all N rows at once. Each register becomes a
`Vec<i64>` of length N.

This is a full rewrite of the interpreter execution model. It subsumes
Designs A and B — if fully implemented, typed kernels become unnecessary
because every opcode is already operating on column vectors.

### Sketch

```rust
struct EvalScratch {
    regs: Vec<Vec<i64>>,    // [num_regs][capacity]
    nulls: Vec<Vec<u64>>,   // [num_regs][(capacity+63)/64] packed bitmask
    bool_out: Vec<bool>,    // for filter output
}

fn eval_batch(prog: &ExprProgram, mb: &MemBatch, n: usize, scratch: &mut EvalScratch) {
    scratch.ensure_capacity(n, prog.num_regs as usize);

    for i in 0..prog.num_instrs as usize {
        let op = prog.code[i * 4];
        let dst = prog.code[i * 4 + 1] as usize;
        let a1 = prog.code[i * 4 + 2] as usize;
        let a2 = prog.code[i * 4 + 3] as usize;

        match op {
            EXPR_LOAD_COL_INT => {
                // Bulk load: deserialize column buffer into register buffer
                let col = mb.col_data[payload_idx(a1, pk_index)];
                for row in 0..n {
                    scratch.regs[dst][row] =
                        i64::from_le_bytes(col[row*8..row*8+8].try_into().unwrap());
                }
            }
            EXPR_INT_ADD => {
                // Element-wise: LLVM auto-vectorizes this
                for row in 0..n {
                    scratch.regs[dst][row] =
                        scratch.regs[a1][row].wrapping_add(scratch.regs[a2][row]);
                }
            }
            EXPR_EMIT => {
                // Bulk store: write register buffer into output column
                let emit_col = &mut out.col_data[emit_payload];
                for row in 0..n {
                    emit_col[row*8..row*8+8].copy_from_slice(
                        &scratch.regs[a1][row].to_le_bytes()
                    );
                }
            }
            // ... all other opcodes follow the same pattern
        }
    }
}
```

### Morsel refinement

Instead of processing all N rows at once, process fixed-size morsels
of M rows (e.g., M=256). This caps intermediate buffer size at
M × num_regs × 8 bytes. For M=256, 8 registers: 16KB — fits in L1.

```rust
for morsel_start in (0..n).step_by(M) {
    let morsel_end = (morsel_start + M).min(n);
    let morsel_n = morsel_end - morsel_start;
    eval_morsel(prog, mb, morsel_start, morsel_n, scratch);
}
```

This bounds memory usage regardless of batch size while preserving the
vectorization benefit.

### Null tracking

Two viable representations:

1. **Packed bitmask**: `nulls[reg]` is a `Vec<u64>` with one bit per row.
   Null propagation for binary ops: `nulls[dst][w] = nulls[a1][w] | nulls[a2][w]`
   (bitwise OR of 64-row chunks). Compact and vectorizable.

2. **Per-row bool**: `nulls[reg]` is a `Vec<u8>` with one byte per row.
   Simpler indexing but 8x memory overhead.

Option 1 is preferred for alignment with the existing `null_bmp` layout
(one u64 per 8-byte null word = one bit per payload column per row).

**Optimization for non-nullable schemas**: At Plan construction time,
check whether any source column is nullable. If none are, skip null
tracking entirely — no null buffers allocated, no null propagation
opcodes executed. This is the common case in production.

### Short-circuit semantics

In the per-row interpreter, `BOOL_AND` of `(x != 0, y/x)` short-circuits
when x=0: the division is never evaluated. In the batch model, all opcodes
are evaluated for all rows.

This matters for `INT_DIV` and `INT_MOD` where the divisor can be zero.
The per-row interpreter sets the result to NULL on division by zero. In
the batch model, division by zero would cause a hardware fault (or produce
garbage via wrapping_div).

Solution: for `INT_DIV`/`INT_MOD` in batch mode, scan the divisor register
buffer for zeros first (one vectorizable pass), mark those rows as null,
then evaluate division only on non-zero rows. Or: unconditionally replace
zero divisors with 1 before dividing, then mask the result as null. The
latter avoids a branch:

```rust
EXPR_INT_DIV => {
    for row in 0..n {
        let d = scratch.regs[a2][row];
        if d == 0 {
            scratch.regs[dst][row] = 0;
            set_null_bit(&mut scratch.nulls[dst], row);
        } else {
            scratch.regs[dst][row] = scratch.regs[a1][row].wrapping_div(d);
            propagate_null2(&scratch.nulls, dst, a1, a2, row);
        }
    }
}
```

This is still a per-row branch but it only applies to division opcodes,
not to all opcodes. The common arithmetic opcodes (add, sub, mul) have
no such issue and remain branch-free.

### String opcodes

`EXPR_STR_COL_EQ_CONST`, `EXPR_STR_COL_LT_CONST`, `EXPR_STR_COL_EQ_COL`,
etc. involve German String comparison: 4-byte prefix check, then
variable-length blob tail comparison. The blob offsets vary per row
(each string's tail may be at a different blob heap offset).

The prefix comparison (first 4 bytes of each 16-byte struct) IS
vectorizable — it's a fixed-offset comparison on a contiguous column
buffer. The tail comparison is not.

In the batch interpreter, string opcodes would be implemented as:

```rust
EXPR_STR_COL_EQ_CONST => {
    // Per-row: must chase blob pointers
    for row in 0..n {
        let (s, blob) = get_str_struct(batch, row, a1, pki);
        scratch.regs[dst][row] = if col_string_equals_const(s, blob, ...) { 1 } else { 0 };
    }
}
```

This is per-row within the batch, but the opcode dispatch only happens
once per batch (not once per row). The benefit is modest — the main gain
from the batch interpreter is for numeric opcodes.

### EvalScratch ownership

GnitzDB is single-threaded per process. Options:

1. **Field inside Plan**: `scratch: RefCell<EvalScratch>`. Zero allocation
   after first use. Each Plan instance (one per operator in the DAG) has
   its own scratch. RefCell has zero overhead in single-threaded code.

2. **Thread-local**: One global EvalScratch, shared across all operators.
   Risk: if operator A calls operator B (indirect recursion via the DAG
   evaluator), the scratch is aliased. In GnitzDB's DAG execution model,
   operators are evaluated sequentially (no recursion), so this is safe
   but fragile.

3. **Parameter**: Pass `&mut EvalScratch` through op_filter/op_map. Most
   explicit, least error-prone, but requires signature changes.

Option 1 is recommended: each Plan owns its scratch, no aliasing possible,
no signature changes needed.

### Impact on op_filter

The batch interpreter returns a boolean register buffer. op_filter reads
it and produces ranges for append_batch:

```rust
// After batch evaluation, scratch.regs[result_reg] has 0/1 per row
let result = &scratch.regs[prog.result_reg as usize];
let null = &scratch.nulls[prog.result_reg as usize];
for i in 0..n {
    let pass = !is_null_in_bitmask(null, i) && result[i] != 0;
    // ... range detection, same as today
}
```

### Impact on expr_map_batch / Plan::execute_map

The batch interpreter replaces the per-row `eval_with_emit` loop. Instead
of EmitTarget raw pointers, the batch interpreter writes EMIT results
directly to output column buffers after evaluation:

```rust
// After batch evaluation, each EMIT register has the computed values
for (emit_idx, &payload) in emit_payloads.iter().enumerate() {
    let reg = emit_register_for(emit_idx);
    let dst = &mut out.col_data[payload];
    for row in 0..n {
        dst[row*8..row*8+8].copy_from_slice(&scratch.regs[reg][row].to_le_bytes());
    }
}
```

The EMIT null bits come from the null bitmask of the EMIT register:
```rust
for row in 0..n {
    let null_word = read_u64_le(&out.null_bmp, row * 8);
    let emit_null = extract_emit_nulls(&scratch.nulls, emit_registers, row);
    write_u64_le(&mut out.null_bmp, row * 8, null_word | emit_null);
}
```

This still has a per-row loop for null propagation, but only for EMIT
columns with nullable inputs. For non-nullable schemas, this loop is
skipped entirely.

### Risk assessment

- **Scope**: This is a rewrite of the interpreter execution model, not a
  targeted optimization. It touches expr.rs (major rewrite),
  scalar_func.rs (moderate changes to Plan::execute_map and filter_batch),
  and ops/linear.rs (op_filter adaptation). ~500 lines changed.
- **Correctness**: Every opcode must produce identical results in batch
  mode as in per-row mode. Test coverage: the existing unit tests in
  expr.rs and scalar_func.rs exercise all opcodes. Running them against
  both the batch and per-row interpreters (keeping the per-row as a
  debug reference) provides a correctness oracle.
- **Performance regression for small N**: The batch interpreter allocates
  register buffers (via scratch) and runs opcode loops even for N=1. The
  per-row interpreter allocates a stack register file (512 bytes) and
  returns immediately. For N<16, the per-row path may be faster. Solution:
  threshold check — if N < threshold, use per-row; else use batch.
- **The EmitTarget raw pointer pattern is eliminated**: The batch
  interpreter writes to output columns after evaluation, not during. This
  removes the safety concern about raw pointers into col_data buffers.


## Recommendation

**Start with Design A** (typed filter kernels). It is the smallest change,
has the clearest benefit (eliminates interpreter dispatch for the most
common filter patterns), and the `Interpreted` fallback guarantees zero
regression. The FilterKernel enum is already in place from Phase 1+2;
adding `AndCompare` and `OrCompare` is additive.

**Skip Design B** (typed map kernels) unless profiling shows map compute
is a bottleneck. The Mixed case (partially typed programs) adds complexity
that is hard to justify without evidence. If map compute IS a bottleneck,
go directly to Design C — the batch register file handles all opcodes
uniformly without the combinatorial complexity of typed kernels.

**Defer Design C** (batch register file) until the simpler approaches
have been measured. If Design A handles filter performance and map compute
is not a bottleneck, Design C is unnecessary. If map compute IS a
bottleneck, Design C is the right answer because it handles all expression
types (including strings) with one implementation.

In summary: **A first, C if needed, skip B.**
