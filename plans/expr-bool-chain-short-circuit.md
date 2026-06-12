# Skip the dead tail of AND-chain predicates per morsel

## Problem

A predicate like `WHERE a = 1 AND b = 2 AND c = 3 AND d = 4 AND e = 5`
compiles to a left-deep chain of four `EXPR_BOOL_AND` opcodes
separated by five `EXPR_CMP_*` producers. In `eval_batch`
(`crates/gnitz-engine/src/expr/batch.rs:390-639`), every opcode runs
for every morsel of 256 rows, regardless of how early the chain has
already eliminated every row in the current morsel.

Most production filter workloads are highly selective on a leading
prefix of clauses (PK equalities, active flags, date ranges); the
remaining clauses exist for correctness on the small subset of rows
that survive the prefix. When the prefix kills every row in a morsel,
every CMP and AND below that point produces output that
`run_filter` (`plan.rs:407-421`) then discards row by row.

The savings are per-morsel, not per-row. When *some* row in a morsel
survives the leading sub-conjunction, the suffix opcodes are still
required for that row. When *no* row in a morsel survives, every
downstream opcode in the chain can be skipped, plus the per-row work
inside `run_filter`'s row scan for that morsel is also trivially zero
(but is paid anyway because the loop is unconditional).

## Step 0 — Microbench

Confirm the win exists before adding bytecode-level control flow.

Conditions for impact:

- A nullable filter program with a left-deep AND chain of length ≥ 3
  (i.e., ≥ 4 CMP-style producers feeding ≥ 3 ANDs).
- The leading prefix has selectivity 0 on a contiguous fraction of
  morsels (not just per-row selectivity 0 across the batch — the
  short-circuit fires only when *every row in a morsel* fails).
- Per-opcode cost is large enough that the skip pays for the check.

Bench shape: a 5-clause AND predicate
`a > $k1 AND b > $k2 AND c > $k3 AND d > $k4 AND e > $k5`
over a 1M-row nullable table. Three runs:

1. **High-selectivity prefix:** `$k1` chosen so the first AND yields
   all-FALSE for ~70% of morsels (e.g., `a > MAX(a) - small_window`
   with `a` clustered). This is the case that benefits most.
2. **No-selectivity prefix:** `$k1, $k2, $k3` chosen so every morsel
   has at least one row passing all five clauses (e.g., `> MIN(a) - 1`).
   The short-circuit never fires; this measures pure overhead.
3. **Non-nullable baseline:** same as (2) on `NOT NULL` columns. The
   non-nullable AND arm autovectorizes and is structurally different;
   it establishes a hard floor.

Ship the plan only if both:

- Run (1) drops below run (2) by ≥ 30% of the filter cost.
- Run (2) shows no regression vs. its pre-plan baseline (the check
  costs nothing when it never fires).

If either fails, stop here.

## Detection

The compile-time pass identifies one linear AND chain per program
that is safe to short-circuit without violating SSA semantics. Run it
once at `ExprProgram` construction, after the existing
`resolve_column_indices`.

### What counts as a skippable chain

A chain is an ordered sequence of `EXPR_BOOL_AND` opcodes
`[and_0, and_1, ..., and_K]` (`K ≥ 1`) such that:

1. **Linear dependency.** For each `j > 0`, exactly one of
   `and_j.a1`, `and_j.a2` equals `and_{j-1}.dst`. The other source
   is produced outside the chain (by an earlier `EXPR_CMP_*`,
   `EXPR_FCMP_*`, `EXPR_STR_COL_*`, `EXPR_IS_NULL`, or
   `EXPR_IS_NOT_NULL`).
2. **Single-use intermediates.** For each `j < K`, the register
   `and_j.dst` is read by exactly one opcode in the entire program:
   `and_{j+1}`. This is the load-bearing invariant — when the chain
   short-circuits and `and_j.dst` is left unwritten for some
   morsels, no other opcode ever observes the missing value.
3. **Terminal at result_reg.** `and_K.dst == prog.result_reg` and the
   program is a filter (predicate mode). This restricts v1 to the
   case where the chain's terminal feeds `run_filter` directly. Map
   programs that select a boolean expression are excluded.
4. **No interleaving readers.** Between `and_0` and `and_K` in
   bytecode order, no instruction reads a chain intermediate other
   than the consuming AND. (Condition 2 already implies this.)

A chain of length 1 (single AND) carries no skip benefit — there is
nothing downstream of it in the chain to skip. Reject `K < 1`.

### Algorithm

```rust
fn detect_and_chain(prog: &ExprProgram) -> Option<ChainInfo> {
    let n = prog.num_instrs as usize;
    if n < 2 { return None; }

    // 1. Use counts on register operands. Only count opcodes whose
    //    a1/a2 are register indices: arithmetic, NEG, INT_TO_FLOAT,
    //    CMP/FCMP, BOOL_AND/OR/NOT, EMIT. Loads, COPY_COL, EMIT_NULL,
    //    IS_NULL/IS_NOT_NULL, STR_COL_*, and LOAD_CONST do not count.
    let mut use_count = [0u8; 64];
    for instr in prog.code.chunks_exact(4) {
        let op = instr[0];
        if is_binary_reg_reader(op) {
            use_count[instr[2] as usize] = use_count[instr[2] as usize].saturating_add(1);
            use_count[instr[3] as usize] = use_count[instr[3] as usize].saturating_add(1);
        } else if is_unary_reg_reader(op) {
            use_count[instr[2] as usize] = use_count[instr[2] as usize].saturating_add(1);
        }
    }

    // 2. Walk backwards from the instruction that writes result_reg.
    //    It must be the terminal AND.
    let term_pc = (0..n).rev().find(|&i| {
        prog.code[i * 4 + 1] == prog.result_reg as i64
    })?;
    if prog.code[term_pc * 4] != EXPR_BOOL_AND { return None; }

    let mut chain = vec![term_pc];
    let mut accumulator_reg = {
        // Determine which source of the terminal AND is the chain
        // accumulator: the one written by an EXPR_BOOL_AND with a
        // single-use destination. Try a1 first (left-deep is the
        // expected shape from the compiler); fall back to a2.
        let a1_writer = find_writer(prog, prog.code[term_pc * 4 + 2], term_pc);
        let a2_writer = find_writer(prog, prog.code[term_pc * 4 + 3], term_pc);
        match (a1_writer, a2_writer) {
            (Some(w), _) if prog.code[w * 4] == EXPR_BOOL_AND
                && use_count[prog.code[w * 4 + 1] as usize] == 1
                => prog.code[term_pc * 4 + 2],
            (_, Some(w)) if prog.code[w * 4] == EXPR_BOOL_AND
                && use_count[prog.code[w * 4 + 1] as usize] == 1
                => prog.code[term_pc * 4 + 3],
            _ => return None,
        }
    };

    loop {
        let writer_pc = find_writer(prog, accumulator_reg, *chain.last().unwrap())?;
        if prog.code[writer_pc * 4] != EXPR_BOOL_AND { break; }
        if use_count[prog.code[writer_pc * 4 + 1] as usize] != 1 { break; }
        chain.push(writer_pc);

        let a1 = prog.code[writer_pc * 4 + 2];
        let a2 = prog.code[writer_pc * 4 + 3];
        let a1_w = find_writer(prog, a1, writer_pc);
        let a2_w = find_writer(prog, a2, writer_pc);
        accumulator_reg = match (a1_w, a2_w) {
            (Some(w), _) if prog.code[w * 4] == EXPR_BOOL_AND
                && use_count[prog.code[w * 4 + 1] as usize] == 1 => a1,
            (_, Some(w)) if prog.code[w * 4] == EXPR_BOOL_AND
                && use_count[prog.code[w * 4 + 1] as usize] == 1 => a2,
            _ => break,
        };
    }

    if chain.len() < 2 { return None; }
    chain.reverse();   // ascending pc order
    Some(ChainInfo {
        chain_ands: chain,
        terminal_reg: prog.result_reg,
        jump_to_pc: term_pc as u32 + 1,
    })
}

fn find_writer(prog: &ExprProgram, reg: i64, before_pc: usize) -> Option<usize> {
    (0..before_pc).rev().find(|&i| prog.code[i * 4 + 1] == reg)
}
```

The detector visits each instruction O(1) times for use-count and
performs O(chain_length × instruction_count) work for the
`find_writer` walks. For chains of practical length (≤ ~16) and
programs of practical size (≤ ~64 instructions), this is bounded
work at construction time. If `find_writer` becomes a hot path on
program load — it shouldn't — pre-build a `Vec<Option<u32>>`
mapping `reg -> writer_pc`.

### What gets stored

```rust
pub(in crate::expr) struct ChainSkip {
    pub trigger_pc: u32,    // an AND opcode whose all-FALSE result triggers a skip
    pub jump_to_pc: u32,    // pc to resume execution at
    pub terminal_reg: u32,  // register to mark all-FALSE before jumping
}
```

```rust
pub struct ExprProgram {
    // ... existing fields ...
    pub(in crate::expr) chain_skips: Vec<ChainSkip>,
}
```

Populated by `ExprProgram::compute_chain_skips`, called from
`resolve_column_indices` after the column-rewrite pass:

```rust
pub(in crate::expr) fn compute_chain_skips(&mut self) {
    self.chain_skips.clear();
    let Some(chain) = detect_and_chain(self) else { return };
    // Every AND in the chain except the terminal one gets a skip
    // entry: when *that* AND yields all-FALSE for the morsel, we
    // know the terminal will too, so jump past the chain and write
    // all-FALSE to terminal_reg.
    for i in 0..chain.chain_ands.len() - 1 {
        self.chain_skips.push(ChainSkip {
            trigger_pc: chain.chain_ands[i] as u32,
            jump_to_pc: chain.jump_to_pc,
            terminal_reg: chain.terminal_reg,
        });
    }
}
```

Stored sorted ascending by `trigger_pc`. The runtime lookup is a
linear scan (chain length ≤ ~16 in practice); if it ever becomes
hot, replace with a perfect-hash `[Option<u16>; num_instrs]` table.

## Runtime mechanism

`eval_batch` currently loops via `for instr in prog.code.chunks_exact(4)`.
Convert to an explicit pc loop:

```rust
let mut pc = 0usize;
let n = prog.num_instrs as usize;
while pc < n {
    let base = pc * 4;
    let op = prog.code[base];
    let dst = prog.code[base + 1] as usize;
    let a1 = prog.code[base + 2];
    let a2 = prog.code[base + 3];
    let mut next_pc = pc + 1;

    match op {
        // ... existing handlers; each computes its result and sets
        //     scratch.regs[dst*..] and (if !no_nulls) scratch.null_bits[dst*..]
        EXPR_BOOL_AND if !scratch.no_nulls => {
            // The existing word loop computes result_bits and nd per
            // word and writes them to regs / null_bits. Extend it to
            // also OR them into an `alive` accumulator so we know,
            // by the end of the loop, whether any row produced
            // anything other than definite-FALSE.
            let ai = a1 as usize;
            let bi = a2 as usize;
            let words = m.div_ceil(64);
            let mut alive: u64 = 0;
            for w in 0..words {
                let base = w * 64;
                let chunk_m = (base + 64).min(m) - base;
                let mut va: u64 = 0;
                let mut vb: u64 = 0;
                for i in 0..chunk_m {
                    if scratch.regs[ai * MORSEL + base + i] != 0 { va |= 1u64 << i; }
                    if scratch.regs[bi * MORSEL + base + i] != 0 { vb |= 1u64 << i; }
                }
                let na = scratch.null_bits[ai * NULL_WORDS_PER_REG + w];
                let nb = scratch.null_bits[bi * NULL_WORDS_PER_REG + w];
                let definite_false = (!na & !va) | (!nb & !vb);
                let nd = !definite_false & (na | nb);
                let result_bits = !nd & va & vb;
                scratch.null_bits[dst * NULL_WORDS_PER_REG + w] = nd;
                for i in 0..chunk_m {
                    scratch.regs[dst * MORSEL + base + i] = ((result_bits >> i) & 1) as i64;
                }
                alive |= result_bits | nd;
            }
            if alive == 0 {
                if let Some(skip) = prog.find_chain_skip(pc as u32) {
                    write_all_false(scratch, skip.terminal_reg as usize, m);
                    next_pc = skip.jump_to_pc as usize;
                }
            }
        }
        // ... other handlers unchanged ...
    }

    pc = next_pc;
}
```

Two things to notice:

- `alive |= result_bits | nd` is appended to the existing per-word
  body. It reads no extra memory (both values are already in
  registers from the just-computed line) and adds a single OR. The
  cost is ≤ 1 cycle per word, ≤ 4 cycles per morsel.
- The `alive == 0` test runs once per AND opcode per morsel. When
  no chain entry matches `pc`, the lookup misses cheaply and the
  branch falls through. The check is a single OR-reduce plus a
  predictable branch (the morsel-level selectivity pattern is
  highly autocorrelated within a batch).

### The all-FALSE check on tail bits

The check `alive == 0` is robust against partial-tail morsels
because, in the AND handler, `va` and `vb` are packed from `regs`
via `if scratch.regs[...] != 0 { va |= 1u64 << i }` over `i in 0..chunk_m`.
Bits `chunk_m..64` of `va` and `vb` stay zero. `na`, `nb` are read
from `null_bits` which `clear_null_reg` already zeroes per opcode.
Therefore `result_bits | nd` has zero tail bits, and `alive == 0`
correctly identifies "every row's row-wise result is definite-FALSE".

The only producer that leaves dirty tail bits in `null_bits` or in
the value space is — at present — none: every CMP/FCMP/STR_COL
clears null bits via `null_or2`, and every non-null arm writes
clean per-row i64s. Adding an opcode that violates this should
trigger a SSA-style assertion at compile time.

### The terminal write

```rust
#[inline]
fn write_all_false(scratch: &mut EvalScratch, reg: usize, m: usize) {
    let base_regs = reg * MORSEL;
    for i in 0..m { scratch.regs[base_regs + i] = 0; }
    if !scratch.no_nulls {
        let base_null = reg * NULL_WORDS_PER_REG;
        let words = m.div_ceil(64);
        for w in 0..words { scratch.null_bits[base_null + w] = 0; }
    }
}
```

The terminal is `result_reg`, read by `run_filter`'s per-row scan as
`scratch.regs[base_r + i] != 0` and (when nullable) by the null
bitmap check. Writing zero to both fields means `run_filter` reads
"every row failed, with no nulls" — the morsel contributes nothing
to `filter_bits`, which is the correct outcome.

The write is `m` i64 stores + ≤ 4 u64 stores per morsel —
significantly cheaper than the opcodes it replaces.

## Cost on representative chains

Bytecode for the 5-clause AND chain from Step 0:

```
0: CMP r1 = a > k1
1: CMP r2 = b > k2
2: AND r3 = r1 AND r2     <- chain_skips[0]: trigger=2, jump=9, terminal=r9
3: CMP r4 = c > k3
4: AND r5 = r3 AND r4     <- chain_skips[1]: trigger=4, jump=9, terminal=r9
5: CMP r6 = d > k4
6: AND r7 = r5 AND r6     <- chain_skips[2]: trigger=6, jump=9, terminal=r9
7: CMP r8 = e > k5
8: AND r9 = r7 AND r8     <- terminal (no entry)
9: (end)
```

**All-FALSE at the first AND (chain_skips[0] fires):**

- Without short-circuit: opcodes 0–8 run. Per morsel, ~9 × 256
  i64-touches in the inner loops plus per-opcode null bookkeeping.
- With short-circuit: opcodes 0, 1, 2 run normally; opcode 2 emits
  one OR-reduce + one branch + one `write_all_false`; opcodes
  3, 4, 5, 6, 7, 8 are skipped. `write_all_false` costs `m` i64
  stores + ≤ 4 u64 stores. Net per morsel: ~3/9 of the original
  opcode cost, plus ~m i64 stores. For a 256-row morsel that is
  roughly a 60% reduction.

**No-selectivity prefix (no chain_skip ever fires):**

- Per morsel: opcodes 0–8 run as before; opcodes 2, 4, 6 each
  perform one extra OR-reduce (4 ORs already inlined into the
  word loop, plus a final `alive == 0` test) and one branch that
  the predictor learns to take "not taken". Cost: ~3 × (≤5 cycles)
  = ~15 cycles overhead per morsel × 4 morsels = ~60 cycles per
  1024-row batch. Negligible relative to the chain's actual work.

**Mixed selectivity (half of morsels short-circuit at AND #1):**

- Half the morsels execute the full chain; the other half execute
  3 opcodes plus the terminal write. Expected savings: roughly 30%
  of total chain cost over the batch.

## Edge cases

**No chain detected.** `chain_skips` is empty. `find_chain_skip`
always returns `None`. The runtime pc loop behaves identically to
the current `for ... in chunks_exact(4)` loop, modulo the
unconditional `alive |= ...` accumulator inside the AND body (which
is dead code from LLVM's perspective when no chain consumes it —
but LLVM cannot prove that statically). Confirm via codegen
inspection that the accumulator is hoisted away when
`chain_skips.is_empty()` — if it isn't, gate the accumulation on
`prog.has_and_chain` (a flag set when `chain_skips` is non-empty).

**`no_nulls` programs.** The non-nullable AND arm is a flat scalar
loop that LLVM autovectorizes; it does not compute `result_bits`
as a u64. Adding the `alive` accumulator there would require
explicitly reducing the scalar `regs[d+i]` writes back into a u64,
defeating the autovectorization. Skip short-circuit on no_nulls
programs entirely:

```rust
if !scratch.no_nulls {
    // try chain_skip lookup
}
```

The benefit on no_nulls programs is marginal anyway because the
inner loop is already SIMD; the prefix opcodes finish in tens of
cycles per morsel.

**Right-deep AND chains.** The detector tries `a1` first and falls
back to `a2`. Either shape is supported. The compiler should emit
left-deep chains for left-to-right parsing of SQL conjunctions; if
it doesn't, the detector handles it correctly anyway.

**Map programs that select a boolean expression.** Excluded in v1
by the `chain.terminal_reg == result_reg && is_filter` requirement.
Map programs go through `execute_map` / `EXPR_EMIT`, where the
boolean is consumed as an i64 column value; we can't synthesize an
all-FALSE column value from a partial morsel skip without explicit
per-row writes, and at that point the short-circuit benefit
collapses.

**Nested AND inside OR (`(a AND b) OR (c AND d)`).** The chain
detector walks back from `result_reg`; if `result_reg` is written
by `EXPR_BOOL_OR`, no AND chain is detected. The OR-chain
extension below handles this case.

**EXPR_BOOL_NOT in the chain.** `NOT (a AND b)` does not form a
short-circuitable chain — the NOT writes a single non-AND opcode
between the AND and the chain's terminal. Detector rejects it.
This is correct: `NOT FALSE = TRUE`, so an all-FALSE intermediate
under a NOT actually produces an all-TRUE downstream result, the
opposite of what we'd short-circuit to.

**Partial-tail morsel.** The check handles `m % 64 != 0` correctly
(see "The all-FALSE check on tail bits" above). The terminal write
respects `m` exactly: only the first `m` i64s are zeroed; later
rows in `regs[result_reg]` are left at whatever the previous batch
or morsel wrote there. That is fine — `run_filter` only reads the
first `m` rows of the current morsel.

**Use-count overflow.** `saturating_add` on `u8` caps at 255; a
register read 255 times is — for our 64-register cap and short
programs — impossible. The saturating add is defensive.

## OR-chain extension

Symmetric to AND. Detection: same algorithm with `EXPR_BOOL_OR` in
place of `EXPR_BOOL_AND`. Trigger condition: definitely-TRUE for
every row. Terminal write: all-TRUE.

The "definitely TRUE" check is tail-sensitive because OR produces
`result_bits = (!na & va) | (!nb & vb)` (`definite_true`), which
can leave 1s in tail bit positions if upstream producers do (none
do today). The safe formulation:

```rust
// Inside the EXPR_BOOL_OR word loop:
let mut all_true_so_far = true;
for w in 0..words {
    // ... compute definite_true, nd, result_bits ...
    let chunk_m = (w * 64 + 64).min(m) - w * 64;
    let valid_mask = if chunk_m == 64 { !0u64 } else { (1u64 << chunk_m) - 1 };
    if (result_bits & valid_mask) != valid_mask || (nd & valid_mask) != 0 {
        all_true_so_far = false;
    }
}
if all_true_so_far {
    // short-circuit: write all-TRUE to terminal
}
```

```rust
fn write_all_true(scratch: &mut EvalScratch, reg: usize, m: usize) {
    let base_regs = reg * MORSEL;
    for i in 0..m { scratch.regs[base_regs + i] = 1; }
    if !scratch.no_nulls {
        let base_null = reg * NULL_WORDS_PER_REG;
        let words = m.div_ceil(64);
        for w in 0..words { scratch.null_bits[base_null + w] = 0; }
    }
}
```

The OR-chain extension is gated on the bench showing residual win
on disjunctive predicates. Most SQL workloads are AND-dominated;
OR-chains as the top-level conjunct are uncommon. Implement AND
first, ship, measure, then decide on OR.

## Out of scope

- **Word-level short-circuit** (skipping individual 64-row words
  within a morsel). Adds inner-loop control flow that fights
  vectorization in non-short-circuited words. Morsel-level is the
  natural granularity for a columnar interpreter.
- **Multiple independent chains per program.** v1 finds at most one
  chain (terminating at `result_reg`). A program like
  `(a AND b AND c) OR (d AND e AND f)` has two sub-AND-chains
  terminating at intermediate registers; their savings depend on
  the OR's runtime behavior. Defer.
- **Selectivity-based clause reordering.** The compiler emits
  clauses in source order; the optimizer is the right place to
  reorder by measured or estimated selectivity, not the
  short-circuit pass.
- **Right-deep normalization in the compiler.** The detector
  handles both shapes; no compiler change is strictly required.
- **Cross-batch memoization** (skipping morsels whose inputs match
  a previous batch). Different problem.
- **Inlining the chain into a synthetic `AND_CHAIN_N` opcode** that
  consumes N producer registers and an early-out target. Would
  obsolete the runtime pc-jump but requires a new opcode, breaks
  the SSA assertions on register aliasing, and complicates
  bytecode rewriters. Not worth the disruption at this point.
- **Mixed AND/OR chains with sign flips at each NOT.** Possible in
  principle (an all-FALSE under one NOT becomes all-TRUE), but the
  detection and runtime mechanics double in size. Defer.

## Testing

Equivalence pins:

- **Differential over chain shapes.** For each `K` in `1..=8`,
  generate random nullable input columns and a `K`-clause AND
  predicate with mixed CMP / FCMP / STR_COL_*_CONST / IS_NOT_NULL
  operands. Run the filter through `Plan::run_filter` with the
  short-circuit pass enabled, and assert the produced `filter_bits`
  equals a scalar reference (a hand-rolled 3VL per-row evaluator).
  Sweep `m` for the final morsel from 1 to 256 to cover tail
  handling.

Targeted behavior tests:

- **Short-circuit fires every morsel.** Predicate
  `WHERE a = -1 AND b = -1 AND c = -1` over data where `a` never
  equals `-1`. Add a `#[cfg(test)]` counter on the short-circuit
  branch; assert it equals the number of morsels processed and
  that opcodes for `b` and `c` did not write to their destination
  registers (verify by pre-poisoning the register slots and
  checking they are unchanged).
- **Short-circuit never fires.** Pass-all predicate
  `WHERE a IS NOT NULL AND b IS NOT NULL AND c IS NOT NULL` over
  data with no nulls. Assert the counter is zero.
- **Half-and-half.** Predicate
  `WHERE a > $threshold AND b > 0 AND c > 0` with `$threshold`
  chosen so half the morsels have all rows failing the first AND.
  Assert counter equals half the morsel count and the produced
  bitmask equals the scalar reference.
- **Terminal write correctness.** Construct a program where, after
  the short-circuit fires, `result_reg`'s previous-batch contents
  would be nonzero for at least one row. Confirm
  `write_all_false` zeroes those positions and `run_filter`
  produces no false positives.
- **Partial tail morsel.** Batch of size `4 * 256 + 17`; the last
  morsel has `m = 17`. Force a short-circuit on the last morsel
  and on a full-size morsel; assert both produce the correct
  bitmask. Specifically check that rows 17..255 of the partial
  morsel's `regs[result_reg]` are not consulted.
- **NULL-flooded morsel.** Data where one morsel has every row
  with `a IS NULL`. Predicate
  `WHERE a IS NOT NULL AND b > 0 AND c > 0`. The leading IS_NOT_NULL
  yields FALSE for every row in that morsel; the first AND is
  definite-FALSE; short-circuit fires. Verify.

Detection unit tests:

- **Pure left-deep chain detected.** Golden bytecode for a 5-clause
  AND chain; assert `chain_skips.len() == 4` and that each entry
  has `terminal_reg == result_reg` and the correct `trigger_pc`.
- **Right-deep chain detected.** Same predicate with the compiler
  emitting `and_j.a2 == and_{j-1}.dst`. Assert the detector finds
  the chain via the `a2` fallback.
- **Multi-use intermediate rejects the chain.** Construct a
  program where an AND intermediate is used both as the next AND's
  source *and* as an input to a CMP that emits another column.
  Assert `chain_skips.is_empty()`.
- **Non-filter program rejected.** A map program selecting a
  boolean expression. Assert `chain_skips.is_empty()`.
- **Single-AND program rejected.** `WHERE a > 1 AND b > 2`.
  Detected chain has `K = 1`; the loop above only emits skip
  entries for `K - 1` ANDs, so `chain_skips.is_empty()`. Assert.
- **Chain interrupted by OR.** `WHERE (a > 1 AND b > 2) OR c > 3`.
  Terminal is OR, not AND. Assert `chain_skips.is_empty()`.
- **NOT in the middle.** `WHERE a > 1 AND NOT (b > 2) AND c > 3`.
  The middle NOT breaks the chain; the detector should either
  find a length-1 chain (rejected) or none. Assert
  `chain_skips.is_empty()`.

The Step-0 bench is a regression gate:

- Run (1) must drop by ≥ 30% of filter cost.
- Run (2) must not regress beyond noise floor (≤ 2%).
- Run (3) must not regress at all (no_nulls path is untouched).

`make test` and `make e2e` must stay green throughout.
