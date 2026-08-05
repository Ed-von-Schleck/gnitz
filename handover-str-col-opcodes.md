# Handover: the `EXPR_STR_COL_*` opcode family (40–45) — measure, then delete

## 1. The problem

There are **two semantically identical ways to compare strings** in the
expression VM, and the **client-side SQL planner** picks between them based on
which one the **engine's kernels** run faster. That is engine physical-plan
knowledge leaking across the wire boundary into `gnitz-sql`.

**Family A — fused column/const opcodes (40–45)**, `crates/gnitz-wire/src/expr.rs`:

```rust
pub const EXPR_STR_COL_EQ_CONST: u32 = 40;
pub const EXPR_STR_COL_LT_CONST: u32 = 41;
pub const EXPR_STR_COL_LE_CONST: u32 = 42;
pub const EXPR_STR_COL_EQ_COL:   u32 = 43;
pub const EXPR_STR_COL_LT_COL:   u32 = 44;
pub const EXPR_STR_COL_LE_COL:   u32 = 45;
```

These read 16-byte German-string cells straight out of the batch and never build
a string register. They carry **no doc comments** in the wire crate, unlike
opcodes 57–75 which are heavily documented.

**Family B — generic string registers**, same file:

```rust
pub const EXPR_LOAD_COL_STR:   u32 = 57;
pub const EXPR_LOAD_CONST_STR: u32 = 58;
pub const EXPR_STR_CMP_EQ:     u32 = 61;
pub const EXPR_STR_CMP_LT:     u32 = 62;
pub const EXPR_STR_CMP_LE:     u32 = 63;
```

`LOAD_COL_STR + LOAD_CONST_STR + STR_CMP_EQ` computes exactly what
`STR_COL_EQ_CONST` computes.

## 2. Family A is live, not legacy

Do not delete it as dead code — it is actively selected. The project's
"no legacy code" rule does **not** apply here.

- `try_compile_string_cmp` (`crates/gnitz-sql/src/lower.rs`, ~line 60) recognises
  `ColRef(string) op LitStr`, `LitStr op ColRef(string)`, and
  `ColRef op ColRef` and routes them to family A.
- `OpcodeBackend::binop` calls it **before** recursing, so those shapes never
  build a string register.
- It is pinned by a test:
  `lower::tests::plain_column_comparisons_keep_the_specialized_opcodes`, which
  asserts `col = 'lit'` lowers to exactly `[EXPR_STR_COL_EQ_CONST]` and
  `col < col` to exactly `[EXPR_STR_COL_LT_COL]`.
- The complementary test
  `lower::tests::computed_operands_compare_through_the_register_channel` asserts
  `UPPER(col) = 'X'` uses `EXPR_STR_CMP_EQ` and *not* `EXPR_STR_COL_EQ_CONST`.

## 3. Why family A plausibly earns its place (the thing to measure)

The fused kernel `eval_str_cmp` (`crates/gnitz-expr/src/batch.rs`) compares
through `gnitz_wire::compare_german_strings`, which short-circuits on a 4-byte
big-endian prefix held in the cell:

```rust
let pfx_a = u32::from_be_bytes(a[4..8].try_into().unwrap());
let pfx_b = u32::from_be_bytes(b[4..8].try_into().unwrap());
if pfx_a != pfx_b { return pfx_a.cmp(&pfx_b); }
german_string_content(a, blob_a).cmp(german_string_content(b, blob_b))
```

One register compare resolves most pairs without touching the blob heap. It also
uses `StrOperand` (same file), whose `stride == 0` trick makes a resolved
constant and a payload column the *same* operand shape — one loop, no per-row
branch, no closure call.

The register path pays more:

- `cell_to_view` (`batch.rs`) **memcpys every short (≤12-byte) string's inline
  bytes into the arena** so that every `StrView` is a uniform
  `(buffer, offset, length)` triple. Short strings are the common case for
  codes/names/enums.
- `eval_str_reg_cmp` then does a full `view_bytes(..).cmp(view_bytes(..))` — no
  prefix short-circuit.
- `LOAD_CONST_STR` fills 256 identical `StrView`s per morsel (~4 KiB), which
  amortises to well under a cycle per row but is not zero.

So the fused family is a *plausible* real win on the hottest string filter shape
(`WHERE name = 'x'`). **It has never been measured against the register path in
this tree.** That measurement is the whole gate on this work.

## 4. Cost of keeping it

- ~88 lines in `try_compile_string_cmp` plus ~150 lines of its tests in
  `lower.rs`.
- 6 wire constants, 6 `ExprBuilder` methods (`str_col_eq_const`, `str_col_lt_const`,
  `str_col_le_const`, `str_col_eq_col`, `str_col_lt_col`, `str_col_le_col` in
  `crates/gnitz-core/src/expr.rs`, ~lines 339–361).
- 2 `LogicalInstr` variants + 2 `Instr` variants (`StrColConst`, `StrColCol`),
  6 `from_wire` arms, plus their `validate` / `reg_use` / `classify_registers` /
  `is_strictly_non_nullable` arms.
- `StrOperand` + `eval_str_cmp` in `batch.rs` (a second compare kernel beside
  `eval_str_reg_cmp`).
- **`ResolvedProgram` carries two independent encodings of the const pool.**
  `const_cells: Vec<[u8;16]>` + `const_blob` (German-string cells, built in
  `resolve_program`) serve family A; `const_arena` (raw bytes) serves
  `LoadConstStr`. `const_cells`/`const_blob` are built **unconditionally over
  every `const_strings` entry** — including `IntInSet` i64 pools and `StrTrim`
  byte sets, which are not strings at all. A pure register-channel program pays
  a full German-string encode of every constant for nothing.

  Note: `const_arena` is **not** redundant with `const_blob` and must not be
  "unified" naively. `view_bytes_at`'s non-arena branch reads the *batch's* blob,
  not `const_blob`, so a const view genuinely cannot resolve against
  `const_blob` without a third `StrView` source discriminator. This was checked;
  do not re-litigate it.
- Two shapes of accidental complexity in `try_compile_string_cmp`: a
  non-comparison decline guard that exists purely so the function stops shadowing
  `||`, and an "emit nothing before deciding" ordering constraint so a bail does
  not leave a dead const-pool entry behind. Both are artifacts of an optimizer
  living inside a lowering arm.

## 5. The proposed end state

Delete family A from the wire and the planner; keep the fast kernel, reached by a
**resolve-time peephole** inside the engine.

1. Delete wire constants 40–45, the 6 `ExprBuilder` methods, the 2 `LogicalInstr`
   variants and their 6 `from_wire` arms, and `try_compile_string_cmp` entirely
   (plus its tests). `str_binop` in `lower.rs` becomes the only string-compare
   lowering path.
2. **Keep `Instr::StrColConst` / `Instr::StrColCol` as resolve-only
   instructions** with no wire counterpart — exactly the precedent
   `Instr::EmitStr` already sets (it is derived at resolve from the source
   register's class and has no `LogicalInstr` or wire form).
3. In `resolve_program`, after the instruction vector is built, run a peephole
   recognising:
   - `LoadColStr{dst: d1, pi: c}` ; `LoadConstStr{dst: d2, k}` ; `StrCmp{op, dst: d, a: d1, b: d2}`
     → `StrColConst{op, dst: d, pi: c, const_idx: k}`
   - the col/col shape → `StrColCol`
   guarded on `use_count[d1] == 1 && use_count[d2] == 1` so no other instruction
   observes the intermediate registers.
4. Build `const_cells`/`const_blob` **only for the indices the peephole actually
   fused**, removing the unconditional encode of every pool entry.

`use_count` is already computed by `classify_registers` (`program.rs`). Ordering
matters: the peephole must run before the `classify_registers` /
`and_chain_mask` / `is_strictly_non_nullable` passes, or they must be re-run,
since fusing changes the register roles and the instruction list.

Payoff: ~250 lines deleted for ~40 added; the planner stops knowing about kernel
costs; and the fast path becomes reachable from shapes the current recogniser
cannot see (e.g. a string comparison that arrives through a different lowering
route), without the planner learning anything.

## 6. The measurement that gates this

There is already a bench harness to extend:
`crates/gnitz-expr/src/eval/tests.rs::str_const_filter_bench` (`#[ignore]`,
~line 679). It builds a 1M-row non-nullable STRING column with a deliberate
mix — ~1/16 rows match, every 7th row is a long (heap-backed) string, the rest
are short — and times `func.filter(...)`, reporting the **best of 30 passes**
(minimum, not mean — robust against thermal noise on this box).

Run it:

```bash
cd crates && cargo test -p gnitz-expr --release str_const_filter_bench \
    -- --ignored --nocapture --test-threads=1
```

**What to add:** a second program in the same harness, over the *same* `TestView`
and the same constant, built from `LoadColStr` + `LoadConstStr` + `StrCmp`
instead of `StrColConst`, and print both numbers side by side for `Eq` and `Lt`.

**Decision rule:**

- Register path within noise of the fused path → delete family A per §5.
- Register path materially slower → keep family A, but **record the measured
  numbers in a comment on `eval_str_cmp`** so the next reader does not have to
  re-derive the justification, and note in `try_compile_string_cmp`'s doc that
  its existence is a measured optimisation, not a structural requirement.

Either outcome resolves the open question; the current state (an unmeasured
claim in a test doc comment) does not.

**Do not skip the measurement.** Two standing project rules apply directly:
never rewrite hot-path code on reasoning alone, and this box's absolute
wall-clock is contaminated — prefer instructions-retired via `perf stat`, or the
best-of-N minimum this harness already uses, over a single wall-clock reading.
`make bench-full` has ±3–4% run-to-run variance and cannot resolve sub-5%
deltas; use the isolated microbench, not the e2e bench.

Consider also checking the generated code (`-O -C target-cpu=x86-64-v3`) for the
two loops — a `memcmp` call versus an inlined register compare is a more durable
signal than a timing delta.

## 7. Adjacent finding, possibly decisive

`cell_to_view`'s mandatory arena copy of every short string is *the reason* the
fused path wins, if it wins. `gnitz_wire::german_string_content` already returns
a **borrow** for short strings; `cell_to_view` copies anyway, purely so `StrView`
stays a two-buffer (arena, blob) triple. `StrView` currently spends a whole `u32`
(`src`, values `SRC_ARENA = 0` / `SRC_BLOB = 1`) on what is a 1-bit tag.

If `src` named a **third** source — the column's own cell region — a
`LoadColStr` would be zero-copy for both short and long values, and the register
compare could keep a prefix short-circuit. That would likely close the gap
entirely and make §5's peephole unnecessary rather than merely better-placed.

Cost: threading the cell region into `view_bytes` / `view_bytes_at` /
`arena_push_span` (which already thread `blob`), and handling the fact that
several string columns can be live at once, so a single extra buffer slot may
not suffice — it may need a small per-program table of source regions.

**Measure this before committing to either §5 or the status quo.** It is
plausible that the right end state is "one string compare kernel, zero-copy
column loads, no fused opcode family and no peephole" — which is strictly
simpler than both current options.

## 8. Constraints and gotchas

- **BLOB rides these opcodes too.** `try_compile_string_cmp` routes a BLOB
  column comparison here deliberately, because STRING and BLOB share the 16-byte
  German-string layout and `compare_german_strings` content-compares both. The
  integer path would read the descriptor bytes as a garbage int. Any replacement
  must keep BLOB working — see `lower::tests` `blob_schema()`.
- **`is_german_string()` matches STRING *and* BLOB**; `== TypeCode::String` does
  not. Several sites depend on the distinction. Check which one you need.
- **The six SQL comparisons ride three primitives.** There is now a single
  mapping, `str_cmp_reduction(op, can_swap)` in `lower.rs`, returning
  `(StrPrim, swap, negate)`. `GT`/`GE` have two realizations: operand swap (one
  opcode) where the site can exchange operands, and `bool_not` (one extra opcode
  and one extra register) where it cannot — family A cannot swap, because its
  opcode shape pins the constant to the right. If family A is deleted, the
  `can_swap` parameter becomes vestigial and should go with it, leaving the pure
  swap form.
- **Register pressure is real:** `MAX_REGS = 64`. The register path costs 2 extra
  registers per comparison (the two loads) versus family A's 0. A wide `WHERE`
  with many string comparisons could hit the cap where it previously did not.
  Worth a test if family A is deleted.
- `EXPR_STR_COL_*` opcodes are payload-only: `validate` pins their column
  operands to `ColKind::GermanString`, which is payload-only, so a PK column is
  rejected (`ColNotPayload`). PK columns are never strings anyway.
- Wire format is **not** a compatibility concern — gnitz is pre-alpha with no
  production users, so renumbering or removing opcodes is free. Leaving a gap at
  40–45 is fine; the space does not need to stay dense.

## 9. Verification

```bash
make verify   # fmt-check + clippy (warnings = errors) + test
make e2e      # 1725 tests, GNITZ_WORKERS=4 — always multi-worker
```

String behaviour is covered end-to-end by
`crates/gnitz-py/tests/test_string_functions.py`. Unit coverage lives in
`crates/gnitz-expr/src/program/tests.rs` (search `StrColConst`, `StrColCol`,
`test_string_eq_const`, `test_string_lt_le_const`, `test_string_col_eq_col`,
`test_string_prefix_ordering`, `test_string_prefix_le_ordering`) and
`crates/gnitz-expr/src/eval/tests.rs`.

Note `make e2e` piped through `tail` reports `tail`'s exit code, not the suite's
— check the printed summary line.
