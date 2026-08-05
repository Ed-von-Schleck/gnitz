# Handover: three per-opcode tables in `gnitz-expr` that must agree

## 1. The problem

`crates/gnitz-expr/src/program.rs` contains **three exhaustive `match` walks over
the same opcode set**, each encoding a different static property of every opcode.
They are independent, they must agree, and nothing enforces that they do.

| Table | Signature | Walks | Encodes |
|---|---|---|---|
| `reg_use` | `fn reg_use(li: &LogicalInstr) -> RegUse` | `LogicalInstr` | the register it writes (+ class), each register it reads (+ class) |
| `classify_registers` | `fn classify_registers(instrs: &[Instr], result_reg, is_filter) -> RegisterRoles` | `Instr` | per-read bool-vs-value role, per-dst bool-produced, `use_count` |
| `is_strictly_non_nullable` | `fn is_strictly_non_nullable(instrs: &[Instr], schema) -> bool` | `Instr` | can this opcode introduce a NULL, and which column it reads |

The enum pair they walk is large: `LogicalInstr` (~48 variants,
`program.rs:173`) and `Instr` (~50 variants, `program.rs:442`) together declare
142 variant heads in the file. The string-expression work added roughly 98 arms
across the three tables in lockstep.

`reg_use` already carries a doc comment calling itself "The **one** per-opcode
register table". That claim is now true for register *identity and class* — see
§3 — but the other two tables still restate the same opcode groupings for their
own facts.

## 2. Why it matters — the failure mode is silent

The dangerous one is `is_strictly_non_nullable`. It computes
`ResolvedProgram::no_nulls`, which decides:

- whether `EvalScratch` allocates `null_bits` / `bool_bits` at all
  (`ensure_capacity`, `batch.rs`), and
- which arm **every kernel** takes.

`merge_fail_mask` (`crates/gnitz-expr/src/batch.rs`, ~line 465) is how a kernel
reports a per-row failure (an out-of-range cast, an unparsable number, a
CONCAT overflow) as a NULL. Its first statement is an early return under
`no_nulls`.

So if an opcode *can* produce a NULL but `is_strictly_non_nullable` does not list
it as null-producing, then:

1. `no_nulls` is wrongly `true`,
2. the kernel still computes and raises its `bad[i]` flag,
3. `merge_fail_mask` returns early and **drops the flag**,
4. the row carries a wrong value with no null bit — no panic, no error, no
   assertion.

The existing code documents that this was already nearly missed once. From
`is_strictly_non_nullable`:

> `StrConcat` belongs here too, which is easy to miss because it looks like a
> pure transform: a combined length above `u32::MAX` yields NULL rather than
> tripping `encode_german_string`'s release assert, and on the `no_nulls` arm
> there is no null word to record that in.

`classify_registers` has a milder failure mode (a wrong `bit_only` /
`bool_pack_mask` / `use_count`), but it is the third copy of the same operand
lists and it grew in lockstep with the other two.

## 3. What has already been done (do not redo)

A previous session unified **`validate` into `reg_use`**. Before, `validate`
(`program.rs`, `LogicalProgram::validate`) contained ~28 `check_reg(...)` calls
inside its own match, re-enumerating exactly the operands `reg_use` returned two
lines later. Now `validate` hoists the bounds check off `reg_use`:

```rust
for instr in &self.instrs {
    let u = reg_use(instr);
    if let Some((dst, _)) = u.dst { check_reg(dst, num_regs)?; }
    for &(reg, _) in u.reads.iter().flatten() { check_reg(reg, num_regs)?; }
    match *instr { /* only aliasing / const-idx / col-kind / cast / trim */ }
    // then: class check + single-assignment (RegRewrite) check
}
```

Constraints that were established and must be preserved:

- **`Emit`'s `src` is deliberately absent from `RegUse::reads`** — its class
  selects which *destination column* rule applies rather than being held to one
  class. The `Emit` arm therefore keeps its own `check_reg(src, ...)`.
- **Diagnosis order is load-bearing for tests.** It must stay: register bounds →
  arm-specific (aliasing / const-idx / col-kind / cast-target / trim-mode) →
  class mismatch → `RegRewrite`. Specifically `LoadColInt dst=0` followed by
  `Select dst=0, cond=0` must report `RegisterAliasing`, not `RegRewrite`.
  Tests that pin exact variants: `test_from_wire_rejects_out_of_range_register`
  (expects `RegOutOfRange { reg: 5, num_regs: 2 }`),
  `test_from_wire_rejects_register_aliasing`,
  `test_from_wire_rejects_const_idx_out_of_range`,
  `test_validate_rejects_pk_column_for_payload_only_opcode`, all in
  `crates/gnitz-expr/src/program/tests.rs`.

A **debug assertion was added** to `merge_fail_mask`'s `no_nulls` early-return so
the silent failure of §2 becomes loud in any debug/test build:

```rust
debug_assert!(
    bad[..m].iter().all(|&b| b == 0),
    "a kernel raised a failure flag under no_nulls: \
     `is_strictly_non_nullable` does not list this opcode as null-producing",
);
```

It does **not** fire across the current suite (1377 engine + 141 expr + 264 sql
unit tests, and 1725 e2e tests), which is positive evidence that the three
tables agree *today*. It is a guard against future drift, not a fix for the
duplication.

## 4. The remaining work

Collapse `classify_registers` and `is_strictly_non_nullable` into the same
single per-opcode table as `reg_use`, or otherwise make it impossible for the
three to disagree.

### The blocker, stated precisely

`reg_use` walks `LogicalInstr`; the other two walk `Instr`. This is not
incidental:

- `reg_use` must run **pre-resolve**, because `validate` runs before
  `resolve_program` and validation is what makes resolution safe.
- `classify_registers` and `is_strictly_non_nullable` must run **post-resolve**,
  because they need resolved facts: `is_strictly_non_nullable` reads the payload
  slot `pi` to ask `schema.col_nullable(...)`, which only exists after column
  locators are resolved.

So a literal single function is not available without addressing that split.

### Options, in ascending order of ambition

**(a) Second `RegUse`-style impl over `Instr`.** Cheapest. Add
`fn instr_info(i: &Instr) -> OpcodeInfo` carrying the union of what
`classify_registers` and `is_strictly_non_nullable` need, and drive both from it.
Reduces three tables to two. Does not remove the `LogicalInstr`/`Instr`
duplication.

**(b) Shared accessor trait.** Define a trait with `dst()`, `reads()`,
`introduces_null()`, `reads_columns()` and implement it for both enums. The two
impls still each need a full match, so this buys type-level uniformity but not
fewer arms. Probably not worth it alone.

**(c) Collapse the `LogicalInstr` / `Instr` enum pair — the real fix.**
Roughly 38 of the ~48 `LogicalInstr` variants are byte-identical to their `Instr`
counterparts. The genuine differences are only:

- column operands: `LogicalInstr` carries a logical `col: u32`, `Instr` carries a
  resolved `pi: u8` / `off: u8` / `ColumnLocator`;
- decoded side tables: `Instr`'s `const_idx` for `IntInSet` / `StrTrim` points at
  `int_sets` / `trim_sets` slots, not the raw const pool; `LoadConstStr` becomes
  an `(off, len)` arena span;
- resolve-time flags: `signed` / `src_signed` / `start_signed` / `len_signed`
  (from the `reg_u64` tracking), `fi: FixedInt`, `wide: bool`;
- `Instr` has `EmitStr`, which has no wire or `LogicalInstr` counterpart — it is
  derived at resolve from the source register's class.

A parameterised enum (e.g. `Ins<C>` where `C` is the column-operand
representation) or a shared discriminant keyed table would let one table serve
all consumers. This is the change that would actually make drift impossible. It
is also the largest, and it touches `from_wire`, `validate`, `resolve_program`,
`eval_batch`, and every test that constructs instructions by hand.

### What the unified record needs to carry

From the three current tables:

```rust
struct OpcodeInfo {
    dst: Option<(u16, Class)>,       // reg_use
    reads: [Option<(u16, Class)>; 3],// reg_use
    // classify_registers:
    produces_bool: bool,             // dst goes in bool_produced
    reads_as_bool: [bool; 3],        // per-read: bool_input vs non_bool_read
    // is_strictly_non_nullable:
    introduces_null: bool,           // unconditionally NULL-producing
    null_columns: [Option<u8>; 2],   // payload slots whose nullability matters
}
```

Note the asymmetries that must survive:

- `Select` / `StrSelect` read `cond` as a **bool** and `a`/`b` as **values**;
  their `dst` is deliberately in *neither* `bool_produced` nor `bool_input`,
  which is what keeps it out of `bit_only`.
- `StrCmp` produces a bool from two **string** operands; both role masks are
  keyed by register index alone, so operand class does not enter there.
- `StrSubstr` is conditionally null-producing: `len_reg: Some(_)` can NULL (a
  negative length), `len_reg: None` cannot. A flat `introduces_null: bool` is
  therefore not sufficient — it needs to be a function of the variant's payload,
  not just its discriminant. Same for the column-nullability arms
  (`LoadPayloadInt`, `LoadPayloadFloat`, `StrColConst`, `LoadColStr`,
  `StrColCol`).
- `Emit` / `EmitStr` / `CopyCol` write no register (`RegUse::nothing()`), but
  `Emit`/`EmitStr` *do* read one (counted in `use_count` as a non-bool read by
  `classify_registers`, while `reg_use` deliberately omits it — see §3).

That last point is a real inconsistency between the two tables today and must be
resolved explicitly when merging, not papered over.

## 5. Current exhaustiveness protections (keep them)

`is_strictly_non_nullable` ends with an explicit exhaustive remainder listing
every non-null-producing variant, with **no `_` wildcard**, and says why:

> Exhaustive remainder (no `_` wildcard): a future null-producing variant must be
> classified here, not silently treated as safe.

Any refactor must keep that property. A `_ => {}` arm anywhere in these three
tables reintroduces exactly the silent failure of §2.

## 6. Verification

```bash
make verify                      # fmt-check + clippy (warnings = errors) + test
cargo test -p gnitz-expr --lib   # 141 tests, fastest inner loop
make e2e                         # 1725 tests, GNITZ_WORKERS=4
```

Targeted tests that exercise these tables:

- `program::tests::test_validate_bounds_checks_the_new_register_operands`
- `program::tests::the_legal_and_chain_still_earns_its_trigger_mask`
  (`and_chain_mask` depends on `use_count` and on the single-writer premise)
- `program::tests::trim_mode_and_cast_target_are_narrowed_at_decode`
- the `no_nulls` classification tests near `is_strictly_non_nullable`'s tests in
  `program/tests.rs` (search for `no_nulls`)

The `merge_fail_mask` `debug_assert` from §3 runs in every debug test, so a
misclassification introduced by the refactor should surface as a test failure
rather than a wrong value. Do not run the verification only in `--release`: that
assertion compiles out.

## 7. Notes and constraints

- `MAX_REGS = 64` (`program.rs`). Every role mask is a `u64` keyed by register
  index; `use_count` is `[u8; MAX_REGS]` saturating at 255.
- `MORSEL = 256` (`batch.rs`).
- `Instr` is `Copy` and currently `size_of == 16` bytes. A previous measurement
  confirmed the string variants did not grow it (the widest, `StrSubstr`, has 12
  bytes of payload and fits under `LoadConst`'s existing 16). If the refactor
  widens `Instr`, that is a real regression on the scalar dispatch loop —
  check `size_of::<Instr>()` before and after.
- Single assignment (`RegRewrite`) is what makes a register's class well-defined
  for its whole life and what `and_chain_mask`'s unique-writer premise rests on.
  Do not relax it.
- Project rules that apply: no legacy/compat shims (pre-alpha, no production
  users); no `#[allow(dead_code)]` or placeholder stubs; comments must state
  whether they describe current behaviour or a cost avoided.
