# One expression evaluator: extract the expr VM into `gnitz-expr`, delete both client interpreters

## Goal

Expression semantics live in three runtime evaluators that must agree by
discipline:

1. the engine expr VM (`crates/gnitz-engine/src/expr/{program,batch,plan}.rs`)
   — authoritative: every view filter/map/HAVING, every ReadSpec predicate;
2. `InterpBackend` (`crates/gnitz-sql/src/exec/eval.rs:30`, `impl
   BoundExprBackend` at `:36`) — an int-only `Out = Option<i64>` tree-walk for
   UPDATE/DELETE residual predicates and numeric SET expressions; `col_ref`
   (`:39-83`) rejects U128/UUID/STRING/BLOB, `lit_float` (`:92-96`) rejects every
   float;
3. `HavingEval` (`crates/gnitz-sql/src/exec/agg_finish.rs:419`, `impl
   BoundExprBackend` at `:449-538`) — a typed `Out = Val` tree-walk for ad-hoc
   aggregate HAVING, whitelist-limited: `col_ref` (`:457-461`) admits only F64
   + I8/I16/I32/I64 + U8/U16/U32 (no U64, no strings), and `null_test`
   (`:520-524`), `case` (`:531-533`), `in_list` (`:535-537`) return `Unsupported`
   unconditionally.

This plan extracts the VM's evaluator core into a new leaf crate
**`gnitz-expr`** (dependency: `gnitz-wire` only) so the SQL client runs the
*same code* the engine runs, and **deletes `HavingEval` and `InterpBackend`
outright**. Afterwards one evaluator defines every expression the system
executes.

The `BoundExpr → ExprProgram` compiler is already shared — `OpcodeBackend`
(`gnitz-sql/src/lower.rs:191`) / `compile_filter_program` (`lower.rs:434`)
lower both the CREATE VIEW filter and the ad-hoc HAVING/residual into the same
`gnitz_core::ExprProgram` (`gnitz-core/src/expr.rs:14`). Today only the
*evaluation* forks.

### Why this is worth a crate move

The honest ledger. What the extraction buys that fusing the two *client*
backends into one `Out = Val` tree-walk would not:

1. **It removes a recurring silent-divergence class, not a one-off bug.** Every
   future expression opcode that the client also evaluates must be re-derived
   bit-for-bit by hand. `plans/fn1-numeric-scalar-functions.md` rule 4 requires
   exactly that of its mirror arms — the `ABS(u64col)` unsigned fold and the
   `CAST(u64col AS SMALLINT)` range check — and nothing in the test suite forces
   agreement. The one instance that already happened was caught only by hand:
   the div/mod zero-divisor divergence, fixed in `e5698732`. (`fn1`'s plan still
   describes the *pre-fix* `eval.rs` as current, four commits after it was
   written — evidence of how fast a hand-maintained mirror's *documentation*
   rots, let alone its semantics.)
2. **It closes a permanent feature gap between the view surface and the ad-hoc /
   DML surface.** `plans/fn2-string-expressions.md` rule 6 and
   `plans/fn3-like-pattern-matching.md` commit to `Unsupported` stubs on the
   client side ("no divergence is possible because only one backend evaluates").
   That is a sound decision *for them*, and it means that with two evaluators,
   string functions, `||`, string casts and `LIKE` can **never** appear in an
   ad-hoc HAVING or a DML residual — users hit "works in CREATE VIEW, rejected
   here" forever, one function at a time. With one evaluator they land once and
   reach every site.
3. **The immediate deletion is net-negative lines**: `exec/eval.rs`'s non-test
   body and the ten-item `HavingEval` cluster go away, and with them
   `having_supported`'s plan-time probe and its CREATE-VIEW-advice routing.

What it does **not** buy, stated plainly so the trade is visible: the mirror
arms fn1/fn2/fn3 specify are ~55 lines in total, of which only fn1's ~35 are
semantically load-bearing. This plan is not justified by that line count. It is
justified by (1) and (2), against a real cost: a new crate, ~2.4k relocated
lines, and ~40 items whose visibility widens from `pub(crate)` to `pub`.

**Land this before fn1/fn2/fn3.** All three rewrite `expr/program.rs` +
`expr/batch.rs`, the two files this plan moves.

### Rejected alternatives

- **Moving aggregate finishing server-side** — the master is a single-threaded
  reactor that today never materializes scan data; the finishing tail would
  head-of-line-block every client, and the deletion claim was gross-not-net.
- **Completing `HavingEval` in place / fusing the two client backends** — see
  above. Note the accurate reason: it is *not* that client-side string order is
  hard (`compare_german_strings` is plain lexicographic byte order and
  `ColData::Strings` holds decoded `String`s, so a client string compare is one
  `as_bytes().cmp()`), it is (1) and (2).
- **A concrete client `RegionBatch` + relocating `MemBatch` into the shared
  crate** — forces the client to eagerly concatenate a full region buffer, moves
  storage's central batch-view type out of the storage layer, and needs a third
  schema struct with two adapters.
- **Folding the evaluator into `gnitz-wire` (no new crate)** — `gnitz-wire`
  already owns the expression *bytecode format* (`decode_load_const`,
  `decode_select_operands`, the `EXPR_*` constants), the OPK codec (`pk.rs`) and
  the German-string codec and comparator, and the four route/native-key functions this plan
  relocates are thin dispatch wrappers over `gnitz_wire::{encode_pk_column,
  decode_pk_column, widen_pk_be}` keyed on wire type codes — so "they have no
  wire representation" would be a false argument, and indeed those four **did**
  end up in `gnitz-wire` rather than `gnitz-expr`. The real reason is cohesion and
  blast radius: `gnitz-wire` is the byte-level contract every crate (including
  `capi`/`py`) links, it is already 6185 lines, and adding 2368 lines of runtime
  evaluator plus a resolved-addressing layer makes that contract materially
  harder to read. `gnitz-expr` is a leaf **below the entire engine** (like
  `gnitz-wire` and `foundation`), so an engine `storage`-layer import of it is
  not an inversion of the engine's internal `storage < expr` layering.
- **Keeping `ColumnLocator` engine-side by unresolving `Instr::CopyCol`** (carry
  `src_col: u32` and have engine `plan.rs` call `schema.locate` when building
  `col_moves`; `SchemaFacts` returns a private 2-variant address enum instead).
  This would shrink `gnitz-expr`'s surface a lot, but it buys that by
  **duplicating the resolved-address representation** — two structurally
  identical enums plus a conversion — and by changing `Instr::CopyCol`'s payload,
  which touches `from_wire`, `validate`, `payload_copy_srcs`,
  `sequential_copy_base` and `copy_column`. One shared address type is the truer
  answer; with `RowView` deleted (below) `ColumnLocator`'s methods depend only on
  `BatchView`, which already lives in the shared crate.

### Access traits: one per-row shape, `RowSource`, with two extensions (**DONE**)

Before this landed the engine had two overlapping row-access traits:
`ColumnarSource` (`storage/repr/columnar.rs` — `get_pk_bytes`, `get_weight`,
`get_null_word`, `get_col_ptr`, `blob_slice`; 7 implementors: `Batch`,
`SortedMemBatch`, `MemBatch`, `MappedShard`, `RowRef`, `CursorSource`,
`TestBatch`) and `RowView<'b>` (`get_null_word`, `get_pk_bytes`, `get_col_ptr`;
sole implementor `MemBatch`) — three methods spelled twice.

Rather than add a *third*, the shared per-row core now lives in `gnitz-expr` and
both existing traits extend it:

```rust
// gnitz-expr
pub trait RowSource {                                   // the one per-row shape
    fn get_pk_bytes(&self, row: usize) -> &[u8];
    fn get_null_word(&self, row: usize) -> u64;
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8];
    fn blob(&self) -> &[u8];
}
pub trait BatchView: RowSource {                        // + whole regions
    fn col_data(&self, payload_col: usize, col_size: usize) -> &[u8];
    fn null_bmp(&self) -> &[u8];
}
// gnitz-engine storage
pub(crate) trait ColumnarSource: RowSource { fn get_weight(&self, row: usize) -> i64; }
```

`RowView` is gone. `ColumnarSource` keeps exactly the one method the evaluator
has no use for; `blob_slice` was renamed to `RowSource::blob` at its ~8 trait
call sites (the inherent `MappedShard::blob_slice` / `ReadCursor::blob_slice`
keep their names). All 7 implementors split into a `RowSource` impl plus a
one-method `ColumnarSource` impl, so `MemBatch` writes each per-row forwarder
**once** instead of twice.

The split must be at the per-row/region seam and cannot go the other way:
`MappedShard`, `RowRef` and `CursorSource` can address a cell but have no
contiguous `rows * col_size` region to hand out (a shard column may be a
`ScalarRegion::Constant`), so `ColumnarSource: BatchView` is impossible.

`ColumnLocator`'s five row-reading methods bind to **`&impl RowSource`**, not
`BatchView` — they touch only per-row accessors. That is what lets
`ColumnarSource`-generic code read through a locator: `ops/util.rs`'s
`canonical_group_key` was a verbatim copy of `ColumnLocator::route_key`'s two
arms, existing *only* because its source was a `ReadCursor` and the locator
demanded `BatchView`. It is deleted; `extract_group_key`, `GroupKeyCols::key_row`
and **both arms of `hash_group_col`** now call `loc.route_key(src, row)`, and
`CanonicalKeyArm` collapses to the `bool` predicate its production consumers
always used (`.is_some()`), taking its arm/locator-mismatch `unreachable!` with
it.

**The same rule applies to `IndexKeySpec`.** Its `write_span` / `write_entry` /
`key_bytes` and `index_gather.rs`'s `row_in_index_range` take **`&impl
RowSource`** — their bodies touch only `get_pk_bytes` / `get_null_word` /
`get_col_ptr`, so a `BatchView` bound would re-create exactly the over-constraint
that forced `canonical_group_key` to exist, locking the index-span writer out of
`MappedShard` / `CursorSource` for nothing. `write_span` itself reads through
`loc.is_null` + `loc.bytes` and spells only the *encoder* per variant.

**Every `ColumnarSource` bound whose body never reads the weight is now
`RowSource`** — the comparators (`compare_rows{,_except,_impl,_fixedint_nonnull}`),
`compare_by_group_cols`, `hash_group_col` / `extract_group_key` /
`GroupKeyCols::key_row`, and `Batch::append_row_from_source{,_bytes}` /
`append_row_tail_from_source` / `append_payload_cols`. `ReadCursor::
current_row_source` returns `&impl RowSource` (a cursor's weight comes off the
cursor, never the positioned source). `ColumnarSource` survives only for `run_merge_body` and `RowRef` — the merge
comparator trio (`merge_less` / `merge_same_pk` / `merge_eq_payload`) reads no
weight either, so the first two bind `RowSource` and `merge_eq_payload` needs no
bound on `S` at all (its body touches only the generic `RowCmp`). It is **no
longer re-exported from `crate::storage`**; it is storage-internal.
`GroupKeyExtractor::gather` likewise takes `&impl RowSource`, not `&MemBatch`.

**The per-row accessors are NOT derived from the region ones.** An earlier draft
rewrote every per-row read as `&col_data(c, s)[row*s .. row*s+s]` /
`read_u64_le(null_bmp(), row*8)`. That form adds a row-count load, a second
multiply and a second range check per read. In an optimized build it is free —
the batch reference is `noalias`, so after inlining the region offset the count
and the region range check are loop-invariant and LICM hoists them. At `-O0` it
is not: no LICM — and `make server` builds debug, which is the binary the whole
E2E suite runs. `RowSource` therefore declares the per-row shape as **required**
methods with no defaults, bound by the contract `assert_batchview_consistent`
checks.

**`#[inline(always)]`, not `#[inline]`, on every dispatch-only accessor.**
Verified by codegen: at `opt-level=0` LLVM runs no inliner *except* the
always-inline pass, so `#[inline]` (an `inlinehint`) is a **no-op** there and a
plain forwarder survives as a real call — a locator read then costs two frames
(generic method → trait forwarder → inherent body) for one load. So the whole
chain carries `always`:

- **all seven `ColumnLocator` methods.** Not just the one-liners (`size`,
  `type_code`, `is_null`, `bytes`): `native_le_bytes`, `native_key` and
  `route_key` do no work of their own either — each is a two-arm match whose arms
  are a single `gnitz_wire` call, so the hint left every per-row caller paying two
  frames (`nm` found all three as real symbols, `route_key` twice).
- `MemBatch`'s / `Batch`'s / `SortedMemBatch`'s / `CursorSource`'s / `RowRef`'s
  `RowSource` / `BatchView` / `ColumnarSource` forwarders, **and the branch-free
  inherent accessors those forwarders UFCS-call** — promoting only the forwarder
  leaves the terminus a real call and does half the job.
- `gnitz_wire::promote_opk_column` (per-row on two hot paths; its identity arm is
  a `copy_from_slice` behind a ~38-instruction `-O0` frame otherwise) and
  `gnitz_wire::null_word_get` / `null_word_set`.
- `MappedShard::data()`, the one-liner all five of its accessors funnel through.
  The multi-arm accessors themselves keep the plain hint deliberately: they are
  cursor-merge, not locator, path, and inlining a 3-arm match with a formatted
  `debug_assert!` into every `-O0` call site is not obviously a win.

`gnitz_wire::read_u64_le` (the `get_null_word` terminus) keeps the plain hint, and
this was **measured**, not assumed: its `-O0` body is a `0x1c2`-byte (450) frame
whose bounds/spill ceremony `always` does not shrink, so promoting it grew the
caller 50 → 151 instructions to save ~10. `FixedInt::decode_le_i64` gained a plain
`#[inline]` for a different reason — its sibling constructors are `const fn` (MIR
exported implicitly), it was not, so without the hint it was an out-of-line
cross-crate call even on the per-row SUM path.

This is what makes the traits vanish — monomorphization alone does not. (It also
means the debug build, not release, is where the attribute pays:
`crates/Cargo.toml` sets `[profile.release] lto = true`, so release inlines these
regardless.)

**Two methods keep an explicit batch lifetime.** Not because a call site needs
the outliving — both survive the narrowing to the *reference* lifetime — but
because under elision the `&self` receiver captures the return and
`native_le_bytes`'s `Payload` arm (which returns `self.bytes(mb, row)` as
`&'a [u8]`) stops compiling:
```rust
fn bytes<'b>(&self, mb: &'b impl RowSource, row: usize) -> &'b [u8]
fn native_le_bytes<'a, 'b: 'a>(&self, mb: &'b impl RowSource, row: usize,
                               scratch: &'a mut [u8; 16]) -> &'a [u8]
```
`native_le_bytes` **keeps its `'b: 'a` bound** (a single `<'a>` over
`mb: &'a impl RowSource` + `scratch: &'a mut [u8;16]` also works, since both
`&'a T` and `&'a mut T` are covariant in `'a`; `'b: 'a` is the weaker constraint
and the zero-diff carry-over): its `Payload` arm returns `self.bytes(mb, row)` —
a batch-borrowed slice — so the two regions must stay related. **Both receivers
stay elided-fresh**: binding the return to `&self` would break
`Accumulator::step_from_batch`, which holds the returned `bytes` live across a
`self.acc = self.acc.wrapping_add(…)` mutation (E0502). The other three
(`is_null`, `native_key`, `route_key`) return `bool`/`u128` and take a bare
`&impl RowSource`; the three `IndexKeySpec` methods and `row_in_index_range`
take `&impl BatchView`. No caller passes a temporary — the only inline
`&batch.as_mem_batch()` call is `ScalarFunc::filter_ranges`, which returns `()`.

`RowSource` is therefore the single per-row access shape for the evaluator, for
locator reads, and for storage; `BatchView` adds the region half the vectorized
kernels need. Net: two traits added, two deleted (`RowView` and
`ColumnarSource`'s four duplicated methods).

## The crates

```
gnitz-wire ── gnitz-expr ─┬── gnitz-engine   (MemBatch impls RowSource+BatchView, ColumnarSource extends RowSource; SchemaDescriptor impls SchemaFacts; plan.rs/ScalarFunc)
                          ├── gnitz-core     (ZSetBatchView + ViewBuffers impl RowSource+BatchView; Schema impls SchemaFacts)
                          └── gnitz-sql      (HAVING / residual / SET over the shared core)
```

`gnitz-expr` is a workspace member and a dependency of `gnitz-engine` already;
**steps 4 and 5 add it to `gnitz-core` and `gnitz-sql`** (it is not yet a
dependency of those two). `gnitz-engine`'s production deps stay
`gnitz-wire` + `gnitz-expr` only (`gnitz-core` remains dev-only,
`gnitz-engine/Cargo.toml:26`). `gnitz-wire`'s only dependency is `xxhash-rust`,
so there is no cycle. No `capi`/`py` surface changes.

`gnitz-expr` carries a `[lints]` block matching `gnitz-engine/Cargo.toml:29-33`
(`rust.warnings = "deny"`, `clippy.all = "deny"`); there is no
`[workspace.lints]`, and `make clippy` runs `--workspace --all-targets -- -D
warnings` regardless.

`lib.rs` uses **private `mod`s plus a flat `pub use module::*;`**, the
`gnitz-wire/src/lib.rs:18-52` leaf-crate convention. An earlier draft demanded
`pub mod` on the theory that a private module plus `pub use` would trip
`dead_code` on items the engine does not reach; that is wrong for a library —
`pub use locator::*;` raises those items' *effective* visibility to exported, and
`dead_code` seeds its live set from effective visibility, so nothing `pub` and
root-reachable in a lib is ever dead. Later steps keep the same shape.

### `gnitz-wire` owns the German-string comparator (the one wire change)

The German-string helpers live in `gnitz-wire/src/german_string.rs`, all `pub`:
`encode_german_string`, `try_decode_german_string`, `german_string_content`,
`compare_german_strings` (`#[inline(always)]`), `canonical_short_cell`,
`german_string_cell_ok`, `blob_extent` and `SHORT_STRING_THRESHOLD`.
`relocate_german_string_vec` (engine `schema.rs`) stays in the engine — it needs
`BlobCache`.

**There are no engine re-exports of these** — engine call sites name
`gnitz_wire::X` directly (the sole `crate::schema::` alias left in this cluster
is `SHORT_STRING_THRESHOLD`). Every later chunk that touches one of them
(`expr/program.rs`'s `encode_german_string`, `expr/batch.rs`'s
`compare_german_strings`) therefore already imports it from `gnitz_wire`, and
the `pub(crate) use`-of-a-`cfg(test)`-only-item hazard cannot arise for this
cluster.

Two shapes later chunks must not re-introduce:

- `blob_extent(blob_len, heap_offset: u64, length) -> Option<Range<usize>>` is
  **the one** heap-extent rule — resolved in `u64` space (an `as usize` narrowing
  first could truncate a 64-bit wire offset back into range on a 32-bit target)
  and returning a range so the caller's `&blob[r]` needs no second bounds check.
  `german_string_content` is likewise **the one** content accessor: ordering,
  hashing and relocation all read a cell through it, so a corrupt long header
  degrades to `&[]` identically everywhere. There is no `german_string_tail` —
  `compare_german_strings` is a `u32` prefix compare followed by
  `german_string_content(a, ba).cmp(german_string_content(b, bb))`, measured at
  throughput parity (±3 % across short/long/mixed corpora, inside the run-to-run
  spread).
- `german_string_cell_ok(cell, blob)` is the **canonical-form** predicate, not a
  bare extent check: a short cell's pad bytes `[4+len, 16)` must be zero and a
  long cell's inline prefix must match its first four heap bytes. Both halves are
  compare-visible but invisible to `german_string_content`, so a skewed cell
  hashes equal while ordering unequal — one Z-set element whose weight splits
  across two rows consolidation never merges. It guards the ingest trust boundary
  (`validate_string_heap_extents`), and `relocate_german_string_vec` re-establishes
  it constructively via `canonical_short_cell` on every merge/scatter/map row.

### The two traits

`RowSource` + `BatchView` are **shipped** in `gnitz-expr`'s `view` module — six
required methods across the pair, no defaults:

```rust
pub trait RowSource {
    // Per-row accessors — address one cell directly (see the -O0 reasoning above).
    fn get_pk_bytes(&self, row: usize) -> &[u8];
    fn get_null_word(&self, row: usize) -> u64;
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8];
    fn blob(&self) -> &[u8];
}

pub trait BatchView: RowSource {
    // Region accessors — the WHOLE column/region, never a morsel slice: the
    // kernels index absolutely (`eval_str_cmp` reads `col_a[row*16 .. row*16+16]`
    // with `row = morsel_start + i`; the int/null kernels likewise). Implementor
    // preconditions: `col_data(pi, sz).len() == rows * sz`,
    // `null_bmp().len() == rows * 8`.
    fn col_data(&self, payload_col: usize, col_size: usize) -> &[u8];
    fn null_bmp(&self) -> &[u8];
}

/// The contract binding the two shapes, checked for `rows` rows over the given
/// `(payload_col, col_size)` pairs:
///   `get_col_ptr(row, pi, sz) == &col_data(pi, sz)[row*sz .. row*sz + sz]`
///   `get_null_word(row)       == gnitz_wire::read_u64_le(null_bmp(), row*8)`
/// A normal `pub fn`, not `#[cfg(test)]`, so cross-crate tests reach it.
pub fn assert_batchview_consistent<B: BatchView>(v: &B, rows: usize, cols: &[(usize, usize)]);
```

Lifetimes are tied to `&self`, NOT decoupled — a client adapter owns the buffers
it materializes and can only lend them for `&self`.

**Step 4's `ZSetBatchView` must call `gnitz_expr::assert_batchview_consistent`.**
It is the implementor that can actually violate the contract: its `col_data` is
not a flat-region slice but a `pi → ci` map through `bufs.pi_to_ci[pi]` plus a
`ColData::{Fixed, Strings, Bytes, U128s}` match, so a `get_col_ptr` that forgets
the `str_cols[pi]` redirect returns wrong DML/HAVING results silently across the
`gnitz-capi` C ABI. For `MemBatch` the same property is near-tautological (both
accessors derive the same `offsets[REG_PAYLOAD_START + pi]` address) and is
checked in `storage/repr/merge.rs`'s test module — one rule, one assertion, every
implementor.

`SchemaFacts` landed in **step 3a**, alongside the seven functions that read it —
its only consumers. Defining it earlier would have put a duplicate API surface on
`SchemaDescriptor` that no call site could reach. It lives in
`gnitz-expr/src/schema_facts.rs`; read the trait there rather than a copy here.
Three decisions it carries:

- **`col_type_code` returns `u8`, not `TypeCode`** — it feeds `reg_tc: [u8;
  MAX_REGS]`, is compared against `type_code::U64`, and is handed to
  `gnitz_wire::is_fixed_int` / `is_float` / `is_german_string`.
- **Every impl must forward** to the type's own fact — its existing inherent
  method, or the column-table field where that *is* the fact — rather than
  reimplement; written UFCS wherever an inherent method exists, since most
  collide by name and Rust prefers the inherent one in receiver-dot position.
- **Six required methods plus two provided** (`locate`, `payload_col_idx`,
  `num_payload_cols`, `num_columns`, `col_type_code`, `col_nullable`; provided
  `payload_slot` and `is_pk_col`). `payload_slot(ci) -> Option<u8>` destructures
  `locate` and `is_pk_col` is `payload_slot(ci).is_none()`, so the locator
  variant *is* the PK marker and an implementor cannot state the two and have
  them disagree. `col_type_code` stays required — deriving it from `locate` is
  not free (`SchemaDescriptor::locate` runs a release-active bound assert and an
  O(pk_count) `pk_byte_offset` walk).

  **`PAYLOAD_MAPPING_PK_SENTINEL` is deliberately NOT in this contract.** An
  earlier shape had a required `payload_mapping_byte(ci) -> u8` returning the
  sentinel for a PK column, with `is_pk_col` defaulted to testing it. That
  exported a poisoned byte across the crate boundary: the five payload-only
  `resolve` arms each needed a `debug_assert!(!is_pk_col(..))` beside it, and the
  step-4 client impl would have had to *synthesise* a sentinel by branching
  around its own `payload_idx` assert — writing a value nothing wants. With
  `Option<u8>` "this column has no payload slot" must be handled, which is the
  rule `SchemaDescriptor::try_payload_idx` already states for the engine. The
  sentinel stays what it is: the in-memory encoding of the `payload_mapping`
  table, read only through `try_payload_idx` / `is_pk_col`, plus `resolve`'s one
  `unwrap_or` fallback (so an *unvalidated* program — tests only; both
  `ScalarFunc` constructors validate first — still trips the kernels' own
  sentinel assertions instead of silently addressing payload slot 0).

**Reached through `&dyn SchemaFacts`, not a `<S: SchemaFacts>` generic.** The
whole trait runs once per program compile, never per row — measured: every call
site of every method is inside `resolve` / `validate` / the three `check_*`
helpers / `is_strictly_non_nullable`. Static dispatch therefore buys nothing, and
it cost a placeholder implementor: a `NoSchema` unit struct with eight
`unreachable!()` bodies existed solely to give `assembled`'s schema-free
`validate(None, None)` a concrete `S`, and its instantiation duplicated the whole
validator (measured: −2 580 instructions, −10.2 KB `.text` on the debug binary
when it went away). With `Option<&dyn SchemaFacts>` the call sites are
byte-identical — every one is `Some(&concrete)`, `None`, or `&concrete`, and
unsizing coercion handles all three. There is no `NoSchema`.

`BatchView`/`RowSource` stay generic: those *are* per-row, and monomorphizing
them in the engine's own codegen unit is what keeps the kernels identical.

### `gnitz-expr` modules (**DONE**)

Signatures only — the bodies are the engine's, moved verbatim.

- **`view.rs`** — `RowSource`, `BatchView`, `assert_batchview_consistent`.
- **`locator.rs`** — `ColumnLocator` (`Pk { byte_off, size, type_code }` /
  `Payload { slot, size, type_code }`) with `size` / `type_code` / `is_null` /
  `bytes` / `native_le_bytes` / `native_key` / `route_key`, all
  `#[inline(always)]`; `PAYLOAD_MAPPING_PK_SENTINEL`; the
  `size_of::<ColumnLocator>() <= 8` static assert.
- **`schema_facts.rs`** — `SchemaFacts`, `assert_schema_facts_consistent`.
- **`program.rs`** — `LogicalProgram` / `LogicalInstr` (wire-mirroring form,
  logical column indices) and the crate-private `ResolvedProgram` / `Instr`
  (resolved payload/PK indices) that `resolve_program` lowers into, plus the
  crate-private `validate` / `validate_predicate` and the free `classify_registers`
  / `and_chain_mask` / `is_strictly_non_nullable` analyses `resolve_program` runs
  (free functions over `&[Instr]`, so `ResolvedProgram` is built once, fully
  resolved, with no placeholder mask fields); `CmpOp`, `StrOp`, `ColKind`,
  `ExprValidateErr`. `classify_registers` is the **one** exhaustive walk over
  register operands — it returns the per-register use counts `and_chain_mask`
  needs, so an opcode that gains an operand cannot be classified correctly and
  counted wrong. `Instr::CopyCol` carries a `ColumnLocator`, which is why
  that type sits in this crate; `eval_batch` never reads one (`CopyCol`/`Emit`
  are no-ops there — engine `expr.rs` materializes them columnar-side).
- **`batch.rs`** — the morsel kernel: `eval_batch`, `EvalScratch` and its
  `ensure_capacity`/`grow`, `MORSEL`, `NULL_WORDS_PER_REG`, `for_each_null_row`,
  and the per-opcode helpers. All crate-private.
- **`eval.rs`** — the public driving surface: the three validating
  `LogicalProgram::resolve_*` constructors and `Evaluator` (`eval_row` /
  `filter` / `eval_morsels` / `copy_moves` / `emit_targets`) and `MorselOut`. The three drive methods carry
  their own bodies — there are no free `filter_batch` / `eval_row` twins for
  them to delegate to. Crate-private: `read_reg_row0` and `scan_filter_bits`.
- **`test_support.rs`** (`#[cfg(test)]`) — `TestView`, `TestSchema`,
  `make_int_view`, `make_n_col_view`, `make_int_row`, `schema_pk_ints`,
  `locator_fixture`, `float_to_bits`/`bits_to_float`.
- **`lib.rs`** — private `mod`s + flat `pub use module::*;`, and the crate-root
  inlining doctrine.

### The public surface (**landed** in 3b)

Everything steps 4/5/6 may name, and nothing else. The resolved form and the
register file are gone from it — a caller outside the crate holds an
[`Evaluator`], never a `(ResolvedProgram, EvalScratch)` pair.

- **Addressing** — `ColumnLocator` (+ its 6 variant fields) and its 7 methods
  `size` / `type_code` / `is_null` / `bytes` / `native_le_bytes` / `native_key` /
  `route_key`; `PAYLOAD_MAPPING_PK_SENTINEL`.
- **Access traits** — `RowSource` (4 methods), `BatchView` (2), plus
  `assert_batchview_consistent`.
- **Schema contract** — `SchemaFacts` (7 required + 1 provided), plus
  `assert_schema_facts_consistent`.
- **Program construction** — `LogicalProgram` with `new`, `copy_cols`,
  `from_wire`, `payload_copy_srcs`, `sequential_copy_base`, and the three
  **validating** resolvers `resolve_filter(schema)` / `resolve_map(in, out)` /
  `resolve_scalar(schema)`, each returning `Result<Evaluator, ExprValidateErr>`;
  `LogicalInstr`, `CmpOp`, `StrOp`, `ColKind`, `ExprValidateErr`.
- **Evaluation** — `Evaluator` (`eval_row`, `filter`, `eval_morsels`,
  `copy_moves`, `emit_targets`) and `MorselOut` (`rows`, `reg_values`,
  `for_each_null_row`). `eval_morsels`' callback is `f(rel_start, &MorselOut)` —
  the morsel's row count is `out.rows()`, not a second parameter. There is no
  `MorselOut::value` / `is_null`: a bit_only register is never unpacked into
  `regs` on the nullable arm, so the only correct single-row read is the
  crate-private `read_reg_row0` that `eval_row` already goes through.

`validate` and `validate_predicate` are **`pub(crate)`**, not public: every
consumer reaches them through the resolver that pairs each with the resolution it
guards. There is no unvalidated public `resolve` and no `is_filter: bool`
parameter — resolution bakes in payload slots, PK byte offsets, type codes and
the nullability verdict, so "this program was checked against the schema it runs
on" has to be a property of the type rather than a convention each consuming
crate remembers. The three resolvers differ in which rules apply, which is also
what fixes the classification each resolves under:

| resolver | checks | classification |
|---|---|---|
| `resolve_filter(schema)` | `validate(Some, None)` + must own a result register | `result_reg` stays bit_only-eligible |
| `resolve_map(in, out)` | `validate(Some, Some)` — every output slot covered once | `result_reg` demoted |
| `resolve_scalar(schema)` | `validate(Some, None)` | `result_reg` demoted |

Every test program in the crate validates, so there is no `resolve_unchecked`.

Deliberately **private to the crate**: `ResolvedProgram` and every field of it
(`instrs`, `const_cells`, `const_blob`, `int_sets`, `no_nulls`, `bit_only_mask`,
`bool_pack_mask`, `chain_trigger_mask`, …), the resolved `Instr` enum,
`classify_registers`, `is_bit_only`, `needs_bool_pack`, `MAX_REGS`,
`EvalScratch` and all its fields, `ensure_capacity`, `MORSEL`,
`NULL_WORDS_PER_REG`, `eval_batch`, `read_reg_row0`. In
particular `chain_trigger_mask` is a `resolve`-computed invariant and is no
longer **assignable** from outside the crate that owns it; the AND-chain A/B
bench that writes it lives in `batch/tests.rs`.

Deliberately **private to `batch.rs`**: the `unsafe` split-borrow helpers
`reg_mut`, `reg3`, `null_words3`, `reg4`, `clear_null_reg`. Those hand out
aliasing split borrows whose soundness rests on an SSA property enforced only by
a `debug_assert!`. No production caller outside `batch.rs` ever existed, and the
two tests that called `reg3`/`null_words3` as inherent methods (so no `use` line
named them) live in `batch/tests.rs`, a child module that reaches them without
any widening.

## Engine side

**Stays in `gnitz-engine`:**
- `MemBatch` (`storage/repr/merge.rs`) — stays in storage, with `impl RowSource`
  + `impl BatchView` + a one-method `impl ColumnarSource`: `col_data`,
  `null_bmp`, `get_pk_bytes` already exist as inherent `&'a`-returning methods
  and coerce to the trait's `&self` lifetime; `fn blob(&self) -> &[u8] {
  self.blob }` (an inherent field / trait method name pair is legal, and the
  inherent methods shadow the trait's for direct calls). Every forwarder is
  `#[inline(always)]`. `impl RowView for MemBatch` is deleted, and
  `ColumnarSource`'s four per-row methods are no longer restated here — they come
  from `RowSource`.
- `REG_*` / `MAX_BATCH_REGIONS`, `Batch`, `Batch::as_mem_batch`.
- `impl SchemaFacts for SchemaDescriptor` — six forwarders: `locate`,
  `payload_col_idx`, `num_payload_cols`, `num_columns` (all UFCS to the inherent
  method) plus `self.columns[ci].type_code` / `self.columns[ci].nullable != 0`,
  where the field *is* the fact and there is no inherent method to forward to.
  No `#[inline]` on any of them — every caller reaches them through
  `&dyn SchemaFacts`, so the hint cannot fire through the vtable. The inherent
  `SchemaDescriptor::payload_mapping_byte` is **deleted**: with the sentinel out
  of the trait its last caller was the forwarder itself, and `expr.rs`'s
  `compute_blob_passthrough` — its one other user — now compares whole locators
  (`cm.src == in_schema.locate(ci)`), which is what a `ColMove`'s source is.
- `compute_mappings` (`schema.rs:229`) — retarget its one sentinel reference to
  `gnitz_expr::PAYLOAD_MAPPING_PK_SENTINEL`.
- `IndexKeySpec` — its three row-reading methods retarget from `RowView` to
  `&impl RowSource` (not `BatchView`: their bodies touch only per-row accessors,
  and the looser bound is what keeps the index-span writer usable over
  `MappedShard` / `CursorSource`).
- `expr.rs` in full: `ScalarFunc` (`from_predicate`, `from_map`,
  `filter_ranges`, `map_out_schema`, `evaluate_map_batch`, `append_map_ranges`),
  `MapPlan` (`map_ranges_into`, `map_rows_into`), `copy_column`, `NullPerm`, and
  its `BlobCache` / `BlobCacheGuard` / `relocate_german_string_vec` uses
  (map-materialization only — the filter path never touches the blob cache).
  `Repr` is `Predicate(Box<Evaluator>) | Map(Box<MapPlan>)` — both boxed, so the
  enum stays pointer-sized whichever half is larger — and the variant is checked
  in exactly two places: `filter_ranges`'s `let Repr::Predicate(ev) = …` and the
  private `ScalarFunc::map() -> &MapPlan`. The map driver chain
  (`map_ranges_into` → `map_rows_into`) lives on `MapPlan` and reads its fields
  directly, so it re-checks nothing.
  The two production filter callers (`catalog/scan_spec.rs`, `ops/linear.rs`) are
  untouched.
- **Re-exports, one rule, stated as a boundary rather than a list: a *schema
  fact* comes through `crate::schema::X` at every engine call site; a *byte
  primitive* never does.** What a column's type is, how many of them there can
  be, how a key is shaped — schema facts, so `crate::schema` re-exports
  `type_code`, `TypeCode`, `MAX_COLUMNS`, `MAX_PK_BYTES`, `MAX_PK_COLUMNS`,
  `ReduceOutKey` and `ColumnLocator` — and **every** engine call site of each
  names `crate::schema::X`, with no surviving `gnitz_wire::` spelling of any of
  them. `PAYLOAD_MAPPING_PK_SENTINEL` is deliberately *not* re-exported: its only
  engine readers are `payload_mapping`'s own encode/decode inside `schema.rs`, so
  it is a plain `use` there. How a value is *encoded or decoded* is a byte
  primitive and is
  named `gnitz_wire::X` directly, including inside `schema.rs` itself:
  `read_{signed,unsigned}{,_exact}`, the four `*_route_key` / `*_native_key`
  derivations, the German-string cluster, the null-bitmap accessors, the aligned
  LE primitives and the type predicates (`is_fixed_int`, `is_routable_int`,
  `is_signed_int`, `SHORT_STRING_THRESHOLD`). `SchemaFacts` is likewise not
  re-exported — the one impl site names `gnitz_expr::SchemaFacts`.

  The boundary matters because the *list* form kept drifting: before it was
  drawn, `MAX_PK_BYTES` was live under both spellings across 20 files and
  `MAX_PK_COLUMNS` under both across 6 — with `runtime/protocol/wire.rs` naming
  `crate::schema::MAX_COLUMNS` and `gnitz_wire::MAX_PK_COLUMNS` five lines
  apart, and `ops/util.rs` labelling match arms from `crate::schema::type_code`
  while their bodies called `gnitz_wire::read_unsigned`. `foundation::codec` is
  gone entirely (above). **`crate::storage` likewise no longer re-exports the
  `schema::key` OPK cluster** (`PkBuf`, `compare_pk_bytes`,
  `compare_pk_ordering`, `opk_key`, `pack_pk_be`, `pk_bytes_eq`,
  `partition_for_key`, `partition_for_pk_bytes`): that facade existed *"so its
  call sites spell them unchanged"* — churn-avoidance ownership, the exact
  justification this plan rejected three times — and it left the §1/§6
  byte-order rule spelled two ways in adjacent lines of `ops/reduce/sort.rs` and
  `catalog/scan_spec.rs`. Every caller now names `crate::schema::key::X`.
  Earlier drafts split this rule by how many call sites would churn rather than
  by ownership: one routed the four key derivations through `schema`, the next
  re-exported three of the four through `foundation::codec`, and 3a left the
  aligned LE primitives re-exported there — visibly, with the expr batch and
  plan files naming one `read_u64_le` two different ways.
- **`expr` is one file**, `expr.rs`, declaring `PkFill` / `ScalarFunc` directly
  plus `#[cfg(test)] mod tests;`. Once `program.rs` and `batch.rs` left, the
  `expr/mod.rs` + `expr/plan.rs` pair was a shim re-exporting two items out of
  its sole child, so it is gone; `crate::expr::{PkFill, ScalarFunc}` is unchanged
  at every call site. It re-exports **nothing** from `gnitz-expr`: every site that named
  `crate::expr::{LogicalProgram, LogicalInstr, CmpOp, StrOp, ExprValidateErr}`
  names `gnitz_expr::` instead. This follows `RowSource`/`BatchView` — moved into
  the same crate, in this same effort, consumed and produced by the module they
  left, and deliberately not re-exported — rather than `ColumnLocator`, which
  `crate::schema` re-exports because a locator *is* a schema fact that
  `SchemaDescriptor::locate` produces. `LogicalProgram` is produced by
  `query/compiler` and consumed by `expr.rs`; `expr` is its home in neither
  direction, and a facade would leave peers of one crate spelled two ways on
  adjacent lines — the drift failure this plan deleted the `crate::storage` OPK
  facade for.

### The expr test suite moved with the code (**DONE** in 3b)

An earlier draft kept it in `gnitz-engine`, on the reading that `plan_tests.rs`
held "byte-identical private copies of `make_schema` and `make_int_batch`". It
has its own `make_schema` but *imported* `make_int_batch` from `tests/mod.rs`,
and `program_tests.rs` named no engine type at all beyond the schema/batch
fixtures. Once `Evaluator` deleted `ScalarFunc::run_filter` and
`evaluate_predicate` — the only things pinning 13 of the 92 tests to the engine —
nothing was left to pin any of them.

All 92 moved into `gnitz-expr` as per-module `tests` submodules (`program/`,
`batch/`, `eval/`, `locator/`, `schema_facts/`, `view/`), each a *descendant* of
the module it tests, so private items are reachable. Two items are `pub(crate)`
across that seam rather than for a production reader — `Evaluator::prog` and
`read_reg_row0`, both reached from `batch/tests.rs` and `program/tests.rs`, which
descend from `batch`/`program` and not from `eval`.

`test_support.rs` holds the one fixture family: `TestView` (owned-buffer
`BatchView`), `TestSchema` (`SchemaFacts` over a `(type_code, nullable)` table +
PK list), `make_int_view`, `make_n_col_view`, `make_int_row`, `schema_pk_ints`,
`locator_fixture`, `float_to_bits`/`bits_to_float` (spelled as the kernel's own
`encode_f64`/`decode_f64`). There is no second family — in particular
`assert_schema_facts_consistent` runs against **`TestSchema` itself**, the
implementor whose payload slots and OPK offsets decide every kernel test's
addressing, with a five-forwarder newtype supplying the off-by-one
`payload_col_idx` that proves the harness is not vacuous.
`gnitz-engine`'s `expr/tests.rs` (7 `ScalarFunc`/`PkFill` tests plus the
`from_predicate` → `filter_ranges` pin) owns the only engine-side `make_schema`
and `make_int_batch`, over the physical `Batch` those tests exist to exercise.

`cargo test -p gnitz-expr` therefore runs the real suite (96 + 2 `#[ignore]`d
benches), not a smoke test.

**Why the hot path is unchanged.** `eval_batch<MemBatch>`,
`Evaluator::filter<MemBatch, _>` and `eval_morsels<MemBatch, _>` are the only
instantiations the server binary compiles, and generics monomorphize **in the
calling crate** — the engine's own codegen unit — so the vectorized kernel bodies
are produced exactly as today, with or without LTO. Verified by `objdump` on the
`-O0` binary across the move: `eval_batch`'s body came out at 12756 instructions
and 693 calls both before and after, with no new call target. The `BatchView`
accessors are `#[inline(always)]` and resolve to `MemBatch`'s inherent bodies.

## Client side

### `gnitz_core::Schema` impls `SchemaFacts`

From existing methods on `gnitz-core/src/protocol/types.rs`: `is_pk_col`
(`:138`), `num_columns` (`:87`), `num_payload_cols` (`:132`), `payload_idx`
(`:168`), `pk_byte_offset` (`:145`), `payload_columns` (`:178`),
`columns[ci].type_code` / `.is_nullable`.
- `locate(ci)`: mirrors engine `locate` (`schema.rs:656`) exactly — `size =
  columns[ci].type_code.wire_stride() as u8` (engine's `SchemaColumn.size` is
  `wire_stride(tc) as u8`, set in `SchemaColumn::new`), then
  `ColumnLocator::Pk { byte_off: pk_byte_offset(ci) as u8, size, type_code: tc as
  u8 }` for a PK column (client `pk_byte_offset` sums PK-list-order
  `wire_stride`s — the same walk as engine `pk_byte_offset`, `schema.rs:641`),
  else `ColumnLocator::Payload { slot: payload_idx(ci) as u8, size, type_code:
  tc as u8 }`.
- `col_type_code(ci)`: `self.columns[ci].type_code as u8` — the client's field is
  a `TypeCode` enum, the trait returns `u8`.
- Neither `payload_slot` nor `is_pk_col` is written: the trait provides both off
  `locate`, so the client impl is `locate` + the five plain accessors and never
  names `PAYLOAD_MAPPING_PK_SENTINEL`. (An earlier shape required a
  `payload_mapping_byte(ci) -> u8`, which would have forced this impl to branch
  around `payload_idx`'s own non-PK `debug_assert!` to synthesise a sentinel.)
- `payload_col_idx(pi)` — **`gnitz_core::Schema` has no such method, but the
  computation already exists privately**: `BatchAppender::new`
  (`protocol/types.rs:990`) builds `payload_to_ci: Vec<usize> =
  (0..num_columns).filter(|ci| !is_pk_col(ci)).collect()`. Promote that to a
  `Schema` method and have `BatchAppender` index it, rather than writing the walk
  a second time; as a method it is
  `self.payload_columns().nth(pi).map(|(_, ci, _)| ci).expect("payload_col_idx:
  pi out of range")` (`payload_columns` yields `(pi, ci, &ColumnDef)`). This is
  load-bearing and silent if wrong: it is the only consumer inside
  `is_strictly_non_nullable` (`program.rs:1315-1317`), i.e. it decides
  `no_nulls`. The obvious wrong implementation (`pi` itself) is correct only when
  the PK sits at column 0; for any interspersed or compound PK it reads another
  column's nullability → null bits silently ignored → wrong rows kept.

**`locate` is the one genuinely new client method, and three partial copies of it
already exist** — delete them as part of this step rather than leaving a fourth
spelling: `gnitz-sql`'s `SortKey` (`exec/order.rs:35-63` — a struct-shaped
`ColumnLocator`: PK-vs-payload address, width, type, null bit),
`pk_col_window` / `pk_col_window_at` (`exec/batch.rs:53-78` — the PK-region
address from `pk_byte_offset` + `wire_stride`), and `gnitz-py`'s `ColLayout`
(`lib.rs:901-937` — a struct-of-arrays `Vec<ColumnLocator>`). None is fixable
today because none of those crates depends on `gnitz-expr` yet; steps 4/5 are
what make them reachable.

Keep `Schema::payload_idx`'s live computation (`col_idx − count(pk < col_idx)`) —
it already equals `compute_mappings`; pointing it at the shared function would
force a `[u8; MAX_COLUMNS]` materialization on a `Vec`-based client.

### `ViewBuffers` + `ZSetBatchView`: the one client adapter (in `gnitz-core`)

A `ZSetBatch` (`protocol/types.rs:744`) is already region-shaped for the fixed
and null parts. The owned scratch is split out of the borrowing view so a loop
over short-lived batches allocates nothing after warmup (a `reset(&mut self,
batch: &'a ZSetBatch)` cannot work: `'a` is fixed at construction and sits in an
invariant position behind `&mut self`, while the ON CONFLICT `existing` is an
owned `ZSetBatch` created *inside* the loop body — `E0597`).

Both types go in `protocol/types.rs`, next to `ZSetBatch`/`ColData` (the `*_into`
encode helpers stay in `wal_block.rs`), and are added to `protocol/mod.rs`'s
`pub use types::{…}` list and `lib.rs`'s `pub use protocol::{…}` list — otherwise
`gnitz-sql` cannot name them.

```rust
#[derive(Default)]
pub struct ViewBuffers {
    /// Indexed by payload slot: German-encoded 16-byte cells of a String/Blob
    /// column (`rows * 16` bytes); empty for every other column. Sized
    /// `schema.num_payload_cols()`.
    str_cols: Vec<Vec<u8>>,
    /// Payload slot → schema column index, so `col_data` is O(1) instead of an
    /// O(cols) `payload_columns()` scan per instruction per morsel.
    pi_to_ci: Vec<usize>,
    blob: Vec<u8>,
    nulls_le: Vec<u8>,
    pk_region: Vec<u8>,
}
impl ViewBuffers {
    /// Clear and refill from `batch`; every `Vec` keeps its capacity.
    /// Starts with `debug_assert_eq!(batch.nulls.len(), batch.len())`.
    pub fn fill(&mut self, batch: &ZSetBatch, schema: &Schema) { … }
}
pub struct ZSetBatchView<'a> { batch: &'a ZSetBatch, schema: &'a Schema, bufs: &'a ViewBuffers }
impl<'a> ZSetBatchView<'a> {
    pub fn new(batch: &'a ZSetBatch, schema: &'a Schema, bufs: &'a ViewBuffers) -> Self { … }
}
impl BatchView for ZSetBatchView<'_> { … }
```

Usage over one batch: `let mut bufs = ViewBuffers::default(); bufs.fill(&batch,
schema); let view = ZSetBatchView::new(&batch, schema, &bufs);`. Usage in a loop
over changing batches: hoist `bufs`, and per iteration `bufs.fill(b, schema); let
view = ZSetBatchView::new(b, schema, &bufs);`.

`fill` builds:
- `pi_to_ci`: from `schema.payload_columns()` (which yields `(pi, ci,
  &ColumnDef)`, `types.rs:178-184`).
- `nulls_le`: `batch.nulls` (`Vec<u64>`, **one word per row** — `is_null` indexes
  `self.nulls[row]`, `types.rs:800`) flattened to LE bytes. Byte-identical to the
  engine's `null_bmp()` region.
- `pk_region`: `build_pk_region_into(&mut self.pk_region, &batch.pks,
  schema.pk_stride(), schema)` — the OPK bytes for every row
  (`Schema::pk_stride`, `types.rs:111`). Required because `PkColumn`
  (`types.rs:329`) holds *decoded native* values (`U64s`/`U128s`) or on-wire LE
  bytes, never OPK — and a SET RHS or residual may legitimately reference the PK
  column (`SET x = pk + 1`), which lowers to `Instr::LoadPk` and
  `decode_pk_column`/`widen_pk_be` (`batch.rs:620-641`).
- `str_cols[pi]` for every `ColData::Strings` (`types.rs:615`) and
  `ColData::Bytes` (`:616`) column, threading **one** growing `blob` across all
  of them so heap offsets are absolute and non-overlapping — the discipline
  `encode_wal_block` (`wal_block.rs:176`) already uses.

**`build_pk_region` and `encode_german_col` need `*_into` variants.** Both are
module-private `fn`s in `protocol/wal_block.rs` (`:15`, `:87`) that **return a
freshly allocated `Vec`** (`vec![0u8; …]` at `:20`;
`Vec::with_capacity(count*16)` at `:89`), so calling them from `fill` would
allocate every iteration and defeat the whole design. Refactor each into a
`pub(crate) fn …_into(dst: &mut Vec<u8>, …)` that clears and appends, with the
existing signature kept as a thin wrapper for `encode_wal_block`.
`encode_german_col`'s cells parameter is `impl Iterator<Item = Option<&'a [u8]>>`
(`:87-93`), so `fill` passes `v.iter().map(|o| o.as_deref().map(str::as_bytes))`
for `Strings` and `v.iter().map(|o| o.as_deref())` for `Bytes`.

`col_data(pi, size)` maps `pi → ci` via `bufs.pi_to_ci[pi]`, then matches
`batch.columns[ci]`:
- `ColData::Fixed` (`:614`) → its bytes in place (`rows * wire_stride`,
  native-LE). Zero-copy.
- `ColData::Strings` / `ColData::Bytes` → `&bufs.str_cols[pi]` (`size` is 16).
- `ColData::U128s` (`:620`) → `unreachable!("16-byte column in an expression
  register")`. Unreachable by construction *and* guarded: `OpcodeBackend::col_ref`
  rejects `is_wide_int` before anything else (`lower.rs:207-215`) and is the only
  producer of `LoadColInt`/`LoadColFloat`; `try_compile_string_cmp` gates on
  `is_german_string()` (`lower.rs:136,164`); the IN-list OR-chain and `case`
  recurse through `binop` → `col_ref` (pinned by
  `in_list_wide_int_operand_rejects`, `lower.rs:868-882`); `IS NULL` on a
  nullable U128 *does* reach the VM but `eval_is_null` reads only `null_bmp()`
  (`batch.rs:210-215`) and `null_test` (`lower.rs:388`) never routes through
  `col_ref`; `IS NULL` on a *PK* column cannot arise because `bind_null_test`
  const-folds null tests on non-nullable columns to `LitInt(0/1)`
  (`bind/structural.rs:496-506`); `CopyCol`/`Emit` are `eval_batch` no-ops and
  `OpcodeBackend` never emits `CopyCol`. **The guard**: every client site calls
  `resolve_scalar` / `resolve_filter`, whose validation returns
  `ColTooWideForRegister`,
  so a future reject-list drift in `lower.rs` (a file fn1/fn2/fn3 all edit)
  surfaces as a typed `Unsupported`, not a panic across the `gnitz-capi` C ABI
  under `panic = "abort"`.

`fill` materializes every string column eagerly rather than only the referenced
ones: `ResolvedProgram` exposes no referenced-column set, and both users are cold
paths.

### Compiling a client program: the shared lines

```rust
// in gnitz-sql, next to the other lowering helpers
fn expr_unsupported(e: gnitz_expr::ExprValidateErr) -> GnitzSqlError {
    match e {
        gnitz_expr::ExprValidateErr::TooManyRegs(n) => GnitzSqlError::Unsupported(format!(
            "expression needs {n} registers; the limit is 64 — split the predicate"
        )),
        other => GnitzSqlError::Unsupported(format!("expression cannot be compiled: {other:?}")),
    }
}

let Some(p) = compile_filter_program(pred, &schema.columns)? else { /* statically true */ };
let ev = LogicalProgram::from_wire(&p.code, p.num_regs, p.result_reg, p.const_strings)
    .map_err(expr_unsupported)?
    // `resolve_scalar` for HAVING / a SET RHS, `resolve_filter` for a residual.
    .resolve_scalar(schema)
    .map_err(expr_unsupported)?;
```
The `ExprProgram` fields (`expr.rs:14`: `code: Vec<u32>`, `num_regs: u32`,
`result_reg: u32`, `const_strings: Vec<Vec<u8>>`) are byte-for-byte
`from_wire`'s parameters (`program.rs:470`) — no round trip. The resolver does
the schema-aware validation, so there is no separate `validate` call; both
failure modes map through `expr_unsupported`.

**The 64-register cap is an accepted narrowing.** `from_wire` runs `validate`
internally via `assembled`, which rejects `num_regs > MAX_REGS = 64`
(`program.rs:26,868`). `ExprBuilder` never reuses a register
(`alloc_reg`, `gnitz-core/src/expr.rs:52-56`), so `WHERE a=1 AND b=2 AND …` with
*k* equality conjuncts costs `4k − 1` registers: **16 conjuncts fit (63), 17 do
not (67)**. Same for an IN-list that misses the `IntInSet` fast path — string IN,
float IN, or non-literal items (`lower.rs:293-313`) — which the code already
documents as failing at N=17 (`lower.rs:748-750`). Today's tree-walks are
unbounded, so this is a real regression; it is exactly the cap the CREATE VIEW
path already has, i.e. it is parity, and the message names the limit.

### HAVING (`gnitz-sql/src/exec/agg_finish.rs`, `dml/select.rs`)

The bound HAVING indexes into the SyntheticFold `partial_schema` layout
`[_group_pk (U128, hidden, PK at ci 0) | group cols (visible payload) | agg
partial cols]`. The ad-hoc path **unconditionally** builds this
(`dml/select.rs:443-447`, `synthetic_fold_cols(&schema,
&layout.group_col_indices, &layout.agg_specs, /* aggs_nullable = */ true)` +
`Schema::from_parts(cols, vec![0])`; `out_key: ReduceOutKey::SyntheticFold`
hard-coded at `select.rs:460`). Three consequences the design uses:
- **No HAVING can reference the PK region.** `group_col_reduce_pos` returns
  `1 + position(...)` for `SyntheticFold` (`agg.rs:196`) and
  `synthetic_fold_cols` clones group columns in as *payload* — even when the
  GROUP BY key is the table's own PK.
- `payload_idx(ci) == ci − 1` throughout (`pk_cols == [0]`).
- `aggs_nullable = true` (`agg.rs:224`) means the agg partial columns are
  declared nullable, so `no_nulls` cannot fire on them and a set null bit is
  honoured.

**Compile at plan time, in `select.rs`, exactly where `having_supported` was.**
Change `AggFinish`'s `having: Option<&'a BoundExpr>` (`agg_finish.rs:43`) to
`having: Option<&'a Evaluator>` — `Evaluator` has no lifetime parameter, so
`AggFinish<'a>`'s other `&'a` fields are unaffected, and `agg_finish.rs:178` is
the only reader of `spec.having` repo-wide. In `select.rs` — after
`bind_having_expr`, replacing the `having_supported` gate and its
CREATE-VIEW-advice rejection (`:471-477`) — bind a local
`let having_ev: Option<Evaluator>` where `bound_having` is bound today, produced
by the shared lines above against `&partial_schema`, and pass
`having_ev.as_ref()` at the construction site (`:522-528`).
This keeps the rejection **pre-dispatch**, as today; compiling inside
`agg_finish` would move it after the fold reply.

`resolve_scalar` (not `resolve_filter`) because HAVING is evaluated row-at-a-time
with `Evaluator::eval_row`: it demotes `result_reg` out of `bit_only` so
`regs[result_reg]` is always live. It does not change `no_nulls`
(`is_strictly_non_nullable` does not read the classification), only register
classification and AND-chain detection.

**Evaluate over a reused one-row batch.** Replace `fill_having_row` +
`eval_having` + `truthy` in the existing single emit loop (`agg_finish.rs:184-194`
— the loop, its `continue` early-out, and `emit_row` all stay):

```rust
// hoisted above the loop, only when spec.having.is_some()
let mut hav = ZSetBatch::new(spec.partial_schema);   // PkColumn::U128s, filler ColData per column
let mut bufs = ViewBuffers::default();
…
// inside the loop, replacing the fill_having_row/eval_having/truthy block
if let Some(ev) = spec.having {
    hav.truncate(0, spec.partial_schema);            // types.rs:846 — clears every per-row vec
    hav.pks.push_u128(g as u128);                    // _group_pk: never referenced
    hav.weights.push(1);
    let mut nw: u64 = 0;
    for gi in 0..n_group {
        let ci = 1 + gi;
        let pi = spec.partial_schema.payload_idx(ci);
        let tc = spec.partial_schema.columns[ci].type_code;
        let rep = rep.expect("a grouped result always has a representative row");
        if partial.is_null(spec.partial_schema, rep, ci) {
            null_word_set(&mut nw, pi, true);
            hav.columns[ci].push_null(tc);
        } else {
            partial.columns[ci].push_row_from(rep, tc.wire_stride(), &mut hav.columns[ci]);
        }
    }
    for (k, acc) in gaccs.iter().enumerate() {
        let ci = 1 + n_group + k;
        let pi = spec.partial_schema.payload_idx(ci);
        let tc = spec.partial_schema.columns[ci].type_code;
        match acc_val(acc) {                          // acc_val, NOT finish_agg — as fill_having_row
            Val::Null      => { null_word_set(&mut nw, pi, true); hav.columns[ci].push_null(tc); }
            Val::Int(b)    => push_fixed_bits(&mut hav.columns[ci], b as u64, tc.wire_stride()),
            Val::Float(f)  => push_fixed_bits(&mut hav.columns[ci], f.to_bits(), tc.wire_stride()),
        }
    }
    hav.nulls.push(nw);
    bufs.fill(&hav, spec.partial_schema);
    let view = ZSetBatchView::new(&hav, spec.partial_schema, &bufs);
    let (v, is_null) = ev.eval_row(&view, 0);
    if is_null || v == 0 { continue; }
}
```

Everything it calls already exists: `ZSetBatch::truncate` (`types.rs:846`),
`PkColumn::push_u128` (`:392`), `ColData::push_row_from` (`:634`) —
**variant-exhaustive, so a `U128s`/`Bytes`/`Strings` group column copies without
a special case** — `ColData::push_null` (`:678`), `null_word_set` (`:729`),
`push_fixed_bits` (`agg_finish.rs:743`), `acc_val` (`:315`), and `null_word_set`
is already imported (`agg_finish.rs:20`). `push_fixed_bits`'s `unreachable!` can
never fire: `agg_output_type` (`gnitz-wire/src/circuit.rs:168-190`) yields only
`I64`/`U64`/`F64`/a fixed-int source type — all ≤ 8 bytes — so an `AggSpec`'s
`out_type` is always a `ColData::Fixed` column. `view` is declared inside the
`if let Some(ev)` block and its last use is `ev.eval_row`, so NLL releases
the `&hav`/`&bufs` borrows before the next iteration's `truncate`/`fill`. The
null bits are
**load-bearing** for the newly-enabled `IS NULL`: `acc_val` yields `Val::Null`
for an all-NULL or uncontributed `SUM`/`MIN`/`MAX` group, and `MIN(v) IS NULL`
compiles to a real null test. `rep == None` is the global ground group, which has
`n_group == 0`. Agg partials are never F32 — `agg_output_type` maps float
MIN/MAX/SUM to `F64` (`gnitz-wire/src/circuit.rs:174-189`) — so the `Val::Float`
arm is always the 8-byte case. Steady-state allocation is zero: `truncate` keeps
capacity, `bufs.fill` reuses its `Vec`s.

The truth test `is_null || v == 0 → drop` is exactly today's `truthy` + NULL-drop
and bit-identical to the engine filter's `bool_bits & !null_bits` (`eval_row`
reads a bit_only result out of `bool_bits`, exactly as the filter does): for a
boolean
result `regs` is 0/1 and `bool_bits` matches; for a float result both sides
bit-test.

**Delete**: `HavingEval` (`:419`), its `impl BoundExprBackend` (`:449-538`),
`is_float_expr` (`:431`), `having_supported` (`:544`), `fill_having_row`
(`:379`), `truthy` (`:404`), `eval_3vl` (`:564`), `eval_cmp` (`:588`),
`eval_arith` (`:608`), the `eval_having` shim, and `having_state`'s
`Vec<TypeCode>` + `Vec<Val>`. **Keep `Val`** (`:99`) — it is the render currency
of `finish_agg` (`:285`), `acc_val` (`:315`) and `emit_row` (`:700`).

String / U64 / CASE / IN / `IS NULL` HAVING now works ad-hoc; the only remaining
rejection is a compile error, identical to CREATE VIEW.

**One intentional semantic change: `-0.0` truthiness.** Today
`truthy(Val::Float(f)) => f != 0.0`, so a bare float HAVING evaluating to `-0.0`
drops the group. The VM holds float results as **bit patterns** and both the
engine filter (`pack_to_bool_bits`) and `Evaluator::eval_row` bit-test, and `(-0.0f64).to_bits() != 0`, so the group is kept — a move *toward*
the CREATE VIEW path. NaN agrees on both (nonzero bits, `NaN != 0.0`).

**Parity caveat — a pre-existing view-path bug.** The engine's physical reduce
output descriptor declares agg columns **non**-nullable (`ops/reduce/plan.rs:79`,
flag `0`) while `emit_agg_col` sets their null bit for an untouched SUM/MIN/MAX
(`ops/reduce/emit.rs:16-28`) and writes zero bytes, so a view's post-reduce HAVING
can take the `no_nulls` arm and compare a NULL aggregate as the value `0`.
Exactly three shapes reproduce — grouped `MIN`/`MAX` over a **nullable** column,
global `Direct SUM` over a **NOT NULL** column, and global `MIN`/`MAX` — because
they alone combine a NULL-capable raw column with a HAVING program that has no
NULL-forcing instruction. (`HAVING SUM(v) …` on a *nullable* source does **not**
reproduce: it routes to the count-companion `NullfillSum` shape whose finalize
`Div` already forces `no_nulls` off, pinned by
`gnitz-py/tests/test_aggregates.py:912-957`.) So `HAVING MIN(v) <= 10` over an
all-NULL group **keeps** a group that correct SQL — and the ad-hoc path after
this plan — drops. That bug is out of scope here and is tracked in
`plans/reduce-agg-output-nullability.md`. Consequence: the parity grid asserts
ad-hoc results against **expected SQL semantics** for those three shapes, and
against the CREATE VIEW result for everything else.

### DML residual (`exec/residual.rs`, `dml/mutate.rs`)

Reply rows already arrive as a decoded `ZSetBatch` (`decode_wal_block`,
`protocol/wal_block.rs:302`, via `protocol/message.rs:422`).

`matching_indices(preds: &[&BoundExpr], batch: &ZSetBatch, schema: &Schema)`
(`residual.rs:29`) becomes:
- an empty `preds` slice → `0..n`;
- else fold `preds` into one left-associated `BoundExpr::BinOp(Box::new(acc),
  BinOp::And, Box::new((*p).clone()))` chain (`BExpr` derives `Clone`,
  `ir.rs:20`; the same construction `in_list_or_chain` already does at
  `lower.rs:95`);
- **`compile_filter_program` returning `Ok(None)` also yields `0..n`** — it is
  reachable, because the binder const-folds a null test on a non-nullable column
  to a literal (`bind/structural.rs:496-506`), so `DELETE FROM t WHERE
  nonnull_col IS NOT NULL` folds to `LitInt(1)` (`lower.rs:437`). The mirror case
  needs nothing: `LitInt(0)` fails the `v != 0` guard, compiles to a real
  `LoadConst 0`, and correctly drops every row;
- else the shared lines with `resolve_filter`, one `ViewBuffers`/`ZSetBatchView`,
  and `ev.filter(&view, n, |start, end|
  matched.extend(start..end))` — **`end` is exclusive, not a length**.

`row_passes_residuals` (`:12`) is deleted. Its own doc comment carries the
soundness argument for the fold: the engine `BoolAnd` nullable arm
(`batch.rs:240-243`) yields definite-false when either side is definite-false and
NULL otherwise, and `Evaluator::filter` merges `bool_bits & !null_bits`, so a NULL
conjunct excludes the row either way — identical to today's per-conjunct
short-circuit. `and_chain_mask` (`program.rs:1163-1218`) requires the terminal
instruction to be a `BoolAnd` writing `result_reg` and then walks whichever
operand `is_link`, so a left-associated `((a∧b)∧c)∧d` is a clean spine and the
AND-chain skip survives.

`schema` is `actual = schema_opt.as_deref().unwrap_or(schema)` at the one caller
(`mutate.rs:265,269`) — the reply schema the batch carries, which is also what
the residual is evaluated against today. `resolve` runs once per query.

String residuals (`UPDATE … WHERE pk = k AND strcol = 'x'`) become supported.
One behaviour change to accept: a conjunct that *errors* (a U128/mixed-type
operand) today errors only when the fetched batch has ≥1 row; compiling before
the loop makes the 0-row case error too — deterministic, and the right direction.

*Perf:* this path only runs on the no-usable-index fallback; it trades an eager
adapter build for vectorized evaluation — a wash on a cold path.

### SET expressions (`dml/mutate.rs`, `dml/insert.rs`)

```rust
pub(crate) enum SetProgram {
    Str(String),          // BoundExpr::LitStr
    StrCol(usize),        // ColRef into a TypeCode::String column
    Num(Evaluator), // everything else — integer-valued
}
```

**Compile against the schema the rows actually carry, not the catalog schema.**
`write_set_rows` is called with `actual_schema = resolved.schema.as_deref()
.unwrap_or(&*schema)` — the *reply* schema (`mutate.rs:328-338`) — while
`bind_mutate_scalar` runs against the catalog `schema` (`mutate.rs:318`). Today
that is harmless because `eval_expr` resolves columns dynamically; after this
plan `resolve` bakes in payload slots, PK byte offsets, type codes **and**
`no_nulls`, so compiling against the wrong one miscomputes silently. So:
`bind_mutate_scalar` keeps returning `BoundExpr`, and a new
`compile_set_programs(assignments: &[(usize, BoundExpr)], schema: &Schema) ->
Result<Vec<(usize, SetProgram)>>` runs **inside the RMW closure, against
`actual_schema`, inside the `if count > 0` arm** — i.e. immediately before
`write_set_rows` (`mutate.rs:329`), not at the top of the closure. Placement is
observable: hoisted above the row-count check, `UPDATE t SET int_col =
float_col` matching zero rows would newly error, where today `eval_set_expr` is
lazy and errors only with ≥1 row. `assignments` (a `Vec<(usize, BoundExpr)>`
bound at `mutate.rs:311-318`) is captured by the closure and `actual_schema` is
bound at `:326`, so both are in scope. The closure is `FnMut`
(`dml/rmw.rs:54`), so a retry recompiles — bounded by `RMW_MAX_ATTEMPTS` on a
cold path. (ON CONFLICT is unaffected — it uses the catalog `schema` throughout,
`insert.rs:431,466`.)

Classification, per assignment, exactly as `eval_set_expr` decides today:
- `LitStr(s)` → `Str(s.clone())`.
- `ColRef(c)` where `schema.columns[c].type_code == TypeCode::String` →
  `StrCol(c)`. **`String` only, not `is_german_string()`**: `ColumnValue::Str`
  holds a `String`, and a BLOB column must keep falling into `Num` where
  `OpcodeBackend::col_ref` rejects it (`lower.rs:215`) — the same `Unsupported`
  `InterpBackend` gives today (`eval.rs:71-73`). Deciding by type code is safe
  because `ColData::empty_for(TypeCode::String) → Strings` (`types.rs:694`) and
  every batch reaching SET is built from a `Schema` (`decode_wal_block`,
  `filler_columns`, `copy_batch_row`, `build_merged_row`, the `excluded` INSERT
  batch), so `type_code == String ⟺ ColData::Strings`.
- everything else → `Num`, via a **new `lower.rs` helper** (necessary because
  `OpcodeBackend`'s fields are module-private, so `mutate.rs` cannot construct
  it):
  ```rust
  /// Compile a scalar (non-predicate) RHS to a program, rejecting a
  /// float-typed result. `OpcodeBackend::Out` is `(reg, is_float)`
  /// (`lower.rs:197`) and `compile_bound_expr` discards the bit (`:412`), so the
  /// float test has to happen here.
  pub(crate) fn compile_int_scalar_program(
      expr: &BoundExpr, cols: &[ColumnDef],
  ) -> Result<gnitz_core::ExprProgram, GnitzSqlError> {
      let mut eb = ExprBuilder::new();
      let (reg, is_float) = lower_bound_expr(expr, &mut OpcodeBackend { cols, eb: &mut eb })?;
      if is_float {
          return Err(GnitzSqlError::Unsupported(
              "SET of an integer column from a floating-point expression is not supported".into(),
          ));
      }
      Ok(eb.build(reg))
  }
  ```
  then the shared `from_wire` / `resolve_scalar` tail.

  The float rejection is **required**: it replaces the check `InterpBackend`
  performs today (`lit_float` returns `(reg, true)` at `lower.rs:235`, float
  `col_ref` at `:225`; `InterpBackend` rejects both outright at
  `eval.rs:63-96`). Without it, a float-bit `i64` written as
  `ColumnValue::Int(bits)` (`codec/colwrite.rs:15`) into a `BIGINT` column
  **passes** `append_column_value`'s `FixedInt::from_type_code` guard
  (`colwrite.rs:125,132-133`) and silently stores raw float bits. The other
  mismatches are unchanged: `SET int = u128` compile-rejects at `lower.rs:209`;
  `SET str = numeric` / `SET int = str` are rejected by `append_column_value`
  (`colwrite.rs:136,140`).

  `resolve_scalar` also keeps a boolean-valued RHS (`SET flag = a AND b`)
  readable: it demotes `result_reg` out of `bit_only`, so `BoolAnd`/`BoolNot`
  run `unpack_bool_to_regs` and `regs[result_reg]` is live.

- `eval_set_program(p: &SetProgram, view: &ZSetBatchView, batch: &ZSetBatch,
  row: usize) -> Result<ColumnValue>`: `Str(s)` →
  `ColumnValue::Str(s.clone())`; `StrCol(c)` → read `ColData::Strings` at `row`
  (as today, `mutate.rs:49-57`); `Num(ev)` → `ev.eval_row(view, row)` →
  `(_, true) => Null`, `(v, false) => Int(v)`. `eval_set_expr` (`mutate.rs:41`)
  is deleted. **No scratch parameter**: each `Evaluator` owns its register file.
- `write_set_rows` (`mutate.rs:147`): takes `&[(usize, SetProgram)]`; builds one
  `ViewBuffers` + `ZSetBatchView` over `current` before the row loop;
  `asn_by_col` becomes `Vec<Option<&SetProgram>>`. Nothing else is threaded
  through it into `build_merged_row`.
- `insert.rs`: `BoundUpdateExpr::{Existing(SetProgram), Excluded(SetProgram)}`
  (neither `ConflictPlan` `:28` nor `BoundUpdateExpr` `:44` derives
  `Clone`/`Debug`, and its only uses are `ConflictPlan::DoUpdatePk` `:37`,
  `bind_do_update_assignments` `:328`, `bind_do_update_rhs` `:341,348,366`,
  `client_side_merge_do_update` `:433,436`, `eval_do_update_rhs` `:478,485-486`).
  **The EXCLUDED short-circuit at `insert.rs:348` builds
  `BoundUpdateExpr::Excluded(BoundExpr::ColRef(col_idx))` directly, bypassing
  `bind_mutate_scalar`** — route that bare `ColRef` through the same classifier.
  Hoist **two** `ViewBuffers` (`bufs_excluded`, `bufs_existing`) above the loop
  — two are required because the `excluded` view
  holds `&bufs_excluded` for the whole loop, so the per-iteration `existing` fill
  needs a separately-owned buffer set (`&mut` vs the live `&`). Build the
  `excluded` view over `batch` once (it outlives the loop); for `existing` — an
  owned 1-row `ZSetBatch` from `effective_row` (`insert.rs:459`,
  `dml/overlay.rs:44-49`) — call `bufs_existing.fill(ex, schema)` and construct a
  short-lived view inside the `Some(ex)` arm, which allocates nothing after
  warmup.

**Delete** `InterpBackend` (`eval.rs:30`) with its `impl BoundExprBackend`
(`:36`), `eval_expr` (`:244`) and `eval_pred_row` (`:254`) — the whole non-test
body of `exec/eval.rs`; the file goes away and `exec/mod.rs:9` loses
`pub(crate) mod eval;`.

**Honest scope note:** SET's *string* half stays structural (`Str`/`StrCol`) —
`ColumnValue::Str` needs an owned `String` and the VM has no string result
register today. Only the numeric half is unified; `fn2` removes that limitation.

### `find_wide_literal` stays

An earlier draft deleted it. It is load-bearing in two of its three sites and is
kept in all three for uniformity:
- `mutate.rs:35` (`bind_mutate_scalar`) — SET now compiles inside the RMW
  closure, i.e. only once rows have been fetched, so without this eager check a
  `LitWide` in a SET RHS would still silently no-op when nothing matches.
- `select.rs:468` (HAVING) — kept for its message: it must not fall into the
  generic compile failure.
- `mutate.rs:260-263` (residual) — redundant now (compilation precedes the row
  loop, and `lower_bound_expr` raises the identical `wide_int_error` at
  `lower.rs:60`), but kept so all three read the same.

## Verification

### Codegen / throughput (mandatory step, not a gate)

At the end of Sequencing **steps 3a, 3b and the `FixedInt` fold**, run the
`#[ignore]`d expr benches in `--release`, interleaved A/B against that
step's parent commit (3a's run is recorded in its checkbox). They moved into
`gnitz-expr` with the code, so they run against `TestView`, not `MemBatch` —
which is why the codegen check below, not the timing, is the real evidence:
```
cd crates && cargo test -p gnitz-expr --release and_chain_skip_bench \
    -- --ignored --nocapture --test-threads=1
cd crates && cargo test -p gnitz-expr --release str_const_filter_bench \
    -- --ignored --nocapture --test-threads=1
```
(`gnitz-expr/src/batch/tests.rs`, `gnitz-expr/src/eval/tests.rs`.)
Acceptance criterion: **throughput
parity**. Supporting evidence if a delta shows: `objdump -d` the `eval_batch`
`Cmp` / `IntAdd` / `LoadPayloadInt` arms before and after — expected identical,
since the generic instantiates in the engine's own codegen unit.

Both benches are `--release` with `lto = true`, so they are structurally blind to
every `-O0` question the trait indirection raises — the `objdump` accessor-call
count on the **debug** server binary is the real evidence there (3a's method).
Their coverage, verified by reading both: `and_chain_skip_bench` drives
`eval_batch` through its own morsel loop, never touching `Evaluator::filter`, and
hits only `LoadPayloadInt`'s 8-byte arm — though its schema is fully nullable, so
it does exercise the null-bit path. `str_const_filter_bench` uses
`Evaluator::filter` over a non-nullable schema, so only the `no_nulls` arm. Neither covers `LoadPk`, the
narrow `load_int!` widths, or `eval_str_col_vs_col`.

### `SchemaFacts` equivalence test (load-bearing)

Client-side `resolve`/`locate` is genuinely new — `gnitz-core`/`gnitz-sql`
reference `resolve`/`ResolvedProgram`/`eval_batch` nowhere today. A silent
OPK-offset or payload-slot mismatch would miscompute rather than error.

Step 3a built the harness this reuses:

```rust
pub fn assert_schema_facts_consistent(s: &dyn SchemaFacts, cols: &[(u8, bool)], pk: &[usize])
```

a `pub fn` taking `&dyn` — because most methods collide by name with a typical
implementor's inherent method and Rust prefers the inherent one in receiver-dot
position, so a check written against the concrete type never enters the impl.
`cols` is the schema's own `(type_code, nullable)` column table and `pk` its PK
list; there is no separate expectation struct to keep in step with it. Step 4
calls it a **second** time, in the same `#[cfg(test)]` module and against the
same table, with `gnitz_core::Schema`.

Driving both impls off one expectation table is stronger than a two-impl
equivalence assertion, not weaker: it pins the absolute answer, so it also
catches two impls that agree with each other and are both wrong. It must not be
weakened to invariants-only (an impl self-consistently shifted by one satisfies
the round-trip and the sentinel rule) — the expectation table is what makes it
bite, which is why the harness asserts each column's exact type code (`wire_stride`
collapses 15 codes onto 5 widths), its nullability, and each PK column's OPK byte
offset. That is the `payload_col_idx` / `col_nullable` failure mode that silently
decides `no_nulls`, and the OPK-offset mismatch that silently reads a neighbouring
PK column.

**PK-list order is a parameter, not an inference.** It is independent of column
order — `PRIMARY KEY (b, a)` yields `pk_indices = [1, 0]`, and both
`SchemaDescriptor::pk_byte_offset` and `Schema::pk_byte_offset` walk PK-list
order — so a harness that derived the expected OPK offsets by scanning columns
left to right could never see the very mismatch it exists to catch. `pk` is
passed in and the offsets are the running widths along it.

The matrix (already exercised for `SchemaDescriptor` in `schema.rs`): signed and
unsigned PK; U64, U128, F64, STRING payload columns; **at least one nullable
payload column** (or `col_nullable` is asserted only against `false`); every fixed
width (1/2/4/8/16); a single PK at column 0; a single PK **not** at column 0; a
compound two-column PK; and a compound PK whose **PK-list order reverses its
column order** and skips a column in between. The last three shapes are where a
naive `payload_col_idx` passes for `payload_idx`, and the last is where a
column-order OPK-offset derivation passes and the real one does not.

The step-4 call site lives in `gnitz-engine`'s test tree: `gnitz-engine` is
binary-only, and its `gnitz-core` dev-dependency (`gnitz-engine/Cargo.toml:26`)
makes it the only place both schema types coexist. The *harness* lives in
`gnitz-expr` rather than there, so it is reachable from any crate that adds an
implementor — the same rule, and the same reason, as `assert_batchview_consistent`.

## Tests

- **Parity grid** (`GNITZ_WORKERS=4`; each case asserts the ad-hoc result equals
  the CREATE VIEW result unless noted): U64 HAVING above 2⁶³; unsigned division;
  negative-literal unsigned compares; string/blob group-column compares (`s =
  'a'`, `s > 'a'`, `s IN (…)`, `str_col < blob_col`); `IS [NOT] NULL` including a
  nullable U128 group column **and `MIN(v) IS NULL` over an all-NULL group**
  (guards null-bit population); a `GROUP BY u128_col` whose HAVING references
  only the aggregates (exercises `push_row_from`'s `U128s` arm); CASE with the
  float-in-ELSE unification shape; IN with a NULL inner; float modulo; a bare
  float HAVING evaluating to **`-0.0`** (asserts the group is kept, matching
  CREATE VIEW).
- **Expected-SQL-semantics cases** (not view-equality, per the parity caveat —
  these three shapes are where the view path is wrong): grouped
  `HAVING MIN(v) <= 10` over an all-NULL group, global `HAVING SUM(x) = 0` over a
  NOT NULL column with an empty source, and global `HAVING MAX(x) >= 0` over an
  empty source must each return no rows.
- **`HAVING 'a' = 'b'` rejects identically on both paths.** A `LitStr op LitStr`
  has no column operand, so `try_compile_string_cmp` (`lower.rs:102`) returns
  `None` and `OpcodeBackend::lit_str` (`:238`) errors — ad-hoc *and* CREATE VIEW.
  Assert the same `Unsupported`, not an empty result.
- **Constant HAVING**: `HAVING 1` (→ `Ok(None)`, every group passes), `HAVING 0`
  (→ real `LoadConst 0`, all dropped), `HAVING NULL` (→ `LoadNull`,
  `is_strictly_non_nullable` false, null bit set → dropped). All three match
  today's `truthy`.
- **Zero surviving groups**: a grouped HAVING that drops every group, and a
  grouped query whose partials are all non-positive (`reps.len() == 0`, reachable
  at `agg_finish.rs:156` when not `global_ground`).
- **64-register cap**: a 17-conjunct `UPDATE … WHERE` and a ≥17-item string IN in
  both a residual and a HAVING reject with the message naming the limit; a
  16-conjunct `WHERE` still works. Pins the narrowing as intentional.
- **Flip the string-HAVING rejection tests to parity.**
  `gnitz-sql/tests/adhoc_surface.rs:402`
  (`string_having_rejection_names_create_view`) and
  `gnitz-py/tests/test_sql.py:355`
  (`test_direct_path_feature_limits_are_not_derivation_errors`, HAVING arm at
  `:375-380`) currently assert `… GROUP BY s HAVING s = 'a'` is rejected with
  CREATE-VIEW advice; after this plan it succeeds — rewrite them to assert the
  ad-hoc result equals the CREATE-VIEW result.
- **Delete the two `having_supported` in-module tests** in `agg_finish.rs`
  (`probe_visits_both_or_branches`:771, `probe_rejects_float_modulo`:790). Their
  properties are guaranteed by the shared compiler — float modulo by
  `(BinOp::Mod, true) => Err(Unsupported("float modulo not supported"))`
  (`lower.rs:355`).
- **DML:** `UPDATE … WHERE pk = k AND strcol = 'x'` (string residual, newly
  served); `DELETE FROM t WHERE nonnull_col IS NOT NULL` (the `Ok(None)` path —
  must delete every row); residual div/mod-by-zero conjuncts filter like the
  view; SET numeric over nullable and U64 columns; `SET x = pk + 1` (PK-region
  read through the adapter); `UPDATE t SET int_col = float_col` **rejects**;
  `UPDATE t SET n = <17-conjunct-free expr>` sanity; `ON CONFLICT DO UPDATE SET x
  = x + 1` still reads the buffered `x` (the per-row `existing` view), and `ON
  CONFLICT DO UPDATE SET s = EXCLUDED.s` on a string column (the `insert.rs:348`
  classifier path). Retarget `eval.rs`'s div/overflow unit tests
  (`test_div_mod_by_zero_is_null`:379 and neighbours) onto `Evaluator::eval_row` and
  move them into `mutate.rs`'s test module. Update
  `gnitz-sql/tests/planner_null_predicate.rs`'s doc comment (lines 4, 10) — it
  narrates residual 3VL "through `eval_expr`"; prose only, still compiles.

## Not in scope

- **The reduce agg-output nullability bug** — `plans/reduce-agg-output-nullability.md`.
- **The `AVG(u64)` render bug** — a `finish_agg` signedness bug in `acc_f64`
  (`agg_finish.rs:357`), disjoint from everything here. Tracked in
  `plans/adhoc-avg-u64-render.md`.
- **Div/mod already agree** — the engine VM and `InterpBackend` both mark a zero
  divisor NULL and use `wrapping_div`/`wrapping_rem` (`batch.rs:503-508,661-665`;
  `eval.rs:191-193`, with `test_div_mod_by_zero_is_null`). Routing residuals
  through the VM is semantics-preserving.
- **Unifying SET's string arm** — needs a string result register (`fn2`).

## Sequencing

- [x] **DONE** — the German-string helpers live in
  `gnitz-wire/src/german_string.rs`, all `pub` and named as `gnitz_wire::X` at
  every engine call site (no engine re-exports). `gnitz-wire` owns the one
  extent rule `blob_extent`, the one content accessor `german_string_content`,
  the canonical-form predicate `german_string_cell_ok` and its constructive
  inverse `canonical_short_cell`.
- [x] **DONE — Create `gnitz-expr`.** The crate exists: dep `gnitz-wire` only,
  workspace member, dep of `gnitz-engine`, `[lints]` mirroring `gnitz-engine`,
  private `mod` + flat `pub use` (**not** `pub mod` — the `dead_code` claim was
  wrong for a lib). Steps 4/5 still have to add it to `gnitz-core` and
  `gnitz-sql`.
  - `view.rs` ships `RowSource` (4 methods) + `BatchView: RowSource` (2) — **six
    required methods across the pair, no defaults** — plus the region/per-row
    contract and the shared `assert_batchview_consistent`, which `MemBatch` and
    the crate's own `TestView` both call. Step 4's `ZSetBatchView` must implement
    **both** traits and must call the assertion.
  - `locator.rs` holds `ColumnLocator` + its 7 methods (retargeted to
    `&impl RowSource`, **all `#[inline(always)]`**) + the static assert +
    `PAYLOAD_MAPPING_PK_SENTINEL`. The four route/native-key fns went to
    `gnitz-wire` instead, and engine call sites name `gnitz_wire::` directly (see
    the modules section) — there is no `foundation::codec` re-export of them.
  - `RowView` is gone and `ColumnarSource` now **extends `RowSource`** with only
    `get_weight`, so its four per-row methods have one definition instead of two
    and `MemBatch` writes each forwarder once (`blob_slice` → `blob` at the trait
    call sites). Every bound whose body never reads the weight was relaxed to
    `RowSource` — including `IndexKeySpec`'s three methods and
    `index_gather.rs`'s `row_in_index_range`, and `ReadCursor::current_row_source`'s
    return — so `ColumnarSource` is now storage-internal and **not re-exported from
    `crate::storage`**. The engine `schema` module re-exports `ColumnLocator` + the
    sentinel so 90 `ColumnLocator` occurrences across 14 files are unchanged;
    `RowSource`/`BatchView` are deliberately **not** re-exported — call sites
    import `gnitz_expr::` directly.
  - Because `ColumnLocator` binds to `RowSource`, `ColumnarSource`-generic code
    can read through a locator: `ops/util.rs`'s `canonical_group_key` — a verbatim
    copy of `route_key`'s two arms that existed only to work around the old bound —
    is deleted, and `CanonicalKeyArm` collapsed to a `bool`.
  - `IndexKeySpec::write_span` reads through the locator (`is_null` gates,
    `bytes` reads) and spells only the *encoder* per variant: a PK source (already
    OPK) goes to the new `gnitz_wire::promote_opk_column` (OPK→OPK, identity when
    unpromoted), a payload source to `encode_pk_column_promoted` (native→OPK).
    `ColPromoter::write_into` uses the same two primitives, so the index span and
    the reindex `_join_pk` cannot drift — they previously agreed only by spelling
    the same branch twice, and `ColPromoter` missed the identity arm for a
    *carried* target equal to its source. `ColPromoter` also now stores the
    **resolved** `out_tc = resolve_reindex_type(src_tc, carried)` rather than the
    raw carried tc, so the `0` self-derive sentinel is undone once in `new`
    instead of twice per row in two different spellings; with that, its `Pk`,
    `Narrow` and `Wide` arms are each a single encoder call (only the XXH3 string
    hash stands apart). `IndexKeySpec::new` asserts the property `write_span`
    actually depends on — `index_key_type(src) == idx_col.type_code` — which
    subsumes the German-string rejection it previously checked.
    The identity is pinned in `gnitz-wire`
    (`promote_opk_column_identity_matches_decode_encode`), as is the closed
    source→index type-pair set (`index_key_type_set_is_closed_and_width_stable`);
    engine-side, `write_span_matches_seek_prefix_across_type_ladder` holds both
    arms to the independent `index_opk_prefix` oracle over 10 types, and
    `write_span_matches_the_oracle_on_compound_null_and_entry_shapes` oracles the
    compound-PK / NULL-skip / `write_entry` shapes against `seek_prefix` (the
    separately-maintained seek-side encoder) rather than a hand-copied twin.
  - `Accumulator` resolves **what one row does to the slot** once at construction,
    as a `StepKind` (`Count | CountNonNull | Sum(SumWiden) | Extreme { max }`),
    where `SumWiden::Int` holds a `gnitz_wire::FixedInt` — replacing three copies
    of the classify-and-widen rule *and* removing three out-of-line calls per row
    per aggregate from the debug binary (two derived-`PartialEq` `AggOp` compares
    and an `Option::expect`; at `-O0` a derived `PartialEq` is a real call).
    Construction is one exhaustive `match` over `AggOp`, so a new opcode cannot
    reach the row path unclassified, and SUM's non-numeric rejection moved from a
    per-row unwrap to a once-per-epoch one.
    `extract_col_key` and `GroupKeyExtractor::gather`
    keep calling `ColumnLocator` methods rather than pre-destructured twins: the
    `#[inline(always)]` accessors make the dispatch free, verified by an
    interleaved A/B of the AVI compose bench (6.10/6.11 vs 6.01/6.06 ns/row
    against a hand-hoisted `GatherCol` variant — parity, inside the run-to-run
    spread, with the simpler form marginally ahead on the full path).
  - `BatchBuilder` gained `begin_row_opk(&[u128], weight)` (plus `put_i32` /
    `put_i64`), so a compound-PK test row goes through the builder instead of
    hand-rolling the `ensure_row_capacity`/`extend_*`/`count += 1` protocol — the
    spelling that invites `extend_pk_bytes` over a native-LE concatenation, which
    is not the at-rest form at all.
- [x] **DONE — 3a: decouple `program.rs` + `batch.rs` from the engine, in
  place.** No file moved; the two files now name nothing from `crate::` (only
  `pub(in crate::expr)` visibility markers and one `super::` sibling import
  remain), which is what makes 3b's body diff empty.
  - `gnitz-expr/src/schema_facts.rs` holds `SchemaFacts` — six required methods
    (`locate` / `payload_col_idx` / `num_payload_cols` / `num_columns` /
    `col_type_code` / `col_nullable`, `col_type_code` returning `u8`) plus two
    provided ones derived off `locate`: `payload_slot(ci) -> Option<u8>` and
    `is_pk_col(ci) = payload_slot(ci).is_none()`, so no implementor can state
    the address and the PK-ness and have them disagree. The sentinel is **not**
    in the contract (see the `SchemaFacts` section). Implemented for
    `SchemaDescriptor` with each forwarder written **UFCS** where an inherent
    method exists — most collide by name with one Rust would prefer in
    receiver-dot position — and as a direct column-table read for
    `col_type_code` / `col_nullable`, where the field *is* the fact.
  - `resolve_program`, `validate`, `validate_predicate`, `check_col`, `check_copy_types`,
    `check_emit_slot` and `is_strictly_non_nullable` read it as
    **`&dyn SchemaFacts`** / `Option<&dyn SchemaFacts>` (seven, not five:
    `af4360a5` merged `check_col_in_range`/`check_col_payload` into `check_col`
    and added the other two). The trait is per-compile, never per-row, so static
    dispatch bought nothing and cost a `NoSchema` placeholder implementor whose
    instantiation duplicated the whole validator; `&dyn` deleted it with **zero**
    call-site churn and −2 580 instructions / −10.2 KB `.text` (measured on the
    debug binary). `check_emit_slot` lost its `SchemaColumn` binding and reads
    `col_type_code(payload_col_idx(out))`, testing the width as
    `wire_stride(type_code) != 8` — the same two facts its two sibling `check_*`
    helpers read, and without `locate`'s release-active bound assert and
    O(pk_count) OPK-offset walk, which buy nothing when `size()` *is*
    `wire_stride(type_code)`.
  - Three `mb`-taking kernels (`eval_str_col_vs_const`, `eval_str_col_vs_col`,
    `eval_batch`) are generic over `BatchView` — the region half, since they index
    `col_data`/`null_bmp` absolutely; `mb.blob` became `mb.blob()`.
    `eval_is_null` and `eval_str_cmp` read only the null bitmap, so they take
    `null_bmp: &[u8]` like their own callee `fill_null_bits_mask` and are not
    generic at all.
  - The `read_{signed,unsigned}{,_exact}` ladder (plus
    `read_unsigned_zero_extends`) lives in `gnitz-wire`; `schema::type_size` is
    deleted and its three survivors read `gnitz_wire::wire_stride` directly (no
    `as usize`).
  - **`gnitz-wire` is the sole home of every LE byte primitive.** `read_i64_le`
    moved there beside `read_u64_le`, and `foundation::codec`'s
    `pub use gnitz_wire::{align8, read_u32_le, read_u64_le, write_u64_le}` is
    gone — one rule with two import paths is how spellings drift, and the diff
    had already put one on each side inside `expr/` (`batch.rs` on `gnitz_wire::`,
    `plan.rs` on `codec::`). `foundation/codec.rs` is then **deleted**: what was
    left had no concept — four unsafe unaligned mmap accessors used only by
    `runtime/protocol/{sal,w2m_ring}.rs` (moved to `foundation::posix_io`, which
    already owns mmap) and a `#[cfg(test)]` typed-slice view used only by four
    `storage/` test modules (moved to `test_support`). The engine `schema` module likewise
    stopped re-exporting `is_fixed_int` / `is_routable_int` / `is_signed_int` /
    `SHORT_STRING_THRESHOLD` (one external call site each, against 21–105 for
    their siblings already named `gnitz_wire::`).
  - `gnitz-expr` also ships `assert_schema_facts_consistent(&dyn SchemaFacts,
    cols: &[(u8, bool)], pk: &[usize])`, a conformance harness driven off the
    schema's own column table and PK list — `pub fn`, not `#[cfg(test)]`, exactly
    like `assert_batchview_consistent`, so step 4 calls it a second time with
    `gnitz_core::Schema`. `&dyn` on purpose: written against a concrete type the
    inherent methods would shadow every trait method and the check would be
    vacuous. It pins each column's type code, nullability, PK-ness, `locate`'s
    variant / width / type, the `locate().slot`↔`payload_col_idx` round trip
    (the one independently-implemented direction, and the one that decides
    `no_nulls`) and — for PK columns — the OPK byte offset **taken along the PK
    list**, plus both counts. Its own `should_panic` test proves an off-by-one
    `payload_col_idx` fails it, and both fixtures use a PK list that is permuted
    relative to column order.
  - Per-row cleanups the decoupling exposed, each measured on the `-O0` binary:
    `LoadPk`'s signed arm hoisted its `[0u8; 8]` decode scratch out of the row
    loop (it was a `memset` PLT call per row for bytes `decode_pk_column`
    overwrites and `read_signed` then reads); `gnitz_wire::decode_pk_column_owned`
    and `NullPerm::apply` became `#[inline(always)]` (both pure forwarders on
    per-row paths, where a plain hint is a no-op); `read_signed`/`read_unsigned`
    took static `debug_assert!` messages, since an `inline(always)` body
    duplicates its `Arguments` block into every site.
    **Measured and declined** — promoting these grows each per-row caller far
    more than it saves, the same profile that kept `read_u64_le` on the plain
    hint: `german_string_content` + `blob_extent` (`eval_str_cmp` 4 338 → 7 782
    instructions; the merge comparator 310 → 811) and `pk_route_key` /
    `pk_native_key` (`hash_group_col` 2 203 → 3 421). Their bodies are dominated
    by formatted `debug_assert!`s and bounds checks, which inlining copies rather
    than removes.
  - Two per-row wins the byte-primitive consolidation then exposed, both
    measured with `perf stat -e instructions:u` on `CARGO_INCREMENTAL=0` debug
    builds (incremental compilation makes `-O0` instruction counts swing ~24 %
    between rebuilds of identical source — CGU-partitioning nondeterminism; do
    not benchmark the debug binary without pinning it):
    `compare_rows_fixedint_nonnull` dropped its O(cols) per-comparison shift
    tripwire in favour of a `gnitz-wire` `const` assert over the whole type-code
    domain (**−21.9 %** on `run_merge_dup_pk_bench`), and the width-form
    `read_{signed,unsigned}` split into an exact-width primitive plus a
    sub-slicing wrapper, which recovered the +2.44 % `cmp_typed_le` regression
    the pair's first form had introduced on the nullable merge path and landed
    **−0.54 % against `af4360a5`**. `NullPerm::apply` also swapped its slice
    iterator for an index walk — at `-O0` the iterator's `next` was an
    out-of-line call per (row × pair) that `inline(always)` on `apply` could not
    remove.
  - Verified: the full 4183-line expr suite passes with **zero edits inside
    `expr/`**; `objdump` over the kernels in the `-O0` server binary shows
    **no** out-of-line `BatchView`/`RowSource` accessor calls (21k disassembled
    lines, 0 matches, in a body that does contain plenty of other calls); the two
    release expr benches are at parity interleaved A/B/A/B against `af4360a5`
    (`str_const eq` 552–553 → 582–592 M rows/s, `and_chain` skip-ON 4.82–5.00 →
    4.69–4.81 ms).
  - **`str_const_filter` is not a usable A/B signal on a loaded machine, and the
    codegen is the evidence that settles it.** Interleaved over 6 passes its two
    arms move in *opposite* directions by ~20 % each (`eq` favours one binary,
    `lt` the other) and the *baseline* alone spans 148–625 M rows/s. Release
    `objdump` of `eval_batch` — which every string kernel inlines into, so there
    is no standalone symbol — is 14 231 → 14 244 instructions, and the opcode
    histogram delta is almost entirely padding (`data16` +9, `int3` +5, `nop*`
    −8). Neither of the changes on this side of the A/B (`eval_is_null` /
    `eval_str_cmp` taking `null_bmp`, and the `LoadPk` scratch hoist) touches any
    instruction the string compare executes, so a difference there is layout, not
    work. Judge this bench on codegen; use `and_chain` (4.68–4.78 ms, parity) for
    the timing check.
- [x] **DONE — 3b: `program.rs` + `batch.rs` live in `gnitz-expr`**, and the
  crate is the evaluator. What landed:
  - **`Evaluator` is the surface**, not `(prog, scratch)` free functions.
    `LogicalProgram::resolve_{filter,map,scalar}(…) -> Result<Evaluator, _>`,
    each validating before it resolves, and the `Evaluator` owns its
    register file; `eval_row(mb, row) -> (i64, bool)`, `filter(mb, n,
    append_range)`, `eval_morsels(mb, start, n, f)` (`f(rel_start, &MorselOut)`;
    row ranges are half-open, `end` **exclusive**, everywhere), `copy_moves() ->
    impl Iterator<Item = (ColumnLocator, u32)>`, `emit_targets() ->
    impl Iterator<Item = (u16, u32)>`. `MorselOut` reads a morsel's results:
    `rows`, `reg_values`, `for_each_null_row` — no `value` / `is_null`, since a
    bit_only register is not in `regs` on the nullable arm and the only correct
    single-row read is the crate-private `read_reg_row0`. Each of the three drive
    methods carries its own body; there are no free `filter_batch` / `eval_row`
    twins. `EvalScratch` (whose `ensure_capacity(&ResolvedProgram, n)` takes the
    program, not its register count and nullability arm, so the scratch cannot be
    sized for one arm and evaluated on the other), `MORSEL`, `NULL_WORDS_PER_REG`,
    `eval_batch`, `read_reg_row0`, `is_bit_only`, `classify_registers`, the
    `unsafe` split-borrow helpers (`reg_mut`, `reg3`, `null_words3`, `reg4`,
    `clear_null_reg`, all `batch.rs`-private), `ResolvedProgram` and the whole
    resolved `Instr` form are **private to the crate**. `LogicalProgram`, `LogicalInstr`, `CmpOp`, `StrOp`, `ColKind` and
    `ExprValidateErr` stay public — they are the program-*construction* surface
    the client legitimately builds against. Rewrite the three client sketches
    (HAVING, residual, SET) against it: no `EvalScratch` is threaded anywhere,
    and `eval_set_program` / `write_set_rows` / `build_merged_row` lose the
    parameter entirely.
  - `ScalarFunc::run_filter` and `evaluate_predicate` are **deleted**;
    `filter_ranges` is the one engine spelling of "which rows pass", and it calls
    `Evaluator::filter` directly. `Repr` is
    `Predicate(Box<Evaluator>) | Map(Box<MapPlan>)`, and the map half's five
    fields plus `map_ranges_into` / `map_rows_into` live on `MapPlan`, so the
    variant is discriminated exactly twice (`filter_ranges` and the private
    `ScalarFunc::map()`) instead of five times. A pure projection has
    `compute: None` and never grows a register file. `MapPlan` resolves the
    instruction stream **once, at construction** — `col_moves` from
    `ev.copy_moves()` and `emits: Vec<(usize, usize)>` from `ev.emit_targets()`.
    Neither is re-derived per morsel: measured at `-O0`, an `emit_targets()`
    rescan inside the morsel callback costs ~52 instructions per program
    instruction per morsel (474 for a 6-instruction program, 1930 for 34),
    against 46 for the indexed walk over the resolved `Vec` — which
    `append_map_ranges` pays per *range*, where `COMPACT_RUN_LEN` deliberately
    lets 16-row ranges through uncompacted.
    `map_rows_into`'s hand-rolled morsel loop is gone — `eval_morsels` is
    the one home of `for morsel_start in (0..n).step_by(MORSEL)`.
  - **The expr suite lives in `gnitz-expr`** — all 92 tests, in per-module
    `tests` submodules (`program/`, `batch/`, `eval/`, `locator/`,
    `schema_facts/`, `view/`) over **one** fixture family in `test_support`:
    `TestView` / `TestSchema` (+ `TestSchema::with_pk_at`), the view builders
    `make_int_view` / `make_int_row(schema, &[i64], null_word)` /
    `make_n_col_view` / `make_string_view`, the schema builders
    `schema_pk_ints` / `schema_pk_strings`, and the two program builders
    `scalar_prog` / `filter_prog`. No test module defines its own schema, view or
    program builder, and none names a bare type-code integer.
    `assert_batchview_consistent` gained the negative case its sibling harness
    already had — a `TestView` newtype whose `get_col_ptr` forgets one slot's
    redirect must fail it. `expr/tests/mod.rs`
    is gone; `plan_tests.rs` is `expr/tests.rs`, owns `make_int_batch`, and gains
    one test pinning `from_predicate` → `filter_ranges` over a real `Batch`.
    `SchemaColumn::corrupt` is deleted — `TestSchema` reports a type code
    verbatim, so the escape hatch has no reason to exist.
  - **No `crate::expr` re-export of anything from `gnitz-expr`** — call sites
    name `gnitz_expr::` directly, matching the shipped `RowSource`/`BatchView`
    decision. With `program.rs`/`batch.rs` gone the `expr/mod.rs` + `expr/plan.rs`
    pair was a shim over a single child, so `expr` is now one file, `expr.rs`,
    declaring `PkFill` / `ScalarFunc` directly plus `#[cfg(test)] mod tests;`.
  - **One inlining doctrine**, stated once at the crate root, with the profile
    fact behind it (no `[profile.test]`/`[profile.bench]`, so `#[inline]` is
    decoration in this workspace). Its refinement is **measured, and is not what
    an earlier draft of 3b assumed**: what gets `#[inline(always)]` is the
    *test*, not the body. A per-row guard/forwarder/dispatch inlines; a per-row
    item whose body is a *loop* stays out of line behind an always-inlined guard.
    `ensure_capacity` (always-inlined length check + `#[cold] #[inline(never)]
    grow`) and `maybe_pack_bool_bits` (always-inlined `no_nulls` /
    `needs_bool_pack` test + out-of-line `pack_to_bool_bits`) are the two
    instances: promoting the bodies too grew `eval_batch` from 12.7k to 22.1k
    `-O0` instructions. Judge such a change on that instruction count, **not** on
    the `-O0` AND-chain bench — re-measured, its run-to-run spread on one fixed
    binary is 1.3×–1.8×, which swamps any effect an inlining decision has, and
    `maybe_pack_bool_bits` runs once per instruction per *morsel* (≤62.5k calls
    per pass), far too rarely for call overhead to move a 300 ms needle.
    `for_each_null_row` is a deliberate, documented exception: a nested loop that
    is still `#[inline(always)]`, since it has two call sites and its out-of-line
    copies were about as large as the splice.
- [ ] **Fold `FixedInt` into the load opcodes** (its own commit — it rewrites a
  per-row loop and deserves its own A/B). Its `validate`-side prerequisite is
  already satisfied: `check_col(…, ColKind::FixedInt)` makes
  `FixedInt::from_type_code` total on a validated program. Measured payoff: on
  the `-O0` binary the `LoadPk` signed row loop still costs, per row,
  `core::array::index_mut`, a **394-instruction** out-of-line
  `gnitz_wire::decode_pk_column` frame (`#[inline]` only — a no-op at `-O0`) that
  re-derives `is_signed_int(tc)` the arm head already hoisted, a `from_le_bytes`
  call inside the inlined `read_signed`, and a `Vec::index_mut`. (3a already took
  the `call memset@plt` out by hoisting the `let mut le = [0u8; 8]` scratch above
  the loop — the *only* one of these fixable without either a new primitive or a
  second spelling of the OPK transform inside the evaluator.)
  - Needs one new `gnitz-wire` primitive, beside `widen_pk_be`:
    ```rust
    // Exact inverse of `encode_pk_column` (big-endian plus `dst[0] ^= 0x80` for
    // signed types) fused with the widening `FixedInt` already defines. Byte-array
    // literals, not `try_into().unwrap()` (an out-of-line chain at -O0), and not
    // `widen_pk_be` (whose inlined body still costs a 16-byte stack zero plus a
    // copy per row).
    #[inline(always)]
    pub fn decode_opk_i64(opk: &[u8], fi: FixedInt) -> i64 {
        debug_assert_eq!(opk.len(), fi.width());
        match fi {
            FixedInt::U8  => opk[0] as i64,
            FixedInt::I8  => (opk[0] ^ 0x80) as i8 as i64,
            FixedInt::U16 => u16::from_be_bytes([opk[0], opk[1]]) as i64,
            FixedInt::I16 => u16::from_be_bytes([opk[0] ^ 0x80, opk[1]]) as i16 as i64,
            FixedInt::U32 => u32::from_be_bytes([opk[0], opk[1], opk[2], opk[3]]) as i64,
            FixedInt::I32 => u32::from_be_bytes([opk[0] ^ 0x80, opk[1], opk[2], opk[3]]) as i32 as i64,
            FixedInt::U64 => u64::from_be_bytes([opk[0], opk[1], opk[2], opk[3],
                                                 opk[4], opk[5], opk[6], opk[7]]) as i64,
            FixedInt::I64 => u64::from_be_bytes([opk[0] ^ 0x80, opk[1], opk[2], opk[3],
                                                 opk[4], opk[5], opk[6], opk[7]]) as i64,
        }
    }
    ```
    Pin it with `decode_opk_i64(encode_pk_column(v)) == v` over all eight variants
    at `{MIN, -1, 0, 1, MAX}` — it is a *third* spelling of the OPK↔native
    transform, and a wrong XOR arm is a silent wrong answer on every PK predicate
    with no bench to catch it.
  - Target shapes: `Instr::LoadPk { dst, off, fi }` (`off` is the OPK byte offset
    and cannot come from `FixedInt`; only `tc` is replaced, read at exactly one
    site) and `Instr::LoadPayloadInt { dst, pi, fi }` — the latter's
    `match (col_size, is_signed)` **is** `FixedInt`'s eight variants and its
    `_ => unreachable!` is the wildcard `FixedInt` exists to delete; `match fi`
    yields identical monomorphic arms because `load_int!`'s `const SZ` is
    unchanged. `LoadPayloadFloat { dst, pi, wide: bool }` belongs in the same
    commit: since `af4360a5` its arm reads `tc == type_code::F32` *after*
    computing a width, so the width lookup is dead weight beside a type compare a
    resolve-time `bool` replaces outright. There is no `size_of::<Instr>` static
    assert to satisfy, and `FixedInt` is a 1-byte fieldless enum with compatible
    derives. **Test churn is zero** — the `matches!` assertions on these opcodes
    use `..` rest-patterns and none binds `tc`.
  - **The `-O0` inlining decision procedure** (recorded here because this is where
    the question belongs; an `#[inline(always)]` sweep of `expr/` was considered
    and **rejected** — it made the throughput A/B unattributable across three
    independent changes, and its own threshold rule promoted a `wire_stride` chain
    costing 345 bytes with 91 call sites across five crates while declining
    `maybe_pack_bool_bits` at 96): promote when the callee body is a spill
    prologue plus a few ALU ops and an epilogue — inlining is then negative-size
    at `-O0`, because the deleted call/prologue/spill/reload round-trip exceeds
    the copied body. Decline when it contains further calls, bounds checks or
    landing pads, because most of the body survives. Weight by call sites ×
    frequency, and decide a forwarder chain as a unit. **Body size in bytes is not
    the criterion.**
- [ ] **Client adapter** in `gnitz-core`: `impl SchemaFacts for Schema` (write
  `payload_col_idx`, return `u8` type codes); add `build_pk_region_into` /
  `encode_german_col_into` (`pub(crate)`, existing signatures kept as wrappers);
  `ViewBuffers` + `ZSetBatchView` (implementing **both** `RowSource` and
  `BatchView`) with the `U128s` `unreachable!` arm; export both
  through `protocol/mod.rs` and `lib.rs`. Add `expr_unsupported` in `gnitz-sql`.
  **Run the `SchemaFacts` equivalence test here.**
- [ ] **Swap HAVING**: compile in `select.rs` where `having_supported` was
  (via `resolve_scalar`); change `AggFinish::having` to
  `Option<&Evaluator>`; replace the `fill_having_row`/`eval_having`/
  `truthy` block with the reused one-row batch + `Evaluator::eval_row`; delete
  `HavingEval` and its cluster (**keep `Val`**); drop the CREATE-VIEW-advice
  message; flip `adhoc_surface.rs` / `test_sql.py`; delete the two in-module
  `having_supported` tests; add the parity grid, the expected-SQL cases, the
  constant-HAVING cases, the zero-group cases and the register-cap cases.
- [ ] **Swap residual + SET**: `matching_indices` onto `resolve_filter` +
  `Evaluator::filter` (empty-slice
  and `Ok(None)` → `0..n`; `|start, end|`); add `compile_int_scalar_program` to
  `lower.rs`; `compile_set_programs` inside the RMW closure against
  `actual_schema`; the `insert.rs:348` EXCLUDED classifier; `write_set_rows` /
  `client_side_merge_do_update` onto `eval_set_program` with hoisted
  `ViewBuffers`; delete `exec/eval.rs`; retarget the div/overflow
  tests; add the DML tests.
- [ ] `make verify` + `make e2e` (`GNITZ_WORKERS=4`).
