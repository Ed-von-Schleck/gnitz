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
cursor, never the positioned source). `ColumnarSource` survives for `run_merge` /
`merge_same_pk` / `RowRef` and is **no longer re-exported from `crate::storage`**;
it is storage-internal.

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
this was **measured**, not assumed: its `-O0` body is a `0xf8`-byte frame whose
bounds/spill ceremony `always` does not shrink, so promoting it grew the caller
50 → 151 instructions to save ~10. `FixedInt::decode_le_i64` gained a plain
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
`&batch.as_mem_batch()` call is `ScalarFunc::run_filter`, which returns `()`.

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

`SchemaFacts` lands in **step 3**, alongside the five functions that become
generic over it — its only consumers. Defining it earlier would put an
eight-method duplicate API surface on `SchemaDescriptor` that no generic call
site could reach.

```rust
/// Exactly the schema surface `resolve`, `validate` and
/// `is_strictly_non_nullable` read.
pub trait SchemaFacts {
    /// `program.rs:645,813`.
    fn locate(&self, ci: usize) -> ColumnLocator;
    /// `program.rs:664,739,749,764,776`; `check_col_payload` `:1037`.
    fn is_pk_col(&self, ci: usize) -> bool;
    /// Dense payload slot of `ci`, or `PAYLOAD_MAPPING_PK_SENTINEL` for a PK
    /// column. `program.rs:669,744,754,770,782-783`.
    fn payload_mapping_byte(&self, ci: usize) -> u8;
    /// Inverse of the above. `program.rs:1317`.
    fn payload_col_idx(&self, pi: usize) -> usize;
    /// `program.rs:880,1317`.
    fn num_payload_cols(&self) -> usize;
    /// `program.rs:1020`.
    fn num_columns(&self) -> usize;
    /// **`u8`, not `TypeCode`** — it feeds `reg_tc: [u8; MAX_REGS]` and is
    /// compared against `type_code::U64` (`program.rs:634,644,670,940,1048`).
    fn col_type_code(&self, ci: usize) -> u8;
    /// `program.rs:1317`.
    fn col_nullable(&self, ci: usize) -> bool;
}
```

Implement every method of both traits `#[inline]`.

### `gnitz-expr` modules

- **`view.rs` (DONE)** — `RowSource`, `BatchView`, `assert_batchview_consistent`.
- **`locator.rs` (DONE)** — the shared **resolved-addressing substrate**, moved
  from engine `schema.rs`; every body touches only `gnitz_wire` + the row
  accessors (no `SchemaDescriptor`, no `MemBatch`, no log macro):
  - `ColumnLocator` — `pub(crate)` → `pub`; its
    `Pk { byte_off, size, type_code }` / `Payload { slot, size, type_code }`
    fields are now cross-crate-public. Methods `size`, `type_code`, `is_null`,
    `bytes`, `native_le_bytes`, `native_key`, `route_key` — the last five
    retargeted from `&impl RowView<'b>` to `&impl RowSource` with the exact
    signatures given in the trait section (`bytes` and `native_le_bytes` keep an
    explicit batch lifetime). The `size_of::<ColumnLocator>() <= 8` static assert
    travelled with it. `size`/`type_code`/`is_null`/`bytes` are
    `#[inline(always)]`; the other three keep `#[inline]`.
  - `PAYLOAD_MAPPING_PK_SENTINEL`.

  `Instr::CopyCol` (`program.rs:390`) carries a `ColumnLocator`, so the type must
  sit in this crate; `eval_batch` never reads one (`CopyCol`/`Emit` are no-ops at
  `batch.rs:533` — engine `plan.rs`'s `evaluate_map_batch`/`copy_column`
  materialize them).

  **The four key derivations live in `gnitz-wire`, not here** (`pk_route_key`,
  `payload_route_key`, `pk_native_key`, `payload_native_key`, with the
  ROUTING-vs-INDEX key-space block comment). An earlier draft kept them beside
  `ColumnLocator` on the theory that the orphan rule required it — but the orphan
  rule constrains the inherent `impl`, not four non-generic free functions, and
  `gnitz-expr` depends on `gnitz-wire` so the methods reach them either way. They
  are thin dispatches over `encode_pk_column` / `decode_pk_column` /
  `widen_pk_be` keyed on a wire type code, and **zero** of their 12 caller files
  is expression code (`ops/util.rs`, `ops/reindex.rs`, `ops/exchange/relay.rs`,
  `runtime/.../{index_router,mod,preflight,unique_filter}.rs`, `catalog/tests/`),
  so `gnitz-wire` owns them under exactly the rule it states for `read_u64_le`.
  **Engine call sites name `gnitz_wire::X` directly** — there is no engine
  re-export, exactly as for the German-string cluster in step 1. (An earlier
  version routed three of the four through `foundation::codec` and left
  `pk_route_key` naming `gnitz_wire::`, which split one rule across two import
  paths — visibly, in adjacent match arms of `ops/reindex.rs`. The four exist to
  agree with each other pairwise; they are one item, not four.) All four keep
  `#[inline]` for MIR export.

  **`gnitz_wire::null_word_get` / `null_word_set` are the one null-bitmap
  convention**, homed the same way and for the same reason: the payload bitmap is
  a §6 region rule (`gnitz-wire` already owns `REG_NULL_BMP`), and it had been
  spelled out three times — engine `schema::{null_bit, set_null_bit}` (deleted),
  `gnitz_core::{null_word_get, null_word_set}` (now a re-export, so its ~23 call
  sites are unchanged), and inline in `ColumnLocator::is_null`.

  **`read_signed` and `read_unsigned` both move to `gnitz-wire` in step 3** —
  never to `gnitz-expr`, and never split. This is the same rule the four key
  functions above already follow, so step 3 is now a precedent-following move
  rather than a new decision. Their caller files have nothing to do with
  expressions (`schema/key.rs`, `storage/repr/shard_file.rs`, `ops/util.rs`,
  `storage/repr/columnar.rs`), and most dispatch between the *pair* inside one
  function; `shard_file.rs`/`key.rs` importing from an *expression* crate would be
  bad cohesion. `gnitz-wire/src/lib.rs:54-57` already declares itself owner of
  exactly this species, next to `read_u64_le`. The engine will name them
  `gnitz_wire::read_signed` / `read_unsigned` **directly**, as it now does for the
  four key derivations and the German-string cluster — no `foundation::codec`
  re-export, which would only re-split one rule across two import paths.
  `read_unsigned_zero_extends` moves with them. `ops/reduce/agg.rs` is **no longer
  a caller**: its SUM widening now holds a `gnitz_wire::FixedInt` (resolved once
  at `Accumulator::new`), which *is* the domain on which "decode LE bytes → i64"
  is total and carries its own width — so it replaced the hand-rolled
  `is_fixed_int`/`is_signed_int`/`read_signed`/`read_unsigned` classifier and the
  duplicate of it in `decode_signed`. The deferral is still forced:
  `read_signed`'s only expression-side consumer is in `expr/batch.rs`, a file
  step 3 moves.
- **`program.rs`** — `LogicalProgram`, `ResolvedProgram`, `Instr`, `CmpOp`,
  `StrOp`, `LogicalInstr`, `ExprValidateErr`, `from_wire` (`:470`), `resolve`
  (`:627`), `validate` (`:860`), the register analyses. Edits:
  - `resolve(self, schema: &SchemaDescriptor, is_filter: bool)` →
    `resolve<S: SchemaFacts>(self, schema: &S, is_filter: bool)`; likewise
    `validate` (`in_schema`/`out_schema: Option<&S>`), `check_col_in_range`
    (`:1018`), `check_col_payload` (`:1034`), `is_strictly_non_nullable`
    (`:1313`).
  - **`assembled` needs a phantom implementor, not a split `validate`.**
    `LogicalProgram::assembled` (`:442`) calls `prog.validate(None, None)`, where
    a generic `S` is unconstrained (`E0282`). Add, **private to `gnitz-expr`**
    (never exported):
    ```rust
    /// Sole purpose: give `assembled`'s schema-free `validate(None, None)` a
    /// concrete `S`. Every method is unreachable — the `Option`s are `None`.
    struct NoSchema;
    impl SchemaFacts for NoSchema {
        // every method: underscore-prefixed params (`_ci`, `_pi`) — bare names
        // trip `unused_variables` under `warnings = "deny"` — and a body of
        // `unreachable!("NoSchema")`, which coerces to any return type.
    }
    ```
    and call `prog.validate::<NoSchema>(None, None)?`. This keeps `validate` as
    **one exhaustive `match`**, preserving the invariant its doc comment names
    ("in one exhaustive `match self` (a new opcode cannot silently bypass a
    bound)", `:852-853`). Splitting the method into a structure pass and a schema
    pass would require duplicating that `match` — the schema-free and
    schema-dependent checks are interleaved per arm (e.g. `L::LoadColInt` at
    `:936-945` does `check_reg` then `check_col_in_range` then the
    `ColTooWideForRegister` test) — and a `_ => {}` arm in the second copy would
    destroy the invariant.
  - direct field/method reads become trait calls: `schema.columns[ci].type_code`
    (`:644,670,940`) → `schema.col_type_code(ci)`;
    `schema.columns[…].nullable != 0` (`:1317`) → `schema.col_nullable(…)`.
  - `crate::schema::type_size(tc) as usize` (`:940`) →
    `gnitz_wire::wire_stride(tc)` — **drop the `as usize`**: engine `type_size`
    returns `u8`, `wire_stride` returns `usize` (`gnitz-wire/src/types.rs:551`).
  - `crate::schema::type_code` (`:628,1048,1055`) → `gnitz_wire::type_code` —
    a **module of constants**, not a function; the `use` line moves, the call
    sites are unchanged.
  - `encode_german_string` (imported at `:10`, used at `:827` to build
    `const_cells`/`const_blob`) already names `gnitz_wire` — the `use` line moves
    unchanged.
  - delete the lone `gnitz_debug!` (`:416`, in `LogicalProgram::new`) — the only
    log macro in `program.rs`/`batch.rs`.
  - `ExprValidateErr`'s doc comment (`:29-31`) currently says "`Debug` only … no
    consumer branches on the variant". The client now branches on
    `TooManyRegs(n)` (below), so update it.
- **`batch.rs`** — `eval_batch` (`:443`), `EvalScratch` (`:24`), every kernel.
  **Five** signatures take `mb: &MemBatch` and all five become `<B: BatchView>`:
  `eval_is_null` (`:195`), `eval_str_cmp<'x>` (`:328`), `eval_str_col_vs_const`
  (`:354`), `eval_str_col_vs_col` (`:387`), `eval_batch` (`:443`). Body edits:
  `mb.blob` → `mb.blob()` at `:380` and `:406`; drop the `as usize` on the
  **three** `type_size` → `wire_stride` calls at `:543,587,613` (the third is
  `Instr::LoadPk`'s). (`col_data` at `:379,405,415,545,588`, `null_bmp` at
  `:212,341,580,602`, `get_pk_bytes` at `:625,638` are already method calls.) The
  `&self`-tied returns are borrow-check-safe: `eval_str_col_vs_col` holds `col_b`,
  `blob` and a `move` closure live across a second `mb` pass, which under
  `BatchView` are shared reborrows of `*mb` unified at one lifetime — exactly what
  `b_of: impl Fn(usize) -> (&'x [u8], &'x [u8])` needs. The **four**
  `#[allow(clippy::too_many_arguments)]` attributes at `:194,327,353,386` travel
  with the code (`:102` is `type_complexity` on `reg4`, unrelated). Remaining leans retarget to `gnitz_wire::{read_u64_le,
  read_signed, wire_stride, is_signed_int, decode_pk_column, widen_pk_be,
  encode_german_string, compare_german_strings}` and
  `locator::PAYLOAD_MAPPING_PK_SENTINEL` — `read_signed` moves to `gnitz-wire`
  (with `read_unsigned` and `read_unsigned_zero_extends`) in this same step, not
  to `locator.rs`.
- **`filter.rs`** — two entry points lifted out of engine `plan.rs`:
  ```rust
  /// Invokes `append_range(start, end)` per maximal run of passing rows —
  /// `end` is EXCLUSIVE, not a length (`scan_filter_bits` passes `row_base` /
  /// `abs` / `n`; the engine consumer is `|start, end| out.push((start, end))`).
  pub fn filter_batch<B: BatchView, F: FnMut(usize, usize)>(
      prog: &ResolvedProgram, mb: &B, n: usize,
      scratch: &mut EvalScratch, mut append_range: F,
  )
  /// Run `prog` over row `row` alone and read its result register.
  pub fn eval_scalar_row<B: BatchView>(
      prog: &ResolvedProgram, mb: &B, row: usize, scratch: &mut EvalScratch,
  ) -> (i64, bool)   // (value or float bits, is_null)
  ```
  `filter_batch` is `ScalarFunc::run_filter`'s body verbatim after the
  `Repr::Predicate` destructure and the `scratch.borrow_mut()` — it touches only
  `prog.{no_nulls, num_regs, result_reg}` and the scratch. `append_range` must be
  bound `mut` (it is passed as `&mut append_range` to `scan_filter_bits`). The
  private `scan_filter_bits` (a free fn closing over nothing) moves with it. Both
  handle `n == 0`: `ensure_capacity` sizes `regs` by `num_regs * MORSEL`
  independent of `n`, the morsel loop is `(0..n).step_by(MORSEL)`, and
  `scan_filter_bits` over an empty bit slice emits nothing.

  `eval_scalar_row` is the existing test helper `eval_predicate_via_batch`
  (`expr/tests/mod.rs:22-33`) promoted to production: `num_regs == 0` returns
  `(0, true)`; else `scratch.ensure_capacity(prog.num_regs as usize,
  /* no_nulls = */ false, 1)` — always the nullable arm, so `null_bits` and
  `bool_bits` are allocated whatever `prog.no_nulls` says; then
  `eval_batch(prog, mb, row, 1, scratch)` and read
  `(scratch.regs[r*MORSEL], scratch.null_bits[r*NULL_WORDS_PER_REG] & 1 != 0)`.
  Forcing the nullable arm is safe on two counts: every kernel branches on
  `scratch.no_nulls` (set by `ensure_capacity`, `batch.rs:46`), never
  `prog.no_nulls` (which only `filter_batch` reads); and reusing one scratch
  across rows cannot leak a stale null bit, because every producer **assigns**
  its destination null word rather than OR-ing into it
  (`fill_null_bits_mask` `:186`, `null_or2` `:145`, `null_copy1` `:158`,
  `clear_null_reg` for `LoadPk`/`LoadConst` `:642,652`).
- **`lib.rs`** — the modules (`locator`, `view`, `program`, `batch`, `filter`)
  as private `mod`s plus a flat `pub use module::*;` of each.

### The public surface: 37 named items

Stated as a number rather than implied: 37 named items, plus the 7
`ColumnLocator` methods and 14 trait methods listed above. This is **not smaller
than before the extraction** — it is the same code with a wider visibility
keyword; `gnitz-expr` is an internal workspace crate with no stability contract.
What matters is that the `unsafe` and optimizer-internal items stay private.

- Types (12): `ColumnLocator` (+ its 6 variant fields), `RowSource`, `BatchView`,
  `SchemaFacts`, `LogicalProgram`, `LogicalInstr`, `Instr`, `ResolvedProgram`,
  `EvalScratch`, `CmpOp`, `StrOp`, `ExprValidateErr`.
- Consts (3): `PAYLOAD_MAPPING_PK_SENTINEL`, `MORSEL`, `NULL_WORDS_PER_REG`.
- Free fns (4): `assert_batchview_consistent`, `eval_batch`, `filter_batch`,
  `eval_scalar_row`. (Neither `read_signed` nor the four `*_route_key` /
  `*_native_key` derivations is among them — all five go to `gnitz-wire`.)
- `LogicalProgram` methods (8): `new` (`:415`), `copy_cols` (`:452`), `from_wire`
  (`:470`), `payload_copy_srcs` (`:597`), `sequential_copy_base` (`:615`),
  `resolve` (`:627`), `validate` (`:860`), and `Debug`/equality as already
  derived. (`copy_cols`, `payload_copy_srcs`, `sequential_copy_base` have
  **production** engine callers in `query/compiler/{emit,mod}.rs`; `new` has
  `#[cfg(test)]` callers in `query/vm/mod.rs:570,1350,1408`,
  `query/compiler/mod.rs:705`, `ops/linear.rs:536,552,767,961`,
  `ops/reduce/tests.rs`, and the expr tests.)
- `ResolvedProgram` (7): fields `instrs`, `num_regs`, `result_reg`, `no_nulls`,
  `bit_only_mask`, `chain_trigger_mask`; methods `is_bit_only`,
  `classify_registers`. (The last three are needed only by the engine-side expr
  tests — `batch_tests.rs:840` reads `bit_only_mask`, `:887` calls
  `classify_registers`, `program_tests.rs:1575,1576,1603,1632,1667,1702` read and
  `batch_tests.rs:1450` **writes** `chain_trigger_mask` to A/B the AND-chain
  skip. See the test decision below.)
- `EvalScratch` (5): `Default`, `ensure_capacity`, fields `regs`, `null_bits`,
  `bool_bits`.

Deliberately **not** public: `const_cells`, `const_blob`, `int_sets`,
`bool_input_mask`, `needs_bool_pack`, `MAX_REGS`, `filter_bits`, `NoSchema`, and
— importantly — the `unsafe` split-borrow helpers `reg_mut` (`batch.rs:62`),
`reg3` (`:68`), `null_words3` (`:81`), `reg4` (`:102`), `clear_null_reg`
(`:122`). Those hand out aliasing split borrows whose soundness rests on an SSA
property enforced only by a `debug_assert!`; nothing outside `gnitz-expr` calls
them (engine `plan.rs` uses only `ensure_capacity` plus direct field indexing).

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
- `impl SchemaFacts for SchemaDescriptor` — forwarding to `locate`
  (`schema.rs:656`), `is_pk_col` (`:571`), `payload_mapping_byte` (`:580`),
  `payload_col_idx` (`:587`), `num_payload_cols` (`:524`), `num_columns`
  (`:393`), `self.columns[ci].type_code`, `self.columns[ci].nullable != 0`.
- `compute_mappings` (`schema.rs:229`) — retarget its one sentinel reference to
  `gnitz_expr::PAYLOAD_MAPPING_PK_SENTINEL`.
- `IndexKeySpec` (`schema.rs:950,981,997`) — its three row-reading methods
  retarget from `RowView` to `&impl BatchView`.
- `expr/plan.rs` in full: `ScalarFunc` (`from_predicate`, `from_map`),
  `evaluate_map_batch`, `map_rows_into`, `append_map_ranges`, `filter_ranges`,
  `covers_all_outputs`, `copy_column`, `NullPerm`, `evaluate_predicate`
  (`#[cfg(test)]`), and its
  `BlobCache` / `BlobCacheGuard` / `relocate_german_string_vec` uses
  (map-materialization only — the filter path never touches the blob cache).
  `run_filter` becomes:
  ```rust
  pub fn run_filter<F: FnMut(usize, usize)>(&self, mb: &MemBatch, n: usize, append_range: F) {
      let Repr::Predicate { prog, scratch } = &self.0 else {
          unreachable!("run_filter on a Map ScalarFunc (the VM dispatch is per-node)")
      };
      gnitz_expr::filter_batch(prog, mb, n, &mut scratch.borrow_mut(), append_range)
  }
  ```
  The two production callers (`catalog/scan_spec.rs`, `ops/linear.rs`) are
  untouched.
- **Re-exports, one rule: each item sits behind the module that owns its
  concept.** `crate::schema` re-exports `ColumnLocator`,
  `PAYLOAD_MAPPING_PK_SENTINEL` and `read_signed` — those *are* schema facts
  (`SchemaDescriptor::locate` produces a locator, `compute_mappings` writes the
  sentinel), so the facade is legitimate and covers the 88 `ColumnLocator`
  references across 14 files unchanged. The four `*_route_key` / `*_native_key`
  derivations deliberately do **not** come through `schema`, and do not come
  through `foundation::codec` either: they are wire byte primitives, so they live
  in `gnitz-wire` and every engine call site names `gnitz_wire::X` — the same home
  and the same spelling step 3 gives `read_signed` / `read_unsigned`, and that
  step 1 already gave the German-string cluster and the null-bitmap accessors. Two
  earlier drafts split this rule by how many call sites would churn rather than by
  ownership: one routed the four through `schema`, the next re-exported three of
  the four through `foundation::codec`.
- **`expr/mod.rs`** shrinks to `mod plan; #[cfg(test)] mod tests; pub use
  plan::{PkFill, ScalarFunc};` (it exports **`PkFill` as well as** `ScalarFunc`
  today — `expr/mod.rs:11` — and must keep both) plus `pub(crate) use gnitz_expr::{ExprValidateErr,
  LogicalProgram};` (`ExprValidateErr` is used by
  `query/compiler/{emit.rs:14,mod.rs:6}`; demoting `LogicalProgram` from `pub
  use` to `pub(crate) use` is equivalent in a binary crate) and the
  `#[cfg(test)] pub(crate) use gnitz_expr::{CmpOp, LogicalInstr, StrOp};`
  re-exports the vm/ops test builders consume.

### The expr test suite stays in `gnitz-engine`, unmoved

Measured: of `batch_tests.rs`'s 28 tests, **13** construct `ScalarFunc`, **12**
build an engine `Batch` and call `eval_batch`, **3** touch neither — and **0**
call `eval_batch` without constructing a `Batch`. `program_tests.rs`'s 56 tests
go through `make_int_batch` (28 of them), `Batch::with_capacity` directly (3),
`SchemaDescriptor::new` inline (1), STRING columns (7, three of which call
`test_support::german_string(b"hello", &mut batch.blob)` — borrowing the batch's
own blob mid-construction), and `schema.payload_columns()` (not a `SchemaFacts`
method). `plan_tests.rs` holds byte-identical private copies of `make_schema` and
`make_int_batch` (`:6`, `:15`) and must stay regardless.

So moving tests is a **rewrite**, not a move: ~120-180 lines of new
`TestSchema`/`TestBatch` fixture in `gnitz-expr`, 12 rewritten `batch_tests`
bodies and ~13 touched `program_tests` bodies, ending with **two** fixture
families where there is one. The cost of keeping them where they are is three
extra `pub` items (`bit_only_mask`, `chain_trigger_mask`, `classify_registers`),
already listed above. Keeping them is the cheaper and simpler choice; take it.

`tests/mod.rs`: `eval_predicate_via_batch` is deleted in favour of
`gnitz_expr::eval_scalar_row`; `eval_with_emit_via_batch` stays (it walks
`Instr::Emit`). Only `use` paths change in the three test files.

`gnitz-expr` therefore ships one small `#[cfg(test)]` smoke test — a hand-built
two-column `BatchView`/`SchemaFacts` pair proving the crate resolves and
evaluates standalone. Its exhaustive coverage lives in `gnitz-engine`'s
`expr/tests/`, which `make test` runs.

**Why the hot path is unchanged.** `eval_batch<MemBatch>` and
`filter_batch<MemBatch, _>` are the only instantiations the server binary
compiles, and generics monomorphize **in the calling crate** — the engine's own
codegen unit — so the vectorized kernel bodies are produced exactly as today,
with or without LTO. The `BatchView` accessors are `#[inline]` and resolve to
`MemBatch`'s inherent bodies. `lto = true` inlines the outer per-morsel call.

## Client side

### `gnitz_core::Schema` impls `SchemaFacts`

From existing methods on `gnitz-core/src/protocol/types.rs`: `is_pk_col`
(`:138`), `num_columns` (`:87`), `num_payload_cols` (`:132`), `payload_idx`
(`:168`), `pk_byte_offset` (`:145`), `payload_columns` (`:178`),
`columns[ci].type_code` / `.is_nullable`.
- `locate(ci)`: mirrors engine `locate` (`schema.rs:656`) exactly — `size =
  columns[ci].type_code.wire_stride() as u8` (engine's `SchemaColumn.size` is
  `type_size(tc) = wire_stride(tc)`, set in `SchemaColumn::new`), then
  `ColumnLocator::Pk { byte_off: pk_byte_offset(ci) as u8, size, type_code: tc as
  u8 }` for a PK column (client `pk_byte_offset` sums PK-list-order
  `wire_stride`s — the same walk as engine `pk_byte_offset`, `schema.rs:641`),
  else `ColumnLocator::Payload { slot: payload_idx(ci) as u8, size, type_code:
  tc as u8 }`.
- `col_type_code(ci)`: `self.columns[ci].type_code as u8` — the client's field is
  a `TypeCode` enum, the trait returns `u8`.
- `payload_mapping_byte(ci)`: `PAYLOAD_MAPPING_PK_SENTINEL` when `is_pk_col(ci)`
  (client `payload_idx` `debug_assert!`s non-PK), else `payload_idx(ci) as u8`.
- `payload_col_idx(pi)` — **`gnitz_core::Schema` has no such method; it must be
  written**, as the inverse of `payload_idx`:
  `self.payload_columns().nth(pi).map(|(_, ci, _)| ci).expect("payload_col_idx:
  pi out of range")` (`payload_columns` yields `(pi, ci, &ColumnDef)`). This is
  load-bearing and silent if wrong: it is the only consumer inside
  `is_strictly_non_nullable` (`program.rs:1315-1317`), i.e. it decides
  `no_nulls`. The obvious wrong implementation (`pi` itself) is correct only when
  the PK sits at column 0; for any interspersed or compound PK it reads another
  column's nullability → null bits silently ignored → wrong rows kept.

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
  `validate(Some(schema), None)` (below), which returns `ColTooWideForRegister`,
  so a future reject-list drift in `lower.rs` (a file fn1/fn2/fn3 all edit)
  surfaces as a typed `Unsupported`, not a panic across the `gnitz-capi` C ABI
  under `panic = "abort"`.

`fill` materializes every string column eagerly rather than only the referenced
ones: `ResolvedProgram` exposes no referenced-column set, and both users are cold
paths.

### Compiling a client program: the shared four lines

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
let logical = LogicalProgram::from_wire(&p.code, p.num_regs, p.result_reg, p.const_strings)
    .map_err(expr_unsupported)?;
logical.validate(Some(schema), None).map_err(expr_unsupported)?;
let prog = logical.resolve(schema, /* is_filter = */ …);
```
The `ExprProgram` fields (`expr.rs:14`: `code: Vec<u32>`, `num_regs: u32`,
`result_reg: u32`, `const_strings: Vec<Vec<u8>>`) are byte-for-byte
`from_wire`'s parameters (`program.rs:470`) — no round trip.

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
`having: Option<&'a ResolvedProgram>` — `ResolvedProgram` has no lifetime
parameter (`program.rs:1065`), so `AggFinish<'a>`'s other `&'a` fields are
unaffected, and `agg_finish.rs:178` is the only reader of `spec.having`
repo-wide. In `select.rs` — after `bind_having_expr`, replacing the
`having_supported` gate and its CREATE-VIEW-advice rejection (`:471-477`) — bind
a local `let having_prog: Option<ResolvedProgram>` where `bound_having` is bound
today, produced by the four lines against `&partial_schema` with `is_filter =
false`, and pass `having_prog.as_ref()` at the construction site (`:522-528`).
This keeps the rejection **pre-dispatch**, as today; compiling inside
`agg_finish` would move it after the fold reply.

`is_filter = false` (not `true`) because HAVING is evaluated row-at-a-time with
`eval_scalar_row`: it demotes `result_reg` out of `bit_only`
(`program.rs:1300-1303`) so `regs[result_reg]` is always live. It does not change
`no_nulls` (`is_strictly_non_nullable` does not read `is_filter`), only register
classification and AND-chain detection.

**Evaluate over a reused one-row batch.** Replace `fill_having_row` +
`eval_having` + `truthy` in the existing single emit loop (`agg_finish.rs:184-194`
— the loop, its `continue` early-out, and `emit_row` all stay):

```rust
// hoisted above the loop, only when spec.having.is_some()
let mut hav = ZSetBatch::new(spec.partial_schema);   // PkColumn::U128s, filler ColData per column
let mut bufs = ViewBuffers::default();
let mut scratch = EvalScratch::default();
…
// inside the loop, replacing the fill_having_row/eval_having/truthy block
if let Some(prog) = spec.having {
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
    let (v, is_null) = eval_scalar_row(prog, &view, 0, &mut scratch);
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
`if let Some(prog)` block and its last use is `eval_scalar_row`, so NLL releases
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
and bit-identical to the engine filter's `bool_bits & !null_bits`: for a boolean
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
engine filter (`pack_to_bool_bits`, `batch.rs:276`) and `eval_scalar_row`
bit-test, and `(-0.0f64).to_bits() != 0`, so the group is kept — a move *toward*
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
- else the four lines with `is_filter = true`, one `ViewBuffers`/`ZSetBatchView`,
  and `filter_batch(&prog, &view, n, &mut EvalScratch::default(), |start, end|
  matched.extend(start..end))` — **`end` is exclusive, not a length**.

`row_passes_residuals` (`:12`) is deleted. Its own doc comment carries the
soundness argument for the fold: the engine `BoolAnd` nullable arm
(`batch.rs:240-243`) yields definite-false when either side is definite-false and
NULL otherwise, and `filter_batch` merges `bool_bits & !null_bits`, so a NULL
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
    Num(ResolvedProgram), // everything else — integer-valued
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
  then the shared `from_wire`/`validate`/`resolve` tail with `is_filter = false`.

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

  `is_filter = false` also keeps a boolean-valued RHS (`SET flag = a AND b`)
  readable: it demotes `result_reg` out of `bit_only` (`program.rs:1303`), so
  `BoolAnd`/`BoolNot` run `unpack_bool_to_regs` (`batch.rs:790,861`) and
  `regs[result_reg]` is live.

- `eval_set_program(p: &SetProgram, view: &ZSetBatchView, batch: &ZSetBatch,
  row: usize, scratch: &mut EvalScratch) -> Result<ColumnValue>`: `Str(s)` →
  `ColumnValue::Str(s.clone())`; `StrCol(c)` → read `ColData::Strings` at `row`
  (as today, `mutate.rs:49-57`); `Num(prog)` → `eval_scalar_row(prog, view, row,
  scratch)` → `(_, true) => Null`, `(v, false) => Int(v)`. `eval_set_expr`
  (`mutate.rs:41`) is deleted.
- `write_set_rows` (`mutate.rs:147`): takes `&[(usize, SetProgram)]`; builds one
  `ViewBuffers` + `ZSetBatchView` over `current` and one `EvalScratch` before the
  row loop; `asn_by_col` becomes `Vec<Option<&SetProgram>>`. `build_merged_row`'s
  closure bound is `F: FnMut` (`mutate.rs:120`), so capturing `&mut EvalScratch`
  is fine.
- `insert.rs`: `BoundUpdateExpr::{Existing(SetProgram), Excluded(SetProgram)}`
  (neither `ConflictPlan` `:28` nor `BoundUpdateExpr` `:44` derives
  `Clone`/`Debug`, and its only uses are `ConflictPlan::DoUpdatePk` `:37`,
  `bind_do_update_assignments` `:328`, `bind_do_update_rhs` `:341,348,366`,
  `client_side_merge_do_update` `:433,436`, `eval_do_update_rhs` `:478,485-486`).
  **The EXCLUDED short-circuit at `insert.rs:348` builds
  `BoundUpdateExpr::Excluded(BoundExpr::ColRef(col_idx))` directly, bypassing
  `bind_mutate_scalar`** — route that bare `ColRef` through the same classifier.
  Hoist **two** `ViewBuffers` (`bufs_excluded`, `bufs_existing`) plus one
  `EvalScratch` above the loop — two are required because the `excluded` view
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

At the end of Sequencing **step 3**, run the engine's `#[ignore]`d expr benches
in `--release`, interleaved A/B against the pre-move commit:
```
cd crates && cargo test -p gnitz-engine --release and_chain_skip_bench \
    -- --ignored --nocapture --test-threads=1
cd crates && cargo test -p gnitz-engine --release str_const_filter_bench \
    -- --ignored --nocapture --test-threads=1
```
(`expr/tests/batch_tests.rs:1363`, `:1463`.) Acceptance criterion: **throughput
parity**. Supporting evidence if a delta shows: `objdump -d` the `eval_batch`
`Cmp` / `IntAdd` / `LoadPayloadInt` arms before and after — expected identical,
since the generic instantiates in the engine's own codegen unit.

### `SchemaFacts` equivalence test (load-bearing)

Client-side `resolve`/`locate` is genuinely new — `gnitz-core`/`gnitz-sql`
reference `resolve`/`ResolvedProgram`/`eval_batch` nowhere today. A silent
OPK-offset or payload-slot mismatch would miscompute rather than error. Add a
`#[cfg(test)]` test **in `gnitz-engine`** (which has `gnitz-core` as a
dev-dependency, so both schema types are in scope) asserting that
`<SchemaDescriptor as SchemaFacts>` and `<gnitz_core::Schema as SchemaFacts>`
agree on **all eight methods** — `locate`, `is_pk_col`, `payload_mapping_byte`,
`payload_col_idx`, `num_payload_cols`, `num_columns`, `col_type_code`,
`col_nullable` — for every `ci`/`pi` over a schema matrix: signed and unsigned
PK; U64, U128, F64, STRING payload columns; **at least one nullable payload
column** (or `col_nullable` is asserted only against `false`); every fixed width
(1/2/4/8/16); a single PK at column 0; a single PK **not** at column 0; and a
compound two-column PK. The last two shapes are where a naive `payload_col_idx`
passes for `payload_idx` and fails here. `payload_mapping_byte` returns `u8`
while `payload_col_idx` takes `usize`, so the round-trip needs a cast.

It must stay a **two-impl equivalence** assertion, not a set of invariants
checked against one impl: invariants cannot catch two self-consistent impls that
disagree, which is exactly the `payload_col_idx` / `col_nullable` failure mode
that silently decides `no_nulls`. It lives in `gnitz-engine`'s test tree because
`gnitz-engine` is binary-only — nothing in `gnitz-core`/`gnitz-sql` can name a
helper defined there — and the engine's `gnitz-core` dev-dependency
(`gnitz-engine/Cargo.toml:26`) makes it the only place both schema types coexist.

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
  (`test_div_mod_by_zero_is_null`:379 and neighbours) onto `eval_scalar_row` and
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
- [ ] **Move `program.rs` + `batch.rs` + the filter entry points** into
  `gnitz-expr`: delete the `gnitz_debug!`; **define `SchemaFacts`, `impl` it for
  `SchemaDescriptor`, and make `resolve`/`validate`/`check_col_in_range`/
  `check_col_payload`/`is_strictly_non_nullable` generic over it** (deferred from
  step 2 — its only consumers move here); **move `read_signed` *and*
  `read_unsigned` (plus `read_unsigned_zero_extends`) to `gnitz-wire`**, with the
  engine naming them `gnitz_wire::` directly (~8 call-site edits); add the
  private `NoSchema` and turbofish `assembled`'s `validate`; make the five
  `mb`-taking fns generic over `BatchView` (the region half — the kernels need
  `col_data`/`null_bmp`, not just `RowSource`); drop the `as usize` on the three
  `wire_stride` calls; apply the public-surface list; update `ExprValidateErr`'s
  doc comment; `filter.rs` gets `filter_batch` + `scan_filter_bits` +
  `eval_scalar_row`; engine `plan.rs` retargets and `ScalarFunc::run_filter`
  wraps `filter_batch`; **`Instr::LoadPk` carries a `gnitz_wire::FixedInt`
  resolved at `resolve` time instead of a raw `tc: u8`** — its body
  (`batch.rs:611-633`) is the last hand-rolled copy of the classify-then-widen
  rule `FixedInt` packages and `SumWiden::Int` already adopted, and folding it in
  here drops the `type_size` / `is_signed_int` / `read_signed` trio from the
  per-instruction path in the same edit that moves the file;
  `expr/mod.rs` shrinks to re-exports (keeping `PkFill`);
  the three expr test files change only their `use` paths and lose
  `eval_predicate_via_batch`. **Run the codegen verification here.** If this step
  is too large, split it 3a (`program.rs` + `SchemaFacts`) / 3b (`batch.rs` +
  `filter.rs` + the codegen A/B).
- [ ] **Client adapter** in `gnitz-core`: `impl SchemaFacts for Schema` (write
  `payload_col_idx`, return `u8` type codes); add `build_pk_region_into` /
  `encode_german_col_into` (`pub(crate)`, existing signatures kept as wrappers);
  `ViewBuffers` + `ZSetBatchView` (implementing **both** `RowSource` and
  `BatchView`) with the `U128s` `unreachable!` arm; export both
  through `protocol/mod.rs` and `lib.rs`. Add `expr_unsupported` in `gnitz-sql`.
  **Run the `SchemaFacts` equivalence test here.**
- [ ] **Swap HAVING**: compile in `select.rs` where `having_supported` was
  (`is_filter = false`); change `AggFinish::having` to
  `Option<&ResolvedProgram>`; replace the `fill_having_row`/`eval_having`/
  `truthy` block with the reused one-row batch + `eval_scalar_row`; delete
  `HavingEval` and its cluster (**keep `Val`**); drop the CREATE-VIEW-advice
  message; flip `adhoc_surface.rs` / `test_sql.py`; delete the two in-module
  `having_supported` tests; add the parity grid, the expected-SQL cases, the
  constant-HAVING cases, the zero-group cases and the register-cap cases.
- [ ] **Swap residual + SET**: `matching_indices` onto `filter_batch` (empty-slice
  and `Ok(None)` → `0..n`; `|start, end|`); add `compile_int_scalar_program` to
  `lower.rs`; `compile_set_programs` inside the RMW closure against
  `actual_schema`; the `insert.rs:348` EXCLUDED classifier; `write_set_rows` /
  `client_side_merge_do_update` onto `eval_set_program` with hoisted
  `ViewBuffers`/`EvalScratch`; delete `exec/eval.rs`; retarget the div/overflow
  tests; add the DML tests.
- [ ] `make verify` + `make e2e` (`GNITZ_WORKERS=4`).
