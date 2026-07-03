# Sorted-merge walks: unification verdict and ownership-doc repair

## Verdict up front

**Do not build a shared merge skeleton across the engine's sorted-walk families.
The unification this plan was commissioned to design already exists — inside
each family — and the residual "duplication" is deliberate specialization with
distinct semantics, distinct k-regimes, and distinct comparator strategies.**
The one plausible cross-family migration (the exchange relay walk onto the
storage consolidation kernel) is rejected below with numbers.

What survives as implementation work, in two parts. **(a) Ownership-doc
repair.** The ownership rules that make this verdict true are recorded in code
comments that cite a section ("§8 cluster N") of a document that no longer
exists; six comment sites carry dangling references, repaired below so each
family's ownership contract is self-contained at its definition site. **(b)
Audited primitive fixes.** Auditing this plan surfaced three
behaviour-preserving speedups to the shared merge-walk key primitives
(`pack_pk_be`, `from_opk`) plus one drop-order correctness fix in the preflight
merge; all are folded in below, each gated by an existing test.

## The verified merge-walk inventory

The engine contains exactly five sorted-walk families. Each is internally
unified behind one body; no sixth hand-rolled walk exists. Verified by two
sweeps: (a) all `compare_pk_bytes` / `compare_pk_ordering` / `pk_sort_key`
users across `ops`, `storage`, `runtime`, `catalog`, `query` — the hits
outside these families are point lookups, sorts, and doc references to the
ordering contract, not merge loops; (b) all `BinaryHeap` / `Ord`-keyed loops
over sorted key streams — which is what surfaces Family E, whose `PkBuf: Ord`
ordering is invisible to sweep (a). `drain_index_scan`
(`runtime/orchestration/master/mod.rs:474`) is sequential per-worker
concatenation, not a merge; the WAL replay does not merge; upsert re-sorts are
sorts.

### Family A — N-way consolidating merge (storage)

One body: `drive_merge` over the keyless `LoserTree`
(`storage/repr/heap.rs:232`, `:48`), driven by `run_merge`
(`storage/repr/merge.rs:526`). Semantics: fold equal-(PK, payload) groups,
sum weights, drop ghosts (§4b/§4c of the foundations). Consumers — all of
them call it, none re-implements it:

- MemTable snapshot merge: `merge_batches` (`storage/repr/merge.rs:628`).
- Shard compaction, single- and multi-target: `compact_routed` /
  `compact_shards` / `merge_and_route` (`storage/lsm/compact/merge.rs:168`),
  whose module doc names `run_merge` "the sole pending-group drain owner".
- Read path: `UnifiedCursor` (`storage/lsm/read_cursor/mod.rs:588`).

### Family B — N-way order-preserving routed scatter (exchange relay)

One body: `relay_walk_inner` (`ops/exchange/relay.rs:204`), driven by
`relay_scatter_merge_walk` for every consolidated relay scatter. Semantics:
**no folding** — every row of K consolidated sources is emitted exactly once,
in global PK order, routed to a per-worker bucket. Comparator: a per-source
cached `pk_sort_key` u128 winner scan (register compare per step), tie-broken
by `compare_pk_ordering` on the live OPK bytes, then source index; plus a
single-source bulk-drain fast path that emits without any comparison.

### Family C — 2-way delta×trace equal-key cogroups (ops)

One body set: `cogroup_intersection` / `cogroup_left` / `cogroup_union`
(`ops/cogroup.rs:116`, `:153`, `:181`) over the `SortedKeyStream` galloping
skip. Semantics: bracket equal-PK groups of two sorted streams and hand each
`(key, left-group, right-group)` to an operator callback. Consumers: the equi
delta-trace join (`ops/join/delta_trace.rs`), the weight clamp
(`ops/distinct.rs`), the sorted union merge (`ops/linear.rs`), the natural-PK
reduce reads.

### Family D — band-join sweeps (range join)

Two strategy bodies behind one size-adaptive selector:
`range_per_row_seek` (`ops/join/range.rs:81`) and `range_merge_walk`
(`ops/join/range.rs:138`). Semantics: **interval** matching — each delta row
(or delta eq-group) matches a `[start, end)` trace span from
`range_cut_points`, and the merge-walk emits a contiguous **delta sub-range
per trace row** via a monotone pointer. The equal-key group contract of
Families A–C does not apply; the file's own doc (`range.rs:23-31`) records
that the two strategies are structurally distinct algorithms and the selector
(`n > trace_len`, a per-epoch runtime quantity) is load-bearing.

### Family E — master preflight unique-check merge

One body: `merge_index_scan`
(`runtime/orchestration/master/preflight.rs:172-215`): a
`BinaryHeap<Reverse<(PkBuf, usize)>>` K-way merge over the per-worker sorted
key streams of a unique-index preflight, detecting adjacent-equal duplicates
with early exit on the first hit. It cannot ride `drive_merge` and must not
try: it `await`s frame continuations from the reactor **mid-merge** (a
synchronous drain kernel cannot pump an async source), its sources are wire
frames rather than `MemBatch`es, and its emission is a duplicate verdict, not
rows. It shares only the concept "K-way ordered walk", none of the mechanics.

## Why cross-family unification is rejected

The families differ on every axis that decides a merge kernel's shape
(Family E is omitted from the table — its async-source/wire-frame/early-exit
shape is covered in its section above and shares no mechanics with A–D):

| | A `drive_merge` | B `relay_walk_inner` | C `cogroup_*` | D range walks |
|---|---|---|---|---|
| Arity | N-way | N-way | 2-way | 2-way |
| Key relation | equal (PK, payload) | equal PK order only | equal PK | interval |
| Weight handling | fold + ghost-drop | passthrough | callback-owned | multiply per pair |
| Winner select | loser tree, byte compare | cached-u128 linear scan | galloping `advance_to` | cut-point seek / monotone pointer |
| k regime | shards/runs (10s) | worker slices (≤ workers) | 2 | 2 |
| Emission | one row per surviving group | every row, routed | per-group callback | join rows per match pair |

### Rejected: migrate `relay_walk_inner` onto `drive_merge`

The only candidate with real shared shape (both are N-way PK-ordered walks).
Concretely evaluated:

- **The cached-key comparator is borrow-impossible inside `drive_merge`.**
  The loser tree is keyless — its `less` is an `Fn` captured by the
  `LoserTree` operations while `advance` holds the only `&mut` borrow of the
  caller's cursor state; `heap.rs:16-22` documents keylessness as exactly this
  constraint. The relay walk's whole design is the opposite trade: its sources
  are immutable `MemBatch`es, so a per-source `order_cache` of `pk_sort_key`
  u128s makes the winner pick a register compare at its k ≤ workers regime.
  Inside `drive_merge`, `less` cannot read what `advance` mutates, so any
  migration is forced onto the byte-compare comparator — turning the relay's
  zero-byte-read cached common path into ~2 `compare_pk_ordering` calls +
  4 `get_pk_bytes` reads per emitted row at k=4. The regression is
  structural, not tunable.
- **Semantics mismatch.** Family B must not fold: cross-source duplicate PKs
  stay separate rows (`op_relay_scatter_consolidated_mode`'s doc,
  `relay.rs:368-370`, and the `single_source` consolidation certification at
  `relay.rs:411-446` depend on this). Driving `drive_merge` with
  constant-`false` `same_pk`/`eq_payload` closures makes the fold loop
  dead code — LLVM removes it, but the source then reads as "a consolidating
  merge with consolidation disabled", which is strictly harder to understand
  than a dedicated 72-line walk whose doc states its own contract.
- **The bulk-drain fast path does not transfer.** `relay_walk_inner` drains a
  sole surviving source with zero comparisons (`relay.rs:217-224`).
  `drive_merge` cannot adopt an equivalent unconditionally: with folding
  enabled (every Family-A consumer) a single source still requires the
  pending-group drain to fold intra-source adjacent duplicates.
- **Net code delta ≈ −30 lines** (delete the 72-line walk; add the
  `LoserTree` build, the byte-compare `less`, and the routing `emit` adapter
  — ~35-40 lines), for a cross-layer behavior dependency on the exchange hot
  path that then carries a perf-proof burden (isolated bench + codegen
  inspection) it cannot meet given the comparator point above. `LoserTree::build`
  also heap-allocates two Vecs per call where the relay walk is
  allocation-free (fixed stack arrays).
- **It relitigates two standing decisions.** The completed engine-architecture
  refactor recorded `drive_merge` as the sole pending-group drain owner (that
  wording survives in `compact/merge.rs:7`) and separately protected
  `relay_walk_inner` as a monomorphized per-row kernel that must not be
  wrapped or re-abstracted. The current `relay_walk_inner` body is itself the
  product of a deliberate width-agnostic unification of the relay scatter
  paths (commit `c6642fbc`), which chose the cached-key scan over the loser
  tree with the rationale in its message and in `relay.rs:189-201`.

### Rejected: migrate the range merge-walk onto the cogroup skeletons

`range_merge_walk` is not an equal-key cogroup: its "group" is a cut-point
interval over the trace, its inner loop emits a contiguous delta sub-range per
trace row, and the four `RangeRel` arms move the sub-range boundary in
opposite directions (`range.rs:198-217`). Fitting this into `SortedKeyStream`
would mean widening the trait with interval semantics that no other consumer
uses — a strictly worse abstraction than the current self-contained algorithm.
`range.rs:23-31` already documents this boundary; no code change.

## Implementation: self-contained ownership docs

The decisions above are enforced today by module docs — but six of them cite
"§8 cluster N" of a retired document, so the contract text is unreachable.
Replace each dangling reference with the self-contained rule. Exact edits:

1. `crates/gnitz-engine/src/query/vm/exec.rs:1-2`

   ```rust
   //! Epoch execution: `execute_epoch` / `execute_epoch_multi` and the opcode
   //! dispatch loop — kept whole: boxing per-opcode handlers or splitting the
   //! match arms would break monomorphization of the dispatch loop.
   ```

2. `crates/gnitz-engine/src/storage/lsm/compact/merge.rs:7-9` (the
   parenthetical spans three lines) — delete the dead pointer, keep the rule:

   ```rust
   //! [`run_merge`](super::super::merge::run_merge) (the sole pending-group
   //! drain owner; re-extracting a local drain loop would fork the
   //! (PK, payload) total order); this module only drives it and materializes
   ```

3. `crates/gnitz-engine/src/ops/join/rowwrite.rs:1-3`

   ```rust
   //! Shared inner-join row writer. Kept `#[inline]` so the per-row column-copy
   //! loops fold into their callers across the join split — the "no per-row
   //! cross-file call boundary" guarantee.
   ```

4. `crates/gnitz-engine/src/ops/distinct.rs:543`

   ```rust
   // Wide-PK distinct tests
   ```

5. `crates/gnitz-engine/src/catalog/write_path.rs:6-7` — the invariant is
   already stated in full by the surrounding sentence; drop only the dead
   parenthetical:

   ```rust
   //! `sys_tables.rs` / `apply_context.rs`. No second ingest entry point may
   //! skip this precheck/hooks path.
   ```

6. `crates/gnitz-engine/src/runtime/orchestration/worker/exchange.rs:1-3` —
   drop the dead sentence; the module doc already names the machinery:

   ```rust
   //! Worker exchange-wait re-entry: the defer-then-replay machinery
   //! (`do_exchange_wait` inline dispatch loop + `dispatch_deferred` /
   //! `replay_deferred_ticks`).
   ```

Additionally, extend `ops/exchange/relay.rs:189`'s `relay_walk_inner` doc with
one sentence stating the family boundary, so the next reader doesn't
re-propose the migration this plan rejects:

```rust
/// Deliberately NOT `storage`'s `drive_merge`: this walk must not fold
/// (cross-source duplicate PKs stay separate rows), its sources are immutable
/// batches (so a cached-key winner scan beats the keyless byte-compare tree at
/// k ≤ workers), and the single-source bulk drain has no folding-safe
/// equivalent in a consolidating kernel.
```

## Implementation: audited primitive fixes

Three fixes to the shared merge-walk key primitives, validated against source
and the existing equivalence tests. All preserve observable behaviour (the
drop-order fix is a compile-time reorder over a `Drop`-free `MemBatch`); each is
pinned by a test already in the `make verify` run.

### Fix 1 — `pack_pk_be`: register-load the wide and common-narrow widths

`crates/gnitz-engine/src/schema/key.rs`. The wildcard arm packs every width
other than 8/16 through a 16-byte stack buffer and a runtime-length
`copy_from_slice` — including all `> 16` keys (whose leading 16 bytes are a plain
register load) and the common `u16`/`u32` single-column PKs. `pack_pk_be` is the
packer behind `pk_sort_key` (relay `order_cache`, `sort_consolidate_inner` sort
keys) and `compare_pk_ordering` / `pk_bytes_eq` (every N-way merge comparison),
so the stack copy sits on the hot comparator path. Fold the `16` arm and the
`> 16` case into one `len >= 16` register load (which also drops the wildcard's
now-dead `len.min(16)`), and add `2`/`4` arms for the narrow scalars:

```rust
#[inline(always)]
pub(crate) fn pack_pk_be(pk_bytes: &[u8]) -> u128 {
    match pk_bytes.len() {
        8 => (u64::from_be_bytes(pk_bytes[..8].try_into().unwrap()) as u128) << 64,
        len if len >= 16 => u128::from_be_bytes(pk_bytes[..16].try_into().unwrap()),
        4 => (u32::from_be_bytes(pk_bytes[..4].try_into().unwrap()) as u128) << 96,
        2 => (u16::from_be_bytes(pk_bytes[..2].try_into().unwrap()) as u128) << 112,
        // 1/3/5/6/7/9..=15: odd narrow widths — pad-and-copy (len < 16 here, so
        // the whole slice is copied and the old `len.min(16)` is unnecessary).
        len => {
            let mut buf = [0u8; 16];
            buf[..len].copy_from_slice(pk_bytes);
            u128::from_be_bytes(buf)
        }
    }
}
```

Update the doc comment's "`{8, 16}` arms" wording to "`{2, 4, 8, ≥16}` arms".
`pack_pk_be_specialization_matches_naive` (widths 1/2/4/8/16/24/80) already pins
every arm value-identical to the pad-and-copy. `u128::from_opk` is `pack_pk_be`,
so it inherits the `≥16` register load for free.

### Fix 2 — `PkSortKey::from_opk`: register-load the dominant seek widths

`crates/gnitz-engine/src/schema/key.rs`. `from_opk` is the comparator body of the
OPK seek fast path: `lower_bound_opk` / `gallop_opk`
(`storage/repr/columnar.rs`) call it once per binary-search / gallop step. The
`u64` and `[u128; 2]` impls pad-copy through a stack buffer even at the exact
dominant stride (8 / 32 bytes). Add the exact-width register load:

```rust
impl PkSortKey for u64 {
    #[inline(always)]
    fn from_opk(opk: &[u8]) -> u64 {
        // Dispatched only for strides ≤ 8; the `== 8` arm loads the dominant
        // U64/I64 key straight into a register (no memcpy). The narrower strides
        // left-align at the MSB end so a raw `u64` compare is the OPK byte order.
        if opk.len() == 8 {
            u64::from_be_bytes(opk.try_into().unwrap())
        } else {
            let mut x = [0u8; 8];
            x[..opk.len()].copy_from_slice(opk);
            u64::from_be_bytes(x)
        }
    }
}

impl PkSortKey for [u128; 2] {
    #[inline(always)]
    fn from_opk(opk: &[u8]) -> [u128; 2] {
        // Dispatched only for 17..=32-byte strides: hi = the full leading 16 bytes
        // (always a register load), lo = the trailing 1..=16 left-aligned. Array
        // `Ord` is lexicographic, so the low limb settles a leading-16-byte tie a
        // bare `u128` prefix would tie on.
        let hi = u128::from_be_bytes(opk[..16].try_into().unwrap());
        if opk.len() == 32 {
            [hi, u128::from_be_bytes(opk[16..32].try_into().unwrap())]
        } else {
            let mut lo = [0u8; 16];
            lo[..opk.len() - 16].copy_from_slice(&opk[16..]);
            [hi, u128::from_be_bytes(lo)]
        }
    }
}
```

`lower_bound_opk_matches_byte_search` (strides 4/8/12/16/24/32/40) drives
`from_opk` through both arms against the byte oracle and pins them. The `u128`
impl needs no change — it delegates to `pack_pk_be`.

### Fix 3 — `PreflightKeyStream`: drop the view before its backing slot

`crates/gnitz-engine/src/runtime/orchestration/master/preflight.rs`. `mb` is a
lifetime-erased zero-copy view into `slot`'s W2M ring bytes; the struct doc and
`attach_frame` both state "the view must die before its backing slot". Rust
drops fields in declaration order, but `slot` is declared before `mb`, so the
struct's own drop glue releases the ring slot *before* the view — the reverse of
the invariant. `MemBatch` has no `Drop` today, so this is latent rather than a
live use-after-free, but the ordering must be correct by construction. Declare
`mb` before `slot`:

```rust
struct PreflightKeyStream {
    /// Worker index (error attribution) and scan request id (frame pulls).
    w: usize,
    req_id: u64,
    /// Zero-copy view of the current frame's key batch (`None` for an empty
    /// terminal frame or after a decode error). Declared before `slot` so it
    /// drops first — it borrows `slot`'s ring bytes with its lifetime erased.
    mb: Option<crate::storage::MemBatch<'static>>,
    /// Ring slot backing `mb`. Holding it parks the frame's ring bytes.
    slot: Option<W2mSlot>,
    /// Cursor into `mb`.
    row: usize,
    /// Current frame is non-terminal: status 0 and no FLAG_SCAN_LAST.
    has_more: bool,
}
```

`new` uses named-field init, so only the declaration order changes.

### Evaluated and rejected

- **`pk_bytes_eq(a, b)` → `a == b`.** The ≤16-byte PK is the dominant case, and
  on the runtime-width slices `get_pk_bytes` yields, `a == b` lowers to an
  out-of-line `bcmp` call whereas `compare_pk_ordering` inlines to a register
  `pack_pk_be` compare — `run_merge_body`'s `same_pk` (`storage/repr/merge.rs`)
  states exactly this and deliberately does not use raw `==`. The redundant
  byte-swap the audit flags is already elided by LLVM in the equality-only path;
  the wide-key double work it correctly notes is removed by Fix 1 above without
  regressing the ten hot per-row `pk_bytes_eq` sites (cogroup, reduce, band-join,
  read-cursor, point-lookup bracketing).
- **`relay_walk_inner` tiebreak: skip `compare_pk_ordering` on a ≤16 cache
  tie.** The tie arm runs only on a cross-source duplicate PK (rare), the
  "redundant" work is two register `pack_pk_be`s (cheaper still after Fix 1), and
  the replacement adds a per-tie `len` branch and ~7 lines — net worse to read
  for a saving on a cold path.
- **Thread-local `SortEntry` scratch in `sort_consolidate_inner`.** Unmeasured;
  adds a thread-local plus a `borrow_mut`/clear/reserve dance (net more code) and
  duplicates what the batch pool already exists to do. No benchmark shows this
  flush-path allocation is material.
- **`SmallVec` for `LoserTree`.** The dominant repositioning pattern rebuilds
  nothing — `advance_to`'s forward fast path (`seek_forward_multi`) maintains the
  tree in place at zero allocation — so the two build `Vec`s are paid only on the
  non-dominant rewinds / backward / absolute seeks and once per compaction.
  `tree` is read per node on the hot `walk_up` / `drive` loop, where a `SmallVec`
  inline-vs-spilled discriminant on every access risks costing more than the rare
  small allocation it removes. Not worth a new dependency.

## Verification

`make verify` (fmt + clippy-as-errors + full unit tests) must pass. The
ownership-doc edits are comment-only. The primitive fixes are
behaviour-preserving and each is pinned by an existing test in the same run:
`pack_pk_be_specialization_matches_naive` and the `opk_proptest` module
(`schema/key.rs`) for Fix 1, `lower_bound_opk_matches_byte_search`
(`storage/repr/columnar.rs`) for Fix 2, and Fix 3 is a compile-time reorder over
a `Drop`-free `MemBatch`. No e2e run required beyond the pre-commit gate.
