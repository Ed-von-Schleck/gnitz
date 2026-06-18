# gnitz-engine architectural refactor — master plan

A structural refactor of `crates/gnitz-engine` (~74k non-test LOC, binary crate,
root `src/main.rs`, no `lib.rs`). The goal is to **regroup the engine into a
directory tree that mirrors its eight architectural layers**, **break the one
real dependency cycle**, **narrow the surface between groups to the few edges
that are real**, and **split the god-files** — with **zero behaviour change, zero
performance regression, and zero code duplication**. Every change below is a
relocation, a visibility narrowing, or a file split along an existing cohesion
seam. No hot loop is rewritten; no comparator, cursor, opcode dispatch, or
const-generic kernel is de-monomorphized.

The plan is staged so the load-bearing structural fixes (Stage A) land first and
the large mechanical file splits + directory regrouping (Stage C) land last, each
behind a green build / `make test` / `make e2e` gate. The engine is shippable and
green after **every** workstream.

---

## 1. Current architecture

Eight logical subsystems, layered overwhelmingly downward already, but living as
**14 flat top-level modules** that hide which layer each belongs to:

| Layer (intended) | Subsystem | Big files |
| --- | --- | --- |
| L0 foundation | `util`, `xxh`, `sys`, `log`, `layout` | — |
| L1 data layout | `schema` | schema.rs (1863) |
| L2 storage repr | `storage` (batch/columnar/merge/heap/range_key) | batch.rs (2546), merge.rs (2880), read_cursor.rs (2604) |
| L3 storage LSM | `storage` (table/memtable/shard_*/wal/compact) | table.rs (1834), memtable.rs (1612), shard_index.rs (1644), shard_reader.rs (1449), compact.rs (1349) |
| L4 operators | `expr`, `ops` | join.rs (2717), exchange.rs (2070), linear.rs (1963) |
| L5 query core | `compiler`, `vm`, `dag` | compiler.rs (4403), vm.rs (2968), dag.rs (2396) |
| L6 catalog | `catalog` | store.rs (1965) |
| L7 runtime | `runtime` (master/worker/reactor/sal/w2m/wire) | master.rs (4366), reactor/mod.rs (4235), worker.rs (3084) |

### The six concrete problems (all verified)

1. **C1 — `schema ↔ storage` is a genuine production cycle.** `schema.rs` reaches
   *up* into `storage` at exactly four sites: `:7` `use crate::storage::MemBatch`,
   `:100` `encode_order_preserving_pk`, `:430` `partition_for_pk_bytes`, `:833`
   `&mut crate::storage::PkBuf`. All four are pure layout/key operations or
   dependency-free value types that belong *in or below* schema, not up in storage.
   `storage → schema` is the legitimate downward direction. **This is the only
   production cycle in the engine.**

2. **God-files braiding 3–6 cohesive concerns.** Twelve non-test files exceed 1900
   lines; each fuses cleanly separable clusters (see §6). They are the dominant
   structural debt.

3. **Wrong-direction intra-`storage` edges (three, not two).** `storage/batch.rs`
   (the pure in-memory representation) reaches *down* into the disk half at three
   places, not the two originally documented:
   - `:11` `use super::shard_file` + `shard_file::write_shard_streaming` (`:1475`
     test, `:1482` prod).
   - `super::wal` block-layout calls (`:1494/1509/1562/1579/1606/1748`).
   - **`Batch::append_row_from_ptable_found` (`:1109`) takes
     `&super::partitioned_table::PartitionedTable` (L3)** and reads its
     `found_col_ptr`/`found_null_word`/`found_blob_slice`; prod consumer
     `dag.rs:1848`. A repr method should not know the LSM partitioned-table shape.

4. **Misplaced cross-cutting global state.** `compiler.rs:23/31` own
   `pub static WORKER_RANK` / `NUM_WORKERS` — a multi-process runtime concern
   written by `runtime/worker.rs:409` and read only inside `compiler.rs`
   (1135, 1609), with **no `ops` readers**. The SQL planner is the wrong owner.

5. **Over-broad `pub` surface.** A binary crate has no external API, so every plain
   `pub` is crate-wide reach but falsely signals a boundary and masks the small
   real ones. The surface sweep (§5 W8) is far larger than first scoped: whole
   subsystems are effectively closed yet expose hundreds of bare-`pub` internals
   (runtime exports ~200 items but only `server_main` + `sal::MAX_WORKERS` escape).

6. **Facade-bypass / raw-pointer leaks.** `compiler.rs` (1066/1845/1869) and
   `runtime/master.rs:3375` reach into `crate::ops::util::` / `crate::ops::index::`
   internals; `ops/index.rs` exposes raw `pub table: *mut storage::Table` on
   `GiDesc`/`AviDesc` (`:12` and `:17`); and **catalog leaks three raw handles, not
   one**: `get_dag_ptr` (`store.rs:1552`, `*mut DagEngine`), `get_ptable_handle`
   (`store.rs:1533`, `*mut PartitionedTable`), `get_index_store_handle`
   (`store.rs:1382`, `*const Table`).

### Three smells that turned out **not** to be production cycles (verified)

- **C2 `compiler → catalog → dag → compiler`:** refuted. The only
  `compiler → catalog` edge is `#[cfg(test)]`-only (compiler.rs:3500/3542, inside
  `mod tests`); `catalog` has zero non-test `crate::compiler` refs. The real edges
  are a clean one-directional chain `catalog → dag → compiler`.
- **C3 `storage → dag`:** refuted as production. The sole `crate::dag` ref under
  `src/storage/` is `partitioned_table.rs:1036`, inside `#[cfg(test)] mod tests`
  (opened at 500). Test-only.
- **C4 `runtime/catalog` tangle:** refuted. `catalog → runtime` non-test edges = 0
  (one doc-comment false positive in `sys_tables.rs:163`). The sole real
  `catalog → runtime` edge is **test-only** (`catalog/tests/compound_pk_smoke.rs:114`).

> All three refutations rest on **test-only** edges. The refactor cleans those too
> (§7), so the acyclic layering holds in `cfg(test)` builds, not just production.

---

## 2. Target architecture — a directory tree that mirrors the layers

The 14 flat modules collapse into **8 top-level directories**, each a layer (or a
pair of adjacent layers) behind one tight outward facade. The acyclicity is
achieved by **relocating ~5 misplaced symbols** and introducing leaf seams
(`schema::key`, `foundation::worker_ctx`, `schema::row_view`) and the extractions
(`storage::repr::{scatter, batch_wire}`, `ops::reindex`, `catalog::write_path`);
everything else is regrouping and splitting.

```
crates/gnitz-engine/src/
├── main.rs                  bin entry; declares the 8 top-level mods + #[cfg(test)] test_support/test_rng
│
├── foundation/              L0 — UMBRELLA of sibling leaves (NOT a unifying facade;
│   │                              mod.rs re-exports each leaf's own narrow surface)
│   ├── log.rs               gnitz_* macros (#[macro_export]) + is_*/_emit/init/levels
│   ├── codec.rs             LE pack: read/write_{u32,u64,i64}_le  (the genuine cross-layer leaf)
│   ├── xxh.rs               XXH3 hash_u128/checksum/checksum_128 (kept whole)
│   ├── posix_io.rs          util fd/sync helpers + crate-root sys.rs FILE syscalls
│   │                          (fallocate/ftruncate/try_set_nocow/madvise_*/server_create/raise_fd_limit)
│   ├── syscall.rs           runtime/sys.rs folded DOWN: eventfd/futex/memfd/mmap_shared/errno
│   └── worker_ctx.rs        NEW: WORKER_RANK/NUM_WORKERS + accessors (lifted from compiler.rs)
│
├── schema/                  L1  (was schema.rs)
│   ├── mod.rs               facade: SchemaDescriptor, SchemaColumn, ColumnLocator, IndexKeySpec, RowView, key::*, type_code
│   ├── descriptor.rs        SchemaColumn + SchemaDescriptor + accessors + PayloadCmpKind + validate_schema_match
│   ├── locator.rs           §8-cluster-3 WHOLE: ColumnLocator + IndexKeySpec + locate + assemble_wide_pk
│   ├── row_view.rs          NEW (W2): the RowView trait (impl'd by storage::MemBatch — keeps the edge DOWN)
│   ├── german_string.rs     Umbra-string toolkit (depends only on foundation::codec + gnitz_wire)
│   └── key.rs               NEW (W1): the OPK §9 cluster moved DOWN —
│                              compare_pk_bytes · encode_order_preserving_pk · opk_key · partition_for_pk_bytes
│                              · partition_for_key · PkBuf (+ its Ord) · pk_sort_key · pack_pk_be
│
├── storage/                 L2 repr + L3 LSM behind ONE facade (storage/mod.rs already curates it)
│   ├── mod.rs               the single combined facade; everything else sealed pub(super)/pub(crate)
│   ├── error.rs             StorageError
│   ├── repr/                  L2 — internal submodule dir (no own outward facade)
│   │   ├── batch.rs           Batch/ConsolidatedBatch/write_to_batch + region layout (write_to_batch STAYS here — pure repr)
│   │   ├── batch_wire.rs      NEW (W6): wire/shard serde; the ONE sibling allowed to depend UP on lsm::{wal,shard_file}
│   │   ├── batch_pool.rs      TLS buffer recycling
│   │   ├── columnar.rs        ColumnarSource + compare_rows (§8-cluster-2 payload half) + with_payload_cmp/with_pk_ord + lower-bound scans
│   │   ├── merge.rs           sort-merge consolidation (merge_batches/sort_and_consolidate/fold_sorted, MemBatch)
│   │   ├── scatter.rs         NEW (W9): exchange-repartition cluster carved from merge.rs
│   │   ├── heap.rs            §8-cluster-1 WHOLE: drive_merge + LoserTree + HeapNode (generic over comparator closures)
│   │   ├── range_key.rs       increment_key_in_place + range_cut_points (cross-layer leaf: ops::join L4 + catalog L6)
│   │   ├── bloom.rs           PK-probe Bloom (L3-internal)
│   │   └── xor8.rs            XOR8 filter codec (L3-internal)
│   └── lsm/                   L3 — internal submodule dir
│       ├── table.rs           Table (split internally: ingest vs two-phase flush state machine)
│       ├── partitioned_table.rs   PartitionedTable (partition_for_* MOVED to schema::key)
│       ├── memtable.rs        MemTable (runs vs lookup)
│       ├── read_cursor.rs     ReadCursor/CursorHandle/DrainGuard (source/merge-state/output/handle)
│       ├── shard_reader.rs    MappedShard/RegionView (cold open vs hot access)
│       ├── shard_file.rs      shard image build/write
│       ├── shard_index.rs     ShardIndex (in-memory index vs manifest persist) + the compaction TRIGGER policy
│       ├── compact.rs         N-way merge kernel + guard-routing
│       ├── manifest.rs        manifest serde (PkBuf MOVED to schema::key)
│       ├── wal.rs             WAL V4 block layout
│       └── layout.rs          shard-format consts (folded in from top-level layout.rs — storage-only)
│
├── expr/                    L4a — scalar-eval LEAF, its OWN top-level dir (distinct consumer set)
│   ├── mod.rs               facade: ExprProgram, ScalarFuncKind, Plan, resolve_column_indices
│   ├── program.rs           ExprProgram + opcodes + OutputColKind + classify_output_cols (STAY — W8 §5 decline)
│   ├── plan.rs              ScalarFuncKind + Plan + FinalizeContext (STAYS — W8 §5 decline: keeps eval cluster sealed)
│   └── batch.rs             §8-cluster-4 expr half WHOLE: eval_batch morsel loop + EvalScratch
│
├── ops/                     L4b — operators (TWO facades: compute→query, exchange→runtime)
│   ├── mod.rs               compute facade: op_* family + {AggDescriptor, AggOp, is_single_col_natural_pk, GiDesc, AviDesc}
│   ├── reindex.rs           NEW (W7): ReindexPacker/PkPromoter/ColPromoter/german_string_promote_key/reindex_hash_row
│   ├── linear.rs            op_filter/op_map/op_negate/op_union/op_null_extend
│   ├── distinct.rs · scan.rs · cogroup.rs · util.rs
│   ├── index.rs             GiDesc/AviDesc/op_integrate_with_indexes + a real make_gi/avi_schema facade fn
│   ├── join/                  §8 clusters 5 + 7 — each WHOLE in one file
│   │   ├── delta_trace.rs     §8-cluster-5 WHOLE: op_{,anti_,semi_}join_delta_trace + _outer
│   │   ├── range.rs           op_join_delta_trace_range + range_per_row_seek/range_merge_walk
│   │   ├── delta_delta.rs     op_{,anti_,semi_}join_delta_delta + §8-cluster-7 filter_join_dd_with_payload_inner
│   │   └── rowwrite.rs        shared write_join_row* (dependency of BOTH delta_trace + delta_delta — MUST stay inlinable)
│   ├── reduce/                mod/op_reduce/op_gather/agg/emit/sort (no finalize.rs — W8 §5 declined the relocation)
│   └── exchange/              SECOND facade — sole client runtime::master; depends DOWN on ops::reindex
│       ├── router.rs          PartitionRouter/worker_for_partition/with_*_indices/RouteMode
│       └── relay.rs           §8-cluster-7 relay_walk_inner WHOLE
│
├── query/                   L5 — compiler + vm + dag behind ONE facade (dag is the de-facto facade)
│   ├── mod.rs               facade: DagEngine, StoreHandle, RelationKind, SysTableRefs, IndexCircuitEntry, ExchangeCallback
│   ├── dag.rs               DagEngine scheduler + StoreHandle adapter (seal DagEngine.tables / TableEntry.handle behind accessors)
│   ├── compiler.rs          compile_view + load/optimize/emit (worker_ctx statics MOVED to foundation)
│   └── vm.rs                §8-cluster-4 WHOLE: execute_epoch opcode-dispatch match + ProgramBuilder (internals sealed)
│
├── catalog/                 L6 — sibling of query (depends DOWN on query/), NOT merged in
│   ├── mod.rs               facade: CatalogEngine + {make_index_schema, project_schema, FIRST_USER_TABLE_ID, SEQ_*, *_TAB_ID, IDXTAB_PAY_*}
│   ├── store.rs             split into 4 files by section: ingest/IO · id-registry · fk/index-metadata · partition+LSN
│   ├── write_path.rs        NEW (W13): §8-cluster-8 WHOLE — submit→precheck→apply_local→fire_hooks + SysFamily + ApplyContext
│   └── hooks.rs · cache.rs · ddl.rs · bootstrap.rs · sys_tables.rs · types.rs · validation.rs · apply_context.rs
│
├── runtime/                 L7 — SEALED subsystem, 2-symbol facade {server_main, sal::MAX_WORKERS}
│   ├── mod.rs               pub use bootstrap::server_main; sal::MAX_WORKERS; everything else pub(crate)/pub(super)
│   ├── bootstrap.rs         server_main entry
│   ├── orchestration/        executor.rs · committer.rs · master/{dispatch,unique_filter,preflight} · worker/{exchange,reply,fsync}
│   ├── protocol/             wire.rs · sal.rs · w2m.rs · w2m_ring.rs
│   └── reactor/              mod · sync · exchange · io · ring · uring  (tests stay INLINE — see §7)
│
├── test_support/            #[cfg(test)] shared fixtures (was test_support.rs; promote to a dir as it grows)
└── test_rng.rs              #[cfg(test)] deterministic PRNG
```

### Layering rule

A directory may depend only on directories below it (L0 lowest). The only intra-dir
exception is **`storage::repr::batch_wire → storage::lsm::{wal, shard_file}`** (W6):
that is *not* a layer cycle — `repr/` and `lsm/` are co-equal internals of the one
`storage` subsystem with no clean seam between them — but a mechanical layer-lint
that treats them as separate layers will false-flag it, so it is documented here
and excused by name.

### Two groupings taken, two rejected — both on the evidence

| Decision | Why |
| --- | --- |
| **TAKE** `storage/` = `repr/` + `lsm/` behind one facade | `storage/mod.rs` already curates the combined L2+L3 surface. **Two-way** split (not three): `heap::drive_merge` is called by repr's `merge`+`scatter` *and* by lsm's `compact`+`read_cursor`; a separate `merge/` subdir would straddle the boundary and force a storage-wide `pub(crate)` web. |
| **TAKE** `query/` = `compiler` + `vm` + `dag` behind one facade | `vm` is a clean leaf (zero outbound sideways edges); `dag` is the single inbound facade catalog targets (`catalog → dag → compiler` is acyclic; `dag` has zero catalog refs). |
| **REJECT** folding `catalog` into `query/` | Runtime drives ~60 `CatalogEngine` methods; folding L6 behind the L5 facade would *union* those with the query exports and **enlarge** the outward surface — backwards. Catalog stays a sibling. |
| **REJECT** folding `expr` under `ops/` | `expr`'s consumer set (`ops::linear` down, `compiler`/`vm` up) is distinct from the `op_*` VM-dispatch ABI; burying it two levels past the ops facade costs more than the saved top-level slot. |

`foundation/` is an **umbrella, not a facade**: its `mod.rs` re-exports each leaf's
own narrow surface; the leaves share nothing but being leaves.

### New seams introduced (all single-definition, no duplication)

- **`schema::key`** (L1) — the OPK/key primitives moved down from storage (W1).
- **`schema::row_view`** (L1) — the `RowView` trait that lets the locator read a
  batch without naming `storage::MemBatch` (W2).
- **`foundation::worker_ctx`** (L0) — the worker-rank globals out of the planner (W3).
- **`storage::repr::scatter` / `storage::repr::batch_wire` / `ops::reindex` /
  `catalog::write_path`** — extractions that remove the wrong-direction edges and
  relieve the god-files.

---

## 3. Guardrails (every workstream obeys these)

- **No de-monomorphization.** All hot paths stay generic-monomorphic or concrete;
  **never** `dyn`/`Box` on a per-row/per-epoch path. Same-crate moves preserve
  cross-module `#[inline]`; keep every `#[inline]` and `const`-generic parameter
  **verbatim**.
- **`RowView` is a generic param, never `&dyn`.** W2's trait is
  `<V: RowView<'b>>` monomorphized per call site. **Validated by prototype:** it
  compiles with the `'b` batch-data lifetimes and monomorphizes to identical
  codegen — the release binary contains **zero `RowView` symbols** (fully inlined,
  no vtable). The fallback once contemplated (keep the up-edge) is unnecessary.
- **Single definition, relocate don't copy.** Every "move" re-homes the one
  definition and (where churn would be large) re-exports it from the old path. A
  forked OPK encoder / promotion key / region-layout helper silently breaks
  byte-equality and co-partitioning, so duplication here is a correctness bug.
- **Preserve the load-bearing invariants** in §8. Splits move these clusters
  whole; they never reorder them.
- **Visibility narrows monotonically; tests never force a widening.** A split may
  introduce a `pub(super)` bump *only* when production code across the new
  sub-files requires it (§7). No item is ever widened *solely* to relocate a test.
- **Gate every workstream** on `cargo build` (warnings denied — fix, never
  suppress), `cargo clippy`, `make test`, and for runtime/IO changes `make e2e`.
  The OPK relocation additionally requires a **byte-diff** check (§9).

---

## 4. Stage A — break the cycle, place the leaves, remove misplaced state (P0)

### W1 — Create `schema::key`; relocate the OPK cluster — ✅ DONE
*Breaks C1 (3 of 4 edges). Depends on: none. Effort: M.*

> **Done.** All listed symbols + private `mix` + `pk_sort_key` moved
> byte-identically into `schema::key`; `schema.rs` repointed; OPK tests followed
> (16, green). Re-exports live in the **leaf** modules (consumers use facade *and*
> leaf paths), so `mod.rs` only sheds the now-test-only `encode_order_preserving_pk`. Gates green.

**Rationale.** OPK encode/route and the PK key buffer are pure layout/key
operations that belong below both schema and storage. Moving them down removes
three `schema → storage` up-edges and makes them available to schema without an
upward reach.

**Steps.**
1. Create `schema/key.rs`. Move **verbatim**, keeping `#[inline]`:
   - `encode_order_preserving_pk` + `opk_key` (`storage/columnar.rs:292/311`),
   - `compare_pk_bytes` (`storage/columnar.rs:146`),
   - `partition_for_pk_bytes` (`storage/partitioned_table.rs:479`) **and its
     narrow-PK sibling `partition_for_key` (`partitioned_table.rs:466`)**,
   - `PkBuf` (`storage/manifest.rs:71`) **together with its `Ord` impl** (which
     delegates to `compare_pk_bytes` — they must travel as one commit),
   - **`pack_pk_be`** (the remaining member of the key-packing cluster).
2. In `storage/mod.rs`, `pub(crate) use crate::schema::key::{…}` re-exporting all
   of them so storage's internal call sites compile unchanged (path alias only —
   no second definition, no perf cost).
3. Repoint `schema.rs:100/430` to `crate::schema::key::` and drop those
   `crate::storage::` paths; `schema.rs:833`'s `PkBuf` is now schema-internal.
4. The OPK property tests in `columnar.rs` (`compare_pk_bytes_*`, `opk_proptest`)
   **follow the functions down** into `schema/key.rs`'s test module (their oracle
   deps `read_signed`/`read_unsigned` and `test_support::arb_pk_type` are all
   crate-visible there — zero widening).
5. **Verify:** `cargo build` + `make test` green; `grep crate::storage src/schema*`
   shows only `MemBatch` remains (handled by W2).

**Perf / Dup.** `encode_order_preserving_pk` and `partition_for_pk_bytes` are
per-row on ingest/backfill/exchange/join. Move bodies **byte-for-byte**; the
narrow (`≤16` → `widen_pk_be`) vs wide (`xxh >> 56`) split in
`partition_for_pk_bytes` must stay bit-identical so producer and consumer
co-partition. Do not leave a compat copy behind — re-export the moved item.

### W2 — Break the last `schema → storage` edge with a `RowView` trait — ✅ DONE
*Breaks C1 (4th edge), fully. Depends on: none. Effort: S.*

> **Done.** Added `pub(crate) trait RowView<'b>` (3 accessors —
> `get_null_word`/`get_pk_bytes`/`get_col_ptr`) inline in `schema.rs`; the six
> locator read methods (`is_null`/`bytes`/`native_key`/`route_key`/`write_span`/
> `key_bytes`) now take `&impl RowView<'b>` instead of `&MemBatch`, bodies
> unchanged. `impl<'b> RowView<'b> for MemBatch<'b>` (UFCS-forwarding, `#[inline]`)
> sits by the `MemBatch` def in `storage/merge.rs`; `use crate::storage::MemBatch`
> deleted from schema; call sites unchanged (inference picks `V = MemBatch`). Gates
> green: no `crate::storage` in `src/schema*`; codegen-identity holds (0 `RowView`
> symbols — fully inlined, no vtable). The `schema/` carve later relocates the trait
> to `row_view.rs`.

**Rationale.** `ColumnLocator`/`IndexKeySpec` are pure schema-derived data; only
their read methods named `MemBatch` (as a `&MemBatch` param). Moving the structs
down into storage would re-form the cycle, so the trait abstracts the batch
parameter and keeps the structs in schema. **Static dispatch only — never
`&dyn RowView`.**

### W3 — Create the `foundation/` umbrella; relocate `WORKER_RANK`/`NUM_WORKERS` — ✅ DONE
*Removes problem #4; lands the L0 leaves. Depends on: none. Effort: M.*

> **Done.** `foundation/mod.rs` (`#[macro_use]`) umbrellas six L0 leaves: `log`
> (whole; macro bodies repointed `$crate::log::` → `$crate::foundation::log::`,
> `is_*`/`_emit` kept `pub` for `#[macro_export]` hygiene), `xxh` and `syscall`
> (whole-file moves of `xxh.rs` / `runtime/sys.rs`), and `codec` (LE pack + raw +
> `align8`/`cstr_from_buf`) + `posix_io` (util fd/sync + crate-root `sys.rs` FILE
> syscalls + `guard_panic` + `raise_fd_limit_for_tests`) carved from
> `util.rs`/`sys.rs`. NEW `worker_ctx` holds the two now-**private** statics +
> `pub(crate)` `set_worker_rank`/`worker_rank`/`num_workers` lifted verbatim from
> `compiler.rs` (worker.rs writes them; compiler.rs imports the two readers). Dead
> `fallocate_keep_size` + its test dropped; ~25 consumer files repointed;
> `util.rs`/`sys.rs` deleted. Accessors stay `Relaxed` atomic loads (no lock, no
> per-call lookup). Gates green incl. `make e2e` (GNITZ_WORKERS=4) for the syscall
> fold; no `compiler::{worker_rank,num_workers,WORKER_RANK,NUM_WORKERS}` refs
> remain. `guard_panic` lives in `posix_io` (libc-free process-safety helper, kept
> out of the byte-codec leaf).

### W4 — Relocate `BatchBuilder` (and its index-meta cluster) to `storage` — ✅ DONE
*Removes a misplaced cross-layer utility; auto-fixes the C2 test edge. Depends on: none. Effort: S.*

> **Done.** `BatchBuilder` + `index_meta_schema_desc` + `INDEX_META_COL_NAMES` +
> `make_index_schema` (from `catalog/utils.rs`) and `project_schema` (from
> `catalog/store.rs`) moved **verbatim** into `storage/batch.rs` beside `Batch`, all
> `pub(crate)`. `catalog/mod.rs` re-exports `BatchBuilder`/`make_index_schema`/
> `project_schema` from `storage` so its ddl/bootstrap/store/hooks callers — and the
> runtime gather/preflight callers of the two free fns — compile unchanged;
> `index_meta_schema_desc`/`INDEX_META_COL_NAMES` have no catalog consumer, so
> `runtime::executor` imports those (and `BatchBuilder`) straight from `storage`,
> deleting the runtime→catalog-for-a-builder edge. The C2 test edge
> (`compiler.rs` `use crate::catalog::BatchBuilder` → `crate::storage::BatchBuilder`)
> is repointed — a clean L5→L2 DOWN edge, not masked by the re-export. The compound-PK
> `physical_col_idx` test followed `BatchBuilder` into `storage/batch.rs`. Gates green:
> build, clippy (all targets), `make test` (1302). (`storage/batch.rs` becomes
> `repr/batch.rs` in W9 — these travel with it.)

### W5 — Test-edge cleanup: extend acyclicity to `cfg(test)` builds — ✅ DONE
*Removes the test-only up-edges the refutations rest on. Depends on: W4. Effort: S.*

> **Done.** The three test-only up-edges each moved to the layer owning the symbol.
> **C3** — the `StoreHandle::Partitioned` recovery_lsn/current_lsn dispatch assertion
> became `dag::tests::store_handle_partitioned_lsn_dispatch`, built on a new
> `#[cfg(test)] pub(crate) partial_flush_lsn_fixture` (the partial-flush min≠max
> table). The fixture lives in `storage::partitioned_table` (re-exported `pub(crate)`),
> **not** `test_support`: the partial flush reads the private per-partition `tables`
> field, so hoisting it would force a production widening the guardrail forbids — the
> setup is still defined once, and the storage test keeps its min/max public-method
> assertions. **C4** — `schema_roundtrip_wire_preserves_pk_order` moved into
> `runtime/tests/wire.rs`; catalog keeps its API-only peer (no coverage lost). **E5** —
> `test_filter_batch_matches_per_row` moved into `ops/linear.rs`, adapted to its
> existing `make_schema_u64_i64`/`make_batch` helpers. **E6** left as the accepted
> `test_support` fixture inversion; **C2** was W4. Cycle gate green incl. `cfg(test)`:
> no `crate::dag` in `storage/`, no `crate::runtime` in `catalog/`, no `crate::ops` in
> `expr/`, no `crate::catalog` in `compiler*`. Gates green: build, clippy,
> `make test` (1303, +1 from the split-out dag dispatch test), `make e2e` (1043).

---

## 5. Stage B — fix wrong-direction edges, narrow surface (P1)

### W6 — Extract `storage::repr::batch_wire`; sever all three repr→disk edges — ✅ DONE
*Removes problem #3; shrinks batch.rs. Depends on: none. Effort: M.*

> **Done.** The wire/shard cluster moved **byte-for-byte** into a new
> `storage::batch_wire` (flat for now — W9 relocates it under `repr/` with `batch.rs`):
> `wire_byte_size`/`wire_byte_size_range`/`encode_range_to_wire`/`encode_to_wire`/
> `decode_from_wal_block` as an `impl Batch` block, the free
> `decode_mem_batch_from_wal_block`, and `write_as_shard`/`write_as_shard_with_flags`.
> `batch.rs` sheds `use super::shard_file` and every `super::wal` call; the lone
> production widening is a `pub(super) region_stride` accessor (the only private field
> the range encoders read). `storage/mod.rs` repoints only the
> `decode_mem_batch_from_wal_block` re-export — the method callers (`ops`,
> `runtime::wire`) call through `Batch` and are unchanged. `write_to_batch` STAYS
> (pure arena/`DirectWriter`/`batch_pool`). **Third edge:**
> `append_row_from_ptable_found(&PartitionedTable)` → `append_found_row(null_word,
> col_ptr: impl Fn, blob_src)`; `dag.rs` reads the found row through the table
> accessors and passes them in, so the repr method names no LSM type — a monomorphized
> closure keeps the per-column copy loop inlinable, no behaviour change. The 5 wire
> tests followed the cluster into `batch_wire`; the found-row test was renamed +
> adapted in place. Gates green: build, clippy (all-targets), `make test` (1303),
> `make e2e` (GNITZ_WORKERS=4, 1043).

### W7 — Extract `ops::reindex` (exchange stops reaching into linear) — ✅ DONE
*Removes an intra-`ops` leaky reach; relieves two files. Depends on: none. Effort: M.*

> **Done.** `PkPromoter`/`ColPromoter`/`ReindexPacker`/`german_string_promote_key`/
> `reindex_hash_row` (+ the private `PromoteKind`) moved **verbatim** into a new
> `ops::reindex`, carrying every `#[inline]` and const-generic arm. Visibilities
> preserved (`german_string_promote_key`/`ReindexPacker` `pub(crate)`, `PkPromoter`
> `pub(super)`), the one production bump being `reindex_hash_row` private →
> `pub(super)` for `op_map`'s cross-file call. `exchange.rs`'s six reindex reaches
> repointed to `crate::ops::reindex::` — it no longer names `crate::ops::linear`.
> `linear.rs` is true linear ops only: `op_map` calls in via
> `use super::reindex::{ReindexPacker, reindex_hash_row}`, doc header fixed, dead
> `ColumnLocator`/`type_code`/`xxh` imports dropped. The 16 reindex-subject tests
> followed their subject into `reindex`; the 24 linear-op tests stay (incl. the
> op_map-reindex and op_filter-blob tests, whose subject is the linear op); 4 trivial
> shared schema/batch builders are duplicated test-side (no forked OPK logic). Gates
> green: build, clippy (all-targets), `make test` (1303), test-count parity (40 = 24
> + 16).

### W8 — Inter-module surface-reduction sweep — ✅ DONE
*Removes problems #5 and #6. Depends on: none. Effort: L.*

> **Done.** Surface narrowed across the crate; gates green (build, clippy
> `--all-targets`, `make test` 1303). The load-bearing realization: in a binary
> crate every top-level module is reachable crate-wide and storage's submodules are
> already private `mod`, so `pub → pub(crate)` on a path-reachable item is an
> intent-signal, not a reachability change — the reachability wins come from *closing
> modules* and `→ private`.
> - **8a.1–3,7 (signal):** storage repr (`columnar`/`merge`/`heap`/`bloom`/`xor8`) +
>   lsm (`read_cursor` create fns, `Table::{snapshot_runs,in_memory_runs,all_shard_arcs}`),
>   query (`compiler`/`vm`/`dag` listed items), and schema (`PayloadCmpKind`,
>   `assemble_wide_pk`, `pk_index_single`, `payload_mapping_byte`, `pk_byte_offset`,
>   `NARROW_PK_MAX_BYTES`, `PAYLOAD_MAPPING_PK_SENTINEL`) → `pub(crate)`;
>   `dist_prefix_len` + `prep_german_string_copy` → **private** (real narrowing).
> - **8a.4 runtime CLOSED:** the effective fix is sealing all `runtime` submodules to
>   private `mod` + re-exporting `MAX_WORKERS` — only `server_main` + `MAX_WORKERS`
>   escape (verified: `main.rs` is the sole external consumer). Per-item `pub→pub(crate)`
>   would be moot inside a `pub(crate) mod`, so it was skipped in favour of the seal.
> - **8a.5 ops facade:** `make_{gi,avi}_schema` + `all_payload_null_mask` +
>   `AVI_AV_BYTES` re-exported from `ops/mod.rs`; `index`+`util` sealed to private
>   `mod`; the 4 compiler/master callers repointed to `crate::ops::X`.
> - **8a.6 raw pointers:** `GiDesc`/`AviDesc` fields → `pub(crate)`; the three catalog
>   handles (`get_dag_ptr`/`get_ptable_handle`/`get_index_store_handle`) → `pub(crate)`,
>   keeping the raw-ptr returns (§8 cluster-6 aliasing discipline — no typed wrapper).
> - **8a.9 catalog facade:** the zero-external re-exports → `pub(in crate::catalog)`
>   (they reach submodules through their existing `use super::*` glob, so this narrows
>   the crate surface with zero submodule churn — chosen over drop-and-direct-import);
>   `ColumnDef`/`SEQ_*`/`*_TAB_ID`/`IDXTAB_PAY_*` stay `pub(crate)`; the 5 methods
>   (`close`/`drain_pending_dir_deletions`/`cancel_gated_deletion`/`get_schema_id`/
>   `raw_store_ingest`) → `pub(crate)`.
> - **8a.10:** `layout.rs` folded into `storage::layout` (`pub(crate)` consts), dropped
>   from `main.rs` (relocated under `lsm/` in W10).
> - **8b:** ~40 `#[cfg(test)] pub` → `pub(super)` (dag::close, 11 reactor scaffolds, 4
>   relay wrappers) / `pub(crate)` (the rest). Corrections vs the prescription:
>   `record_routing` needs `pub(crate)` (a `runtime/master.rs` test consumes it cross-dir);
>   `compute_worker_indices`'s `ops/mod.rs` re-export dropped to `pub(crate) use`;
>   `append_row`/`append_row_simple` no longer exist. `REWIND_CALLS` / `log` macros /
>   `align8` kept as-is.
> - **DECLINED — 8a.8 relocation.** Moving `FinalizeContext` + `OutputColKind` into
>   `ops::reduce::finalize` would force widening the §8 do-not-touch eval cluster
>   (`EvalScratch`, `eval_batch`, `MORSEL`, `NULL_WORDS_PER_REG`, scratch fields — all
>   `pub(in crate::expr)`) to `pub(crate)`: a net surface **widening** that inverts
>   W8's goal. `FinalizeContext` is precisely the boundary that keeps the eval
>   internals sealed in `expr`, so it stays. Only `is_strictly_non_nullable` →
>   `pub(in crate::expr)` was applied.

---

## 6. Stage C — split the god-files and place them in the layer tree (P2)

All splits are mechanical carves along the seams below; each preserves the
do-not-touch clusters in §8 **whole** and places the resulting files into the §2
directory tree. They land after W8 so new sub-modules get correct
`pub(super)`/`pub(crate)` visibility. Splits within one subsystem are independent.

### File-split reference

| File | LOC | Split into | Seam |
| --- | --- | --- | --- |
| `schema.rs` | 1863 | `schema/{descriptor, german_string, locator, row_view, key}.rs` + `mod.rs` | descriptor (layout core) · german_string (blob-string codec) · locator (`ColumnLocator`/`IndexKeySpec`) · row_view (`RowView`, W2) · key (W1) |
| `storage/batch.rs` | 2546 | `repr/batch.rs` (residual repr, incl. `write_to_batch`) + `repr/batch_wire.rs` (W6) | repr vs wire/shard serialization |
| `storage/merge.rs` | 2880 | `repr/merge.rs` (residual) + `repr/scatter.rs` | sort-merge consolidation vs exchange repartition (`scatter_copy:668`/`scatter_multi_source:895`/`scatter_unified_sources_with_weights:1049`) |
| `storage/read_cursor.rs` | 2604 | `lsm/read_cursor/{source, mod, output, handle}.rs` | source accessors · merge state machine · output/drain · public `CursorHandle` |
| `storage/table.rs` | 1834 | `lsm/table/{flush, mod}.rs` | two-phase flush state machine vs `Table` ingest/cursor/lookup |
| `storage/shard_index.rs` | 1644 | `lsm/shard_index/{index, persist}.rs` | in-memory index state vs manifest serialize/load/recover (the compaction *trigger* policy `should_compact`/`run_compact` stays in `index`) |
| `storage/memtable.rs` | 1612 | `lsm/memtable/{runs, lookup}.rs` | sorted-run management vs point-lookup/bloom |
| `storage/shard_reader.rs` | 1449 | `lsm/shard_reader/{open, access}.rs` | cold open-time validation vs hot per-row `RegionView` accessors |
| `storage/compact.rs` | 1349 | `lsm/compact/{merge, route}.rs` | N-way (PK,payload) merge kernel (`open_and_merge*`/`merge_and_route`) vs guard-routing (`find_guard_for_key`, `compact_shards` orchestration). **Not** "policy" — the when-to-compact trigger lives in `shard_index`. |
| `ops/join.rs` | 2717 | `join/{delta_trace, range, delta_delta, rowwrite}.rs` (+ `test_common`) | delta-trace (incl. outer/anti/semi) · range merge · shared row writers · delta-delta |
| `ops/exchange.rs` | 2070 | `exchange/{router, relay}.rs` | routing vs relay (the relay merge walk 570–790 is the dense hot loop) |
| `ops/linear.rs` | 1963 | `linear.rs` (residual) + `reindex.rs` (W7) | true linear ops vs PK-promotion infra |
| `compiler.rs` | 4403 | `query/compiler/{load, optimize, emit, mod}.rs` | load (`load_circuit:319`/`topo_sort`) · optimize (`annotate:709`/`opt_*`) · emit (`emit_node`/`build_plan:2050`) · `compile_view` orchestrator |
| `vm.rs` | 2968 | `query/vm/{builder, exec, mod}.rs` | `ProgramBuilder` vs `execute_epoch:750` + opcode match (kept whole) vs shared `Instr`/`Program`/`RegisterFile` types |
| `dag.rs` | 2396 | `query/dag/{store_handle, mod}.rs` | `StoreHandle` storage adapter (catalog's inbound target) vs `DagEngine` scheduler |
| `catalog/store.rs` | 1965 | `catalog/{store_io, registry, metadata, partition_lsn}.rs` + `write_path.rs` | the single `impl CatalogEngine` + `impl CatalogDeltaSink` (today **one** impl block subdivided by ~13 section comments, not four) carved by section: io/ingest · id registry · fk/index metadata · partition+LSN · and the §8-cluster-8 write-path spine into `write_path.rs` |
| `runtime/master.rs` | 4366 | `orchestration/master/{dispatch, unique_filter, preflight}.rs` | SAL dispatcher · unique-filter cache · PK/unique preflight + violation formatting (`GatherMap:3004`) |
| `runtime/worker.rs` | 3084 | `orchestration/worker/{exchange, reply, fsync}.rs` | exchange-wait re-entry (`do_exchange_wait:1801`) · response framing · io_uring fsync helpers |
| `runtime/reactor/mod.rs` | 4235 | `reactor/{runloop, futures, conn, mod}.rs` | run loop + `REACTOR` ptr · the 7 IO futures (1609–1953) · client-conn framing · `dispatch_cqe`/`route_*` glue. **Tests stay inline** (see §7) — `ReactorShared`/`Reactor` definitions stay in `mod.rs`. |

The directory regrouping (`storage/{repr,lsm}/`, `query/`, `runtime/{orchestration,
protocol,reactor}/`) happens *with* these carves: each subsystem's workstream both
splits its god-files and moves the residual files into the §2 tree.

### Workstream grouping for Stage C

- **W9 (storage-repr, L) — ✅ DONE.** Created `storage/repr/`; moved the nine L2 files
  (`batch`/`batch_wire`/`batch_pool`/`columnar`/`merge`/`heap`/`range_key`/`bloom`/`xor8`)
  in **byte-identically** — `storage/mod.rs` aliases the submodules (`use repr::{…}` +
  `pub(crate) use repr::batch_pool`) so the LSM siblings keep their `super::<mod>` /
  `crate::storage::<mod>` paths and the in-storage `with_pk_ord!` resolves untouched; the
  only body edits were `batch_wire`'s repr→lsm up-refs (`super::`→`super::super::`) and
  `batch.rs`'s region consts + builders read by lsm bumped `pub(super)`→`pub(in crate::storage)`
  (preserving pre-move reach, not a widening). `merge.rs`→`merge.rs` + `scatter.rs` (the 3
  scatter entry points + const-`PKS`/`N` gather helpers verbatim; `DirectWriter`'s
  fixed-region fields → `pub(super)` for the sibling, scatter tests rebuilt on the public
  `DirectWriter::new()` per §7-rule-4 — no test-driven widening). `read_cursor.rs`→
  `read_cursor/{mod,source,output,handle}.rs` (stays at `storage/` for now — W10 relocates
  under `lsm/`): source accessors (`CursorSource`, `pub(super)`) · merge state machine +
  `CursorState` (`mod`) · drain/materialize/scatter (`output`, an `impl ReadCursor`
  continuation reading the parent's private fields — submodules are descendants, so **no
  field bumps**) · `CursorHandle` (`handle`). Tests stay in `mod.rs` (subject is
  `ReadCursor`; they build cursors only via `create_read_cursor`). Gates green: build, clippy
  `--all-targets`, `make test` (1303, parity), `make e2e` (GNITZ_WORKERS=4).
- **W10 (storage-LSM, L) — ✅ DONE.** Created `storage/lsm/`; moved the eleven L3 files
  (`table`·`partitioned_table`·`memtable`·`shard_reader`·`shard_file`·`shard_index`·`compact`·
  `manifest`·`wal`·`layout` + W9's `read_cursor/`) under it. `storage/mod.rs` re-exports the LSM
  facade through `lsm::`; `lsm/mod.rs` aliases the repr submodules + `error`/`cstr_with_tmp_suffix`/
  `compare_pk_bytes`/`with_*` macros so the siblings keep their `super::<mod>` paths, and repoints
  the `repr::batch_wire → lsm::{wal,shard_file}` exception (those two `pub(super)`). Five god-files
  split **byte-identically** along the §6 seams (every `#[inline]`/const-generic carried;
  `drive_merge` call sites untouched — §8 cluster-1): `compact` → `merge` (kernel + `merge_and_route`)
  + `route` (`find_guard_for_key` / `GuardResult` / `compact_shards`); `shard_reader` → `open` (cold
  validation) + `access` (hot `RegionView` accessors + `ColumnarSource`); `memtable` → `runs` +
  `lookup`; `shard_index` → `index` (state + the `should_compact` / `run_compact` trigger) + `persist`
  (manifest serde); `table` → `mod` (ingest/cursor/lookup) + `flush` (two-phase state machine). Each split keeps its type
  defs + tests in `mod.rs` (read_cursor precedent — tests hit the public surface via `use super::*`,
  reading sibling-defined private fields as descendants needs no bumps); the only visibility bumps
  are §7-rule-3 cross-sub-file production reaches — `pub(super)` **restoring the original
  module-wide scope** (shard_index's 9 index helpers reached by persist/tests, memtable's
  `consolidate_batches`, table's `flush_inner`/`open_dirfd`/`in_memory_bytes`), never a test-driven
  widening of a never-exposed private. Gates green: build, clippy `--all-targets`, `make test`
  (1303, parity), `make e2e` (GNITZ_WORKERS=4, 1043).
- **W11 (ops, L) — ✅ DONE.** `join.rs` carved **byte-identically** into
  `join/{delta_trace, range, delta_delta, rowwrite}.rs` + `mod.rs`: the shared
  `write_join_row*` writers land in `rowwrite` as `#[inline] pub(super)` (kept
  inlinable across the split — §8 cluster-7), `advance_delta_ptr` stays range-private,
  every const-generic/`#[inline]` carried verbatim. `exchange.rs` → `exchange/{router,
  relay}.rs`: `router` owns `PartitionRouter` + the routing-key helpers (4 bumped
  `pub(super)` for `relay`'s cross-file reach) + the shared `SCATTER_INDICES`; `relay`
  owns the scatter/repartition ops + `relay_walk_inner`; the `cfg(test)`-only scatter
  helpers gate `relay`'s `SCATTER_INDICES`/`ReindexPacker` imports `#[cfg(test)]`.
  `reduce/` was already split (`mod/op_reduce/op_gather/agg/emit/sort`, no `finalize.rs`
  — W8 §5); `expr` stays a top-level leaf. Tests follow their subjects (join 24/20/27/4
  + `test_common` `pub(super)` builders; exchange 7 router / 24 relay, the 4 trivial
  shared builders duplicated router-side per the W7 precedent — no exchange `test_common`).
  Gates green: build, clippy `--all-targets`, `make test` (1303, parity), `make e2e`
  (GNITZ_WORKERS=4).
- **W12 (query-core, L) — ✅ DONE.** Created `query/`; moved `compiler`/`vm`/`dag` under
  it behind the `query/mod.rs` facade (`pub(crate) use dag::{DagEngine, ExchangeCallback,
  IndexCircuitEntry, RelationKind, StoreHandle, SysTableRefs}`) — `compiler`+`vm` are
  query-internal (`dag` is the sole inbound target; catalog+runtime repoint `crate::dag::`
  → `crate::query::`). God-files split **byte-identically** along the §6 seams (every
  `#[inline]`/const-generic carried): `vm` → `mod` (shared `Instr`/`Program`/`RegisterFile`
  + tests) + `builder` (`ProgramBuilder`) + `exec` (the opcode match WHOLE — §8 cluster 4);
  `dag` → `mod` (`DagEngine` scheduler, `evaluate_dag` snapshot/scheduling untouched — §8
  cluster 5) + `store_handle` (`StoreHandle`); `compiler` → `mod` (`compile_view` + shared
  type/const defs incl. `decode_expr_blob`/`PlanBuildResult` + tests) + `load` + `optimize`
  (the schema-construction helpers travel here) + `emit` (incl. `EmitState`). Carved free
  fns bumped private→`pub(super)` (§7-rule-3, restoring module scope); sub-files reach
  types/consts via `use super::*`, cross-cluster fns via mod's `use {optimize,emit}::*`. One
  relocation bump: `DagEngine::close` (`cfg(test)`) `pub(super)`→`pub(crate)` — `pub(super)`
  was crate-wide at the old root but is query-wide one level deeper, and catalog's
  `cfg(test)` `close` reaches it (caught by `clippy --all-targets`, not `cargo build`).
  `clippy.toml` disallowed-method path `dag::`→`query::dag::`. Gates green: build, clippy
  `--all-targets`, `make test` (1303, parity 112), `make e2e` (1043).
- **W13 (catalog, L) — ✅ DONE.** `store.rs` (a single `impl CatalogEngine`)
  dissolved **byte-identically** into five flat `use super::*` siblings of
  `catalog/mod.rs` — the type lives up in `mod.rs`, so each is just an `impl
  CatalogEngine` block (no subdir / `mod.rs` residual; every `super::` path
  unchanged): `write_path` (§8 cluster-8 spine WHOLE — `CatalogDeltaSink` +
  `submit`/`apply_local`/`precheck_sys_ingest` + broadcast & dir-deletion queues +
  Stage-A rollback), `store_io` (ingest/scan/seek/flush/replay), `metadata`
  (FK/index queries + handle accessors + `CachedSchemaWire` + unique-violation
  formatting), `registry` (id alloc/lookup + `sys_columns` readers + sequences),
  `partition_lsn` (fork/trim/rehome + flushed-LSN). **Zero visibility bumps** (each
  private item lands with its only callers — §7-rule-2); no tests moved.
  `SysFamily`/`ApplyContext`/`fire_hooks` stay in their own files. Sole external
  edge: `mod.rs`'s `CatalogDeltaSink` re-export repointed to `write_path`. Gates
  green: build, clippy `--all-targets`, `make test` (1303, parity), `make e2e`
  (1043).
- **W14 (runtime, XL):** create `runtime/{orchestration,protocol,reactor}/`, split
  `master`, `worker`, `reactor`. Depends on W8; gate on `make e2e`.

Two test files are oversized — `ops/reduce/tests.rs` (5450) and
`catalog/tests/index_tests.rs` (2857) — and remain **deferred** (§7): they carry no
inter-module surface, do not partition along the production seams, and splitting
them is navigation-only value. Revisit only if they slow iteration.

---

## 7. Test handling (cross-cutting — applies to every Stage-C carve)

The engine's unit tests are ~50% inline `#[cfg(test)] mod tests` (testing each
file's own internals via `super::`, able to see its **privates**) and ~50%
dedicated `tests/` dirs + the two big standalone files. The governing finding: the
**"tests follow their subject"** rule splits every inline test module along the
same seam as its production code with **zero net production-visibility widening**,
because the god-files were written test-first against a deliberately
`pub`/`pub(crate)` surface, and where a test touches a private, that private lands
on the same side of the cohesion seam as the test's subject.

### The five-rule policy

1. **Split `mod tests` along the production seam.** Each test goes inline in the
   sub-file owning the items it exercises. This is the default.
2. **Co-locate, never widen.** If a test touches a private bound for another
   sub-file, the test's true subject *is* that sub-file — move the test there.
   Never widen a private's visibility solely to host a test elsewhere.
3. **Production-mandated `pub(super)` is fine; test-mandated widening is not.** Some
   carves require a `pub(super)` bump for *production* reasons (a helper shared
   across the new sub-files, struct fields read by sibling sub-files). Do those as
   part of the carve, then place tests on that existing surface.
4. **Local-fixture escape hatch.** When a cross-sub-file private reach is only
   *fixture scaffolding*, build a local fixture instead of reaching across.
5. **One `#[cfg(test)] mod test_common` (`pub(super)`) per split-dir** for
   cross-cutting builders. Declared as a **child** from the dir's `mod.rs`, kept
   flat (a nested `tests/` subdir would drop a privacy level and lose `pub(super)`).
   It touches only public + `pub(crate)` `test_support`, so it widens nothing.

### Per-carve specifics (validated)

- **join (74 tests) → clean.** Partition: `delta_trace` (24), `delta_delta` (26),
  `range` (20, incl. the 11 that reach the private `range_per_row_seek`/
  `range_merge_walk` — they belong in `range.rs`), `rowwrite` (4, incl.
  `test_write_join_row_compound_pk_bytes` currently mis-filed under the DD header).
  Add `join/test_common` for the shared `make_schema_*`/`make_batch_*` builders.
- **compiler (57) → mostly.** Co-locate the 5 cross-file tests in the sub-file that
  owns the private (`compute_join_shard_map`/`compute_co_partitioned`/
  `split_fold_programs` tests → `optimize.rs`; `decode_expr_blob` test → `emit.rs`).
  The one genuine `pub(super)` — `decode_expr_blob` + `EXPR_BLOB_*` shared across
  `optimize` + `emit`, and `Rewrites`'s fields — is **production-mandated**, not
  test-driven.
- **master (26) → mostly.** The 3 tests that reach dispatch's
  `build_check_batch_pkbuf` use it only as fixture scaffolding → build a local
  compound-OPK fixture in `unique_filter`'s test mod (rule 4). No widening.
- **reactor (81) → keep tests INLINE.** The plan's earlier "extract ~2280 LOC to
  `tests.rs`" is wrong: a sibling `tests.rs` is not a child of `reactor/mod.rs` and
  loses access to the privates 36 reactor-state tests need. Instead: the production
  carve already forces `ReactorShared`'s fields + `Reactor.inner` to `pub(super)`
  (futures/runloop/conn read them); once that lands, keep each test inline in the
  sub-file owning the state it asserts on. The 6 run-loop tests touching
  `RunQueue.queue` stay with `RunQueue`'s owner — do **not** widen `RunQueue.queue`.
- **vm (38), worker (30), read_cursor (37), merge (46), and all six 2-way
  storage/schema splits → clean.** Tests follow their (crate-visible) subjects with
  zero widening. The W1 OPK tests follow `compare_pk_bytes`/
  `encode_order_preserving_pk` down into `schema/key`'s test mod. The few cross-side
  reaches in `shard_index`/`memtable` are PROD-to-PROD (resolved by `pub(super)`
  within the shared parent), not test-driven.

### Test-only surface

Folded into **W8 batch 8b** above (the ~43 `#[cfg(test)] pub` → `pub(crate)`/
`pub(super)` narrowings).

### Shared fixtures

`test_support` (131 LOC, `pub(crate)`) is the single source of truth for the
canonical wide-PK schema, the OPK-encoding batch builders (`opk_pk`/`make_wide_batch`
— so no test re-derives the §9 layout), and the proptest type-code strategies. It
depends **up** on schema+storage, and storage's tests depend **down** on it (E6) —
that bidirectional `cfg(test)` coupling is legal only at crate scope, so it stays at
the crate root and **must not** sink under `foundation/`. It **will grow** (W5's
shared `PartitionedTable` fixture; helpers shared between split halves), so promote
it to a directory module `src/test_support/{mod, batch_builders, strategies,
fixtures}.rs` when it exceeds a screen — single definitions only, never a fork.
`test_rng` (30 LOC, zero deps) co-locates with it.

---

## 8. Do-not-touch — hot paths and load-bearing invariants

Splits **move these clusters verbatim**; they never reorder, wrap in `dyn`, cache a
sort key, or re-extract a local copy of them.

1. **The fused k-way merge + inline consolidation kernel.** `repr/heap.rs::drive_merge`
   is the **sole** pending-group drain owner (called by `merge.rs`, `compact.rs`,
   `read_cursor.rs`); plus `merge_batches_inner`, `sort_and_consolidate`,
   `fold_sorted`, the keyless 8-byte-`HeapNode` loser tree, and the
   `with_pk_ord!`/`with_payload_cmp!` stride-dispatch macros. The read_cursor and
   compact splits must keep **calling** `drive_merge`, never re-extract a drain loop.
2. **`compare_pk_bytes`** (the single OPK memcmp every ordered op runs),
   **`compare_rows`** (`#[inline]`, `total_cmp` float order),
   **`encode_order_preserving_pk`** (the §9 OPK bijection), **`partition_for_pk_bytes`**
   (narrow/wide hash). W1 must move the PK-byte half (`compare_pk_bytes`, the
   encoders, `partition_for_*`, `PkBuf`+`Ord`) to `schema::key` **byte-for-byte**;
   `compare_rows` (the payload half) stays in `columnar.rs`. `drive_merge` takes both
   as generic comparator args — the family is neither split-in-the-sense-that-matters
   nor boxed.
3. **`ColumnLocator`/`IndexKeySpec` per-row accessors** (`bytes`/`native_key`/
   `route_key`/`write_span`/`key_bytes`), their `'b` batch-memory borrows, and
   `key_bytes`'s `PkBuf` reuse. W2's `RowView` generalization keeps `#[inline]`, the
   lifetimes, and the reuse exactly; **static dispatch only** (§3 guardrail; verified
   zero-cost — W2).
4. **`query::vm::execute_epoch`'s opcode-dispatch match** and `expr::batch`'s
   `eval_batch` per-op interpreter loop. The vm split keeps the **whole** match in
   `vm/exec.rs`; boxing per-opcode handlers or splitting arms breaks monomorphization.
5. **The bilinear join 2-term symmetric form** and delta-trace **cursor snapshotting
   at tick start** (`z⁻¹(I)`); **single-source-per-epoch** scheduling in
   `dag.evaluate_dag`. The join (W11) and dag (W12) splits must not reorder epoch
   scheduling or snapshot timing.
6. **Exchange-wait defer-then-replay ordering** (`do_exchange_wait`), the **w2m
   lock-free protocol** (`fetch_or(MASTER_PARKED)` **before** the `reader_seq`
   snapshot), and the **no-aliasing-across-`await`** raw-pointer discipline
   (`executor::Shared` `cat()`/`disp()`, `get_dag_ptr`). The W14 master/worker/reactor
   splits move these whole and keep accesses on the reactor thread.
7. **Const-generic / comparator-monomorphized per-row kernels:**
   `scatter_unified_pk_nbm<const PKS>`, `relay_walk_inner<const DO_REFILL, Sel, Route>`,
   `filter_join_dd_with_payload_inner<RowCmp>`, `cogroup<M: SortedKeyStream>`,
   `op_union_merge_inner<RowCmp>`, `ReindexPacker`/`german_string_promote_key`. The
   W7/W9/W11 moves are safe **only** if the const generics and `#[inline]` are carried
   verbatim. **`join/rowwrite.rs`'s `write_join_row*` is shared by both
   `delta_trace` and `delta_delta`** — it must stay inlinable (no per-row cross-file
   call boundary).
8. **The single catalog write-path spine:** `submit → precheck_sys_ingest →
   apply_local → fire_hooks`, and the hand-maintained per-family `fire_hooks` dispatch
   order. W13 moves this whole into `catalog/write_path.rs`; the four `store.rs`
   carves must not create a second ingest entry point that skips precheck/hooks.

---

## 9. Verification strategy

- **Per workstream:** `cargo build` (warnings denied — fix, never `#[allow]`),
  `cargo clippy` clean, `make test`.
- **Runtime/IO workstreams (W3 step 1 syscall fold, W5, W6 third-edge, W14):**
  additionally `make e2e` (`GNITZ_WORKERS=4`) with a generous timeout; the e2e suite
  must rebuild the server (no stale binary). Always clean up the test data dirs
  afterward.
- **W1 (and any move of an OPK/promotion/region helper):** a **byte-diff** gate, not
  just a green test run. Build before and after, and confirm a fixed corpus of PK
  values encodes to **byte-identical** OPK and routes to the **same** partition under
  both the old and new definition. A forked encoder passes most tests while silently
  corrupting consolidation and routing.
- **W2 (and any trait-abstraction of a per-row call):** a **codegen-identity** gate.
  Build the release binary before and after; confirm the new trait leaves **zero
  symbols** (full monomorphization + inlining, no vtable) and the anchor functions'
  normalized disassembly (`%rip` displacements / panic-location constants stripped)
  is unchanged. Catches an accidental `&dyn`/vtable that a green test run would miss.
  *(Done for W2: 0 `RowView` symbols, `write_span` instruction-identical.)*
- **Cycle gate (after Stage A), extended to `cfg(test)`:** assert no `crate::storage`
  ref in `src/schema*`, no `crate::dag` ref under `src/storage/`, no `crate::catalog`
  in `compiler*`, and no `crate::runtime` in `catalog/` — **including `#[cfg(test)]`
  blocks and `*/tests/` dirs** (modulo the one accepted E6 `test_support` fixture
  inversion). This is what W5 buys: the acyclic layering holds in test builds too.
- **Test-count parity (after every Stage-C carve):** the number of `#[test]` /
  proptest functions is identical before and after the split (the refactor relocates
  tests, never drops or merges them). **Necessary but not sufficient** — it proves no
  test was dropped, nothing about behaviour.
- **Invariant-pin gate (prerequisite for Stage C).** A coverage audit of the §8
  do-not-touch clusters found **0 strong / 6 partial / 2 weak** — *every* invariant
  has a realistic, mechanical carve-induced violation that the current suite would
  ship green (e.g. a PK-only heap `less` that breaks `drive_merge` adjacency, a forked
  wide `partition_for_pk_bytes` branch, a dropped `PkBuf` tail-zero, a mis-routed
  unsigned opcode, a `Join`-after-`Integrate` reorder, the w2m `fetch_or`/snapshot
  reorder, a `fire_hooks` dispatch reorder). So before any cluster is carved, it must
  be guarded by a **behaviour pin** — a characterization test that passes on the
  current (correct) code and is **teeth-validated**: transiently inject the specific
  violation, confirm the pin goes **red**, then revert. A pin that doesn't go red on
  the injection has no teeth and does not count. The pin set (one per §8 cluster):
  C1 `drive_merge` non-adjacent `payloadA/B/A` fold across ≥3 sources (compact +
  merge + read_cursor, the last reading the payload value); C2 wide (>16B) producer==
  consumer co-partition built via the two independent key paths; C3 `key_bytes`
  wide-then-narrow `PkBuf` tail re-zeroing; C4 unsigned-opcode swap + eval for values
  ≥ 2⁶³; C5 `Join` opcode emitted before `Integrate` in the compiled program (+ the
  single-source-per-epoch `(dep_id, source_id)` scheduling); C6 the w2m
  `FLAG_MASTER_PARKED`-before-`reader_seq`-snapshot ordering, isolated from the
  tick() safety-net drain; C7 the Generic `RowCmp` shared-PK string/null payload merge
  (`op_union`, anti-DD) + the const `PKS=8/16` scatter arms; C8 `fire_hooks`
  children-before-parents `pending_broadcasts` order + qname-before-id retraction.
  These guard *behaviour* across the carve where test-count parity and a green run
  cannot.

---

## 10. Sequencing summary

```
Stage A (P0, structural)    W1 ─┬─ W2            break C1 (key seam ✅ + RowView ✅)
                            W3  │                foundation/ umbrella + worker_ctx ✅
                            W4  │                BatchBuilder → storage (auto-fixes C2 test edge) ✅
                            W5 ─┘                test-edge cleanup (C3/C4/E5) → cfg(test) acyclicity ✅
Stage B (P1, edges+surface) W6 ─ W7   W8         batch_wire + 3rd edge ✅ · ops::reindex ✅ · surface sweep ✅
Stage C (P2, splits+regroup) W9 ✅ W10 ✅ W11 ✅ W12 ✅ W13 ✅ W14   god-file carves into the layer tree (gated on W6/W7/W8)
```

W1–W8 are small, independent (except W5→W4; W2 was prototyped standalone, so it does
not depend on W1), high-value, and low-risk; they
deliver the acyclic layering (production *and* test), the leaf regrouping, and the
narrowed surface. W9–W14 are larger but purely mechanical carves behind the §8
guardrails and the §7 test policy.

### Decisions to confirm before committing the affected seam

- **Commit ordering:** `schema::key` (W1) moves as ONE commit carrying `PkBuf`
  *together with* `compare_pk_bytes` (its `Ord` delegates to it); W4 must land
  before or with the W12 query regroup (or the repointed `compiler` tests break).
