# gnitz-engine architectural refactor — master plan

A structural refactor of `crates/gnitz-engine` (~74k non-test LOC, binary crate,
root `src/main.rs`, no `lib.rs`). The goal is to **modularize and regroup** the
engine, **break the one real dependency cycle**, **narrow the surface between
groups**, and **split the god-files** — with **zero behaviour change, zero
performance regression, and zero code duplication**. Every change below is a
relocation, a visibility narrowing, or a file split along an existing cohesion
seam. No hot loop is rewritten; no comparator, cursor, opcode dispatch, or
const-generic kernel is de-monomorphized.

The plan is staged so the load-bearing structural fixes (Stage A) land first and
the large mechanical file splits (Stage C) land last, each behind a green build /
`make test` / `make e2e` gate.

---

## 1. Current architecture

Eight logical subsystems, layered overwhelmingly downward already:

| Layer (intended) | Subsystem | Big files |
| --- | --- | --- |
| foundation | `util`, `xxh`, `sys`, `log`, `layout` | — |
| data layout | `schema` | schema.rs (1863) |
| storage repr | `storage` (batch/columnar/merge/heap/range_key) | batch.rs (2546), merge.rs (2880), read_cursor.rs (2604) |
| storage LSM | `storage` (table/memtable/shard_*/wal/compact) | table.rs (1834), memtable.rs (1612), shard_index.rs (1644), shard_reader.rs (1449), compact.rs (1349) |
| operators | `expr`, `ops` | join.rs (2717), exchange.rs (2070), linear.rs (1963) |
| query core | `compiler`, `vm`, `dag` | compiler.rs (4403), vm.rs (2968), dag.rs (2396) |
| catalog | `catalog` | store.rs (1965) |
| runtime | `runtime` (master/worker/reactor/sal/w2m/wire) | master.rs (4366), reactor/mod.rs (4235), worker.rs (3084) |

### The six concrete problems

1. **C1 — `schema ↔ storage` is a genuine production cycle.** `schema.rs` reaches
   *up* into `storage` at exactly four sites: `:7` `use crate::storage::MemBatch`,
   `:100` `encode_order_preserving_pk`, `:430` `partition_for_pk_bytes`, `:833`
   `&mut crate::storage::PkBuf`. All four are pure layout/key operations or
   dependency-free value types that belong *in or below* schema, not up in storage.
   `storage → schema` is ~57 legitimate downward refs. **This is the only
   production cycle in the engine.**

2. **God-files braiding 3–6 cohesive concerns.** Twelve non-test files exceed 1900
   lines; each fuses cleanly separable clusters (see §6). They are the dominant
   structural debt.

3. **Wrong-direction intra-`storage` edge.** `storage/batch.rs` (the pure
   in-memory representation) reaches *down* into the disk half: `:11`
   `use super::shard_file`, plus `super::wal` block-layout calls (batch.rs
   1475/1482/1494/1562/1606/1748). The repr type should not know the WAL/shard
   wire format.

4. **Misplaced cross-cutting global state.** `compiler.rs:23/31` own
   `pub static WORKER_RANK` / `NUM_WORKERS` — a multi-process runtime concern
   written by `runtime/worker.rs:409` and read only inside `compiler.rs`
   (1135, 1609). The SQL planner is the wrong owner.

5. **Over-broad `pub` surface.** A binary crate has no external API, so every plain
   `pub` is crate-wide reach but falsely signals a boundary and masks the small
   real ones. Many storage/query-core/runtime internals are `pub` with zero
   out-of-module consumers.

6. **Facade-bypass / raw-pointer leaks.** `compiler.rs` (1066/1845/1869) and
   `runtime/master.rs:3375` reach into `crate::ops::util::` / `crate::ops::index::`
   internals; `ops/index.rs` exposes a raw `pub table: *mut storage::Table`;
   `catalog/store.rs` hands out `get_dag_ptr -> *mut DagEngine`.

### Three smells that turned out **not** to be cycles (verified)

- **C2 `compiler → catalog → dag → compiler`:** refuted. The only
  `compiler → catalog` edge is `#[cfg(test)]`-only (compiler.rs:3500/3542, inside
  `mod tests`); `catalog` has zero non-test `crate::compiler` refs. The real edges
  are a clean one-directional chain `catalog → dag → compiler`.
- **C3 `storage → dag`:** refuted as production. The sole `crate::dag` ref under
  `src/storage/` is `partitioned_table.rs:1036`, inside `#[cfg(test)] mod tests`
  (opened at 499–500). Test-only.
- **C4 `runtime/catalog` tangle:** refuted. `catalog → runtime` non-test edges = 0
  (one doc comment false positive). `runtime → dag`/`compiler` are clean downward
  spokes.

---

## 2. Target architecture

A strictly **acyclic, bottom-up layering**. Each layer may depend only on layers
below it. The acyclicity is achieved by **relocating ~5 misplaced symbols** and
introducing **two new leaf seams** (`schema::key`, `worker_ctx`) and one trait
(`RowView`); everything else is regrouping and splitting.

```
L0  foundation leaves   util · xxh · sys · log · worker_ctx(NEW)
                        (depend only on external crates: libc, xxhash-rust, gnitz_wire, io-uring)

L1  data layout         schema  =  schema/{descriptor, german_string, locator, key(NEW)}
                        key owns: encode_order_preserving_pk · opk_key · compare_pk_bytes
                                  · partition_for_pk_bytes · PkBuf   (all relocated DOWN from storage)
                        depends on: util · xxh · worker_ctx · gnitz_wire
                        ── schema imports NOTHING from storage (C1 broken) ──

L2  storage repr        batch · columnar · merge · scatter(NEW) · heap · range_key · batch_pool · BatchBuilder(moved in)
                        depends on: schema · util · xxh

L3  storage LSM         table · memtable · shard_index · shard_reader · shard_file · wal
                        · manifest · compact · read_cursor · batch_wire(NEW) · layout(folded in)
                        depends on: storage-repr · schema · util · xxh · sys

L4  operators           expr · ops::{linear, reindex(NEW), join, exchange, distinct, cogroup, reduce, scan, index, util}
                        depends on: storage · schema · expr · util · xxh

L5  query core          compiler::{load, optimize, emit} · vm::{builder, exec} · dag::{store_handle, scheduler}
                        depends on: ops · expr · storage · schema · worker_ctx · util

L6  catalog             catalog::{store_io, registry, metadata, partition_lsn, ddl, hooks, cache, bootstrap, validation}
                        depends on: dag · storage · schema · gnitz_wire

L7  runtime             runtime::{bootstrap, master/*, worker/*, executor, committer, reactor/*, sal, wire, w2m, sys}
                        depends on: catalog · dag · compiler · ops · storage · schema · worker_ctx · sys · util · xxh
```

New seams introduced (all single-definition, no duplication):

- **`schema::key`** (L1 leaf submodule) — the OPK/key primitives moved down from
  storage. Co-located with the schema that defines their column order.
- **`worker_ctx`** (L0 leaf) — the worker-rank globals moved out of the planner.
- **`RowView`** (`pub(crate)` trait in schema) — the 3-method read interface that
  lets schema's locator methods read a batch without naming `storage::MemBatch`.
- **`storage::scatter`, `storage::batch_wire`, `ops::reindex`** — extractions that
  remove the wrong-direction edges and relieve three god-files.

---

## 3. Guardrails (every workstream obeys these)

- **No de-monomorphization.** All hot paths stay generic-monomorphic or
  concrete; **never** `dyn`/`Box` on a per-row/per-epoch path. Same-crate moves
  preserve cross-module `#[inline]`; keep every `#[inline]` and `const`-generic
  parameter **verbatim**.
- **Single definition, relocate don't copy.** Every "move" re-homes the one
  definition and (where churn would be large) re-exports it from the old path.
  A forked OPK encoder / promotion key / region-layout helper silently breaks
  byte-equality and co-partitioning, so duplication here is a correctness bug,
  not just debt.
- **Preserve the load-bearing invariants** in §7 (region convention, (PK,payload)
  sort, single-source-per-epoch, trace-cursor snapshotting, w2m lock-free
  protocol). Splits move these clusters whole; they never reorder them.
- **Gate every workstream** on `cargo build` (warnings denied — fix, never
  suppress), `make test`, and for runtime/IO changes `make e2e`. The OPK
  relocation additionally requires a **byte-diff** check (§8), not just a green
  test run.

---

## 4. Stage A — break the cycle, remove misplaced state (P0)

### W1 — Create `schema::key`; relocate OPK encoder, partition hash, `PkBuf`
*Breaks C1 (3 of 4 edges). Depends on: none. Effort: M.*

**Rationale.** OPK encode/route and the PK key buffer are pure layout/key
operations that belong below both schema and storage. Moving them down removes
three `schema → storage` up-edges and makes them available to schema without an
upward reach.

**Scope.** new `src/schema/key.rs`; `src/schema.rs`; `src/storage/columnar.rs`;
`src/storage/partitioned_table.rs`; `src/storage/manifest.rs`; `src/storage/mod.rs`.

**Steps.**
1. Create `schema/key.rs` (turning `schema.rs` into the `schema/` directory module
   — see W2/§6 for the rest of the split).
2. Move **verbatim**, keeping `#[inline]`:
   - `encode_order_preserving_pk` + `opk_key` from `storage/columnar.rs:292`,
   - `compare_pk_bytes` from `storage/columnar.rs:146`,
   - `partition_for_pk_bytes` from `storage/partitioned_table.rs:479`,
   - `PkBuf` from `storage/manifest.rs:71` (dependency-free value type).
3. In `storage/mod.rs`, `pub(crate) use crate::schema::key::{…}` re-exporting all
   five so storage's ~57 internal call sites compile unchanged (path alias only —
   no second definition, no perf cost).
4. Repoint `schema.rs:100/430` to `crate::schema::key::` and drop those
   `crate::storage::` paths; `schema.rs:833`'s `PkBuf` is now schema-internal.
5. **Verify:** `cargo build` + `make test` green; `grep crate::storage src/schema*`
   shows only `MemBatch` remains (handled by W2).

**Perf.** `encode_order_preserving_pk` and `partition_for_pk_bytes` are per-row on
ingest/backfill/exchange/join. Move bodies **byte-for-byte**; the
narrow (`≤16` → `widen_pk_be`) vs wide (`xxh >> 56`) split in
`partition_for_pk_bytes` must stay bit-identical so producer and consumer
co-partition. **Dup.** Do not leave a compat copy behind — re-export the moved
item. A forked encoder breaks byte-equality ⟺ PK-equality.

### W2 — Break the last `schema → storage` edge with a `RowView` trait
*Breaks C1 (4th edge), fully. Depends on: W1. Effort: M.*

**Rationale and chosen design.** `ColumnLocator` (a 4-byte `Copy` enum) and
`IndexKeySpec` are **pure schema-derived data**; only their *read methods*
(`is_null`/`bytes`/`native_key`/`route_key`/`write_span`/`key_bytes`) name
`MemBatch`, as a `&MemBatch` parameter. `SchemaDescriptor::locate()`
(schema.rs:604) **returns `ColumnLocator`**, and `IndexKeySpec` has private fields
(`n`, `locators`, `idx_cols`). Therefore **moving these structs into storage would
re-create the cycle** (schema's `locate()` would return a storage type) or force
exposing `IndexKeySpec`'s private fields. The clean break keeps the structs in
schema and abstracts only the batch parameter:

1. Define in schema (e.g. `schema/locator.rs`) a `pub(crate) trait RowView<'b>`
   with the three accessors the methods actually use:
   `get_null_word(row) -> u64`, `get_pk_bytes(row) -> &'b [u8]`,
   `get_col_ptr(row, slot, size) -> &'b [u8]`.
2. Change the six read methods from `&MemBatch` to `&impl RowView<'b>` /
   `<V: RowView<'b>>`, keeping `#[inline]` and the `'b` batch-data lifetimes exactly.
3. In storage (next to the `MemBatch` definition in `storage/merge.rs`),
   `impl<'b> RowView<'b> for MemBatch<'b>` forwarding to the existing accessors.
4. Delete `use crate::storage::MemBatch` from `schema.rs`. Call sites
   (`ops/exchange.rs:59/234`, master/worker preflight, catalog validators) are
   unchanged — type inference picks `V = MemBatch`.
5. **Verify:** `grep crate::storage src/schema*` is empty; `cargo build` +
   `make test` green; `cargo build` shows no inlining/codegen warnings.

**Perf.** Zero-cost: the trait is monomorphized at every concrete call site
(all same-crate), so codegen is identical to the current direct calls;
`#[inline]`, the `'b` borrows, and `key_bytes`'s `PkBuf` reuse (no re-copy) are
preserved. **Never** take `&dyn RowView` — static dispatch only. **Dup.** One
`RowView` impl; the `§6` null-bitmap/offset math stays solely in the locator
methods — callers must not inline their own.

### W3 — Relocate `WORKER_RANK`/`NUM_WORKERS` into a `worker_ctx` leaf
*Removes problem #4. Depends on: none. Effort: S.*

**Rationale.** Moves multi-process global state out of the SQL planner into an L0
leaf both the one writer and the one reader depend on downward. The only edges
are `runtime/worker.rs:409` (writer, via `set_worker_rank`) and `compiler.rs`
internal reads (1135, 1609) — there are **no `ops` readers**.

**Steps.**
1. Create `src/worker_ctx.rs` (L0 leaf) holding the two statics **private to the
   module** plus `pub(crate)` accessors `set_worker_rank` / `worker_rank` /
   `num_workers` (moved verbatim from compiler.rs:37–47).
2. Repoint `runtime/worker.rs:409` to `crate::worker_ctx::set_worker_rank`.
3. Repoint `compiler.rs:1135/1609` to `crate::worker_ctx::worker_rank` /
   `num_workers`; delete the statics + accessors from compiler.rs.
4. **Verify:** no `compiler::{worker_rank,num_workers,WORKER_RANK,NUM_WORKERS}`
   refs remain crate-wide; build + test green.

**Perf.** Accessors stay plain `Relaxed` atomic loads — no lock, no per-call
lookup, no parameter threading onto hot loops.

### W4 — Relocate `BatchBuilder` to `storage`
*Removes a misplaced cross-layer utility. Depends on: none. Effort: S.*

**Rationale.** `BatchBuilder` (catalog/utils.rs:9) builds a `storage::Batch` from a
schema and holds **no catalog state**, yet it is consumed in production by
`runtime/executor.rs:1012` (a runtime → catalog reach for a storage-level utility),
plus catalog internals and compiler tests. Its home is storage.

**Steps.**
1. Move `BatchBuilder` into storage (alongside `Batch` in `storage/batch.rs`).
2. `pub(crate) use crate::storage::BatchBuilder;` from `catalog/mod.rs:64` so all
   ~150 existing `crate::catalog::BatchBuilder` sites (overwhelmingly tests) and
   catalog's own ddl/bootstrap/store callers compile unchanged.
3. Repoint `runtime/executor.rs:27` to `crate::storage::BatchBuilder` (one line) —
   deleting the runtime → catalog-for-a-builder edge.
4. **Verify:** build + test green.

**Dup.** Single definition in storage; catalog and the compiler test re-import it,
they do not get their own copy.

### W5 — Relocate the `storage → dag` test assertion (test-only C3)
*Makes storage `dag`-free at every level. Depends on: none. Effort: S.*

Move the `StoreHandle::Partitioned` recovery_lsn/current_lsn dispatch assertion at
`partitioned_table.rs:1036` (with its setup, inside `mod tests` at 499–500) into a
`dag`-side test that owns `StoreHandle`; share any `PartitionedTable` fixture via
`test_support` (do not copy the min-vs-max-LSN setup). **Verify:** no `crate::dag`
ref remains anywhere under `src/storage/`; `make test` green.

---

## 5. Stage B — fix wrong-direction edges, narrow surface (P1)

### W6 — Extract `storage::batch_wire` (repr stops reaching into the disk half)
*Removes problem #3; shrinks batch.rs. Depends on: none. Effort: M.*

Move the ~700-LOC wire/shard serialization cluster (`wire_byte_size`,
`wire_byte_size_range`, `encode_range_to_wire`, `encode_to_wire`,
`decode_from_wal_block`, `write_as_shard`, `write_as_shard_with_flags`, and the
free `write_to_batch` / `decode_mem_batch_from_wal_block`; batch.rs ~1473–1748)
into a new `src/storage/batch_wire.rs` in the LSM cluster. Delete
`use super::shard_file` and the `super::wal` calls from `batch.rs`; re-export the
moved items from `storage/mod.rs` for callers in `ops/{scan,distinct,exchange}`
and `runtime/wire`. **Verify:** `batch.rs` imports no disk-half module; build +
test green.

**Perf.** Serialization runs per-flush/per-IPC, not per-row — perf-neutral. Keep
region-copy loops `#[inline]`; **no** trait-object `SerializeTarget`; region
layout / `pk_stride` byte-identical (§6). **Dup.** `batch_wire` reads stride/offset
from the `SchemaDescriptor` / `MemBatch` view; it does **not** re-derive region math.

### W7 — Extract `ops::reindex` (exchange stops reaching into linear)
*Removes an intra-`ops` leaky reach; relieves two files. Depends on: none. Effort: M.*

`ops/exchange.rs` reaches up into `ops::linear`'s reindex internals
(exchange.rs 240/301/303/1943/2009/2056); half of `linear.rs` is in fact PK-promotion
infrastructure, not linear operators. Create `src/ops/reindex.rs` and move
`PkPromoter`, `ColPromoter`, `ReindexPacker`, `german_string_promote_key`, and the
private `reindex_hash_row` (linear.rs ~398–700) into it. Repoint `exchange.rs` and
`linear.rs` to `crate::ops::reindex::`; leave `linear.rs` as true linear ops only
(`op_filter`/`op_map`/`op_negate`/`op_union`/`op_null_extend`) and fix its doc
header. **Verify:** `exchange` no longer names `crate::ops::linear`; build + test
green.

**Perf.** `ReindexPacker` / `german_string_promote_key` are per-row and **must**
produce byte-identical keys on producer and consumer (hash invariant). Keep them
`#[inline]` and monomorphic; same-crate move preserves inlining. **Dup.** Extract
once, depend twice — set each item's visibility **deliberately** (`reindex_hash_row`
becomes `pub(super)`/`pub(crate)` as needed); never let exchange grow its own
promotion copy.

### W8 — Inter-module surface-reduction sweep
*Removes problems #5 and #6. Depends on: none (lands before the Stage C splits so
the new sub-modules inherit correct visibility). Effort: M.*

Narrowing is compile-time only and can only **help** the optimizer (it never
blocks inlining when the item itself — not a forwarding wrapper — is re-exported).
Apply in batches, building between each:

1. **storage/columnar.rs → pub(crate):** `ColumnarSource`, `compare_rows`,
   `binary_lower_bound`, `gallop_lower_bound_bytes` (zero out-of-storage consumers).
2. **query-core → pub(crate):** `compiler::{compile_view, CompileOutput, SubPlan,
   SideBPlan, ExternalTable, cursor_read_i64}` (only `dag` consumes);
   `vm::{execute_epoch, execute_epoch_multi, ProgramBuilder, VmHandle, Instr}`
   (only compiler + dag consume).
3. **reactor test scaffolding →** `#[cfg(test)] pub(crate)/pub(super)`:
   `inject_cqe`, `inject_parked_reply`, `test_init_state`, `test_route_reply`,
   `test_route_scan_slot`, `Ring`, `Cqe`.
4. **runtime → pub(crate):** `ServerExecutor`, `WorkerProcess`, `MasterDispatcher`
   and their bare-`pub` method clusters; `sal` `FLAG_*`/`BACKFILL_*` + wire codec
   fns; `w2m_ring` `init_region`/`try_publish`/`try_consume`. **Keep public:**
   `server_main`, `sal::MAX_WORKERS`.
5. **Facade re-exports** in `ops/mod.rs` for `make_gi_schema`, `make_avi_schema`,
   `all_payload_null_mask`, `AVI_AV_BYTES` so `compiler.rs` (1066/1845/1869) and
   `master.rs:3375` import `crate::ops::X`, not `crate::ops::index::X` /
   `crate::ops::util::X`.
6. **Raw-pointer leaks →** make `ops/index.rs` `GiDesc`/`AviDesc`
   `pub table: *mut storage::Table` a `pub(crate)`/private field behind constrained
   accessors; give `catalog::get_dag_ptr` (`store.rs` ~1536) a typed handle return
   or `pub(crate)` visibility instead of a bare public `*mut DagEngine`.
7. **schema → pub(crate):** `PayloadCmpKind`, `assemble_wide_pk` (two in-crate
   runtime callers).
8. **Fold `layout.rs` into `storage::layout`** (`pub(crate)` constants; only
   storage consumes it) and drop it from the `main.rs` module roster.
9. **Delete dead `sys::fallocate_keep_size`** (sys.rs:16) **together with** its only
   caller, the self-referential test `test_fallocate_keep_size` (sys.rs:175–180).
   It has no production caller; the test exists only to exercise otherwise-dead
   code. Removing the function without the test would break the build.

**Verify:** build + test green after each batch.

---

## 6. Stage C — split the god-files (P2)

All splits are mechanical carves along the seams below; each preserves the
do-not-touch clusters in §7 **whole**. They land after W8 so new sub-modules get
correct `pub(super)`/`pub(crate)` visibility. Splits within one subsystem are
independent and can land in any order.

### File-split reference

| File | LOC | Split into | Seam |
| --- | --- | --- | --- |
| `schema.rs` | 1863 | `schema/{descriptor, german_string, locator, key}.rs` + `mod.rs` | descriptor (layout core) · german_string (blob-string codec) · locator (`ColumnLocator`/`IndexKeySpec`/`RowView`) · key (W1) |
| `storage/batch.rs` | 2546 | `batch.rs` (residual repr) + `batch_wire.rs` (W6) | repr vs wire/shard serialization |
| `storage/merge.rs` | 2880 | `merge.rs` (residual) + `scatter.rs` | sort-merge consolidation vs exchange repartition (`scatter_copy:668`/`scatter_multi_source:895`/`scatter_unified_sources_with_weights:1049`) |
| `storage/read_cursor.rs` | 2604 | `read_cursor/{source, mod, output, handle}.rs` | source accessors · merge state machine · output/drain paths · public `CursorHandle` |
| `storage/table.rs` | 1834 | `table/{flush, mod}.rs` | two-phase flush state machine (`flush*`/`FlushOutcome`/`ShardRename`/fd `Drop`) vs `Table` ingest/cursor/lookup/`compact_if_needed` |
| `storage/shard_index.rs` | 1644 | `shard_index/{index, persist}.rs` | in-memory `ShardIndex`/`PendingShard`/`MappedShard` state vs manifest serialize/load/recover glue |
| `storage/memtable.rs` | 1612 | `memtable/{runs, lookup}.rs` | sorted-run management (`upsert_sorted_batch`/`consolidate_*`/`snapshot_runs`) vs point-lookup (`lookup_pk_bytes`/`find_*`/bloom) |
| `storage/shard_reader.rs` | 1449 | `shard_reader/{open, access}.rs` | cold open-time region/encoding validation vs hot per-row `RegionView` accessors + xor8 probe |
| `storage/compact.rs` | 1349 | `compact/{merge, policy}.rs` | N-way (PK,payload) merge kernel (`merge_and_route`) vs `compact_shards` orchestration + shard selection |
| `ops/join.rs` | 2717 | `join/{delta_trace, range, rowwrite, delta_delta}.rs` | delta-trace (incl. outer/anti/semi) · range merge (`range_merge_walk:352`) · shared row writers (`write_join_row*`) · delta-delta (`filter_join_dd_with_payload_inner<RowCmp>`) |
| `ops/exchange.rs` | 2070 | `exchange/{router, relay}.rs` | routing (`PartitionRouter`/`worker_for_partition`/`route_partition_key`/`RouteMode`) vs relay (`relay_walk_inner`/`op_relay_*`/`op_repartition_*`) — the relay merge walk (570–790) is the dense hot loop |
| `ops/linear.rs` | 1963 | `linear.rs` (residual) + `reindex.rs` (W7) | true linear ops vs PK-promotion infra |
| `compiler.rs` | 4403 | `compiler/{load, optimize, emit, mod}.rs` | load (`load_circuit:319`/`topo_sort:438`, the dag-facing API) · optimize (`annotate:708`/`opt_*`) · emit (`emit_node:1191`/`build_plan:2050`) · `compile_view:2321` orchestrator |
| `vm.rs` | 2968 | `vm/{builder, exec, mod}.rs` | `ProgramBuilder` vs `execute_epoch:750` + the opcode match (kept whole) vs shared `Instr`/`Program`/`RegisterFile` types |
| `dag.rs` | 2396 | `dag/{store_handle, mod}.rs` | `StoreHandle` storage adapter (catalog's real inbound target) vs `DagEngine` scheduler |
| `catalog/store.rs` | 1965 | `catalog/{store_io, registry, metadata, partition_lsn}.rs` | four `impl CatalogEngine` blocks: io/ingest · id registry · fk/index metadata · partition+LSN bookkeeping |
| `runtime/master.rs` | 4366 | `master/{dispatch, unique_filter, preflight}.rs` | SAL dispatcher · unique-filter cache · PK/unique preflight + violation formatting (`GatherMap:3004`, ~1200 LOC) |
| `runtime/worker.rs` | 3084 | `worker/{exchange, reply, fsync}.rs` | exchange-wait re-entry (`do_exchange_wait:1801`/`replay_deferred_ticks`) · response framing · io_uring fsync helpers |
| `runtime/reactor/mod.rs` | 4235 | `reactor/{runloop, futures, conn, mod, tests}.rs` | run loop + `REACTOR` ptr · the 7 IO futures (1609–1953) · client-conn framing (merge `io.rs`) · `dispatch_cqe`/`route_*` glue · ~2280 inline test LOC → `tests.rs` |

Two test files are also oversized — `ops/reduce/tests.rs` (5450) and
`catalog/tests/index_tests.rs` (2857). They are **deferred**: test files carry no
inter-module surface and splitting them yields lower architectural value than the
production carves above. Revisit only if they slow iteration.

### Workstream grouping for Stage C

- **W9 (storage-repr, L):** `batch.rs` residual, `merge.rs`→`scatter.rs`,
  `read_cursor.rs` four-way. Depends on W6.
- **W10 (storage-LSM, L):** `table.rs`, `shard_index.rs`, `memtable.rs`,
  `shard_reader.rs`, `compact.rs`. Depends on none (independent of W9).
- **W11 (ops, L):** `join.rs` four-way, `exchange.rs` router/relay. Depends on W7
  (linear/reindex) for the linear residual; otherwise independent.
- **W12 (query-core, L):** `compiler.rs`, `vm.rs`, `dag.rs`. Depends on W3, W8.
- **W13 (catalog, L):** `store.rs` four-way `impl` blocks. Depends on W8.
- **W14 (runtime, XL):** `master.rs`, `worker.rs`, `reactor/mod.rs`. Depends on W8;
  gate on `make e2e`.

---

## 7. Do-not-touch — hot paths and load-bearing invariants

Splits **move these clusters verbatim**; they never reorder, wrap in `dyn`, cache a
sort key, or re-extract a local copy of them.

1. **The fused k-way merge + inline consolidation kernel.** `heap.rs::drive_merge`
   is the **sole** pending-group drain owner (called by `merge.rs:513`,
   `compact.rs:214`, `read_cursor.rs:1048`); plus `merge_batches_inner`,
   `sort_and_consolidate`, `fold_sorted`, the keyless 8-byte-`HeapNode` loser tree,
   and the `with_pk_ord!`/`with_payload_cmp!` stride-dispatch macros. The
   read_cursor and compact splits must keep **calling** `drive_merge`, never
   re-extract a drain loop (would fork the §2/§4 (PK,payload) total order).
2. **`compare_pk_bytes`** (the single OPK memcmp every ordered op runs),
   **`compare_rows`** (`#[inline]`, `total_cmp` float order),
   **`encode_order_preserving_pk`** (the §1/§6 OPK bijection — unsigned byte order
   == typed PK order, byte-equal == PK-equal), **`partition_for_pk_bytes`**
   (narrow/wide hash). W1 must move these **byte-for-byte**; a second definition
   silently breaks consolidation/dedup and co-partitioning.
3. **`ColumnLocator`/`IndexKeySpec` per-row accessors** (`bytes`/`native_key`/
   `route_key`/`write_span`/`key_bytes`), their `'b` batch-memory borrows, and
   `key_bytes`'s `PkBuf` reuse (no re-copy). W2's `RowView` generalization must keep
   `#[inline]`, the lifetimes, and the reuse exactly; static dispatch only.
4. **`vm::execute_epoch`'s opcode-dispatch match** and `expr`'s `eval_batch`
   per-op interpreter loop. The vm split keeps the **whole** match in `vm/exec.rs`;
   boxing per-opcode handlers or splitting arms breaks monomorphization.
5. **The bilinear join 2-term symmetric form** and delta-trace **cursor
   snapshotting at tick start** (`z⁻¹(I)`); **single-source-per-epoch** scheduling
   in `dag.evaluate_dag`. Correctness, not just perf — the join (W11) and dag (W12)
   splits must not reorder epoch scheduling or snapshot timing (would violate
   `dA ⋈ dB = 0`).
6. **Exchange-wait defer-then-replay ordering** (TICK/DDL deferred inside
   `do_exchange_wait`), the **w2m lock-free protocol** (`fetch_or(MASTER_PARKED)`
   **before** the `reader_seq` snapshot — the lost-wake guard), and the
   **no-aliasing-across-`await`** raw-pointer discipline (`executor::Shared`
   `cat()`/`disp()`, `get_dag_ptr`). The W14 master/worker/reactor splits move these
   whole and keep accesses on the reactor thread.
7. **Const-generic / comparator-monomorphized per-row kernels:**
   `scatter_unified_pk_nbm<const PKS>`, `relay_walk_inner<const DO_REFILL, Sel, Route>`,
   `filter_join_dd_with_payload_inner<RowCmp>`, `cogroup<M: SortedKeyStream>`,
   `op_union_merge_inner<RowCmp>`, `ReindexPacker`/`german_string_promote_key`.
   The W7/W9/W11 moves are safe **only** if the const generics and `#[inline]` are
   carried verbatim — no `dyn`, no `Box`, no second promotion copy.
8. **The single catalog write-path spine:** `submit → precheck_sys_ingest →
   apply_local → fire_hooks`, and the hand-maintained per-family `fire_hooks`
   dispatch order. The W13 `store.rs` split must not create a second ingest entry
   point that skips precheck/hooks, and must keep the hook ordering in one place.

---

## 8. Verification strategy

- **Per workstream:** `cargo build` (warnings denied — fix, never `#[allow]`),
  `cargo clippy` clean, `make test`.
- **Runtime/IO workstreams (W5, W8 step 3–4, W14):** additionally `make e2e`
  (`GNITZ_WORKERS=4`) with a generous timeout; the e2e suite must rebuild the
  server (no stale binary). Always clean up the test data dirs afterward.
- **W1 (and any move of an OPK/promotion/region helper):** a **byte-diff** gate, not
  just a green test run. Build before and after, and confirm a fixed corpus of PK
  values encodes to **byte-identical** OPK and routes to the **same** partition
  under both the old and new definition. A forked encoder passes most tests while
  silently corrupting consolidation and routing — only a byte-diff catches it.
- **Cycle gate (after Stage A):** assert no `crate::storage` ref in `src/schema*`
  and no `crate::dag` ref under `src/storage/` (including `cfg(test)`), so C1 and
  C3 stay broken.

---

## 9. Sequencing summary

```
Stage A (P0, structural)   W1 ─┬─ W2            break C1
                           W3  │                worker_ctx leaf
                           W4  │                BatchBuilder → storage
                           W5  │                test-only C3 move
Stage B (P1, edges+surface) W6 │  W7  W8        batch_wire · ops::reindex · surface sweep
Stage C (P2, splits)       W9 W10 W11 W12 W13 W14   god-file carves (gated on W6/W7/W8 as noted)
```

W1–W8 are small, independent (except W2→W1), high-value, and low-risk; they
deliver the acyclic layering and the narrowed surface. W9–W14 are larger but
purely mechanical carves behind the §7 guardrails. The engine is shippable and
green after **every** workstream.
