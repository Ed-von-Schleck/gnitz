# Gnitz in Gnitz — is it worth it, where, and how far?

Read of the tree at `1c57266c`.

**Answer, in two halves.**

*Replacing* engine machinery with gnitz machinery is not worth it — every
candidate is priced and rejected in §3, and §1–§2 explain why the storage layer
is already fully dogfooded and the catalog's derived state correctly is not.

Where dogfooding does pay is narrower and sits elsewhere. gnitz has **two kinds
of derived relation — views and indexes — and treats them as unrelated
species when they are one concept in two roles**: a view is a derived relation
you *query*, an index is a derived relation you *probe*. The ingest path already
treats them identically. Everything else does not:

| | view | secondary index |
|---|---|---|
| maintained by | compiled circuit | inline projection |
| **recovery** | checkpoint + generation validity + resume | **erased and fully rebuilt every boot** |
| placement | inferred from sources | always local-unpartitioned |
| **may exist on a view** | yes (view-on-view) | **no** — though maintenance would already work |
| dependency ordering | `order_by_view_deps` | none |

The two gaps in bold are §5. They are the cheap half of unification — giving
indexes what views already have — and neither requires the expensive half
(making an index a view, which §3 rejects on measurement).

§6 gives the rule that decides future cases.

---

## 1. Storage: already fully dogfooded

Every production site that creates durable state in the engine:

| Site | What it stores |
|---|---|
| `catalog/bootstrap.rs:19` | the nine system catalog tables |
| `storage/lsm/partitioned_table.rs:168` (via `hooks.rs::build_partitioned_storage`) | base tables **and** view output stores |
| `catalog/utils.rs:116` `new_index_table` | secondary indexes and the AggValueIndex |
| `catalog/ddl.rs:428` | the boot index rebuild |
| `query/vm/mod.rs:399` | **operator trace tables — the DBSP integrals themselves** |

That is the complete list. The integral `I(A)` is a gnitz `Table`; a secondary
index is a gnitz `Table`; the catalog is nine gnitz `Table`s. There is no
long-lived, data-scaled structure in the engine that is not one.

The transient exceptions are all correctly plain memory: `pending_deltas` /
`pending_relays` (one tick of in-flight batches, `worker/mod.rs:135,176`),
`AdhocFold::by_hash` (request-scoped, capped), the CREATE UNIQUE INDEX `seen`
set and its `SpillSort`, and `enforce_unique_pk`'s per-batch state.

The one long-lived structure that scales with **data** and is not a table is the
master's unique filter (`unique_filter.rs`) — see §4. It should become a Bloom
filter, not a table.

## 2. Reactivity: dogfooded where it pays

The catalog is genuinely delta-driven and it is the right call:

- DDL is `±1` weighted rows; a rename is a `-1,+1` rewrite pair whose net stays
  live, and the register hooks gate on *net* liveness, not on their own sign.
- Rollback is **negation** (`compensate_stage_a`) — the Z-set group inverse used
  as transaction compensation.
- A drop cascade is a **key-range retraction** over `[pack(owner,0),
  pack(owner+1,0))`.
- The query plan is data: `circuit_nodes` / `circuit_edges` /
  `circuit_node_columns`. Compiling a view is a scan.
- The system tables self-describe from the same `gnitz-wire` column slices the
  physical schemas are built from, and are readable through the ordinary
  seek/scan path (`catalog/store_io.rs:40,302`).

**Where it stops, and why that is right.** The tempting next step is to make the
*derived* catalog state (`catalog/cache.rs`) into views over those tables. A
sweep of every long-lived map in `gnitz-engine` shows why not:

| State | Scales with | Read on |
|---|---|---|
| `schema_by_name/_by_id`, `entity_by_qname/_by_id` | relation count | DDL, name resolution |
| `members_by_schema` | relation count | DROP guard only (`registry.rs:138`) |
| `col_defs`, `schema_wire_cache`, `schema_version` | columns ≤ `MAX_COLUMNS` = 65 | reply path, DDL |
| `fk_by_child/_by_parent`, `needs_lock` | FK edge count | **push hot path** |
| `indices_by_owner`, `index_by_name/_by_id` | index count | DDL |
| `DepMap.forward/reverse` | circuit node count | tick dispatch |
| `user_sequences`, `invalid_views` | relation count | boot, SERIAL |

Every one is **schema-scaled** — bounded by relation, column or index count, and
rebuilt only on DDL. For those a view is strictly worse:

1. **The read gets slower.** `fk_lock_set` is documented as "a plain borrow" on
   the push path. A view's output is an LSM table, so that read becomes a bloom
   probe plus a memtable/shard walk. Trading a `FxHashMap::get` for an LSM point
   lookup on the write path is a regression with no offsetting win.
2. **Nothing to amortize.** Incremental maintenance pays when recomputation is
   expensive. Recomputing `members_by_schema` costs a scan of a table with one
   row per relation.
3. **The bootstrap is circular.** The catalog must be readable before any view
   exists, so the base sys tables stay as they are regardless; only the derived
   layer could move — the layer that is already cheapest.

The hand-written appliers are the correct engineering choice. Their ordering
hazards (sign-partitioning, net-live gating, purge-after-cascade) are real
complexity, paid once in one 363-line file, against a hot-path read a view
cannot match.

## 3. The DBSP language: priced, and rejected

### Constraints as violation views (PK / UNIQUE / FK)
The four preflight rules really are "is this Z-set difference empty?". But the
physical work is identical either way: an anti-join against a distributed
relation *is* the key probe `preflight.rs` already issues. The change buys a
declarative spelling and costs a speculative-apply / compensate cycle on every
DML write plus a new "maintain this view before the ACK" execution mode.

### Secondary indexes as views
The projection is already a shared inline packer: `batch_project_index`
(`query/dag/ingest.rs:374`) drives `IndexKeySpec::write_entry` — the same span
encoder the unique filter and the seek path use — into a stack buffer with no
per-row allocation. A compiled circuit adds VM dispatch to the push path and
removes no shared code. The one genuine prize, indexes inheriting the view
checkpoint/resume path instead of being rebuilt every boot, is available on its
own without making an index a view.

### Recursion / fixpoint (for the dependency graph)
Sized against Feldera (`~/git/feldera`, `8681eaa11`). Recursion there is not an
operator; it is a **nested circuit with its own logical clock**:
`Circuit::fixedpoint` / `iterate` build a child circuit
(`circuit/circuit_builder.rs`, 9 339 lines with the fixedpoint machinery woven
through), every operator implements `fixedpoint(scope) -> bool`
(`operator_traits.rs:1085`), streams cross the boundary via `delta0`
(`operator/delta0.rs`) and feed back via `Z1`-with-export, and the child needs
its own scheduler.

gnitz is flat by construction — a topo-sorted DAG compiled to a VM program, one
`execute_epoch` per delta, cycles rejected outright — and distributed, so each
inner iteration would need its own exchange barrier through the master-mediated
SAL relay. This is a large product feature (recursive CTEs), decided on product
grounds. It is not a simplification: the ~250 lines it would displace
(`DepMap::closure`, `order_by_view_deps`) are correct, cheap and schema-scaled.

### Folding the ad-hoc read path into the view engine
There is no second engine to fold in. The kernels are already shared:

- `catalog/scan_spec.rs:642,654` — `compile_predicate` / `compile_projection`
  build `ScalarFunc::from_predicate` / `from_map`, and say so: "the same path the
  circuit compiler runs".
- `ops/reduce/adhoc_fold.rs` header — the accumulation kernel, group comparator,
  group-key hash and emission path "are the exact shared code a view's reduce
  runs".

What differs is the *execution strategy* — stream through a bounded scan versus
materialize operator state — which is a legitimate two-strategy design. The
client-side `exec/order.rs` is the merge half of a two-phase top-k whose first
phase is the worker's bounded sink; also not duplication.

### Ad-hoc joins via transient views
`dml/select.rs::reject_derivation` means no ad-hoc JOIN, set op or subquery is
answerable without `CREATE VIEW`. A real product gap, and transient views are
the natural fix — but it *adds* a lifetime-and-resource-bounding mechanism. A
feature decision, not a codebase improvement.

### Manifest / SAL / LSM / reactor / W2M as Z-sets
`RunSet` *is* an integral with a fold trigger; `merge_runs_to_consolidated` *is*
Z-set addition. The manifest is the file you need in order to read the store, so
storing it in a store is a bootstrapping loop. The reactor and rings are
microsecond-scale control plane.

## 4. The one gap

The master's `UniqueFilter` is the only long-lived structure in the engine whose
size scales with data and is not a gnitz table. It should not become one — it
should become a Bloom filter, reusing `storage/repr/bloom.rs`, which already
serves this exact "prove absence" role in `RunSet`. Measured 82× memory
reduction at every size, and it removes a hot-path cliff (the exact set caps at
1M spans and then permanently disables the broadcast elision that
`preflight.rs:894` calls "the steady state of a fresh-key insert stream").

Written up in `handovers/unique-filter-bloom.md`.

## 5. The two-species asymmetry — where dogfooding *is* worth it

Both findings are the same observation: a view and a secondary index are both
"a relation derived from another relation", and the engine only admits that on
the ingest path.

### 5a. An index is rebuilt from scratch at every boot; a view is not

`new_index_table` (`catalog/utils.rs:121`) stamps `RecoverySource::Rederive`:
erased at open, rebuilt by `backfill_all_indexes` (`catalog/ddl.rs:392`) from a
full scan of the local base slice, once per indexed table per worker per boot.
View outputs and operator traces get `RederiveCheckpointed` instead — load the
checkpointed shards iff the manifest generation matches, else rebuild.

The exclusion is stated as an implementation accident, not a design:

> Index tables are excluded: they live in `TableEntry::index_circuits`, not the
> plan cache, and stay erase-at-boot. — `query/dag/ingest.rs:240`

They are already visited by the base round (`collect_base_flush_tables` pushes
`ic.table_mut()`), where they hit `flush_prepare`'s early-out — `generation
.is_none() && recovery_source != SalReplay` — fold to RAM and publish nothing.
Nothing else stands in the way.

**The win.** Boot stops paying `O(base rows × indexes)` per worker and pays an
mmap instead. It also deletes `backfill_all_indexes` *and* its ordering
invariant, which is currently load-bearing and subtle:

> Must run after trim/rehome and BEFORE SAL replay — replay projects the
> unflushed committed tail into the index exactly once through
> `ingest_store_and_indices`, so a rebuild *after* replay would double-count
> every replayed row. — `catalog/ddl.rs:381`

With a checkpointed index that constraint evaporates: the index loads its shards
like the base loads its own, and the shared SAL-tail replay projects the tail
into both, exactly once. No separate index watermark is needed, because the
index is fed from the base's already-deduplicated replay.

**The cost, honestly.** This extends the generation-validity protocol to a third
relation kind, and that protocol — the recovery-start bump, the
flush-ordering invariant, the base-round early-out whose doc comment explains
that publishing on the wrong round yields weight-2 rows — is the most intricate
machinery in the codebase. The base and ephemeral rounds run inside one
committer sequence with pushes queued, so base@G and index@G are the same cut;
that is the property to verify first, not assume.

### 5b. A materialized view cannot be indexed

`validate_index_registration` (`catalog/write_path.rs:586`) rejects any owner
that is not a base table. Its stated justification:

> index projection runs only on the base-table DML paths
> (`ingest_store_and_indices`); view deltas land via the circuit-evaluation
> terminal-view moves, which never project into `index_circuits`. An index on a
> view would backfill once and then silently serve stale results.

**That does not describe the code.** Both view-output ingest paths reach
`ingest_store_and_indices`, which projects every `index_circuit`:

- terminal view — `evaluate_dag_multi_worker` calls
  `self.ingest_relation(view_id, out_delta.unwrap())` (`query/dag/exec.rs:337`)
  → `ingest_returning_effective` (`ingest.rs:88`) → not a base table, so the
  batch passes through unchanged → `ingest_store_and_indices` (`ingest.rs:119`);
- non-terminal view — `self.ingest_by_ref(view_id, …)` (`exec.rs:340`) →
  `ingest_store_and_indices` (`ingest.rs:79`);
- backfill — `execute_backfill_step` → `ingest_relation` (`exec.rs:291`).

`IntegrateSink` confirms it from the other side: it **emits no instruction** —
"the sink register's batch is what `execute_epoch_multi` extracts at epoch end"
(`query/compiler/emit.rs:572`). The only `Instr::Integrate` the VM runs is the
one emitted for `IntegrateTrace`, which writes an operator trace, not the view's
output store. (`gnitz-wire/src/circuit.rs:452` describing `OPCODE_INTEGRATE` as
"writes to view storage" does not match the emitter.)

So **steady-state maintenance would work unchanged**. But the guard is not the
only thing standing in the way, and the obstacles that remain are not the one it
names. Four gaps, each small and local:

1. **Boot double-count.** `backfill_all_indexes` runs at recovery step 3, before
   the invalid-view reset and rebuild at step 6. It iterates every relation with
   index circuits — views included — so it would build the index from the
   view's pre-reset store. `reset_view_output_for_rebuild`
   (`catalog/partition_lsn.rs:121`) replaces `entry.handle` with a fresh empty
   store and **does not touch `entry.index_circuits`**, so those rows survive the
   reset and the rebuild projects them a second time.
2. **No drop cascade.** `cascade_retract_indices` is called only from
   `hook_table_register`'s drop branch (`catalog/hooks.rs:413`);
   `hook_view_register`'s drop branch cascades circuit rows and columns only. A
   dropped view would orphan its IDX_TAB rows and index directory.
3. **No drain before backfill.** `CREATE INDEX` on a view scans the view's output
   store, which may have pending un-ticked deltas; the tick would then project
   the same rows again.
4. **The key is not identifying** (below).

**Why this matters more than it sounds.** In a database whose entire pitch is
incrementally maintained materialized views, every non-PK lookup on a view is a
full scan. The view is precisely the relation a user wants a predicate on.

**The key design.** A base table's PK is
unique per live row, so the index entry key `[span ‖ src_pk]` identifies exactly
one row and the index-gather is exact. A view's output PK is *not* unique — a
join view's output PK is the left input's PK, i.e. the join key, and several
output rows share it (`CLAUDE.md` §1). Two view rows sharing a PK but differing
in payload collide on one index key, so a seek resolves to a PK whose gather
returns rows that do not match the indexed value.

Two ways out, and this is the decision to make before anything is built:

1. **Row-identity in the key** — `[span ‖ content_hash]`, using the row-hash
   primitive the engine already has (`MapKind::HashRow` / `OPCODE_MAP_HASH_ROW`,
   built for set-op and DISTINCT identity). Exact, and self-contained.
2. **Re-filter after the gather** — cheaper to build, but it makes the index a
   hint rather than an access path, and `exec/residual.rs` today serves
   UPDATE/DELETE resolution, not index reads, so the re-filter does not exist yet.

A view index is also strictly *simpler* than a base-table one in one respect: it
is the non-unique case only. No UNIQUE on a view means no CREATE-UNIQUE-INDEX
pre-flight, no cross-worker duplicate merge, no unique filter.

## 6. The rule, for future cases

The whole analysis reduces to one test, worth applying before any future "should
this be a view / a table?" question:

> **Dogfood state that is long-lived and scales with data.
> Do not dogfood derivations that are schema-scaled, or that are read on the
> write path.**

Everything passing that test is already a gnitz `Table` (§1). Everything failing
it is correctly a `FxHashMap` (§2). The DBSP-language candidates (§3) all fail
it for the same reason — they move schema-scaled or hot-path work into a
mechanism built for data-scaled, amortizable work.
