# Replicated tables

## 1. Summary

A **replicated** table keeps a full copy on every worker. Joining a partitioned
fact to a replicated dimension is always local on every worker, with no exchange
on either side — and, unlike hash co-partitioning, with no constraint on the
fact's own distribution, so one fact can join *many* replicated dimensions
locally. This is the complement of `plans/per-relation-distribution-keys.md`
(hash distribution by a PK prefix): same *goal* (local joins against small
dimensions), disjoint *mechanism* (broadcast full copy vs hash-redistribute),
and a strictly wider applicability (the fact need not be distributed by the join
key).

`REPLICATED` is its own plan because it needs four things hash distribution does
not, none of which exist today:

- a **data-write broadcast** path (every worker ingests *and durably logs* the
  full row set, not a PK-scattered subset),
- a **single-source read** path (a scan/aggregate of a replicated table must
  consume one copy, not gather N),
- **distribution tracking through the circuit** — a relation is *partitioned*
  (disjoint across workers, reads gather) or *replicated* (full on every worker,
  reads single-source); replicated-ness propagates through joins, reduces, and
  set-ops and every consuming operator must respect it (§4.2), and
- **per-worker full replay** on recovery (each worker's SAL holds the whole
  table, not a partition subset).

The join itself needs **no new exchange machinery**: because a replicated
table's *writes* are already broadcast (every worker ingests the full delta),
the join only has to *skip* the exchange on both sides (§4.5). The substantial
new work is the write-broadcast path, the single-source read path, and the
distribution-tracking that keeps reduces/distinct over a replicated relation
from N-fold-multiplying.

## 2. Scope and non-goals

**In scope**
- `REPLICATED` DDL surface and validation, encoded as a `Distribution` enum
  mutually exclusive with hash-prefix distribution.
- Per-worker full-copy storage (single-partition `Table` on every worker).
- Broadcast of replicated-table DML to every worker's ingest **and SAL**.
- Local uniqueness enforcement on each worker's full copy, with a single-worker
  preflight for the master's accept/reject ACK.
- Single-source reads / no-gather for replicated relations (scan, point-lookup,
  and — the subtle case — aggregate/distinct/set-op directly over a replicated
  relation).
- Always-local join treatment against a replicated table (both sides skip
  exchange).
- FK-check treatment against a replicated parent (the existing broadcast check
  is already correct; a local check is an optimization).
- Per-worker full replay on recovery.

**Non-goals**
- Hash distribution by a PK prefix — see `plans/per-relation-distribution-keys.md`.
- A size cap on replicated tables (the cost is the user's to bound).
- **Master-resident replicated copies.** The master holds no user-table data
  today (§3); a design that lets the master serve replicated reads from its own
  copy is described as an alternative in §4.4 but is **not** the chosen path —
  it would teach the master to ingest and replay user data, a larger change.
- Replicating views or secondary indexes *as a declarable property*. (A view
  whose inputs are all replicated is *derived*-replicated; that propagation is
  in scope under §4.2, but a user cannot declare a view `REPLICATED`.)
- `ALTER … SET REPLICATED` / re-distributing an existing table. Distribution is
  fixed at `CREATE TABLE` (pre-production: no migration concern).

## 3. Current architecture (anchors)

Line numbers are approximate anchors (the tree drifts; cross-check by symbol).

- **Storage shapes** (`storage/partitioned_table.rs`): user tables are
  `PartitionedTable`s with `num_partitions == 256`; a hard assert restricts
  `num_partitions ∈ {1, 256}` (`:53-56`). `PartitionedTable::new` allocates one
  child `Table` per partition in `[part_start, part_end)` (`:73-82`), so the
  *number of physical partition Tables built is governed by the active range*,
  not by `num_partitions` alone. Single-partition tables take `Table[0]` bypass
  branches at `:114-116` (ingest), `:172-174` (open_cursor), `:197-199`
  (compacting cursor), `:259-261` (retract). A replicated table is a
  **single-partition `Table` holding the full copy on every worker**.
- **Master holds no user-table data** (`runtime/bootstrap.rs`): each worker is
  assigned a disjoint partition range via `partition_range(w, num_workers)`
  (`:27-36`) + `set_active_partitions` (`:454-455`); the **master**, after
  forking workers, calls `close_user_table_partitions()` then
  `set_active_partitions(0, 0)` (`:528-529`) — an **empty** range. The master
  builds no user-table partition Tables and the write path never ingests into
  the master (`write_commit_group` only scatters to worker SALs; see below). So
  a replicated read **cannot** be served from a master copy — single-source
  reads must target one worker. System tables are the only master-resident
  store, held as `StoreHandle::Borrowed` (`dag.rs:20-28`).
- **DDL** (`catalog/hooks.rs`): `hook_table_register` (`:164-243`) fires on the
  master **and every worker** (hooks run wherever a system-table batch is
  ingested), so per-worker storage construction needs no new fan-out. It decodes
  `flags` at `:178-179` (`is_unique = flags & TABLETAB_FLAG_UNIQUE_PK`) and
  builds storage via `build_partitioned_storage` (`:140-162`), which **hard-codes
  `NUM_PARTITIONS = 256`** at `:150` and passes the node's
  `active_part_start/active_part_end`. `TABLETAB_FLAG_UNIQUE_PK = 1`
  (`sys_tables.rs:43`) is the only flag bit; bits 1..63 are free.
  `TABLE_TAB` = PK `id:u64` + payload `[schema_id, name, dir, pk_packed,
  created_lsn, flags]` (`sys_tables.rs:184-186`); `flags` is written at
  `client.rs:528`.
- **Fan-out conventions** (`runtime/master.rs:48-56`): `CheckPayload::Broadcast`
  replicates a batch to every worker (each filters locally) and
  `CheckPayload::ScatterSource` pre-partitions by PK. These are **check-only**
  (FK existence / unique-index preflight) — they neither ingest nor write the
  SAL. A replicated **data-write** broadcast is a new path (§4.3).
- **Existing broadcast primitives** (reusable as a *model*, not as-is): DDL is
  broadcast via `write_broadcast` (`master.rs:460-490`) →
  `write_broadcast_direct` (`sal.rs:909`), which copies one wire group into every
  worker's SAL slot under `FLAG_DDL_SYNC`. `op_relay_broadcast`
  (`ops/exchange.rs:824-845`) clones a batch to every worker at the relay layer
  (used by pure-range joins). `op_partition_filter` (`exchange.rs:91`) keeps only
  the rows a given worker owns. (Note: `fan_out_push`/`fan_out_ingest` named in
  `dev-guide.md` are documentation placeholders — no such symbols exist.)
- **DML scatter** (`write_commit_group`, `master.rs:2597-2617`): routes by
  `schema.pk_indices()` through `with_worker_indices` → `fill_worker_indices`
  (`exchange.rs:306-338`), which hashes each PK with `partition_for_pk_bytes` and
  pushes the row index into its owning worker's slot, then
  `sal.scatter_wire_group(... FLAG_PUSH ...)`. Each worker's SAL slot holds only
  its partition subset. There is no path that writes the same rows to every
  worker's ingest + SAL.
- **Uniqueness** (`enforce_unique_pk`, `dag.rs:~1820-1890`): per-partition via
  `ptable.retract_pk_bytes(pkb)`; on a single-partition full copy this reduces to
  per-table local enforcement (all rows route to `Table[0]`). PK uniqueness on
  the write path is preflighted distributively today via `CheckPayload::Scatter
  Source` (route each PK to its owning worker, check there).
- **Reads** (`runtime/master.rs`): `fan_out_scan_async` (`:1187-1237`)
  broadcasts the SCAN to **every** worker and concatenates their streams to the
  client — correct for partitioned data, **N-fold-multiplying** for a replicated
  table. `fan_out_seek_async` (`:947-968`) hashes the PK to one partition and
  **unicasts** to one worker via `single_worker_async` (`:3167-3214`) — this
  *accidentally works* for a replicated table (the target worker holds the full
  copy). Aggregates are circuits (`circuit.rs reduce`, `:452-491`): each worker
  reduces its local input and the master sums partials via `execute_gather_async`
  (`:1348-1429`) — for a replicated base table each worker's reduce sees the
  **whole** table, so the gather sums N full aggregates (§4.4). System tables are
  read master-locally via `scan_family` (`executor.rs:1125-1138`,
  `catalog/store.rs:785-801`) — a single-source read path, but master-only and
  thus not directly reusable for worker-resident replicated tables.
- **Co-partition / exchange-skip** (`dag.rs`): the join-shard arm
  (`:1349-1366`) decides **per source** via `plan_source_co_partitioned`
  (`:926-937`), which consults `compute_co_partitioned` (`compiler.rs:597-622`)
  — a source skips its exchange iff it carries no type-promotion (`tc == 0`) and
  its reindex key equals its PK exactly (`shard_cols_match_pk`,
  `schema.rs:455-458`). Skipping a source elides the network scatter (and its
  post-exchange consolidate); the always-local `map_reindex` relabel still runs.
- **Recovery** (`runtime/bootstrap.rs:160-189`): each worker replays its own SAL
  `FLAG_PUSH` groups; a worker's SAL holds only the partitions it owns. A worker
  push routes through `ingest_returning_effective` (`worker.rs:1430`) →
  `enforce_unique_pk` → `ingest_owned_batch`.

## 4. Design

### 4.1 Storage and DDL

- A `Distribution` enum on `TableEntry` — `Hash { prefix_len }` (the
  distribution-key plan) or `Replicated` — so "replicated" and "hash prefix" are
  mutually exclusive by construction. The two plans share the `TABLE_TAB.flags`
  column; allocate bits explicitly: bit 0 = `unique_pk`; one bit =
  `TABLETAB_FLAG_REPLICATED`; the hash-prefix `k` field (bits `[1..4)` in the
  companion plan) is **meaningless when the replicated bit is set**. Decode order:
  replicated bit set ⇒ `Replicated`; else `Hash { prefix_len = k }`.
- Build a single-partition (`num_partitions = 1`) `Table` holding the **full
  copy** on every worker. `build_partitioned_storage` must, for a replicated
  table, pass `num_partitions = 1` **and override the active range to `[0, 1)`**
  — *not* the worker's assigned sub-range of 256. Omitting the override is a bug:
  the worker would build zero or a wrong-numbered partition and the full copy
  would never materialize.
- **Master placement.** The master's active range is empty (`[0, 0)`), so for a
  replicated table `build_partitioned_storage` on the master must build **no**
  partition Table (and the single-partition `Table[0]` bypass must never be hit
  on the master). Guard construction on an empty active range, or skip building
  replicated storage on the master entirely. Reads and joins never touch a master
  copy.

### 4.2 The distribution model: partitioned vs replicated (and propagation)

Replication is not only a storage+read property of a base table; it is a
**distribution property of every relation in the circuit**, and each operator
must know its inputs' distribution to stay correct:

- **partitioned** — rows are disjoint across workers; reads gather (union) across
  workers; an aggregate runs per-worker on its slice and the master sums partials.
- **replicated** — every worker holds the full relation; reads must consume a
  single copy; an aggregate run per-worker would see the whole relation N times.

Propagation rules (these govern §4.4–4.5 and the read-gather decision):

| Operator | input distributions | result |
|---|---|---|
| join | partitioned ⋈ replicated | **partitioned** (output lives on the partitioned side's workers; §4.5) |
| join | partitioned ⋈ partitioned | partitioned (after exchange — unchanged) |
| join | replicated ⋈ replicated | **replicated** (every worker computes the full join; reads single-source it) |
| reduce / distinct / set-op | partitioned | partitioned (exchange by group/key — unchanged) |
| reduce / distinct / set-op | replicated | **must single-source** (run on one worker) **or partition-filter** the input to 1/W by group key before reducing (§4.4) |
| scan / project | replicated | **replicated** (read single-source) |

The headline case — replicated dimension joined to a partitioned fact yielding a
**partitioned** result — needs no special read handling on the *result* (it is
ordinary partitioned data). The cases that need new care are those that *consume
a replicated relation directly*: scan, aggregate, distinct, set-op. The engine
must therefore carry a per-relation "is this register replicated?" bit so the
read-gather and the single-source operators branch correctly. The minimum viable
scope (§2) restricts the user-facing surface to base-table dimensions and handles
the direct-consume cases by single-sourcing; full propagation through arbitrary
nested views is a superset and can follow.

### 4.3 Writes — broadcast to every worker (new path)

- Replicated DML broadcasts the **full** row set to every worker's ingest **and
  SAL**, so each worker's durable log holds the whole table. Model the new path
  on `write_broadcast` / `write_broadcast_direct` (`master.rs:460`,
  `sal.rs:909`) — they already copy one wire group into every worker's SAL slot —
  but under `FLAG_PUSH` (so workers route it through the data-ingest path, not the
  DDL path) and **with `fdatasync` before ACK** (it upserts table data; the SAL
  durability contract requires the sync, which `broadcast_ddl` also does but
  `op_relay_broadcast` at the relay layer does not). `CheckPayload::Broadcast`
  cannot be reused — it is check-only (no ingest, no SAL).
- Concretely, the new `write_commit_group`-sibling routes **every** row to
  **every** worker's slot (instead of `fill_worker_indices` scattering by PK),
  and otherwise reuses the existing wire encode + SAL group machinery.
- **Uniqueness** is enforced locally on each worker's full copy: every worker
  sees the identical broadcast batch against an identical full copy, so all
  workers compute the identical effective delta (retract + insert) and the copies
  stay byte-identical. No distributed preflight across partitions is needed for
  the *enforcement*.
- **Accept/reject ACK** (non-UPSERT conflict modes) still needs a preflight so
  the master can reject a duplicate before ACKing. Preflight **one** worker (any
  worker holds the full copy): a single-worker `has_pk` check, not a PK-scatter.
  (The existing `ScatterSource` preflight *happens* to be correct here — it routes
  the check to one hash-chosen worker, which holds the full copy — but a direct
  single-worker check is clearer and avoids depending on that coincidence.)
- Because each worker ingests the row once into its own copy, per-worker weights
  are correct; reads must not sum across workers (§4.4).

### 4.4 Reads — single source (new path)

- **Scan** (`SELECT *`): must read **one** worker, not all. The master holds no
  copy (§3), so route a replicated scan through the existing unicast mechanism
  (`single_worker_async`, already used by SEEK) to a designated worker (e.g.
  worker 0). `fan_out_scan_async` currently broadcasts-and-concatenates and would
  return N copies.
- **Point lookup** (SEEK): already correct — `fan_out_seek_async` unicasts to one
  worker, which holds the full copy. No change.
- **Aggregate / DISTINCT / set-op directly over a replicated relation — the real
  subtlety, and the part the previous framing missed.** This is *not* only a
  result-gather problem. An aggregate is a `reduce` circuit: each worker reduces
  its local input and the master sums partials (`execute_gather_async`). If the
  input is replicated, each worker's local input is the **whole** table, so each
  worker emits the full aggregate and the gather sums N of them — silently
  N-fold-wrong, with no error. Two correct options:
  1. **Single-worker reduce.** Run the reduce on one designated worker only
     (the other workers contribute nothing); gather from that one worker. Simple,
     correct, but serializes the aggregate onto one worker.
  2. **Partition-filter then reduce.** Each worker keeps only the 1/W of the
     replicated input whose group key hashes to a partition it "owns"
     (`op_partition_filter` already exists), reduces locally, and the master
     gathers. This de-multiplies *and* keeps the reduce distributed. It is the
     same trick the pure-range join uses on its trace.

  Note that an aggregate *over a join output* (replicated dim ⋈ partitioned fact)
  is **fine without special handling** — the join output is partitioned (§4.2),
  so the ordinary per-worker-reduce-then-gather is correct. Only a reduce/distinct
  whose *direct* source is replicated needs (1) or (2).
- The engine must therefore consult the input relation's distribution (§4.2) at
  read/gather planning time and at the reduce/distinct/set-op exchange decision —
  it is **not** enough to special-case `SELECT *`.

### 4.5 Joins (no new exchange machinery)

A join `fact ⋈ dim` with `dim` replicated runs **locally on every worker with no
exchange on either side**, and this falls out of machinery that already exists:

- **The dim side skips its exchange.** Its delta was already broadcast at the
  write layer (§4.3), so every worker has ingested the full `Δdim` and holds the
  full trace `I(dim)`. Skipping the exchange leaves the full copy in place. **No
  join-relay broadcast (`op_relay_broadcast`) is needed** — the broadcast already
  happened at ingest.
- **The fact side also skips its exchange** — *not* because it is co-partitioned,
  but because **its partner is replicated**. The fact stays in its own PK
  partitioning; `map_reindex` still relabels each fact row's PK region to the
  join key locally (skip elides only the network scatter), so the local `cogroup`
  matches `fact`-on-W (re-keyed to the join key) against the full `dim`-on-W.
- **Correctness** (symmetric 2-term DBSP join, single-source-per-epoch):
  - Fact-push epoch: term `Δfact ⋈ z⁻¹(I(dim))` runs on each worker as
    `Δfact_W ⋈ full I(dim)`; union over W = `Δfact ⋈ I(dim)` (each fact row on
    exactly one worker — no double count).
  - Dim-push epoch: term `Δdim ⋈ z⁻¹(I(fact))` runs on each worker as
    `full Δdim ⋈ I(fact)_W`; union over W = `Δdim ⋈ I(fact)` (the *fact trace* is
    partitioned, so each fact trace row contributes on exactly one worker — no
    double count, even though `Δdim` is present everywhere).
  - The cross term `Δfact ⋈ Δdim` is zero under single-source-per-epoch.
  - The join output PK is the left input's PK (the join key), but the output rows
    are physically produced on the partitioned (fact) side's workers, so the
    result is **partitioned** and the ordinary union-gather is correct (§4.2).
- **The one new piece of planner logic**: the per-source exchange-skip condition
  widens from "this source is co-partitioned" to "this source is co-partitioned
  **or its join partner is replicated**," and a replicated source always skips.
  No new node type, no new exchange/relay path. `compute_co_partitioned` /
  `plan_source_co_partitioned` need each source's `Distribution` and the
  partner's.
- **Edge case** `replicated ⋈ replicated`: both deltas broadcast, both traces
  full ⇒ every worker computes the full join ⇒ the output is **replicated**
  (§4.2). Reads of such a view must single-source it. Either propagate the
  replicated bit to the view's output register or reject declaring/aggregating
  such a view in the MVP.

### 4.6 FK checks and uniqueness against a replicated parent

- **FK existence checks need no change for correctness.** The existing
  `CheckPayload::Broadcast` path is already correct against a replicated parent:
  every worker checks its (full) copy and reports existence; the master's
  "exists on any worker" aggregation is right, and because the copies are
  identical every worker agrees. A *local* check on the child's worker is an
  optimization (it avoids the broadcast round-trip), not a requirement — defer it.
  (`is_parent_pk` scatter at `master.rs:1786` also stays correct: it routes to one
  worker, which holds the full copy.)
- **Local uniqueness** on the replicated table itself is enforced per worker
  (§4.3); the single-worker preflight covers the ACK.

### 4.7 Recovery

- Each worker replays its own SAL `FLAG_PUSH` groups; because §4.3 broadcasts the
  full row set into **every** worker's SAL slot, each worker reconstructs the full
  copy with **no change to `replay_ingest`/`bootstrap.rs`**. The replay routes
  through `ingest_returning_effective` → `enforce_unique_pk` → `ingest_owned_batch`;
  on a single-partition replicated table the ingest routing collapses to
  `Table[0]`, so the full set lands correctly. The `TABLE_TAB` row (with the
  replicated bit) replays before the data by LSN order, so the worker knows to
  build single-partition full-copy storage before ingesting.
- Each worker replays the full table (W× the replay work of a partitioned table);
  acceptable for small dimensions, and inherent to replication.

## 5. Correctness hazards (bugs to avoid)

1. **Reduce/distinct over a replicated relation silently N-fold-multiplies**
   (§4.4). This is the highest-risk gap: it produces wrong results with no error.
   Must be handled (single-worker reduce or partition-filter) or rejected at
   planning before replicated tables ship.
2. **`build_partitioned_storage` must override the active range to `[0,1)`** for a
   replicated table (§4.1). Reusing the worker's 256-sub-range would build the
   wrong (or zero) partition Tables and lose the full copy.
3. **Master `Table[0]` bypass must not be reached** for a replicated table
   (master active range is empty; §4.1). Build no master copy and never read it.
4. **The new write-broadcast path must `fdatasync` before ACK** (§4.3). Modeling
   on `op_relay_broadcast` (relay layer, no sync) instead of
   `write_broadcast_direct` would violate the SAL durability contract for upserts.
5. **`replicated ⋈ replicated` yields a replicated output** (§4.5); a read/gather
   that treats it as partitioned multiplies it. Track or reject.
6. **Exchange-skip must fire for the partitioned side because the partner is
   replicated**, not only when co-partitioned (§4.5). Forgetting this leaves the
   fact being exchanged against a replicated dim — still *correct* (a no-op
   scatter-then-local-join wastes work) only if the dim is also left intact, but
   it defeats the entire feature.

## 6. Performance

- **Write amplification is the dominant cost.** A replicated write costs W×
  ingest, W× SAL space (the same group copied into every worker's slot), and W×
  the enforce-unique work, against one `fdatasync`. Storage is W× the table size.
  SAL pressure is W× per write on the shared mmap — the existing per-round SAL
  reclaim still applies but peak occupancy is W× higher for replicated writes.
  This is acceptable for small, rarely-updated dimensions and pathological for a
  large or write-hot replicated table; it is the user's to bound (no size cap is
  a non-goal).
- **Reads have no parallelism.** A replicated scan is served by one worker; fine
  for small dimensions, a bottleneck if the replicated table is large or
  read-hot. A single designated worker also concentrates read load; round-robin
  across workers is a later refinement.
- **The join win is the payoff and it is large.** A replicated-dim join drops
  straight to the local pre-phase: no per-row IPC scatter, no wire
  serialize/deserialize, no post-exchange `consolidate_exchanged` on **either**
  side — only the always-local `map_reindex` relabel remains. Crucially the
  **fact** side (the big input) is not shuffled at all, regardless of its own
  distribution, so a single fact can join several replicated dimensions locally —
  the star-schema case hash co-partitioning cannot serve (a fact co-partitions
  with at most one dimension).
- **Recovery** replays the full table on every worker (W× work); bounded by the
  dimension size.

## 7. Open problems to resolve before implementation

1. **Single-source aggregate/distinct/set-op over a replicated relation** (§4.4):
   choose single-worker-reduce vs partition-filter; this is the correctness
   gap, not just the scan-gather one.
2. **Distribution tracking** (§4.2): thread a per-relation distribution
   (partitioned/replicated) so the read-gather and the reduce/join exchange
   decisions branch correctly; decide how far it propagates through nested views
   in the MVP.
3. **Master placement of replicated storage** (§4.1): confirm the empty-range
   guard and that reads/joins never touch a master copy. (Alternative, explicitly
   *not* chosen: make the master hold replicated copies for trivially
   single-source reads via the system-table read path — rejected because it would
   teach the master to ingest and replay user data.)
4. **Write-broadcast path** (§4.3): build the `FLAG_PUSH` broadcast variant with
   `fdatasync`, and the single-worker uniqueness preflight for the ACK.

## 8. Testing

- A replicated dimension joined to a partitioned fact runs locally on every
  worker; assert **no relay scatter on either side**, including when the fact is
  *not* distributed by the join key (the case hash co-partitioning cannot serve).
- Writes to a replicated table appear on all workers; `SELECT *` returns **one**
  copy; `COUNT(*)` / `SUM` over the replicated table returns the single-copy
  value (the §4.4 regression — without single-sourcing it returns N× the count).
- Multi-dimension star join: one fact joined to two replicated dims runs fully
  local.
- `replicated ⋈ replicated` view is read single-source (no N× on read).
- Reboot under `GNITZ_WORKERS=4` restores the full copy on every worker; a write
  then a crash-replay reconstructs the full set (and the SAL slot for every
  worker held the full batch).
- FK existence checks against a replicated parent stay correct (broadcast path,
  no change).
- Local uniqueness rejects a duplicate PK in a replicated table (single-worker
  preflight rejects before ACK), and an UPSERT retracts the old payload exactly
  once on **every** worker (copies stay identical).
- Point lookup (SEEK) on a replicated table returns one row (already works).

## 9. Invariants

- Every worker holds an identical full copy; reads of the replicated relation
  itself go to a single source; reads of a partitioned result that merely *uses*
  a replicated dimension gather normally.
- A relation is either partitioned or replicated; the property propagates through
  the circuit (§4.2) and every consuming operator (join, reduce, distinct,
  set-op, gather) respects it.
- Replicated and hash-prefix distribution are mutually exclusive (`Distribution`
  enum), so no table is both.
- A replicated write is durable on every worker's SAL before its ACK
  (`fdatasync`), so per-worker full replay reconstructs the full copy.
