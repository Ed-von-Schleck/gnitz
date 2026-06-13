# Band-join equality-prefix co-partition scatter

## Goal

A **band join** — `n_eq ≥ 1` equality conjuncts plus exactly one range conjunct
(`a.k = b.k AND a.lo <= b.t`) — currently distributes its per-epoch input delta
by **broadcasting** the whole delta to every worker (the deliberately-minimal
distribution the range join shipped with). This plan replaces the broadcast for
band joins with an **equality-prefix scatter**: route each delta row to the one
worker owning its equality-prefix partition, exactly as an equi-join routes by
its full key. Both sides scatter by the same eq prefix, so equal eq-values
co-partition; the range walk then runs partition-local, and the broadcast's
`O(num_workers × |Δ|)` per-epoch (and per-backfill) cost collapses to
`O(|Δ|)` — each worker receives, deserializes, **reindexes**, and **probes** only
its `|Δ|/W` eq-prefix partition instead of the full broadcast delta, and the
redundant `PartitionFilter` pass the broadcast path runs (to trim the full
reindexed delta before integrating) disappears. Trace integration itself is *not*
the win: it was already `O(|Δ|/W)` per worker under broadcast, since
`PartitionFilter` trimmed the delta before `integrate_trace`. The savings are the
relay volume (`W× → 1×`), the reindex (`W·|Δ| → |Δ|` cluster-wide), and the
mostly-empty probe lookups (`W·|Δ| → |Δ|`) the broadcast path repeats on every
worker.

A **pure** range join (`n_eq == 0`) has no equality prefix to scatter by and
keeps broadcasting — unchanged.

```sql
-- band join (n_eq = 1): scattered by the eq prefix [k] after this change
CREATE VIEW v AS SELECT a.id, b.id, b.t
  FROM a JOIN b ON a.k = b.k AND a.lo <= b.t;
-- pure range join (n_eq = 0): still broadcast
CREATE VIEW w AS SELECT a.id, b.id FROM a JOIN b ON a.x < b.y;
```

## Design summary (what changes, in one paragraph)

The band join's reindex shape is unchanged: both sides reindex onto
`[eq slots…, range slot]` at the pair-wise common promoted types, so each side's
`IntegrateTrace` table is an ordered arrangement by the range key, and equal
eq-values pack byte-identically. The trace tables are **per-worker operator
state** that integrate *exactly the rows their worker received* (no re-hash by
their own `_join_pk` — proven by the fact that the broadcast path needs a
`PartitionFilter` at all). So whatever partitioning the **input relay** imposes
is the trace's partitioning. For a band join we change that relay from broadcast
to a scatter on the **first `n_eq` reindex slots** (the eq prefix), routed
through the same `RouteMode::JoinPromote` packer an equi-join uses — so a row
lands on the worker owning its eq-prefix partition, and both sides co-partition
by the eq group. The trace is then eq-prefix-partitioned for free, the
`PartitionFilter` becomes unnecessary and is dropped from the circuit, and the
probe is partition-local (a delta row with eq prefix `e` lands on `hash(e)`,
which holds every trace row with eq prefix `e`; `range_cut_points` already bounds
the walk to that eq group). The output re-key onto the source-PK pair and the
output `ExchangeShard` are **unchanged** — the join output is still produced on
the eq-prefix owner, not the pair-PK owner, so it must still be exchanged onto
the view PK. No new operator, no new VM instruction, no wire-format change: a
planner branch (drop `PartitionFilter` for `n_eq ≥ 1`) and a master-relay branch
(eq-prefix scatter for `n_eq ≥ 1`, broadcast for `n_eq == 0`).

## Background — current state (verified against code)

**Range-join circuit** (`gnitz-sql/src/planner.rs:1334`, `build_range_join_view`):
per side `input_delta_tagged` → optional NULL `Filter` over all key cols
(:1409-1414) → `map_reindex([eq cols…, range col], target_tcs, …)` (:1416-1417)
→ **`partition_filter`** (:1421/1423) → `integrate_trace` (:1422/1424); two range
terms `join_with_trace_range_node(reindex_a, trace_b, n_eq, rel_ab)` /
`(reindex_b, trace_a, n_eq, rel_ba)` (:1425-1426) consuming the **unfiltered**
reindex; per-term normalize `Map` (:1430-1435); `union` (:1436);
**`map_reindex` onto the source-PK pair** `[a.pk…, b.pk…]` (:1453-1457); user
projection `Map` dropping the `k = n_eq + 1` `_join_pk` slots (:1486-1491);
**`shard` on exactly the pair-PK columns** (:1495-1496); `sink`. `n_eq` is
`left_join_cols.len()` (:1352). INNER only; LEFT/self/string-range-pair rejected
upstream.

**Trace tables are per-worker local operator state.** The `IntegrateTrace`
scratch tables (`_int_{view}_{nid}`) are named under the view directory with the
worker rank embedded, flush in-memory (`in_memory_l0`), and are rebuilt from
sources — they **integrate the rows the worker received, with no cross-worker
re-hash by their own PK** (the `WORKER_RANK` doc, `compiler.rs:12-30`, which also
notes `num_workers` defaults to 1 in single-process — making `PartitionFilter` a
keep-all identity there). The proof this is load-bearing: the broadcast path
*needs* `op_partition_filter`
(`ops/exchange.rs:88`) to trim the replicated delta down to the worker-owned
slice before integrating — which would be redundant if the trace re-hashed by its
`_join_pk`. So the trace's partitioning is **exactly** the input relay's
partitioning. This is what makes the eq-prefix scatter sufficient: scatter by the
eq prefix ⇒ trace partitioned by the eq prefix ⇒ no `PartitionFilter`.

**The input relay today broadcasts** (`runtime/master.rs:734`, `prepare_relay`):

- `source_id > 0` → join-shard input relay; `get_join_shard_cols(view_id,
  source_id)` (`dag.rs:664`) returns the reindex Map's `(col, carried_tc)` pairs
  via `reindex_cols_through_filters` (`compiler.rs:503`) — for a range join that
  is **`[(eq col, tc)…, (range col, tc)]`**, length `n_eq + 1` verbatim (never
  deduplicated; `wide-pk-incremental-views.md` §3). `is_join = !pairs.is_empty()`
  (:746-754).
- The range-join arm (:766-768): `if is_join && view_is_range_join(view_id)` →
  `op_relay_broadcast(&sources, &schema, num_workers)` (`ops/exchange.rs:821`):
  concatenate the disjoint per-worker source slices into the full delta once,
  then clone it to all `num_workers` destinations — `num_workers` physical copies.
- The else-arm (:769-790) is the **equality scatter**: `RouteMode::JoinPromote`
  over `col_indices = shard_cols` + `target_tcs`, via
  `op_relay_scatter_consolidated_mode` (consolidated input) or
  `op_repartition_batches_mode` (raw). This is the path the band join will reuse,
  truncated to the eq prefix.

**The JoinPromote scatter already co-partitions with the trace `_join_pk`**
(`ops/exchange.rs:440-477`): for a single non-PK eq column, `route_partition_key`
(:222-241) routes via `loc.route_key` — the OPK-encode+widen that "agrees with
the `_join_pk` `PkPromoter::promote_into` writes" (:229-231); for a compound or
cross-width-promoted eq prefix, `compound_join_packer` (:293, fires when
`col_indices.len() > 1 || key_is_promoted(target_tcs)`) packs via `ReindexPacker`
into the same `_join_pk` bytes. `partition_for_pk_bytes` then hashes the packed
region. **Routing by the eq prefix is byte-identical to an equi-join whose key is
the eq columns** — already-proven machinery: a non-PK cross-width equi-join hits
the exact asymmetry a promoted eq prefix does — the promoted side packs through
the `ReindexPacker` (`partition_for_pk_bytes` of the packed `T`-wide bytes) while
the native side routes through the single-column `route_key` (`partition_for_key`
of the OPK-widened value), and the hash invariant (`foundations.md`: the OPK
region read big-endian into a left-zero-padded `u128`) makes the two agree for
equal numeric values. `test_inner_join_cross_width_int_bigint`
(`crates/gnitz-py/tests/test_joins.py:521`, `lt.k I32 = rt.k I64` on a **non-PK**
key, duplicate key co-locating across workers) is the canonical witness.
`test_copartitioned_join` (`crates/gnitz-py/tests/test_workers.py:564`) is NOT —
it joins on the PK and takes the co-partition exchange-**skip**, never routing
through this scatter.

**The dag driver branch** (`dag.rs:1236-1267`, `evaluate_dag_multi_worker`):
keyed on `view_is_range_join(view_id)`, placed first in the chain (before the
`has_exchange` arms). It does `do_exchange(input, src_id)` (input relay) → `pre`
→ `do_exchange(pre, 0)` (output relay) → `consolidate_exchanged` → `post`. The
input-relay **mode** is decided entirely master-side in `prepare_relay`, so this
branch is mode-agnostic and serves both broadcast and eq-prefix scatter
unchanged. Single-worker (`execute_epoch_for_dag`, `dag.rs:1476`) runs
`pre → consolidate → post` for any `has_post` view; the scatter degenerates to
identity (`num_workers == 1`) and there is no `PartitionFilter` in the band
circuit, so it works with no special case.

**Discriminator** (`dag.rs:793` `view_is_range_join` → `compiler.rs:549`
`circuit_is_range_join`): any `Join(DeltaTraceRange { .. })` node. Memoized in
`range_join_cache`, evicted with the sibling view caches. A band join *is* a
range join (its terms are `DeltaTraceRange { n_eq ≥ 1, .. }`), so it stays in
this branch — only the relay mode differentiates.

## Design

### 1. The trace partitioning follows the input relay

Term AB = `ΔA ⋈θ I(B)`, term BA = `ΔB ⋈θ I(A)` (the bilinear 2-term DBSP form,
unchanged). A match for a logical pair `(a, b)` requires `eq(a) == eq(b)` (the
equality conjuncts) **and** the range relation on the range slot. Equal
eq-values pack byte-identically at the common eq types, so:

- Scatter `ΔA` by `[eq cols…]` (first `n_eq` reindex slots) and `ΔB` by its
  `[eq cols…]`. A row with eq prefix `e` lands on `worker_for_partition(
  partition_for_pk_bytes(pack(e)), num_workers)` on **both** sides ⇒ both sides
  co-partition by the eq group.
- `integrate_trace` accumulates each worker's received (eq-prefix-partitioned)
  rows into its local trace ⇒ `trace_A`, `trace_B` are eq-prefix-partitioned.
- Term AB on worker `w`: `ΔA`-rows with eq prefix `e ∈ w` probe `trace_B`; every
  `trace_B` row with eq prefix `e` is on `w`; the cut-points
  (`storage/range_key.rs::range_cut_points`) bound the half-open `[start, end)`
  walk to the eq group `e`. So the match set is complete locally and each match
  is emitted **exactly once** cluster-wide (on `hash(e)`). Symmetric for term BA.

This is the equi-join co-partition argument applied to the eq prefix, with the
range slot riding inside the (locally-complete) eq group.

### 2. Drop `PartitionFilter` for band joins

`op_partition_filter` exists only to trim the **broadcast** (replicated) delta to
the worker-owned slice before integration. Under the eq-prefix scatter there is
nothing to trim — the relay delivered exactly the worker's eq-prefix partition.
Worse, **keeping it would be a correctness bug**: it hashes the *full* `_join_pk`
(`[eq…, range]`, `ops/exchange.rs:108`) while the scatter routed by the eq
*prefix*; for any row whose full-key partition differs from its eq-prefix
partition, the filter would drop a row the scatter correctly delivered. So the
band circuit must **omit** `PartitionFilter`: `integrate_trace` consumes the
reindex node directly.

The pure range join (`n_eq == 0`) keeps `PartitionFilter` (it broadcasts).

### 3. The output stays re-keyed and exchanged

The eq-prefix scatter changes only where the join *output* is produced, not where
it must *live*. The join output for pair `(a, b)` is produced on `hash(eq(a))`,
but the view is PK-partitioned by the pair-PK `(a.pk…, b.pk…)` for scan/seek
(`hash(pair-PK)`). These differ, so the existing re-key onto the source-PK pair
(`planner.rs:1457`) + `ExchangeShard` on the pair-PK (:1496) are **unchanged and
still required**. Cross-term cancellation is incidentally easier (both terms for
a pair now emit on the same worker), but the output exchange's
`consolidate_exchanged` is what makes a `+1`/`-1` cancel, exactly as before — no
change to that path.

### 4. Distribution flow (band join, multi-worker)

```
worker: local ΔA slice ──FLAG_EXCHANGE──► master: prepare_relay(src_id > 0)
                                            └─ band join (n_eq ≥ 1)?
                                               → SCATTER by shard_cols[..n_eq]   (one dest/row)
                                               (n_eq == 0 → BROADCAST, unchanged)
worker: pre plan on its eq-prefix slice of ΔA:
    Filter(NOT NULL keys) → map_reindex[eq…,range]
        ├─ integrate_trace_A           (eq-prefix-partitioned; NO PartitionFilter)
        └─ JoinDTRange(ΔA_w, trace_B_w)  (partition-local probe, rel_ab)
    → Map(normalize) → union → map_reindex[(a.pk…, b.pk…)] → Map(user proj)
                       ──FLAG_EXCHANGE──► master: prepare_relay(src_id == 0)
                                            └─ ExchangeShard(pair-PK) → GroupKey scatter
worker: post plan: consolidate_exchanged → IntegrateSink
```

Two master round-trips per epoch (as today). Both `do_exchange` calls run
unconditionally, including empty deltas (barrier participation). The dag driver
branch is byte-identical to the broadcast case — only `prepare_relay` differs.

## The change, by file

### 1. `crates/gnitz-sql/src/planner.rs` — conditional `PartitionFilter`

In `build_range_join_view` (:1419-1426), gate the `partition_filter` on
`n_eq == 0`:

```rust
// Band join (n_eq ≥ 1): the input relay scatters by the eq prefix, so each
// side's trace is already eq-prefix-partitioned — integrate the scattered
// reindex directly. Pure range join (n_eq == 0): the input is broadcast, so
// each worker must drop the rows it does not own (PartitionFilter) before
// integrating, or traces replicate and matches duplicate.
let (int_a, int_b) = if n_eq == 0 {
    (cb.partition_filter(reindex_a), cb.partition_filter(reindex_b))
} else {
    (reindex_a, reindex_b)
};
let trace_a = cb.integrate_trace(int_a);
let trace_b = cb.integrate_trace(int_b);
let join_ab = cb.join_with_trace_range_node(reindex_a, trace_b, n_eq as u8, rel_ab);
let join_ba = cb.join_with_trace_range_node(reindex_b, trace_a, n_eq as u8, rel_ba);
```

Everything downstream (normalize, re-key, user projection, shard, sink) is
unchanged. The probe still consumes the unfiltered `reindex_a`/`reindex_b`, which
for a band join *is* the worker's scattered eq-prefix slice.

### 2. `crates/gnitz-engine/src/runtime/master.rs` — eq-prefix scatter in `prepare_relay`

A band join changes exactly one thing about the relay: the routing-key **length**.
It scatters by the eq **prefix**, dropping the trailing range slot from
`shard_cols`/`target_tcs`; the consolidated/raw dispatch and the `JoinPromote`
mode are the existing equi path. So rather than a second copy of the scatter
block beside the broadcast, compute the route length and reuse the one scatter
(`:766-790` becomes):

```rust
// A range-join INPUT relay (is_join over a DeltaTraceRange view): shard_cols is
// the reindex sequence [eq cols…, range col] (len n_eq + 1). A band join (n_eq ≥
// 1) scatters by the eq PREFIX — drop the trailing range slot from the routing
// key, so equal eq-values co-partition both sides and the range probe is
// partition-local. A pure range join (n_eq == 0) has no eq prefix and broadcasts.
let is_range_join = is_join && cat.dag.view_is_range_join(view_id);
let n_eq = if is_range_join {
    debug_assert!(!shard_cols.is_empty(), "range-join shard_cols = [eq…, range], never empty");
    shard_cols.len() - 1
} else { 0 };

let dest_batches = if is_range_join && n_eq == 0 {
    // Pure range join: broadcast the full delta to every worker.
    let sources: Vec<Option<&Batch>> = payloads.iter().map(|o| o.as_ref()).collect();
    op_relay_broadcast(&sources, &schema, self.num_workers)
} else {
    // Scatter. Band join: route by the eq prefix [..n_eq]. Equi-join: full shard
    // cols. Both JoinPromote. GROUP BY / set-op: full shard cols, GroupKey.
    let route_len = if is_range_join { n_eq } else { shard_cols.len() };
    let col_indices: Vec<u32> = shard_cols[..route_len].iter().map(|&c| c as u32).collect();
    // target_tcs is EMPTY for a GroupKey scatter (no promotion) and has length
    // shard_cols.len() for any join; slice it only when we truncated shard_cols.
    // (`&target_tcs[..]`, not `&target_tcs`, so both arms are `&[u8]`.)
    let route_tcs: &[u8] = if is_range_join { &target_tcs[..route_len] } else { &target_tcs[..] };
    let mode = if is_join { RouteMode::JoinPromote } else { RouteMode::GroupKey };
    let consolidated_sources: Option<Vec<Option<&ConsolidatedBatch>>> = payloads.iter()
        .map(|opt| match opt {
            None => Some(None),
            Some(b) => ConsolidatedBatch::from_batch_ref(b).map(Some),
        })
        .collect();
    match consolidated_sources {
        Some(sources) => op_relay_scatter_consolidated_mode(
            &sources, &col_indices, route_tcs, &schema, self.num_workers, mode),
        None => {
            let sources: Vec<Option<&Batch>> = payloads.iter().map(|o| o.as_ref()).collect();
            op_repartition_batches_mode(
                &sources, &col_indices, route_tcs, &schema, self.num_workers, mode)
        }
    }
};
```

This is the current equi / GROUP-BY scatter with one line of band logic
(`route_len = n_eq`) plus the pure-range broadcast lifted into its own arm — the
duplicated scatter block the broadcast arm used to sit beside is gone, and the
non-range paths are byte-for-byte unchanged (`route_len == shard_cols.len()`,
`route_tcs == &target_tcs[..]`, the full list — including the empty `target_tcs`
of a `GroupKey` scatter). `view_is_range_join` is already memoized and called once. The
`master.rs:759-765` "must BROADCAST" comment block is replaced by the inline
comments above.

**Invariant relied on:** for a range join, `get_join_shard_cols` returns the
**single** reindex Map's `[eq…, range]` sequence (length `n_eq + 1`), with
`target_tcs` the parallel carried-tc list of the same length. "Single" is what
makes `len() - 1 == n_eq` robust: `reindex_cols_through_filters`
(`compiler.rs:503`) concatenates *every distinct* reindex sequence reachable from
a source's `ScanDelta`, so a source feeding two reindex Maps would inflate the
length past `n_eq + 1`. A range join has exactly one reindex per side because
**self-joins are rejected upstream** (`planner.rs:1065`, before
`build_range_join_view`) — one `ScanDelta` per source ⇒ one reindex Map. An
overlapping key (`a.k = b.k AND a.k <= b.t`, side-A reindex `[k, k]`) *preserves*
its duplicate slot rather than deduplicating it (`compiler.rs:508-513`), so the
length is still `n_eq + 1` and `shard_cols[..n_eq]` is still exactly the eq
prefix. So `shard_cols[..n_eq]` / `target_tcs[..n_eq]` are the eq-prefix
`(col, carried_tc)` slots — never the empty-`target_tcs` `GroupKey` case, which is
non-range and keeps the full length. The `debug_assert` documents the `len() - 1`.
(`wide-pk-incremental-views.md` §3; built at `planner.rs:1380-1383`.)

### 3. Documentation — `PartitionFilter` and the range-join input relay are now pure-range-only

No code changes here, but `PartitionFilter` and the range-join input relay are
documented across **four crates** as *the* range-join distribution mechanism.
After §1–§2 that is true only for a **pure** range join (`n_eq == 0`); a band join
scatters by the eq prefix and carries no `PartitionFilter`. Each stale doc below
would tell the next reader that band joins broadcast — fix them so the construct
reads as pure-range-only:

- `dag.rs:1237` / `:1253` — driver-branch comments: "input relay (eq-prefix
  scatter for a band join, broadcast for a pure range join)" / "(1) Relay the
  source delta (eq-prefix scatter or broadcast, decided master-side in
  `prepare_relay`)" instead of "broadcast".
- `dag.rs:1240-1242` — "No co-partition shortcut applies: a range probe needs the
  full delta even when the join key equals the source PK" is the *pure-range*
  reason only. A band join also skips the co-partition shortcut, but by **decision**
  (Scope boundaries below), not because it needs the full delta. State both
  reasons so the comment is not silently wrong for the band case.
- `dag.rs:788` — `view_is_range_join` doc: "…the range-join driver branch and the
  range-join input relay (eq-prefix scatter for a band join, broadcast for a pure
  range join)".
- `master.rs:759-765` — the "must BROADCAST" block is replaced by §2's inline
  comments (already noted in §2).
- `planner.rs:1323-1332` — `build_range_join_view` doc: the active delta is
  "eq-prefix-scattered (band join) or broadcast (pure range join)", not
  unconditionally "broadcast and probed against the other side's owned trace".
- `ops/exchange.rs:80-87` — `op_partition_filter` doc: it is the counterpart of
  the **pure** range-join broadcast input relay; a band join's scattered trace is
  already eq-prefix-partitioned and integrates with no filter.
- `core/circuit.rs:398-401` — `partition_filter` builder doc: "…before they
  integrate into a **pure** range-join trace under the broadcast input relay (a
  band join's eq-prefix scatter omits this node)".
- `wire/circuit.rs:53-54` — `OPCODE_PARTITION_FILTER` doc: "Drop trace rows this
  worker does not own (**pure** range-join broadcast input)".

The driver branch itself (`dag.rs:1236`) is mode-agnostic and **unchanged**: the
relay mode is decided entirely in `prepare_relay` (§2), so this branch serves both
the band-join scatter and the pure-range broadcast with no code change.

## Correctness invariants to preserve

- **One packer routes both sides at the eq prefix.** Both legs scatter through
  `RouteMode::JoinPromote` on their eq-prefix `(col, carried_tc)` slots, the same
  `ReindexPacker`/`route_key` bytes the reindex Map wrote as the leading
  `_join_pk` slots. Equal eq-values ⇒ identical packed bytes ⇒ same partition.
  This is the equi-join co-partition guarantee restricted to the prefix. The two
  legs may reach the same bytes by different routes: a compound prefix
  (`n_eq ≥ 2`) packs both legs through `ReindexPacker`; a single cross-width eq
  column packs only the **promoted** leg (`carried_tc != 0`, `key_is_promoted`
  forces the packer off the narrow native fast-path) while the **already-at-`T`**
  leg (`carried_tc == 0`) routes via the single-column `route_key`, which
  OPK-encodes at its own width — which *is* `T`, since `carried_tc == 0` means the
  source already sits at the common type. Both emit identical `T`-wide OPK bytes,
  and the hash invariant (OPK region → left-zero-padded `u128`) makes
  `partition_for_pk_bytes(packed)` and `partition_for_key(route_key)` agree.
- **Trace partitioning = relay partitioning.** The per-worker trace integrates
  exactly the scattered rows (no self re-hash). Dropping `PartitionFilter` for
  band joins is therefore not just an optimization but *required* — keeping it
  would hash the full `_join_pk` and drop scattered rows whose full-key partition
  ≠ eq-prefix partition.
- **Match completeness within an eq group.** All rows of one eq prefix land on
  one worker, so the `range_cut_points` half-open walk (bounded to the eq group)
  sees the complete group. No match crosses a worker boundary; none is
  double-counted.
- **Symmetric output identity is unchanged.** Re-key onto the source-PK pair
  *after* the normalize projections, then `ExchangeShard` on exactly the pair-PK
  in order (`shard_cols == view_pk`, asserted at `planner.rs:1501`), so the
  output routes by `partition_for_pk_bytes` identically to the view scan/seek.
  The `k` `_join_pk` slots (which differ per term) are dropped before the output
  exchange, as today.
- **Pure range join unchanged.** `n_eq == 0` keeps `PartitionFilter` + broadcast,
  byte-identical to the current circuit and relay.
- **Equi-join byte-stability.** This touches only the `view_is_range_join`
  relay arm and the `n_eq == 0` vs `≥ 1` circuit split; an equi-join's circuit and
  relay are not on either path.
- **Barrier participation.** Both `do_exchange` calls run every epoch on every
  worker, including empty deltas (the scatter of an empty delta yields per-worker
  empty batches, like the equi path).
- **NULL exclusion** precedes reindex on both sides (unchanged); NULL-keyed rows
  never enter the scatter or the trace.

## Migration order

1. **Planner** (§1): conditional `PartitionFilter`. Add the engine circuit-shape
   test (band has no `PartitionFilter`; pure range has two). Single-process band
   joins keep working (scatter degenerate, no filter).
2. **Master relay** (§2): route by the eq prefix (route-length tweak) + the
   pure-range broadcast arm. Multi-worker band joins now scatter; pure range
   joins still broadcast.
3. **Docs** (§3): the cross-crate `PartitionFilter` / range-join-relay comment
   sweep (`dag.rs`, `master.rs`, `planner.rs`, `ops/exchange.rs`, `core/circuit.rs`,
   `wire/circuit.rs`). No code change.
4. `make e2e` (`GNITZ_WORKERS=4`) with the suite below.

Steps 1–2 are independently compilable; after step 1 the circuit is correct
single-process, step 2 makes the multi-worker distribution efficient.

## Testing

**Circuit shape (engine unit, `gnitz-engine/src/compiler.rs` tests).** A band-join
meta-circuit (`n_eq ≥ 1`) has **no `PartitionFilter`** node and two
`Join(DeltaTraceRange { n_eq ≥ 1 })` nodes; a pure-range meta-circuit
(`n_eq == 0`) has **two `PartitionFilter`** nodes. Pins the §1 split. Model on
`test_circuit_is_range_join_discriminator` (`compiler.rs:3782`) and
`test_reindex_cols_through_filters_with_partition_filter_after_map`
(`compiler.rs:3819`, which already hand-builds a `ScanDelta → Map(reindex) →
PartitionFilter → IntegrateTrace` range-join shape), both of which introspect a
`LoadedCircuit`. The shape assertion lives engine-side because the existing
`planner_join.rs` range tests (`test_range_join_accepts_band`, :485) are
admission-only — they assert the view registers, not its node shape.

**Multi-worker E2E (`make e2e`, `GNITZ_WORKERS=4`,
`crates/gnitz-py/tests/test_workers.py`, `TestRangeJoin`).** Four existing band
tests now route through the eq-prefix scatter instead of broadcast +
`PartitionFilter` and must stay green — a mis-routed scatter (missing matches) or
a wrongly-retained `PartitionFilter` (which hashes the full `_join_pk` and drops
rows whose full-key partition ≠ eq-prefix partition) fails each:

- `test_band_join_eq_prefix_plus_range` (:1218) — single eq column.
- `test_band_join_compound_eq_prefix` (:1544) — `n_eq = 2`, now exercises the
  `ReindexPacker` (compound) scatter path rather than the single-column
  `route_key` path.
- `test_band_join_retraction` (:1621) — cross-worker `±1` cancellation plus an
  UPDATE moving the eq key to a different group.
- `test_null_key_either_side_matches_nothing` (:1353) — band join, NULL eq/range
  on both sides; the NULL filter drops those rows before the scatter.

New coverage this change requires:

- **Eq groups spanning all workers.** Strengthen `test_band_join_eq_prefix_plus_range`
  (its eq key is only `k ∈ {0,1,2,3}` today, which need not touch all 4
  partitions): widen the eq-key domain to dozens of distinct values so groups
  demonstrably spread across all workers, with each group's rows scattered across
  workers by base-table PK before the scatter re-homes them. Result equals a
  Python-side cross-filter reference.
- **Cross-width-promoted eq key** (e.g. `a.k INT UNSIGNED = b.k BIGINT AND
  a.lo <= b.t`): the U32 side routes through the `ReindexPacker` (packs at
  `T = I64`) while the I64 side routes through the single-column `route_key`; both
  must co-partition equal values. No existing band test promotes the **eq** key —
  `test_signed_band_over_the_wire` (:1379) promotes the range column.
- **Band-join backfill.** CREATE VIEW with an eq prefix over already-populated
  tables yields the full match set via the eq-prefix scatter. Distinct from the
  existing `test_backfill_over_populated_tables` (:1289), which is a **pure** range
  join (`>=`) and still broadcasts. Confirms the backfill replay rides the same
  relay, so each source is **partitioned, not replicated** — the band-join case no
  longer contributes the `O(num_workers × N)` backfill blow-up that
  `chunked-distributed-backfill.md` addresses for the broadcast paths.

Pure-range regression is already guarded by `test_pure_range_all_ops_both_orders`
(:1188) and the other `n_eq == 0` tests (`test_cross_worker_retraction` :1246,
`test_empty_delta_epochs_no_deadlock` :1405, `test_view_over_range_join_view`
:1431), which keep broadcasting unchanged.

## Scope boundaries (by design, not deferred work)

- **Pure range joins keep broadcasting.** With no equality prefix there is no
  single destination; the order-preserving (range-partitioned) exchange that
  would let a pure range probe touch only boundary-overlapping workers is a much
  larger effort (partition-boundary metadata + rebalancing) and is the
  forward-looking range-join distribution item in `wide-pk-incremental-views.md`
  §1.
- **Band joins always route through the relay** — no co-partition exchange-skip,
  by decision. Even when a band join's eq prefix equals a source's PK (the source
  is already eq-prefix-partitioned, so the scatter is a no-op), the input still
  goes through `do_exchange`. Skipping it would mean widening
  `compute_co_partitioned` to compare the eq prefix against the source PK, and the
  payoff is small: scattering an already-co-partitioned delta is already cheap
  (each row hashes straight to its own worker, one pass). Not worth the added
  co-partition-detection complexity; deliberately not done.
