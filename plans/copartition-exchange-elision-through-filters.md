# Co-partition exchange elision through placement-preserving prefixes

## Goal

A co-partitioned view — one whose output `ExchangeShard` keys on exactly the source
distribution prefix — already has every row on its correct worker, so the network shuffle is
a no-op and is elided (`view_skips_exchange`, `dag.rs:748`; the elided branch reuses the local
pre-phase batch at `dag.rs:1346-1347` instead of running `do_exchange`). The co-partition test
is `SchemaDescriptor::shard_cols_match_dist_key` (`schema.rs:548`): the shard key must equal the
table's distribution prefix `pk_indices()[..k]` (`k = dist_prefix_len()`), and it governs **every**
single-source `ExchangeShard` view (`GROUP BY`, `SELECT DISTINCT`, equijoin pre-index), not just
joins. For a `CLUSTER BY <prefix>` table this already lets an unfiltered `GROUP BY <prefix>` /
reduce run shuffle-free.

But the elision triggers **only when the shard reads directly from a bare scan** (the bare-scan
gate in `compute_view_skips_exchange`, `dag.rs:767-776`). Any placement-preserving node between
the scan and the shard — most commonly a `WHERE` `Filter` — defeats it, so `GROUP BY <prefix>
WHERE …` pays a **full per-tick network shuffle** that `GROUP BY <prefix>` (no `WHERE`) skips.
Widen the gate to walk back through `Filter` prefixes. Pure optimization; no correctness change
(it only elides a provably-redundant exchange), and inert on single-worker deployments (which
never exchange).

## 1. The mechanism and the gap

`compute_view_skips_exchange` (`dag.rs:757-793`) finds the view's `ExchangeShard`, collects its
`shard_cols`, and co-partitions only when the shard's sole input is a node with **no incoming
edge** — i.e. a bare `ScanDelta`/`ScanTrace`:

```rust
let [src_nid] = incoming.as_slice() else { return false };
if loaded.edges.iter().any(|&(_, dst, _)| dst == *src_nid) {   // src has an incoming edge → not a bare scan
    return false;
}
let tid = match loaded.nodes.get(src_nid) {
    Some(gnitz_wire::OpNode::ScanDelta(t)) | Some(gnitz_wire::OpNode::ScanTrace(t)) => *t as i64,
    _ => return false,
};
self.tables.get(&tid)
    .is_some_and(|entry| entry.schema.shard_cols_match_dist_key(&shard_cols))
```

For `GROUP BY <prefix> WHERE x > 5` the planner emits `cb.filter(input, …)` (`circuit.rs:263`)
then `cb.reduce_multi(filtered, group_cols, …)` (`circuit.rs:465`); `reduce_multi` is
`shard(input, group_cols)` (`circuit.rs:492`) + reduce (no intervening reindex Map when the group
set equals the distribution prefix), so the circuit is

```
ScanDelta → Filter → ExchangeShard(group_cols) → Reduce → IntegrateSink
```

The shard's input is the `Filter`, which *has* an incoming edge, so the bare-scan gate fails and
the IPC shuffle runs every tick (`dag.rs:1349`) even though the filter never moved a row off its
owner. (The no-`WHERE` form has the `Filter` absent, the shard's input is the bare `ScanDelta`,
and the elision already fires.)

## 2. Correctness invariant

The shard may be elided iff every node on the path from the `ExchangeShard` back to the source
scan is **placement-preserving** (never repartitions: no reindex, no content-rekey, no exchange)
**and schema-preserving** (so `shard_cols` still index the source key columns), AND the shard key
equals the source distribution prefix (`shard_cols_match_dist_key`). Schema preservation is what
makes the `shard_cols`-vs-distribution-prefix index comparison meaningful: through a Filter,
shard column index *i* still refers to source column *i*; a reindex/projection would break that
mapping.

- **`Filter`** (`OpNode::Filter`, predicate blob irrelevant): drops rows, never moves them, never
  changes the schema. **Qualifies** — the common, safe case. WHERE is the only `Filter` on this
  path; a HAVING `Filter` sits *after* the reduce, downstream of the shard.
- **Not placement-preserving (stop the walk):** `Map(Expression { reindex_cols ≠ [] })` and
  `Map(HashRow)` (both rewrite the PK to a new key), `Join`, `Reduce`, `Union`, `ExchangeShard`,
  `PartitionFilter`.
- **Placement-preserving but schema-changing (stop the walk):** `Map(Projection)`,
  `Map(KeyOnly)`, `Map(Expression { reindex_cols == [] })` (compute map), `NullExtend`, `Negate`,
  `Delay`. Each keeps every row on its worker but shifts, drops, or re-types columns, so
  `shard_cols` indices no longer map 1:1 to the source key columns.

The walk treats only `Filter` and the scans specially and stops at **everything else**, so the
second and third bullets collapse to one default-reject arm; the classification above documents
*why* each is rejected. Extending the walk through the schema-changing-but-placement-preserving
nodes (remapping `shard_cols` through each node's column transform) is possible but out of scope:
only worth it if profiling shows co-partitioned views routinely carry projections/null-extends
before the shard.

## 3. The fix

`compiler.rs` already walks `Filter` chains *forward* from a scan in `reindex_cols_through_filters`
(`compiler.rs:503`). Add its *backward* sibling beside it, so it shares the `make_loaded`-based
unit-test harness (`compiler.rs:3521`) and the (`compiler`-private) `incoming` adjacency
(`LoadedCircuit.incoming`, `compiler.rs:70`), rather than re-scanning `loaded.edges` per hop from
`dag.rs`:

```rust
/// The source table behind an `ExchangeShard`, if every node from the shard back
/// to its scan is a single-fan-in `Filter` — a node that drops rows but never
/// moves one across PK partitions and never changes the schema. `None` if any
/// node on the path repartitions (reindex/HashRow Map, Join, Reduce, Union,
/// another ExchangeShard, PartitionFilter) or changes the schema
/// (Projection/KeyOnly/NullExtend/Negate/compute Map/Delay): the shard key can
/// then no longer be the source key in source order, so the exchange is not a
/// no-op.
pub(crate) fn copartition_source_behind_shard(
    loaded: &LoadedCircuit, shard_nid: i32,
) -> Option<gnitz_wire::TableId> {
    let mut cur = single_input(loaded, shard_nid)?;
    loop {
        match loaded.nodes.get(&cur)? {
            gnitz_wire::OpNode::ScanDelta(t) | gnitz_wire::OpNode::ScanTrace(t) => return Some(*t),
            gnitz_wire::OpNode::Filter(_) => cur = single_input(loaded, cur)?,
            _ => return None,
        }
    }
}

/// The unique predecessor of `nid`, or `None` for a fan-in (≥2 inputs — not a
/// simple prefix) or a source (0 inputs — never queried, the Scan arm returns first).
fn single_input(loaded: &LoadedCircuit, nid: i32) -> Option<i32> {
    match loaded.incoming.get(&nid).map(Vec::as_slice) {
        Some([(src, _port)]) => Some(*src),
        _ => None,
    }
}
```

Integrate at the co-partition decision in `compute_view_skips_exchange` (`dag.rs:757-793`):
replace the bare-scan input check (`dag.rs:767-776`) with the walk, keeping the
`shard_cols_match_dist_key` check on the source the walk lands on.

```rust
let skip = crate::compiler::copartition_source_behind_shard(&loaded, enid)
    .filter(|&tid| tid > 0)
    .and_then(|tid| self.tables.get(&(tid as i64)))
    .is_some_and(|entry| entry.schema.shard_cols_match_dist_key(&shard_cols));
```

Fan-in (a `Filter` reached by two paths, or a shard with ≠1 input) returns `None` and the view
stays un-elided — conservatively correct. Cycle guarding is unnecessary: a circuit DAG cannot
loop, and `single_input` advances strictly toward the source. The change is confined to `dag.rs`
(one call site) plus one new helper in `compiler.rs` — no change to operators, the wire format,
the circuit builder, or the SQL planner, and no new execution path: the newly-qualifying views
take the existing skip-exchange branch (`dag.rs:1346-1347`) unchanged.

**Latent (pre-existing, not addressed here):** the `ExchangeShard` is located by `find_map` over a
`HashMap` (`dag.rs:761`), nondeterministic if a view holds more than one. Safe today —
single-shard views have exactly one, and two-sided set-ops (two shards) route through the
`view_has_side_b` arm (`dag.rs:1324`) and never consult `view_skips_exchange`.

## 4. Testing

Unit-test the helper directly (no multi-worker harness) with `make_loaded`, mirroring the existing
`reindex_cols_through_filters` tests:

- **Walk accepts a Filter prefix.** `ScanDelta(t) → Filter → ExchangeShard` ⇒
  `copartition_source_behind_shard` returns `Some(t)`; chained `Filter → Filter` likewise.
- **Walk rejects.** A reindex/HashRow `Map`, a `Projection`/`KeyOnly`/`NullExtend`/`Negate`, a
  second `ExchangeShard`, or a `PartitionFilter` anywhere on the path ⇒ `None`. A fan-in `Filter`
  (two incoming edges) ⇒ `None`. A bare `ScanDelta → ExchangeShard` (no filter) ⇒ `Some(t)` (the
  no-`WHERE` case still works).

End-to-end on a multi-worker harness, grouping by a relation's distribution prefix (use a
`CLUSTER BY <prefix>` table so the prefix is a non-unique distribution key — the case that
actually shuffles without elision):

- **Elision fires.** `GROUP BY <dist_prefix> WHERE …`: assert `view_skips_exchange` is true (or
  that no IPC exchange round occurs), and that results match a non-elided reference. Compare to
  the no-`WHERE` form (already elided) — both must produce identical view contents.
- **Negative cases stay un-elided.** `GROUP BY <non-prefix>` (shard key ≠ distribution prefix), a
  permuted key, a join view (reindex prefix), and `GROUP BY <prefix>` with a reindex/HashRow map
  (or a `Filter` *and* a reindex) before the shard ⇒ `view_skips_exchange` false.
- **Correctness under retraction/cross-worker.** Insert/delete rows that, without the filter,
  would land on different workers; assert the elided path matches the shuffled reference exactly
  (the filter is row-local and cannot hide a needed repartition — but pin it).
