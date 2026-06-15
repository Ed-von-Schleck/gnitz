# Filtered GROUP BY co-partition (view exchange-skip through Filters)

## 1. Summary

`compute_view_skips_exchange` (`dag.rs:757-789`) decides whether a single-source
`ExchangeShard` view can skip its network shuffle and run its pre-phase locally. It
fires only when the shard's sole input is a **bare scan** (`dag.rs:767-776`: exactly
one incoming edge, from a node that itself has no incoming edge). An intervening
`Filter` node breaks that test, so on a table distributed by a PK prefix an
**unfiltered** `GROUP BY <prefix>` co-partitions and runs locally, but
`GROUP BY <prefix> … WHERE <pred>` does not — it exchanges, even though a filter is
row-selective and never moves a row off its worker.

The **join** detector already handles this: `compute_co_partitioned` resolves the
reindex key *through* Filter nodes via `reindex_cols_through_filters`
(`compiler.rs:503`). This plan makes `compute_view_skips_exchange` walk Filters the
same way, so a filtered `GROUP BY`/reduce on the distribution prefix co-partitions
too, and the two detectors share one Filter-walking discipline.

Depends on `plans/per-relation-distribution-keys.md` (the base distribution-prefix
feature and `shard_cols_match_dist_key`). This is purely a detector relaxation — no
routing, storage, persistence, or wire change. It is correctness-neutral: it only
*adds* skips that were already safe, never removes one.

## 2. Scope

**In scope:** relax the bare-scan precondition in `compute_view_skips_exchange` to a
walk through `Filter` nodes down to the `ScanDelta`/`ScanTrace` source.

**Non-goals:** widening the shard-key match (still exact-prefix
`shard_cols_match_dist_key`, see the base plan §4.3); walking through any node other
than `Filter`; DISTINCT/set-op sides (they reindex to a synthetic content-hash PK and
never read a bare scan, so they are unaffected either way).

## 3. Why it is correct

A `Filter` is a linear, row-selective operator: it drops rows but never rewrites a
row's PK region and never moves a row to another worker (filters compile into the
local pre-phase). So if the base rows are physically distributed by the distribution
prefix, they remain so after any chain of Filters. A `GROUP BY` whose key equals the
distribution prefix therefore still has every group value wholly on one worker after
filtering — the local reduce + multi-worker gather stays correct, exactly as in the
unfiltered case (already live and tested: `test_group_by_compound_pk_multiworker`,
`planner_group_by.rs:305-345`).

The walk must stop at the first **non-`Filter`, non-scan** node. It must NOT cross a
`Map` (which can reindex/relabel the PK region via `map_reindex`), a `Reduce`, a
`Distinct`, or a join: each changes the key or its distribution. Only `Filter` and the
terminal `ScanDelta`/`ScanTrace` are transparent to the shard key — the same set
`reindex_cols_through_filters` treats as transparent. The shard columns themselves are
read off the `ExchangeShard` node and are unchanged by Filters, so the existing
`shard_cols` stays the comparison key; only the *source table id* resolution walks.

## 4. Current code (`dag.rs:757-789`)

```rust
// The shard's sole input must be a bare scan: exactly one incoming edge,
// from a node that itself has none.
let incoming: Vec<i32> = loaded.edges.iter()
    .filter(|&&(_, dst, _)| dst == enid)
    .map(|&(src, _, _)| src)
    .collect();
let [src_nid] = incoming.as_slice() else { return false };
if loaded.edges.iter().any(|&(_, dst, _)| dst == *src_nid) {
    return false;
}
let tid = match loaded.nodes.get(src_nid) {
    Some(gnitz_wire::OpNode::ScanDelta(t))
    | Some(gnitz_wire::OpNode::ScanTrace(t)) => *t as i64,
    _ => return false,
};
self.tables.get(&tid)
    .is_some_and(|entry| entry.schema.shard_cols_match_dist_key(&shard_cols))
```

## 5. Change

Replace the single-hop bare-scan resolution with a walk that follows the single input
chain through `Filter` nodes to a scan. Every hop must have **exactly one** incoming
edge (a fan-in is not a linear chain and must bail), and only `Filter` is traversed:

```rust
// Walk the single input chain through Filter nodes to the source scan.
// Each hop must be linear (exactly one incoming edge); only Filter is transparent
// to the shard key — Map/Reduce/Distinct/join change the key or its distribution.
let mut cur = enid;
let tid = loop {
    let incoming: Vec<i32> = loaded.edges.iter()
        .filter(|&&(_, dst, _)| dst == cur)
        .map(|&(src, _, _)| src)
        .collect();
    let [src_nid] = incoming.as_slice() else { return false };
    match loaded.nodes.get(src_nid) {
        Some(gnitz_wire::OpNode::ScanDelta(t))
        | Some(gnitz_wire::OpNode::ScanTrace(t)) => break *t as i64,
        Some(gnitz_wire::OpNode::Filter(_)) => cur = *src_nid,
        _ => return false,
    }
};
self.tables.get(&tid)
    .is_some_and(|entry| entry.schema.shard_cols_match_dist_key(&shard_cols))
```

The walk is bounded by the node count (the chain is acyclic and strictly descends one
edge per hop); a `visited` guard is unnecessary because each hop consumes a distinct
upstream node along a single-incoming chain. The memoization wrapper
(`view_skips_exchange`, `skip_exchange_cache`, `dag.rs:748-755`) is unchanged.

## 6. Testing

- **Filtered GROUP BY locality:** `GROUP BY prefix … WHERE pred` on a
  prefix-distributed table skips its exchange (assert no relay scatter) and returns
  results identical to the exchanged path. Run under `GNITZ_WORKERS=4`.
- **Walk stops at Map:** a view with a reindex/`Map` between the shard and the scan
  still exchanges (the walk bails at the non-`Filter` node).
- **Fan-in bails:** a shard fed (directly or through Filters) by a node with two
  incoming edges still exchanges.
- **Default parity:** an unfiltered `GROUP BY prefix` still skips (no regression);
  `GROUP BY` on a non-prefix column still exchanges (`shard_cols_match_dist_key`
  unchanged).
