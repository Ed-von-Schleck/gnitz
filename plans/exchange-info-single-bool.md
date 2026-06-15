# Collapse `ExchangeInfo` to a skip flag; memoize `view_needs_exchange`

## Goal

Two adjacent queries on the multi-worker hot path (`dag.rs:1259-1269`) waste work
per view per tick:

1. `get_exchange_info` (`dag.rs:731-789`) caches a two-field `ExchangeInfo {
   is_trivial, is_co_partitioned }` (`dag.rs:373-377`). `is_co_partitioned` is set
   **only** inside the `is_trivial` branch (`dag.rs:762`/`777`), so
   `is_co_partitioned ⟹ is_trivial` is an invariant — yet the product type can
   represent the impossible `{ is_trivial: false, is_co_partitioned: true }`. The
   struct's **sole** reader (`dag.rs:1266-1269`) ANDs both fields:

   ```rust
   let skip_exchange = {
       let info = self.get_exchange_info(view_id);
       info.is_trivial && info.is_co_partitioned
   };
   ```

   The two bools encode exactly one bit of usable information.

2. `view_needs_exchange` (`dag.rs:814-817`) is **uncached** — it calls
   `load_meta_circuit` (a `load_circuit` + `topo_sort`, `O(V+E)` with several
   allocations) on **every** call, and the hot path (`dag.rs:1259`) calls it once
   per view per tick.

Fix both with the codebase's established idiom — one `FxHashMap<i64, _>` per
memoized query, evicted in lockstep with the sibling view caches
(`shard_cols_cache`, `join_shard_cols_cache`, `range_join_cache`):

- Collapse `get_exchange_info` to `view_skips_exchange` returning a single cached
  `bool`, making the impossible state unrepresentable.
- Memoize `view_needs_exchange`'s node-existence check in a sibling `bool` cache.

Both are pure perf/representation refactors: the set of views whose exchange is
elided is byte-for-byte unchanged, and `view_needs_exchange` returns the same value
for every view. Widening *which* views qualify for skip is a separate plan
(`copartition-exchange-elision-through-filters.md`).

## Change

### `view_skips_exchange` (replaces `get_exchange_info`)

`get_exchange_info` has exactly one caller, so rename it to the question it
answers and return `bool` directly:

```rust
/// True iff the view's output `ExchangeShard` is a no-op (every row already on its
/// PK-owner) and can be skipped: the shard reads from a bare scan whose PK is the
/// shard key.
fn view_skips_exchange(&mut self, view_id: i64) -> bool {
    if let Some(&skip) = self.skip_exchange_cache.get(&view_id) {
        return skip;
    }
    let loaded = self.load_meta_circuit(view_id);
    let mut skip = false;

    // The view's output ExchangeShard and its shard columns.
    let exchange = loaded.nodes.iter().find_map(|(&nid, op)| match op {
        gnitz_wire::OpNode::ExchangeShard { shard_cols } =>
            Some((nid, shard_cols.iter().map(|&c| c as i32).collect::<Vec<_>>())),
        _ => None,
    });
    if let Some((enid, shard_cols)) = exchange {
        let incoming_srcs: Vec<i32> = loaded.edges.iter()
            .filter(|&&(_, dst, _)| dst == enid)
            .map(|&(src, _, _)| src)
            .collect();
        // Sole input is a bare scan (no incoming edge) whose PK is the shard key.
        if let [src_nid] = incoming_srcs.as_slice() {
            if !loaded.edges.iter().any(|&(_, dst, _)| dst == *src_nid) {
                let tid = match loaded.nodes.get(src_nid) {
                    Some(gnitz_wire::OpNode::ScanDelta(t))
                    | Some(gnitz_wire::OpNode::ScanTrace(t)) => *t,
                    _ => 0,
                };
                if tid > 0 {
                    if let Some(entry) = self.tables.get(&(tid as i64)) {
                        skip = entry.schema.shard_cols_match_pk(&shard_cols);
                    }
                }
            }
        }
    }

    self.skip_exchange_cache.insert(view_id, skip);
    skip
}
```

The bare-scan / `shard_cols_match_pk` logic is identical to today — only the
cached representation changes.

### `view_needs_exchange` (memoized)

Keep the existing node-existence check; cache it in a sibling `bool` map.

```rust
/// Check if a view needs exchange (has an `ExchangeShard` node).
pub fn view_needs_exchange(&mut self, view_id: i64) -> bool {
    if let Some(&needs) = self.needs_exchange_cache.get(&view_id) {
        return needs;
    }
    let loaded = self.load_meta_circuit(view_id);
    let needs = loaded.nodes.values()
        .any(|op| matches!(op, gnitz_wire::OpNode::ExchangeShard { .. }));
    self.needs_exchange_cache.insert(view_id, needs);
    needs
}
```

`view_needs_exchange` stays `pub` — `bootstrap.rs:202` (`backfill_exchange_views`)
and `hooks.rs:347` call it. Both must keep working for **uncompiled** views:
`backfill_exchange_views` runs at startup over every user-table id before any view
is compiled. The check derives from `load_meta_circuit` (the system tables), not
from `self.cache` (the compiled plan), so it is correct regardless of compile
state — caching changes only cost, not the result.

> Do **not** rewrite this as `!self.get_shard_cols(view_id).is_empty()` to reuse
> `shard_cols_cache`. `get_shard_cols` returns `[]` both for "no `ExchangeShard`
> node" *and* for an `ExchangeShard` with empty `shard_cols` — which a global
> aggregate built on empty group columns (`reduce_multi(input, &[], …)` →
> `shard(input, &[])`) produces. That shortcut would report such a view as needing
> no exchange and silently drop its shuffle. Test node existence, not shard-col
> emptiness.

### Caller

`dag.rs:1266-1269` becomes a single cached call; `dag.rs:1259` is unchanged at the
call site (now cached internally):

```rust
let has_exchange = self.view_needs_exchange(view_id);

let has_join_shard = {
    let cols = self.get_join_shard_cols(view_id, src_id);
    !cols.is_empty()
};

let skip_exchange = self.view_skips_exchange(view_id);
```

### Field / cache edits

- Delete `struct ExchangeInfo` (`dag.rs:373-377`).
- Replace `exchange_info_cache: FxHashMap<i64, ExchangeInfo>` (field `dag.rs:410`,
  init `dag.rs:441`) with two sibling `bool` caches:
  `skip_exchange_cache: FxHashMap<i64, bool>` and
  `needs_exchange_cache: FxHashMap<i64, bool>`.
- Evict **both** at the four sites the old cache used: `unregister_table`
  (`dag.rs:480`), `invalidate` (`dag.rs:588`), `invalidate_all` (`dag.rs:597`), and
  `close` (`dag.rs:2013`) — the same lockstep pattern the sibling view caches
  already follow.
- Drop `get_exchange_info`'s side-population of `shard_cols_cache` (old `dag.rs:786`):
  `get_shard_cols` (`dag.rs:676-691`) already finds the same `ExchangeShard` node and
  warms that cache lazily, so the population was a duplicate. `view_skips_exchange`
  no longer touches `shard_cols_cache`.

## Testing

- `test_invalidation` (`dag.rs:2062-2084`): replace the `ExchangeInfo { is_trivial,
  is_co_partitioned }` constructor (`dag.rs:2066-2069`) with
  `dag.skip_exchange_cache.insert(42, true)` and `dag.needs_exchange_cache.insert(42,
  true)`, and assert both keys are evicted on `invalidate`.
- Behavior preservation, unchanged from the pre-refactor `is_trivial &&
  is_co_partitioned` outcomes:
  - a `GROUP BY <pk>` view (no `WHERE`) reports `view_skips_exchange == true` and
    `view_needs_exchange == true`;
  - a `GROUP BY <non-pk>` view and a join view report `view_skips_exchange == false`
    and `view_needs_exchange == true`;
  - a view with no exchange reports both `false`.
- `view_needs_exchange` on an **uncompiled** view still returns the meta-circuit
  answer (regression guard for the startup `backfill_exchange_views` path): assert it
  is `true` for a freshly-registered exchange view before any `ensure_compiled` call.
