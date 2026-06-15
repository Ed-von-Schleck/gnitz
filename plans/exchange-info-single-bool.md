# Collapse `ExchangeInfo` to a single skip-exchange flag

## Goal

`get_exchange_info` (`dag.rs:731-789`) caches a two-field `ExchangeInfo { is_trivial,
is_co_partitioned }` (`dag.rs:373-377`). `is_co_partitioned` is set **only** inside the
`is_trivial` branch (`dag.rs:762`/`777`), so `is_co_partitioned ⟹ is_trivial` is an
invariant — yet the product type can represent the impossible `{ is_trivial: false,
is_co_partitioned: true }`. The struct's **sole** reader (`dag.rs:1266-1269`) ANDs both
fields:

```rust
let skip_exchange = {
    let info = self.get_exchange_info(view_id);
    info.is_trivial && info.is_co_partitioned
};
```

So the two bools encode exactly one bit of usable information. Collapse them to a single
cached bool and make the impossible state unrepresentable. Pure refactor — the set of views
whose exchange is elided is byte-for-byte unchanged.

## Change

`get_exchange_info` has exactly one caller, so rename it to the question it answers and
return `bool` directly:

```rust
/// True iff the view's output `ExchangeShard` is a no-op (every row already on its
/// PK-owner) and can be skipped: the shard reads from a bare scan whose PK is the
/// shard key.
pub fn view_skips_exchange(&mut self, view_id: i64) -> bool {
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

The caller (`dag.rs:1266-1269`) becomes:

```rust
let skip_exchange = self.view_skips_exchange(view_id);
```

The bare-scan / `shard_cols_match_pk` logic is identical to today — only the cached
representation changes. (Widening *which* views qualify is a separate plan,
`copartition-exchange-elision-through-filters.md`.)

### Field/cache edits

- Delete `struct ExchangeInfo` (`dag.rs:373-377`).
- Replace `exchange_info_cache: FxHashMap<i64, ExchangeInfo>` (field `dag.rs:410`, init
  `dag.rs:441`) with `skip_exchange_cache: FxHashMap<i64, bool>`.
- Evict it at the same three sites the old cache used: `invalidate` (`dag.rs:480`),
  `invalidate_view` (`dag.rs:588`), and the two clear-all paths (`dag.rs:595-597`,
  `dag.rs:2011-2013`).
- Drop `get_exchange_info`'s side-population of `shard_cols_cache` (old `dag.rs:786`):
  `get_shard_cols` (`dag.rs:676-691`) already finds the same `ExchangeShard` node and warms
  that cache lazily, so the population was a duplicate. `view_skips_exchange` no longer
  touches `shard_cols_cache`.

## Testing

- `test_invalidation` (`dag.rs:2062-2084`): replace the `ExchangeInfo { is_trivial,
  is_co_partitioned }` constructor (`dag.rs:2066-2069`) with
  `dag.skip_exchange_cache.insert(42, true)` and assert the key is evicted on `invalidate`.
- Behavior preservation: a `GROUP BY <pk>` view (no `WHERE`) still reports
  `view_skips_exchange == true`; a `GROUP BY <non-pk>` view and a join view report `false`.
  These match the pre-refactor `is_trivial && is_co_partitioned` outcomes exactly.
