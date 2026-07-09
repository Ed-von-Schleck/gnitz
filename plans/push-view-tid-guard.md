# Reject plain pushes addressed to a view tid

## Problem

A `FLAG_PUSH` frame addressed to a **view's** table id is accepted and
committed into the view's output store, silently corrupting derived state.

- `has_id` is only `dag.tables.contains_key` (`catalog/registry.rs:119-121`),
  and views register into `dag.tables` under the same id space as base
  tables (`catalog/hooks.rs:443`, shared `next_table_id`), so the push
  arm's existence check (`executor.rs:939-943`) passes for a view tid.
- A view never enters `needs_lock` (`catalog/cache.rs`), so `fk_lock_set`
  is empty; it registers `RelationKind::View` (`query/dag/mod.rs:103-113`)
  with `unique_pk = false` and has no FK/unique metadata, so
  `validate_all_distributed_async` early-outs (`master/preflight.rs`).
  Nothing rejects the frame; the committer emits a normal `FLAG_PUSH` group
  and workers ingest it into the view's family — rows that the view's
  circuit never produced, invisible to its operator traces, and permanently
  divergent from what a rebuild would produce (a generation-invalid restart
  silently "repairs" the corruption, masking it).

Views are correctly unreachable through SQL DML (the binder resolves DML
targets to base tables), so the exposure is the raw client API
(`GnitzClient::push` / `push_with_mode` / `delete` with a view id) — a
misuse that today corrupts instead of erroring.

Only a **non-empty** push reaches the corruption path. The client sets
`FLAG_HAS_DATA` only for a non-empty batch (`protocol/message.rs:208`), so
an empty push arrives with `has_batch = false` and routes to `handle_scan`
(a harmless read); the guard therefore belongs on the non-empty-batch push
route, which is exactly where the INSERT block lives.

## Change

### 1. `relation_kind` accessor on the catalog

Add beside `table_has_unique_pk` in
`crates/gnitz-engine/src/catalog/metadata.rs`. `RelationKind` is already in
scope via `use super::*` (`catalog/mod.rs:50`); `TableEntry.kind` and
`RelationKind::is_base_table` (`query/dag/mod.rs:138`) are public.

```rust
    /// The registered `RelationKind` of a table/view id, or `None` if the id
    /// is absent. One `dag.tables` lookup serves both the existence and the
    /// writability check on the push path.
    pub fn relation_kind(&self, table_id: i64) -> Option<RelationKind> {
        self.dag.tables.get(&table_id).map(|e| e.kind)
    }
```

### 2. Existence + writability guard in the push arm

In `crates/gnitz-engine/src/runtime/orchestration/executor.rs`, replace the
`has_id` existence check at the head of the "User-table INSERT" block
(`executor.rs:938-943`) — under the same catalog read lock — with a single
`relation_kind` match that folds existence and writability together:

```rust
    // ---------- User-table INSERT ----------
    if target_id >= FIRST_USER_TABLE_ID && has_batch && batch_count > 0 {
        let mode = ipc::wire_flags_get_conflict_mode(flags);
        let batch = decoded.data_batch.unwrap();

        let _cat = shared.catalog_rwlock.read().await;
        // Existence + writability in one registry lookup. A view registers
        // into the same id space as base tables (shared next_table_id), so a
        // raw client push addressed to a view tid would otherwise commit rows
        // into the view's output store that its circuit never produced —
        // permanently divergent derived state. Only base tables are push
        // targets; System catalog tids are already excluded by the
        // FIRST_USER_TABLE_ID gate above.
        match shared.cat().relation_kind(target_id) {
            None => {
                let msg = format!("table {target_id} not found");
                send_error(peer, target_id, client_id, msg.as_bytes()).await;
                return;
            }
            Some(kind) if !kind.is_base_table() => {
                let msg =
                    format!("table {target_id} is not writable: pushes must target a base table");
                send_error(peer, target_id, client_id, msg.as_bytes()).await;
                return;
            }
            _ => {}
        }
        // Acquire all FK-related table locks in ascending tid order …
```

The raw `delete` path needs no separate guard: `delete` is a plain push
(`gnitz-core/src/client.rs:543-590`), so it lands in the same INSERT block.

Scans, seeks, and view ticks are unaffected: the guard is on the
non-empty-batch push route only.

## Tests

- E2E (`GNITZ_WORKERS=4`): raw-client push to a view tid returns the error
  and the view's content is unchanged (scan before/after identical);
  same for `delete` with a view tid; a subsequent restart does not change
  the view's content (no divergent state was persisted).
- The error text (`is not writable: pushes must target a base table`) is
  asserted so the SQL layer's binder behavior (which never routes DML to
  views) is not accidentally relied upon as the only guard.
- Push to a genuinely absent tid still returns `table <id> not found`
  (the `None` arm), confirming the consolidation preserved the
  existence-check semantics.
