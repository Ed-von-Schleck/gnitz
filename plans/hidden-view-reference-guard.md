# Reject reserved-prefix relation references in the binder

## Problem

The reserved leading-`_` identifier rule is enforced only for the entity being
**created or dropped** (`validate_user_name` at CREATE TABLE
`plan/ddl.rs:205`, DROP TABLE/VIEW `plan/ddl.rs:583/587`, CREATE VIEW
`plan/view/dispatch.rs:33`, client `create_schema` `client.rs:562`). A
**referenced** relation name is never checked: `Binder::resolve`
(`crates/gnitz-sql/src/bind/resolve.rs:131`) probes the catalog via
`resolve_table_or_view_id` with no guard, so a user query can name a
synthesized hidden chain segment (`__h{owner_vid}_{idx}`) directly.

Verified live (4-worker server, planner path):

- `CREATE VIEW s AS WITH cc AS (SELECT id, v FROM t WHERE v > 10) SELECT id
  FROM cc` → `ViewCreated{18}`, hidden segment `__h18_0` (vid 17) registered
  in VIEW_TAB.
- `SELECT * FROM __h18_0` → **succeeds** (returns the hidden filter view's
  rows).
- `CREATE VIEW v2 AS SELECT * FROM __h18_0` → **succeeds**, writing a DEP row
  `v2 → __h18_0`.
- `DROP VIEW s` → **fails forever** with `View dependency: entity
  '….__h18_0'`: the engine RESTRICT check
  (`crates/gnitz-engine/src/catalog/write_path.rs:522-538`) sees `v2` as a
  live dependent of `__h18_0` outside the drop bundle
  `{s, __h18_0}`, so the owner view is permanently undroppable while `v2`
  exists. (`drop_schema` still converges — its `drain_drops` retry removes
  `v2` first — so the blast radius is exactly the single-view DROP.)

Hidden segments are internal circuit plumbing: their ownership cascade
(`drop_view`'s `__h{vid}_` prefix scan, `client.rs:1062`) assumes no external
dependents can exist. User references must be rejected, not honored.

## Design

One guard at the single read-resolution funnel. In `Binder::resolve`
(`crates/gnitz-sql/src/bind/resolve.rs:131`), after the cache-hit early
return and before the catalog probe:

```rust
    pub(crate) fn resolve(&mut self, client: &mut GnitzClient, name: &str) -> Result<(u64, Rc<Schema>), GnitzSqlError> {
        if let Some(entry) = self.cache.get(name) {
            return Ok((entry.table_id, Rc::clone(&entry.schema)));
        }
        // Referenced relations obey the same reserved-prefix rule as created
        // ones: a fresh catalog probe of a leading-`_` name can only be a user
        // naming internal plumbing (a hidden chain segment), and honoring it
        // leaks a dependency that makes the owner view undroppable. Placed
        // after the cache check — chain segments legitimately resolve through
        // `cache_alias` under their CTE/derived-table alias, never by their
        // `__h…` catalog name.
        crate::plan::validate::validate_user_name(name)?;
        let (tid, schema) = client
            .resolve_table_or_view_id(self.schema_name, name)
            .map_err(GnitzSqlError::Exec)?;
        ...
    }
```

Load-bearing facts, verified at HEAD (9911297f):

- **The funnel is complete.** Every planner path that resolves a *read*
  relation goes through `Binder::resolve`: bare SELECT (`dml/select.rs:100`),
  linear views (`plan/lp.rs:58`), pass-through CTEs
  (`plan/view/dispatch.rs:420`), operator inputs (`dispatch.rs`
  `resolve_operator_input`), join sides (`plan/view/join.rs:257/275`), set-op
  sides (`plan/view/set_op.rs:32`), EXISTS relations
  (`plan/view/exists.rs:89/152`). Write/index targets go through
  `resolve_base_table`, which already rejects any view (hidden included) as a
  non-table.
- **The cache placement is required, and sufficient.** Chain segments reach
  downstream resolution only via `cache_alias` under their user-visible
  CTE/derived-table alias (`dispatch.rs` `compile_hidden_body`,
  `inline_passthrough_cte`); circuits reference upstream segments by
  pre-allocated vid, never by name. The raw `__h…` string is never a cache
  key and never legitimately reaches the catalog probe.
- **No legitimate name is rejected.** Every creatable relation already passed
  `validate_user_name` at its CREATE, so the guard is a no-op for all real
  relations. System tables are not rows in TABLE_TAB/VIEW_TAB and cannot
  resolve through this probe at all.
- **The raw client API is out of scope by design.**
  `resolve_table_or_view_id` / `scan(vid)` remain a trusted low-level surface
  (already able to scan arbitrary ids and push arbitrary batches); the
  `view_chain.rs` integration tests legitimately introspect hidden segments
  through it. The SQL planner is the user-facing integrity boundary.

The `__h{vid}_{idx}` name convention itself is sound and unchanged: the `_`
terminator makes cross-owner prefix matches impossible (`__h5_` cannot match
`__h51_0`; `format!` emits no leading zeros), and `hidden_view_name` /
`hidden_view_prefix` / `is_hidden_view_name` stay the single shared
definitions.

## Files touched

- `crates/gnitz-sql/src/bind/resolve.rs` — the guard (one statement + doc
  comment).
- `crates/gnitz-sql/tests/view_chain.rs` — two integration tests.
- `crates/gnitz-py/tests/test_cte_chain.py` — one E2E test.

## Tests

Rust (`crates/gnitz-sql/tests/view_chain.rs`, `integration` feature):

1. `hidden_segment_reference_rejected`: create a WHERE'd-CTE view `s`
   (planner returns `ViewCreated{view_id}`; the hidden segment is
   `__h{view_id}_0`). Assert `CREATE VIEW v2 AS SELECT * FROM __h{view_id}_0`
   errors with the reserved-prefix plan error, and that `DROP VIEW s` then
   **succeeds** — the regression the guard exists for.
2. `hidden_segment_select_rejected`: assert a bare
   `SELECT * FROM __h{view_id}_0` through the planner errors; then assert the
   same name still resolves through the raw
   `client.resolve_table_or_view_id` (the trusted surface is deliberately
   unguarded).

Python E2E (`crates/gnitz-py/tests/test_cte_chain.py`):

3. `test_reference_to_hidden_segment_rejected`: same shape end-to-end —
   create the chain view, attempt `CREATE VIEW v2 AS SELECT * FROM
   __h{vid}_0` (expect an exception), then `DROP VIEW` the owner and assert
   it succeeds and both VIEW_TAB rows are gone (owner unresolvable).

Existing coverage that must stay green untouched: the `view_chain.rs` drop
cascade / drop-schema drain tests (raw-client hidden introspection), and
`validate.rs`'s unit pin of `validate_user_name("__h5_0") → Err`.

## Sequencing

- [ ] 1. Guard in `Binder::resolve` + the three tests; `make verify` green.
- [ ] 2. `make e2e` green (W=4).
