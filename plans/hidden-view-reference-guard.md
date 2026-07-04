# Enforce the reserved-name policy on every binder and DDL surface

## Problem

The reserved leading-`_` identifier rule (the system prefix, plus synthesized
hidden chain segments `__h{owner_vid}_{idx}`) is enforced today only for the
entity being **created** (`validate_user_name` at CREATE TABLE `plan/ddl.rs`,
CREATE VIEW `plan/view/dispatch.rs`, client `create_schema`) or **dropped as a
table/view** (DROP TABLE/VIEW `plan/ddl.rs`). Four resolution / definition
surfaces skip the rule, each honoring or leaking a reserved name:

1. **Read references — `Binder::resolve`** (`crates/gnitz-sql/src/bind/resolve.rs`):
   probes the catalog via `resolve_table_or_view_id` with no guard, so a user
   query can name a hidden segment directly. Verified live (4-worker server):

   - `CREATE VIEW s AS WITH cc AS (SELECT id, v FROM t WHERE v > 10) SELECT id
     FROM cc` → `ViewCreated{18}`, hidden segment `__h18_0` (vid 17) registered
     in VIEW_TAB.
   - `SELECT * FROM __h18_0` → **succeeds** (returns the hidden filter's rows).
   - `CREATE VIEW v2 AS SELECT * FROM __h18_0` → **succeeds**, writing a DEP row
     `v2 → __h18_0`.
   - `DROP VIEW s` → **fails forever** with `View dependency: entity
     '….__h18_0'`: the engine RESTRICT check
     (`crates/gnitz-engine/src/catalog/write_path.rs`) sees `v2` as a live
     dependent of `__h18_0` outside the drop bundle `{s, __h18_0}`, so the owner
     view is permanently undroppable while `v2` exists. (`drop_schema` still
     converges — its `drain_drops` retry removes `v2` first — so the blast radius
     is exactly the single-view DROP.)

   Hidden segments are internal circuit plumbing: their ownership cascade
   (`drop_view`'s `__h{vid}_` prefix scan) assumes no external dependents exist.

2. **Write / index targets — `Binder::resolve_base_table`** (same file): INSERT,
   UPDATE, DELETE and CREATE INDEX resolve here. It probes `resolve_table_id`
   (TABLE_TAB only), and on a miss re-probes `resolve_table_or_view_id` to tell
   "is a view" from "not found". For a hidden segment `__h18_0` this returns
   `Unsupported("'__h18_0' is a view …")`, and for a name that does not exist it
   returns `not found` — the differing errors are an **existence side-channel**
   for transient hidden segments, and the "is a view" text is a confusing message
   for internal plumbing a user should not be able to name at all.

3. **CTE / derived-table aliases — `inline_ctes` / `compile_derived_tables`**
   (`crates/gnitz-sql/src/plan/view/dispatch.rs`): a CTE/derived alias is a
   user-chosen name registered into the binder cache via `cache_alias`. Nothing
   validates it, so `WITH __h5_0 AS (…) SELECT … FROM __h5_0` or `(…) AS _d`
   compiles — reserved names usable locally, inconsistent with every CREATE
   surface. (No catalog corruption: the real hidden segment still gets its own
   allocated `__h{vid}_{idx}` name; the cache is per-statement and local.)

4. **DROP INDEX — `execute_drop`** (`crates/gnitz-sql/src/plan/ddl.rs`): the
   `ObjectType::Index` arm calls `client.drop_index_by_name` directly, skipping
   `validate_user_index_name`, which CREATE INDEX enforces (it rejects the
   `__fk_` infix and leading-`_` names).

One rule, applied at every surface.

## Design

### 1. The read funnel — guard in `Binder::resolve`

After the cache-hit early return and before the catalog probe:

```rust
    pub(crate) fn resolve(&mut self, client: &mut GnitzClient, name: &str) -> Result<(u64, Rc<Schema>), GnitzSqlError> {
        if let Some(entry) = self.cache.get(name) {
            return Ok((entry.table_id, Rc::clone(&entry.schema)));
        }
        // Referenced relations obey the same reserved-prefix rule as created
        // ones: a fresh catalog probe of a leading-`_` name can only be a user
        // naming internal plumbing (a hidden chain segment), and honoring it
        // leaks a dependency that makes the owner view undroppable. Placed after
        // the cache check — a CTE/derived-table sub-plan resolves through
        // `cache_alias` under its user alias (itself validated at definition,
        // §3), never by a raw `__h…` catalog name.
        crate::plan::validate::validate_user_name(name)?;
        let (tid, schema) = client
            .resolve_table_or_view_id(self.schema_name, name)
            .map_err(GnitzSqlError::Exec)?;
        let rc = Rc::new(schema);
        self.cache_relation(name.to_string(), tid, Rc::clone(&rc));
        Ok((tid, rc))
    }
```

Load-bearing facts, verified against current source:

- **The funnel is complete.** Every planner path that resolves a *read* relation
  goes through `Binder::resolve`: bare SELECT (`dml/select.rs`), linear views
  (`plan/lp.rs`), pass-through CTEs (`dispatch.rs`), operator inputs
  (`resolve_operator_input`), join sides (`plan/view/join.rs`), set-op sides
  (`plan/view/set_op.rs`), EXISTS/IN relations (`plan/view/exists.rs`).
- **The cache placement is required, and sufficient.** Chain segments reach
  downstream resolution only via `cache_alias` under their user CTE/derived-table
  alias (`compile_hidden_body`, `inline_passthrough_cte`); circuits reference
  upstream segments by pre-allocated vid, never by name. The raw `__h…` string is
  never a cache key and never legitimately reaches the catalog probe.
- **No legitimate name is rejected.** Every creatable relation already passed
  `validate_user_name` at its CREATE, so the guard is a no-op for all real
  relations. System tables are not rows in TABLE_TAB/VIEW_TAB and cannot resolve
  through this probe at all.
- **The raw client API is out of scope by design.**
  `resolve_table_or_view_id` / `scan(vid)` remain a trusted low-level surface
  (already able to scan arbitrary ids and push arbitrary batches); the
  `view_chain.rs` integration tests legitimately introspect hidden segments
  through it. The SQL planner is the user-facing integrity boundary.

### 2. Write / index targets — guard in `Binder::resolve_base_table`

The same rule at the head of the write/index funnel, before either catalog
probe. This is the write-side mirror of §1 and additionally closes the
existence side-channel:

```rust
    pub(crate) fn resolve_base_table(
        &mut self,
        client: &mut GnitzClient,
        name: &str,
    ) -> Result<(u64, Rc<Schema>), GnitzSqlError> {
        // Same reserved-prefix rule as the read funnel: a write/index target
        // naming a leading-`_` relation can only be a user reaching for system
        // plumbing (a hidden segment). Rejecting here — before the catalog probe —
        // also closes the existence side-channel the two-probe fallback below
        // would otherwise open (a hidden segment returns "is a view", a missing
        // name returns "not found").
        crate::plan::validate::validate_user_name(name)?;
        // `resolve_table_id` consults TABLE_TAB only, so a view name misses it.
        match client.resolve_table_id(self.schema_name, name) {
            Ok((tid, schema)) => {
                let rc = Rc::new(schema);
                self.cache_relation(name.to_string(), tid, Rc::clone(&rc));
                Ok((tid, rc))
            }
            Err(not_found) => match client.resolve_table_or_view_id(self.schema_name, name) {
                Ok(_) => Err(GnitzSqlError::Unsupported(format!(
                    "'{name}' is a view; INSERT, UPDATE, DELETE and CREATE INDEX \
                     require a base table"
                ))),
                Err(_) => Err(GnitzSqlError::Exec(not_found)),
            },
        }
    }
```

Every real INSERT/UPDATE/DELETE/CREATE INDEX target passed `validate_user_name`
at its table's CREATE, so the guard is a no-op for them; only a reserved-prefix
target (system/hidden) is rejected — with the reserved-prefix error, uniformly,
regardless of whether the hidden segment currently exists.

### 3. CTE / derived-table aliases — validate at definition

A CTE/derived alias is a user-chosen name; hold it to the same rule as a created
relation, at the point it is introduced. In `inline_ctes`, at the top of the
per-CTE loop (covers both the pass-through and hidden-body branches):

```rust
    for cte in &with.cte_tables {
        // A CTE name is a user-chosen alias: same reserved-prefix rule as a
        // created relation, so `__h…` / `_x` cannot be used even locally.
        validate_user_name(&cte.alias.name.value)?;
        let ctx = format!("CTE '{}'", cte.alias.name.value);
        ...
```

In `compile_derived_tables`, right after the required-alias check:

```rust
        let Some(alias) = alias else {
            return Err(GnitzSqlError::Unsupported(
                "a derived table (subquery in FROM) needs an alias".to_string(),
            ));
        };
        // The derived-table alias is a user-chosen name — same reserved-prefix rule.
        validate_user_name(&alias.name.value)?;
        let ctx = format!("derived table '{}'", alias.name.value);
```

`validate_user_name` is already imported in `dispatch.rs`. With this, the binder
cache can only ever hold validated (non-reserved) keys, so §1's guard-after-cache
placement rejects exactly the direct `__h…` catalog references it targets.

### 4. DROP INDEX — validate the name

Mirror CREATE INDEX's name rule on the drop path. In `execute_drop`:

```rust
            ObjectType::Index => {
                // Mirror CREATE INDEX's name rule on the drop path: reject a
                // reserved-prefix or `__fk_`-infixed name planner-side. The engine
                // already refuses to retract an FK-backing index
                // (`precheck_sys_ingest`: "Integrity violation: cannot drop an
                // internal FK index"), so this is defense-in-depth plus a clearer
                // planner-side error and one fewer server round-trip.
                validate_user_index_name(&name)?;
                client.drop_index_by_name(&name).map_err(GnitzSqlError::Exec)?;
            }
```

`validate_user_index_name` is already imported in `ddl.rs`. Auto-generated index
names (`{schema}__{table}__idx_…`) and every valid user name pass it, so no
droppable index becomes undroppable.

## Name-convention soundness (unchanged)

The `__h{vid}_{idx}` convention itself is sound: the `_` terminator makes
cross-owner prefix matches impossible (`__h5_` cannot match `__h51_0`; `format!`
emits no leading zeros), and `hidden_view_name` / `hidden_view_prefix` /
`is_hidden_view_name` stay the single shared definitions.

## Files touched

- `crates/gnitz-sql/src/bind/resolve.rs` — the guard in `resolve` (§1) and
  `resolve_base_table` (§2).
- `crates/gnitz-sql/src/plan/view/dispatch.rs` — CTE + derived-table alias
  validation (§3).
- `crates/gnitz-sql/src/plan/ddl.rs` — DROP INDEX validation (§4).
- `crates/gnitz-sql/tests/view_chain.rs` — read + write reference tests.
- `crates/gnitz-sql/tests/planner_cte.rs` — CTE/derived alias rejection.
- `crates/gnitz-sql/tests/planner_create_index.rs` — DROP INDEX rejection.
- `crates/gnitz-py/tests/test_cte_chain.py` — one E2E test.

## Tests

Rust (`integration` feature):

1. `hidden_segment_reference_rejected` (`view_chain.rs`): create a WHERE'd-CTE
   view `s` (planner returns `ViewCreated{view_id}`; hidden segment
   `__h{view_id}_0`). Assert `CREATE VIEW v2 AS SELECT * FROM __h{view_id}_0`
   errors with the reserved-prefix plan error, and that `DROP VIEW s` then
   **succeeds** — the regression the guard exists for.
2. `hidden_segment_select_rejected` (`view_chain.rs`): assert a bare
   `SELECT * FROM __h{view_id}_0` through the planner errors; then assert the
   same name still resolves through the raw `client.resolve_table_or_view_id`
   (the trusted surface is deliberately unguarded).
3. `hidden_segment_write_target_rejected` (`view_chain.rs`): assert
   `INSERT INTO __h{view_id}_0 VALUES (…)` and `CREATE INDEX i ON __h{view_id}_0 (…)`
   both error with the **reserved-prefix** plan error (not "is a view"), and
   assert `INSERT INTO __h999999_0 VALUES (…)` (a non-existent hidden name) yields
   the **same** reserved-prefix error — the existence side-channel is closed.
4. `reserved_prefix_cte_and_derived_alias_rejected` (`planner_cte.rs`): over a
   real base table `t(id …)`, assert
   `CREATE VIEW … AS WITH _cte AS (SELECT id FROM t WHERE id > 0) SELECT id FROM _cte`
   and `CREATE VIEW … AS SELECT x FROM (SELECT id AS x FROM t) AS __h0_0` each
   error with the reserved-prefix plan error — the name is rejected at definition,
   before body compilation (a valid body isolates the alias as the cause).
5. `drop_index_reserved_name_rejected` (`planner_create_index.rs`): assert
   `DROP INDEX "__invalid"` and `DROP INDEX "x__fk_y"` error with the reserved
   index-name plan error before any server round-trip.

Python E2E (`crates/gnitz-py/tests/test_cte_chain.py`):

6. `test_reference_to_hidden_segment_rejected`: same shape end-to-end — create
   the chain view, attempt `CREATE VIEW v2 AS SELECT * FROM __h{vid}_0` (expect
   an exception), then `DROP VIEW` the owner and assert it succeeds and both
   VIEW_TAB rows are gone (owner unresolvable).

Existing coverage that must stay green untouched: the `view_chain.rs` drop
cascade / drop-schema drain tests (raw-client hidden introspection), the
`planner_create_index` drop-by-name tests (auto-generated names pass
`validate_user_index_name`), and `validate.rs`'s unit pin of
`validate_user_name("__h5_0") → Err`.

## Sequencing

- [ ] 1. §1 read guard + §2 write guard in `resolve.rs`; tests 1–3; `make verify` green.
- [ ] 2. §3 CTE/derived alias validation + §4 DROP INDEX validation; tests 4–5; `make verify` green.
- [ ] 3. `make e2e` green (W=4), test 6 included.
