# Catalog DDL: production DROP SCHEMA cascade & compound-create compensation

## Goal

Close two reachable catalog-integrity defects that share one root cause — **the
catalog has no transaction spanning multiple entities or multiple client RPCs**:

- **Production `DROP SCHEMA` orphans every member.** `GnitzClient::drop_schema`
  (`client.rs:470-484`) emits a single `SCHEMA_TAB -1`; the engine's schema hook
  deletes only the schema directory and never retracts member tables/views (or,
  transitively, their columns/indices/deps/circuit rows). The schema becomes
  un-restorable and its on-disk storage is wiped while the catalog rows survive.
- **Compound creates orphan on partial failure → name lockout.** A `CREATE VIEW`
  that builds internal helpers across several RPCs before its own registration
  (today: pure-range LEFT's `__<v>__m` / `__<v>__sent`) leaves the helpers
  committed if a later step fails, with stable names that block any retry.

Both are instances of the same gap, and the fixes are symmetric: a **drop-side
member cascade** at the production chokepoint plus an **engine guard**, and a
**create-side compensation** that retracts already-committed pieces on failure.

This plan does **not** re-cover the `__m`/`__sent` surface hardening (reserved
`_`-prefix, `__sent` deletion, the owned-entity *drop* cascade that closes the
programmatic-`drop_view` leak) — that is owned by
`plans/pure-range-left-internal-helper-isolation.md`. See §5 for the exact
boundary.

## 1. Root cause: lifecycle binding only where one row triggers one hook

Production DDL is system-table delta rows pushed over the wire; each
`conn.push(<FAMILY>, …)` is a **separate, fully-committed RPC** (ingest +
broadcast + fsync + ACK). The atomicity unit is a single `ingest_to_family`
batch: a Stage-A failure runs `compensate_stage_a` (`store.rs:696`) over **that
one batch only**. No transaction wraps a multi-push client operation or a
multi-entity relationship.

A dependent catalog object's lifecycle is therefore bound to its owner **only
where the owner's own `±1` row fires an engine hook that cascades the
dependent.** Where that holds, the boundary is safe; the holes are exactly the
edges it does not cover:

| Owner → dependent | Create | Drop (production) | Mechanism |
|---|---|---|---|
| table → columns | 2 RPCs | **cascades** | `cascade_retract_columns` (`hooks.rs:231`) |
| table → indices / `__fk_` | FK idx atomic in TABLE_TAB delta | **cascades** | `cascade_retract_indices` (`hooks.rs:227`) |
| table → FK children | — | **blocks** | integrity precheck (`store.rs:439`) |
| view → circuit/dep/col rows | up to 6 RPCs | **cascades** | `cascade_retract_circuit_and_deps` (`hooks.rs:356`) |
| view → dependent views | — | **blocks** | view-dep precheck (`store.rs:466`) |
| **schema → members** | — | **❌ orphans** | `hook_schema_dir` deletes only the dir (`hooks.rs:99-127`) |
| **view → `__m`/`__sent`** | **❌ orphan window** (helpers built before `v`) | ⚠ planner-only probe (`planner.rs:519-535`) | — |

The two cross-entity cascades that *were* written live at the wrong altitude and
leak on the path that bypasses them: the schema-member cascade
(`collect_schema_members` + `drain_drop_targets`, `ddl.rs:52-54`) is
`#[cfg(test)]` and never compiled into production; the `__m`/`__sent` cascade is
in the SQL planner and skipped by `GnitzClient::drop_view`. This plan fixes the
schema-member edge and the `__m`/`__sent` *create* edge; the helper-isolation
plan fixes the `__m`/`__sent` *drop* edge.

The benign cousins confirm the boundary: a failed `create_table` / `create_view`
also orphans its prior-RPC `COL_TAB` / circuit rows (the engine documents it at
`hooks.rs:228`: "the column records were committed by prior RPCs"), but those sit
under a freshly-allocated, never-reused tid — invisible, no lockout. The harm in
the `__m`/`__sent` case comes specifically from the helpers having **stable,
resolvable names**, not from the non-atomicity alone.

## 2. The two defects (both empirically reproduced)

### 2a. DROP SCHEMA orphans members

`hook_schema_dir` (`hooks.rs:99-127`) is the only side effect of `SCHEMA_TAB -1`
(dispatch at `hooks.rs:51-55`): it queues `<base>/<schema>` for checkpoint-gated
deletion and removes the name↔id cache entries. It does **not** retract member
rows, and `precheck_sys_ingest` has no `SCHEMA_TAB` arm (`store.rs:232-484`), so
a non-empty schema drop is neither rejected nor cascaded. The precheck's own
comment already *assumes* the opposite — "A schema's own members were already
dropped … by the drop_schema cascade before this row is emitted" (`store.rs:463`)
— an invariant true only for the `#[cfg(test)]` cascade.

Reproduced (production `GnitzClient` path):

```
create_schema dsx (sid=4); create_table dsx.mt (tid=16)
drop_schema('dsx')        -> Ok
create_schema dsx         -> Ok, new sid=5            (name freed)
create_table dsx.mt       -> ERROR "Table or view already exists: dsx.mt"
```

Consequences: (1) member `TABLE_TAB`/`VIEW_TAB` rows and all their sub-objects
orphan; (2) the schema is un-restorable to its prior shape (lockout above); (3)
the checkpoint-gated `remove_dir_all` of `<base>/<schema>` wipes member storage
while the rows and `dag.tables` handles persist — a disk/catalog divergence that
`gc_orphan_directories` does not reconcile because it skips schemas absent from
`schema_by_id` (`store.rs:606`). Reachable via `py`/C-API `drop_schema` (which
call `GnitzClient::drop_schema` directly); **not** via SQL — `execute_drop`
supports only Table/View/Index (`planner.rs:506-540`), so there is no SQL
`DROP SCHEMA`.

### 2b. Compound-create orphan → lockout

`build_pure_range_left_aggregate` (`planner.rs:2004`) creates `__<v>__sent` (a
full `create_table` + data `push`) and `__<v>__m` (a full
`create_view_with_circuit`) as independent top-level operations, **before**
`v_left` is registered at the tail of `build_range_join_view`
(`create_view_with_circuit`, `planner.rs:1714`). Six fallible steps run after the
helpers exist; the trivially-reachable one is `reject_duplicate_column_names`
(`planner.rs:1706`), which fires for a duplicate output alias.

Reproduced:

```
CREATE VIEW ov AS SELECT oa.id AS dup, ob.id AS dup
    FROM oa LEFT JOIN ob ON oa.x < ob.y
  -> Plan("duplicate column name 'dup' in range join view")
after: ov absent; __ov__m and __ov__sent committed (orphaned)
retry (fixed aliases) -> Exec("Table or view already exists: s0.__ov__sent")
```

`execute_drop`'s helper cascade cannot recover it: it runs `drop_view(v)` first
(`planner.rs:520`), which errors because `v` never existed, so the `__m`/`__sent`
probes (`planner.rs:528-535`) never run.

## 3. Fix 1 — production DROP SCHEMA member cascade

Two changes, defense-in-depth: an **engine guard** makes orphaning impossible,
and a **client cascade** makes a normal drop succeed.

### 3a. Engine guard: reject `SCHEMA_TAB -1` on a non-empty schema

Add a `SCHEMA_TAB_ID` arm to `precheck_sys_ingest` (`store.rs:232`), alongside
the existing IDX/TABLE/VIEW arms, enforcing the invariant `store.rs:463` already
claims. The arm is keyed by schema id against the per-schema member sets
(`cache.rs:16-17`), so the schema-id-vs-table-id space collision that bars a
`dep_map` guard (the reason given at `store.rs:456-465`) does not apply:

```rust
if table_id == SCHEMA_TAB_ID {
    for i in 0..batch.count {
        if batch.get_weight(i) >= 0 { continue; }
        let sid = batch.get_pk(i) as i64;
        let n = self.caches.tables_by_schema.get(&sid).map_or(0, |s| s.len())
              + self.caches.views_by_schema.get(&sid).map_or(0, |s| s.len());
        if n > 0 {
            return Err(format!("Schema not empty: {n} relation(s) remain; drop them first"));
        }
    }
}
```

This converts the silent orphan into a loud error and makes the orphan
unreachable regardless of caller. Replay is unaffected: the client cascade (§3b)
guarantees member retractions precede the schema row, so a faithful WAL never
contains a non-empty-schema retraction (and gnitz is pre-alpha — no pre-fix
persisted data to recover).

Update the `store.rs:456-465` comment: the "members already dropped before this
row" property is now **enforced** here, not assumed.

### 3b. Client cascade: sweep members in `GnitzClient::drop_schema`

Replace the body of `GnitzClient::drop_schema` (`client.rs:470-484`) so it drops
members before retiring the schema row, reusing the existing wire drops
(`drop_view` `client.rs:783`, `drop_table` `client.rs:534`) — each already
broadcasts and triggers the per-member engine cascade (columns/indices/circuit +
dir). Structure (mirrors the `#[cfg(test)]` `ddl.rs:37-67` cascade, but over the
wire):

1. Resolve `sid` (as today).
2. Enumerate members: scan `VIEW_TAB` and `TABLE_TAB`, collect `(schema, name)`
   for rows with this `schema_id`. (`drop_table` already scans `TABLE_TAB` and
   parses rows via `find_table_record`, `client.rs:540-545` — reuse that shape.)
3. Drop members **retry-until-stable** — the client-side analog of
   `drain_drop_targets` (`ddl.rs:96-124`): each pass attempts every remaining
   member; a member whose drop returns a dependency error (view-on-view, FK
   chain — another member not yet dropped) is requeued; stop when the queue is
   empty (success) or a full pass makes no progress (return the last error). Drop
   views before tables to reduce passes. Inter-member ordering needs no global
   sort: every member is being dropped, so the leaves succeed first and the loop
   converges.
4. Push `SCHEMA_TAB -1` (now guaranteed to pass §3a).

**Semantics — RESTRICT on external dependents.** If a member is referenced from
*outside* the schema (a cross-schema FK child or view-on-view), its drop stays
blocked by the existing precheck (`store.rs:439`, `:466`); the retry loop makes
no progress and `drop_schema` returns that error with nothing dropped past the
leaves. This is safe (no orphan, clear error) and the correct default; full
cross-schema CASCADE is a non-goal (§7).

**Alternative considered — engine-hook cascade on `SCHEMA_TAB -1`.** Submitting
member retractions from inside the schema hook (mirroring `cascade_retract_*`)
would be caller-agnostic at the engine, but adds reentrant-submit, ordering, and
guard-suppression complexity for no production benefit: every production schema
drop already funnels through `GnitzClient::drop_schema` (SQL has no DROP SCHEMA;
`py`/C-API bind to the client method). The client chokepoint plus the §3a guard
covers every reachable path. Rejected on risk/benefit.

## 4. Fix 2 — compound-create compensation (pure-range LEFT helpers)

The load-bearing fix is compensation; the hoist is an optimization.

### 4a. Compensation (load-bearing)

In `build_range_join_view`, once `build_pure_range_left_aggregate` has returned
the helper ids, **any subsequent `Err` must retract the helpers before
propagating.** Capture `m_view_id` and the `__sent` tid, and on every error path
after the helper build (the `alloc_table_id`, the `multi_null_filter_prog`s,
`reject_duplicate_column_names`, and the final `create_view_with_circuit` for
`v_left`) drop `__m` then `__sent` (child-before-parent, the `execute_drop`
order) via the client wire drops, best-effort, then return the original error.

Implement as a scope guard rather than hand-threading every `?`: build the
helpers, then run the remainder in a closure whose `Err` triggers the cleanup,
e.g.

```rust
let helpers = build_pure_range_left_aggregate(client, schema_name, view_name, …)?;
let result = (|| {
    // … alloc v_left id, build circuit, reject_duplicate_column_names,
    //     create_view_with_circuit(v_left) …
})();
if result.is_err() {
    // child before parent; ignore cleanup errors, surface the original.
    let _ = client.drop_view(schema_name, &format!("__{view_name}__m"));
    let _ = client.drop_table(schema_name, &format!("__{view_name}__sent"));
}
result
```

This closes the lockout for **all** post-helper failures, not just the dup-alias.

### 4b. Hoist cheap validation above the helper build (optimization)

`reject_duplicate_column_names` (`planner.rs:1706`) is the most-reachable trigger
and depends only on output column *names*. Move `build_join_view_projection` +
`reject_duplicate_column_names` above `build_pure_range_left_aggregate`
(`planner.rs:1436`) so the common typo fails **before** any catalog object is
created, avoiding the create/compensate churn. `build_join_view_projection`
derives `final_cols` from the join column structure, not from `__m`; confirm it
takes no helper output before moving it. If the hoist proves entangled, drop it —
§4a alone is correct.

## 5. Boundary with `pure-range-left-internal-helper-isolation.md`

That plan and this one are disjoint:

- **It owns** the `__m`/`__sent` *steady-state* hardening: deleting `__sent` via
  an identity-on-empty reduce (its Fix A), enforcing the reserved `_`-prefix so
  users cannot resolve/DML/squat the helpers (its Fix B), and the **owned-entity
  *drop* cascade** that retires the planner-side probe and closes the
  programmatic-`drop_view` / crash-window *drop* leaks (its Fix B).
- **This plan owns** the production `DROP SCHEMA` member cascade (its §6
  explicitly defers this as "a separate … gap … with its own fix") and the
  **create-side** compensation, which neither it nor the current code addresses —
  its Fix B's owned cascade fires on *drop of `v`*, never on a *failed create of
  `v`*, and its reserved-prefix check exempts the planner's own internal create,
  so the orphan→lockout survives without §4.

**Coordination:** if helper-isolation's Fix A lands first, `__sent` no longer
exists and §4a's compensation reduces to dropping `__m` only. The two plans touch
the same `build_range_join_view` create sequence; land order is free, but the
later one rebases its helper list onto the surviving objects.

## 6. Testing

- **DROP SCHEMA cascades (wire path).** Replace the false-confidence assertion in
  `test_drop_nonempty_schema_cascades` (`gnitz-py/tests/test_catalog.py:46`):
  after `drop_schema`, assert the member table row is **gone** (re-create the
  same `schema.table` succeeds, and/or a `TABLE_TAB` scan shows no row for it) —
  not merely that the schema name frees up. Add a view + a secondary index + a
  multi-table view-on-view to exercise the dependency-ordered retry.
- **Guard rejects manual non-empty drop.** A `SCHEMA_TAB -1` pushed while a member
  exists (bypassing the client cascade) returns "Schema not empty"; nothing is
  orphaned.
- **RESTRICT on external dependents.** `DROP SCHEMA s1` where `s2.v` reads `s1.t`
  errors and leaves `s1` and its members intact.
- **Create compensation (no lockout).** After a failed pure-range LEFT create
  (duplicate alias), `resolve_table_or_view_id` finds neither `__<v>__m` nor
  `__<v>__sent`, and an immediate corrected `CREATE VIEW <v>` succeeds. Cover
  both the dup-alias path (§4b territory) and a forced `v_left`-registration
  failure (pure §4a) so the compensation, not just the hoist, is exercised.
- The engine's `#[cfg(test)]` `drop_schema` cascade tests (`ddl_tests.rs`) stay
  green but are no longer the only coverage of member-cascade behavior.

## 7. Scope / non-goals

- **No general multi-entity DDL transaction.** Each defect is closed at its
  production chokepoint (client cascade + engine guard for drop; planner
  compensation for create). A real cross-RPC transaction would subsume both but
  is a far larger change and unwarranted to close these two reachable holes.
- **No SQL `DROP SCHEMA`.** The bug is reachable only via `py`/C-API; this plan
  fixes those without adding a planner `DROP SCHEMA` arm. Adding SQL DDL for
  schemas is independent.
- **No cross-schema CASCADE.** External dependents block the drop (RESTRICT);
  recursively dropping across schemas is not implemented.
- **`__m`/`__sent` steady-state hardening** (reserved prefix, `__sent` removal,
  owned-entity drop cascade) is `pure-range-left-internal-helper-isolation.md`,
  not here.
