# Catalog DDL: production DROP SCHEMA member cascade

## Goal

Close a reachable catalog-integrity defect: **production `DROP SCHEMA` orphans
every member.** `GnitzClient::drop_schema` (`gnitz-core/src/client.rs:470`) scans
`SCHEMA_TAB`, resolves the schema id, and pushes a single `SCHEMA_TAB -1`; it never
retracts the schema's member tables/views (or, transitively, their
columns/indices/deps/circuit rows). The schema becomes un-restorable and its
on-disk storage is wiped while the catalog rows — and the stale in-memory caches
that point at them — survive.

The root cause is that the catalog has **no transaction spanning multiple entities
or multiple client RPCs** (§1). The fix is two symmetric, defense-in-depth changes,
both inside the established catalog DDL machinery — exactly where CREATE TABLE /
CREATE INDEX enforcement already lives:

- an **engine guard** in the sys-ingest precheck that makes a non-empty schema drop
  impossible regardless of caller (§3a), and
- a **client-side member cascade** in `drop_schema` that drops members first so a
  normal drop succeeds with PostgreSQL `DROP SCHEMA … CASCADE` semantics (§3b).

> File:line anchors below are current as of this audit; the catalog `store.rs`
> god-file was carved into `write_path.rs` + section files, so validate against
> source before trusting any number.

## 1. Root cause: lifecycle binding only where one row triggers one hook

Production DDL is system-table delta rows applied engine-side: every
`conn.push(<FAMILY>, …)` is a **separate, fully-committed RPC** that lands in
`CatalogDeltaSink::submit` (`write_path.rs:35`) → `precheck_sys_ingest`
(`write_path.rs:219`, which rejects *before* any WAL write) → `apply_local`
(`write_path.rs:112`, applies the rows and fires the catalog hooks via
`fire_hooks`). The atomicity unit is a single `submit` batch; no transaction wraps
a multi-push client operation or a multi-entity relationship.

A dependent catalog object's lifecycle is therefore bound to its owner **only
where the owner's own `±1` row fires a catalog hook that cascades the dependent.**
Where that holds the boundary is safe; the hole is the one edge it does not cover:

| Owner → dependent | Drop (production) | Mechanism |
|---|---|---|
| table → columns | cascades | `cascade_retract_columns` (`hooks.rs:323`) |
| table → indices / `__fk_` | cascades | `cascade_retract_indices` (`hooks.rs:302`) |
| table → FK children | blocks (RESTRICT) | FK-child precheck (`write_path.rs:426`) |
| view → circuit/dep/col rows | cascades | `cascade_retract_circuit_and_deps` (`hooks.rs:445`) |
| view → dependent views | blocks (RESTRICT) | view-dep precheck (`write_path.rs:453`) |
| **schema → members** | **❌ orphans** | `hook_schema_dir` queues only the dir (`hooks.rs:99`) |

The four cascade hooks all retract via **re-entrant `self.submit`** (the same
`precheck → apply_local → fire_hooks → broadcast` pipeline), so a cascaded
retraction recursively fires its own per-entity hooks. `cascade_retract_indices`
additionally wraps its loop in `with_cascade_drop` (`apply_context.rs:111`) to
suppress the IDX_TAB FK-target guard for legitimate owner-drop retractions — i.e.
**re-entrant submit and per-cascade guard suppression already exist in the catalog**
(relevant to the alternative weighed in §3).

A schema-member cascade exists only as a `#[cfg(test)]` helper (`drop_schema` →
`collect_schema_members` + `drain_drop_targets`, `ddl.rs:37`/`:74`/`:96`); it is
never compiled into production, so the production `GnitzClient::drop_schema` path
has no cascade at all.

## 2. The defect (empirically reproduced)

A `SCHEMA_TAB -1` fires three things, in this order, inside `fire_hooks`
(`hooks.rs:51-55`): `apply_schema_by_name` and `apply_schema_by_id` remove the
name↔id cache entries, then `hook_schema_dir` (`hooks.rs:99`) queues
`<base>/<schema>` for checkpoint-gated deletion. (Correction to a prior reading:
the name↔id removal lives in the two appliers, **not** in `hook_schema_dir` — the
hook is purely the directory-deletion queue.) None of the three retracts member
rows, and the drop section of `precheck_sys_ingest` has **no `SCHEMA_TAB` arm** —
its FK-child and view-dependency guards run only for `TABLE_TAB`/`VIEW_TAB`
(`write_path.rs:426-470`), so a non-empty schema drop is neither rejected nor
cascaded; the schema id falls straight through `drop_ids` to the final `Ok(())`
(`write_path.rs:471`). The comment just above the view-dep guard
(`write_path.rs:444-452`) even *assumes* the opposite — "A schema's own members
were already dropped … by the drop_schema cascade before this row is emitted" — an
invariant true only for the `#[cfg(test)]` cascade, never in production.

Reproduced (production `GnitzClient` path):

```
create_schema dsx (sid=4); create_table dsx.mt (tid=16)
drop_schema('dsx')        -> Ok          (no cascade; the mt row orphans under sid=4)
create_schema dsx         -> Ok, new sid=5            (name freed)
create_table dsx.mt       -> ERROR "Table or view already exists: dsx.mt"
```

The lockout is exact: the orphan leaves `entity_by_qname["dsx.mt"] = 16` in place
(no `TABLE_TAB -1` ever removes it), so when the re-created table reaches
`precheck_qname_unique` (`write_path.rs:176-186`) the stale entry trips
`"Table or view already exists: dsx.mt"` (`write_path.rs:182`).

Consequences: (1) the member `TABLE_TAB`/`VIEW_TAB` rows and all their sub-objects
orphan, alongside the stale `entity_by_qname` / `tables_by_schema` / `entity_by_id`
/ `schema_of` cache entries pointing at a tid in a now-deleted schema; (2) the
schema is un-restorable to its prior shape (the lockout above); (3) the
checkpoint-gated `remove_dir_all` of `<base>/<schema>` wipes member storage while
the rows and `dag.tables` handles persist — a disk/catalog divergence that
`gc_orphan_directories` (`write_path.rs:570`) does not reconcile: it removes only
*directories* (never catalog rows) and scans only `schema_by_id.values()`, from
which the dropped schema is already absent. Reachable via `py`
(`gnitz-py/src/lib.rs:1386`) / C-API (`gnitz-capi/src/lib.rs:600`) `drop_schema`,
which call `GnitzClient::drop_schema` directly; **not** via SQL — `execute_drop`
(`gnitz-sql/src/planner.rs:566`) supports only Table/View/Index and returns
`Unsupported` for `ObjectType::Schema`, so there is no SQL `DROP SCHEMA`.

## 3. Fix: production DROP SCHEMA member cascade

Two changes, defense-in-depth: an **engine guard** makes orphaning impossible, and
a **client cascade** makes a normal drop succeed. Both stay inside the existing
catalog DDL path — the guard in the sys-ingest precheck (engine-side, caller-
agnostic), the cascade as ordinary member-drop RPCs whose `±1` rows fire the same
per-member hooks a standalone DROP TABLE / DROP VIEW already fires.

### 3a. Engine guard: reject `SCHEMA_TAB -1` on a non-empty schema

The enforcement is engine-side, in the **same sys-ingest precheck path
(`submit` → `precheck_sys_ingest`, before any WAL write) that already validates
every CREATE TABLE / CREATE VIEW / CREATE INDEX delta** — so no caller (SQL,
`py`, C-API, or a raw `conn.push`) can bypass it. Master is the single catalog
authority that runs `precheck_sys_ingest`; workers receive applied DDL via
`ddl_sync` and never re-precheck, so the guard's one enforcement point is exactly
right. Add a `SCHEMA_TAB_ID` arm at the **top of the drop section** of
`precheck_sys_ingest` (right after `drop_ids` is sorted/deduped,
`write_path.rs:362`), keyed by schema id against the per-schema member caches
(`tables_by_schema` / `views_by_schema`, `cache.rs:16-17`, which are maintained in
production by `apply_schema_of`):

```rust
// First arm of the DROP section, right after drop_ids dedup.
if table_id == SCHEMA_TAB_ID {
    for &sid in &drop_ids {
        let n = self.schema_member_count(sid);
        if n > 0 {
            return Err(format!(
                "Schema not empty: {n} relation(s) remain; drop them first"));
        }
    }
    return Ok(());
}
```

Factor the cache probe into one production helper (alongside `schema_is_empty` in
`registry.rs`) so the guard and the test helper share it instead of duplicating the
two-map probe:

```rust
// registry.rs — production (not #[cfg(test)]).
pub(crate) fn schema_member_count(&self, sid: i64) -> usize {
    self.caches.tables_by_schema.get(&sid).map_or(0, |s| s.len())
        + self.caches.views_by_schema.get(&sid).map_or(0, |s| s.len())
}

// registry.rs:136 — schema_is_empty refactored to delegate, staying #[cfg(test)].
#[cfg(test)]
pub(crate) fn schema_is_empty(&self, schema_name: &str) -> bool {
    match self.caches.schema_by_name.get(schema_name) {
        Some(&sid) => self.schema_member_count(sid) == 0,
        None => true,
    }
}
```

This converts the silent orphan into a loud error, atomically: the precheck runs
before any WAL write, so a rejected non-empty drop queues no dir deletion and
retracts no rows. `tables_by_schema`/`views_by_schema` are `FxHashMap<i64,
FxHashSet<i64>>` keyed by *schema* id, and `apply_schema_of` removes a set once it
empties, so `schema_member_count` returns 0 exactly when no member remains.

**No cascade exemption is needed** (unlike the adjacent IDX_TAB guard's
`if self.ctx.in_cascade_drop()` short-circuit): a `SCHEMA_TAB -1` is never submitted
from inside an engine cascade. Both the production client cascade (§3b) and the
`#[cfg(test)]` cascade drop every member as prior, separate submissions, so by the
time the schema row reaches precheck the member caches for that sid are already
empty and the guard passes.

Update the `write_path.rs:444-452` comment: keep the schema-id-vs-table-id
collision rationale for why a schema drop is *not* dep-probed, and replace the
"members already dropped by the cascade … needs no guard" tail with a pointer to
the new member-count arm — the property is now **enforced** there, not assumed:

```rust
// The view-dependency guard applies only to TABLE/VIEW drops, never to a
// SCHEMA_TAB drop. A schema drop is gated separately by the member-count arm
// at the top of this section; it must not be dep-probed here, because schema
// ids (from FIRST_USER_SCHEMA_ID = 3) share an i64 space with table ids (from
// FIRST_USER_TABLE_ID = 16), so probing the table-keyed dep_map with a schema
// id would spuriously match an unrelated table's dependents.
```

Replay is unaffected: the client cascade (§3b) guarantees member retractions
precede the schema row, so a faithful WAL never contains a non-empty-schema
retraction. (gnitz is pre-alpha; there is no pre-fix persisted data to recover.)

### 3b. Client cascade: sweep members in `GnitzClient::drop_schema`

Replace the body of `GnitzClient::drop_schema` (`gnitz-core/src/client.rs:470`) so
it drops members before retiring the schema row, reusing the existing wire drops
(`drop_view` `client.rs:791`, `drop_table` `client.rs:542`). Each broadcasts a
member `±1` delta that fires the per-member engine hooks (columns / indices /
circuit + dir teardown) through the very `submit` path above — i.e. the member
teardown is itself pure catalog/hooks work; only the *enumeration and drop
sequencing* is client-side. Each `conn.push` is synchronous and fully committed
(the durability contract: ACK implies applied), so every member retraction is
applied on master — and reflected in master's member caches — before the next RPC,
and before the final `SCHEMA_TAB -1`.

No client-side member-enumeration helper exists today; `find_table_record`
(`client.rs:944`) and `find_view_record` (`client.rs:966`) each filter
`(schema_id, name)` to one row. Add a sibling that drops the name filter and keeps
the whole set — both `TABLE_TAB` and `VIEW_TAB` carry `schema_id` at column 1 and
the entity name at column 2:

```rust
// Collect the entity names of every live row in a TABLE_TAB/VIEW_TAB batch
// whose schema_id column matches. Mirrors find_table_record's scan, minus the
// name filter.
fn collect_schema_member_names(batch: &ZSetBatch, schema_id: u64)
    -> Result<Vec<String>, ClientError>
{
    let mut out = Vec::new();
    for i in 0..batch.len() {
        if batch.weights[i] <= 0 { continue; }
        if col_u64(&batch.columns[1], i)? != schema_id { continue; }
        if let Some(name) = col_str(&batch.columns[2], i)? {
            out.push(name.to_string());
        }
    }
    Ok(out)
}
```

The drop loop is a **retry-until-stable drain** — the client-side analog of
`drain_drop_targets` (`ddl.rs:96`): each pass attempts every remaining member;
any that fails is requeued; stop when the queue empties (success) or a full pass
makes no progress (return the last error). It requeues on **any** `Err`, not just a
dependency error: `ClientError` collapses every engine precheck rejection into
`ServerError(String)` with no structured dependency variant (`error.rs:4-10`,
`connection.rs:41`), so progress — not error-string-matching — is the correct,
robust termination signal. Convergence is guaranteed because every member is being
dropped: leaves succeed first and unblock their parents.

```rust
fn drain_drops<F>(&mut self, targets: Vec<String>, mut drop_one: F)
    -> Result<(), ClientError>
where F: FnMut(&mut Self, &str) -> Result<(), ClientError>,
{
    let mut pending = targets;
    while !pending.is_empty() {
        let before = pending.len();
        let mut retry = Vec::new();
        let mut last_err = None;
        for name in std::mem::take(&mut pending) {
            if let Err(e) = drop_one(self, &name) { last_err = Some(e); retry.push(name); }
        }
        if retry.len() == before {
            return Err(last_err.expect("a non-empty no-progress pass recorded an error"));
        }
        pending = retry;
    }
    Ok(())
}

pub fn drop_schema(&mut self, name: &str) -> Result<(), ClientError> {
    let (_, sdata, _) = self.conn.scan(SCHEMA_TAB, &mut self.schema_cache)?;
    let sdata = sdata.ok_or_else(|| ClientError::ServerError(format!("Schema '{name}' not found")))?;
    let schema_id = find_schema_id(&sdata, name)?;

    // Drop views before tables (a view may read a member table). The drain
    // handles intra-schema view-on-view and FK chains by retrying.
    let (_, vdata, _) = self.conn.scan(VIEW_TAB, &mut self.schema_cache)?;
    let views = vdata.map_or(Ok(Vec::new()), |b| collect_schema_member_names(&b, schema_id))?;
    self.drain_drops(views, |c, m| c.drop_view(name, m))?;

    let (_, tdata, _) = self.conn.scan(TABLE_TAB, &mut self.schema_cache)?;
    let tables = tdata.map_or(Ok(Vec::new()), |b| collect_schema_member_names(&b, schema_id))?;
    self.drain_drops(tables, |c, m| c.drop_table(name, m))?;

    // Schema now empty; the §3a guard accepts this row.
    let schema = schema_tab_schema();
    let mut batch = ZSetBatch::new(schema);
    BatchAppender::new(&mut batch, schema).add_row(schema_id as u128, -1).str_val(name);
    self.conn.push(SCHEMA_TAB, schema, &batch, &mut self.schema_cache)?;
    Ok(())
}
```

This re-scans `TABLE_TAB`/`VIEW_TAB` per `drop_*` call (each resolves its own
record), so the drain costs O(passes × members) catalog scans — accepted: schema
drop is rare and the catalog is small, and the simplicity of reusing the proven
`drop_table`/`drop_view` paths outweighs the round-trips.

**Semantics — RESTRICT on external dependents.** If a member is referenced from
*outside* the schema (a cross-schema FK child or view-on-view), its drop stays
blocked by the existing precheck (`write_path.rs:426`, `:453`; `dep_map` is global
by table id, so cross-schema dependents block correctly); the drain makes no
progress and `drop_schema` returns that error. Already-dropped leaves stay dropped,
but the still-referenced member and the `SCHEMA_TAB` row are never retracted, so no
orphan results and the error is clear. This is the correct default; cross-schema
CASCADE is a non-goal (§5).

**Alternative considered — engine-hook cascade on `SCHEMA_TAB -1`.** Submitting the
member retractions from inside the schema-drop path (mirroring `cascade_retract_*`)
is feasible — re-entrant submit and per-cascade guard suppression already exist
(§1) — but is rejected for this case on three grounds, none of which the client
cascade incurs:

1. **Hook firing order.** `apply_schema_by_name`/`apply_schema_by_id` run *before*
   `hook_schema_dir` within `fire_hooks`, so by hook time the schema is already
   gone from `schema_by_id`/`schema_by_name`; there is no clean point inside the
   schema submit to enumerate and drop members *before* the schema row, short of
   special-casing the generic `submit`.
2. **Member-ordering suppression.** The existing `cascade_drop` flag suppresses only
   the IDX_TAB guard; an engine cascade dropping members in arbitrary order would
   need a *new* schema-scoped suppression of the TABLE-FK and VIEW-dep guards, or an
   in-hook topo-sort/retry — net-new machinery.
3. **No bypass to defend.** Every production schema drop funnels through
   `GnitzClient::drop_schema` (SQL has no DROP SCHEMA; `py`/C-API bind to the client
   method), so the client chokepoint plus the §3a engine guard already covers every
   reachable path.

## 4. Testing

- **DROP SCHEMA cascades (wire path).** Strengthen `test_drop_nonempty_schema_cascades`
  (`gnitz-py/tests/test_catalog.py:46`): today it drops a non-empty schema and only
  checks that the *schema name* frees up (it re-creates the schema, never the
  member) — which passes *even with the orphan bug*. After `drop_schema`, assert the
  **member is gone**: re-create the *same* `schema.table` and have it succeed
  (and/or scan `TABLE_TAB` for no surviving row). Add a view + a secondary index +
  an intra-schema view-on-view so the drain's dependency-ordered retry is exercised
  (views-before-tables plus view-on-view across passes).
- **Guard rejects manual non-empty drop (Rust engine test, `catalog/tests/`).** Once
  §3b lands, the `py` `drop_schema` always empties the schema first, so the bare
  guard cannot be hit from `py`. Cover it engine-side: with a member present, submit
  a `SCHEMA_TAB -1` directly and assert `Err` "Schema not empty"; assert nothing was
  orphaned (member row + caches intact, no dir queued).
- **RESTRICT on external dependents (wire path).** `drop_schema(s1)` where `s2.v`
  reads `s1.t` errors and leaves the still-referenced member (and the `s1` schema
  row) intact; `s2.v` is untouched.
- The engine's `#[cfg(test)]` `drop_schema` cascade tests (`ddl_tests.rs`,
  `atomicity_tests.rs`, `dir_deletion_tests.rs`) stay green — they empty the schema
  before emitting `SCHEMA_TAB -1`, so the new §3a guard passes — but are no longer
  the only coverage of member-cascade behavior.

## 5. Scope / non-goals

- **No general multi-entity DDL transaction.** The defect is closed at its
  production chokepoint (client cascade + engine guard). A real cross-RPC
  transaction would subsume it but is a far larger change, unwarranted for this one
  reachable hole. Consequence: a cascade that blocks partway (RESTRICT on an
  external dependent) leaves already-dropped leaves dropped — acceptable, since no
  orphan results and the schema row itself is never retracted.
- **No SQL `DROP SCHEMA`.** The bug is reachable only via `py`/C-API; this plan
  fixes those without adding a planner `DROP SCHEMA` arm. Adding SQL DDL for schemas
  is independent.
- **No cross-schema CASCADE.** External dependents block the drop (RESTRICT);
  recursively dropping across schemas is not implemented.
