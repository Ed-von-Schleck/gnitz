# Catalog DDL: production DROP SCHEMA member cascade

## Goal

Close a reachable catalog-integrity defect: **production `DROP SCHEMA` orphans
every member.** `GnitzClient::drop_schema` (`client.rs:470-484`) emits a single
`SCHEMA_TAB -1` and never retracts the schema's member tables/views (or,
transitively, their columns/indices/deps/circuit rows). The schema becomes
un-restorable and its on-disk storage is wiped while the catalog rows survive.

The root cause is that the catalog has **no transaction spanning multiple entities
or multiple client RPCs** (§1). The fix is two symmetric, defense-in-depth changes,
both inside the established catalog DDL machinery — exactly where CREATE TABLE /
CREATE INDEX enforcement already lives:

- an **engine guard** in the sys-ingest precheck that makes a non-empty schema drop
  impossible regardless of caller (§3a), and
- a **client-side member cascade** in `drop_schema` that drops members first so a
  normal drop succeeds (§3b).

## 1. Root cause: lifecycle binding only where one row triggers one hook

Production DDL is system-table delta rows applied engine-side: every
`conn.push(<FAMILY>, …)` is a **separate, fully-committed RPC** that lands in
`CatalogStore::submit` (`store.rs:48`) → `precheck_sys_ingest` (`store.rs:57`,
which rejects *before* any WAL write) → `apply_local` (applies the rows and fires
the catalog hooks). The atomicity unit is a single `ingest_to_family` batch; no
transaction wraps a multi-push client operation or a multi-entity relationship.

A dependent catalog object's lifecycle is therefore bound to its owner **only
where the owner's own `±1` row fires a catalog hook that cascades the dependent.**
Where that holds the boundary is safe; the hole is the one edge it does not cover:

| Owner → dependent | Drop (production) | Mechanism |
|---|---|---|
| table → columns | cascades | `cascade_retract_columns` (`hooks.rs:270`) |
| table → indices / `__fk_` | cascades | `cascade_retract_indices` (`hooks.rs:249`) |
| table → FK children | blocks (RESTRICT) | integrity precheck (`store.rs:439`) |
| view → circuit/dep/col rows | cascades | `cascade_retract_circuit_and_deps` (`hooks.rs:378`) |
| view → dependent views | blocks (RESTRICT) | view-dep precheck (`store.rs:466`) |
| **schema → members** | **❌ orphans** | `hook_schema_dir` deletes only the dir (`hooks.rs:99`) |

A schema-member cascade exists only as a `#[cfg(test)]` helper (`drop_schema` →
`collect_schema_members` + `drain_drop_targets`, `ddl.rs:37`/`:74`/`:97`); it is
never compiled into production, so the production `GnitzClient::drop_schema` path
has no cascade at all.

## 2. The defect (empirically reproduced)

`hook_schema_dir` (`hooks.rs:99`) is the only side effect of `SCHEMA_TAB -1`: it
queues `<base>/<schema>` for checkpoint-gated deletion and removes the name↔id
cache entries. It does **not** retract member rows, and the drop section of
`precheck_sys_ingest` has no `SCHEMA_TAB` arm — its FK-child and view-dependency
guards run only for `TABLE_TAB`/`VIEW_TAB` (`store.rs:439-483`), so a non-empty
schema drop is neither rejected nor cascaded. The comment there (`store.rs:457-465`)
even *assumes* the opposite — "A schema's own members were already dropped … by the
drop_schema cascade before this row is emitted" — an invariant true only for the
`#[cfg(test)]` cascade, never in production.

Reproduced (production `GnitzClient` path):

```
create_schema dsx (sid=4); create_table dsx.mt (tid=16)
drop_schema('dsx')        -> Ok          (no cascade; the mt row orphans under sid=4)
create_schema dsx         -> Ok, new sid=5            (name freed)
create_table dsx.mt       -> ERROR "Table or view already exists: dsx.mt"
```

Consequences: (1) member `TABLE_TAB`/`VIEW_TAB` rows and all their sub-objects
orphan; (2) the schema is un-restorable to its prior shape (the lockout above);
(3) the checkpoint-gated `remove_dir_all` of `<base>/<schema>` wipes member storage
while the rows and `dag.tables` handles persist — a disk/catalog divergence that
`gc_orphan_directories` (`store.rs:583`) does not reconcile because it skips schemas
absent from `schema_by_id`. Reachable via `py`/C-API `drop_schema` (which call
`GnitzClient::drop_schema` directly); **not** via SQL — `execute_drop` supports only
Table/View/Index (`planner.rs:527`), so there is no SQL `DROP SCHEMA`.

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
`py`, C-API, or a raw `conn.push`) can bypass it. Add a `SCHEMA_TAB_ID` arm to the
**drop section** of `precheck_sys_ingest` (`store.rs:364-484`), beside the existing
TABLE/VIEW guards. `drop_ids` (`store.rs:365-375`) already collects every retracted
PK generically, so a `SCHEMA_TAB -1` row's schema id is present there. Key the
check by schema id against the per-schema member caches (`tables_by_schema` /
`views_by_schema`, `cache.rs:16-17`):

```rust
// Drop section, beside the TABLE/VIEW FK and view-dependency guards.
// The schema-id-vs-table-id space collision that bars a dep_map probe (see the
// comment above the view-dep guard) does not apply here: the member caches are
// keyed by *schema* id, so this never aliases an unrelated table's dependents.
if table_id == SCHEMA_TAB_ID {
    for &sid in &drop_ids {
        let n = self.caches.tables_by_schema.get(&sid).map_or(0, |s| s.len())
              + self.caches.views_by_schema.get(&sid).map_or(0, |s| s.len());
        if n > 0 {
            return Err(format!(
                "Schema not empty: {n} relation(s) remain; drop them first"));
        }
    }
}
```

This converts the silent orphan into a loud error. The `#[cfg(test)]`
`schema_is_empty` helper (`store.rs:1700-1709`) already uses this exact cache
probe; the guard is its production counterpart.

Update the `store.rs:457-465` comment: the "members already dropped before this
row" property is now **enforced** by this arm, not assumed.

Replay is unaffected: the client cascade (§3b) guarantees member retractions
precede the schema row, so a faithful WAL never contains a non-empty-schema
retraction. (gnitz is pre-alpha; there is no pre-fix persisted data to recover.)

### 3b. Client cascade: sweep members in `GnitzClient::drop_schema`

Replace the body of `GnitzClient::drop_schema` (`client.rs:470-484`) so it drops
members before retiring the schema row, reusing the existing wire drops
(`drop_view` `client.rs:789`, `drop_table` `client.rs:540`). Each of those
broadcasts a member `±1` delta that fires the per-member engine hooks (columns /
indices / circuit + dir teardown) through the very `submit` path above — i.e. the
member teardown is itself pure catalog/hooks work; only the *enumeration and drop
sequencing* is client-side. Structure (mirrors the `#[cfg(test)]` cascade,
`ddl.rs:37`, but over the wire):

1. Resolve `sid` (as today, via `find_schema_id`, `client.rs:914`).
2. Enumerate members: scan `VIEW_TAB` and `TABLE_TAB`, collect `(schema, name)`
   for rows with this `schema_id` (reuse the `find_table_record` scan shape,
   `client.rs:924`).
3. Drop members **retry-until-stable** — the client-side analog of
   `drain_drop_targets` (`ddl.rs:97`): each pass attempts every remaining member;
   a member whose drop returns a dependency error (view-on-view, FK chain — another
   member not yet dropped) is requeued; stop when the queue empties (success) or a
   full pass makes no progress (return the last error). Drop views before tables to
   reduce passes. No global sort is needed: every member is being dropped, so the
   leaves succeed first and the loop converges.
4. Push `SCHEMA_TAB -1` (now guaranteed to pass §3a).

**Semantics — RESTRICT on external dependents.** If a member is referenced from
*outside* the schema (a cross-schema FK child or view-on-view), its drop stays
blocked by the existing precheck (`store.rs:439`, `:466`); the retry loop makes no
progress and `drop_schema` returns that error. No orphan is created and the error
is clear. This is the correct default; cross-schema CASCADE is a non-goal (§5).

**Alternative considered — engine-hook cascade on `SCHEMA_TAB -1`.** Submitting the
member retractions from inside `hook_schema_dir` (mirroring `cascade_retract_*`)
would put the enumeration in the hook too, but adds reentrant-submit, ordering, and
guard-suppression complexity for no production benefit: every production schema drop
already funnels through `GnitzClient::drop_schema` (SQL has no DROP SCHEMA; `py`/
C-API bind to the client method). The client chokepoint plus the §3a engine guard
covers every reachable path. Rejected on risk/benefit.

## 4. Testing

- **DROP SCHEMA cascades (wire path).** Strengthen `test_drop_nonempty_schema_cascades`
  (`gnitz-py/tests/test_catalog.py:46`): today it drops a non-empty schema and only
  checks that the name frees up — which passes *even with the orphan bug* because it
  never re-creates the member. After `drop_schema`, assert the member is **gone**:
  re-create the *same* `schema.table` and have it succeed (and/or scan `TABLE_TAB`
  for no surviving row). Add a view + a secondary index + a multi-table
  view-on-view to exercise the dependency-ordered retry.
- **Guard rejects manual non-empty drop.** A `SCHEMA_TAB -1` pushed while a member
  exists (bypassing the client cascade) returns "Schema not empty"; nothing is
  orphaned.
- **RESTRICT on external dependents.** `drop_schema(s1)` where `s2.v` reads `s1.t`
  errors and leaves the still-referenced member (and the schema row) intact.
- The engine's `#[cfg(test)]` `drop_schema` cascade tests (`ddl_tests.rs`) stay
  green but are no longer the only coverage of member-cascade behavior.

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
