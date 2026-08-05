# Catalog mutations are verbs: the master derives what it can already see

## Goal

Stop the client from reading the catalog back in order to describe a change the
master can derive itself. Every DROP, every ALTER, and CREATE INDEX become
**verbs** — what to change and how — executed under the catalog write lock the
DDL path already takes. CREATE TABLE / CREATE VIEW keep shipping rows, because
those payloads are built client-side, not read back.

Alongside that, two client-side assertions the engine does not check today move
to the master: the view dependency graph (DEP_TAB) and the DROP COLUMN
FK/SERIAL guards.

## The problem, measured

Traced on a live 4-worker server, per statement (client→server requests, and
reply bytes summed over them):

| Statement | Requests | Shape | Reply bytes |
|---|---|---|---|
| `CREATE TABLE` | 3 | alloc, scan, txn | 1 264 |
| `CREATE INDEX` | 6 | scan ×4, alloc, txn | 12 434 |
| `DROP INDEX` | 2 | scan IDX_TAB, txn | 856 |
| `ALTER … RENAME COLUMN` | 4 | scan ×3, txn | 10 229 |
| `ALTER … DROP COLUMN` | 5 | scan ×3, GET_INDICES, txn | 10 685 |
| `ALTER … RENAME TO` | 3 | scan ×2, txn | 2 939 |
| `CREATE VIEW` | 6 | alloc, scan ×3, GET_INDICES, txn | 10 741 |
| `DROP VIEW` | 3 | scan ×2, txn | 1 842 |
| `DROP TABLE` | 3 | scan ×2, txn | 3 595 |

`DROP SCHEMA … CASCADE` over `n` member tables is exactly **`3n + 4`** requests
— 7, 10, 16, 28, 52 at n = 1, 2, 4, 8, 16 — because each member is dropped by its
own `drop_table` RPC that re-scans the catalog first. **Reachable size:** over
every cascade a full E2E run performs, the mean member count is **0.73** and the
maximum is **9**. So the cascade's value here is not the `3n+4 → 1` request
collapse at large `n` (nothing in the tree reaches large `n`); it is that each
member's drop is its own durable commit — barrier, tick quiesce, W-worker
broadcast, `fdatasync` — and the whole cascade collapses to one.

The asymmetry that matters: over a full run, CREATE-side statements contribute
roughly a sixth of the DDL path's catalog scans and DROP/ALTER-side statements
the rest. **CREATE's cost is allocations and the commit; DROP/ALTER's cost is
reading the catalog back** — which is exactly what this plan removes.

## The principle

**A retraction row carries zero information.** `check_cas_and_net`
(`crates/gnitz-engine/src/catalog/write_path.rs:242-268`) requires, for every
`-1` in a system-family batch, that a live row exist at that PK *and* that the
retracted row content-equal it:

```rust
let Some((lb, _)) = live.as_ref() else {
    return Err(format!("catalog changed concurrently: retracting a {noun} that no longer exists"));
};
...
if batch.get_pk(j) == sig.pk
    && batch.get_weight(j) < 0
    && compare_rows(&schema, lb, 0, batch, j) != Ordering::Equal
{
    return Err(format!("catalog changed concurrently: the retracted {noun} differs from the current one"));
}
```

So the client scans whole system tables, decodes rows, re-encodes them verbatim,
and ships them — and the master compares them away against the copy it already
had. Only the **PK** survives.

The decoded payload is not even interpretable by the client. `TableRecord` /
`ViewRecord` (`client.rs:129-147`) carry `directory`, `created_lsn`,
`cache_directory`, and `sql_definition`; across all of `gnitz-core` and
`gnitz-sql` those four fields have exactly five consumers — `append_view_row`
(`client.rs:1861-1863`) and `append_table_tab_row` (`:1875-1877`), i.e.
re-encoding them into the row being shipped back.

**An ALTER's `+1` row is a verb too**, and the engine already decodes it back
into one. `precheck_retraction_contract` (`write_path.rs:289-325`) permits a
relation-family pair to differ from the live row only in `name`, and
`precheck_column_family` (`write_path.rs:385-435`) diffs the pair to work out
which of `{name, is_hidden, is_nullable}` moved, then enforces the direction:

```rust
if hid_new != hid_old && !(hid_old == 0 && hid_new == 1) {
    return Err("a column-ALTER may only set is_hidden 0→1 (DROP COLUMN)".into());
}
```

The client encodes a verb as a pair of rows; the master decodes the pair back
into a verb.

**The master already derives rows in production.** `retract_single_row`
(`catalog/utils.rs:257`) and `retract_key_range` (`:235`) author byte-exact `-1`
batches from the live store, used at `hooks.rs:403` (index cascade), `:418`
(column cascade), and `:607` (circuit/dep key-range cascade). So "retract one
IDX_TAB row" has two implementations in tree: one line server-side inside the
DROP TABLE cascade, and 41 lines plus a whole IDX_TAB scan client-side for the
standalone `DROP INDEX` (`client.rs:631-671`).

The generic entry point is written and reachable only from tests:

```rust
/// Only the test-only direct DDL drop paths (`ddl.rs`) retract engine-side;
/// production retractions arrive as wire deltas.
#[cfg(test)]
pub(crate) fn submit_retraction(&mut self, family: SysFamily, pk: u128) -> Result<(), String> {
```

`catalog/ddl.rs` is 770 lines, of which a complete server-side DDL surface is
`#[cfg(test)]`: `create_schema` (:45), `drop_schema` (:69),
`collect_schema_members` (:105), `drain_drop_targets` (:128), `create_table`
(:168), `drop_table` (:223), `drop_view` (:246), `create_index` (:273),
`drop_index` (:349), `build_col_batch` (:609), `write_column_records` (:630),
`write_view_deps` (:641). The client's own cascade driver names the duplication
outright — *"The client-side analog of the engine's `drain_drop_targets`"*
(`client.rs:825`).

## Design

### 1. A DDL bundle is all rows, or exactly one op

`FLAG_DDL_TXN` encodes a prologue (`client_id`, flags, count) followed by N WAL
blocks, one per system family (`encode_ddl_txn`,
`gnitz-core/src/protocol/message.rs:298-304`; `decode_ddl_txn`,
`runtime/protocol/wire.rs:703-713`). Each item gains a leading **kind byte** —
`0` = family WAL block (exactly as today), `1` = a `CatalogOp` record. There is
precedent in the sibling frame: `decode_push_txn`'s `TxnFamilyWire` already
carries a per-family `mode: u8` (`wire.rs:715-725`). A kind byte costs no flag
bit; the free bits (55 and 63) stay free.

**The decoder rejects a bundle that mixes kinds.** That restriction is not
cosmetic — it is what preserves three batch-local invariants the engine relies
on:

- `family_pks_by_sign`'s own contract: *"**Batch-local**: the whole family (both
  signs of a pair) arrives as one batch on every path"* (`sys_tables.rs:362-363`);
- `view_row_order` (`hooks.rs:492-509`) orders `weight <= 0` before `weight > 0`
  **within one batch**, inside one `fire_hooks`;
- `precheck_qname_unique` (`write_path.rs:194-206`) admits an incumbent only via
  `net_dead`, computed per batch by `precheck_retraction_contract` (`:296-297`).

Split a rename's `-1` from its `+1` across an op item and a row item and all
three become dependent on the client's item ordering. Forbidding mixed bundles
keeps them structural.

The invariant these guards actually need is **one family per submitted batch**,
not one family per op. An op may expand to several families server-side — a
`Retract` on a TABLE_TAB id runs `cascade_retract_indices` (`hooks.rs:369`) and
`cascade_retract_columns` (`:381`); on a VIEW_TAB id it runs
`cascade_retract_circuit_and_deps` over four more (`hooks.rs:592-611`). Each
cascade `submit`s its own family-local, sign-homogeneous batch, so every guard
above still sees exactly the shape it expects. What matters is that no *client*
frame interleaves an op item with a row item of the same family.

No current call site mixes: all eleven `push_ddl` sites in `client.rs` (`:621`,
`:667`, `:810`, `:887`, `:999`, `:1015`, `:1242`, `:1269`, `:1330`, `:1342`,
`:1439`) are one statement each and single-kind, and the two multi-family bundles
— `create_table` (`:999`) and `create_view_chain` (`:1242`) — are all rows and
stay all rows. `alter_col_pair` pushes exactly `[(COL_TAB, …)]`
(`client.rs:1439`); the relation renames push one of TABLE_TAB / VIEW_TAB.

The one shape that genuinely mixes signs across a bundle is `create_view_chain`'s
`replaces` path — ALTER VIEW folds the outgoing view's VIEW_TAB `-1` rows (and
every hidden segment it owns) into the same bundle as the new chain's `+1` rows
(`client.rs:1170-1219`). **ALTER VIEW therefore keeps shipping rows.** That is
the deliberate carve-out; it costs one duplicated hidden-segment enumeration
(client for ALTER VIEW, master for DROP VIEW) and buys the invariant above.

### 2. The op set

The op lives in `gnitz-wire`, so it names a family by its **tid** — the existing
`SCHEMA_TAB` = 1 … `DEP_TAB` = 6 constants (`gnitz-wire/src/catalog.rs:191-196`)
— not by the engine's `SysFamily`, which is `pub(crate)` inside `gnitz-engine`
(`catalog/sys_tables.rs:617`). The master converts with `SysFamily::from_id(tid)`
(`sys_tables.rs:664`), exactly as `decode_ddl_txn` → `decode_sys_family` does for
a row item today; an unknown tid is the trust-boundary error, not a panic.

```rust
enum OpKey { Pk(u128), Name(String), Qualified { schema: String, name: String } }
enum ColFlag { Hidden, Nullable }

enum CatalogOp {
    /// DROP TABLE / DROP VIEW / DROP INDEX row retraction.
    /// `if_exists` makes a resolution miss `Ok(())` instead of an error.
    Retract        { family_tid: u64, key: OpKey, if_exists: bool },
    /// ALTER … RENAME (relation, by qname; or column, by owner + column name).
    SetName        { family_tid: u64, key: OpKey, col: Option<String>, name: String },
    /// ALTER TABLE … DROP COLUMN / … DROP NOT NULL. The flag fixes the
    /// direction: both are 0→1 only.
    SetColumnFlag  { owner: OpKey, col: String, flag: ColFlag },
    /// CREATE INDEX, including auto-name disambiguation.
    CreateIndex    { owner: OpKey, cols: Vec<u32>, name: Option<String>, unique: bool },
    /// DROP SCHEMA … CASCADE: members first (views before tables), then the
    /// SCHEMA_TAB row — one commit for the whole cascade.
    DropSchemaCascade { name: String },
}
```

**A generic `SetPayloadSlot { family, key, slot, value }` is rejected.** The
engine's guards are per-slot *semantic* rules — `is_hidden` and `is_nullable` may
move only `0→1` (`write_path.rs:401-406`), a relation pair may change only `name`
(`:314`). A generic slot-setter would force the handler to re-derive which slot
moved and re-apply that classification, recreating inside the op handler exactly
the decode-the-verb-from-a-diff problem this plan removes. Named ops make the
direction a property of the op's type, so the guard degrades from
diff-and-classify to `assert current == 0`.

**The column ops carry a column *name*, not a `col_idx`.** An index would defeat
the whole point: producing it client-side means `resolve_table_id`
(`client.rs:1443-1452`), which is the three `scan_catalog` calls at `:1509`,
`:1523`, `:1546` — exactly the traffic this plan removes. The master resolves the
name against `caches.col_defs` (`cache.rs:41`), which the FK and SERIAL guards
(§5) must consult anyway.

`OpKey::Name` / `Qualified` resolve against maps the master already maintains:
`schema_by_name` (`catalog/cache.rs:30`), `entity_by_qname` (`:32`, fed by
TABLE_TAB *and* VIEW_TAB), `index_by_name` (`:56`). Resolution happens under the
same write lock as the mutation, so there is no window between resolving a name
and acting on it.

### 3. Master-side derivation

Under the DDL write lock, each op expands to the family batches
`apply_and_enqueue_family` already consumes:

- `Retract` — `retract_single_row(sys_store(family), &schema, pk)`. A VIEW_TAB
  retraction additionally retracts every hidden segment the view owns, enumerated
  from `entity_by_id` (`cache.rs:33`) and `members_by_schema` (`:37`) by the
  shared name prefix; that helper (`hidden_view_prefix`, `client.rs:203`) moves
  to `gnitz-wire` so producer and consumer keep one definition.
- `SetName` / `SetColumnFlag` — seek the live row (`seek_live_sys_row`,
  `write_path.rs:213`), emit it at `-1`, emit it again at `+1` with exactly one
  payload slot overridden.
- `CreateIndex` — `idx_tab_row` (`ddl.rs:8-19`) is already production via
  `create_fk_indices` (`:587`), and the row-building body of the engine's
  `create_index` (`ddl.rs:277-333`) is reusable. Two changes it needs: its tail
  `self.submit(SysFamily::Index, batch)` (`:334`) becomes "return the batch", and
  it takes `col_names: &[&str]` (`:284-296`) — the op's `Vec<u32>` must be
  resolved back to names for the auto name (`col_names.join("_")`, `:299`) from
  `caches.col_defs`. Most of the remaining checks are redundant with
  `precheck_index_family`, which the derived batch flows through anyway; the one
  that must be lifted is the UNIQUE-on-STRING/BLOB reject (`ddl.rs:305-310`), its
  only production home. `rollback_index_registration` (`ddl.rs:27-35`) has one
  call site — `create_index`'s submit-failure arm (`:341`) — and becomes **dead
  code to delete**, since the apply now happens in the ingest loop where
  `applied_not_enqueued` + `compensate_stage_a` is the rollback.

  Auto-name disambiguation (`disambiguate_index_name`, fed today by the client's
  whole-IDX_TAB scan `index_name_cols`, `client.rs:680-697` →
  `gnitz-sql/src/ddl/table.rs:737-752`) moves to the master, which has
  `index_by_name`.

  **The derivation must run before the unique-index pre-flight** at
  `executor.rs:2232-2255` — not as an optimization but because that pre-flight
  reads `bundle_family(&families, SysFamily::Index)`, which is `None` until the
  op is expanded, so without the reordering it would silently never run.
  Allocating the index id before a pre-flight that may fail is safe and not new:
  `allocate_index_id` (`registry.rs:180-185`) writes its SEQ_TAB bump directly
  through `sys_store_mut(Sequence).ingest_borrowed_batch` (`:214-228`), not
  through `submit`, so it enqueues no broadcast and needs no open zone. A failed
  pre-flight leaks one id — exactly what happens today, where the client
  allocates via `FLAG_ALLOCATE_INDEX_ID` (`executor.rs:1119`) before pushing.

- `DropSchemaCascade` — un-`cfg(test)` `collect_schema_members` and
  `drain_drop_targets` (`ddl.rs:105`, `:128`) and drive them in process. Member
  ordering (views before tables) and the retry-until-no-progress convergence are
  already written and already tested. Two adjustments are required, both because
  the test-only path was never a *cascade inside one zone*:
  - **Members drop through the same `Retract` derivation**, not through
    `ddl.rs:246-269`'s `drop_view`, which retracts only the vid row. The
    test-path `collect_schema_members` (`ddl.rs:105-121`) also does not filter
    hidden views, unlike the client's `views.retain(|n| !is_hidden_view_name(n))`
    (`client.rs:874`), so it would target each `__h…` segment as a standalone
    drop. Routing through `Retract` keeps one implementation of the
    hidden-segment cascade and preserves the co-drop batch the RESTRICT carve-out
    at `write_path.rs:656-671` expects.
  - **A mid-cascade failure must abort the zone, not requeue.**
    `drain_drop_targets` requeues on **any** `Err`, which is sound today because
    each member drop is its own transaction and any post-apply failure aborts
    that transaction into `compensate_stage_a`. Folded into one zone, a *hook*
    failure would be swallowed and retried against a half-mutated catalog with
    rows already in `pending_broadcasts`. The drain must distinguish a
    pre-mutation RESTRICT rejection (requeue — that is the dependency-ordering
    mechanism) from any other error (propagate, so the zone compensates).

**Replay is unaffected.** What reaches the log is `drain_pending_broadcasts()` —
the batches `apply_and_enqueue_family` pushed *after* applying them
(`write_path.rs:154-159`) — emitted by `emit_zone_to_sal(shared, "DDL", &drained,
zone_lsn)` (`executor.rs:2346-2349`). The client's frame is never the durable
artifact, so deriving rows master-side is bit-for-bit replay-neutral, and worker
`ddl_sync` (`store_io.rs:315`) is untouched. LSN pinning is likewise unaffected:
derivation flows through `submit` → `apply_and_enqueue_family` →
`apply_local(..., ctx.ddl_zone_lsn())` (`write_path.rs:48-49`, `:155`), never
through `submit_local`'s deliberately unpinned rollback path (`:52-58`).

### 4. The post-fsync invalidation lists must be re-derived

`handle_ddl_txn` extracts two lists from the **client's** rows *before* the
ingest loop:

```rust
// executor.rs:2275-2279 — before the loop at :2301
let dropped_tids: Vec<i64> =
    bundle_family(&families, SysFamily::Table).map_or_else(Vec::new, |b| family_pks_by_sign(b, false));
let dropped_indices: Vec<(i64, u64)> = bundle_family(&families, SysFamily::Index)
    .map(idx_tab_drops).unwrap_or_default();
```

and consumes them **after** the fsync (`:2367-2372`) to clear unique-filter
state, *"so a recreated table with the same ID does not inherit stale filter
entries"* (`:2365-2366`). An op-driven drop ships no rows, so both lists would be
empty and neither invalidation would fire — a silent stale-filter bug on the
durability path.

Both lists are therefore re-derived from `drained` (`:2346`) after the ingest
loop instead of from `families` before it. `drained` carries what is needed:
`apply_local` hands `&*batch` to `ingest_borrowed_batch`, which clones into a
fresh owned batch and never mutates the caller's (`storage/lsm/table/mod.rs:320-326`),
`apply_and_enqueue_family` then moves that same batch into `pending_broadcasts`
(`write_path.rs:151-157`), and `drain_pending_broadcasts` is a bare
`std::mem::take` (`sys_tables.rs:786-788`). The `-1` rows and the IDX_TAB
payloads `idx_tab_drops` reads survive verbatim, and `apply_local` re-stamps
`batch.set_schema(family.schema())` (`:120`) so the payload indices resolve.

**The re-derivation must fold over every `drained` entry, not the first per
tid.** `bundle_family` (`executor.rs:2119-2121`) returns only the first match and
documents *"A DDL bundle holds at most one family per tid"* — true of a client
frame, false of `drained`: `cascade_retract_indices` calls `submit` **once per
index id** (`hooks.rs:400-406`), so dropping a table with three secondary indexes
produces three separate `(IDX_TAB, batch)` entries. Reusing `bundle_family` here
would silently under-invalidate. `drained` is therefore strictly **wider** than
the client's bundle, not narrower — it includes cascade-derived retractions the
client never sent.

That widening is benign on the current row path: DROP TABLE today clears only via
`unique_filter_invalidate_table(tid)`, which already `retain`s `t != table_id`
(`runtime/orchestration/master/unique_filter.rs:217-222`) — a strict superset of
the per-index `unique_filter_remove(owner, packed)` calls the fold adds. No
latent bug is fixed and none is introduced.

### 5. Guards move to where the data is

Three client-side assertions the engine does not check today:

- **DROP COLUMN on an FK column** and **on a SERIAL column** are rejected only by
  the SQL planner (`gnitz-sql/src/ddl/alter.rs:214-224`), reading `cd.fk_table_id`
  and `cd.is_serial` off the resolved schema. The engine's
  `precheck_column_family` guards owner kind, PK membership, and dependent views
  (`write_path.rs:408-431`) — neither of these. A binding calling
  `alter_drop_column` directly bypasses both. Server-side, the FK check reads
  `fk_by_child` (`cache.rs:59`), already keyed by child tid with the column
  positions; `is_serial` comes off the live COL_TAB row the op is about to
  rewrite — which needs a new `COLTAB_PAY_IS_SERIAL` payload index alongside the
  eight at `sys_tables.rs:198-205`, since the engine's `ColumnDef`
  (`catalog/types.rs:7-17`) does not carry the flag and `caches.col_defs` cannot
  serve it.

  The third guard on that statement moves with them: **DROP COLUMN on a column
  covered by a secondary index** costs a `table_indexes(tid)` round trip today
  (`gnitz-sql/src/ddl/alter.rs:225-235`) — the 5th request in the measured
  `DROP COLUMN`. The master answers it from `entry.index_circuits`
  (`catalog/metadata.rs:22`) for free. Without moving it, the op cannot reach one
  request.

  (The planner keeps all three checks for the typed
  `GnitzSqlError::Unsupported` message quality; these are the enforcement.)

- **The view dependency graph is authored by the client and never validated.**
  `create_view_chain` writes DEP_TAB from the circuit it ships in the same bundle:

  ```rust
  for dep_tid in pv.circuit.dependencies() {          // client.rs:1193
      let pk = (vid as u128) | ((dep_tid as u128) << 64);
      dep_a.add_row(pk, 1).u64_val(0);
  }
  ```

  and `Circuit::dependencies` (`gnitz-core/src/circuit.rs:39-53`) is just "the
  `ScanDelta` sources in `self.nodes`". The engine checks nothing:
  `precheck_family`'s arm is `SysFamily::ViewDep | … => Ok(())`
  (`write_path.rs:534-538`) and the hook only invalidates the map
  (`hooks.rs:104-106`). Yet DEP_TAB is the sole source of the dependency map
  (`get_dep_map` → `DepMap::get_or_rebuild(self.sys.dep_tab)`,
  `query/dag/meta.rs:133-134`), which drives both which views tick on a source
  delta (`query/dag/exec.rs:308-309`) and the DROP TABLE "view dependency"
  RESTRICT (`write_path.rs:656-671`). A bundle whose DEP_TAB omits a source its
  circuit scans yields a view never maintained from that source *and* a base
  table that can be dropped out from under it.

  The master derives DEP_TAB from the bundle's own CIRCUIT_NODES batch, with no
  `OpNode` decode: `source_table` is a dedicated nullable U64 column in
  `CIRCUIT_NODES_COLS` (`gnitz-wire/src/catalog.rs:159`), and `encode_op_node`
  populates it **only** for `ScanDelta` (`gnitz-core/src/circuit.rs:165-167` is
  the sole `source` arm). So "every positive-weight CIRCUIT_NODES row with a
  non-null `source_table`" *is* the dependency set, readable straight off the
  wire batch before apply. Two requirements:
  - **Dedup.** `Circuit::dependencies` dedups (`circuit.rs:44-51`), so a view
    scanning one source twice yields one DEP_TAB row today. A naive per-node
    derivation would emit two `+1`s on the same PK — a net-2 row that
    `precheck_family`'s `ViewDep => Ok(())` arm will not catch.
  - **Ordering.** `write_view_deps` (`ddl.rs:641-655`) ends in
    `self.submit(SysFamily::ViewDep, batch)`, i.e. it applies immediately. Only
    its row-building loop is reusable: the derived batch must be inserted into
    `families` **before** `families.sort_by_key(topo_priority)`
    (`executor.rs:2298`), so DEP_TAB lands ahead of VIEW_TAB and `get_source_ids`
    resolves during the in-loop source drain (`:2303-2308`).

  This deletes the client's dep-row emission (`client.rs:1193-1197` and the
  `dep_batch`/`dep_s` plumbing at `:1147`, `:1154`, `:1162`, `:1228-1230`). The
  drop side needs nothing: `cascade_retract_circuit_and_deps` (`hooks.rs:592-611`)
  already key-range-retracts `SysFamily::ViewDep` server-side, which is also what
  the ALTER VIEW `replaces` path relies on for the outgoing view.

  DEP_TAB carries nothing the circuit does not determine: `DEP_TAB_COLS`
  (`gnitz-wire/src/catalog.rs:143-147`) is PK `(view_id, dep_table_id)` plus one
  payload `dep_view_id`, hard-coded to `0` by both writers (`client.rs:1196`,
  `ddl.rs:647`) and read by nobody — `DepMap::get_or_rebuild`
  (`query/dag/meta.rs:88-104`) rebuilds both maps from `current_pk_bytes` alone.

### 6. `DROP INDEX … IF EXISTS` stops being a TOCTOU

`drop_index_by_name` (`client.rs:631-671`) documents its contract as:

> `if_exists` swallows a missing index (returns `Ok(())`) — honored at the
> primitive's own not-found path, NOT via a client-side existence pre-check,
> which would be a TOCTOU

but the body scans IDX_TAB, locates the row, and *then* pushes the retraction —
a client-side existence pre-check followed by a separate round trip. A concurrent
`DROP INDEX` landing in that gap makes `check_cas_and_net` fail with *"catalog
changed concurrently: retracting a … that no longer exists"*, a hard
`ClientError` in exactly the case `IF EXISTS` must suppress. As
`Retract { key: Name(..), if_exists }` the resolution and the retraction are one
lock-held operation and the race has no window. The same `if_exists` carries to
`DROP TABLE` / `DROP VIEW`.

### 7. What does not change

Stated explicitly, because the framing above invites the opposite conclusion:

- **`compensate_stage_a` (`write_path.rs:981`) and the Stage-A rollback path
  stay in full.** Compensation exists because hooks do failable, side-effectful
  work *after* apply — `hook_index_register` (`hooks.rs:615`) stages a directory,
  opens a `Table`, and runs `backfill_index`; `preflight_view_compile` runs after
  VIEW_TAB is applied (`executor.rs:2320-2322`). Neither failure is a property of
  a row's content, so master-side derivation cannot make them pre-validatable.
- **`PkSignature` / `pk_signatures` / `family_pks_by_sign` stay.**
  `hook_column_alter` keys on the pair shape and its doc says why: *"Trigger is
  batch-shape-derived, never context-derived: this hook fires on live apply,
  worker sync, and boot replay alike"* (`hooks.rs:444-447`). Ops change only the
  *authoring* side; the applied artifact stays rows on the broadcast, `ddl_sync`,
  and SAL-replay paths.
- **`check_cas_and_net`'s `-1` content comparison stays, and so do the pair-diff
  guards and `compare_rows_except`.** A derived retraction passes the comparison
  trivially, but `FLAG_DDL_TXN` remains open for CREATE rows and
  `decode_client_batch` bulk-copies the weight region without a sign check
  (`batch_wire.rs:227-242`), so a hand-crafted bundle can still present a `-1` or
  a rewrite pair on any family. These are the trust boundary. Deleting them would
  also buy nothing measurable: `compare_rows` already monomorphizes with the skip
  test compiled out (`storage/repr/columnar.rs:46`, and the comment at `:52-53`),
  so the hot merge comparator is unaffected either way.
- **CREATE TABLE and CREATE VIEW keep shipping rows.** A retraction row is
  redundant with the master's live state; a CREATE row is not redundant with
  anything. The COL_TAB row layout *is* the one shared serialization of a column
  definition — `gnitz-core`'s `ColumnDef` (`protocol/types.rs:17-39`, 7 fields
  incl. `is_serial`) and the engine's (`catalog/types.rs:7-17`, 6 fields) are
  unrelated structs, so a `CreateTable` verb would need a *second* encoding of
  the same data in `gnitz-wire`. Net-added code for the payload half.

### 8. Deletions

Client (`gnitz-core/src/client.rs`), by span:

| lines | item |
|---|---|
| ~9 | `append_table_tab_row` |
| 25 | `alter_col_pair` |
| 22 | `drain_drops` |
| ~33 | `drop_index_by_name` net of its wrapper |
| 18 | `index_name_cols` |
| ~12 | `collect_schema_member_names` |
| ~25 | the DEP_TAB emission in `create_view_chain` |
| ~120 | the row-building halves of `drop_schema`, `drop_table`, `drop_view`, `alter_rename_relation`, `alter_rename_column`, `alter_drop_column`, `alter_drop_not_null`, `create_index` |
| 2 fields | `directory`, `created_lsn` on `TableRecord` |

≈240 lines. Two groups that look deletable are **not**:

- **`extract_col_entries`, `find_table_record`, `decode_view_record`,
  `find_view_record`, and both record structs survive** — they are on the *query*
  path (`load_owner_schema` `:1550` ← `resolve_table_id` `:1450`;
  `lookup_table_record` `:1525`; `lookup_relation` `:1541`), reached by every
  SELECT and INSERT, not only by DDL. Their unit tests (`client.rs:2188-2300`)
  survive with them.
- **`append_view_row`, `view_drop_records`, and `collect_view_records_with_prefix`
  survive, and so do `ViewRecord`'s `sql_definition`, `cache_directory`, and
  `created_lsn`** — they are exactly what §1's ALTER VIEW carve-out needs.
  `create_view_chain` writes the outgoing view's retractions with
  `append_view_row(&mut view_a, -1, rec)` (`client.rs:1170-1172`), and
  `check_cas_and_net` requires each `-1` to byte-equal the live row, so those
  three payload fields are load-bearing on that path. Only `TableRecord`'s two
  fields are genuinely freed, because its sole `-1`/pair writers — `drop_table`
  (`:1013`) and `alter_rename_relation` (`:1342`) — both become ops.

Engine: `#[cfg(test)]` comes off `submit_retraction`, `collect_schema_members`,
`drain_drop_targets`, `create_index`, `write_view_deps`, and the helpers they
call.

Additions: the op codec in `gnitz-wire`, the kind byte plus the no-mixing check
in `encode_ddl_txn` / `decode_ddl_txn`, the op→batch derivation, the
`handle_ddl_txn` op arm, the `drained`-based invalidation lists (§4), the DEP_TAB
derivation (§5), and the FK/SERIAL/covered-by-index guards. **Honest projection:
roughly line-neutral.** The win is not line count — it is that the *retraction*
and *ALTER* paths have one implementation instead of two, the surviving one is
the tested one, and four client-side assertions (FK, SERIAL, covered-by-index,
and the view dependency graph) become engine-enforced.

IDX_TAB row authoring remains in three places even after this change —
`create_table`'s inline UNIQUE (`client.rs:983-996`, kept by §7), the master's
`CreateIndex`, and `idx_tab_row` via `create_fk_indices` (`ddl.rs:8-19`, `:587`).
The last two are already the same helper; the first stays client-side because it
rides a CREATE bundle.

## Accounting

| Statement | Today | After |
|---|---|---|
| `DROP INDEX` | 2 req, 856 B | **1 req** |
| `DROP TABLE` | 3 req, 3 595 B | **1 req** |
| `DROP VIEW` | 3 req, 1 842 B | **1 req** |
| `ALTER … RENAME TO` | 3 req, 2 939 B | **1 req** |
| `ALTER … RENAME COLUMN` | 4 req, 10 229 B | **1 req** |
| `ALTER … DROP COLUMN` | 5 req, 10 685 B | **1 req** |
| `CREATE INDEX` | 6 req, 12 434 B | **1 req** |
| `DROP SCHEMA CASCADE` | 3n+4 req, n commits | **1 req, 1 commit** |
| `CREATE TABLE` / `CREATE VIEW` | unchanged | unchanged |

Every statement that collapses to one request ships no catalog bytes at all.
`DROP SCHEMA CASCADE`'s value at the sizes that actually occur (mean 0.73
members, max 9) is the collapse from `n+1` durable commits to one.

## Tests

Engine unit (`catalog/tests/`):
- each op derives the same family batch the equivalent client-shipped rows
  produce today: byte-equality of the resulting `pending_broadcasts` batch
  against the row-shipping path, per family.
- a bundle mixing a row item and an op item is **rejected** at decode.
- `Retract` on a VIEW_TAB id also retracts every hidden segment the view owns and
  no segment owned by another view (`__h5_` must not match `__h51_0`).
- `SetColumnFlag` rejects a PK column, a non-base owner, a relation with
  dependent views, an **FK-carrying** column, and a **SERIAL** column — the last
  two being new engine-side guards.
- `Retract { if_exists: true }` on a missing index/table/view is `Ok(())`; with
  `if_exists: false` it is the friendly not-found error.
- `CreateIndex` disambiguates an auto-generated name against `index_by_name`, and
  its derivation runs before the unique pre-flight (a UNIQUE index over duplicate
  data still fails before the zone LSN is reserved).
- `DropSchemaCascade` drops views before tables, converges over an intra-schema
  view-on-view chain and an FK chain, leaves the schema row live when an external
  dependent blocks a member, and emits **one** zone.
- DEP_TAB derived from a bundle's CIRCUIT_NODES equals what the client writes
  today, for a single-source view, a multi-source chain, and a view-on-view; and
  a view that scans one source **twice** yields exactly one DEP_TAB row at
  weight 1, not a net-2 row.
- `dropped_tids` / `dropped_indices` re-derived from `drained` fire the
  unique-filter invalidations for an **op-driven** DROP TABLE and DROP INDEX —
  the regression test for §4. A table with **three** secondary indexes clears all
  three, which is the fold-over-all-entries case a `bundle_family`-style
  first-match would miss.
- `DropSchemaCascade` over a schema holding a view with hidden segments emits the
  same VIEW_TAB co-drop batch per user view that a standalone DROP VIEW does, and
  never targets a `__h…` segment as its own member.
- a hook failure part-way through a cascade aborts the zone into
  `compensate_stage_a` rather than being requeued by the drain; a RESTRICT
  rejection is still requeued.
- replay: the SAL bytes for an op-driven DROP equal those for the row-driven DROP
  at the same zone LSN.

E2E (`gnitz-py`, `GNITZ_WORKERS=4`):
- every DDL statement shape end-to-end, asserting post-state via the system
  catalogs, with a boot after each to prove recovery replays it.
- `DROP INDEX … IF EXISTS` racing a concurrent `DROP INDEX` of the same index
  from a second connection returns `Ok(())` — the TOCTOU regression test.
- a raw binding calling `alter_drop_column` directly on an FK column and on a
  SERIAL column is rejected by the engine.
- a UNIQUE index created, dropped, and recreated on the same columns of the same
  table enforces uniqueness — the stale-unique-filter regression test.
- ALTER VIEW (the `replaces` path) still commits atomically as a row bundle.

## Sequencing

- [ ] **Op codec and frame:** `CatalogOp` / `OpKey` / `ColFlag` in `gnitz-wire`,
  the kind byte and the no-mixing check in `encode_ddl_txn` / `decode_ddl_txn`,
  round-trip tests; `hidden_view_prefix` moves to `gnitz-wire`.
- [ ] **Invalidation lists:** re-derive `dropped_tids` / `dropped_indices` from
  `drained` after the ingest loop, folding over **all** entries per tid. Lands
  before any op exists, so the current row path exercises it — including the
  multi-index cascade case; the op-driven regression test arrives with `Retract`.
- [ ] **`Retract` and `DropSchemaCascade`:** derivation over `retract_single_row`,
  name resolution through the existing maps, hidden-segment enumeration, the
  `handle_ddl_txn` op arm; un-`cfg(test)` `submit_retraction`,
  `collect_schema_members`, `drain_drop_targets`, routing cascade members through
  `Retract` and splitting the drain's error classification (requeue a RESTRICT
  rejection, propagate anything else); engine unit tests.
- [ ] **`SetName` and `SetColumnFlag`,** plus `COLTAB_PAY_IS_SERIAL` and the FK,
  SERIAL, and covered-by-index guards in `precheck_column_family`.
- [ ] **`CreateIndex`,** derived ahead of the unique pre-flight (mandatory — the
  pre-flight reads the bundle's Index family); reshape the engine `create_index`
  to return its batch instead of submitting, lift the UNIQUE-on-STRING/BLOB
  reject, delete `rollback_index_registration`; delete `index_name_cols` and move
  auto-name disambiguation to the master.
- [ ] **DEP_TAB derivation** from the bundle's CIRCUIT_NODES `source_table`
  column, deduped, inserted into `families` ahead of the topo sort; delete the
  client's dep-row emission; reuse `write_view_deps`' row-building loop only.
- [ ] **Client switch:** each DDL method becomes an op encoder; the deletions in
  §8.
- [ ] **E2E suite** per the test list above; full `make verify` + `make e2e`.
