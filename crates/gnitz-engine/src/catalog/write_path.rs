//! Catalog write-path spine — the single ingest/apply pipeline every
//! system-table mutation flows through: `submit` → `precheck_sys_ingest`
//! → `apply_local` → `fire_hooks`, plus the broadcast queue, the
//! directory-deletion queues, and Stage-A (DDL rollback) compensation.
//! `fire_hooks` lives in `hooks.rs`; `SysFamily` / `ApplyContext` in
//! `sys_tables.rs` / `apply_context.rs`. No second ingest entry point may
//! skip this precheck/hooks path.

use std::cmp::Ordering;
use std::num::NonZeroU64;

use super::*;
use crate::schema::make_index_schema;
use crate::storage::{compare_rows, compare_rows_except};

/// The only handle the DDL/imperative layer has on catalog state: submit one
/// system-family delta. It cannot name a `sys_*` table, so DDL code physically
/// cannot mutate a dependent family except by emitting a delta that flows
/// through the single precheck → persist → `fire_hooks` → broadcast path —
/// identical on live apply, WAL replay, and worker sync. The cascades that drop
/// columns/indices/circuit rows are the applier's declared reaction to a
/// retraction, fired from inside `fire_hooks`, not the emitter's concern.
pub(crate) trait CatalogDeltaSink {
    /// Apply one system-family delta and enqueue it for broadcast: precheck →
    /// storage write → `fire_hooks` → enqueue. The batch is taken by value so
    /// the applier moves it straight into `pending_broadcasts` (one storage
    /// clone, no hooks clone).
    fn submit(&mut self, family: SysFamily, batch: Batch) -> Result<(), String>;

    /// Apply locally without enqueuing a broadcast. ONLY for rows the workers
    /// already produce themselves (FK indices auto-created from the same
    /// `TABLE_TAB` delta) and for rollback compensation. Re-broadcasting these
    /// would deliver phantom deltas. Documented and audited; not a general escape.
    fn submit_local(&mut self, family: SysFamily, batch: Batch) -> Result<(), String>;
}

impl CatalogDeltaSink for CatalogEngine {
    fn submit(&mut self, family: SysFamily, batch: Batch) -> Result<(), String> {
        if self.ctx.in_rollback() {
            // During rollback all cascade writes must bypass pending_broadcasts
            // so no compensating row is re-broadcast to workers.
            return self.submit_local(family, batch);
        }
        // `submit` is the composition of the two steps the DDL_TXN handler drives
        // separately per bundle family: precheck (no mutation) then
        // apply-and-enqueue. Cascade callers (hooks) keep the atomic composition.
        self.precheck_family(family.id(), &batch)?;
        self.apply_and_enqueue_family(family.id(), batch)
    }

    fn submit_local(&mut self, family: SysFamily, mut batch: Batch) -> Result<(), String> {
        // No LSN pin: local applies are rollback compensation or rows the
        // workers already produce, neither of which owns this zone's durability.
        // `None` is deliberate even while a DDL zone is active — these rows are
        // not in the SAL, so pinning their family's current_lsn would advance the
        // recovery dedup watermark with no matching SAL group.
        self.apply_local(family, &mut batch, None)
        // Deliberately no push to pending_broadcasts.
    }
}

impl CatalogEngine {
    // -- System table accessors ------------------------------------------------
    //
    // Ids are non-contiguous, so every id-keyed access routes through
    // `SysFamily::from_id`; the by-family accessors are infallible.

    /// This family's owned store.
    pub(crate) fn sys_store(&self, family: SysFamily) -> &Table {
        &self.sys_stores[family.index()]
    }

    pub(crate) fn sys_store_mut(&mut self, family: SysFamily) -> &mut Table {
        &mut self.sys_stores[family.index()]
    }

    /// Raw pointer to this family's store — stable across engine moves (the
    /// store is boxed), for the `Borrowed` DAG registrations.
    pub(crate) fn sys_store_ptr(&mut self, family: SysFamily) -> *mut Table {
        &mut *self.sys_stores[family.index()]
    }

    /// Map a system table ID to a reference. Returns None for unknown IDs.
    pub(crate) fn sys_table(&self, table_id: i64) -> Option<&Table> {
        SysFamily::from_id(table_id).map(|f| self.sys_store(f))
    }

    pub(crate) fn sys_table_mut(&mut self, table_id: i64) -> Option<&mut Table> {
        SysFamily::from_id(table_id).map(|f| &mut *self.sys_stores[f.index()])
    }

    /// Apply one delta to its family's storage and fire the reaction hooks —
    /// the shared tail of [`CatalogDeltaSink::submit`] / `submit_local`. When
    /// `pin_lsn` is `Some(lsn)` it pins the family's `current_lsn` to `lsn.get()`
    /// (the DDL zone LSN) so recovery's dedup check (`msg.lsn <= flushed`) matches
    /// the SAL group LSN. Pins never regress the counter: every zone is reserved
    /// with a floor that dominates the pinned family's `current_lsn`
    /// (`ZoneLsnAllocator::reserve`), so even counters drifted by un-pinned
    /// auto-bump ingests sit strictly below the zone LSN pinning them. Does NOT
    /// broadcast.
    pub(super) fn apply_local(
        &mut self,
        family: SysFamily,
        batch: &mut Batch,
        pin_lsn: Option<NonZeroU64>,
    ) -> Result<(), String> {
        let id = family.id();
        let table = self
            .sys_table_mut(id)
            .expect("SysFamily::id() maps to a known sys table");
        // Live DDL's one sound non-fatal exit: a failure here is pre-broadcast
        // (nothing durable, nothing broadcast, client not ACKed), so Stage-A
        // compensation can unwind it. Propagate rather than abort.
        table
            .ingest_borrowed_batch(batch)
            .map_err(|e| format!("apply_local: sys-table ingest failed (family={id}): {e}"))?;
        if let Some(lsn) = pin_lsn {
            table.pin_lsn(lsn);
        }
        batch.set_schema(sys_tab_schema(id));
        self.fire_hooks(family, batch)
    }

    // -- System ingestion entry + precheck / broadcast / dir-deletion -------

    /// Ingest a batch into a table family (unique_pk + store + index projection + hooks).
    /// System tables go through the [`CatalogDeltaSink::submit`] applied-delta
    /// path (precheck → ingest → hooks → broadcast-queue). User tables delegate
    /// to `DagEngine::ingest_by_ref`. This `&Batch` entry serves external/wire
    /// callers that hold a borrow; the DDL emitters call `submit` directly with
    /// an owned batch to skip the clone.
    pub fn ingest_to_family(&mut self, table_id: i64, batch: &Batch) -> Result<(), String> {
        if table_id < FIRST_USER_TABLE_ID {
            let family = SysFamily::from_id(table_id).ok_or_else(|| format!("Unknown system family {table_id}"))?;
            self.submit(family, batch.clone())
        } else {
            let rc = self.dag.ingest_by_ref(table_id, batch);
            if rc < 0 {
                Err(format!("ingest_to_family failed for table_id={table_id} rc={rc}"))
            } else {
                Ok(())
            }
        }
    }

    /// Apply one system-family delta to storage, fire its hooks, and enqueue it
    /// for broadcast — the mutating half of [`CatalogDeltaSink::submit`], pinned
    /// to the open DDL zone LSN. Takes the batch by value so it moves straight
    /// into `pending_broadcasts` (one storage clone, no hooks clone). Enqueue
    /// happens after hooks so nested cascade pushes land first and the executor
    /// broadcasts children → parent; empty batches are dropped so worker-side
    /// no-op cascades don't accumulate unread entries.
    pub(crate) fn apply_and_enqueue_family(&mut self, table_id: i64, mut batch: Batch) -> Result<(), String> {
        let family = SysFamily::from_id(table_id).ok_or_else(|| format!("Unknown system family {table_id}"))?;
        self.apply_local(family, &mut batch, self.ctx.ddl_zone_lsn())?;
        if batch.count > 0 {
            self.pending_broadcasts.push((table_id, batch));
        }
        Ok(())
    }

    /// The schema descriptor for a system-family `tid`, or `None` if `tid` is not
    /// a system family. Unlike `get_schema_desc`, this never panics on an unknown
    /// id in the system range, so the `DDL_TXN` decoder can reject a bogus family
    /// tid before decoding its wal-block slice into a `Batch`.
    pub(crate) fn sys_family_schema(&self, tid: i64) -> Option<SchemaDescriptor> {
        SysFamily::from_id(tid).map(|f| sys_tab_schema(f.id()))
    }

    /// Emit the single retraction delta for `pk` in `family`: seek the live row,
    /// copy it with weight −1, and submit it through the one applied-delta path.
    /// The drop cascade is the applier's reaction to that −1 (fired from
    /// `fire_hooks`), not the caller's concern. Uses the immutable `sys_table`
    /// accessor because `retract_single_row` only reads; the `submit` move comes
    /// after. `retract_single_row` returns an empty batch when the PK is absent
    /// or already retracted; emitters resolve the friendly "does not exist"
    /// message from the caches before calling, so the `count == 0` arm only
    /// fires on cache/storage divergence.
    /// Only the test-only direct DDL drop paths (`ddl.rs`) retract engine-side;
    /// production retractions arrive as wire deltas.
    #[cfg(test)]
    pub(crate) fn submit_retraction(&mut self, family: SysFamily, pk: u128) -> Result<(), String> {
        let schema = sys_tab_schema(family.id());
        let batch = {
            let table = self
                .sys_table(family.id())
                .expect("SysFamily::id() maps to a known sys table");
            retract_single_row(table, &schema, pk)
        };
        if batch.count == 0 {
            return Err("Entity does not exist in catalog".into());
        }
        self.submit(family, batch)
    }

    /// Reject a CREATE whose qualified `schema.name` collides with an existing
    /// entity. Re-ingesting the same row (e.g. a pk-col update of `self_id`) is
    /// allowed; only a genuinely different entity under that name is rejected.
    /// Also resolves `sid`, erroring if the schema does not exist.
    fn precheck_qname_unique(&self, sid: i64, name: &str, self_id: i64) -> Result<(), String> {
        let schema_name = self
            .caches
            .schema_by_id
            .get(&sid)
            .ok_or_else(|| format!("Schema with ID {sid} does not exist"))?;
        let qualified = format!("{schema_name}.{name}");
        if let Some(&existing) = self.caches.entity_by_qname.get(&qualified) {
            if existing != self_id {
                return Err(format!("Table or view already exists: {qualified}"));
            }
        }
        Ok(())
    }

    /// Materialize the live (net-positive) row for the OPK `pk_bytes` in
    /// `family`'s store into a 1-row batch, with its net weight. `None` if no
    /// live row. Reads only — the `copy_current_row_into` precedent is
    /// `retract_single_row`. `ReadCursor` is not a `ColumnarSource`, so the CAS
    /// needs the row materialized into a `Batch` before `compare_rows`.
    fn seek_live_sys_row(&self, family: SysFamily, pk_bytes: &[u8]) -> Option<(Batch, i64)> {
        let mut cursor = self.sys_store(family).open_cursor();
        if !cursor.seek_exact_live(pk_bytes) {
            return None;
        }
        let w = cursor.current_weight;
        let mut b = Batch::with_schema(sys_tab_schema(family.id()), 1);
        cursor.copy_current_row_into(&mut b, w);
        Some((b, w))
    }

    /// §3.3(A): the post-image retraction contract for a rewrite-pair-capable
    /// family (TABLE_TAB / VIEW_TAB / COL_TAB). For every distinct PK the batch
    /// touches:
    /// 1. **CAS** — every `-1` row must content-equal the current live row (you
    ///    may only retract the element that exists). Via the generic
    ///    `compare_rows`, which routes STRING/BLOB (`name`/`sql_definition`)
    ///    through each side's own blob heap, so a valid rename to a name > 12
    ///    bytes is accepted (a raw region `memcmp` would false-reject it) while a
    ///    stale-snapshot `-1` is rejected.
    /// 2. **Per-PK net** — `live_weight + Σ batch weights` must be 0 or 1 (sys
    ///    stores are not `unique_pk`, so nothing else stops a duplicate live head
    ///    or a persistent negative ghost).
    /// 3. **Pair fields** — a PK carrying both signs (a rewrite pair) may differ
    ///    from the live row only in `name` (`name_pay`); for a relation family
    ///    (`is_relation`) a rewrite pair on a system-range id is additionally
    ///    rejected.
    ///
    /// Returns the PKs whose net is dead (`≤ 0`) — the genuine drops — so the
    /// relation drop guards re-key on net-liveness (a rename's net-live `-1` is
    /// excluded) rather than raw batch weights.
    fn precheck_retraction_contract(
        &self,
        family: SysFamily,
        batch: &Batch,
        name_pay: usize,
        is_relation: bool,
    ) -> Result<Vec<i64>, String> {
        let schema = sys_tab_schema(family.id());
        let mut net_dead: Vec<i64> = Vec::new();
        let mut seen: Vec<u128> = Vec::new();
        for i in 0..batch.count {
            let pk = batch.get_pk(i);
            if seen.contains(&pk) {
                continue;
            }
            seen.push(pk);

            // Per-PK batch signature (the batch is a handful of rows).
            let mut batch_sum = 0i64;
            let (mut has_neg, mut has_pos) = (false, false);
            for j in 0..batch.count {
                if batch.get_pk(j) != pk {
                    continue;
                }
                let w = batch.get_weight(j);
                batch_sum += w;
                has_neg |= w < 0;
                has_pos |= w > 0;
            }

            // (0) System-range rewrite guard (§3.3(B), relation families only):
            // a rewrite pair (both signs) on a system-range id is rejected up
            // front — before the CAS, so it fires regardless of whether a live
            // row exists. Closes the pre-existing gap that no precheck guarded
            // system-range payload ids.
            if is_relation && has_neg && has_pos && (pk as i64) < FIRST_USER_TABLE_ID {
                return Err(format!(
                    "cannot ALTER a system relation (id {pk} < {FIRST_USER_TABLE_ID})"
                ));
            }

            let live = self.seek_live_sys_row(family, batch.get_pk_bytes(i));
            let live_weight = live.as_ref().map_or(0, |(_, w)| *w);
            let live_batch: Option<&Batch> = live.as_ref().map(|(b, _)| b);

            // (1) CAS: every -1 row must content-equal the live row.
            if has_neg {
                let Some(lb) = live_batch else {
                    return Err(
                        "catalog changed concurrently: retracting a system-catalog row that no longer exists".into(),
                    );
                };
                for j in 0..batch.count {
                    if batch.get_pk(j) == pk
                        && batch.get_weight(j) < 0
                        && compare_rows(&schema, lb, 0, batch, j) != Ordering::Equal
                    {
                        return Err(
                            "catalog changed concurrently: the retracted system-catalog row differs from the current one"
                                .into(),
                        );
                    }
                }
            }

            // (2) Per-PK net ∈ {0, 1}.
            let net = live_weight + batch_sum;
            if !(0..=1).contains(&net) {
                return Err(format!(
                    "system-catalog write would leave PK {pk} at net weight {net} (expected 0 or 1)"
                ));
            }

            // (3) Pair fields: a rewrite pair's `+1` may differ from the live row
            // only in `name` (the CAS above already required a live row).
            // `compare_rows_except` routes STRING/BLOB through each side's own
            // blob heap, so a name > 12 bytes is compared by content, never a
            // raw region `memcmp`.
            if has_neg && has_pos {
                let lb = live_batch.expect("a pair's -1 CAS already required a live row");
                for j in 0..batch.count {
                    if batch.get_pk(j) == pk
                        && batch.get_weight(j) > 0
                        && compare_rows_except(&schema, lb, 0, batch, j, 1 << name_pay) != Ordering::Equal
                    {
                        return Err("a system-catalog rewrite pair may only change the name".into());
                    }
                }
            }

            if net <= 0 {
                net_dead.push(pk as i64);
            }
        }
        Ok(net_dead)
    }

    /// §3.1: DROP-shape validation for the COL_TAB family, replacing the bare
    /// retraction contract. Runs only on the master live-DDL `submit` path (worker
    /// `ddl_sync` and SAL recovery bypass precheck). Enforces, per distinct column
    /// PK on a registered owner:
    ///
    /// - **At most one row per sign** — a live ALTER is exactly one rewrite pair;
    ///   nothing legitimate repeats a sign on one column PK.
    /// - **CAS + net** (kept from the contract): the `-1` byte-equals the live
    ///   row; per-PK `net ∈ {0,1}`.
    /// - **Rewrite pair** (`-1` + `+1`, same id): every payload field other than
    ///   `{name, is_hidden, is_nullable}` must match, and `is_hidden` /
    ///   `is_nullable` may change only `0→1` (forward path; compensation replays
    ///   `1→0` through `submit_local`, which bypasses precheck).
    /// - **Transition-scoped guards** (load-bearing — a blanket guard would regress
    ///   RENAME): a `name`-only pair (RENAME COLUMN) is always accepted; an
    ///   `is_hidden 0→1` (DROP COLUMN) or `is_nullable 0→1` (DROP NOT NULL) pair
    ///   requires the owner to be a registered user base table (not a view, not
    ///   system-range) with **no dependent views**, and the column not to be a PK
    ///   column.
    /// - **Unpaired `-1`** on a registered owner is rejected (physical column
    ///   removal does not exist); **unpaired `+1`** is rejected (no column-append
    ///   feature). An unregistered owner is a live CREATE TABLE COL append (the
    ///   owner table registers later in the bundle), so `+1`-only rows pass.
    fn precheck_column_family(&mut self, batch: &Batch) -> Result<(), String> {
        let schema = sys_tab_schema(COL_TAB_ID);
        // The payload fields a rewrite pair may change; everything else must match.
        let pair_mask: u64 = (1 << COLTAB_PAY_NAME) | (1 << COLTAB_PAY_IS_HIDDEN) | (1 << COLTAB_PAY_IS_NULLABLE);
        let mut seen: Vec<u128> = Vec::new();
        for i in 0..batch.count {
            let pk = batch.get_pk(i);
            if seen.contains(&pk) {
                continue;
            }
            seen.push(pk);
            let owner_id = gnitz_wire::unpack_col_id(pk as u64).0 as i64;

            // Per-PK batch signature + the -1/+1 row indices for the pair check.
            let mut batch_sum = 0i64;
            let (mut neg_j, mut pos_j): (Option<usize>, Option<usize>) = (None, None);
            for j in 0..batch.count {
                if batch.get_pk(j) != pk || batch.get_weight(j) == 0 {
                    continue;
                }
                let w = batch.get_weight(j);
                batch_sum += w;
                let slot = if w < 0 { &mut neg_j } else { &mut pos_j };
                if slot.replace(j).is_some() {
                    return Err(format!(
                        "system-catalog write carries multiple same-sign rows for column {pk}"
                    ));
                }
            }

            // Unregistered owner: a live CREATE TABLE applies COL before TABLE, so
            // the owner registers later in this bundle. A `+1` append is valid;
            // a `-1` has no live row to retract.
            let Some((is_base, owner_schema)) = self
                .dag
                .tables
                .get(&owner_id)
                .map(|e| (e.kind.is_base_table(), e.schema))
            else {
                if neg_j.is_some() {
                    return Err(format!(
                        "catalog changed concurrently: retracting a column of unregistered owner {owner_id}"
                    ));
                }
                continue;
            };

            // (1) CAS: the `-1` must byte-equal the live row.
            let live = self.seek_live_sys_row(SysFamily::Column, batch.get_pk_bytes(i));
            if let Some(nj) = neg_j {
                let Some((lb, _)) = live.as_ref() else {
                    return Err(
                        "catalog changed concurrently: retracting a system-catalog column that no longer exists".into(),
                    );
                };
                if compare_rows(&schema, lb, 0, batch, nj) != Ordering::Equal {
                    return Err(
                        "catalog changed concurrently: the retracted column row differs from the current one".into(),
                    );
                }
            }

            // (2) Per-PK net ∈ {0,1}.
            let live_weight = live.as_ref().map_or(0, |(_, w)| *w);
            let net = live_weight + batch_sum;
            if !(0..=1).contains(&net) {
                return Err(format!(
                    "system-catalog write would leave column {pk} at net weight {net} (expected 0 or 1)"
                ));
            }

            match (neg_j, pos_j) {
                (Some(nj), Some(pj)) => {
                    // (3) Pair-field delta: every payload field outside
                    // {name, is_hidden, is_nullable} must match (null- and
                    // blob-aware, like the CAS).
                    if compare_rows_except(&schema, batch, nj, batch, pj, pair_mask) != Ordering::Equal {
                        return Err(
                            "a column-ALTER rewrite pair may change only name, is_hidden, or is_nullable".into(),
                        );
                    }
                    let hid_old = batch.read_payload_u64(nj, COLTAB_PAY_IS_HIDDEN);
                    let hid_new = batch.read_payload_u64(pj, COLTAB_PAY_IS_HIDDEN);
                    let null_old = batch.read_payload_u64(nj, COLTAB_PAY_IS_NULLABLE);
                    let null_new = batch.read_payload_u64(pj, COLTAB_PAY_IS_NULLABLE);
                    // Direction: is_hidden / is_nullable only 0→1 (forward path).
                    if hid_new != hid_old && !(hid_old == 0 && hid_new == 1) {
                        return Err("a column-ALTER may only set is_hidden 0→1 (DROP COLUMN)".into());
                    }
                    if null_new != null_old && !(null_old == 0 && null_new == 1) {
                        return Err("a column-ALTER may only set is_nullable 0→1 (DROP NOT NULL)".into());
                    }

                    let is_drop = (hid_old == 0 && hid_new == 1) || (null_old == 0 && null_new == 1);
                    if is_drop {
                        // Owner must be a registered user base table; the column
                        // must not be a PK column.
                        if !is_base || owner_id < FIRST_USER_TABLE_ID {
                            return Err(format!(
                                "cannot DROP COLUMN / DROP NOT NULL on a column of {owner_id}: not a user base table"
                            ));
                        }
                        let col_idx = gnitz_wire::unpack_col_id(pk as u64).1 as u32;
                        if owner_schema.pk_indices().contains(&col_idx) {
                            return Err("cannot DROP COLUMN / DROP NOT NULL on a primary-key column".into());
                        }
                        // Dependent-view RESTRICT (defense-in-depth; the friendly
                        // client-side reject is in `plan/alter.rs`). A DROP COLUMN
                        // / DROP NOT NULL must not proceed while a view scans the
                        // base table: its operator traces hold re-keyed base rows
                        // under the pre-ALTER comparator.
                        if self.dag.get_dep_map().get(&owner_id).is_some_and(|v| !v.is_empty()) {
                            return Err(format!(
                                "cannot DROP COLUMN / DROP NOT NULL on table {owner_id}: it has dependent views (drop them first)"
                            ));
                        }
                    }
                    // name-only / no-op pair (RENAME COLUMN, already-nullable DROP
                    // NOT NULL): accepted with no further guard — always safe,
                    // including with dependent views (views bind columns by ordinal).
                }
                (Some(_), None) => {
                    return Err(
                        "cannot retract a column of a registered table (physical column removal is not supported)"
                            .into(),
                    );
                }
                (None, Some(_)) => {
                    return Err(
                        "cannot append a column to a registered table (ALTER TABLE ADD COLUMN is not supported)".into(),
                    );
                }
                (None, None) => {}
            }
        }
        Ok(())
    }

    /// Visit every positive-weight `sys_indices` row whose owner and **exact
    /// column list** match `(owner_id, cols)`, invoking `f(index_id, is_unique)`
    /// for each. Centralises the IDX_TAB cursor walk shared by the DROP INDEX
    /// uniqueness checks (the drop-time FK guard in `precheck_sys_ingest` and the
    /// post-retraction circuit demotion in `hook_index_register`). The persisted
    /// `source_cols` field is the packed `u64` (flag bit 63 set for the packed
    /// form), so decode it via `unpack_pk_cols` and compare ordered lists — a
    /// bare compare would never match a packed row. Rows that have already netted
    /// to zero weight are skipped by the cursor.
    pub(crate) fn for_each_index_on_cols(&self, owner_id: i64, cols: &[u32], mut f: impl FnMut(i64, bool)) {
        let mut cursor = self.sys_store(SysFamily::Index).open_cursor();
        while cursor.valid {
            if cursor.current_weight > 0 {
                let row_owner = cursor_read_u64(&cursor, IDXTAB_COL_OWNER_ID) as i64;
                let row_cols = gnitz_wire::unpack_pk_cols(cursor_read_u64(&cursor, IDXTAB_COL_SOURCE_COLS));
                if row_owner == owner_id && row_cols.as_slice() == cols {
                    let row_id = cursor.current_key_narrow() as u64 as i64;
                    let is_uniq = cursor_read_u64(&cursor, IDXTAB_COL_IS_UNIQUE) != 0;
                    f(row_id, is_uniq);
                }
            }
            cursor.advance();
        }
    }

    /// The shared IDX_TAB registration guards: a well-formed column list and a
    /// base-table owner (returned for the callers' schema/context reads).
    ///
    /// Only base tables can own a secondary index: index projection runs only
    /// on the base-table DML paths (`ingest_store_and_indices`); view deltas
    /// land via the circuit-evaluation terminal-view moves, which never
    /// project into `index_circuits`. An index on a view would backfill once
    /// and then silently serve stale results. The SQL binder rejects this by
    /// name resolution; the precheck rejects a raw wire push before the row is
    /// persisted or broadcast, and `hook_index_register` re-checks for the
    /// paths that skip precheck (boot replay, worker ddl_sync).
    pub(crate) fn validate_index_registration(
        &self,
        owner_id: i64,
        cols: &PkColList,
    ) -> Result<&crate::query::TableEntry, String> {
        if !cols.is_well_formed() {
            return Err(format!(
                "Index: column list count {} out of range 1..={}",
                cols.decoded_count(),
                gnitz_wire::PK_LIST_MAX_COLS
            ));
        }
        let entry = self
            .dag
            .tables
            .get(&owner_id)
            .ok_or_else(|| format!("Index: owner table {owner_id} not found"))?;
        if !entry.kind.is_base_table() {
            return Err(format!("Index: owner {owner_id} is not a base table"));
        }
        Ok(entry)
    }

    /// Validate a system-table write before any mutation (memtable or hooks) —
    /// the read-only half of [`CatalogDeltaSink::submit`]. Covers both
    /// positive-weight (CREATE) invariants and negative-weight (DROP) integrity
    /// guards so that no invalid state is ever written. Also called directly by
    /// the `DDL_TXN` handler, which prechecks a family, sets its rollback
    /// marker, then applies it — leaving the marker `None` iff the precheck
    /// failed (so a precheck rejection reconstructs no ghost row on rollback).
    pub(crate) fn precheck_family(&mut self, table_id: i64, batch: &Batch) -> Result<(), String> {
        // Exhaustive over `SysFamily` (like `fire_hooks`): a newly-added family
        // must decide here whether it carries precheck guards.
        let family = match SysFamily::from_id(table_id) {
            Some(f) => f,
            None => return Ok(()),
        };
        // §3.3(A): the retraction contract (CAS + per-PK net + pair-fields) runs
        // for every rewrite-pair-capable family and yields the net-dead PKs.
        // COL_TAB runs ONLY this layer (a column record has no relation guards);
        // TABLE/VIEW run it, then the relation guards below re-key their drop
        // checks on the net-dead set. SCHEMA/INDEX carry no rewrite pair, so they
        // keep the raw weight<0 drop set (already net-dead) and skip the contract.
        let net_dead: Vec<i64> = match family {
            SysFamily::Table => self.precheck_retraction_contract(family, batch, TABTAB_PAY_NAME, true)?,
            SysFamily::View => self.precheck_retraction_contract(family, batch, VIEWTAB_PAY_NAME, true)?,
            SysFamily::Column => {
                // Whole-table COL retractions during a DROP TABLE/VIEW cascade run
                // while the owner is still registered and are unpaired `-1`s, which
                // the arm would reject — the cascade wraps them in `with_cascade_drop`.
                if self.ctx.in_cascade_drop() {
                    return Ok(());
                }
                self.precheck_column_family(batch)?;
                return Ok(());
            }
            SysFamily::Schema | SysFamily::Index => Vec::new(),
            SysFamily::ViewDep
            | SysFamily::Sequence
            | SysFamily::CircuitNodes
            | SysFamily::CircuitEdges
            | SysFamily::CircuitNodeColumns => return Ok(()),
        };

        // -- Positive-weight (CREATE) checks ----------------------------------
        for i in 0..batch.count {
            if batch.get_weight(i) <= 0 {
                continue;
            }

            // Enforce the durable relation-id ceiling here — the one place an id
            // ENTERS the `dag.tables` namespace before any mutation. Guarding
            // `allocate_table_id` alone would not cover it: the register hooks take
            // the id straight off the ingested row and `raise_id_counter` it, and
            // the id is caller-chosen (a client may preset `circuit.view_id`), so a
            // crafted CREATE VIEW could otherwise register a durable view at or
            // above the ceiling — a conservative tripwire held safely short of the
            // u32 physical contract every id narrows to (see `RELATION_ID_CEILING`).
            // TABLE_TAB / VIEW_TAB are the only families whose PK is a
            // `dag.tables` id (index ids are a disjoint namespace —
            // `next_index_id`/`SEQ_ID_INDICES` — and index tables live in
            // `entry.index_circuits`, not `dag.tables`).
            if matches!(table_id, TABLE_TAB_ID | VIEW_TAB_ID) {
                let id = batch.get_pk(i) as i64;
                let ceiling = sys_tables::RELATION_ID_CEILING;
                if id >= ceiling {
                    return Err(format!(
                        "relation id {id} is at or above the relation-id ceiling ({ceiling})"
                    ));
                }
            }

            match table_id {
                SCHEMA_TAB_ID => {
                    let name = batch.read_payload_string(i, SCHEMATAB_PAY_NAME);
                    if self.caches.schema_by_name.contains_key(&name) {
                        return Err(format!("Schema already exists: {name}"));
                    }
                }
                TABLE_TAB_ID => {
                    let tid = batch.get_pk(i) as i64;
                    let (sid, name, pk, _flags) = read_table_tab_row(batch, i);

                    // Collect ColumnDefs, rejecting any gap/duplicate in the
                    // column-index sequence (would mismap columns downstream).
                    let col_defs = self.scan_column_defs(tid, true)?;
                    validate_relation_defs("table", tid, &name, &col_defs, &pk)?;

                    let first_pk = pk.as_slice()[0];
                    let self_pk_type = col_defs[first_pk as usize].type_code;
                    for cd in col_defs.iter().filter(|cd| cd.fk_table_id != 0) {
                        self.validate_fk_column(cd, tid, first_pk, self_pk_type)?;
                    }

                    self.precheck_qname_unique(sid, &name, tid)?;
                }
                VIEW_TAB_ID => {
                    let vid = batch.get_pk(i) as i64;
                    let (sid, name, pk) = read_view_tab_row(batch, i);

                    // Reject any gap/duplicate in the column-index sequence.
                    // Rejecting here — before apply_entity_caches mutates the
                    // caches — leaves clean state on a failed DDL, mirroring the
                    // TABLE_TAB arm; hook_view_register re-runs the same
                    // validator for the paths that skip precheck.
                    let col_defs = self.scan_column_defs(vid, true)?;
                    validate_relation_defs("view", vid, &name, &col_defs, &pk)?;
                    self.precheck_qname_unique(sid, &name, vid)?;
                }
                IDX_TAB_ID => {
                    let (owner_id, cols, _is_unique) = read_idx_tab_row(batch, i);
                    let index_name = batch.read_payload_string(i, IDXTAB_PAY_NAME);
                    let entry = self.validate_index_registration(owner_id, &cols)?;

                    // Bounds, per-column eligibility (STRING/BLOB/float), and
                    // arity/stride limits, identical to what registration will
                    // enforce — only the table-name context is added here.
                    make_index_schema(cols.as_slice(), &entry.schema).map_err(|e| {
                        format!(
                            "{} for table '{}' (tid={})",
                            e,
                            self.caches
                                .entity_by_id
                                .get(&owner_id)
                                .map(|(_, n)| n.as_str())
                                .unwrap_or("?"),
                            owner_id
                        )
                    })?;

                    let idx_id = batch.get_pk(i) as i64;
                    if let Some(&existing) = self.caches.index_by_name.get(&index_name) {
                        if existing != idx_id {
                            return Err(format!("Index already exists: {index_name}"));
                        }
                    }
                }
                _ => {}
            }
        }

        // -- Negative-weight (DROP) checks ------------------------------------
        // §3.3(B): re-key the drop integrity guards on PKs whose bundle net is
        // DEAD, not raw weight<0 — so a rename pair's net-live `-1` is excluded
        // and never rejected as "referenced by FK" / "View dependency". For
        // TABLE/VIEW that is the contract's `net_dead`; SCHEMA/INDEX carry no
        // rewrite pair, so raw weight<0 is already net-dead.
        let mut drop_ids: Vec<i64> = match family {
            SysFamily::Table | SysFamily::View => net_dead,
            _ => (0..batch.count)
                .filter(|&i| batch.get_weight(i) < 0)
                .map(|i| batch.get_pk(i) as i64)
                .collect(),
        };
        if drop_ids.is_empty() {
            return Ok(());
        }
        drop_ids.sort_unstable();
        drop_ids.dedup();

        // First DROP arm: reject a SCHEMA_TAB -1 on a non-empty schema. This is
        // the engine-side, caller-agnostic half of the DROP SCHEMA member
        // cascade — it runs before any WAL write, so a rejected non-empty drop
        // queues no dir deletion and retracts no rows, converting the silent
        // member-orphan into a loud error. The production client cascade
        // (`GnitzClient::drop_schema`) and the `#[cfg(test)]` engine cascade both
        // drop every member as prior, separate submissions, so by the time the
        // schema row reaches here its member caches for that sid are empty and
        // the guard passes. No cascade exemption is needed (unlike the IDX_TAB
        // guard below): a SCHEMA_TAB -1 is never submitted from inside an engine
        // cascade.
        if table_id == SCHEMA_TAB_ID {
            for &sid in &drop_ids {
                let n = self.schema_member_count(sid);
                if n > 0 {
                    return Err(format!("Schema not empty: {n} relation(s) remain; drop them first"));
                }
            }
            return Ok(());
        }

        // A UNIQUE index referenced by a FK, and any internal `__fk_` index the
        // RESTRICT seek depends on, are load-bearing — block their drop. A
        // DROP TABLE cascade legitimately retracts the owner's own indices, so
        // it is exempt (the table drop already passed its own precheck).
        if table_id == IDX_TAB_ID {
            if self.ctx.in_cascade_drop() {
                return Ok(());
            }
            for &idx_id in &drop_ids {
                if let Some(name) = self.caches.index_by_id.get(&idx_id) {
                    if name.contains(FK_INDEX_INFIX) {
                        return Err("Integrity violation: cannot drop an internal FK index".into());
                    }
                }
                let (owner_id, cols) = {
                    let mut cursor = self.sys_store(SysFamily::Index).open_cursor();
                    // sys_indices has a single U64 PK; OPK == big-endian.
                    cursor.seek_bytes(&(idx_id as u64).to_be_bytes());
                    if !cursor.valid || cursor.current_key_narrow() as u64 != idx_id as u64 {
                        continue;
                    }
                    (
                        cursor_read_u64(&cursor, IDXTAB_COL_OWNER_ID) as i64,
                        gnitz_wire::unpack_pk_cols(cursor_read_u64(&cursor, IDXTAB_COL_SOURCE_COLS)),
                    )
                };
                // FK backing is single-column: a composite index never satisfies
                // a single-column FK/uniqueness requirement, so dropping
                // one is never blocked by the FK-target guard.
                if cols.as_slice().len() != 1 {
                    continue;
                }
                let src_col = cols.as_slice()[0] as usize;
                if self
                    .fk_children_of(owner_id)
                    .iter()
                    .any(|r| r.parent_col_idx == src_col)
                {
                    // The FK target column must retain uniqueness for FK child
                    // inserts to validate. The drop is allowed when uniqueness is
                    // structurally preserved: the column is the lone PK (the PK
                    // itself guarantees uniqueness), or another unique secondary
                    // index survives the drop.
                    let is_lone_pk = self.dag.tables.get(&owner_id).is_some_and(|e| {
                        let pk = e.schema.pk_indices();
                        pk.len() == 1 && pk[0] as usize == src_col
                    });
                    if !is_lone_pk {
                        // Scan sys_indices (pre-drop: the rows being dropped are
                        // still present) for any other unique index on this column
                        // that would survive (exclude every id in this drop batch).
                        let mut unique_remains = false;
                        self.for_each_index_on_cols(owner_id, &[src_col as u32], |row_id, is_uniq| {
                            if is_uniq && drop_ids.binary_search(&row_id).is_err() {
                                unique_remains = true;
                            }
                        });
                        if !unique_remains {
                            let (sn, tn) = self.caches.entity_by_id.get(&owner_id).cloned().unwrap_or_default();
                            return Err(format!(
                                "Integrity violation: index on '{sn}.{tn}' is referenced by a \
                                 foreign key and no unique index would remain on the column"
                            ));
                        }
                    }
                }
            }
            return Ok(());
        }

        if table_id == TABLE_TAB_ID {
            for &tid in &drop_ids {
                // A FK child being co-dropped in this same batch is
                // self-resolving — only a child *outside* the batch blocks the
                // drop. Mirrors the view-dependency binary_search filter below.
                let blocking = self
                    .fk_children_of(tid)
                    .iter()
                    .find(|r| drop_ids.binary_search(&r.child_tid).is_err());
                if let Some(r) = blocking {
                    let (sn, tn) = self.caches.entity_by_id.get(&r.child_tid).cloned().unwrap_or_default();
                    return Err(format!("Integrity violation: table referenced by '{sn}.{tn}'"));
                }
            }
        }

        // The view-dependency guard applies only to TABLE/VIEW drops, never to a
        // SCHEMA_TAB drop. A schema drop is gated separately by the member-count
        // arm at the top of this section (which returns before reaching here); it
        // must not be dep-probed, because schema ids (allocated from
        // FIRST_USER_SCHEMA_ID = 3) share an i64 space with table ids (from
        // FIRST_USER_TABLE_ID = 16) — as both counters climb, a schema id
        // eventually equals an earlier table id, so probing the table-keyed
        // dep_map with a schema id would spuriously match an unrelated table's
        // dependents.
        if table_id == TABLE_TAB_ID || table_id == VIEW_TAB_ID {
            let dep_map = self.dag.get_dep_map();
            for &id in &drop_ids {
                if let Some(dependents) = dep_map.get(&id) {
                    // A dependent that is itself being dropped in this same
                    // batch is self-resolving — only an *outside* dependent
                    // blocks the drop. drop_ids is sorted+deduped above, so
                    // binary_search is O(N log M) vs the O(N·M) of `contains`.
                    let still_active = dependents
                        .iter()
                        .any(|&dep_id| drop_ids.binary_search(&dep_id).is_err());
                    if still_active {
                        let (sn, tn) = self.caches.entity_by_id.get(&id).cloned().unwrap_or_default();
                        return Err(format!("View dependency: entity '{sn}.{tn}'"));
                    }
                }
            }
        }
        Ok(())
    }

    /// Drain the pending-broadcast queue. Master calls this once per
    /// top-level DDL and forwards each entry to `broadcast_ddl`. Workers
    /// receive system-table changes via FLAG_DDL_SYNC → `ddl_sync`, which
    /// bypasses `ingest_to_family` entirely, so the queue stays empty there.
    pub fn drain_pending_broadcasts(&mut self) -> Vec<(i64, Batch)> {
        std::mem::take(&mut self.pending_broadcasts)
    }

    /// Physically remove a batch of queued directory paths. An existence guard
    /// keeps a re-queued path (drop applied, dir already gone) quiet.
    fn remove_queued_dirs(dirs: Vec<String>) {
        for dir in dirs {
            if std::path::Path::new(&dir).exists() {
                let _ = std::fs::remove_dir_all(&dir);
            }
        }
    }

    /// Physically remove the directories queued by table/view/index drop
    /// hooks. The executor calls this only after the DDL zone's fdatasync
    /// confirms the drop is durable — see `pending_dir_deletions`.
    pub(crate) fn drain_pending_dir_deletions(&mut self) {
        Self::remove_queued_dirs(std::mem::take(&mut self.pending_dir_deletions));
    }

    /// Drop the queued directory paths *without* deleting them. Used when a DDL
    /// fails or to clear stale entries left by a prior failed DDL — the entity
    /// did not durably drop, so its files must survive.
    pub fn discard_pending_dir_deletions(&mut self) {
        self.pending_dir_deletions.clear();
    }

    /// Move durably-dropped directories from the in-flight DDL queue into the
    /// checkpoint-gated queue instead of removing them now. See
    /// `checkpoint_gated_deletions`. Used on the DROP-success path where worker
    /// processes may still be applying the entity's CREATE.
    pub fn defer_pending_dir_deletions(&mut self) {
        self.checkpoint_gated_deletions.append(&mut self.pending_dir_deletions);
    }

    /// Physically remove every checkpoint-gated directory. SAFE only at a
    /// checkpoint boundary, after the per-worker FLAG_FLUSH ACKs prove all
    /// workers consumed past the DROP that queued each entry.
    pub fn drain_checkpoint_gated_deletions(&mut self) {
        Self::remove_queued_dirs(std::mem::take(&mut self.checkpoint_gated_deletions));
    }

    /// Cancel a pending removal of `dir` from *both* deletion queues. Required
    /// when an entity whose on-disk path is *name-based* (not `<name>_<tid>`) is
    /// recreated before the gating checkpoint drains the queue: only schemas
    /// have name-based paths (`<base>/<schema>`), so a `DROP SCHEMA s` +
    /// `CREATE SCHEMA s` would otherwise leave `<base>/s` queued, and the next
    /// checkpoint's `remove_dir_all` would wipe the recreated schema and every
    /// new table beneath it. Tables/indices encode a monotonic tid in their
    /// path, so a recreate never collides with a gated entry and needs no
    /// cancellation.
    ///
    /// Both queues are filtered: in normal operation the DROP and CREATE are
    /// separate DDL RPCs, so the DROP's entry was already moved to the gated
    /// queue by its own `defer` and clearing `pending_dir_deletions` is a no-op.
    /// During SAL recovery, however, a replayed `DROP s` and a replayed
    /// `CREATE s` land in the *same* `pending_dir_deletions` with no intervening
    /// `defer`; the CREATE's own pre-stage `truncate` removes only its push and
    /// leaves the DROP's `<base>/s` — the live, recreated path — in the queue.
    /// Clearing it here lets the recreating hook reclaim that residue before the
    /// boot-time `gc_orphan_directories` drain (or any later checkpoint) can
    /// `remove_dir_all` the live schema and the tables beneath it.
    pub(crate) fn cancel_gated_deletion(&mut self, dir: &str) {
        self.checkpoint_gated_deletions.retain(|d| d != dir);
        self.pending_dir_deletions.retain(|d| d != dir);
    }

    /// Remove table, view, and index directories on disk that belong to no live
    /// entity — the residue of a DROP whose checkpoint-gated deletion was lost to
    /// a crash before the next checkpoint drained it. Best-effort: a failure to
    /// remove one orphan is logged and never aborts recovery.
    ///
    /// Two reclamation mechanisms run here:
    /// - A schema-scoped path scan removes orphan table/view/index dirs under
    ///   every live schema (the drop-flushed-but-gating-checkpoint-missed window,
    ///   where the entity is absent from both the shard scan and the SAL, so
    ///   nothing re-queues its dir).
    /// - A `drain_pending_dir_deletions` removes every dir that SAL replay
    ///   re-queued (the drop-committed-to-SAL-but-unflushed window), including a
    ///   dropped schema subtree the path scan cannot reach because the schema is
    ///   gone from `schema_by_id`.
    ///
    /// Must run only after BOTH shard replay (`replay_catalog`) and SAL replay
    /// (`recover_system_tables_from_sal`) have populated `dag.tables`; otherwise a
    /// table whose CREATE committed to the SAL but was not yet flushed would be
    /// absent from `dag.tables` and its live directory wrongly deleted.
    ///
    /// Requires the `cancel_gated_deletion` fix that also clears
    /// `pending_dir_deletions`; without it the drain could remove a recreated
    /// same-name schema whose live path SAL replay left in the queue.
    pub(crate) fn gc_orphan_directories(&mut self) {
        // Full on-disk path of every live table/view (user + system).
        let live_tables: rustc_hash::FxHashSet<&str> = self.dag.tables.values().map(|e| e.directory.as_str()).collect();

        // Full on-disk path of every live index: `<owner_dir>/idx_<idx_id>`.
        let mut live_indices: rustc_hash::FxHashSet<String> = rustc_hash::FxHashSet::default();
        for (owner_id, idx_ids) in &self.caches.indices_by_owner {
            if let Some(owner) = self.dag.tables.get(owner_id) {
                for idx_id in idx_ids {
                    live_indices.insert(index_dir(&owner.directory, *idx_id));
                }
            }
        }

        // Scan only schemas the catalog knows about. We never enumerate
        // `base_dir` for unknown directories: a schema path is an arbitrary user
        // name with no structural marker (`<base>/<schema>`), so removing an
        // unrecognized entry could wipe unrelated host data if base_dir is
        // shared. The real system catalog (`<base>/_system_catalog`) is never a
        // registered schema name, so it is never reached; the `_system`/`public`
        // logical-schema dirs are scanned but the system tables live under
        // `_system_catalog`, so nothing system-owned is ever a candidate.
        for schema_name in self.caches.schema_by_id.values() {
            let schema_dir = schema_dir(&self.base_dir, schema_name);
            for name in subdir_names(&schema_dir) {
                let full = format!("{schema_dir}/{name}");

                if live_tables.contains(full.as_str()) {
                    // Live table/view: sweep orphaned `idx_<id>` sub-dirs left by
                    // a standalone DROP INDEX whose gated deletion was lost to a
                    // crash.
                    for idx_name in subdir_names(&full) {
                        if !is_index_dir_name(&idx_name) {
                            continue;
                        }
                        let idx_full = format!("{full}/{idx_name}");
                        if live_indices.contains(&idx_full) {
                            remove_stale_index_rank_dirs(&idx_full);
                            continue;
                        }
                        match std::fs::remove_dir_all(&idx_full) {
                            Ok(()) => gnitz_debug!("recovery: removed orphan index dir {}", idx_full),
                            Err(e) => gnitz_debug!("recovery: failed to remove orphan index dir {}: {}", idx_full, e),
                        }
                    }
                    continue;
                }

                // Defense in depth: only `<something>_<digits>` dirs — the shape
                // of table (`<name>_<tid>`) and view (`view_<name>_<vid>`)
                // creation — are eligible for removal. Never touch an unexpected
                // entry. The only component that writes a directory directly
                // under a schema dir is table/view creation, so a table-shaped
                // name absent from `live_tables` is necessarily an orphaned drop.
                if !is_table_dir_name(&name) {
                    continue;
                }
                match std::fs::remove_dir_all(&full) {
                    Ok(()) => gnitz_debug!("recovery: removed orphan table/view dir {}", full),
                    Err(e) => gnitz_debug!("recovery: failed to remove orphan dir {}: {}", full, e),
                }
            }
        }

        // SAL replay of any DROP fired hooks that re-pushed the dropped directory
        // onto `pending_dir_deletions` (the committed-but-unflushed crash window).
        // The schema-scoped scan above already removed the orphans under live
        // schemas, but a dropped *schema*'s subtree is unreachable by that scan
        // (the schema is gone from `schema_by_id`). Physically remove everything
        // the replay re-queued so those dirs are reclaimed and no recovery residue
        // is carried into the first DDL/checkpoint. Safe because the
        // `cancel_gated_deletion` fix guarantees no recreated same-name (live)
        // schema path survives in the queue.
        self.drain_pending_dir_deletions();
    }

    // -----------------------------------------------------------------------
    // Stage-A compensation (DDL rollback)
    // -----------------------------------------------------------------------

    /// Topological creation priority for the catalog family order (see
    /// `SysFamilyInfo::topo_priority`); 99 for a non-family tid.
    pub(crate) fn catalog_topo_priority(tid: i64) -> u8 {
        SysFamily::from_id(tid).map_or(99, |f| f.info().topo_priority)
    }

    /// Compensate a failed `DDL_TXN` bundle: undo every in-memory mutation that
    /// was applied before the failure so the catalog is exactly as it was, in
    /// master memory, before any worker sees a byte.
    ///
    /// The drained `pending_broadcasts` already holds every family that was
    /// applied **and enqueued** (the families before the failing one). The
    /// handler additionally passes the single family that was applied but **not
    /// yet enqueued** — a hook/panic failure inside `apply_and_enqueue_family` —
    /// as `applied_not_enqueued`, so it too is negated. On a **precheck** failure
    /// the handler passes `None`: nothing was applied for that family, so nothing
    /// is reconstructed and **no ghost `-1` is written**. At most one family is
    /// ever applied-not-enqueued, so `Option` is the exact type.
    pub(crate) fn compensate_stage_a(&mut self, applied_not_enqueued: Option<(i64, Batch)>) {
        let mut rollback_list = self.drain_pending_broadcasts();

        if let Some((tid, mut batch)) = applied_not_enqueued {
            batch.set_schema(sys_tab_schema(tid));
            rollback_list.push((tid, batch));
        }

        // Precheck-failed first family: nothing applied, trivial no-op.
        if rollback_list.is_empty() {
            return;
        }

        // Derive the rollback direction from the (weight-uniform) list: a bundle
        // is either a CREATE (all +1) or a DROP (all -1).
        let is_create = rollback_list
            .iter()
            .any(|(_, b)| (0..b.count).any(|i| b.get_weight(i) > 0));

        // §3.4: each family in the rollback list must be internally homogeneous
        // (a pure CREATE or pure DROP) OR fully paired — every negative PK also
        // appears positively in that same family and vice versa (a rewrite pair,
        // e.g. a rename). A rename's single COL/TABLE/VIEW family is fully paired;
        // `is_create` (computed above on the original un-negated weights) then
        // classifies it either way, since a pure rename pair leaves an empty
        // `pending_dir_deletions` (§3.2's reconciling hooks no-op the whole
        // registration), so both drain/discard are no-ops on the empty vec.
        debug_assert!(
            rollback_list.iter().all(|(_, b)| {
                let neg: Vec<u128> = (0..b.count)
                    .filter(|&i| b.get_weight(i) < 0)
                    .map(|i| b.get_pk(i))
                    .collect();
                let pos: Vec<u128> = (0..b.count)
                    .filter(|&i| b.get_weight(i) > 0)
                    .map(|i| b.get_pk(i))
                    .collect();
                let homogeneous = neg.is_empty() || pos.is_empty();
                let fully_paired = neg.iter().all(|pk| pos.contains(pk)) && pos.iter().all(|pk| neg.contains(pk));
                homogeneous || fully_paired
            }),
            "compensate_stage_a assumes each rollback family is weight-homogeneous \
             OR fully paired (a rename rewrite pair); a genuinely mixed CREATE/DROP \
             family would misclassify the rollback direction and mishandle \
             pending_dir_deletions"
        );

        // For CREATE rollback: dependents unregistered before dependencies — DESCENDING.
        // For DROP rollback: dependencies restored before dependents — ASCENDING.
        if is_create {
            rollback_list.sort_by_key(|(tid, _)| std::cmp::Reverse(Self::catalog_topo_priority(*tid)));
        } else {
            rollback_list.sort_by_key(|(tid, _)| Self::catalog_topo_priority(*tid));
        }

        // Replay each with negated weight through the no-broadcast path.
        // fire_hooks still fires so caches, dag.tables, and pending_dir_deletions
        // are updated. The rollback gate in `submit` ensures any cascade that
        // calls back into `submit` also bypasses broadcasts.
        let result = self.with_rollback_compensation(|s| -> Result<(), String> {
            for (tid, mut batch) in rollback_list {
                batch.map_weights(i64::wrapping_neg);
                let family = SysFamily::from_id(tid).ok_or_else(|| format!("rollback: unknown system family {tid}"))?;
                s.submit_local(family, batch)?;
            }
            Ok(())
        });

        // For CREATE: drain cleans up any pre-staged directories from hooks.
        // For DROP: discard keeps the entity files on disk (drop was not durable).
        if is_create {
            self.drain_pending_dir_deletions();
        } else {
            self.discard_pending_dir_deletions();
        }

        result.unwrap_or_else(|e| {
            gnitz_fatal_abort!(
                "Stage-A DDL compensation failed — catalog cannot be restored; \
                 aborting to prevent serving a diverged catalog. Cause: {}",
                e
            );
        });
    }
}
