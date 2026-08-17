//! Catalog write-path spine — the single ingest/apply pipeline every
//! system-table mutation flows through: `submit` → `precheck_family`
//! → `apply_local` → `fire_hooks`, plus the broadcast queue, the
//! directory-deletion queues, and Stage-A (DDL rollback) compensation.
//! `fire_hooks` lives in `hooks.rs`; `SysFamily` / `ApplyContext` in
//! `sys_tables.rs` / `apply_context.rs`. No second ingest entry point may
//! skip this precheck/hooks path.

use rustc_hash::FxHashMap;
use std::cmp::Ordering;
use std::num::NonZeroU64;

use super::*;
use crate::schema::make_index_schema;
use crate::storage::{compare_rows, compare_rows_except};
use gnitz_wire::{
    COLTAB_PAY_COL_IDX, COLTAB_PAY_FK_COL_IDX, COLTAB_PAY_FK_TABLE_ID, COLTAB_PAY_IS_HIDDEN, COLTAB_PAY_IS_NULLABLE,
    COLTAB_PAY_IS_SERIAL, COLTAB_PAY_NAME, COLTAB_PAY_OWNER_ID, COLTAB_PAY_OWNER_KIND, COLTAB_PAY_TYPE_CODE,
    IDXTAB_PAY_NAME, SCHEMATAB_PAY_NAME, TABTAB_PAY_NAME,
};

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
        self.precheck_family(family, &batch)?;
        self.apply_and_enqueue_family(family, batch)
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

/// Reject a mutation of a bootstrap-owned id in one of the catalog's id spaces
/// (`what` names it, `first_user` is its floor). The reject is a property of
/// the id space, not of the mutation's shape, so it covers every sign: a `-1`
/// drops a bootstrap row, a bare `+1` aliases a bootstrap id into the caches,
/// and a pair renames one. Runs before the CAS, so it fires whether or not the
/// family holds a live row at that id.
fn reject_system_id(sig: &PkSignature, what: &str, first_user: i64) -> Result<(), String> {
    let id = sig.pk as i64;
    if id < first_user {
        return Err(format!(
            "cannot {} a system {what} (id {id} < {first_user})",
            sig.verb()
        ));
    }
    Ok(())
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
        let table = self.sys_store_mut(family);
        // Live DDL's one sound non-fatal exit: a failure here is pre-broadcast
        // (nothing durable, nothing broadcast, client not ACKed), so Stage-A
        // compensation can unwind it. Propagate rather than abort.
        table
            .ingest_borrowed_batch(batch)
            .map_err(|e| format!("apply_local: sys-table ingest failed (family={id}): {e}"))?;
        if let Some(lsn) = pin_lsn {
            table.pin_lsn(lsn);
        }
        batch.set_schema(family.schema());
        self.fire_hooks(family, batch)
    }

    // -- System ingestion entry + precheck / broadcast / dir-deletion -------

    /// Ingest a batch into a table family (PK enforcement + store + index
    /// projection + hooks).
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
    pub(crate) fn apply_and_enqueue_family(&mut self, family: SysFamily, mut batch: Batch) -> Result<(), String> {
        self.apply_local(family, &mut batch, self.ctx.ddl_zone_lsn())?;
        if batch.count > 0 {
            self.pending_broadcasts.push((family, batch));
        }
        Ok(())
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
        let schema = family.schema();
        let batch = retract_single_row(self.sys_store(family), &schema, pk);
        if batch.count == 0 {
            return Err("Entity does not exist in catalog".into());
        }
        self.submit(family, batch)
    }

    /// Reject a CREATE whose qualified `schema.name` collides with an existing
    /// entity. Re-ingesting the same row (e.g. a pk-col update of `self_id`) is
    /// allowed; only a genuinely different entity under that name is rejected.
    ///
    /// `net_dead` is this same bundle's net-dead PKs in this same family. An
    /// incumbent among them is not a collision: one bundle may retire an id and
    /// register a different one under the same name — an ALTER VIEW is exactly
    /// that. Precheck reads the caches *before* apply, so they still map the name
    /// to the outgoing id.
    ///
    /// Also resolves `sid`, erroring if the schema does not exist.
    fn precheck_qname_unique(&self, sid: i64, name: &str, self_id: i64, net_dead: &[i64]) -> Result<(), String> {
        let schema_name = self
            .caches
            .schema_by_id
            .get(&sid)
            .ok_or_else(|| format!("Schema with ID {sid} does not exist"))?;
        let qualified = format!("{schema_name}.{name}");
        if let Some(&existing) = self.caches.entity_by_qname.get(&qualified) {
            if existing != self_id && !net_dead.contains(&existing) {
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
        if !cursor.advance_to_exact_live(pk_bytes) {
            return None;
        }
        let w = cursor.current_weight;
        let mut b = Batch::with_capacity(family.schema(), 1);
        cursor.copy_current_row_into(&mut b, w);
        Some((b, w))
    }

    /// The post-image retraction contract for a rewrite-pair-capable
    /// family (TABLE_TAB / VIEW_TAB / COL_TAB), per distinct PK:
    ///
    /// 1. **CAS** — every `-1` row must content-equal the current live row (you
    ///    may only retract the element that exists). Via the generic
    ///    `compare_rows`, which routes STRING/BLOB (`name`/`sql_definition`)
    ///    through each side's own blob heap, so a valid rename to a name > 12
    ///    bytes is accepted (a raw region `memcmp` would false-reject it) while a
    ///    stale-snapshot `-1` is rejected.
    /// 2. **Per-PK net** — `live_weight + Σ batch weights` must be 0 or 1 (sys
    ///    stores run no `enforce_unique_pk`, so nothing else stops a duplicate
    ///    live head or a persistent negative ghost).
    ///
    /// `noun` names the row kind in the messages. Returns the live row (for the
    /// caller's pair-field comparison) and the net weight; the per-family guards
    /// on top of this live in `precheck_relation_family` /
    /// `precheck_column_family`.
    fn check_cas_and_net(
        &self,
        family: SysFamily,
        batch: &Batch,
        sig: &PkSignature,
        noun: &str,
    ) -> Result<(Option<Batch>, i64), String> {
        let live = self.seek_live_sys_row(family, batch.get_pk_bytes(sig.row));

        if sig.neg.is_some() {
            let Some((lb, _)) = live.as_ref() else {
                return Err(format!(
                    "catalog changed concurrently: retracting a {noun} that no longer exists"
                ));
            };
            let schema = family.schema();
            for j in 0..batch.count {
                if batch.get_pk(j) == sig.pk
                    && batch.get_weight(j) < 0
                    && compare_rows(&schema, lb, 0, batch, j) != Ordering::Equal
                {
                    return Err(format!(
                        "catalog changed concurrently: the retracted {noun} differs from the current one"
                    ));
                }
            }
        }

        let net = live.as_ref().map_or(0, |(_, w)| *w) + sig.sum;
        if !(0..=1).contains(&net) {
            return Err(format!(
                "system-catalog write would leave {noun} {} at net weight {net} (expected 0 or 1)",
                sig.pk
            ));
        }
        Ok((live.map(|(b, _)| b), net))
    }

    /// The TABLE_TAB / VIEW_TAB precheck — the shared CAS + net contract plus
    /// the two relation-only guards: no mutation of a system-range id passes,
    /// whatever its sign, and a rewrite pair's `+1` may differ from the live row
    /// only in `name`. TABLE_TAB and VIEW_TAB agree on the name slot (asserted
    /// in gnitz-wire), so one constant serves both. `compare_rows_except` routes
    /// STRING/BLOB through each side's own blob heap, so a name > 12 bytes is
    /// compared by content.
    ///
    /// Returns the PKs whose net is dead (`≤ 0`) — the genuine drops — so the
    /// relation drop guards re-key on net-liveness (a rename's net-live `-1` is
    /// excluded) rather than raw batch weights.
    fn precheck_relation_signatures(&self, family: SysFamily, batch: &Batch) -> Result<Vec<i64>, String> {
        let schema = family.schema();
        let mut net_dead: Vec<i64> = Vec::new();
        for sig in pk_signatures(batch) {
            reject_system_id(&sig, "relation", FIRST_USER_TABLE_ID)?;

            let (live, net) = self.check_cas_and_net(family, batch, &sig, "system-catalog row")?;

            if sig.is_pair() {
                let lb = live.as_ref().expect("a pair's -1 CAS already required a live row");
                for j in 0..batch.count {
                    if batch.get_pk(j) == sig.pk
                        && batch.get_weight(j) > 0
                        && compare_rows_except(&schema, lb, 0, batch, j, 1 << TABTAB_PAY_NAME) != Ordering::Equal
                    {
                        return Err("a system-catalog rewrite pair may only change the name".into());
                    }
                }
            }

            if net <= 0 {
                net_dead.push(sig.pk as i64);
            }
        }
        Ok(net_dead)
    }

    /// Column-ALTER shape validation for the COL_TAB family, replacing the bare
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
    /// - **Owner guard** — every transition, RENAME included, requires a
    ///   registered user base table owner (not a view, not system-range).
    /// - **Transition-scoped guards** — scoped to the transition rather than
    ///   blanket, which would reject RENAME: an `is_hidden 0→1` (DROP COLUMN) or
    ///   `is_nullable 0→1` (DROP NOT NULL) pair additionally requires **no
    ///   dependent views** and that the column not be a PK column; a `name`-only
    ///   pair (RENAME COLUMN) is accepted with neither (views bind columns by
    ///   ordinal, and renaming a PK column is legal).
    /// - **Unpaired `-1`** on a registered owner is rejected (physical column
    ///   removal does not exist). **Unpaired `+1`** on a registered owner is
    ///   ADD COLUMN — see [`precheck_column_append`](Self::precheck_column_append).
    ///   An unregistered owner is a live CREATE TABLE COL append (the owner table
    ///   registers later in the bundle), so its `+1`-only rows pass untouched.
    fn precheck_column_family(&mut self, batch: &Batch) -> Result<(), String> {
        let schema = SysFamily::Column.schema();
        // The payload fields a rewrite pair may change; everything else must match.
        let pair_mask: u64 = (1 << COLTAB_PAY_NAME) | (1 << COLTAB_PAY_IS_HIDDEN) | (1 << COLTAB_PAY_IS_NULLABLE);
        for sig in pk_signatures(batch) {
            let pk = sig.pk;
            let (owner_id, col_idx) = gnitz_wire::unpack_col_id(pk as u64);
            let owner_id = owner_id as i64;

            // A live ALTER is exactly one rewrite pair; nothing legitimate
            // repeats a sign on one column PK.
            if sig.repeats_a_sign {
                return Err(format!(
                    "system-catalog write carries multiple same-sign rows for column {pk}"
                ));
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
                if sig.neg.is_some() {
                    return Err(format!(
                        "catalog changed concurrently: retracting a column of unregistered owner {owner_id}"
                    ));
                }
                continue;
            };

            self.check_cas_and_net(SysFamily::Column, batch, &sig, "system-catalog column")?;

            // Every column transition — RENAME, DROP, ADD — needs a user base
            // table owner, so every other `RelationKind` fails here.
            if !is_base {
                return Err(format!("cannot ALTER a column of {owner_id}: not a user base table"));
            }

            match (sig.neg, sig.pos) {
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
                        if owner_schema.pk_indices().contains(&(col_idx as u32)) {
                            return Err("cannot DROP COLUMN / DROP NOT NULL on a primary-key column".into());
                        }
                        self.reject_if_dependent_views(owner_id, "DROP COLUMN / DROP NOT NULL")?;
                    }
                }
                (Some(_), None) => {
                    return Err(
                        "cannot retract a column of a registered table (physical column removal is not supported)"
                            .into(),
                    );
                }
                (None, Some(pj)) => self.precheck_column_append(batch, pj, owner_id, col_idx, &owner_schema)?,
                (None, None) => {}
            }
        }
        Ok(())
    }

    /// Reject a column transition on `owner_id` while any view scans it: a
    /// compiled circuit's `ScanDelta` register schema is baked from the base
    /// descriptor and its operator traces hold re-keyed base rows at the old
    /// shape. Defense-in-depth; the friendly client-side reject is in
    /// `plan/alter.rs`.
    fn reject_if_dependent_views(&mut self, owner_id: i64, op: &str) -> Result<(), String> {
        if self.dag.get_dep_map().get(&owner_id).is_some_and(|v| !v.is_empty()) {
            return Err(format!(
                "cannot {op} on table {owner_id}: it has dependent views (drop them first)"
            ));
        }
        Ok(())
    }

    /// ADD COLUMN: one unpaired `+1` appending a trailing nullable payload
    /// column to registered base table `owner_id`.
    ///
    /// The caller's `check_cas_and_net` already closed the concurrent-append
    /// race with no second probe — `pack_col_id` is a pure function of the
    /// client-read physical column count, so two connections racing pick the
    /// *same* `column_id`, and the second's `+1` lands on a now-live row where
    /// `live_weight + Σ = 2` fails the per-PK net bound.
    fn precheck_column_append(
        &mut self,
        batch: &Batch,
        pj: usize,
        owner_id: i64,
        col_idx: u64,
        owner_schema: &SchemaDescriptor,
    ) -> Result<(), String> {
        self.reject_if_dependent_views(owner_id, "ADD COLUMN")?;
        // The append must be *trailing*, and this is the only check that makes
        // it so: the rebuild path reads the owner's defs with contiguity
        // checking off and maps them positionally, so a gap would silently shift
        // every column past it rather than fail. (A duplicate index is a
        // duplicate COL_TAB PK, which the net bound already rejects.)
        if !self.is_trailing_col_append(owner_id, col_idx) {
            return Err(format!(
                "cannot ADD COLUMN at index {col_idx} on table {owner_id}: \
                 columns must be appended at index {}",
                owner_schema.num_columns()
            ));
        }
        // Decode the appended row once; every rule below reads the decoded def.
        let appended = ColumnDef {
            name: batch.read_payload_string(pj, COLTAB_PAY_NAME),
            type_code: batch.read_payload_u64(pj, COLTAB_PAY_TYPE_CODE) as u8,
            is_nullable: batch.read_payload_u64(pj, COLTAB_PAY_IS_NULLABLE) != 0,
            fk_table_id: batch.read_payload_u64(pj, COLTAB_PAY_FK_TABLE_ID) as i64,
            fk_col_idx: batch.read_payload_u64(pj, COLTAB_PAY_FK_COL_IDX) as u32,
            is_serial: batch.read_payload_u64(pj, COLTAB_PAY_IS_SERIAL) != 0,
            is_hidden: batch.read_payload_u64(pj, COLTAB_PAY_IS_HIDDEN) != 0,
        };
        // Run the prospective column set through `check_col_defs` — the sole home
        // of the column-record rules — so a rule added there reaches ADD COLUMN
        // too. `validate_pk_cols` is not re-run: a trailing non-PK append cannot
        // invalidate an already-valid PK list.
        let mut prospective = (*self.read_column_defs(owner_id)).clone();
        prospective.push(appended.clone());
        check_col_defs(&prospective).map_err(|e| format!("cannot ADD COLUMN on table {owner_id}: {e}"))?;
        // A new column over existing rows is unconditionally nullable, carries
        // no SERIAL/FK, and is visible.
        if !appended.is_nullable {
            return Err("ADD COLUMN must append a nullable column".into());
        }
        if appended.is_serial || appended.is_hidden || appended.fk_table_id != 0 {
            return Err("ADD COLUMN must not append a SERIAL, hidden, or foreign-key column".into());
        }
        // Nothing else anchors these three on an unpaired row, and they are what
        // clients read back.
        if batch.read_payload_u64(pj, COLTAB_PAY_OWNER_KIND) as i64 != OWNER_KIND_TABLE
            || batch.read_payload_u64(pj, COLTAB_PAY_OWNER_ID) as i64 != owner_id
            || batch.read_payload_u64(pj, COLTAB_PAY_COL_IDX) != col_idx
        {
            return Err("an appended column's owner/index fields must match its packed id".into());
        }
        Ok(())
    }

    /// Visit every positive-weight `sys_indices` row whose owner and **exact
    /// column list** match `(owner_id, cols)`, invoking `f(index_id, is_unique)`
    /// for each. Centralises the IDX_TAB cursor walk shared by the DROP INDEX
    /// uniqueness checks (the drop-time FK guard in `precheck_family` and the
    /// post-retraction circuit demotion in `hook_index_register`). The persisted
    /// `source_cols` field is the packed `u64` (flag bit 63 set for the packed
    /// form), so decode it via `unpack_pk_cols` and compare ordered lists — a
    /// bare compare would never match a packed row. Rows that have already netted
    /// to zero weight are skipped by the cursor.
    pub(crate) fn for_each_index_on_cols(&self, owner_id: i64, cols: &[u32], mut f: impl FnMut(i64, bool)) {
        let mut cursor = self.sys_store(SysFamily::Index).open_cursor();
        while cursor.valid {
            if cursor.current_weight > 0 {
                let (row_owner, row_cols, is_uniq) = read_idx_tab_cursor_row(&cursor);
                if row_owner == owner_id && row_cols.as_slice() == cols {
                    f(cursor.current_key_narrow() as u64 as i64, is_uniq);
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
            return Err(format!(
                "Index: owner {owner_id} is a {}; only a base table can be indexed",
                entry.kind.noun()
            ));
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
    ///
    /// Exhaustive over `SysFamily` (like `fire_hooks`): a newly-added family
    /// must decide here whether it carries precheck guards, rather than falling
    /// into a silent `_` arm.
    pub(crate) fn precheck_family(&mut self, family: SysFamily, batch: &Batch) -> Result<(), String> {
        match family {
            SysFamily::Schema => self.precheck_schema_family(batch),
            SysFamily::Table | SysFamily::View => self.precheck_relation_family(family, batch),
            SysFamily::Column => {
                // Whole-table COL retractions during a DROP TABLE/VIEW cascade run
                // while the owner is still registered and are unpaired `-1`s, which
                // the arm would reject — the cascade wraps them in `with_cascade_drop`.
                if self.ctx.in_cascade_drop() {
                    return Ok(());
                }
                self.precheck_column_family(batch)
            }
            SysFamily::Index => self.precheck_index_family(batch),
            SysFamily::Sequence | SysFamily::CircuitNodes | SysFamily::CircuitEdges | SysFamily::CircuitNodeColumns => {
                Ok(())
            }
        }
    }

    /// SCHEMA_TAB: the per-PK CAS + net contract, then a CREATE must not collide
    /// with a live schema name and a DROP must find the schema empty.
    ///
    /// The CAS is what lets `apply_schema_caches` and `hook_schema_dir` act on the
    /// batch payload's name: an applied `-1` carries the retracted row's own name,
    /// so neither can be aimed at a different live schema.
    ///
    /// The empty-schema guard is the engine-side, caller-agnostic half of the
    /// DROP SCHEMA member cascade — it runs before any WAL write, so a rejected
    /// non-empty drop queues no dir deletion and retracts no rows, converting a
    /// silent member-orphan into a loud error. Both cascading callers drop every
    /// member as prior, separate submissions, so by the time the schema row
    /// reaches here its member set is empty and the guard passes. No cascade
    /// exemption is needed (unlike IDX_TAB): a SCHEMA_TAB `-1` is never
    /// submitted from inside an engine cascade.
    ///
    /// Schema ids share an i64 space with relation ids, so a schema drop must
    /// NOT be probed against the relation-keyed dep map — the member count is
    /// the whole guard.
    fn precheck_schema_family(&mut self, batch: &Batch) -> Result<(), String> {
        for sig in pk_signatures(batch) {
            reject_system_id(&sig, "schema", FIRST_USER_SCHEMA_ID)?;
            self.check_cas_and_net(SysFamily::Schema, batch, &sig, "system-catalog schema")?;
        }
        for i in 0..batch.count {
            if batch.get_weight(i) > 0 {
                let name = batch.read_payload_string(i, SCHEMATAB_PAY_NAME);
                if self.has_schema(&name) {
                    return Err(format!("Schema already exists: {name}"));
                }
            } else {
                let n = self.schema_member_count(batch.get_pk(i) as i64);
                if n > 0 {
                    return Err(format!("Schema not empty: {n} relation(s) remain; drop them first"));
                }
            }
        }
        Ok(())
    }

    /// TABLE_TAB / VIEW_TAB: the retraction contract, then the CREATE
    /// guards (relation-id ceiling, column-record admissibility, FK column
    /// types, qualified-name uniqueness) and the DROP guards (FK children, view
    /// dependents).
    ///
    /// The drop guards key on PKs whose bundle net is DEAD, not raw
    /// `weight < 0` — so a rename pair's net-live `-1` is never rejected as
    /// "referenced by FK" / "View dependency".
    fn precheck_relation_family(&mut self, family: SysFamily, batch: &Batch) -> Result<(), String> {
        let is_table = family == SysFamily::Table;
        let net_dead = self.precheck_relation_signatures(family, batch)?;

        for i in 0..batch.count {
            if batch.get_weight(i) <= 0 {
                continue;
            }
            let id = batch.get_pk(i) as i64;

            // The relation-id ceiling is enforced here — the one place an id
            // ENTERS the `dag.tables` namespace before any mutation. Guarding
            // `allocate_table_id` alone would not cover it: the register hooks
            // take the id straight off the ingested row and `raise_id_counter`
            // it, and the id is caller-chosen (a client may preset
            // `circuit.view_id`), so a crafted CREATE VIEW could otherwise
            // register a durable view at or above the ceiling. TABLE_TAB /
            // VIEW_TAB are the only families whose PK is a `dag.tables` id.
            let ceiling = sys_tables::RELATION_ID_CEILING;
            if id >= ceiling {
                return Err(format!(
                    "relation id {id} is at or above the relation-id ceiling ({ceiling})"
                ));
            }

            // Reject any gap/duplicate in the column-index sequence, which would
            // mismap columns downstream. Rejecting here — before the appliers
            // mutate the caches — leaves clean state on a failed DDL; the
            // register hooks re-run the same validator for the paths that skip
            // precheck (boot replay, worker ddl_sync).
            let col_defs = self.scan_column_defs(id, true)?;
            let (sid, name, pk, kind) = if is_table {
                let (sid, name, pk, flags) = read_table_tab_row(batch, i);
                (sid, name, pk, RelationKind::from_table_flags(flags))
            } else {
                let (sid, name, pk, _capacity) = read_view_tab_row(batch, i);
                (sid, name, pk, RelationKind::View)
            };
            validate_relation_defs(kind, id, &name, &col_defs, &pk)?;

            if is_table {
                // A stream push must stay a pure append: SERIAL would draw from a
                // durable sequence and an FK would probe a parent store, putting a
                // catalog write or a store read on every one.
                if kind == RelationKind::Stream {
                    if let Some(cd) = col_defs.iter().find(|cd| cd.is_serial || cd.fk_table_id != 0) {
                        return Err(format!(
                            "relation {id} is a stream: column '{}' may not be SERIAL or carry a FOREIGN KEY",
                            cd.name
                        ));
                    }
                }
                let self_pk_type = col_defs[pk.as_slice()[0] as usize].type_code;
                for cd in col_defs.iter().filter(|cd| cd.fk_table_id != 0) {
                    self.validate_fk_column(cd, id, pk.as_slice(), self_pk_type)?;
                }
            }

            self.precheck_qname_unique(sid, &name, id, &net_dead)?;
        }

        let mut drop_ids = net_dead;
        if drop_ids.is_empty() {
            return Ok(());
        }
        drop_ids.sort_unstable();
        drop_ids.dedup();

        if is_table {
            for &tid in &drop_ids {
                // A FK child being co-dropped in this same batch is
                // self-resolving — only a child *outside* the batch blocks the
                // drop. Mirrors the view-dependency filter below.
                let blocking = self
                    .fk_children_of(tid)
                    .iter()
                    .find(|r| drop_ids.binary_search(&r.child_tid).is_err());
                if let Some(r) = blocking {
                    let (sn, tn) = self.qualified_name_or_unknown(r.child_tid);
                    return Err(format!("Integrity violation: table referenced by '{sn}.{tn}'"));
                }
            }
        }

        let dep_map = self.dag.get_dep_map();
        for &id in &drop_ids {
            if let Some(dependents) = dep_map.get(&id) {
                // A dependent that is itself being dropped in this same batch is
                // self-resolving — only an *outside* dependent blocks the drop.
                // drop_ids is sorted+deduped, so binary_search is O(N log M)
                // vs the O(N·M) of `contains`.
                let still_active = dependents
                    .iter()
                    .any(|&dep_id| drop_ids.binary_search(&dep_id).is_err());
                if still_active {
                    let (sn, tn) = self.qualified_name_or_unknown(id);
                    return Err(format!("View dependency: entity '{sn}.{tn}'"));
                }
            }
        }
        Ok(())
    }

    /// IDX_TAB: a CREATE must name a base-table owner with an admissible column
    /// list and a free index name; a DROP must pass the per-PK CAS + net contract
    /// and must not strip the uniqueness an FK depends on.
    ///
    /// The CAS is what lets `apply_index_caches` unmap `index_by_name` by the
    /// batch payload's name: an applied `-1` carries the retracted row's own name,
    /// so it cannot unmap a different live index.
    ///
    /// IDX_TAB carries no rewrite pair, so raw `weight < 0` is already net-dead.
    fn precheck_index_family(&mut self, batch: &Batch) -> Result<(), String> {
        for i in 0..batch.count {
            if batch.get_weight(i) <= 0 {
                continue;
            }
            let (owner_id, cols, _is_unique) = read_idx_tab_row(batch, i);
            let index_name = batch.read_payload_string(i, IDXTAB_PAY_NAME);
            let entry = self.validate_index_registration(owner_id, &cols)?;

            // Bounds, per-column eligibility (STRING/BLOB/float), and
            // arity/stride limits, identical to what registration will enforce —
            // only the table-name context is added here.
            make_index_schema(cols.as_slice(), &entry.schema).map_err(|e| {
                format!(
                    "{e} for table '{}' (tid={owner_id})",
                    self.qualified_name_or_unknown(owner_id).1
                )
            })?;

            let idx_id = batch.get_pk(i) as i64;
            if let Some(&existing) = self.caches.index_by_name.get(&index_name) {
                if existing != idx_id {
                    return Err(format!("Index already exists: {index_name}"));
                }
            }
        }

        // A DROP TABLE cascade legitimately retracts the owner's own indices, so
        // it is exempt (the table drop already passed its own precheck).
        if self.ctx.in_cascade_drop() {
            return Ok(());
        }
        // After the exemption, like the Column family's: a cascade's `-1` is a
        // `retract_single_row` copy of the very row the CAS re-reads, so the
        // contract could reject nothing there.
        for sig in pk_signatures(batch) {
            self.check_cas_and_net(SysFamily::Index, batch, &sig, "system-catalog index")?;
        }
        let mut drop_ids: Vec<i64> = (0..batch.count)
            .filter(|&i| batch.get_weight(i) < 0)
            .map(|i| batch.get_pk(i) as i64)
            .collect();
        if drop_ids.is_empty() {
            return Ok(());
        }
        drop_ids.sort_unstable();
        drop_ids.dedup();

        let schema = SysFamily::Index.schema();
        for &idx_id in &drop_ids {
            // An internal `__fk_` index backs the RESTRICT seek; dropping one
            // would silently disarm FK enforcement.
            if self
                .caches
                .index_by_id
                .get(&idx_id)
                .is_some_and(|n| n.contains(FK_INDEX_INFIX))
            {
                return Err("Integrity violation: cannot drop an internal FK index".into());
            }
            let (owner_id, cols) = {
                let mut cursor = self.sys_store(SysFamily::Index).open_cursor();
                if !cursor.advance_to_exact_live(sys_opk(&schema, idx_id as u128).pk_bytes()) {
                    continue;
                }
                let (owner_id, cols, _) = read_idx_tab_cursor_row(&cursor);
                (owner_id, cols)
            };
            // FK backing is single-column: a composite index never satisfies a
            // single-column FK/uniqueness requirement, so dropping one is never
            // blocked by the FK-target guard.
            if cols.as_slice().len() != 1 {
                continue;
            }
            let src_col = cols.as_slice()[0] as usize;
            if !self.fk_children_of(owner_id).iter().any(|r| r.parent_col == src_col) {
                continue;
            }
            // The FK target column must retain uniqueness for FK child inserts
            // to validate. The drop is allowed when uniqueness is structurally
            // preserved: the column is the lone PK, or another unique secondary
            // index survives the drop.
            let is_lone_pk = self.dag.tables.get(&owner_id).is_some_and(|e| {
                let pk = e.schema.pk_indices();
                pk.len() == 1 && pk[0] as usize == src_col
            });
            if is_lone_pk {
                continue;
            }
            // Scan sys_indices (pre-drop: the rows being dropped are still
            // present) for another unique index on this column that survives.
            let mut unique_remains = false;
            self.for_each_index_on_cols(owner_id, &[src_col as u32], |row_id, is_uniq| {
                if is_uniq && drop_ids.binary_search(&row_id).is_err() {
                    unique_remains = true;
                }
            });
            if !unique_remains {
                let (sn, tn) = self.qualified_name_or_unknown(owner_id);
                return Err(format!(
                    "Integrity violation: index on '{sn}.{tn}' is referenced by a \
                     foreign key and no unique index would remain on the column"
                ));
            }
        }
        Ok(())
    }

    /// Drain the pending-broadcast queue. Master calls this once per
    /// top-level DDL and forwards each entry to `broadcast_ddl`. Workers
    /// receive system-table changes via FLAG_DDL_SYNC → `ddl_sync`, which
    /// bypasses `ingest_to_family` entirely, so the queue stays empty there.
    pub fn drain_pending_broadcasts(&mut self) -> Vec<(SysFamily, Batch)> {
        std::mem::take(&mut self.pending_broadcasts)
    }

    /// Physically remove a batch of queued directory paths. An existence guard
    /// keeps a re-queued path (drop applied, dir already gone) quiet.
    pub(super) fn remove_queued_dirs(dirs: Vec<String>) {
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
        // Named from each circuit's `index_id` — the id the directory was
        // created under — and not from `caches.indices_by_owner`, which holds
        // every IDX_TAB id. The two diverge when a second index registers on a
        // column list already covered and the first is then dropped: the circuit
        // and its directory survive under the first registrant's id, so a cache
        // read would find no live entry and remove a live index tree.
        let mut live_indices: rustc_hash::FxHashSet<String> = rustc_hash::FxHashSet::default();
        for entry in self.dag.tables.values() {
            for ic in &entry.index_circuits {
                live_indices.insert(index_dir(&entry.directory, ic.index_id));
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
                            // Its per-worker children are `reconcile_child_dirs`'
                            // job — the sweep descends into an index dir.
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
                // of table (`t_<tid>`), view (`v_<vid>`), and the pre-flight's
                // throwaway root (`_preflight_<vid>`) — are eligible for removal.
                // Never touch an unexpected entry. Those three are the only
                // writers directly under a schema dir, so a matching name absent
                // from `live_tables` is orphaned either way.
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

    /// Compile a just-registered view's circuit and throw the result away, so a
    /// circuit the engine cannot run is rejected while the DDL is still undoable.
    /// The path is built here because `catalog::utils` owns every entity
    /// directory's shape, and `query` sits below it.
    pub(crate) fn preflight_view_compile(&self, vid: i64) -> Result<(), String> {
        let Some((schema_name, _)) = self.caches.entity_by_id.get(&vid) else {
            return Err(format!("pre-flight: view {vid} is not registered"));
        };
        let root = preflight_dir(&self.base_dir, schema_name, vid);
        self.dag.preflight_compile(vid, &root).map_err(|e| e.to_string())
    }

    // -----------------------------------------------------------------------
    // Stage-A compensation (DDL rollback)
    // -----------------------------------------------------------------------

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
    pub(crate) fn compensate_stage_a(&mut self, applied_not_enqueued: Option<(SysFamily, Batch)>) {
        let mut rollback_list = self.drain_pending_broadcasts();

        if let Some((family, mut batch)) = applied_not_enqueued {
            batch.set_schema(family.schema());
            rollback_list.push((family, batch));
        }

        // Precheck-failed first family: nothing applied, trivial no-op.
        if rollback_list.is_empty() {
            return;
        }

        // Directories of entities the bundle DROPPED are queued for removal, and
        // this rollback restores those entities — so their files must survive.
        // (A hook that staged a directory and then failed already reclaimed it
        // itself; see `with_staged_dir`.) What the rollback queues *below* is a
        // different thing: residue of a creation that never committed.
        self.discard_pending_dir_deletions();

        // Undo each PK by what the bundle did to *it*, not by what the
        // bundle did overall — one family can do both. An ALTER VIEW retires the
        // old vid and registers the new chain in a single VIEW_TAB batch, so the
        // rollback has to tear one down and restore the other, and the two need
        // opposite family orders.
        //
        // A PK carrying both signs is a rewrite pair (a rename): its net stays
        // live, so it counts as a creation and its rows stay in ONE submit. Split
        // across the two phases they would drop the entity's net weight to zero
        // mid-rollback, firing the teardown hook and queueing the live entity's
        // directory for removal.
        let mut undo_create: Vec<(SysFamily, Batch)> = Vec::new();
        let mut undo_drop: Vec<(SysFamily, Batch)> = Vec::new();
        for (family, batch) in rollback_list {
            let mut net: FxHashMap<u128, i64> = FxHashMap::default();
            for i in 0..batch.count {
                *net.entry(batch.get_pk(i)).or_default() += batch.get_weight(i);
            }
            let (created, dropped): (Vec<u32>, Vec<u32>) =
                (0..batch.count as u32).partition(|&i| net[&batch.get_pk(i as usize)] >= 0);
            if dropped.is_empty() {
                undo_create.push((family, batch));
            } else if created.is_empty() {
                undo_drop.push((family, batch));
            } else {
                let schema = family.schema();
                let mem = batch.as_mem_batch();
                undo_create.push((family, Batch::from_indexed_rows(&mem, &created, &[], &schema)));
                undo_drop.push((family, Batch::from_indexed_rows(&mem, &dropped, &[], &schema)));
            }
        }

        // Tear down what the bundle created — dependents before dependencies,
        // DESCENDING…
        undo_create.sort_by_key(|(f, _)| std::cmp::Reverse(f.topo_priority()));
        // …then restore what it dropped — dependencies before dependents,
        // ASCENDING, so a restored view finds its columns, deps, and circuit rows
        // already back when its own VIEW_TAB row re-registers it. Creations first,
        // so a name the bundle moved from one id to another is free again by the
        // time the incumbent reclaims it.
        undo_drop.sort_by_key(|(f, _)| f.topo_priority());
        undo_create.append(&mut undo_drop);

        // Replay each with negated weight through the no-broadcast path.
        // fire_hooks still fires so caches, dag.tables, and pending_dir_deletions
        // are updated. The rollback gate in `submit` ensures any cascade that
        // calls back into `submit` also bypasses broadcasts.
        let result = self.with_rollback_compensation(|s| -> Result<(), String> {
            for (family, mut batch) in undo_create {
                batch.map_weights(i64::wrapping_neg);
                s.submit_local(family, batch)?;
            }
            Ok(())
        });

        // Everything the rollback queued is a creation that never committed.
        self.drain_pending_dir_deletions();

        result.unwrap_or_else(|e| {
            gnitz_fatal_abort!(
                "Stage-A DDL compensation failed — catalog cannot be restored; \
                 aborting to prevent serving a diverged catalog. Cause: {}",
                e
            );
        });
    }
}
