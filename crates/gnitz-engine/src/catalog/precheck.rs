//! Catalog precheck — the read-only validation every live system-table write
//! passes before any of it is applied. `submit` runs it, and the `DDL_TXN`
//! handler runs it as its own step so a precheck rejection (which wrote nothing)
//! stays distinguishable from a post-apply failure (which must be compensated).
//!
//! The paths that skip it do so because their rows were validated when first
//! written: boot shard replay, SAL recovery, and worker `ddl_sync`. The register
//! hooks in `hooks.rs` therefore re-check what they structurally depend on;
//! everything here is the master's trust boundary against a client that can push
//! arbitrary system-table deltas.

use std::cmp::Ordering;

use rustc_hash::FxHashSet;

use super::*;
use crate::schema::make_index_schema;
use crate::storage::{compare_rows, compare_rows_except};
use gnitz_wire::{
    COLTAB_PAY_COL_IDX, COLTAB_PAY_IS_HIDDEN, COLTAB_PAY_IS_NULLABLE, COLTAB_PAY_NAME, COLTAB_PAY_OWNER_ID,
    COLTAB_PAY_OWNER_KIND, IDXTAB_PAY_NAME, SCHEMATAB_PAY_NAME, TABTAB_PAY_NAME,
};

/// Reject a mutation of a bootstrap-owned id in one of the catalog's id spaces
/// (`first_user` is its floor). The reject is a property of the id space, not of
/// the mutation's shape, so it covers every sign: a `-1` drops a bootstrap row, a
/// bare `+1` aliases a bootstrap id into the caches, and a pair renames one. Runs
/// before the CAS, so it fires whether or not the family holds a live row at that
/// id.
fn reject_system_id(sig: &PkSignature, family: SysFamily, first_user: i64) -> Result<(), String> {
    let id = sig.pk as i64;
    if id < first_user {
        return Err(format!(
            "cannot {} a system {} (id {id} < {first_user})",
            sig.verb(),
            family.row_noun()
        ));
    }
    Ok(())
}

impl CatalogEngine {
    /// Check every FK-carrying column of a relation about to be registered.
    /// `pk` must already have passed `validate_relation_defs`, which is what
    /// makes `pk[0]` an in-bounds, PK-eligible column index.
    pub(super) fn validate_fk_columns(&self, tid: i64, col_defs: &[ColumnDef], pk: &[u32]) -> Result<(), String> {
        let self_pk_type = col_defs[pk[0] as usize].type_code;
        for cd in col_defs.iter().filter(|cd| cd.fk_table_id != 0) {
            self.validate_fk_column(cd, tid, pk, self_pk_type)?;
        }
        Ok(())
    }

    fn validate_fk_column(
        &self,
        col: &ColumnDef,
        self_table_id: i64,
        self_pk: &[u32],
        self_pk_type: u8,
    ) -> Result<(), String> {
        // `col.fk_col_idx` here is the PARENT's referenced column index (the
        // planner sets the child column's fk_col_idx to it). The target is a
        // legal reference iff it is the parent's lone PK column, or it carries
        // its own UNIQUE index. Mirrors the production planner gate.
        let target_type = if col.fk_table_id == self_table_id {
            // Self-referential FK: the table has no UNIQUE index yet, so the
            // target must be its lone PK column. The downstream probe reads the
            // referenced value out of the packed PK region, which is the whole
            // key only when the PK is a single column.
            if self_pk != [col.fk_col_idx] {
                return Err("FK must reference the primary key or a UNIQUE-indexed column".into());
            }
            self_pk_type
        } else {
            let entry = self
                .dag
                .tables
                .get(&col.fk_table_id)
                .ok_or_else(|| format!("FK references unknown table_id {}", col.fk_table_id))?;
            // Not covered by the PK/UNIQUE tests below, which read only the schema:
            // a stream and a view both have a PK that looks exactly like a base
            // table's without being the unique, stored key the parent probe reads.
            if !entry.kind.is_base_table() {
                return Err(format!(
                    "FK references relation {}, which is a {}; a FOREIGN KEY must reference a base table",
                    col.fk_table_id,
                    entry.kind.noun()
                ));
            }
            let pk = entry.schema.pk_indices();
            let is_lone_pk = pk.len() == 1 && pk[0] == col.fk_col_idx;
            if !is_lone_pk {
                // A composite index does not satisfy a single-column FK: a
                // unique (a, b) does not guarantee uniqueness of `a` alone, so
                // match only a single-column unique index on the referenced col.
                let has_unique = entry
                    .index_circuits
                    .iter()
                    .any(|ic| ic.unique_cols() == Some(&[col.fk_col_idx][..]));
                if !has_unique {
                    return Err("FK must reference the primary key or a UNIQUE-indexed column".into());
                }
            }
            entry.schema.columns[col.fk_col_idx as usize].type_code
        };

        // Promote BOTH sides before comparing. `index_key_type` maps each
        // ≤8-byte int to its index-key code (signed I8..I64 → I64, unsigned
        // U8..U64 → U64) and is idempotent on the already-promoted widths.
        // Comparing the promoted child against the parent's raw type_code would
        // wrongly reject identical-type FKs once a narrower signed column
        // promotes to I64.
        let promoted = gnitz_wire::index_key_type(col.type_code)?;
        let target_promoted = gnitz_wire::index_key_type(target_type)?;
        if promoted != target_promoted {
            return Err(format!(
                "FK type mismatch: promoted code {promoted} vs target {target_promoted}"
            ));
        }
        Ok(())
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
    /// Also resolves `sid`, erroring if the schema does not exist. `claimed`
    /// collects the qualified names this same batch has already registered, so
    /// two `+1` rows naming one relation under different ids are rejected too —
    /// unchecked, both would register and the second would overwrite
    /// `entity_by_qname`, stranding the first's store under a name nothing
    /// resolves.
    fn precheck_qname_unique(
        &self,
        sid: i64,
        name: &str,
        self_id: i64,
        net_dead: &[i64],
        claimed: &mut FxHashSet<String>,
    ) -> Result<(), String> {
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
        if !claimed.insert(qualified.clone()) {
            return Err(format!("Table or view already exists: {qualified}"));
        }
        Ok(())
    }

    /// Materialize the live (net-positive) row for the OPK `pk_bytes` in
    /// `family`'s store into a 1-row batch, with its net weight. `None` if no
    /// live row. Reads only — the `copy_current_row_into` precedent is
    /// `retract_single_row`. `ReadCursor` is not a `ColumnarSource`, so the CAS
    /// needs the row materialized into a `Batch` before `compare_rows`.
    fn seek_live_sys_row(&self, family: SysFamily, pk_bytes: &[u8]) -> Option<(Batch, i64)> {
        let mut cursor = self.sys_store(family).open_cursor_in_range(pk_bytes, Some(pk_bytes));
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
    /// Returns the live row (for the caller's pair-field comparison) and the net
    /// weight; the per-family guards on top of this live in
    /// `precheck_relation_family` / `precheck_column_family`.
    fn check_cas_and_net(
        &self,
        family: SysFamily,
        batch: &Batch,
        sig: &PkSignature,
    ) -> Result<(Option<Batch>, i64), String> {
        let noun = family.row_noun();
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
            reject_system_id(&sig, family, FIRST_USER_TABLE_ID)?;

            let (live, net) = self.check_cas_and_net(family, batch, &sig)?;

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
    /// - **CAS + net**: the `-1` byte-equals the live row; per-PK `net ∈ {0,1}`.
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

            self.check_cas_and_net(SysFamily::Column, batch, &sig)?;

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
                        if owner_schema.is_pk_col(col_idx as usize) {
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
    /// The caller's `check_cas_and_net` already closes the concurrent-append
    /// race: `pack_col_id` is a pure function of the client-read physical column
    /// count, so two connections racing pick the *same* `column_id`, and the
    /// second's `+1` lands on a now-live row where `live_weight + Σ = 2` fails
    /// the per-PK net bound.
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
        let appended = read_col_tab_row(batch, pj);
        // Run the prospective column set through `check_col_defs` — the sole home
        // of the column-record rules — so a rule added there reaches ADD COLUMN
        // too. The PK rules are not re-run here: a trailing non-PK append cannot
        // invalidate an already-valid PK list, and the rebuild in
        // `hook_column_alter` re-checks them against the new defs regardless.
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
    pub(super) fn for_each_index_on_cols(&self, owner_id: i64, cols: &[u32], mut f: impl FnMut(i64, bool)) {
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
    /// the read-only half of [`Self::submit`]. Covers both
    /// positive-weight (CREATE) invariants and negative-weight (DROP) integrity
    /// guards so that no invalid state is ever written. Also called directly by
    /// the `DDL_TXN` handler, which prechecks a family, sets its rollback
    /// marker, then applies it — leaving the marker `None` iff the precheck
    /// failed (so a precheck rejection reconstructs no ghost row on rollback).
    ///
    /// Exhaustive over `SysFamily` (like `fire_hooks`): a newly-added family
    /// must decide here whether it carries precheck guards, rather than falling
    /// into a silent `_` arm.
    pub fn precheck_family(&mut self, family: SysFamily, batch: &Batch) -> Result<(), String> {
        match family {
            SysFamily::Schema => self.precheck_schema_family(batch),
            SysFamily::Table | SysFamily::View => self.precheck_relation_family(family, batch),
            SysFamily::Column => self.precheck_column_family(batch),
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
            reject_system_id(&sig, SysFamily::Schema, FIRST_USER_SCHEMA_ID)?;
            self.check_cas_and_net(SysFamily::Schema, batch, &sig)?;
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
        let mut claimed: FxHashSet<String> = FxHashSet::default();

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
            // mutate the caches — leaves clean state on a failed DDL. This is the
            // only place the contiguity rule runs: the register hooks read the
            // owner's defs with the check off and map them positionally, so the
            // paths that skip precheck (boot replay, worker ddl_sync) rely on it
            // having held when the rows were first written.
            let col_defs = self.scan_column_defs(id, true)?;
            let (sid, name, pk, kind) = if is_table {
                let (sid, name, pk, flags) = read_table_tab_row(batch, i);
                (sid, name, pk, RelationKind::from_table_flags(flags))
            } else {
                let (sid, name, pk, _capacity, _delta) = read_view_tab_row(batch, i);
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
                // `validate_relation_defs` above proved the PK list non-empty
                // and in range, which is what makes the self-reference type
                // lookup inside sound.
                self.validate_fk_columns(id, &col_defs, pk.as_slice())?;
            }

            self.precheck_qname_unique(sid, &name, id, &net_dead, &mut claimed)?;
        }

        // One entry per distinct PK already (`pk_signatures`); sorted here only so
        // the two guards below can `binary_search` it.
        let mut drop_ids = net_dead;
        if drop_ids.is_empty() {
            return Ok(());
        }
        drop_ids.sort_unstable();

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

        for sig in pk_signatures(batch) {
            self.check_cas_and_net(SysFamily::Index, batch, &sig)?;
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
            // The CAS above already proved a live row exists at every dropped id.
            let (owner_id, cols, name) = {
                let key = sys_opk(&schema, idx_id as u128);
                let mut cursor = self
                    .sys_store(SysFamily::Index)
                    .open_cursor_in_range(key.pk_bytes(), Some(key.pk_bytes()));
                if !cursor.advance_to_exact_live(key.pk_bytes()) {
                    continue;
                }
                let (owner_id, cols, _) = read_idx_tab_cursor_row(&cursor);
                (owner_id, cols, cursor_read_string(&cursor, gnitz_wire::IDXTAB_COL_NAME))
            };
            // An internal `__fk_` index backs the RESTRICT seek; dropping one
            // would silently disarm FK enforcement.
            if name.contains(FK_INDEX_INFIX) {
                return Err("Integrity violation: cannot drop an internal FK index".into());
            }
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
}
