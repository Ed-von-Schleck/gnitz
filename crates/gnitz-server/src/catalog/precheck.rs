//! Catalog precheck — the master's trust boundary against a client that can push
//! arbitrary system-table deltas. `submit` runs it, and the `DDL_TXN` handler
//! runs it as its own step so a precheck rejection (which wrote nothing) stays
//! distinguishable from a post-apply failure (which must be compensated).
//!
//! Eight paths write without it — boot shard replay, SAL recovery, worker
//! `ddl_sync`, `submit_cascade`, Stage-A compensation, the FK auto-index's
//! `submit_local`, `bootstrap_ingest` and `advance_sequence` — all but one
//! because their rows were validated when first written. The exception is the FK
//! auto-index, safe by construction and unable to pass anyway: its own `__fk_`
//! name is one [`reject_unstorable_name`] rejects. So this file holds the
//! precheck arms *and* the registration guards `hooks.rs` re-runs on those
//! paths.

use std::cmp::Ordering;

use rustc_hash::{FxHashMap, FxHashSet};

use super::*;
use gnitz_store::schema::make_index_schema;
use gnitz_store::storage::{compare_rows, compare_rows_except};
use gnitz_wire::{COLTAB_PAY_IS_HIDDEN, COLTAB_PAY_IS_NULLABLE, COLTAB_PAY_NAME, IDXTAB_PAY_NAME, SCHEMATAB_PAY_NAME};

/// The name rules a relation or index row must satisfy to be *stored*: non-empty
/// `[A-Za-z0-9_]` (a name is interpolated into index names), already canonical
/// (every cache key here is compared byte-wise against the client's folded form),
/// and free of the reserved `__fk_` infix.
///
/// Deliberately **not** `validate_user_identifier`, whose leading-`_` reservation
/// is client-side *policy*: the engine must accept the `__h…` segment rows the
/// client writes, and spelling a `__h` grammar here would give a forger a target.
/// The residual — a raw bundle naming a relation `_foo` — is unreferenceable from
/// SQL and lives in a `t_<id>` directory.
fn reject_unstorable_name(name: &str, noun: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err(format!("{noun} name cannot be empty"));
    }
    if !name.bytes().all(gnitz_wire::is_valid_ident_char) {
        return Err(format!("{noun} name contains invalid characters: {name}"));
    }
    reject_non_canonical(name, noun)?;
    gnitz_wire::reject_reserved_infix(name)
}

/// Reject a name that is not already ASCII-lowercase. Every cache key and
/// qualified name here is compared byte-wise against the folded form the client
/// stores, so a mixed-case row would register a relation no lookup finds.
fn reject_non_canonical(name: &str, noun: &str) -> Result<(), String> {
    if name.bytes().any(|c| c.is_ascii_uppercase()) {
        return Err(format!(
            "{noun} name '{name}' is not canonical: catalog names are stored ASCII-lowercase"
        ));
    }
    Ok(())
}

/// How a guard message names one row of `family`. A COL_TAB PK packs
/// `(owner_id, col_idx)` and a circuit PK packs `(view_id, sub)`, so neither is
/// meaningful rendered as the one number it is stored as.
fn pk_label(family: SysFamily, pk: u128) -> String {
    match family {
        SysFamily::Column => {
            let (owner_id, col_idx) = gnitz_wire::unpack_col_id(pk as u64);
            format!("column {col_idx} of owner {owner_id}")
        }
        SysFamily::CircuitNodes | SysFamily::CircuitEdges | SysFamily::CircuitNodeColumns => {
            let (view_id, sub) = unpack_circuit_pk(pk);
            format!("view {view_id} sub {sub}")
        }
        _ => format!("id {pk}"),
    }
}

/// A zero-weight row is not a Z-set element: `pk_signatures` skips it, so it
/// would reach the appliers carrying no contract at all. A row past ±1 is worse
/// than useless — `retract_key_range` and `retract_pk_list` emit a hard `-1`
/// after gating on the live weight, so anything above 1 is under-retracted by
/// `w - 1` and leaves a permanent live ghost.
fn check_row_weights(family: SysFamily, batch: &Batch) -> Result<(), String> {
    for i in 0..batch.len() {
        let w = batch.get_weight(i);
        if w == 0 {
            return Err("catalog delta carries a zero-weight row".into());
        }
        if w.unsigned_abs() != 1 {
            return Err(format!(
                "catalog delta carries a {} row at weight {w} (expected ±1)",
                family.row_noun()
            ));
        }
    }
    Ok(())
}

/// A family that admits a rewrite pair takes at most one row per sign; one that
/// admits none takes at most one row per PK. For the circuit families this is
/// the whole batch-local contract: a duplicate `(view_id, sub)` makes
/// `load_circuit`'s node insert last-writer-wins in cursor order.
fn check_pk_multiplicity(family: SysFamily, sig: &PkSignature) -> Result<(), String> {
    if sig.repeats_a_sign || (family.pair_change_mask().is_none() && sig.is_pair()) {
        return Err(format!(
            "system-catalog write carries more than one row for {} {}",
            family.row_noun(),
            pk_label(family, sig.pk)
        ));
    }
    Ok(())
}

/// Bootstrap-owned ids sit below the floor, unreachable ones at or above the
/// ceiling. Both are properties of the id space rather than of the mutation's
/// shape, so they cover every sign: a `-1` drops a bootstrap row, a bare `+1`
/// aliases a bootstrap id into the caches, and a pair renames one. This is the
/// one point an id ENTERS a catalog namespace before any mutation — the register
/// hooks take it straight off the ingested row and `raise_id_counter` it, and it
/// is caller-chosen.
fn check_id_range(family: SysFamily, sig: &PkSignature) -> Result<(), String> {
    let id = sig.pk as i64;
    if family.first_user_id().is_some_and(|floor| id < floor) {
        return Err(format!(
            "cannot {} a system {} ({})",
            sig.verb(),
            family.row_noun(),
            pk_label(family, sig.pk)
        ));
    }
    if let Some(ceiling) = family.id_ceiling() {
        if id >= ceiling {
            return Err(format!(
                "{} {} is at or above the id ceiling ({ceiling})",
                family.row_noun(),
                pk_label(family, sig.pk)
            ));
        }
    }
    Ok(())
}

/// A rewrite pair's `+1` may differ from its `-1` only in the family's declared
/// mask. Comparing the two batch rows is equivalent to comparing the live row
/// against the `+1`, because the CAS proved the `-1` content-equals live.
fn check_pair_fields(family: SysFamily, batch: &Batch, sig: &PkSignature) -> Result<(), String> {
    let (Some(nj), Some(pj)) = (sig.neg, sig.pos) else {
        return Ok(());
    };
    let mask = family
        .pair_change_mask()
        .expect("check_pk_multiplicity rejects a pair in a family declaring no mask");
    if compare_rows_except(&family.schema(), batch, nj, batch, pj, mask) != Ordering::Equal {
        return Err(format!(
            "a system-catalog rewrite pair on {} {} changes a field it may not",
            family.row_noun(),
            pk_label(family, sig.pk)
        ));
    }
    Ok(())
}

/// A column record's payload must agree with its packed PK on which column of
/// which owner it is, and on the owner's kind. Nothing else anchors those three
/// fields, and they are what the FK cache and every client read back.
fn check_col_ident(batch: &Batch, row: usize, expect_kind: i64) -> Result<(), String> {
    let (owner_id, col_idx) = gnitz_wire::unpack_col_id(batch.get_pk(row) as u64);
    let ident = read_col_tab_ident(batch, row);
    if ident.owner_id != owner_id as i64 || ident.col_idx != col_idx {
        return Err(format!(
            "column record claims ({}, {}) but its packed id says (owner {owner_id}, column {col_idx})",
            ident.owner_id, ident.col_idx
        ));
    }
    if ident.owner_kind != expect_kind {
        return Err(format!(
            "column record of owner {owner_id} declares owner_kind {}, which is not what that relation is",
            ident.owner_kind
        ));
    }
    Ok(())
}

/// A circuit `+1` may only name a view this same transaction creates. One under
/// a foreign `view_id` either makes `has_dependents` of its `source_table`
/// permanently true, blocking `DROP TABLE` forever, or injects nodes into a
/// *running* view's circuit that `load_circuit` picks up at its next load.
/// `create_view_chain` is the one legitimate producer and always targets fresh
/// vids; every retraction goes through `submit_cascade`, which skips this as it
/// skips the arms.
fn check_circuit_view_ids(batch: &Batch, new_view_ids: &[i64]) -> Result<(), String> {
    for i in (0..batch.len()).filter(|&i| batch.get_weight(i) > 0) {
        let (view_id, _sub) = unpack_circuit_pk(batch.get_pk(i));
        if !new_view_ids.contains(&view_id) {
            return Err(format!(
                "circuit row names view {view_id}, which this transaction does not create"
            ));
        }
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
                .registry
                .table_entry(col.fk_table_id)
                .ok()
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
            if !entry.schema.is_lone_pk_col(col.fk_col_idx as usize) {
                // A composite index does not satisfy a single-column FK: a
                // unique (a, b) does not guarantee uniqueness of `a` alone, so
                // match only a single-column unique index on the referenced col.
                let has_unique = entry.index_circuit_on(&[col.fk_col_idx]).is_some_and(|ic| ic.is_unique);
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
        if let Some(dup) = claimed.replace(qualified) {
            return Err(format!("Table or view already exists: {dup}"));
        }
        Ok(())
    }

    /// The `-1` must content-equal the live row — you may only retract the
    /// element that exists — and `live + Σ batch` must land in `{0, 1}`: the sys
    /// stores run no `enforce_unique_pk`, so nothing else stops a duplicate live
    /// head or a persistent negative ghost. Returns the net weight.
    ///
    /// `compare_rows` routes a STRING/BLOB column through each side's own blob
    /// heap, so a name past the inline German-string prefix compares by content
    /// where a raw region `memcmp` would false-reject a valid rename.
    fn check_cas_and_net(&self, family: SysFamily, batch: &Batch, sig: &PkSignature) -> Result<i64, String> {
        let noun = family.row_noun();
        let (live_weight, live) = self.sys_store(family).live_row_at(batch.get_pk_bytes(sig.row));
        if let Some(nj) = sig.neg {
            let Some(sr) = live.as_ref() else {
                return Err(format!(
                    "catalog changed concurrently: retracting a {noun} that no longer exists"
                ));
            };
            let (src, ri) = sr.source();
            if compare_rows(&family.schema(), src, ri, batch, nj) != Ordering::Equal {
                return Err(format!(
                    "catalog changed concurrently: the retracted {noun} differs from the current one"
                ));
            }
        }
        let net = live_weight + sig.sum;
        if !(0..=1).contains(&net) {
            return Err(format!(
                "system-catalog write would leave {noun} {} at net weight {net} (expected 0 or 1)",
                pk_label(family, sig.pk)
            ));
        }
        Ok(net)
    }

    /// The shape rules every system row passes before its family's arm runs, in
    /// the order each becomes checkable. `check_pk_multiplicity` is what lets the
    /// two rules after it index `sig.neg` / `sig.pos` directly rather than
    /// rescanning: it makes those the only rows of their sign.
    ///
    /// Returns the per-PK signatures and the PKs whose net is dead — the genuine
    /// drops, which the drop guards key on so a rename pair's net-live `-1` is
    /// never read as one.
    fn check_family_contract(&self, family: SysFamily, batch: &Batch) -> Result<(Vec<PkSignature>, Vec<i64>), String> {
        check_row_weights(family, batch)?;
        let sigs = pk_signatures(batch);
        let mut net_dead: Vec<i64> = Vec::new();
        for sig in &sigs {
            check_pk_multiplicity(family, sig)?;
            check_id_range(family, sig)?;
            if family.pk_is_live_row_identity() && self.check_cas_and_net(family, batch, sig)? <= 0 {
                net_dead.push(sig.pk as i64);
            }
            check_pair_fields(family, batch, sig)?;
        }
        Ok((sigs, net_dead))
    }

    /// What a COL_TAB write means once [`Self::check_family_contract`] has settled
    /// its shape: everything below depends on the *owner*, which the contract
    /// cannot see. The guards are scoped to the transition rather than blanket —
    /// a blanket one would reject RENAME COLUMN, which is legal on a PK column
    /// and under a dependent view because views bind columns by ordinal.
    fn precheck_column_family(&mut self, batch: &Batch, sigs: &[PkSignature]) -> Result<(), String> {
        for sig in sigs {
            let (owner_id, col_idx) = gnitz_wire::unpack_col_id(sig.pk as u64);
            let owner_id = owner_id as i64;

            let Some((is_base, owner_schema)) = self
                .registry
                .table_entry(owner_id)
                .ok()
                .map(|e| (e.kind.is_base_table(), e.schema))
            else {
                if sig.neg.is_some() {
                    return Err(format!(
                        "catalog changed concurrently: retracting a column of unregistered owner {owner_id}"
                    ));
                }
                continue;
            };

            // Every column transition — RENAME, DROP, ADD — needs a user base
            // table owner, so every other `RelationKind` fails here.
            if !is_base {
                return Err(format!("cannot ALTER a column of {owner_id}: not a user base table"));
            }

            match (sig.neg, sig.pos) {
                (Some(nj), Some(pj)) => {
                    // Read raw rather than through `read_col_tab_row`'s `bool`:
                    // this demands exactly 1, and the pair mask excludes both
                    // slots from the contract's field comparison, so a payload
                    // word of 2 would otherwise slip through a `!= 0` decode.
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
                    // The renamed-to name is client-supplied and reaches
                    // `make_fk_index_name` the next time an FK index is minted
                    // over this column, so it takes the infix rule too.
                    gnitz_wire::reject_reserved_infix(&batch.read_payload_string(pj, COLTAB_PAY_NAME))?;

                    let is_drop = (hid_old == 0 && hid_new == 1) || (null_old == 0 && null_new == 1);
                    if is_drop {
                        if owner_schema.is_pk_col(col_idx as usize) {
                            return Err("cannot DROP COLUMN / DROP NOT NULL on a primary-key column".into());
                        }
                        self.reject_if_dependent_views(owner_id, "DROP COLUMN / DROP NOT NULL")?;
                    }
                }
                (None, Some(pj)) => self.precheck_column_append(batch, pj, owner_id, col_idx, &owner_schema)?,
                // The contract rejects a PK carrying neither sign, so this is
                // the unpaired `-1`.
                _ => {
                    return Err(
                        "cannot retract a column of a registered table (physical column removal is not supported)"
                            .into(),
                    );
                }
            }
        }
        Ok(())
    }

    /// Reject a column transition on `owner_id` while any view scans it: a
    /// compiled circuit's `ScanDelta` register schema is baked from the base
    /// descriptor and its operator traces hold re-keyed base rows at the old
    /// shape.
    fn reject_if_dependent_views(&mut self, owner_id: i64, op: &str) -> Result<(), String> {
        if self.dag.has_dependents(&self.registry, owner_id) {
            return Err(format!(
                "cannot {op} on table {owner_id}: it has dependent views (drop them first)"
            ));
        }
        Ok(())
    }

    /// ADD COLUMN: one unpaired `+1` appending a trailing nullable payload
    /// column to registered base table `owner_id`.
    ///
    /// The contract's per-PK net bound already closes the concurrent-append
    /// race: `pack_col_id` is a pure function of the client-read physical column
    /// count, so two connections racing pick the *same* `column_id`, and the
    /// second's `+1` lands on a now-live row where `live_weight + Σ = 2` fails
    /// that bound.
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
        // it so: the rebuild path maps the owner's defs positionally, so a gap
        // would silently shift every column past it rather than fail. (A
        // duplicate index is a duplicate COL_TAB PK, which the net bound already
        // rejects.)
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
        // `make_fk_index_name` interpolates a column name into an index name, so
        // a reserved infix here would yield an index the drop guard refuses to
        // drop. Not part of `check_col_defs`, which also runs for views (where
        // `SELECT a AS x__fk_y` is legal) and on boot replay, where a
        // pre-existing name would make the database un-openable.
        gnitz_wire::reject_reserved_infix(&appended.name)?;
        // A new column over existing rows is unconditionally nullable, carries
        // no SERIAL/FK, and is visible.
        if !appended.is_nullable {
            return Err("ADD COLUMN must append a nullable column".into());
        }
        if appended.is_serial || appended.is_hidden || appended.fk_table_id != 0 {
            return Err("ADD COLUMN must not append a SERIAL, hidden, or foreign-key column".into());
        }
        check_col_ident(batch, pj, OWNER_KIND_TABLE)
    }

    /// The shared VIEW_TAB registration guards: what a `WITH (…)` option may be
    /// declared on, and what a bounded view may read. `source_ids` is the view's
    /// resolved `ScanDelta` sources.
    ///
    /// Run from the precheck, and again from `view_registration` for the paths
    /// that skip it (boot replay, worker `ddl_sync`). A within-bundle hidden
    /// segment is unresolvable at precheck time and never carries a `WITH`
    /// option, which is what keeps the second run meaningful.
    pub(in crate::catalog) fn validate_view_options(
        &self,
        vid: i64,
        name: &str,
        budgets: gnitz_store::relation::ViewBudgets,
        source_ids: &[i64],
    ) -> Result<(), String> {
        // A hidden chain segment is an internal relation the planner mints, never
        // something an option clause may name.
        if (budgets.capacity_bytes.is_some() || budgets.delta_bytes.is_some())
            && name.starts_with(gnitz_wire::HIDDEN_VIEW_PREFIX)
        {
            return Err(format!(
                "catalog invariant violated: hidden segment '{name}' (vid={vid}) carries a WITH option."
            ));
        }
        // A bounded view's `Delta(0)` cannot be a function of the tick round, and
        // the whole feed contract is that it is: hydrating a skeleton key reads the
        // source's live store, which `handle_push` advances outside any tick, so
        // the bootstrap would carry a push the next poll delivers again.
        if budgets.capacity_bytes.is_some() && budgets.delta_bytes.is_some() {
            return Err(format!(
                "view '{name}' (vid={vid}) declares both `capacity` and `delta`; \
                 a capacity-bounded view cannot carry a delta feed"
            ));
        }
        // Both rules trace to skeleton rows being recomputed from the *source*
        // store: a bounded view's own store is skeletonized, and a stream's holds
        // nothing to recompute from. `ScanDelta` is the only external-source
        // opcode, so `source_ids` covers every circuit's every source.
        for &src in source_ids {
            let Some(e) = self.registry.entry(src) else {
                continue;
            };
            if e.budgets.capacity_bytes.is_some() {
                return Err(format!(
                    "view '{name}' (vid={vid}) reads relation {src}, which is a \
                     capacity-bounded view; views cannot be created over one"
                ));
            }
            if budgets.capacity_bytes.is_some() && e.kind == RelationKind::Stream {
                return Err(format!(
                    "view '{name}' (vid={vid}) reads relation {src}, which is a stream; \
                     a capacity-bounded view cannot be created over one"
                ));
            }
        }
        Ok(())
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
    pub(in crate::catalog) fn validate_index_registration(
        &self,
        owner_id: i64,
        cols: &PkColList,
    ) -> Result<&gnitz_store::relation::TableEntry, String> {
        if !cols.is_well_formed() {
            return Err(format!(
                "Index: column list count {} out of range 1..={}",
                cols.decoded_count(),
                gnitz_wire::PK_LIST_MAX_COLS
            ));
        }
        let entry = self
            .registry
            .table_entry(owner_id)
            .ok()
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
    /// the read-only half of [`Self::submit`], and the `DDL_TXN` handler's own
    /// first step, which is what lets it leave the rollback marker `None` on a
    /// rejection and so reconstruct no ghost row.
    ///
    /// Exhaustive over `SysFamily` (like `fire_hooks`): a newly-added family must
    /// decide here whether it carries guards beyond the contract, rather than
    /// falling into a silent `_` arm.
    pub(crate) fn precheck_family(&mut self, family: SysFamily, batch: &Batch) -> Result<(), String> {
        let (sigs, net_dead) = self.check_family_contract(family, batch)?;
        match family {
            SysFamily::Schema => self.precheck_schema_family(batch, &net_dead),
            SysFamily::Table | SysFamily::View => self.precheck_relation_family(family, batch, net_dead),
            SysFamily::Column => self.precheck_column_family(batch, &sigs),
            SysFamily::Index => self.precheck_index_family(batch, net_dead),
            SysFamily::Sequence | SysFamily::CircuitNodes | SysFamily::CircuitEdges | SysFamily::CircuitNodeColumns => {
                Ok(())
            }
        }
    }

    /// The cross-family rules of one `DDL_TXN` bundle: what no family's own arm
    /// can see, because each is prechecked against a catalog the bundle's other
    /// families have not reached yet. Run before the first family is applied, so
    /// a rejection has written nothing and needs no compensation.
    pub(crate) fn precheck_bundle(
        &self,
        families: &[Option<Batch>; SysFamily::COUNT],
        new_view_ids: &[i64],
    ) -> Result<(), String> {
        if let Some(cols) = families[SysFamily::Column.index()].as_ref() {
            self.check_column_owners(cols, families)?;
        }
        for family in [
            SysFamily::CircuitNodes,
            SysFamily::CircuitEdges,
            SysFamily::CircuitNodeColumns,
        ] {
            if let Some(b) = families[family.index()].as_ref() {
                check_circuit_view_ids(b, new_view_ids)?;
            }
        }
        Ok(())
    }

    /// Every column record must name an owner this bundle creates or the registry
    /// already holds, and agree with it on identity and kind. A row on a phantom
    /// owner is **unretractable** — the only COL_TAB retractor is the owner's own
    /// drop cascade, which returns early on an unregistered id — and
    /// `apply_fk_constraints` builds a permanent `FkEdge` from its payload. The
    /// kind clause matters on its own: that field alone decides a row declares an
    /// FK, so a view's columns claiming `OWNER_KIND_TABLE` plant an unvalidated edge.
    fn check_column_owners(&self, cols: &Batch, families: &[Option<Batch>; SysFamily::COUNT]) -> Result<(), String> {
        // The owners the bundle registers itself, by the kind its block implies.
        let mut created: FxHashMap<i64, i64> = FxHashMap::default();
        for (family, kind) in [(SysFamily::Table, OWNER_KIND_TABLE), (SysFamily::View, OWNER_KIND_VIEW)] {
            let Some(b) = families[family.index()].as_ref() else {
                continue;
            };
            for i in (0..b.len()).filter(|&i| b.get_weight(i) > 0) {
                created.insert(b.get_pk(i) as i64, kind);
            }
        }
        for i in (0..cols.len()).filter(|&i| cols.get_weight(i) > 0) {
            let owner_id = gnitz_wire::unpack_col_id(cols.get_pk(i) as u64).0 as i64;
            // What the catalog already says the owner is, else what this
            // transaction is making it.
            let owner_kind = self
                .registry
                .table_entry(owner_id)
                .ok()
                .map(|e| {
                    if e.kind.is_view() {
                        OWNER_KIND_VIEW
                    } else {
                        OWNER_KIND_TABLE
                    }
                })
                .or_else(|| created.get(&owner_id).copied())
                .ok_or_else(|| {
                    format!(
                        "column record names owner {owner_id}, which this transaction \
                         does not create and the catalog does not hold"
                    )
                })?;
            check_col_ident(cols, i, owner_kind)?;
        }
        Ok(())
    }

    /// SCHEMA_TAB: a CREATE must not collide with a live schema name — nor with
    /// one this same batch already claims — and a DROP must find the schema
    /// empty. The empty-schema guard runs before any WAL write, so a rejected
    /// non-empty drop queues no dir deletion and retracts no rows, converting a
    /// silent member-orphan into a loud error; a DROP SCHEMA bundle's members are
    /// retracted by earlier families, so its count is 0 by the time it runs.
    ///
    /// Schema ids share an i64 space with relation ids, so a schema drop must NOT
    /// be probed against the relation-keyed dep map — the member count is the
    /// whole guard.
    fn precheck_schema_family(&mut self, batch: &Batch, net_dead: &[i64]) -> Result<(), String> {
        // Two `+1` rows under one name both pass the cache check below and both
        // apply: `schema_by_name` keeps the second, leaving the first id live and
        // unreachable, and dropping the reachable one deletes the orphan's
        // directory (the deletion is queued by name).
        let mut claimed: FxHashSet<String> = FxHashSet::default();
        for i in (0..batch.len()).filter(|&i| batch.get_weight(i) > 0) {
            let name = batch.read_payload_string(i, SCHEMATAB_PAY_NAME);
            // The full identifier rule, leading-`_` included: a schema name is
            // the one the engine interpolates into a filesystem path
            // (`hook_schema_dir` → `create_dir_all`, and `remove_dir_all` on
            // the `-1` arm). Nothing synthesizes one, so no carve-out.
            validate_user_identifier(&name)?;
            reject_non_canonical(&name, "schema")?;
            if self.has_schema(&name) {
                return Err(format!("Schema already exists: {name}"));
            }
            if let Some(dup) = claimed.replace(name) {
                return Err(format!("Schema already exists: {dup}"));
            }
        }
        for &sid in net_dead {
            let n = self.schema_member_count(sid);
            if n > 0 {
                return Err(format!("Schema not empty: {n} relation(s) remain; drop them first"));
            }
        }
        Ok(())
    }

    /// TABLE_TAB / VIEW_TAB: the CREATE guards (column-record admissibility, FK
    /// column types, qualified-name uniqueness) and the DROP guards (FK children,
    /// view dependents).
    ///
    /// The drop guards key on `net_dead` — the PKs whose bundle net is dead — not
    /// raw `weight < 0`, so a rename pair's net-live `-1` is never rejected as
    /// "referenced by FK" / "View dependency".
    fn precheck_relation_family(&mut self, family: SysFamily, batch: &Batch, net_dead: Vec<i64>) -> Result<(), String> {
        let is_table = family == SysFamily::Table;
        let mut claimed: FxHashSet<String> = FxHashSet::default();

        for i in (0..batch.len()).filter(|&i| batch.get_weight(i) > 0) {
            let id = batch.get_pk(i) as i64;

            // Reject any gap/duplicate in the column-index sequence, which would
            // mismap columns downstream. Rejecting here — before the appliers
            // mutate the caches — leaves clean state on a failed DDL. This is the
            // only place the contiguity rule runs: the register hooks read the
            // owner's defs positionally, so the paths that skip precheck (boot
            // replay, worker ddl_sync) rely on it having held when the rows were
            // first written.
            self.check_column_contiguity(id)?;
            let col_defs = self.read_column_defs(id);
            let (sid, name, pk, kind) = if is_table {
                let (sid, name, pk, kind, _placement) = read_table_tab_row(batch, i, id)?;
                (sid, name, pk, kind)
            } else {
                let (sid, name, pk, budgets) = read_view_tab_row(batch, i);
                // `topo_priority` applies CircuitNodes (2) before View (6) in a
                // creating bundle, so the view's sources resolve here; an
                // all-negative bundle sorts descending but carries no `+1` VIEW_TAB
                // row to validate.
                let source_ids = self.dag.get_source_ids(&self.registry, id);
                self.validate_view_options(id, &name, budgets, &source_ids)?;
                (sid, name, pk, RelationKind::View)
            };
            validate_relation_defs(kind, id, &name, &col_defs, &pk)?;
            reject_unstorable_name(&name, family.row_noun())?;

            if is_table {
                // `make_fk_index_name` interpolates a column name into an index
                // name, so a column carrying the reserved infix would yield an
                // index the drop guard refuses to drop. Base-table-scoped: a
                // view's column names are never interpolated, and a legal
                // `SELECT a AS x__fk_y` must keep working.
                for cd in col_defs.iter() {
                    gnitz_wire::reject_reserved_infix(&cd.name)?;
                }
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

        let dep_map = self.dag.get_dep_map(&self.registry);
        for &id in &drop_ids {
            if let Some(dependents) = dep_map.get(&id) {
                // A dependent that is itself being dropped in this same batch is
                // self-resolving — only an *outside* dependent blocks the drop.
                // drop_ids is sorted, so binary_search.
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
    /// list and a free index name; a DROP must not strip the uniqueness an FK
    /// depends on. The drop guards read the batch row rather than probing: the
    /// contract's CAS proved every `-1` content-equals the live one, `name` and
    /// `source_cols` included.
    fn precheck_index_family(&mut self, batch: &Batch, net_dead: Vec<i64>) -> Result<(), String> {
        // The names this batch has already claimed, as `precheck_qname_unique`
        // threads one for relations. Without it two rows under one name both pass
        // the persisted-cache check and the second overwrites the first in
        // `index_by_name`, leaving one index live and unreachable.
        let mut claimed: FxHashSet<String> = FxHashSet::default();
        let noun = SysFamily::Index.row_noun();
        for i in (0..batch.len()).filter(|&i| batch.get_weight(i) > 0) {
            let (owner_id, cols, _is_unique) = read_idx_tab_row(batch, i);
            let index_name = batch.read_payload_string(i, IDXTAB_PAY_NAME);
            reject_unstorable_name(&index_name, noun)?;
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
            if let Some(dup) = claimed.replace(index_name) {
                return Err(format!("Index already exists: {dup}"));
            }
        }

        let mut drop_ids = net_dead;
        if drop_ids.is_empty() {
            return Ok(());
        }
        drop_ids.sort_unstable();

        for i in (0..batch.len()).filter(|&i| batch.get_weight(i) < 0) {
            let (owner_id, cols, _) = read_idx_tab_row(batch, i);
            let name = batch.read_payload_string(i, IDXTAB_PAY_NAME);
            // An internal `__fk_` index backs the RESTRICT seek; dropping one
            // would silently disarm FK enforcement. Keyed on the raw `-1` rows,
            // not on net-dead: a rewrite pair must not slip past it.
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
            let is_lone_pk = self
                .registry
                .entry(owner_id)
                .is_some_and(|e| e.schema.is_lone_pk_col(src_col));
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

#[cfg(test)]
#[path = "tests/precheck.rs"]
mod tests;
