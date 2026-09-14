//! Catalog precheck — the master's trust boundary against client-pushed
//! system-table deltas — and the registration guards `hooks.rs` re-runs for the
//! paths whose rows are not a client's.

use std::cmp::Ordering;

use rustc_hash::{FxHashMap, FxHashSet};

use super::*;
use gnitz_store::schema::make_index_schema;
use gnitz_store::storage::{compare_rows, compare_rows_except};
use gnitz_wire::MAX_COLUMNS;
use gnitz_wire::{
    COLTAB_PAY_FK_COL_IDX, COLTAB_PAY_IS_HIDDEN, COLTAB_PAY_IS_NULLABLE, COLTAB_PAY_IS_SERIAL, COLTAB_PAY_OWNER_KIND,
    COLTAB_PAY_SCALE, COLTAB_PAY_TYPE_CODE, IDXTAB_PAY_NAME, SCHEMATAB_PAY_NAME,
};

/// The name rules a relation or index row must satisfy to be *stored*: non-empty
/// `[A-Za-z0-9_]` and already canonical (every cache key here is compared
/// byte-wise against the client's folded form).
///
/// Deliberately **not** `validate_user_identifier`, whose leading-`_` reservation
/// is client-side *policy*: the engine must accept the `__h…` segment rows the
/// client writes. The residual — a raw bundle naming a relation `_foo` — is
/// unreferenceable from SQL and lives in a `t_<id>` directory.
fn reject_unstorable_name(name: &str, noun: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err(format!("{noun} name cannot be empty"));
    }
    if !name.bytes().all(gnitz_wire::is_valid_ident_char) {
        return Err(format!("{noun} name contains invalid characters: {name}"));
    }
    reject_non_canonical(name, noun)
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

/// The rules a relation's column records must satisfy on their own, with no PK
/// list in hand; messages are bare predicates, and callers add the context. The
/// duplicate-name rule is ingestion points only — a view segment legitimately
/// carries two visible columns of one name (a join chain's output).
pub(super) fn check_col_defs(kind: RelationKind, col_defs: &[ColumnDef]) -> Result<(), String> {
    if col_defs.is_empty() {
        return Err("has no column records".into());
    }
    // Reachable from a plain view as well as a wide CREATE TABLE: a compound-PK
    // plain projection prepends the k source PK columns.
    if col_defs.len() > MAX_COLUMNS {
        return Err(format!("has {} columns (max {})", col_defs.len(), MAX_COLUMNS));
    }
    if let Some(cd) = col_defs.iter().find(|cd| !gnitz_wire::is_valid_type_code(cd.type_code)) {
        return Err(format!("column '{}' has invalid type code {}", cd.name, cd.type_code));
    }
    if let Some(cd) = col_defs.iter().find(|cd| {
        cd.scale > gnitz_wire::decimal::MAX_DECIMAL_SCALE
            || (cd.scale != 0 && cd.type_code != gnitz_wire::type_code::DECIMAL)
    }) {
        return Err(format!(
            "column '{}' has type code {} and cannot carry scale {}",
            cd.name, cd.type_code, cd.scale
        ));
    }
    if kind.is_ingestion_point() {
        let mut seen = FxHashSet::default();
        if let Some(cd) = col_defs
            .iter()
            .filter(|c| !c.is_hidden)
            .find(|c| !seen.insert(c.name.to_ascii_lowercase()))
        {
            return Err(format!("has duplicate column name '{}'", cd.name));
        }
    }
    Ok(())
}

/// `gnitz-wire`'s PK rule set, in wire's own wording. Also run by
/// `build_schema_from_col_defs` over the pair it is about to construct from,
/// which is what makes that builder total where `SchemaDescriptor::new_with_placement`
/// would abort.
pub(super) fn validate_pk_against_cols(col_defs: &[ColumnDef], pk_cols: &[u32]) -> Result<(), String> {
    gnitz_wire::validate_pk_tuple(pk_cols, col_defs.len(), |c| {
        let cd = &col_defs[c as usize];
        (cd.type_code, cd.is_nullable)
    })
    .map(|_stride| ())
    .map_err(|rule| rule.to_string())
}

/// A system row carries weight ±1: a zero weight carries no contract, and the
/// engine's retractions emit a hard `-1`, which would leave `w - 1` of a heavier row.
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
/// admits none takes at most one row per PK. For the circuit family this is
/// the whole batch-local contract: a duplicate `(view_id, node_id)` makes
/// `load_circuit`'s node insert last-writer-wins in cursor order.
fn check_pk_multiplicity(family: SysFamily, sig: &PkSignature) -> Result<(), String> {
    if sig.repeats_a_sign || (family.pair_change_mask().is_none() && sig.is_pair()) {
        return Err(format!(
            "system-catalog write carries more than one row for {} {}",
            family.row_noun(),
            family.pk_label(sig.pk)
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
    let id = sig.leading;
    if family.first_user_id().is_some_and(|floor| id < floor) {
        return Err(format!(
            "cannot {} a system {} ({})",
            sig.verb(),
            family.row_noun(),
            family.pk_label(sig.pk)
        ));
    }
    if let Some(ceiling) = family.id_ceiling() {
        if id >= ceiling {
            return Err(format!(
                "{} {} is at or above the id ceiling ({ceiling})",
                family.row_noun(),
                family.pk_label(sig.pk)
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
    if compare_rows_except(family.schema(), batch, nj, batch, pj, mask) != Ordering::Equal {
        return Err(format!(
            "a system-catalog rewrite pair on {} {} changes a field it may not",
            family.row_noun(),
            family.pk_label(sig.pk)
        ));
    }
    Ok(())
}

/// Every COL_TAB word `read_col_tab_row` narrows, against the width it narrows
/// to. One that overflows decodes to something `write_col_tab_row` could never
/// emit (`type_code` `0x104` → `4`), leaving a stored row no client can
/// reproduce and so no later `-1` can retract. It also bounds the booleans to
/// `{0, 1}` for the ALTER direction test below.
fn check_col_narrowings(batch: &Batch, row: usize) -> Result<(), String> {
    for (field, pi, max) in [
        ("type_code", COLTAB_PAY_TYPE_CODE, u8::MAX as u64),
        ("is_nullable", COLTAB_PAY_IS_NULLABLE, 1),
        ("fk_col_idx", COLTAB_PAY_FK_COL_IDX, u32::MAX as u64),
        ("is_serial", COLTAB_PAY_IS_SERIAL, 1),
        ("is_hidden", COLTAB_PAY_IS_HIDDEN, 1),
        ("scale", COLTAB_PAY_SCALE, u8::MAX as u64),
    ] {
        let w = payload_u64(batch, row, pi);
        if w > max {
            return Err(format!(
                "column record carries {field} = {w}, past the {max} its stored width holds"
            ));
        }
    }
    Ok(())
}

/// A column record must agree with its owner on what that owner is: `owner_kind`
/// alone decides whether the row declares a foreign key, so a view's columns
/// claiming `OWNER_KIND_TABLE` would plant an unvalidated `FkEdge`.
fn check_col_ident(batch: &Batch, row: usize, owner_id: i64, expect_kind: i64) -> Result<(), String> {
    let kind = payload_u64(batch, row, COLTAB_PAY_OWNER_KIND) as i64;
    if kind != expect_kind {
        return Err(format!(
            "column record of owner {owner_id} declares owner_kind {kind}, which is not what that relation is"
        ));
    }
    Ok(())
}

/// A circuit `+1` may only name a view this same transaction creates: one under a
/// foreign `view_id` would pin its source table's drop or rewrite a running circuit.
fn check_circuit_view_ids(batch: &Batch, new_view_ids: &[i64]) -> Result<(), String> {
    // Sorted once: this runs per row of a client-supplied block bounded only by
    // the 64 MB frame.
    let mut created: Vec<i64> = new_view_ids.to_vec();
    created.sort_unstable();
    for i in batch.live_rows() {
        let view_id = SysFamily::CircuitNodes.leading_id(batch.get_pk(i));
        if created.binary_search(&view_id).is_err() {
            return Err(format!(
                "circuit row names view {view_id}, which this transaction does not create"
            ));
        }
    }
    Ok(())
}

impl CatalogEngine {
    /// Check every FK-carrying column of a relation about to be registered.
    /// `pk` must already have passed [`validate_pk_against_cols`], which is what
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
                .relation(col.fk_table_id)
                .ok_or_else(|| format!("FK references unknown table_id {}", col.fk_table_id))?;
            // Not covered by the PK/UNIQUE tests below, which read only the schema:
            // a stream and a view both have a PK that looks exactly like a base
            // table's without being the unique, stored key the parent probe reads.
            if !entry.kind().is_base_table() {
                return Err(format!(
                    "FK references relation {}, which is a {}; a FOREIGN KEY must reference a base table",
                    col.fk_table_id,
                    entry.kind().noun()
                ));
            }
            if !entry.schema().is_lone_pk_col(col.fk_col_idx as usize) {
                // A composite index does not satisfy a single-column FK: a
                // unique (a, b) does not guarantee uniqueness of `a` alone, so
                // match only a single-column unique index on the referenced col.
                let has_unique = entry.index_on(&[col.fk_col_idx]).is_some_and(|ic| ic.is_unique());
                if !has_unique {
                    return Err("FK must reference the primary key or a UNIQUE-indexed column".into());
                }
            }
            entry.schema().columns[col.fk_col_idx as usize].type_code
        };

        // Domain fit, not promoted equality: the lone-PK probe encodes the child
        // value into a slot of the parent's *raw* width, so an I64 child and an
        // I32 parent promote alike and still have no slot to encode into.
        if !gnitz_wire::fk_child_fits(col.type_code, target_type) {
            return Err(format!(
                "FK type mismatch: child type code {} cannot reference target type code {target_type}",
                col.type_code
            ));
        }
        Ok(())
    }

    /// Reject a CREATE whose qualified `schema.name` collides with an existing
    /// entity. A rewrite pair that leaves the name unchanged re-registers the
    /// incumbent, so `existing == self_id` is not one.
    ///
    /// `net_dead` is this same bundle's net-dead PKs in this same family. An
    /// incumbent among them is not a collision either: one bundle may retire an
    /// id and register a different one under the same name — an ALTER VIEW is
    /// exactly that. Precheck reads the caches *before* apply, so they still map
    /// the name to the outgoing id.
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
        let qualified = gnitz_wire::qualified_key(schema_name, name);
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
        let (live_weight, live) = self.sys_relation(family).live_row_at(batch.get_pk_bytes(sig.row));
        if let Some(nj) = sig.neg {
            let Some(sr) = live.as_ref() else {
                return Err(format!(
                    "catalog changed concurrently: retracting a {noun} that no longer exists"
                ));
            };
            let (src, ri) = sr.source();
            if compare_rows(family.schema(), src, ri, batch, nj) != Ordering::Equal {
                return Err(format!(
                    "catalog changed concurrently: the retracted {noun} differs from the current one"
                ));
            }
        }
        let net = live_weight + sig.sum;
        if !(0..=1).contains(&net) {
            return Err(format!(
                "system-catalog write would leave {noun} {} at net weight {net} (expected 0 or 1)",
                family.pk_label(sig.pk)
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
    /// never read as one — a list only the single-column-key arms read, so it
    /// carries the whole key rather than [`PkSignature::leading`].
    fn check_family_contract(&self, family: SysFamily, batch: &Batch) -> Result<(Vec<PkSignature>, Vec<i64>), String> {
        check_row_weights(family, batch)?;
        let sigs = pk_signatures(family, batch);
        let mut net_dead: Vec<i64> = Vec::new();
        for sig in &sigs {
            check_pk_multiplicity(family, sig)?;
            check_id_range(family, sig)?;
            if family.pk_is_live_row_identity() && self.check_cas_and_net(family, batch, sig)? <= 0 {
                net_dead.push(sig.pk as i64);
            }
            check_pair_fields(family, batch, sig)?;
        }
        // Sorted so the two drop guards can `binary_search` it
        // (`precheck_qname_unique` reads it linearly); one entry per distinct PK
        // already, from `pk_signatures`.
        net_dead.sort_unstable();
        Ok((sigs, net_dead))
    }

    /// What a COL_TAB write means once [`Self::check_family_contract`] has settled
    /// its shape: everything below depends on the *owner*, which the contract
    /// cannot see. The guards are scoped to the transition rather than blanket —
    /// a blanket one would reject RENAME COLUMN, which is legal on a PK column
    /// and under a dependent view because views bind columns by ordinal.
    fn precheck_column_family(&mut self, batch: &Batch, sigs: &[PkSignature]) -> Result<(), String> {
        for sig in sigs {
            // COL_TAB PK = `(owner_id, col_idx)`.
            let (owner_id, col_idx) = (sig.leading, sig.pk as u64);
            for row in [sig.neg, sig.pos].into_iter().flatten() {
                check_col_narrowings(batch, row)?;
            }

            let Some((is_base, owner_schema)) = self
                .registry
                .relation(owner_id)
                .map(|e| (e.kind().is_base_table(), e.schema()))
            else {
                if sig.neg.is_some() {
                    return Err(format!(
                        "catalog changed concurrently: retracting a column of unregistered owner {owner_id}"
                    ));
                }
                // Normal: a CREATE TABLE bundle's COL_TAB block applies before
                // TABLE_TAB. `check_column_owners` owns these rows.
                continue;
            };

            // Every column transition — RENAME, DROP, ADD — needs a user base
            // table owner, so every other `RelationKind` fails here.
            if !is_base {
                return Err(format!("cannot ALTER a column of {owner_id}: not a user base table"));
            }

            match (sig.neg, sig.pos) {
                (Some(nj), Some(pj)) => {
                    // `check_col_narrowings` bounded these booleans to `{0, 1}`.
                    let (old, new) = (read_col_tab_row(batch, nj), read_col_tab_row(batch, pj));
                    // Direction: is_hidden / is_nullable only false→true.
                    if new.is_hidden != old.is_hidden && !new.is_hidden {
                        return Err("a column-ALTER may only set is_hidden 0→1 (DROP COLUMN)".into());
                    }
                    if new.is_nullable != old.is_nullable && !new.is_nullable {
                        return Err("a column-ALTER may only set is_nullable 0→1 (DROP NOT NULL)".into());
                    }
                    let is_drop = (new.is_hidden && !old.is_hidden) || (new.is_nullable && !old.is_nullable);
                    let prospective = self.col_defs_with(owner_id, col_idx, new);
                    check_col_defs(RelationKind::BaseTable, &prospective)
                        .map_err(|e| format!("cannot ALTER COLUMN on table {owner_id}: {e}"))?;
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
        // Name the table and one blocking view: the recovery is to drop that
        // view, which a bare id leaves the author to go and look up.
        let blockers = self.dag.get_dep_map(&self.registry).get(&owner_id).cloned();
        let Some(blockers) = blockers else { return Ok(()) };
        let name = |id: i64| {
            self.caches
                .entity_by_id
                .get(&id)
                .map_or_else(|| id.to_string(), |(sn, en)| format!("{sn}.{en}"))
        };
        let named: Vec<String> = blockers.iter().map(|id| name(*id)).collect();
        Err(format!(
            "cannot {op} on '{}': it has dependent views ({}) — drop them first",
            name(owner_id),
            named.join(", ")
        ))
    }

    /// `owner_id`'s column records as this batch's `+1` row for `col_idx` leaves
    /// them: the decoded row replaces the live record at that index, or extends
    /// the set when the transition appends one.
    fn col_defs_with(&mut self, owner_id: i64, col_idx: u64, row: ColumnDef) -> Vec<ColumnDef> {
        let mut defs = (*self.read_column_defs(owner_id)).clone();
        match defs.get_mut(col_idx as usize) {
            Some(live) => *live = row,
            None => defs.push(row),
        }
        defs
    }

    /// ADD COLUMN: one unpaired `+1` appending a trailing nullable payload
    /// column to registered base table `owner_id`.
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
        if col_idx as usize != owner_schema.num_columns() {
            return Err(format!(
                "cannot ADD COLUMN at index {col_idx} on table {owner_id}: \
                 columns must be appended at index {}",
                owner_schema.num_columns()
            ));
        }
        let appended = read_col_tab_row(batch, pj);
        // The PK rules are not re-run: a trailing non-PK append cannot invalidate
        // an already-valid PK list.
        let prospective = self.col_defs_with(owner_id, col_idx, appended.clone());
        check_col_defs(RelationKind::BaseTable, &prospective)
            .map_err(|e| format!("cannot ADD COLUMN on table {owner_id}: {e}"))?;
        // A new column over existing rows is unconditionally nullable, carries
        // no SERIAL/FK, and is visible.
        if !appended.is_nullable {
            return Err("ADD COLUMN must append a nullable column".into());
        }
        if appended.is_serial || appended.is_hidden || appended.fk_table_id != 0 {
            return Err("ADD COLUMN must not append a SERIAL, hidden, or foreign-key column".into());
        }
        check_col_ident(batch, pj, owner_id, OWNER_KIND_TABLE)
    }

    /// A `+1` VIEW_TAB row's `owner_view_id` must name `0` (a user view), a view
    /// this bundle creates, or one the registry holds — the drop cascade keys on
    /// it, so a forged owner would point a cascade at nothing. Precheck-only:
    /// the paths that skip it replay rows this already accepted.
    /// A rewrite pair is absent from `sorted_creates` and needs no entry: it
    /// renames a view that already exists, so it falls through to the registry.
    fn validate_view_owner(
        &self,
        vid: i64,
        name: &str,
        owner_view_id: i64,
        sorted_creates: &[i64],
    ) -> Result<(), String> {
        if owner_view_id == 0 {
            return Ok(());
        }
        if owner_view_id == vid {
            return Err(format!("view '{name}' (vid={vid}) declares itself its own owner"));
        }
        if sorted_creates.binary_search(&owner_view_id).is_ok() || self.registry.has_id(owner_view_id) {
            return Ok(());
        }
        Err(format!(
            "view '{name}' (vid={vid}) names owner_view_id={owner_view_id}, which no relation holds"
        ))
    }

    /// The shared VIEW_TAB registration guards: what a `WITH (…)` option may be
    /// declared on, and what a bounded view may read. `source_ids` is the view's
    /// resolved `ScanDelta` sources.
    ///
    /// Run from the precheck and again from `view_registration`. A within-bundle
    /// internal segment never carries a `WITH` option, which is what keeps the
    /// second run meaningful.
    pub(in crate::catalog) fn validate_view_options(
        &self,
        vid: i64,
        name: &str,
        budgets: gnitz_store::relation::ViewBudgets,
        owner_view_id: i64,
        source_ids: &[i64],
    ) -> Result<(), String> {
        // An internal chain segment is a relation the planner mints, never
        // something an option clause may name.
        if (budgets.capacity_bytes.is_some() || budgets.delta_bytes.is_some()) && owner_view_id != 0 {
            return Err(format!(
                "catalog invariant violated: internal segment '{name}' (vid={vid}) carries a WITH option."
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
            let Some(e) = self.registry.relation(src) else {
                continue;
            };
            if e.is_bounded() {
                return Err(format!(
                    "view '{name}' (vid={vid}) reads relation {src}, which is a \
                     capacity-bounded view; views cannot be created over one"
                ));
            }
            if budgets.capacity_bytes.is_some() && e.kind() == RelationKind::Stream {
                return Err(format!(
                    "view '{name}' (vid={vid}) reads relation {src}, which is a stream; \
                     a capacity-bounded view cannot be created over one"
                ));
            }
        }
        Ok(())
    }

    /// The shared IDX_TAB registration guard: a base-table owner, returned for
    /// the callers' schema/context reads.
    ///
    /// Only base tables can own a secondary index: index projection runs on the
    /// base-table DML paths (`ingest_store_and_indices`) alone, and view deltas
    /// land via the circuit-evaluation terminal-view moves, which never project
    /// into a secondary index. The SQL binder rejects this by name resolution;
    /// this rejects a raw wire push before the row is persisted or broadcast.
    pub(in crate::catalog) fn validate_index_registration(
        &self,
        owner_id: i64,
    ) -> Result<&gnitz_store::relation::Relation, String> {
        let entry = self
            .registry
            .relation(owner_id)
            .ok_or_else(|| format!("Index: owner table {owner_id} not found"))?;
        if !entry.kind().is_base_table() {
            return Err(format!(
                "Index: owner {owner_id} is a {}; only a base table can be indexed",
                entry.kind().noun()
            ));
        }
        Ok(entry)
    }

    /// Validate one family of a `DDL_TXN` before any of it is applied, so a
    /// rejection needs no compensation.
    ///
    /// Exhaustive over `SysFamily` (like `fire_hooks`): a newly-added family must
    /// decide here whether it carries guards beyond the contract, rather than
    /// falling into a silent `_` arm.
    pub(crate) fn precheck_family(&mut self, family: SysFamily, batch: &Batch) -> Result<(), String> {
        let (sigs, net_dead) = self.check_family_contract(family, batch)?;
        match family {
            SysFamily::Schema => self.precheck_schema_family(batch, &net_dead),
            SysFamily::Table | SysFamily::View => self.precheck_relation_family(family, batch, &net_dead),
            SysFamily::Column => self.precheck_column_family(batch, &sigs),
            SysFamily::Index => self.precheck_index_family(batch, &net_dead),
            SysFamily::Sequence | SysFamily::CircuitNodes => Ok(()),
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
        if let Some(b) = families[SysFamily::CircuitNodes.index()].as_ref() {
            check_circuit_view_ids(b, new_view_ids)?;
        }
        Ok(())
    }

    /// Every column record must name an owner this bundle creates or the registry
    /// holds, of the kind its `owner_kind` claims: a phantom owner is never dropped,
    /// so nothing would ever retract the row or the `FkEdge` it plants.
    fn check_column_owners(&self, cols: &Batch, families: &[Option<Batch>; SysFamily::COUNT]) -> Result<(), String> {
        // The owners the bundle registers itself, by the kind its block implies.
        let mut created: FxHashMap<i64, i64> = FxHashMap::default();
        for (family, kind) in [(SysFamily::Table, OWNER_KIND_TABLE), (SysFamily::View, OWNER_KIND_VIEW)] {
            let Some(b) = families[family.index()].as_ref() else {
                continue;
            };
            for i in b.live_rows() {
                created.insert(b.get_pk(i) as i64, kind);
            }
        }
        let mut altered: Option<i64> = None;
        for i in cols.live_rows() {
            let owner_id = SysFamily::Column.leading_id(cols.get_pk(i));
            // An ALTER must be its bundle's only change, on one owner: nothing can undo
            // its descriptor swap, so the swap must be the bundle's last fallible step.
            if self.registry.has_id(owner_id) {
                if altered.is_some_and(|a| a != owner_id) || families.iter().flatten().count() > 1 {
                    return Err("a column ALTER must be the only change in its DDL transaction".into());
                }
                altered = Some(owner_id);
            }
            // What the catalog already says the owner is, else what this
            // transaction is making it.
            let owner_kind = self
                .registry
                .relation(owner_id)
                .map(|e| {
                    if e.kind().is_view() {
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
            check_col_ident(cols, i, owner_id, owner_kind)?;
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
        for i in batch.live_rows() {
            let name = payload_string(batch, i, SCHEMATAB_PAY_NAME);
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
    fn precheck_relation_family(&mut self, family: SysFamily, batch: &Batch, net_dead: &[i64]) -> Result<(), String> {
        let is_table = family == SysFamily::Table;
        let mut claimed: FxHashSet<String> = FxHashSet::default();
        // Sorted for `validate_view_owner`'s probe, which runs per `+1` row.
        let mut view_creates: Vec<i64> = if is_table {
            Vec::new()
        } else {
            family_pk_partition(family, batch).creates
        };
        view_creates.sort_unstable();

        for i in batch.live_rows() {
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
                let r = read_table_tab_row(batch, i).map_err(|e| format!("{e} (tid={id})"))?;
                (r.schema_id, r.name, r.pk, r.kind)
            } else {
                let v = read_view_tab_row(batch, i).map_err(|e| format!("{e} (vid={id})"))?;
                // `topo_priority` applies CircuitNodes (2) before View (6) in a
                // creating bundle, so the view's sources resolve here; an
                // all-negative bundle sorts descending but carries no `+1` VIEW_TAB
                // row to validate.
                let source_ids = self.dag.get_source_ids(&self.registry, id);
                self.validate_view_options(id, v.name, v.budgets, v.owner_view_id, &source_ids)?;
                self.validate_view_owner(id, v.name, v.owner_view_id, &view_creates)?;
                (v.schema_id, v.name, v.pk, RelationKind::View)
            };
            check_col_defs(kind, &col_defs)
                .and_then(|()| validate_pk_against_cols(&col_defs, pk.as_slice()))
                .map_err(|e| format!("{} '{name}' (id={id}) {e}", kind.noun()))?;
            reject_unstorable_name(name, kind.noun())?;

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
                // `validate_pk_against_cols` above proved the PK list non-empty
                // and in range, which is what makes the self-reference type
                // lookup inside sound.
                self.validate_fk_columns(id, &col_defs, pk.as_slice())?;
            }

            self.precheck_qname_unique(sid, name, id, net_dead, &mut claimed)?;
        }

        if is_table {
            for &tid in net_dead {
                // A FK child being co-dropped in this same batch is
                // self-resolving — only a child *outside* the batch blocks the
                // drop. Mirrors the view-dependency filter below.
                let blocking = self
                    .fk_children_of(tid)
                    .iter()
                    .find(|r| net_dead.binary_search(&r.child_tid).is_err());
                if let Some(r) = blocking {
                    let (sn, tn) = self.qualified_name_or_unknown(r.child_tid);
                    return Err(format!("Integrity violation: table referenced by '{sn}.{tn}'"));
                }
            }
        }

        let dep_map = self.dag.get_dep_map(&self.registry);
        for &id in net_dead {
            if let Some(dependents) = dep_map.get(&id) {
                // A dependent that is itself being dropped in this same batch is
                // self-resolving — only an *outside* dependent blocks the drop.
                // net_dead is sorted, so binary_search.
                let still_active = dependents
                    .iter()
                    .any(|&dep_id| net_dead.binary_search(&dep_id).is_err());
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
    fn precheck_index_family(&mut self, batch: &Batch, net_dead: &[i64]) -> Result<(), String> {
        // The names this batch has already claimed, as `precheck_qname_unique`
        // threads one for relations. Without it two rows under one name both pass
        // the persisted-cache check and the second overwrites the first in
        // `index_by_name`, leaving one index live and unreachable.
        let mut claimed: FxHashSet<String> = FxHashSet::default();
        let noun = SysFamily::Index.row_noun();
        for i in batch.live_rows() {
            let (owner_id, cols, _) =
                read_idx_tab_row(batch, i).map_err(|rule| format!("Index: column list {rule}"))?;
            let index_name = payload_string(batch, i, IDXTAB_PAY_NAME);
            reject_unstorable_name(&index_name, noun)?;
            let entry = self.validate_index_registration(owner_id)?;

            // Bounds, per-column eligibility (STRING/BLOB/float), and
            // arity/stride limits, identical to what registration will enforce —
            // only the table-name context is added here.
            make_index_schema(cols.as_slice(), &entry.schema()).map_err(|e| {
                format!(
                    "{e} for table '{}' (tid={owner_id})",
                    self.qualified_name_or_unknown(owner_id).1
                )
            })?;

            // Only live indices are mapped, and a `+1` on one already failed the
            // net bound — so any hit is a different index.
            if self.caches.index_by_name.contains_key(&index_name) {
                return Err(format!("Index already exists: {index_name}"));
            }
            if let Some(dup) = claimed.replace(index_name) {
                return Err(format!("Index already exists: {dup}"));
            }
        }

        for i in batch.retracted_rows() {
            // The row, not `net_dead`: this needs `cols`, which a list of ids
            // does not carry.
            let (owner_id, cols, _) =
                read_idx_tab_row(batch, i).map_err(|rule| format!("Index: column list {rule}"))?;
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
                .relation(owner_id)
                .is_some_and(|e| e.schema().is_lone_pk_col(src_col));
            if is_lone_pk {
                continue;
            }
            // Scan sys_indices (pre-drop: the rows being dropped are still
            // present) for another unique index on this column that survives.
            let unique_remains = self
                .indices_on_cols(owner_id, &[src_col as u32])
                .iter()
                .any(|&(id, u)| u && net_dead.binary_search(&id).is_err());
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
