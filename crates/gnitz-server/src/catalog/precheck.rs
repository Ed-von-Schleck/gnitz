//! Catalog precheck — the master's trust boundary against client-pushed
//! system-table deltas.
use std::borrow::Cow;

use gnitz_store::relation::{IndexClaim, RelationKind};
use gnitz_wire::{
    low_bits_mask, payload_bytes, validate_user_identifier, BitIter, CatalogColumn, ColTabRow, ColType, FixedInt,
    FkRef, IdxTabRow, IdxTabSlot, RowSource, SchemaTabRow, TableProps, ViewParts,
};
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::{key::KeySpec, SchemaDescriptor};
use rustc_hash::FxHashSet;

use super::sys_reads::IdSet;
use super::sys_tables::{
    build_schema_from_col_defs, pk_signatures, read_rel_row, DdlBundle, PkShape, PkSignature, RelDetail, RelRow,
    SysFamily, SYSTEM_SCHEMA_ID,
};
use super::CatalogEngine;

/// The type rule on a SERIAL table's PK columns, given as `(name, type)`.
fn validate_serial_key<'a>(pk: impl IntoIterator<Item = (&'a str, ColType)>) -> Result<(), String> {
    match pk.into_iter().find(|(_, ty)| FixedInt::exact(ty.tc).is_none()) {
        Some((name, ty)) => Err(format!(
            "SERIAL primary key column '{name}' is {ty}; SERIAL needs an integer of at most 8 bytes"
        )),
        None => Ok(()),
    }
}

/// The name rule a catalog row must satisfy to be stored: a chain segment's
/// name (`segment`) is `_` then `[A-Za-z0-9_]`, every other name a user
/// identifier; and every one already canonical, since every name-index key is
/// compared byte-wise against the client's folded form.
fn check_stored_name(name: &str, noun: &str, segment: bool) -> Result<(), String> {
    if !segment {
        validate_user_identifier(name)?;
    } else if !name.starts_with('_') {
        return Err(format!("{noun} '{name}': a chain segment's name starts with '_'"));
    } else if !name.bytes().all(gnitz_wire::is_valid_ident_char) {
        return Err(format!("{noun} name contains invalid characters: {name}"));
    }
    if name.bytes().any(|c| c.is_ascii_uppercase()) {
        return Err(format!(
            "{noun} name '{name}' is not canonical: catalog names are stored ASCII-lowercase"
        ));
    }
    Ok(())
}

/// A system row carries weight ±1: a zero weight carries no contract, and the
/// engine's retractions emit a hard `-1`, which would leave `w - 1` of a heavier row.
fn check_row_weights(family: SysFamily, batch: &Batch) -> Result<(), String> {
    for i in 0..batch.len() {
        let w = batch.get_weight(i);
        if w.unsigned_abs() != 1 {
            return Err(format!(
                "catalog delta carries a {} row at weight {w} (expected ±1)",
                family.row_noun()
            ));
        }
    }
    Ok(())
}

/// At most one row per sign: two `+1` rows on one PK consolidate to weight 2.
fn check_pk_multiplicity(family: SysFamily, sig: &PkSignature) -> Result<(), String> {
    if sig.repeats_a_sign {
        return Err(format!(
            "system-catalog write carries more than one row for {}",
            family.pk_label(sig)
        ));
    }
    Ok(())
}

/// Bootstrap-owned ids sit below the floor, and one at or above `next_id`, the
/// id counter, was never allocated. Both are properties of the id space rather
/// than of the mutation's shape, so they cover every sign: a `-1` drops a
/// bootstrap row, a bare `+1` aliases a bootstrap id into the name indexes, and
/// a pair renames one.
fn check_id_range(family: SysFamily, sig: &PkSignature, next_id: u64) -> Result<(), String> {
    let id = sig.leading;
    if id < gnitz_wire::FIRST_USER_TABLE_ID {
        return Err(format!(
            "cannot {} a system {} ({})",
            sig.verb(),
            family.row_noun(),
            family.pk_label(sig)
        ));
    }
    if family.allocates_ids() && id >= next_id {
        return Err(format!(
            "{} was never allocated (next id {next_id})",
            family.pk_label(sig)
        ));
    }
    Ok(())
}

/// Whether row `ra` of `a` and row `rb` of `b` differ in a payload column outside
/// `mask` (bit `pi` exempts payload column `pi`). A system schema holds no
/// nullable column; STRING/BLOB compare by content through each side's own heap.
fn payload_differs(
    schema: &SchemaDescriptor,
    mask: u64,
    a: &impl RowSource,
    ra: usize,
    b: &impl RowSource,
    rb: usize,
) -> bool {
    BitIter(low_bits_mask(schema.num_payload_cols()) & !mask).any(|pi| {
        schema
            .locate(schema.payload_col_idx(pi))
            .cmp_non_null(a, ra, b, rb)
            .is_ne()
    })
}

/// A rewrite pair's `+1` may differ from its `-1` only in the family's declared
/// mask; a family declaring none admits no pair.
fn check_pair_fields(family: SysFamily, batch: &Batch, sig: &PkSignature) -> Result<(), String> {
    let (Some(nj), Some(pj)) = (sig.neg, sig.pos) else {
        return Ok(());
    };
    let Some(mask) = family.pair_change_mask() else {
        return Err(format!(
            "a {} admits no rewrite pair ({})",
            family.row_noun(),
            family.pk_label(sig)
        ));
    };
    if payload_differs(family.schema(), mask, batch, nj, batch, pj) {
        return Err(format!(
            "a system-catalog rewrite pair on {} changes a field it may not",
            family.pk_label(sig)
        ));
    }
    Ok(())
}

/// The rules a system batch must satisfy on its own, before any store is read,
/// with the id counter at `next_id`.
fn check_batch_shape(family: SysFamily, batch: &Batch, next_id: u64) -> Result<Vec<PkSignature>, String> {
    check_row_weights(family, batch)?;
    let sigs = pk_signatures(batch);
    for sig in &sigs {
        check_pk_multiplicity(family, sig)?;
        if family.retracts_with_owner() && sig.shape() == PkShape::Drop {
            return Err(format!(
                "a {} is retracted only with its owner ({})",
                family.row_noun(),
                family.pk_label(sig)
            ));
        }
        check_id_range(family, sig, next_id)?;
        check_pair_fields(family, batch, sig)?;
    }
    Ok(sigs)
}

/// A circuit `+1` may only name a view this same transaction creates — one under
/// a foreign `view_id` would pin its source table's drop or rewrite a running
/// circuit.
fn check_circuit_rows(batch: &Batch, created: &IdSet) -> Result<(), String> {
    for i in batch.live_rows() {
        let view_id = gnitz_wire::sys_leading_id(batch.get_pk_bytes(i));
        if !created.contains(view_id) {
            return Err(format!(
                "a circuit names view {view_id}, which this transaction does not create"
            ));
        }
    }
    Ok(())
}

impl CatalogEngine {
    /// Check FK column `col` of relation `self_table_id`, about to be registered
    /// with `self_schema` over `self_cols`. `net_dead` is what the same TABLE_TAB
    /// delta drops.
    fn validate_fk_column(
        &self,
        col: &CatalogColumn,
        fk: FkRef,
        self_table_id: u64,
        self_cols: &[CatalogColumn],
        self_schema: &SchemaDescriptor,
        net_dead: &IdSet,
    ) -> Result<(), String> {
        // The target is a legal reference iff it is the parent's lone PK column,
        // or it carries its own UNIQUE index. Mirrors the production planner gate.
        let target_type = if fk.table_id == self_table_id {
            // Self-referential FK: the table has no UNIQUE index yet, so the
            // target must be its lone PK column. The downstream probe reads the
            // referenced value out of the packed PK region, which is the whole
            // key only when the PK is a single column.
            if self_schema.lone_pk_col() != Some(fk.col as usize) {
                return Err("FK must reference the primary key or a UNIQUE-indexed column".into());
            }
            self_cols[fk.col as usize].def.ty
        } else {
            if net_dead.contains(fk.table_id) {
                return Err(format!(
                    "FK references table {}, which this transaction drops",
                    fk.table_id
                ));
            }
            let entry = self
                .registry
                .relation(fk.table_id)
                .ok_or_else(|| format!("FK references unknown table_id {}", fk.table_id))?;
            // Not covered by the PK/UNIQUE tests below, which read only the schema:
            // a stream and a view both have a PK that looks exactly like a base
            // table's without being the unique, stored key the parent probe reads.
            if !entry.kind().is_base_table() {
                return Err(format!(
                    "FK references relation {}, which is a {}; a FOREIGN KEY must reference a base table",
                    fk.table_id,
                    entry.kind().noun()
                ));
            }
            if entry.schema().lone_pk_col() != Some(fk.col as usize) {
                // A composite index does not satisfy a single-column FK: a
                // unique (a, b) does not guarantee uniqueness of `a` alone, so
                // match only a single-column unique index on the referenced col.
                let has_unique = entry.index_on(&[fk.col]).is_some_and(|ic| ic.is_unique());
                if !has_unique {
                    return Err("FK must reference the primary key or a UNIQUE-indexed column".into());
                }
            }
            // In range: the checks above admit only the parent's lone PK column
            // or a column a unique index names.
            self.read_column_defs(fk.table_id)?[fk.col as usize].def.ty
        };

        // The preflight compares child and parent values as the referenced
        // column's key image, so both columns carry one type; SQL adopts the
        // parent's type before the engine sees the column.
        if col.def.ty != target_type {
            return Err(format!(
                "FK type mismatch: child type {} cannot reference target type {target_type}",
                col.def.ty
            ));
        }
        Ok(())
    }

    /// Claim `(schema, name)` for `self_id`, refused when an existing relation
    /// holds it. A rewrite pair that leaves the name unchanged re-registers the
    /// incumbent, so `existing == self_id` is not one.
    ///
    /// `net_dead` is this same bundle's net-dead PKs in this same family. An
    /// incumbent among them is not a collision either: one bundle may retire an
    /// id and register a different one under the same name — an ALTER VIEW is
    /// exactly that — and that retired id is returned. Precheck reads the name
    /// index *before* apply, so it still maps the name to the outgoing id.
    fn claim_qname<'a>(
        &self,
        sid: u64,
        name: Cow<'a, str>,
        self_id: u64,
        net_dead: &IdSet,
        claimed: &mut FxHashSet<(u64, Cow<'a, str>)>,
    ) -> Result<Option<u64>, String> {
        let schema_name = self
            .schema_name(sid)
            .ok_or_else(|| format!("Schema with ID {sid} does not exist"))?;
        let incumbent = self.relation_id(sid, &name).filter(|&existing| existing != self_id);
        let displaced = incumbent.filter(|&e| net_dead.contains(e));
        if incumbent != displaced || !claimed.insert((sid, name.clone())) {
            return Err(format!(
                "Table or view already exists: {}",
                gnitz_wire::qualified_key(&schema_name, &name)
            ));
        }
        Ok(displaced)
    }

    /// The `-1` must content-equal the live row — you may only retract the
    /// element that exists — and `live + Σ batch` must land in `{0, 1}`: the sys
    /// stores run no `enforce_unique_pk`, so nothing else stops a duplicate live
    /// head or a persistent negative ghost. Returns the net weight.
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
            if payload_differs(family.schema(), 0, src, ri, batch, nj) {
                return Err(format!(
                    "catalog changed concurrently: the retracted {noun} differs from the current one"
                ));
            }
        }
        // `check_batch_shape` admitted at most one row of each sign, each of weight ±1.
        let net = live_weight + sig.pos.is_some() as i64 - sig.neg.is_some() as i64;
        if !(0..=1).contains(&net) {
            return Err(format!(
                "system-catalog write would leave {} at net weight {net} (expected 0 or 1)",
                family.pk_label(sig)
            ));
        }
        Ok(net)
    }

    /// [`check_batch_shape`], then the CAS and net bound per PK against the live
    /// store. Returns the per-PK signatures and the PKs whose net is dead: the
    /// genuine drops, which a rename pair's net-live `-1` is not.
    fn check_family_contract(&self, family: SysFamily, batch: &Batch) -> Result<(Vec<PkSignature>, IdSet), String> {
        let sigs = check_batch_shape(family, batch, self.next_id)?;
        let mut net_dead: Vec<u64> = Vec::new();
        for sig in &sigs {
            if self.check_cas_and_net(family, batch, sig)? <= 0 {
                net_dead.push(sig.leading);
            }
        }
        Ok((sigs, IdSet::new(net_dead)))
    }

    /// What a COL_TAB write means once [`Self::check_family_contract`] has settled
    /// its shape: everything below depends on the *owner*, which the contract
    /// cannot see. The guards are scoped to the transition rather than blanket —
    /// a blanket one would reject RENAME COLUMN, which is legal on a PK column
    /// and under a dependent view because views bind columns by ordinal.
    fn precheck_column_family(&self, batch: &Batch, sigs: &[PkSignature]) -> Result<(), String> {
        for sig in sigs {
            // COL_TAB PK = `(owner_id, col_idx)`.
            let (owner_id, col_idx) = (sig.leading, sig.member.expect("a COL_TAB key is a pair"));
            let decode = |r| ColTabRow::read(batch, r).and_then(|r| r.column());
            let old = sig.neg.map(decode).transpose()?;
            let new = sig.pos.map(decode).transpose()?;

            let Some(owner) = self.registry.relation(owner_id) else {
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
            if !owner.kind().is_base_table() {
                return Err(format!("cannot ALTER a column of {owner_id}: not a user base table"));
            }

            let Some(new) = new else {
                unreachable!("the contract refuses a column PK with no `+1`");
            };
            match old {
                Some(old) => {
                    // A dropped column is gone from every surface: nothing may
                    // rename, re-show or re-null it.
                    if old.def.is_hidden {
                        return Err(format!("cannot ALTER dropped column {col_idx} of table {owner_id}"));
                    }
                    if new.def.is_nullable != old.def.is_nullable && !new.def.is_nullable {
                        return Err("a column-ALTER may only set is_nullable 0→1 (DROP NOT NULL)".into());
                    }
                    let (hides, unnulls) = (new.def.is_hidden, new.def.is_nullable != old.def.is_nullable);
                    let op = match (hides, unnulls) {
                        (true, _) => Some("DROP COLUMN"),
                        (false, true) => Some("DROP NOT NULL"),
                        // A rename: legal on a PK column and under dependent
                        // views, which bind columns by ordinal.
                        (false, false) => None,
                    };
                    let name = &old.def.name;
                    // Before the schema build, whose PK rules would refuse a PK
                    // column's DROP NOT NULL with a less direct message.
                    if let Some(op) = op.filter(|_| owner.schema().is_pk_col(col_idx as usize)) {
                        return Err(format!("cannot {op} '{name}': it is a primary-key column"));
                    }
                    let prospective = self.col_defs_with(owner_id, col_idx, new)?;
                    build_schema_from_col_defs(RelationKind::BaseTable, &prospective, owner.schema().pk_cols())
                        .map_err(|e| format!("cannot ALTER COLUMN on table {owner_id}: {e}"))?;
                    let Some(op) = op else { continue };
                    if hides {
                        // Before the index test: an FK column also carries its FK index.
                        if old.fk.is_some() {
                            return Err(format!("cannot DROP COLUMN '{name}': it carries a foreign key"));
                        }
                        let indexed = owner
                            .indexes()
                            .iter()
                            .any(|ix| ix.cols().as_slice().contains(&(col_idx as u32)));
                        if indexed {
                            return Err(format!(
                                "cannot DROP COLUMN '{name}': it is covered by a secondary index; \
                                 drop the index first"
                            ));
                        }
                    }
                    self.reject_if_dependent_views(owner_id, op, &IdSet::default())?;
                }
                None => self.precheck_column_append(new, owner_id, col_idx, &owner.schema())?,
            }
        }
        Ok(())
    }

    /// Reject `op` on `owner_id` while a view outside `dropped` scans it: a
    /// compiled circuit's `ScanDelta` register schema is baked from its source's
    /// descriptor, its operator traces hold that source's rows at the old shape,
    /// and a dropped source leaves it nothing to read.
    fn reject_if_dependent_views(&self, owner_id: u64, op: &str, dropped: &IdSet) -> Result<(), String> {
        // Name the relation and the user views that block it: the recovery is to
        // drop those, which neither a bare id nor a chain segment's minted name
        // tells the author.
        let blockers = IdSet::new(
            self.dag
                .dependents_of(owner_id)
                .iter()
                .map(|&dep| self.dag.chain_of(dep))
                .filter(|&view| !dropped.contains(view)),
        );
        if blockers.is_empty() {
            return Ok(());
        }
        let named: Vec<String> = blockers.ids().iter().map(|&id| self.qualified_name(id)).collect();
        Err(format!(
            "cannot {op} '{}': it has dependent views ({}) — drop them first",
            self.qualified_name(owner_id),
            named.join(", ")
        ))
    }

    /// `owner_id`'s column records as this batch's `+1` row for `col_idx` leaves
    /// them: the decoded row replaces the live record at that index, or extends
    /// the set when the transition appends one.
    fn col_defs_with(&self, owner_id: u64, col_idx: u64, row: CatalogColumn) -> Result<Vec<CatalogColumn>, String> {
        let mut defs = self.read_column_defs(owner_id)?;
        match defs.get_mut(col_idx as usize) {
            Some(live) => *live = row,
            None => defs.push(row),
        }
        Ok(defs)
    }

    /// ADD COLUMN: one unpaired `+1` appending a trailing nullable payload
    /// column to registered base table `owner_id`.
    fn precheck_column_append(
        &self,
        appended: CatalogColumn,
        owner_id: u64,
        col_idx: u64,
        owner_schema: &SchemaDescriptor,
    ) -> Result<(), String> {
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
        let prospective = self.col_defs_with(owner_id, col_idx, appended.clone())?;
        build_schema_from_col_defs(RelationKind::BaseTable, &prospective, owner_schema.pk_cols())
            .map_err(|e| format!("cannot ADD COLUMN on table {owner_id}: {e}"))?;
        // A new column over existing rows is unconditionally nullable, carries
        // no FK, and is visible.
        if !appended.def.is_nullable {
            return Err("ADD COLUMN must append a nullable column".into());
        }
        if appended.def.is_hidden || appended.fk.is_some() {
            return Err("ADD COLUMN must not append a hidden or foreign-key column".into());
        }
        // Last, as in the rewrite-pair arm: a malformed append is reported as
        // malformed whatever depends on the table.
        self.reject_if_dependent_views(owner_id, "ADD COLUMN", &IdSet::default())
    }

    /// Validate one family of a `DDL_TXN` against the catalog as the bundle's
    /// earlier families left it. Returns the ids the delta drops.
    ///
    /// Exhaustive over `SysFamily` (like `fire_hooks`): a newly-added family must
    /// decide here whether it carries guards beyond the contract, rather than
    /// falling into a silent `_` arm.
    pub(in crate::catalog) fn precheck_family(&self, family: SysFamily, batch: &Batch) -> Result<IdSet, String> {
        let (sigs, net_dead) = self.check_family_contract(family, batch)?;
        match family {
            SysFamily::Schema => self.precheck_schema_family(batch, &net_dead),
            SysFamily::Table | SysFamily::View => self.precheck_relation_family(family, batch, &sigs, &net_dead),
            SysFamily::Column => self.precheck_column_family(batch, &sigs),
            SysFamily::Index => self.precheck_index_family(batch, &net_dead),
            SysFamily::Sequence | SysFamily::Circuit => Ok(()),
        }?;
        Ok(net_dead)
    }

    /// The cross-family rules of one `DDL_TXN` bundle: what no family's own arm
    /// can see, because each is prechecked against a catalog the bundle's other
    /// families have not reached yet. Run before the first family is applied, so
    /// a rejection has written nothing.
    pub(in crate::catalog) fn precheck_bundle(&self, families: &DdlBundle) -> Result<(), String> {
        if let Some(f) = SysFamily::ALL
            .into_iter()
            .find(|&f| f.master_only() && families.get(f).is_some())
        {
            return Err(format!(
                "family {} ({}) is not writable from the wire",
                f.id(),
                f.name()
            ));
        }
        if let Some(cols) = families.get(SysFamily::Column) {
            self.check_column_owners(cols, families)?;
        }
        if let Some(b) = families.get(SysFamily::Circuit) {
            let created = families.get(SysFamily::View).map_or_else(IdSet::default, |v| {
                IdSet::new(
                    pk_signatures(v)
                        .into_iter()
                        .filter(|s| s.shape() == PkShape::Create)
                        .map(|s| s.leading),
                )
            });
            check_circuit_rows(b, &created)?;
        }
        Ok(())
    }

    /// Every column record must name an owner this bundle creates or the registry
    /// holds: a phantom owner is never dropped, so nothing would ever retract the row.
    fn check_column_owners(&self, cols: &Batch, families: &DdlBundle) -> Result<(), String> {
        let created = IdSet::new(
            [SysFamily::Table, SysFamily::View]
                .into_iter()
                .filter_map(|family| families.get(family))
                .flat_map(|b| b.live_rows().map(|i| gnitz_wire::sys_leading_id(b.get_pk_bytes(i)))),
        );
        let mut altered: Option<u64> = None;
        for i in cols.live_rows() {
            let owner_id = gnitz_wire::sys_leading_id(cols.get_pk_bytes(i));
            // An ALTER must be its bundle's only change, on one owner: nothing can undo
            // its descriptor swap, so the swap must be the bundle's last fallible step.
            if self.registry.relation(owner_id).is_some() {
                if altered.is_some_and(|a| a != owner_id) || families.family_count() > 1 {
                    return Err("a column ALTER must be the only change in its DDL transaction".into());
                }
                altered = Some(owner_id);
            } else if !created.contains(owner_id) {
                return Err(format!(
                    "column record names owner {owner_id}, which this transaction \
                     does not create and the catalog does not hold"
                ));
            }
        }
        Ok(())
    }

    /// SCHEMA_TAB: a CREATE must not collide with a live schema name — nor with one
    /// this batch already claims — and a DROP must find the schema empty. Schema ids
    /// share one id space with relation ids, so the member count, not the
    /// relation-keyed dep map, is the whole drop guard.
    fn precheck_schema_family(&self, batch: &Batch, net_dead: &IdSet) -> Result<(), String> {
        // Two `+1` rows under one name both miss the live schemas and both apply:
        // the name would resolve to the second, leaving the first id live and
        // unreachable.
        let mut claimed: FxHashSet<Cow<'_, str>> = FxHashSet::default();
        for i in batch.live_rows() {
            let name = SchemaTabRow::read(batch, i).map_err(|e| format!("Schema: {e}"))?.name;
            // Nothing synthesizes a schema name, so the reserved `_` prefix
            // applies with no carve-out.
            check_stored_name(&name, "schema", false)?;
            if self.schema_id(&name).is_some() || !claimed.insert(name.clone()) {
                return Err(format!("Schema already exists: {name}"));
            }
        }
        for &sid in net_dead.ids() {
            let n = self.caches.relation_by_name.get(&sid).map_or(0, |names| names.len());
            if n > 0 {
                return Err(format!("Schema not empty: {n} relation(s) remain; drop them first"));
            }
        }
        Ok(())
    }

    /// TABLE_TAB / VIEW_TAB: the CREATE guards (a free relation id, column-record
    /// admissibility, FK column types, qualified-name uniqueness) and the DROP
    /// guards (FK children, view dependents).
    ///
    /// The drop guards key on `net_dead` — the PKs whose bundle net is dead — not
    /// raw `weight < 0`, so a rename pair's net-live `-1` is never rejected as
    /// "referenced by FK" / "has dependent views".
    fn precheck_relation_family(
        &self,
        family: SysFamily,
        batch: &Batch,
        sigs: &[PkSignature],
        net_dead: &IdSet,
    ) -> Result<(), String> {
        let mut claimed: FxHashSet<(u64, Cow<'_, str>)> = FxHashSet::default();
        // The retired ids a relation of the same name takes over: an ALTER VIEW.
        let mut replaced: Vec<u64> = Vec::new();
        let creates = IdSet::new(sigs.iter().filter(|s| s.shape() == PkShape::Create).map(|s| s.leading));
        if let Some(id) = creates.ids().iter().find(|&&id| self.registry.relation(id).is_some()) {
            return Err(format!("relation id {id} already exists"));
        }
        // The only owners a created view may name: a chain is created whole.
        let rels: Vec<RelRow<'_>> = batch
            .live_rows()
            .map(|i| read_rel_row(family, batch, i))
            .collect::<Result<_, _>>()?;
        let created_users = IdSet::new(
            rels.iter()
                .filter(|rel| {
                    creates.contains(rel.id) && matches!(rel.detail, RelDetail::View(ViewParts { owner: None, .. }))
                })
                .map(|rel| rel.id),
        );

        for rel in &rels {
            // The system schema holds the catalog's own relations and nothing else.
            if rel.schema_id == SYSTEM_SCHEMA_ID {
                return Err(format!("{rel}: the system schema takes no relation"));
            }
            let col_defs = self.read_column_defs(rel.id)?;
            let schema = build_schema_from_col_defs(rel.kind(), &col_defs, rel.pk.as_slice())
                .map_err(|e| format!("{rel} {e}"))?;
            // A leading `_` names a chain segment and nothing else.
            let segment = matches!(rel.detail, RelDetail::View(ViewParts { owner: Some(_), .. }));
            check_stored_name(&rel.name, rel.kind().noun(), segment)?;
            // An FK probes its parent's store on every write of the child, which only
            // a base table's ingest runs; a stream push must stay a pure append.
            if !rel.kind().is_base_table() {
                if let Some(cd) = col_defs.iter().find(|cd| cd.fk.is_some()) {
                    return Err(format!(
                        "{rel}: column '{}' may not carry a FOREIGN KEY; only a base table's may",
                        cd.def.name
                    ));
                }
            }
            match rel.detail {
                RelDetail::Table(TableProps { serial, .. }) => {
                    if serial {
                        let pk_cols = rel.pk.as_slice().iter().map(|&c| &col_defs[c as usize]);
                        validate_serial_key(pk_cols.map(|cd| (cd.def.name.as_str(), cd.def.ty)))
                            .map_err(|e| format!("{rel}: {e}"))?;
                    }
                    for cd in &col_defs {
                        if let Some(fk) = cd.fk {
                            self.validate_fk_column(cd, fk, rel.id, &col_defs, &schema, net_dead)?;
                        }
                    }
                }
                RelDetail::View(ViewParts { owner: Some(owner), .. })
                    if creates.contains(rel.id) && !created_users.contains(owner) =>
                {
                    return Err(format!(
                        "{rel} names owner {owner}, which is not a user view this bundle creates"
                    ));
                }
                RelDetail::View(_) => {}
            }
            let displaced = self.claim_qname(rel.schema_id, rel.name.clone(), rel.id, net_dead, &mut claimed)?;
            // The drop cascade would take the outgoing relation's indexes with it.
            let owns_index = |old: u64| {
                let named = |c: &IndexClaim| matches!(c, IndexClaim::Index { .. });
                self.registry
                    .relation(old)
                    .is_some_and(|e| e.indexes().iter().any(|ix| ix.claims().iter().any(named)))
            };
            if displaced.is_some_and(owns_index) {
                return Err(format!(
                    "{rel} owns an index, which replacing it would drop; DROP INDEX first"
                ));
            }
            replaced.extend(displaced);
        }

        if family == SysFamily::Table {
            for &tid in net_dead.ids() {
                // A FK child being co-dropped in this same batch is
                // self-resolving — only a child *outside* the batch blocks the
                // drop. Mirrors the view-dependency filter below.
                let blocking = self
                    .fk_children_of(tid)
                    .iter()
                    .find(|r| !net_dead.contains(r.child_tid));
                if let Some(r) = blocking {
                    let child = self.qualified_name(r.child_tid);
                    return Err(format!("Integrity violation: table referenced by '{child}'"));
                }
            }
        }

        // A dependent that is itself being dropped in this same batch is
        // self-resolving — only an *outside* dependent blocks the drop.
        net_dead.ids().iter().try_for_each(|&id| {
            let op = if replaced.contains(&id) { "replace" } else { "DROP" };
            self.reject_if_dependent_views(id, op, net_dead)
        })
    }

    /// The owner and column rules of a CREATE INDEX on `owner_id`, `unique` or
    /// not, and the key span they admit.
    pub(crate) fn validate_index_create(&self, owner_id: u64, cols: &[u32], unique: bool) -> Result<KeySpec, String> {
        let entry = self.registry.index_owner(owner_id, unique)?;
        // A segment drops its rows once its chain is built, and its index would
        // keep their entries.
        if self.dag.chain_of(owner_id) != owner_id {
            return Err(format!("Index: owner {owner_id} is a segment of a view's chain"));
        }
        let spec = KeySpec::new(cols, &entry.schema()).map_err(|e| {
            format!(
                "{e} for '{}' (tid={owner_id})",
                self.qualified_name_or_unknown(owner_id).1
            )
        })?;
        let defs = self.read_column_defs(owner_id)?;
        if let Some(&c) = cols
            .iter()
            .find(|&&c| defs.get(c as usize).is_some_and(|d| d.def.is_hidden))
        {
            // A table hides the columns it dropped, a view the key slots it adds.
            let why = if entry.kind().is_base_table() {
                "dropped"
            } else {
                "hidden"
            };
            return Err(format!(
                "Index: column {c} of {} {owner_id} is {why}",
                entry.kind().noun()
            ));
        }
        Ok(spec)
    }

    /// IDX_TAB: a CREATE must name an owner that admits the index, an admissible
    /// column list and a free index name; a DROP must not strip the uniqueness an FK
    /// depends on. The drop guards read the batch row rather than probing: the
    /// contract's CAS proved every `-1` content-equals the live one, `name` and
    /// `source_cols` included.
    fn precheck_index_family(&self, batch: &Batch, net_dead: &IdSet) -> Result<(), String> {
        // Every live index name, then the ones this batch claims: only live
        // indexes are scanned, and a `+1` on one already failed the net bound, so
        // any hit is a different index.
        let live = self.sys_relation(SysFamily::Index).full_scan();
        let mut taken: FxHashSet<Cow<'_, [u8]>> = (0..live.len())
            .map(|i| payload_bytes(&*live, i, IdxTabSlot::name as usize))
            .collect();
        let noun = SysFamily::Index.row_noun();
        for i in batch.live_rows() {
            let r = IdxTabRow::read(batch, i).map_err(|e| format!("Index: {e}"))?;
            let index_name = r.name;
            check_stored_name(&index_name, noun, false)?;
            self.validate_index_create(r.owner_id, r.source_col_idx.as_slice(), r.is_unique)?;
            if !taken.insert(Cow::Owned(index_name.as_bytes().to_vec())) {
                return Err(format!("Index already exists: {index_name}"));
            }
        }

        for i in batch.retracted_rows() {
            // The row, not `net_dead`: this needs `cols`, which a list of ids
            // does not carry.
            let IdxTabRow { owner_id, source_col_idx: cols, .. } =
                IdxTabRow::read(batch, i).map_err(|e| format!("Index: {e}"))?;
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
            let owner = self.registry.relation(owner_id);
            if owner.is_some_and(|e| e.schema().lone_pk_col() == Some(src_col)) {
                continue;
            }
            // The claims are pre-drop: this batch has not been applied yet.
            let unique_remains = owner.and_then(|r| r.index_on(&[src_col as u32])).is_some_and(|ix| {
                ix.claims()
                    .iter()
                    .any(|c| matches!(*c, IndexClaim::Index { id, unique: true } if !net_dead.contains(id)))
            });
            if !unique_remains {
                return Err(format!(
                    "Integrity violation: index on '{}' is referenced by a \
                     foreign key and no unique index would remain on the column",
                    self.qualified_name(owner_id),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/precheck.rs"]
mod tests;
