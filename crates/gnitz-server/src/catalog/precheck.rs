//! Catalog precheck — the master's trust boundary against client-pushed
//! system-table deltas: row shape, the CAS against the stored row, and the
//! catalog-level guards. A created view's circuit and the rules on what it
//! scans are refused by its register hook, and `apply_bundle` undoes the
//! bundle.
use std::borrow::Cow;

use gnitz_store::relation::{IndexClaim, Relation};
use gnitz_wire::{
    BitIter, CatalogColumn, ColTabRow, FixedInt, FkRef, IdxTabRow, IdxTabSlot, RowSource, SchemaTabRow, SchemaTabSlot,
    TableProps, ViewParts, payload_bytes, validate_user_identifier,
};
use gnitz_zset::algebra::KeySpec;
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaDescriptor;
use rustc_hash::FxHashSet;

use super::CatalogEngine;
use super::sys_tables::{
    DdlBundle, IdSet, PkShape, PkSignature, RelDetail, RelRow, SYSTEM_SCHEMA_ID, SysFamily, build_schema_from_col_defs,
    pk_signatures, read_rel_row,
};

/// The name rule a schema, relation or index row must satisfy to be stored: a
/// chain segment's name (`segment`) is `_` then `[A-Za-z0-9_]`, every other
/// name a user identifier; and every one already canonical, since every name-index key is
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

/// Bootstrap-owned ids sit below [`gnitz_wire::FIRST_USER_TABLE_ID`]. That is a
/// property of the id space rather than of the mutation's shape, so it covers
/// every sign: a `-1` drops a bootstrap row, a bare `+1` aliases a bootstrap id
/// into the name indexes, and a pair renames one.
fn check_bootstrap_id(family: SysFamily, sig: &PkSignature) -> Result<(), String> {
    if sig.leading < gnitz_wire::FIRST_USER_TABLE_ID {
        return Err(format!(
            "cannot {} a system {} ({})",
            sig.verb(),
            family.row_noun(),
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
    BitIter(schema.payload_slots() & !mask).any(|pi| schema.payload_locator(pi).cmp_non_null(a, ra, b, rb).is_ne())
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

/// The rules a system batch must satisfy on its own, before any store is read.
fn check_batch_shape(family: SysFamily, batch: &Batch) -> Result<Vec<PkSignature>, String> {
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
        check_bootstrap_id(family, sig)?;
        check_pair_fields(family, batch, sig)?;
    }
    Ok(sigs)
}

/// Whether column `col` of `parent` is a legal FK target: its lone PK column, or a
/// column a single-column UNIQUE index covers through a claim outside `dropping`.
/// A composite index never qualifies: a unique `(a, b)` does not make `a` alone
/// unique.
fn holds_fk_target(parent: &Relation, col: u32, dropping: &IdSet) -> bool {
    parent.schema().lone_pk_col() == Some(col as usize)
        || parent.index_on(&[col]).is_some_and(|ix| {
            ix.claims()
                .iter()
                .any(|c| matches!(*c, IndexClaim::Index { id, unique: true } if !dropping.contains(id)))
        })
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
    /// with `self_schema` over `self_cols`. `dropped` is what the same TABLE_TAB
    /// delta drops.
    fn validate_fk_column(
        &self,
        col: &CatalogColumn,
        fk: FkRef,
        self_table_id: u64,
        self_cols: &[CatalogColumn],
        self_schema: &SchemaDescriptor,
        dropped: &IdSet,
    ) -> Result<(), String> {
        let child = &col.def.name;
        let target_type = if fk.table_id == self_table_id {
            let Some(target) = self_cols.get(fk.col as usize) else {
                return Err(format!(
                    "FK column '{child}' references column {} of its own table, which has {} columns",
                    fk.col,
                    self_cols.len()
                ));
            };
            // Self-referential FK: the table has no UNIQUE index yet — not even
            // one its own CREATE declares — so the target must be its lone PK
            // column. The downstream probe reads the referenced value out of the
            // packed PK region, which is the whole key only when the PK is a
            // single column.
            if self_schema.lone_pk_col() != Some(fk.col as usize) {
                return Err(format!(
                    "FK column '{child}' references '{}' of its own table; a self-referencing FK \
                     must reference the table's lone primary key column",
                    target.def.name
                ));
            }
            target.def.ty
        } else {
            if dropped.contains(fk.table_id) {
                return Err(format!(
                    "FK column '{child}' references {}, which this transaction drops",
                    self.qualified_name(fk.table_id)
                ));
            }
            let entry = self
                .registry
                .relation(fk.table_id)
                .ok_or_else(|| format!("FK column '{child}' references unknown table_id {}", fk.table_id))?;
            let parent = self.qualified_name(fk.table_id);
            // Not covered by the PK/UNIQUE tests below, which read the parent's
            // key and index claims but not its kind: a stream's or a view's PK
            // looks like a base table's without being the unique, stored key the
            // parent probe reads.
            if !entry.kind().is_base_table() {
                return Err(format!(
                    "FK column '{child}' references {parent}, which is a {}; \
                     a FOREIGN KEY must reference a base table",
                    entry.kind().noun()
                ));
            }
            let defs = self.read_column_defs(fk.table_id)?;
            let Some(target) = defs.get(fk.col as usize) else {
                return Err(format!(
                    "FK column '{child}' references column {} of {parent}, which has {} columns",
                    fk.col,
                    defs.len()
                ));
            };
            // No index counts as dropping here: `dropped` holds this delta's
            // relation ids, and an index drop arrives in the index family's delta.
            if !holds_fk_target(entry, fk.col, &IdSet::default()) {
                return Err(format!(
                    "FK column '{child}' references {parent}.{}, which is neither its lone \
                     primary key nor covered by a single-column UNIQUE index",
                    target.def.name
                ));
            }
            target.def.ty
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
    /// `dropped` is what this same family's delta drops. An incumbent among them
    /// is not a collision: an ALTER VIEW retires an id and registers another
    /// under the same name, and the retired id is returned. The name index is
    /// read before apply, so it still maps the name to the outgoing id.
    fn claim_qname<'a>(
        &self,
        sid: u64,
        name: Cow<'a, str>,
        self_id: u64,
        dropped: &IdSet,
        claimed: &mut FxHashSet<(u64, Cow<'a, str>)>,
    ) -> Result<Option<u64>, String> {
        let schema_name = self
            .schema_name(sid)
            .ok_or_else(|| format!("Schema with ID {sid} does not exist"))?;
        let incumbent = self.relation_id(sid, &name).filter(|&existing| existing != self_id);
        let displaced = incumbent.filter(|&e| dropped.contains(e));
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
    /// head or a persistent negative ghost.
    fn check_cas_and_net(&self, family: SysFamily, batch: &Batch, sig: &PkSignature) -> Result<(), String> {
        let noun = family.row_noun();
        let live = self
            .sys_relation(family)
            .unique_live_row_at(batch.get_pk_bytes(sig.row));
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
        let net = i64::from(live.is_some()) + sig.pos.is_some() as i64 - sig.neg.is_some() as i64;
        if !(0..=1).contains(&net) {
            return Err(format!(
                "system-catalog write would leave {} at net weight {net} (expected 0 or 1)",
                family.pk_label(sig)
            ));
        }
        Ok(())
    }

    /// What a COL_TAB write means once [`Self::precheck_family`] has settled
    /// its shape: everything below depends on the *owner*, which the contract
    /// cannot see. The guards are scoped to the transition rather than blanket —
    /// a blanket one would reject RENAME COLUMN, which is legal on a PK column
    /// and under a dependent view because views bind columns by ordinal.
    fn precheck_column_family(&self, batch: &Batch, sigs: &[PkSignature]) -> Result<(), String> {
        for sig in sigs {
            // COL_TAB PK = `(owner_id, col_idx)`.
            let (owner_id, col_idx) = (sig.leading, sig.member.expect("a COL_TAB key is a pair"));
            // Decoded before the owner lookup, so a CREATE bundle's malformed
            // record is refused here, before anything is written.
            let decode = |r| ColTabRow::read(batch, r).and_then(|r| r.column());
            // The shape rules refuse a column PK with no `+1`.
            let new = decode(sig.pos.expect("a column PK carries a +1"))?;
            let old = sig.neg.map(decode).transpose()?;

            // An unregistered owner is one this bundle creates;
            // `check_column_owners` owns those rows. A `-1` names a live record,
            // whose owner is registered.
            let Some(owner) = self.registry.relation(owner_id) else {
                continue;
            };

            // Every column transition — RENAME, DROP, ADD — needs a user base
            // table owner, so every other `RelationKind` fails here.
            if !owner.kind().is_base_table() {
                return Err(format!("cannot ALTER a column of {owner_id}: not a user base table"));
            }
            let schema = owner.schema();
            let op: Option<&str> = match old {
                None => {
                    // ADD COLUMN. The append must be *trailing*, and this is the
                    // only check that makes it so: the rebuild path maps the
                    // owner's defs positionally, so a gap would silently shift
                    // every column past it rather than fail. (A duplicate index
                    // is a duplicate COL_TAB PK, which the net bound already
                    // rejects.)
                    if col_idx as usize != schema.num_columns() {
                        return Err(format!(
                            "cannot ADD COLUMN at index {col_idx} on table {owner_id}: \
                             columns must be appended at index {}",
                            schema.num_columns()
                        ));
                    }
                    // A new column over existing rows is unconditionally
                    // nullable, carries no FK, and is visible.
                    if !new.def.is_nullable {
                        return Err("ADD COLUMN must append a nullable column".into());
                    }
                    if new.def.is_hidden || new.fk.is_some() {
                        return Err("ADD COLUMN must not append a hidden or foreign-key column".into());
                    }
                    Some("ADD COLUMN")
                }
                Some(old) => {
                    // A dropped column is gone from every surface: nothing may
                    // rename, re-show or re-null it.
                    if old.def.is_hidden {
                        return Err(format!("cannot ALTER dropped column {col_idx} of table {owner_id}"));
                    }
                    if old.def.is_nullable && !new.def.is_nullable {
                        return Err("a column-ALTER may only set is_nullable 0→1 (DROP NOT NULL)".into());
                    }
                    let hides = new.def.is_hidden;
                    let op = if hides {
                        Some("DROP COLUMN")
                    } else if new.def.is_nullable != old.def.is_nullable {
                        Some("DROP NOT NULL")
                    } else {
                        // A rename: legal on a PK column and under dependent
                        // views, which bind columns by ordinal.
                        None
                    };
                    let name = &old.def.name;
                    // Before the schema build, whose PK rules would refuse a PK
                    // column's DROP NOT NULL with a less direct message.
                    if let Some(op) = op
                        && schema.is_pk_col(col_idx as usize)
                    {
                        return Err(format!("cannot {op} '{name}': it is a primary-key column"));
                    }
                    // Hiding a column cannot fail the schema build, so these
                    // refusals may precede it.
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
                    op
                }
            };
            // The owner's column records as this `+1` leaves them: it replaces
            // the live record at `col_idx`, or extends the set on an append.
            let mut defs = self.read_column_defs(owner_id)?;
            match defs.get_mut(col_idx as usize) {
                Some(live) => *live = new,
                None => defs.push(new),
            }
            unique_visible_names(&defs)
                .and_then(|()| build_schema_from_col_defs(&defs, schema.pk_cols()))
                .map_err(|e| format!("cannot {} on table {owner_id}: {e}", op.unwrap_or("RENAME COLUMN")))?;
            // Last: a malformed ALTER is reported as malformed whatever depends
            // on the table.
            if let Some(op) = op {
                self.reject_if_dependent_views(owner_id, op, &IdSet::default())?;
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

    /// Validate one family of a `DDL_TXN` against the catalog as the bundle's
    /// earlier families left it: [`check_batch_shape`], the CAS and net bound per
    /// PK against the live store, then the family's own guards. Returns the ids
    /// the delta drops: the PKs of Drop shape (a rename pair's `-1` is not a
    /// drop). With every system PK held to net weight 0 or 1, those are exactly
    /// the PKs the delta leaves dead.
    ///
    /// Exhaustive over `SysFamily` (like `fire_hooks`): a newly-added family must
    /// decide here whether it carries guards beyond the contract, rather than
    /// falling into a silent `_` arm.
    pub(in crate::catalog) fn precheck_family(&self, family: SysFamily, batch: &Batch) -> Result<IdSet, String> {
        let sigs = check_batch_shape(family, batch)?;
        for sig in &sigs {
            self.check_cas_and_net(family, batch, sig)?;
        }
        let dropped = IdSet::new(sigs.iter().filter(|s| s.shape() == PkShape::Drop).map(|s| s.leading));
        match family {
            SysFamily::Schema => self.precheck_schema_family(batch, &dropped),
            SysFamily::Table | SysFamily::View => self.precheck_relation_family(family, batch, &sigs, &dropped),
            SysFamily::Column => self.precheck_column_family(batch, &sigs),
            SysFamily::Index => self.precheck_index_family(batch, &dropped),
            SysFamily::Sequence | SysFamily::Circuit => Ok(()),
        }?;
        Ok(dropped)
    }

    /// The cross-family rules of one `DDL_TXN` bundle: what no family's own arm
    /// can see, because each is prechecked against a catalog the bundle's other
    /// families have not reached yet. Run before the first family is applied, so
    /// a rejection has written nothing. Answers the ids the bundle creates.
    pub(in crate::catalog) fn precheck_bundle(&self, families: &DdlBundle) -> Result<FxHashSet<u64>, String> {
        let created = self.check_single_use(families)?;
        let created_views = IdSet::new(families.creates(SysFamily::View));
        if let Some(cols) = families.get(SysFamily::Column) {
            let created = IdSet::new(
                families
                    .creates(SysFamily::Table)
                    .chain(created_views.ids().iter().copied()),
            );
            self.check_column_owners(cols, &created, families.family_count() == 1)?;
        }
        if let Some(b) = families.get(SysFamily::Circuit) {
            check_circuit_rows(b, &created_views)?;
        }
        Ok(created)
    }

    /// A create takes an id this boot allocated, once, across every family that
    /// allocates ids from the one counter: an id the counter has not reached, one
    /// an earlier boot handed out, one a committed bundle of this process
    /// created, or one the bundle creates twice is refused. So an id never names
    /// two objects, and a create never reopens a dropped relation's store. A
    /// retraction needs no such rule: it names a live row, and none sits at or
    /// above the counter. Answers the bundle's created ids.
    fn check_single_use(&self, families: &DdlBundle) -> Result<FxHashSet<u64>, String> {
        let mut created = FxHashSet::default();
        let next_id = self.next_catalog_id();
        for family in SysFamily::ALL.into_iter().filter(|f| f.allocates_ids()) {
            for id in families.creates(family) {
                if id >= next_id {
                    return Err(format!(
                        "{} {id} was never allocated (next id {next_id})",
                        family.row_noun()
                    ));
                }
                // An id below the bootstrap floor is the shape check's refusal.
                let earlier_boot = (gnitz_wire::FIRST_USER_TABLE_ID..self.id_floor).contains(&id);
                if earlier_boot || self.used_ids.contains(&id) || !created.insert(id) {
                    return Err(format!(
                        "{} {id} was allocated before this boot or already used; ids are single-use",
                        family.row_noun()
                    ));
                }
            }
        }
        Ok(created)
    }

    /// Every column record must name an owner this bundle creates (`created`) or
    /// the registry holds: a phantom owner is never dropped, so nothing would ever
    /// retract the row. `alone` is whether COL_TAB is the bundle's only family.
    fn check_column_owners(&self, cols: &Batch, created: &IdSet, alone: bool) -> Result<(), String> {
        for i in cols.live_rows() {
            let owner_id = gnitz_wire::sys_leading_id(cols.get_pk_bytes(i));
            // An ALTER changes one column (a rewrite pair is one live row) and is
            // its bundle's only change: its descriptor swap runs last, and an ADD
            // COLUMN's cannot be undone, since the store refuses to narrow a schema.
            if self.registry.relation(owner_id).is_some() {
                if !alone || cols.live_rows().nth(1).is_some() {
                    return Err("a column ALTER must be the only change in its DDL transaction".into());
                }
            } else if !created.contains(owner_id) {
                return Err(format!(
                    "column record names owner {owner_id}, which this transaction \
                     does not create and the catalog does not hold"
                ));
            }
        }
        Ok(())
    }

    /// The names the live rows of `family` hold at payload slot `slot`. A caller
    /// inserts each name its batch's `+1` rows claim, so a name is claimed once:
    /// two `+1` rows under one name would both miss the live names and both
    /// apply, leaving the first id live and unreachable.
    fn live_names(&self, family: SysFamily, slot: usize) -> FxHashSet<Box<[u8]>> {
        let live = self.sys_relation(family).full_scan();
        (0..live.len()).map(|i| payload_bytes(&*live, i, slot).into()).collect()
    }

    /// SCHEMA_TAB: a CREATE must not collide with a live schema name — nor with one
    /// this batch already claims — and a DROP must find the schema empty. Schema ids
    /// share one id space with relation ids, so the member count, not the
    /// relation-keyed dep map, is the whole drop guard.
    fn precheck_schema_family(&self, batch: &Batch, dropped: &IdSet) -> Result<(), String> {
        let mut taken = self.live_names(SysFamily::Schema, SchemaTabSlot::name as usize);
        for i in batch.live_rows() {
            let name = SchemaTabRow::read(batch, i).map_err(|e| format!("Schema: {e}"))?.name;
            // Nothing synthesizes a schema name, so the reserved `_` prefix
            // applies with no carve-out.
            check_stored_name(&name, "schema", false)?;
            if !taken.insert(name.as_bytes().into()) {
                return Err(format!("Schema already exists: {name}"));
            }
        }
        for &sid in dropped.ids() {
            if let Some(names) = self.caches.relation_by_name.get(&sid) {
                return Err(format!(
                    "Schema not empty: {} relation(s) remain; drop them first",
                    names.len()
                ));
            }
        }
        Ok(())
    }

    /// The rules a created relation's row and column records must satisfy.
    /// `created_users` are the user views this bundle creates, the only owners a
    /// created segment may name.
    fn precheck_relation_create(&self, rel: &RelRow<'_>, created_users: &IdSet, dropped: &IdSet) -> Result<(), String> {
        // The system schema holds the catalog's own relations and nothing else.
        if rel.schema_id == SYSTEM_SCHEMA_ID {
            return Err(format!("{rel}: the system schema takes no relation"));
        }
        let col_defs = self.read_column_defs(rel.id)?;
        // A view segment legitimately carries two visible columns of one name
        // (a join chain's output).
        if rel.kind().is_ingestion_point() {
            unique_visible_names(&col_defs).map_err(|e| format!("{rel} {e}"))?;
        }
        let schema = build_schema_from_col_defs(&col_defs, rel.pk.as_slice()).map_err(|e| format!("{rel} {e}"))?;
        // An FK probes its parent's store on every write of the child, which only
        // a base table's ingest runs; a stream push must stay a pure append.
        if !rel.kind().is_base_table()
            && let Some(cd) = col_defs.iter().find(|cd| cd.fk.is_some())
        {
            return Err(format!(
                "{rel}: column '{}' may not carry a FOREIGN KEY; only a base table's may",
                cd.def.name
            ));
        }
        match rel.detail {
            RelDetail::Table(TableProps { serial, .. }) => {
                // `TableProps::validate` holds a SERIAL table to one PK column.
                if serial {
                    let pk = &col_defs[rel.pk.as_slice()[0] as usize].def;
                    if FixedInt::exact(pk.ty.tc).is_none() {
                        return Err(format!(
                            "{rel}: SERIAL primary key column '{}' is {}; SERIAL needs an integer of at most 8 bytes",
                            pk.name, pk.ty
                        ));
                    }
                }
                for cd in &col_defs {
                    if let Some(fk) = cd.fk {
                        self.validate_fk_column(cd, fk, rel.id, &col_defs, &schema, dropped)?;
                    }
                }
            }
            RelDetail::View(ViewParts { owner: Some(owner), .. }) if !created_users.contains(owner) => {
                return Err(format!(
                    "{rel} names owner {owner}, which is not a user view this bundle creates"
                ));
            }
            RelDetail::View(_) => {}
        }
        Ok(())
    }

    /// TABLE_TAB / VIEW_TAB: on every live row the name rule and qualified-name
    /// uniqueness; on a create also the rules of
    /// [`Self::precheck_relation_create`]; and the DROP guards (FK children, view
    /// dependents). A rename pair's `+1` differs from the live row only in `name`
    /// (the pair mask plus the CAS), so it is not re-derived. The drop guards key
    /// on `dropped`, the PKs of Drop shape, so a rename pair is never rejected as
    /// referenced or depended on.
    fn precheck_relation_family(
        &self,
        family: SysFamily,
        batch: &Batch,
        sigs: &[PkSignature],
        dropped: &IdSet,
    ) -> Result<(), String> {
        let mut claimed: FxHashSet<(u64, Cow<'_, str>)> = FxHashSet::default();
        // The retired ids a relation of the same name takes over: an ALTER VIEW.
        let mut replaced: Vec<u64> = Vec::new();
        let creates = IdSet::new(sigs.iter().filter(|s| s.shape() == PkShape::Create).map(|s| s.leading));
        let rels: Vec<RelRow<'_>> = batch
            .live_rows()
            .map(|i| read_rel_row(family, batch, i))
            .collect::<Result<_, _>>()?;
        // The only owners a created view may name: a chain is created whole.
        let created_users = IdSet::new(
            rels.iter()
                .filter(|rel| {
                    creates.contains(rel.id) && matches!(rel.detail, RelDetail::View(ViewParts { owner: None, .. }))
                })
                .map(|rel| rel.id),
        );

        for rel in &rels {
            // A leading `_` names a chain segment and nothing else.
            let segment = matches!(rel.detail, RelDetail::View(ViewParts { owner: Some(_), .. }));
            check_stored_name(&rel.name, rel.kind().noun(), segment)?;
            if creates.contains(rel.id) {
                self.precheck_relation_create(rel, &created_users, dropped)?;
            }
            if let Some(old) = self.claim_qname(rel.schema_id, rel.name.clone(), rel.id, dropped, &mut claimed)? {
                // The drop cascade would take the outgoing relation's indexes with it.
                let named = |c: &IndexClaim| matches!(c, IndexClaim::Index { .. });
                let outgoing = self.registry.relation(old);
                if let Some(e) = outgoing.filter(|e| e.indexes().iter().any(|ix| ix.claims().iter().any(named))) {
                    return Err(format!(
                        "{} '{}' (id={old}) owns an index, which replacing it would drop; DROP INDEX first",
                        e.kind().noun(),
                        rel.name
                    ));
                }
                replaced.push(old);
            }
        }

        if family == SysFamily::Table {
            for &tid in dropped.ids() {
                // A FK child being co-dropped in this same batch is
                // self-resolving — only a child *outside* the batch blocks the
                // drop. Mirrors the view-dependency filter below.
                let blocking = self.fks_as_parent(tid).iter().find(|r| !dropped.contains(r.child_tid));
                if let Some(r) = blocking {
                    let child = self.qualified_name(r.child_tid);
                    return Err(format!(
                        "cannot DROP '{}': it is referenced by a foreign key of '{child}' — drop that first",
                        self.qualified_name(tid)
                    ));
                }
            }
        }

        // A dependent that is itself being dropped in this same batch is
        // self-resolving — only an *outside* dependent blocks the drop.
        dropped.ids().iter().try_for_each(|&id| {
            let op = if replaced.contains(&id) { "replace" } else { "DROP" };
            self.reject_if_dependent_views(id, op, dropped)
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
            // A table hides the columns it dropped, a view the key slots it adds and the
            // operator state it stores beside its columns.
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
    fn precheck_index_family(&self, batch: &Batch, dropped: &IdSet) -> Result<(), String> {
        // A `+1` on a live index already failed the net bound, so any hit is a
        // different index.
        let mut taken = self.live_names(SysFamily::Index, IdxTabSlot::name as usize);
        let noun = SysFamily::Index.row_noun();
        for i in batch.live_rows() {
            let r = IdxTabRow::read(batch, i).map_err(|e| format!("Index: {e}"))?;
            let index_name = r.name;
            check_stored_name(&index_name, noun, false)?;
            self.validate_index_create(r.owner_id, r.source_col_idx.as_slice(), r.is_unique)?;
            if !taken.insert(index_name.as_bytes().into()) {
                return Err(format!("Index already exists: {index_name}"));
            }
        }

        for i in batch.retracted_rows() {
            // The row, not `dropped`: this needs `cols`, which a list of ids
            // does not carry.
            let IdxTabRow { owner_id, source_col_idx: cols, .. } =
                IdxTabRow::read(batch, i).map_err(|e| format!("Index: {e}"))?;
            // FK backing is single-column: a composite index is never an FK
            // target, so dropping one is never blocked by the FK-target guard.
            let &[src_col] = cols.as_slice() else {
                continue;
            };
            if !self
                .fks_as_parent(owner_id)
                .iter()
                .any(|r| r.parent_col == src_col as usize)
            {
                continue;
            }
            // The FK target column must stay a legal target for FK child inserts
            // to validate: the lone PK, or a column a unique index this batch does
            // not drop still covers. The claims are pre-drop: this batch has not
            // been applied yet.
            let remains = self
                .registry
                .relation(owner_id)
                .is_some_and(|o| holds_fk_target(o, src_col, dropped));
            if !remains {
                return Err(format!(
                    "cannot DROP the index on '{}': a foreign key references its \
                     column and no unique index would remain on the column",
                    self.qualified_name(owner_id),
                ));
            }
        }
        Ok(())
    }
}

/// No two visible columns of `defs` share a name: the rule of a relation clients
/// write to.
fn unique_visible_names(defs: &[CatalogColumn]) -> Result<(), String> {
    let visible = defs.iter().filter(|c| !c.def.is_hidden);
    match gnitz_wire::first_duplicate(visible.map(|c| &*c.def.name)) {
        Some(name) => Err(format!("has duplicate column name '{name}'")),
        None => Ok(()),
    }
}

#[cfg(test)]
#[path = "tests/precheck.rs"]
mod tests;
