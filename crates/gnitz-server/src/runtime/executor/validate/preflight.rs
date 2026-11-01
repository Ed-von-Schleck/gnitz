//! Distributed PK / FK / unique-index validation of a user write, before any
//! SAL byte is written.
//!
//! Every user write — a plain push and an atomic multi-family transaction
//! alike — is validated by `Checks::validate_txn`: a plain push is a
//! bundle of one family, so each rule has one implementation:
//!
//! - **U-PK**: an Error-mode insert's key is not held.
//! - **U-SEC**: a unique index's spans are unique.
//! - **F1**: a child's reference exists after the write.
//! - **F2**: a removed referenced value has no surviving referrer.
//!
//! Validating a write also completes it: the rows its
//! deletes reach through `ON DELETE CASCADE` edges join the bundle as families
//! of their own, under the same rules.

use std::collections::hash_map::Entry;
use std::collections::BTreeMap;
use std::num::NonZeroU64;

use rustc_hash::FxHashSet;

use super::*;

use crate::catalog::{FkEdge, RowConstraints};
use gnitz_expr::ColumnLocator;
use gnitz_store::relation::Relation;
use gnitz_wire::{FkAction, PkBuf, PkKeys, WireConflictMode};
use gnitz_zset::algebra::KeySpec;
use gnitz_zset::repr::MemBatch;
use gnitz_zset::schema::SchemaDescriptor;

/// One decoded, shape-validated transaction family: the target `tid`, its
/// conflict `mode`, and the decoded batch. The executor decodes into these, the
/// validator borrows them and extends them with the families a delete cascades
/// to, and the committer takes them by value.
pub(in crate::runtime::executor) struct TxnFamily {
    pub(in crate::runtime::executor) tid: u64,
    pub(in crate::runtime::executor) mode: WireConflictMode,
    pub(in crate::runtime::executor) batch: Batch,
}

/// One probe of relation `tid` at `keys`, queued for a burst, with the state
/// `plan` its replies fold into.
struct Check<P> {
    tid: u64,
    probe: Probe,
    /// The probed keys, as [`PkKeys::encode_probe`] lays them out.
    keys: Vec<u8>,
    /// The schema the replies decode against.
    reply: SchemaDescriptor,
    plan: P,
}

impl<P> Check<P> {
    fn new(rel: &Relation, probe: Probe, keys: PkKeys, plan: P) -> Result<Self, WireFault> {
        let reply = rel.probe_reply_schema(probe)?;
        Ok(Check {
            tid: rel.id(),
            probe,
            keys: keys.encode_probe(),
            reply,
            plan,
        })
    }
}

/// Per-PK last operation in a table's whole-bundle fold. `Inserted` names the
/// surviving row by `(family index into the bundle's family list, row index)`.
#[derive(Clone, Copy)]
enum FoldOp {
    Inserted(u32, u32),
    Deleted,
}

/// One PK's fold over its table's families, in frame order.
struct PkFold {
    /// The latest row op on the PK; once the bundle is folded, the op that survives.
    last: FoldOp,
    /// An Error family's insert was the first op on the PK.
    needs_probe: bool,
    /// A burst-1 probe found the PK committed.
    committed: bool,
}

/// One table's fold, keyed by the row's OPK bytes.
type Fold<'a> = FxHashMap<&'a [u8], PkFold>;

/// What a key collided with.
#[derive(Clone, Copy)]
enum Clash {
    /// Another row of the same write.
    InBatch,
    /// A committed row.
    Committed,
}

/// A PK rendered from its OPK bytes for an error message: the per-column native
/// values in PK-list order, comma-separated. A calendar or decimal column
/// renders as the integer it is stored as.
fn format_pk_bytes(schema: &SchemaDescriptor, pk_bytes: &[u8]) -> String {
    use gnitz_wire::TypeCode;
    let mut off = 0usize;
    schema
        .pk_columns()
        .map(|col| {
            let cell = &pk_bytes[off..off + col.size()];
            off += cell.len();
            let tc = col.type_code;
            match gnitz_wire::FixedInt::from_type_code(tc) {
                Some(fi) if fi.is_signed() => format!("{}", gnitz_wire::decode_opk_i64(cell, fi)),
                Some(fi) => format!("{}", gnitz_wire::decode_opk_i64(cell, fi) as u64),
                None => {
                    let native = gnitz_wire::decode_opk_u128(cell, tc);
                    match tc {
                        TypeCode::UUID => gnitz_wire::format_uuid(native),
                        TypeCode::I128 => format!("{}", native as i128),
                        _ => format!("{native}"),
                    }
                }
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// The PG-style PK-uniqueness rejection for the key `pk_bytes` of `schema`.
fn pk_violation_err(
    cat: &CatalogEngine,
    tid: u64,
    schema: &SchemaDescriptor,
    pk_bytes: &[u8],
    clash: Clash,
) -> WireFault {
    let key_str = format_pk_bytes(schema, pk_bytes);
    let (sn, tn) = cat.qualified_name_or_unknown(tid);
    let cols = cat.column_names(tid, schema.pk_cols());
    let what = match clash {
        Clash::InBatch => format!("Batch contains multiple rows with key ({cols})=({key_str})"),
        Clash::Committed => format!("Key ({cols})=({key_str}) already exists"),
    };
    integrity_violation(format!(
        "duplicate key value violates unique constraint \"{sn}_{tn}_pkey\": {what}"
    ))
}

/// The rejection of a write colliding on the unique index over `col_indices`.
fn unique_violation_err(cat: &CatalogEngine, tid: u64, col_indices: &[u32], clash: Clash) -> WireFault {
    let (name, cols) = (cat.qualified_name(tid), cat.column_names(tid, col_indices));
    let text = match clash {
        Clash::InBatch => format!("Unique index violation on '{name}' column '{cols}': duplicate in batch"),
        Clash::Committed => format!("Unique index violation on '{name}' column '{cols}'"),
    };
    integrity_violation(text)
}

/// How a bundled parent write removed a referenced value, which decides the
/// verb of the RESTRICT rejection.
#[derive(Clone, Copy)]
enum RetireVerb {
    /// The holding row is gone after the transaction.
    Delete,
    /// The holding row survives holding a different value.
    Update,
}

/// The referenced-value sets of one bundled FK parent column.
#[derive(Default)]
struct ParentDelta {
    /// Values the bundle removed, each under the verb of the write that retired it.
    retired: FxHashMap<u128, RetireVerb>,
    /// The surviving rows' non-NULL values.
    added: FxHashSet<u128>,
}

/// `(parent tid, referenced col)` → its delta. Resolved once per key and shared
/// by rules F1 and F2, which both turn on it.
type ParentDeltas = FxHashMap<(u64, usize), ParentDelta>;

/// The [`ParentDeltas`] key `e`'s referenced value lives under.
fn delta_key(e: &FkEdge) -> (u64, usize) {
    (e.parent_tid, e.parent_col)
}

/// One table of the bundle a rule reads: its catalog schema, its constraints,
/// and its fold.
struct TxnTable<'a> {
    tid: u64,
    schema: SchemaDescriptor,
    cons: RowConstraints,
    fold: Fold<'a>,
    /// The keys whose first op is an Error family's insert, in row order: a
    /// client's ascending keys reach the probe's sort ascending.
    probe_keys: Vec<&'a [u8]>,
}

/// A decoded transaction bundle plus everything its rules share. Built once;
/// every rule reads it instead of re-walking the families.
struct TxnBundle<'a> {
    /// The bundle's families, indexed by family number.
    families: &'a [TxnFamily],
    /// The tables with an Error family or a constraint, in first-appearance
    /// order, found by linear scan over the write's own table count.
    tables: Vec<TxnTable<'a>>,
}

impl<'a> TxnBundle<'a> {
    fn new(cat: &CatalogEngine, families: &'a [TxnFamily]) -> Result<Self, WireFault> {
        let mut tables: Vec<TxnTable<'a>> = Vec::new();
        // Each table judged once, at its first family.
        for (i, fam) in families.iter().enumerate() {
            if families[..i].iter().any(|f| f.tid == fam.tid) {
                continue;
            }
            let reads_committed = families[i..]
                .iter()
                .any(|f| f.tid == fam.tid && cat.push_reads_committed_state(f.tid, f.mode));
            if !reads_committed {
                continue;
            }
            tables.push(TxnTable {
                tid: fam.tid,
                schema: cat.registry.relation_or_err(fam.tid)?.schema(),
                cons: cat.row_constraints(fam.tid),
                fold: Fold::default(),
                probe_keys: Vec::new(),
            });
        }
        for t in &mut tables {
            let (tid, schema) = (t.tid, t.schema);
            for (fi, fam) in families.iter().enumerate().filter(|(_, f)| f.tid == tid) {
                let error = matches!(fam.mode, WireConflictMode::Error);
                let batch = &fam.batch;
                t.fold.reserve(batch.len());
                if error {
                    t.probe_keys.reserve(batch.len());
                }
                for row in 0..batch.len() {
                    let w = batch.get_weight(row);
                    if w == 0 {
                        continue;
                    }
                    let pk = batch.get_pk_bytes(row);
                    let op = if w > 0 {
                        FoldOp::Inserted(fi as u32, row as u32)
                    } else {
                        FoldOp::Deleted
                    };
                    let in_batch = || pk_violation_err(cat, tid, &schema, pk, Clash::InBatch);
                    let inserts = error && w > 0;
                    match t.fold.entry(pk) {
                        Entry::Occupied(o) => {
                            let e = o.into_mut();
                            if inserts {
                                match e.last {
                                    FoldOp::Inserted(f, _) if f == fi as u32 => return Err(in_batch()),
                                    // Existence is cumulative in frame order, and a
                                    // cascade's families come later and only delete.
                                    FoldOp::Inserted(..) => {
                                        return Err(pk_violation_err(cat, tid, &schema, pk, Clash::Committed))
                                    }
                                    FoldOp::Deleted => {}
                                }
                            }
                            e.last = op;
                        }
                        Entry::Vacant(v) => {
                            v.insert(PkFold {
                                last: op,
                                needs_probe: inserts,
                                committed: false,
                            });
                            if inserts {
                                t.probe_keys.push(pk);
                            }
                        }
                    }
                    // `+w` is the row pushed `w` times: its second copy lands on its first.
                    if inserts && w > 1 {
                        return Err(in_batch());
                    }
                }
            }
        }
        Ok(TxnBundle { families, tables })
    }

    fn get(&self, tid: u64) -> Option<&TxnTable<'a>> {
        self.tables.iter().find(|t| t.tid == tid)
    }

    fn get_mut(&mut self, tid: u64) -> Option<&mut TxnTable<'a>> {
        self.tables.iter_mut().find(|t| t.tid == tid)
    }

    fn table(&self, tid: u64) -> &TxnTable<'a> {
        self.get(tid).expect("a bundled table")
    }

    /// Family `fam`'s rows.
    fn mem(&self, fam: u32) -> &'a Batch {
        &self.families[fam as usize].batch
    }

    /// The rows of `tid` that survive the whole bundle: `(pk, family, row)`.
    fn surviving(&self, tid: u64) -> impl Iterator<Item = (&'a [u8], u32, u32)> + '_ {
        self.table(tid).fold.iter().filter_map(|(pk, e)| match e.last {
            FoldOp::Inserted(f, r) => Some((*pk, f, r)),
            FoldOp::Deleted => None,
        })
    }

    /// Whether the bundle retires `holder`'s claim on `span` in `tid`'s index
    /// `spec`: its surviving state is deleted, or holds a NULL or another span
    /// there. A holder the bundle does not touch, in a table it lists or not,
    /// keeps its claim.
    fn retires(&self, tid: u64, spec: &KeySpec, holder: &[u8], span: &[u8]) -> bool {
        match self.get(tid).and_then(|t| t.fold.get(holder)).map(|e| e.last) {
            None => false,
            Some(FoldOp::Deleted) => true,
            Some(FoldOp::Inserted(f, r)) => spec
                .span(self.mem(f), r as usize)
                .is_none_or(|own| own.pk_bytes() != span),
        }
    }
}

/// What a burst-1 check's reply rows are folded into.
enum Rule<'a> {
    /// U-PK: a reply refuses an Error family's first insert of its key, and
    /// marks any other key committed on the fold.
    Pk,
    /// [`Rule::Pk`] and the before-image of a referenced payload column at once:
    /// a reply row also records the value there, unless NULL.
    Gather { col: usize, old: FxHashMap<&'a [u8], u128> },
    /// U-SEC on `cols`: `spec` splits a reply entry into `[span ‖ holder PK]`.
    Unique { cols: PkColList, spec: KeySpec },
    /// F1 on `edge`: each probed reference, flagged once a reply finds it
    /// committed. A reply's leading `span` bytes are the key it answers.
    FkExists {
        edge: FkEdge,
        span: usize,
        values: FxHashMap<u128, bool>,
    },
}

/// Fire every check in `checks` as one SAL cut, handing each reply frame's rows
/// to `sink` with the check they answer.
async fn execute_probe_burst<P>(
    disp: &MasterDispatcher,
    checks: &mut [Check<P>],
    mut sink: impl FnMut(&mut Check<P>, &MemBatch<'_>) -> Result<(), WireFault>,
) -> Result<(), WireFault> {
    if checks.is_empty() {
        return Ok(());
    }
    let leases = disp
        .scan_cut(|cut| {
            let mut leases = Vec::with_capacity(checks.len());
            for check in checks.iter() {
                leases.push(cut.read(Read::HasPk {
                    tid: check.tid,
                    probe: check.probe,
                    keys: (&check.keys[..]).into(),
                })?);
            }
            Ok(leases)
        })
        .await?;

    for (lease, check) in leases.iter().zip(checks) {
        let reply = check.reply;
        while let Some(f) = lease.next().await? {
            sink(check, &f.rows(&reply))?;
        }
    }
    Ok(())
}

impl Checks {
    /// Validate a write bundle against the state its families leave together,
    /// appending the families its deletes cascade to; an Error family's PK
    /// existence alone is cumulative in frame order. The first violation, in
    /// the order U-PK, U-SEC, F1, F2, refuses the bundle; U-PK's is final at
    /// once, where the others wait out the cascade.
    pub(in crate::runtime::executor) async fn validate_txn(
        &self,
        disp: &MasterDispatcher,
        families: &mut Vec<TxnFamily>,
    ) -> Result<(), WireFault> {
        // No family whose write reads committed state ⇒ no table would be folded
        // and no rule would probe, so a plain push returns without building the
        // bundle.
        let reads_committed = {
            let cat = disp.cat();
            families.iter().any(|f| cat.push_reads_committed_state(f.tid, f.mode))
        };
        if !reads_committed {
            return Ok(());
        }
        let mut cascade = Cascade {
            budget: self.cascade_bytes,
            probed: FxHashSet::default(),
        };
        // A pass that finds further rows to delete gives no verdict: the rules
        // hold of the bundle those rows are part of.
        loop {
            let further = self.validate_bundle(disp, families, &mut cascade).await?;
            if further.is_empty() {
                return Ok(());
            }
            families.extend(further);
        }
    }

    /// The four rules over `families` — or, where their deletes cascade to rows
    /// outside them, those rows' families and no verdict.
    async fn validate_bundle(
        &self,
        disp: &MasterDispatcher,
        families: &[TxnFamily],
        cascade: &mut Cascade,
    ) -> Result<Vec<TxnFamily>, WireFault> {
        let mut b = TxnBundle::new(&disp.cat(), families)?;

        for t in &b.tables {
            if !t.cons.uniques.is_empty() && b.surviving(t.tid).next().is_some() {
                self.ensure_unique_filters_warm(disp, t.tid, &t.cons.uniques).await?;
            }
        }

        // ── Burst 1: every probe the bundle and the catalog alone decide ──
        // U-SEC's first violation is held until its turn in the verdict order.
        let mut unique_violation: Option<WireFault> = None;
        let mut checks: Vec<Check<Rule>> = Vec::new();
        // U-PK's checks go first: its sink refuses before any other rule's
        // reply is read.
        plan_pk_checks(disp, &b, &mut checks)?;
        plan_unique_checks(disp, self, &b, &mut checks, &mut unique_violation)?;
        plan_fk_existence(disp, self, &b, &mut checks)?;

        execute_probe_burst(disp, &mut checks, |check, rows| {
            let Check { tid, probe, reply, plan, .. } = check;
            let mut answered = (0..rows.len())
                .filter(|&j| rows.get_weight(j) == 1)
                .map(|j| (j, rows.get_pk_bytes(j)));
            // An Error family's first insert of a committed key, refused; any
            // other committed key, marked.
            let found = |t: &mut TxnTable<'_>, pk: &[u8]| -> Result<bool, WireFault> {
                let Some(e) = t.fold.get_mut(pk) else {
                    return Ok(false);
                };
                if e.needs_probe {
                    return Err(pk_violation_err(&disp.cat(), t.tid, &t.schema, pk, Clash::Committed));
                }
                e.committed = true;
                Ok(true)
            };
            match plan {
                Rule::Pk => {
                    let t = b.get_mut(*tid).expect("a bundled table");
                    for (_, pk) in answered {
                        found(t, pk)?;
                    }
                }
                Rule::Gather { old, .. } => {
                    let loc = reply.locate(reply.payload_col_idx(0));
                    let t = b.get_mut(*tid).expect("a bundled table");
                    for (j, pk) in answered {
                        if found(t, pk)? && !loc.is_null(rows, j) {
                            // Keyed by the fold's own key, which outlives the reply.
                            let (&key, _) = t.fold.get_key_value(pk).expect("found above");
                            old.insert(key, loc.opk_image(rows, j));
                        }
                    }
                }
                // The in-bundle duplicate check left each span one claimant, so
                // a holder that is a fold key is that claimant or gave the span up.
                Rule::Unique { cols, spec } => {
                    let fold = &b.table(*tid).fold;
                    if unique_violation.is_none() && answered.any(|(_, e)| !fold.contains_key(spec.split_entry(e).1)) {
                        let err = unique_violation_err(&disp.cat(), *tid, cols.as_slice(), Clash::Committed);
                        unique_violation = Some(err);
                    }
                }
                Rule::FkExists { span, values, .. } => {
                    for (_, e) in answered {
                        let v = gnitz_wire::widen_pk_be(&e[..*span]);
                        if let Some(found) = values.get_mut(&v) {
                            *found = true;
                            if matches!(probe, Probe::Pk) {
                                self.fk_presence_found(*tid, v);
                            }
                        }
                    }
                }
            }
            Ok(())
        })
        .await?;

        // ── Burst 2: every FK whose parent is bundled, keyed by the deltas ──
        let children: Vec<FkEdge> = b
            .tables
            .iter()
            .flat_map(|t| t.cons.fks_as_parent.iter().copied())
            .collect();
        let deltas = resolve_parent_deltas(&b, &checks, &children);
        let (further, restricted) = removed_references(disp, &b, &children, &deltas, cascade).await?;
        if !further.is_empty() {
            return Ok(further);
        }

        if let Some(e) = unique_violation {
            return Err(e);
        }
        fk_existence_verdict(disp, &checks, &deltas)?;
        restricted.map_or(Ok(Vec::new()), Err)
    }
}

/// Rule U-PK, planning half, and the before-images of referenced payload
/// columns, whose replies answer U-PK as well.
fn plan_pk_checks<'a>(
    disp: &MasterDispatcher,
    b: &TxnBundle<'a>,
    checks: &mut Vec<Check<Rule<'a>>>,
) -> Result<(), WireFault> {
    for t in &b.tables {
        let cat = disp.cat();
        let rel = cat.registry.relation_or_err(t.tid)?;
        let stride = t.schema.pk_stride();
        let parent_cols = || t.cons.fks_as_parent.iter().map(|e| e.parent_col);
        let mut payload_refs: Vec<usize> = parent_cols().filter(|&c| !t.schema.is_pk_col(c)).collect();
        payload_refs.sort_unstable();
        payload_refs.dedup();
        if !payload_refs.is_empty() {
            let keys = PkKeys::from_keys(stride, t.fold.keys().copied());
            if keys.is_empty() {
                continue;
            }
            for col in payload_refs {
                let plan = Rule::Gather { col, old: FxHashMap::default() };
                checks.push(Check::new(rel, Probe::PkColumn(col as u32), keys.clone(), plan)?);
            }
            continue;
        }
        // A deleted key's PK-column values are retired only where it was committed.
        let part_pk_ref = parent_cols().any(|c| t.schema.lone_pk_col() != Some(c));
        let deleted = t
            .fold
            .iter()
            .filter(|(_, e)| part_pk_ref && matches!(e.last, FoldOp::Deleted))
            .map(|(pk, _)| *pk);
        let keys = PkKeys::from_keys(stride, t.probe_keys.iter().copied().chain(deleted));
        if keys.is_empty() {
            continue;
        }
        checks.push(Check::new(rel, Probe::Pk, keys, Rule::Pk)?);
    }
    Ok(())
}

/// Rule U-SEC, planning half: every (table, unique index)'s surviving spans,
/// with one committed-holder probe appended to `checks` for those its filter
/// cannot prove absent. Two surviving rows claiming one span fill an empty
/// `violation`, and their index plans no probe.
fn plan_unique_checks<'a>(
    disp: &MasterDispatcher,
    caches: &Checks,
    b: &TxnBundle<'a>,
    checks: &mut Vec<Check<Rule<'a>>>,
    violation: &mut Option<WireFault>,
) -> Result<(), WireFault> {
    for t in &b.tables {
        let tid = t.tid;
        for &(col_indices, ref spec) in &t.cons.uniques {
            let key_size = spec.key_size();

            let mut spans: Vec<u8> = Vec::with_capacity(t.fold.len() * key_size);
            for (_pk, fam, row) in b.surviving(tid) {
                // NULL in an indexed column ⇒ unindexed.
                if let Some(span) = spec.span(b.mem(fam), row as usize) {
                    spans.extend_from_slice(span.pk_bytes());
                }
            }
            let keys = PkKeys::from_keys(key_size, spans.chunks_exact(key_size));
            // `surviving` yields each PK once, so a span the dedup collapsed is
            // two rows of the bundle claiming it.
            if keys.len() * key_size != spans.len() {
                violation.get_or_insert_with(|| {
                    unique_violation_err(&disp.cat(), tid, col_indices.as_slice(), Clash::InBatch)
                });
                continue;
            }

            // A span the filter proves absent has no committed holder to answer for.
            let keys = caches.unique_filter_retain_possible(tid, col_indices, keys);
            if keys.is_empty() {
                continue;
            }
            let cat = disp.cat();
            let rel = cat.registry.relation_or_err(tid)?;
            let plan = Rule::Unique { cols: col_indices, spec: spec.clone() };
            checks.push(Check::new(rel, Probe::Index(col_indices, NonZeroU64::MAX), keys, plan)?);
        }
    }
    Ok(())
}

/// Rule F1, planning half: one committed-occupancy probe per FK constraint
/// whose child is bundled, over the distinct non-NULL FK values of that child's
/// surviving rows — less, where the reference is the parent's whole key, those
/// the bundle writes into the parent or its presence cache proves committed.
fn plan_fk_existence<'a>(
    disp: &MasterDispatcher,
    caches: &Checks,
    b: &TxnBundle<'a>,
    checks: &mut Vec<Check<Rule<'a>>>,
) -> Result<(), WireFault> {
    for &edge in b.tables.iter().flat_map(|t| &t.cons.fks_as_child) {
        let FkEdge {
            child_tid: tid,
            parent_tid,
            parent_col,
            fk_col,
            on_delete: _,
        } = edge;
        let loc = b.table(tid).schema.locate(fk_col);

        let mut values: FxHashMap<u128, bool> = FxHashMap::default();
        for (_pk, fam, row) in b.surviving(tid) {
            let m = b.mem(fam);
            let r = row as usize;
            if !loc.is_null(m, r) {
                values.insert(loc.opk_image(m, r), false);
            }
        }
        if values.is_empty() {
            continue;
        }

        // The parent's own PK store where the referenced column is its lone PK
        // column, its UNIQUE index on the column otherwise.
        let cat = disp.cat();
        let parent = cat.registry.relation_or_err(parent_tid)?;
        let parent_schema = parent.schema();
        // An FK column has its parent column's type, so its image is the key's.
        let (span, probe) = if parent_schema.lone_pk_col() == Some(parent_col) {
            (parent_schema.pk_stride(), Probe::Pk)
        } else {
            let index = parent
                .index_on(&[parent_col as u32])
                .ok_or_else(|| format!("FK check: no unique index on parent {parent_tid} col {parent_col}"))?;
            let cols = PkColList::from_slice(&[parent_col as u32]);
            (index.key_spec().key_size(), Probe::Index(cols, NonZeroU64::MAX))
        };
        if matches!(probe, Probe::Pk) {
            // The reference is the parent's whole key, so a key the bundle writes
            // is decided by the bundle and any other by whether it is committed.
            let fold = b.get(parent_tid).map(|t| &t.fold);
            let cache = caches.fk_presence_of(parent_tid);
            let mut key = [0u8; 16];
            values.retain(|&v, _| {
                gnitz_wire::store_opk(&mut key[..span], v, false);
                match fold.and_then(|f| f.get(&key[..span])).map(|e| e.last) {
                    // The parent's delta adds it.
                    Some(FoldOp::Inserted(..)) => false,
                    // The parent's delta retires it: kept for the verdict to refuse.
                    Some(FoldOp::Deleted) => true,
                    None => !cache.as_ref().is_some_and(|c| c.holds(v)),
                }
            });
            if values.is_empty() {
                continue;
            }
        }
        let keys = PkKeys::from_images(span, values.keys().copied());
        checks.push(Check::new(parent, probe, keys, Rule::FkExists { edge, span, values })?);
    }
    Ok(())
}

/// Rule F1, verdict half: every surviving row's FK value must reference a row
/// that exists after the transaction.
fn fk_existence_verdict(
    disp: &MasterDispatcher,
    checks: &[Check<Rule<'_>>],
    deltas: &ParentDeltas,
) -> Result<(), WireFault> {
    // A non-bundled parent has no delta (the degenerate plain-push case).
    let no_delta = ParentDelta::default();
    for check in checks {
        let Rule::FkExists { edge, values, .. } = &check.plan else {
            continue;
        };
        let ParentDelta { retired, added } = deltas.get(&delta_key(edge)).unwrap_or(&no_delta);
        for (v, &in_committed) in values {
            if (in_committed && !retired.contains_key(v)) || added.contains(v) {
                continue;
            }
            let cat = disp.cat();
            return Err(integrity_violation(format!(
                "Foreign Key violation in '{}': value not found in target '{}'",
                cat.qualified_name(edge.child_tid),
                cat.qualified_name(edge.parent_tid),
            )));
        }
    }
    Ok(())
}

/// The [`ParentDelta`] of each referenced column rules F1 and F2 turn on,
/// one per `(parent tid, referenced col)` of `children`.
fn resolve_parent_deltas(b: &TxnBundle<'_>, checks: &[Check<Rule<'_>>], children: &[FkEdge]) -> ParentDeltas {
    let mut needed: Vec<(u64, usize)> = children.iter().map(delta_key).collect();
    needed.sort_unstable();
    needed.dedup();

    let mut deltas = ParentDeltas::default();
    for (ptid, pcol) in needed {
        let t = b.table(ptid);
        let delta = match t.schema.locate(pcol) {
            ColumnLocator::Pk { byte_off, size, .. } => {
                let (off, size) = (byte_off as usize, size as usize);
                let lone = t.schema.lone_pk_col() == Some(pcol);
                // A surviving row holds its own key; a deleted one's value is the
                // whole key, which no other row holds, where it is the lone column.
                parent_retired_added(b, ptid, pcol, |pk, e| {
                    (matches!(e.last, FoldOp::Deleted) && (lone || e.committed))
                        .then(|| gnitz_wire::widen_pk_be(&pk[off..off + size]))
                })
            }
            _ => {
                let gathered = checks.iter().find_map(|c| match &c.plan {
                    Rule::Gather { col, old, .. } if c.tid == ptid && *col == pcol => Some(old),
                    _ => None,
                });
                parent_retired_added(b, ptid, pcol, |pk, _| gathered.and_then(|g| g.get(pk).copied()))
            }
        };
        deltas.insert((ptid, pcol), delta);
    }
    deltas
}

/// One parent column's [`ParentDelta`]: `added` are the surviving rows'
/// non-NULL values; `retired` are the before-image values `old_of` yields that
/// the PK's surviving state no longer holds.
fn parent_retired_added(
    b: &TxnBundle<'_>,
    parent_tid: u64,
    ref_col: usize,
    old_of: impl Fn(&[u8], &PkFold) -> Option<u128>,
) -> ParentDelta {
    let t = b.table(parent_tid);
    let loc = t.schema.locate(ref_col);
    let mut added: FxHashSet<u128> = FxHashSet::default();
    let mut retired: FxHashMap<u128, RetireVerb> = FxHashMap::default();
    for (p, e) in &t.fold {
        let surviving_val: Option<u128> = match e.last {
            FoldOp::Inserted(f, r) => {
                let (m, r) = (b.mem(f), r as usize);
                (!loc.is_null(m, r)).then(|| loc.opk_image(m, r))
            }
            FoldOp::Deleted => None,
        };
        if let Some(sv) = surviving_val {
            added.insert(sv);
        }
        if let Some(ov) = old_of(p, e) {
            if surviving_val != Some(ov) {
                // A value both deleted and updated away reads as deleted, so
                // the verb does not depend on the fold's iteration order.
                let slot = retired.entry(ov).or_insert(RetireVerb::Update);
                if matches!(e.last, FoldOp::Deleted) {
                    *slot = RetireVerb::Delete;
                }
            }
        }
    }
    ParentDelta { retired, added }
}

/// What one write's cascade may still delete, and what it has asked, across
/// the passes of its validation.
struct Cascade {
    /// Row bytes left, at each deleted row's fixed width.
    budget: usize,
    /// `(child tid, FK col, referenced value)`: the references already read for
    /// deletion. A later pass folds the rows they named, and finds none new.
    probed: FxHashSet<(u64, usize, u128)>,
}

/// The rows of one table a pass's cascade deletes.
struct Cascaded {
    schema: SchemaDescriptor,
    /// Their PKs, back to back, in the order they were found.
    pks: Vec<u8>,
    seen: FxHashSet<PkBuf>,
}

/// One planned probe of a child's FK index for the committed rows referencing
/// values that leave the parent.
struct ReferencePlan {
    edge: FkEdge,
    /// Splits a reply entry into `[span ‖ holder PK]`, and re-encodes a
    /// surviving holder's own span for the exemption test.
    spec: KeySpec,
    /// The values' rows are deleted and `edge` cascades, so a referencing row is
    /// deleted with them; otherwise it is an F2 violation.
    cascades: bool,
}

/// Rule F2 and the cascade, which read the same rows: the committed rows that
/// reference a value the bundle removes and does not re-add — `retired ∖ added`
/// — and whose reference the bundle does not itself retire. Through an `ON
/// DELETE CASCADE` edge from a deleted row, such a row is deleted too, and so
/// are the rows referencing it in turn; anywhere else it is a violation.
///
/// Answers one `-1` family per table the cascade deletes from, and F2's first
/// violation, which stands only where there is no such family: a later pass may
/// find the violating row deleted.
async fn removed_references(
    disp: &MasterDispatcher,
    b: &TxnBundle<'_>,
    children: &[FkEdge],
    deltas: &ParentDeltas,
    cascade: &mut Cascade,
) -> Result<(Vec<TxnFamily>, Option<WireFault>), WireFault> {
    let over_budget = |edge: &FkEdge| -> WireFault {
        let cat = disp.cat();
        format!(
            "a delete from '{}' cascades to more rows of '{}' than one write deletes; delete them first",
            cat.qualified_name(edge.parent_tid),
            cat.qualified_name(edge.child_tid),
        )
        .into()
    };
    // `(edge, cascades, values)`: one probe each.
    let mut seeds: Vec<(FkEdge, bool, Vec<u128>)> = Vec::new();
    for &edge in children {
        let ParentDelta { retired, added } = &deltas[&delta_key(&edge)];
        let (mut deleted, mut restricted) = (Vec::new(), Vec::new());
        for (&v, verb) in retired.iter().filter(|(v, _)| !added.contains(v)) {
            let cascades = edge.on_delete == FkAction::Cascade && matches!(verb, RetireVerb::Delete);
            if cascades { &mut deleted } else { &mut restricted }.push(v);
        }
        seeds.push((edge, true, deleted));
        seeds.push((edge, false, restricted));
    }
    let mut cascaded: BTreeMap<u64, Cascaded> = BTreeMap::new();
    let mut violation: Option<WireFault> = None;
    loop {
        for (edge, cascades, values) in &mut seeds {
            if *cascades {
                // A value a surviving row of the bundle holds has not left.
                if let Some(d) = deltas.get(&delta_key(edge)) {
                    values.retain(|v| !d.added.contains(v));
                }
                values.retain(|&v| cascade.probed.insert((edge.child_tid, edge.fk_col, v)));
            }
        }
        seeds.retain(|(.., values)| !values.is_empty());
        if seeds.is_empty() {
            break;
        }

        let mut checks: Vec<Check<ReferencePlan>> = Vec::new();
        for (edge, cascades, values) in seeds.drain(..) {
            let FkEdge { child_tid, fk_col, .. } = edge;
            let cat = disp.cat();
            let child = cat.registry.relation_or_err(child_tid)?;
            let cols = edge.child_index_cols();
            let index = child
                .index_on(cols.as_slice())
                .ok_or_else(|| format!("FK check: no index on child {child_tid} col {fk_col}"))?;
            let schema = child.schema();
            // Only a key of the child's fold is exempt, so one entry past its
            // key count is a violation. A cascade's reply holds as well the rows
            // already found and the new ones the budget has room for: cut short
            // at that, it holds one more new row than the budget pays for.
            let mut admitted = b.get(child_tid).map_or(0, |t| t.fold.len());
            if cascades {
                let c = cascaded.entry(child_tid).or_insert_with(|| Cascaded {
                    schema,
                    pks: Vec::new(),
                    seen: FxHashSet::default(),
                });
                admitted += c.seen.len() + cascade.budget / schema.row_width();
            }
            let cap = NonZeroU64::MIN.saturating_add(admitted as u64);
            let spec = index.key_spec();
            // The parent column has the FK column's type, so its image is the span's.
            let keys = PkKeys::from_images(spec.key_size(), values);
            let plan = ReferencePlan { edge, spec: spec.clone(), cascades };
            checks.push(Check::new(child, Probe::Index(cols, cap), keys, plan)?);
        }
        let found: Vec<usize> = cascaded.values().map(|c| c.pks.len()).collect();
        execute_probe_burst(disp, &mut checks, |check, rows| {
            let ReferencePlan { edge, spec, cascades } = &check.plan;
            for j in 0..rows.len() {
                if rows.get_weight(j) != 1 {
                    continue;
                }
                let (span, holder) = spec.split_entry(rows.get_pk_bytes(j));
                if b.retires(edge.child_tid, spec, holder, span) {
                    continue;
                }
                if *cascades {
                    let c = cascaded.get_mut(&edge.child_tid).expect("entered with its check");
                    if c.seen.insert(PkBuf::from_bytes(holder)) {
                        let left = cascade.budget.checked_sub(c.schema.row_width());
                        cascade.budget = left.ok_or_else(|| over_budget(edge))?;
                        c.pks.extend_from_slice(holder);
                    }
                } else if violation.is_none() {
                    // Anything but a surviving row holding a new value reads as a delete.
                    let verb = match deltas[&delta_key(edge)].retired.get(&gnitz_wire::widen_pk_be(span)) {
                        Some(RetireVerb::Update) => "update",
                        _ => "delete from",
                    };
                    let cat = disp.cat();
                    violation = Some(integrity_violation(format!(
                        "Foreign Key violation: cannot {verb} '{}', row still referenced by '{}'",
                        cat.qualified_name(edge.parent_tid),
                        cat.qualified_name(edge.child_tid),
                    )));
                }
            }
            Ok(())
        })
        .await?;

        // What the rows just found are referenced by. A found row is committed,
        // so its key holds a PK column's value; a payload column's is read by
        // the next pass, which folds the row.
        for ((&tid, c), from) in cascaded.iter().zip(found) {
            let stride = c.schema.pk_stride();
            let new = &c.pks[from..];
            if new.is_empty() {
                continue;
            }
            let cat = disp.cat();
            let cascading = cat
                .fks_as_parent(tid)
                .iter()
                .filter(|e| e.on_delete == FkAction::Cascade);
            for &edge in cascading {
                if let ColumnLocator::Pk { byte_off, size, .. } = c.schema.locate(edge.parent_col) {
                    let at = byte_off as usize..byte_off as usize + size as usize;
                    let values = new
                        .chunks_exact(stride)
                        .map(|pk| gnitz_wire::widen_pk_be(&pk[at.clone()]));
                    seeds.push((edge, true, values.collect()));
                }
            }
        }
    }

    let further = cascaded
        .into_iter()
        .filter(|(_, c)| !c.pks.is_empty())
        .map(|(tid, c)| TxnFamily {
            tid,
            mode: WireConflictMode::Update,
            batch: Batch::key_retractions(&c.schema, &c.pks),
        })
        .collect();
    Ok((further, violation))
}

#[cfg(test)]
#[path = "tests/preflight.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/preflight.rs"]
mod bench;
