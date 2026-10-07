//! Distributed PK / FK / unique-index validation of a user write, before any
//! SAL byte is written.
//!
//! Every user write — a plain push and an atomic multi-family transaction
//! alike — is validated by `validate_txn_distributed`: a plain push is a
//! bundle of one family, so the four rules (U-PK, U-SEC, F1, F2) have one
//! implementation each. Validating a write also completes it: the rows its
//! deletes reach through `ON DELETE CASCADE` edges join the bundle as families
//! of their own, under the same rules. The DDL-time pre-flight for CREATE UNIQUE INDEX is
//! `unique_preflight.rs`, which shares nothing with this file but the
//! `UniqueFilter` it seeds.

use std::collections::hash_map::Entry;
use std::collections::BTreeMap;
use std::num::NonZeroU64;

use rustc_hash::FxHashSet;

use super::*;

use super::scatter::with_routed;
use super::train::drain_rows;
use crate::catalog::{FkEdge, RowConstraints};
use crate::runtime::orchestration::TxnFamily;
use gnitz_expr::{ColumnLocator, ColumnTable, SchemaFacts};
use gnitz_store::relation::Relation;
use gnitz_wire::sys_rows::FkAction;
use gnitz_wire::{PkBuf, PkColList, PkKeys, Probe, WireConflictMode, WireStatus, MAX_PK_BYTES};
use gnitz_zset::repr::MemBatch;
use gnitz_zset::schema::{encode_schema_block, KeySpec};

/// One probe of relation `tid` at `keys`, queued for a burst, with the state
/// `plan` its replies fold into.
struct Check<P> {
    tid: u64,
    probe: Probe,
    /// Distinct keys, ascending for every probe but `Probe::Pk`.
    keys: Batch,
    /// The schema the replies decode against.
    reply: SchemaDescriptor,
    plan: P,
}

impl<P> Check<P> {
    /// A probe whose replies are in its keys' layout.
    fn new(tid: u64, probe: Probe, keys: Batch, plan: P) -> Self {
        debug_assert!(
            matches!(probe, Probe::Pk) || gnitz_wire::strictly_ascending(keys.pk_data(), keys.schema().pk_stride()),
            "{probe:?}: keys ascend"
        );
        let reply = *keys.schema();
        Check { tid, probe, keys, reply, plan }
    }
}

/// Where an own-PK probe of `rel` goes: to the owners of its keys — or, every
/// worker holding a replicated relation whole, spread over the full key.
fn probe_placement(rel: &Relation) -> Placement {
    match rel.placement() {
        Placement::Replicated => Placement::full_pk(&rel.schema()),
        p => p,
    }
}

/// A PK-sorted check batch whose row `j` is `keys[j]`, an image of `schema`'s one key
/// column. Sorts `keys` first: an image orders as its OPK bytes do.
fn build_check_batch(schema: &SchemaDescriptor, keys: &mut [u128]) -> Batch {
    keys.sort_unstable();
    debug_assert_eq!(schema.pk_cols().len(), 1, "a check batch keys on one column");
    let w = schema.pk_stride();
    let mut key = [0u8; 16];
    let mut batch = Batch::with_capacity(schema, keys.len());
    for &k in keys.iter() {
        gnitz_wire::store_opk(&mut key[..w], k, false);
        batch.push_key_row(&key[..w], 1);
    }
    batch
}

/// Build a check batch from `keys`, OPK byte spans already in the target
/// schema's key layout. Each lands verbatim in the PK region, where
/// [`build_check_batch`] encodes an image.
fn build_check_batch_pk_bytes<'k>(schema: &SchemaDescriptor, keys: impl ExactSizeIterator<Item = &'k [u8]>) -> Batch {
    let mut batch = Batch::with_capacity(schema, keys.len());
    for k in keys {
        batch.push_key_row(k, 1);
    }
    batch
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
    /// An Error family inserted the PK over another family's insert.
    exists_in_bundle: bool,
    /// An Error family's insert was the first op on the PK.
    needs_probe: bool,
    /// A burst-1 probe found the PK committed.
    committed: bool,
}

impl PkFold {
    /// Whether the bundle retires the value this key holds in a PK column, the
    /// `lone` one or one of several; `None` where that is whether the key is
    /// committed.
    fn retires_pk_value(&self, lone: bool) -> Option<bool> {
        match self.last {
            // The surviving row holds its own key.
            FoldOp::Inserted(..) => Some(false),
            // The value is the whole key, which no other row holds.
            FoldOp::Deleted if lone => Some(true),
            FoldOp::Deleted => None,
        }
    }
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

/// The PG-style PK-uniqueness rejection for the key `pk_bytes` of `schema`.
fn pk_violation_err(
    cat: &CatalogEngine,
    tid: u64,
    schema: &SchemaDescriptor,
    pk_bytes: &[u8],
    clash: Clash,
) -> WireFault {
    let key_str = schema.format_pk_bytes(pk_bytes);
    let (sn, tn) = cat.qualified_name_or_unknown(tid);
    let cols = cat.column_names(tid, schema.pk_cols());
    let what = match clash {
        Clash::InBatch => format!("Batch contains multiple rows with key ({cols})=({key_str})"),
        Clash::Committed => format!("Key ({cols})=({key_str}) already exists"),
    };
    WireFault {
        status: WireStatus::IntegrityViolation,
        text: format!("duplicate key value violates unique constraint \"{sn}_{tn}_pkey\": {what}"),
    }
}

/// The rejection of a write colliding on the unique index over `col_indices`.
fn unique_violation_err(cat: &CatalogEngine, tid: u64, col_indices: &[u32], clash: Clash) -> WireFault {
    let (name, cols) = (cat.qualified_name(tid), cat.column_names(tid, col_indices));
    let text = match clash {
        Clash::InBatch => format!("Unique index violation on '{name}' column '{cols}': duplicate in batch"),
        Clash::Committed => format!("Unique index violation on '{name}' column '{cols}'"),
    };
    WireFault {
        status: WireStatus::IntegrityViolation,
        text,
    }
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

/// The `(retired, added)` referenced-value sets of one bundled FK parent
/// column, each retired value under the verb of the write that retired it.
type ParentDelta = (FxHashMap<u128, RetireVerb>, FxHashSet<u128>);

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
}

/// A decoded transaction bundle plus everything its rules share. Built once;
/// every rule reads it instead of re-walking the families.
struct TxnBundle<'a> {
    /// One borrowed columnar view per family, so a rule's row walk takes
    /// `&MemBatch` by family index instead of threading the owning `Batch`.
    mems: Vec<MemBatch<'a>>,
    /// The tables with an Error family or a constraint, in first-appearance
    /// order, found by linear scan over the write's own table count.
    tables: Vec<TxnTable<'a>>,
}

impl<'a> TxnBundle<'a> {
    fn new(disp: &MasterDispatcher, families: &'a [TxnFamily]) -> Result<Self, WireFault> {
        let mut tables: Vec<TxnTable<'a>> = Vec::new();
        for fam in families {
            if tables.iter().any(|t| t.tid == fam.tid) {
                continue;
            }
            let error = families
                .iter()
                .any(|f| f.tid == fam.tid && matches!(f.mode, WireConflictMode::Error));
            let cons = disp.cat().row_constraints(fam.tid);
            if !error && cons.is_empty() {
                continue;
            }
            tables.push(TxnTable {
                tid: fam.tid,
                schema: disp.cat().registry.relation_or_err(fam.tid)?.schema(),
                cons,
                fold: Fold::default(),
            });
        }
        for t in &mut tables {
            let (tid, schema) = (t.tid, t.schema);
            for (fi, fam) in families.iter().enumerate().filter(|(_, f)| f.tid == tid) {
                let error = matches!(fam.mode, WireConflictMode::Error);
                let batch = &fam.batch;
                t.fold.reserve(batch.len());
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
                    let in_batch = || pk_violation_err(disp.cat(), tid, &schema, pk, Clash::InBatch);
                    let inserts = error && w > 0;
                    match t.fold.entry(pk) {
                        Entry::Occupied(o) => {
                            let e = o.into_mut();
                            if inserts {
                                match e.last {
                                    FoldOp::Inserted(f, _) if f == fi as u32 => return Err(in_batch()),
                                    FoldOp::Inserted(..) => e.exists_in_bundle = true,
                                    FoldOp::Deleted => {}
                                }
                            }
                            e.last = op;
                        }
                        Entry::Vacant(v) => {
                            v.insert(PkFold {
                                last: op,
                                exists_in_bundle: false,
                                needs_probe: inserts,
                                committed: false,
                            });
                        }
                    }
                    // `+w` is the row pushed `w` times: its second copy lands on its first.
                    if inserts && w > 1 {
                        return Err(in_batch());
                    }
                }
            }
        }
        Ok(TxnBundle {
            mems: families.iter().map(|f| f.batch.as_mem_batch()).collect(),
            tables,
        })
    }

    fn table(&self, tid: u64) -> &TxnTable<'a> {
        self.tables.iter().find(|t| t.tid == tid).expect("a bundled table")
    }

    fn schema(&self, tid: u64) -> &SchemaDescriptor {
        &self.table(tid).schema
    }

    fn fold(&self, tid: u64) -> &Fold<'a> {
        &self.table(tid).fold
    }

    fn fold_mut(&mut self, tid: u64) -> &mut Fold<'a> {
        &mut self
            .tables
            .iter_mut()
            .find(|t| t.tid == tid)
            .expect("a bundled table")
            .fold
    }

    /// Family `fam`'s columnar view, built once in `new`.
    fn mem(&self, fam: u32) -> &MemBatch<'a> {
        &self.mems[fam as usize]
    }

    /// The rows of `tid` that survive the whole bundle: `(pk, family, row)`.
    fn surviving(&self, tid: u64) -> impl Iterator<Item = (&'a [u8], u32, u32)> + '_ {
        self.fold(tid).iter().filter_map(|(pk, e)| match e.last {
            FoldOp::Inserted(f, r) => Some((*pk, f, r)),
            FoldOp::Deleted => None,
        })
    }

    /// Whether the bundle retires `holder`'s claim on `span` in `tid`'s index
    /// `spec`: its surviving state is deleted, or holds a NULL or another span
    /// there. A holder the bundle does not touch, in a table it lists or not,
    /// keeps its claim.
    fn retires(&self, tid: u64, spec: &KeySpec, holder: &[u8], span: &[u8]) -> bool {
        let fold = self.tables.iter().find(|t| t.tid == tid).map(|t| &t.fold);
        match fold.and_then(|fold| fold.get(holder)).map(|e| e.last) {
            None => false,
            Some(FoldOp::Deleted) => true,
            Some(FoldOp::Inserted(f, r)) => {
                let mut own = [0u8; MAX_PK_BYTES];
                !spec.write_span(self.mem(f), r as usize, &mut own) || own[..spec.key_size()] != *span
            }
        }
    }
}

/// What a burst-1 check's reply rows are folded into.
enum Rule<'a> {
    /// U-PK: a reply marks its key committed on the fold.
    Pk,
    /// U-PK and the before-image of a referenced payload column at once: a reply
    /// row marks its key committed and records the value there, unless NULL.
    Gather {
        col: usize,
        /// The column in the reply's layout.
        loc: ColumnLocator,
        old: FxHashMap<&'a [u8], u128>,
    },
    /// U-SEC on `cols`: a reply entry is `[span ‖ holder PK]`, `span` bytes of span.
    Unique { cols: PkColList, span: usize },
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
    let nw = disp.num_workers();
    let leases = disp
        .scan_cut(|cut| {
            for check in checks.iter() {
                let record = encode_schema_block(check.keys.schema());
                let probe = DirectGroup {
                    schema: Some(&record),
                    ..DirectGroup::new(Read::HasPk { tid: check.tid, probe: check.probe })
                };
                match check.probe {
                    // Each worker is sent the keys it holds; one holding none is
                    // not sent the probe.
                    Probe::Pk | Probe::PkColumn(_) => {
                        let rel = disp
                            .cat()
                            .registry
                            .relation(check.tid)
                            .expect("a probed relation is registered under the catalog lock");
                        with_routed(&check.keys, probe_placement(rel), nw, |data| {
                            cut.push(data.holders(), DirectGroup { data, ..probe.clone() })
                        })?
                    }
                    // Index entries are partitioned independently of the probe
                    // key, so every worker holding the relation is probed.
                    Probe::Index(..) | Probe::IndexAll(..) => cut.read(DirectGroup {
                        data: GroupData::Same(check.keys.wire_whole()),
                        ..probe
                    })?,
                }
            }
            Ok(())
        })
        .await?;

    for (lease, check) in leases.iter().zip(checks) {
        let reply = check.reply;
        drain_rows(lease, &reply, |rows| sink(check, rows)).await?;
    }
    Ok(())
}

impl MasterDispatcher {
    /// Validate a write bundle against the state its families leave together,
    /// appending the families its deletes cascade to; an Error family's PK
    /// existence alone is cumulative in frame order. The first violation, in
    /// the order U-PK, U-SEC, F1, F2, refuses the bundle.
    pub async fn validate_txn_distributed(&self, families: &mut Vec<TxnFamily>) -> Result<(), WireFault> {
        // No family whose write reads committed state ⇒ every rule below would
        // find nothing to check, so the bundle (an O(rows) fold) is not built.
        let cat = self.cat();
        let reads_committed = families.iter().any(|f| cat.push_reads_committed_state(f.tid, f.mode));
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
            let further = self.validate_bundle(families, &mut cascade).await?;
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
        families: &[TxnFamily],
        cascade: &mut Cascade,
    ) -> Result<Vec<TxnFamily>, WireFault> {
        let mut b = TxnBundle::new(self, families)?;

        for t in &b.tables {
            if !t.cons.uniques.is_empty() && b.surviving(t.tid).next().is_some() {
                self.ensure_unique_filters_warm(t.tid, &t.cons.uniques).await?;
            }
        }

        // ── Burst 1: every probe the bundle and the catalog alone decide ──
        let mut checks: Vec<Check<Rule>> = Vec::new();
        plan_pk_checks(&b, &mut checks);
        plan_unique_checks(self, &b, &mut checks)?;
        plan_fk_existence(self, &b, &mut checks)?;

        // U-SEC's first violation is held until its turn in the verdict order.
        let mut unique_violation: Option<WireFault> = None;
        execute_probe_burst(self, &mut checks, |check, rows| {
            let Check { tid, probe, plan, .. } = check;
            let mut answered = (0..rows.len())
                .filter(|&j| rows.get_weight(j) == 1)
                .map(|j| (j, rows.get_pk_bytes(j)));
            match plan {
                Rule::Pk => {
                    let fold = b.fold_mut(*tid);
                    for (_, pk) in answered {
                        if let Some(e) = fold.get_mut(pk) {
                            e.committed = true;
                        }
                    }
                }
                Rule::Gather { loc, old, .. } => {
                    let fold = b.fold_mut(*tid);
                    for (j, pk) in answered {
                        let Some(e) = fold.get_mut(pk) else {
                            continue;
                        };
                        e.committed = true;
                        if !loc.is_null(rows, j) {
                            // Keyed by the fold's own key, which outlives the reply.
                            let (&key, _) = fold.get_key_value(pk).expect("found above");
                            old.insert(key, loc.opk_image(rows, j));
                        }
                    }
                }
                // The in-bundle duplicate check left each span one claimant, so
                // a holder that is a fold key is that claimant or gave the span up.
                Rule::Unique { cols, span } => {
                    let fold = b.fold(*tid);
                    if unique_violation.is_none() && answered.any(|(_, e)| !fold.contains_key(&e[*span..])) {
                        let err = unique_violation_err(self.cat(), *tid, cols.as_slice(), Clash::Committed);
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

        // A cascade deletes rows, which no Error family's insert turns on.
        pk_verdict(self, &b)?;

        // ── Burst 2: every FK whose parent is bundled, keyed by the deltas ──
        let children: Vec<FkEdge> = b
            .tables
            .iter()
            .flat_map(|t| t.cons.fks_as_parent.iter().copied())
            .collect();
        let deltas = resolve_parent_deltas(&b, &checks, &children);
        let (further, restricted) = removed_references(self, &b, &children, &deltas, cascade).await?;
        if !further.is_empty() {
            return Ok(further);
        }

        if let Some(e) = unique_violation {
            return Err(e);
        }
        fk_existence_verdict(self, &checks, &deltas)?;
        restricted.map_or(Ok(Vec::new()), Err)
    }
}

/// Rule U-PK, planning half, and the before-images of referenced payload
/// columns, whose replies answer U-PK as well.
fn plan_pk_checks<'a>(b: &TxnBundle<'a>, checks: &mut Vec<Check<Rule<'a>>>) {
    for t in &b.tables {
        let pk_only = t.schema.pk_only();
        let parent_cols = || t.cons.fks_as_parent.iter().map(|e| e.parent_col);
        let mut payload_refs: Vec<usize> = parent_cols().filter(|&c| !t.schema.is_pk_col(c)).collect();
        payload_refs.sort_unstable();
        payload_refs.dedup();
        if !payload_refs.is_empty() {
            let keys = PkKeys::from_keys(pk_only.pk_stride(), t.fold.keys().copied());
            if keys.is_empty() {
                continue;
            }
            for col in payload_refs {
                // The constructor the worker's projection uses.
                let reply = gnitz_zset::schema::project_schema(&t.schema, &[col as u32])
                    .expect("an FK edge's parent column is a payload column of its parent");
                let plan = Rule::Gather {
                    col,
                    loc: reply.locate(reply.payload_col_idx(0)),
                    old: FxHashMap::default(),
                };
                let keys = build_check_batch_pk_bytes(&pk_only, keys.iter());
                checks.push(Check {
                    reply,
                    ..Check::new(t.tid, Probe::PkColumn(col as u32), keys, plan)
                });
            }
            continue;
        }
        let part_pk_ref = parent_cols().any(|c| t.schema.lone_pk_col() != Some(c));
        let keys: Vec<&[u8]> = t
            .fold
            .iter()
            .filter(|(_, e)| e.needs_probe || (part_pk_ref && e.retires_pk_value(false).is_none()))
            .map(|(pk, _)| *pk)
            .collect();
        if keys.is_empty() {
            continue;
        }
        let keys = build_check_batch_pk_bytes(&pk_only, keys.into_iter());
        checks.push(Check::new(t.tid, Probe::Pk, keys, Rule::Pk));
    }
}

/// Rule U-PK, verdict half: an Error family's insert collides with an insert an
/// earlier family left, or with a committed row where no earlier family touched
/// the PK.
fn pk_verdict(disp: &MasterDispatcher, b: &TxnBundle<'_>) -> Result<(), WireFault> {
    for t in &b.tables {
        if let Some((pk, _)) = t
            .fold
            .iter()
            .find(|(_, e)| e.exists_in_bundle || (e.needs_probe && e.committed))
        {
            return Err(pk_violation_err(disp.cat(), t.tid, &t.schema, pk, Clash::Committed));
        }
    }
    Ok(())
}

/// Rule U-SEC, planning half: every (table, unique index)'s surviving spans,
/// with one committed-holder probe appended to `checks` for those its filter
/// cannot prove absent.
fn plan_unique_checks<'a>(
    disp: &MasterDispatcher,
    b: &TxnBundle<'a>,
    checks: &mut Vec<Check<Rule<'a>>>,
) -> Result<(), WireFault> {
    for t in &b.tables {
        let tid = t.tid;
        for &(col_indices, idx_schema, spec) in &t.cons.uniques {
            let key_size = spec.key_size();

            // The surviving spans as a flat arena plus an order vector, so the
            // sort moves 4-byte indices.
            let mut spans: Vec<u8> = Vec::with_capacity(t.fold.len() * key_size);
            for (_pk, fam, row) in b.surviving(tid) {
                let at = spans.len();
                spans.resize(at + key_size, 0);
                // NULL in an indexed column ⇒ unindexed.
                if !spec.write_span(b.mem(fam), row as usize, &mut spans[at..]) {
                    spans.truncate(at);
                }
            }
            let span = |i: u32| &spans[i as usize * key_size..(i as usize + 1) * key_size];
            let mut order: Vec<u32> = Vec::new();
            gnitz_zset::schema::key::sort_indices(&spans, key_size, &mut order);
            // `surviving` yields each PK once, so an adjacent-equal pair is two
            // rows of the bundle claiming one span.
            if order.windows(2).any(|w| span(w[0]) == span(w[1])) {
                return Err(unique_violation_err(
                    disp.cat(),
                    tid,
                    col_indices.as_slice(),
                    Clash::InBatch,
                ));
            }

            // A span the filter proves absent has no committed holder to answer for.
            disp.unique_filter_retain_possible(tid, col_indices, &mut order, span);
            if order.is_empty() {
                continue;
            }
            checks.push(Check {
                reply: idx_schema,
                ..Check::new(
                    tid,
                    Probe::Index(col_indices),
                    build_check_batch_pk_bytes(&spec.span_schema(), order.iter().map(|&x| span(x))),
                    Rule::Unique { cols: col_indices, span: key_size },
                )
            });
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
        let loc = b.schema(tid).locate(fk_col);

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
        let parent = disp.cat().registry.relation_or_err(parent_tid)?;
        let parent_schema = parent.schema();
        // An FK column has its parent column's type, so its image is the key's.
        let (key_schema, reply, probe) = if parent_schema.lone_pk_col() == Some(parent_col) {
            let pk_only = parent_schema.pk_only();
            (pk_only, pk_only, Probe::Pk)
        } else {
            let index = parent
                .index_on(&[parent_col as u32])
                .ok_or_else(|| format!("FK check: no unique index on parent {parent_tid} col {parent_col}"))?;
            let cols = PkColList::from_slice(&[parent_col as u32]);
            (index.key_spec().span_schema(), index.schema(), Probe::Index(cols))
        };
        let span = key_schema.pk_stride();
        if matches!(probe, Probe::Pk) {
            // The reference is the parent's whole key, so a key the bundle writes
            // is decided by the bundle and any other by whether it is committed.
            let fold = b.tables.iter().find(|t| t.tid == parent_tid).map(|t| &t.fold);
            let cache = disp.fk_presence_of(parent_tid);
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
        let mut keys: Vec<u128> = values.keys().copied().collect();
        let keys = build_check_batch(&key_schema, &mut keys);
        checks.push(Check {
            reply,
            ..Check::new(parent_tid, probe, keys, Rule::FkExists { edge, span, values })
        });
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
    let no_delta: ParentDelta = (FxHashMap::default(), FxHashSet::default());
    for check in checks {
        let Rule::FkExists { edge, values, .. } = &check.plan else {
            continue;
        };
        let (retired, added) = deltas.get(&delta_key(edge)).unwrap_or(&no_delta);
        for (v, &in_committed) in values {
            if (in_committed && !retired.contains_key(v)) || added.contains(v) {
                continue;
            }
            let cat = disp.cat();
            return Err(WireFault {
                status: WireStatus::IntegrityViolation,
                text: format!(
                    "Foreign Key violation in '{}': value not found in target '{}'",
                    cat.qualified_name(edge.child_tid),
                    cat.qualified_name(edge.parent_tid),
                ),
            });
        }
    }
    Ok(())
}

/// The `(retired, added)` referenced-column value sets rules F1 and F2 turn on,
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
                let old = t
                    .fold
                    .iter()
                    .filter(|(_, e)| e.retires_pk_value(lone).unwrap_or(e.committed))
                    .map(|(pk, _)| (*pk, gnitz_wire::widen_pk_be(&pk[off..off + size])))
                    .collect();
                parent_retired_added(b, ptid, pcol, &old)
            }
            _ => {
                let gathered = checks.iter().find_map(|c| match &c.plan {
                    Rule::Gather { col, old, .. } if c.tid == ptid && *col == pcol => Some(old),
                    _ => None,
                });
                parent_retired_added(b, ptid, pcol, gathered.unwrap_or(&FxHashMap::default()))
            }
        };
        deltas.insert((ptid, pcol), delta);
    }
    deltas
}

/// One parent column's `(retired, added)` sets: `added` are the surviving rows'
/// non-NULL values; `retired` are the `old` values, by PK, that the PK's
/// surviving state no longer holds.
fn parent_retired_added(
    b: &TxnBundle<'_>,
    parent_tid: u64,
    ref_col: usize,
    old: &FxHashMap<&[u8], u128>,
) -> ParentDelta {
    let loc = b.schema(parent_tid).locate(ref_col);
    let mut added: FxHashSet<u128> = FxHashSet::default();
    let mut retired: FxHashMap<u128, RetireVerb> = FxHashMap::default();
    for (p, e) in b.fold(parent_tid) {
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
        if let Some(&ov) = old.get(p) {
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
    (retired, added)
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
        let (retired, added) = &deltas[&delta_key(&edge)];
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
                if let Some((_, added)) = deltas.get(&delta_key(edge)) {
                    values.retain(|v| !added.contains(v));
                }
                values.retain(|&v| cascade.probed.insert((edge.child_tid, edge.fk_col, v)));
            }
        }
        seeds.retain(|(.., values)| !values.is_empty());
        if seeds.is_empty() {
            break;
        }

        let mut checks: Vec<Check<ReferencePlan>> = Vec::new();
        for (edge, cascades, mut values) in seeds.drain(..) {
            let FkEdge { child_tid, fk_col, .. } = edge;
            let child = disp.cat().registry.relation_or_err(child_tid)?;
            let cols = PkColList::from_slice(&[fk_col as u32]);
            let index = child
                .index_on(cols.as_slice())
                .ok_or_else(|| format!("FK check: no index on child {child_tid} col {fk_col}"))?;
            let schema = child.schema();
            // Only a key of the child's fold is exempt, so one entry past its
            // key count is a violation. A cascade's reply holds as well the rows
            // already found and the new ones the budget has room for: cut short
            // at that, it holds one more new row than the budget pays for.
            let mut admitted = b.tables.iter().find(|t| t.tid == child_tid).map_or(0, |t| t.fold.len());
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
            let keys = build_check_batch(&spec.span_schema(), &mut values);
            checks.push(Check {
                reply: index.schema(),
                ..Check::new(
                    child_tid,
                    Probe::IndexAll(cols, cap),
                    keys,
                    ReferencePlan { edge, spec, cascades },
                )
            });
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
                    let verb = match deltas[&delta_key(edge)].0.get(&gnitz_wire::widen_pk_be(span)) {
                        Some(RetireVerb::Update) => "update",
                        _ => "delete from",
                    };
                    let cat = disp.cat();
                    violation = Some(WireFault {
                        status: WireStatus::IntegrityViolation,
                        text: format!(
                            "Foreign Key violation: cannot {verb} '{}', row still referenced by '{}'",
                            cat.qualified_name(edge.parent_tid),
                            cat.qualified_name(edge.child_tid),
                        ),
                    });
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
            let cascading = disp
                .cat()
                .fk_children_of(tid)
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
