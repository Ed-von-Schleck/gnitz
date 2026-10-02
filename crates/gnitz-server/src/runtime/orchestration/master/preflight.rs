//! Distributed PK / FK / unique-index validation of a user write, before any
//! SAL byte is written.
//!
//! Every user write — a plain push and an atomic multi-family transaction
//! alike — is validated by `validate_txn_distributed`: a plain push is a
//! bundle of one family, so the four rules (U-PK, U-SEC, F1, F2) have one
//! implementation each. The DDL-time pre-flight for CREATE UNIQUE INDEX is
//! `unique_preflight.rs`, which shares nothing with this file but the
//! `UniqueFilter` it seeds.

use std::collections::hash_map::Entry;
use std::num::NonZeroU64;

use rustc_hash::FxHashSet;

use super::*;

use super::scatter::with_routed;
use super::train::drain_rows;
use crate::catalog::{FkEdge, RowConstraints};
use crate::runtime::orchestration::TxnFamily;
use gnitz_expr::{ColumnLocator, ColumnTable, SchemaFacts};
use gnitz_store::relation::Relation;
use gnitz_wire::{PkColList, PkKeys, Probe, WireConflictMode, WireStatus};
use gnitz_zset::repr::MemBatch;
use gnitz_zset::schema::key::PkBuf;
use gnitz_zset::schema::KeySpec;

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

/// The row of the ascending `keys` that `key` prefixes.
fn row_of(keys: &Batch, key: &[u8]) -> Option<usize> {
    let lo = keys.find_lower_bound_bytes(PkBuf::from_bytes(key).padded(keys.schema().pk_stride()));
    (lo < keys.len() && keys.get_pk_bytes(lo).starts_with(key)).then_some(lo)
}

/// Where an own-PK probe of `rel` goes: to the owners of its keys — or, every
/// worker holding a replicated relation whole, spread over the full key.
fn probe_placement(rel: &Relation) -> Placement {
    match rel.placement() {
        Placement::Replicated => Placement::full_pk(&rel.schema()),
        p => p,
    }
}

/// A PK-sorted check batch whose row `j` carries `keys[j]` — an image at type
/// `ref_tc` — in the leading key column of `schema`'s PK, the rest of each key
/// zero. Sorts `keys` first: an image orders as its OPK bytes do.
fn build_check_batch(schema: &SchemaDescriptor, keys: &mut [u128], ref_tc: gnitz_wire::TypeCode) -> Batch {
    keys.sort_unstable();
    let key_tc = schema.columns[schema.pk_cols()[0] as usize].type_code;
    let (ref_w, key_w) = (ref_tc.wire_stride(), key_tc.wire_stride());
    let mut key = [0u8; gnitz_wire::MAX_PK_BYTES];
    let mut batch = Batch::with_capacity(schema, keys.len());
    for &k in keys.iter() {
        gnitz_wire::store_opk_image(k, ref_tc, ref_w, key_tc, &mut key[..key_w]);
        batch.push_key_row(&key[..schema.pk_stride()], 1);
    }
    batch
}

/// Build a check batch from `keys`, OPK byte spans already in the target
/// schema's key layout. Each lands verbatim in the PK region, where
/// [`build_check_batch`] re-encodes column 0.
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

    /// Is `tid` one of the bundle's listed tables?
    fn has(&self, tid: u64) -> bool {
        self.tables.iter().any(|t| t.tid == tid)
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
    /// there. A holder the bundle does not touch keeps its claim.
    fn retires(&self, tid: u64, spec: &KeySpec, holder: &[u8], span: &[u8], buf: &mut PkBuf) -> bool {
        match self.fold(tid).get(holder).map(|e| e.last) {
            None => false,
            Some(FoldOp::Deleted) => true,
            Some(FoldOp::Inserted(f, r)) => !spec.key_bytes(self.mem(f), r as usize, buf) || buf.pk_bytes() != span,
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
    /// F1 on `edge`: key row `j` carries `values[j]`, flagged once a reply finds
    /// it committed. A reply's leading `span` bytes are the key it answers.
    FkExists {
        edge: FkEdge,
        span: usize,
        values: Vec<(u128, bool)>,
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
                let (probe_mode, arg0, arg1) = check.probe.wire();
                let schema = wire::WireSchema::encoded(check.tid, check.keys.schema());
                let template = schema.frame(wire::WireMsg {
                    arg1,
                    arg0,
                    flags: WireFlags { probe_mode, ..Default::default() },
                    ..Default::default()
                });
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
                            cut.push(
                                data.holders(),
                                DirectGroup {
                                    template,
                                    data,
                                    ..DirectGroup::new(SalMessageKind::HasPk)
                                },
                            )
                        })?
                    }
                    // Index entries are partitioned independently of the probe
                    // key, so every worker holding the relation is probed.
                    Probe::Index(..) => cut.read(DirectGroup {
                        template,
                        data: GroupData::Same(check.keys.wire_whole()),
                        ..DirectGroup::new(SalMessageKind::HasPk)
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
    /// Validate a write bundle against the state its families leave together;
    /// an Error family's PK existence alone is cumulative in frame order. The
    /// first violation, in the order U-PK, U-SEC, F1, F2, refuses the bundle.
    pub async fn validate_txn_distributed(&self, families: &[TxnFamily]) -> Result<(), WireFault> {
        // No family whose write reads committed state ⇒ every rule below would
        // find nothing to check, so the bundle (an O(rows) fold) is not built.
        let cat = self.cat();
        let reads_committed = families.iter().any(|f| cat.push_reads_committed_state(f.tid, f.mode));
        if !reads_committed {
            return Ok(());
        }
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
            let Check { tid, keys, plan, .. } = check;
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
                        if let Some(r) = row_of(keys, &e[..*span]) {
                            values[r].1 = true;
                        }
                    }
                }
            }
            Ok(())
        })
        .await?;

        pk_verdict(self, &b)?;
        if let Some(e) = unique_violation {
            return Err(e);
        }
        // Every FK whose parent is bundled: F2 checks the values it removes
        // are unreferenced.
        let children: Vec<FkEdge> = b
            .tables
            .iter()
            .flat_map(|t| t.cons.fks_as_parent.iter().copied())
            .collect();
        let deltas = resolve_parent_deltas(&b, &checks, &children);
        fk_existence_verdict(self, &checks, &deltas)?;

        // ── Burst 2: F2, keyed by the deltas ─────────────────────────────
        txn_check_fk_restrict(self, &b, &children, &deltas).await
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
            let stride = idx_schema.pk_stride();

            // The surviving spans as a flat arena plus an order vector, so the
            // sort moves 4-byte indices.
            let mut spans: Vec<u8> = Vec::with_capacity(t.fold.len() * stride);
            let mut keybuf = PkBuf::zeroed(0);
            for (_pk, fam, row) in b.surviving(tid) {
                // NULL in an indexed column ⇒ unindexed.
                if spec.key_bytes(b.mem(fam), row as usize, &mut keybuf) {
                    spans.extend_from_slice(keybuf.padded(stride));
                }
            }
            // Each arena slot is a span zero-padded to the index stride, which
            // is the probe key.
            let key_size = spec.key_size();
            let probe_key = |i: u32| &spans[i as usize * stride..(i as usize + 1) * stride];
            let span = |i: u32| &probe_key(i)[..key_size];
            let mut order: Vec<u32> = Vec::new();
            gnitz_zset::schema::key::sort_indices(&spans, stride, &mut order);
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
            checks.push(Check::new(
                tid,
                Probe::Index(col_indices, NonZeroU64::MIN),
                build_check_batch_pk_bytes(&idx_schema, order.iter().map(|&x| probe_key(x))),
                Rule::Unique { cols: col_indices, span: key_size },
            ));
        }
    }
    Ok(())
}

/// Rule F1, planning half: one committed-occupancy probe per FK constraint
/// whose child is bundled, over the distinct non-NULL FK values of that child's
/// surviving rows.
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
        } = edge;
        let loc = b.schema(tid).locate(fk_col);

        let mut seen: FxHashSet<u128> = FxHashSet::default();
        let mut values: Vec<u128> = Vec::new();
        for (_pk, fam, row) in b.surviving(tid) {
            let m = b.mem(fam);
            let r = row as usize;
            if !loc.is_null(m, r) {
                let v = loc.opk_image(m, r);
                if seen.insert(v) {
                    values.push(v);
                }
            }
        }
        if values.is_empty() {
            continue;
        }

        // The parent's own PK store where the referenced column is its lone PK
        // column, its UNIQUE index on the column otherwise.
        let parent = disp.cat().registry.relation_or_err(parent_tid)?;
        let parent_schema = parent.schema();
        let (key_schema, probe, span) = if parent_schema.lone_pk_col() == Some(parent_col) {
            (parent_schema.pk_only(), Probe::Pk, parent_schema.pk_stride())
        } else {
            let index = parent
                .index_on(&[parent_col as u32])
                .ok_or_else(|| format!("FK check: no unique index on parent {parent_tid} col {parent_col}"))?;
            let cols = PkColList::from_slice(&[parent_col as u32]);
            (
                index.schema(),
                Probe::Index(cols, NonZeroU64::MIN),
                index.key_spec().key_size(),
            )
        };
        let keys = build_check_batch(&key_schema, &mut values, loc.type_code());
        let values = values.into_iter().map(|v| (v, false)).collect();
        checks.push(Check::new(
            parent_tid,
            probe,
            keys,
            Rule::FkExists { edge, span, values },
        ));
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
        for (v, in_committed) in values {
            if (*in_committed && !retired.contains_key(v)) || added.contains(v) {
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

/// One planned FK RESTRICT probe on one child index.
struct RestrictPlan {
    edge: FkEdge,
    /// Splits a reply entry into `[span ‖ holder PK]`, and re-encodes a
    /// surviving holder's own span for the exemption test.
    spec: KeySpec,
    /// The probed referenced values' key images, sorted: key row `j` encodes
    /// `values[j]`. A reply names a span, the rejection names a value.
    values: Vec<u128>,
    /// The bundle touches this child, so a committed holder it retires is
    /// exempt; for an unbundled child the first committed holder is fatal.
    bundled: bool,
}

/// Rule F2: a referenced value the bundle removes and does not re-add —
/// `retired ∖ added` — must have no surviving child row referencing it.
async fn txn_check_fk_restrict(
    disp: &MasterDispatcher,
    b: &TxnBundle<'_>,
    children: &[FkEdge],
    deltas: &ParentDeltas,
) -> Result<(), WireFault> {
    let mut checks: Vec<Check<RestrictPlan>> = Vec::new();
    for &edge in children {
        let FkEdge {
            child_tid,
            fk_col,
            parent_tid,
            parent_col,
        } = edge;
        let (retired, added) = &deltas[&delta_key(&edge)];
        let mut values: Vec<u128> = retired.keys().copied().filter(|v| !added.contains(v)).collect();
        if values.is_empty() {
            continue;
        }
        let (idx_schema, spec) = disp
            .cat()
            .registry
            .relation(child_tid)
            .and_then(|r| r.index_on(&[fk_col as u32]))
            .map(|ic| (ic.schema(), ic.key_spec()))
            .ok_or_else(|| format!("FK RESTRICT: no index on child {child_tid} col {fk_col}"))?;
        let ref_tc = b.schema(parent_tid).columns[parent_col].type_code;
        let keys = build_check_batch(&idx_schema, &mut values, ref_tc);
        let bundled = b.has(child_tid);
        // Past `K` holders one is not a key of the child's `K`-key fold, and
        // only a fold key is exempt. An unbundled child exempts none.
        let exempt = if bundled { b.fold(child_tid).len() as u64 } else { 0 };
        let probe = Probe::Index(
            PkColList::from_slice(&[fk_col as u32]),
            NonZeroU64::MIN.saturating_add(exempt),
        );
        let plan = RestrictPlan { edge, spec, values, bundled };
        checks.push(Check::new(child_tid, probe, keys, plan));
    }

    // The first non-exempt holder aborts the drain, whose lease drop discards
    // every train still in flight.
    let mut hspan = PkBuf::zeroed(0);
    execute_probe_burst(disp, &mut checks, |check, rows| {
        let plan = &check.plan;
        let retired = &deltas[&delta_key(&plan.edge)].0;
        for j in 0..rows.len() {
            if rows.get_weight(j) != 1 {
                continue;
            }
            let (span, holder) = plan.spec.split_entry(rows.get_pk_bytes(j));
            let Some(r) = row_of(&check.keys, span) else {
                continue;
            };
            if plan.bundled && b.retires(plan.edge.child_tid, &plan.spec, holder, span, &mut hspan) {
                continue;
            }
            // Anything but a surviving row holding a new value reads as a delete.
            let verb = match retired.get(&plan.values[r]) {
                Some(RetireVerb::Update) => "update",
                _ => "delete from",
            };
            let cat = disp.cat();
            return Err(WireFault {
                status: WireStatus::IntegrityViolation,
                text: format!(
                    "Foreign Key violation: cannot {verb} '{}', row still referenced by '{}'",
                    cat.qualified_name(plan.edge.parent_tid),
                    cat.qualified_name(plan.edge.child_tid),
                ),
            });
        }
        Ok(())
    })
    .await
}

#[cfg(test)]
#[path = "tests/preflight.rs"]
mod tests;
