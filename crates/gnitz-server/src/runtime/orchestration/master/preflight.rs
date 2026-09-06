//! Distributed PK / FK / unique-index validation of a user write, before any
//! SAL byte is written.
//!
//! Every user write — a plain push and an atomic multi-family transaction
//! alike — is validated by `validate_txn_distributed`: a plain push is a
//! bundle of one family, so the four rules (U-PK, U-SEC, F1, F2) have one
//! implementation each. The DDL-time pre-flight for CREATE UNIQUE INDEX is
//! `unique_preflight.rs`, which shares nothing with this file but the
//! `UniqueFilter` it seeds.

use super::*;

use super::unique_filter::ensure_unique_filters_warm;
use crate::catalog::FkEdge;
use gnitz_expr::{ColumnLocator, SchemaFacts};
use gnitz_store::storage::MemBatch;

// ---------------------------------------------------------------------------
// Pipelined validation checks
// ---------------------------------------------------------------------------

/// Which keyspace a check probes, and so how its rows must reach the workers.
/// Index entries are placed by their OWNER's PK, not by the span they carry,
/// so an index probe cannot be scattered by its own key.
enum Keyspace {
    /// The relation's own PK store: scattered, each worker sent the keys it holds.
    OwnPk,
    /// A secondary index, as `pack_pk_cols(&[col…])`: broadcast.
    Index(u64),
}

impl Keyspace {
    fn index(cols: &[u32]) -> Self {
        Keyspace::Index(gnitz_wire::pack_pk_cols(cols))
    }

    /// The worker's `seek_col_idx`, which `gnitz_wire::probe_key_columns`
    /// reads back.
    fn seek_col_idx(&self) -> u64 {
        match self {
            Keyspace::OwnPk => gnitz_wire::PROBE_KEYSPACE_PK,
            Keyspace::Index(packed) => *packed,
        }
    }

    fn scatters(&self) -> bool {
        matches!(self, Keyspace::OwnPk)
    }
}

/// A single distributed has-pk check queued for pipelined execution (always
/// dispatched under HasPk).
///
/// `schema` is carried, not looked up from its tid: an index check sends the
/// INDEX table's schema `(indexed_col, src_pk…)` under the OWNER table's id,
/// and the owner's own schema would give the wrong key width. It is always
/// payload-free, so `batch` is built row-by-key.
struct PipelinedCheck {
    keyspace: Keyspace,
    mode: gnitz_wire::WireProbeMode,
    /// The parameter the modes that take one ride in, travelling in `seek_pk`:
    /// `AllHolders`' per-value holder cap, or `Project`'s column index. They are
    /// mutually exclusive; `0` for the modes that take none.
    mode_param: u64,
    batch: Batch,
    schema: wire::WireSchema,
    /// The reply's schema when it is not the probe's own — only `Project`,
    /// which answers `(key, projected column)`.
    reply: Option<SchemaDescriptor>,
}

impl PipelinedCheck {
    /// The schema this check's replies decode against.
    fn reply_schema(&self) -> &SchemaDescriptor {
        self.reply.as_ref().unwrap_or_else(|| self.schema.descriptor())
    }
}

/// The PK-only image of `schema`, at the source relation's placement. A probe
/// carries keys and nothing else, and it is routed by them: `project_schema`
/// resets placement to the keyed default, which on a `CLUSTER BY` table is a
/// different router width and so a different worker.
fn probe_schema(schema: &SchemaDescriptor) -> SchemaDescriptor {
    gnitz_store::schema::project_schema(schema, &[])
        .expect("a PK-only projection fits MAX_COLUMNS")
        .with_placement(schema.placement())
}

/// Build a check batch from narrow `u128` keys, encoded through [`enc_key`].
/// `src_type` is the type of the column they came from — the child FK column,
/// or the parent PK/indexed column — which is what a signed source sign-extends
/// from. The source-PK suffix of an index key is left zero: only the leading
/// column is prefix-matched.
fn build_check_batch(schema: &SchemaDescriptor, keys: &[u128], src_type: u8) -> Batch {
    let mut batch = Batch::with_capacity(*schema, keys.len());
    for &k in keys {
        batch.push_key_row(enc_key(schema, k, src_type).pk_bytes(), 1);
    }
    batch
}

/// Build a check batch from `keys`, OPK byte spans already in the target
/// schema's key layout. Each lands verbatim in the PK region, where
/// [`build_check_batch`] re-encodes column 0.
fn build_check_batch_pk_bytes<'k>(schema: &SchemaDescriptor, keys: impl ExactSizeIterator<Item = &'k [u8]>) -> Batch {
    let mut batch = Batch::with_capacity(*schema, keys.len());
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

/// What one FAMILY does to one PK. `net` and `dups` are per-family by
/// definition: the Error-mode duplicate rule counts a PK's insertions *within
/// one family*, which the last-op-wins whole-table merge cannot express.
#[derive(Clone, Copy)]
struct PkFold {
    /// Summed weight over the PK's rows in this family.
    net: i64,
    /// Saturating "did this PK see two or more insertions?" counter: `0`, `1`,
    /// or `2` for anything above. A `+w` row counts as `w` insertions —
    /// Error-mode duplicate rejection treats it like the `w` separate `+1` rows
    /// it encodes — so any value `> 1` is a within-family duplicate.
    dups: u32,
    /// This family's last op on the PK.
    last: FoldOp,
}

/// One PK's entry in a table's merged fold.
struct TableFold {
    /// The op that survives the whole bundle: the last family's, in frame order.
    last: FoldOp,
    /// The PK exists in committed state, as U-PK's probe answered it. `false`
    /// until that burst drains, and for a table it did not probe — which is
    /// also the right answer there, since it probes every table with an Error
    /// family or an FK child and those are its only readers.
    committed: bool,
}

/// One table's fold: the last op per PK, merged over its families in frame
/// order. The key is the row's OPK bytes borrowed from the family batch's PK
/// region — the families outlive every bundle built over them, and a bulk push
/// folds millions of rows, where an owned 81-byte `PkBuf` key would cost
/// several times the borrowed slice.
type Overlay<'a> = FxHashMap<&'a [u8], TableFold>;

/// One table's whole-bundle fold: the per-family folds in frame order, and
/// their merge, materialized so `merged.get(pk)` — the hottest lookup in U-SEC
/// and F2 — stays O(1) rather than O(#families).
struct Fold<'a> {
    families: Vec<FxHashMap<&'a [u8], PkFold>>,
    merged: Overlay<'a>,
}

/// The verb for the RESTRICT rejection on referenced value `v`: how the bundled
/// parent write removed it. Both removals are a "cannot do this to the row"
/// rejection, so anything but a surviving row holding a new value reads as a
/// delete.
fn restrict_verb(retired: &FxHashMap<u128, RetireVerb>, v: u128) -> &'static str {
    match retired.get(&v) {
        Some(RetireVerb::Update) => "update",
        _ => "delete from",
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

/// The `(retired, added)` referenced-value sets of one bundled FK parent column
/// (see `parent_retired_added`). Each retired value carries the verb of the
/// write that retired it; a value retired by both a delete and an update reads
/// as a delete, so the verdict does not depend on fold order.
type ParentDelta = (FxHashMap<u128, RetireVerb>, FxHashSet<u128>);

/// `(parent tid, referenced col)` → its delta. Resolved once per key and shared
/// by rules F1 and F2, which both turn on it.
type ParentDeltas = FxHashMap<(i64, usize), ParentDelta>;

/// The [`ParentDeltas`] key `e`'s referenced value lives under.
fn delta_key(e: &FkEdge) -> (i64, usize) {
    (e.parent_tid, e.parent_col)
}

/// One table of the bundle: its families in frame order, its catalog schema,
/// and its fold.
struct TxnTable<'a> {
    tid: i64,
    schema: SchemaDescriptor,
    /// Indices into the bundle's family list, in frame order.
    family_indices: Vec<usize>,
    /// `Some` exactly when the table has an Error family or a row constraint —
    /// the tables the rules read. A plain `INSERT` into a table with neither
    /// skips the O(rows) walk, and the `Option` is what makes reading a fold
    /// that was never built a compile-time obligation rather than an empty map.
    fold: Option<Fold<'a>>,
}

/// A decoded transaction bundle plus everything its rules share. Built once;
/// every rule reads it instead of re-walking the families.
struct TxnBundle<'a> {
    families: &'a [TxnFamily],
    /// One borrowed columnar view per family, so a rule's row walk takes
    /// `&MemBatch` by family index instead of threading the owning `Batch`.
    mems: Vec<MemBatch<'a>>,
    /// The bundle's tables in first-appearance order, found by linear scan over
    /// the write's own table count.
    tables: Vec<TxnTable<'a>>,
}

impl<'a> TxnBundle<'a> {
    fn new(disp: &MasterDispatcher, families: &'a [TxnFamily]) -> Result<Self, String> {
        let mut tables: Vec<TxnTable<'a>> = Vec::new();
        for (fi, fam) in families.iter().enumerate() {
            match tables.iter_mut().find(|t| t.tid == fam.tid) {
                Some(t) => t.family_indices.push(fi),
                None => tables.push(TxnTable {
                    tid: fam.tid,
                    schema: disp.cat().registry().table_entry(fam.tid)?.schema,
                    family_indices: vec![fi],
                    fold: None,
                }),
            }
        }
        for t in &mut tables {
            // Per TABLE, never per family: the client's "delete k; insert k"
            // idiom is an `Update` family `[D(k)]` then an `Error` family
            // `[I(k)]`, and a per-family gate would leave the Error family's
            // prefix empty and reject the re-insert.
            let error_mode = t
                .family_indices
                .iter()
                .any(|&fi| matches!(families[fi].mode, WireConflictMode::Error));
            if !error_mode && !disp.cat().has_row_constraints(t.tid) {
                continue;
            }
            let mut per_family: Vec<FxHashMap<&'a [u8], PkFold>> = Vec::with_capacity(t.family_indices.len());
            let mut merged: Overlay<'a> = Overlay::default();
            for &fi in &t.family_indices {
                let batch = &families[fi].batch;
                let mut fold: FxHashMap<&'a [u8], PkFold> =
                    FxHashMap::with_capacity_and_hasher(batch.len(), Default::default());
                for row in 0..batch.len() {
                    let w = batch.get_weight(row);
                    if w == 0 {
                        continue;
                    }
                    let last = if w > 0 {
                        FoldOp::Inserted(fi as u32, row as u32)
                    } else {
                        FoldOp::Deleted
                    };
                    let e = fold
                        .entry(batch.get_pk_bytes(row))
                        .or_insert(PkFold { net: 0, dups: 0, last });
                    e.net += w;
                    e.last = last;
                    if w > 0 {
                        e.dups += if w > 1 { 2 } else { 1 };
                    }
                }
                // Merge in frame order, taking the family's last op: iterating
                // the family's DISTINCT PKs, not its rows, so the whole rule
                // costs one pass over rows rather than one per reader.
                for (&pk, f) in &fold {
                    merged
                        .entry(pk)
                        .and_modify(|e| e.last = f.last)
                        .or_insert(TableFold { last: f.last, committed: false });
                }
                per_family.push(fold);
            }
            t.fold = Some(Fold { families: per_family, merged });
        }
        Ok(TxnBundle {
            mems: families.iter().map(|f| f.batch.as_mem_batch()).collect(),
            families,
            tables,
        })
    }

    /// Is `tid` one of the bundle's tables?
    fn has(&self, tid: i64) -> bool {
        self.tables.iter().any(|t| t.tid == tid)
    }

    fn table(&self, tid: i64) -> &TxnTable<'a> {
        self.tables.iter().find(|t| t.tid == tid).expect("a bundled table")
    }

    fn schema(&self, tid: i64) -> &SchemaDescriptor {
        &self.table(tid).schema
    }

    fn fold(&self, tid: i64) -> &Fold<'a> {
        self.table(tid).fold.as_ref().expect("a folded table")
    }

    fn overlay(&self, tid: i64) -> &Overlay<'a> {
        &self.fold(tid).merged
    }

    /// Family `fam`'s columnar view, built once in `new`.
    fn mem(&self, fam: u32) -> &MemBatch<'a> {
        &self.mems[fam as usize]
    }

    /// The rows of `tid` that survive the whole bundle: `(pk, family, row)` —
    /// the `Inserted` projection of its fold. Walked, never materialized: every
    /// reader consumes it in one pass, so a list would be a second copy.
    fn surviving(&self, tid: i64) -> impl Iterator<Item = (&'a [u8], u32, u32)> + '_ {
        self.overlay(tid).iter().filter_map(|(pk, e)| match e.last {
            FoldOp::Inserted(f, r) => Some((*pk, f, r)),
            FoldOp::Deleted => None,
        })
    }

    /// The touched PKs of `tid` that exist in committed state — the only ones
    /// with an old referenced value to retire.
    fn touched_committed(&self, tid: i64) -> impl Iterator<Item = &'a [u8]> + '_ {
        self.overlay(tid).iter().filter(|(_, e)| e.committed).map(|(pk, _)| *pk)
    }

    /// The merged fold of `tid`, for U-PK's burst to record its answers into.
    fn fold_mut(&mut self, tid: i64) -> &mut Overlay<'a> {
        &mut self
            .tables
            .iter_mut()
            .find(|t| t.tid == tid)
            .expect("a bundled table")
            .fold
            .as_mut()
            .expect("a folded table")
            .merged
    }
}

/// One planned FK existence probe: a committed-occupancy check for `values`.
/// Which side of `edge` is probed is fixed by the rule, not stored.
///
/// The encoded key images are not kept: `build_check_batch` wrote row `j` from
/// `values[j]` and the batch outlives the verdict, so `batch.get_pk_bytes(j)`
/// IS the image the reply echoes. Both routes filter their own index lists,
/// never the master's batch, so the correspondence holds on either.
struct FkProbePlan {
    edge: FkEdge,
    values: Vec<u128>,
}

/// One planned unique-secondary-index check: the circuit's columns, the key
/// encoder that splits a reply entry into `[span ‖ holder]`, and the
/// `(span, surviving claimant PK)` pairs to verify the reply against.
///
/// The pairs are columnar — a flat span arena at `stride` plus a claimant
/// vector — with `order` naming them in span order, which is also the order the
/// check batch ships, so a reply is a binary search over `order`.
struct UniquePlan<'a> {
    tid: i64,
    col_indices: PkColList,
    spec: IndexKeySpec,
    stride: usize,
    spans: Vec<u8>,
    holders: Vec<&'a [u8]>,
    order: Vec<u32>,
}

impl UniquePlan<'_> {
    /// Entry `i`'s probe key: the leading-key span zero-padded to the index's
    /// full PK stride, which is what the check batch ships.
    fn probe_key(&self, i: u32) -> &[u8] {
        &self.spans[i as usize * self.stride..(i as usize + 1) * self.stride]
    }

    /// Entry `i`'s leading-key span alone — what a reply entry splits back to,
    /// and what the unique filter is keyed on.
    fn span(&self, i: u32) -> &[u8] {
        &self.probe_key(i)[..self.spec.key_size()]
    }
}

/// Encode a native value `v` (from a column of type `src_type`) into the OPK
/// leading-key image of `schema`'s primary key — the one encoder for these
/// probe keys, so a reply that echoes one compares byte-identical.
///
/// The leading key column is `pk_indices()[0]`, not `columns[0]`: it is column
/// 0 for an index schema and for a projected probe schema, but a catalog
/// schema's lone PK may be declared at any position.
fn enc_key(schema: &SchemaDescriptor, v: u128, src_type: u8) -> PkBuf {
    let key_col = schema.pk_indices()[0] as usize;
    let idx_key_type = schema.columns[key_col].type_code;
    gnitz_store::schema::key::index_opk_prefix(v, src_type, idx_key_type).widened(schema.pk_stride() as usize)
}

/// Fire every check in `checks` as ONE SAL cut and drain the replies, handing
/// each frame's rows to `sink` as `(check index, rows)`. One write can need a
/// check per table, per unique index and per FK edge; issuing them one at a
/// time would cost that many round trips.
///
/// **Replies are trains, not single frames.** Two probe shapes are unbounded by
/// the write that issued them: a unique-index probe whose promoted
/// `[span ‖ holder]` entries are wider than the source rows, and a
/// holder-listing one, whose row count is a property of committed state.
///
/// Draining check `i` fully before `i+1` is what `dispatch_scan_multi_fanout`'s
/// FIFO flag is for.
async fn execute_probe_burst(
    disp: &MasterDispatcher,
    reactor: &crate::runtime::reactor::Reactor,
    checks: &[PipelinedCheck],
    mut sink: impl FnMut(usize, &MemBatch<'_>) -> Result<(), WorkerFault>,
) -> Result<(), WorkerFault> {
    if checks.is_empty() {
        return Ok(());
    }
    // Every probe reaches every worker: a scattered one because each worker
    // holds a slice of the key list, a broadcast one because index entries are
    // partitioned independently of the probe key.
    let fanouts = vec![Fanout::Broadcast; checks.len()];
    let dispatches = dispatch_scan_multi_fanout(disp, reactor, &fanouts, |i, targets, wire_flags| {
        let check = &checks[i];
        let g = DirectGroup {
            template: check.schema.frame(wire::WireMsg {
                seek_col_idx: check.keyspace.seek_col_idx(),
                seek_pk: check.mode_param as u128,
                flags: gnitz_wire::wire_flags_set_probe_mode(wire_flags, check.mode),
                ..Default::default()
            }),
            data: GroupData::Same(wire::WireData::Whole(Some(&check.batch))),
            targets,
            ..DirectGroup::new(SalMessageKind::HasPk)
        };
        if check.keyspace.scatters() {
            disp.write_scatter_group(&check.batch, &check.schema, g)
        } else {
            disp.write_group(&g)
        }
    })
    .await?;

    for (i, scan) in dispatches.iter().enumerate() {
        let slots = scan.await_slots(reactor).await;
        drain_index_scan(slots, scan, reactor, "pipeline", checks[i].reply_schema(), |b, _| {
            sink(i, b)
        })
        .await?;
    }
    Ok(())
}

impl MasterDispatcher {
    /// Validate a user-table write bundle against its post-transaction state
    /// (the simulated fold of all families), except Error-mode PK existence
    /// which is cumulative in frame order. The whole bundle passes or one
    /// violation aborts it — all pre-SAL. Every write comes through here: a
    /// plain push is a bundle of one family, so each of the four rules has one
    /// implementation. Committed state is stable across every probe because the
    /// caller holds the involved tables' locks and the catalog read lock through
    /// the ACK.
    ///
    /// **Three bursts, not one per rule.** U-PK, U-SEC and F1 plan every probe
    /// they need from the bundle and the catalog alone, so they share one; the
    /// parent gathers take a second, because their key list is U-PK's answer;
    /// and F2 takes a third, because its probed values are the gathers' answer.
    /// Merging the probes does not merge the verdicts — those still run in the
    /// order below, which is the order violations are reported in.
    pub async fn validate_txn_distributed(
        &self,
        reactor: &crate::runtime::reactor::Reactor,
        families: &[TxnFamily],
    ) -> Result<(), WorkerFault> {
        // No family whose write reads committed state ⇒ every rule below would
        // find nothing to check, so the bundle (an O(rows) fold) is not built.
        let cat = self.cat();
        let reads_committed = families.iter().any(|f| cat.push_reads_committed_state(f.tid, f.mode));
        if !reads_committed {
            return Ok(());
        }
        let mut b = TxnBundle::new(self, families)?;

        // Warm the unique filters before planning, so no await sits between the
        // bursts: nothing consumes a filter until U-SEC's elision test, and a
        // warm one returns before awaiting anything. A write that will fail
        // U-PK therefore pays the warm-up scan first, once per table per boot.
        for t in &b.tables {
            if self.cat().registry().has_any_unique_index(t.tid) && b.surviving(t.tid).next().is_some() {
                ensure_unique_filters_warm(self, reactor, t.tid).await?;
            }
        }

        // Every FK whose child is bundled (F1 checks those rows' values exist),
        // and every FK whose parent is bundled (F2 checks the values it removes
        // are unreferenced).
        let mut constraints: Vec<FkEdge> = Vec::new();
        let mut children: Vec<FkEdge> = Vec::new();
        for t in &b.tables {
            let cat = self.cat();
            constraints.extend(cat.fk_constraints_of(t.tid).iter().copied());
            children.extend(cat.fk_children_of(t.tid).iter().copied());
        }

        // ── Burst 1 ──────────────────────────────────────────────────────
        let mut checks: Vec<PipelinedCheck> = Vec::new();
        let pk_tids = plan_pk_checks(self, &b, &mut checks)?;
        let n_pk = checks.len();
        let uniq_plans = plan_unique_checks(self, &b, &mut checks)?;
        let n_uniq = checks.len();
        let f1_plans = plan_fk_existence(self, &b, &constraints, &mut checks)?;

        // U-PK's answer lands on the fold; U-SEC's and F1's are found sets.
        let mut found: Vec<FxHashSet<PkBuf>> = (0..checks.len()).map(|_| FxHashSet::default()).collect();
        execute_probe_burst(self, reactor, &checks, |i, rows| {
            if i < n_pk {
                let merged = b.fold_mut(pk_tids[i]);
                for j in 0..rows.len() {
                    if rows.get_weight(j) == 1 {
                        if let Some(e) = merged.get_mut(rows.get_pk_bytes(j)) {
                            e.committed = true;
                        }
                    }
                }
            } else {
                for j in 0..rows.len() {
                    if rows.get_weight(j) == 1 {
                        found[i].insert(PkBuf::from_bytes(rows.get_pk_bytes(j)));
                    }
                }
            }
            Ok(())
        })
        .await?;

        pk_verdict(self, &b)?;

        // ── Burst 2: the parent gathers, keyed by U-PK's answer ──────────
        let deltas = resolve_parent_deltas(self, reactor, &b, &constraints, &children).await?;

        unique_verdict(self, &b, &uniq_plans, &found[n_pk..n_uniq])?;
        fk_existence_verdict(self, &f1_plans, &checks[n_uniq..], &found[n_uniq..], &deltas)?;

        // ── Burst 3: F2, keyed by the deltas ─────────────────────────────
        txn_check_fk_restrict(self, reactor, &b, &children, &deltas).await
    }
}

/// Rule U-PK, planning half: one committed-existence probe per probed table,
/// appended to `checks`; returns each one's table in the same order.
///
/// The in-batch duplicate rule needs no probe, so it fires here — before the
/// round trip, and before any existence check on the table, which is what makes
/// "duplicate in batch" win over "already exists" rather than race it.
fn plan_pk_checks(
    disp: &MasterDispatcher,
    b: &TxnBundle<'_>,
    checks: &mut Vec<PipelinedCheck>,
) -> Result<Vec<i64>, WorkerFault> {
    let mut probed: Vec<i64> = Vec::new();
    for t in &b.tables {
        let Some(fold) = t.fold.as_ref() else {
            continue;
        };
        let error_families: Vec<usize> = (0..t.family_indices.len())
            .filter(|&n| matches!(b.families[t.family_indices[n]].mode, WireConflictMode::Error))
            .collect();
        // A bundled FK parent makes the probe worth issuing even without an
        // Error family: `parent_retired_added` decides which touched PKs have
        // an old referenced value from exactly this answer.
        let fk_parent = !disp.cat().fk_children_of(t.tid).is_empty();
        if error_families.is_empty() && !fk_parent {
            continue;
        }
        for &n in &error_families {
            for (&pk, f) in &fold.families[n] {
                if f.dups > 1 {
                    return Err(disp.cat().pk_violation_err(t.tid, &t.schema, pk, true).into());
                }
            }
        }
        // For an FK parent, every touched PK: a deleted one retires its
        // referenced value just as an overwritten one does, so narrowing this
        // to the net-positive keys would silently break F2. For Error mode
        // alone only a net-positive key can collide.
        let keys: Vec<&[u8]> = if fk_parent {
            fold.merged.keys().copied().collect()
        } else {
            let mut candidate: FxHashSet<&[u8]> = FxHashSet::default();
            for &n in &error_families {
                candidate.extend(fold.families[n].iter().filter(|(_, f)| f.net > 0).map(|(pk, _)| *pk));
            }
            candidate.into_iter().collect()
        };
        if keys.is_empty() {
            continue;
        }
        let pk_only = probe_schema(&t.schema);
        checks.push(PipelinedCheck {
            keyspace: Keyspace::OwnPk,
            mode: gnitz_wire::WireProbeMode::Exists,
            mode_param: 0,
            batch: build_check_batch_pk_bytes(&pk_only, keys.into_iter()),
            schema: wire::WireSchema::encoded(t.tid, pk_only),
            reply: None,
        });
        probed.push(t.tid);
    }
    Ok(probed)
}

/// Rule U-PK, verdict half: each table's families are walked in frame order and
/// each Error family checked against the running prefix fold, then committed
/// state. The prefix accumulates over the per-family folds — distinct PKs, not
/// rows.
fn pk_verdict(disp: &MasterDispatcher, b: &TxnBundle<'_>) -> Result<(), WorkerFault> {
    for t in &b.tables {
        let Some(fold) = t.fold.as_ref() else {
            continue;
        };
        let fis = &t.family_indices;
        // A single-family table needs no prefix at all — the common case, since
        // a plain push is one family.
        let mut prefix: FxHashMap<&[u8], FoldOp> = FxHashMap::default();
        for (n, &fi) in fis.iter().enumerate() {
            if matches!(b.families[fi].mode, WireConflictMode::Error) {
                for (&pk, f) in &fold.families[n] {
                    if f.net <= 0 {
                        continue;
                    }
                    let exists = match prefix.get(pk) {
                        Some(FoldOp::Inserted(..)) => true,
                        Some(FoldOp::Deleted) => false,
                        None => fold.merged[pk].committed,
                    };
                    if exists {
                        return Err(disp.cat().pk_violation_err(t.tid, &t.schema, pk, false).into());
                    }
                }
            }
            if n + 1 < fis.len() {
                for (&pk, f) in &fold.families[n] {
                    prefix.insert(pk, f.last);
                }
            }
        }
    }
    Ok(())
}

/// Rule U-SEC, planning half: every (table, unique circuit)'s surviving spans,
/// with a committed-occupancy probe appended to `checks` for each that the warm
/// filter cannot prove absent. Under `WireProbeMode::FirstHolder` an occupied
/// span comes back as `[span ‖ committed holder PK]`, so the answer that
/// decides the verdict rides the same reply that established the occupancy —
/// no interval in which it could go stale.
fn plan_unique_checks<'a>(
    disp: &MasterDispatcher,
    b: &TxnBundle<'a>,
    checks: &mut Vec<PipelinedCheck>,
) -> Result<Vec<UniquePlan<'a>>, WorkerFault> {
    let mut plans: Vec<UniquePlan<'a>> = Vec::new();
    for t in &b.tables {
        let tid = t.tid;
        // Copied out of the catalog in one walk: `disp.cat()` hands out
        // `&mut CatalogEngine` from `&self`, so the borrow must end before the
        // dispatcher calls below. It comes first because a table with no unique
        // index may have no fold to walk at all.
        let uniques: Vec<(PkColList, SchemaDescriptor, IndexKeySpec)> = disp
            .cat()
            .registry()
            .index_circuits(tid)
            .iter()
            .filter(|ic| ic.is_unique)
            .map(|ic| (ic.col_indices, ic.index_schema, ic.key_spec))
            .collect();
        if uniques.is_empty() || b.surviving(tid).next().is_none() {
            continue;
        }
        for (col_indices, idx_schema, spec) in uniques {
            let cols = col_indices.as_slice();
            let stride = idx_schema.pk_stride() as usize;

            // Surviving span → holder PK as a flat arena plus an order vector:
            // 12 bytes per row against the 104 a `(PkBuf, &[u8])` pair costs,
            // and the sort moves 4-byte indices rather than 104-byte tuples.
            let cap = b.overlay(tid).len();
            let mut spans: Vec<u8> = Vec::with_capacity(cap * stride);
            let mut holders: Vec<&'a [u8]> = Vec::with_capacity(cap);
            let mut order: Vec<u32> = Vec::with_capacity(cap);
            let mut keybuf = PkBuf::zeroed(0);
            for (pk, fam, row) in b.surviving(tid) {
                if !spec.key_bytes(b.mem(fam), row as usize, &mut keybuf) {
                    continue; // NULL in an indexed column ⇒ unindexed
                }
                order.push(holders.len() as u32);
                spans.extend_from_slice(keybuf.padded(stride));
                holders.push(pk);
            }
            if order.is_empty() {
                continue;
            }
            // The arena holds each span zero-padded to the index stride, which
            // is the probe key; the span itself is its leading `key_size` bytes,
            // and is what everything below compares.
            let key_size = spec.key_size();
            let probe_key = |i: u32| &spans[i as usize * stride..(i as usize + 1) * stride];
            let span = |i: u32| &probe_key(i)[..key_size];
            // Also the order the check batch is emitted in: the worker probes
            // it with one cursor, and `advance_to` gallops in place only on a
            // strictly greater key. `sort_unstable_by`, not `_by_key`, which
            // re-invokes the extractor per comparison.
            order.sort_unstable_by(|&x, &y| gnitz_store::schema::key::compare_pk_bytes(span(x), span(y)));
            // `surviving` yields each PK once, so an adjacent-equal pair comes
            // from two different rows: this IS the in-bundle duplicate rule. It
            // must fire whatever committed state holds, so it stays above the
            // filter elision below.
            if order.windows(2).any(|w| span(w[0]) == span(w[1])) {
                return Err(disp.cat().unique_violation_err(tid, cols, true).into());
            }

            // Every planned span provably absent ⇒ the probe would answer "none
            // occupied" and leave nothing to verify.
            if disp.unique_filter_all_absent(tid, col_indices, order.iter().map(|&x| span(x))) {
                continue;
            }
            checks.push(PipelinedCheck {
                keyspace: Keyspace::index(cols),
                // The reply must name the committed holder of each occupied
                // span, not echo the probe key back.
                mode: gnitz_wire::WireProbeMode::FirstHolder,
                mode_param: 0,
                batch: build_check_batch_pk_bytes(&idx_schema, order.iter().map(|&x| probe_key(x))),
                schema: wire::WireSchema::encoded(tid, idx_schema),
                reply: None,
            });
            plans.push(UniquePlan {
                tid,
                col_indices,
                spec,
                stride,
                spans,
                holders,
                order,
            });
        }
    }
    Ok(plans)
}

/// Rule U-SEC, verdict half. Each reply entry is an occupied span plus the
/// committed row holding it, `[span ‖ holder PK]`, split apart by index layout
/// alone. A span held on two workers contributes two entries, each verified on
/// its own; the `FxHashSet` collapses a replicated owner's `W` identical ones.
fn unique_verdict(
    disp: &MasterDispatcher,
    b: &TxnBundle<'_>,
    plans: &[UniquePlan<'_>],
    results: &[FxHashSet<PkBuf>],
) -> Result<(), WorkerFault> {
    let mut hspan = PkBuf::zeroed(0);
    for (plan, occupied) in plans.iter().zip(results) {
        for entry in occupied {
            let (span, holder) = plan.spec.split_entry(entry.pk_bytes());
            // Every entry answers a span this plan probed, so the claimer is
            // always present.
            let Ok(i) = plan
                .order
                .binary_search_by(|&x| gnitz_store::schema::key::compare_pk_bytes(plan.span(x), span))
            else {
                continue;
            };
            let claimer = plan.holders[plan.order[i] as usize];
            // The holder IS the surviving row claiming the span — nothing to
            // vacate.
            if holder == claimer {
                continue;
            }
            // Otherwise the bundle must retire it: the holder's surviving state
            // is absent, or it no longer holds this span.
            let retired = match b.overlay(plan.tid).get(holder) {
                None => false,
                Some(e) => match e.last {
                    FoldOp::Deleted => true,
                    FoldOp::Inserted(hf, hr) => {
                        !plan.spec.key_bytes(b.mem(hf), hr as usize, &mut hspan) || hspan.pk_bytes() != span
                    }
                },
            };
            if !retired {
                return Err(disp
                    .cat()
                    .unique_violation_err(plan.tid, plan.col_indices.as_slice(), false)
                    .into());
            }
        }
    }
    Ok(())
}

/// Rule F1, planning half: one committed-occupancy probe per FK constraint
/// whose child is bundled, over the distinct non-NULL FK values of that child's
/// surviving rows.
fn plan_fk_existence(
    disp: &MasterDispatcher,
    b: &TxnBundle<'_>,
    constraints: &[FkEdge],
    checks: &mut Vec<PipelinedCheck>,
) -> Result<Vec<FkProbePlan>, WorkerFault> {
    let mut plans: Vec<FkProbePlan> = Vec::new();
    for &edge in constraints {
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
            let Some(v) = loc.native_key_opt(b.mem(fam), row as usize) else {
                continue;
            };
            if seen.insert(v) {
                values.push(v);
            }
        }
        if values.is_empty() {
            continue;
        }

        // PK fast-path only when the referenced column *is* the parent's lone
        // PK; otherwise probe the parent's UNIQUE index, which the keyspace
        // selector routes by broadcast since index entries are distributed
        // independently of the PK.
        let parent_schema = disp.cat().registry().table_entry(parent_tid)?.schema;
        let src_type = loc.type_code();
        let (key_schema, keyspace) = if parent_schema.is_lone_pk_col(parent_col) {
            (probe_schema(&parent_schema), Keyspace::OwnPk)
        } else {
            let idx_schema = disp
                .cat()
                .registry()
                .index_circuit_for_cols(parent_tid, &[parent_col as u32])
                .map(|ic| ic.index_schema)
                .ok_or_else(|| format!("FK check: no unique index on parent {parent_tid} col {parent_col}"))?;
            (idx_schema, Keyspace::index(&[parent_col as u32]))
        };
        checks.push(PipelinedCheck {
            keyspace,
            mode: gnitz_wire::WireProbeMode::Exists,
            mode_param: 0,
            batch: build_check_batch(&key_schema, &values, src_type),
            schema: wire::WireSchema::encoded(parent_tid, key_schema),
            reply: None,
        });
        plans.push(FkProbePlan { edge, values });
    }
    Ok(plans)
}

/// Rule F1, verdict half: every surviving row's FK value must reference a row
/// that exists after the transaction — present in committed state and not
/// retired by the bundle, or added by it.
fn fk_existence_verdict(
    disp: &MasterDispatcher,
    plans: &[FkProbePlan],
    checks: &[PipelinedCheck],
    results: &[FxHashSet<PkBuf>],
    deltas: &ParentDeltas,
) -> Result<(), WorkerFault> {
    // A non-bundled parent has no delta (the degenerate plain-push case).
    let no_delta: ParentDelta = (FxHashMap::default(), FxHashSet::default());
    for ((plan, check), probed) in plans.iter().zip(checks).zip(results) {
        let (retired, added) = deltas.get(&delta_key(&plan.edge)).unwrap_or(&no_delta);
        for (j, v) in plan.values.iter().enumerate() {
            let in_committed = probed.contains(check.batch.get_pk_bytes(j));
            if (in_committed && !retired.contains_key(v)) || added.contains(v) {
                continue;
            }
            return Err(disp
                .cat()
                .fk_missing_err(plan.edge.child_tid, plan.edge.parent_tid)
                .into());
        }
    }
    Ok(())
}

/// The `(retired, added)` referenced-column value sets rules F1 and F2 turn on,
/// resolved once per `(parent tid, referenced col)` — two FKs onto the same
/// column would otherwise redo the identical walk and gather.
///
/// The gathers ride ONE burst: each is an ordinary probe of the parent's own PK
/// store under `WireProbeMode::Project`, which differs from an existence probe
/// only in the bytes that come back.
async fn resolve_parent_deltas(
    disp: &MasterDispatcher,
    reactor: &crate::runtime::reactor::Reactor,
    b: &TxnBundle<'_>,
    constraints: &[FkEdge],
    children: &[FkEdge],
) -> Result<ParentDeltas, WorkerFault> {
    let mut needed: Vec<(i64, usize)> = constraints
        .iter()
        .filter(|e| b.has(e.parent_tid))
        .chain(children.iter())
        .map(delta_key)
        .collect();
    needed.sort_unstable();
    needed.dedup();
    if needed.is_empty() {
        return Ok(ParentDeltas::default());
    }

    // A PK referenced column needs no gather: the value is already in the key
    // the fold holds.
    let mut checks: Vec<PipelinedCheck> = Vec::new();
    let mut check_of: Vec<Option<usize>> = Vec::with_capacity(needed.len());
    for &(ptid, pcol) in &needed {
        let schema = *b.schema(ptid);
        if schema.is_pk_col(pcol) {
            check_of.push(None);
            continue;
        }
        // Sorted so each worker's sublist reaches its cursor ascending: the
        // scatter preserves per-worker relative order, so one global sort does
        // it.
        let mut keys: Vec<&[u8]> = b.touched_committed(ptid).collect();
        if keys.is_empty() {
            check_of.push(None);
            continue;
        }
        keys.sort_unstable();
        let pk_only = probe_schema(&schema);
        // The constructor the worker's projection uses, so a matching reply
        // validates by construction.
        let reply = gnitz_store::schema::project_schema(&schema, &[pcol as u32])
            .expect("a one-column projection fits MAX_COLUMNS");
        checks.push(PipelinedCheck {
            keyspace: Keyspace::OwnPk,
            mode: gnitz_wire::WireProbeMode::Project,
            mode_param: pcol as u64,
            batch: build_check_batch_pk_bytes(&pk_only, keys.into_iter()),
            schema: wire::WireSchema::encoded(ptid, pk_only),
            reply: Some(reply),
        });
        check_of.push(Some(checks.len() - 1));
    }

    // The reply's one payload column, off the projected schema rather than the
    // parent's — and not column 0, since `project_schema` keeps the PK region
    // ahead of it.
    let locators: Vec<(usize, usize, u8)> = checks
        .iter()
        .map(|c| {
            let expected = c.reply.as_ref().expect("a projecting probe carries its reply schema");
            let projected = SchemaFacts::payload_col_idx(expected, 0);
            let ColumnLocator::Payload { slot, size, type_code } = expected.locate(projected) else {
                unreachable!("a projecting probe answers with one payload column")
            };
            (slot as usize, size as usize, type_code)
        })
        .collect();

    // `pk → promoted index key`, absent when the committed row is absent or
    // holds NULL there — a NULL referenced value is unindexed either way.
    let mut gathered: Vec<FxHashMap<PkBuf, u128>> = (0..checks.len()).map(|_| FxHashMap::default()).collect();
    execute_probe_burst(disp, reactor, &checks, |i, rows| {
        let (slot, size, type_code) = locators[i];
        // Invariant across a frame's rows, where `ColumnLocator`'s own readers
        // re-resolve the window per row through `get_col_ptr`.
        let col_data = rows.col_data(slot, size);
        for j in 0..rows.len() {
            if !gnitz_wire::null_word_get(rows.get_null_word(j), slot) {
                gathered[i].insert(
                    PkBuf::from_bytes(rows.get_pk_bytes(j)),
                    payload_native_key(col_data, j * size, size, type_code),
                );
            }
        }
        Ok(())
    })
    .await?;

    let mut deltas = ParentDeltas::default();
    for (&(ptid, pcol), ci) in needed.iter().zip(check_of) {
        deltas.insert(
            (ptid, pcol),
            parent_retired_added(b, ptid, pcol, ci.map(|c| &gathered[c])),
        );
    }
    Ok(deltas)
}

/// One parent column's `(retired, added)` sets: `added` are the surviving rows'
/// non-NULL values; `retired` are the old committed values of touched PKs whose
/// surviving state is absent or holds something else. Old values come from the
/// packed PK, or from `gathered` for a non-PK column.
///
/// Only a PK that exists committed has an old value to retire, so U-PK's
/// `committed` flag bounds the walk — a pure INSERT empties it.
fn parent_retired_added(
    b: &TxnBundle<'_>,
    parent_tid: i64,
    ref_col: usize,
    gathered: Option<&FxHashMap<PkBuf, u128>>,
) -> ParentDelta {
    let parent_schema = b.schema(parent_tid);
    let loc = parent_schema.locate(ref_col);

    // Old committed value per touched PK (non-NULL only).
    let mut old_of: FxHashMap<&[u8], u128> = FxHashMap::default();
    if let ColumnLocator::Pk { byte_off, size, type_code } = loc {
        let (off, size) = (byte_off as usize, size as usize);
        for p in b.touched_committed(parent_tid) {
            old_of.insert(p, pk_native_key(p, off, size, type_code));
        }
    } else if let Some(g) = gathered {
        for p in b.touched_committed(parent_tid) {
            if let Some(&v) = g.get(p) {
                old_of.insert(p, v);
            }
        }
    }

    let mut added: FxHashSet<u128> = FxHashSet::default();
    let mut retired: FxHashMap<u128, RetireVerb> = FxHashMap::default();
    for (p, e) in b.overlay(parent_tid) {
        let surviving_val: Option<u128> = match e.last {
            FoldOp::Inserted(f, r) => loc.native_key_opt(b.mem(f), r as usize),
            FoldOp::Deleted => None,
        };
        if let Some(sv) = surviving_val {
            added.insert(sv);
        }
        if let Some(&ov) = old_of.get(p) {
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
    spec: IndexKeySpec,
    /// Probed span → the native referenced value it encodes, paired while the
    /// probe batch is built: a reply names a span, the rejection names a value.
    values: FxHashMap<PkBuf, u128>,
    /// The bundle touches this child, so a committed holder it retires is
    /// exempt; for an unbundled child the first committed holder is fatal.
    bundled: bool,
}

/// Rule F2: a referenced value the bundle removes and does not re-add must
/// have no surviving child row referencing it. `exists_after(v)` for
/// `v ∈ retired` reduces to `added.contains(v)`, so the checked set is
/// `retired ∖ added`.
///
/// "Which committed rows hold this value" is a key question, and the child's FK
/// index already stores `[span ‖ holder PK]` entries as its answer — one burst
/// answers it for every value at once. That index is never unique, so a holder
/// list is unbounded by the write; the per-value cap below is what bounds both
/// sides by the write's own row count instead.
///
/// The holders must be enumerated rather than deduced: `retraction_batch` fills
/// a deletion's payload with filler columns, so a DELETE carries no
/// before-image of the FK value it removes.
async fn txn_check_fk_restrict(
    disp: &MasterDispatcher,
    reactor: &crate::runtime::reactor::Reactor,
    b: &TxnBundle<'_>,
    children: &[FkEdge],
    deltas: &ParentDeltas,
) -> Result<(), WorkerFault> {
    let mut plans: Vec<RestrictPlan> = Vec::new();
    let mut checks: Vec<PipelinedCheck> = Vec::new();
    for &edge in children {
        let FkEdge {
            child_tid,
            fk_col,
            parent_tid,
            parent_col,
        } = edge;
        let (retired, added) = &deltas[&delta_key(&edge)];
        let v_check: Vec<u128> = retired.keys().copied().filter(|v| !added.contains(v)).collect();
        if v_check.is_empty() {
            continue;
        }
        let (idx_schema, spec) = disp
            .cat()
            .registry()
            .index_circuit_for_cols(child_tid, &[fk_col as u32])
            .map(|ic| (ic.index_schema, ic.key_spec))
            .ok_or_else(|| format!("FK RESTRICT: no index on child {child_tid} col {fk_col}"))?;
        // `enc_key`, not `IndexKeySpec::seek_prefix`: `v` comes from the
        // PARENT's column while the index belongs to the child, and the index
        // owner's own source type would sign-mangle a negative narrow value.
        let src_type = b.schema(parent_tid).columns[parent_col].type_code;
        let batch = build_check_batch(&idx_schema, &v_check, src_type);
        let key_size = spec.key_size();
        let mut values: FxHashMap<PkBuf, u128> = FxHashMap::default();
        for (j, &v) in v_check.iter().enumerate() {
            values.insert(PkBuf::from_bytes(&batch.get_pk_bytes(j)[..key_size]), v);
        }
        // A value with no committed holder contributes no reply rows in either
        // mode, so both ride the same burst.
        let bundled = b.has(child_tid);
        // `K+1` holders is a violation by pigeonhole: every exempt holder is a
        // key of the child fold, so past `K` distinct ones at least one is not.
        let (mode, cap) = if bundled {
            (
                gnitz_wire::WireProbeMode::AllHolders,
                b.overlay(child_tid).len() as u64 + 1,
            )
        } else {
            (gnitz_wire::WireProbeMode::Exists, 0)
        };
        checks.push(PipelinedCheck {
            keyspace: Keyspace::index(&[fk_col as u32]),
            mode,
            mode_param: cap,
            batch,
            schema: wire::WireSchema::encoded(child_tid, idx_schema),
            reply: None,
        });
        plans.push(RestrictPlan { edge, spec, values, bundled });
    }

    // A streaming fold: the first non-exempt holder aborts the drain, whose
    // `ScanLease` drop discards every train still in flight.
    let mut hspan = PkBuf::zeroed(0);
    execute_probe_burst(disp, reactor, &checks, |i, rows| {
        let plan = &plans[i];
        let retired = &deltas[&delta_key(&plan.edge)].0;
        for j in 0..rows.len() {
            if rows.get_weight(j) != 1 {
                continue;
            }
            let (span, holder) = plan.spec.split_entry(rows.get_pk_bytes(j));
            let Some(&v) = plan.values.get(span) else {
                continue;
            };
            // U-SEC's predicate, on the same bytes: does the bundle's
            // surviving version of this holder still carry this key?
            let exempt = plan.bundled
                && match b.overlay(plan.edge.child_tid).get(holder) {
                    None => false, // untouched committed child still references v
                    Some(e) => match e.last {
                        FoldOp::Deleted => true,
                        FoldOp::Inserted(cf, cr) => {
                            !plan.spec.key_bytes(b.mem(cf), cr as usize, &mut hspan) || hspan.pk_bytes() != span
                        }
                    },
                };
            if exempt {
                continue;
            }
            let verb = restrict_verb(retired, v);
            return Err(disp
                .cat()
                .fk_restrict_err(plan.edge.parent_tid, plan.edge.child_tid, verb)
                .into());
        }
        Ok(())
    })
    .await
}

#[cfg(test)]
#[path = "tests/preflight.rs"]
mod tests;
