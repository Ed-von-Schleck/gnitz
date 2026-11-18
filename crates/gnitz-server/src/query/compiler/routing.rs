//! How each source's delta is routed into a view's circuit, where each node's
//! rows sit across the workers, and the placement the view's store registers
//! under. Nothing here emits: the emitter hands [`node_dist`] each node's output
//! schema as it emits the node.

use std::collections::BTreeMap;

use super::*;
use gnitz_wire::TypeCode;

/// Per-view circuit metadata, derived from one circuit load: every worker routes
/// its own partitions by it, so no two workers can route one round differently.
pub(super) struct ViewMeta<'a> {
    /// Every source is replicated, so every worker computes the view's whole
    /// result from its own full copies.
    replicated: bool,
    /// Each worker computes the view's whole result locally: it is replicated, or
    /// there is one worker.
    pub(super) self_contained: bool,
    /// Every scanned source, by id.
    sources: BTreeMap<u64, SourceMeta<'a>>,
}

/// One scanned source of a view's circuit.
struct SourceMeta<'a> {
    relation: &'a Relation,
    /// How a worker relays the delta this source scatters by — a share of it
    /// where the source is replicated, a round otherwise; `None` where it does
    /// not scatter. Derived as beside other workers, whatever the worker count,
    /// so that a key no relay could route by is refused at any.
    route: Option<Relay>,
    /// Where its scan's rows sit, as beside other workers.
    dist: Dist,
}

impl<'a> ViewMeta<'a> {
    /// Derive `circuit`'s routing metadata.
    pub(super) fn derive(circuit: &Circuit, registry: &'a RelationRegistry) -> Result<ViewMeta<'a>, String> {
        let uses = source_uses(circuit);
        // Each scan leaves its rows as its store places them unless the loop
        // below relays its delta or finds it routed by its scatter key.
        let mut sources: BTreeMap<u64, SourceMeta> = uses
            .keys()
            .map(|&tid| {
                let relation = registry.relation_or_err(tid)?;
                Ok((
                    tid,
                    SourceMeta {
                        relation,
                        route: None,
                        dist: native_dist(relation),
                    },
                ))
            })
            .collect::<Result<_, String>>()?;
        if let Some((tid, _)) = uses.iter().find(|(_, u)| u.reaches_join && u.key.is_none()) {
            return Err(format!("source {tid} feeds a join and states no scatter key"));
        }
        let all_replicated = sources.values().all(|s| s.relation.placement().is_replicated());
        let self_contained = all_replicated || registry.slot().of <= 1;

        let replicated = |tid: u64| sources[&tid].relation.placement().is_replicated();
        let keyed = || uses.iter().filter_map(|(&tid, u)| Some((tid, u, u.key?)));
        // A replicated partner holds every row on every worker, so each match is
        // made once, on the other side's own worker — unless that side's delta is
        // broadcast, or the circuit also reads the replicated delta outside its
        // join terms, which counts it once per worker.
        let has_replicated_partner = keyed().any(|(tid, ..)| replicated(tid))
            && keyed().all(|(tid, u, key)| !key.is_empty() && (!replicated(tid) || !u.outside_join));

        for (tid, use_, key) in keyed() {
            let source = sources.get_mut(&tid).expect("a scanned source");
            let replicated = source.relation.placement().is_replicated();
            let partner_makes_every_match = has_replicated_partner
                // A clamp over one worker's slice admits the key once per worker.
                && (replicated || !use_.set_fed);
            let plan = match key.is_empty() {
                // Every worker already holds the whole delta a broadcast would
                // hand it.
                true if replicated => continue,
                true => ScatterPlan::broadcast(),
                false => {
                    // Refused whether or not the relay is skipped below.
                    let plan = ScatterPlan::join(&source.relation.schema(), key)
                        .map_err(|e| format!("source {tid} scatter key: {e}"))?;
                    // Only a keyed relay can skip: a broadcast's matches spread
                    // over the whole other side.
                    if plan.routes_to_native_owner(source.relation.placement()) {
                        source.dist = Dist::on(key);
                        continue;
                    }
                    if partner_makes_every_match {
                        continue;
                    }
                    plan
                }
            };
            // A broadcast hands every worker the whole delta; a keyed route, or
            // a replicated source's share, its part.
            source.dist = if key.is_empty() { Dist::Whole } else { Dist::on(key) };
            source.route = Some(Relay { plan: Rc::new(plan), key: key.into() });
        }
        refuse_exchange_behind_ground(circuit)?;
        Ok(ViewMeta {
            replicated: all_replicated,
            self_contained,
            sources,
        })
    }

    /// The scanned source `source_id`. Panics unless the circuit scans it.
    pub(super) fn source(&self, source_id: u64) -> &'a Relation {
        self.sources[&source_id].relation
    }

    /// Beside other workers, `source_id`'s delta would not enter the view where
    /// the source's store places it — whatever the worker count.
    pub(super) fn states_route(&self, source_id: u64) -> bool {
        self.sources.get(&source_id).is_some_and(|s| s.route.is_some())
    }

    /// How a worker relays `source_id`'s delta into this view — two views
    /// relaying one source by equal keys relay it by equal plans. `None` when
    /// that source does not scatter: its rows are already where the view needs
    /// them, and a route would name a key they were never stored under, or every
    /// worker computes the view's whole result.
    pub(super) fn source_route(&self, source_id: u64) -> Option<&Relay> {
        self.sources
            .get(&source_id)?
            .route
            .as_ref()
            .filter(|_| !self.self_contained)
    }

    /// Where the rows of `source_id`'s scan sit beside other workers. Panics
    /// unless the circuit scans it.
    pub(super) fn scan_dist(&self, source_id: u64) -> Dist {
        self.sources[&source_id].dist.clone()
    }
}

/// Where a scan of `source` leaves its rows when nothing relays its delta:
/// where its store places them.
fn native_dist(source: &Relation) -> Dist {
    let schema = source.schema();
    match source.placement() {
        Placement::Replicated => Dist::Whole,
        Placement::Local => Dist::Anywhere,
        Placement::Keyed { dist_stride } => {
            match (1..=schema.pk_cols().len()).find(|&k| schema.pk_prefix_stride(k) == dist_stride as usize) {
                Some(k) => Dist::on(&pk_slots(&schema, k)),
                None => Dist::Anywhere,
            }
        }
    }
}

/// Refuses an exchange whose placement no worker count could serve, so a view's
/// acceptance never depends on the worker count.
fn refuse_exchange_behind_ground(circuit: &Circuit) -> Result<(), String> {
    // Some node it reads, directly or transitively, owes a ground row.
    let mut grounded: Vec<bool> = Vec::with_capacity(circuit.ops().len());
    for (nid, op) in circuit.ops() {
        let behind_ground = circuit.inputs(nid).iter().any(|&p| grounded[p]);
        // The ground row is minted in an epoch of its own on one worker,
        // entered behind the reduce, which no other worker runs: a round
        // behind it would be skipped, and a share would keep only the
        // minting worker's part.
        if op.exchanges() && behind_ground {
            return Err("an exchange behind a global aggregate".into());
        }
        grounded.push(behind_ground || op.owes_ground_row());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Row distribution
// ---------------------------------------------------------------------------

/// One slot of a key rows sit by: the type it packs at, and every column of
/// the node holding the value it packs.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct KeySlot {
    /// Ascending, never empty.
    cols: Vec<u32>,
    ty: TypeCode,
}

impl KeySlot {
    fn of((col, ty): gnitz_wire::ReindexSlot) -> KeySlot {
        KeySlot { cols: vec![col], ty }
    }
}

/// Where a node's rows sit across the workers, beside other workers.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum Dist {
    /// Every worker computes every row.
    Whole,
    /// Each row sits on the owner of the key these slots pack to, as a join's
    /// scatter key packs it.
    On(Vec<KeySlot>),
    /// Each row sits on one worker, which no key of it names.
    Anywhere,
}

impl Dist {
    /// The placement a view registers under whose circuit's output node, of
    /// schema `out`, leaves its rows by `self`. `out`'s PK region is the
    /// view's. A `Whole` output is an identical full copy on every worker.
    pub(super) fn placement(&self, out: &SchemaDescriptor) -> Placement {
        match self {
            Dist::Whole => Placement::Replicated,
            Dist::On(slots) => pk_prefix_routed(out, slots).map_or(Placement::Local, |k| Placement::keyed(out, k)),
            // No key's owner alone holds each row.
            Dist::Anywhere => Placement::Local,
        }
    }

    fn on(slots: &[gnitz_wire::ReindexSlot]) -> Dist {
        Dist::On(slots.iter().copied().map(KeySlot::of).collect())
    }

    /// This distribution over a node whose columns `onto(c, ty)` hold the value
    /// its input's column `c` packs at `ty`: a key slot no column holds any more
    /// names no key.
    fn mapped(&self, onto: impl Fn(u32, TypeCode) -> Vec<u32>) -> Dist {
        let Dist::On(slots) = self else {
            return self.clone();
        };
        let mut out = Vec::with_capacity(slots.len());
        for slot in slots {
            let mut cols: Vec<u32> = slot.cols.iter().flat_map(|&c| onto(c, slot.ty)).collect();
            cols.sort_unstable();
            cols.dedup();
            if cols.is_empty() {
                return Dist::Anywhere;
            }
            out.push(KeySlot { cols, ty: slot.ty });
        }
        Dist::On(out)
    }

    /// Where the rows of a node reading `self`'s and `other`'s, laid out alike,
    /// sit: a key both sit by, in the columns both hold it in. Refuses rows
    /// every worker computes beside rows one worker holds, which a read
    /// gathering every worker's rows would count once per worker.
    fn common(&self, other: &Dist) -> Result<Dist, String> {
        Ok(match (self, other) {
            (Dist::On(a), Dist::On(b)) if a.len() == b.len() => {
                let mut slots = Vec::with_capacity(a.len());
                for (a, b) in a.iter().zip(b) {
                    let cols: Vec<u32> = a.cols.iter().copied().filter(|c| b.cols.contains(c)).collect();
                    if a.ty != b.ty || cols.is_empty() {
                        return Ok(Dist::Anywhere);
                    }
                    slots.push(KeySlot { cols, ty: a.ty });
                }
                Dist::On(slots)
            }
            (a, b) if a == b => a.clone(),
            (Dist::Whole, _) | (_, Dist::Whole) => {
                return Err("rows every worker computes beside rows one worker holds".into());
            }
            _ => Dist::Anywhere,
        })
    }

    /// The union of `self` and `other`'s columns, slot by slot, for two keys
    /// that pack the same bytes.
    fn merged(mut self, other: &Dist) -> Dist {
        if let (Dist::On(a), Dist::On(b)) = (&mut self, other) {
            for (a, b) in a.iter_mut().zip(b).filter(|(a, b)| a.ty == b.ty) {
                a.cols.extend(&b.cols);
                a.cols.sort_unstable();
                a.cols.dedup();
            }
        }
        self
    }
}

/// The leading `k` PK columns of `schema`, each at its own type.
fn pk_slots(schema: &SchemaDescriptor, k: usize) -> Vec<gnitz_wire::ReindexSlot> {
    let cols = schema.columns();
    schema.pk_cols()[..k]
        .iter()
        .map(|&c| (c, cols[c as usize].type_code))
        .collect()
}

/// How many leading PK columns of `schema` route a row to the owner `slots`
/// route it to, where some do: one per slot, each slot holding that PK column.
pub(super) fn pk_prefix_routed(schema: &SchemaDescriptor, slots: &[KeySlot]) -> Option<usize> {
    let (pk, cols) = (schema.pk_cols(), schema.columns());
    if slots.is_empty() || slots.len() > pk.len() || !slots.iter().zip(pk).all(|(slot, c)| slot.cols.contains(c)) {
        return None;
    }
    // A PK column packed at its own type packs its PK region's bytes.
    if slots
        .iter()
        .zip(pk)
        .all(|(slot, &c)| cols[c as usize].type_code == slot.ty)
    {
        return Some(slots.len());
    }
    // At another type, it packs them where the packed key is read in place.
    let key: Vec<gnitz_wire::ReindexSlot> = slots.iter().zip(pk).map(|(slot, &c)| (c, slot.ty)).collect();
    let plan = ScatterPlan::join(schema, &key).ok()?;
    plan.routes_to_native_owner(Placement::keyed(schema, slots.len()))
        .then_some(slots.len())
}

/// The column of `out` holding PK column `c` of `input`, where `out`'s PK
/// region is `input`'s, shifted `by` columns.
fn pk_col_onto(input: &SchemaDescriptor, out: &SchemaDescriptor, c: u32, by: usize) -> Option<u32> {
    let j = input.pk_cols().iter().position(|&p| p == c)?;
    out.pk_cols().get(j + by).copied()
}

/// The columns of `out` holding column `c` of `input`, where `out` holds
/// `input`'s PK columns `pk_by` columns into its PK region and its payload
/// `payload_by` slots into its payload.
fn shifted(input: &SchemaDescriptor, out: &SchemaDescriptor, c: u32, pk_by: usize, payload_by: usize) -> Vec<u32> {
    match input.payload_slot(c as usize) {
        None => pk_col_onto(input, out, c, pk_by).into_iter().collect(),
        Some(slot) => vec![out.payload_col_idx(slot + payload_by) as u32],
    }
}

/// Where node `nid`'s rows sit, given where its inputs' do; `schema` answers
/// each node's output schema, `nid`'s own included. Refuses an operator that
/// would read rows of one element, or of one join key, spread over several
/// workers.
pub(super) fn node_dist(
    circuit: &Circuit,
    nid: NodeId,
    meta: &ViewMeta,
    dists: &[Dist],
    schema: impl Fn(NodeId) -> SchemaDescriptor,
) -> Result<Dist, String> {
    // Every worker computes every node from its own full copies: nothing
    // relays or exchanges.
    if meta.replicated {
        return Ok(Dist::Whole);
    }
    let inputs = circuit.inputs(nid);
    let out = schema(nid);
    let unary = || &dists[inputs[0]];
    Ok(match circuit.op(nid) {
        OpNode::ScanDelta { source, .. } => meta.scan_dist(*source),
        // Every row is handed on, or dropped, on its worker and under its columns.
        OpNode::Filter(_) | OpNode::Negate => unary().clone(),
        // A set admits an element once, so one whose weight is split over
        // workers would be admitted once per part. A positive part over rows
        // no key co-locates is how an outer join's null-fill cancels per worker.
        OpNode::WeightClamp(ClampKind::Distinct) if *unary() == Dist::Anywhere => {
            return Err("a distinct over rows no key co-locates".into());
        }
        OpNode::WeightClamp(_) => unary().clone(),
        OpNode::Map(mk) => {
            let input = schema(inputs[0]);
            let plan = MapPlan::from_wire(&input, mk)?;
            // Elided: it reproduces its input.
            if plan.is_identity() {
                return Ok(unary().clone());
            }
            unary().mapped(|c, ty| {
                let src = input.locate(c as usize);
                let own = input.columns()[c as usize].type_code;
                // A column copied at another type no longer holds the bytes it did.
                let mut cols: Vec<u32> = (plan.copies().iter())
                    .filter(|cc| cc.src == src && cc.width == src.size())
                    .map(|cc| out.payload_col_idx(cc.slot))
                    .filter(|&at| out.columns()[at].type_code == own)
                    .map(|at| at as u32)
                    .collect();
                if plan.inherits_pk() {
                    cols.extend(pk_col_onto(&input, &out, c, 0));
                }
                // A join key packs each slot on its own, so a key slot packing `c`
                // at `ty` holds those bytes.
                if let MapKind::Reindex { key, .. } = mk {
                    let pk = out.pk_cols();
                    cols.extend(
                        (key.iter().enumerate())
                            .filter(|&(i, &slot)| slot == (c, ty) && out.columns()[pk[i] as usize].type_code == ty)
                            .map(|(i, _)| pk[i]),
                    );
                }
                cols
            })
        }
        OpNode::NullExtend { type_codes, nulls_first } => {
            let input = schema(inputs[0]);
            let ahead = if *nulls_first { type_codes.len() } else { 0 };
            unary().mapped(|c, _| shifted(&input, &out, c, 0, ahead))
        }
        OpNode::Union => dists[inputs[0]].common(&dists[inputs[1]])?,
        OpNode::Join(kind) => join_dist(*kind, inputs, dists, &schema, &out)?,
        // An exchange leaves each row on the owner of the key it scatters by,
        // the PK its operator emits, wherever its input sat.
        OpNode::ExchangeShard | OpNode::Reduce { local: false, .. } | OpNode::TopN { .. } | OpNode::Window { .. } => {
            Dist::on(&pk_slots(&out, out.pk_cols().len()))
        }
        // A local reduce aggregates each worker's slice as it holds it.
        OpNode::Reduce { local: true, .. } => match unary() {
            Dist::Whole => Dist::Whole,
            _ => Dist::Anywhere,
        },
    })
}

/// Where a join of `kind` over `[ΔA, ΔB, iA, iB]` leaves its rows, `out` its
/// output schema: where each term leaves them, where both agree. Refuses a
/// term whose two operands are each spread over the workers by other than the
/// key's leading columns the join matches on equal.
fn join_dist(
    kind: JoinKind,
    inputs: &[NodeId],
    dists: &[Dist],
    schema: &impl Fn(NodeId) -> SchemaDescriptor,
    out: &SchemaDescriptor,
) -> Result<Dist, String> {
    let &[da, db, ia, ib] = inputs else {
        unreachable!("a join is wired on four inputs")
    };
    let left_payload = schema(da).num_payload_cols();
    // A cross join's pair key is A's PK region, then B's.
    let right_pk = match kind {
        JoinKind::Cross => schema(da).pk_cols().len(),
        _ => 0,
    };
    // The output columns holding column `c` of side `side`, B iff `right`.
    let onto = |side: NodeId, right: bool| {
        let input = schema(side);
        let (pk_by, payload_by) = if right { (right_pk, left_payload) } else { (0, 0) };
        move |c: u32, _| shifted(&input, out, c, pk_by, payload_by)
    };
    // How many leading key columns a side's rows sit by.
    let routed_by = |side: NodeId| match &dists[side] {
        Dist::On(slots) => pk_prefix_routed(&schema(side), slots),
        _ => None,
    };
    let term = |delta: NodeId, integrand: NodeId, right: bool| -> Result<Dist, String> {
        let (d, i) = (
            dists[delta].mapped(onto(delta, right)),
            dists[integrand].mapped(onto(integrand, !right)),
        );
        Ok(match (&dists[delta], &dists[integrand]) {
            (Dist::Whole, Dist::Whole) => Dist::Whole,
            // Every worker meets the whole of one side with its part of the other.
            (Dist::Whole, _) => i,
            (_, Dist::Whole) => d,
            _ => {
                let key_cols = schema(delta).pk_cols().len();
                let matched = match kind {
                    JoinKind::Equi => key_cols,
                    JoinKind::Range { .. } => key_cols - 1,
                    JoinKind::Cross => 0,
                };
                match (routed_by(delta), routed_by(integrand)) {
                    // Matched rows share the key bytes the output leads with.
                    (Some(a), Some(b)) if a == b && a <= matched => {
                        let lead = |dist: Dist| match dist {
                            Dist::On(mut slots) => {
                                slots.truncate(a);
                                Dist::On(slots)
                            }
                            dist => dist,
                        };
                        Dist::on(&pk_slots(out, a)).merged(&lead(d)).merged(&lead(i))
                    }
                    _ => return Err("join: a term's operands are not co-located by its key".into()),
                }
            }
        })
    };
    let ab = term(da, ib, false)?;
    let ba = term(db, ia, true)?;
    ab.common(&ba)
}

// ---------------------------------------------------------------------------
// Circuit walks
// ---------------------------------------------------------------------------

/// What the circuit does with one scanned source's delta.
#[derive(Default)]
struct SourceUse<'c> {
    /// The scatter key its scan states: `(source column, slot type)` slots in
    /// the source relation's own column indices; empty for a broadcast.
    key: Option<&'c [gnitz_wire::ReindexSlot]>,
    /// The scan's rows reach something other than a join operand.
    outside_join: bool,
    /// They cross a `Distinct`.
    set_fed: bool,
    /// They reach a `Join`, so the source must state a scatter key.
    reaches_join: bool,
}

/// One forward pass over the circuit: what it does with each source's delta, by
/// source in ascending order — so every process reports the same offender.
fn source_uses(circuit: &Circuit) -> BTreeMap<u64, SourceUse<'_>> {
    let mut uses: BTreeMap<u64, SourceUse<'_>> = BTreeMap::new();
    // Whose scan's delta flows into each node, propagated along the way.
    let mut owner: Vec<Option<u64>> = vec![None; circuit.ops().len()];

    for (nid, op) in circuit.ops() {
        for &p in circuit.inputs(nid) {
            if let Some(tid) = owner[p] {
                note_read(uses.entry(tid).or_default(), Some(op));
            }
        }
        if propagates(op) {
            owner[nid] = owner[circuit.inputs(nid)[0]];
        }
        match op {
            OpNode::ScanDelta { source, scatter, .. } => {
                uses.entry(*source).or_default().key = scatter.as_deref();
                owner[nid] = Some(*source);
            }
            OpNode::WeightClamp(ClampKind::Distinct) => {
                if let Some(tid) = owner[nid] {
                    uses.entry(tid).or_default().set_fed = true;
                }
            }
            _ => {}
        }
    }
    if let Some(tid) = owner[circuit.out()] {
        note_read(uses.entry(tid).or_default(), None);
    }
    uses
}

/// `op` hands its input's rows on as that input's source's delta.
fn propagates(op: &OpNode) -> bool {
    matches!(
        op,
        OpNode::Filter(_) | OpNode::Map(_) | OpNode::WeightClamp(ClampKind::Distinct)
    )
}

/// Record in `use_` that `reader` — `None` for the view's output — reads a
/// node carrying the source's delta.
fn note_read(use_: &mut SourceUse<'_>, reader: Option<&OpNode>) {
    match reader {
        Some(OpNode::Join(_)) => use_.reaches_join = true,
        Some(op) if propagates(op) => {}
        _ => use_.outside_join = true,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/routing.rs"]
mod tests;
