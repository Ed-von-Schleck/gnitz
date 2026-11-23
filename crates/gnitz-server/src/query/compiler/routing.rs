//! How each source's delta is routed into a view's circuit, and where a node's
//! rows sit across the workers. Nothing here emits: the emitter derives each
//! node's [`Dist`] as it emits the node.

use std::collections::BTreeMap;

use super::*;
use gnitz_wire::{ReindexSlot, TypeCode};

/// Per-view circuit metadata, derived from one circuit load: every worker routes
/// its own partitions by it, so no two workers can route one round differently.
pub(super) struct ViewMeta<'a> {
    /// Every source is replicated, so every worker computes the view's whole
    /// result from its own full copies.
    pub(super) replicated: bool,
    /// Each worker computes the view's whole result locally: it is replicated, or
    /// there is one worker.
    pub(super) self_contained: bool,
    /// Every scanned source, by id.
    pub(super) sources: BTreeMap<u64, SourceMeta<'a>>,
}

/// One scanned source of a view's circuit.
pub(super) struct SourceMeta<'a> {
    pub(super) relation: &'a Relation,
    /// How a worker relays the delta this source scatters by — a share of it
    /// where the source is replicated, a round otherwise; `None` where it does
    /// not scatter. Derived as beside other workers, whatever the worker count,
    /// so that a key no relay could route by is refused at any.
    pub(super) route: Option<Relay>,
    /// Where its scan's rows sit, as beside other workers.
    pub(super) dist: Dist,
}

/// What the circuit does with one scanned source's delta.
#[derive(Default)]
struct SourceUse<'c> {
    /// The scatter key its scan states: `(source column, slot type)` slots in
    /// the source relation's own column indices; empty for a broadcast.
    key: Option<&'c [ReindexSlot]>,
    /// The scan's rows reach something other than a join operand.
    outside_join: bool,
    /// They cross a `Distinct`.
    set_fed: bool,
    /// They reach a `Join`, so the source must state a scatter key.
    reaches_join: bool,
}

impl<'a> ViewMeta<'a> {
    /// Derive `circuit`'s routing metadata.
    pub(super) fn derive(circuit: &Circuit, registry: &'a RelationRegistry) -> Result<ViewMeta<'a>, String> {
        // One forward pass: what the circuit does with each source's delta, by
        // source in ascending order — so every process reports the same offender.
        let mut uses: BTreeMap<u64, SourceUse> = BTreeMap::new();
        // Whose scan's delta flows into each node, and whether some node it
        // reads, directly or transitively, owes a ground row.
        let mut owner: Vec<Option<u64>> = Vec::with_capacity(circuit.ops().len());
        let mut grounded: Vec<bool> = Vec::with_capacity(circuit.ops().len());
        for (nid, op) in circuit.ops() {
            let inputs = circuit.inputs(nid);
            // `op` hands its input's rows on as that input's source's delta.
            let carries = matches!(
                op,
                OpNode::Filter(_) | OpNode::Map(_) | OpNode::WeightClamp(ClampKind::Distinct)
            );
            for tid in inputs.iter().filter_map(|&p| owner[p]) {
                let use_ = uses.entry(tid).or_default();
                match op {
                    OpNode::Join(_) => use_.reaches_join = true,
                    _ if carries => {}
                    _ => use_.outside_join = true,
                }
            }
            owner.push(match op {
                OpNode::ScanDelta { source, scatter, .. } => {
                    uses.entry(*source).or_default().key = scatter.as_deref();
                    Some(*source)
                }
                _ if carries => owner[inputs[0]],
                _ => None,
            });
            if let (OpNode::WeightClamp(ClampKind::Distinct), Some(tid)) = (op, owner[nid]) {
                uses.entry(tid).or_default().set_fed = true;
            }
            let behind_ground = inputs.iter().any(|&p| grounded[p]);
            // The ground row is minted in an epoch of its own on one worker,
            // entered behind the reduce, which no other worker runs: a round
            // behind it would be skipped, and a share would keep only the
            // minting worker's part. Refused at any worker count, so a view's
            // acceptance never depends on it.
            if op.exchanges() && behind_ground {
                return Err("an exchange behind a global aggregate".into());
            }
            grounded.push(behind_ground || op.owes_ground_row());
        }
        // The view's store integrates the output node's rows.
        if let Some(tid) = owner[circuit.out()] {
            uses.entry(tid).or_default().outside_join = true;
        }

        // Each scan leaves its rows as its store places them unless the loop
        // below relays its delta or finds it routed by its scatter key.
        let mut sources: BTreeMap<u64, SourceMeta> = uses
            .keys()
            .map(|&tid| {
                let relation = registry.relation_or_err(tid)?;
                let dist = Dist::placed(&relation.schema(), relation.placement());
                Ok((tid, SourceMeta { relation, route: None, dist }))
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
            let plan = if key.is_empty() {
                // Every worker already holds the whole delta a broadcast would
                // hand it.
                if replicated {
                    continue;
                }
                ScatterPlan::broadcast()
            } else {
                // Refused whether or not the relay is skipped below.
                let plan = ScatterPlan::join(&source.relation.schema(), key)
                    .map_err(|e| format!("source {tid} scatter key: {e}"))?;
                // Only a keyed relay can skip: a broadcast's matches spread
                // over the whole other side.
                if plan.routes_to_native_owner(source.relation.placement()) {
                    source.dist = Dist::on(key);
                    continue;
                }
                plan
            };
            // Every key is non-empty where the partner makes every match. A
            // clamp over one worker's slice admits the key once per worker.
            if has_replicated_partner && (replicated || !use_.set_fed) {
                continue;
            }
            // A broadcast hands every worker the whole delta; a keyed route, or
            // a replicated source's share, its part.
            source.dist = if key.is_empty() { Dist::Whole } else { Dist::on(key) };
            source.route = Some(Relay { plan: Rc::new(plan), key: key.into() });
        }
        Ok(ViewMeta {
            replicated: all_replicated,
            self_contained,
            sources,
        })
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
    /// Where rows of `schema` sit that a store placed by `placement` holds.
    fn placed(schema: &SchemaDescriptor, placement: Placement) -> Dist {
        match placement {
            Placement::Replicated => Dist::Whole,
            Placement::Local => Dist::Anywhere,
            Placement::Keyed { dist_stride } => (1..=schema.pk_cols().len())
                .find(|&k| schema.pk_prefix_stride(k) == dist_stride as usize)
                .map_or(Dist::Anywhere, |k| Dist::pk(schema, k)),
        }
    }

    /// The placement a store of `schema` registers under whose rows sit by
    /// `self`: the inverse of [`Self::placed`]. A `Whole` store is an identical
    /// full copy on every worker; no key's owner alone holds an `Anywhere` row.
    pub(super) fn placement(&self, schema: &SchemaDescriptor) -> Placement {
        match self {
            Dist::Whole => Placement::Replicated,
            dist => dist
                .pk_prefix(schema)
                .map_or(Placement::Local, |k| Placement::keyed(schema, k)),
        }
    }

    /// Rows sitting by the key `slots` pack.
    fn on(slots: &[ReindexSlot]) -> Dist {
        Dist::On(slots.iter().map(|&(col, ty)| KeySlot { cols: vec![col], ty }).collect())
    }

    /// Rows sitting by the leading `k` PK columns of `schema`, each at its own
    /// type.
    pub(super) fn pk(schema: &SchemaDescriptor, k: usize) -> Dist {
        let cols = schema.columns();
        Dist::On(
            (schema.pk_cols()[..k].iter())
                .map(|&c| KeySlot {
                    cols: vec![c],
                    ty: cols[c as usize].type_code,
                })
                .collect(),
        )
    }

    /// How many leading PK columns of `schema` route a row to the owner `self`
    /// routes it to, where some do: one per slot, each slot holding that PK
    /// column.
    pub(super) fn pk_prefix(&self, schema: &SchemaDescriptor) -> Option<usize> {
        let Dist::On(slots) = self else {
            return None;
        };
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
        let key: Vec<ReindexSlot> = slots.iter().zip(pk).map(|(slot, &c)| (c, slot.ty)).collect();
        let plan = ScatterPlan::join(schema, &key).ok()?;
        plan.routes_to_native_owner(Placement::keyed(schema, slots.len()))
            .then_some(slots.len())
    }

    /// This distribution over a node whose columns `onto(c, ty)` hold the value
    /// its input's column `c` packs at `ty`: a key slot no column holds any more
    /// names no key.
    pub(super) fn mapped<I: IntoIterator<Item = u32>>(&self, onto: impl Fn(u32, TypeCode) -> I) -> Dist {
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
    pub(super) fn common(&self, other: &Dist) -> Result<Dist, String> {
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
    pub(super) fn merged(mut self, other: &Dist) -> Dist {
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

/// The column of `out` holding column `c` of `input`, where `input`'s PK columns
/// and payload each sit at the head of `out`'s region, or at its tail iff `trails`.
pub(super) fn shifted(input: &SchemaDescriptor, out: &SchemaDescriptor, c: u32, trails: bool) -> Option<u32> {
    let (pk_by, payload_by) = match trails {
        false => (0, 0),
        true => (
            out.pk_cols().len() - input.pk_cols().len(),
            out.num_payload_cols() - input.num_payload_cols(),
        ),
    };
    match input.payload_slot(c as usize) {
        None => {
            let j = input.pk_cols().iter().position(|&p| p == c)?;
            out.pk_cols().get(j + pk_by).copied()
        }
        Some(slot) => Some(out.payload_col_idx(slot + payload_by) as u32),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/routing.rs"]
mod tests;
