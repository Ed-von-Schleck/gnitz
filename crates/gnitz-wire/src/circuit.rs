//! The circuit graph a view is compiled from: the typed `OpNode` a planner builds
//! and the engine compiles, the graph they make up, and the codec that lays a
//! whole circuit out as one catalog cell.

use crate::codec::{decode_all, Reader, Writer};
use crate::operator::{col_list, order_keys, ComputeMap, GroupAggs, OrderKey};
use crate::{TypeCode, MAX_COLUMNS};

// ---------------------------------------------------------------------------
// Circuit opcodes
// ---------------------------------------------------------------------------

wire_enum! {
    /// The layout tag each operator leads with in [`Circuit::encode`]. `0` names no
    /// operator.
    enum Opcode: u8 {
        ScanDelta = 1,
        Filter = 2,
        /// MAP sub-variant: expression program (compute), inheriting the input PK.
        MapExpr = 4,
        /// MAP sub-variant: keep the listed columns as payload, set PK = hash of
        /// those columns' bytes.
        MapHashRow = 5,
        /// MAP sub-variant: re-key onto a synthetic PK built from the named source
        /// columns, keeping the named columns as payload.
        MapReindex = 6,
        Negate = 7,
        Union = 8,
        Distinct = 9,
        PositivePart = 10,
        Reduce = 11,
        /// Symmetric delta-trace join, equal-key seek probe.
        JoinEqui = 12,
        /// Symmetric delta-trace join, ordered half-open range walk over the trace.
        JoinRange = 13,
        /// Symmetric delta-trace join, full cross product.
        JoinCross = 14,
        ExchangeShard = 16,
        NullExtend = 17,
        WorkerFilter = 18,
        /// Per-group top-N: the rows filling the first `limit` weight slots past
        /// `offset` of each group in ORDER BY order.
        TopN = 19,
        /// Per-group running values: every row of a group, widened by its
        /// values over the rows at or before it in ORDER BY order.
        Window = 20,
    }
}

/// The layout of a `CIRCUIT_TAB` cell ([`Circuit::encode`]), folded into
/// [`crate::SYS_SCHEMA_DIGEST`].
pub(crate) const CIRCUIT_VERSION: u8 = 15;

// ---------------------------------------------------------------------------
// Typed circuit-node representation
// ---------------------------------------------------------------------------

wire_enum! {
    /// The range relation between the two **SQL sides** of a join:
    /// `left_slot REL right_slot`, the ON clause's `a.x OP b.y` verbatim. Wire
    /// values are stable.
    pub enum RangeRel: u8 {
        Lt = 0,
        Le = 1,
        Gt = 2,
        Ge = 3,
    }
}

impl RangeRel {
    /// The order-reversing converse: `x OP y` ⟺ `y OP.converse() x`.
    pub fn converse(self) -> RangeRel {
        match self {
            RangeRel::Lt => RangeRel::Gt,
            RangeRel::Le => RangeRel::Ge,
            RangeRel::Gt => RangeRel::Lt,
            RangeRel::Ge => RangeRel::Le,
        }
    }

    /// Whether `x REL v` bounds `x` from below (`>`, `>=`).
    pub fn bounds_below(self) -> bool {
        matches!(self, RangeRel::Gt | RangeRel::Ge)
    }

    /// Whether `x REL v` admits `v` itself (`>=`, `<=`).
    pub fn admits_equal(self) -> bool {
        matches!(self, RangeRel::Ge | RangeRel::Le)
    }
}

/// Join physical strategy carried by `OpNode::Join`, one variant per join
/// opcode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinKind {
    Equi,
    /// Non-equi (range) join: the probe is an ordered half-open range walk over
    /// the trace instead of an equal-key seek. Every key column but the last is
    /// matched for equality; `rel` relates the last.
    Range {
        rel: RangeRel,
    },
    /// Keyless (cross) join: the probe pairs every delta row with every trace
    /// row.
    Cross,
}

/// Which weight clamp an [`OpNode::WeightClamp`] is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClampKind {
    /// Set membership: `[0, 1]`.
    Distinct,
    /// Bag multiplicity: `[0, i64::MAX]`.
    PositivePart,
}

/// Whether a reindex `Map` states the route of the source it reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReindexRole {
    /// A re-key that states no route.
    Auxiliary,
    /// The columns, in the source whose scan the node reads, of the leading slots
    /// of the Map's `key` that source's delta is scattered by — each at its key
    /// slot's type; none broadcasts it.
    ScatterKey { source_cols: Vec<u32> },
}

wire_enum! {
    /// What a reindex does with a row NULL in a key column. SQL 3VL: a NULL join key
    /// matches nothing, so a join's key re-key drops the row; a re-key whose NULL-keyed
    /// rows must survive (an outer side's unmatched minuend, a null-fill) keeps it.
    pub enum NullKeys: u8 {
        Keep = 0,
        Drop = 1,
    }
}

/// One slot of a reindex or hash-row list: a source column, and the type the
/// map gives it.
pub type ReindexSlot = (u32, TypeCode);

/// MAP sub-variant discriminant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MapKind {
    /// Projection by an expression program (`SELECT a + b`); a pure column copy
    /// is the instruction-free program.
    Compute(ComputeMap),
    /// Re-key onto a synthetic PK: `key` is the source columns in key order, each
    /// with the type its key slot packs at, and `keep` the columns surviving as
    /// payload behind them, in output order.
    Reindex {
        keep: Vec<u32>,
        key: Vec<ReindexSlot>,
        role: ReindexRole,
        nulls: NullKeys,
    },
    /// A projection of `cols`, each at its output column's type, keyed by a hash
    /// of the row's content instead of the source PK.
    HashRow { cols: Vec<ReindexSlot> },
}

/// Typed operator-node payload. Expression blobs are stored as raw `Vec<u8>` and decoded
/// with `gnitz_expr::LogicalProgram::from_blob`, which validates the program the
/// framing here only frames.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpNode {
    /// Delta input for `source`, whose backfill scan `bound` narrows.
    ScanDelta {
        source: u64,
        bound: crate::ReadBound,
    },
    /// Expression predicate blob. "No `WHERE`" is spelled by emitting no node at
    /// all, so no absent cell can decode to a silent `WHERE TRUE`.
    Filter(Vec<u8>),
    Map(MapKind),
    Negate,
    Union,
    WeightClamp(ClampKind),
    /// Over no group columns, behind an `ExchangeShard` it aggregates the whole
    /// relation and owes one row over an empty input; with no exchange it
    /// aggregates each worker's slice and owes none. Carries a `Count`; a
    /// reduce without one is refused.
    Reduce(GroupAggs),
    /// The bilinear join of SQL sides A and B over slots `[ΔA, ΔB, iA, iB]`:
    /// `ΔA ⋈ z⁻¹(I(iB)) + ΔB ⋈ z⁻¹(I(iA))`, written `[key, A payload…, B
    /// payload…]`. A side's integrand is its delta, or the slice of it a worker
    /// keeps.
    Join(JoinKind),
    /// Co-locates its input by the key of what reads it — the group columns of a
    /// `Reduce`, `TopN` or `Window` reader, else the input's own PK.
    ExchangeShard,
    /// Widen every row with NULL columns of `type_codes`, ahead of the input's
    /// payload under `nulls_first` and behind it otherwise — the side order an
    /// outer join's null-fill needs.
    NullExtend {
        type_codes: Vec<TypeCode>,
        nulls_first: bool,
    },
    /// Keep only the rows whose PK this worker owns.
    WorkerFilter,
    /// Per-group top-N: for each `group_cols` value, the rows filling weight slots
    /// `offset .. offset + limit` of the group in `order`, each at the weight of
    /// the slots it fills. Keyed like a `Reduce` over the same group set.
    TopN {
        group_cols: Vec<u32>,
        order: Vec<OrderKey>,
        limit: u64,
        offset: u64,
    },
    /// Per-group running values: every row of each `group_cols` value at its own
    /// weight, widened by its values over the rows of the group at or before its
    /// peers in `order` — the rows equal to it on every order key. The values are
    /// `rank`, one more than the weight of the rows before the row's peers; then
    /// `dense_rank`, the number of peer groups up to and including the row's;
    /// then each of `aggs` over those rows, the peers included. Keyed like a
    /// `Reduce` over the same group set.
    Window {
        group: GroupAggs,
        order: Vec<OrderKey>,
        rank: bool,
        dense_rank: bool,
    },
}

impl OpNode {
    /// The columns of its input this operator groups by, for one that groups:
    /// the key an [`OpNode::ExchangeShard`] in front of it co-locates by.
    pub fn group_cols(&self) -> Option<&[u32]> {
        match self {
            OpNode::Reduce(GroupAggs { group_cols, .. })
            | OpNode::TopN { group_cols, .. }
            | OpNode::Window { group: GroupAggs { group_cols, .. }, .. } => Some(group_cols),
            OpNode::ScanDelta { .. }
            | OpNode::Filter(_)
            | OpNode::Map(_)
            | OpNode::Negate
            | OpNode::Union
            | OpNode::WeightClamp(_)
            | OpNode::Join(_)
            | OpNode::ExchangeShard
            | OpNode::NullExtend { .. }
            | OpNode::WorkerFilter => None,
        }
    }

    /// How many input slots this operator is wired on.
    const fn arity(&self) -> usize {
        match self {
            // The circuit's own input: fed by the source drive, not by a producer.
            OpNode::ScanDelta { .. } => 0,
            OpNode::Join(_) => 4,
            OpNode::Union => 2,
            OpNode::Filter(_)
            | OpNode::Map(_)
            | OpNode::Negate
            | OpNode::WeightClamp(_)
            | OpNode::Reduce(_)
            | OpNode::ExchangeShard
            | OpNode::NullExtend { .. }
            | OpNode::WorkerFilter
            | OpNode::TopN { .. }
            | OpNode::Window { .. } => 1,
        }
    }

    /// What holds of an operator under every schema.
    fn check(&self) -> Result<(), String> {
        match self {
            OpNode::Map(MapKind::Reindex { key, role, .. }) => {
                if key.is_empty() {
                    return Err("a reindex names no key columns".into());
                }
                if let ReindexRole::ScatterKey { source_cols } = role {
                    if source_cols.len() > key.len() {
                        return Err("a scatter key is longer than its reindex key".into());
                    }
                }
            }
            // An empty list hashes no bytes, collapsing every row onto one PK.
            OpNode::Map(MapKind::HashRow { cols }) if cols.is_empty() => {
                return Err("a hash-row map names no columns".into());
            }
            // The engine reads whether a group exists from its count.
            OpNode::Reduce(g) if !g.aggs.iter().any(|d| d.agg_op == crate::AggFunc::Count) => {
                return Err("a reduce carries no COUNT(*)".into());
            }
            _ => {}
        }
        Ok(())
    }

    /// The operator's tag, then its fields.
    fn write(&self, w: &mut Writer) {
        match self {
            OpNode::ScanDelta { source, bound } => w.put(&Opcode::ScanDelta).u64(*source).put(bound),
            OpNode::Filter(program) => w.put(&Opcode::Filter).bytes32(program),
            OpNode::Map(MapKind::Compute(map)) => w.put(&Opcode::MapExpr).put(map),
            OpNode::Map(MapKind::Reindex { keep, key, role, nulls }) => {
                w.put(&Opcode::MapReindex).put(nulls).list(key).list(keep);
                match role {
                    ReindexRole::Auxiliary => w.bool(false),
                    ReindexRole::ScatterKey { source_cols } => w.bool(true).list(source_cols),
                }
            }
            OpNode::Map(MapKind::HashRow { cols }) => w.put(&Opcode::MapHashRow).list(cols),
            OpNode::Negate => w.put(&Opcode::Negate),
            OpNode::Union => w.put(&Opcode::Union),
            OpNode::WeightClamp(ClampKind::Distinct) => w.put(&Opcode::Distinct),
            OpNode::WeightClamp(ClampKind::PositivePart) => w.put(&Opcode::PositivePart),
            OpNode::Reduce(g) => w.put(&Opcode::Reduce).put(g),
            OpNode::Join(kind) => match kind {
                JoinKind::Equi => w.put(&Opcode::JoinEqui),
                JoinKind::Range { rel } => w.put(&Opcode::JoinRange).put(rel),
                JoinKind::Cross => w.put(&Opcode::JoinCross),
            },
            OpNode::ExchangeShard => w.put(&Opcode::ExchangeShard),
            OpNode::NullExtend { type_codes, nulls_first } => {
                w.put(&Opcode::NullExtend).bool(*nulls_first).list(type_codes)
            }
            OpNode::WorkerFilter => w.put(&Opcode::WorkerFilter),
            OpNode::TopN { group_cols, order, limit, offset } => w
                .put(&Opcode::TopN)
                .u64(*limit)
                .u64(*offset)
                .list(group_cols)
                .list(order),
            OpNode::Window { group, order, rank, dense_rank } => w
                .put(&Opcode::Window)
                .bool(*rank)
                .bool(*dense_rank)
                .put(group)
                .list(order),
        };
    }

    /// [`Self::write`]'s inverse. Framing only: [`Self::check`] is [`Circuit::push`]'s.
    fn read(r: &mut Reader) -> Result<OpNode, String> {
        fn slots(r: &mut Reader) -> Result<Vec<ReindexSlot>, String> {
            r.list("slot list", MAX_COLUMNS)
        }
        Ok(match r.get::<Opcode>()? {
            Opcode::ScanDelta => OpNode::ScanDelta { source: r.u64()?, bound: r.get()? },
            Opcode::Filter => OpNode::Filter(r.bytes32()?.to_vec()),
            Opcode::MapExpr => OpNode::Map(MapKind::Compute(r.get()?)),
            Opcode::MapReindex => {
                let nulls = r.get()?;
                let key = slots(r)?;
                let keep = col_list(r)?;
                let role = match r.bool()? {
                    false => ReindexRole::Auxiliary,
                    true => ReindexRole::ScatterKey { source_cols: col_list(r)? },
                };
                OpNode::Map(MapKind::Reindex { keep, key, role, nulls })
            }
            Opcode::MapHashRow => OpNode::Map(MapKind::HashRow { cols: slots(r)? }),
            Opcode::Negate => OpNode::Negate,
            Opcode::Union => OpNode::Union,
            Opcode::Distinct => OpNode::WeightClamp(ClampKind::Distinct),
            Opcode::PositivePart => OpNode::WeightClamp(ClampKind::PositivePart),
            Opcode::Reduce => OpNode::Reduce(r.get()?),
            Opcode::JoinEqui => OpNode::Join(JoinKind::Equi),
            Opcode::JoinRange => OpNode::Join(JoinKind::Range { rel: r.get()? }),
            Opcode::JoinCross => OpNode::Join(JoinKind::Cross),
            Opcode::ExchangeShard => OpNode::ExchangeShard,
            Opcode::NullExtend => {
                let nulls_first = r.bool()?;
                let type_codes = r.list("type list", MAX_COLUMNS)?;
                OpNode::NullExtend { type_codes, nulls_first }
            }
            Opcode::WorkerFilter => OpNode::WorkerFilter,
            Opcode::TopN => {
                let limit = r.u64()?;
                let offset = r.u64()?;
                let group_cols = col_list(r)?;
                let order = order_keys(r)?;
                OpNode::TopN { group_cols, order, limit, offset }
            }
            Opcode::Window => {
                let (rank, dense_rank) = (r.bool()?, r.bool()?);
                let group = r.get()?;
                let order = order_keys(r)?;
                OpNode::Window { group, order, rank, dense_rank }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// The circuit graph
// ---------------------------------------------------------------------------

pub type NodeId = usize;

/// The most nodes one circuit may hold: the count [`Circuit::decode`] refuses
/// past.
const MAX_CIRCUIT_NODES: usize = 16_384;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Node {
    op: OpNode,
    /// Slot 0 first; a slot past `op.arity()` is unused and zero.
    inputs: [NodeId; 4],
}

impl Node {
    /// The producers, in slot order.
    fn inputs(&self) -> &[NodeId] {
        &self.inputs[..self.op.arity()]
    }
}

/// A node's id is its index and every input names an earlier node: every builder
/// method and [`Self::decode`] append through one private `push`, so index order is
/// a topological order. Its last node is its output.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Circuit {
    nodes: Vec<Node>,
}

impl Circuit {
    /// Append `op` wired on `inputs`, refusing an operator that holds under no
    /// schema, an input count other than its [`OpNode::arity`], and an input that
    /// is not an earlier node.
    fn push(&mut self, op: OpNode, inputs: &[NodeId]) -> Result<NodeId, String> {
        op.check()?;
        if op.arity() != inputs.len() {
            return Err("node's inputs do not match its operator's arity".into());
        }
        if inputs.iter().any(|&p| p >= self.nodes.len()) {
            return Err("a node's input is not an earlier node".into());
        }
        let mut slots = [0; 4];
        slots[..inputs.len()].copy_from_slice(inputs);
        self.nodes.push(Node { op, inputs: slots });
        Ok(self.nodes.len() - 1)
    }

    /// The `CIRCUIT_TAB` cell: the node count, then each node as its opcode, its
    /// fields, and one id per input.
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.count(self.nodes.len());
        for node in &self.nodes {
            node.op.write(&mut w);
            for &input in node.inputs() {
                w.u16(input as u16);
            }
        }
        w.into_vec()
    }

    /// [`Self::encode`]'s inverse, appending through the builders' `push`: a decoded
    /// circuit is held to everything a built one is, and has an output.
    pub fn decode(buf: &[u8]) -> Result<Circuit, String> {
        decode_all(buf, "circuit", |r| {
            let mut circuit = Circuit::default();
            r.items("nodes", MAX_CIRCUIT_NODES, |r| {
                let op = OpNode::read(r)?;
                let mut inputs = [0; 4];
                let inputs = &mut inputs[..op.arity()];
                for slot in inputs.iter_mut() {
                    *slot = r.u16()? as NodeId;
                }
                circuit.push(op, inputs).map(drop)
            })?;
            if circuit.nodes.is_empty() {
                return Err("a circuit has no nodes".into());
            }
            Ok(circuit)
        })
    }

    /// The node whose rows are the circuit's. Panics on a circuit of no nodes, which
    /// [`Self::decode`] refuses.
    pub fn out(&self) -> NodeId {
        self.nodes.len() - 1
    }

    pub fn op(&self, nid: NodeId) -> &OpNode {
        &self.nodes[nid].op
    }

    /// `nid`'s producers, in slot order.
    pub fn inputs(&self, nid: NodeId) -> &[NodeId] {
        self.nodes[nid].inputs()
    }

    /// Every operator, in topological order.
    pub fn ops(&self) -> impl ExactSizeIterator<Item = (NodeId, &OpNode)> {
        self.nodes.iter().map(|n| &n.op).enumerate()
    }

    /// Every node reading `nid`, in topological order.
    pub fn readers(&self, nid: NodeId) -> impl Iterator<Item = NodeId> + '_ {
        (nid + 1..self.nodes.len()).filter(move |&n| self.inputs(n).contains(&nid))
    }

    /// `nid` is a reduce that owes a row over an empty input: one over no group
    /// columns reading an `ExchangeShard`.
    pub fn owes_ground_row(&self, nid: NodeId) -> bool {
        matches!(self.op(nid), OpNode::Reduce(g) if g.group_cols.is_empty())
            && matches!(self.op(self.inputs(nid)[0]), OpNode::ExchangeShard)
    }

    /// Every relation the circuit scans, once per scan.
    pub fn sources(&self) -> impl Iterator<Item = u64> + '_ {
        self.nodes.iter().filter_map(|n| match n.op {
            OpNode::ScanDelta { source, .. } => Some(source),
            _ => None,
        })
    }

    /// Every relation id the circuit names, for the client's segment substitution.
    pub fn sources_mut(&mut self) -> impl Iterator<Item = &mut u64> {
        self.nodes.iter_mut().filter_map(|n| match &mut n.op {
            OpNode::ScanDelta { source, .. } => Some(source),
            _ => None,
        })
    }

    fn add(&mut self, op: OpNode, inputs: &[NodeId]) -> NodeId {
        self.push(op, inputs)
            .expect("a builder wires a well-formed operator on its arity to earlier nodes")
    }

    /// A source's delta input. `bound` narrows only the source's backfill scan,
    /// never the rows the view holds, so the caller still emits the full `Filter`.
    pub fn input_delta(&mut self, source: u64, bound: crate::ReadBound) -> NodeId {
        self.add(OpNode::ScanDelta { source, bound }, &[])
    }

    /// [`OpNode::Filter`] over an encoded predicate program.
    pub fn filter(&mut self, input: NodeId, program: Vec<u8>) -> NodeId {
        self.add(OpNode::Filter(program), &[input])
    }

    /// [`MapKind::Compute`].
    pub fn map_expr(&mut self, input: NodeId, map: ComputeMap) -> NodeId {
        self.add(OpNode::Map(MapKind::Compute(map)), &[input])
    }

    /// [`MapKind::Reindex`].
    pub fn map_reindex(
        &mut self,
        input: NodeId,
        key: &[ReindexSlot],
        keep: &[u32],
        role: ReindexRole,
        nulls: NullKeys,
    ) -> NodeId {
        let op = OpNode::Map(MapKind::Reindex {
            keep: keep.to_vec(),
            key: key.to_vec(),
            role,
            nulls,
        });
        self.add(op, &[input])
    }

    /// [`MapKind::HashRow`] behind an [`OpNode::ExchangeShard`] on the hash. The key
    /// is computed in-circuit, so nothing upstream can have partitioned by it; the
    /// exchange puts equal rows on one worker.
    pub fn map_hash_row(&mut self, input: NodeId, cols: &[ReindexSlot]) -> NodeId {
        let map = self.add(OpNode::Map(MapKind::HashRow { cols: cols.to_vec() }), &[input]);
        self.shard(map)
    }

    pub fn negate(&mut self, input: NodeId) -> NodeId {
        self.add(OpNode::Negate, &[input])
    }

    pub fn union(&mut self, a: NodeId, b: NodeId) -> NodeId {
        self.add(OpNode::Union, &[a, b])
    }

    pub fn distinct(&mut self, input: NodeId) -> NodeId {
        self.add(OpNode::WeightClamp(ClampKind::Distinct), &[input])
    }

    /// The Z-set difference `minuend − subtrahend`.
    pub fn difference(&mut self, minuend: NodeId, subtrahend: NodeId) -> NodeId {
        let neg = self.negate(subtrahend);
        // The operand nothing else reads goes first, where a union can take it.
        self.union(neg, minuend)
    }

    /// The weight-exact clamped difference `positive_part(minuend − subtrahend)`:
    /// [`Self::difference`] → [`OpNode::WeightClamp`] of [`ClampKind::PositivePart`].
    pub fn positive_diff(&mut self, minuend: NodeId, subtrahend: NodeId) -> NodeId {
        let diff = self.difference(minuend, subtrahend);
        self.add(OpNode::WeightClamp(ClampKind::PositivePart), &[diff])
    }

    /// [`OpNode::Join`]: `da` joins the integral of `ib`, `db` that of `ia`, each
    /// as it stood before this epoch.
    pub fn join(&mut self, [da, db]: [NodeId; 2], [ia, ib]: [NodeId; 2], kind: JoinKind) -> NodeId {
        self.add(OpNode::Join(kind), &[da, db, ia, ib])
    }

    /// [`OpNode::WorkerFilter`].
    pub fn worker_filter(&mut self, input: NodeId) -> NodeId {
        self.add(OpNode::WorkerFilter, &[input])
    }

    /// [`OpNode::Reduce`] behind an [`OpNode::ExchangeShard`] on its group columns.
    /// Over no group columns it owes a row over an empty input.
    pub fn reduce(&mut self, input: NodeId, agg: GroupAggs) -> NodeId {
        let sharded = self.shard(input);
        self.reduce_local(sharded, agg)
    }

    /// [`OpNode::Reduce`] with **no upstream exchange**: it aggregates `input` as
    /// each worker holds it, and emits nothing over an empty input.
    pub fn reduce_local(&mut self, input: NodeId, agg: GroupAggs) -> NodeId {
        self.add(OpNode::Reduce(agg), &[input])
    }

    /// [`OpNode::TopN`] behind an [`OpNode::ExchangeShard`] on its group columns.
    pub fn top_n(&mut self, input: NodeId, group_cols: &[u32], order: &[OrderKey], limit: u64, offset: u64) -> NodeId {
        let sharded = self.shard(input);
        let op = OpNode::TopN {
            group_cols: group_cols.to_vec(),
            order: order.to_vec(),
            limit,
            offset,
        };
        self.add(op, &[sharded])
    }

    /// [`OpNode::Window`] behind an [`OpNode::ExchangeShard`] on its group columns.
    pub fn window(
        &mut self,
        input: NodeId,
        group: GroupAggs,
        order: &[OrderKey],
        (rank, dense_rank): (bool, bool),
    ) -> NodeId {
        let sharded = self.shard(input);
        let op = OpNode::Window {
            group,
            order: order.to_vec(),
            rank,
            dense_rank,
        };
        self.add(op, &[sharded])
    }

    /// [`OpNode::ExchangeShard`].
    pub fn shard(&mut self, input: NodeId) -> NodeId {
        self.add(OpNode::ExchangeShard, &[input])
    }

    /// [`OpNode::NullExtend`]. `nulls_first` places the NULL columns ahead of the
    /// input's payload.
    pub fn null_extend(&mut self, input: NodeId, type_codes: &[TypeCode], nulls_first: bool) -> NodeId {
        let op = OpNode::NullExtend {
            type_codes: type_codes.to_vec(),
            nulls_first,
        };
        self.add(op, &[input])
    }
}

#[cfg(test)]
#[path = "tests/circuit.rs"]
mod tests;
