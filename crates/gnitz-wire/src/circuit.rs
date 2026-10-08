//! Circuit-layer wire definitions: the aggregate discriminants, the typed `OpNode`
//! representation shared between gnitz-core and gnitz-server, the graph they make
//! up, and the codec that lays a whole circuit out as one catalog cell.

use crate::codec::{decode_all, Reader, Wire, Writer};
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
        /// MAP sub-variant: pure projection (column reorder/drop).
        MapProj = 3,
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
    }
}

/// The layout of a `CIRCUIT_TAB` cell ([`Circuit::encode`]), folded into
/// [`crate::SYS_SCHEMA_DIGEST`].
pub(crate) const CIRCUIT_VERSION: u8 = 10;

// ---------------------------------------------------------------------------
// Typed circuit-node representation (shared between gnitz-core and gnitz-server)
// ---------------------------------------------------------------------------

wire_enum! {
    /// Aggregate function discriminant. The values are durable catalog state
    /// (a `Reduce` node's aggregates) and a wire value on the ad-hoc fold path.
    pub enum AggFunc: u8 {
        Count = 1,
        Sum = 2,
        Min = 3,
        Max = 4,
        CountNonNull = 5,
    }
}

impl AggFunc {
    /// True iff `Agg(A + B) == Agg(A) + Agg(B)`: a delta folds with no history
    /// replay, and the value over no rows is `0`.
    pub const fn is_linear(self) -> bool {
        match self {
            AggFunc::Count | AggFunc::CountNonNull | AggFunc::Sum => true,
            AggFunc::Min | AggFunc::Max => false,
        }
    }

    /// The op a reduce folds this aggregate's own output with: linear partials add,
    /// extremes compete.
    pub const fn merge_op(self) -> AggFunc {
        if self.is_linear() {
            AggFunc::Sum
        } else {
            self
        }
    }

    /// True iff a reduce's raw output column for this aggregate can hold NULL.
    pub const fn raw_output_nullable(self, src_nullable: bool, ungrouped: bool) -> bool {
        !self.is_linear() && (src_nullable || ungrouped)
    }
}

/// One aggregate of a reduce: which function, over which column of the reduce's
/// input. Carries no column *type* — that is `schema.columns[col_idx]`, which
/// every consumer already holds. The one spelling on both wire paths: a
/// `Reduce` node's parameters and an ad-hoc fold's `ReadSpec` ship this, and
/// the reduce and the ad-hoc fold consume it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AggDescriptor {
    pub col_idx: u32,
    pub agg_op: AggFunc,
}

impl AggDescriptor {
    /// COUNT(*), which reads no column: column 0 is a placeholder.
    pub const COUNT_STAR: AggDescriptor = AggDescriptor { col_idx: 0, agg_op: AggFunc::Count };
}

/// The function, then its column.
impl Wire for AggDescriptor {
    fn write(&self, w: &mut Writer) {
        w.put(&self.agg_op).u32(self.col_idx);
    }
    fn read(r: &mut Reader) -> Result<Self, String> {
        Ok(AggDescriptor { agg_op: r.get()?, col_idx: r.u32()? })
    }
}

/// A compiled expression program and the payload slots it writes, as
/// `(type_code, nullable)` in output order; the PK region is inherited verbatim.
/// The declarations travel because a computed projection has no copy list the
/// engine could derive a schema from. One type on both wire paths — a circuit's
/// `Map` and a `ReadSpec` fold's pre-map are the same device.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComputeMap {
    pub program: Vec<u8>,
    pub out_cols: Vec<(TypeCode, bool)>,
}

/// The declared payload slots, then the program.
impl Wire for ComputeMap {
    fn write(&self, w: &mut Writer) {
        w.list(&self.out_cols).bytes32(&self.program);
    }
    fn read(r: &mut Reader) -> Result<Self, String> {
        let out_cols = r.list("compute map", MAX_COLUMNS)?;
        Ok(ComputeMap { program: r.bytes32()?.to_vec(), out_cols })
    }
}

/// Output type code of an aggregate over a source column of type `src_tc`, or
/// `None` when that type does not admit it — the one rule both the planner's
/// declared schema and the engine's emitted batch read.
///
/// A sum is typed as its 8-byte register image, so SUM over U64 is U64: the
/// wrapping i64 accumulator's bits are the true sum mod 2^64.
pub const fn agg_output_type(func: AggFunc, src_tc: TypeCode) -> Option<TypeCode> {
    match func {
        AggFunc::Count | AggFunc::CountNonNull => Some(TypeCode::I64),
        // Adding two calendar values, or two truth values, is meaningless.
        AggFunc::Sum => {
            if crate::ScalarKind::from_type_code(src_tc).is_some()
                && !src_tc.is_temporal()
                && !matches!(src_tc, TypeCode::Bool)
            {
                Some(src_tc.register_image())
            } else {
                None
            }
        }
        AggFunc::Min | AggFunc::Max => Some(src_tc),
    }
}

wire_enum! {
    /// The range relation between the two **SQL sides** of a join:
    /// `left_slot REL right_slot`, the ON clause's `a.x OP b.y` verbatim. Each
    /// join instruction resolves it against its own `delta_is_right`. Wire values
    /// are stable.
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

/// How a reduce keys its output. For a `Reduce` node it is derived, never
/// transmitted: both sides call [`Self::for_group_cols`] over facts they already
/// hold — the source PK column list and the GROUP BY column list. The fold sink's
/// reply is keyed the same way.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReduceOutKey {
    /// The group columns are the output PK: the source's whole PK list, or one
    /// non-nullable PK-eligible column.
    Natural,
    /// A leading hidden `_group_pk` U128, a NULL-distinct key of the group
    /// columns, which ride as payload. Every other group set, the empty
    /// (global) one included.
    SyntheticFold,
}

impl ReduceOutKey {
    /// The output key a reduce grouped by `group_cols` warrants, from the facts
    /// both sides already hold: the source's PK column list, the GROUP BY column
    /// list, and a `(type_code, nullable)` reader for a group column.
    pub fn for_group_cols(pk_cols: &[u32], group_cols: &[u32], col: impl Fn(u32) -> (TypeCode, bool)) -> Self {
        let natural = group_cols == pk_cols
            || matches!(*group_cols, [c] if {
                let (type_code, nullable) = col(c);
                // The PK region carries no null bitmap.
                !nullable && type_code.is_pk_eligible()
            });
        if natural {
            ReduceOutKey::Natural
        } else {
            ReduceOutKey::SyntheticFold
        }
    }

    /// An output's layout ahead of its aggregates: the key region (or the synthetic
    /// key), then each `row` column the key region does not spell.
    pub fn output_layout(self, group_cols: &[u32], row: impl IntoIterator<Item = u32>) -> Vec<ReduceOutSlot> {
        let (lead, spelled): (Vec<ReduceOutSlot>, &[u32]) = match self {
            ReduceOutKey::SyntheticFold => (vec![ReduceOutSlot::SyntheticKey], &[]),
            ReduceOutKey::Natural => (group_cols.iter().map(|&c| ReduceOutSlot::Key(c)).collect(), group_cols),
        };
        lead.into_iter()
            .chain(
                row.into_iter()
                    .filter(|c| !spelled.contains(c))
                    .map(ReduceOutSlot::Carried),
            )
            .collect()
    }
}

/// One slot of a reduce's output layout, ahead of the aggregate columns. See
/// [`ReduceOutKey::output_layout`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReduceOutSlot {
    /// The synthetic `U128` fold key: slot 0 of the fold arm, and its whole PK.
    SyntheticKey,
    /// Source column `col`, in the output's PK region.
    Key(u32),
    /// Source column `col`, carried into the payload — a column of the output's
    /// row the key region does not spell.
    Carried(u32),
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
    /// Pure projection/column-reorder. Carries payload column indices to keep.
    Projection(Vec<u32>),
    /// Computed projection (`SELECT a + b`).
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
    /// aggregates each worker's slice and owes none.
    Reduce {
        group_cols: Vec<u32>,
        /// Aggregate specs `(func, source column)`. Carries a `Count`.
        agg: Vec<AggDescriptor>,
    },
    /// `slot 0 ⋈ z⁻¹(I(slot 1))`, written `[key, left payload…, right payload…]`:
    /// `delta_is_right` names the SQL side slot 0 carries.
    Join {
        kind: JoinKind,
        delta_is_right: bool,
    },
    /// Co-locates its input by the key of what reads it — the group columns of a
    /// `Reduce` or `TopN` reader, else the input's own PK.
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
        order: Vec<crate::OrderKey>,
        limit: u64,
        offset: u64,
    },
}

impl OpNode {
    /// How many input slots this operator is wired on.
    pub const fn arity(&self) -> usize {
        match self {
            // The circuit's own input: fed by the source drive, not by a producer.
            OpNode::ScanDelta { .. } => 0,
            OpNode::Union | OpNode::Join { .. } => 2,
            OpNode::Filter(_)
            | OpNode::Map(_)
            | OpNode::Negate
            | OpNode::WeightClamp(_)
            | OpNode::Reduce { .. }
            | OpNode::ExchangeShard
            | OpNode::NullExtend { .. }
            | OpNode::WorkerFilter
            | OpNode::TopN { .. } => 1,
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
            _ => {}
        }
        Ok(())
    }

    /// The operator's tag, then its fields.
    fn write(&self, w: &mut Writer) {
        match self {
            OpNode::ScanDelta { source, bound } => w.put(&Opcode::ScanDelta).u64(*source).put(bound),
            OpNode::Filter(program) => w.put(&Opcode::Filter).bytes32(program),
            OpNode::Map(MapKind::Projection(cols)) => w.put(&Opcode::MapProj).list(cols),
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
            OpNode::Reduce { group_cols, agg } => w.put(&Opcode::Reduce).list(group_cols).list(agg),
            OpNode::Join { kind, delta_is_right } => match kind {
                JoinKind::Equi => w.put(&Opcode::JoinEqui),
                JoinKind::Range { rel } => w.put(&Opcode::JoinRange).put(rel),
                JoinKind::Cross => w.put(&Opcode::JoinCross),
            }
            .bool(*delta_is_right),
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
        };
    }

    /// [`Self::write`]'s inverse. Framing only: [`Self::check`] is [`Circuit::push`]'s.
    fn read(r: &mut Reader) -> Result<OpNode, String> {
        fn cols(r: &mut Reader) -> Result<Vec<u32>, String> {
            r.list("column list", MAX_COLUMNS)
        }
        fn slots(r: &mut Reader) -> Result<Vec<ReindexSlot>, String> {
            r.list("slot list", MAX_COLUMNS)
        }
        Ok(match r.get::<Opcode>()? {
            Opcode::ScanDelta => OpNode::ScanDelta { source: r.u64()?, bound: r.get()? },
            Opcode::Filter => OpNode::Filter(r.bytes32()?.to_vec()),
            Opcode::MapProj => OpNode::Map(MapKind::Projection(cols(r)?)),
            Opcode::MapExpr => OpNode::Map(MapKind::Compute(r.get()?)),
            Opcode::MapReindex => {
                let nulls = r.get()?;
                let key = slots(r)?;
                let keep = cols(r)?;
                let role = match r.bool()? {
                    false => ReindexRole::Auxiliary,
                    true => ReindexRole::ScatterKey { source_cols: cols(r)? },
                };
                OpNode::Map(MapKind::Reindex { keep, key, role, nulls })
            }
            Opcode::MapHashRow => OpNode::Map(MapKind::HashRow { cols: slots(r)? }),
            Opcode::Negate => OpNode::Negate,
            Opcode::Union => OpNode::Union,
            Opcode::Distinct => OpNode::WeightClamp(ClampKind::Distinct),
            Opcode::PositivePart => OpNode::WeightClamp(ClampKind::PositivePart),
            Opcode::Reduce => {
                let group_cols = cols(r)?;
                let agg = r.list("aggregate list", MAX_COLUMNS)?;
                OpNode::Reduce { group_cols, agg }
            }
            Opcode::JoinEqui => OpNode::Join {
                kind: JoinKind::Equi,
                delta_is_right: r.bool()?,
            },
            Opcode::JoinRange => OpNode::Join {
                kind: JoinKind::Range { rel: r.get()? },
                delta_is_right: r.bool()?,
            },
            Opcode::JoinCross => OpNode::Join {
                kind: JoinKind::Cross,
                delta_is_right: r.bool()?,
            },
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
                let group_cols = cols(r)?;
                let order = r.list("order keys", crate::MAX_ORDER_KEYS)?;
                OpNode::TopN { group_cols, order, limit, offset }
            }
        })
    }
}

// ---------------------------------------------------------------------------
// The circuit graph
// ---------------------------------------------------------------------------

pub type NodeId = usize;

/// The most nodes one circuit may hold: the count [`Circuit::decode`] refuses
/// past, and what bounds the engine's `u16` register and child-store ids.
pub const MAX_CIRCUIT_NODES: usize = 16_384;

const ARITY_MISMATCH: &str = "node's inputs do not match its operator's arity";
const EARLIER_NODE: &str = "a node's input is not an earlier node";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Node {
    pub op: OpNode,
    /// Slot 0 first; a slot past `op.arity()` is unused and zero.
    inputs: [NodeId; 2],
}

impl Node {
    /// The producers, in slot order.
    pub fn inputs(&self) -> &[NodeId] {
        &self.inputs[..self.op.arity()]
    }
}

/// A node's id is its index and every input names an earlier node: [`Self::push`]
/// is the only constructor, so index order is a topological order. Its last node
/// is its output.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Circuit {
    nodes: Vec<Node>,
}

impl Circuit {
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    /// Append `op` wired on `inputs`, refusing an operator that holds under no
    /// schema, an input count other than its [`OpNode::arity`], and an input that
    /// is not an earlier node.
    pub fn push(&mut self, op: OpNode, inputs: &[NodeId]) -> Result<NodeId, String> {
        op.check()?;
        if op.arity() != inputs.len() {
            return Err(ARITY_MISMATCH.into());
        }
        if inputs.iter().any(|&p| p >= self.nodes.len()) {
            return Err(EARLIER_NODE.into());
        }
        let mut slots = [0; 2];
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

    /// [`Self::encode`]'s inverse, through [`Self::push`]: a decoded circuit is
    /// held to everything a built one is, and has an output.
    pub fn decode(buf: &[u8]) -> Result<Circuit, String> {
        decode_all(buf, "circuit", |r| {
            let mut circuit = Circuit::default();
            for _ in 0..r.count("nodes", MAX_CIRCUIT_NODES)? {
                let op = OpNode::read(r)?;
                let mut inputs = [0; 2];
                let inputs = &mut inputs[..op.arity()];
                for slot in inputs.iter_mut() {
                    *slot = r.u16()? as NodeId;
                }
                circuit.push(op, inputs)?;
            }
            if circuit.nodes.is_empty() {
                return Err("a circuit has no nodes".into());
            }
            Ok(circuit)
        })
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

    /// [`MapKind::Projection`].
    pub fn map(&mut self, input: NodeId, projection: &[u32]) -> NodeId {
        self.add(OpNode::Map(MapKind::Projection(projection.to_vec())), &[input])
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

    /// [`OpNode::Join`]: `delta` probes the integral of `integrand`, as it stood
    /// before this epoch. `delta_is_right` names the SQL side the delta carries.
    pub fn join(&mut self, delta: NodeId, integrand: NodeId, kind: JoinKind, delta_is_right: bool) -> NodeId {
        self.add(OpNode::Join { kind, delta_is_right }, &[delta, integrand])
    }

    /// `ΔA ⋈ z⁻¹I(B) + ΔB ⋈ z⁻¹I(A)`, both terms in side order `[key, A, B]`:
    /// `da` joins the integral of `ib`, `db` that of `ia`.
    pub fn join_terms(&mut self, [da, db]: [NodeId; 2], [ia, ib]: [NodeId; 2], kind: JoinKind) -> NodeId {
        let ab = self.join(da, ib, kind, false);
        let ba = self.join(db, ia, kind, true);
        self.union(ab, ba)
    }

    /// [`OpNode::WorkerFilter`].
    pub fn worker_filter(&mut self, input: NodeId) -> NodeId {
        self.add(OpNode::WorkerFilter, &[input])
    }

    /// [`OpNode::Reduce`] behind an [`OpNode::ExchangeShard`] on its group columns.
    /// Over no group columns it owes a row over an empty input.
    pub fn reduce_multi(&mut self, input: NodeId, group_cols: &[u32], agg_specs: &[AggDescriptor]) -> NodeId {
        let sharded = self.shard(input);
        self.reduce(sharded, group_cols, agg_specs)
    }

    /// [`OpNode::Reduce`] with **no upstream exchange**: it aggregates `input` as
    /// each worker holds it, and emits nothing over an empty input.
    pub fn reduce_multi_local(&mut self, input: NodeId, group_cols: &[u32], agg_specs: &[AggDescriptor]) -> NodeId {
        self.reduce(input, group_cols, agg_specs)
    }

    fn reduce(&mut self, input: NodeId, group_cols: &[u32], agg: &[AggDescriptor]) -> NodeId {
        let op = OpNode::Reduce {
            group_cols: group_cols.to_vec(),
            agg: agg.to_vec(),
        };
        self.add(op, &[input])
    }

    /// [`OpNode::TopN`] behind an [`OpNode::ExchangeShard`] on its group columns.
    pub fn top_n(
        &mut self,
        input: NodeId,
        group_cols: &[u32],
        order: &[crate::OrderKey],
        limit: u64,
        offset: u64,
    ) -> NodeId {
        let sharded = self.shard(input);
        let op = OpNode::TopN {
            group_cols: group_cols.to_vec(),
            order: order.to_vec(),
            limit,
            offset,
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
