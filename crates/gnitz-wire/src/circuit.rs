//! Circuit-layer wire definitions: the operator opcode space, aggregate
//! discriminants, and the typed `OpNode` representation shared between
//! gnitz-core and gnitz-server — plus the `params` codec that carries each
//! node's per-opcode parameters as one blob.

use crate::codec::{decode_all, Reader, Writer};
use crate::TypeCode;

// ---------------------------------------------------------------------------
// Circuit opcodes
// ---------------------------------------------------------------------------

wire_enum! {
    /// The operator space. A wire enum so [`encode_op_node`] and
    /// [`decode_op_node`] match exhaustively. `0` names no operator, so a zeroed
    /// `CIRCUIT_NODES` opcode cell fails [`Opcode::from_wire`] rather than
    /// decoding as one.
    pub enum Opcode: u64 {
        /// Delta input source for a base table. The table id lives in the node
        /// row's `source_table` column.
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
        /// columns, keeping the named columns as payload. A separate opcode rather
        /// than a `MapExpr` carrying an empty key list: the two share no parameter
        /// shape — a compute map has a program blob and declared output columns, a
        /// reindex two column lists and no program.
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
        IntegrateSink = 15,
        IntegrateTrace = 16,
        ExchangeShard = 17,
        NullExtend = 18,
        WorkerFilter = 19,
        /// Per-group top-N: the rows filling the first `limit` weight slots past
        /// `offset` of each group in ORDER BY order.
        TopN = 20,
    }
}

/// The `params` blob layout, folded into [`crate::SYS_SCHEMA_DIGEST`] rather
/// than written into the blob: the digest is what refuses a stored
/// `CIRCUIT_NODES` blob decoded under a new layout.
pub(crate) const CIRCUIT_PARAMS_VERSION: u8 = 8;

// ---------------------------------------------------------------------------
// Typed circuit-node representation (shared between gnitz-core and gnitz-server)
// ---------------------------------------------------------------------------

wire_enum! {
    /// Aggregate function discriminant. The values are durable catalog state
    /// (a `Reduce` node's `params`) and a wire value on the ad-hoc fold path.
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

/// Output type code of an aggregate over a source column of type `src_tc`, or
/// `None` when that type does not admit it — the one rule both the planner's
/// declared schema and the engine's emitted batch read.
///
/// A sum is typed as its 8-byte register image, so SUM over U64 is U64: the
/// wrapping i64 accumulator's bits are the true sum mod 2^64.
pub const fn agg_output_type(func: AggFunc, src_tc: TypeCode) -> Option<TypeCode> {
    match func {
        AggFunc::Count | AggFunc::CountNonNull => Some(TypeCode::I64),
        // Adding two calendar values is meaningless.
        AggFunc::Sum => {
            if crate::ScalarKind::from_type_code(src_tc).is_some() && !src_tc.is_temporal() {
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
/// hold — the source PK column list and the GROUP BY column list. A fold sink's
/// reply is always [`Self::SyntheticFold`], whose 16-byte key the client combine
/// hashes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReduceOutKey {
    /// Leading synthetic `_group_pk` U128 = null-distinct group fold; the
    /// group columns ride as payload. What every group set that is neither
    /// natural kind gets, including the empty (global) group set.
    SyntheticFold,
    /// The group set is the source PK list; the output PK is the source PK
    /// region verbatim.
    SourcePk,
    /// A single non-nullable U64/U128/UUID group column is the output PK
    /// directly.
    SingleNaturalCol,
}

impl ReduceOutKey {
    /// The one precedence chain (eq-PK ▷ single-natural ▷ synthetic), from the
    /// facts both sides already hold: the source's PK column list, the GROUP BY
    /// column list, and a `(type_code, nullable)` reader for a group column.
    pub fn for_group_cols(pk_cols: &[u32], group_cols: &[u32], col: impl Fn(u32) -> (TypeCode, bool)) -> Self {
        let eq_pk = group_cols == pk_cols;
        let single_natural = match *group_cols {
            [c] => {
                let (type_code, nullable) = col(c);
                // A nullable column can never key the output: the PK region
                // carries no null bitmap.
                !nullable && type_code.is_natural_reduce_key()
            }
            _ => false,
        };
        if eq_pk {
            ReduceOutKey::SourcePk
        } else if single_natural {
            ReduceOutKey::SingleNaturalCol
        } else {
            ReduceOutKey::SyntheticFold
        }
    }

    /// An output's layout ahead of its aggregates: the key region (or the synthetic
    /// key), then each `row` column the key region does not spell.
    pub fn output_layout(self, group_cols: &[u32], row: impl IntoIterator<Item = u32>) -> Vec<ReduceOutSlot> {
        let (lead, spelled): (Vec<ReduceOutSlot>, &[u32]) = match self {
            ReduceOutKey::SyntheticFold => (vec![ReduceOutSlot::SyntheticKey], &[]),
            _ => (group_cols.iter().map(|&c| ReduceOutSlot::Key(c)).collect(), group_cols),
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
    /// the trace instead of an equal-key seek.
    Range {
        n_eq: u8,
        rel: RangeRel,
    },
    /// Keyless (cross) join: the probe pairs every delta row with every trace
    /// row.
    Cross,
}

/// What a reindex `Map` re-keys *for*. A scan can fan out into several reindex
/// Maps on different keys, and a re-key of already-routed rows looks the same
/// from the graph, so the planner states which is which where it knows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReindexRole {
    /// A re-key of rows a `ScatterKey` already placed.
    Auxiliary,
    /// The join key of `source`, in `source`'s own column indices, which is
    /// what the master's relay scatters that delta by. The Map's own `key` is
    /// that key in this node's input layout; a `Map` in between moves one and
    /// not the other.
    ///
    /// The engine checks `source_key` is well-formed over `source`, never that
    /// it names the same columns the Map's own `key` does: the correspondence
    /// is a trusted client claim.
    ScatterKey { source: u64, source_key: Vec<ReindexSlot> },
}

const ROLE_AUXILIARY: u8 = 0;
const ROLE_SCATTER_KEY: u8 = 1;

/// One slot of a reindex or hash-row key list: a source column, and the
/// promotion target the planner carried for it. What an absent target means is
/// the field's rule, not this alias's — read [`MapKind::Reindex`] or
/// [`MapKind::HashRow`].
pub type ReindexSlot = (u32, Option<TypeCode>);

/// MAP sub-variant discriminant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MapKind {
    /// Pure projection/column-reorder. Carries payload column indices to keep.
    Projection(Vec<u32>),
    /// Computed projection (`SELECT a + b`). `MapPlan::from_map` validates the
    /// program against the declared slots.
    Compute(ComputeMap),
    /// Re-key onto a synthetic PK for equijoin/group pre-indexing: `key` is the
    /// source columns in key order, each with its slot's promoted type, and
    /// `keep` the columns surviving as payload behind them, in output order.
    ///
    /// `None` means the engine derives the slot type
    /// (`reindex_output_type_code`), so only a *disagreement* with that travels —
    /// otherwise the planner would be a second producer of a derived fact. `key`
    /// is never empty: a map that re-keys nothing is a [`MapKind::Compute`].
    Reindex {
        keep: Vec<u32>,
        key: Vec<ReindexSlot>,
        role: ReindexRole,
    },
    /// A projection of `cols`, each widened to its target (`None` keeps the
    /// source type), keyed by a hash of the row's content instead of the source
    /// PK — so a row's identity is its projected content.
    HashRow { cols: Vec<ReindexSlot> },
}

/// Typed operator-node payload. Expression blobs are stored as raw `Vec<u8>` and decoded
/// with `gnitz_expr::LogicalProgram::from_blob`, which validates the program the
/// framing here only frames.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpNode {
    /// Delta input for `source`.
    ///
    /// `bound` narrows only the backfill scan: the downstream `Filter` stays
    /// authoritative, and the engine opens an index walk `Optional` whatever the
    /// cell says.
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
    Distinct,
    /// Multiplicity-preserving counterpart to `Distinct`: both emit
    /// `clamp(w_new, lo, hi) − clamp(w_old, lo, hi)` per consolidated
    /// (PK, payload), differing only in the preset the engine's `ClampPreset`
    /// resolves. Two opcodes rather than one node carrying `(lo, hi)`, so a
    /// forged circuit cannot name `min > max`.
    PositivePart,
    Reduce {
        group_cols: Vec<u32>,
        /// Aggregate specs `(func, source column)`. Carries a `Count`;
        /// `ReducePlan::from_wire` refuses a list without one.
        agg: Vec<AggDescriptor>,
        /// The user's scalar aggregate, which emits one row over an empty source
        /// (COUNT(*)=0, the rest NULL). Other group-less reduces leave it false.
        global_ground: bool,
    },
    /// The delta-trace join. `delta_is_right` says which SQL side the delta port
    /// carries, so either term of the two-term form writes
    /// `[key, left payload…, right payload…]`.
    Join {
        kind: JoinKind,
        delta_is_right: bool,
    },
    /// Primary INTEGRATE: writes to view storage.
    IntegrateSink,
    /// Accumulates Z-set for a join trace.
    IntegrateTrace,
    ExchangeShard {
        shard_cols: Vec<u32>,
    },
    /// Widen every row with NULL columns of `type_codes`, ahead of the input's
    /// payload under `nulls_first` and behind it otherwise — the side order an
    /// outer join's null-fill needs.
    NullExtend {
        type_codes: Vec<TypeCode>,
        nulls_first: bool,
    },
    /// Keep only rows whose packed-PK partition is owned by this worker (**pure**
    /// range-join broadcast input; a band join scatters by its eq prefix and omits
    /// this node). Worker identity is a compile-time constant, so no payload
    /// travels on the wire.
    WorkerFilter,
    /// Per-group top-N over the input: for each `group_cols` value, the rows
    /// occupying weight slots `offset .. offset + limit` of the group sorted by
    /// `order` (then by the whole row, so the order is total over Z-set
    /// elements), each at the weight of the slots it fills. The output is keyed
    /// like a `Reduce` over the same group set ([`ReduceOutKey`]) and carries
    /// every input column behind that key. `order` columns index the input
    /// schema. `TopNPlan::from_wire` refuses a `limit` of `0`.
    TopN {
        group_cols: Vec<u32>,
        order: Vec<crate::OrderKey>,
        limit: u64,
        offset: u64,
    },
}

impl OpNode {
    /// How many input slots this operator is wired on: slot 0 is a unary
    /// operator's input and a binary one's delta/left operand, slot 1 the
    /// trace/right operand. The sole authority for both the builder and the
    /// loader, hence the wildcard-free match.
    pub const fn arity(&self) -> usize {
        match self {
            // The circuit's own input: fed by the source drive, not by a producer.
            OpNode::ScanDelta { .. } => 0,
            OpNode::Union | OpNode::Join { .. } => 2,
            OpNode::Filter(_)
            | OpNode::Map(_)
            | OpNode::Negate
            | OpNode::Distinct
            | OpNode::PositivePart
            | OpNode::Reduce { .. }
            | OpNode::IntegrateSink
            | OpNode::IntegrateTrace
            | OpNode::ExchangeShard { .. }
            | OpNode::NullExtend { .. }
            | OpNode::WorkerFilter
            | OpNode::TopN { .. } => 1,
        }
    }
}

// ---------------------------------------------------------------------------
// The circuit graph
// ---------------------------------------------------------------------------

pub type NodeId = usize;

/// A node's producers, in the shape its operator's arity allows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeInputs {
    /// A `ScanDelta`: fed by the source drive, not by a producer.
    Source,
    Unary(NodeId),
    /// `a` is slot 0 — a join's delta side, a union's left operand; `b` is slot
    /// 1, the trace / right operand.
    Binary {
        a: NodeId,
        b: NodeId,
    },
}

impl NodeInputs {
    /// The input slots of one `CircuitNodes` row. A filled slot 1 behind an empty
    /// slot 0 is no shape an operator is wired on.
    pub fn from_slots(slots: [Option<u64>; 2]) -> Result<NodeInputs, String> {
        let id = |raw: u64| usize::try_from(raw).map_err(|_| EARLIER_NODE.to_string());
        Ok(match slots {
            [None, None] => NodeInputs::Source,
            [Some(src), None] => NodeInputs::Unary(id(src)?),
            [Some(a), Some(b)] => NodeInputs::Binary { a: id(a)?, b: id(b)? },
            [None, Some(_)] => return Err(ARITY_MISMATCH.to_string()),
        })
    }

    /// [`Self::from_slots`]'s inverse.
    pub(crate) fn to_slots(self) -> [Option<u64>; 2] {
        match self {
            NodeInputs::Source => [None, None],
            NodeInputs::Unary(src) => [Some(src as u64), None],
            NodeInputs::Binary { a, b } => [Some(a as u64), Some(b as u64)],
        }
    }

    /// The producer of a unary operator's operand.
    pub fn unary(&self) -> NodeId {
        match self {
            NodeInputs::Unary(src) => *src,
            _ => unreachable!("a unary operator fills exactly its one input slot"),
        }
    }

    /// The producers of a binary operator's two operands, in port order.
    pub fn binary(&self) -> (NodeId, NodeId) {
        match self {
            NodeInputs::Binary { a, b } => (*a, *b),
            _ => unreachable!("a binary operator is wired on both ports"),
        }
    }

    /// Every producer, for the walks that do not care about the operator.
    pub fn iter(&self) -> impl Iterator<Item = NodeId> {
        match *self {
            NodeInputs::Source => [None, None],
            NodeInputs::Unary(src) => [Some(src), None],
            NodeInputs::Binary { a, b } => [Some(a), Some(b)],
        }
        .into_iter()
        .flatten()
    }
}

const ARITY_MISMATCH: &str = "node's inputs do not match its operator's arity";
const EARLIER_NODE: &str = "a node's input is not an earlier node";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Node {
    pub op: OpNode,
    pub inputs: NodeInputs,
}

/// A node's id is its index and every input names an earlier node: [`Self::push`]
/// is the only constructor, so index order is a topological order and a cycle
/// cannot be expressed. The engine emits instructions in this order, so the
/// order builder methods are called in is the order the VM runs — a register
/// read after the reader that could have moved it costs a copy per epoch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Circuit {
    nodes: Vec<Node>,
}

impl Circuit {
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    /// Append `op` wired on `inputs`, refusing an input count other than the
    /// operator's [`OpNode::arity`] and an input that is not an earlier node.
    pub fn push(&mut self, op: OpNode, inputs: NodeInputs) -> Result<NodeId, String> {
        if op.arity() != inputs.iter().count() {
            return Err(ARITY_MISMATCH.into());
        }
        if inputs.iter().any(|p| p >= self.nodes.len()) {
            return Err(EARLIER_NODE.into());
        }
        self.nodes.push(Node { op, inputs });
        Ok(self.nodes.len() - 1)
    }

    /// Every relation id a node names — a `ScanDelta`'s source and the relation
    /// a reindex states its route over — for the client's segment substitution,
    /// which must move both or route a delta by a relation nothing scans.
    pub fn sources_mut(&mut self) -> impl Iterator<Item = &mut u64> {
        self.nodes.iter_mut().filter_map(|n| match &mut n.op {
            OpNode::ScanDelta { source, .. } => Some(source),
            OpNode::Map(MapKind::Reindex {
                role: ReindexRole::ScatterKey { source, .. },
                ..
            }) => Some(source),
            _ => None,
        })
    }

    fn add(&mut self, op: OpNode, inputs: NodeInputs) -> NodeId {
        self.push(op, inputs)
            .expect("a builder wires each operator on its arity to earlier nodes")
    }

    /// A source's delta input. `bound` narrows only the source's backfill scan,
    /// never the rows the view holds, so the caller still emits the full `Filter`.
    pub fn input_delta(&mut self, source: u64, bound: crate::ReadBound) -> NodeId {
        self.add(OpNode::ScanDelta { source, bound }, NodeInputs::Source)
    }

    /// [`OpNode::Filter`] over an encoded predicate program.
    pub fn filter(&mut self, input: NodeId, program: Vec<u8>) -> NodeId {
        self.add(OpNode::Filter(program), NodeInputs::Unary(input))
    }

    /// [`MapKind::Compute`].
    pub fn map_expr(&mut self, input: NodeId, map: ComputeMap) -> NodeId {
        self.add(OpNode::Map(MapKind::Compute(map)), NodeInputs::Unary(input))
    }

    /// [`MapKind::Reindex`].
    pub fn map_reindex(&mut self, input: NodeId, key: &[ReindexSlot], keep: &[u32], role: ReindexRole) -> NodeId {
        assert!(!key.is_empty(), "a reindex map must name its key columns");
        let op = OpNode::Map(MapKind::Reindex {
            keep: keep.to_vec(),
            key: key.to_vec(),
            role,
        });
        self.add(op, NodeInputs::Unary(input))
    }

    /// [`MapKind::HashRow`] behind an [`OpNode::ExchangeShard`] on the hash. The key
    /// is computed in-circuit, so nothing upstream can have partitioned by it; the
    /// exchange puts equal rows on one worker.
    pub fn map_hash_row(&mut self, input: NodeId, cols: &[ReindexSlot]) -> NodeId {
        let map = self.add(
            OpNode::Map(MapKind::HashRow { cols: cols.to_vec() }),
            NodeInputs::Unary(input),
        );
        self.shard(map, &[0])
    }

    /// [`MapKind::Projection`].
    pub fn map(&mut self, input: NodeId, projection: &[u32]) -> NodeId {
        self.add(
            OpNode::Map(MapKind::Projection(projection.to_vec())),
            NodeInputs::Unary(input),
        )
    }

    pub fn negate(&mut self, input: NodeId) -> NodeId {
        self.add(OpNode::Negate, NodeInputs::Unary(input))
    }

    pub fn union(&mut self, a: NodeId, b: NodeId) -> NodeId {
        self.add(OpNode::Union, NodeInputs::Binary { a, b })
    }

    pub fn distinct(&mut self, input: NodeId) -> NodeId {
        self.add(OpNode::Distinct, NodeInputs::Unary(input))
    }

    /// The Z-set difference `minuend − subtrahend` as `negate` → `union`.
    ///
    /// The operand order is a **cost** contract, not a correctness one: the engine
    /// takes a union's first operand in place where nothing reads it later, and
    /// clones it otherwise. `negate(subtrahend)` is freshly allocated and read
    /// nowhere else, so it earns the take; the `minuend` may be shared, where the
    /// swap would cost a clone every epoch.
    pub fn difference(&mut self, minuend: NodeId, subtrahend: NodeId) -> NodeId {
        let neg = self.negate(subtrahend);
        self.union(neg, minuend)
    }

    /// The weight-exact clamped difference `positive_part(minuend − subtrahend)`:
    /// [`Self::difference`] → [`OpNode::PositivePart`].
    pub fn positive_diff(&mut self, minuend: NodeId, subtrahend: NodeId) -> NodeId {
        let diff = self.difference(minuend, subtrahend);
        self.add(OpNode::PositivePart, NodeInputs::Unary(diff))
    }

    /// [`OpNode::Join`]: `delta` probes the integral `trace`. `delta_is_right`
    /// names the SQL side the delta port carries.
    pub fn join(&mut self, delta: NodeId, trace: NodeId, kind: JoinKind, delta_is_right: bool) -> NodeId {
        self.add(
            OpNode::Join { kind, delta_is_right },
            NodeInputs::Binary { a: delta, b: trace },
        )
    }

    /// `ΔA ⋈ z⁻¹I(B) + ΔB ⋈ z⁻¹I(A)`, both terms in side order `[key, A, B]`.
    pub fn join_terms(&mut self, [da, db]: [NodeId; 2], [ta, tb]: [NodeId; 2], kind: JoinKind) -> NodeId {
        let ab = self.join(da, tb, kind, false);
        let ba = self.join(db, ta, kind, true);
        self.union(ab, ba)
    }

    /// [`OpNode::WorkerFilter`].
    pub fn worker_filter(&mut self, input: NodeId) -> NodeId {
        self.add(OpNode::WorkerFilter, NodeInputs::Unary(input))
    }

    /// [`OpNode::Reduce`] behind an [`OpNode::ExchangeShard`] on its group columns.
    pub fn reduce_multi(
        &mut self,
        input: NodeId,
        group_cols: &[u32],
        agg_specs: &[AggDescriptor],
        global_ground: bool,
    ) -> NodeId {
        let sharded = self.shard(input, group_cols);
        self.reduce_multi_local(sharded, group_cols, agg_specs, global_ground)
    }

    /// [`OpNode::Reduce`] with **no upstream exchange**: it aggregates `input` as
    /// each worker holds it.
    pub fn reduce_multi_local(
        &mut self,
        input: NodeId,
        group_cols: &[u32],
        agg_specs: &[AggDescriptor],
        global_ground: bool,
    ) -> NodeId {
        let op = OpNode::Reduce {
            group_cols: group_cols.to_vec(),
            agg: agg_specs.to_vec(),
            global_ground,
        };
        self.add(op, NodeInputs::Unary(input))
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
        let sharded = self.shard(input, group_cols);
        let op = OpNode::TopN {
            group_cols: group_cols.to_vec(),
            order: order.to_vec(),
            limit,
            offset,
        };
        self.add(op, NodeInputs::Unary(sharded))
    }

    /// [`OpNode::ExchangeShard`].
    pub fn shard(&mut self, input: NodeId, shard_cols: &[u32]) -> NodeId {
        let op = OpNode::ExchangeShard { shard_cols: shard_cols.to_vec() };
        self.add(op, NodeInputs::Unary(input))
    }

    /// [`OpNode::IntegrateTrace`].
    pub fn integrate_trace(&mut self, input: NodeId) -> NodeId {
        self.add(OpNode::IntegrateTrace, NodeInputs::Unary(input))
    }

    /// [`OpNode::NullExtend`]. `nulls_first` places the NULL columns ahead of the
    /// input's payload.
    pub fn null_extend(&mut self, input: NodeId, type_codes: &[TypeCode], nulls_first: bool) -> NodeId {
        let op = OpNode::NullExtend {
            type_codes: type_codes.to_vec(),
            nulls_first,
        };
        self.add(op, NodeInputs::Unary(input))
    }

    /// [`OpNode::IntegrateSink`].
    pub fn sink(&mut self, input: NodeId) -> NodeId {
        self.add(OpNode::IntegrateSink, NodeInputs::Unary(input))
    }
}

// ---------------------------------------------------------------------------
// The `params` codec
// ---------------------------------------------------------------------------
//
// One fixed layout per opcode, through the crate's shared `Writer`/`Reader`. The
// opcode *is* the layout tag, so no field carries a discriminant.
//
// The list codecs below are `pub(crate)` so `read_spec`'s sinks ship the same
// shapes through them, never a second spelling of their widths or domains.

/// Write a counted list's length, saturating: an over-long list encodes a count
/// its reader's cap refuses.
fn write_count(w: &mut Writer, n: usize) {
    w.u16(u16::try_from(n).unwrap_or(u16::MAX));
}

/// Read a counted list's length, refusing one past `cap` before anything sizes a
/// `Vec` off it.
fn read_count(r: &mut Reader, what: &str, cap: usize) -> Result<usize, String> {
    debug_assert!(cap < u16::MAX as usize, "a saturated count must stay refusable");
    let n = r.u16()? as usize;
    if n > cap {
        return Err(format!("{what}: {n} entries exceeds cap {cap}"));
    }
    Ok(n)
}

pub(crate) fn write_cols(w: &mut Writer, cols: &[u32]) {
    write_count(w, cols.len());
    for &c in cols {
        w.u32(c);
    }
}

pub(crate) fn read_cols(r: &mut Reader) -> Result<Vec<u32>, String> {
    let n = read_count(r, "column list", crate::MAX_COLUMNS)?;
    let mut cols = Vec::with_capacity(n);
    for _ in 0..n {
        cols.push(r.u32()?);
    }
    Ok(cols)
}

/// A counted `(source column, promoted target type)` list; `0` on the wire is
/// "no target". `TypeCode::from_wire` is the whole domain check here — which
/// targets a particular node admits is that arm's own business.
fn read_cols_with_tcs(r: &mut Reader) -> Result<Vec<ReindexSlot>, String> {
    let n = read_count(r, "slot list", crate::MAX_COLUMNS)?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let col = r.u32()?;
        let target = match r.u8()? {
            0 => None,
            tc => Some(TypeCode::from_wire(tc).ok_or_else(|| format!("unknown promotion type code {tc}"))?),
        };
        out.push((col, target));
    }
    Ok(out)
}

fn write_cols_with_tcs(w: &mut Writer, slots: &[ReindexSlot]) {
    write_count(w, slots.len());
    for &(col, tc) in slots {
        w.u32(col).u8(tc.map_or(0, TypeCode::as_wire));
    }
}

const ORDER_DESC: u8 = 1 << 0;
const ORDER_NULLS_FIRST: u8 = 1 << 1;

/// One order key's three wire bytes: column, flags.
fn write_order_key(w: &mut Writer, key: &crate::OrderKey) {
    let mut flags = 0u8;
    if key.desc {
        flags |= ORDER_DESC;
    }
    if key.nulls_first {
        flags |= ORDER_NULLS_FIRST;
    }
    w.u16(key.col).u8(flags);
}

/// [`write_order_key`]'s inverse; an unknown flag bit is a refusal.
fn read_order_key(r: &mut Reader) -> Result<crate::OrderKey, String> {
    let col = r.u16()?;
    let flags = r.flags(ORDER_DESC | ORDER_NULLS_FIRST)?;
    Ok(crate::OrderKey {
        col,
        desc: flags & ORDER_DESC != 0,
        nulls_first: flags & ORDER_NULLS_FIRST != 0,
    })
}

/// A counted order-key list, shared by the rows sink and a circuit's `TopN`
/// node, so the two cannot drift.
pub(crate) fn write_order_keys(w: &mut Writer, keys: &[crate::OrderKey]) {
    write_count(w, keys.len());
    for k in keys {
        write_order_key(w, k);
    }
}

pub(crate) fn read_order_keys(r: &mut Reader) -> Result<Vec<crate::OrderKey>, String> {
    let n = read_count(r, "order keys", crate::MAX_ORDER_KEYS)?;
    (0..n).map(|_| read_order_key(r)).collect()
}

pub(crate) fn write_aggs(w: &mut Writer, aggs: &[AggDescriptor]) {
    write_count(w, aggs.len());
    for d in aggs {
        w.u8(d.agg_op.as_wire()).u32(d.col_idx);
    }
}

pub(crate) fn read_aggs(r: &mut Reader) -> Result<Vec<AggDescriptor>, String> {
    let n = read_count(r, "aggregate list", crate::MAX_COLUMNS)?;
    let mut aggs = Vec::with_capacity(n);
    for _ in 0..n {
        let func_byte = r.u8()?;
        let agg_op = AggFunc::from_wire(func_byte).ok_or_else(|| format!("unknown agg func id {func_byte}"))?;
        aggs.push(AggDescriptor { agg_op, col_idx: r.u32()? });
    }
    Ok(aggs)
}

/// The declared payload slots, then the program.
pub(crate) fn write_compute_map(w: &mut Writer, map: &ComputeMap) {
    write_count(w, map.out_cols.len());
    for &(tc, nullable) in &map.out_cols {
        w.u8(tc.as_wire()).bool(nullable);
    }
    w.bytes32(&map.program);
}

fn read_type_code(r: &mut Reader) -> Result<TypeCode, String> {
    let v = r.u8()?;
    TypeCode::from_wire(v).ok_or_else(|| format!("invalid type code {v}"))
}

pub(crate) fn read_compute_map(r: &mut Reader) -> Result<ComputeMap, String> {
    let n = read_count(r, "compute map", crate::MAX_COLUMNS)?;
    let mut out_cols = Vec::with_capacity(n);
    for _ in 0..n {
        let tc = read_type_code(r)?;
        out_cols.push((tc, r.bool()?));
    }
    Ok(ComputeMap { program: r.bytes32()?.to_vec(), out_cols })
}

/// Encode a typed `OpNode` into its `CircuitNodes` row fields — the inverse of
/// [`decode_op_node`]. The expression blob is carried opaquely (each crate
/// encodes it with its own encoder before building the `OpNode`).
pub fn encode_op_node(op: &OpNode) -> (Opcode, Option<u64>, Option<Vec<u8>>) {
    let mut w = Writer::new();
    match op {
        // An unbounded `ScanDelta` carries no params at all: the common shape
        // costs nothing, and "absent" and "empty" stay distinguishable.
        OpNode::ScanDelta { source, bound: crate::ReadBound::None } => (Opcode::ScanDelta, Some(*source), None),
        OpNode::ScanDelta { source, bound } => {
            crate::read_spec::write_read_bound(&mut w, bound);
            (Opcode::ScanDelta, Some(*source), Some(w.into_vec()))
        }
        OpNode::Filter(program) => {
            w.bytes32(program);
            (Opcode::Filter, None, Some(w.into_vec()))
        }
        OpNode::Map(MapKind::Projection(cols)) => {
            write_cols(&mut w, cols);
            (Opcode::MapProj, None, Some(w.into_vec()))
        }
        OpNode::Map(MapKind::Compute(map)) => {
            write_compute_map(&mut w, map);
            (Opcode::MapExpr, None, Some(w.into_vec()))
        }
        OpNode::Map(MapKind::Reindex { keep, key, role }) => {
            match role {
                ReindexRole::Auxiliary => {
                    w.u8(ROLE_AUXILIARY);
                }
                ReindexRole::ScatterKey { source, source_key } => {
                    w.u8(ROLE_SCATTER_KEY).u64(*source);
                    write_cols_with_tcs(&mut w, source_key);
                }
            }
            write_cols_with_tcs(&mut w, key);
            write_cols(&mut w, keep);
            (Opcode::MapReindex, None, Some(w.into_vec()))
        }
        OpNode::Map(MapKind::HashRow { cols }) => {
            write_cols_with_tcs(&mut w, cols);
            (Opcode::MapHashRow, None, Some(w.into_vec()))
        }
        OpNode::Negate => (Opcode::Negate, None, None),
        OpNode::Union => (Opcode::Union, None, None),
        OpNode::Distinct => (Opcode::Distinct, None, None),
        OpNode::PositivePart => (Opcode::PositivePart, None, None),
        OpNode::Reduce { group_cols, agg, global_ground } => {
            w.bool(*global_ground);
            write_cols(&mut w, group_cols);
            write_aggs(&mut w, agg);
            (Opcode::Reduce, None, Some(w.into_vec()))
        }
        // The side flag leads every join's params, so all three opcodes carry one.
        OpNode::Join { kind, delta_is_right } => {
            w.bool(*delta_is_right);
            let opcode = match kind {
                JoinKind::Equi => Opcode::JoinEqui,
                JoinKind::Range { n_eq, rel } => {
                    w.u8(*n_eq).u8(rel.as_wire());
                    Opcode::JoinRange
                }
                JoinKind::Cross => Opcode::JoinCross,
            };
            (opcode, None, Some(w.into_vec()))
        }
        OpNode::IntegrateSink => (Opcode::IntegrateSink, None, None),
        OpNode::IntegrateTrace => (Opcode::IntegrateTrace, None, None),
        OpNode::ExchangeShard { shard_cols } => {
            write_cols(&mut w, shard_cols);
            (Opcode::ExchangeShard, None, Some(w.into_vec()))
        }
        OpNode::NullExtend { type_codes, nulls_first } => {
            w.bool(*nulls_first);
            write_count(&mut w, type_codes.len());
            for &tc in type_codes {
                w.u8(tc.as_wire());
            }
            (Opcode::NullExtend, None, Some(w.into_vec()))
        }
        OpNode::WorkerFilter => (Opcode::WorkerFilter, None, None),
        OpNode::TopN { group_cols, order, limit, offset } => {
            w.u64(*limit).u64(*offset);
            write_cols(&mut w, group_cols);
            write_order_keys(&mut w, order);
            (Opcode::TopN, None, Some(w.into_vec()))
        }
    }
}

/// Reconstruct an `OpNode` from one `CircuitNodes` row. `params` is the blob
/// cell as stored: `None` is "this opcode carries no parameters", and a present
/// but empty cell is damaged — no layout here encodes to zero bytes.
pub fn decode_op_node(opcode: u64, src_tab: Option<u64>, params: Option<&[u8]>) -> Result<OpNode, String> {
    // One reader for every layout. A parameterless opcode reads nothing from it,
    // so a blob it should not carry is refused as trailing bytes.
    decode_all(params.unwrap_or(&[]), "circuit params", |r| {
        decode_params(r, opcode, src_tab, params)
    })
}

fn decode_params(r: &mut Reader, opcode: u64, src_tab: Option<u64>, params: Option<&[u8]>) -> Result<OpNode, String> {
    let op = Opcode::from_wire(opcode).ok_or_else(|| format!("unknown opcode {opcode}"))?;
    if src_tab.is_some() && !matches!(op, Opcode::ScanDelta) {
        return Err(format!("{op:?} carries a source_table"));
    }
    if params.is_some_and(<[u8]>::is_empty) {
        return Err(format!("{op:?} carries an empty parameter cell"));
    }
    Ok(match op {
        Opcode::ScanDelta => OpNode::ScanDelta {
            source: src_tab.ok_or_else(|| "SCAN_DELTA missing source_table".to_string())?,
            bound: match params {
                None => crate::ReadBound::None,
                Some(_) => match crate::read_spec::read_read_bound(r)? {
                    crate::ReadBound::None => {
                        return Err("a ScanDelta with no bound carries no params cell".to_string());
                    }
                    bound => bound,
                },
            },
        },
        Opcode::Filter => OpNode::Filter(r.bytes32()?.to_vec()),
        Opcode::MapProj => OpNode::Map(MapKind::Projection(read_cols(r)?)),
        Opcode::MapExpr => OpNode::Map(MapKind::Compute(read_compute_map(r)?)),
        Opcode::MapReindex => {
            // The role decides which worker a row lands on, so an unknown value is
            // a refusal — unlike a `ScanDelta` bound, which only decides scan speed.
            let role_byte = r.u8()?;
            let role = match role_byte {
                ROLE_AUXILIARY => ReindexRole::Auxiliary,
                ROLE_SCATTER_KEY => {
                    let source = r.u64()?;
                    let source_key = read_cols_with_tcs(r)?;
                    if source_key.is_empty() {
                        return Err("MAP_REINDEX scatter key names no source columns".to_string());
                    }
                    ReindexRole::ScatterKey { source, source_key }
                }
                other => return Err(format!("MAP_REINDEX unknown route-key role {other}")),
            };
            let key = read_cols_with_tcs(r)?;
            if key.is_empty() {
                return Err("MAP_REINDEX names no key columns".to_string());
            }
            OpNode::Map(MapKind::Reindex { keep: read_cols(r)?, key, role })
        }
        Opcode::MapHashRow => {
            // No target-domain gate here: `MapPlan::from_wire` types the output
            // column at the target, so `check_copy_types` sees the promotion.
            let cols = read_cols_with_tcs(r)?;
            // An empty list hashes no bytes, collapsing every row onto one PK —
            // the same self-consistency refusal `MAP_REINDEX` gets above.
            if cols.is_empty() {
                return Err("MAP_HASH_ROW names no columns".to_string());
            }
            OpNode::Map(MapKind::HashRow { cols })
        }
        Opcode::Negate => OpNode::Negate,
        Opcode::Union => OpNode::Union,
        Opcode::Distinct => OpNode::Distinct,
        Opcode::PositivePart => OpNode::PositivePart,
        Opcode::Reduce => {
            let global_ground = r.bool()?;
            let group_cols = read_cols(r)?;
            // The ground row carries no group columns.
            if global_ground && !group_cols.is_empty() {
                return Err("REDUCE global-ground over a non-empty group set".to_string());
            }
            let agg = read_aggs(r)?;
            OpNode::Reduce { group_cols, agg, global_ground }
        }
        Opcode::JoinEqui => OpNode::Join {
            kind: JoinKind::Equi,
            delta_is_right: r.bool()?,
        },
        Opcode::JoinRange => {
            let delta_is_right = r.bool()?;
            let n_eq = r.u8()?;
            let rel_byte = r.u8()?;
            let rel = RangeRel::from_wire(rel_byte).ok_or_else(|| format!("JOIN unknown rel {rel_byte}"))?;
            OpNode::Join {
                kind: JoinKind::Range { n_eq, rel },
                delta_is_right,
            }
        }
        Opcode::JoinCross => OpNode::Join {
            kind: JoinKind::Cross,
            delta_is_right: r.bool()?,
        },
        Opcode::IntegrateSink => OpNode::IntegrateSink,
        Opcode::IntegrateTrace => OpNode::IntegrateTrace,
        Opcode::ExchangeShard => OpNode::ExchangeShard { shard_cols: read_cols(r)? },
        Opcode::NullExtend => {
            let nulls_first = r.bool()?;
            let n = read_count(r, "NULL_EXTEND", crate::MAX_COLUMNS)?;
            let type_codes = (0..n).map(|_| read_type_code(r)).collect::<Result<_, _>>()?;
            OpNode::NullExtend { type_codes, nulls_first }
        }
        Opcode::WorkerFilter => OpNode::WorkerFilter,
        Opcode::TopN => {
            let limit = r.u64()?;
            let offset = r.u64()?;
            let group_cols = read_cols(r)?;
            let order = read_order_keys(r)?;
            OpNode::TopN { group_cols, order, limit, offset }
        }
    })
}

#[cfg(test)]
#[path = "tests/circuit.rs"]
mod tests;
