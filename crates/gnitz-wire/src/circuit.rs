//! Circuit-layer wire definitions: the operator opcode space, aggregate
//! discriminants, and the typed `OpNode` representation shared between
//! gnitz-core and gnitz-server — plus the `params` codec that carries each
//! node's per-opcode parameters as one blob.

use crate::codec::{Reader, Writer};
use crate::TypeCode;

// ---------------------------------------------------------------------------
// Circuit opcodes
// ---------------------------------------------------------------------------

wire_enum! {
    /// The operator space. A wire enum so [`encode_op_node`] and
    /// [`decode_op_node`] match exhaustively; the discriminants are durable
    /// catalog state, so the sparse numbering stays as it is.
    pub enum Opcode: u64 {
        Filter = 1,
        Negate = 3,
        Union = 4,
        /// Symmetric delta-trace join. Which probe — equal-key seek, ordered
        /// range walk, or full cross product — is a [`JoinKind`] tag leading the
        /// params blob, not a second opcode.
        Join = 5,
        /// Primary INTEGRATE: writes to view storage.
        Integrate = 7,
        Reduce = 9,
        Distinct = 10,
        /// Delta input source for a base table. The table id lives in the node
        /// row's `source_table` column.
        ScanDelta = 11,
        ExchangeShard = 20,
        NullExtend = 23,
        /// Discriminates IntegrateTrace from IntegrateSink without a nullable
        /// column.
        IntegrateTrace = 25,
        /// MAP sub-variant: pure projection (column reorder/drop).
        MapProj = 26,
        /// MAP sub-variant: expression program (compute), inheriting the input PK.
        MapExpr = 27,
        /// MAP sub-variant: copy all columns to payload, set PK = hash of full row.
        MapHashRow = 29,
        /// Drop trace rows this worker does not own — the trace side of a broadcast
        /// join input (a **pure** range join and the keyless cross join; a band join
        /// scatters by its eq prefix and omits this node). Worker identity is a
        /// compile-time constant, so the node carries no parameters.
        WorkerFilter = 33,
        /// Multiplicity-preserving sibling of DISTINCT: clamps each consolidated
        /// (PK, payload)'s net weight to `[0, i64::MAX]` (vs DISTINCT's `[-1, 1]`).
        /// The bag preset for EXCEPT ALL / INTERSECT ALL; shares DISTINCT's engine
        /// body.
        PositivePart = 34,
        /// MAP sub-variant: re-key onto a synthetic PK built from the named source
        /// columns, keeping the named columns as payload. A separate opcode rather
        /// than a `MapExpr` carrying an empty key list: the two share no parameter
        /// shape — a compute map has a program blob and declared output columns, a
        /// reindex two column lists and no program.
        MapReindex = 35,
    }
}

/// The `params` blob layout. Folded into [`crate::SYS_SCHEMA_DIGEST`] rather
/// than written into the blob, so a bump rejects an existing data directory at
/// boot instead of reinterpreting it.
pub(crate) const CIRCUIT_PARAMS_VERSION: u8 = 2;

// ---------------------------------------------------------------------------
// Circuit-layer type aliases
// ---------------------------------------------------------------------------

pub type TableId = u64;

// ---------------------------------------------------------------------------
// Typed circuit-node representation (shared between gnitz-core and gnitz-server)
// ---------------------------------------------------------------------------

wire_enum! {
    /// Aggregate function discriminant. The values are durable catalog state
    /// (a `Reduce` node's `params`) and a wire value on the ad-hoc fold path.
    pub enum AggFunc: u64 {
        Count = 1,
        Sum = 2,
        Min = 3,
        Max = 4,
        CountNonNull = 5,
        /// `Sum`'s fold (`acc += value × weight`) with `Count`'s `0` identity
        /// (grounds to `0`, renders `0` when untouched). The two-phase
        /// global-aggregate combine sums per-worker partial COUNT/COUNT_NON_NULL
        /// columns with this — a plain `Sum` would render their empty value as
        /// NULL instead of `0`.
        SumZero = 6,
    }
}

impl AggFunc {
    /// True iff the aggregate folds a delta with no history replay
    /// (`Agg(A + B) == Agg(A) + Agg(B)`). The complement retracts its extremum
    /// from the aggregate-value index, which is then the value's source of truth.
    pub const fn is_linear(self) -> bool {
        match self {
            AggFunc::Count | AggFunc::Sum | AggFunc::CountNonNull | AggFunc::SumZero => true,
            AggFunc::Min | AggFunc::Max => false,
        }
    }

    /// True iff an untouched accumulator renders a concrete `0` rather than
    /// NULL — the zero-identity family. COUNT / COUNT_NON_NULL count rows
    /// (empty = 0); SumZero is Sum's fold under Count's `0` identity (the
    /// two-phase partial-count combine). SUM / MIN / MAX have a NULL empty
    /// value.
    pub const fn empty_renders_zero(self) -> bool {
        match self {
            AggFunc::Count | AggFunc::CountNonNull | AggFunc::SumZero => true,
            AggFunc::Sum | AggFunc::Min | AggFunc::Max => false,
        }
    }

    /// True iff a reduce's **raw** output column for this aggregate can render
    /// NULL, and must therefore be declared nullable — the declaration half of
    /// the null-bit rule `emit_agg_col` writes.
    ///
    /// A row carries an untouched accumulator only when the aggregate's source
    /// column is nullable (the null gate is the group walk's one skip) or when
    /// the group set is empty, where the ground row stands in for a
    /// never-populated / fully-retracted source. A surviving *group* is never
    /// null-filled from emptiness — it is retracted instead.
    ///
    /// Read by `build_reduce_output_schema` and by the planner's
    /// `agg_raw_nullable`; a NOT NULL declaration puts the row on a null-blind
    /// comparator that would rank a NULL cell as a real `0`.
    pub const fn raw_output_nullable(self, src_nullable: bool, ungrouped: bool) -> bool {
        !self.empty_renders_zero() && (src_nullable || ungrouped)
    }

    /// The aggregate that merges this aggregate's per-worker **partials**, read
    /// by the planner's two-phase combine reduce and by the ad-hoc fold's
    /// client-side combiner. COUNT/COUNT_NON_NULL partials sum with `SumZero` (a
    /// count's empty value is 0, not NULL); SUM partials sum with plain `Sum`
    /// (NULL ground); MIN/MAX partials merge by re-applying themselves.
    pub fn merge_func(self) -> AggFunc {
        match self {
            AggFunc::Count | AggFunc::CountNonNull | AggFunc::SumZero => AggFunc::SumZero,
            AggFunc::Sum => AggFunc::Sum,
            AggFunc::Min => AggFunc::Min,
            AggFunc::Max => AggFunc::Max,
        }
    }
}

/// One aggregate of a reduce: which function, over which column of the reduce's
/// input. Carries no column *type* — that is `schema.columns[col_idx]`, which
/// every consumer already holds. The one spelling on both wire paths: a
/// `Reduce` node's parameters and an ad-hoc fold's `ReadSpec` ship this, and
/// `ReducePlan::new` consumes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AggDescriptor {
    pub col_idx: u32,
    pub agg_op: AggFunc,
}

/// A compiled expression program and the payload slots it writes, as
/// `(type_code, nullable)` in output order; the PK region is inherited verbatim.
/// The declarations travel because a computed projection has no copy list the
/// engine could derive a schema from. One type on both wire paths — a circuit's
/// `Map` and a `ReadSpec` fold's pre-map are the same device.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComputeMap {
    pub program: Vec<u8>,
    pub out_cols: Vec<(u8, bool)>,
}

/// Output type code of an aggregate over a source column of type `src_tc` — the
/// one typing rule both the planner's declared view schema and the engine's
/// emitted batch read, so neither can scramble the other's column widths.
///   COUNT, COUNT_NON_NULL → I64 (SUM_ZERO sums integer count/sum columns and
///   likewise produces I64)
///   SUM on float → F64, else the source's 8-byte register image
///   MIN/MAX on float → F64; on a ≤8-byte integer → that source type; else I64
///
/// MIN/MAX *select* an existing row, so a ≤8-byte integer extremum is itself a
/// value of the source type and always representable in it: `MIN(INT)` is `INT`,
/// `MAX(SMALLINT UNSIGNED)` is `SMALLINT UNSIGNED`. The engine's row emitters
/// serialize the accumulator at the output column width, and the width-gated
/// trace read-back reconstructs the 8-byte accumulator from it. The `I64` arm a
/// STRING or 16-byte source falls to is a total-function default only — the SQL
/// binder and the engine's order-encodability guard both reject such a MIN/MAX.
///
/// SUM over a U64 source is typed **U64**: the i64 `wrapping_add` accumulator's
/// bit pattern already *is* the true sum mod 2^64 at the same width, so the label
/// is the only choice, and it lets a downstream unsigned compare re-seed
/// correctly. A narrow unsigned source still widens to I64 (its sum stays
/// < 2^63). AVG is planner-lowered before the wire and never reaches this rule.
pub const fn agg_output_type(func: AggFunc, src_tc: u8) -> u8 {
    use crate::types::type_code;
    let is_float = crate::types::is_float(src_tc);
    match func {
        AggFunc::Count | AggFunc::CountNonNull | AggFunc::SumZero => type_code::I64,
        // Exactly the 8-byte register image the accumulator holds.
        AggFunc::Sum => crate::types::register_image_type(src_tc),
        AggFunc::Min | AggFunc::Max => {
            if is_float {
                type_code::F64
            } else if crate::types::is_fixed_int(src_tc) {
                src_tc
            } else {
                type_code::I64
            }
        }
    }
}

wire_enum! {
    /// The relation a **trace** slot must satisfy versus the **delta** slot in a
    /// range-join probe (`{ trace_slot REL delta_slot }`). Canonicalized from the
    /// ON clause's `L.x OP R.y`: term AB's rel is the converse of OP, term BA's
    /// rel is OP itself. Wire values are stable.
    pub enum RangeRel: u64 {
        Lt = 0,
        Le = 1,
        Gt = 2,
        Ge = 3,
    }
}

wire_enum! {
    /// How a `Reduce` node keys its output. The planner decides it with
    /// [`Self::for_group_cols`] and ships it; the engine re-derives through that
    /// same function to *validate* what arrived, and everything downstream —
    /// [`Self::output_layout`] included — obeys the kind rather than re-deriving
    /// it. One implementation on both sides is what makes the check compare a
    /// rule against a transmission rather than two guesses.
    pub enum ReduceOutKey: u64 {
        /// Leading synthetic `_group_pk` U128 = null-distinct group fold; the
        /// group columns ride as payload. What every group set that is neither
        /// natural kind gets, including the empty (global) group set.
        SyntheticFold = 0,
        /// The group set is a permutation of the source PK; the output PK is the
        /// source PK columns in pk-list order, verbatim.
        PkPermutation = 1,
        /// A single non-nullable U64/U128/UUID group column is the output PK
        /// directly.
        SingleNaturalCol = 2,
    }
}

impl ReduceOutKey {
    /// The one precedence chain (eq-PK ▷ single-natural ▷ synthetic), from the
    /// facts both sides already hold: the source's PK column list, the GROUP BY
    /// column list, and a `(type_code, nullable)` reader for a group column.
    pub fn for_group_cols(pk_cols: &[u32], group_cols: &[u32], col: impl Fn(u32) -> (u8, bool)) -> Self {
        // A permutation, not a prefix: the SQL may list PK columns in any order.
        let eq_pk = group_cols.len() == pk_cols.len() && pk_cols.iter().all(|p| group_cols.contains(p));
        let single_natural = match *group_cols {
            [c] => {
                let (type_code, nullable) = col(c);
                // A nullable column can never key the output: the PK region
                // carries no null bitmap.
                !nullable && crate::TypeCode::try_from_u8(type_code).is_some_and(|t| t.is_natural_reduce_key())
            }
            _ => false,
        };
        if eq_pk {
            ReduceOutKey::PkPermutation
        } else if single_natural {
            ReduceOutKey::SingleNaturalCol
        } else {
            ReduceOutKey::SyntheticFold
        }
    }

    /// The output layout this kind selects, up to the aggregate columns each side
    /// types for itself.
    ///
    /// `SingleNaturalCol` names `group_cols[0]`, which [`Self::for_group_cols`]
    /// only selects for a single-column group set; the engine validates the
    /// transmitted kind against the input schema before laying anything out.
    pub fn output_layout(self, pk_cols: &[u32], group_cols: &[u32]) -> Vec<ReduceOutSlot> {
        match self {
            // The output PK region mirrors the source's PK byte layout, so it
            // walks the PK list in order rather than `group_cols` order.
            ReduceOutKey::PkPermutation => pk_cols.iter().map(|&c| ReduceOutSlot::Key(c)).collect(),
            ReduceOutKey::SingleNaturalCol => vec![ReduceOutSlot::Key(group_cols[0])],
            ReduceOutKey::SyntheticFold => std::iter::once(ReduceOutSlot::SyntheticKey)
                .chain(group_cols.iter().map(|&c| ReduceOutSlot::Carried(c)))
                .collect(),
        }
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
    /// Source column `col`, carried into the payload — the fold arm's group
    /// columns, which its synthetic key does not spell.
    Carried(u32),
}

/// Join physical strategy carried by `OpNode::Join`. `DeltaTraceRange` keeps
/// `JoinKind: Copy` (its fields are `Copy`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinKind {
    DeltaTrace,
    /// Non-equi (range) join: the probe is an ordered half-open range walk over
    /// the trace instead of an equal-key seek.
    DeltaTraceRange {
        n_eq: u8,
        rel: RangeRel,
    },
    /// Keyless (cross) join: the probe pairs every delta row with every trace
    /// row.
    DeltaTraceCross,
}

wire_enum! {
    /// Which probe [`JoinKind`] names, leading `Opcode::Join`'s params blob. A
    /// separate enum because `JoinKind` carries data, which a wire enum cannot;
    /// named for the probe rather than mirroring `JoinKind`'s shared
    /// `DeltaTrace` prefix, which discriminates nothing.
    pub(crate) enum JoinKindTag: u8 {
        Equi = 0,
        Range = 1,
        Cross = 2,
    }
}

wire_enum! {
    /// What a reindex `Map` re-keys *for*. One scan can fan out into several
    /// reindex Maps (`t JOIN t1 ON t.a = t1.x JOIN t2 ON t.b = t2.y`) and can also
    /// carry re-keys that only move already-routed rows, so the two cannot be told
    /// apart by graph shape — the planner states which is which at the call site,
    /// where it knows.
    pub enum ReindexRole: u64 {
        /// A re-key of rows a `ScatterKey` already placed — an outer join's
        /// null-fill putting its preserved side back on that side's own PK so the
        /// set difference stays partition-local, or any re-key of an operator's
        /// output.
        Auxiliary = 0,
        /// The join/group key of this Map's source relation — the key the exchange
        /// scatters that source's delta by.
        ///
        /// Not "an exchange is needed": a replicated or co-partitioned source is a
        /// `ScatterKey` too, and whether the exchange runs stays the engine's call
        /// (`compute_co_partitioned`). Naming only the scattered sides would move
        /// that decision into the planner.
        ScatterKey = 1,
    }
}

/// One slot of a reindex key: a source column, and the promotion target the
/// planner carried for it — `None` where the engine derives it
/// ([`resolve_reindex_type`]). The shape every producer and consumer of a
/// reindex or hash-row key list passes around.
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
    /// Full-row-identity reindex. Like `Projection` (keep the listed columns as
    /// payload, in order), but the synthetic PK is set to a hash of the kept
    /// payload bytes. Used by EXCEPT/INTERSECT/DISTINCT so set membership is
    /// decided by the projected row content, not by the source PK.
    ///
    /// Each column carries the promoted payload type the widening projection
    /// coerces it into (`None` = keep the source type; always a ≤8-byte integer),
    /// so a cross-width pair like `I32 UNION I64` hashes one physical layout.
    ///
    /// `branch_id` is mixed into the hash so identical payloads on the two sides
    /// of a `UNION ALL` get distinct PKs and accumulate weight +2 rather than
    /// collapsing: 0 and 1 there, 0 on both sides of a deduplicating set-op.
    HashRow { cols: Vec<ReindexSlot>, branch_id: u8 },
}

/// A secondary-index range bound for a `ScanDelta`'s backfill scan: the index's
/// declared column list and the half-open range over its leading columns.
/// Resolved server-side against the source table's index circuits.
///
/// A **physical access hint, never a semantic filter**. The circuit's `Filter`
/// still carries the full predicate, so a bounded and an unbounded scan produce
/// the same view — a bound only narrows which rows the initial scan reads. That
/// is what lets every consumer degrade to a full scan (a dropped index, an
/// unselective range, a malformed wire row) without changing results, and what
/// makes a malformed bound decode to `None` rather than `Err`: one node `Err`
/// aborts the whole view load.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanBound {
    pub idx_cols: crate::PkColList,
    pub desc: crate::RangeDescriptor,
}

/// Typed operator-node payload. Expression blobs are stored as raw `Vec<u8>` and decoded
/// with `gnitz_wire::decode_expr_blob`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpNode {
    /// Delta input for `source`.
    ///
    /// `bound` is a **backfill-scan hint only**: steady-state deltas never open
    /// the source cursor and ignore it entirely. A non-`None` bound narrows the
    /// initial full-source scan to a secondary-index range, leaving the
    /// downstream `Filter` — and therefore the view — unchanged.
    ScanDelta {
        source: TableId,
        bound: Option<ScanBound>,
    },
    /// Optional expression predicate blob. `None` is "no `WHERE`"; a present but
    /// undecodable program is the compile's rejection, not this decode's.
    Filter(Option<Vec<u8>>),
    Map(MapKind),
    Negate,
    Union,
    Distinct,
    /// Multiplicity-preserving counterpart to `Distinct`: per consolidated
    /// (PK, payload) emits `clamp(w_new, 0, i64::MAX) − clamp(w_old, 0, i64::MAX)`,
    /// where `Distinct` clamps to `[-1, 1]`. Shares `Distinct`'s engine body; the
    /// bag preset for `EXCEPT ALL = positive_part(A − B)` and
    /// `INTERSECT ALL = A − positive_part(A − B)`.
    PositivePart,
    Reduce {
        group_cols: Vec<u32>,
        /// Aggregate specs `(func, source column)`. Never empty: a spec-less
        /// REDUCE is rejected at decode (every producer ships at least one —
        /// the SQL planner injects a companion COUNT for group-only reduces).
        agg: Vec<AggDescriptor>,
        /// True only for the user's ungrouped (global) scalar aggregate — the
        /// reduce that must emit exactly one row over an empty/fully-retracted
        /// source (COUNT(*)=0, SUM/MIN/MAX/AVG=NULL). A **SQL-intent
        /// discriminator**, not a Z-set property: the LEFT range-join's threshold
        /// reduce (`reduce_multi_local`) also has empty group cols but must NOT
        /// seed a ground row, so the flag cannot be derived from
        /// `group_cols.is_empty()` and travels explicitly from the planner.
        global_ground: bool,
        /// How the output is keyed. Decided by the planner, validated (never
        /// re-decided) by the engine compiler.
        out_key: ReduceOutKey,
    },
    Join(JoinKind),
    /// Primary INTEGRATE: writes to view storage.
    IntegrateSink,
    /// Accumulates Z-set for a join trace.
    IntegrateTrace,
    ExchangeShard {
        shard_cols: Vec<u32>,
    },
    NullExtend {
        type_codes: Vec<u8>,
    },
    /// Keep only rows whose packed-PK partition is owned by this worker (**pure**
    /// range-join broadcast input; a band join scatters by its eq prefix and omits
    /// this node). Worker identity is a compile-time constant, so no payload
    /// travels on the wire.
    WorkerFilter,
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
            OpNode::Union | OpNode::Join(_) => 2,
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
            | OpNode::WorkerFilter => 1,
        }
    }
}

// ---------------------------------------------------------------------------
// The `params` codec
// ---------------------------------------------------------------------------
//
// One fixed layout per opcode, through the crate's shared `Writer`/`Reader`. The
// opcode *is* the layout tag, so no field carries a discriminant; counts go
// through the `*_cols` helpers below (`u16`, bounded by `MAX_COLUMNS`), a
// program through `bytes32`, and a `wire_enum!` value as one byte.

const PARAMS_CTX: &str = "circuit params";

fn write_cols(w: &mut Writer, cols: &[u32]) {
    w.u16(cols.len() as u16);
    for &c in cols {
        w.u32(c);
    }
}

fn read_cols(r: &mut Reader) -> Result<Vec<u32>, String> {
    let n = r.u16()? as usize;
    let mut cols = Vec::with_capacity(n);
    for _ in 0..n {
        cols.push(r.u32()?);
    }
    Ok(cols)
}

/// A counted `(source column, promoted target type)` list; `0` on the wire is
/// "no target", and any other code failing `valid` refuses the node rather than
/// decoding to one, which would pack this slot narrower than its partner. The
/// master's only gate: `ViewMeta::for_view` feeds these straight into
/// `ReindexPacker::new(..).expect(..)`.
fn read_cols_with_tcs(
    r: &mut Reader,
    valid: fn(u8) -> bool,
    err: impl Fn(u8) -> String,
) -> Result<Vec<ReindexSlot>, String> {
    let n = r.u16()? as usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let col = r.u32()?;
        let target = match r.u8()? {
            0 => None,
            tc => match TypeCode::try_from_u8(tc) {
                Some(t) if valid(tc) => Some(t),
                _ => return Err(err(tc)),
            },
        };
        out.push((col, target));
    }
    Ok(out)
}

fn write_cols_with_tcs(w: &mut Writer, slots: &[ReindexSlot]) {
    w.u16(slots.len() as u16);
    for &(col, tc) in slots {
        w.u32(col).u8(tc.map_or(0, |t| t as u8));
    }
}

/// A `ScanDelta`'s bound, or `None` for absent / truncated / malformed. Never an
/// `Err`: see [`ScanBound`].
fn read_scan_bound(params: &[u8]) -> Option<ScanBound> {
    let mut r = Reader::new(params, PARAMS_CTX);
    let idx_cols = crate::unpack_pk_cols(r.u64().ok()?).ok()?;
    let desc = crate::range::read_range_descriptor(&mut r).ok()?;
    r.expect_consumed().ok()?;
    Some(ScanBound { idx_cols, desc })
}

/// Encode a typed `OpNode` into its `CircuitNodes` row fields — the inverse of
/// [`decode_op_node`]. The expression blob is carried opaquely (each crate
/// encodes it with its own encoder before building the `OpNode`).
pub fn encode_op_node(op: OpNode) -> (Opcode, Option<TableId>, Option<Vec<u8>>) {
    let mut w = Writer::with_capacity(32);
    match op {
        // An unbounded `ScanDelta` carries no params at all: the common shape
        // costs nothing, and "absent" and "empty" stay distinguishable.
        OpNode::ScanDelta { source, bound: None } => (Opcode::ScanDelta, Some(source), None),
        OpNode::ScanDelta { source, bound: Some(b) } => {
            w.u64(crate::pack_pk_cols(b.idx_cols.as_slice())).raw(&b.desc.encode());
            (Opcode::ScanDelta, Some(source), Some(w.into_vec()))
        }
        OpNode::Filter(None) => (Opcode::Filter, None, None),
        OpNode::Filter(Some(program)) => {
            w.bytes32(&program);
            (Opcode::Filter, None, Some(w.into_vec()))
        }
        OpNode::Map(MapKind::Projection(cols)) => {
            write_cols(&mut w, &cols);
            (Opcode::MapProj, None, Some(w.into_vec()))
        }
        OpNode::Map(MapKind::Compute(map)) => {
            w.u16(map.out_cols.len() as u16);
            for (tc, nullable) in map.out_cols {
                w.u8(tc).u8(nullable as u8);
            }
            w.bytes32(&map.program);
            (Opcode::MapExpr, None, Some(w.into_vec()))
        }
        OpNode::Map(MapKind::Reindex { keep, key, role }) => {
            w.u8(role.as_wire() as u8);
            write_cols_with_tcs(&mut w, &key);
            write_cols(&mut w, &keep);
            (Opcode::MapReindex, None, Some(w.into_vec()))
        }
        OpNode::Map(MapKind::HashRow { cols, branch_id }) => {
            w.u8(branch_id);
            write_cols_with_tcs(&mut w, &cols);
            (Opcode::MapHashRow, None, Some(w.into_vec()))
        }
        OpNode::Negate => (Opcode::Negate, None, None),
        OpNode::Union => (Opcode::Union, None, None),
        OpNode::Distinct => (Opcode::Distinct, None, None),
        OpNode::PositivePart => (Opcode::PositivePart, None, None),
        OpNode::Reduce { group_cols, agg, global_ground, out_key } => {
            w.u8(out_key.as_wire() as u8).u8(global_ground as u8);
            write_cols(&mut w, &group_cols);
            w.u16(agg.len() as u16);
            for d in agg {
                w.u8(d.agg_op.as_wire() as u8).u32(d.col_idx);
            }
            (Opcode::Reduce, None, Some(w.into_vec()))
        }
        OpNode::Join(kind) => {
            match kind {
                JoinKind::DeltaTrace => w.u8(JoinKindTag::Equi.as_wire()),
                JoinKind::DeltaTraceRange { n_eq, rel } => {
                    w.u8(JoinKindTag::Range.as_wire()).u8(n_eq).u8(rel.as_wire() as u8)
                }
                JoinKind::DeltaTraceCross => w.u8(JoinKindTag::Cross.as_wire()),
            };
            (Opcode::Join, None, Some(w.into_vec()))
        }
        OpNode::IntegrateSink => (Opcode::Integrate, None, None),
        OpNode::IntegrateTrace => (Opcode::IntegrateTrace, None, None),
        OpNode::ExchangeShard { shard_cols } => {
            write_cols(&mut w, &shard_cols);
            (Opcode::ExchangeShard, None, Some(w.into_vec()))
        }
        OpNode::NullExtend { type_codes } => {
            w.u16(type_codes.len() as u16);
            for tc in type_codes {
                w.u8(tc);
            }
            (Opcode::NullExtend, None, Some(w.into_vec()))
        }
        OpNode::WorkerFilter => (Opcode::WorkerFilter, None, None),
    }
}

/// Reconstruct an `OpNode` from one `CircuitNodes` row. `params` is the blob
/// cell as stored, so `None` (no parameters) and `Some(&[])` (a damaged cell,
/// which every layout rejects) stay distinct. An expression program is copied
/// out undecoded — callers do that on their side of the crate boundary.
pub fn decode_op_node(opcode: u64, src_tab: Option<TableId>, params: Option<&[u8]>) -> Result<OpNode, String> {
    let op = Opcode::from_wire(opcode).ok_or_else(|| format!("unknown opcode {opcode}"))?;
    // One reader for every layout. A parameterless opcode reads nothing from it,
    // so a blob it should not carry falls out of `expect_consumed` below.
    let mut r = Reader::new(params.unwrap_or(&[]), PARAMS_CTX);
    let node = match op {
        // Returns early: a bound degrades where every other layout rejects, and
        // `read_scan_bound` runs its own reader to the end.
        Opcode::ScanDelta => {
            return Ok(OpNode::ScanDelta {
                source: src_tab.ok_or_else(|| "SCAN_DELTA missing source_table".to_string())?,
                bound: params.and_then(read_scan_bound),
            })
        }
        Opcode::Filter => OpNode::Filter(match params {
            Some(_) => Some(r.bytes32()?.to_vec()),
            None => None,
        }),
        Opcode::MapProj => OpNode::Map(MapKind::Projection(read_cols(&mut r)?)),
        Opcode::MapExpr => {
            let n = r.u16()? as usize;
            let mut out_cols = Vec::with_capacity(n);
            for _ in 0..n {
                // These become schema columns verbatim (`is_valid_type_code`).
                let tc = r.u8()?;
                if !crate::is_valid_type_code(tc) {
                    return Err(format!("MAP_EXPR output column type code {tc} is invalid"));
                }
                out_cols.push((tc, r.u8()? != 0));
            }
            OpNode::Map(MapKind::Compute(ComputeMap {
                program: r.bytes32()?.to_vec(),
                out_cols,
            }))
        }
        Opcode::MapReindex => {
            // The role decides which worker a row lands on, so an unknown value is
            // a refusal — unlike a `ScanDelta` bound, which only decides scan speed.
            let role_byte = r.u8()?;
            let role = ReindexRole::from_wire(role_byte as u64)
                .ok_or_else(|| format!("MAP_REINDEX unknown route-key role {role_byte}"))?;
            // Reject a non-zero target that is not PK-eligible: the reindex
            // targets flow into the 16-byte-capable OPK promoter
            // (`encode_pk_column_promoted`), so its trust boundary admits exactly
            // that domain.
            let key = read_cols_with_tcs(&mut r, crate::is_pk_eligible, |tc| {
                format!("MAP_REINDEX target type code {tc} is not PK-eligible")
            })?;
            if key.is_empty() {
                return Err("MAP_REINDEX names no key columns".to_string());
            }
            OpNode::Map(MapKind::Reindex { keep: read_cols(&mut r)?, key, role })
        }
        Opcode::MapHashRow => {
            let branch_id = r.u8()?;
            // The promotion domain is a ≤8-byte fixed-width integer — stricter
            // than the reindex's `is_pk_eligible`, which also admits the 16-byte
            // U128/UUID/I128 that the payload widen in `copy_column` cannot hold.
            let cols = read_cols_with_tcs(&mut r, crate::is_fixed_int, |tc| {
                format!("MAP_HASH_ROW target type code {tc} is not a fixed-width integer")
            })?;
            OpNode::Map(MapKind::HashRow { cols, branch_id })
        }
        Opcode::Negate => OpNode::Negate,
        Opcode::Union => OpNode::Union,
        Opcode::Distinct => OpNode::Distinct,
        Opcode::PositivePart => OpNode::PositivePart,
        Opcode::Reduce => {
            let out_key_byte = r.u8()?;
            let out_key = ReduceOutKey::from_wire(out_key_byte as u64)
                .ok_or_else(|| format!("REDUCE unknown out_key kind {out_key_byte}"))?;
            let global_ground = r.u8()? != 0;
            let group_cols = read_cols(&mut r)?;
            let n_agg = r.u16()? as usize;
            let mut agg = Vec::with_capacity(n_agg);
            for _ in 0..n_agg {
                let func_byte = r.u8()?;
                let agg_op =
                    AggFunc::from_wire(func_byte as u64).ok_or_else(|| format!("unknown agg func id {func_byte}"))?;
                agg.push(AggDescriptor { agg_op, col_idx: r.u32()? });
            }
            if agg.is_empty() {
                return Err("REDUCE node carries no aggregate spec".to_string());
            }
            // `emit_global_ground` writes its aggregate columns at payload index
            // 0, so a ground row over a group set overwrites the exemplar slots.
            // A cross-check, not a derivation: the implication runs one way (a
            // threshold reduce is group-less with `global_ground = false`).
            if global_ground && !group_cols.is_empty() {
                return Err("REDUCE is global-ground over a non-empty group set".to_string());
            }
            OpNode::Reduce { group_cols, agg, global_ground, out_key }
        }
        Opcode::Join => {
            let tag_byte = r.u8()?;
            let tag = JoinKindTag::from_wire(tag_byte).ok_or_else(|| format!("JOIN unknown kind {tag_byte}"))?;
            OpNode::Join(match tag {
                JoinKindTag::Equi => JoinKind::DeltaTrace,
                JoinKindTag::Range => {
                    let n_eq = r.u8()?;
                    let rel_byte = r.u8()?;
                    let rel =
                        RangeRel::from_wire(rel_byte as u64).ok_or_else(|| format!("JOIN unknown rel {rel_byte}"))?;
                    JoinKind::DeltaTraceRange { n_eq, rel }
                }
                JoinKindTag::Cross => JoinKind::DeltaTraceCross,
            })
        }
        Opcode::Integrate => OpNode::IntegrateSink,
        Opcode::IntegrateTrace => OpNode::IntegrateTrace,
        Opcode::ExchangeShard => OpNode::ExchangeShard { shard_cols: read_cols(&mut r)? },
        Opcode::NullExtend => {
            let n = r.u16()? as usize;
            let mut type_codes = Vec::with_capacity(n);
            for _ in 0..n {
                // These become schema columns verbatim (`is_valid_type_code`).
                let tc = r.u8()?;
                if !crate::is_valid_type_code(tc) {
                    return Err(format!("NULL_EXTEND: invalid column type code {tc}"));
                }
                type_codes.push(tc);
            }
            OpNode::NullExtend { type_codes }
        }
        Opcode::WorkerFilter => OpNode::WorkerFilter,
    };
    r.expect_consumed()?;
    Ok(node)
}

#[cfg(test)]
#[path = "tests/circuit.rs"]
mod tests;
