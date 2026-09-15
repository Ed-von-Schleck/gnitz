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
pub(crate) const CIRCUIT_PARAMS_VERSION: u8 = 4;

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
    /// `agg_col_def`; a NOT NULL declaration puts the row on a null-blind
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
/// `ReducePlan::from_wire` consumes it.
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
    pub out_cols: Vec<(u8, bool)>,
}

/// Output type code of an aggregate over a source column of type `src_tc` — the
/// one typing rule both the planner's declared view schema and the engine's
/// emitted batch read, so neither can scramble the other's column widths.
///
/// MIN/MAX *select* an existing row, so the extremum is itself a value of the
/// source type: `MIN(INT)` is `INT`, `MAX(TEXT)` is `TEXT`. A float widens to
/// F64, the width the accumulator holds it at.
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
        // Exactly the 8-byte register image the accumulator holds. A temporal
        // image names a narrower slot than that image, and a sum of calendar
        // values is not one — the planner rejects SUM over a temporal column
        // before it gets here, so this is what a forged circuit lands on.
        AggFunc::Sum => match crate::types::register_image_type(src_tc) {
            t if crate::is_temporal(t) => type_code::I64,
            t => t,
        },
        AggFunc::Min | AggFunc::Max => {
            if is_float {
                type_code::F64
            } else {
                src_tc
            }
        }
    }
}

wire_enum! {
    /// The relation a **trace** slot must satisfy versus the **delta** slot in a
    /// range-join probe (`{ trace_slot REL delta_slot }`). Canonicalized from the
    /// ON clause's `L.x OP R.y`: term AB's rel is the converse of OP, term BA's
    /// rel is OP itself. Wire values are stable.
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
}

/// How a `Reduce` node keys its output. Derived, never transmitted: both sides
/// call [`Self::for_group_cols`] over facts they already hold — the source PK
/// column list and the GROUP BY column list — so there is one producer and
/// nothing to disagree with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReduceOutKey {
    /// Leading synthetic `_group_pk` U128 = null-distinct group fold; the
    /// group columns ride as payload. What every group set that is neither
    /// natural kind gets, including the empty (global) group set.
    SyntheticFold,
    /// The group set is a permutation of the source PK; the output PK is the
    /// source PK columns in pk-list order, verbatim.
    PkPermutation,
    /// A single non-nullable U64/U128/UUID group column is the output PK
    /// directly.
    SingleNaturalCol,
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

    /// The source columns spelling the output's PK region, in output PK order, or
    /// `None` for the synthetic fold key. Borrowed from the caller's own lists —
    /// no kind builds a region of its own.
    ///
    /// `SingleNaturalCol` names `group_cols[0]`, which [`Self::for_group_cols`]
    /// only selects for a single-column group set — so the index is in range by
    /// construction. The ad-hoc fold's hardcoded `SyntheticFold` never reaches
    /// that arm.
    pub fn key_region<'a>(self, pk_cols: &'a [u32], group_cols: &'a [u32]) -> Option<&'a [u32]> {
        match self {
            // The output PK region mirrors the source's PK byte layout, so it
            // walks the PK list in order rather than `group_cols` order.
            ReduceOutKey::PkPermutation => Some(pk_cols),
            ReduceOutKey::SingleNaturalCol => Some(&group_cols[..1]),
            ReduceOutKey::SyntheticFold => None,
        }
    }

    /// An output's layout ahead of its aggregates: the key region (or the synthetic
    /// key), then each `row` column the key region does not spell.
    pub fn output_layout(
        self,
        pk_cols: &[u32],
        group_cols: &[u32],
        row: impl IntoIterator<Item = u32>,
    ) -> Vec<ReduceOutSlot> {
        let keys = self.key_region(pk_cols, group_cols);
        let lead: Vec<ReduceOutSlot> = match keys {
            Some(k) => k.iter().map(|&c| ReduceOutSlot::Key(c)).collect(),
            None => vec![ReduceOutSlot::SyntheticKey],
        };
        let spelled = keys.unwrap_or_default();
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
/// opcode. `Range` keeps `JoinKind: Copy` (its fields are `Copy`).
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

wire_enum! {
    /// What a reindex `Map` re-keys *for*. One scan can fan out into several
    /// reindex Maps on different keys — a band outer join hangs its null-fill's
    /// source-PK re-key and its scatter-key re-key off the same `ScanDelta`
    /// (`SELECT * FROM a LEFT JOIN b ON a.k = b.k AND a.v < b.w`) — and can also
    /// carry re-keys that only move already-routed rows, so the two cannot be told
    /// apart by graph shape. The planner states which is which at the call site,
    /// where it knows.
    pub enum ReindexRole: u8 {
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

/// Typed operator-node payload. Expression blobs are stored as raw `Vec<u8>` and decoded
/// with `gnitz_expr::LogicalProgram::from_blob`, which validates the program the
/// framing here only frames.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpNode {
    /// Delta input for `source`.
    ///
    /// `bound` is a **backfill-scan hint only**: steady-state deltas never open
    /// the source cursor. It narrows the initial full-source scan, leaves the
    /// downstream `Filter` and so the view unchanged, and the engine may ignore
    /// it (`IndexWalk::Optional`). It travels because the engine cannot derive
    /// it — the planner reads the predicate's `BoundExpr` conjuncts, which the
    /// compiled program does not carry.
    ScanDelta {
        source: u64,
        bound: Option<crate::IndexBound>,
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
    /// Per-group top-N over the input: for each `group_cols` value, the rows
    /// occupying weight slots `offset .. offset + limit` of the group sorted by
    /// `order` (then by the whole row, so the order is total over Z-set
    /// elements), each at the weight of the slots it fills. The output is keyed
    /// like a `Reduce` over the same group set ([`ReduceOutKey`]) and carries
    /// every input column behind that key. `order` columns index the input
    /// schema. `limit` is never `0`.
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
            | OpNode::WorkerFilter
            | OpNode::TopN { .. } => 1,
        }
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

const PARAMS_CTX: &str = "circuit params";

/// Write a counted list's length. Asserted, not rejected: the encode side is the
/// trusted producer (`read_count` rejects), and `len() as u16` would truncate a
/// longer list into a valid shorter one. Producers check their own lists — an
/// output schema does not bound an intermediate node.
fn write_count(w: &mut Writer, n: usize, what: &str) {
    assert!(
        n <= crate::MAX_COLUMNS,
        "{what}: {n} entries exceeds the {}-column cap",
        crate::MAX_COLUMNS
    );
    w.u16(n as u16);
}

/// Read a counted list's length, bounded before anything sizes a `Vec` off it.
/// This decoder holds no schema, so [`crate::MAX_COLUMNS`] is the only bound
/// available; the compiler bounds each index against the register schema.
fn read_count(r: &mut Reader, what: &str) -> Result<usize, String> {
    let n = r.u16()? as usize;
    if n > crate::MAX_COLUMNS {
        return Err(format!("{what}: {n} entries exceeds cap {}", crate::MAX_COLUMNS));
    }
    Ok(n)
}

pub(crate) fn write_cols(w: &mut Writer, cols: &[u32]) {
    write_count(w, cols.len(), "column list");
    for &c in cols {
        w.u32(c);
    }
}

pub(crate) fn read_cols(r: &mut Reader) -> Result<Vec<u32>, String> {
    let n = read_count(r, "column list")?;
    let mut cols = Vec::with_capacity(n);
    for _ in 0..n {
        cols.push(r.u32()?);
    }
    Ok(cols)
}

/// A counted `(source column, promoted target type)` list; `0` on the wire is
/// "no target". `TypeCode::try_from_u8` is the whole domain check here — which
/// targets a particular node admits is that arm's own business.
fn read_cols_with_tcs(r: &mut Reader) -> Result<Vec<ReindexSlot>, String> {
    let n = read_count(r, "slot list")?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let col = r.u32()?;
        let target = match r.u8()? {
            0 => None,
            tc => Some(TypeCode::try_from_u8(tc).ok_or_else(|| format!("unknown promotion type code {tc}"))?),
        };
        out.push((col, target));
    }
    Ok(out)
}

fn write_cols_with_tcs(w: &mut Writer, slots: &[ReindexSlot]) {
    write_count(w, slots.len(), "slot list");
    for &(col, tc) in slots {
        w.u32(col).u8(tc.map_or(0, |t| t as u8));
    }
}

const ORDER_DESC: u8 = 1 << 0;
const ORDER_NULLS_FIRST: u8 = 1 << 1;

/// One order key's four wire bytes: column, flags, one reserved byte.
fn write_order_key(w: &mut Writer, key: &crate::OrderKey) {
    let mut flags = 0u8;
    if key.desc {
        flags |= ORDER_DESC;
    }
    if key.nulls_first {
        flags |= ORDER_NULLS_FIRST;
    }
    w.u16(key.col).u8(flags).u8(0);
}

/// [`write_order_key`]'s inverse; an unknown flag bit is a refusal.
fn read_order_key(r: &mut Reader) -> Result<crate::OrderKey, String> {
    let col = r.u16()?;
    let flags = r.u8()?;
    if flags & !(ORDER_DESC | ORDER_NULLS_FIRST) != 0 {
        return Err(format!("order key has unknown flag bits {flags:#04x}"));
    }
    let _rsv = r.u8()?;
    Ok(crate::OrderKey {
        col,
        desc: flags & ORDER_DESC != 0,
        nulls_first: flags & ORDER_NULLS_FIRST != 0,
    })
}

/// A counted order-key list, shared by the rows sink and a circuit's `TopN`
/// node, so the two cannot drift.
pub(crate) fn write_order_keys(w: &mut Writer, keys: &[crate::OrderKey]) {
    write_count(w, keys.len(), "order keys");
    for k in keys {
        write_order_key(w, k);
    }
}

pub(crate) fn read_order_keys(r: &mut Reader) -> Result<Vec<crate::OrderKey>, String> {
    let n = read_count(r, "order keys")?;
    if n > crate::MAX_ORDER_KEYS {
        return Err(format!("order keys: {n} exceeds cap {}", crate::MAX_ORDER_KEYS));
    }
    (0..n).map(|_| read_order_key(r)).collect()
}

pub(crate) fn write_aggs(w: &mut Writer, aggs: &[AggDescriptor]) {
    write_count(w, aggs.len(), "aggregate list");
    for d in aggs {
        w.u8(d.agg_op.as_wire()).u32(d.col_idx);
    }
}

pub(crate) fn read_aggs(r: &mut Reader) -> Result<Vec<AggDescriptor>, String> {
    let n = read_count(r, "aggregate list")?;
    let mut aggs = Vec::with_capacity(n);
    for _ in 0..n {
        let func_byte = r.u8()?;
        let agg_op = AggFunc::from_wire(func_byte).ok_or_else(|| format!("unknown agg func id {func_byte}"))?;
        aggs.push(AggDescriptor { agg_op, col_idx: r.u32()? });
    }
    Ok(aggs)
}

/// The declared payload slots, then the program. Taken as parts rather than as a
/// `&ComputeMap` because the `ReadSpec` fold spells "no pre-map" as an empty
/// program and holds no struct to borrow.
pub(crate) fn write_compute_map(w: &mut Writer, out_cols: &[(u8, bool)], program: &[u8]) {
    write_count(w, out_cols.len(), "compute map");
    for &(tc, nullable) in out_cols {
        w.u8(tc).u8(nullable as u8);
    }
    w.bytes32(program);
}

pub(crate) fn read_compute_map(r: &mut Reader) -> Result<ComputeMap, String> {
    let n = read_count(r, "compute map")?;
    let mut out_cols = Vec::with_capacity(n);
    for _ in 0..n {
        // These become schema columns verbatim (`is_valid_type_code`).
        let tc = r.u8()?;
        if !crate::is_valid_type_code(tc) {
            return Err(format!("compute map output column type code {tc} is invalid"));
        }
        let nullable = r.u8()?;
        if nullable > 1 {
            return Err(format!("compute map output column nullable flag is {nullable}"));
        }
        out_cols.push((tc, nullable == 1));
    }
    Ok(ComputeMap { program: r.bytes32()?.to_vec(), out_cols })
}

/// Encode a typed `OpNode` into its `CircuitNodes` row fields — the inverse of
/// [`decode_op_node`]. The expression blob is carried opaquely (each crate
/// encodes it with its own encoder before building the `OpNode`).
pub fn encode_op_node(op: OpNode) -> (Opcode, Option<u64>, Option<Vec<u8>>) {
    let mut w = Writer::with_capacity(32);
    match op {
        // An unbounded `ScanDelta` carries no params at all: the common shape
        // costs nothing, and "absent" and "empty" stay distinguishable.
        OpNode::ScanDelta { source, bound: None } => (Opcode::ScanDelta, Some(source), None),
        OpNode::ScanDelta { source, bound: Some(b) } => {
            crate::range::write_index_bound(&mut w, &b);
            (Opcode::ScanDelta, Some(source), Some(w.into_vec()))
        }
        OpNode::Filter(program) => {
            w.bytes32(&program);
            (Opcode::Filter, None, Some(w.into_vec()))
        }
        OpNode::Map(MapKind::Projection(cols)) => {
            write_cols(&mut w, &cols);
            (Opcode::MapProj, None, Some(w.into_vec()))
        }
        OpNode::Map(MapKind::Compute(map)) => {
            write_compute_map(&mut w, &map.out_cols, &map.program);
            (Opcode::MapExpr, None, Some(w.into_vec()))
        }
        OpNode::Map(MapKind::Reindex { keep, key, role }) => {
            w.u8(role.as_wire());
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
        OpNode::Reduce { group_cols, agg, global_ground } => {
            w.u8(global_ground as u8);
            write_cols(&mut w, &group_cols);
            write_aggs(&mut w, &agg);
            (Opcode::Reduce, None, Some(w.into_vec()))
        }
        OpNode::Join(JoinKind::Equi) => (Opcode::JoinEqui, None, None),
        OpNode::Join(JoinKind::Range { n_eq, rel }) => {
            w.u8(n_eq).u8(rel.as_wire());
            (Opcode::JoinRange, None, Some(w.into_vec()))
        }
        OpNode::Join(JoinKind::Cross) => (Opcode::JoinCross, None, None),
        OpNode::IntegrateSink => (Opcode::IntegrateSink, None, None),
        OpNode::IntegrateTrace => (Opcode::IntegrateTrace, None, None),
        OpNode::ExchangeShard { shard_cols } => {
            write_cols(&mut w, &shard_cols);
            (Opcode::ExchangeShard, None, Some(w.into_vec()))
        }
        OpNode::NullExtend { type_codes } => {
            write_count(&mut w, type_codes.len(), "NULL_EXTEND");
            for tc in type_codes {
                w.u8(tc);
            }
            (Opcode::NullExtend, None, Some(w.into_vec()))
        }
        OpNode::WorkerFilter => (Opcode::WorkerFilter, None, None),
        OpNode::TopN { group_cols, order, limit, offset } => {
            w.u64(limit).u64(offset);
            write_cols(&mut w, &group_cols);
            write_order_keys(&mut w, &order);
            (Opcode::TopN, None, Some(w.into_vec()))
        }
    }
}

/// Reconstruct an `OpNode` from one `CircuitNodes` row. `params` is the blob
/// cell as stored: `None` is "this opcode carries no parameters", and a present
/// but empty cell is damaged — no layout here encodes to zero bytes.
pub fn decode_op_node(opcode: u64, src_tab: Option<u64>, params: Option<&[u8]>) -> Result<OpNode, String> {
    let op = Opcode::from_wire(opcode).ok_or_else(|| format!("unknown opcode {opcode}"))?;
    if params.is_some_and(<[u8]>::is_empty) {
        return Err(format!("{op:?} carries an empty parameter cell"));
    }
    // One reader for every layout. A parameterless opcode reads nothing from it,
    // so a blob it should not carry falls out of `expect_consumed` below.
    let mut r = Reader::new(params.unwrap_or(&[]), PARAMS_CTX);
    let node = match op {
        Opcode::ScanDelta => OpNode::ScanDelta {
            source: src_tab.ok_or_else(|| "SCAN_DELTA missing source_table".to_string())?,
            bound: params.map(|_| crate::range::read_index_bound(&mut r)).transpose()?,
        },
        Opcode::Filter => OpNode::Filter(r.bytes32()?.to_vec()),
        Opcode::MapProj => OpNode::Map(MapKind::Projection(read_cols(&mut r)?)),
        Opcode::MapExpr => OpNode::Map(MapKind::Compute(read_compute_map(&mut r)?)),
        Opcode::MapReindex => {
            // The role decides which worker a row lands on, so an unknown value is
            // a refusal — unlike a `ScanDelta` bound, which only decides scan speed.
            let role_byte = r.u8()?;
            let role = ReindexRole::from_wire(role_byte)
                .ok_or_else(|| format!("MAP_REINDEX unknown route-key role {role_byte}"))?;
            let key = read_cols_with_tcs(&mut r)?;
            if key.is_empty() {
                return Err("MAP_REINDEX names no key columns".to_string());
            }
            // The decode-time screen, which has no schema: `ReindexPacker::new`
            // holds the target to its source column's promotion domain, which is
            // stricter and subsumes this.
            for (_, tc) in key.iter().filter_map(|&(c, tc)| tc.map(|t| (c, t))) {
                if !crate::is_pk_eligible(tc as u8) {
                    return Err(format!("MAP_REINDEX target type code {} is not PK-eligible", tc as u8));
                }
            }
            OpNode::Map(MapKind::Reindex { keep: read_cols(&mut r)?, key, role })
        }
        Opcode::MapHashRow => {
            let branch_id = r.u8()?;
            // No target-domain gate here: `MapPlan::from_wire` types the output
            // column at the target, so `check_copy_types` sees the promotion.
            let cols = read_cols_with_tcs(&mut r)?;
            // An empty list hashes no bytes, collapsing every row onto one PK —
            // the same self-consistency refusal `MAP_REINDEX` gets above.
            if cols.is_empty() {
                return Err("MAP_HASH_ROW names no columns".to_string());
            }
            OpNode::Map(MapKind::HashRow { cols, branch_id })
        }
        Opcode::Negate => OpNode::Negate,
        Opcode::Union => OpNode::Union,
        Opcode::Distinct => OpNode::Distinct,
        Opcode::PositivePart => OpNode::PositivePart,
        Opcode::Reduce => {
            let global_ground = r.u8()? != 0;
            let group_cols = read_cols(&mut r)?;
            let agg = read_aggs(&mut r)?;
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
            OpNode::Reduce { group_cols, agg, global_ground }
        }
        Opcode::JoinEqui => OpNode::Join(JoinKind::Equi),
        Opcode::JoinRange => {
            let n_eq = r.u8()?;
            let rel_byte = r.u8()?;
            let rel = RangeRel::from_wire(rel_byte).ok_or_else(|| format!("JOIN unknown rel {rel_byte}"))?;
            OpNode::Join(JoinKind::Range { n_eq, rel })
        }
        Opcode::JoinCross => OpNode::Join(JoinKind::Cross),
        Opcode::IntegrateSink => OpNode::IntegrateSink,
        Opcode::IntegrateTrace => OpNode::IntegrateTrace,
        Opcode::ExchangeShard => OpNode::ExchangeShard { shard_cols: read_cols(&mut r)? },
        Opcode::NullExtend => {
            let n = read_count(&mut r, "NULL_EXTEND")?;
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
        Opcode::TopN => {
            let limit = r.u64()?;
            let offset = r.u64()?;
            if limit == 0 {
                return Err("TOP_N carries a zero limit".to_string());
            }
            let group_cols = read_cols(&mut r)?;
            let order = read_order_keys(&mut r)?;
            OpNode::TopN { group_cols, order, limit, offset }
        }
    };
    r.expect_consumed()?;
    Ok(node)
}

#[cfg(test)]
#[path = "tests/circuit.rs"]
mod tests;
