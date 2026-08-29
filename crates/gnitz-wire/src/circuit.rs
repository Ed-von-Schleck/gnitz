//! Circuit-layer wire definitions: operator opcodes, ports, aggregate IDs, and
//! the typed `OpNode` representation shared between gnitz-core and gnitz-engine.

// ---------------------------------------------------------------------------
// Circuit opcodes
// ---------------------------------------------------------------------------

pub const OPCODE_FILTER: u64 = 1;
pub const OPCODE_NEGATE: u64 = 3;
pub const OPCODE_UNION: u64 = 4;
pub const OPCODE_JOIN_DELTA_TRACE: u64 = 5;
pub const OPCODE_INTEGRATE: u64 = 7;
pub const OPCODE_REDUCE: u64 = 9;
pub const OPCODE_DISTINCT: u64 = 10;
/// Delta input source for a base table. The table id lives in the node row's
/// `source_table` column.
pub const OPCODE_SCAN_DELTA: u64 = 11;
pub const OPCODE_EXCHANGE_SHARD: u64 = 20;
pub const OPCODE_NULL_EXTEND: u64 = 23;
/// Discriminates IntegrateTrace from IntegrateSink (OPCODE_INTEGRATE=7)
/// without a nullable column.
pub const OPCODE_INTEGRATE_TRACE: u64 = 25;
/// MAP sub-variant: pure projection (column reorder/drop).
pub const OPCODE_MAP_PROJ: u64 = 26;
/// MAP sub-variant: expression program (compute) with optional PK reindex.
pub const OPCODE_MAP_EXPR: u64 = 27;
/// MAP sub-variant: copy all columns to payload, set PK = hash of full row.
pub const OPCODE_MAP_HASH_ROW: u64 = 29;
/// Non-equi (range) join: symmetric delta-trace join whose probe is an ordered
/// half-open range walk over the trace instead of an equal-key seek.
pub const OPCODE_JOIN_DELTA_TRACE_RANGE: u64 = 32;
/// Drop trace rows this worker does not own (**pure** range-join broadcast input;
/// a band join scatters by its eq prefix and omits this node). Worker identity is
/// a compile-time constant, so the node carries no payload.
pub const OPCODE_WORKER_FILTER: u64 = 33;
/// Multiplicity-preserving sibling of DISTINCT: clamps each consolidated
/// (PK, payload)'s net weight to `[0, i64::MAX]` (vs DISTINCT's `[-1, 1]`). The
/// bag preset for EXCEPT ALL / INTERSECT ALL; shares DISTINCT's engine body.
pub const OPCODE_POSITIVE_PART: u64 = 34;

// ---------------------------------------------------------------------------
// Circuit-layer type aliases
// ---------------------------------------------------------------------------

pub type TableId = u64;

// ---------------------------------------------------------------------------
// Port constants
// ---------------------------------------------------------------------------

pub const PORT_IN: u64 = 0;
pub const PORT_TRACE: u64 = 1;
pub const PORT_IN_A: u64 = 0;
pub const PORT_IN_B: u64 = 1;

// ---------------------------------------------------------------------------
// CircuitNodeColumns `kind` discriminator values
// ---------------------------------------------------------------------------
//
// Every "ordered list of column indices" attached to a circuit node lives
// in `CircuitNodeColumns` keyed by (view_id, node_id, kind, position). The
// `kind` discriminator selects which list is being addressed:

pub const NODE_COL_KIND_GROUP: u64 = 0; // REDUCE group-by columns
pub const NODE_COL_KIND_SHARD: u64 = 1; // EXCHANGE_SHARD shard columns
pub const NODE_COL_KIND_PROJ: u64 = 2; // MAP projection columns
pub const NODE_COL_KIND_NULL_EXT: u64 = 3; // NULL_EXTEND payload type codes
pub const NODE_COL_KIND_AGG_SPEC: u64 = 4; // REDUCE aggregate specs (value1=func_id, value2=col_idx)
pub const NODE_COL_KIND_BRANCH_ID: u64 = 5; // MAP_HASH_ROW per-side branch discriminator (value1=branch_id)
pub const NODE_COL_KIND_REINDEX: u64 = 6; // MAP_EXPR equijoin pre-index cols (value1=col_idx, position=key order)
pub const NODE_COL_KIND_RANGE_JOIN: u64 = 7; // JOIN_DELTA_TRACE_RANGE params (value1=n_eq, value2=rel)
pub const NODE_COL_KIND_GLOBAL_GROUND: u64 = 8; // REDUCE global-aggregate ground discriminator (value1=bool)
pub const NODE_COL_KIND_REDUCE_OUT_KEY: u64 = 9; // REDUCE output-key kind (value1=ReduceOutKey); absent ⇒ SyntheticFold
pub const NODE_COL_KIND_SCAN_BOUND: u64 = 10; // SCAN_DELTA backfill-scan index column list (value1=col_idx, position=key order)
pub const NODE_COL_KIND_ROUTE_KEY: u64 = 11; // MAP_EXPR reindex role (value1 = ReindexRole)
pub const NODE_COL_KIND_MAP_OUT_COLS: u64 = 12; // MAP_EXPR compute payload columns (value1=type_code, value2=nullable)

// ---------------------------------------------------------------------------
// Aggregate function IDs
// ---------------------------------------------------------------------------

pub const AGG_COUNT: u64 = 1;
pub const AGG_SUM: u64 = 2;
pub const AGG_MIN: u64 = 3;
pub const AGG_MAX: u64 = 4;
pub const AGG_COUNT_NON_NULL: u64 = 5;
/// `Sum`'s fold (`acc += value × weight`) with `Count`'s `0` identity (grounds to
/// `0`, renders `0` when untouched). The two-phase global-aggregate combine sums
/// per-worker partial COUNT/COUNT_NON_NULL columns with this — a plain `Sum` would
/// render their empty value as NULL instead of `0`.
pub const AGG_SUM_ZERO: u64 = 6;

// ---------------------------------------------------------------------------
// Typed circuit-node representation (shared between gnitz-core and gnitz-engine)
// ---------------------------------------------------------------------------

wire_enum! {
    /// Aggregate function discriminant. Values match the `AGG_*` wire constants.
    pub enum AggFunc: u64 {
        Count = AGG_COUNT,
        Sum = AGG_SUM,
        Min = AGG_MIN,
        Max = AGG_MAX,
        CountNonNull = AGG_COUNT_NON_NULL,
        SumZero = AGG_SUM_ZERO,
    }
}

/// How a reduce maintains one aggregate. The two predicates below are
/// complementary because they read this one classification rather than each
/// carrying its own whitelist, where a new opcode would be silently `false` at
/// both.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AggClass {
    /// `Agg(A + B) == Agg(A) + Agg(B)`: a delta's contribution folds into the
    /// running accumulator with no history replay.
    Linear,
    /// Maintained through the combined aggregate-value index. Retracting the
    /// current extremum needs the next value out of the history, so the index —
    /// not the delta — is the value's source of truth.
    ValueIndexed,
}

impl AggFunc {
    /// This aggregate's maintenance strategy — the one classification both
    /// predicates below read.
    pub const fn class(self) -> AggClass {
        match self {
            AggFunc::Count | AggFunc::Sum | AggFunc::CountNonNull | AggFunc::SumZero => AggClass::Linear,
            AggFunc::Min | AggFunc::Max => AggClass::ValueIndexed,
        }
    }

    /// True iff the aggregate folds a delta with no history replay.
    pub const fn is_linear(self) -> bool {
        matches!(self.class(), AggClass::Linear)
    }

    /// True iff this aggregate is maintained through the combined AggValueIndex.
    /// Also fixes the ordinal order: the engine's index bake selects its entries
    /// by this predicate, in descriptor order.
    pub const fn uses_value_index(self) -> bool {
        matches!(self.class(), AggClass::ValueIndexed)
    }

    /// True iff an untouched accumulator renders a concrete `0` rather than
    /// NULL — the zero-identity family. COUNT / COUNT_NON_NULL count rows
    /// (empty = 0); SumZero is Sum's fold under Count's `0` identity (the
    /// two-phase partial-count combine). SUM / MIN / MAX have a NULL empty
    /// value.
    pub const fn empty_renders_zero(self) -> bool {
        matches!(self, AggFunc::Count | AggFunc::CountNonNull | AggFunc::SumZero)
    }

    /// True iff a reduce's **raw** output column for this aggregate can render
    /// NULL, and must therefore be declared nullable — THE shared rule, and the
    /// declaration half of the null-bit rule `emit_agg_col` writes
    /// (`is_untouched() && !empty_renders_zero()`).
    ///
    /// A row carries an untouched accumulator only when the aggregate's source
    /// column is nullable (the null gate is the group walk's one skip) or when
    /// the group set is empty, where the ground row stands in for a
    /// never-populated / fully-retracted source. A surviving *group* is never
    /// null-filled from emptiness — it is retracted instead.
    ///
    /// Its two consumers must never drift: the engine's physical reduce output
    /// schema (`build_reduce_output_schema`, where a NOT NULL declaration puts
    /// the row on a null-blind comparator that would rank a NULL cell as a real
    /// `0`) and the planner's finalize-level nullability (`agg_output_nullable`,
    /// which widens this for the companion-carrying shapes).
    pub const fn raw_output_nullable(self, src_nullable: bool, ungrouped: bool) -> bool {
        !self.empty_renders_zero() && (src_nullable || ungrouped)
    }

    /// The aggregate that merges this aggregate's per-worker **partials** —
    /// THE shared combine rule (its two consumers must never drift: the
    /// planner's two-phase global-aggregate combine reduce, and the ad-hoc
    /// fold's client-side cross-worker combiner). COUNT/COUNT_NON_NULL
    /// partials sum with `SumZero` (a count's empty value is 0, not NULL);
    /// SUM partials sum with plain `Sum` (NULL ground); MIN/MAX partials
    /// merge by re-applying themselves.
    pub fn merge_func(self) -> AggFunc {
        match self {
            AggFunc::Count | AggFunc::CountNonNull | AggFunc::SumZero => AggFunc::SumZero,
            AggFunc::Sum => AggFunc::Sum,
            AggFunc::Min => AggFunc::Min,
            AggFunc::Max => AggFunc::Max,
        }
    }
}

/// Output type code of an aggregate over a source column of type `src_tc` —
/// THE shared planner/engine typing rule (a mismatch would silently scramble a
/// view's output column positions, widths, and types).
///   COUNT, COUNT_NON_NULL → I64 (SUM_ZERO sums integer count/sum columns and
///   likewise produces I64)
///   SUM on float → F64, else I64  (SUM can overflow the source width)
///   MIN/MAX on float → F64; on a ≤8-byte integer → that source type; else I64
///
/// MIN/MAX *select* an existing row, so a ≤8-byte integer extremum is itself a
/// value of the source type T and is always representable in it — `MIN(INT)` is
/// `INT`, `MAX(SMALLINT UNSIGNED)` is `SMALLINT UNSIGNED`, etc. Those keep their
/// own type: the engine's row emitters serialize the accumulator at the output
/// column width, so a narrow column writes the correct low bytes, and the
/// width-gated trace read-back reconstructs the 8-byte accumulator from that
/// width. Any non-float, non-≤8-byte-integer source (STRING / 16-byte types)
/// falls to the `I64` arm, but only as a total-function default: a MIN/MAX over
/// such a source is rejected at compile — by the SQL binder upstream, and by
/// the engine's order-encodability guard on the low-level circuit API that
/// bypasses the binder — so that arm never reaches execution. SUM over a U64
/// source is typed **U64**: the engine's i64 `wrapping_add` accumulator's bit
/// pattern already *is* the true sum mod 2^64, same 8-byte width, so the label
/// is the only choice — and U64 lets a downstream unsigned compare re-seed
/// correctly (like MIN/MAX preserving their source type). A narrow unsigned
/// source (U8/U16/U32) still widens to I64 (its sum stays < 2^63, so signed
/// order is correct). AVG is planner-lowered (SUM/COUNT + finalize divide)
/// before the wire and never reaches this rule.
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
    /// How a `Reduce` node keys its output. **Decided once, by the circuit
    /// author** (the SQL planner, which tracks schemas) via
    /// [`ReduceOutKey::decide`], shipped on the wire, and *validated* — never
    /// re-decided — by the engine compiler: a shipped kind that differs from what
    /// the input schema warrants under the same `decide` chain is a hard compile
    /// rejection, so the output column layout can never silently scramble.
    /// Everything downstream of that validation (output schema construction,
    /// runtime row keying) obeys the kind rather than re-deriving it.
    ///
    /// On the wire the kind rides as one param row
    /// (`NODE_COL_KIND_REDUCE_OUT_KEY`), present iff not `SyntheticFold` — the
    /// sparse-default param-row idiom (`NODE_COL_KIND_GLOBAL_GROUND` works the
    /// same way).
    pub enum ReduceOutKey: u64 {
        /// Leading synthetic `_group_pk` U128 = null-distinct group fold; the
        /// group columns ride as payload. What every group set that is neither
        /// natural kind gets — including the empty (global) group set and the
        /// schema-blind low-level `CircuitBuilder::reduce` surface.
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
    /// The one precedence chain (eq-PK ▷ single-natural ▷ synthetic). Planner
    /// decision and engine validation both route through here, each feeding its
    /// own schema representation's two predicates, so the sides cannot drift.
    /// The whole decision from the facts both sides already hold: the source's
    /// PK column list, the GROUP BY column list, and a `(type_code, nullable)`
    /// reader for a group column.
    ///
    /// The planner decides with this and ships the verdict; the engine
    /// re-derives it here to *validate* what was shipped, and rejects the
    /// circuit on a mismatch. Since a disagreement is what that check is
    /// looking for, the two predicates it compares — the PK-permutation test and
    /// the single-natural-column test — must not themselves be written twice,
    /// or the check compares two independent implementations rather than one
    /// rule against one transmission.
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
        Self::decide(eq_pk, single_natural)
    }

    /// The output layout this kind selects, up to the aggregate columns each side
    /// types for itself.
    ///
    /// Both sides must lay a reduce's output out identically — the planner
    /// declares the view's schema, the engine builds the batch — so the layout
    /// lives beside the decision that picks it ([`Self::for_group_cols`]) rather
    /// than as two matches kept in step by hand.
    ///
    /// `SingleNaturalCol` names `group_cols[0]`, which `for_group_cols` only
    /// selects for a single-column group set; the engine validates the transmitted
    /// kind against the input schema before laying anything out.
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

    pub fn decide(group_cols_eq_pk: bool, single_col_natural: bool) -> Self {
        if group_cols_eq_pk {
            ReduceOutKey::PkPermutation
        } else if single_col_natural {
            ReduceOutKey::SingleNaturalCol
        } else {
            ReduceOutKey::SyntheticFold
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
    DeltaTraceRange { n_eq: u8, rel: RangeRel },
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

/// MAP sub-variant discriminant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MapKind {
    /// Pure projection/column-reorder. Carries payload column indices to keep.
    Projection(Vec<u32>),
    /// Computed projection (`SELECT a + b`). `program` is an opaque `ExprProgram`
    /// blob writing one payload slot each; `out_cols` declares those slots as
    /// `(type_code, nullable)` in payload order. The PK region is inherited from
    /// the input verbatim, so it is not listed.
    ///
    /// The declared columns travel because a computed projection has no copy list
    /// the engine could derive a schema from — `payload_copy_srcs` is `None` for
    /// `a + b`. `MapPlan::from_map` validates the program against them.
    Compute {
        program: Vec<u8>,
        out_cols: Vec<(u8, bool)>,
    },
    /// Re-key: `reindex_cols` lists the source columns, in key order, that become
    /// the synthetic PK for equijoin/group pre-indexing, and `program` is the copy
    /// list placing the surviving payload behind them. Never empty — a map that
    /// re-keys nothing is a [`MapKind::Compute`].
    ///
    /// `reindex_target_tcs` is parallel to `reindex_cols`. Entry `i` is the
    /// promoted key type code `T` for slot `i` of a cross-width equijoin key, or
    /// `0` meaning "derive the slot type from the source column" (the same-type
    /// path, byte-identical to non-promoted circuits).
    Reindex {
        program: Vec<u8>,
        reindex_cols: Vec<u32>,
        reindex_target_tcs: Vec<u8>,
        role: ReindexRole,
    },
    /// Full-row-identity reindex. Like `Projection` (keep the listed columns as
    /// payload, in order), but the synthetic PK is set to a hash of the kept
    /// payload bytes. Used by EXCEPT/INTERSECT/DISTINCT so set membership is
    /// decided by the projected row content, not by the source PK.
    ///
    /// `target_tcs` is parallel to the projection columns (`0` = keep the source
    /// column's type). A non-zero entry `i` is the promoted payload type code the
    /// widening projection coerces column `i` into, so a cross-width set-op pair
    /// (e.g. `I32 UNION I64`) gives equal logical values one physical layout for
    /// the content hash and the downstream union/positive_part merge. It is
    /// always a ≤8-byte fixed-width integer; all-zero for same-type set-ops and
    /// `SELECT DISTINCT`.
    ///
    /// The `branch_id` is a per-side discriminator mixed into the hash so that
    /// identical payloads on the left vs right branch of a `UNION ALL` get
    /// distinct synthetic PKs (and therefore accumulate weight +2 rather than
    /// collapsing). Deduplicating set-ops (UNION/EXCEPT/INTERSECT) use 0 on both
    /// sides; UNION ALL uses 0 on the left and 1 on the right.
    HashRow(Vec<u32>, Vec<u8>, u8),
}

/// A secondary-index range bound for a `ScanDelta`'s backfill scan: the index's
/// declared column list and the half-open range over its leading columns.
/// Resolved server-side against the source table's index circuits.
///
/// A **physical access hint, never a semantic filter**. The circuit's `Filter`
/// still carries the full predicate, so a bounded and an unbounded scan produce
/// the same view — a bound only narrows which rows the initial scan reads. That
/// is what lets every consumer degrade to a full scan (a dropped index, an
/// unselective range, a malformed wire row) without changing results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanBound {
    pub idx_cols: crate::PkColList,
    pub desc: crate::RangeDescriptor,
}

/// Typed operator-node payload. Expression blobs are stored as raw `Vec<u8>` and decoded
/// with `gnitz_wire::decode_expr_blob`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpNode {
    /// `OPCODE_SCAN_DELTA = 11`. Delta input for `source`.
    ///
    /// `bound` is a **backfill-scan hint only**: steady-state deltas never open
    /// the source cursor and ignore it entirely. A non-`None` bound narrows the
    /// initial full-source scan to a secondary-index range, leaving the
    /// downstream `Filter` — and therefore the view — unchanged.
    ScanDelta {
        source: TableId,
        bound: Option<ScanBound>,
    },
    /// `OPCODE_FILTER = 1`. Optional expression predicate blob.
    Filter(Option<Vec<u8>>),
    Map(MapKind),
    Negate,
    Union,
    Distinct,
    /// `OPCODE_POSITIVE_PART = 34`. Multiplicity-preserving counterpart to
    /// `Distinct`: per consolidated (PK, payload) emits
    /// `clamp(w_new, 0, i64::MAX) − clamp(w_old, 0, i64::MAX)`, where `Distinct`
    /// clamps to `[-1, 1]`. Shares `Distinct`'s engine body; the bag preset for
    /// `EXCEPT ALL = positive_part(A − B)` and `INTERSECT ALL = A − positive_part(A − B)`.
    PositivePart,
    Reduce {
        group_cols: Vec<u32>,
        /// Aggregate specs `(func, source column)`. Never empty: a spec-less
        /// REDUCE is rejected at decode (every producer ships at least one —
        /// the SQL planner injects a companion COUNT for group-only reduces).
        agg: Vec<(AggFunc, u32)>,
        /// True only for the user's ungrouped (global) scalar aggregate — the
        /// reduce that must emit exactly one row over an empty/fully-retracted
        /// source (COUNT(*)=0, SUM/MIN/MAX/AVG=NULL). A **SQL-intent
        /// discriminator**, not a Z-set property: the LEFT range-join's threshold
        /// reduce (`reduce_multi_local`) also has empty group cols but must NOT
        /// seed a ground row, so the flag cannot be derived from
        /// `group_cols.is_empty()` and travels explicitly from the planner.
        global_ground: bool,
        /// How the output is keyed. Decided by the planner, validated (never
        /// re-decided) by the engine compiler. Wire-absent ⇒ `SyntheticFold`.
        out_key: ReduceOutKey,
    },
    Join(JoinKind),
    /// `OPCODE_INTEGRATE = 7`. Primary INTEGRATE: writes to view storage.
    IntegrateSink,
    /// `OPCODE_INTEGRATE_TRACE = 25`. Accumulates Z-set for join trace.
    IntegrateTrace,
    ExchangeShard {
        shard_cols: Vec<u32>,
    },
    NullExtend {
        type_codes: Vec<u8>,
    },
    /// `OPCODE_WORKER_FILTER = 33`. Keep only rows whose packed-PK partition is
    /// owned by this worker (**pure** range-join broadcast input; a band join
    /// scatters by its eq prefix and omits this node). Worker identity is a
    /// compile-time constant, so no payload travels on the wire.
    WorkerFilter,
}

impl OpNode {
    /// The input ports this operator is wired on, in port order. A function of
    /// the variant alone, so a circuit's whole edge set can be held to it once at
    /// load instead of every consumer re-checking arity where it reads an operand.
    ///
    /// `PORT_IN == PORT_IN_A == 0` and `PORT_TRACE == PORT_IN_B == 1`, so a
    /// binary operator's set is `[0, 1]` however its ports are spelled.
    pub const fn ports(&self) -> &'static [u64] {
        match self {
            // The circuit's own input: fed by the source drive, not by an edge.
            OpNode::ScanDelta { .. } => &[],
            OpNode::Union => &[PORT_IN_A, PORT_IN_B],
            OpNode::Join(_) => &[PORT_IN_A, PORT_TRACE],
            _ => &[PORT_IN],
        }
    }
}

/// One decoded row of the `CircuitNodeColumns` system table for a single node,
/// sorted by (kind, position). `value1`/`value2` are interpreted per `kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CircuitNodeColumn {
    pub kind: u64,
    pub position: u16,
    pub value1: u64,
    pub value2: u64,
}

/// This node's rows of one `kind`, in `position` order.
///
/// Column order is semantic for every list kind (`group_cols`, `shard_cols`,
/// `proj_cols`, `reindex_cols`, the NULL_EXTEND type codes, the SCAN_BOUND
/// column list), and `position` is what carries it. Ordering here — rather than
/// making pre-sorted input an unstated precondition — puts the invariant in the
/// one function that depends on it, so no caller has to know it exists.
fn rows_of(cols: &[CircuitNodeColumn], kind: u64) -> Vec<&CircuitNodeColumn> {
    let mut rows: Vec<&CircuitNodeColumn> = cols.iter().filter(|c| c.kind == kind).collect();
    rows.sort_by_key(|c| c.position);
    rows
}

/// Collect one column kind's `(value1, value2)` rows into parallel
/// (source column index, promoted target type code) vectors, in column order.
/// `value2 == 0` means "keep/derive from the source type"; a non-zero target
/// must satisfy `valid` — this decode is the trust boundary where catalog bytes
/// become a typed node, so a bogus target is rejected here rather than left to
/// drive a wrong slot width downstream.
fn collect_cols_with_tcs(
    cols: &[CircuitNodeColumn],
    kind: u64,
    valid: fn(u8) -> bool,
    err: impl Fn(u8) -> String,
) -> Result<(Vec<u32>, Vec<u8>), String> {
    let mut out_cols: Vec<u32> = Vec::new();
    let mut out_tcs: Vec<u8> = Vec::new();
    for c in rows_of(cols, kind) {
        let tc = c.value2 as u8;
        if tc != 0 && !valid(tc) {
            return Err(err(tc));
        }
        out_cols.push(c.value1 as u32);
        out_tcs.push(tc);
    }
    Ok((out_cols, out_tcs))
}

/// `(opcode, source_table, expr_program_blob)` — the `nodes` system-table row
/// fields excluding the node id. The reindex column list is stored separately
/// in `CircuitNodeColumns` under `NODE_COL_KIND_REINDEX`.
pub type NodeFields = (u64, Option<TableId>, Option<Vec<u8>>);

/// One `node_columns` system-table row payload:
/// `(kind, position, value1, value2)` — the node id is prepended by the caller.
pub type NodeColumnPayload = (u64, u16, u64, u64);

fn encode_col_list<I, T>(kind: u64, iter: I) -> Vec<NodeColumnPayload>
where
    I: IntoIterator<Item = T>,
    T: Into<u64>,
{
    iter.into_iter()
        .enumerate()
        .map(|(i, v)| (kind, i as u16, v.into(), 0u64))
        .collect()
}

/// Like [`encode_col_list`], but carries a per-column promoted target type code
/// in `value2` (0 = keep/derive from the source type). The two vectors are
/// parallel: a caller with nothing to promote passes a zero vector of the same
/// length, never an empty one. Padding a short one would make two unequal nodes
/// encode to identical bytes.
fn encode_col_list_with_tcs(kind: u64, cols: &[u32], target_tcs: &[u8]) -> Vec<NodeColumnPayload> {
    debug_assert_eq!(cols.len(), target_tcs.len(), "promoted target tcs run parallel to cols");
    cols.iter()
        .enumerate()
        .map(|(i, &col)| (kind, i as u16, col as u64, target_tcs[i] as u64))
        .collect()
}

/// Encode a typed `OpNode` into its `nodes`-row fields + `node_columns` payload
/// rows — the inverse of [`decode_op_node`]. The `ExprProgram` blob is carried
/// opaquely (each crate encodes it with its own encoder before building the
/// `OpNode`).
pub fn encode_op_node(op: OpNode) -> (NodeFields, Vec<NodeColumnPayload>) {
    match op {
        // An unbounded `ScanDelta` carries no param rows and no blob: the common
        // shape costs nothing, and "absent" and "empty" stay the same bytes.
        OpNode::ScanDelta { source, bound: None } => ((OPCODE_SCAN_DELTA, Some(source), None), Vec::new()),
        OpNode::ScanDelta { source, bound: Some(b) } => (
            (OPCODE_SCAN_DELTA, Some(source), Some(b.desc.encode())),
            encode_col_list(NODE_COL_KIND_SCAN_BOUND, b.idx_cols.as_slice().iter().copied()),
        ),
        OpNode::Filter(blob) => ((OPCODE_FILTER, None, blob), Vec::new()),
        OpNode::Map(MapKind::Projection(cols)) => {
            ((OPCODE_MAP_PROJ, None, None), encode_col_list(NODE_COL_KIND_PROJ, cols))
        }
        OpNode::Map(MapKind::Compute { program, out_cols }) => {
            let kind_rows = out_cols
                .into_iter()
                .enumerate()
                .map(|(i, (tc, nullable))| (NODE_COL_KIND_MAP_OUT_COLS, i as u16, tc as u64, nullable as u64))
                .collect();
            ((OPCODE_MAP_EXPR, None, Some(program)), kind_rows)
        }
        OpNode::Map(MapKind::Reindex {
            program,
            reindex_cols,
            reindex_target_tcs,
            role,
        }) => {
            let mut kind_rows = encode_col_list_with_tcs(NODE_COL_KIND_REINDEX, &reindex_cols, &reindex_target_tcs);
            // Written for both roles, unlike the sparse GLOBAL_GROUND /
            // REDUCE_OUT_KEY rows: the decode rejects an absent row rather than
            // guessing which side of the routing boundary the node sits on.
            kind_rows.push((NODE_COL_KIND_ROUTE_KEY, 0, role.as_wire(), 0));
            ((OPCODE_MAP_EXPR, None, Some(program)), kind_rows)
        }
        OpNode::Map(MapKind::HashRow(cols, target_tcs, branch_id)) => {
            let mut kind_rows = encode_col_list_with_tcs(NODE_COL_KIND_PROJ, &cols, &target_tcs);
            // Always written, unlike the sparse GLOBAL_GROUND / REDUCE_OUT_KEY
            // rows: a branch id of 0 is a real branch, not an absence.
            kind_rows.push((NODE_COL_KIND_BRANCH_ID, 0, branch_id as u64, 0));
            ((OPCODE_MAP_HASH_ROW, None, None), kind_rows)
        }
        OpNode::Negate => ((OPCODE_NEGATE, None, None), Vec::new()),
        OpNode::Union => ((OPCODE_UNION, None, None), Vec::new()),
        OpNode::Distinct => ((OPCODE_DISTINCT, None, None), Vec::new()),
        OpNode::PositivePart => ((OPCODE_POSITIVE_PART, None, None), Vec::new()),
        OpNode::Reduce {
            group_cols,
            agg,
            global_ground,
            out_key,
        } => {
            let mut kind_rows = encode_col_list(NODE_COL_KIND_GROUP, group_cols);
            kind_rows.reserve(agg.len() + 2);
            for (i, (func, col)) in agg.into_iter().enumerate() {
                kind_rows.push((NODE_COL_KIND_AGG_SPEC, i as u16, func.as_wire(), col as u64));
            }
            // Only the user's global scalar aggregate carries the row; an
            // ordinary grouped / range-join reduce omits it and decodes to `false`.
            if global_ground {
                kind_rows.push((NODE_COL_KIND_GLOBAL_GROUND, 0, 1, 0));
            }
            // Sparse-default param row (the GLOBAL_GROUND idiom above): present
            // iff not `SyntheticFold`; an absent row decodes to the default.
            if out_key != ReduceOutKey::SyntheticFold {
                kind_rows.push((NODE_COL_KIND_REDUCE_OUT_KEY, 0, out_key.as_wire(), 0));
            }
            ((OPCODE_REDUCE, None, None), kind_rows)
        }
        OpNode::Join(JoinKind::DeltaTrace) => ((OPCODE_JOIN_DELTA_TRACE, None, None), Vec::new()),
        OpNode::Join(JoinKind::DeltaTraceRange { n_eq, rel }) => (
            (OPCODE_JOIN_DELTA_TRACE_RANGE, None, None),
            vec![(NODE_COL_KIND_RANGE_JOIN, 0, n_eq as u64, rel.as_wire())],
        ),
        OpNode::IntegrateSink => ((OPCODE_INTEGRATE, None, None), Vec::new()),
        OpNode::IntegrateTrace => ((OPCODE_INTEGRATE_TRACE, None, None), Vec::new()),
        OpNode::ExchangeShard { shard_cols } => (
            (OPCODE_EXCHANGE_SHARD, None, None),
            encode_col_list(NODE_COL_KIND_SHARD, shard_cols),
        ),
        OpNode::NullExtend { type_codes } => (
            (OPCODE_NULL_EXTEND, None, None),
            encode_col_list(NODE_COL_KIND_NULL_EXT, type_codes),
        ),
        OpNode::WorkerFilter => ((OPCODE_WORKER_FILTER, None, None), Vec::new()),
    }
}

/// Reconstruct an `OpNode` from the three-table row bundle.
///
/// `cols` is the sorted (kind, position, value1, value2) slice for this node,
/// pre-filtered to the current `node_id`. `expr_blob` is stored as-is without
/// any attempt to decode the `ExprProgram` — callers do that on their side of
/// the crate boundary.
pub fn decode_op_node(
    opcode: u64,
    src_tab: Option<TableId>,
    expr_blob: Option<Vec<u8>>,
    cols: &[CircuitNodeColumn],
) -> Result<OpNode, String> {
    let collect_cols = |kind: u64| -> Vec<u32> { rows_of(cols, kind).iter().map(|c| c.value1 as u32).collect() };
    // The null-fill type codes become schema columns verbatim, so this decode is
    // their trust boundary (see `is_valid_type_code` for why an unknown code is
    // not inert) — the same rule `collect_cols_with_tcs` applies to a carried
    // promotion target.
    let collect_typecodes = |kind: u64| -> Result<Vec<u8>, String> {
        rows_of(cols, kind)
            .iter()
            .map(|c| {
                let tc = c.value1 as u8;
                match crate::is_valid_type_code(tc) {
                    true => Ok(tc),
                    false => Err(format!("NULL_EXTEND: invalid column type code {tc}")),
                }
            })
            .collect()
    };
    let collect_aggs = || -> Result<Vec<(AggFunc, u32)>, String> {
        rows_of(cols, NODE_COL_KIND_AGG_SPEC)
            .iter()
            .map(|c| {
                AggFunc::from_wire(c.value1)
                    .ok_or_else(|| format!("unknown agg func id {}", c.value1))
                    .map(|f| (f, c.value2 as u32))
            })
            .collect()
    };
    Ok(match opcode {
        OPCODE_SCAN_DELTA => {
            let source = src_tab.ok_or_else(|| "SCAN_DELTA missing source_table".to_string())?;
            // A malformed bound degrades to `None` — it is NEVER an `Err`. The bound
            // decides scan speed, never correctness (the `Filter` is authoritative), and
            // `assemble_circuit` aborts the whole view load on any node `Err` — so
            // erroring here would let one corrupt hint row make a stored view
            // unloadable. An absent list, an over-long one, a missing blob, and an
            // undecodable descriptor all mean the same thing: no usable hint.
            let bound = crate::PkColList::try_from_slice(&collect_cols(NODE_COL_KIND_SCAN_BOUND))
                .zip(
                    expr_blob
                        .as_deref()
                        .and_then(|b| crate::RangeDescriptor::decode(b).ok()),
                )
                .map(|(idx_cols, desc)| ScanBound { idx_cols, desc });
            OpNode::ScanDelta { source, bound }
        }
        OPCODE_FILTER => OpNode::Filter(expr_blob),
        OPCODE_MAP_PROJ => OpNode::Map(MapKind::Projection(collect_cols(NODE_COL_KIND_PROJ))),
        OPCODE_MAP_EXPR => {
            let program = expr_blob.ok_or_else(|| "MAP_EXPR missing expr_program blob".to_string())?;
            // Reject a non-zero target that is not PK-eligible: the reindex
            // targets flow into the 16-byte-capable OPK promoter
            // (`encode_pk_column_promoted`), so its trust boundary admits exactly
            // that domain.
            let (reindex_cols, reindex_target_tcs) =
                collect_cols_with_tcs(cols, NODE_COL_KIND_REINDEX, crate::is_pk_eligible, |tc| {
                    format!("MAP_EXPR reindex target type code {tc} is not PK-eligible")
                })?;
            // The reindex columns are the discriminator: a map that re-keys nothing
            // is a computed projection, which is why the two cannot be confused.
            if reindex_cols.is_empty() {
                let out_cols = rows_of(cols, NODE_COL_KIND_MAP_OUT_COLS)
                    .iter()
                    .map(|c| {
                        let tc = c.value1 as u8;
                        match crate::is_valid_type_code(tc) {
                            true => Ok((tc, c.value2 != 0)),
                            false => Err(format!("MAP_EXPR output column type code {tc} is invalid")),
                        }
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                return Ok(OpNode::Map(MapKind::Compute { program, out_cols }));
            }
            // An `Err` rather than a default, unlike SCAN_BOUND above: that bound
            // decides scan speed and never correctness, so one corrupt hint row
            // must not make a stored view unloadable. The role decides which
            // worker a row lands on — so a missing row and an unreadable value are
            // both refusals.
            let role_row = cols
                .iter()
                .find(|c| c.kind == NODE_COL_KIND_ROUTE_KEY)
                .ok_or_else(|| "MAP_EXPR missing its route-key row".to_string())?;
            let role = ReindexRole::from_wire(role_row.value1)
                .ok_or_else(|| format!("MAP_EXPR unknown route-key role {}", role_row.value1))?;
            OpNode::Map(MapKind::Reindex {
                program,
                reindex_cols,
                reindex_target_tcs,
                role,
            })
        }
        OPCODE_MAP_HASH_ROW => {
            let branch_id = cols
                .iter()
                .find(|c| c.kind == NODE_COL_KIND_BRANCH_ID)
                .map(|c| c.value1 as u8)
                .unwrap_or(0);
            // The promotion domain is a ≤8-byte fixed-width integer — stricter
            // than the Expression reindex's `is_pk_eligible`, which also admits
            // the 16-byte U128/UUID/I128 that the payload widen in `copy_column`
            // cannot hold.
            let (proj_cols, target_tcs) = collect_cols_with_tcs(cols, NODE_COL_KIND_PROJ, crate::is_fixed_int, |tc| {
                format!("MAP_HASH_ROW target type code {tc} is not a fixed-width integer")
            })?;
            OpNode::Map(MapKind::HashRow(proj_cols, target_tcs, branch_id))
        }
        OPCODE_NEGATE => OpNode::Negate,
        OPCODE_UNION => OpNode::Union,
        OPCODE_DISTINCT => OpNode::Distinct,
        OPCODE_POSITIVE_PART => OpNode::PositivePart,
        OPCODE_REDUCE => {
            let group_cols = collect_cols(NODE_COL_KIND_GROUP);
            let agg = collect_aggs()?;
            if agg.is_empty() {
                return Err("REDUCE node carries no aggregate spec".to_string());
            }
            // Absent row ⇒ `false` (the ordinary grouped / range-join reduce); a
            // present row carries the global-aggregate intent in value1. One param
            // row, exactly like NODE_COL_KIND_RANGE_JOIN.
            let global_ground = cols
                .iter()
                .find(|c| c.kind == NODE_COL_KIND_GLOBAL_GROUND)
                .is_some_and(|c| c.value1 != 0);
            // Absent row ⇒ `SyntheticFold` (the low-level `reduce` surface and
            // every payload/empty-group planner reduce); a present row carries a
            // natural-key kind in value1. An unknown value1 is a corrupt circuit —
            // reject at this trust boundary rather than mis-keying the output.
            let out_key = match cols.iter().find(|c| c.kind == NODE_COL_KIND_REDUCE_OUT_KEY) {
                Some(c) => ReduceOutKey::from_wire(c.value1)
                    .ok_or_else(|| format!("REDUCE unknown out_key kind {}", c.value1))?,
                None => ReduceOutKey::SyntheticFold,
            };
            // `global_ground` and the group columns arrive independently, and the
            // ground row is only well-formed group-less: `emit_global_ground`
            // writes the aggregate columns at payload index 0, which with a
            // non-empty group set overwrites the exemplar slots and leaves their
            // regions short — a malformed batch in release. The implication runs
            // one way only (`ground ⇒ empty`; a threshold reduce and a two-phase
            // phase-1 partial are group-less with `global_ground = false`), so the
            // flag cannot be derived — hence a cross-check and not a derivation.
            if global_ground && !group_cols.is_empty() {
                return Err("REDUCE is global-ground over a non-empty group set".to_string());
            }
            OpNode::Reduce {
                group_cols,
                agg,
                global_ground,
                out_key,
            }
        }
        OPCODE_JOIN_DELTA_TRACE => OpNode::Join(JoinKind::DeltaTrace),
        OPCODE_JOIN_DELTA_TRACE_RANGE => {
            // n_eq + rel ride in a single NODE_COL_KIND_RANGE_JOIN row. Reject a
            // missing row or an unknown rel at this decode trust boundary rather
            // than letting a bogus probe shape survive downstream.
            let row = cols
                .iter()
                .find(|c| c.kind == NODE_COL_KIND_RANGE_JOIN)
                .ok_or_else(|| "JOIN_DELTA_TRACE_RANGE missing range-join param row".to_string())?;
            let n_eq = row.value1 as u8;
            let rel = RangeRel::from_wire(row.value2)
                .ok_or_else(|| format!("JOIN_DELTA_TRACE_RANGE unknown rel {}", row.value2))?;
            OpNode::Join(JoinKind::DeltaTraceRange { n_eq, rel })
        }
        OPCODE_INTEGRATE => OpNode::IntegrateSink,
        OPCODE_INTEGRATE_TRACE => OpNode::IntegrateTrace,
        OPCODE_EXCHANGE_SHARD => OpNode::ExchangeShard {
            shard_cols: collect_cols(NODE_COL_KIND_SHARD),
        },
        OPCODE_NULL_EXTEND => OpNode::NullExtend {
            type_codes: collect_typecodes(NODE_COL_KIND_NULL_EXT)?,
        },
        OPCODE_WORKER_FILTER => OpNode::WorkerFilter,
        _ => return Err(format!("unknown opcode {opcode}")),
    })
}

#[cfg(test)]
#[path = "tests/circuit.rs"]
mod tests;
