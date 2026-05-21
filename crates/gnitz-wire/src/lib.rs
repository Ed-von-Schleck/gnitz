//! Shared wire-protocol definitions for GnitzDB.
//!
//! Single source of truth for constants and codecs that both the client
//! (gnitz-protocol, gnitz-core) and server (gnitz-engine) must agree on.

#[cfg(not(target_endian = "little"))]
compile_error!("GnitzDB requires a little-endian target; the wire format is LE-only.");

/// Re-declare gnitz-wire constants as a different integer type.
///
/// ```ignore
/// gnitz_wire::cast_consts! { i32;
///     OPCODE_FILTER, OPCODE_MAP, OPCODE_NEGATE,
/// }
/// // expands to:
/// // const OPCODE_FILTER: i32 = gnitz_wire::OPCODE_FILTER as i32;
/// // const OPCODE_MAP: i32 = gnitz_wire::OPCODE_MAP as i32;
/// // ...
/// ```
///
/// Use `pub` visibility with: `gnitz_wire::cast_consts! { pub i64; ... }`
#[macro_export]
macro_rules! cast_consts {
    ($vis:vis $ty:ty; $($name:ident),+ $(,)?) => {
        $($vis const $name: $ty = $crate::$name as $ty;)+
    };
}

// ---------------------------------------------------------------------------
// Type codes
// ---------------------------------------------------------------------------

#[allow(dead_code)]
pub mod type_code {
    pub const U8:     u8 = 1;
    pub const I8:     u8 = 2;
    pub const U16:    u8 = 3;
    pub const I16:    u8 = 4;
    pub const U32:    u8 = 5;
    pub const I32:    u8 = 6;
    pub const F32:    u8 = 7;
    pub const U64:    u8 = 8;
    pub const I64:    u8 = 9;
    pub const F64:    u8 = 10;
    pub const STRING: u8 = 11;
    pub const U128:   u8 = 12;
    pub const UUID:   u8 = 13;
    pub const BLOB:   u8 = 14;
}

/// Typed column type code enum, mirroring the `type_code::*` constants.
///
/// `#[repr(u8)]` — discriminants equal the corresponding `type_code::*` constant.
/// Stored as `u8` on disk (`SchemaColumn.type_code`); use `from_validated_u8` to
/// convert in-memory data that has already passed DDL validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TypeCode {
    U8     = type_code::U8,
    I8     = type_code::I8,
    U16    = type_code::U16,
    I16    = type_code::I16,
    U32    = type_code::U32,
    I32    = type_code::I32,
    F32    = type_code::F32,
    U64    = type_code::U64,
    I64    = type_code::I64,
    F64    = type_code::F64,
    String = type_code::STRING,
    U128   = type_code::U128,
    UUID   = type_code::UUID,
    Blob   = type_code::BLOB,
}

impl TypeCode {
    /// Convert a wire u8 that has already passed DDL validation. Panics on unknown codes.
    #[inline]
    pub fn from_validated_u8(v: u8) -> Self {
        Self::try_from_u8(v)
            .unwrap_or_else(|| panic!("invalid type_code {} in validated schema", v))
    }

    /// Convert a raw u8 wire value. Returns `None` for unknown codes.
    #[inline]
    pub fn try_from_u8(v: u8) -> Option<Self> {
        use type_code as tc;
        match v {
            tc::U8     => Some(TypeCode::U8),
            tc::I8     => Some(TypeCode::I8),
            tc::U16    => Some(TypeCode::U16),
            tc::I16    => Some(TypeCode::I16),
            tc::U32    => Some(TypeCode::U32),
            tc::I32    => Some(TypeCode::I32),
            tc::F32    => Some(TypeCode::F32),
            tc::U64    => Some(TypeCode::U64),
            tc::I64    => Some(TypeCode::I64),
            tc::F64    => Some(TypeCode::F64),
            tc::STRING => Some(TypeCode::String),
            tc::U128   => Some(TypeCode::U128),
            tc::UUID   => Some(TypeCode::UUID),
            tc::BLOB   => Some(TypeCode::Blob),
            _          => None,
        }
    }

    /// Byte stride of this type in a column payload.
    pub const fn stride(&self) -> u8 {
        match self {
            TypeCode::U8  | TypeCode::I8  => 1,
            TypeCode::U16 | TypeCode::I16 => 2,
            TypeCode::F32 | TypeCode::U32 | TypeCode::I32 => 4,
            TypeCode::F64 | TypeCode::U64 | TypeCode::I64 => 8,
            TypeCode::U128 | TypeCode::UUID | TypeCode::String | TypeCode::Blob => 16,
        }
    }

    pub const fn is_float(&self) -> bool {
        matches!(self, TypeCode::F32 | TypeCode::F64)
    }

    /// Wire stride in bytes. Alias for `stride()` returning `usize`.
    #[inline]
    pub fn wire_stride(self) -> usize {
        self.stride() as usize
    }
}

/// Wire stride (byte width) for a column type code.
/// Returns 8 for unknown codes (engine compare_rows depends on this default).
pub const fn wire_stride(tc: u8) -> usize {
    match tc {
        1 | 2           => 1,   // U8, I8
        3 | 4           => 2,   // U16, I16
        5..=7           => 4,   // U32, I32, F32
        8..=10          => 8,   // U64, I64, F64
        11..=14           => 16,  // STRING, U128, UUID, BLOB
        _                 => 8,
    }
}

// ---------------------------------------------------------------------------
// Expression bytecode opcodes
// ---------------------------------------------------------------------------

pub const EXPR_LOAD_COL_INT:     u32 = 1;
pub const EXPR_LOAD_COL_FLOAT:   u32 = 2;
pub const EXPR_LOAD_CONST:       u32 = 3;
pub const EXPR_INT_ADD:          u32 = 4;
pub const EXPR_INT_SUB:          u32 = 5;
pub const EXPR_INT_MUL:          u32 = 6;
pub const EXPR_INT_DIV:          u32 = 7;
pub const EXPR_INT_MOD:          u32 = 8;
pub const EXPR_INT_NEG:          u32 = 9;
pub const EXPR_FLOAT_ADD:        u32 = 10;
pub const EXPR_FLOAT_SUB:        u32 = 11;
pub const EXPR_FLOAT_MUL:        u32 = 12;
pub const EXPR_FLOAT_DIV:        u32 = 13;
pub const EXPR_FLOAT_NEG:        u32 = 14;
pub const EXPR_CMP_EQ:           u32 = 15;
pub const EXPR_CMP_NE:           u32 = 16;
pub const EXPR_CMP_GT:           u32 = 17;
pub const EXPR_CMP_GE:           u32 = 18;
pub const EXPR_CMP_LT:           u32 = 19;
pub const EXPR_CMP_LE:           u32 = 20;
pub const EXPR_FCMP_EQ:          u32 = 21;
pub const EXPR_FCMP_NE:          u32 = 22;
pub const EXPR_FCMP_GT:          u32 = 23;
pub const EXPR_FCMP_GE:          u32 = 24;
pub const EXPR_FCMP_LT:          u32 = 25;
pub const EXPR_FCMP_LE:          u32 = 26;
pub const EXPR_BOOL_AND:         u32 = 27;
pub const EXPR_BOOL_OR:          u32 = 28;
pub const EXPR_BOOL_NOT:         u32 = 29;
pub const EXPR_IS_NULL:          u32 = 30;
pub const EXPR_IS_NOT_NULL:      u32 = 31;
pub const EXPR_EMIT:             u32 = 32;
pub const EXPR_INT_TO_FLOAT:     u32 = 33;
pub const EXPR_COPY_COL:         u32 = 34;
pub const EXPR_STR_COL_EQ_CONST: u32 = 40;
pub const EXPR_STR_COL_LT_CONST: u32 = 41;
pub const EXPR_STR_COL_LE_CONST: u32 = 42;
pub const EXPR_STR_COL_EQ_COL:   u32 = 43;
pub const EXPR_STR_COL_LT_COL:   u32 = 44;
pub const EXPR_STR_COL_LE_COL:   u32 = 45;
pub const EXPR_EMIT_NULL:        u32 = 46;
// Resolved-column opcodes: emitted by ExprProgram::resolve_column_indices.
// LOAD_COL_INT/FLOAT use logical (schema) indices; these use physical payload
// indices (pk_index already stripped) so the interpreter inner loop is branch-free.
pub const EXPR_LOAD_PAYLOAD_INT:   u32 = 47;
pub const EXPR_LOAD_PAYLOAD_FLOAT: u32 = 48;
pub const EXPR_LOAD_PK_INT:        u32 = 49;

// ---------------------------------------------------------------------------
// Circuit opcodes
// ---------------------------------------------------------------------------

pub const OPCODE_FILTER:                u64 = 1;
pub const OPCODE_MAP:                   u64 = 2;
pub const OPCODE_NEGATE:                u64 = 3;
pub const OPCODE_UNION:                 u64 = 4;
pub const OPCODE_JOIN_DELTA_TRACE:      u64 = 5;
pub const OPCODE_JOIN_DELTA_DELTA:      u64 = 6;
pub const OPCODE_INTEGRATE:             u64 = 7;
pub const OPCODE_DELAY:                 u64 = 8;
pub const OPCODE_REDUCE:                u64 = 9;
pub const OPCODE_DISTINCT:              u64 = 10;
/// Delta input source. Replaces both `input_delta()` and
/// `input_delta_tagged()` from the legacy layout — the actual table_id
/// lives in the node row's `source_table` column for both.
/// (Numeric value preserved from the prior `OPCODE_SCAN_TRACE = 11`.)
pub const OPCODE_SCAN_DELTA:            u64 = 11;
/// Legacy alias retained temporarily during the circuit-graph schema
/// rewrite. New code should use `OPCODE_SCAN_DELTA`.
pub const OPCODE_SCAN_TRACE:            u64 = OPCODE_SCAN_DELTA;
pub const OPCODE_SEEK_TRACE:            u64 = 12;
pub const OPCODE_CLEAR_DELTAS:          u64 = 15;
pub const OPCODE_ANTI_JOIN_DELTA_TRACE: u64 = 16;
pub const OPCODE_ANTI_JOIN_DELTA_DELTA: u64 = 17;
pub const OPCODE_SEMI_JOIN_DELTA_TRACE: u64 = 18;
pub const OPCODE_SEMI_JOIN_DELTA_DELTA: u64 = 19;
pub const OPCODE_EXCHANGE_SHARD:        u64 = 20;
pub const OPCODE_EXCHANGE_GATHER:       u64 = 21;
pub const OPCODE_JOIN_DELTA_TRACE_OUTER: u64 = 22;
pub const OPCODE_NULL_EXTEND:            u64 = 23;
pub const OPCODE_GATHER_REDUCE:          u64 = 24;
/// Discriminates IntegrateTrace from IntegrateSink (OPCODE_INTEGRATE=7)
/// without a nullable column.
pub const OPCODE_INTEGRATE_TRACE:        u64 = 25;
/// MAP sub-variant: pure projection (column reorder/drop).
pub const OPCODE_MAP_PROJ:               u64 = 26;
/// MAP sub-variant: expression program (compute) with optional PK reindex.
pub const OPCODE_MAP_EXPR:               u64 = 27;
/// MAP sub-variant: drop payload, keep PK only.
pub const OPCODE_MAP_KEY_ONLY:           u64 = 28;
/// Read-only trace source for join trace ports; never participates in cascade.
pub const OPCODE_SCAN_TRACE_TABLE:       u64 = 31;

// ---------------------------------------------------------------------------
// Circuit-layer type aliases
// ---------------------------------------------------------------------------

pub type TableId = u64;

// ---------------------------------------------------------------------------
// Port constants
// ---------------------------------------------------------------------------

pub const PORT_IN:    u64 = 0;
pub const PORT_TRACE: u64 = 1;
pub const PORT_IN_A:  u64 = 0;
pub const PORT_IN_B:  u64 = 1;

// ---------------------------------------------------------------------------
// CircuitNodeColumns `kind` discriminator values
// ---------------------------------------------------------------------------
//
// Every "ordered list of column indices" attached to a circuit node lives
// in `CircuitNodeColumns` keyed by (view_id, node_id, kind, position). The
// `kind` discriminator selects which list is being addressed:

pub const NODE_COL_KIND_GROUP:    u64 = 0;  // REDUCE group-by columns
pub const NODE_COL_KIND_SHARD:    u64 = 1;  // EXCHANGE_SHARD shard columns
pub const NODE_COL_KIND_PROJ:     u64 = 2;  // MAP projection columns
pub const NODE_COL_KIND_NULL_EXT: u64 = 3;  // NULL_EXTEND payload type codes
pub const NODE_COL_KIND_AGG_SPEC: u64 = 4;  // REDUCE aggregate specs (value1=func_id, value2=col_idx)

// ---------------------------------------------------------------------------
// Aggregate function IDs
// ---------------------------------------------------------------------------

pub const AGG_COUNT:          u64 = 1;
pub const AGG_SUM:            u64 = 2;
pub const AGG_MIN:            u64 = 3;
pub const AGG_MAX:            u64 = 4;
pub const AGG_COUNT_NON_NULL: u64 = 5;

// ---------------------------------------------------------------------------
// Typed circuit-node representation (shared between gnitz-core and gnitz-engine)
// ---------------------------------------------------------------------------

/// Aggregate function discriminant. Values match the `AGG_*` wire constants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u64)]
pub enum AggFunc {
    Count        = AGG_COUNT,
    Sum          = AGG_SUM,
    Min          = AGG_MIN,
    Max          = AGG_MAX,
    CountNonNull = AGG_COUNT_NON_NULL,
}

impl AggFunc {
    pub fn from_wire(v: u64) -> Option<Self> {
        match v {
            AGG_COUNT          => Some(AggFunc::Count),
            AGG_SUM            => Some(AggFunc::Sum),
            AGG_MIN            => Some(AggFunc::Min),
            AGG_MAX            => Some(AggFunc::Max),
            AGG_COUNT_NON_NULL => Some(AggFunc::CountNonNull),
            _                  => None,
        }
    }
    pub fn as_u64(self) -> u64 { self as u64 }
}

/// REDUCE aggregation kind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AggKind {
    Null,
    Specs(Vec<(AggFunc, u16)>),
}

/// Join physical strategy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinKind { DeltaTrace, DeltaTraceOuter, DeltaDelta }

/// MAP sub-variant discriminant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MapKind {
    /// Pure projection/column-reorder. Carries payload column indices to keep.
    Projection(Vec<u16>),
    /// Expression-based map. `program` is an opaque `ExprProgram` blob;
    /// each crate decodes it with its own decoder. `reindex_col` flips the PK
    /// column for equijoin pre-indexing.
    Expression { program: Vec<u8>, reindex_col: Option<u16> },
    /// Drop all payload columns, keep only PK and weight.
    KeyOnly,
}

/// Typed operator-node payload. Expression blobs are stored as raw `Vec<u8>`;
/// each crate decodes them independently (gnitz-core uses `ExprProgram::decode`;
/// gnitz-engine uses its own `decode_expr_blob`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpNode {
    /// `OPCODE_SCAN_DELTA = 11`. Delta input; carries the source `table_id`.
    ScanDelta(TableId),
    /// `OPCODE_SCAN_TRACE_TABLE = 31`. Read-only trace source for join trace ports.
    ScanTrace(TableId),
    /// `OPCODE_FILTER = 1`. Optional expression predicate blob.
    Filter(Option<Vec<u8>>),
    Map(MapKind),
    Negate,
    Union,
    Delay,
    Distinct,
    Reduce { group_cols: Vec<u16>, agg: AggKind },
    Join(JoinKind),
    AntiJoin(JoinKind),
    SemiJoin(JoinKind),
    /// `OPCODE_INTEGRATE = 7`. Primary INTEGRATE: writes to view storage.
    IntegrateSink,
    /// `OPCODE_INTEGRATE_TRACE = 25`. Accumulates Z-set for join trace.
    IntegrateTrace,
    ExchangeShard { shard_cols: Vec<u16> },
    /// `OPCODE_EXCHANGE_GATHER = 21`. Register pass-through; no VM instruction emitted.
    ExchangeGather,
    NullExtend { type_codes: Vec<u8> },
    /// `OPCODE_GATHER_REDUCE = 24`. Server-internal; carries no typed fields until
    /// the GatherReduce planning milestone adds `agg: AggKind`.
    GatherReduce,
    /// `OPCODE_SEEK_TRACE = 12`. Server-internal.
    SeekTrace,
    /// `OPCODE_CLEAR_DELTAS = 15`. Server-internal.
    ClearDeltas,
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
    reindex: Option<u16>,
    expr_blob: Option<Vec<u8>>,
    cols: &[(u64, u16, u64, u64)],
) -> Result<OpNode, String> {
    let collect_cols = |kind: u64| -> Vec<u16> {
        cols.iter()
            .filter(|(k, _, _, _)| *k == kind)
            .map(|(_, _, v1, _)| *v1 as u16)
            .collect()
    };
    let collect_typecodes = |kind: u64| -> Vec<u8> {
        cols.iter()
            .filter(|(k, _, _, _)| *k == kind)
            .map(|(_, _, v1, _)| *v1 as u8)
            .collect()
    };
    let collect_aggs = || -> Result<Vec<(AggFunc, u16)>, String> {
        cols.iter()
            .filter(|(k, _, _, _)| *k == NODE_COL_KIND_AGG_SPEC)
            .map(|(_, _, v1, v2)| {
                AggFunc::from_wire(*v1)
                    .ok_or_else(|| format!("unknown agg func id {}", v1))
                    .map(|f| (f, *v2 as u16))
            })
            .collect()
    };
    Ok(match opcode {
        x if x == OPCODE_SCAN_DELTA        => OpNode::ScanDelta(src_tab.unwrap_or(0)),
        x if x == OPCODE_SCAN_TRACE_TABLE  => OpNode::ScanTrace(src_tab.unwrap_or(0)),
        x if x == OPCODE_FILTER            => OpNode::Filter(expr_blob),
        x if x == OPCODE_MAP_PROJ          => OpNode::Map(MapKind::Projection(collect_cols(NODE_COL_KIND_PROJ))),
        x if x == OPCODE_MAP_EXPR          => {
            let program = expr_blob.ok_or_else(|| "MAP_EXPR missing expr_program blob".to_string())?;
            OpNode::Map(MapKind::Expression { program, reindex_col: reindex })
        }
        x if x == OPCODE_MAP_KEY_ONLY      => OpNode::Map(MapKind::KeyOnly),
        x if x == OPCODE_NEGATE            => OpNode::Negate,
        x if x == OPCODE_UNION             => OpNode::Union,
        x if x == OPCODE_DELAY             => OpNode::Delay,
        x if x == OPCODE_DISTINCT          => OpNode::Distinct,
        x if x == OPCODE_REDUCE            => {
            let group_cols = collect_cols(NODE_COL_KIND_GROUP);
            let specs = collect_aggs()?;
            let agg = if specs.is_empty() { AggKind::Null } else { AggKind::Specs(specs) };
            OpNode::Reduce { group_cols, agg }
        }
        x if x == OPCODE_JOIN_DELTA_TRACE        => OpNode::Join(JoinKind::DeltaTrace),
        x if x == OPCODE_JOIN_DELTA_TRACE_OUTER  => OpNode::Join(JoinKind::DeltaTraceOuter),
        x if x == OPCODE_JOIN_DELTA_DELTA        => OpNode::Join(JoinKind::DeltaDelta),
        x if x == OPCODE_ANTI_JOIN_DELTA_TRACE   => OpNode::AntiJoin(JoinKind::DeltaTrace),
        x if x == OPCODE_ANTI_JOIN_DELTA_DELTA   => OpNode::AntiJoin(JoinKind::DeltaDelta),
        x if x == OPCODE_SEMI_JOIN_DELTA_TRACE   => OpNode::SemiJoin(JoinKind::DeltaTrace),
        x if x == OPCODE_SEMI_JOIN_DELTA_DELTA   => OpNode::SemiJoin(JoinKind::DeltaDelta),
        x if x == OPCODE_INTEGRATE               => OpNode::IntegrateSink,
        x if x == OPCODE_INTEGRATE_TRACE         => OpNode::IntegrateTrace,
        x if x == OPCODE_EXCHANGE_SHARD          => OpNode::ExchangeShard { shard_cols: collect_cols(NODE_COL_KIND_SHARD) },
        x if x == OPCODE_EXCHANGE_GATHER         => OpNode::ExchangeGather,
        x if x == OPCODE_NULL_EXTEND             => OpNode::NullExtend { type_codes: collect_typecodes(NODE_COL_KIND_NULL_EXT) },
        x if x == OPCODE_GATHER_REDUCE           => OpNode::GatherReduce,
        x if x == OPCODE_SEEK_TRACE              => OpNode::SeekTrace,
        x if x == OPCODE_CLEAR_DELTAS            => OpNode::ClearDeltas,
        _ => return Err(format!("unknown opcode {}", opcode)),
    })
}

// ---------------------------------------------------------------------------
// System table column descriptors — shared single source of truth
// ---------------------------------------------------------------------------

pub struct WireSysCol {
    pub name:      &'static str,
    pub type_code: u8,
    pub nullable:  bool,
}

pub const CIRCUIT_NODES_COLS: &[WireSysCol] = &[
    WireSysCol { name: "node_pk",      type_code: type_code::U128, nullable: false },
    WireSysCol { name: "view_id",      type_code: type_code::U64,  nullable: false },
    WireSysCol { name: "node_id",      type_code: type_code::U64,  nullable: false },
    WireSysCol { name: "opcode",       type_code: type_code::U64,  nullable: false },
    WireSysCol { name: "source_table", type_code: type_code::U64,  nullable: true  },
    WireSysCol { name: "reindex_col",  type_code: type_code::U64,  nullable: true  },
    WireSysCol { name: "expr_program", type_code: type_code::BLOB, nullable: true  },
];

pub const CIRCUIT_EDGES_COLS: &[WireSysCol] = &[
    WireSysCol { name: "edge_pk",  type_code: type_code::U128, nullable: false },
    WireSysCol { name: "view_id",  type_code: type_code::U64,  nullable: false },
    WireSysCol { name: "dst_node", type_code: type_code::U64,  nullable: false },
    WireSysCol { name: "dst_port", type_code: type_code::U64,  nullable: false },
    WireSysCol { name: "src_node", type_code: type_code::U64,  nullable: false },
];

pub const CIRCUIT_NODE_COLUMNS_COLS: &[WireSysCol] = &[
    WireSysCol { name: "node_col_pk", type_code: type_code::U128, nullable: false },
    WireSysCol { name: "view_id",     type_code: type_code::U64,  nullable: false },
    WireSysCol { name: "node_id",     type_code: type_code::U64,  nullable: false },
    WireSysCol { name: "kind",        type_code: type_code::U64,  nullable: false },
    WireSysCol { name: "position",    type_code: type_code::U64,  nullable: false },
    WireSysCol { name: "value1",      type_code: type_code::U64,  nullable: false },
    WireSysCol { name: "value2",      type_code: type_code::U64,  nullable: false },
];

// ---------------------------------------------------------------------------
// System table IDs
// ---------------------------------------------------------------------------

pub const SCHEMA_TAB:           u64 = 1;
pub const TABLE_TAB:            u64 = 2;
pub const VIEW_TAB:             u64 = 3;
pub const COL_TAB:              u64 = 4;
pub const IDX_TAB:              u64 = 5;
pub const DEP_TAB:              u64 = 6;
pub const SEQ_TAB:              u64 = 7;
pub const CIRCUIT_NODES_TAB:        u64 = 11;
pub const CIRCUIT_EDGES_TAB:        u64 = 12;
pub const CIRCUIT_NODE_COLUMNS_TAB: u64 = 13;
// IDs 14 and 15 were previously CIRCUIT_PARAMS_TAB and CIRCUIT_GROUP_COLS_TAB,
// now folded into CircuitNodes / CircuitNodeColumns.

pub const FIRST_USER_TABLE_ID:  u64 = 16;
pub const FIRST_USER_SCHEMA_ID: u64 = 3;

pub const OWNER_KIND_TABLE: u64 = 0;
pub const OWNER_KIND_VIEW:  u64 = 1;

// ---------------------------------------------------------------------------
// Frame size limits
// ---------------------------------------------------------------------------

/// Maximum frame payload the server will accept from a client.
/// Tighter than the client limit: protects the master process from a
/// misbehaving or malicious peer before any allocation occurs.
pub const MAX_FRAME_PAYLOAD_SERVER: usize = 64 * 1024 * 1024;  // 64 MB

/// Maximum frame payload the client library will accept from the server.
/// Larger than the server limit: a legitimate batch push can be hundreds
/// of MB; the server is trusted so the risk model is different.
pub const MAX_FRAME_PAYLOAD_CLIENT: usize = 256 * 1024 * 1024; // 256 MB

// ---------------------------------------------------------------------------
// HELLO handshake
// ---------------------------------------------------------------------------
//
// Layout (length-prefixed; both sides use the standard 4-byte LE u32 prefix):
//
//   HELLO  (client → server, total wire size 12 bytes)
//     [length=8 LE u32][magic: u32 LE][version: u16 LE][flags: u16 LE]
//
//   ACK    (server → client on success, total wire size 16 bytes)
//     [length=12 LE u32][magic: u32 LE][status: u16 LE][_pad: u16 LE]
//     [limit_bytes: u32 LE]
//
// On version mismatch / auth failure the server replies with a standard
// length-prefixed STATUS_ERROR control block (>= 248 bytes) and closes
// the fd. The length prefix alone discriminates: 8 ⇒ HELLO, 12 ⇒ ACK,
// anything else ⇒ control block. Magic checks remain as defence-in-depth.

/// Magic value carried in HELLO and ACK frames. ASCII "GNTZ" interpreted
/// as a little-endian u32. Defence-in-depth on top of the length-prefix
/// discriminant; a legacy peer cannot collide because the smallest legacy
/// frame (a control WAL block) is at least `CTRL_BLOCK_SIZE_NO_BLOB` bytes.
pub const HELLO_MAGIC: u32 = u32::from_le_bytes(*b"GNTZ");

/// HELLO payload length in bytes (excluding the 4-byte length prefix).
pub const HELLO_PAYLOAD_LEN: u32 = 8;

/// ACK payload length in bytes (excluding the 4-byte length prefix).
pub const HELLO_ACK_PAYLOAD_LEN: u32 = 12;

/// Total wire size of a HELLO frame (length prefix + payload).
pub const HELLO_FRAME_SIZE: usize = 4 + HELLO_PAYLOAD_LEN as usize;

/// Total wire size of an ACK frame (length prefix + payload).
pub const HELLO_ACK_FRAME_SIZE: usize = 4 + HELLO_ACK_PAYLOAD_LEN as usize;

/// Status field in the ACK frame.
pub const HELLO_STATUS_OK:    u16 = 0;
pub const HELLO_STATUS_ERROR: u16 = 1;

/// Build a HELLO frame ready to ship over the wire (length prefix + payload).
pub const fn encode_hello_frame(version: u16, flags: u16) -> [u8; HELLO_FRAME_SIZE] {
    let len = HELLO_PAYLOAD_LEN.to_le_bytes();
    let mag = HELLO_MAGIC.to_le_bytes();
    let ver = version.to_le_bytes();
    let flg = flags.to_le_bytes();
    [
        len[0], len[1], len[2], len[3],
        mag[0], mag[1], mag[2], mag[3],
        ver[0], ver[1],
        flg[0], flg[1],
    ]
}

/// Parsed HELLO payload (the 8 bytes following the length prefix).
#[derive(Debug, Clone, Copy)]
pub struct HelloHeader {
    pub magic: u32,
    pub version: u16,
    pub flags: u16,
}

/// Decode a HELLO payload. The caller must have already consumed the
/// 4-byte length prefix and verified that it equalled `HELLO_PAYLOAD_LEN`.
pub fn decode_hello_payload(payload: &[u8]) -> Result<HelloHeader, &'static str> {
    if payload.len() != HELLO_PAYLOAD_LEN as usize {
        return Err("hello payload wrong size");
    }
    let magic = u32::from_le_bytes(payload[0..4].try_into().unwrap());
    let version = u16::from_le_bytes(payload[4..6].try_into().unwrap());
    let flags = u16::from_le_bytes(payload[6..8].try_into().unwrap());
    Ok(HelloHeader { magic, version, flags })
}

/// Build an ACK frame ready to ship over the wire (length prefix + payload).
pub const fn encode_hello_ack(status: u16, limit_bytes: u32) -> [u8; HELLO_ACK_FRAME_SIZE] {
    let len = HELLO_ACK_PAYLOAD_LEN.to_le_bytes();
    let mag = HELLO_MAGIC.to_le_bytes();
    let st  = status.to_le_bytes();
    let lim = limit_bytes.to_le_bytes();
    [
        len[0], len[1], len[2], len[3],
        mag[0], mag[1], mag[2], mag[3],
        st[0],  st[1],
        0,      0, // _pad
        lim[0], lim[1], lim[2], lim[3],
    ]
}

/// Parsed ACK payload (the 12 bytes following the length prefix).
#[derive(Debug, Clone, Copy)]
pub struct HelloAck {
    pub magic: u32,
    pub status: u16,
    pub limit_bytes: u32,
}

/// Decode an ACK payload. The caller must have already consumed the
/// 4-byte length prefix and verified that it equalled `HELLO_ACK_PAYLOAD_LEN`.
pub fn decode_hello_ack(payload: &[u8]) -> Result<HelloAck, &'static str> {
    if payload.len() != HELLO_ACK_PAYLOAD_LEN as usize {
        return Err("hello ack payload wrong size");
    }
    let magic = u32::from_le_bytes(payload[0..4].try_into().unwrap());
    let status = u16::from_le_bytes(payload[4..6].try_into().unwrap());
    // bytes [6..8] are reserved padding
    let limit_bytes = u32::from_le_bytes(payload[8..12].try_into().unwrap());
    Ok(HelloAck { magic, status, limit_bytes })
}

// ---------------------------------------------------------------------------
// Wire protocol flags
// ---------------------------------------------------------------------------

/// Bits 0-15 are the SAL-level flags carried verbatim from the on-disk log
/// into every control block.  Bits 16+ are wire-level fields (conflict mode,
/// schema version) that are encoded by the sender and decoded by the receiver.
pub const SAL_FLAGS_MASK: u64 = 0x0000_FFFF;

pub const FLAG_SHUTDOWN:       u64 = 4;
pub const FLAG_DDL_SYNC:       u64 = 8;
pub const FLAG_EXCHANGE:       u64 = 16;
pub const FLAG_PUSH:           u64 = 32;
pub const FLAG_HAS_PK:         u64 = 64;
pub const FLAG_SEEK:           u64 = 128;
pub const FLAG_SEEK_BY_INDEX:  u64 = 256;
pub const FLAG_HAS_SCHEMA:     u64 = 1 << 48;
pub const FLAG_HAS_DATA:       u64 = 1 << 49;
/// Set on every per-worker scan response frame. Absent on the terminal
/// frame sent by the master after all worker frames. Clients loop on
/// `recv_message` until they see a frame without this bit.
pub const FLAG_CONTINUATION:   u64 = 1 << 52;

// ---------------------------------------------------------------------------
// Wire-level packed fields: bits 16-39 of wire_flags
// ---------------------------------------------------------------------------

/// Bits 16-23: conflict mode (8 bits). Value 0 = Update (default).
pub const WIRE_CONFLICT_MODE_SHIFT: u32 = 16;
pub const WIRE_CONFLICT_MODE_MASK:  u64 = 0xFF_u64 << WIRE_CONFLICT_MODE_SHIFT;
/// Bits 24-39: schema version (16 bits). Value 0 = client has no cached schema.
pub const WIRE_SCHEMA_VERSION_SHIFT: u32 = 24;
pub const WIRE_SCHEMA_VERSION_MASK:  u64 = 0xFFFF_u64 << WIRE_SCHEMA_VERSION_SHIFT;

// Compile-time guard: SAL flags (bits 0-15) must not overlap the wire-level
// packed fields (bits 16-39).
const _: () = assert!(SAL_FLAGS_MASK & (WIRE_CONFLICT_MODE_MASK | WIRE_SCHEMA_VERSION_MASK) == 0);

#[inline]
pub fn wire_flags_set_conflict_mode(flags: u64, mode: WireConflictMode) -> u64 {
    (flags & !WIRE_CONFLICT_MODE_MASK) | ((mode as u64) << WIRE_CONFLICT_MODE_SHIFT)
}
#[inline]
pub fn wire_flags_get_conflict_mode(flags: u64) -> WireConflictMode {
    WireConflictMode::from_u8(((flags & WIRE_CONFLICT_MODE_MASK) >> WIRE_CONFLICT_MODE_SHIFT) as u8)
}
#[inline]
pub fn wire_flags_set_schema_version(flags: u64, version: u16) -> u64 {
    (flags & !WIRE_SCHEMA_VERSION_MASK) | ((version as u64) << WIRE_SCHEMA_VERSION_SHIFT)
}
#[inline]
pub fn wire_flags_get_schema_version(flags: u64) -> u16 {
    ((flags & WIRE_SCHEMA_VERSION_MASK) >> WIRE_SCHEMA_VERSION_SHIFT) as u16
}
/// Returns true when the server should include a schema block in its response.
/// `client_version == 0` means the client has no cached schema; any non-zero
/// mismatch means the server's schema has changed since the client last saw it.
#[inline]
pub fn wire_should_include_schema(client_version: u16, server_version: u16) -> bool {
    client_version == 0 || client_version != server_version
}

// ---------------------------------------------------------------------------
// Wire-level conflict mode for INSERT / UPSERT semantics
// ---------------------------------------------------------------------------

/// Conflict-resolution mode packed into bits 16-23 of `wire_flags` on
/// FLAG_PUSH messages. Discriminant 0 = Update (default), so zero-filled
/// flags resolve to the upsert default without explicit encoding.
#[repr(u8)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WireConflictMode {
    /// Retract-and-insert on PK conflict. Used for SQL `UPDATE`,
    /// `INSERT ... ON CONFLICT ... DO UPDATE` (after client-side
    /// merging), and explicit Python `push(conflict_mode="update")`.
    #[default]
    Update = 0,
    /// Reject the batch on any PK conflict. The master runs both an
    /// intra-batch duplicate check and an against-store PK existence
    /// check, and returns a PG-style `duplicate key value violates
    /// unique constraint` error.
    Error = 1,
}

impl WireConflictMode {
    #[inline]
    pub const fn as_u8(self) -> u8 { self as u8 }

    /// Unknown bytes decode as `Update` so forward-compatible decoders
    /// still see last-write-wins semantics.
    #[inline]
    pub const fn from_u8(v: u8) -> Self {
        match v {
            1 => WireConflictMode::Error,
            _ => WireConflictMode::Update,
        }
    }
}

pub const STATUS_OK:             u32 = 0;
pub const STATUS_ERROR:          u32 = 1;
/// Server-side version mismatch on schema-less PUSH: client must evict its
/// schema cache entry for the target table and retry with the full schema.
pub const STATUS_SCHEMA_MISMATCH: u32 = 2;

pub const META_FLAG_NULLABLE: u64 = 1;
pub const META_FLAG_IS_PK:    u64 = 2;

/// PK position (0-indexed) within the PK tuple for the column carrying
/// `META_FLAG_IS_PK`. Bits 8..16 of the per-column flags word. Single-PK
/// schemas leave this at 0; compound-PK schemas encode each PK column's
/// position so the decoder can reconstruct `pk_indices` in declaration
/// order rather than column-position order (e.g. `PRIMARY KEY (b, a)`
/// with `a` at column 1 and `b` at column 2 must decode to `[2, 1]`).
pub const META_FLAG_PK_POS_SHIFT: u32 = 8;
pub const META_FLAG_PK_POS_MASK:  u64 = 0xFF << META_FLAG_PK_POS_SHIFT;

/// Maximum number of columns (PK + payload) in any table or view schema.
/// Capped at 65 by the row-major null bitmap: each row stores one u64 word
/// with one bit per nullable payload column, so payload columns ≤ 64.
pub const MAX_COLUMNS: usize = 65;

/// Sizing cap for the compound-PK column list.
///
/// Set to 5 to cover the user-facing PK cap (4 columns, planner-enforced)
/// plus one indexed-column prefix used by secondary index schemas — modeled
/// as `(indexed_col, src_pk_0, …, src_pk_{k-1})`.
pub const MAX_PK_COLUMNS: usize = 5;

/// Maximum byte width of a PK region per row. Product of `MAX_PK_COLUMNS`
/// and the per-column ceiling (16 == max wire stride of any type valid as a
/// PK column — U128, UUID; STRING and BLOB are rejected by schema
/// validation). Auto-tracks growth of `MAX_PK_COLUMNS`.
pub const MAX_PK_BYTES: usize = MAX_PK_COLUMNS * 16;

// ---------------------------------------------------------------------------
// Compound-PK list encoding for the persisted `TABLE_TAB.pk_col_idx` u64.
//
// Two forms share the same column slot:
//   * Bare scalar (flag bit clear): a single PK column index in bits [0..63).
//     Written by an unmodified single-PK client and by engine-side bootstrap
//     for system tables.
//   * Packed list (flag bit set):
//        bit 63        : PK_LIST_PACKED_FLAG
//        bits [0..4)   : decoded count (1..=4 valid; >4 reserved for tests)
//        bits [4+7i..) : i-th column index, 7 bits each
//
// Both client (gnitz-core) and engine (gnitz-engine catalog) MUST share this
// encoder/decoder so they cannot drift on the encoding.
// ---------------------------------------------------------------------------

pub const PK_LIST_PACKED_FLAG: u64 = 1 << 63;

/// Decoded PK column list — backing storage sized to 4 entries.
/// `decoded_count()` returns the raw decoded count from the wire (may be 0
/// or 5..=15 for a crafted packed value); `as_slice()` is panic-free and
/// clamps the slice to at most 4 entries. Out-of-range counts must reach
/// schema-validation code as `Err`, not as a panic here.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct PkColList {
    cols: [u32; 4],
    len: usize,
}

impl PkColList {
    /// Single-column PK with `len = 1`.
    pub fn single(idx: u32) -> Self {
        PkColList { cols: [idx, 0, 0, 0], len: 1 }
    }
    /// The count exactly as decoded from the wire. May be 0 or >4 for a
    /// malformed/crafted packed value — deliberately NOT clamped, because
    /// `validate_pk_cols` gates on this raw value to reject out-of-range
    /// counts. Not a safe slice length: iterate `as_slice()` instead.
    pub fn decoded_count(&self) -> usize { self.len }
    /// Always in bounds: indexes at most the 4-element backing array even
    /// when the decoded count is out of range. A crafted wire count of
    /// 5..=15 must NOT panic here — it has to survive long enough to
    /// reach `validate_pk_cols` and be returned as `Err`.
    pub fn as_slice(&self) -> &[u32] { &self.cols[..self.len.min(4)] }
}

/// Pack a PK column-index list into the persisted `u64` form. Panics on a
/// violated contract because a silent truncation here corrupts the
/// catalog encoding; callers (client + engine) must reject out-of-range
/// lists before calling this.
pub fn pack_pk_cols(pk_cols: &[u32]) -> u64 {
    assert!((1..=4).contains(&pk_cols.len()), "pack_pk_cols: count out of range 1..=4");
    let mut v = pk_cols.len() as u64;            // bits [0..4)
    for (i, &c) in pk_cols.iter().enumerate() {
        assert!(c < 128, "pack_pk_cols: column index {c} exceeds 7-bit field");
        v |= (c as u64 & 0x7f) << (4 + 7 * i);
    }
    v | PK_LIST_PACKED_FLAG
}

/// Decode the persisted `u64` PK-list form. Handles both the bare scalar
/// (flag bit clear → single index) and packed list forms. Out-of-range
/// counts are returned as-is via `decoded_count()` so the catalog
/// validator can reject them.
pub fn unpack_pk_cols(packed: u64) -> PkColList {
    if packed & PK_LIST_PACKED_FLAG == 0 {
        // Bare single index: an unmodified gnitz-core client, or an
        // engine-written system-table row (always bare `0`).
        return PkColList { cols: [packed as u32, 0, 0, 0], len: 1 };
    }
    let n = (packed & 0xf) as usize;             // 0..=15, validated later
    let mut cols = [0u32; 4];
    for (i, slot) in cols.iter_mut().enumerate().take(n.min(4)) {
        *slot = ((packed >> (4 + 7 * i)) & 0x7f) as u32;
    }
    PkColList { cols, len: n }
}

// ---------------------------------------------------------------------------
// WAL header constants
// ---------------------------------------------------------------------------

pub const WAL_HEADER_SIZE:   usize = 48;
pub const WAL_FORMAT_VERSION: u32  = 4;
pub const IPC_CONTROL_TID:    u32  = 0xFFFF_FFFF;

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

/// Align `n` up to an 8-byte boundary.
pub const fn align8(n: usize) -> usize {
    (n + 7) & !7
}

/// Threshold for inline German String storage (bytes).
pub const SHORT_STRING_THRESHOLD: usize = 12;

// ---------------------------------------------------------------------------
// IPC control-block wire layout
// ---------------------------------------------------------------------------
//
// The control WAL block carries the per-message header. Both `gnitz-engine`
// (server) and `gnitz-protocol` (client) build/parse this block; the column
// indices, payload indices, and null-bit positions live here so the two
// implementations cannot drift.
//
// Schema (10 columns, pk_index = 0):
//   col  0: msg_idx       U64   (PK placeholder; always 0)
//   col  1: status        U64
//   col  2: client_id     U64
//   col  3: target_id     U64
//   col  4: flags         U64
//   col  5: seek_pk       U128
//   col  6: seek_col_idx  U64
//   col  7: request_id    U64    -- reactor reply-routing key
//   col  8: error_msg     STRING (nullable)
//   col  9: seek_pk_extra BLOB   (nullable) -- PK region bytes 16.. for a wide PK
//
// Reserved request_id values:
//   0          -- "unsolicited"/"untagged" (pre-reactor reply path)
//   u64::MAX   -- broadcast reply (one reply per worker per broadcast)
//   other      -- master-allocated, monotonic per request
pub mod control {
    use super::{WireSysCol, type_code};

    pub const CONTROL_COLS: &[WireSysCol] = &[
        WireSysCol { name: "msg_idx",      type_code: type_code::U64,    nullable: false },
        WireSysCol { name: "status",       type_code: type_code::U64,    nullable: false },
        WireSysCol { name: "client_id",    type_code: type_code::U64,    nullable: false },
        WireSysCol { name: "target_id",    type_code: type_code::U64,    nullable: false },
        WireSysCol { name: "flags",        type_code: type_code::U64,    nullable: false },
        WireSysCol { name: "seek_pk",      type_code: type_code::U128,   nullable: false },
        WireSysCol { name: "seek_col_idx", type_code: type_code::U64,    nullable: false },
        WireSysCol { name: "request_id",   type_code: type_code::U64,    nullable: false },
        WireSysCol { name: "error_msg",    type_code: type_code::STRING, nullable: true  },
        WireSysCol { name: "seek_pk_extra", type_code: type_code::BLOB,   nullable: true  },
    ];

    pub const NUM_COLUMNS: usize = CONTROL_COLS.len();

    pub const fn col_index(name: &str) -> usize {
        let mut i = 0;
        while i < CONTROL_COLS.len() {
            let a = CONTROL_COLS[i].name.as_bytes();
            let b = name.as_bytes();
            if a.len() == b.len() {
                let mut j = 0;
                let mut matched = true;
                while j < a.len() {
                    if a[j] != b[j] { matched = false; break; }
                    j += 1;
                }
                if matched { return i; }
            }
            i += 1;
        }
        panic!("Column not found in CONTROL_COLS")
    }

    pub const COL_MSG_IDX:      usize = col_index("msg_idx");
    pub const COL_STATUS:       usize = col_index("status");
    pub const COL_CLIENT_ID:    usize = col_index("client_id");
    pub const COL_TARGET_ID:    usize = col_index("target_id");
    pub const COL_FLAGS:        usize = col_index("flags");
    pub const COL_SEEK_PK:      usize = col_index("seek_pk");
    pub const COL_SEEK_COL_IDX: usize = col_index("seek_col_idx");
    pub const COL_REQUEST_ID:   usize = col_index("request_id");
    pub const COL_ERROR_MSG:    usize = col_index("error_msg");
    pub const COL_SEEK_PK_EXTRA: usize = col_index("seek_pk_extra");

    const fn payload_index(col_idx: usize) -> usize {
        assert!(col_idx != COL_MSG_IDX, "PK column has no payload index");
        // COL_MSG_IDX == 0 (first column), so all non-PK indices are > 0
        col_idx - 1
    }

    pub const PAYLOAD_STATUS:       usize = payload_index(COL_STATUS);
    pub const PAYLOAD_CLIENT_ID:    usize = payload_index(COL_CLIENT_ID);
    pub const PAYLOAD_TARGET_ID:    usize = payload_index(COL_TARGET_ID);
    pub const PAYLOAD_FLAGS:        usize = payload_index(COL_FLAGS);
    pub const PAYLOAD_SEEK_PK:      usize = payload_index(COL_SEEK_PK);
    pub const PAYLOAD_SEEK_COL_IDX: usize = payload_index(COL_SEEK_COL_IDX);
    pub const PAYLOAD_REQUEST_ID:   usize = payload_index(COL_REQUEST_ID);
    pub const PAYLOAD_ERROR_MSG:    usize = payload_index(COL_ERROR_MSG);
    pub const PAYLOAD_SEEK_PK_EXTRA: usize = payload_index(COL_SEEK_PK_EXTRA);

    /// Null-bit position for `error_msg` in the row null bitmap.
    /// Equals the payload index of the column.
    pub const NULL_BIT_ERROR_MSG: u64 = 1u64 << PAYLOAD_ERROR_MSG;

    /// Null-bit position for `seek_pk_extra` in the row null bitmap.
    /// Equals the payload index of the column.
    pub const NULL_BIT_SEEK_PK_EXTRA: u64 = 1u64 << PAYLOAD_SEEK_PK_EXTRA;

    /// WAL region count for a CONTROL_SCHEMA block (V3 format):
    /// 3 fixed regions (pk 16B, weight, null_bmp) + (NUM_COLUMNS - 1) payload columns
    /// + 1 blob region.
    pub const NUM_REGIONS: usize = 3 + (NUM_COLUMNS - 1) + 1;

    // Region indices. V3 format: 3 system regions (pk=0, weight=1, null_bmp=2)
    // followed by payload columns in schema order, then blob last.
    pub const REGION_PK:           usize = 0;
    pub const REGION_WEIGHT:       usize = 1;
    pub const REGION_NULL_BMP:     usize = 2;
    pub const REGION_STATUS:       usize = 3 + PAYLOAD_STATUS;
    pub const REGION_CLIENT_ID:    usize = 3 + PAYLOAD_CLIENT_ID;
    pub const REGION_TARGET_ID:    usize = 3 + PAYLOAD_TARGET_ID;
    pub const REGION_FLAGS:        usize = 3 + PAYLOAD_FLAGS;
    pub const REGION_SEEK_PK:      usize = 3 + PAYLOAD_SEEK_PK;
    pub const REGION_SEEK_COL_IDX: usize = 3 + PAYLOAD_SEEK_COL_IDX;
    pub const REGION_REQUEST_ID:   usize = 3 + PAYLOAD_REQUEST_ID;
    pub const REGION_ERROR_MSG:    usize = 3 + PAYLOAD_ERROR_MSG;
    pub const REGION_SEEK_PK_EXTRA: usize = 3 + PAYLOAD_SEEK_PK_EXTRA;
    pub const REGION_BLOB:         usize = NUM_REGIONS - 1;
}

// ---------------------------------------------------------------------------
// German string codec
// ---------------------------------------------------------------------------

/// Encode a byte slice as a 16-byte German String struct, appending overflow
/// data to `blob`.
///
/// Layout:
///   [0..4]  length (u32 LE)
///   [4..8]  prefix — first min(4, len) bytes, zero-padded
///   [8..16] if len ≤ 12: suffix bytes [4..len], zero-padded
///           if len > 12: blob arena offset (u64 LE)
pub fn encode_german_string(s: &[u8], blob: &mut Vec<u8>) -> [u8; 16] {
    let len = s.len();
    let mut st = [0u8; 16];
    st[0..4].copy_from_slice(&(len as u32).to_le_bytes());
    if len == 0 {
        return st;
    }
    let pfx = len.min(4);
    st[4..4 + pfx].copy_from_slice(&s[..pfx]);
    if len <= SHORT_STRING_THRESHOLD {
        if len > 4 {
            st[8..8 + (len - 4)].copy_from_slice(&s[4..len]);
        }
    } else {
        let off = blob.len();
        blob.extend_from_slice(s);
        st[8..16].copy_from_slice(&(off as u64).to_le_bytes());
    }
    st
}

/// Decode a 16-byte German String struct into raw bytes.
/// `blob` is the shared blob arena.
pub fn decode_german_string(st: &[u8; 16], blob: &[u8]) -> Vec<u8> {
    let len = u32::from_le_bytes(st[0..4].try_into().unwrap()) as usize;
    if len == 0 {
        return Vec::new();
    }
    if len <= SHORT_STRING_THRESHOLD {
        let mut out = Vec::with_capacity(len);
        let pfx = len.min(4);
        out.extend_from_slice(&st[4..4 + pfx]);
        if len > 4 {
            out.extend_from_slice(&st[8..8 + (len - 4)]);
        }
        out
    } else {
        let off = u64::from_le_bytes(st[8..16].try_into().unwrap()) as usize;
        blob[off..off + len].to_vec()
    }
}

#[cfg(test)]
mod hello_tests {
    use super::*;

    #[test]
    fn hello_magic_is_ascii_gntz_le() {
        // Magic must be the LE-bytes interpretation of "GNTZ" so the
        // wire layout is stable across LE-only builds (the BE
        // compile_error guard at the top of this crate is a load-bearing
        // precondition for this test).
        assert_eq!(HELLO_MAGIC, u32::from_le_bytes(*b"GNTZ"));
    }

    #[test]
    fn hello_frame_layout_is_stable() {
        // Length prefix must be exactly HELLO_PAYLOAD_LEN, magic must
        // sit at offsets 4..8, version at 8..10, flags at 10..12.
        let frame = encode_hello_frame(0x1234, 0x0001);
        assert_eq!(frame.len(), HELLO_FRAME_SIZE);
        let prefix = u32::from_le_bytes(frame[0..4].try_into().unwrap());
        assert_eq!(prefix, HELLO_PAYLOAD_LEN);
        let magic = u32::from_le_bytes(frame[4..8].try_into().unwrap());
        assert_eq!(magic, HELLO_MAGIC);
        let version = u16::from_le_bytes(frame[8..10].try_into().unwrap());
        assert_eq!(version, 0x1234);
        let flags = u16::from_le_bytes(frame[10..12].try_into().unwrap());
        assert_eq!(flags, 0x0001);
    }

    #[test]
    fn hello_payload_decode_roundtrip() {
        let frame = encode_hello_frame(7, 0xCAFE);
        let h = decode_hello_payload(&frame[4..]).unwrap();
        assert_eq!(h.magic, HELLO_MAGIC);
        assert_eq!(h.version, 7);
        assert_eq!(h.flags, 0xCAFE);
    }

    #[test]
    fn hello_payload_decode_rejects_wrong_size() {
        // Decoder must reject anything other than HELLO_PAYLOAD_LEN
        // bytes; the caller is expected to have framed the payload via
        // the standard length prefix.
        assert!(decode_hello_payload(&[0u8; 7]).is_err());
        assert!(decode_hello_payload(&[0u8; 9]).is_err());
    }

    #[test]
    fn ack_frame_layout_is_stable() {
        let ack = encode_hello_ack(HELLO_STATUS_OK, 16 * 1024 * 1024);
        assert_eq!(ack.len(), HELLO_ACK_FRAME_SIZE);
        let prefix = u32::from_le_bytes(ack[0..4].try_into().unwrap());
        assert_eq!(prefix, HELLO_ACK_PAYLOAD_LEN);
        let magic = u32::from_le_bytes(ack[4..8].try_into().unwrap());
        assert_eq!(magic, HELLO_MAGIC);
        let status = u16::from_le_bytes(ack[8..10].try_into().unwrap());
        assert_eq!(status, HELLO_STATUS_OK);
        // [10..12] are reserved padding.
        let limit = u32::from_le_bytes(ack[12..16].try_into().unwrap());
        assert_eq!(limit, 16 * 1024 * 1024);
    }

    #[test]
    fn ack_decode_roundtrip() {
        let ack = encode_hello_ack(HELLO_STATUS_ERROR, 64 * 1024 * 1024);
        let parsed = decode_hello_ack(&ack[4..]).unwrap();
        assert_eq!(parsed.magic, HELLO_MAGIC);
        assert_eq!(parsed.status, HELLO_STATUS_ERROR);
        assert_eq!(parsed.limit_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn hello_payload_size_excludes_legacy_frame_collision() {
        // The HELLO payload (8 bytes) is far smaller than any legacy
        // framed control block. A peer mistakenly sending a control
        // block first is rejected at the length-prefix check before
        // the magic is even parsed. The pre-handshake recv ceiling on
        // the server matches HELLO_PAYLOAD_LEN exactly, so this is a
        // hard guarantee, not just a likelihood.
        assert_eq!(HELLO_PAYLOAD_LEN as usize, 8);
    }
}
