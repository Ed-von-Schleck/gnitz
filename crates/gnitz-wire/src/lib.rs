//! Shared wire-protocol definitions for GnitzDB.
//!
//! Single source of truth for constants and codecs that both the client
//! (gnitz-protocol, gnitz-core) and server (gnitz-engine) must agree on.

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
}

/// Wire stride (byte width) for a column type code.
/// Returns 8 for unknown codes (engine compare_rows depends on this default).
pub const fn wire_stride(tc: u8) -> usize {
    match tc {
        1 | 2           => 1,   // U8, I8
        3 | 4           => 2,   // U16, I16
        5 | 6 | 7       => 4,   // U32, I32, F32
        8 | 9 | 10      => 8,   // U64, I64, F64
        11 | 12          => 16,  // STRING, U128
        _                => 8,
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
pub const OPCODE_SCAN_TRACE:            u64 = 11;
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

// ---------------------------------------------------------------------------
// Port constants
// ---------------------------------------------------------------------------

pub const PORT_IN:    u64 = 0;
pub const PORT_TRACE: u64 = 1;
pub const PORT_IN_A:  u64 = 0;
pub const PORT_IN_B:  u64 = 1;

// ---------------------------------------------------------------------------
// Param slot constants
// ---------------------------------------------------------------------------

pub const PARAM_FUNC_ID:              u64 = 0;
pub const PARAM_AGG_FUNC_ID:          u64 = 1;
pub const PARAM_TABLE_ID:             u64 = 3;
pub const PARAM_AGG_COL_IDX:          u64 = 6;
pub const PARAM_EXPR_NUM_REGS:        u64 = 7;
pub const PARAM_EXPR_RESULT_REG:      u64 = 8;
pub const PARAM_GATHER_WORKER:        u64 = 9;
pub const PARAM_REINDEX_COL:          u64 = 10;
pub const PARAM_JOIN_SOURCE_TABLE:    u64 = 11;
pub const PARAM_AGG_COUNT:            u64 = 12;
pub const PARAM_AGG_SPEC_BASE:        u64 = 13;
pub const PARAM_KEY_ONLY:             u64 = 14;
pub const PARAM_NULL_EXTEND_COUNT:    u64 = 15;
pub const PARAM_PROJ_BASE:            u64 = 32;
pub const PARAM_EXPR_BASE:            u64 = 64;
pub const PARAM_SHARD_COL_BASE:       u64 = 128;
pub const PARAM_CONST_STR_BASE:       u64 = 160;
pub const PARAM_NULL_EXTEND_COL_BASE: u64 = 192;

// ---------------------------------------------------------------------------
// Aggregate function IDs
// ---------------------------------------------------------------------------

pub const AGG_COUNT:          u64 = 1;
pub const AGG_SUM:            u64 = 2;
pub const AGG_MIN:            u64 = 3;
pub const AGG_MAX:            u64 = 4;
pub const AGG_COUNT_NON_NULL: u64 = 5;

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
pub const CIRCUIT_NODES_TAB:    u64 = 11;
pub const CIRCUIT_EDGES_TAB:    u64 = 12;
pub const CIRCUIT_SOURCES_TAB:  u64 = 13;
pub const CIRCUIT_PARAMS_TAB:   u64 = 14;
pub const CIRCUIT_GROUP_COLS_TAB: u64 = 15;

pub const FIRST_USER_TABLE_ID:  u64 = 16;
pub const FIRST_USER_SCHEMA_ID: u64 = 3;

pub const OWNER_KIND_TABLE: u64 = 0;
pub const OWNER_KIND_VIEW:  u64 = 1;

// ---------------------------------------------------------------------------
// Wire protocol flags
// ---------------------------------------------------------------------------

pub const FLAG_SHUTDOWN:       u64 = 4;
pub const FLAG_DDL_SYNC:       u64 = 8;
pub const FLAG_EXCHANGE:       u64 = 16;
pub const FLAG_PUSH:           u64 = 32;
pub const FLAG_HAS_PK:         u64 = 64;
pub const FLAG_SEEK:           u64 = 128;
pub const FLAG_SEEK_BY_INDEX:  u64 = 256;
/// Marker bit set on FLAG_PUSH messages that carry an explicit
/// `WireConflictMode` in the low byte of `seek_col_idx`. When the bit
/// is absent, decoders default to `WireConflictMode::Update`, so SAL
/// entries written without a mode retain silent-upsert semantics.
pub const FLAG_CONFLICT_MODE_PRESENT: u64 = 1 << 15;
pub const FLAG_HAS_SCHEMA:     u64 = 1 << 48;
pub const FLAG_HAS_DATA:       u64 = 1 << 49;

// ---------------------------------------------------------------------------
// Wire-level conflict mode for INSERT / UPSERT semantics
// ---------------------------------------------------------------------------

/// Conflict-resolution mode carried on FLAG_PUSH messages. Encoded as
/// the low byte of `seek_col_idx` when `FLAG_CONFLICT_MODE_PRESENT` is
/// set.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireConflictMode {
    /// Reject the batch on any PK conflict. The master runs both an
    /// intra-batch duplicate check and an against-store PK existence
    /// check, and returns a PG-style `duplicate key value violates
    /// unique constraint` error.
    Error = 0,
    /// Retract-and-insert on PK conflict. Used for SQL `UPDATE`,
    /// `INSERT ... ON CONFLICT ... DO UPDATE` (after client-side
    /// merging), and explicit Python `push(conflict_mode="update")`.
    Update = 1,
}

impl WireConflictMode {
    #[inline]
    pub const fn as_u8(self) -> u8 { self as u8 }

    /// Unknown bytes decode as `Update` so forward-compatible decoders
    /// still see last-write-wins semantics.
    #[inline]
    pub const fn from_u8(v: u8) -> Self {
        match v {
            0 => WireConflictMode::Error,
            _ => WireConflictMode::Update,
        }
    }
}

pub const STATUS_OK:    u32 = 0;
pub const STATUS_ERROR: u32 = 1;

pub const META_FLAG_NULLABLE: u64 = 1;
pub const META_FLAG_IS_PK:    u64 = 2;

// ---------------------------------------------------------------------------
// WAL header constants
// ---------------------------------------------------------------------------

pub const WAL_HEADER_SIZE:   usize = 48;
pub const WAL_FORMAT_VERSION: u32  = 2;
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
