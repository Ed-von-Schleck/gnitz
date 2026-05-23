//! IPC control-block wire layout.
//!
//! The control WAL block carries the per-message header. Both `gnitz-engine`
//! (server) and `gnitz-protocol` (client) build/parse this block; the column
//! indices, payload indices, and null-bit positions live here so the two
//! implementations cannot drift.
//!
//! Schema (10 columns, pk_index = 0):
//!   col  0: msg_idx       U64   (PK placeholder; always 0)
//!   col  1: status        U64
//!   col  2: client_id     U64
//!   col  3: target_id     U64
//!   col  4: flags         U64
//!   col  5: seek_pk       U128
//!   col  6: seek_col_idx  U64
//!   col  7: request_id    U64    -- reactor reply-routing key
//!   col  8: error_msg     STRING (nullable)
//!   col  9: seek_pk_extra BLOB   (nullable) -- PK region bytes 16.. for a wide PK
//!
//! Reserved request_id values:
//!   0          -- "unsolicited"/"untagged" (pre-reactor reply path)
//!   u64::MAX   -- broadcast reply (one reply per worker per broadcast)
//!   other      -- master-allocated, monotonic per request

use crate::{type_code, WireSysCol};

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
