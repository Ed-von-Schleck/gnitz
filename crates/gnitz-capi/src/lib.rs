//! # Safety contract for every `pub unsafe extern "C" fn` below
//!
//! All FFI entry points share the same caller obligations:
//! - any raw pointer argument must either be null or point to a valid object
//!   of the documented type, produced by a prior `gnitz_*` call;
//! - the pointed-to object must not be aliased by another concurrent
//!   `gnitz_*` call from a different thread (handles are not thread-safe).
//!   A handle may be *moved* between threads, but concurrent use of one
//!   connection was always undefined behavior — and on a `tls://` target it
//!   is a data race on the TLS session state machine, not benign fd
//!   interleaving, so it corrupts the record stream;
//! - C string pointers must be non-null and terminate before the first
//!   character outside the caller's allocation;
//! - buffer pointers paired with a length must address at least that many
//!   bytes of readable / writable memory.
//!
//! Functions that violate these conditions exhibit undefined behavior.
//! The per-function `# Safety` clippy lint is silenced because the contract
//! is uniform across the crate.
#![allow(clippy::missing_safety_doc)]

use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};

use gnitz_core::protocol::types::type_code_from_u64;
use gnitz_core::{CircuitBuilder, ExprBuilder, GnitzClient};
use gnitz_core::{ColData, ColumnDef, Schema, TypeCode, ZSetBatch};
use gnitz_sql::SqlPlanner;

// ---------------------------------------------------------------------------
// TypeCode constants — cbindgen emits these as #define macros
// ---------------------------------------------------------------------------

pub const GNITZ_TYPE_U8: u32 = 1;
pub const GNITZ_TYPE_I8: u32 = 2;
pub const GNITZ_TYPE_U16: u32 = 3;
pub const GNITZ_TYPE_I16: u32 = 4;
pub const GNITZ_TYPE_U32: u32 = 5;
pub const GNITZ_TYPE_I32: u32 = 6;
pub const GNITZ_TYPE_F32: u32 = 7;
pub const GNITZ_TYPE_U64: u32 = 8;
pub const GNITZ_TYPE_I64: u32 = 9;
pub const GNITZ_TYPE_F64: u32 = 10;
pub const GNITZ_TYPE_STRING: u32 = 11;
pub const GNITZ_TYPE_U128: u32 = 12;
pub const GNITZ_TYPE_UUID: u32 = 13;
pub const GNITZ_TYPE_BLOB: u32 = 14;

// ---------------------------------------------------------------------------
// Opaque handle types
// cbindgen is told to exclude these struct bodies; forward declarations are
// injected via after_includes in cbindgen.toml.
// ---------------------------------------------------------------------------

pub struct GnitzConn(GnitzClient);

pub struct GnitzSchema(Schema);

pub struct GnitzBatch {
    schema: Schema,
    batch: ZSetBatch,
    /// Cached CStrings keyed by (col_idx, row) for gnitz_batch_get_string.
    /// Keyed dedup avoids unbounded growth on repeated reads; stale entries
    /// are pruned by gnitz_batch_rollback_last_row and gnitz_batch_set_string.
    cstring_cache: std::cell::RefCell<std::collections::HashMap<(usize, usize), CString>>,
}

impl GnitzBatch {
    fn from_parts(schema: Schema, batch: ZSetBatch) -> Self {
        Self {
            schema,
            batch,
            cstring_cache: std::cell::RefCell::new(std::collections::HashMap::new()),
        }
    }
}

pub struct GnitzExprBuilder(ExprBuilder);

pub struct GnitzExprProgram(gnitz_core::ExprProgram);

pub struct GnitzCircuitBuilder(CircuitBuilder);

pub struct GnitzCircuit(gnitz_core::Circuit);

// ---------------------------------------------------------------------------
// Thread-local error buffer
// ---------------------------------------------------------------------------

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_error(e: impl std::fmt::Display) {
    LAST_ERROR.with(|cell| {
        *cell.borrow_mut() = CString::new(e.to_string()).ok();
    });
}

fn clear_error() {
    LAST_ERROR.with(|cell| {
        *cell.borrow_mut() = None;
    });
}

// ---------------------------------------------------------------------------
// Null-safety macros
// ---------------------------------------------------------------------------

macro_rules! check_ptr {
    ($ptr:expr, $ret:expr) => {{
        if $ptr.is_null() {
            set_error("null pointer");
            return $ret;
        }
        unsafe { &*$ptr }
    }};
}

macro_rules! check_ptr_mut {
    ($ptr:expr, $ret:expr) => {{
        if $ptr.is_null() {
            set_error("null pointer");
            return $ret;
        }
        unsafe { &mut *$ptr }
    }};
}

/// Convert a C string pointer to &str. Returns "" for null pointers.
fn cstr<'a>(p: *const c_char) -> &'a str {
    if p.is_null() {
        return "";
    }
    unsafe { CStr::from_ptr(p) }.to_str().unwrap_or("")
}

/// Convert a C string pointer to &str with UTF-8 validation.
/// Returns Ok("") for null pointers, Err on invalid UTF-8.
fn cstr_checked<'a>(p: *const c_char) -> Result<&'a str, std::str::Utf8Error> {
    if p.is_null() {
        return Ok("");
    }
    unsafe { CStr::from_ptr(p) }.to_str()
}

// ---------------------------------------------------------------------------
// Error query
// ---------------------------------------------------------------------------

/// Returns the last error message for this thread.
/// The pointer is valid until the next gnitz API call on this thread.
/// Do NOT free this pointer.
#[no_mangle]
pub unsafe extern "C" fn gnitz_last_error() -> *const c_char {
    LAST_ERROR.with(|cell| cell.borrow().as_ref().map(|s| s.as_ptr()).unwrap_or(std::ptr::null()))
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// Connect to a running gnitz-server. Returns NULL on error.
#[no_mangle]
pub unsafe extern "C" fn gnitz_connect(socket_path: *const c_char) -> *mut GnitzConn {
    clear_error();
    match GnitzClient::connect(cstr(socket_path)) {
        Ok(c) => Box::into_raw(Box::new(GnitzConn(c))),
        Err(e) => {
            set_error(e);
            std::ptr::null_mut()
        }
    }
}

/// Disconnect and free. Safe to call with NULL.
#[no_mangle]
pub unsafe extern "C" fn gnitz_disconnect(conn: *mut GnitzConn) {
    if !conn.is_null() {
        unsafe { Box::from_raw(conn) }.0.close();
    }
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// Create a new empty schema with one or more PK columns.
///
/// `pk_cols[0..pk_count]` are 0-based column indices listed in compound-key
/// **sort order** — that is, `pk_cols=[2, 1]` means "sort by column 2 first,
/// then column 1". Single-PK callers pass `pk_count = 1` and a pointer to the
/// one PK index.
///
/// Full validation against the column list (in-bounds, non-nullable,
/// PK-eligible types) is deferred until the schema is consumed
/// (`gnitz_batch_new`, DDL calls), because columns are added *after* schema
/// creation. Duplicates and `pk_count` out of range are caught here.
///
/// Returns NULL on error.
#[no_mangle]
pub unsafe extern "C" fn gnitz_schema_new(pk_count: u32, pk_cols: *const u32) -> *mut GnitzSchema {
    clear_error();
    if pk_cols.is_null() || pk_count == 0 || pk_count as usize > gnitz_core::PK_LIST_MAX_COLS {
        set_error("gnitz_schema_new: invalid pk_count or null pk_cols");
        return std::ptr::null_mut();
    }
    let cols = unsafe { std::slice::from_raw_parts(pk_cols, pk_count as usize) };
    let pk_cols: Vec<usize> = cols.iter().map(|&c| c as usize).collect();
    // Defer ncols check to gnitz_batch_new (columns are added after this
    // returns); validate non-empty / arity / duplicates now.
    if let Err(e) = Schema::validate_pk_cols(&pk_cols, usize::MAX) {
        set_error(format!("gnitz_schema_new: {e}"));
        return std::ptr::null_mut();
    }
    Box::into_raw(Box::new(GnitzSchema(Schema {
        columns: vec![],
        pk_cols,
    })))
}

/// Returns the packed PK stride of the schema in bytes (sum of each PK
/// column's wire stride). Returns 0 if `schema` is null.
#[no_mangle]
pub unsafe extern "C" fn gnitz_schema_pk_stride(schema: *const GnitzSchema) -> usize {
    if schema.is_null() {
        return 0;
    }
    unsafe { (*schema).0.pk_stride() }
}

/// Append a column to the schema. type_code must be a GNITZ_TYPE_* constant.
/// Returns 0 on success, -1 on invalid type_code.
#[no_mangle]
pub unsafe extern "C" fn gnitz_schema_add_col(
    schema: *mut GnitzSchema,
    name: *const c_char,
    type_code: c_int,
    nullable: c_int,
) -> c_int {
    let s = check_ptr_mut!(schema, -1);
    let tc = match type_code_from_u64(type_code as u64) {
        Ok(t) => t,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };
    // Per-column PK rules (non-nullable, PK-eligible type) are enforced by
    // `Schema::validate_parts` at the consuming choke points (`gnitz_batch_new`,
    // the DDL calls), where the full column list exists.
    s.0.columns
        .push(ColumnDef::new(cstr(name).to_owned(), tc, nullable != 0));
    0
}

/// Number of columns in schema.
#[no_mangle]
pub unsafe extern "C" fn gnitz_schema_col_count(schema: *const GnitzSchema) -> u32 {
    check_ptr!(schema, 0).0.columns.len() as u32
}

/// Free a GnitzSchema. Safe to call with NULL.
#[no_mangle]
pub unsafe extern "C" fn gnitz_schema_free(schema: *mut GnitzSchema) {
    if !schema.is_null() {
        unsafe {
            drop(Box::from_raw(schema));
        }
    }
}

// ---------------------------------------------------------------------------
// Batch — construction
// ---------------------------------------------------------------------------

/// Create a new empty batch for the given schema.
/// Clones the schema internally; the GnitzSchema may be freed after this call.
#[no_mangle]
pub unsafe extern "C" fn gnitz_batch_new(schema: *const GnitzSchema) -> *mut GnitzBatch {
    clear_error();
    let s = check_ptr!(schema, std::ptr::null_mut());
    // Full schema admissibility (column cap, PK rules) — the first point where
    // the complete column list exists. Implies PK stride ≤ MAX_PK_BYTES: at most
    // PK_LIST_MAX_COLS eligible columns of wire stride ≤ 16 each.
    if let Err(e) = Schema::validate_parts(&s.0.pk_cols, &s.0.columns) {
        set_error(format!("gnitz_batch_new: {e}"));
        return std::ptr::null_mut();
    }
    let schema_clone = s.0.clone();
    let batch = ZSetBatch::new(&schema_clone);
    Box::into_raw(Box::new(GnitzBatch::from_parts(schema_clone, batch)))
}

/// Returns the packed PK stride of the batch's underlying schema in bytes.
/// Returns 0 if `batch` is null.
#[no_mangle]
pub unsafe extern "C" fn gnitz_batch_pk_stride(batch: *const GnitzBatch) -> usize {
    if batch.is_null() {
        return 0;
    }
    unsafe { (*batch).schema.pk_stride() }
}

/// Append one row.
///
/// - `pk_bytes` must point to exactly `gnitz_schema_pk_stride(schema)` bytes
///   of packed PK column values in compound-key order.
/// - `col_data` is a flat buffer of all non-PK fixed-width column values in
///   schema order, packed as little-endian bytes
///   (stride = `TypeCode::wire_stride`).
/// - String columns are NOT supported here; use `gnitz_batch_set_string` (and
///   `gnitz_batch_rollback_last_row` to recover if a subsequent set_string
///   call fails).
/// - `null_mask` bits map to **payload column indices** (dense, 0-based,
///   skipping all PK columns).
///
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn gnitz_batch_append_row(
    batch: *mut GnitzBatch,
    pk_bytes: *const u8,
    weight: i64,
    null_mask: u64,
    col_data: *const c_void,
    col_data_len: usize,
) -> c_int {
    let b = check_ptr_mut!(batch, -1);
    if pk_bytes.is_null() {
        set_error("pk_bytes is null");
        return -1;
    }
    let pk_stride = b.schema.pk_stride();
    let pk_slice = unsafe { std::slice::from_raw_parts(pk_bytes, pk_stride) };
    let t = gnitz_core::PkTuple::from_bytes(pk_slice);

    let col_data_slice: &[u8] = if col_data_len == 0 {
        &[]
    } else {
        if col_data.is_null() {
            set_error("col_data is null");
            return -1;
        }
        unsafe { std::slice::from_raw_parts(col_data as *const u8, col_data_len) }
    };
    match append_row_inner(b, t, weight, null_mask, col_data_slice) {
        Ok(()) => 0,
        Err(e) => {
            set_error(e);
            -1
        }
    }
}

fn append_row_inner(
    b: &mut GnitzBatch,
    pk: gnitz_core::PkTuple,
    weight: i64,
    null_mask: u64,
    col_data: &[u8],
) -> Result<(), String> {
    b.batch.pks.push_tuple(&pk);
    b.batch.weights.push(weight);
    b.batch.nulls.push(null_mask);

    let mut offset = 0usize;

    for ci in 0..b.batch.columns.len() {
        if b.schema.is_pk_col(ci) {
            continue;
        }
        let tc = b.schema.columns[ci].type_code;
        let stride = tc.wire_stride();
        match &mut b.batch.columns[ci] {
            ColData::Fixed(buf) => {
                if offset + stride > col_data.len() {
                    return Err(format!(
                        "col_data too short at col {}: need {} bytes, have {}",
                        ci,
                        stride,
                        col_data.len().saturating_sub(offset)
                    ));
                }
                buf.extend_from_slice(&col_data[offset..offset + stride]);
                offset += stride;
            }
            ColData::Strings(v) => {
                v.push(None); // placeholder; caller fills via gnitz_batch_set_string
            }
            ColData::Bytes(_) => {
                return Err(format!(
                    "col {ci} is a BLOB column; not yet supported in C API row writer"
                ));
            }
            ColData::U128s(v) => {
                if offset + 16 > col_data.len() {
                    return Err(format!("col_data too short for U128 at col {ci}"));
                }
                let lo = u64::from_le_bytes(col_data[offset..offset + 8].try_into().unwrap());
                let hi = u64::from_le_bytes(col_data[offset + 8..offset + 16].try_into().unwrap());
                v.push(((hi as u128) << 64) | lo as u128);
                offset += 16;
            }
        }
    }
    Ok(())
}

/// Truncate the last partially-written row from a batch. Call this when a
/// follow-up `gnitz_batch_set_string` fails after `gnitz_batch_append_row`
/// succeeded, so the batch is left at a consistent row count and may be
/// reused or retried.
#[no_mangle]
pub unsafe extern "C" fn gnitz_batch_rollback_last_row(batch: *mut GnitzBatch) {
    if batch.is_null() {
        return;
    }
    let b = unsafe { &mut *batch };
    let n = b.batch.pks.len();
    if n == 0 {
        return;
    }
    let removed = n - 1;
    b.batch.truncate(removed, &b.schema);
    b.cstring_cache.borrow_mut().retain(|&(_, row), _| row != removed);
}

/// Set a String column value for the last appended row.
/// Must be called after gnitz_batch_append_row for the same row.
/// value may be NULL for SQL NULL. Returns 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn gnitz_batch_set_string(batch: *mut GnitzBatch, col_idx: usize, value: *const c_char) -> c_int {
    let b = check_ptr_mut!(batch, -1);
    if col_idx >= b.batch.columns.len() {
        set_error("col_idx out of range");
        return -1;
    }
    let s = if value.is_null() {
        None
    } else {
        match unsafe { CStr::from_ptr(value) }.to_str() {
            Ok(s) => Some(s.to_owned()),
            Err(e) => {
                set_error(e);
                return -1;
            }
        }
    };
    match &mut b.batch.columns[col_idx] {
        ColData::Strings(v) => {
            let n_rows = b.batch.pks.len();
            if v.len() == n_rows {
                // Overwrite the None placeholder left by append_row_inner.
                // Invalidate any cached CString so subsequent get_string returns
                // the new value, not the pre-overwrite one.
                *v.last_mut().expect("v non-empty: v.len() == n_rows >= 1") = s;
                b.cstring_cache.borrow_mut().remove(&(col_idx, n_rows - 1));
                0
            } else {
                set_error(format!(
                    "col {}: already has {} strings for {} rows",
                    col_idx,
                    v.len(),
                    n_rows
                ));
                -1
            }
        }
        _ => {
            set_error("column is not a String column");
            -1
        }
    }
}

// ---------------------------------------------------------------------------
// Batch — accessors
// ---------------------------------------------------------------------------

/// Number of rows.
#[no_mangle]
pub unsafe extern "C" fn gnitz_batch_len(batch: *const GnitzBatch) -> usize {
    check_ptr!(batch, 0).batch.len()
}

/// Copy the raw packed PK bytes for `row` into `buf[0..pk_stride]`.
///
/// Returns 0 on success, -1 on error (`buf` null, `buf_len` < `pk_stride`,
/// or `row` out of range). Query `pk_stride` with `gnitz_batch_pk_stride`.
#[no_mangle]
pub unsafe extern "C" fn gnitz_batch_get_pk_bytes(
    batch: *const GnitzBatch,
    row: usize,
    buf: *mut u8,
    buf_len: usize,
) -> c_int {
    let b = check_ptr!(batch, -1);
    if row >= b.batch.pks.len() {
        set_error("row out of range");
        return -1;
    }
    if buf.is_null() {
        set_error("null buffer");
        return -1;
    }
    let pk_stride = b.schema.pk_stride();
    if buf_len < pk_stride {
        set_error("buffer too small for PK stride");
        return -1;
    }
    let t = b.batch.pks.get_tuple(row, pk_stride as u8);
    unsafe {
        std::ptr::copy_nonoverlapping(t.buf.as_ptr(), buf, pk_stride);
    }
    0
}

/// Weight for row i.
#[no_mangle]
pub unsafe extern "C" fn gnitz_batch_get_weight(batch: *const GnitzBatch, row: usize) -> i64 {
    let b = check_ptr!(batch, 0);
    *b.batch.weights.get(row).unwrap_or(&0)
}

/// Read an integer column value as i64 (works for all Fixed integer columns).
/// Returns 0 and sets last_error on type mismatch or out-of-bounds.
///
/// Dispatches on `TypeCode` so unsigned values keep their full range
/// (a U8 of 200 returns 200, not -56).
#[no_mangle]
pub unsafe extern "C" fn gnitz_batch_get_i64(batch: *const GnitzBatch, col_idx: usize, row: usize) -> i64 {
    let b = check_ptr!(batch, 0);
    if col_idx >= b.batch.columns.len() {
        set_error("column index out of range");
        return 0;
    }
    match b.batch.columns.get(col_idx) {
        Some(ColData::Fixed(buf)) => {
            let tc = b.schema.columns[col_idx].type_code;
            let stride = tc.wire_stride();
            let start = row * stride;
            if start + stride > buf.len() {
                set_error("row out of range");
                return 0;
            }
            let slice = &buf[start..start + stride];
            match tc {
                TypeCode::U8 => slice[0] as u64 as i64,
                TypeCode::I8 => slice[0] as i8 as i64,
                TypeCode::U16 => u16::from_le_bytes(slice.try_into().unwrap()) as u64 as i64,
                TypeCode::I16 => i16::from_le_bytes(slice.try_into().unwrap()) as i64,
                TypeCode::U32 => u32::from_le_bytes(slice.try_into().unwrap()) as u64 as i64,
                TypeCode::I32 => i32::from_le_bytes(slice.try_into().unwrap()) as i64,
                TypeCode::U64 | TypeCode::I64 => i64::from_le_bytes(slice.try_into().unwrap()),
                _ => {
                    set_error("unsupported integer column type");
                    0
                }
            }
        }
        _ => {
            set_error("column is not a Fixed integer column");
            0
        }
    }
}

/// Read a string column value.
/// Returns a pointer valid until gnitz_batch_free (or next append), or NULL for SQL NULL.
/// Sets last_error on type mismatch or out-of-bounds.
#[no_mangle]
pub unsafe extern "C" fn gnitz_batch_get_string(batch: *const GnitzBatch, col_idx: usize, row: usize) -> *const c_char {
    let b = check_ptr!(batch, std::ptr::null());
    if col_idx >= b.batch.columns.len() {
        set_error("column index out of range");
        return std::ptr::null();
    }
    match b.batch.columns.get(col_idx) {
        Some(ColData::Strings(v)) => match v.get(row) {
            Some(Some(s)) => {
                let mut cache = b.cstring_cache.borrow_mut();
                let entry = cache.entry((col_idx, row));
                match entry {
                    std::collections::hash_map::Entry::Occupied(oe) => oe.get().as_ptr(),
                    std::collections::hash_map::Entry::Vacant(ve) => match CString::new(s.as_str()) {
                        Ok(cs) => ve.insert(cs).as_ptr(),
                        Err(_) => {
                            set_error("String contains interior null bytes");
                            std::ptr::null()
                        }
                    },
                }
            }
            Some(None) => std::ptr::null(),
            None => {
                set_error("row out of range");
                std::ptr::null()
            }
        },
        _ => {
            set_error("column is not a String column");
            std::ptr::null()
        }
    }
}

/// Free a GnitzBatch. Safe to call with NULL.
#[no_mangle]
pub unsafe extern "C" fn gnitz_batch_free(batch: *mut GnitzBatch) {
    if !batch.is_null() {
        unsafe {
            drop(Box::from_raw(batch));
        }
    }
}

// ---------------------------------------------------------------------------
// DDL
// ---------------------------------------------------------------------------

/// Allocate a fresh table ID (needed before calling gnitz_circuit_new).
/// Returns 0 on error.
#[no_mangle]
pub unsafe extern "C" fn gnitz_alloc_table_id(conn: *mut GnitzConn) -> u64 {
    clear_error();
    let c = check_ptr_mut!(conn, 0);
    match c.0.alloc_table_id() {
        Ok(id) => id,
        Err(e) => {
            set_error(e);
            0
        }
    }
}

/// Allocate a fresh schema ID. Returns 0 on error.
#[no_mangle]
pub unsafe extern "C" fn gnitz_alloc_schema_id(conn: *mut GnitzConn) -> u64 {
    clear_error();
    let c = check_ptr_mut!(conn, 0);
    match c.0.alloc_schema_id() {
        Ok(id) => id,
        Err(e) => {
            set_error(e);
            0
        }
    }
}

/// Create a logical schema (namespace). Returns new schema ID, or 0 on error.
#[no_mangle]
pub unsafe extern "C" fn gnitz_create_schema(conn: *mut GnitzConn, name: *const c_char) -> u64 {
    clear_error();
    let c = check_ptr_mut!(conn, 0);
    match c.0.create_schema(cstr(name)) {
        Ok(id) => id,
        Err(e) => {
            set_error(e);
            0
        }
    }
}

/// Drop a logical schema. Returns 0 on success, -1 on error.
/// Does NOT cascade-drop tables or views.
#[no_mangle]
pub unsafe extern "C" fn gnitz_drop_schema(conn: *mut GnitzConn, name: *const c_char) -> c_int {
    clear_error();
    let c = check_ptr_mut!(conn, -1);
    match c.0.drop_schema(cstr(name)) {
        Ok(()) => 0,
        Err(e) => {
            set_error(e);
            -1
        }
    }
}

/// Create a table. pk_index is read from schema->pk_index.
/// unique_pk != 0 enables the unique-PK fast-path on the server.
/// Returns new table ID, or 0 on error.
#[no_mangle]
pub unsafe extern "C" fn gnitz_create_table(
    conn: *mut GnitzConn,
    schema_name: *const c_char,
    table_name: *const c_char,
    schema: *const GnitzSchema,
    unique_pk: c_int,
) -> u64 {
    clear_error();
    let c = check_ptr_mut!(conn, 0);
    let s = check_ptr!(schema, 0);
    let pk_slice: Vec<u32> = s.0.pk_cols.iter().map(|&c| c as u32).collect();
    match c.0.create_table(
        cstr(schema_name),
        cstr(table_name),
        // `false`/`0` = partitioned, default full-PK distribution; the C API has
        // no REPLICATED or CLUSTER BY surface (those ride on SQL DDL).
        &s.0.columns,
        &pk_slice,
        unique_pk != 0,
        false,
        0,
        // No inline UNIQUE constraint surface in the C API (those ride on SQL DDL).
        &[],
    ) {
        Ok(id) => id,
        Err(e) => {
            set_error(e);
            0
        }
    }
}

/// Drop a table. Returns 0 on success, -1 on error.
/// Does NOT cascade-drop dependent views.
#[no_mangle]
pub unsafe extern "C" fn gnitz_drop_table(
    conn: *mut GnitzConn,
    schema_name: *const c_char,
    table_name: *const c_char,
) -> c_int {
    clear_error();
    let c = check_ptr_mut!(conn, -1);
    match c.0.drop_table(cstr(schema_name), cstr(table_name)) {
        Ok(()) => 0,
        Err(e) => {
            set_error(e);
            -1
        }
    }
}

// ---------------------------------------------------------------------------
// DML
// ---------------------------------------------------------------------------

/// Push a batch to a table. Returns 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn gnitz_push(
    conn: *mut GnitzConn,
    table_id: u64,
    schema: *const GnitzSchema,
    batch: *const GnitzBatch,
) -> c_int {
    clear_error();
    let c = check_ptr_mut!(conn, -1);
    let s = check_ptr!(schema, -1);
    let b = check_ptr!(batch, -1);
    match c.0.push(table_id, &s.0, &b.batch) {
        Ok(_) => 0,
        Err(e) => {
            set_error(e);
            -1
        }
    }
}

/// Scan a table or view. Returns an owned GnitzBatch or NULL on error.
/// Caller must free with gnitz_batch_free.
///
/// schema: if non-NULL, its clone is embedded in the returned batch for column
/// accessor functions. If NULL, the server-returned schema is used; returns NULL
/// with an error if neither is available.
#[no_mangle]
pub unsafe extern "C" fn gnitz_scan(
    conn: *mut GnitzConn,
    table_id: u64,
    schema: *const GnitzSchema,
) -> *mut GnitzBatch {
    clear_error();
    let c = check_ptr_mut!(conn, std::ptr::null_mut());
    match c.0.scan(table_id) {
        Ok((server_schema, data, _)) => {
            let used_schema = if !schema.is_null() {
                unsafe { (*schema).0.clone() }
            } else if let Some(ss) = server_schema {
                (*ss).clone()
            } else {
                set_error("scan returned no schema; pass schema parameter");
                return std::ptr::null_mut();
            };
            let batch = data.unwrap_or_else(|| ZSetBatch::new(&used_schema));
            Box::into_raw(Box::new(GnitzBatch::from_parts(used_schema, batch)))
        }
        Err(e) => {
            set_error(e);
            std::ptr::null_mut()
        }
    }
}

/// Delete rows by primary key.
///
/// `pks_bytes` is a flat buffer of `n_rows * gnitz_schema_pk_stride(schema)`
/// bytes containing packed PK values for each row.
///
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn gnitz_delete(
    conn: *mut GnitzConn,
    table_id: u64,
    schema: *const GnitzSchema,
    pks_bytes: *const u8,
    n_rows: usize,
) -> c_int {
    clear_error();
    let c = check_ptr_mut!(conn, -1);
    let s = check_ptr!(schema, -1);
    let stride = s.0.pk_stride();
    if stride == 0 {
        set_error("gnitz_delete: schema has no primary-key columns or zero-stride PK");
        return -1;
    }
    if n_rows > 0 && pks_bytes.is_null() {
        set_error("pks_bytes is null");
        return -1;
    }
    let flat = if n_rows > 0 {
        let Some(total_bytes) = n_rows.checked_mul(stride) else {
            set_error("gnitz_delete: n_rows * stride overflows usize");
            return -1;
        };
        unsafe { std::slice::from_raw_parts(pks_bytes, total_bytes) }
    } else {
        &[]
    };
    let mut pk_col = gnitz_core::PkColumn::empty_for_schema(&s.0);
    for chunk in flat.chunks_exact(stride) {
        pk_col.push_tuple(&gnitz_core::PkTuple::from_bytes(chunk));
    }
    match c.0.delete(table_id, &s.0, pk_col) {
        Ok(()) => 0,
        Err(e) => {
            set_error(e);
            -1
        }
    }
}

// ---------------------------------------------------------------------------
// Views
// ---------------------------------------------------------------------------

/// Create a simple scan+integrate view. Returns new view ID, or 0 on error.
#[no_mangle]
pub unsafe extern "C" fn gnitz_create_view(
    conn: *mut GnitzConn,
    schema_name: *const c_char,
    view_name: *const c_char,
    source_table_id: u64,
    output_schema: *const GnitzSchema,
) -> u64 {
    clear_error();
    let c = check_ptr_mut!(conn, 0);
    let s = check_ptr!(output_schema, 0);
    match c
        .0
        .create_view(cstr(schema_name), cstr(view_name), source_table_id, &s.0.columns)
    {
        Ok(id) => id,
        Err(e) => {
            set_error(e);
            0
        }
    }
}

/// Create a view from a pre-built circuit.
/// CONSUMES circuit — do not free it after this call.
/// Returns new view ID, or 0 on error.
#[no_mangle]
pub unsafe extern "C" fn gnitz_create_view_with_circuit(
    conn: *mut GnitzConn,
    schema_name: *const c_char,
    view_name: *const c_char,
    circuit: *mut GnitzCircuit,
    output_schema: *const GnitzSchema,
) -> u64 {
    clear_error();
    let c = check_ptr_mut!(conn, 0);
    let s = check_ptr!(output_schema, 0);
    if circuit.is_null() {
        set_error("null circuit");
        return 0;
    }
    let graph = unsafe { Box::from_raw(circuit) }.0;
    // The C API's hand-built circuits emit a single output PK at slot 0.
    match c
        .0
        .create_view_with_circuit(cstr(schema_name), cstr(view_name), "", graph, &s.0.columns, &[0])
    {
        Ok(id) => id,
        Err(e) => {
            set_error(e);
            0
        }
    }
}

/// Drop a view. Returns 0 on success, -1 on error.
#[no_mangle]
pub unsafe extern "C" fn gnitz_drop_view(
    conn: *mut GnitzConn,
    schema_name: *const c_char,
    view_name: *const c_char,
) -> c_int {
    clear_error();
    let c = check_ptr_mut!(conn, -1);
    match c.0.drop_view(cstr(schema_name), cstr(view_name)) {
        Ok(()) => 0,
        Err(e) => {
            set_error(e);
            -1
        }
    }
}

// ---------------------------------------------------------------------------
// ExprBuilder
// ---------------------------------------------------------------------------

/// Create a new expression builder.
#[no_mangle]
pub unsafe extern "C" fn gnitz_expr_new() -> *mut GnitzExprBuilder {
    Box::into_raw(Box::new(GnitzExprBuilder(ExprBuilder::new())))
}

#[no_mangle]
pub unsafe extern "C" fn gnitz_expr_load_col_int(b: *mut GnitzExprBuilder, col_idx: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.load_col_int(col_idx as usize)
}

#[no_mangle]
pub unsafe extern "C" fn gnitz_expr_load_col_float(b: *mut GnitzExprBuilder, col_idx: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.load_col_float(col_idx as usize)
}

#[no_mangle]
pub unsafe extern "C" fn gnitz_expr_load_const(b: *mut GnitzExprBuilder, value: i64) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.load_const(value)
}

#[no_mangle]
pub unsafe extern "C" fn gnitz_expr_add(b: *mut GnitzExprBuilder, a: u32, r: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.add(a, r)
}

#[no_mangle]
pub unsafe extern "C" fn gnitz_expr_sub(b: *mut GnitzExprBuilder, a: u32, r: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.sub(a, r)
}

#[no_mangle]
pub unsafe extern "C" fn gnitz_expr_cmp_eq(b: *mut GnitzExprBuilder, a: u32, r: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.cmp_eq(a, r)
}

#[no_mangle]
pub unsafe extern "C" fn gnitz_expr_cmp_ne(b: *mut GnitzExprBuilder, a: u32, r: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.cmp_ne(a, r)
}

#[no_mangle]
pub unsafe extern "C" fn gnitz_expr_cmp_gt(b: *mut GnitzExprBuilder, a: u32, r: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.cmp_gt(a, r)
}

#[no_mangle]
pub unsafe extern "C" fn gnitz_expr_cmp_ge(b: *mut GnitzExprBuilder, a: u32, r: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.cmp_ge(a, r)
}

#[no_mangle]
pub unsafe extern "C" fn gnitz_expr_cmp_lt(b: *mut GnitzExprBuilder, a: u32, r: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.cmp_lt(a, r)
}

#[no_mangle]
pub unsafe extern "C" fn gnitz_expr_cmp_le(b: *mut GnitzExprBuilder, a: u32, r: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.cmp_le(a, r)
}

#[no_mangle]
pub unsafe extern "C" fn gnitz_expr_bool_and(b: *mut GnitzExprBuilder, a: u32, r: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.bool_and(a, r)
}

#[no_mangle]
pub unsafe extern "C" fn gnitz_expr_bool_or(b: *mut GnitzExprBuilder, a: u32, r: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.bool_or(a, r)
}

#[no_mangle]
pub unsafe extern "C" fn gnitz_expr_bool_not(b: *mut GnitzExprBuilder, a: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.bool_not(a)
}

#[no_mangle]
pub unsafe extern "C" fn gnitz_expr_is_null(b: *mut GnitzExprBuilder, col_idx: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.is_null(col_idx as usize)
}

#[no_mangle]
pub unsafe extern "C" fn gnitz_expr_is_not_null(b: *mut GnitzExprBuilder, col_idx: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.is_not_null(col_idx as usize)
}

/// Consume the builder and return a compiled ExprProgram.
/// After this call the builder pointer is INVALID. Do NOT call gnitz_expr_builder_free.
#[no_mangle]
pub unsafe extern "C" fn gnitz_expr_build(builder: *mut GnitzExprBuilder, result_reg: u32) -> *mut GnitzExprProgram {
    if builder.is_null() {
        set_error("null pointer");
        return std::ptr::null_mut();
    }
    let prog = unsafe { Box::from_raw(builder) }.0.build(result_reg);
    Box::into_raw(Box::new(GnitzExprProgram(prog)))
}

/// Free a builder without building (error-path cleanup only).
#[no_mangle]
pub unsafe extern "C" fn gnitz_expr_builder_free(b: *mut GnitzExprBuilder) {
    if !b.is_null() {
        unsafe {
            drop(Box::from_raw(b));
        }
    }
}

/// Free an ExprProgram (only if not consumed by gnitz_circuit_filter).
#[no_mangle]
pub unsafe extern "C" fn gnitz_expr_program_free(p: *mut GnitzExprProgram) {
    if !p.is_null() {
        unsafe {
            drop(Box::from_raw(p));
        }
    }
}

// ---------------------------------------------------------------------------
// CircuitBuilder
// ---------------------------------------------------------------------------

/// Create a new circuit builder. view_id must have been obtained from
/// gnitz_alloc_table_id. primary_source_id is the source table's ID.
#[no_mangle]
pub unsafe extern "C" fn gnitz_circuit_new(view_id: u64, primary_source_id: u64) -> *mut GnitzCircuitBuilder {
    Box::into_raw(Box::new(GnitzCircuitBuilder(CircuitBuilder::new(
        view_id,
        primary_source_id,
    ))))
}

/// Add the primary input delta node. Returns node ID, or 0 on error.
#[no_mangle]
pub unsafe extern "C" fn gnitz_circuit_input_delta(cb: *mut GnitzCircuitBuilder) -> u64 {
    check_ptr_mut!(cb, 0).0.input_delta()
}

/// Add a trace-scan node for the given table. Returns node ID, or 0 on error.
#[no_mangle]
pub unsafe extern "C" fn gnitz_circuit_trace_scan(cb: *mut GnitzCircuitBuilder, table_id: u64) -> u64 {
    check_ptr_mut!(cb, 0).0.trace_scan(table_id)
}

/// Add a filter node. expr may be NULL (emits PARAM_FUNC_ID=0 only).
/// If non-NULL, CONSUMES expr — do not free it afterwards.
/// Returns node ID, or 0 on error.
#[no_mangle]
pub unsafe extern "C" fn gnitz_circuit_filter(
    cb: *mut GnitzCircuitBuilder,
    input: u64,
    expr: *mut GnitzExprProgram,
) -> u64 {
    let cb_ref = check_ptr_mut!(cb, 0);
    let expr_opt = if expr.is_null() {
        None
    } else {
        Some(unsafe { Box::from_raw(expr) }.0)
    };
    cb_ref.0.filter(input, expr_opt)
}

/// Build a slice from a C `ptr`+`len`, treating a NULL pointer or zero length
/// as empty. Caller guarantees a non-null `ptr` addresses `len` valid `T`s.
unsafe fn slice_or_empty<'a, T>(ptr: *const T, len: usize) -> &'a [T] {
    if ptr.is_null() || len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(ptr, len) }
    }
}

/// Add a map/projection node.
/// projection: array of output column indices (0-based), n_cols entries; may be NULL.
#[no_mangle]
pub unsafe extern "C" fn gnitz_circuit_map(
    cb: *mut GnitzCircuitBuilder,
    input: u64,
    projection: *const usize,
    n_cols: usize,
) -> u64 {
    let cb_ref = check_ptr_mut!(cb, 0);
    let cols = unsafe { slice_or_empty(projection, n_cols) };
    cb_ref.0.map(input, cols)
}

#[no_mangle]
pub unsafe extern "C" fn gnitz_circuit_negate(cb: *mut GnitzCircuitBuilder, input: u64) -> u64 {
    check_ptr_mut!(cb, 0).0.negate(input)
}

#[no_mangle]
pub unsafe extern "C" fn gnitz_circuit_union(cb: *mut GnitzCircuitBuilder, a: u64, b: u64) -> u64 {
    check_ptr_mut!(cb, 0).0.union(a, b)
}

#[no_mangle]
pub unsafe extern "C" fn gnitz_circuit_distinct(cb: *mut GnitzCircuitBuilder, input: u64) -> u64 {
    check_ptr_mut!(cb, 0).0.distinct(input)
}

/// Join: delta→port 0, internal trace_scan(trace_table_id)→port 1.
#[no_mangle]
pub unsafe extern "C" fn gnitz_circuit_join(cb: *mut GnitzCircuitBuilder, delta: u64, trace_table_id: u64) -> u64 {
    check_ptr_mut!(cb, 0).0.join(delta, trace_table_id)
}

/// Reduce with automatic shard insertion.
/// group_cols: array of column indices, n_group_cols entries; may be NULL/0 for no grouping.
#[no_mangle]
pub unsafe extern "C" fn gnitz_circuit_reduce(
    cb: *mut GnitzCircuitBuilder,
    input: u64,
    group_cols: *const usize,
    n_group_cols: usize,
    agg_func_id: u64,
    agg_col_idx: usize,
) -> u64 {
    let cb_ref = check_ptr_mut!(cb, 0);
    let gcols = unsafe { slice_or_empty(group_cols, n_group_cols) };
    cb_ref.0.reduce(input, gcols, agg_func_id, agg_col_idx)
}

/// Exchange shard. shard_cols: array of column indices, n_shard_cols entries.
#[no_mangle]
pub unsafe extern "C" fn gnitz_circuit_shard(
    cb: *mut GnitzCircuitBuilder,
    input: u64,
    shard_cols: *const usize,
    n_shard_cols: usize,
) -> u64 {
    let cb_ref = check_ptr_mut!(cb, 0);
    let cols = unsafe { slice_or_empty(shard_cols, n_shard_cols) };
    cb_ref.0.shard(input, cols)
}

/// Exchange gather. Collects results from all workers.
#[no_mangle]
pub unsafe extern "C" fn gnitz_circuit_gather(cb: *mut GnitzCircuitBuilder, input: u64) -> u64 {
    check_ptr_mut!(cb, 0).0.gather(input)
}

/// Add the integrate (sink) node.
#[no_mangle]
pub unsafe extern "C" fn gnitz_circuit_sink(cb: *mut GnitzCircuitBuilder, input: u64) -> u64 {
    check_ptr_mut!(cb, 0).0.sink(input)
}

/// Consume the builder and return a GnitzCircuit for gnitz_create_view_with_circuit.
/// After this call the builder pointer is INVALID. Do NOT call gnitz_circuit_builder_free.
#[no_mangle]
pub unsafe extern "C" fn gnitz_circuit_build(cb: *mut GnitzCircuitBuilder) -> *mut GnitzCircuit {
    if cb.is_null() {
        set_error("null pointer");
        return std::ptr::null_mut();
    }
    let graph = unsafe { Box::from_raw(cb) }.0.build();
    Box::into_raw(Box::new(GnitzCircuit(graph)))
}

/// Free a circuit builder without building (error-path cleanup only).
#[no_mangle]
pub unsafe extern "C" fn gnitz_circuit_builder_free(cb: *mut GnitzCircuitBuilder) {
    if !cb.is_null() {
        unsafe {
            drop(Box::from_raw(cb));
        }
    }
}

/// Free a GnitzCircuit (only if not consumed by gnitz_create_view_with_circuit).
#[no_mangle]
pub unsafe extern "C" fn gnitz_circuit_free(c: *mut GnitzCircuit) {
    if !c.is_null() {
        unsafe {
            drop(Box::from_raw(c));
        }
    }
}

// ---------------------------------------------------------------------------
// Seek (point lookup)
// ---------------------------------------------------------------------------

/// Point lookup by primary key. Returns a GnitzBatch with 0 or 1 rows via
/// `out_batch` (caller must free with `gnitz_batch_free`).
///
/// `pk_bytes` must point to `pk_len` bytes (`1 <= pk_len <= MAX_PK_BYTES`)
/// containing the packed PK in compound-key order. Returns 0 on success, -1
/// on error (check `gnitz_last_error`).
#[no_mangle]
pub unsafe extern "C" fn gnitz_seek(
    conn: *mut GnitzConn,
    table_id: u64,
    pk_bytes: *const u8,
    pk_len: usize,
    out_batch: *mut *mut GnitzBatch,
) -> c_int {
    clear_error();
    let c = check_ptr_mut!(conn, -1);
    if pk_bytes.is_null() || pk_len == 0 || pk_len > gnitz_core::MAX_PK_BYTES {
        set_error("gnitz_seek: invalid pk_bytes or pk_len");
        return -1;
    }
    let pk_slice = unsafe { std::slice::from_raw_parts(pk_bytes, pk_len) };
    let t = gnitz_core::PkTuple::from_bytes(pk_slice);
    match c.0.seek(table_id, &t) {
        Ok((server_schema, data, _)) => {
            if !out_batch.is_null() {
                let used_schema = server_schema
                    .map(|s| (*s).clone())
                    .unwrap_or_else(empty_schema_sentinel);
                let batch = data.unwrap_or_else(|| ZSetBatch::new(&used_schema));
                *out_batch = Box::into_raw(Box::new(GnitzBatch::from_parts(used_schema, batch)));
            }
            0
        }
        Err(e) => {
            set_error(e);
            -1
        }
    }
}

/// Placeholder schema returned to C callers when the server omits the schema
/// in a seek/seek_by_index response (today: no row matched). `pk_cols` is
/// empty so `pk_stride()` returns 0 instead of panicking on the unindexed
/// `self.columns[0]`.
fn empty_schema_sentinel() -> Schema {
    Schema {
        columns: vec![],
        pk_cols: vec![],
    }
}

/// Low-level seek on secondary index.
/// table_id: source TABLE id (not the index id).
/// col_indices_ptr/n_cols: the index's FULL declared column list (the server
///   matches the circuit by exact list), 0-based column indices.
/// key_vals_ptr/n_vals: `n_vals` 128-bit native key values, each stored as two
///   consecutive `u64` (`lo` then `hi`) — so the buffer holds `2 * n_vals` u64
///   elements. `n_vals` may be `< n_cols` for a leading-prefix seek. The lo/hi
///   split is the same portable encoding as the rest of this FFI; a raw
///   `*const u128` array is avoided because its `__int128` layout is arch-
///   dependent.
/// out_batch: receives pointer to result batch (NULL if not found).
/// Returns 0 on success, -1 on error (call gnitz_last_error for message).
#[no_mangle]
pub unsafe extern "C" fn gnitz_seek_by_index(
    conn: *mut GnitzConn,
    table_id: u64,
    col_indices_ptr: *const u32,
    n_cols: usize,
    key_vals_ptr: *const u64,
    n_vals: usize,
    out_batch: *mut *mut GnitzBatch,
) -> c_int {
    clear_error();
    let c = check_ptr_mut!(conn, -1);
    let cols: &[u32] = std::slice::from_raw_parts(col_indices_ptr, n_cols);
    // Read the lo/hi halves element-wise and reassemble each u128 by shifting —
    // never reinterpret `*const u64` as `*const u128` (a u64 buffer is only
    // 8-byte aligned, and a 16-byte u128 load over it is undefined behaviour).
    let halves: &[u64] = std::slice::from_raw_parts(key_vals_ptr, n_vals * 2);
    let keys: Vec<u128> = (0..n_vals)
        .map(|i| (halves[2 * i] as u128) | ((halves[2 * i + 1] as u128) << 64))
        .collect();
    match c.0.seek_by_index(table_id, cols, &keys) {
        Ok((server_schema, data, _)) => {
            if !out_batch.is_null() {
                match data {
                    Some(b) => {
                        let used_schema = server_schema
                            .map(|s| (*s).clone())
                            .unwrap_or_else(empty_schema_sentinel);
                        *out_batch = Box::into_raw(Box::new(GnitzBatch::from_parts(used_schema, b)));
                    }
                    None => {
                        *out_batch = std::ptr::null_mut();
                    }
                }
            }
            0
        }
        Err(e) => {
            set_error(e);
            -1
        }
    }
}

/// Execute a SQL statement. Returns the first result's ID (table_id/view_id/index_id/row_count).
/// Returns 0 on DROP/other, -1 on error. Check gnitz_last_error on -1.
/// out_id may be NULL.
#[no_mangle]
pub unsafe extern "C" fn gnitz_execute_sql(
    conn: *mut GnitzConn,
    sql: *const c_char,
    schema: *const c_char,
    out_id: *mut u64,
) -> c_int {
    clear_error();
    let c = check_ptr_mut!(conn, -1);
    let sql_str = match cstr_checked(sql) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return -1;
        }
    };
    let schema_name = cstr(schema);
    let mut planner = SqlPlanner::new(&mut c.0, schema_name);
    match planner.execute(sql_str) {
        Ok(results) => {
            if !out_id.is_null() {
                if let Some(r) = results.into_iter().next() {
                    use gnitz_sql::SqlResult;
                    *out_id = match r {
                        SqlResult::TableCreated { table_id } => table_id,
                        SqlResult::ViewCreated { view_id } => view_id,
                        SqlResult::IndexCreated { index_id } => index_id,
                        SqlResult::RowsAffected { count } => count as u64,
                        SqlResult::Dropped => 0,
                        SqlResult::Rows { .. } => 0,
                    };
                } else {
                    *out_id = 0;
                }
            }
            0
        }
        Err(e) => {
            set_error(e);
            -1
        }
    }
}

/// Returns 1 if the value at (col_idx, row) is SQL NULL, 0 if non-null.
/// Returns 1 and sets last_error on out-of-bounds arguments.
/// PK columns are always non-nullable; returns 0 for them.
#[no_mangle]
pub unsafe extern "C" fn gnitz_batch_is_null(batch: *const GnitzBatch, col_idx: usize, row: usize) -> c_int {
    let b = check_ptr!(batch, 1);
    if col_idx >= b.batch.columns.len() {
        set_error("column index out of range");
        return 1;
    }
    if row >= b.batch.pks.len() {
        set_error("row index out of range");
        return 1;
    }
    if b.schema.is_pk_col(col_idx) {
        return 0;
    }
    let payload_idx = b.schema.payload_idx(col_idx);
    if b.batch.nulls[row] & (1u64 << payload_idx) != 0 {
        1
    } else {
        0
    }
}

/// Read a float/double column value as f64.
/// F32 columns are widened to f64. Returns 0.0 and sets last_error on type
/// mismatch or out-of-bounds.
#[no_mangle]
pub unsafe extern "C" fn gnitz_batch_get_f64(batch: *const GnitzBatch, col_idx: usize, row: usize) -> f64 {
    let b = check_ptr!(batch, 0.0);
    if col_idx >= b.batch.columns.len() {
        set_error("column index out of range");
        return 0.0;
    }
    match b.batch.columns.get(col_idx) {
        Some(ColData::Fixed(buf)) => {
            let tc = b.schema.columns[col_idx].type_code;
            let stride = tc.wire_stride();
            let start = row * stride;
            if start + stride > buf.len() {
                set_error("row out of range");
                return 0.0;
            }
            let slice = &buf[start..start + stride];
            match tc {
                TypeCode::F32 => f32::from_le_bytes(slice.try_into().unwrap()) as f64,
                TypeCode::F64 => f64::from_le_bytes(slice.try_into().unwrap()),
                _ => {
                    set_error("column is not a float column");
                    0.0
                }
            }
        }
        _ => {
            set_error("column is not a Fixed float column");
            0.0
        }
    }
}

/// Execute a SQL query and return the result batch, or NULL on non-query
/// statements, empty results, or error. Caller must free with gnitz_batch_free.
#[no_mangle]
pub unsafe extern "C" fn gnitz_execute_sql_query(
    conn: *mut GnitzConn,
    sql: *const c_char,
    schema: *const c_char,
) -> *mut GnitzBatch {
    clear_error();
    let c = check_ptr_mut!(conn, std::ptr::null_mut());
    let sql_str = match cstr_checked(sql) {
        Ok(s) => s,
        Err(e) => {
            set_error(e);
            return std::ptr::null_mut();
        }
    };
    let schema_name = cstr(schema);
    let mut planner = SqlPlanner::new(&mut c.0, schema_name);
    match planner.execute(sql_str) {
        Ok(results) => {
            for r in results {
                if let gnitz_sql::SqlResult::Rows { schema, batch } = r {
                    return Box::into_raw(Box::new(GnitzBatch::from_parts(schema, batch)));
                }
            }
            std::ptr::null_mut()
        }
        Err(e) => {
            set_error(e);
            std::ptr::null_mut()
        }
    }
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

/// Free a C string allocated by the Rust library (forward-compatibility).
/// Safe to call with NULL.
#[no_mangle]
pub unsafe extern "C" fn gnitz_free_string(s: *mut c_char) {
    if !s.is_null() {
        unsafe {
            drop(CString::from_raw(s));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gnitz_core::TypeCode;
    use std::ffi::CString;

    #[test]
    fn test_set_string_alignment_ok() {
        // After Fix 4: gnitz_batch_append_row must succeed on String-column schemas.
        unsafe {
            let pk_cols: [u32; 1] = [0];
            let sch = build_schema(
                &pk_cols,
                &[("id", GNITZ_TYPE_U64, false), ("name", GNITZ_TYPE_STRING, false)],
            );
            let b = gnitz_batch_new(sch);
            assert!(!b.is_null());

            let pk: u64 = 1;
            assert_eq!(
                gnitz_batch_append_row(b, &pk as *const u64 as *const u8, 1, 0, std::ptr::null(), 0),
                0,
                "append_row must succeed on String-column schema"
            );

            let val = CString::new("hello").unwrap();
            assert_eq!(gnitz_batch_set_string(b, 1, val.as_ptr()), 0);

            gnitz_batch_free(b);
            gnitz_schema_free(sch);
        }
    }

    /// After Fix 4: a second gnitz_batch_set_string on the same column for the
    /// same row overwrites the placeholder and returns 0; gnitz_batch_get_string
    /// returns the new value.
    #[test]
    fn test_set_string_overwrites() {
        unsafe {
            let pk_cols: [u32; 1] = [0];
            let sch = build_schema(
                &pk_cols,
                &[("id", GNITZ_TYPE_U64, false), ("name", GNITZ_TYPE_STRING, false)],
            );
            let b = gnitz_batch_new(sch);
            assert!(!b.is_null());

            let pk: u64 = 1;
            assert_eq!(
                gnitz_batch_append_row(b, &pk as *const u64 as *const u8, 1, 0, std::ptr::null(), 0),
                0
            );

            let first = CString::new("hello").unwrap();
            assert_eq!(gnitz_batch_set_string(b, 1, first.as_ptr()), 0);

            let second = CString::new("world").unwrap();
            assert_eq!(
                gnitz_batch_set_string(b, 1, second.as_ptr()),
                0,
                "second set_string must overwrite, not overflow"
            );

            let stored = gnitz_batch_get_string(b, 1, 0);
            assert!(!stored.is_null());
            assert_eq!(CStr::from_ptr(stored).to_str().unwrap(), "world");

            gnitz_batch_free(b);
            gnitz_schema_free(sch);
        }
    }

    #[test]
    fn test_cstr_checked_valid() {
        let s = CString::new("hello").unwrap();
        assert_eq!(cstr_checked(s.as_ptr()), Ok("hello"));
    }

    #[test]
    fn test_cstr_checked_null() {
        assert_eq!(cstr_checked(std::ptr::null()), Ok(""));
    }

    #[test]
    fn test_cstr_checked_invalid() {
        // 0xFF is not valid UTF-8, followed by null terminator
        let bytes: [u8; 2] = [0xFF, 0x00];
        let ptr = bytes.as_ptr() as *const c_char;
        assert!(cstr_checked(ptr).is_err());
    }

    // -----------------------------------------------------------------
    // Compound-PK / pk_bytes API surface
    // -----------------------------------------------------------------

    /// Helper: drive `gnitz_schema_new` + `gnitz_schema_add_col` to build a
    /// schema the way C callers would (so tests exercise the FFI, not just
    /// the internal `Schema` struct).
    unsafe fn build_schema(pk_cols: &[u32], cols: &[(&str, u32, bool)]) -> *mut GnitzSchema {
        let sch = gnitz_schema_new(pk_cols.len() as u32, pk_cols.as_ptr());
        assert!(!sch.is_null(), "gnitz_schema_new returned NULL");
        for (name, tc, nullable) in cols {
            let cname = CString::new(*name).unwrap();
            assert_eq!(
                gnitz_schema_add_col(sch, cname.as_ptr(), *tc as c_int, if *nullable { 1 } else { 0 }),
                0,
                "gnitz_schema_add_col failed for {name}",
            );
        }
        sch
    }

    #[test]
    fn schema_new_rejects_zero_pk_count() {
        let cols: [u32; 1] = [0];
        let sch = unsafe { gnitz_schema_new(0, cols.as_ptr()) };
        assert!(sch.is_null());
    }

    #[test]
    fn schema_new_rejects_null_pk_cols() {
        let sch = unsafe { gnitz_schema_new(1, std::ptr::null()) };
        assert!(sch.is_null());
    }

    #[test]
    fn schema_new_rejects_duplicate_pk_cols() {
        let cols: [u32; 2] = [0, 0];
        let sch = unsafe { gnitz_schema_new(2, cols.as_ptr()) };
        assert!(sch.is_null());
    }

    #[test]
    fn schema_new_rejects_excess_pk_count() {
        // The FFI PK cap is PK_LIST_MAX_COLS (the persisted PK-list codec width);
        // one past it must be rejected. Built relative to the constant so a future
        // bump tracks here rather than hardcoding the count.
        let n = gnitz_core::PK_LIST_MAX_COLS + 1;
        let cols: Vec<u32> = (0..n as u32).collect();
        let sch = unsafe { gnitz_schema_new(n as u32, cols.as_ptr()) };
        assert!(sch.is_null());
    }

    #[test]
    fn schema_pk_stride_single_u64() {
        unsafe {
            let pk: [u32; 1] = [0];
            let sch = build_schema(&pk, &[("id", GNITZ_TYPE_U64, false), ("v", GNITZ_TYPE_I64, false)]);
            assert_eq!(gnitz_schema_pk_stride(sch), 8);
            gnitz_schema_free(sch);
        }
    }

    #[test]
    fn schema_pk_stride_compound_u64_u32() {
        unsafe {
            let pk: [u32; 2] = [0, 1];
            let sch = build_schema(
                &pk,
                &[
                    ("a", GNITZ_TYPE_U64, false),
                    ("b", GNITZ_TYPE_U32, false),
                    ("v", GNITZ_TYPE_I64, false),
                ],
            );
            assert_eq!(gnitz_schema_pk_stride(sch), 12);
            gnitz_schema_free(sch);
        }
    }

    #[test]
    fn schema_pk_stride_null_returns_zero() {
        assert_eq!(unsafe { gnitz_schema_pk_stride(std::ptr::null()) }, 0);
        assert_eq!(unsafe { gnitz_batch_pk_stride(std::ptr::null()) }, 0);
    }

    #[test]
    fn batch_new_rejects_oob_pk_cols() {
        unsafe {
            // PK index 5, but the schema only has 2 columns.
            let pk: [u32; 1] = [5];
            let sch = gnitz_schema_new(1, pk.as_ptr());
            assert!(!sch.is_null());
            let id = CString::new("id").unwrap();
            let v = CString::new("v").unwrap();
            assert_eq!(gnitz_schema_add_col(sch, id.as_ptr(), GNITZ_TYPE_U64 as c_int, 0), 0);
            assert_eq!(gnitz_schema_add_col(sch, v.as_ptr(), GNITZ_TYPE_I64 as c_int, 0), 0);
            let b = gnitz_batch_new(sch);
            assert!(b.is_null());
            gnitz_schema_free(sch);
        }
    }

    #[test]
    fn batch_get_i64_unsigned_above_signed_range() {
        // U8 column with value 200 must return 200, not -56 (sign-extension bug).
        let schema = Schema {
            columns: vec![
                ColumnDef::new("id", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::U8, false),
            ],
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        let pk = gnitz_core::PkTuple::from_u128(8, 1u128);
        batch.pks.push_tuple(&pk);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] {
            buf.push(200u8);
        }
        let b = Box::into_raw(Box::new(GnitzBatch::from_parts(schema, batch)));
        assert_eq!(unsafe { gnitz_batch_get_i64(b, 1, 0) }, 200);
        unsafe {
            drop(Box::from_raw(b));
        }
    }

    #[test]
    fn batch_get_i64_signed_negative_i8() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("id", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::I8, false),
            ],
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        let pk = gnitz_core::PkTuple::from_u128(8, 1u128);
        batch.pks.push_tuple(&pk);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] {
            buf.push((-56i8) as u8);
        }
        let b = Box::into_raw(Box::new(GnitzBatch::from_parts(schema, batch)));
        assert_eq!(unsafe { gnitz_batch_get_i64(b, 1, 0) }, -56);
        unsafe {
            drop(Box::from_raw(b));
        }
    }

    #[test]
    fn batch_append_row_then_get_pk_bytes_single_u64() {
        unsafe {
            let pk_cols: [u32; 1] = [0];
            let sch = build_schema(&pk_cols, &[("id", GNITZ_TYPE_U64, false), ("v", GNITZ_TYPE_I64, false)]);
            let b = gnitz_batch_new(sch);
            assert!(!b.is_null());
            let pk_le: u64 = 42;
            let v: i64 = 1234;
            let rc = gnitz_batch_append_row(
                b,
                &pk_le as *const u64 as *const u8,
                1,
                0,
                &v as *const i64 as *const c_void,
                8,
            );
            assert_eq!(
                rc,
                0,
                "append_row failed: {:?}",
                std::ffi::CStr::from_ptr(gnitz_last_error()).to_string_lossy()
            );
            assert_eq!(gnitz_batch_pk_stride(b), 8);

            let mut out = [0u8; 8];
            assert_eq!(gnitz_batch_get_pk_bytes(b, 0, out.as_mut_ptr(), out.len()), 0);
            assert_eq!(u64::from_le_bytes(out), 42);
            assert_eq!(gnitz_batch_get_i64(b, 1, 0), 1234);

            // Buffer too small → -1.
            let mut small = [0u8; 4];
            assert_eq!(gnitz_batch_get_pk_bytes(b, 0, small.as_mut_ptr(), small.len()), -1);

            gnitz_batch_free(b);
            gnitz_schema_free(sch);
        }
    }

    /// Fix 4: gnitz_batch_append_row must succeed on String-column schemas and
    /// gnitz_batch_set_string must set the value for that row.
    #[test]
    fn test_append_row_string_column_ok() {
        unsafe {
            let pk_cols: [u32; 1] = [0];
            let sch = build_schema(
                &pk_cols,
                &[
                    ("id", GNITZ_TYPE_U64, false),
                    ("name", GNITZ_TYPE_STRING, false),
                    ("val", GNITZ_TYPE_I64, false),
                ],
            );
            let b = gnitz_batch_new(sch);
            assert!(!b.is_null());

            let pk: u64 = 7;
            let v: i64 = 100;
            assert_eq!(
                gnitz_batch_append_row(
                    b,
                    &pk as *const u64 as *const u8,
                    1,
                    0,
                    &v as *const i64 as *const c_void,
                    8,
                ),
                0,
                "append_row with String col must succeed"
            );

            let name_val = CString::new("alice").unwrap();
            assert_eq!(gnitz_batch_set_string(b, 1, name_val.as_ptr()), 0);

            assert_eq!(gnitz_batch_len(b), 1);
            let stored = gnitz_batch_get_string(b, 1, 0);
            assert!(!stored.is_null());
            assert_eq!(CStr::from_ptr(stored).to_str().unwrap(), "alice");

            gnitz_batch_free(b);
            gnitz_schema_free(sch);
        }
    }

    /// PK rules are enforced at the `gnitz_batch_new` choke point via
    /// `Schema::validate_parts`: nullable, String/Blob, and float PK columns
    /// are all rejected there (adding the column itself succeeds).
    #[test]
    fn test_batch_new_rejects_invalid_pk_column() {
        for (tc, nullable) in [
            (GNITZ_TYPE_U64, 1),    // nullable PK
            (GNITZ_TYPE_STRING, 0), // ineligible type
            (GNITZ_TYPE_BLOB, 0),   // ineligible type
            (GNITZ_TYPE_F64, 0),    // ineligible type (deny-lists missed floats)
        ] {
            unsafe {
                let pk_cols: [u32; 1] = [0];
                let sch = gnitz_schema_new(1, pk_cols.as_ptr());
                assert!(!sch.is_null());
                let name = CString::new("id").unwrap();
                assert_eq!(gnitz_schema_add_col(sch, name.as_ptr(), tc as c_int, nullable), 0);
                let b = gnitz_batch_new(sch);
                assert!(b.is_null(), "PK column (tc={tc}, nullable={nullable}) must be rejected");
                assert!(!gnitz_last_error().is_null());
                gnitz_schema_free(sch);
            }
        }
    }

    /// Fix 7: gnitz_batch_get_string with an interior null byte must return NULL
    /// and set last_error, rather than silently returning an empty string.
    #[test]
    fn test_get_string_interior_null_error() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("id", TypeCode::U64, false),
                ColumnDef::new("name", TypeCode::String, false),
            ],
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        batch.pks.push_u128(1u128);
        batch.weights.push(1);
        batch.nulls.push(0);
        // Inject a string with an embedded null byte.
        if let ColData::Strings(v) = &mut batch.columns[1] {
            v.push(Some("hel\0lo".to_string()));
        }
        let b = Box::into_raw(Box::new(GnitzBatch::from_parts(schema, batch)));
        unsafe {
            let ptr = gnitz_batch_get_string(b, 1, 0);
            assert!(ptr.is_null(), "embedded null byte must produce NULL return");
            assert!(!gnitz_last_error().is_null(), "last_error must be set");
            drop(Box::from_raw(b));
        }
    }

    /// Fix 7: after gnitz_batch_rollback_last_row the stale cache entry for that
    /// row must be purged so re-append returns the new string, not the old one.
    #[test]
    fn test_rollback_clears_string_cache_entry() {
        unsafe {
            let pk_cols: [u32; 1] = [0];
            let sch = build_schema(
                &pk_cols,
                &[("id", GNITZ_TYPE_U64, false), ("name", GNITZ_TYPE_STRING, false)],
            );
            let b = gnitz_batch_new(sch);
            assert!(!b.is_null());

            let pk: u64 = 1;
            assert_eq!(
                gnitz_batch_append_row(b, &pk as *const u64 as *const u8, 1, 0, std::ptr::null(), 0),
                0
            );
            let first = CString::new("before").unwrap();
            assert_eq!(gnitz_batch_set_string(b, 1, first.as_ptr()), 0);

            // Prime the cache by reading.
            let _ = gnitz_batch_get_string(b, 1, 0);

            // Rollback the row.
            gnitz_batch_rollback_last_row(b);
            assert_eq!(gnitz_batch_len(b), 0);

            // Re-append with a different string.
            let pk2: u64 = 2;
            assert_eq!(
                gnitz_batch_append_row(b, &pk2 as *const u64 as *const u8, 1, 0, std::ptr::null(), 0),
                0
            );
            let second = CString::new("after").unwrap();
            assert_eq!(gnitz_batch_set_string(b, 1, second.as_ptr()), 0);

            let stored = gnitz_batch_get_string(b, 1, 0);
            assert!(!stored.is_null());
            assert_eq!(
                CStr::from_ptr(stored).to_str().unwrap(),
                "after",
                "rollback must purge stale cache; re-appended string must be returned"
            );

            gnitz_batch_free(b);
            gnitz_schema_free(sch);
        }
    }

    /// Fix 9: gnitz_batch_is_null returns 0 for a non-null value.
    #[test]
    fn test_batch_is_null_non_null_value() {
        unsafe {
            let pk_cols: [u32; 1] = [0];
            let sch = build_schema(&pk_cols, &[("id", GNITZ_TYPE_U64, false), ("v", GNITZ_TYPE_I64, true)]);
            let b = gnitz_batch_new(sch);
            assert!(!b.is_null());
            let pk: u64 = 1;
            let v: i64 = 42;
            assert_eq!(
                gnitz_batch_append_row(
                    b,
                    &pk as *const u64 as *const u8,
                    1,
                    0,
                    &v as *const i64 as *const c_void,
                    8
                ),
                0
            );
            assert_eq!(gnitz_batch_is_null(b, 1, 0), 0, "non-null value must return 0");
            gnitz_batch_free(b);
            gnitz_schema_free(sch);
        }
    }

    /// Fix 9: gnitz_batch_is_null returns 1 for a null value.
    #[test]
    fn test_batch_is_null_null_value() {
        unsafe {
            let pk_cols: [u32; 1] = [0];
            let sch = build_schema(&pk_cols, &[("id", GNITZ_TYPE_U64, false), ("v", GNITZ_TYPE_I64, true)]);
            let b = gnitz_batch_new(sch);
            assert!(!b.is_null());
            let pk: u64 = 1;
            let zero: i64 = 0;
            // null_mask bit 0 set → payload column 0 (which is "v") is NULL.
            // Still provide zero bytes in col_data; the null bit overrides the value.
            assert_eq!(
                gnitz_batch_append_row(
                    b,
                    &pk as *const u64 as *const u8,
                    1,
                    1,
                    &zero as *const i64 as *const c_void,
                    8
                ),
                0
            );
            assert_eq!(gnitz_batch_is_null(b, 1, 0), 1, "null value must return 1");
            gnitz_batch_free(b);
            gnitz_schema_free(sch);
        }
    }

    /// Fix 9: gnitz_batch_is_null always returns 0 for PK columns (non-nullable).
    #[test]
    fn test_batch_is_null_pk_col_always_zero() {
        unsafe {
            let pk_cols: [u32; 1] = [0];
            let sch = build_schema(&pk_cols, &[("id", GNITZ_TYPE_U64, false), ("v", GNITZ_TYPE_I64, false)]);
            let b = gnitz_batch_new(sch);
            assert!(!b.is_null());
            let pk: u64 = 5;
            let v: i64 = 0;
            assert_eq!(
                gnitz_batch_append_row(
                    b,
                    &pk as *const u64 as *const u8,
                    1,
                    0,
                    &v as *const i64 as *const c_void,
                    8
                ),
                0
            );
            assert_eq!(gnitz_batch_is_null(b, 0, 0), 0, "PK column is never null");
            gnitz_batch_free(b);
            gnitz_schema_free(sch);
        }
    }

    /// Fix 9: gnitz_batch_is_null returns 1 and sets last_error on OOB col/row.
    #[test]
    fn test_batch_is_null_oob_returns_1_with_error() {
        unsafe {
            let pk_cols: [u32; 1] = [0];
            let sch = build_schema(&pk_cols, &[("id", GNITZ_TYPE_U64, false)]);
            let b = gnitz_batch_new(sch);
            assert!(!b.is_null());
            // Out-of-range column.
            let rc = gnitz_batch_is_null(b, 99, 0);
            assert_eq!(rc, 1);
            assert!(!gnitz_last_error().is_null());
            gnitz_batch_free(b);
            gnitz_schema_free(sch);
        }
    }

    /// Fix 10: gnitz_batch_get_f64 reads an F32 column (widened to f64).
    #[test]
    fn test_batch_get_f64_reads_f32_col() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("id", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::F32, false),
            ],
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        let pk = gnitz_core::PkTuple::from_u128(8, 1u128);
        batch.pks.push_tuple(&pk);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] {
            buf.extend_from_slice(&(2.5f32).to_le_bytes());
        }
        let b = Box::into_raw(Box::new(GnitzBatch::from_parts(schema, batch)));
        let got = unsafe { gnitz_batch_get_f64(b, 1, 0) };
        let expected = 2.5f32 as f64;
        assert!((got - expected).abs() < 1e-6, "F32 widened to f64: got {got}");
        unsafe {
            drop(Box::from_raw(b));
        }
    }

    /// Fix 10: gnitz_batch_get_f64 reads an F64 column directly.
    #[test]
    fn test_batch_get_f64_reads_f64_col() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("id", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::F64, false),
            ],
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        let pk = gnitz_core::PkTuple::from_u128(8, 1u128);
        batch.pks.push_tuple(&pk);
        batch.weights.push(1);
        batch.nulls.push(0);
        if let ColData::Fixed(buf) = &mut batch.columns[1] {
            buf.extend_from_slice(&(100.25f64).to_le_bytes());
        }
        let b = Box::into_raw(Box::new(GnitzBatch::from_parts(schema, batch)));
        let got = unsafe { gnitz_batch_get_f64(b, 1, 0) };
        assert!((got - 100.25f64).abs() < 1e-10, "F64 read: got {got}");
        unsafe {
            drop(Box::from_raw(b));
        }
    }

    /// Fix 10: gnitz_batch_get_f64 returns 0.0 and sets last_error on non-float col.
    #[test]
    fn test_batch_get_f64_type_mismatch() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("id", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::I64, false),
            ],
            pk_cols: vec![0],
        };
        let batch = ZSetBatch::new(&schema);
        let b = Box::into_raw(Box::new(GnitzBatch::from_parts(schema, batch)));
        let got = unsafe { gnitz_batch_get_f64(b, 1, 0) };
        assert_eq!(got, 0.0);
        assert!(
            unsafe { !gnitz_last_error().is_null() },
            "last_error must be set on type mismatch"
        );
        unsafe {
            drop(Box::from_raw(b));
        }
    }

    #[test]
    fn batch_append_row_then_get_pk_bytes_compound() {
        unsafe {
            let pk_cols: [u32; 2] = [0, 1];
            let sch = build_schema(
                &pk_cols,
                &[
                    ("a", GNITZ_TYPE_U64, false),
                    ("b", GNITZ_TYPE_U32, false),
                    ("v", GNITZ_TYPE_I64, false),
                ],
            );
            let b = gnitz_batch_new(sch);
            assert!(!b.is_null());

            // Packed PK bytes: 8-byte a (LE) || 4-byte b (LE).
            let mut pk_bytes = [0u8; 12];
            pk_bytes[..8].copy_from_slice(&7u64.to_le_bytes());
            pk_bytes[8..].copy_from_slice(&9u32.to_le_bytes());
            let v: i64 = 555;
            assert_eq!(
                gnitz_batch_append_row(b, pk_bytes.as_ptr(), 1, 0, &v as *const i64 as *const c_void, 8),
                0,
            );
            assert_eq!(gnitz_batch_pk_stride(b), 12);
            let mut out = [0u8; 12];
            assert_eq!(gnitz_batch_get_pk_bytes(b, 0, out.as_mut_ptr(), out.len()), 0);
            assert_eq!(&out[..], &pk_bytes[..]);
            assert_eq!(gnitz_batch_get_i64(b, 2, 0), 555);

            gnitz_batch_free(b);
            gnitz_schema_free(sch);
        }
    }

    #[test]
    fn batch_rollback_last_row_restores_lengths() {
        // After Fix 4: append_row works on String schemas. Append then rollback;
        // verify gnitz_batch_len drops from 1 → 0 and the Fixed col is cleared.
        unsafe {
            let pk_cols: [u32; 1] = [0];
            let sch = build_schema(
                &pk_cols,
                &[
                    ("id", GNITZ_TYPE_U64, false),
                    ("name", GNITZ_TYPE_STRING, false),
                    ("v", GNITZ_TYPE_U64, false),
                ],
            );
            let b = gnitz_batch_new(sch);
            assert!(!b.is_null());

            let pk_le: u64 = 1;
            let v: u64 = 99;
            assert_eq!(
                gnitz_batch_append_row(
                    b,
                    &pk_le as *const u64 as *const u8,
                    1,
                    0,
                    &v as *const u64 as *const c_void,
                    8,
                ),
                0,
                "append_row should succeed"
            );
            assert_eq!(gnitz_batch_len(b), 1);

            gnitz_batch_rollback_last_row(b);
            assert_eq!(gnitz_batch_len(b), 0);

            let br = &*b;
            if let ColData::Fixed(buf) = &br.batch.columns[2] {
                assert_eq!(buf.len(), 0);
            } else {
                panic!("col 2 should be Fixed");
            }

            // Rollback on empty batch is a no-op.
            gnitz_batch_rollback_last_row(b);
            assert_eq!(gnitz_batch_len(b), 0);

            gnitz_batch_free(b);
            gnitz_schema_free(sch);
        }
    }
}
