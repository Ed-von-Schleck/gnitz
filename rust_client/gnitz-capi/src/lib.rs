use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};

use gnitz_core::{CircuitBuilder, ExprBuilder, GnitzClient};
use gnitz_protocol::{ColData, ColumnDef, Schema, TypeCode, ZSetBatch};
use gnitz_sql::SqlPlanner;

// ---------------------------------------------------------------------------
// TypeCode constants — cbindgen emits these as #define macros
// ---------------------------------------------------------------------------

pub const GNITZ_TYPE_U8: u32     = 1;
pub const GNITZ_TYPE_I8: u32     = 2;
pub const GNITZ_TYPE_U16: u32    = 3;
pub const GNITZ_TYPE_I16: u32    = 4;
pub const GNITZ_TYPE_U32: u32    = 5;
pub const GNITZ_TYPE_I32: u32    = 6;
pub const GNITZ_TYPE_F32: u32    = 7;
pub const GNITZ_TYPE_U64: u32    = 8;
pub const GNITZ_TYPE_I64: u32    = 9;
pub const GNITZ_TYPE_F64: u32    = 10;
pub const GNITZ_TYPE_STRING: u32 = 11;
pub const GNITZ_TYPE_U128: u32   = 12;

// ---------------------------------------------------------------------------
// Opaque handle types
// cbindgen is told to exclude these struct bodies; forward declarations are
// injected via after_includes in cbindgen.toml.
// ---------------------------------------------------------------------------

pub struct GnitzConn(GnitzClient);

pub struct GnitzSchema(Schema);

pub struct GnitzBatch {
    schema: Schema,
    batch:  ZSetBatch,
}

pub struct GnitzExprBuilder(ExprBuilder);

pub struct GnitzExprProgram(gnitz_core::ExprProgram);

pub struct GnitzCircuitBuilder(CircuitBuilder);

pub struct GnitzCircuit(gnitz_core::CircuitGraph);

// ---------------------------------------------------------------------------
// Thread-local error buffer
// ---------------------------------------------------------------------------

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = RefCell::new(None);
}

fn set_error(e: impl std::fmt::Display) {
    LAST_ERROR.with(|cell| {
        *cell.borrow_mut() = CString::new(e.to_string()).ok();
    });
}

fn clear_error() {
    LAST_ERROR.with(|cell| { *cell.borrow_mut() = None; });
}

// ---------------------------------------------------------------------------
// Null-safety macros
// ---------------------------------------------------------------------------

macro_rules! check_ptr {
    ($ptr:expr, $ret:expr) => {{
        if $ptr.is_null() { set_error("null pointer"); return $ret; }
        unsafe { &*$ptr }
    }};
}

macro_rules! check_ptr_mut {
    ($ptr:expr, $ret:expr) => {{
        if $ptr.is_null() { set_error("null pointer"); return $ret; }
        unsafe { &mut *$ptr }
    }};
}

/// Convert a C string pointer to &str. Returns "" for null pointers.
fn cstr<'a>(p: *const c_char) -> &'a str {
    if p.is_null() { return ""; }
    unsafe { CStr::from_ptr(p) }.to_str().unwrap_or("")
}

// ---------------------------------------------------------------------------
// Error query
// ---------------------------------------------------------------------------

/// Returns the last error message for this thread.
/// The pointer is valid until the next gnitz API call on this thread.
/// Do NOT free this pointer.
#[no_mangle]
pub extern "C" fn gnitz_last_error() -> *const c_char {
    LAST_ERROR.with(|cell| {
        cell.borrow()
            .as_ref()
            .map(|s| s.as_ptr())
            .unwrap_or(std::ptr::null())
    })
}

// ---------------------------------------------------------------------------
// Connection
// ---------------------------------------------------------------------------

/// Connect to a running gnitz-server. Returns NULL on error.
#[no_mangle]
pub extern "C" fn gnitz_connect(socket_path: *const c_char) -> *mut GnitzConn {
    clear_error();
    match GnitzClient::connect(cstr(socket_path)) {
        Ok(c)  => Box::into_raw(Box::new(GnitzConn(c))),
        Err(e) => { set_error(e); std::ptr::null_mut() }
    }
}

/// Disconnect and free. Safe to call with NULL.
#[no_mangle]
pub extern "C" fn gnitz_disconnect(conn: *mut GnitzConn) {
    if !conn.is_null() {
        unsafe { Box::from_raw(conn) }.0.close();
    }
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// Create a new empty schema. pk_index is the 0-based index of the PK column
/// (must equal the index of the column that will be added for the primary key).
#[no_mangle]
pub extern "C" fn gnitz_schema_new(pk_index: u32) -> *mut GnitzSchema {
    clear_error();
    Box::into_raw(Box::new(GnitzSchema(Schema {
        columns:  vec![],
        pk_index: pk_index as usize,
    })))
}

/// Append a column to the schema. type_code must be a GNITZ_TYPE_* constant.
/// Returns 0 on success, -1 on invalid type_code.
#[no_mangle]
pub extern "C" fn gnitz_schema_add_col(
    schema:    *mut GnitzSchema,
    name:      *const c_char,
    type_code: c_int,
    nullable:  c_int,
) -> c_int {
    let s = check_ptr_mut!(schema, -1);
    let tc = match TypeCode::try_from_u64(type_code as u64) {
        Ok(t)  => t,
        Err(e) => { set_error(e); return -1; }
    };
    s.0.columns.push(ColumnDef {
        name:        cstr(name).to_owned(),
        type_code:   tc,
        is_nullable: nullable != 0,
    });
    0
}

/// Number of columns in schema.
#[no_mangle]
pub extern "C" fn gnitz_schema_col_count(schema: *const GnitzSchema) -> u32 {
    check_ptr!(schema, 0).0.columns.len() as u32
}

/// Free a GnitzSchema. Safe to call with NULL.
#[no_mangle]
pub extern "C" fn gnitz_schema_free(schema: *mut GnitzSchema) {
    if !schema.is_null() { unsafe { drop(Box::from_raw(schema)); } }
}

// ---------------------------------------------------------------------------
// Batch — construction
// ---------------------------------------------------------------------------

/// Create a new empty batch for the given schema.
/// Clones the schema internally; the GnitzSchema may be freed after this call.
#[no_mangle]
pub extern "C" fn gnitz_batch_new(schema: *const GnitzSchema) -> *mut GnitzBatch {
    clear_error();
    let s = check_ptr!(schema, std::ptr::null_mut());
    let schema_clone = s.0.clone();
    let batch = ZSetBatch::new(&schema_clone);
    Box::into_raw(Box::new(GnitzBatch { schema: schema_clone, batch }))
}

/// Append one row. col_data is a flat buffer of all non-PK column values in
/// schema order, packed as little-endian bytes (stride = TypeCode::wire_stride).
/// String and U128 columns are NOT supported here; use gnitz_batch_set_string.
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn gnitz_batch_append_row(
    batch:        *mut GnitzBatch,
    pk_lo:        u64,
    pk_hi:        u64,
    weight:       i64,
    null_mask:    u64,
    col_data:     *const c_void,
    col_data_len: usize,
) -> c_int {
    let b = check_ptr_mut!(batch, -1);
    let col_data_slice: &[u8] = if col_data_len == 0 {
        &[]
    } else {
        if col_data.is_null() { set_error("col_data is null"); return -1; }
        unsafe { std::slice::from_raw_parts(col_data as *const u8, col_data_len) }
    };
    match append_row_inner(b, pk_lo, pk_hi, weight, null_mask, col_data_slice) {
        Ok(())  => 0,
        Err(e)  => { set_error(e); -1 }
    }
}

fn append_row_inner(
    b:         &mut GnitzBatch,
    pk_lo:     u64,
    pk_hi:     u64,
    weight:    i64,
    null_mask: u64,
    col_data:  &[u8],
) -> Result<(), String> {
    b.batch.pk_lo.push(pk_lo);
    b.batch.pk_hi.push(pk_hi);
    b.batch.weights.push(weight);
    b.batch.nulls.push(null_mask);

    let pk_idx = b.schema.pk_index;
    let mut offset = 0usize;

    for (ci, col) in b.batch.columns.iter_mut().enumerate() {
        if ci == pk_idx { continue; }
        let tc     = b.schema.columns[ci].type_code;
        let stride = tc.wire_stride();
        match col {
            ColData::Fixed(buf) => {
                if offset + stride > col_data.len() {
                    return Err(format!(
                        "col_data too short at col {}: need {} bytes, have {}",
                        ci, stride, col_data.len().saturating_sub(offset)));
                }
                buf.extend_from_slice(&col_data[offset..offset + stride]);
                offset += stride;
            }
            ColData::Strings(_) => {
                return Err(format!(
                    "col {} is a String column; use gnitz_batch_set_string", ci));
            }
            ColData::U128s(v) => {
                if offset + 16 > col_data.len() {
                    return Err(format!("col_data too short for U128 at col {}", ci));
                }
                let lo = u64::from_le_bytes(col_data[offset..offset+8].try_into().unwrap());
                let hi = u64::from_le_bytes(col_data[offset+8..offset+16].try_into().unwrap());
                v.push(((hi as u128) << 64) | lo as u128);
                offset += 16;
            }
        }
    }
    Ok(())
}

/// Set a String column value for the last appended row.
/// Must be called after gnitz_batch_append_row for the same row.
/// value may be NULL for SQL NULL. Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn gnitz_batch_set_string(
    batch:   *mut GnitzBatch,
    col_idx: usize,
    value:   *const c_char,
) -> c_int {
    let b = check_ptr_mut!(batch, -1);
    if col_idx >= b.batch.columns.len() {
        set_error("col_idx out of range"); return -1;
    }
    let s = if value.is_null() {
        None
    } else {
        match unsafe { CStr::from_ptr(value) }.to_str() {
            Ok(s)  => Some(s.to_owned()),
            Err(e) => { set_error(e); return -1; }
        }
    };
    match &mut b.batch.columns[col_idx] {
        ColData::Strings(v) => { v.push(s); 0 }
        _                   => { set_error("column is not a String column"); -1 }
    }
}

// ---------------------------------------------------------------------------
// Batch — accessors
// ---------------------------------------------------------------------------

/// Number of rows.
#[no_mangle]
pub extern "C" fn gnitz_batch_len(batch: *const GnitzBatch) -> usize {
    check_ptr!(batch, 0).batch.len()
}

/// pk_lo for row i. Returns 0 on null or out-of-range.
#[no_mangle]
pub extern "C" fn gnitz_batch_get_pk_lo(batch: *const GnitzBatch, row: usize) -> u64 {
    let b = check_ptr!(batch, 0);
    *b.batch.pk_lo.get(row).unwrap_or(&0)
}

/// pk_hi for row i.
#[no_mangle]
pub extern "C" fn gnitz_batch_get_pk_hi(batch: *const GnitzBatch, row: usize) -> u64 {
    let b = check_ptr!(batch, 0);
    *b.batch.pk_hi.get(row).unwrap_or(&0)
}

/// Weight for row i.
#[no_mangle]
pub extern "C" fn gnitz_batch_get_weight(batch: *const GnitzBatch, row: usize) -> i64 {
    let b = check_ptr!(batch, 0);
    *b.batch.weights.get(row).unwrap_or(&0)
}

/// Read an integer column value as i64 (works for all Fixed integer columns).
/// Returns 0 and sets last_error on type mismatch or out-of-bounds.
#[no_mangle]
pub extern "C" fn gnitz_batch_get_i64(
    batch:   *const GnitzBatch,
    col_idx: usize,
    row:     usize,
) -> i64 {
    let b = check_ptr!(batch, 0);
    match b.batch.columns.get(col_idx) {
        Some(ColData::Fixed(buf)) => {
            let stride = b.schema.columns[col_idx].type_code.wire_stride();
            let start  = row * stride;
            if start + stride > buf.len() { set_error("row out of range"); return 0; }
            let slice = &buf[start..start + stride];
            match stride {
                1 => slice[0] as i8  as i64,
                2 => i16::from_le_bytes(slice.try_into().unwrap()) as i64,
                4 => i32::from_le_bytes(slice.try_into().unwrap()) as i64,
                8 => i64::from_le_bytes(slice.try_into().unwrap()),
                _ => { set_error("unexpected stride"); 0 }
            }
        }
        _ => { set_error("column is not a Fixed integer column"); 0 }
    }
}

/// Read a string column value.
/// Returns a pointer valid until gnitz_batch_free (or next append), or NULL for SQL NULL.
/// Sets last_error on type mismatch or out-of-bounds.
#[no_mangle]
pub extern "C" fn gnitz_batch_get_string(
    batch:   *const GnitzBatch,
    col_idx: usize,
    row:     usize,
) -> *const c_char {
    let b = check_ptr!(batch, std::ptr::null());
    match b.batch.columns.get(col_idx) {
        Some(ColData::Strings(v)) => match v.get(row) {
            Some(Some(s)) => s.as_bytes().as_ptr() as *const c_char,
            Some(None)    => std::ptr::null(),
            None          => { set_error("row out of range"); std::ptr::null() }
        },
        _ => { set_error("column is not a String column"); std::ptr::null() }
    }
}

/// Free a GnitzBatch. Safe to call with NULL.
#[no_mangle]
pub extern "C" fn gnitz_batch_free(batch: *mut GnitzBatch) {
    if !batch.is_null() { unsafe { drop(Box::from_raw(batch)); } }
}

// ---------------------------------------------------------------------------
// DDL
// ---------------------------------------------------------------------------

/// Allocate a fresh table ID (needed before calling gnitz_circuit_new).
/// Returns 0 on error.
#[no_mangle]
pub extern "C" fn gnitz_alloc_table_id(conn: *mut GnitzConn) -> u64 {
    clear_error();
    let c = check_ptr_mut!(conn, 0);
    match c.0.alloc_table_id() { Ok(id) => id, Err(e) => { set_error(e); 0 } }
}

/// Allocate a fresh schema ID. Returns 0 on error.
#[no_mangle]
pub extern "C" fn gnitz_alloc_schema_id(conn: *mut GnitzConn) -> u64 {
    clear_error();
    let c = check_ptr_mut!(conn, 0);
    match c.0.alloc_schema_id() { Ok(id) => id, Err(e) => { set_error(e); 0 } }
}

/// Create a logical schema (namespace). Returns new schema ID, or 0 on error.
#[no_mangle]
pub extern "C" fn gnitz_create_schema(conn: *mut GnitzConn, name: *const c_char) -> u64 {
    clear_error();
    let c = check_ptr_mut!(conn, 0);
    match c.0.create_schema(cstr(name)) { Ok(id) => id, Err(e) => { set_error(e); 0 } }
}

/// Drop a logical schema. Returns 0 on success, -1 on error.
/// Does NOT cascade-drop tables or views.
#[no_mangle]
pub extern "C" fn gnitz_drop_schema(conn: *mut GnitzConn, name: *const c_char) -> c_int {
    clear_error();
    let c = check_ptr_mut!(conn, -1);
    match c.0.drop_schema(cstr(name)) { Ok(()) => 0, Err(e) => { set_error(e); -1 } }
}

/// Create a table. pk_index is read from schema->pk_index.
/// unique_pk != 0 enables the unique-PK fast-path on the server.
/// Returns new table ID, or 0 on error.
#[no_mangle]
pub extern "C" fn gnitz_create_table(
    conn:        *mut GnitzConn,
    schema_name: *const c_char,
    table_name:  *const c_char,
    schema:      *const GnitzSchema,
    unique_pk:   c_int,
) -> u64 {
    clear_error();
    let c = check_ptr_mut!(conn, 0);
    let s = check_ptr!(schema, 0);
    match c.0.create_table(
        cstr(schema_name), cstr(table_name),
        &s.0.columns, s.0.pk_index, unique_pk != 0,
    ) {
        Ok(id) => id, Err(e) => { set_error(e); 0 }
    }
}

/// Drop a table. Returns 0 on success, -1 on error.
/// Does NOT cascade-drop dependent views.
#[no_mangle]
pub extern "C" fn gnitz_drop_table(
    conn:        *mut GnitzConn,
    schema_name: *const c_char,
    table_name:  *const c_char,
) -> c_int {
    clear_error();
    let c = check_ptr_mut!(conn, -1);
    match c.0.drop_table(cstr(schema_name), cstr(table_name)) {
        Ok(()) => 0, Err(e) => { set_error(e); -1 }
    }
}

// ---------------------------------------------------------------------------
// DML
// ---------------------------------------------------------------------------

/// Push a batch to a table. Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn gnitz_push(
    conn:     *mut GnitzConn,
    table_id: u64,
    schema:   *const GnitzSchema,
    batch:    *const GnitzBatch,
) -> c_int {
    clear_error();
    let c = check_ptr_mut!(conn, -1);
    let s = check_ptr!(schema, -1);
    let b = check_ptr!(batch, -1);
    match c.0.push(table_id, &s.0, &b.batch) {
        Ok(()) => 0, Err(e) => { set_error(e); -1 }
    }
}

/// Scan a table or view. Returns an owned GnitzBatch or NULL on error.
/// Caller must free with gnitz_batch_free.
///
/// schema: if non-NULL, its clone is embedded in the returned batch for column
/// accessor functions. If NULL, the server-returned schema is used; returns NULL
/// with an error if neither is available.
#[no_mangle]
pub extern "C" fn gnitz_scan(
    conn:     *mut GnitzConn,
    table_id: u64,
    schema:   *const GnitzSchema,
) -> *mut GnitzBatch {
    clear_error();
    let c = check_ptr_mut!(conn, std::ptr::null_mut());
    match c.0.scan(table_id) {
        Ok((server_schema, data)) => {
            let used_schema = if !schema.is_null() {
                unsafe { (*schema).0.clone() }
            } else if let Some(ss) = server_schema {
                ss
            } else {
                set_error("scan returned no schema; pass schema parameter");
                return std::ptr::null_mut();
            };
            let batch = data.unwrap_or_else(|| ZSetBatch::new(&used_schema));
            Box::into_raw(Box::new(GnitzBatch { schema: used_schema, batch }))
        }
        Err(e) => { set_error(e); std::ptr::null_mut() }
    }
}

/// Delete rows by primary key. pks_lo and pks_hi must each contain n_rows entries.
/// Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn gnitz_delete(
    conn:     *mut GnitzConn,
    table_id: u64,
    schema:   *const GnitzSchema,
    pks_lo:   *const u64,
    pks_hi:   *const u64,
    n_rows:   usize,
) -> c_int {
    clear_error();
    let c = check_ptr_mut!(conn, -1);
    let s = check_ptr!(schema, -1);
    if pks_lo.is_null() || pks_hi.is_null() { set_error("null pk arrays"); return -1; }
    let lo = unsafe { std::slice::from_raw_parts(pks_lo, n_rows) };
    let hi = unsafe { std::slice::from_raw_parts(pks_hi, n_rows) };
    let pks: Vec<(u64, u64)> = lo.iter().copied().zip(hi.iter().copied()).collect();
    match c.0.delete(table_id, &s.0, &pks) {
        Ok(()) => 0, Err(e) => { set_error(e); -1 }
    }
}

// ---------------------------------------------------------------------------
// Views
// ---------------------------------------------------------------------------

/// Create a simple scan+integrate view. Returns new view ID, or 0 on error.
#[no_mangle]
pub extern "C" fn gnitz_create_view(
    conn:            *mut GnitzConn,
    schema_name:     *const c_char,
    view_name:       *const c_char,
    source_table_id: u64,
    output_schema:   *const GnitzSchema,
) -> u64 {
    clear_error();
    let c = check_ptr_mut!(conn, 0);
    let s = check_ptr!(output_schema, 0);
    match c.0.create_view(cstr(schema_name), cstr(view_name),
                           source_table_id, &s.0.columns) {
        Ok(id) => id, Err(e) => { set_error(e); 0 }
    }
}

/// Create a view from a pre-built circuit.
/// CONSUMES circuit — do not free it after this call.
/// Returns new view ID, or 0 on error.
#[no_mangle]
pub extern "C" fn gnitz_create_view_with_circuit(
    conn:          *mut GnitzConn,
    schema_name:   *const c_char,
    view_name:     *const c_char,
    circuit:       *mut GnitzCircuit,
    output_schema: *const GnitzSchema,
) -> u64 {
    clear_error();
    let c = check_ptr_mut!(conn, 0);
    let s = check_ptr!(output_schema, 0);
    if circuit.is_null() { set_error("null circuit"); return 0; }
    let graph = unsafe { Box::from_raw(circuit) }.0;
    match c.0.create_view_with_circuit(cstr(schema_name), cstr(view_name), "",
                                        graph, &s.0.columns) {
        Ok(id) => id, Err(e) => { set_error(e); 0 }
    }
}

/// Drop a view. Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn gnitz_drop_view(
    conn:        *mut GnitzConn,
    schema_name: *const c_char,
    view_name:   *const c_char,
) -> c_int {
    clear_error();
    let c = check_ptr_mut!(conn, -1);
    match c.0.drop_view(cstr(schema_name), cstr(view_name)) {
        Ok(()) => 0, Err(e) => { set_error(e); -1 }
    }
}

// ---------------------------------------------------------------------------
// ExprBuilder
// ---------------------------------------------------------------------------

/// Create a new expression builder.
#[no_mangle]
pub extern "C" fn gnitz_expr_new() -> *mut GnitzExprBuilder {
    Box::into_raw(Box::new(GnitzExprBuilder(ExprBuilder::new())))
}

#[no_mangle]
pub extern "C" fn gnitz_expr_load_col_int(b: *mut GnitzExprBuilder, col_idx: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.load_col_int(col_idx as usize)
}

#[no_mangle]
pub extern "C" fn gnitz_expr_load_col_float(b: *mut GnitzExprBuilder, col_idx: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.load_col_float(col_idx as usize)
}

#[no_mangle]
pub extern "C" fn gnitz_expr_load_const(b: *mut GnitzExprBuilder, value: i64) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.load_const(value)
}

#[no_mangle]
pub extern "C" fn gnitz_expr_add(b: *mut GnitzExprBuilder, a: u32, r: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.add(a, r)
}

#[no_mangle]
pub extern "C" fn gnitz_expr_sub(b: *mut GnitzExprBuilder, a: u32, r: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.sub(a, r)
}

#[no_mangle]
pub extern "C" fn gnitz_expr_cmp_eq(b: *mut GnitzExprBuilder, a: u32, r: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.cmp_eq(a, r)
}

#[no_mangle]
pub extern "C" fn gnitz_expr_cmp_ne(b: *mut GnitzExprBuilder, a: u32, r: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.cmp_ne(a, r)
}

#[no_mangle]
pub extern "C" fn gnitz_expr_cmp_gt(b: *mut GnitzExprBuilder, a: u32, r: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.cmp_gt(a, r)
}

#[no_mangle]
pub extern "C" fn gnitz_expr_cmp_ge(b: *mut GnitzExprBuilder, a: u32, r: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.cmp_ge(a, r)
}

#[no_mangle]
pub extern "C" fn gnitz_expr_cmp_lt(b: *mut GnitzExprBuilder, a: u32, r: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.cmp_lt(a, r)
}

#[no_mangle]
pub extern "C" fn gnitz_expr_cmp_le(b: *mut GnitzExprBuilder, a: u32, r: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.cmp_le(a, r)
}

#[no_mangle]
pub extern "C" fn gnitz_expr_bool_and(b: *mut GnitzExprBuilder, a: u32, r: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.bool_and(a, r)
}

#[no_mangle]
pub extern "C" fn gnitz_expr_bool_or(b: *mut GnitzExprBuilder, a: u32, r: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.bool_or(a, r)
}

#[no_mangle]
pub extern "C" fn gnitz_expr_bool_not(b: *mut GnitzExprBuilder, a: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.bool_not(a)
}

#[no_mangle]
pub extern "C" fn gnitz_expr_is_null(b: *mut GnitzExprBuilder, col_idx: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.is_null(col_idx as usize)
}

#[no_mangle]
pub extern "C" fn gnitz_expr_is_not_null(b: *mut GnitzExprBuilder, col_idx: u32) -> u32 {
    check_ptr_mut!(b, u32::MAX).0.is_not_null(col_idx as usize)
}

/// Consume the builder and return a compiled ExprProgram.
/// After this call the builder pointer is INVALID. Do NOT call gnitz_expr_builder_free.
#[no_mangle]
pub extern "C" fn gnitz_expr_build(
    builder:    *mut GnitzExprBuilder,
    result_reg: u32,
) -> *mut GnitzExprProgram {
    if builder.is_null() { set_error("null pointer"); return std::ptr::null_mut(); }
    let prog = unsafe { Box::from_raw(builder) }.0.build(result_reg);
    Box::into_raw(Box::new(GnitzExprProgram(prog)))
}

/// Free a builder without building (error-path cleanup only).
#[no_mangle]
pub extern "C" fn gnitz_expr_builder_free(b: *mut GnitzExprBuilder) {
    if !b.is_null() { unsafe { drop(Box::from_raw(b)); } }
}

/// Free an ExprProgram (only if not consumed by gnitz_circuit_filter).
#[no_mangle]
pub extern "C" fn gnitz_expr_program_free(p: *mut GnitzExprProgram) {
    if !p.is_null() { unsafe { drop(Box::from_raw(p)); } }
}

// ---------------------------------------------------------------------------
// CircuitBuilder
// ---------------------------------------------------------------------------

/// Create a new circuit builder. view_id must have been obtained from
/// gnitz_alloc_table_id. primary_source_id is the source table's ID.
#[no_mangle]
pub extern "C" fn gnitz_circuit_new(
    view_id:           u64,
    primary_source_id: u64,
) -> *mut GnitzCircuitBuilder {
    Box::into_raw(Box::new(GnitzCircuitBuilder(
        CircuitBuilder::new(view_id, primary_source_id),
    )))
}

/// Add the primary input delta node. Returns node ID, or 0 on error.
#[no_mangle]
pub extern "C" fn gnitz_circuit_input_delta(cb: *mut GnitzCircuitBuilder) -> u64 {
    check_ptr_mut!(cb, 0).0.input_delta()
}

/// Add a trace-scan node for the given table. Returns node ID, or 0 on error.
#[no_mangle]
pub extern "C" fn gnitz_circuit_trace_scan(
    cb:       *mut GnitzCircuitBuilder,
    table_id: u64,
) -> u64 {
    check_ptr_mut!(cb, 0).0.trace_scan(table_id)
}

/// Add a filter node. expr may be NULL (emits PARAM_FUNC_ID=0 only).
/// If non-NULL, CONSUMES expr — do not free it afterwards.
/// Returns node ID, or 0 on error.
#[no_mangle]
pub extern "C" fn gnitz_circuit_filter(
    cb:    *mut GnitzCircuitBuilder,
    input: u64,
    expr:  *mut GnitzExprProgram,
) -> u64 {
    let cb_ref = check_ptr_mut!(cb, 0);
    let expr_opt = if expr.is_null() {
        None
    } else {
        Some(unsafe { Box::from_raw(expr) }.0)
    };
    cb_ref.0.filter(input, expr_opt)
}

/// Add a map/projection node.
/// projection: array of output column indices (0-based), n_cols entries; may be NULL.
#[no_mangle]
pub extern "C" fn gnitz_circuit_map(
    cb:         *mut GnitzCircuitBuilder,
    input:      u64,
    projection: *const usize,
    n_cols:     usize,
) -> u64 {
    let cb_ref = check_ptr_mut!(cb, 0);
    let cols = if projection.is_null() { &[][..] }
               else { unsafe { std::slice::from_raw_parts(projection, n_cols) } };
    cb_ref.0.map(input, cols)
}

#[no_mangle]
pub extern "C" fn gnitz_circuit_negate(cb: *mut GnitzCircuitBuilder, input: u64) -> u64 {
    check_ptr_mut!(cb, 0).0.negate(input)
}

#[no_mangle]
pub extern "C" fn gnitz_circuit_union(
    cb: *mut GnitzCircuitBuilder, a: u64, b: u64,
) -> u64 {
    check_ptr_mut!(cb, 0).0.union(a, b)
}

#[no_mangle]
pub extern "C" fn gnitz_circuit_delay(cb: *mut GnitzCircuitBuilder, input: u64) -> u64 {
    check_ptr_mut!(cb, 0).0.delay(input)
}

#[no_mangle]
pub extern "C" fn gnitz_circuit_distinct(cb: *mut GnitzCircuitBuilder, input: u64) -> u64 {
    check_ptr_mut!(cb, 0).0.distinct(input)
}

/// Join: delta→port 0, internal trace_scan(trace_table_id)→port 1.
#[no_mangle]
pub extern "C" fn gnitz_circuit_join(
    cb:             *mut GnitzCircuitBuilder,
    delta:          u64,
    trace_table_id: u64,
) -> u64 {
    check_ptr_mut!(cb, 0).0.join(delta, trace_table_id)
}

#[no_mangle]
pub extern "C" fn gnitz_circuit_anti_join(
    cb:             *mut GnitzCircuitBuilder,
    delta:          u64,
    trace_table_id: u64,
) -> u64 {
    check_ptr_mut!(cb, 0).0.anti_join(delta, trace_table_id)
}

#[no_mangle]
pub extern "C" fn gnitz_circuit_semi_join(
    cb:             *mut GnitzCircuitBuilder,
    delta:          u64,
    trace_table_id: u64,
) -> u64 {
    check_ptr_mut!(cb, 0).0.semi_join(delta, trace_table_id)
}

/// Reduce with automatic shard insertion.
/// group_cols: array of column indices, n_group_cols entries; may be NULL/0 for no grouping.
#[no_mangle]
pub extern "C" fn gnitz_circuit_reduce(
    cb:           *mut GnitzCircuitBuilder,
    input:        u64,
    group_cols:   *const usize,
    n_group_cols: usize,
    agg_func_id:  u64,
    agg_col_idx:  usize,
) -> u64 {
    let cb_ref = check_ptr_mut!(cb, 0);
    let gcols = if group_cols.is_null() { &[][..] }
                else { unsafe { std::slice::from_raw_parts(group_cols, n_group_cols) } };
    cb_ref.0.reduce(input, gcols, agg_func_id, agg_col_idx)
}

/// Exchange shard. shard_cols: array of column indices, n_shard_cols entries.
#[no_mangle]
pub extern "C" fn gnitz_circuit_shard(
    cb:           *mut GnitzCircuitBuilder,
    input:        u64,
    shard_cols:   *const usize,
    n_shard_cols: usize,
) -> u64 {
    let cb_ref = check_ptr_mut!(cb, 0);
    let cols = if shard_cols.is_null() { &[][..] }
               else { unsafe { std::slice::from_raw_parts(shard_cols, n_shard_cols) } };
    cb_ref.0.shard(input, cols)
}

/// Exchange gather. Collects results from all workers at worker_id.
#[no_mangle]
pub extern "C" fn gnitz_circuit_gather(
    cb:        *mut GnitzCircuitBuilder,
    input:     u64,
    worker_id: u64,
) -> u64 {
    check_ptr_mut!(cb, 0).0.gather(input, worker_id)
}

/// Add the integrate (sink) node. view_id is informational only.
#[no_mangle]
pub extern "C" fn gnitz_circuit_sink(
    cb:      *mut GnitzCircuitBuilder,
    input:   u64,
    view_id: u64,
) -> u64 {
    check_ptr_mut!(cb, 0).0.sink(input, view_id)
}

/// Consume the builder and return a GnitzCircuit for gnitz_create_view_with_circuit.
/// After this call the builder pointer is INVALID. Do NOT call gnitz_circuit_builder_free.
#[no_mangle]
pub extern "C" fn gnitz_circuit_build(cb: *mut GnitzCircuitBuilder) -> *mut GnitzCircuit {
    if cb.is_null() { set_error("null pointer"); return std::ptr::null_mut(); }
    let graph = unsafe { Box::from_raw(cb) }.0.build();
    Box::into_raw(Box::new(GnitzCircuit(graph)))
}

/// Free a circuit builder without building (error-path cleanup only).
#[no_mangle]
pub extern "C" fn gnitz_circuit_builder_free(cb: *mut GnitzCircuitBuilder) {
    if !cb.is_null() { unsafe { drop(Box::from_raw(cb)); } }
}

/// Free a GnitzCircuit (only if not consumed by gnitz_create_view_with_circuit).
#[no_mangle]
pub extern "C" fn gnitz_circuit_free(c: *mut GnitzCircuit) {
    if !c.is_null() { unsafe { drop(Box::from_raw(c)); } }
}

// ---------------------------------------------------------------------------
// Seek (point lookup)
// ---------------------------------------------------------------------------

/// Point lookup by primary key. Returns a GnitzBatch with 0 or 1 rows.
/// Returns NULL on error (check gnitz_last_error).
/// Caller must free with gnitz_batch_free.
#[no_mangle]
pub unsafe extern "C" fn gnitz_seek(
    conn:      *mut GnitzConn,
    table_id:  u64,
    pk_lo:     u64,
    pk_hi:     u64,
    out_batch: *mut *mut GnitzBatch,
) -> c_int {
    clear_error();
    let c = check_ptr_mut!(conn, -1);
    match c.0.seek(table_id, pk_lo, pk_hi) {
        Ok((server_schema, data)) => {
            if !out_batch.is_null() {
                let used_schema = server_schema.unwrap_or_else(|| Schema { columns: vec![], pk_index: 0 });
                let batch = data.unwrap_or_else(|| ZSetBatch::new(&used_schema));
                *out_batch = Box::into_raw(Box::new(GnitzBatch { schema: used_schema, batch }));
            }
            0
        }
        Err(e) => { set_error(e); -1 }
    }
}

/// Execute a SQL statement. Returns the first result's ID (table_id/view_id/row_count).
/// Returns 0 on DROP/other, -1 on error. Check gnitz_last_error on -1.
/// out_id may be NULL.
#[no_mangle]
pub unsafe extern "C" fn gnitz_execute_sql(
    conn:    *mut GnitzConn,
    sql:     *const c_char,
    _sql_len: usize,
    schema:  *const c_char,
    out_id:  *mut u64,
) -> c_int {
    clear_error();
    let c = check_ptr_mut!(conn, -1);
    let sql_str = cstr(sql);
    let schema_name = cstr(schema);
    let planner = SqlPlanner::new(&c.0, schema_name);
    match planner.execute(sql_str) {
        Ok(results) => {
            if !out_id.is_null() {
                if let Some(r) = results.into_iter().next() {
                    use gnitz_sql::SqlResult;
                    *out_id = match r {
                        SqlResult::TableCreated { table_id } => table_id,
                        SqlResult::ViewCreated  { view_id  } => view_id,
                        SqlResult::RowsAffected { count    } => count as u64,
                        SqlResult::Dropped                   => 0,
                        SqlResult::Rows { .. }               => 0,
                    };
                } else {
                    *out_id = 0;
                }
            }
            0
        }
        Err(e) => { set_error(e); -1 }
    }
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

/// Free a C string allocated by the Rust library (forward-compatibility).
/// Safe to call with NULL.
#[no_mangle]
pub extern "C" fn gnitz_free_string(s: *mut c_char) {
    if !s.is_null() { unsafe { drop(CString::from_raw(s)); } }
}
