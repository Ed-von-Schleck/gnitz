use gnitz_store::schema::key::PkBuf;
use gnitz_store::schema::{SchemaColumn, SchemaDescriptor};
use gnitz_store::storage::Batch;
use gnitz_wire::type_code;

/// A single U64 PK column, no payload.
pub(super) fn u64_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0])
}

/// PK U64 at index 0, payload U64 at index 1.
pub(super) fn two_col_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::U64, 1),
        ],
        &[0],
    )
}

/// Rows are `(pk, weight, null_word, payload_col1_value)`.
pub(super) fn make_row_batch(schema: SchemaDescriptor, rows: &[(u128, i64, u64, i64)]) -> Batch {
    let mut batch = Batch::with_capacity(schema, rows.len().max(1));
    for &(pk, weight, null_word, payload_val) in rows {
        let lo = [payload_val];
        let hi = [0u64];
        let null_ptr: *const u8 = std::ptr::null();
        let ptrs = [null_ptr];
        let lens = [0u32];
        unsafe {
            batch.append_row_simple(pk, weight, null_word, &lo, &hi, &ptrs, &lens);
        }
    }
    batch
}

/// Concatenate per-column OPK byte images into one compound PK.
pub(super) fn compound_pk_bytes(parts: &[&[u8]]) -> PkBuf {
    let mut v = Vec::new();
    for p in parts {
        v.extend_from_slice(p);
    }
    PkBuf::from_bytes(&v)
}
