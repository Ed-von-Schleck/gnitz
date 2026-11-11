//! The refusals of a write that collides on a primary key or a unique index.

use super::*;

use gnitz_zset::schema::SchemaDescriptor;

/// What a key collided with.
#[derive(Clone, Copy)]
pub(super) enum Clash {
    /// Another row of the same write.
    InBatch,
    /// A committed row.
    Committed,
}

/// A PK rendered from its OPK bytes for an error message: the per-column native
/// values in PK-list order, comma-separated. A calendar or decimal column
/// renders as the integer it is stored as.
fn format_pk_bytes(schema: &SchemaDescriptor, pk_bytes: &[u8]) -> String {
    use gnitz_wire::TypeCode;
    let mut off = 0usize;
    schema
        .pk_columns()
        .map(|col| {
            let cell = &pk_bytes[off..off + col.size()];
            off += cell.len();
            let tc = col.type_code;
            match gnitz_wire::FixedInt::from_type_code(tc) {
                Some(fi) if fi.is_signed() => format!("{}", gnitz_wire::decode_opk_i64(cell, fi)),
                Some(fi) => format!("{}", gnitz_wire::decode_opk_i64(cell, fi) as u64),
                None => {
                    let native = gnitz_wire::decode_opk_u128(cell, tc);
                    match tc {
                        TypeCode::UUID => gnitz_wire::format_uuid(native),
                        TypeCode::I128 => format!("{}", native as i128),
                        _ => format!("{native}"),
                    }
                }
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// The PG-style PK-uniqueness rejection for the key `pk_bytes` of `schema`.
pub(super) fn pk_violation_err(
    cat: &CatalogEngine,
    tid: u64,
    schema: &SchemaDescriptor,
    pk_bytes: &[u8],
    clash: Clash,
) -> WireFault {
    let key_str = format_pk_bytes(schema, pk_bytes);
    let (sn, tn) = cat.qualified_name_or_unknown(tid);
    let cols = cat.column_names(tid, schema.pk_cols());
    let what = match clash {
        Clash::InBatch => format!("Batch contains multiple rows with key ({cols})=({key_str})"),
        Clash::Committed => format!("Key ({cols})=({key_str}) already exists"),
    };
    integrity_violation(format!(
        "duplicate key value violates unique constraint \"{sn}_{tn}_pkey\": {what}"
    ))
}

/// The rejection of a write colliding on the unique index over `col_indices`.
pub(super) fn unique_violation_err(cat: &CatalogEngine, tid: u64, col_indices: &[u32], clash: Clash) -> WireFault {
    let (name, cols) = (cat.qualified_name(tid), cat.column_names(tid, col_indices));
    let text = match clash {
        Clash::InBatch => format!("Unique index violation on '{name}' column '{cols}': duplicate in batch"),
        Clash::Committed => format!("Unique index violation on '{name}' column '{cols}'"),
    };
    integrity_violation(text)
}

#[cfg(test)]
#[path = "tests/violation.rs"]
mod tests;
