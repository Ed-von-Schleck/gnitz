//! The key and payload-order kernels over a schema, and the shaping of an
//! operator's output schema. The descriptor itself is `gnitz-expr`'s.

pub(crate) use gnitz_wire::ReduceOutKey;
pub(crate) use gnitz_wire::TypeCode;
#[cfg(test)]
pub(crate) use gnitz_wire::MAX_COLUMNS;
pub(crate) use gnitz_wire::{MAX_PK_BYTES, MAX_PK_COLUMNS};

/// Resolved column addressing, homed in the leaf `gnitz-expr` crate so the
/// expression evaluator (and, through it, the SQL client) shares one definition
/// with the engine. Re-exported here because it *is* a schema fact —
/// `SchemaDescriptor::locate` produces one.
pub(crate) use gnitz_expr::ColumnLocator;

/// The schema descriptor, homed in `gnitz-expr` so the client links the same
/// definition. Re-exported so a crate linking `gnitz-zset` alone can name it.
pub use gnitz_expr::{decode_schema_block, encode_schema_block, SchemaColumn, SchemaDescriptor};

/// Order-preserving primary-key (OPK) primitives — every native→OPK encoder
/// (whole PK, index leading span), compare/pack, and the
/// re-export of the width-tagged `PkBuf` those encoders return.
pub mod key;

/// The payload row order — the second term of the (PK, payload) total order —
/// and the per-schema selection between its two comparators.
pub(crate) mod payload_order;

/// The precomputed per-row read/encode plan for an index's OPK leading-key span.
/// Lives in [`key`] with the rest of the native→OPK encoders it shares its byte
/// contract with; re-exported here because a spec is derived from a pair of
/// schemas.
pub use key::KeySpec;

/// Accumulator for an operator's output schema: its PK columns, then its
/// payload columns.
pub(crate) struct DerivedSchema {
    cols: Vec<SchemaColumn>,
    pk_len: usize,
}

impl DerivedSchema {
    pub(crate) fn new() -> Self {
        DerivedSchema { cols: Vec::new(), pk_len: 0 }
    }

    /// Append one payload column.
    pub(crate) fn push(&mut self, col: SchemaColumn) {
        self.cols.push(col);
    }

    /// Append one PK column. Panics behind a payload column: `finish` numbers the
    /// PK `0..pk_len`.
    pub(crate) fn push_pk(&mut self, col: SchemaColumn) {
        assert_eq!(
            self.pk_len,
            self.cols.len(),
            "DerivedSchema: key column pushed behind a payload column"
        );
        self.cols.push(col);
        self.pk_len += 1;
    }

    /// Append `schema`'s PK columns in PK-list order.
    pub(crate) fn push_pk_of(&mut self, schema: &SchemaDescriptor) {
        schema.pk_columns().for_each(|(_, c)| self.push_pk(*c));
    }

    /// Append `schema`'s payload columns in schema order.
    pub(crate) fn push_payload_of(&mut self, schema: &SchemaDescriptor) {
        schema.payload_columns().for_each(|(_, c)| self.push(*c));
    }

    /// The schema pushed so far, as [`SchemaDescriptor::try_new`] admits it.
    pub(crate) fn finish(&self) -> Result<SchemaDescriptor, String> {
        let pk: Vec<u32> = (0..self.pk_len as u32).collect();
        Ok(SchemaDescriptor::try_new(&self.cols, &pk)?)
    }
}

// ---------------------------------------------------------------------------
// Schema-shaping free functions
//
// Derived purely from a `SchemaDescriptor` (no catalog or storage state), and
// shared across layers.
// ---------------------------------------------------------------------------

/// `schema`'s PK columns, then the payload columns `project` names, in that
/// order. `Err` for an entry that names no payload column, and for a list
/// overflowing the column limit — `project` may repeat an index.
pub fn project_schema(schema: &SchemaDescriptor, project: &[u32]) -> Result<SchemaDescriptor, String> {
    let mut b = DerivedSchema::new();
    b.push_pk_of(schema);
    for &p in project {
        if schema.payload_slot(p as usize).is_none() {
            return Err(format!(
                "column {p} is not a payload column of a {}-column schema",
                schema.num_columns()
            ));
        }
        b.push(schema.columns()[p as usize]);
    }
    b.finish()
}

#[cfg(test)]
#[path = "tests/schema.rs"]
mod tests;
