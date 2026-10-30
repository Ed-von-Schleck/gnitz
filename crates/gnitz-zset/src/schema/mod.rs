//! The key and payload-order kernels over a schema, and the shaping of a derived
//! schema: an operator's output or an index's layout. The descriptor itself is
//! `gnitz-expr`'s.

use gnitz_expr::SchemaRefusal;

/// The schema descriptor, homed in `gnitz-expr` so the client links the same
/// definition. Re-exported so a crate linking `gnitz-zset` alone can name it.
pub use gnitz_expr::{decode_schema_block, encode_schema_block, SchemaColumn, SchemaDescriptor};
pub use key::{group_band, key_above, probe_key, range_band};

pub(crate) mod key;
pub(crate) mod payload_order;

/// Builder for a derived schema: its PK columns, then its payload columns.
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
        schema.pk_columns().for_each(|c| self.push_pk(*c));
    }

    /// Append `schema`'s payload columns in schema order.
    pub(crate) fn push_payload_of(&mut self, schema: &SchemaDescriptor) {
        schema.payload_columns().for_each(|(_, c)| self.push(*c));
    }

    /// The payload columns pushed so far, in order.
    pub(crate) fn payload(&self) -> &[SchemaColumn] {
        &self.cols[self.pk_len..]
    }

    /// The schema pushed so far, as [`SchemaDescriptor::try_new`] admits it.
    pub(crate) fn finish(&self) -> Result<SchemaDescriptor, SchemaRefusal> {
        let pk: Vec<u32> = (0..self.pk_len as u32).collect();
        SchemaDescriptor::try_new(&self.cols, &pk)
    }
}

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
    Ok(b.finish()?)
}

#[cfg(test)]
#[path = "tests/schema.rs"]
mod tests;
