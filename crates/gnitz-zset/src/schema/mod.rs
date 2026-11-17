//! The key and payload-order kernels over a schema, the shaping of a derived
//! schema (an operator's output or an index's layout), and the nameless
//! schema-record codec a descriptor is stored and shipped as. The descriptor
//! itself is `gnitz-expr`'s.

use gnitz_expr::{SchemaRefusal, leading_pk};
use gnitz_wire::schema_record::RecordCol;
use gnitz_wire::{ColType, MAX_COLUMNS};
use smallvec::SmallVec;

mod key;
mod payload_order;

/// The schema descriptor, homed in `gnitz-expr` so the client links the same
/// definition. Re-exported so a crate linking `gnitz-zset` alone can name it.
pub use gnitz_expr::{SchemaColumn, SchemaDescriptor};
pub use key::{PROBE_KEY_INJECTIVE_BYTES, group_band, key_above, probe_key, range_band};

pub(crate) use key::{band, compare_pk_ordering, group_floor};
pub(crate) use payload_order::{FixedIntNonnull, Generic, PayloadOrder, with_payload_cmp};

/// Rebuild a [`SchemaDescriptor`] from a meta-schema record. Column names are
/// carried on the wire but nothing engine-side reads one.
pub fn decode_schema_record(data: &[u8]) -> Result<SchemaDescriptor, String> {
    let mut cols: SmallVec<[SchemaColumn; MAX_COLUMNS]> = SmallVec::new();
    let pk = gnitz_wire::schema_record::decode(data, |c| {
        cols.push(SchemaColumn::new(c.ty.tc, c.nullable));
        Ok(())
    })?;
    Ok(SchemaDescriptor::try_new(&cols, pk.as_slice())?)
}

/// Encode `schema`'s physical column shape — the inverse of
/// [`decode_schema_record`]: no names, no hidden flag, every scale zero.
pub fn encode_schema_record(schema: &SchemaDescriptor) -> Vec<u8> {
    let cols = schema.columns().iter().map(|c| RecordCol {
        ty: ColType::of(c.type_code),
        nullable: c.nullable,
        hidden: false,
        name: b"",
    });
    gnitz_wire::schema_record::encode(cols, schema.pk_cols())
}

/// Builder for a derived schema: its PK columns, then its payload columns.
/// Inline up to `MAX_COLUMNS`; a longer list spills and `finish` refuses it.
pub(crate) struct DerivedSchema {
    cols: SmallVec<[SchemaColumn; MAX_COLUMNS]>,
    pk_len: usize,
}

impl DerivedSchema {
    pub(crate) fn new() -> Self {
        DerivedSchema { cols: SmallVec::new(), pk_len: 0 }
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
        self.push(col);
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
        SchemaDescriptor::try_new(&self.cols, leading_pk(self.pk_len))
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
