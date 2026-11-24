//! The key and payload-order kernels over a schema, the radix sort under every
//! key sort, the shaping of a derived schema (an operator's output, a reduce's
//! output key and layout, or an index's layout), and a descriptor read from a
//! meta-schema record. The descriptor itself is `gnitz-expr`'s.

use gnitz_expr::{SchemaRefusal, leading_pk};
use gnitz_wire::MAX_COLUMNS;
use smallvec::SmallVec;

mod key;
mod payload_order;
mod radix;
mod reduce_out;
mod sort_key;

/// The schema descriptor, homed in `gnitz-expr` because its evaluator reads
/// through it. Re-exported so a crate linking `gnitz-zset` alone can name it.
pub use gnitz_expr::{SchemaColumn, SchemaDescriptor};
pub use key::{PROBE_KEY_INJECTIVE_BYTES, band, group_band, group_floor, key_above, probe_key, range_band};
pub use payload_order::{FixedIntNonnull, Generic, PayloadOrder, with_payload_cmp};
pub(crate) use radix::stable_radix_sort_by_key;
pub use radix::{RadixKey, radix_sort_by_key};
pub(crate) use reduce_out::natural_key;
pub use reduce_out::{ReduceOutSlot, reduce_out_layout};
pub(crate) use sort_key::{PkSortKey, pack_pk_be, pk_width_dispatch};
pub use sort_key::{compare_pk_ordering, pk_keys, pk_keys_of_images};

/// Rebuild a [`SchemaDescriptor`] from a meta-schema record, named or shape:
/// nothing engine-side reads a name, a hidden flag or a scale.
pub fn decode_schema_record(data: &[u8]) -> Result<SchemaDescriptor, String> {
    let mut cols: SmallVec<[SchemaColumn; MAX_COLUMNS]> = SmallVec::new();
    let pk = gnitz_wire::schema_record::decode(data, |c| cols.push(SchemaColumn::new(c.ty.tc, c.nullable)))?;
    Ok(SchemaDescriptor::try_new(&cols, pk.as_slice())?)
}

/// Builder for a derived schema: its PK columns, then its payload columns.
/// Inline up to `MAX_COLUMNS`; a longer list spills and `finish` refuses it.
#[derive(Default)]
pub struct DerivedSchema {
    cols: SmallVec<[SchemaColumn; MAX_COLUMNS]>,
    pk_len: usize,
}

impl DerivedSchema {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append one payload column.
    pub fn push(&mut self, col: SchemaColumn) {
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
    pub fn push_pk_of(&mut self, schema: &SchemaDescriptor) {
        schema.pk_columns().for_each(|c| self.push_pk(*c));
    }

    /// Append `schema`'s payload columns in schema order.
    pub fn push_payload_of(&mut self, schema: &SchemaDescriptor) {
        schema.payload_columns().for_each(|(_, c)| self.push(*c));
    }

    /// The payload columns pushed so far, in order.
    pub fn payload(&self) -> &[SchemaColumn] {
        &self.cols[self.pk_len..]
    }

    /// The schema pushed so far, as [`SchemaDescriptor::try_new`] admits it.
    pub fn finish(&self) -> Result<SchemaDescriptor, SchemaRefusal> {
        SchemaDescriptor::try_new(&self.cols, leading_pk(self.pk_len))
    }
}

/// `schema`'s PK columns, then the payload columns of the slots `read` names,
/// in slot order. `Err` for a bit past `schema`'s payload slots.
pub fn project_schema(schema: &SchemaDescriptor, read: u64) -> Result<SchemaDescriptor, String> {
    if read & !schema.payload_slots() != 0 {
        return Err(format!(
            "payload bits {read:#x} past a {}-column schema",
            schema.num_columns()
        ));
    }
    let mut b = DerivedSchema::new();
    b.push_pk_of(schema);
    (schema.payload_columns())
        .filter(|&(pi, _)| read >> pi & 1 != 0)
        .for_each(|(_, c)| b.push(*c));
    Ok(b.finish().expect("a subset of a schema's columns is a schema"))
}

#[cfg(test)]
#[path = "tests/schema.rs"]
mod tests;
