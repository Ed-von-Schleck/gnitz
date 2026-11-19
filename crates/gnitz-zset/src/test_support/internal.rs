//! The test helpers only this crate uses.
//!
//! Unlike [`super::shared`], this file is compiled once, inside `gnitz-zset`,
//! so it names crate-internals through `crate::` and needs nothing published on
//! its behalf.

use crate::repr::{Batch, BatchBuilder};
use crate::schema::{SchemaColumn, SchemaDescriptor};
use gnitz_wire::TypeCode;

use super::shared::u64_pk_schema;
use gnitz_foundation::rng::Rng;

/// A native little-endian cell of up to 16 bytes as the zero-extended value
/// `BatchBuilder::put_int` writes back at the cell's own width.
pub(crate) fn le_cell(cell: &[u8]) -> u128 {
    let mut v = [0u8; 16];
    v[..cell.len()].copy_from_slice(cell);
    u128::from_le_bytes(v)
}

/// A native of the integer type `tc` drawn from its zero, one, signed extremes
/// and all-ones, zero-extended: values tie often, so a tie falls through to the
/// next column.
pub(crate) fn edge_native(rng: &mut Rng, tc: TypeCode) -> u128 {
    let mask = gnitz_wire::image_mask(tc.wire_stride());
    let top = (mask >> 1) + 1;
    rng.pick(&[0, 1, top - 1, top, mask])
}

/// A random schema: `1..=MAX_PK_COLUMNS` PK columns of PK-eligible types and
/// `0..=6` payload columns drawn from `payload`, nullable or not, all
/// interleaved at random positions, with the PK list in random order.
pub(crate) fn random_schema(rng: &mut Rng, payload: &[TypeCode], nullable: bool) -> SchemaDescriptor {
    let pk_types: Vec<TypeCode> = TypeCode::ALL.iter().copied().filter(|t| t.is_pk_eligible()).collect();
    let n_pk = 1 + rng.gen_range(gnitz_wire::MAX_PK_COLUMNS as u64) as usize;
    let n_payload = rng.gen_range(7) as usize;
    let mut cols: Vec<SchemaColumn> = (0..n_pk)
        .map(|_| SchemaColumn::new(rng.pick(&pk_types), false))
        .collect();
    for _ in 0..n_payload {
        let null = nullable && rng.gen_range(2) == 1;
        cols.push(SchemaColumn::new(rng.pick(payload), null));
    }
    // `pos[i]` is column i's position; the PK list is the PK columns' positions.
    let mut pos: Vec<u32> = (0..cols.len() as u32).collect();
    rng.shuffle(&mut pos);
    let mut placed = cols.clone();
    for (c, &p) in cols.iter().zip(&pos) {
        placed[p as usize] = *c;
    }
    SchemaDescriptor::new(&placed, &pos[..n_pk])
}

/// A `U64` key over `U8`, nullable `I64` and `U16` columns: a row is 35 bytes
/// and no payload region starts at a multiple of 8 of them.
pub(crate) fn ragged_schema() -> SchemaDescriptor {
    u64_pk_schema(&[
        SchemaColumn::new(TypeCode::U8, false),
        SchemaColumn::new(TypeCode::I64, true),
        SchemaColumn::new(TypeCode::U16, false),
    ])
}

/// `rows` rows of [`ragged_schema`], every fifth with its `I64` NULL.
pub(crate) fn ragged_batch(rows: usize) -> Batch {
    let mut b = BatchBuilder::new(&ragged_schema());
    for i in 0..rows as u128 {
        b.begin_row(i, 1 + i as i64 % 3);
        b.put_int(i % 251);
        b.put_opt_int((i % 5 != 0).then_some(i * 10));
        b.put_int(i % 65_521);
        b.end_row();
    }
    b.finish()
}

/// U64 pk + a single BLOB payload column.
pub(crate) fn make_schema_pk_u64_payload_blob() -> SchemaDescriptor {
    u64_pk_schema(&[SchemaColumn::new(TypeCode::Blob, false)])
}

/// `(pk, weight, payload cells)` rows against `schema`, `None` a NULL cell.
pub(crate) fn make_int_batch(schema: &SchemaDescriptor, rows: &[(u64, i64, &[Option<i64>])]) -> Batch {
    let mut batch = BatchBuilder::new(schema);
    for &(pk, weight, cells) in rows {
        batch.begin_row(pk as u128, weight);
        for &cell in cells {
            batch.put_opt_int(cell.map(|v| v as u128));
        }
        batch.end_row();
    }
    batch.finish()
}
