//! The **meta-schema record**: the one encoding that carries a relation's column
//! shape across the wire — engine → client on every schema-bearing reply, client
//! → engine on every schema-bearing push and per-family `PUSH_TXN` record.
//!
//! ```text
//! u32              column_count
//! u8               pk_count
//! u8 × pk_count    PK column indices, in declared PK-tuple order
//! per column:
//!   u8             type_code
//!   u8             flags        NULLABLE | HIDDEN
//!   u8             scale
//!   u32 + bytes    name
//! ```
//!
//! The record carries no length of its own; every carrier length-prefixes it.

use crate::codec::{decode_all, Reader, Writer};
use crate::{ColType, MAX_COLUMNS, MAX_PK_COLUMNS};

/// One column as the record carries it. `name` borrows the record's own bytes on
/// decode; on encode it is whatever the caller has (an empty slice for the
/// anonymous records that describe physical shape only).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SchemaBlockCol<'a> {
    pub ty: ColType,
    pub meta: ColMeta,
    pub name: &'a [u8],
}

/// One column's catalog facts as the record carries them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ColMeta {
    pub nullable: bool,
    /// A hidden key slot: a physical schema column (it holds a real PK/routing
    /// value) that no presentation surface exposes. The PK region, routing, sort
    /// and consolidation are all blind to it.
    pub hidden: bool,
}

const NULLABLE: u8 = 1 << 0;
const HIDDEN: u8 = 1 << 1;
const DEFINED_FLAGS: u8 = NULLABLE | HIDDEN;

impl ColMeta {
    fn flags(self) -> u8 {
        (if self.nullable { NULLABLE } else { 0 }) | (if self.hidden { HIDDEN } else { 0 })
    }

    fn from_flags(flags: u8) -> ColMeta {
        ColMeta {
            nullable: flags & NULLABLE != 0,
            hidden: flags & HIDDEN != 0,
        }
    }
}

/// Bytes one column occupies before its name: type code, flags, scale, and the
/// name's own length prefix.
const COL_FIXED_BYTES: usize = 3 + 4;

/// Written for a PK count or column index the record's `u8` fields cannot hold.
const PK_FIELD_OVERFLOW: u8 = u8::MAX;
const _: () = assert!(
    PK_FIELD_OVERFLOW as usize >= MAX_COLUMNS,
    "a PK index this wide must be refused"
);
const _: () = assert!(
    PK_FIELD_OVERFLOW as usize > MAX_PK_COLUMNS,
    "a PK count this wide must be refused"
);

/// One column, read forward.
fn take_col<'a>(r: &mut Reader<'a>) -> Result<SchemaBlockCol<'a>, String> {
    let code = r.u8()?;
    let flags = r.flags(DEFINED_FLAGS)?;
    let scale = r.u8()?;
    let ty = ColType::from_wire(code, scale).ok_or_else(|| format!("invalid column type {code}/{scale}"))?;
    let name = r.bytes32()?;
    Ok(SchemaBlockCol {
        ty,
        meta: ColMeta::from_flags(flags),
        name,
    })
}

/// The record for `cols` keyed by `pk_cols`, in declared PK-tuple order. `cols`
/// is walked twice: a clone sizes the record, the original writes it.
pub fn encode<'a>(cols: impl Iterator<Item = SchemaBlockCol<'a>> + Clone, pk_cols: &[u32]) -> Vec<u8> {
    let (count, columns) = cols.clone().fold((0usize, 0usize), |(n, b), c| {
        (n + 1, b + COL_FIXED_BYTES + c.name.len())
    });
    let mut w = Writer::with_capacity(4 + 1 + pk_cols.len() + columns);
    w.u32(count as u32)
        .u8(u8::try_from(pk_cols.len()).unwrap_or(PK_FIELD_OVERFLOW));
    for &c in pk_cols {
        debug_assert!(u8::try_from(c).is_ok(), "PK column index {c} does not fit the record");
        w.u8(u8::try_from(c).unwrap_or(PK_FIELD_OVERFLOW));
    }
    for c in cols {
        w.u8(c.ty.tc.as_wire())
            .u8(c.meta.flags())
            .u8(c.ty.scale)
            .bytes32(c.name);
    }
    w.into_vec()
}

/// A record's PK column indices, in declared PK-tuple order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PkIndices {
    idx: [u32; MAX_PK_COLUMNS],
    len: usize,
}

impl PkIndices {
    /// Not yet checked against the columns.
    #[inline]
    pub fn as_slice(&self) -> &[u32] {
        &self.idx[..self.len]
    }
}

/// Decode and validate `buf`, handing each column to `on_col` in physical order
/// once the column count is known to lie within [`MAX_COLUMNS`].
pub fn decode<'a>(
    buf: &'a [u8],
    mut on_col: impl FnMut(SchemaBlockCol<'a>) -> Result<(), String>,
) -> Result<PkIndices, String> {
    decode_all(buf, "schema record", |r| {
        let (count, pk) = take_header(r)?;
        for _ in 0..count {
            on_col(take_col(r)?)?;
        }
        Ok(pk)
    })
}

/// The header both records open with: column count and PK list, validated as
/// [`decode`] validates them.
fn take_header(r: &mut Reader<'_>) -> Result<(usize, PkIndices), String> {
    let count = r.u32()? as usize;
    if count == 0 || count > MAX_COLUMNS {
        return Err(format!("column count {count} out of range 1..={MAX_COLUMNS}"));
    }
    let len = r.u8()? as usize;
    if len > MAX_PK_COLUMNS {
        return Err(format!("pk column count {len} exceeds {MAX_PK_COLUMNS}"));
    }
    let mut idx = [0u32; MAX_PK_COLUMNS];
    for slot in &mut idx[..len] {
        *slot = r.u8()? as u32;
    }
    Ok((count, PkIndices { idx, len }))
}

/// `Err` naming the first difference unless `got` lays out the same columns as
/// the well-formed record `want`: column count, PK list, and per column its type
/// (scale included) and nullability. Names and the hidden flag are not compared.
pub fn check_same_types(got: &[u8], want: &[u8]) -> Result<(), String> {
    debug_assert!(decode(want, |_| Ok(())).is_ok(), "`want` is a well-formed record");
    if got == want {
        return Ok(());
    }
    let mut w = Reader::new(want);
    let first_difference = decode_all(got, "schema record", |g| {
        let (want_count, want_pk) = take_header(&mut w)?;
        let (count, pk) = take_header(g)?;
        if count != want_count {
            return Ok(Some(format!("expected {want_count} columns, got {count}")));
        }
        if pk != want_pk {
            return Ok(Some(format!(
                "expected PK columns {:?}, got {:?}",
                want_pk.as_slice(),
                pk.as_slice()
            )));
        }
        let null = |nullable: bool| if nullable { "NULL" } else { "NOT NULL" };
        for ci in 0..count {
            let (want_col, col) = (take_col(&mut w)?, take_col(g)?);
            if col.ty != want_col.ty || col.meta.nullable != want_col.meta.nullable {
                return Ok(Some(format!(
                    "column {ci}: expected {} {}, got {} {}",
                    want_col.ty,
                    null(want_col.meta.nullable),
                    col.ty,
                    null(col.meta.nullable),
                )));
            }
        }
        Ok(None)
    })?;
    first_difference.map_or(Ok(()), |d| Err(format!("Schema mismatch: {d}")))
}

#[cfg(test)]
#[path = "tests/schema_block.rs"]
mod tests;
