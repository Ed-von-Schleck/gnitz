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

/// The record for `cols` keyed by `pk_cols`, in declared PK-tuple order.
pub fn encode(cols: &[SchemaBlockCol], pk_cols: &[u32]) -> Vec<u8> {
    let columns: usize = cols.iter().map(|c| COL_FIXED_BYTES + c.name.len()).sum();
    let mut w = Writer::with_capacity(4 + 1 + pk_cols.len() + columns);
    w.u32(cols.len() as u32)
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
        for _ in 0..count {
            on_col(take_col(r)?)?;
        }
        Ok(PkIndices { idx, len })
    })
}

#[cfg(test)]
#[path = "tests/schema_block.rs"]
mod tests;
