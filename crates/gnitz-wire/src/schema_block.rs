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
//!   u8             flags        NULLABLE | HIDDEN | SERIAL
//!   u8             scale
//!   u32 + bytes    name
//! ```
//!
//! The record carries no length of its own; every carrier length-prefixes it.

use crate::codec::{Reader, Writer};
use crate::{is_valid_type_code, MAX_COLUMNS, MAX_PK_COLUMNS};

const CTX: &str = "schema record";

/// One column as the record carries it. `name` borrows the record's own bytes on
/// decode; on encode it is whatever the caller has (an empty slice for the
/// anonymous records that describe physical shape only).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SchemaBlockCol<'a> {
    pub type_code: u8,
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
    /// Values are assigned from a server-side sequence (SQL `SERIAL`).
    pub serial: bool,
    /// A DECIMAL column's scale; 0 for every other type.
    pub scale: u8,
}

const NULLABLE: u8 = 1 << 0;
const HIDDEN: u8 = 1 << 1;
const SERIAL: u8 = 1 << 2;
const DEFINED_FLAGS: u8 = NULLABLE | HIDDEN | SERIAL;

impl ColMeta {
    fn flags(self) -> u8 {
        (if self.nullable { NULLABLE } else { 0 })
            | (if self.hidden { HIDDEN } else { 0 })
            | (if self.serial { SERIAL } else { 0 })
    }

    fn from_parts(flags: u8, scale: u8) -> ColMeta {
        ColMeta {
            nullable: flags & NULLABLE != 0,
            hidden: flags & HIDDEN != 0,
            serial: flags & SERIAL != 0,
            scale,
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
    let type_code = r.u8()?;
    if !is_valid_type_code(type_code) {
        return Err(format!("{CTX}: invalid type code {type_code}"));
    }
    let flags = r.u8()?;
    if flags & !DEFINED_FLAGS != 0 {
        return Err(format!("{CTX}: unknown column meta bits {flags:#x}"));
    }
    let scale = r.u8()?;
    let name = r.bytes32()?;
    Ok(SchemaBlockCol {
        type_code,
        meta: ColMeta::from_parts(flags, scale),
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
        w.u8(c.type_code).u8(c.meta.flags()).u8(c.meta.scale).bytes32(c.name);
    }
    w.into_vec()
}

/// A well-formed meta-schema record, borrowing the bytes it was decoded from.
pub struct SchemaBlock<'a> {
    count: usize,
    cols: &'a [u8],
    pk_indices: [u32; MAX_PK_COLUMNS],
    pk_count: usize,
}

/// Decode and validate `buf`.
pub fn decode(buf: &[u8]) -> Result<SchemaBlock<'_>, String> {
    let mut r = Reader::new(buf, CTX);
    let count = r.u32()? as usize;
    if count == 0 || count > MAX_COLUMNS {
        return Err(format!("{CTX}: column count {count} out of range 1..={MAX_COLUMNS}"));
    }
    let pk_count = r.u8()? as usize;
    if pk_count == 0 || pk_count > MAX_PK_COLUMNS {
        return Err(format!(
            "{CTX}: pk column count {pk_count} out of range 1..={MAX_PK_COLUMNS}"
        ));
    }
    let mut pk_indices = [0u32; MAX_PK_COLUMNS];
    for k in 0..pk_count {
        let ci = r.u8()? as u32;
        if ci as usize >= count {
            return Err(format!("{CTX}: pk index {ci} names no column"));
        }
        if pk_indices[..k].contains(&ci) {
            return Err(format!("{CTX}: pk names column {ci} twice"));
        }
        pk_indices[k] = ci;
    }

    let rest = r.remaining();
    let cols = r.take(rest)?;
    let mut cr = Reader::new(cols, CTX);
    for _ in 0..count {
        take_col(&mut cr)?;
    }
    cr.expect_consumed()?;

    Ok(SchemaBlock { count, cols, pk_indices, pk_count })
}

impl<'a> SchemaBlock<'a> {
    #[inline]
    pub fn num_columns(&self) -> usize {
        self.count
    }

    /// Every column in physical order.
    pub fn columns(&self) -> impl Iterator<Item = SchemaBlockCol<'a>> {
        let mut r = Reader::new(self.cols, CTX);
        (0..self.count).map(move |_| take_col(&mut r).expect("decode walked this section"))
    }

    /// The PK column indices, in declared PK-tuple order. Never empty.
    #[inline]
    pub fn pk_indices(&self) -> &[u32] {
        &self.pk_indices[..self.pk_count]
    }
}

#[cfg(test)]
#[path = "tests/schema_block.rs"]
mod tests;
