//! The **meta-schema record**: the one encoding that carries a relation's column
//! shape across the wire. Engine → client, a RESOLVE's descriptor carries the
//! named record. Client → engine, the blob of a `PUSH` and of each `PUSH_TXN`
//! family carries the nameless **shape record**: every name empty, every hidden
//! flag clear ([`crate::ColumnDef::shape_col`]).
//!
//! ```text
//! u16              pk_count
//! u8 × pk_count    PK column indices, in declared PK-tuple order
//! u16              column_count
//! per column:
//!   u8             type_code
//!   u8             flags        NULLABLE | HIDDEN
//!   u8             scale
//!   u32 + bytes    name
//! ```
//!
//! Both lists are the codec's counted lists ([`Writer::count`]). The record
//! carries no length of its own; every carrier length-prefixes it.

use smallvec::SmallVec;

use crate::codec::{Reader, Writer, decode_all};
use crate::{ColType, MAX_COLUMNS, MAX_PK_COLUMNS};

/// One column as the record carries it. `name` borrows the record's own bytes on
/// decode, which refuses a name that is not UTF-8; on encode it is whatever the
/// caller has (an empty string for the shape records that describe physical
/// shape only).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordCol<'a> {
    pub ty: ColType,
    pub nullable: bool,
    /// A physical column no name the user writes reaches — a hidden key slot or
    /// a dropped column. Name resolution, wildcard expansion and client rows
    /// skip it; to the PK region, routing, sort and consolidation it is an
    /// ordinary column.
    pub hidden: bool,
    pub name: &'a str,
}

const NULLABLE: u8 = 1 << 0;
const HIDDEN: u8 = 1 << 1;
const DEFINED_FLAGS: u8 = NULLABLE | HIDDEN;

/// Bytes one column occupies before its name: type code, flags, scale, and the
/// name's own length prefix.
const COL_FIXED_BYTES: usize = 3 + 4;

/// Written for a PK column index the record's `u8` cannot hold.
const PK_INDEX_OVERFLOW: u8 = u8::MAX;
const _: () = assert!(
    PK_INDEX_OVERFLOW as usize >= MAX_COLUMNS,
    "a PK index this wide must be refused"
);

/// One column, read forward.
fn take_col<'a>(r: &mut Reader<'a>) -> Result<RecordCol<'a>, String> {
    let code = r.u8()?;
    let flags = r.flags(DEFINED_FLAGS)?;
    let scale = r.u8()?;
    let ty = ColType::from_wire(code, scale).ok_or_else(|| format!("invalid column type {code}/{scale}"))?;
    let name = r.str32()?;
    Ok(RecordCol {
        ty,
        nullable: flags & NULLABLE != 0,
        hidden: flags & HIDDEN != 0,
        name,
    })
}

/// The record for `cols` keyed by `pk_cols`, in declared PK-tuple order. `cols`
/// is walked twice: a clone sizes the record, the original writes it.
pub fn encode<'a>(cols: impl Iterator<Item = RecordCol<'a>> + Clone, pk_cols: &[u32]) -> Vec<u8> {
    let (count, columns) = cols.clone().fold((0usize, 0usize), |(n, b), c| {
        (n + 1, b + COL_FIXED_BYTES + c.name.len())
    });
    let mut w = Writer::with_capacity(2 + pk_cols.len() + 2 + columns);
    w.count(pk_cols.len());
    for &c in pk_cols {
        debug_assert!(u8::try_from(c).is_ok(), "PK column index {c} does not fit the record");
        w.u8(u8::try_from(c).unwrap_or(PK_INDEX_OVERFLOW));
    }
    w.count(count);
    for c in cols {
        w.u8(c.ty.tc.as_wire())
            .u8((if c.nullable { NULLABLE } else { 0 }) | (if c.hidden { HIDDEN } else { 0 }))
            .u8(c.ty.scale)
            .bytes32(c.name.as_bytes());
    }
    w.into_vec()
}

/// A record's PK column indices, in declared PK-tuple order, not yet checked
/// against the columns.
type PkIndices = SmallVec<[u32; MAX_PK_COLUMNS]>;

/// Decode and validate `buf`, handing each column to `on_col` in physical order
/// once the column count is known to lie within [`MAX_COLUMNS`]; returns the PK
/// column indices.
pub fn decode<'a>(
    buf: &'a [u8],
    mut on_col: impl FnMut(RecordCol<'a>) -> Result<(), String>,
) -> Result<PkIndices, String> {
    decode_all(buf, "schema record", |r| {
        let mut pk = PkIndices::new();
        r.items("pk columns", MAX_PK_COLUMNS, |r| {
            pk.push(r.u8()?.into());
            Ok(())
        })?;
        if r.items("columns", MAX_COLUMNS, |r| on_col(take_col(r)?))?.is_empty() {
            return Err("a record holds at least one column".into());
        }
        Ok(pk)
    })
}

/// `Err` naming the first difference unless `got` is the well-formed shape
/// record `want` byte for byte. A shape record is canonical, so the decode only
/// words a mismatch: column count, PK list, a column's type (scale included) or
/// nullability, or else a name or hidden flag no shape record carries.
pub fn check_shape(got: &[u8], want: &[u8]) -> Result<(), String> {
    debug_assert!(decode(want, |_| Ok(())).is_ok(), "`want` is a well-formed record");
    if got == want {
        return Ok(());
    }
    fn layout(record: &[u8]) -> Result<(Vec<(ColType, bool)>, PkIndices), String> {
        let mut cols = Vec::new();
        let pk = decode(record, |c| {
            cols.push((c.ty, c.nullable));
            Ok(())
        })?;
        Ok((cols, pk))
    }
    let (want_cols, want_pk) = layout(want)?;
    let (cols, pk) = layout(got)?;
    let null = |nullable: bool| if nullable { "NULL" } else { "NOT NULL" };
    let difference = if cols.len() != want_cols.len() {
        format!("expected {} columns, got {}", want_cols.len(), cols.len())
    } else if pk != want_pk {
        format!("expected PK columns {:?}, got {:?}", want_pk.as_slice(), pk.as_slice())
    } else if let Some((ci, ((wt, wn), (t, n)))) = want_cols.iter().zip(&cols).enumerate().find(|(_, (w, g))| w != g) {
        format!("column {ci}: expected {wt} {}, got {t} {}", null(*wn), null(*n))
    } else {
        "a shape record carries no column name and no hidden flag".to_string()
    };
    Err(format!("Schema mismatch: {difference}"))
}

#[cfg(test)]
#[path = "tests/schema_record.rs"]
mod tests;
