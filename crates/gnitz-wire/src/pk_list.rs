//! A key or index column list: its admission rules and its packed `u64` word.

use crate::codec::{Reader, Wire, Writer};
use crate::{MAX_COLUMNS, TypeCode};

/// Which rule a candidate primary key broke, worded by [`PkRule::named`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PkRule {
    /// The word carries no [`PK_LIST_PACKED_FLAG`].
    NotPacked,
    /// The word sets a bit outside its flag, its count and its columns.
    NonCanonical,
    /// No PK columns at all. Every base table has an enforced primary key.
    Empty,
    /// Arity past the cap the caller passed, which rides along so the message
    /// names the bound that was actually applied.
    TooManyColumns { count: usize, max: usize },
    /// A PK index that names no column.
    IndexOutOfRange { col: u32 },
    /// The same column listed twice — it would double-count in the stride and
    /// yield duplicates from the PK-column walk.
    Duplicate { col: u32 },
    /// STRING/BLOB (an unrelocatable heap offset) or a float (IEEE-754 breaks
    /// the byte-equal key contract the OPK encoder rests on).
    NotEligible { col: u32, type_code: TypeCode },
    /// The PK region carries no null bitmap, so a NULL has nowhere to live.
    Nullable { col: u32 },
}

/// Which list a [`PkRule`] is about: the four structural rules of a primary key
/// are equally a secondary index's column-list rules, so only the noun differs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PkListRole {
    PrimaryKey,
    ColumnList,
}

impl PkRule {
    /// This rule's message, worded for the list it is about, with every column
    /// rendered positionally.
    pub fn for_role(&self, role: PkListRole) -> String {
        self.named(role, |_| None)
    }

    /// [`Self::for_role`] with each column rendered as `'name'` where `name`
    /// answers its index, and as the index where it does not.
    pub fn named<'a>(&self, role: PkListRole, name: impl Fn(u32) -> Option<&'a str>) -> String {
        let what = match role {
            PkListRole::PrimaryKey => "primary key",
            PkListRole::ColumnList => "column list",
        };
        let col = |c: u32| match name(c) {
            Some(n) => format!("'{n}'"),
            None => c.to_string(),
        };
        match *self {
            PkRule::NotPacked => format!("{what} word carries no packed-list flag"),
            PkRule::NonCanonical => format!("{what} word carries bits outside its columns"),
            PkRule::Empty => format!("{what} must name at least one column"),
            PkRule::TooManyColumns { count, max } => {
                format!("{what} column count {count} out of range 1..={max}")
            }
            PkRule::IndexOutOfRange { col } => format!("{what} index {col} out of bounds"),
            PkRule::Duplicate { col: c } => format!("{what} names column {} twice", col(c)),
            PkRule::NotEligible { col: c, type_code } => format!(
                "{what} column {} has type_code {type_code}; only fixed-width integer, \
                 U128, UUID, and I128 columns can be key columns \
                 (String, Blob, and float columns cannot)",
                col(c)
            ),
            PkRule::Nullable { col: c } => format!("{what} column {} must not be nullable", col(c)),
        }
    }
}

/// The structural half of the primary-key admission rule: non-empty, within the
/// `max_pk` arity cap, every index naming a real column, no duplicates. Split
/// from the column-type half so a caller holding only the PK list can run it
/// alone; `ncols` is whatever bound that caller has — a real column count, or a
/// field width for a list whose columns do not exist yet.
///
/// `max_pk` is the caller's own arity cap: [`PK_LIST_MAX_COLS`] for a
/// user-declared key, which must round-trip through the persisted PK-list word,
/// and [`MAX_PK_COLUMNS`] for an engine schema derived from one.
fn validate_pk_indices(pk_cols: &[u32], ncols: usize, max_pk: usize) -> Result<(), PkRule> {
    // Also bounds the duplicate scan below, which then unrolls.
    assert!(max_pk <= MAX_PK_COLUMNS, "a PK arity cap past MAX_PK_COLUMNS");
    if pk_cols.is_empty() {
        return Err(PkRule::Empty);
    }
    if pk_cols.len() > max_pk {
        return Err(PkRule::TooManyColumns { count: pk_cols.len(), max: max_pk });
    }
    for (j, &c) in pk_cols.iter().enumerate() {
        if c as usize >= ncols {
            return Err(PkRule::IndexOutOfRange { col: c });
        }
        if pk_cols[..j].contains(&c) {
            return Err(PkRule::Duplicate { col: c });
        }
    }
    Ok(())
}

/// Both halves of the primary-key admission rule — the structural half, then
/// each column's type and nullability — for the callers that hold the columns up
/// front.
pub fn validate_pk_tuple(
    pk_cols: &[u32],
    ncols: usize,
    max_pk: usize,
    col: impl Fn(u32) -> (TypeCode, bool),
) -> Result<(), PkRule> {
    validate_pk_indices(pk_cols, ncols, max_pk)?;
    for &c in pk_cols {
        let (type_code, nullable) = col(c);
        if !type_code.is_pk_eligible() {
            return Err(PkRule::NotEligible { col: c, type_code });
        }
        if nullable {
            return Err(PkRule::Nullable { col: c });
        }
    }
    Ok(())
}

/// `key` as the column list of an index over a relation of `ncols` columns,
/// `src_pk_len` of them its PK, or the rule it breaks: an index key column is a
/// PK column of the index, so the PK rule holds with nullability waived, and the
/// index record — `key` then the source PK — keeps the PK arity limit, which is
/// within the list's.
pub fn validate_index_key(
    key: &[u32],
    ncols: usize,
    src_pk_len: usize,
    type_code: impl Fn(u32) -> TypeCode,
) -> Result<PkColList, PkRule> {
    validate_pk_tuple(key, ncols, MAX_PK_COLUMNS - src_pk_len, |c| (type_code(c), false))?;
    PkColList::checked(key, ncols)
}

/// Sizing cap for an engine schema's PK: the widest user-declared PK plus the
/// one key column a derived schema leads it with.
pub const MAX_PK_COLUMNS: usize = PK_LIST_MAX_COLS + 1;

// ---------------------------------------------------------------------------
// The packed column-list word, carried by TABLE_TAB/VIEW_TAB `pk_col_idx`,
// IDX_TAB `source_col_idx`, and the column lists of a KeyRange, a RelIndex and
// a mirror record.
//
// Layout:
//        bit 63        : PK_LIST_PACKED_FLAG
//        bits [0..4)   : decoded count (1..=PK_LIST_MAX_COLS valid)
//        bits [4+7i..) : i-th column index, 7 bits each
// Every other bit is zero: the decode refuses a word that does not re-pack to
// itself.
// ---------------------------------------------------------------------------

/// Capacity of the persisted PK-list codec ([`PkColList`]): the most columns a
/// table or view PK, or an index column list, may declare. The static guards
/// below catch a value the encoding can't hold.
///
/// Distinct from [`MAX_PK_COLUMNS`], which sizes the engine's in-memory PK
/// arrays.
pub const PK_LIST_MAX_COLS: usize = 4;

/// Width of the packed decoded-count field (bits `[0..4)`).
const PK_LIST_COUNT_BITS: u32 = 4;
/// Width of each packed column-index field (bits `[4 + 7i..)`), and so the
/// exclusive ceiling on a column index the list can carry.
const PK_LIST_COL_BITS: u32 = 7;
const PK_LIST_COL_MAX: u32 = (1 << PK_LIST_COL_BITS) - 1;

// Bumping PK_LIST_MAX_COLS past what the encoding can hold fails to build
// instead of silently corrupting the catalog word.
const _: () = assert!(PK_LIST_MAX_COLS >= 1);
const _: () = assert!(
    PK_LIST_MAX_COLS < (1 << PK_LIST_COUNT_BITS),
    "PK_LIST_MAX_COLS overflows the packed count field"
);
const _: () = assert!(
    PK_LIST_COUNT_BITS as usize + PK_LIST_COL_BITS as usize * PK_LIST_MAX_COLS <= 63,
    "PK_LIST_MAX_COLS overflows the packed u64 column region" // bit 63 is the flag
);
const _: () = assert!(
    MAX_COLUMNS <= 1 << PK_LIST_COL_BITS,
    "a schema column index no longer fits the packed 7-bit field"
);

/// Bit 63: set on every packed list, so a zero word is never one.
pub const PK_LIST_PACKED_FLAG: u64 = 1 << 63;

/// A column list — a PK's or an index's: `1..=PK_LIST_MAX_COLS` distinct columns,
/// each below `MAX_COLUMNS`, inline. [`Self::checked`] is the one way to build
/// one, so a holder never re-checks it. The tail past the list is zero, so the
/// derived `Hash` and `PartialEq` see no padding and a list can key a map.
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub struct PkColList {
    cols: [u32; PK_LIST_MAX_COLS],
    len: usize,
}

impl PkColList {
    /// `cols` as a column list of a relation with `ncols` columns: `Err` where it
    /// is empty, longer than `PK_LIST_MAX_COLS`, names a column twice, or names
    /// one at or past `ncols`. A caller holding no column count passes
    /// `MAX_COLUMNS`.
    pub fn checked(cols: &[u32], ncols: usize) -> Result<Self, PkRule> {
        validate_pk_indices(cols, ncols.min(MAX_COLUMNS), PK_LIST_MAX_COLS)?;
        let mut arr = [0u32; PK_LIST_MAX_COLS];
        arr[..cols.len()].copy_from_slice(cols);
        Ok(PkColList { cols: arr, len: cols.len() })
    }

    /// [`Self::checked`] for a list the caller already knows is one. Panics
    /// otherwise.
    pub fn from_slice(cols: &[u32]) -> Self {
        Self::checked(cols, MAX_COLUMNS).unwrap_or_else(|rule| {
            panic!(
                "PkColList::from_slice({cols:?}): {}",
                rule.for_role(PkListRole::ColumnList)
            )
        })
    }

    /// The column indices, in list order.
    pub fn as_slice(&self) -> &[u32] {
        &self.cols[..self.len]
    }

    /// The persisted `u64` form.
    pub fn pack(self) -> u64 {
        let mut w = PK_LIST_PACKED_FLAG | self.len as u64;
        for (i, &c) in self.as_slice().iter().enumerate() {
            w |= (c as u64) << (PK_LIST_COUNT_BITS + PK_LIST_COL_BITS * i as u32);
        }
        w
    }

    /// Decode the persisted `u64` form: the one word [`Self::pack`] writes for
    /// the list it names.
    fn unpack(word: u64) -> Result<Self, PkRule> {
        if word & PK_LIST_PACKED_FLAG == 0 {
            return Err(PkRule::NotPacked);
        }
        let n = (word & ((1 << PK_LIST_COUNT_BITS) - 1)) as usize;
        if n > PK_LIST_MAX_COLS {
            return Err(PkRule::TooManyColumns { count: n, max: PK_LIST_MAX_COLS });
        }
        let mut cols = [0u32; PK_LIST_MAX_COLS];
        for (i, slot) in cols[..n].iter_mut().enumerate() {
            *slot = ((word >> (PK_LIST_COUNT_BITS + PK_LIST_COL_BITS * i as u32)) & PK_LIST_COL_MAX as u64) as u32;
        }
        let list = Self::checked(&cols[..n], MAX_COLUMNS)?;
        if list.pack() != word {
            return Err(PkRule::NonCanonical);
        }
        Ok(list)
    }

    /// Decode the persisted `u64` form, its refusal rendered for `role`.
    pub fn unpack_as(word: u64, role: PkListRole) -> Result<Self, String> {
        Self::unpack(word).map_err(|rule| rule.for_role(role))
    }
}

/// Lists order by their packed word: the order a persisted set of them is kept
/// canonical in.
impl Ord for PkColList {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.pack().cmp(&other.pack())
    }
}

impl PartialOrd for PkColList {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// A column list on the wire: its packed word, a refusal worded for a
/// [`PkListRole::ColumnList`].
impl Wire for PkColList {
    fn write(&self, w: &mut Writer) {
        w.u64(self.pack());
    }

    fn read(r: &mut Reader) -> Result<Self, String> {
        Self::unpack_as(r.u64()?, PkListRole::ColumnList)
    }
}

#[cfg(test)]
#[path = "tests/pk_list.rs"]
mod tests;
