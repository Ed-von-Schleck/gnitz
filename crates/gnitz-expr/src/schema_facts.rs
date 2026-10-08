//! The schema contract the expression compiler reads through — the *schema*
//! peer of [`crate::BatchView`]'s *access* contract.

use crate::ColumnLocator;

/// A schema's column table and PK list — the only thing an implementor writes.
pub trait ColumnTable {
    /// The PK columns in PK-list order, which is the order they pack into the
    /// OPK region.
    fn pk_cols(&self) -> &[u32];
    /// Number of logical columns (PK + payload).
    fn num_columns(&self) -> usize;
    /// Column `ci`'s type code.
    fn col_type_code(&self, ci: usize) -> gnitz_wire::TypeCode;
    /// True iff column `ci` admits NULL.
    fn col_nullable(&self, ci: usize) -> bool;
}

/// Everything derived from a [`ColumnTable`]. Blanket-implemented, so every
/// implementor derives these the same way.
pub trait SchemaFacts: ColumnTable {
    /// Where column `ci`'s value physically lives, or `None` when `ci` is out of
    /// range.
    fn try_locate(&self, ci: usize) -> Option<ColumnLocator> {
        if ci >= self.num_columns() {
            return None;
        }
        let type_code = self.col_type_code(ci);
        let size = type_code.wire_stride() as u8;
        Some(match gnitz_wire::payload_slot(self.pk_cols(), ci) {
            Some(slot) => ColumnLocator::Payload { slot: slot as u8, size, type_code },
            None => ColumnLocator::Pk {
                byte_off: self
                    .pk_cols()
                    .iter()
                    .take_while(|&&p| p as usize != ci)
                    .map(|&p| self.col_type_code(p as usize).wire_stride())
                    .sum::<usize>() as u8,
                size,
                type_code,
            },
        })
    }
    /// [`Self::try_locate`] for a column that must exist. Panics on an
    /// out-of-range `ci`.
    fn locate(&self, ci: usize) -> ColumnLocator {
        self.try_locate(ci).unwrap_or_else(|| {
            panic!(
                "locate: col_idx {ci} out of bounds (num_columns = {})",
                self.num_columns()
            )
        })
    }
    /// True iff column `ci` is a PK column; false for an out-of-range `ci`.
    fn is_pk_col(&self, ci: usize) -> bool {
        self.pk_cols().iter().any(|&p| p as usize == ci)
    }
    /// Dense payload slot of `ci`, or `None` for a PK column or an out-of-range
    /// `ci`.
    fn payload_slot(&self, ci: usize) -> Option<usize> {
        self.try_locate(ci)?.payload_slot()
    }
    /// Inverse of [`Self::payload_slot`]. Panics unless `pi < num_payload_cols()`.
    fn payload_col_idx(&self, pi: usize) -> usize {
        assert!(pi < self.num_payload_cols(), "payload_col_idx: pi {pi} out of range");
        gnitz_wire::payload_col_idx(self.pk_cols(), pi)
    }
    /// Every payload column's locator, in slot order.
    fn payload_locators(&self) -> Vec<ColumnLocator> {
        payload_cols(self).map(|ci| self.locate(ci)).collect()
    }
    /// Number of non-PK columns.
    fn num_payload_cols(&self) -> usize {
        self.num_columns() - self.pk_cols().len()
    }
    /// The PK column when the PK is exactly one column.
    fn lone_pk_col(&self) -> Option<usize> {
        match self.pk_cols() {
            [c] => Some(*c as usize),
            _ => None,
        }
    }
    /// The digest a read request names its reply layout by: its regions' types.
    /// Column numbering and nullability are not part of it.
    fn layout_digest(&self) -> u64 {
        gnitz_wire::layout_digest(self.pk_cols().len(), region_types(self))
    }
    /// Same PK list and per-column type codes: the same regions under the same
    /// column numbering. Nullability is not compared.
    fn same_layout(&self, other: &dyn SchemaFacts) -> bool {
        self.pk_cols() == other.pk_cols()
            && self.num_columns() == other.num_columns()
            && (0..self.num_columns()).all(|c| self.col_type_code(c) == other.col_type_code(c))
    }
    /// Whether a batch of `self` and one of `other` have the same regions — what
    /// [`Self::layout_digest`] digests. Column numbering and nullability are
    /// labels over those bytes.
    fn same_region_types(&self, other: &dyn SchemaFacts) -> bool {
        self.pk_cols().len() == other.pk_cols().len() && region_types(self).eq(region_types(other))
    }
    /// Total encoded PK width — the PK region's per-row stride.
    fn pk_stride(&self) -> usize {
        self.pk_cols()
            .iter()
            .map(|&p| self.col_type_code(p as usize).wire_stride())
            .sum()
    }

    /// Bit `pi` set iff payload slot `pi`'s column admits NULL.
    fn nullable_payload_slots(&self) -> u64 {
        payload_cols(self)
            .enumerate()
            .filter(|&(_, ci)| self.col_nullable(ci))
            .fold(0u64, |m, (pi, _)| m | 1u64 << pi)
    }

    /// Bit `pi` set iff payload slot `pi`'s column is a German string (STRING
    /// or BLOB).
    fn string_payload_slots(&self) -> u64 {
        payload_cols(self)
            .enumerate()
            .filter(|&(_, ci)| self.col_type_code(ci).is_german_string())
            .fold(0u64, |m, (pi, _)| m | 1u64 << pi)
    }

    /// The null bits a conforming batch never sets: every bit but the nullable
    /// slots', those past the last payload slot included.
    fn not_null_payload_slots(&self) -> u64 {
        !self.nullable_payload_slots()
    }

    /// The output-key kind a reduce grouped by `group` over this schema warrants.
    fn reduce_out_key(&self, group: &[u32]) -> gnitz_wire::ReduceOutKey {
        gnitz_wire::ReduceOutKey::for_group_cols(self.pk_cols(), group, |c| {
            (self.col_type_code(c as usize), self.col_nullable(c as usize))
        })
    }

    /// One row's PK as OPK bytes, from one native value per PK column in PK-list
    /// order. A signed value is passed sign-extended (`v as u128`).
    fn opk_key_cols(&self, natives: &[u128]) -> gnitz_wire::PkBuf {
        debug_assert_eq!(
            natives.len(),
            self.pk_cols().len(),
            "opk_key_cols: one native value per PK column",
        );
        let mut key = gnitz_wire::PkBuf::zeroed(0);
        for (&p, &v) in self.pk_cols().iter().zip(natives) {
            let tc = self.col_type_code(p as usize);
            debug_assert!(
                {
                    let w = tc.wire_stride();
                    let dropped = v.checked_shr(w as u32 * 8).unwrap_or(0);
                    dropped == 0 || dropped == u128::MAX >> (w * 8)
                },
                "opk_key_cols: {v:#x} does not fit a {tc:?} column",
            );
            key.push(tc.wire_stride(), v, tc.is_signed_int());
        }
        key
    }
}

impl<T: ColumnTable + ?Sized> SchemaFacts for T {}

/// The types of a batch's regions under `s`: the PK columns in PK-list order,
/// then the payload columns in payload order.
fn region_types<S: SchemaFacts + ?Sized>(s: &S) -> impl Iterator<Item = gnitz_wire::TypeCode> + '_ {
    let pk = s.pk_cols().iter().map(|&p| s.col_type_code(p as usize));
    pk.chain(payload_cols(s).map(|c| s.col_type_code(c)))
}

/// The payload columns' indices in slot order: the `pi`-th one is slot `pi`.
pub fn payload_cols<S: ColumnTable + ?Sized>(s: &S) -> impl Iterator<Item = usize> + '_ {
    let mut pk = [0u64; 2];
    for &c in s.pk_cols() {
        if let Some(w) = pk.get_mut(c as usize >> 6) {
            *w |= 1 << (c & 63);
        }
    }
    (0..s.num_columns()).filter(move |&ci| pk.get(ci >> 6).is_none_or(|w| w >> (ci & 63) & 1 == 0))
}

#[cfg(test)]
#[path = "tests/schema_facts.rs"]
mod tests;
