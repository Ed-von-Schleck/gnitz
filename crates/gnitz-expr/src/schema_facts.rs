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
/// implementor derives these the same way. `SchemaDescriptor` answers
/// `pk_stride`, `num_payload_cols` and `payload_col_idx` from values it computed
/// at construction.
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
        match self.try_locate(ci)? {
            ColumnLocator::Payload { slot, .. } => Some(slot as usize),
            ColumnLocator::Pk { .. } => None,
        }
    }
    /// Inverse of [`Self::payload_slot`]. Panics unless `pi < num_payload_cols()`.
    fn payload_col_idx(&self, pi: usize) -> usize {
        assert!(pi < self.num_payload_cols(), "payload_col_idx: pi {pi} out of range");
        gnitz_wire::payload_col_idx(self.pk_cols(), pi)
    }
    /// Every payload column's locator, in slot order.
    fn payload_locators(&self) -> Vec<ColumnLocator> {
        (0..self.num_payload_cols())
            .map(|pi| self.locate(self.payload_col_idx(pi)))
            .collect()
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
    /// The digest a read request names its reply layout by: the PK list and every
    /// column's type code. Nullability is not part of it.
    fn layout_digest(&self) -> u64 {
        gnitz_wire::layout_digest(self.pk_cols(), (0..self.num_columns()).map(|c| self.col_type_code(c)))
    }
    /// Same PK list and per-column type codes — what [`Self::layout_digest`]
    /// digests. Nullability is not compared.
    fn same_layout(&self, other: &dyn SchemaFacts) -> bool {
        self.pk_cols() == other.pk_cols()
            && self.num_columns() == other.num_columns()
            && (0..self.num_columns()).all(|c| self.col_type_code(c) == other.col_type_code(c))
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
        (0..self.num_columns())
            .filter(|&ci| self.col_nullable(ci))
            .filter_map(|ci| self.payload_slot(ci))
            .fold(0u64, |m, pi| m | 1u64 << pi)
    }

    /// Bit `pi` set iff payload slot `pi`'s column is a German string (STRING
    /// or BLOB).
    fn string_payload_slots(&self) -> u64 {
        (0..self.num_columns())
            .filter(|&ci| self.col_type_code(ci).is_german_string())
            .filter_map(|ci| self.payload_slot(ci))
            .fold(0u64, |m, pi| m | 1u64 << pi)
    }

    /// The null bits a conforming batch never sets: every payload slot's but
    /// the nullable ones'.
    fn not_null_payload_slots(&self) -> u64 {
        gnitz_wire::low_bits_mask(self.num_payload_cols()) & !self.nullable_payload_slots()
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

#[cfg(test)]
#[path = "tests/schema_facts.rs"]
mod tests;
