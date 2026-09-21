//! The schema contract the expression compiler reads through — the *schema*
//! peer of [`crate::BatchView`]'s *access* contract.

use crate::ColumnLocator;

/// A schema's column table and PK list, and everything derived from them. Every
/// implementor runs [`assert_schema_facts_matrix`] in its own tests.
pub trait SchemaFacts {
    /// The PK columns in PK-list order, which is the order they pack into the
    /// OPK region.
    fn pk_cols(&self) -> &[u32];
    /// Where column `ci`'s value physically lives. Panics on an out-of-range `ci`.
    fn locate(&self, ci: usize) -> ColumnLocator {
        assert!(
            ci < self.num_columns(),
            "locate: col_idx {ci} out of bounds (num_columns = {})",
            self.num_columns()
        );
        let type_code = self.col_type_code(ci);
        let size = gnitz_wire::wire_stride(type_code) as u8;
        let (mut byte_off, mut pk_below) = (0usize, 0usize);
        for &p in self.pk_cols() {
            let p = p as usize;
            if p == ci {
                return ColumnLocator::Pk {
                    byte_off: byte_off as u8,
                    size,
                    type_code,
                };
            }
            byte_off += gnitz_wire::wire_stride(self.col_type_code(p));
            pk_below += usize::from(p < ci);
        }
        ColumnLocator::Payload {
            slot: (ci - pk_below) as u8,
            size,
            type_code,
        }
    }
    /// Dense payload slot of `ci`, or `None` for a PK column.
    fn payload_slot(&self, ci: usize) -> Option<u8> {
        match self.locate(ci) {
            ColumnLocator::Payload { slot, .. } => Some(slot),
            ColumnLocator::Pk { .. } => None,
        }
    }
    /// True iff column `ci` is a PK column.
    fn is_pk_col(&self, ci: usize) -> bool {
        self.payload_slot(ci).is_none()
    }
    /// Inverse of [`Self::payload_slot`], for `pi < num_payload_cols()`.
    fn payload_col_idx(&self, pi: usize) -> usize {
        (0..self.num_columns())
            .filter(|&ci| !self.is_pk_col(ci))
            .nth(pi)
            .expect("payload_col_idx: pi out of range")
    }
    /// Number of non-PK columns.
    fn num_payload_cols(&self) -> usize {
        self.num_columns() - self.pk_cols().len()
    }
    /// Number of logical columns (PK + payload).
    fn num_columns(&self) -> usize;
    /// Column `ci`'s wire type code.
    fn col_type_code(&self, ci: usize) -> u8;
    /// True iff column `ci` admits NULL.
    fn col_nullable(&self, ci: usize) -> bool;

    /// Bit `pi` set iff payload slot `pi`'s column admits NULL.
    fn nullable_payload_slots(&self) -> u64 {
        (0..self.num_columns())
            .filter(|&ci| self.col_nullable(ci))
            .filter_map(|ci| self.payload_slot(ci))
            .fold(0u64, |m, pi| m | 1u64 << pi)
    }

    /// The null bits a conforming batch never sets: every payload slot's but
    /// the nullable ones'.
    fn not_null_payload_slots(&self) -> u64 {
        gnitz_wire::all_payload_null_mask(self.num_payload_cols()) & !self.nullable_payload_slots()
    }
}

/// One [`SCHEMA_FACTS_CASES`] entry: a schema's column table as
/// `(type_code, nullable)`, and its PK list in PK-LIST order.
type SchemaFactsCase = (&'static [(u8, bool)], &'static [usize]);

/// The shape matrix every implementor is driven against.
const SCHEMA_FACTS_CASES: &[SchemaFactsCase] = {
    use gnitz_wire::type_code as tc;
    &[
        // Single unsigned PK at column 0; U64/F64/STRING/U128 payload, one nullable.
        (
            &[
                (tc::U64, false),
                (tc::U64, false),
                (tc::F64, true),
                (tc::STRING, false),
                (tc::U128, false),
            ],
            &[0],
        ),
        // Single signed PK NOT at column 0 — the payload slots renumber around
        // it, so the `ci - 1` closed form does not hold.
        (
            &[(tc::STRING, true), (tc::I64, false), (tc::U64, false), (tc::F32, true)],
            &[1],
        ),
        // Compound two-column PK (signed + unsigned, mixed widths).
        (
            &[(tc::I32, false), (tc::U16, false), (tc::F64, false), (tc::BLOB, true)],
            &[0, 1],
        ),
        // Compound PK whose PK-LIST order REVERSES its column order
        // (`PRIMARY KEY (b, a)`), skipping a column in between: the OPK offsets
        // follow the pk list, so column 3 sits at offset 0 and column 0 at 8.
        (
            &[(tc::U32, false), (tc::STRING, true), (tc::F64, false), (tc::I64, false)],
            &[3, 0],
        ),
        // Every fixed width as a PK column: 1/2/4/8 signed, plus a 16-byte
        // payload column.
        (
            &[
                (tc::I8, false),
                (tc::U16, false),
                (tc::I32, false),
                (tc::U64, false),
                (tc::I128, true),
                (tc::UUID, false),
            ],
            &[0, 1, 2, 3],
        ),
        // Every fixed width as a payload column, all nullable.
        (
            &[
                (tc::U64, false),
                (tc::U8, true),
                (tc::I16, true),
                (tc::U32, true),
                (tc::I64, true),
                (tc::U128, true),
            ],
            &[0],
        ),
        // PK-only: no payload columns at all.
        (&[(tc::U32, false)], &[0]),
    ]
};

/// Drive [`assert_schema_facts_consistent`] over every [`SCHEMA_FACTS_CASES`]
/// shape, building the implementor from each case's column table and PK list.
pub fn assert_schema_facts_matrix<S: SchemaFacts>(build: impl Fn(&[(u8, bool)], &[usize]) -> S) {
    for &(cols, pk) in SCHEMA_FACTS_CASES {
        assert_schema_facts_consistent(&build(cols, pk), cols, pk);
    }
}

/// Assert that `s` is the schema of column table `cols` and PK list `pk`, and
/// that its [`SchemaFacts`] answers agree with each other.
pub(crate) fn assert_schema_facts_consistent(s: &dyn SchemaFacts, cols: &[(u8, bool)], pk: &[usize]) {
    assert_eq!(s.num_columns(), cols.len(), "num_columns()");
    assert_eq!(s.num_payload_cols(), cols.len() - pk.len(), "num_payload_cols()");

    // Expected OPK byte offset per PK column, walked in PK-list order.
    let mut pk_off = vec![0usize; cols.len()];
    let mut running = 0usize;
    for &ci in pk {
        pk_off[ci] = running;
        running += gnitz_wire::wire_stride(cols[ci].0);
    }

    // Expected payload slots: non-PK columns numbered left to right.
    let mut want_nullable_mask = 0u64;
    let mut want_not_null_mask = 0u64;
    let mut next_pi = 0usize;

    for (ci, &(want_tc, want_nullable)) in cols.iter().enumerate() {
        let want_pk = pk.contains(&ci);
        let want_slot = next_pi;
        if !want_pk {
            let bit = 1u64 << next_pi;
            *if want_nullable {
                &mut want_nullable_mask
            } else {
                &mut want_not_null_mask
            } |= bit;
            next_pi += 1;
        }
        assert_eq!(s.col_type_code(ci), want_tc, "col_type_code({ci})");
        assert_eq!(s.col_nullable(ci), want_nullable, "col_nullable({ci})");
        assert_eq!(s.is_pk_col(ci), want_pk, "is_pk_col({ci})");

        let loc = s.locate(ci);
        assert_eq!(
            matches!(loc, ColumnLocator::Pk { .. }),
            want_pk,
            "locate({ci}) variant disagrees with is_pk_col({ci})",
        );
        assert_eq!(loc.type_code(), want_tc, "locate({ci}).type_code()");
        assert_eq!(loc.size(), gnitz_wire::wire_stride(want_tc), "locate({ci}).size()");
        match loc {
            ColumnLocator::Payload { slot, .. } => {
                assert_eq!(slot as usize, want_slot, "locate({ci}) payload slot");
                assert_eq!(s.payload_col_idx(want_slot), ci, "payload_col_idx({want_slot})");
            }
            ColumnLocator::Pk { byte_off, .. } => {
                assert_eq!(byte_off as usize, pk_off[ci], "locate({ci}) OPK byte offset");
            }
        }
    }

    assert_eq!(
        s.nullable_payload_slots(),
        want_nullable_mask,
        "nullable_payload_slots()"
    );
    assert_eq!(
        s.not_null_payload_slots(),
        want_not_null_mask,
        "not_null_payload_slots()"
    );
}

#[cfg(test)]
#[path = "tests/schema_facts.rs"]
mod tests;
