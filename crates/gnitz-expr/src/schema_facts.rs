//! The schema contract the expression compiler reads through — the *schema*
//! peer of [`crate::BatchView`]'s *access* contract.

use crate::ColumnLocator;

/// Exactly the schema surface `resolve`, `validate` and their helpers read.
/// Implementors: the engine's `SchemaDescriptor` and the client-side
/// `gnitz_core::Schema`, so one compiler serves both.
///
/// **Forward** every method to the type's own fact — its existing inherent
/// method, or the column-table field where that *is* the fact — never
/// recompute: a `col_type_code` that disagrees with the schema's own changes
/// which client programs `check_col` accepts.
///
/// Reached through `&dyn SchemaFacts`: the whole trait runs once per program
/// compile, never per row, so static dispatch buys nothing and the `Option<&_>`
/// schema arguments stay spellable without a placeholder implementor.
pub trait SchemaFacts {
    /// Where column `ci`'s value physically lives (PK region vs payload slot).
    fn locate(&self, ci: usize) -> ColumnLocator;
    /// Dense payload slot of `ci`, or `None` for a PK column. Derived from
    /// [`Self::locate`], never separately implemented — the locator variant *is*
    /// the PK marker, so the two cannot disagree — and `Option`-shaped so "this
    /// column has no payload slot" must be handled at each site rather than
    /// carried around as [`crate::PAYLOAD_MAPPING_PK_SENTINEL`], which an
    /// implementor would then have to synthesise.
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
    /// Inverse of [`Self::payload_slot`]: dense payload slot → column index.
    /// Caller must ensure `pi < num_payload_cols()`.
    ///
    /// Derived from [`Self::locate`] like the two above, so the two directions
    /// cannot disagree — and this is the direction that names the *output*
    /// column a COPY_COL or EMIT is type-checked against, so a hand-written one
    /// that is off by one approves the write against a neighbouring column's
    /// type. Override only to answer it in O(1) from a precomputed table.
    fn payload_col_idx(&self, pi: usize) -> usize {
        (0..self.num_columns())
            .filter(|&ci| !self.is_pk_col(ci))
            .nth(pi)
            .expect("payload_col_idx: pi out of range")
    }
    /// Number of non-PK columns.
    fn num_payload_cols(&self) -> usize;
    /// Number of logical columns (PK + payload).
    fn num_columns(&self) -> usize;
    /// Column `ci`'s SQL type code. **A `u8`, not a `TypeCode`** — it feeds the
    /// compiler's `reg_u64: [bool; MAX_REGS]` tracking, is compared against
    /// `gnitz_wire::type_code::U64`, and is handed to `gnitz_wire::is_fixed_int`
    /// / `is_float` / `is_german_string` by the column-operand check.
    fn col_type_code(&self, ci: usize) -> u8;
    /// True iff column `ci` admits NULL.
    fn col_nullable(&self, ci: usize) -> bool;
}

/// One [`SCHEMA_FACTS_CASES`] entry: a schema's column table as
/// `(type_code, nullable)`, and its PK list in PK-LIST order.
type SchemaFactsCase = (&'static [(u8, bool)], &'static [usize]);

/// The shape matrix every implementor is driven against. It lives beside the
/// harness so each crate tests its own implementor against the same absolute
/// expectations, instead of one crate reaching into another's types.
///
/// Every case is admissible on the *client* side too (`Schema::validate_parts`):
/// each PK column is non-nullable and PK-eligible, the widest schema is 6
/// columns against `MAX_COLUMNS`, and the widest PK list is 4 —
/// `PK_LIST_MAX_COLS` exactly.
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
        // payload column (a PK column is never wide).
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
///
/// The whole harness's public surface, and a normal `pub fn` rather than a
/// `#[cfg(test)]` helper, so every crate that adds an implementor calls it from
/// its own test tree — the same shape as [`crate::assert_batchview_consistent`],
/// and for the same reason: the engine's `SchemaDescriptor` and the client's
/// `Schema` live in crates that cannot see each other's tests. The case table
/// and the per-schema assertions behind it stay private, so a caller cannot
/// drive a subset of the matrix and believe it checked the whole thing.
pub fn assert_schema_facts_matrix<S: SchemaFacts>(build: impl Fn(&[(u8, bool)], &[usize]) -> S) {
    for &(cols, pk) in SCHEMA_FACTS_CASES {
        assert_schema_facts_consistent(&build(cols, pk), cols, pk);
    }
}

/// Assert that `s` reports `cols` column-for-column and that its
/// [`SchemaFacts`] methods agree with each other. Every implementor must pass.
///
/// `cols` is the schema's column table as `(type_code, nullable)` — the same
/// table the schema itself is built from — and `pk` is its **PK list**, in
/// PK-list order, which is where each PK column's OPK byte offset comes from.
/// PK-list order is independent of column order (`PRIMARY KEY (b, a)` yields
/// `[1, 0]`), so it is passed rather than inferred: a harness that assumed the
/// two coincide could never see the offset mismatch it exists to catch.
///
/// The expected OPK offset is pinned absolutely rather than left derived,
/// because a wrong offset reads a neighbouring PK column's bytes and still
/// produces a value.
///
/// It takes `&dyn` only to match the trait's own dispatch (see the trait doc); a
/// `<S: SchemaFacts>` generic would resolve to the trait methods just as well,
/// since a type parameter has no inherent impls to shadow them — which is why
/// [`assert_schema_facts_matrix`], which has to *build* the implementor, is
/// generic.
pub(crate) fn assert_schema_facts_consistent(s: &dyn SchemaFacts, cols: &[(u8, bool)], pk: &[usize]) {
    assert_eq!(s.num_columns(), cols.len(), "num_columns()");
    assert_eq!(s.num_payload_cols(), cols.len() - pk.len(), "num_payload_cols()");

    // Expected OPK byte offset per PK column: the running sum of the preceding
    // PK columns' widths, walked in PK-LIST order (not column order).
    let mut pk_off = vec![0usize; cols.len()];
    let mut running = 0usize;
    for &ci in pk {
        pk_off[ci] = running;
        running += gnitz_wire::wire_stride(cols[ci].0);
    }

    for (ci, &(want_tc, want_nullable)) in cols.iter().enumerate() {
        let want_pk = pk.contains(&ci);
        // The exact type code, not merely a matching width: `wire_stride`
        // collapses 15 codes onto 5 widths, so a forwarder handing back U64 for
        // F64 would satisfy every width-shaped check below while breaking the
        // compiler's per-register type tracking and the `is_fixed_int` /
        // `is_float` / `is_german_string` admissibility dispatch it feeds.
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
                // The round trip is what an impl that *overrides*
                // `payload_col_idx` off a precomputed table is checked by; the
                // provided body derives it from `locate` and is tautological
                // here, which is the point.
                let pi = slot as usize;
                assert!(pi < cols.len() - pk.len(), "locate({ci}) slot {pi} out of range");
                assert_eq!(s.payload_col_idx(pi), ci, "payload_col_idx(locate({ci}).slot)");
            }
            ColumnLocator::Pk { byte_off, .. } => {
                assert_eq!(byte_off as usize, pk_off[ci], "locate({ci}) OPK byte offset");
            }
        }
    }
}

#[cfg(test)]
mod tests;
