//! The schema contract the expression compiler reads through — the *schema*
//! peer of [`crate::BatchView`]'s *access* contract.

use crate::ColumnLocator;

/// Exactly the schema surface `resolve`, `validate` and their helpers read.
/// Implementors: the engine's `SchemaDescriptor` and the client-side
/// `gnitz_core::Schema`, so one compiler serves both.
///
/// **Forward** every method to the type's own fact — its existing inherent
/// method, or the column-table field where that *is* the fact — never
/// recompute. A hand-written `payload_col_idx` that disagrees with the schema's
/// own decides `no_nulls` differently and miscomputes silently; a
/// `col_type_code` that disagrees changes which client programs `check_col`
/// accepts.
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
    /// column has no payload slot" must be handled rather than poisoned with
    /// [`crate::PAYLOAD_MAPPING_PK_SENTINEL`], which stays what it is: the
    /// in-memory encoding of a `payload_mapping` table, not a value the compiler
    /// hands around.
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
    /// Inverse of [`Self::payload_slot`]: dense payload slot → column
    /// index. Caller must ensure `pi < num_payload_cols()`.
    fn payload_col_idx(&self, pi: usize) -> usize;
    /// Number of non-PK columns.
    fn num_payload_cols(&self) -> usize;
    /// Number of logical columns (PK + payload).
    fn num_columns(&self) -> usize;
    /// Column `ci`'s SQL type code. **A `u8`, not a `TypeCode`** — it feeds the
    /// compiler's `reg_tc: [u8; MAX_REGS]` tracking, is compared against
    /// `gnitz_wire::type_code::U64`, and is handed to `gnitz_wire::is_fixed_int`
    /// / `is_float` / `is_german_string` by the column-operand check.
    fn col_type_code(&self, ci: usize) -> u8;
    /// True iff column `ci` admits NULL.
    fn col_nullable(&self, ci: usize) -> bool;
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
/// A normal `pub fn`, not a `#[cfg(test)]` helper, so every crate that adds an
/// implementor calls it from its own test tree — the same shape as
/// [`crate::assert_batchview_consistent`], and for the same reason: the engine's
/// `SchemaDescriptor` and the client's `Schema` live in crates that cannot see
/// each other's tests.
///
/// Takes `&dyn` for the reason it exists: most methods collide by name with an
/// inherent method on a typical implementor, and Rust prefers the inherent one
/// in receiver-dot position — a check written against the concrete type would
/// never enter the impl, so the exact failure mode this catches (a forwarder
/// that reimplements rather than forwards) would pass. Behind `&dyn` only the
/// trait method is nameable.
pub fn assert_schema_facts_consistent(s: &dyn SchemaFacts, cols: &[(u8, bool)], pk: &[usize]) {
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
                // The round trip is the real check: `payload_col_idx` is the one
                // independently-implemented direction, and it is what decides
                // `no_nulls`. An off-by-one there reads a neighbouring column's
                // nullability and silently drops null handling.
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
