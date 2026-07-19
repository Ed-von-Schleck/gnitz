//! The backfill-scan index bound a `ScanDelta` carries — the **AST view-bridge**
//! into the [`crate::access`] recognizer leaf.
//!
//! A **physical access hint**, never a semantic filter: the caller emits the FULL
//! `Filter` downstream regardless, so a bound only narrows what the initial
//! full-source scan reads. That is why a bound may be dropped anywhere (no index,
//! a non-catalog id, a dropped index at run time) with no effect on results.
//!
//! This module binds the raw view WHERE and hands the bound conjuncts to
//! `access::best_index_bound`. It imports **down** from the `access` leaf and is
//! the last AST island of the access-path surface.

use crate::access::best_index_bound;
use crate::bind::bind_single_table;
use crate::error::GnitzSqlError;
use gnitz_core::{GnitzClient, Schema};
use gnitz_wire::ScanBound;
use std::rc::Rc;
use std::sync::Arc;

/// The index bound for an operator input, or `None` when the input cannot carry
/// one.
///
/// `src_is_catalog` is the catalog-provenance gate, computed where the input was
/// resolved: a chain-minted CTE/derived-table id aliases real relation ids, so an
/// ungated extractor would match a *foreign* table's index column indices against
/// this segment's schema by numeric coincidence. This is the only entry point;
/// every caller reaches a bound through it.
pub(crate) fn scan_bound_for_input(
    client: &mut GnitzClient,
    selection: Option<&sqlparser::ast::Expr>,
    src: (u64, &Rc<Schema>),
    src_is_catalog: bool,
) -> Result<Option<ScanBound>, GnitzSqlError> {
    match selection {
        Some(e) if src_is_catalog => scan_bound_from(e, src.1, || client.table_indexes(src.0)),
        _ => Ok(None),
    }
}

/// Bind the raw WHERE against the source schema, then take the best index bound its
/// bound conjuncts admit (or `None`). `fetch` (the GET_INDICES probe) is injected —
/// which index to pick is a pure decision over `(bound WHERE, schema, index list)`,
/// so it tests without a live catalog — and is called at most once.
///
/// The Filter emitter downstream binds this same WHERE again to compile the actual
/// predicate; bind is pure, so the double-bind has no observable effect.
fn scan_bound_from<F>(
    where_expr: &sqlparser::ast::Expr,
    schema: &Schema,
    fetch: F,
) -> Result<Option<ScanBound>, GnitzSqlError>
where
    F: FnMut() -> Result<Arc<Vec<gnitz_core::IndexMeta>>, gnitz_core::ClientError>,
{
    let bound = bind_single_table(where_expr, schema)?;
    Ok(best_index_bound(&bound, schema, fetch)?.map(|c| ScanBound {
        idx_cols: c.idx_cols,
        desc: c.desc,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{col_def, idx_metas, parse_expr_sql};
    use gnitz_core::TypeCode;
    use gnitz_wire::Cut;
    use std::cell::Cell;

    /// `(id U64 pk, a U64, b U64 [nullable per arg])` — indexable cols a=1, b=2.
    fn schema(b_nullable: bool) -> Schema {
        Schema {
            columns: vec![
                col_def("id", TypeCode::U64, false),
                col_def("a", TypeCode::U64, false),
                col_def("b", TypeCode::U64, b_nullable),
            ],
            pk_cols: vec![0],
        }
    }

    /// Extract the bound for `where_sql`, plus how many times the index list was
    /// fetched — a probe is a wire round-trip, so "zero" is part of the contract.
    fn bound_of(where_sql: &str, sch: &Schema, lists: &[&[u32]]) -> (Option<ScanBound>, u32) {
        let calls = Cell::new(0);
        let b = scan_bound_from(&parse_expr_sql(where_sql), sch, || {
            calls.set(calls.get() + 1);
            Ok(idx_metas(lists))
        })
        .unwrap();
        (b, calls.get())
    }

    /// A pure equality on a 1-column index lowers to a degenerate point range.
    #[test]
    fn equality_lowers_to_a_degenerate_point_range() {
        let (b, calls) = bound_of("a = 5", &schema(false), &[&[1]]);
        let b = b.expect("a = 5 on an index over `a` must bound");
        assert_eq!(b.idx_cols.as_slice(), &[1]);
        assert_eq!(b.desc.eq_vals(), &[] as &[u128]);
        assert_eq!((b.desc.start, b.desc.end), (Cut::Before(5), Cut::After(5)));
        assert_eq!(calls, 1, "one round-trip serves both collectors");
    }

    /// A two-column equality pins the leading column and points at the last.
    #[test]
    fn compound_equality_pins_the_leading_column() {
        let (b, _) = bound_of("a = 5 AND b = 7", &schema(false), &[&[1, 2]]);
        let b = b.expect("a compound equality must bound the compound index");
        assert_eq!(b.idx_cols.as_slice(), &[1, 2]);
        assert_eq!(b.desc.eq_vals(), &[5u128]);
        assert_eq!((b.desc.start, b.desc.end), (Cut::Before(7), Cut::After(7)));
    }

    /// An equality prefix plus a range on ONE index takes the range candidate.
    #[test]
    fn equality_prefix_plus_range_takes_the_range_candidate() {
        let (b, _) = bound_of("a = 5 AND b > 10", &schema(false), &[&[1, 2]]);
        let b = b.expect("an eq-prefix + range must bound");
        assert_eq!(b.desc.eq_vals(), &[5u128], "`a` is the pinned prefix");
        assert_eq!(b.desc.start, Cut::After(10), "`b > 10` is an exclusive lower cut");
        assert_ne!(b.desc.end, Cut::After(10), "the upper side stays open, not a point");
    }

    /// Across SEPARATE indexes, most-pinned wins.
    #[test]
    fn point_on_one_index_beats_half_open_range_on_another() {
        let (b, _) = bound_of("a = 5 AND b > 10", &schema(false), &[&[1], &[2]]);
        let b = b.expect("the point candidate must bound");
        assert_eq!(b.idx_cols.as_slice(), &[1], "INDEX(a)'s point beats INDEX(b)'s range");
        assert_eq!((b.desc.start, b.desc.end), (Cut::Before(5), Cut::After(5)));
    }

    /// A PK predicate never bounds: the collectors skip PK columns unconditionally.
    #[test]
    fn pk_equality_never_bounds() {
        assert_eq!(bound_of("id = 5", &schema(false), &[&[1]]).0, None);
    }

    /// An uncovered NULLABLE trailing index column must NOT bound.
    #[test]
    fn uncovered_nullable_trailing_column_never_bounds() {
        assert_eq!(bound_of("a = 5", &schema(true), &[&[1, 2]]).0, None);
        assert!(bound_of("a = 5", &schema(false), &[&[1, 2]]).0.is_some());
    }

    /// A WHERE no index covers costs ZERO round-trips on the range path and at most
    /// one overall — the collectors are lazy by contract.
    #[test]
    fn unindexed_column_bounds_nothing() {
        let (b, calls) = bound_of("a = 5", &schema(false), &[&[2]]);
        assert_eq!(b, None);
        assert_eq!(calls, 1, "the eq collector probes once, then finds no match");
        let (b, calls) = bound_of("a + b > 3", &schema(false), &[&[1]]);
        assert_eq!(b, None);
        assert_eq!(calls, 0, "a non-servable WHERE must cost no wire traffic");
    }

    /// A BETWEEN is a two-sided range over one column (desugared at bind).
    #[test]
    fn between_bounds_both_sides() {
        let (b, _) = bound_of("a BETWEEN 5 AND 9", &schema(false), &[&[1]]);
        let b = b.expect("BETWEEN must bound");
        assert_eq!(b.desc.eq_vals(), &[] as &[u128]);
        assert_eq!((b.desc.start, b.desc.end), (Cut::Before(5), Cut::After(9)));
    }
}
