//! The backfill-scan index bound a `ScanDelta` carries.
//!
//! A **physical access hint**, never a semantic filter: the caller emits the FULL
//! `Filter` downstream regardless, so a bound only narrows what the initial
//! full-source scan reads. That is why a bound may be dropped anywhere (no index,
//! a non-catalog id, a dropped index at run time) with no effect on results — and
//! why it is extracted from the raw AST here rather than recovered server-side from
//! the opaque `ExprProgram` blob, where a mistake would be a *narrowing* one:
//! silently wrong rows.

use crate::dml::plan::{collect_index_range_candidates, collect_index_seek_candidates, IndexListMemo};
use crate::error::GnitzSqlError;
use gnitz_core::{GnitzClient, Schema};
use gnitz_wire::{Cut, RangeDescriptor, ScanBound};
use std::cmp::Reverse;
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

/// The best index range/equality bound for `where_expr`, or `None` when no index
/// covers a leading conjunct. `fetch` (the GET_INDICES probe) is injected — which
/// index to pick is a pure decision over `(where_expr, schema, index list)`, so it
/// tests without a live catalog — and is called **at most once**: both collectors
/// are lazy (a WHERE with no usable conjunct fetches no indexes) and share one
/// [`IndexListMemo`].
///
/// Every candidate unifies as a range. A pure n-column equality lowers to a
/// degenerate point range over its LAST column — eq = vals[..n-1], start =
/// `Before(v_last)`, end = `After(v_last)` — since `RangeDescriptor` cannot
/// express "no range column", and without the lowering the headline
/// `WHERE indexed = 5` would carry no bound at all. The engine then cuts
/// `[seek_prefix(vals), seek_prefix(vals)+1)` — exactly that key group, which also
/// covers every `(vals, *)` entry when the index has trailing columns the equality
/// does not pin. (`consume_leading_eq_prefix` breaks at the first uncovered
/// column, so `1 <= n <= idx_cols.len() <= PK_LIST_MAX_COLS`: `n-1 <
/// PK_LIST_MAX_COLS` satisfies `RangeDescriptor::new`'s strict arity assert, and
/// `n_eq = n-1 < idx_cols.len()` satisfies the engine's arity check.)
///
/// The merged candidates are ranked by [`pinned_score`] — most-constrained
/// first — and the head taken outright. There is no residual pre-filter and no
/// probe: unlike the thin seek path, the caller emits the FULL predicate as the
/// `Filter`, so each candidate's residual is discarded. Both collectors reject a
/// candidate with an uncovered nullable trailing column (a NULL in any indexed
/// column omits the whole row from the index, so such a bound would drop rows the
/// predicate matches).
fn scan_bound_from<F>(
    where_expr: &sqlparser::ast::Expr,
    schema: &Schema,
    mut fetch: F,
) -> Result<Option<ScanBound>, GnitzSqlError>
where
    F: FnMut() -> Result<Arc<Vec<gnitz_core::IndexMeta>>, gnitz_core::ClientError>,
{
    let mut memo = IndexListMemo::default();
    let ranges =
        collect_index_range_candidates(where_expr, schema, || memo.get(&mut fetch)).map_err(GnitzSqlError::Exec)?;
    let seeks =
        collect_index_seek_candidates(where_expr, schema, || memo.get(&mut fetch)).map_err(GnitzSqlError::Exec)?;

    let mut cands: Vec<ScanBound> = ranges
        .into_iter()
        .map(|c| ScanBound {
            idx_cols: c.idx_cols,
            desc: c.desc,
        })
        .chain(seeks.into_iter().map(|(idx_cols, vals, _residual)| {
            let n = vals.len();
            let last = vals[n - 1];
            ScanBound {
                idx_cols,
                desc: RangeDescriptor::new(&vals[..n - 1], Cut::Before(last), Cut::After(last)),
            }
        }))
        .collect();
    // Stable sort: on a full tie the range collector's candidate (listed first)
    // wins, matching each collector's own internal preference order.
    cands.sort_by_key(|b| Reverse(pinned_score(b, schema)));
    Ok(cands.into_iter().next())
}

/// How constrained a candidate leaves the index walk, as a totally ordered key:
/// 2 per equality-pinned column plus 1 per real range side, then (descending in
/// the sort, so ascending arity) the tighter index on a tie.
///
/// One rule ranks equality and range candidates together — "most pinned columns
/// wins" — where a "range beats equality" tier would mis-pick across indexes:
/// `WHERE a = 5 AND b > 10` with `INDEX(a)` and `INDEX(b)` must bound on the
/// point `a = 5` (score 2), not the half-open `b > 10` (score 1) that the
/// selectivity gate then likely rejects, full-scanning past the usable bound.
/// A side widened to the column type's edge cut by the collector IS the
/// unbounded side, so it scores 0. (A point cut on a type-edge value — `a =
/// u64::MAX` — is indistinguishable from the widened edge and forfeits that
/// side's credit: a mis-*ranking* only, never a mis-*bound*.)
fn pinned_score(b: &ScanBound, schema: &Schema) -> (u32, Reverse<usize>) {
    let n_eq = b.desc.eq_vals().len();
    let range_col = b.idx_cols.as_slice()[n_eq];
    let sides = match Cut::type_edges(schema.columns[range_col as usize].type_code) {
        Some((lo, hi)) => (b.desc.start != lo) as u32 + (b.desc.end != hi) as u32,
        // Unreachable: both collectors only emit range-servable column types.
        None => 2,
    };
    (2 * n_eq as u32 + sides, Reverse(b.idx_cols.as_slice().len()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{col_def, idx_metas, parse_expr_sql};
    use gnitz_core::TypeCode;
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

    /// A pure equality on a 1-column index lowers to a degenerate point range —
    /// `RangeDescriptor` cannot express "no range column", and without this the
    /// headline `WHERE indexed = 5` would carry no bound at all.
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

    /// An equality prefix plus a range on ONE index takes the range candidate: it
    /// pins the prefix (score 2) AND a range side (score 3) where the bare prefix
    /// seek pins only the prefix (score 2).
    #[test]
    fn equality_prefix_plus_range_takes_the_range_candidate() {
        let (b, _) = bound_of("a = 5 AND b > 10", &schema(false), &[&[1, 2]]);
        let b = b.expect("an eq-prefix + range must bound");
        assert_eq!(b.desc.eq_vals(), &[5u128], "`a` is the pinned prefix");
        assert_eq!(b.desc.start, Cut::After(10), "`b > 10` is an exclusive lower cut");
        assert_ne!(b.desc.end, Cut::After(10), "the upper side stays open, not a point");
    }

    /// Across SEPARATE indexes, most-pinned wins: `a = 5 AND b > 10` with
    /// `INDEX(a)` and `INDEX(b)` must bound on the point `a = 5` (score 2), not
    /// the half-open `b > 10` (score 1) — a "range beats equality" tier would
    /// ship the wide range and the engine's selectivity gate would then likely
    /// full-scan past the usable point bound.
    #[test]
    fn point_on_one_index_beats_half_open_range_on_another() {
        let (b, _) = bound_of("a = 5 AND b > 10", &schema(false), &[&[1], &[2]]);
        let b = b.expect("the point candidate must bound");
        assert_eq!(b.idx_cols.as_slice(), &[1], "INDEX(a)'s point beats INDEX(b)'s range");
        assert_eq!((b.desc.start, b.desc.end), (Cut::Before(5), Cut::After(5)));
    }

    /// A PK predicate never bounds: the collectors skip PK columns unconditionally,
    /// whatever index circuits exist. So a bound always names a secondary index.
    #[test]
    fn pk_equality_never_bounds() {
        assert_eq!(bound_of("id = 5", &schema(false), &[&[1]]).0, None);
    }

    /// An uncovered NULLABLE trailing index column must NOT bound. A NULL in any
    /// indexed column omits the whole row from the index, so bounding `a = 5` on
    /// index `(a, b)` with `b` nullable would silently drop every `(a=5, b=NULL)`
    /// row the `Filter` accepts — a subset, not a superset.
    #[test]
    fn uncovered_nullable_trailing_column_never_bounds() {
        assert_eq!(bound_of("a = 5", &schema(true), &[&[1, 2]]).0, None);
        // Non-nullable `b`: the same shape DOES bound — pinning that the guard
        // above is the nullability, not the uncovered column.
        assert!(bound_of("a = 5", &schema(false), &[&[1, 2]]).0.is_some());
    }

    /// A WHERE no index covers costs ZERO round-trips on the range path and at most
    /// one overall — the collectors are lazy by contract.
    #[test]
    fn unindexed_column_bounds_nothing() {
        // An index on `b` only; the WHERE names `a`.
        let (b, calls) = bound_of("a = 5", &schema(false), &[&[2]]);
        assert_eq!(b, None);
        assert_eq!(calls, 1, "the eq collector probes once, then finds no match");
        // No `col OP literal` conjunct at all ⇒ no collector probes the wire.
        let (b, calls) = bound_of("a + b > 3", &schema(false), &[&[1]]);
        assert_eq!(b, None);
        assert_eq!(calls, 0, "a non-servable WHERE must cost no wire traffic");
    }

    /// A BETWEEN is a two-sided range over one column.
    #[test]
    fn between_bounds_both_sides() {
        let (b, _) = bound_of("a BETWEEN 5 AND 9", &schema(false), &[&[1]]);
        let b = b.expect("BETWEEN must bound");
        assert_eq!(b.desc.eq_vals(), &[] as &[u128]);
        assert_eq!((b.desc.start, b.desc.end), (Cut::Before(5), Cut::After(9)));
    }
}
