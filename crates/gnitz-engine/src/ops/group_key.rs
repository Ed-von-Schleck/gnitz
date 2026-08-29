//! The group key shared by the reduce and the exchange scatter: whether a group
//! set has a canonical (order-preserving, injective) single-column key, and the
//! 128-bit key of a row either way.

use crate::schema::key::hash_fold;
use crate::schema::{ColumnLocator, SchemaDescriptor};
use gnitz_expr::RowSource;

/// Whether the group key of `group_by_cols` can be emitted through the
/// canonical (order-preserving) fast path — `ColumnLocator::route_key` on the
/// single group column — rather than the XXH3 fold (multi-column, nullable, or
/// non-routable type). Two shapes qualify: a single PK (sub-)column, whose OPK
/// window widens directly; and a single non-nullable routable-int payload
/// column, which OPK-encodes then widens to the same image — so a value routes
/// identically whether it is the PK on one side of a join or a payload FK on
/// the other. `route_key` dispatches on the locator, so the two need no
/// separate arm here.
///
/// A canonical key is **injective** on the group value, which is what lets a
/// key-equality test stand in for a value comparison (the ad-hoc fold's
/// per-row confirmation), and it is order-preserving, so sorting by it visits
/// groups in ascending output-PK order.
#[inline]
pub(super) fn single_col_canonical_group_key(schema: &SchemaDescriptor, group_by_cols: &[u32]) -> bool {
    if group_by_cols.len() != 1 {
        return false;
    }
    let c = group_by_cols[0] as usize;
    if schema.is_pk_col(c) {
        return true;
    }
    // `try_payload_idx` is the totality gate: `Some` proves `c` names a real
    // payload column, so the read below cannot land on a padding slot.
    schema
        .try_payload_idx(c)
        .is_some_and(|_| schema.columns[c].nullable == 0 && gnitz_wire::is_pk_eligible(schema.columns[c].type_code))
}

/// The 128-bit group key of a row: the canonical single-column route key where
/// the group set has one, else an XXH3 fold of the per-column canonical
/// material. Per-column locators are resolved once at bake time, so the per-row
/// body is the fold alone.
///
/// The one implementation: the scatter's routing key and `op_reduce`'s output
/// PK both come off it, so they cannot drift.
pub(super) struct GroupKeyCols {
    /// See [`single_col_canonical_group_key`]. Private, and read only through
    /// [`GroupKeyCols::canonical_col`], so the flag and the single column it
    /// promises can never be consulted apart.
    canonical: bool,
    /// The group columns' resolved locators, in group-set order. Empty for a
    /// global (ungrouped) aggregate, whose key is the constant
    /// `gnitz_wire::global_group_key()` — which is what `key_row` folds to over
    /// zero columns.
    pub(super) cols: Vec<ColumnLocator>,
}

impl GroupKeyCols {
    pub(super) fn new(schema: &SchemaDescriptor, group_by_cols: &[u32]) -> Self {
        // Resolve first: `locate` carries the release-active in-range assert, so
        // every index is proven before anything else reads `schema.columns`.
        let cols: Vec<ColumnLocator> = group_by_cols.iter().map(|&c| schema.locate(c as usize)).collect();
        GroupKeyCols {
            canonical: single_col_canonical_group_key(schema, group_by_cols),
            cols,
        }
    }

    /// The single column the group key is the canonical `route_key` of, or
    /// `None` when the key is the hash fold. `Some` is exactly "the key is
    /// injective and order-preserving on the group value".
    #[inline]
    pub(super) fn canonical_col(&self) -> Option<ColumnLocator> {
        self.canonical.then(|| self.cols[0])
    }

    /// The 128-bit group key of `row`. Over an empty group set this is the fold
    /// of nothing — `gnitz_wire::global_group_key()`, the V₀ every global
    /// aggregate keys its one row by.
    #[inline]
    pub(super) fn key_row<R: RowSource>(&self, src: &R, row: usize) -> u128 {
        if let Some(col) = self.canonical_col() {
            return col.route_key(src, row);
        }
        hash_fold(&self.cols, src, row, src.get_null_word(row))
    }
}

#[cfg(test)]
#[path = "tests/group_key.rs"]
mod tests;
