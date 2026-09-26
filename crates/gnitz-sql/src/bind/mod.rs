//! Binding: AST → `BoundExpr`, and name → relation resolution.
//!
//! `resolve` (column lookup, snapshot probes, positional aliases) and `structural`
//! (the one `Expr → BoundExpr` recursion + its leaves) are one unit: the
//! dependency runs `structural → resolve` and never the reverse — `structural`
//! calls down for column lookup, and `bind_single_table` drives the recursion
//! through the `SingleTable` leaf. Were `bind/` ever promoted to its own crate
//! the two files would move together.

mod resolve;
pub(crate) mod structural;

pub(crate) use resolve::{
    apply_positional_aliases, find_unique_column, output_column, probe, probe_relation, require_column,
};
pub(crate) use structural::{
    bind_conjuncts, bind_single_table, bind_structural, reject_foreign_qualifier, single_relation_col_idx, LeafBinder,
    SingleTable,
};
