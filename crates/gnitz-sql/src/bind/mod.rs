//! Binding: AST → `BoundExpr`, and name → relation resolution.
//!
//! `resolve` (the catalog cache, column lookup, alias maps) and `structural`
//! (the one `Expr → BoundExpr` recursion + its leaves) are one unit: the
//! dependency runs `structural → resolve` and never the reverse — `structural`
//! calls down for column lookup, and `bind_single_table` drives the recursion
//! through the `SingleTable` leaf. Were `bind/` ever promoted to its own crate
//! the two files would move together.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

mod resolve;
pub(crate) mod structural;

pub(crate) use resolve::{
    apply_positional_aliases, find_unique_column, output_column, probe, probe_relation, require_column, Binder,
};
pub(crate) use structural::{
    bind_conjuncts, bind_single_table, bind_structural, reject_foreign_qualifier, single_relation_col_idx, LeafBinder,
    SingleTable,
};
