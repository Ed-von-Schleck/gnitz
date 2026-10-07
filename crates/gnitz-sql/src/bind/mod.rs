//! Binding: AST → `BoundExpr`, and name → relation resolution.
//!
//! `resolve` holds the column lookup, the `Catalog` a plan reads and positional
//! aliases; `structural` holds the one `Expr → BoundExpr` recursion and its
//! leaves, and calls down into `resolve` for column lookup.

mod resolve;
pub(crate) mod structural;

pub(crate) use resolve::{apply_positional_aliases, find_unique_column, output_column, require_column, Catalog};
pub(crate) use structural::{
    bind_conjuncts, bind_single_table, bind_structural, reject_foreign_qualifier, single_relation_col_idx, LeafBinder,
    SingleTable,
};
