//! Binding: AST → `BExpr`, and name → relation resolution.
//!
//! `resolve` holds the column lookup, the `Catalog` a plan reads and positional
//! aliases; `structural` holds the one `Expr → BExpr` recursion and the leaf
//! interface it binds through, and calls down into `resolve` for column lookup.

mod resolve;
mod structural;

pub(crate) use resolve::{apply_positional_aliases, find_unique_column, output_column, require_column, Catalog};
pub(crate) use structural::{
    bind_conjuncts, bind_constant, bind_structural, maybe_negate, single_relation_col_idx, unsupported_subquery,
    LeafBinder,
};
