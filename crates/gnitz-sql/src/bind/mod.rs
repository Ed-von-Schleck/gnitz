//! Binding: AST → `BoundExpr`, and name → relation resolution.
//!
//! `resolve` (the catalog cache, column lookup, alias maps) and `structural`
//! (the one `Expr → BoundExpr` recursion + its leaves) are a mutually-recursive
//! pair: `structural` calls down into `resolve` for column lookup, and
//! `bind_single_table` — defined in `structural` — drives the recursion using
//! `resolve`'s column lookup through the `SingleTable` leaf. The dependency is one-directional
//! (`structural → resolve`, never the reverse); were `bind/` ever promoted to
//! its own crate the two files would move together.

mod resolve;
pub(crate) mod structural;

pub(crate) use resolve::{apply_positional_aliases, cte_passthrough, find_unique_column, Binder};
pub(crate) use structural::{bind_single_table, bind_structural, fold_null_test, LeafBinder, SingleTable};
