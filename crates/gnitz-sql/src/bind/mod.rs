//! Binding: AST → `BoundExpr`, and name → relation resolution.
//!
//! `resolve` (the catalog cache, column lookup, alias maps) and `structural`
//! (the one `Expr → BoundExpr` recursion + its leaves) are a mutually-recursive
//! pair: `structural` calls down into `resolve` for column lookup, and
//! `Binder::bind_expr` — defined in `structural` — drives the recursion over a
//! `Binder` whose data lives in `resolve`. The dependency is one-directional
//! (`structural → resolve`, never the reverse); were `bind/` ever promoted to
//! its own crate the two files would move together.

mod resolve;
mod structural;

pub(crate) use resolve::{
    find_unique_column, resolve_qualified_column, resolve_unqualified_column, AliasMap, Binder, ResolvedRelation,
};
pub(crate) use structural::{bind_structural, fold_null_test, LeafBinder};
