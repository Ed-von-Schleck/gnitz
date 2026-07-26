//! The one expression evaluator GnitzDB runs — on the engine and in the SQL
//! client alike.
//!
//! A leaf crate below the entire engine: its only dependency is `gnitz-wire`
//! (type codes, the OPK codec, the German-string layout), so both the server
//! and the client-side planner can link it without pulling in storage, the
//! catalog, or the runtime. Whatever the engine computes for an expression, the
//! client computes bit-for-bit, because it is the same code.
//!
//! The crate is organized into topic modules with every item re-exported flat at
//! the crate root (`gnitz_expr::FOO`), matching `gnitz-wire`'s leaf-crate shape.
//!
//! What lives here is the *resolved addressing* substrate — [`ColumnLocator`]
//! (where a logical column physically sits in a row), the access traits it
//! reads through ([`RowSource`], and [`BatchView`] for the vectorized kernels),
//! and [`SchemaFacts`], the schema surface the compiler resolves against.

mod locator;
mod schema_facts;
mod view;

pub use locator::*;
pub use schema_facts::*;
pub use view::*;

#[cfg(test)]
mod tests;
