//! L5 query core — the circuit compiler, the DBSP bytecode VM, and the DAG
//! scheduler, behind one facade.
//!
//! `dag` is the de-facto facade: it owns the plan cache and the epoch
//! evaluator, and is the single inbound target catalog + runtime reach for.
//! `compiler` (view → circuit → VM program) and `vm` (program execution) are
//! query-internal — only `dag` and each other call into them. `RelayRoute`, the
//! enum the master relay matches on, is the one name that leaves.

mod compiler;
mod dag;
mod vm;

pub(crate) use compiler::RelayRoute;
pub(crate) use dag::{DagEngine, ExchangeCallback};
