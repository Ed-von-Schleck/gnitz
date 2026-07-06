//! L5 query core — the circuit compiler, the DBSP bytecode VM, and the DAG
//! scheduler, behind one facade.
//!
//! `dag` is the de-facto facade: it owns the plan cache and the epoch
//! evaluator, and is the single inbound target catalog + runtime reach for.
//! `compiler` (view → circuit → VM program) and `vm` (program execution) are
//! query-internal — only `dag` and each other name them.

mod compiler;
mod dag;
mod vm;

pub(crate) use dag::{
    is_worker_scratch_dir_name, DagEngine, ExchangeCallback, IndexCircuitEntry, RelationKind, StoreHandle, SysTableRefs,
};
