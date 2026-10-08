//! The query core — the circuit compiler, the DBSP bytecode VM, and the DAG
//! scheduler. `dag` is the facade the catalog and the runtime reach for;
//! `compiler` and `vm` are internal to it.

mod compiler;
mod dag;
mod vm;

pub(crate) use dag::{drive, DagEngine, Drive, DriveHost};
