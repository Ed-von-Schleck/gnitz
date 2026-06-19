//! The client-side read path: one IR interpreter (`eval`) and the batch
//! reshaping (`residual`, `batch`) that `dml` drives after a seek/scan reply.
//! Sinks only into the shared lower layers (`lower`, `codec`, `bind`); holds no
//! edge back up into `dml` or `plan`.

pub(crate) mod batch;
pub(crate) mod eval;
pub(crate) mod residual;
