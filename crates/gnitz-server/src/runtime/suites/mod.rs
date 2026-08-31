//! Tests no single module in `runtime` owns: each drives a path that crosses
//! `orchestration`, `protocol` and `read`, reaching this subsystem's surface
//! rather than any one module's private items. A test that belongs to one
//! module goes in that module's own `tests/<module>.rs`.

mod block_integrity;
mod unique_preflight;
