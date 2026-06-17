//! Runtime coordination subsystem: IPC channels, master/worker/executor/committer/bootstrap.

// Runtime is a CLOSED subsystem: only `server_main` + `MAX_WORKERS` escape the
// crate-wide surface (W8). The submodules are private `mod`, so their `pub`
// internals are reachable within runtime (cross-submodule refs via
// `crate::runtime::X::…` / `super::X::…` still resolve — descendants can name a
// private sibling module) but not from any other subsystem.
mod w2m_ring;
mod wire;
mod sal;
mod w2m;
mod reactor;
mod master;
mod worker;
mod committer;
mod executor;
mod bootstrap;

pub use bootstrap::server_main;
pub(crate) use sal::MAX_WORKERS;

#[cfg(test)]
mod tests;
