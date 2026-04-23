//! Runtime coordination subsystem: IPC channels, master/worker/executor/committer/bootstrap.

pub(crate) mod sys;
pub(crate) mod w2m_ring;
pub(crate) mod wire;
pub(crate) mod sal;
pub(crate) mod w2m;
pub(crate) mod reactor;
pub(crate) mod master;
pub(crate) mod worker;
pub(crate) mod committer;
pub(crate) mod executor;
pub(crate) mod bootstrap;

pub use bootstrap::server_main;

#[cfg(test)]
mod tests;
