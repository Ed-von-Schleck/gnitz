pub mod ring;
pub mod recv;
pub mod send;
pub mod conn;
pub mod transport;
pub mod uring;
pub mod ffi;

#[cfg(test)]
pub mod mock;

// Re-export mock for integration tests
#[cfg(test)]
pub use mock::MockRing;
