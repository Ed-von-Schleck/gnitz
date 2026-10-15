//! Fuzzing gnitz through what a network delivers: the frames a client sends a
//! server, and the frames a server sends back.
//!
//! **Seeds are recorded traffic.** A process whose `GNITZ_RECORD_FRAMES` names
//! a directory records every connection it opens ([`gnitz_core::recording`]);
//! the test suites run that way leave one recording per test server, which
//! [`seed::import`] turns into seeds. A recording replays deterministically on
//! a fresh server: catalog ids come from a counter and descriptor tokens hash
//! what they describe.
//!
//! **A fuzz input is a program of edits to one seed** ([`input::Input`]). An
//! edit names a field, a list item, a string, a byte or a whole frame of the
//! seed's traffic, located by the [`anatomy`] the production decoders read off
//! each frame — so the inputs reach whatever structure those decoders accept,
//! and follow it when a format changes. Every byte string decodes to some
//! program, so a coverage-guided engine mutating the bytes explores edits.
//!
//! Two kinds of target run inputs:
//!
//! - [`session`] plays the edited traffic into a real multi-process server
//!   booted for the one input, and judges what the server did. It is the only
//!   target that reaches everything between a socket and the stores.
//! - In-process targets, in the `fuzz/` modules of the crates whose code they
//!   reach, feed the same edited traffic to the functions the server calls at
//!   one boundary — many times faster, and under AddressSanitizer.

pub mod anatomy;
pub mod edit;
pub mod input;
pub mod seed;
pub mod session;
#[cfg(test)]
mod test_support;
