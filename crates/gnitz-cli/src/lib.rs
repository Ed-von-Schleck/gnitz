//! `gnitz`, the interactive SQL shell: a line editor with highlighting and
//! catalog-aware completion over the SQL front end, psql-style meta-commands,
//! and `\watch` over a view's delta feed. Given no server, the binary runs a
//! separately installed one for the session.
//!
//! The library is the shell the binary and the integration suite drive; its
//! parts are private. The crate-root `tests/` is the integration suite, gated
//! on the `integration` feature.

mod complete;
mod editor;
mod highlight;
mod lex;
mod meta;
mod render;
mod settings;
mod shell;
mod value;
mod watch;

pub use editor::run as repl;
pub use settings::{Expanded, Format, Settings};
pub use shell::{Failed, Flow, Options, Shell};
