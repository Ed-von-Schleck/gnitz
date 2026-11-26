//! Numeric, boolean and path `GNITZ_*` overrides. The parse rules are pure functions
//! of the raw value, shared with `fault`'s seams. A reader returns the `default`
//! it names only for an unset variable; a value its rule rejects is refused, so
//! no reader selects a value the operator did not ask for.
//!
//! A refusal panics unless the process chose an exit status for it with
//! [`exit_on_refusal`]: clients and `gnitz-mirror` link this too, and a library
//! does not end its host, while a binary's supervisor reads its exit status. A
//! knob confined to a range is read with [`env_num_in`], so an override outside
//! it is refused rather than clamped.

use std::io::Write;
use std::sync::OnceLock;

/// The status and message prefix [`exit_on_refusal`] chose; unset, a refusal
/// panics.
static REFUSAL_EXIT: OnceLock<(i32, &'static str)> = OnceLock::new();

/// From now on, a value a rule rejects ends this process with `status`, its
/// message on stderr behind `prefix`, instead of panicking. For a binary, whose
/// supervisor tells a refused configuration from a crash by the status; forked
/// children inherit the choice. The first call decides.
pub fn exit_on_refusal(status: i32, prefix: &'static str) {
    let _ = REFUSAL_EXIT.set((status, prefix));
}

/// Refuse a value with `message`: the process's chosen exit, else a panic.
fn refuse(message: String) -> ! {
    if let Some(&(status, prefix)) = REFUSAL_EXIT.get() {
        let _ = writeln!(std::io::stderr(), "{prefix}{message}");
        // SAFETY: `_exit` ends the process without unwinding, atexit handlers
        // or stdio buffers; stderr is unbuffered and was written above.
        unsafe { libc::_exit(status) }
    }
    panic!("{message}")
}

/// A positive number, or `None` for a zero or unparseable value: an override
/// never zeroes a knob.
pub(crate) fn positive<T: std::str::FromStr + Default + PartialOrd>(v: &str) -> Option<T> {
    v.parse::<T>().ok().filter(|n| *n > T::default())
}

/// `0`, `false`, `no` or the empty string is off, `1`, `true` or `yes` on, in
/// any case; `None` for anything else.
pub(crate) fn flag(v: &str) -> Option<bool> {
    match v.to_ascii_lowercase().as_str() {
        "" | "0" | "false" | "no" => Some(false),
        "1" | "true" | "yes" => Some(true),
        _ => None,
    }
}

/// What a value [`positive`] refuses should have been.
pub(crate) const POSITIVE: &str = "a positive integer";
/// What a value [`flag`] refuses should have been.
pub(crate) const BOOLEAN: &str = "a boolean (0|1|true|false|yes|no)";

/// `name`'s value, `None` when unset.
///
/// # Panics
///
/// On a value that is not UTF-8, which no rule reads, unless
/// [`exit_on_refusal`] chose an exit instead.
pub fn raw(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(v) => Some(v),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(v)) => refuse(format!("{name}={v:?} is not UTF-8")),
    }
}

/// `raw`, `name`'s value, as `rule` reads it; `None` when unset.
///
/// # Panics
///
/// On a value `rule` rejects, naming the variable, the value and `what` it
/// should have been, unless [`exit_on_refusal`] chose an exit instead.
pub(crate) fn parse<T>(name: &str, raw: Option<&str>, rule: impl FnOnce(&str) -> Option<T>, what: &str) -> Option<T> {
    raw.map(|raw| rule(raw).unwrap_or_else(|| refuse(format!("{name}={raw:?} is not {what}"))))
}

/// A positive-integer env override; unset reads as `default`, and a zero or
/// unparseable value is refused.
pub fn env_num<T: std::str::FromStr + Default + PartialOrd>(name: &str, default: T) -> T {
    parse(name, raw(name).as_deref(), positive, POSITIVE).unwrap_or(default)
}

/// An integer env override within `range`; unset reads as `default`, and a
/// value outside it, or unparseable, is refused.
pub fn env_num_in<T, R>(name: &str, default: T, range: R) -> T
where
    T: std::str::FromStr + PartialOrd,
    R: std::ops::RangeBounds<T> + std::fmt::Debug,
{
    parse(
        name,
        raw(name).as_deref(),
        |v| v.parse().ok().filter(|n| range.contains(n)),
        &format!("an integer in {range:?}"),
    )
    .unwrap_or(default)
}

/// A boolean env override under [`flag`]'s rule; unset reads as `default`, and
/// a value the rule does not read is refused.
pub fn env_flag(name: &str, default: bool) -> bool {
    parse(name, raw(name).as_deref(), flag, BOOLEAN).unwrap_or(default)
}

/// A path env override, `None` when unset; an empty value, which names no
/// path, is refused.
pub fn env_path(name: &str) -> Option<std::path::PathBuf> {
    let raw = std::env::var_os(name)?;
    if raw.is_empty() {
        refuse(format!("{name}=\"\" is not a path"));
    }
    Some(raw.into())
}

#[cfg(test)]
#[path = "tests/env.rs"]
mod tests;
