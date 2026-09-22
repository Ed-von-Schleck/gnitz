//! Numeric and boolean `GNITZ_*` overrides. The parse rules are pure functions
//! of the raw value, shared with `fault`'s seams. A reader returns the `default`
//! it names only for an unset variable; a value its rule rejects panics, so no
//! reader selects a value the operator did not ask for.
//!
//! A panic and not an exit: clients and `gnitz-mirror` link this too, and a
//! library does not end its host. Call-site-specific clamps and `OnceLock`
//! caching stay local to the consumer.

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

/// `name`'s override: `default` when unset, else `raw` as `rule` reads it.
///
/// # Panics
///
/// On a value `rule` rejects, naming the variable, the value and `what` it
/// should have been.
fn read<T>(name: &str, raw: Option<&str>, default: T, rule: impl Fn(&str) -> Option<T>, what: &str) -> T {
    match raw {
        None => default,
        Some(raw) => rule(raw).unwrap_or_else(|| panic!("{name}={raw:?} is not {what}")),
    }
}

/// A positive-integer env override; unset reads as `default`, and a zero or
/// unparseable value panics.
pub fn env_num<T: std::str::FromStr + Default + PartialOrd>(name: &str, default: T) -> T {
    let raw = std::env::var(name).ok();
    read(name, raw.as_deref(), default, positive, "a positive integer")
}

/// A boolean env override under [`flag`]'s rule; unset reads as `default`, and
/// a value the rule does not read panics.
pub fn env_flag(name: &str, default: bool) -> bool {
    let raw = std::env::var(name).ok();
    read(name, raw.as_deref(), default, flag, "a boolean (0|1|true|false|yes|no)")
}

#[cfg(test)]
#[path = "tests/env.rs"]
mod tests;
