//! Numeric and boolean `GNITZ_*` overrides. The parse rules are pure functions
//! of the raw value, shared with `fault`'s seams; the readers fall back to the
//! `default` they name when the variable is unset or the rule rejects it.
//! Call-site-specific clamps and `OnceLock` caching stay local to the consumer.

/// A positive number, or `None` for a zero or unparseable value: an override
/// never zeroes a knob.
pub(crate) fn positive<T: std::str::FromStr + Default + PartialOrd>(v: &str) -> Option<T> {
    v.parse::<T>().ok().filter(|n| *n > T::default())
}

/// `0` or the empty string is off, any other value on.
pub(crate) fn flag(v: &str) -> bool {
    !v.is_empty() && v != "0"
}

/// A positive-integer env override: unset, 0 or unparseable falls back to
/// `default`.
pub fn env_num<T: std::str::FromStr + Default + PartialOrd>(name: &str, default: T) -> T {
    std::env::var(name).ok().and_then(|v| positive(&v)).unwrap_or(default)
}

/// A boolean env override under [`flag`]'s rule; unset falls back to `default`.
pub fn env_flag(name: &str, default: bool) -> bool {
    std::env::var(name).map_or(default, |v| flag(&v))
}

#[cfg(test)]
#[path = "tests/env.rs"]
mod tests;
