//! Environment-variable overrides — the one parse rule for every `GNITZ_*`
//! knob: a missing, unparseable, or zero value falls back to `default`, so an
//! override can never zero out a knob whose consumer requires a positive value.
//! Call-site-specific clamps and `OnceLock` caching stay local to the consumer.

/// A positive-integer env override: a 0 or unparseable value falls back to
/// `default`.
pub fn env_num<T: std::str::FromStr + Default + PartialOrd>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<T>().ok())
        .filter(|n| *n > T::default())
        .unwrap_or(default)
}

/// A boolean env override: set to anything but `0` or the empty string turns
/// the flag on. Unset is off.
pub fn env_flag(name: &str) -> bool {
    std::env::var(name).is_ok_and(|v| !v.is_empty() && v != "0")
}
