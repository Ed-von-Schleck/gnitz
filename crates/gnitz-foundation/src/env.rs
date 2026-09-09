//! Numeric and boolean `GNITZ_*` overrides, under one parse rule: a missing or
//! unparseable value falls back to the `default` the reader names. `env_num`
//! additionally refuses a zero, because its consumers require a positive value.
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

/// A boolean env override: `0` or the empty string turn it off, any other value
/// turns it on. Unset falls back to `default`.
pub fn env_flag(name: &str, default: bool) -> bool {
    std::env::var(name).map_or(default, |v| !v.is_empty() && v != "0")
}

#[cfg(test)]
#[path = "tests/env.rs"]
mod tests;
