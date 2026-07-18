//! Numeric environment-variable overrides — the one parse rule for every
//! `GNITZ_*` knob: a missing, unparseable, or zero value falls back to
//! `default`, so an override can never zero out a knob whose consumer
//! requires a positive value. Call-site-specific clamps and `OnceLock`
//! caching stay local to the consumer.

/// A positive-`usize` env override: a 0 or unparseable value falls back to
/// `default`.
pub(crate) fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

/// A positive-`u64` env override: a 0 or unparseable value falls back to
/// `default`.
pub(crate) fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}
