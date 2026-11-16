//! A float's narrowing to F32 and its decimal text.

/// `v` rounded to F32; `None` when a finite `v` rounds past F32's range. The
/// rounded value is what is tested, so a double just above `f32::MAX` that
/// rounds down onto it is that value.
pub fn narrow_f32(v: f64) -> Option<f32> {
    let f = v as f32;
    (f.is_finite() || !v.is_finite()).then_some(f)
}

/// A float's decimal text: shortest round-trip, scientific outside
/// `[1e-4, 1e15)` so the text stays short, non-finite values as PostgreSQL
/// spells them. An `f32` keeps its own shortest form. The engine's float-to-text
/// cast writes this spelling, and a client prints a float with it.
pub struct FloatText<T>(pub T);

impl<T: Copy + Into<f64> + core::fmt::Display + core::fmt::LowerExp> core::fmt::Display for FloatText<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let v: f64 = self.0.into();
        if v.is_nan() {
            f.write_str("NaN")
        } else if v.is_infinite() {
            f.write_str(if v > 0.0 { "Infinity" } else { "-Infinity" })
        } else if v == 0.0 || (1e-4..1e15).contains(&v.abs()) {
            core::fmt::Display::fmt(&self.0, f)
        } else {
            core::fmt::LowerExp::fmt(&self.0, f)
        }
    }
}

#[cfg(test)]
#[path = "tests/float.rs"]
mod tests;
