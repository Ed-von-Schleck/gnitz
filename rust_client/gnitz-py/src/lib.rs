use pyo3::prelude::*;

/// Minimal stub — full API added in Phase 7.
#[pymodule]
fn gnitz(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let _ = m;
    Ok(())
}
