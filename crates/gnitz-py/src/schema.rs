//! The schema surface: the `ColumnDef` and `Schema` pyclasses, and the
//! conversions between them and `gnitz_core`'s `Schema`.
//!
//! Holds no codec: turning a *value* into wire bytes or back is `write`'s and
//! `read`'s, and this module only ever describes the columns those two address.

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::{PyList, PyString};

use gnitz_core::protocol::types::type_code_from_u64;
use gnitz_core::{ColType, ColumnDef, Schema};

use crate::build_pylist;

/// `name` is held as an interned `PyString`, not a `String`: a `#[pyclass]`
/// getter over a Rust `String` builds a fresh `PyString` on *every* read, so
/// `c.name` would hand back a different object each time. Interned, every read
/// returns the identical object — the one a dict keyed by that name, or a
/// `**kwargs` splat built from it, hits on by pointer.
#[pyclass(name = "ColumnDef", get_all, frozen)]
pub struct PyColumnDef {
    pub name: Py<PyString>,
    pub type_code: u32,
    pub is_nullable: bool,
    pub primary_key: bool,
    pub is_hidden: bool,
    /// A DECIMAL column's scale; 0 for every other type.
    pub scale: u8,
}

#[pymethods]
impl PyColumnDef {
    #[new]
    #[pyo3(signature = (name, type_code, is_nullable = false, primary_key = false, is_hidden = false, scale = 0))]
    pub fn new(
        py: Python<'_>,
        name: &str,
        type_code: u32,
        is_nullable: bool,
        primary_key: bool,
        is_hidden: bool,
        scale: u8,
    ) -> Self {
        PyColumnDef {
            name: PyString::intern(py, name).unbind(),
            type_code,
            is_nullable,
            primary_key,
            is_hidden,
            scale,
        }
    }

    pub fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "ColumnDef(name={:?}, type_code={}, is_nullable={}, primary_key={}, is_hidden={}, scale={})",
            self.name.bind(py).to_cow()?,
            self.type_code,
            self.is_nullable,
            self.primary_key,
            self.is_hidden,
            self.scale
        ))
    }
}

fn py_col_to_rust(py: Python<'_>, c: &PyColumnDef) -> PyResult<ColumnDef> {
    let name = c.name.bind(py).to_cow()?.into_owned();
    type_code_from_u64(c.type_code as u64)
        .map(|tc| {
            let cd = ColumnDef::typed(name, ColType { tc, scale: c.scale }, c.is_nullable);
            if c.is_hidden {
                cd.hidden()
            } else {
                cd
            }
        })
        .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))
}

fn rust_col_to_py(py: Python<'_>, c: &ColumnDef, primary_key: bool) -> PyResult<Py<PyAny>> {
    Ok(Py::new(
        py,
        PyColumnDef {
            name: PyString::intern(py, &c.name).unbind(),
            type_code: c.type_code as u32,
            is_nullable: c.is_nullable,
            primary_key,
            is_hidden: c.is_hidden,
            scale: c.scale,
        },
    )?
    .into_any())
}

/// A validated Rust `Schema`, and nothing else: `columns` is derived from it on
/// access, so there is no second representation that could drift.
///
/// The PK column indices are held in **sort order** — e.g. `pk_indices=[2, 1]`
/// sorts by col 2 first, then col 1. Order matters for seek/range semantics.
#[pyclass(name = "Schema", frozen)]
pub struct PySchema {
    rust: Arc<Schema>,
}

#[pymethods]
impl PySchema {
    #[new]
    #[pyo3(signature = (columns, pk_indices = None))]
    pub fn new(columns: Bound<'_, PyList>, pk_indices: Option<Vec<u32>>) -> PyResult<Self> {
        if columns.is_empty() {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Schema must have at least 1 column",
            ));
        }
        // One pass: convert each column and note which ones carry the PK flag,
        // so inferring the PK list needs no second walk of the list.
        let mut cols = Vec::with_capacity(columns.len());
        let mut flagged = Vec::new();
        for (i, item) in columns.iter().enumerate() {
            let c: PyRef<'_, PyColumnDef> = item.extract()?;
            if c.primary_key {
                flagged.push(i as u32);
            }
            cols.push(py_col_to_rust(columns.py(), &c)?);
        }
        // No explicit list: every flagged column, in declaration order. Nothing
        // flagged leaves the list empty, which `validate_pk_indices` rejects —
        // as `CREATE TABLE` rejects a PK-less table, so the two surfaces agree
        // on what a keyless relation is.
        let pk_cols = pk_indices.unwrap_or(flagged);
        // `Schema::from_parts` applies the shared rule set — the MAX_COLUMNS cap,
        // the structural PK rules, and per-PK-column nullability/eligibility.
        let rust = Schema::from_parts(cols, pk_cols).map_err(pyo3::exceptions::PyValueError::new_err)?;
        Ok(PySchema { rust: Arc::new(rust) })
    }

    #[getter]
    pub fn columns<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        rust_columns_to_py(py, &self.rust)
    }

    #[getter]
    pub fn pk_indices(&self) -> Vec<u32> {
        self.rust.pk_cols.to_vec()
    }

    pub fn __repr__(&self) -> String {
        format!(
            "Schema(pk_indices={:?}, ncols={})",
            self.rust.pk_cols,
            self.rust.columns.len()
        )
    }
}

/// Resolve a Python "schema-ish" argument to the Rust `Schema` behind it: a
/// `Schema`, or a list of `ColumnDef` built into one. The single adapter for
/// every method that accepts either form, applied at the parameter through
/// `#[pyo3(from_py_with = resolve_py_schema)]` so no body restates it.
pub(crate) fn resolve_py_schema(obj: &Bound<'_, PyAny>) -> PyResult<Arc<Schema>> {
    // Bare `Schema` first: it is the common case.
    if let Ok(s) = obj.cast::<PySchema>() {
        return Ok(Arc::clone(&s.get().rust));
    }
    // A sequence of ColumnDef → build one (the PK rules are PySchema's). The
    // explicit `TypeError` names the two forms actually accepted, where the raw
    // cast failure would name `PyList`.
    let Ok(list) = obj.cast::<PyList>() else {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "expected a Schema or a list of ColumnDef",
        ));
    };
    Ok(PySchema::new(list.clone(), None)?.rust)
}

/// The schema's columns as a fresh Python list of `ColumnDef`, each flagged
/// with whether it is a PK column. Derived from the Rust `Schema` on every
/// access — it is the only representation, so `Schema.columns[i].primary_key`
/// always reflects the PK list the schema actually validated.
fn rust_columns_to_py<'py>(py: Python<'py>, s: &Schema) -> PyResult<Bound<'py, PyList>> {
    build_pylist(
        py,
        s.columns
            .iter()
            .enumerate()
            .map(|(i, c)| rust_col_to_py(py, c, s.is_pk_col(i))),
    )
}

pub(crate) fn rust_schema_to_py(py: Python<'_>, s: &Arc<Schema>) -> PyResult<Py<PySchema>> {
    Py::new(py, PySchema { rust: Arc::clone(s) })
}
