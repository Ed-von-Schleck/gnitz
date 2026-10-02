//! The schema surface: the `ColumnDef` and `Schema` pyclasses, and the
//! conversions between them and `gnitz_core`'s `Schema`.
//!
//! Holds no codec: turning a *value* into wire bytes or back is `write`'s and
//! `read`'s, and this module only ever describes the columns those two address.

use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyString;

use gnitz_core::Schema;
use gnitz_wire::{ColType, ColumnDef};

/// One column: a name, a validated type, and its nullability and hidden flags.
/// A column carries no PK flag — the PK is the schema's ordered `pk_indices`.
///
/// `name` is held as an interned `PyString`, not a `String`: a `#[pyclass]`
/// getter over a Rust `String` builds a fresh `PyString` on *every* read, so
/// `c.name` would hand back a different object each time. Interned, every read
/// returns the identical object — the one a dict keyed by that name, or a
/// `**kwargs` splat built from it, hits on by pointer.
#[pyclass(name = "ColumnDef", frozen)]
pub struct PyColumnDef {
    #[pyo3(get)]
    name: Py<PyString>,
    ty: ColType,
    #[pyo3(get)]
    is_nullable: bool,
    #[pyo3(get)]
    is_hidden: bool,
}

#[pymethods]
impl PyColumnDef {
    /// An unknown type code, or a scale on a type that takes none, is refused
    /// here rather than when a `Schema` is built from the column.
    #[new]
    #[pyo3(signature = (name, type_code, is_nullable = false, is_hidden = false, scale = 0))]
    pub fn new(
        py: Python<'_>,
        name: &str,
        type_code: u8,
        is_nullable: bool,
        is_hidden: bool,
        scale: u8,
    ) -> PyResult<Self> {
        let ty = ColType::from_wire(type_code, scale).ok_or_else(|| {
            PyValueError::new_err(format!("unknown type code {type_code} or inadmissible scale {scale}"))
        })?;
        Ok(PyColumnDef {
            name: PyString::intern(py, name).unbind(),
            ty,
            is_nullable,
            is_hidden,
        })
    }

    #[getter]
    pub fn type_code(&self) -> u8 {
        self.ty.tc.as_wire()
    }

    /// A DECIMAL column's scale; 0 for every other type.
    #[getter]
    pub fn scale(&self) -> u8 {
        self.ty.scale
    }

    pub fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "ColumnDef(name={:?}, type_code={}, is_nullable={}, is_hidden={}, scale={})",
            self.name.bind(py).to_cow()?,
            self.ty.tc.as_wire(),
            self.is_nullable,
            self.is_hidden,
            self.ty.scale
        ))
    }
}

impl PyColumnDef {
    fn from_rust(py: Python<'_>, c: &ColumnDef) -> Self {
        PyColumnDef {
            name: PyString::intern(py, &c.name).unbind(),
            ty: c.ty,
            is_nullable: c.is_nullable,
            is_hidden: c.is_hidden,
        }
    }

    fn to_rust(&self, py: Python<'_>) -> PyResult<ColumnDef> {
        let name = self.name.bind(py).to_cow()?.into_owned();
        Ok(ColumnDef {
            is_hidden: self.is_hidden,
            ..ColumnDef::typed(name, self.ty, self.is_nullable)
        })
    }
}

/// A validated Rust `Schema`, and nothing else: `columns` is derived from it on
/// access, so there is no second representation that could drift.
///
/// The PK is `pk_indices`, held in **sort order** — e.g. `pk_indices=[2, 1]`
/// sorts by col 2 first, then col 1. Order matters for seek/range semantics.
#[pyclass(name = "Schema", frozen, from_py_object)]
#[derive(Clone)]
pub struct PySchema {
    pub(crate) rust: Arc<Schema>,
}

#[pymethods]
impl PySchema {
    /// `Schema::from_parts` applies the shared rule set — the MAX_COLUMNS cap,
    /// the structural PK rules, and per-PK-column nullability/eligibility.
    #[new]
    pub fn new(py: Python<'_>, columns: Vec<PyRef<'_, PyColumnDef>>, pk_indices: Vec<u32>) -> PyResult<Self> {
        let cols = columns.iter().map(|c| c.to_rust(py)).collect::<PyResult<_>>()?;
        let rust = Schema::from_parts(cols, pk_indices).map_err(PyValueError::new_err)?;
        Ok(PySchema { rust: Arc::new(rust) })
    }

    #[getter]
    pub fn columns(&self, py: Python<'_>) -> Vec<PyColumnDef> {
        self.rust
            .columns
            .iter()
            .map(|c| PyColumnDef::from_rust(py, c))
            .collect()
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

/// sys_schema(table_id) -> Schema: the schema of system table `table_id`, the
/// layout a read of it must name.
#[pyfunction]
pub(crate) fn sys_schema(table_id: u64) -> PyResult<PySchema> {
    if gnitz_wire::sys_family_index(table_id).is_none() {
        return Err(PyValueError::new_err(format!("{table_id} is not a system table id")));
    }
    Ok(PySchema {
        rust: Arc::clone(gnitz_core::sys_schema(table_id)),
    })
}
