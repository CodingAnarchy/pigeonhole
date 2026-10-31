//! Python bindings for [Pigeonhole](https://github.com/CodingAnarchy/pigeonhole) (D307), over
//! PyO3, published to PyPI as `pigeonhole-db` and imported as `pigeonhole`.
//!
//! The extension module is `pigeonhole._native`; the `pigeonhole` package re-exports it and
//! carries the typed stubs (`__init__.pyi`), which are the Python API's snapshot (ICR 0102,
//! checked by `mypy.stubtest`). Every engine call runs with the GIL released, its arguments
//! converted before and its results after.
#![forbid(unsafe_code)]

use pyo3::prelude::*;

mod convert;
mod db;
mod error;
mod read;
mod table;

/// The native module.
#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(db::open, m)?)?;
    m.add_function(wrap_pyfunction!(db::open_reader, m)?)?;
    m.add_class::<db::Database>()?;
    m.add_class::<db::Reader>()?;
    m.add_class::<db::WriteBatch>()?;
    m.add_class::<db::Transaction>()?;
    m.add_class::<table::Table>()?;
    m.add_class::<table::TableBuilder>()?;
    m.add_class::<table::Family>()?;
    m.add_class::<table::Condition>()?;
    m.add_class::<table::Durability>()?;
    m.add_class::<table::Mutation>()?;
    m.add_class::<read::Cell>()?;
    m.add_class::<read::Row>()?;
    m.add_class::<read::Snapshot>()?;
    m.add_class::<read::ScanIter>()?;
    error::register(m)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
