//! The 1.1 reads: multi-get, traversal, and walking a wide row in pages from a position.

use pigeonhole::KeySet;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::convert;
use crate::error::OrPy;
use crate::read::{Row, Snapshot, snapshot_arg};
use crate::table::Handle;

/// Runs `$body` with `$t` bound to either kind of table.
macro_rules! either {
    ($h:expr, |$t:ident| $body:expr) => {
        match $h {
            Handle::Writer($t) => $body,
            Handle::Reader($t) => $body,
        }
    };
}

/// The rows of `keys` (D299), in key order, with the read's selection; a key whose row has
/// no cell the selection keeps is left out.
#[allow(clippy::too_many_arguments)]
pub(crate) fn multi_get(
    py: Python<'_>,
    h: &Handle,
    keys: &[Bound<'_, PyAny>],
    families: Option<Vec<String>>,
    qualifier_prefix: Option<&Bound<'_, PyAny>>,
    versions: u32,
    column_limit: Option<u32>,
    snapshot: Option<&Bound<'_, Snapshot>>,
) -> PyResult<Vec<Row>> {
    let keys = KeySet::from_unsorted(
        keys.iter()
            .map(convert::bytes)
            .collect::<PyResult<Vec<_>>>()?,
    );
    let qp = qualifier_prefix.map(convert::bytes).transpose()?;
    let snap = snapshot_arg(snapshot)?;
    let h = h.clone();
    py.detach(move || {
        either!(&h, |t| {
            let mut r = t.keys(&keys).versions(versions);
            if let Some(f) = &families {
                r = r.families(f.iter().map(String::as_str));
            }
            if let Some(q) = &qp {
                r = r.qualifier_prefix(q);
            }
            if let Some(n) = column_limit {
                r = r.column_limit(n);
            }
            if let Some(s) = &snap {
                r = r.snapshot(s);
            }
            let mut rows = r.read()?;
            let mut out = Vec::new();
            while let Some((_, row)) = rows.next()? {
                out.push(row.to_owned());
            }
            Ok(out)
        })
    })
    .or_py()
    .map(|rows| rows.into_iter().map(Row).collect())
}

/// One hop of a traversal: its new nodes, sorted, and with `parents=True` each node's parent
/// as an index into the previous hop's nodes (or the traversal's `seeds`, for hop 1).
#[pyclass(module = "pigeonhole", frozen)]
pub(crate) struct Hop {
    nodes: Vec<Vec<u8>>,
    parents: Option<Vec<u32>>,
}

#[pymethods]
impl Hop {
    /// The hop's new nodes, sorted.
    #[getter]
    fn nodes<'py>(&self, py: Python<'py>) -> Vec<Bound<'py, PyBytes>> {
        self.nodes.iter().map(|n| PyBytes::new(py, n)).collect()
    }

    /// Each node's parent index, with `parents=True`.
    #[getter]
    fn parents(&self) -> Option<Vec<u32>> {
        self.parents.clone()
    }

    fn __len__(&self) -> usize {
        self.nodes.len()
    }

    fn __repr__(&self) -> String {
        format!("Hop(nodes={})", self.nodes.len())
    }
}

/// A traversal's result: its hops in order, the seeds as the engine sorted them (what hop
/// 1's parents index), and its counts.
#[pyclass(module = "pigeonhole", frozen)]
pub(crate) struct Traversal {
    seeds: Vec<Vec<u8>>,
    hops: Vec<Py<Hop>>,
    rows: u64,
    edges: u64,
    hops_cut: u32,
    cut_by_max_nodes: bool,
}

#[pymethods]
impl Traversal {
    /// The hops, hop 1 first.
    #[getter]
    fn hops(&self, py: Python<'_>) -> Vec<Py<Hop>> {
        self.hops.iter().map(|h| h.clone_ref(py)).collect()
    }

    /// The seeds, sorted and deduplicated: hop 1's parents index into them.
    #[getter]
    fn seeds<'py>(&self, py: Python<'py>) -> Vec<Bound<'py, PyBytes>> {
        self.seeds.iter().map(|n| PyBytes::new(py, n)).collect()
    }

    /// Frontier rows with an edge the selection keeps.
    #[getter]
    fn rows(&self) -> u64 {
        self.rows
    }

    /// Edges read.
    #[getter]
    fn edges(&self) -> u64 {
        self.edges
    }

    /// Hops that `hop_limit` or `max_nodes` cut short.
    #[getter]
    fn hops_cut(&self) -> u32 {
        self.hops_cut
    }

    /// Whether `max_nodes` was reached with hops left to take.
    #[getter]
    fn cut_by_max_nodes(&self) -> bool {
        self.cut_by_max_nodes
    }

    fn __repr__(&self) -> String {
        format!("Traversal(hops={})", self.hops.len())
    }
}

/// A breadth-first traversal (D304) over `family` from `seeds`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn traverse(
    py: Python<'_>,
    h: &Handle,
    seeds: &[Bound<'_, PyAny>],
    family: String,
    hops: u32,
    per_node_limit: Option<u32>,
    hop_limit: Option<u64>,
    max_nodes: Option<u64>,
    parents: bool,
    qualifier_prefix: Option<&Bound<'_, PyAny>>,
    snapshot: Option<&Bound<'_, Snapshot>>,
    parallel: Option<usize>,
) -> PyResult<Traversal> {
    let seeds = KeySet::from_unsorted(
        seeds
            .iter()
            .map(convert::bytes)
            .collect::<PyResult<Vec<_>>>()?,
    );
    let qp = qualifier_prefix.map(convert::bytes).transpose()?;
    let snap = snapshot_arg(snapshot)?;
    let h = h.clone();
    let (sorted, t) = py
        .detach(move || {
            let t = either!(&h, |t| {
                let mut r = t
                    .traverse(&seeds)
                    .family(&family)
                    .hops(hops)
                    .parents(parents);
                if let Some(n) = per_node_limit {
                    r = r.per_node_limit(n);
                }
                if let Some(n) = hop_limit {
                    r = r.hop_limit(n);
                }
                if let Some(n) = max_nodes {
                    r = r.max_nodes(n);
                }
                if let Some(q) = &qp {
                    r = r.qualifier_prefix(q);
                }
                if let Some(s) = &snap {
                    r = r.snapshot(s);
                }
                if let Some(n) = parallel {
                    r = r.parallel(n);
                }
                r.run()
            })?;
            Ok::<_, pigeonhole::Error>((seeds.iter().map(<[u8]>::to_vec).collect::<Vec<_>>(), t))
        })
        .or_py()?;
    let stats = t.stats();
    let hops = t
        .hops()
        .iter()
        .map(|hop| {
            Py::new(
                py,
                Hop {
                    nodes: hop.nodes().iter().map(<[u8]>::to_vec).collect(),
                    parents: hop.parents().map(<[u32]>::to_vec),
                },
            )
        })
        .collect::<PyResult<_>>()?;
    Ok(Traversal {
        seeds: sorted,
        hops,
        rows: stats.rows,
        edges: stats.edges,
        hops_cut: stats.hops_cut,
        cut_by_max_nodes: stats.cut_by_max_nodes,
    })
}

/// Where a walk of a row stopped: the column of the last cell of the last page returned. Its
/// bytes (`to_bytes`) are stable, so it can be handed to a client and resumed later; it
/// names no row.
#[pyclass(module = "pigeonhole", frozen)]
#[derive(Clone)]
pub(crate) struct RowPosition(pub(crate) pigeonhole::RowPosition);

#[pymethods]
impl RowPosition {
    /// The position as bytes.
    fn to_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.0.to_bytes())
    }

    /// The position `to_bytes` wrote.
    #[staticmethod]
    fn from_bytes(b: &Bound<'_, PyAny>) -> PyResult<Self> {
        pigeonhole::RowPosition::from_bytes(&convert::bytes(b)?)
            .or_py()
            .map(Self)
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        other
            .cast::<RowPosition>()
            .is_ok_and(|o| o.get().0.to_bytes() == self.0.to_bytes())
    }
}

/// A wide row's pages (D297), each at most `size` cells (all of a column's versions on one
/// page), each read when it is asked for: the walk holds no read open between pages.
/// `position` is where the last page ended, to resume from in a later walk. Without a
/// snapshot each page reads the latest data.
#[pyclass(module = "pigeonhole")]
pub(crate) struct Pages {
    h: Handle,
    row: Vec<u8>,
    size: usize,
    families: Option<Vec<String>>,
    qualifier_prefix: Option<Vec<u8>>,
    versions: u32,
    snapshot: Option<pigeonhole::Snapshot>,
    position: Option<pigeonhole::RowPosition>,
    done: bool,
}

impl Pages {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        h: &Handle,
        row: &Bound<'_, PyAny>,
        size: usize,
        families: Option<Vec<String>>,
        qualifier_prefix: Option<&Bound<'_, PyAny>>,
        versions: u32,
        snapshot: Option<&Bound<'_, Snapshot>>,
        after: Option<&RowPosition>,
    ) -> PyResult<Self> {
        if size == 0 {
            return Err(PyValueError::new_err("size must be at least 1"));
        }
        Ok(Self {
            h: h.clone(),
            row: convert::bytes(row)?,
            size,
            families,
            qualifier_prefix: qualifier_prefix.map(convert::bytes).transpose()?,
            versions,
            snapshot: snapshot_arg(snapshot)?,
            position: after.map(|p| p.0.clone()),
            done: false,
        })
    }
}

#[pymethods]
impl Pages {
    /// Where the last page ended (`None` before the first page, unless the walk started
    /// from one).
    #[getter]
    fn position(&self) -> Option<RowPosition> {
        self.position.clone().map(RowPosition)
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Row>> {
        if self.done {
            return Ok(None);
        }
        let (h, row, size, versions) = (self.h.clone(), self.row.clone(), self.size, self.versions);
        let (families, qp, snap, pos) = (
            self.families.clone(),
            self.qualifier_prefix.clone(),
            self.snapshot.clone(),
            self.position.clone(),
        );
        let page = py
            .detach(move || {
                either!(&h, |t| {
                    let mut r = t.row(&row).versions(versions);
                    if let Some(f) = &families {
                        r = r.families(f.iter().map(String::as_str));
                    }
                    if let Some(q) = &qp {
                        r = r.qualifier_prefix(q);
                    }
                    if let Some(s) = &snap {
                        r = r.snapshot(s);
                    }
                    if let Some(p) = &pos {
                        r = r.resume_after(p);
                    }
                    let mut pages = r.pages(size)?;
                    let page = pages.next()?.map(|p| p.to_owned());
                    Ok::<_, pigeonhole::Error>(page.map(|p| (p, pages.position())))
                })
            })
            .or_py()?;
        match page {
            Some((p, position)) => {
                self.position = position;
                Ok(Some(Row(p)))
            }
            None => {
                self.done = true;
                Ok(None)
            }
        }
    }
}
