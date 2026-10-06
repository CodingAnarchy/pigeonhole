//! A graph as adjacency lists: one row per node, one column per edge (qualifier = the other
//! node) in an `out` and an `in` family. Edges are written in both directions atomically
//! with a write batch; neighbors are one row read, edge tests one point get, and the whole
//! graph one scan through the zero-copy cursor.
//!
//! Run with `cargo run -p pigeonhole --example adjacency_scan`.

use std::path::Path;

use pigeonhole::{Family, Options, Pigeonhole, Table};

/// Runs the example in a temporary directory, removed afterwards.
pub fn main() -> pigeonhole::Result<()> {
    let dir = std::env::temp_dir().join(format!(
        "pigeonhole-example-adjacency-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create a temporary directory");
    let result = run(&dir.join("graph.phdb"));
    let _ = std::fs::remove_dir_all(&dir);
    result
}

fn node(name: &str) -> Vec<u8> {
    format!("node:{name}").into_bytes()
}

fn add_edge(db: &Pigeonhole, g: &Table, from: &str, to: &str) -> pigeonhole::Result<()> {
    // Both directions in one atomic batch, even if the rows live on different shards.
    let mut wb = db.write_batch();
    wb.put(g, &node(from), "out", &node(to), b"")
        .put(g, &node(to), "in", &node(from), b"");
    wb.commit()?;
    Ok(())
}

fn run(path: &Path) -> pigeonhole::Result<()> {
    let db = Pigeonhole::open(path, Options::default())?;
    let g = db
        .table("graph")?
        .family("out", Family::default().bloom_bits(10).max_versions(1))
        .family("in", Family::default().bloom_bits(10).max_versions(1))
        .create_if_missing()?;

    for (from, to) in [
        ("a", "b"),
        ("a", "c"),
        ("a", "d"),
        ("b", "c"),
        ("c", "a"),
        ("d", "b"),
    ] {
        add_edge(&db, &g, from, to)?;
    }

    // Out-neighbors of a: one row read, projected to one family.
    let row = g.row(&node("a")).family("out").read()?.expect("node a");
    let out: Vec<String> = row
        .iter()
        .map(|e| String::from_utf8_lossy(e.qualifier).into_owned())
        .collect();
    println!("a -> {out:?}");
    assert_eq!(out, ["node:b", "node:c", "node:d"]);

    // Is there an edge c -> a? One point get.
    assert!(g.get(&node("c"), "out", &node("a"))?.is_some());
    assert!(g.get(&node("b"), "out", &node("a"))?.is_none());

    // Remove an edge (both directions), then page through every node's first two
    // in-neighbors with the zero-copy cursor.
    let mut wb = db.write_batch();
    wb.delete_column(&g, &node("a"), "out", &node("d"))
        .delete_column(&g, &node("d"), "in", &node("a"));
    wb.commit()?;

    let mut it = g
        .scan_prefix(b"node:")
        .family("in")
        .columns_per_row(2)
        .iter()?;
    let mut total = 0;
    while let Some(row) = it.next_ref()? {
        let preds: Vec<&[u8]> = row.iter().map(|e| e.qualifier).collect();
        println!(
            "{} <- {:?}",
            String::from_utf8_lossy(row.key()),
            preds
                .iter()
                .map(|p| String::from_utf8_lossy(p))
                .collect::<Vec<_>>()
        );
        total += preds.len();
    }
    // a<-c, b<-{a,d}, c<-{a,b}; d lost its only in-edge.
    assert_eq!(total, 5);
    db.close()
}
