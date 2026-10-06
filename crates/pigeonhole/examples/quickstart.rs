//! Open a database, create a table, write a row atomically, and read it back as a cell, a
//! row and a scan.
//!
//! Run with `cargo run -p pigeonhole --example quickstart`.

use std::path::Path;

use pigeonhole::{Durability, Family, Options, Pigeonhole};

/// Runs the example in a temporary directory, removed afterwards.
pub fn main() -> pigeonhole::Result<()> {
    let dir = std::env::temp_dir().join(format!(
        "pigeonhole-example-quickstart-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).expect("create a temporary directory");
    let result = run(&dir.join("quickstart.phdb"));
    let _ = std::fs::remove_dir_all(&dir);
    result
}

fn run(path: &Path) -> pigeonhole::Result<()> {
    let db = Pigeonhole::open(path, Options::default())?;
    let users = db
        .table("users")?
        .family("profile", Family::default().max_versions(1))
        .family("stats", Family::default())
        .create_if_missing()?;

    // One row, two families, all or nothing.
    let info = users
        .mutate(b"user:42")
        .put("profile", b"name", b"Ada")
        .put("profile", b"email", b"ada@example.com")
        .incr("stats", b"logins", 1)
        .commit()?;
    println!("committed seqno {} at {:?}", info.seqno, info.durability);

    // A point read borrows the value; `to_owned` keeps it past the borrow.
    if let Some(name) = users.get(b"user:42", "profile", b"name")? {
        println!("name = {}", String::from_utf8_lossy(name.value()));
    }

    // Several rows in one atomic batch, with a durability override.
    let mut wb = db.write_batch();
    for (id, name) in [(&b"user:7"[..], &b"Grace"[..]), (b"user:9", b"Linus")] {
        wb.put(&users, id, "profile", b"name", name);
    }
    wb.commit_with(Durability::Buffered)?;

    // A whole row, projected to one family.
    let row = users.row(b"user:42").family("profile").read()?;
    if let Some(row) = row {
        for e in row.iter() {
            println!(
                "user:42 {}:{} = {}",
                e.family,
                String::from_utf8_lossy(e.qualifier),
                String::from_utf8_lossy(e.cell.value())
            );
        }
    }

    // Every user, in key order.
    for row in users.scan_prefix(b"user:").family("profile").iter()? {
        let row = row?;
        let name = row.get("profile", b"name").map(|c| c.value().to_vec());
        println!(
            "{} -> {}",
            String::from_utf8_lossy(row.key()),
            String::from_utf8_lossy(&name.unwrap_or_default())
        );
    }

    let logins = users
        .get(b"user:42", "stats", b"logins")?
        .and_then(|c| c.as_i64());
    assert_eq!(logins, Some(1));
    db.close()
}
