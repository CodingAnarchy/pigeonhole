//! Replication apply (D315 PR 3): a replica applies a primary's catalog changes with the
//! primary's ids, so the tables and families it mirrors keep them, and gives the tables it
//! creates tablets of its own.

use crate::common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use pigeonhole_engine::{
    CatalogEvent, Engine, Error, FamilyOptions, FeedOptions, FeedReader, FeedSpec, FeedStart, Next,
    ValueRef, WriteBatch,
};
use pigeonhole_format::manifest::Edit;
use pigeonhole_format::{Durability, FamilyId, TableId};
use pigeonhole_io::sim::SimVfs;

const PRIMARY: &str = "/db/primary.phdb";
const REPLICA: &str = "/db/replica.phdb";
const WAIT: Duration = Duration::from_secs(10);

fn open(vfs: &Arc<SimVfs>, path: &str) -> Arc<Engine> {
    let mut o = common::options(Arc::clone(vfs), 2, 16 << 20);
    let mut f = FeedOptions::default();
    f.retention_bytes = 1 << 20;
    o.feed = Some(f);
    Engine::open(Path::new(path), o).unwrap()
}

fn every_table(db: &Engine) -> FeedReader {
    db.feed(FeedSpec {
        tables: Vec::new(),
        prefix: Vec::new(),
        durable: true,
        start: FeedStart::Now,
    })
    .unwrap()
}

fn next_catalog(r: &mut FeedReader) -> CatalogEvent {
    loop {
        match r.wait(WAIT).unwrap() {
            Next::Catalog(ev) => return ev.clone(),
            Next::Commit(_) => {}
            other => panic!("{other:?}"),
        }
    }
}

/// A replica applies a primary's catalog changes with the primary's ids: a created table and
/// its families, a family added, a table dropped. The replica's tables carry those ids, the
/// table it created has a tablet of its own (it takes writes), and the replica's own next
/// table and family ids are past every applied one.
#[test]
fn a_replica_applies_catalog_changes_with_the_primary_s_ids() {
    let vfs = SimVfs::new(5101);
    let primary = open(&vfs, PRIMARY);
    let replica = open(&vfs, REPLICA);
    let mut r = every_table(&primary);
    // Burn some of the primary's ids, so they differ from the replica's own sequence.
    for i in 0..3 {
        let t = primary
            .create_table(
                &format!("scratch{i}"),
                &[("f".into(), FamilyOptions::default())],
            )
            .unwrap();
        next_catalog(&mut r);
        primary.drop_table(t.id).unwrap();
        next_catalog(&mut r);
    }
    let orders = primary
        .create_table(
            "orders",
            &[
                ("f".into(), FamilyOptions::default()),
                ("g".into(), FamilyOptions::default()),
            ],
        )
        .unwrap();
    let ev = next_catalog(&mut r);
    replica.apply_catalog(&ev.edits).unwrap();
    let mirrored = replica.table("orders").expect("the replicated table");
    assert_eq!(mirrored.id, orders.id, "the primary's table id");
    let ids = |t: &pigeonhole_engine::TableInfo| -> Vec<(String, FamilyId)> {
        t.families.iter().map(|f| (f.name.clone(), f.id)).collect()
    };
    assert_eq!(ids(&mirrored), ids(&orders), "the primary's family ids");

    let orders = primary
        .add_family(orders.id, "h", FamilyOptions::default())
        .unwrap();
    let ev = next_catalog(&mut r);
    replica.apply_catalog(&ev.edits).unwrap();
    assert_eq!(ids(&replica.table("orders").unwrap()), ids(&orders));

    // Its own tablet: the replica takes a write to the table.
    let f = orders.families[0].id;
    let mut wb = WriteBatch::new();
    wb.put(orders.id, f, b"row", b"q", Some(7), ValueRef::Bytes(b"v"))
        .unwrap();
    replica.commit(wb, Some(Durability::GroupSync)).unwrap();

    // The replica's own allocations are past every applied id.
    let local = replica
        .create_table("local", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    assert!(local.id.0 > orders.id.0, "{local:?} vs {orders:?}");
    assert!(local.families[0].id.0 > orders.families.iter().map(|f| f.id.0).max().unwrap());

    primary.drop_table(orders.id).unwrap();
    let ev = next_catalog(&mut r);
    replica.apply_catalog(&ev.edits).unwrap();
    assert!(replica.table("orders").is_none(), "dropped");
    drop(r);
    primary.close().unwrap();
    replica.close().unwrap();
}

/// The changes a replica refuses: a reserved id (the replica's own state, R1), an id or name
/// in use, a family whose table is missing, and edits that mix a drop with anything else.
#[test]
fn a_replica_refuses_catalog_changes_it_cannot_apply_as_given() {
    let vfs = SimVfs::new(5102);
    let replica = open(&vfs, REPLICA);
    let create = |id: u32, name: &str, family: u32| {
        vec![
            Edit::CreateTable {
                table: TableId(id),
                name: name.into(),
            },
            Edit::PutFamily {
                table: TableId(id),
                family: FamilyId(family),
                name: "f".into(),
                options: FamilyOptions::default(),
            },
        ]
    };
    let invalid = |r: pigeonhole_engine::Result<()>| {
        assert!(matches!(r, Err(Error::InvalidArgument(_))), "{r:?}")
    };
    invalid(replica.apply_catalog(&create(u32::MAX, "state", 40)));
    invalid(replica.apply_catalog(&create(40, "t", u32::MAX)));
    replica.apply_catalog(&create(40, "t", 41)).unwrap();
    invalid(replica.apply_catalog(&create(40, "u", 42)));
    assert!(matches!(
        replica.apply_catalog(&create(43, "t", 44)),
        Err(Error::TableExists(_))
    ));
    invalid(replica.apply_catalog(&create(45, "v", 41)));
    assert!(matches!(
        replica.apply_catalog(&[Edit::PutFamily {
            table: TableId(99),
            family: FamilyId(100),
            name: "g".into(),
            options: FamilyOptions::default(),
        }]),
        Err(Error::TableNotFound(_))
    ));
    let mut mixed = create(46, "w", 47);
    mixed.push(Edit::DropTable { table: TableId(40) });
    invalid(replica.apply_catalog(&mixed));
    invalid(replica.apply_catalog(&[]));
    replica.close().unwrap();
}
