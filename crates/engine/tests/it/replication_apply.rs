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

/// A replica's state table is no feed's at the engine layer too (simp724's S2 on #1562):
/// naming its id in a feed is refused.
#[test]
fn a_feed_naming_the_replica_state_table_is_refused() {
    let vfs = SimVfs::new(5103);
    let db = open(&vfs, PRIMARY);
    let e = db
        .feed(FeedSpec {
            tables: vec![pigeonhole_format::manifest::REPLICA_TABLE],
            prefix: Vec::new(),
            durable: true,
            start: FeedStart::Now,
        })
        .expect_err("refused");
    assert!(matches!(e, Error::InvalidArgument(_)), "{e:?}");
    db.close().unwrap();
}

/// Sets `ro_compat` in both superblock pages of the closed database at `path`, as a crash
/// after a promoted replica's drop and before its bit's clearing leaves it.
fn set_ro_compat(vfs: &SimVfs, path: &str, bits: u64) {
    use pigeonhole_format::superblock::{SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B, Superblock};
    use pigeonhole_io::{OpenOptions, Vfs};
    const PAGE: u64 = 4096;
    let mut opts = OpenOptions::read();
    opts.write = true;
    let f = vfs.open(Path::new(path), opts).unwrap();
    for p in [SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B] {
        let mut buf = [0u8; PAGE as usize];
        f.read_at(&mut buf, p * PAGE).unwrap();
        let mut sb = Superblock::decode(&buf).unwrap();
        sb.features.ro_compat = bits;
        sb.encode(&mut buf);
        f.write_at(&buf, p * PAGE).unwrap();
    }
    f.sync_all().unwrap();
}

/// A promoted replica's bit left behind by a crash between its state table's drop and the
/// bit's clearing (#1436, ICR 0125; simp724's note): a writable open clears it; a read-only
/// one leaves it as it is.
#[test]
fn an_open_clears_a_replica_bit_left_without_its_state_table() {
    let vfs = SimVfs::new(5104);
    let db = open(&vfs, PRIMARY);
    db.close().unwrap();
    drop(db);
    set_ro_compat(
        &vfs,
        PRIMARY,
        pigeonhole_format::manifest::RO_COMPAT_REPLICA,
    );
    let mut o = common::options(Arc::clone(&vfs), 2, 16 << 20);
    o.read_only = true;
    let ro = Engine::open(Path::new(PRIMARY), o).unwrap();
    assert_eq!(
        ro.info().unwrap().features.ro_compat,
        1,
        "a read-only open writes nothing"
    );
    ro.close().unwrap();
    drop(ro);
    // Feed off: an open that commits nothing else, so the repair alone clears it.
    let db = Engine::open(
        Path::new(PRIMARY),
        common::options(Arc::clone(&vfs), 2, 16 << 20),
    )
    .unwrap();
    assert_eq!(db.info().unwrap().features.ro_compat, 0, "repaired");
    db.close().unwrap();
}

/// simp724's P1 on #1436: a replica's state write that raced `promote` (its batch built
/// before the promote's drop, committed after) fails, and nothing brings the state table,
/// the bit or the write refusal back.
#[test]
fn a_state_write_racing_promote_fails_and_brings_nothing_back() {
    use pigeonhole_format::manifest::{REPLICA_FAMILY, REPLICA_TABLE};
    let vfs = SimVfs::new(5105);
    let db = open(&vfs, REPLICA);
    db.begin_replication(b"state", false).unwrap();
    assert_eq!(db.info().unwrap().features.ro_compat, 1);
    // What an apply had built when the promote ran: its state cell's put.
    let mut raced = WriteBatch::new();
    raced
        .push_logged(
            REPLICA_TABLE,
            REPLICA_FAMILY,
            pigeonhole_format::Kind::Put,
            b"replication",
            b"state",
            1,
            &[0, b'x'],
        )
        .unwrap();
    db.promote().unwrap();
    assert!(
        db.commit(raced, None).is_err(),
        "the raced state write fails"
    );
    assert!(matches!(
        db.store_replication_state(b"later"),
        Err(Error::InvalidArgument(_))
    ));
    assert!(matches!(
        db.begin_replication(b"again", true),
        Err(Error::InvalidArgument(_))
    ));
    assert_eq!(db.info().unwrap().features.ro_compat, 0);
    assert_eq!(db.replication_state().unwrap(), None);
    // Local writes are taken.
    let t = db
        .create_table("local", &[("f".into(), FamilyOptions::default())])
        .unwrap();
    let mut wb = WriteBatch::new();
    wb.put(
        t.id,
        t.families[0].id,
        b"r",
        b"q",
        None,
        pigeonhole_engine::ValueRef::Bytes(b"v"),
    )
    .unwrap();
    db.commit(wb, None).unwrap();
    db.close().unwrap();
}
