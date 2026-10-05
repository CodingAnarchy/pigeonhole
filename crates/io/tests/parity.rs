//! The backend parity suite: every test body here runs against both `PreadVfs` (real files)
//! and `SimVfs` (in memory), and must behave identically.

mod common;

use std::sync::atomic::Ordering;

use common::{Backend, block_on, same_name, unique};
use pigeonhole_io::{ErrorKind, IoBuf, LockMode, OpenOptions, SharedOpen};

macro_rules! parity {
    ($($name:ident),* $(,)?) => {
        #[cfg(not(miri))]
        mod pread {
            $(#[test]
            fn $name() {
                super::$name(&super::Backend::pread(stringify!($name)));
            })*
        }
        mod sim {
            $(#[test]
            fn $name() {
                super::$name(&super::Backend::sim(0x5eed));
            })*
        }
    };
}

parity!(
    open_modes,
    read_write_round_trip,
    lengths_and_allocation,
    submitted_io,
    namespace_operations,
    syncs,
    read_only_handle_rejects_writes,
    exclusive_conflicts,
    shared_locks_coexist,
    upgrade_and_downgrade,
    closing_a_handle_releases_its_locks,
    exclusive_lock_needs_a_writable_handle,
    identity_and_locality,
    shared_memory_in_a_directory,
    clocks_and_processes,
);

fn open_modes(b: &Backend) {
    let path = b.path("f");
    let err = b.vfs.open(&path, OpenOptions::read()).unwrap_err();
    assert_eq!(err.kind, ErrorKind::NotFound, "{}", b.name);

    let mut create_new = OpenOptions::read_write_create();
    create_new.create_new = true;
    drop(b.vfs.open(&path, create_new).unwrap());
    let err = b.vfs.open(&path, create_new).unwrap_err();
    assert_eq!(err.kind, ErrorKind::AlreadyExists, "{}", b.name);

    // Plain create opens an existing file without truncating it.
    b.vfs
        .open(&path, OpenOptions::read_write_create())
        .unwrap()
        .write_at(b"abc", 0)
        .unwrap();
    let f = b.vfs.open(&path, OpenOptions::read_write_create()).unwrap();
    assert_eq!(f.len().unwrap(), 3);
}

fn read_write_round_trip(b: &Backend) {
    let f = b.create("f");
    assert!(f.is_empty().unwrap());
    f.write_at(b"hello world", 0).unwrap();
    f.write_at(b"W", 6).unwrap();
    let mut buf = [0u8; 11];
    f.read_at(&mut buf, 0).unwrap();
    assert_eq!(&buf, b"hello World");

    // Writing past the end zero-fills the gap.
    f.write_at(b"!", 20).unwrap();
    assert_eq!(f.len().unwrap(), 21);
    let mut gap = [0xFFu8; 9];
    f.read_at(&mut gap, 11).unwrap();
    assert_eq!(gap, [0; 9]);

    // A read past the end fails rather than returning short.
    let mut over = [0u8; 4];
    assert_eq!(
        f.read_at(&mut over, 19).unwrap_err().kind,
        ErrorKind::UnexpectedEof
    );
    f.read_at(&mut [], 100).unwrap();

    // Another handle sees the same bytes.
    let g = b.vfs.open(&b.path("f"), OpenOptions::read()).unwrap();
    let mut one = [0u8; 1];
    g.read_at(&mut one, 20).unwrap();
    assert_eq!(&one, b"!");
}

fn lengths_and_allocation(b: &Backend) {
    let f = b.create("f");
    f.write_at(&[7; 100], 0).unwrap();
    f.set_len(10).unwrap();
    assert_eq!(f.len().unwrap(), 10);
    f.set_len(50).unwrap();
    let mut buf = [0xFFu8; 40];
    f.read_at(&mut buf, 10).unwrap();
    assert_eq!(buf, [0; 40], "set_len extends with zeros");

    f.allocate(0, 4096).unwrap();
    assert_eq!(f.len().unwrap(), 4096);
    let mut head = [0u8; 10];
    f.read_at(&mut head, 0).unwrap();
    assert_eq!(head, [7; 10], "allocate keeps existing data");
    f.allocate(0, 100).unwrap();
    assert_eq!(f.len().unwrap(), 4096, "allocate never shrinks");
    f.allocate(8192, 100).unwrap();
    assert_eq!(f.len().unwrap(), 8292);
}

fn submitted_io(b: &Backend) {
    let f = b.create("f");
    let mut buf = IoBuf::zeroed(4096);
    buf.fill(0x5A);
    let buf = f.submit_write(buf, 4096).wait().unwrap();
    assert_eq!(buf.len(), 4096, "the buffer comes back");
    f.submit_sync_data().wait().unwrap();

    let read = f.submit_read(IoBuf::zeroed(4096), 4096).wait().unwrap();
    assert!(read.iter().all(|&x| x == 0x5A));

    // As futures.
    let read = block_on(f.submit_read(IoBuf::zeroed(10), 4096)).unwrap();
    assert_eq!(&read[..], &[0x5A; 10]);
    block_on(f.submit_sync_data()).unwrap();

    // Errors arrive through the completion.
    let err = f
        .submit_read(IoBuf::zeroed(10), 1 << 20)
        .wait()
        .unwrap_err();
    assert_eq!(err.kind, ErrorKind::UnexpectedEof);

    // Many in flight at once.
    let pending: Vec<_> = (0..32u64)
        .map(|i| {
            let mut buf = IoBuf::zeroed(512);
            buf.fill(i as u8);
            f.submit_write(buf, i * 512)
        })
        .collect();
    for c in pending {
        c.wait().unwrap();
    }
    for i in 0..32u64 {
        let buf = f.submit_read(IoBuf::zeroed(512), i * 512).wait().unwrap();
        assert!(buf.iter().all(|&x| x == i as u8));
    }
}

fn namespace_operations(b: &Backend) {
    assert!(!b.vfs.exists(&b.path("a")).unwrap());
    drop(b.create("b"));
    drop(b.create("a"));
    assert!(b.vfs.exists(&b.path("a")).unwrap());
    let listed = b.vfs.list_dir(&b.root).unwrap();
    assert_eq!(listed.len(), 2);
    assert!(same_name(&listed[0], &b.path("a")) && same_name(&listed[1], &b.path("b")));

    b.vfs.remove(&b.path("a")).unwrap();
    assert!(!b.vfs.exists(&b.path("a")).unwrap());
    assert_eq!(
        b.vfs.remove(&b.path("a")).unwrap_err().kind,
        ErrorKind::NotFound
    );
    assert_eq!(b.vfs.list_dir(&b.root).unwrap().len(), 1);
}

fn syncs(b: &Backend) {
    let f = b.create("f");
    f.write_at(b"data", 0).unwrap();
    f.sync_data().unwrap();
    f.sync_all().unwrap();
    b.vfs.sync_dir(&b.root).unwrap();
}

fn read_only_handle_rejects_writes(b: &Backend) {
    drop(b.create("f"));
    let f = b.vfs.open(&b.path("f"), OpenOptions::read()).unwrap();
    let err = f.write_at(b"x", 0).unwrap_err();
    assert_eq!(err.kind, ErrorKind::Other, "{}: {err}", b.name);
}

fn exclusive_conflicts(b: &Backend) {
    let a = b.create("f");
    let c = b
        .vfs
        .open(&b.path("f"), OpenOptions::read_write_create())
        .unwrap();
    a.lock(8192, LockMode::Exclusive).unwrap();
    a.lock(8192, LockMode::Exclusive).unwrap(); // re-taking is a no-op
    assert_eq!(
        c.lock(8192, LockMode::Exclusive).unwrap_err().kind,
        ErrorKind::Locked
    );
    assert_eq!(
        c.lock(8192, LockMode::Shared).unwrap_err().kind,
        ErrorKind::Locked
    );
    // Other bytes are independent.
    c.lock(8193, LockMode::Exclusive).unwrap();
    a.unlock(8192).unwrap();
    c.lock(8192, LockMode::Exclusive).unwrap();
    // Unlocking a byte this handle does not hold is a no-op.
    a.unlock(8192).unwrap();
    assert_eq!(
        a.lock(8192, LockMode::Shared).unwrap_err().kind,
        ErrorKind::Locked
    );
}

fn shared_locks_coexist(b: &Backend) {
    let a = b.create("f");
    let c = b
        .vfs
        .open(&b.path("f"), OpenOptions::read_write_create())
        .unwrap();
    let d = b
        .vfs
        .open(&b.path("f"), OpenOptions::read_write_create())
        .unwrap();
    a.lock(8193, LockMode::Shared).unwrap();
    c.lock(8193, LockMode::Shared).unwrap();
    assert_eq!(
        d.lock(8193, LockMode::Exclusive).unwrap_err().kind,
        ErrorKind::Locked
    );
    a.unlock(8193).unwrap();
    assert_eq!(
        d.lock(8193, LockMode::Exclusive).unwrap_err().kind,
        ErrorKind::Locked,
        "one shared holder remains"
    );
    c.unlock(8193).unwrap();
    d.lock(8193, LockMode::Exclusive).unwrap();
}

fn upgrade_and_downgrade(b: &Backend) {
    let a = b.create("f");
    let c = b
        .vfs
        .open(&b.path("f"), OpenOptions::read_write_create())
        .unwrap();
    a.lock(8193, LockMode::Shared).unwrap();
    c.lock(8193, LockMode::Shared).unwrap();
    // The "last one out" check: an upgrade fails while another holder remains, and the
    // failed attempt keeps the shared lock.
    assert_eq!(
        a.lock(8193, LockMode::Exclusive).unwrap_err().kind,
        ErrorKind::Locked
    );
    c.unlock(8193).unwrap();
    c.lock(8193, LockMode::Shared).unwrap();
    assert_eq!(
        c.lock(8193, LockMode::Exclusive).unwrap_err().kind,
        ErrorKind::Locked,
        "a still holds shared"
    );
    c.unlock(8193).unwrap();
    a.lock(8193, LockMode::Exclusive).unwrap();
    assert_eq!(
        c.lock(8193, LockMode::Shared).unwrap_err().kind,
        ErrorKind::Locked
    );
    // Downgrade lets shared holders in but still excludes writers.
    a.lock(8193, LockMode::Shared).unwrap();
    c.lock(8193, LockMode::Shared).unwrap();
    c.unlock(8193).unwrap();
    assert_eq!(
        c.lock(8193, LockMode::Exclusive).unwrap_err().kind,
        ErrorKind::Locked
    );
    a.unlock(8193).unwrap();
    c.lock(8193, LockMode::Exclusive).unwrap();
}

fn closing_a_handle_releases_its_locks(b: &Backend) {
    let a = b.create("f");
    let keep = b
        .vfs
        .open(&b.path("f"), OpenOptions::read_write_create())
        .unwrap();
    let other = b
        .vfs
        .open(&b.path("f"), OpenOptions::read_write_create())
        .unwrap();
    a.lock(8192, LockMode::Exclusive).unwrap();
    keep.lock(8194, LockMode::Exclusive).unwrap();
    drop(a);
    other.lock(8192, LockMode::Exclusive).unwrap();
    // Closing `a` dropped only `a`'s lock.
    assert_eq!(
        other.lock(8194, LockMode::Shared).unwrap_err().kind,
        ErrorKind::Locked
    );
}

fn exclusive_lock_needs_a_writable_handle(b: &Backend) {
    drop(b.create("f"));
    let r = b.vfs.open(&b.path("f"), OpenOptions::read()).unwrap();
    assert_eq!(
        r.lock(8192, LockMode::Exclusive).unwrap_err().kind,
        ErrorKind::Unsupported
    );
    r.lock(8193, LockMode::Shared).unwrap();
    assert_eq!(
        r.lock(8193, LockMode::Exclusive).unwrap_err().kind,
        ErrorKind::Unsupported
    );
}

fn identity_and_locality(b: &Backend) {
    let a = b.create("a");
    let a2 = b.vfs.open(&b.path("a"), OpenOptions::read()).unwrap();
    let other = b.create("b");
    assert_eq!(a.identity().unwrap(), a2.identity().unwrap());
    assert_ne!(a.identity().unwrap(), other.identity().unwrap());
    assert!(
        a.is_local().unwrap(),
        "{}: temp dir should be local",
        b.name
    );
}

fn shared_memory_in_a_directory(b: &Backend) {
    let name = unique("d");
    let dir = Some(b.root.as_path());
    let region = b
        .vfs
        .open_shared(&name, dir, 8192, SharedOpen::CreateNew)
        .unwrap();
    assert_eq!(region.len(), 8192);
    let mut zeros = [1u8; 64];
    region.read(8128, &mut zeros);
    assert_eq!(zeros, [0; 64], "new regions are zeroed");
    assert_eq!(
        b.vfs
            .open_shared(&name, dir, 8192, SharedOpen::CreateNew)
            .unwrap_err()
            .kind,
        ErrorKind::AlreadyExists
    );

    let attached = b
        .vfs
        .open_shared(&name, dir, 8192, SharedOpen::Attach)
        .unwrap();
    region.write(100, b"shared bytes");
    region.atomic_u64(8).store(77, Ordering::Release);
    let mut buf = [0u8; 12];
    attached.read(100, &mut buf);
    assert_eq!(&buf, b"shared bytes");
    assert_eq!(attached.atomic_u64(8).load(Ordering::Acquire), 77);
    attached.atomic_u32(4).fetch_add(3, Ordering::AcqRel);
    assert_eq!(region.atomic_u32(4).load(Ordering::Acquire), 3);
    region.bind_numa(0, 4096, 0).ok(); // may be refused on non-NUMA hosts; must not crash

    // Removing the name leaves existing mappings valid.
    b.vfs.remove_shared(&name, dir).unwrap();
    assert_eq!(attached.atomic_u64(8).load(Ordering::Acquire), 77);
    assert_eq!(
        b.vfs
            .open_shared(&name, dir, 8192, SharedOpen::Attach)
            .unwrap_err()
            .kind,
        ErrorKind::NotFound
    );
    assert_eq!(
        b.vfs.remove_shared(&name, dir).unwrap_err().kind,
        ErrorKind::NotFound
    );

    // Bad requests.
    assert!(
        b.vfs
            .open_shared("a/b", dir, 64, SharedOpen::CreateNew)
            .is_err()
    );
    assert!(
        b.vfs
            .open_shared(&unique("z"), dir, 0, SharedOpen::CreateNew)
            .is_err()
    );
}

fn clocks_and_processes(b: &Backend) {
    let t0 = b.vfs.monotonic_nanos();
    let t1 = b.vfs.monotonic_nanos();
    assert!(t1 >= t0);
    // After 2025-01-01, in microseconds.
    assert!(b.vfs.now_micros() > 1_735_689_600_000_000);

    let me = b.vfs.current_process();
    assert_eq!(me, b.vfs.current_process());
    assert!(b.vfs.process_alive(me));
}
