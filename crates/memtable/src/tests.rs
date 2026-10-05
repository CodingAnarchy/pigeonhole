//! Unit tests: layout, allocation, validation and offset-only linking. Concurrency is covered
//! by `loom_tests` (model checked) and `tests/concurrent.rs` (real threads).

use super::*;
use pigeonhole_format::{Kind, encode_key};

fn key(row: &[u8], seqno: u64) -> Vec<u8> {
    let mut k = Vec::new();
    encode_key(&mut k, row, b"q", 0, seqno, Kind::Put).unwrap();
    k
}

fn small() -> ShardArena {
    ShardArena::new(ArenaRegion::heap(64 * 1024), 1024)
}

fn scan(reader: &MemtableReader) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut it = reader.iter();
    it.seek_to_first().unwrap();
    let mut out = Vec::new();
    while it.valid() {
        out.push((it.key().to_vec(), it.value().to_vec()));
        it.next().unwrap();
    }
    out
}

#[test]
fn empty_memtable() {
    let mut arena = small();
    let mt = Memtable::create(&mut arena).unwrap();
    assert_eq!(mt.len(), 0);
    assert!(mt.is_empty());
    assert_eq!(mt.seqno_range(), None);
    assert!(!mt.is_frozen());
    assert_eq!(
        mt.root(),
        64,
        "first memtable sits after the reserved prefix"
    );
    let reader = mt.reader();
    assert!(reader.is_empty());
    assert_eq!(reader.region().len(), 64 * 1024);
    let mut it = reader.iter();
    assert!(!it.valid());
    it.seek_to_first().unwrap();
    assert!(!it.valid());
    it.seek(&key(b"x", 1)).unwrap();
    assert!(!it.valid());
    it.next().unwrap();
    assert!(!it.valid());
}

#[test]
fn insert_and_scan_in_key_order() {
    let mut arena = small();
    let mut mt = Memtable::create(&mut arena).unwrap();
    let rows: [&[u8]; 5] = [b"m", b"c", b"zz", b"a", b"c\x00"];
    let mut expected = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        let k = key(row, i as u64 + 1);
        let v = format!("\x00{i}").into_bytes();
        mt.insert(&mut arena, &k, &v).unwrap();
        expected.push((k, v));
    }
    expected.sort();
    assert_eq!(mt.len(), 5);
    assert_eq!(mt.seqno_range(), Some((1, 5)));
    let reader = mt.reader();
    assert_eq!(reader.len(), 5);
    assert_eq!(scan(&reader), expected);

    let mut it = reader.iter();
    for (k, v) in &expected {
        it.seek(k).unwrap();
        assert!(it.valid());
        assert_eq!(it.key(), &k[..]);
        assert_eq!(it.value(), &v[..]);
        assert_eq!(&*it.value_slice(), &v[..]);
    }
    // Between two keys: lands on the successor.
    it.seek(&key(b"b", 1)).unwrap();
    assert_eq!(it.key(), &expected[1].0[..]);
    // Before everything and after everything.
    it.seek(b"").unwrap();
    assert_eq!(it.key(), &expected[0].0[..]);
    it.seek(&key(b"zzz", 1)).unwrap();
    assert!(!it.valid());
    // `skip_row` (the default implementation) works over internal keys.
    it.seek_to_first().unwrap();
    it.skip_row().unwrap();
    assert_eq!(it.key(), &expected[1].0[..]);
}

#[test]
fn header_follows_format() {
    let mut arena = small();
    let mut mt = Memtable::create(&mut arena).unwrap();
    let region = arena.region().clone();
    let mem = &region.mem;
    let root = mt.root() as usize;
    assert_eq!(mem.read_u32(root + layout::H_MAGIC), MEMTABLE_MAGIC);
    assert_eq!(mem.copy(root + layout::H_MAGIC, 4), b"MEMT");
    assert_eq!(mem.read_u8(root + layout::H_VERSION), 1);
    assert_eq!(mem.read_u8(root + layout::H_VERSION + 1), 0);
    assert_eq!(mem.read_u8(root + layout::H_FLAGS), 0);
    let head = mem.read_u32(root + layout::H_HEAD) as usize;
    assert_eq!(head, root + layout::HEADER_LEN);
    assert_eq!(mem.read_u8(head + layout::N_HEIGHT), 16);
    assert_eq!(mem.load_u32(root + layout::H_COUNT, Ordering::Acquire), 0);
    assert_eq!(
        mem.load_u64(root + layout::H_BYTES, Ordering::Acquire),
        (layout::HEADER_LEN + HEAD_LEN) as u64
    );

    mt.insert(&mut arena, &key(b"r", 7), b"\x00v").unwrap();
    mt.insert(&mut arena, &key(b"s", 3), b"\x00w").unwrap();
    assert_eq!(mem.load_u32(root + layout::H_COUNT, Ordering::Acquire), 2);
    assert_eq!(
        mem.load_u64(root + layout::H_MIN_SEQNO, Ordering::Acquire),
        3
    );
    assert_eq!(
        mem.load_u64(root + layout::H_MAX_SEQNO, Ordering::Acquire),
        7
    );
    assert!(mem.load_u64(root + layout::H_BYTES, Ordering::Acquire) > 140);
    assert_eq!(mt.seqno_range(), Some((3, 7)));

    mt.freeze();
    assert!(mt.is_frozen());
    assert_eq!(mem.load_u8(root + layout::H_FLAGS, Ordering::Acquire), 1);
}

#[test]
fn node_layout_follows_format() {
    let mut arena = small();
    let mut mt = Memtable::create(&mut arena).unwrap();
    let k = key(b"row", 1);
    mt.insert(&mut arena, &k, b"\x00value").unwrap();
    let mem = &arena.region().mem;
    let head = mt.head as usize;
    let node = mem.load_u32(head + layout::N_TOWER, Ordering::Acquire) as usize;
    assert_eq!(node % 4, 0);
    assert_eq!(mem.read_u32(node + layout::N_KEY_LEN) as usize, k.len());
    assert_eq!(mem.read_u32(node + layout::N_VALUE_LEN), 6);
    let height = mem.read_u8(node + layout::N_HEIGHT) as usize;
    assert!((1..=16).contains(&height));
    assert_eq!(mem.copy(node + layout::N_HEIGHT + 1, 3), [0, 0, 0]);
    for level in 0..height {
        assert_eq!(
            mem.load_u32(tower(node as u32, level), Ordering::Acquire),
            0
        );
    }
    let key_off = node + layout::N_TOWER + 4 * height;
    assert_eq!(mem.copy(key_off, k.len()), k);
    assert_eq!(mem.copy(key_off + k.len(), 6), b"\x00value");
}

#[test]
fn chunks_spill_fill_and_reclaim() {
    let mut arena = small();
    assert_eq!(arena.free_bytes(), 64 * 1024);
    let mut mt = Memtable::create(&mut arena).unwrap();
    assert_eq!(mt.allocated_bytes(), 1024);
    assert_eq!(arena.free_bytes(), 63 * 1024);

    let value = vec![0u8; 300];
    let mut n = 0u64;
    let err = loop {
        n += 1;
        match mt.insert(&mut arena, &key(&n.to_be_bytes(), n), &value) {
            Ok(()) => {}
            Err(e) => break e,
        }
    };
    assert_eq!(err, Error::ArenaFull);
    // Each node is 346..406 bytes, so two fit a 1 KiB chunk (chunk 0 included: 140 bytes of
    // header and head plus two nodes stay under its 960 usable bytes): 64 chunks, 128 entries.
    assert_eq!(mt.len(), 128);
    assert_eq!(mt.allocated_bytes(), 64 * 1024);
    assert_eq!(arena.free_bytes(), 0);
    assert_eq!(scan(&mt.reader()).len(), mt.len());

    // Reclaim, then a new memtable reuses the dirty chunks and starts clean.
    let old_root = mt.root();
    arena.reclaim(mt.retire());
    assert_eq!(arena.free_bytes(), 64 * 1024);
    let mut mt2 = Memtable::create(&mut arena).unwrap();
    assert_eq!(mt2.root(), old_root);
    assert!(scan(&mt2.reader()).is_empty());
    mt2.insert(&mut arena, &key(b"k", 1), b"").unwrap();
    assert_eq!(scan(&mt2.reader()).len(), 1);
}

#[test]
fn large_entries_take_contiguous_runs() {
    let mut arena = small();
    let mut mt = Memtable::create(&mut arena).unwrap();
    // Larger than a chunk: a run of three.
    let big = vec![9u8; 2500];
    mt.insert(&mut arena, &key(b"big", 1), &big).unwrap();
    assert_eq!(mt.allocated_bytes(), 4 * 1024);
    assert_eq!(mt.runs.len(), 2);
    assert_eq!(mt.runs[1].count, 3);
    // Larger than the arena can ever hold.
    assert_eq!(
        mt.insert(&mut arena, &key(b"huge", 2), &vec![0; 64 * 1024]),
        Err(Error::EntryTooLarge)
    );
    assert_eq!(
        mt.insert(&mut arena, &key(b"huge", 2), &vec![0; 64 * 1024 - 100]),
        Err(Error::EntryTooLarge),
        "the reserved prefix and the header count against the arena"
    );
    let mut it = mt.reader().iter();
    it.seek_to_first().unwrap();
    assert_eq!(it.value(), &big[..]);
    let slice = it.value_slice();
    drop(it);
    assert_eq!(slice.len(), 2500);
}

#[test]
fn first_fit_skips_allocated_chunks() {
    let mut arena = ShardArena::new(ArenaRegion::heap(8 * 1024), 1024);
    let a = Memtable::create(&mut arena).unwrap();
    let b = Memtable::create(&mut arena).unwrap();
    let c = Memtable::create(&mut arena).unwrap();
    assert_eq!((a.root(), b.root(), c.root()), (64, 1024, 2048));
    arena.reclaim(b.retire());
    let d = Memtable::create(&mut arena).unwrap();
    assert_eq!(d.root(), 1024, "the freed middle chunk is reused first");
    // A run of two cannot use the single free chunk between a and c... it is taken by d
    // now, so the run comes after c.
    let mut d = d;
    d.insert(&mut arena, &key(b"x", 1), &vec![0; 1500]).unwrap();
    assert_eq!(d.runs[1], Run { first: 3, count: 2 });
    assert_eq!(arena.free_bytes(), 3 * 1024);
}

#[test]
#[should_panic(expected = "frozen")]
fn insert_into_frozen_panics() {
    let mut arena = small();
    let mut mt = Memtable::create(&mut arena).unwrap();
    mt.freeze();
    let _ = mt.insert(&mut arena, &key(b"a", 1), b"");
}

#[test]
#[should_panic(expected = "chunk size")]
fn bad_chunk_size_panics() {
    ShardArena::new(ArenaRegion::heap(4096), 100);
}

#[test]
fn malformed_key_rejected() {
    let mut arena = small();
    let mut mt = Memtable::create(&mut arena).unwrap();
    assert_eq!(
        mt.insert(&mut arena, b"short", b""),
        Err(Error::Corrupt("insert key is not an internal key"))
    );
    assert!(mt.is_empty());
}

#[test]
fn duplicate_keys_are_both_visible() {
    let mut arena = small();
    let mut mt = Memtable::create(&mut arena).unwrap();
    let k = key(b"dup", 1);
    mt.insert(&mut arena, &k, b"\x00a").unwrap();
    mt.insert(&mut arena, &k, b"\x00b").unwrap();
    let entries = scan(&mt.reader());
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().all(|(key, _)| key == &k));
}

#[test]
fn create_fails_when_arena_is_full() {
    let mut arena = ShardArena::new(ArenaRegion::heap(2048), 1024);
    let _a = Memtable::create(&mut arena).unwrap();
    let _b = Memtable::create(&mut arena).unwrap();
    assert_eq!(Memtable::create(&mut arena).unwrap_err(), Error::ArenaFull);
    let mut tiny = ShardArena::new(ArenaRegion::heap(512), 1024);
    assert_eq!(
        Memtable::create(&mut tiny).unwrap_err(),
        Error::EntryTooLarge
    );
}

#[test]
fn arena_region_validation() {
    let region = SharedRegion::heap(8192);
    let err = |r: Result<ArenaRegion>| r.unwrap_err();
    assert_eq!(
        err(ArenaRegion::new(region.clone(), 32, 64)),
        Error::Corrupt("arena offset is not 64-byte aligned")
    );
    assert_eq!(
        err(ArenaRegion::new(region.clone(), 64, 8192)),
        Error::Corrupt("arena extends past its region")
    );
    assert_eq!(
        err(ArenaRegion::new(region.clone(), 0, MAX_ARENA_LEN + 1)),
        Error::Corrupt("arena longer than 4 GiB")
    );
    let arena = ArenaRegion::new(region.clone(), 4096, 4096).unwrap();
    assert_eq!(arena.len(), 4096);
    assert!(ArenaRegion::new(region, 8192, 0).unwrap().is_empty());

    // A memtable in a sub-range of a region is readable through the same region bytes.
    let mut shard = ShardArena::new(arena, 1024);
    let mut mt = Memtable::create(&mut shard).unwrap();
    mt.insert(&mut shard, &key(b"k", 1), b"\x00v").unwrap();
    assert_eq!(scan(&mt.reader()).len(), 1);
}

#[test]
fn open_validates_header() {
    let mut arena = small();
    let mut mt = Memtable::create(&mut arena).unwrap();
    mt.insert(&mut arena, &key(b"k", 1), b"\x00v").unwrap();
    let region = arena.region().clone();
    let root = mt.root();
    assert!(MemtableReader::open(region.clone(), root).is_ok());

    let corrupt = |root: u32| MemtableReader::open(region.clone(), root).unwrap_err();
    assert_eq!(corrupt(0), Error::Corrupt("memtable root is misaligned"));
    assert_eq!(
        corrupt(root + 4),
        Error::Corrupt("memtable root is misaligned")
    );
    assert_eq!(
        corrupt(64 * 1024 - 32),
        Error::Corrupt("memtable header outside arena")
    );
    assert_eq!(corrupt(root + 128), Error::Corrupt("memtable header magic"));

    let mem = &region.mem;
    let saved = mem.copy(root as usize, layout::HEADER_LEN);
    mem.write(root as usize + layout::H_VERSION, &[2, 0]);
    assert_eq!(corrupt(root), Error::Corrupt("memtable header version"));
    mem.write(root as usize, &saved);

    mem.write_u32(root as usize + layout::H_HEAD, 64 * 1024 - 4);
    assert_eq!(corrupt(root), Error::Corrupt("node header outside arena"));
    mem.write_u32(root as usize + layout::H_HEAD, 3);
    assert_eq!(corrupt(root), Error::Corrupt("node offset"));
    // Point the head at the data node: wrong height for a head.
    let node = mem.load_u32(tower(mt.head, 0), Ordering::Acquire);
    mem.write_u32(root as usize + layout::H_HEAD, node);
    assert_eq!(corrupt(root), Error::Corrupt("memtable head node"));
    mem.write(root as usize, &saved);
    assert!(MemtableReader::open(region.clone(), root).is_ok());
}

#[test]
fn reader_reports_corrupt_nodes_instead_of_faulting() {
    let mut arena = small();
    let mut mt = Memtable::create(&mut arena).unwrap();
    for i in 1..=3 {
        mt.insert(&mut arena, &key(&[i as u8], i), b"\x00v")
            .unwrap();
    }
    let reader = mt.reader();
    let mem = &arena.region().mem;
    let first = mem.load_u32(tower(mt.head, 0), Ordering::Acquire);
    let second = mem.load_u32(tower(first, 0), Ordering::Acquire);
    let saved = mem.copy(second as usize, 12);

    let step = || {
        let mut it = reader.iter();
        it.seek_to_first()?;
        it.next()
    };
    mem.write(second as usize + layout::N_HEIGHT, &[0]);
    assert_eq!(step(), Err(Error::Corrupt("node height")));
    mem.write(second as usize + layout::N_HEIGHT, &[17]);
    assert_eq!(step(), Err(Error::Corrupt("node height")));
    mem.write(second as usize, &saved);
    mem.write_u32(second as usize + layout::N_KEY_LEN, u32::MAX);
    assert_eq!(step(), Err(Error::Corrupt("node outside arena")));
    mem.write(second as usize, &saved);
    mem.write_u32(second as usize + layout::N_VALUE_LEN, 64 * 1024);
    assert_eq!(step(), Err(Error::Corrupt("node outside arena")));
    mem.write(second as usize, &saved);

    // Bad links: misaligned, past the arena, and a seek that crosses them.
    mem.store_u32(tower(first, 0), 64 * 1024 - 2, Ordering::Release);
    assert_eq!(step(), Err(Error::Corrupt("node offset")));
    mem.store_u32(tower(first, 0), 64 * 1024 - 8, Ordering::Release);
    assert_eq!(step(), Err(Error::Corrupt("node header outside arena")));
    let mut it = reader.iter();
    assert_eq!(
        it.seek(&key(&[9], 9)),
        Err(Error::Corrupt("node header outside arena"))
    );
    mem.store_u32(tower(first, 0), second, Ordering::Release);
    assert_eq!(step(), Ok(()));
    assert_eq!(scan(&reader).len(), 3);
}

#[test]
fn no_pointers_in_the_arena() {
    let mut arena = ShardArena::new(ArenaRegion::heap(256 * 1024), 4096);
    let mut mt = Memtable::create(&mut arena).unwrap();
    for i in 1..=1000u64 {
        mt.insert(&mut arena, &key(&i.to_le_bytes(), i), &i.to_le_bytes())
            .unwrap();
    }
    let mem = &arena.region().mem;
    let bytes = mem.copy(0, mem.len());
    let base = mem.base_addr();
    let needles = [
        base.to_ne_bytes(),
        (base + mt.root() as usize).to_ne_bytes(),
    ];
    for needle in needles {
        assert!(
            !bytes.windows(needle.len()).any(|w| w == needle),
            "found an absolute address in the arena"
        );
    }
}

#[test]
fn arena_bytes_relocate_to_another_address() {
    let mut arena = ShardArena::new(ArenaRegion::heap(256 * 1024), 4096);
    let mut mt = Memtable::create(&mut arena).unwrap();
    let mut expected = Vec::new();
    for i in 1..=500u64 {
        let k = key(&(i * 7919 % 1000).to_be_bytes(), i);
        let v = i.to_le_bytes().to_vec();
        mt.insert(&mut arena, &k, &v).unwrap();
        expected.push((k, v));
    }
    expected.sort();
    mt.freeze();

    // Copy the raw arena to a different allocation: offsets must still resolve.
    let copy = ArenaRegion::heap(256 * 1024);
    assert_ne!(copy.mem.base_addr(), arena.region().mem.base_addr());
    let bytes = arena.region().mem.copy(0, 256 * 1024);
    copy.mem.write(0, &bytes);
    let reader = MemtableReader::open(copy, mt.root()).unwrap();
    assert_eq!(reader.len(), 500);
    assert_eq!(scan(&reader), expected);
    let mut it = reader.iter();
    for (k, v) in expected.iter().step_by(37) {
        it.seek(k).unwrap();
        assert_eq!((it.key(), it.value()), (&k[..], &v[..]));
    }
}

#[test]
fn heights_are_geometric_and_bounded() {
    let mut arena = small();
    let mut mt = Memtable::create(&mut arena).unwrap();
    let mut tall = 0;
    for _ in 0..10_000 {
        let h = mt.random_height();
        assert!((1..=MAX_HEIGHT).contains(&h));
        if h >= 2 {
            tall += 1;
        }
    }
    // One in four nodes reaches level 2 (binomial: 2500 ± ~45).
    assert!((2200..2800).contains(&tall), "{tall} tall nodes");
}

#[test]
fn error_display() {
    assert_eq!(Error::ArenaFull.to_string(), "shard arena is full");
    assert_eq!(
        Error::EntryTooLarge.to_string(),
        "entry is larger than the arena can hold"
    );
    assert_eq!(Error::Corrupt("x").to_string(), "corrupt memtable: x");
}

#[test]
fn reader_and_slices_are_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<MemtableReader>();
    assert_send_sync::<MemIter>();
    assert_send_sync::<ArenaSlice>();
    assert_send_sync::<ArenaRegion>();
    fn assert_send<T: Send>() {}
    assert_send::<Memtable>();
    assert_send::<ShardArena>();
    assert_send::<Retired>();
}

#[test]
fn reclaim_waits_for_in_process_readers() {
    let mut arena = small();
    let mut mt = Memtable::create(&mut arena).unwrap();
    let value = vec![0x5a; 100];
    mt.insert(&mut arena, &key(b"k", 1), &value).unwrap();
    let root = mt.root();
    let reader = mt.reader();
    let mut it = reader.iter();
    it.seek_to_first().unwrap();
    let slice = it.value_slice();

    arena.reclaim(mt.retire());
    assert_eq!(
        arena.free_bytes(),
        63 * 1024,
        "still borrowed by the cursor"
    );
    // A new memtable does not reuse the borrowed chunk; the borrowed bytes stay intact.
    let mut other = Memtable::create(&mut arena).unwrap();
    assert_ne!(other.root(), root);
    other.insert(&mut arena, &key(b"x", 2), &[1; 100]).unwrap();
    assert_eq!(it.value(), &value[..]);
    assert_eq!(&*slice, &value[..]);
    assert_eq!(reader.len(), 1);

    // The last handle to go releases the chunk, at the next reclaim or allocation.
    drop(it);
    drop(reader);
    assert_eq!(arena.free_bytes(), 62 * 1024, "the slice still pins");
    drop(slice);
    assert_eq!(
        arena.free_bytes(),
        62 * 1024,
        "nothing has run the release yet"
    );
    let third = Memtable::create(&mut arena).unwrap();
    assert_eq!(third.root(), root, "the released chunk is reused first");
    assert_eq!(arena.free_bytes(), 62 * 1024);
    arena.reclaim(third.retire());
    arena.reclaim(other.retire());
    assert_eq!(arena.free_bytes(), 64 * 1024);
}

#[test]
fn reader_opened_by_root_pins_in_the_writer_process() {
    let mut arena = small();
    let mt = Memtable::create(&mut arena).unwrap();
    let root = mt.root();
    let opened = MemtableReader::open(arena.region().clone(), root).unwrap();
    arena.reclaim(mt.retire());
    assert_eq!(arena.free_bytes(), 63 * 1024);
    assert_ne!(Memtable::create(&mut arena).unwrap().root(), root);
    drop(opened);
    let probe = Memtable::create(&mut arena).unwrap();
    assert_eq!(
        arena.free_bytes(),
        62 * 1024,
        "the old chunk was released on allocation, then one was taken"
    );
    arena.reclaim(probe.retire());
    assert_eq!(arena.free_bytes(), 63 * 1024);
    assert_eq!(Memtable::create(&mut arena).unwrap().root(), root);

    // A reader opened on a different `ArenaRegion` value (another process's mapping, or
    // raw bytes copied elsewhere) pins nothing here, as it cannot.
    let copy = ArenaRegion::heap(64 * 1024);
    copy.mem.write(0, &arena.region().mem.copy(0, 64 * 1024));
    let _foreign = MemtableReader::open(copy, root).unwrap();
}

#[test]
fn pinned_retired_chunks_count_as_allocated() {
    let mut arena = ShardArena::new(ArenaRegion::heap(2048), 1024);
    let a = Memtable::create(&mut arena).unwrap();
    let reader = a.reader();
    arena.reclaim(a.retire());
    let _b = Memtable::create(&mut arena).unwrap();
    assert_eq!(Memtable::create(&mut arena).unwrap_err(), Error::ArenaFull);
    drop(reader);
    assert_eq!(Memtable::create(&mut arena).unwrap().root(), 64);
}

#[test]
#[should_panic(expected = "different shard arena")]
fn insert_through_another_arena_panics() {
    let mut arena = small();
    let mut other = small();
    let mut mt = Memtable::create(&mut arena).unwrap();
    let _ = mt.insert(&mut other, &key(b"k", 1), b"");
}

#[test]
fn reader_rejects_a_node_linked_above_its_height() {
    let mut arena = ShardArena::new(ArenaRegion::heap(256 * 1024), 4096);
    let mut mt = Memtable::create(&mut arena).unwrap();
    for i in 1..=200u64 {
        mt.insert(&mut arena, &key(&i.to_be_bytes(), i), b"")
            .unwrap();
    }
    let region = arena.region().clone();
    let mem = &region.mem;
    let tall = mem.load_u32(tower(mt.head, 1), Ordering::Acquire);
    assert_ne!(tall, 0, "some node reaches level 1");
    assert!(mem.read_u8(tall as usize + layout::N_HEIGHT) >= 2);
    mem.write(tall as usize + layout::N_HEIGHT, &[1]);
    let mut it = mt.reader().iter();
    assert_eq!(
        it.seek(b""),
        Err(Error::Corrupt("node linked above its height"))
    );
    // Level 0 never follows a link above a height of 1, so a scan still works.
    it.seek_to_first().unwrap();
    assert!(it.valid());
}

#[test]
fn reader_reports_link_cycles() {
    let mut arena = small();
    let mut mt = Memtable::create(&mut arena).unwrap();
    for i in 1..=3u8 {
        mt.insert(&mut arena, &key(&[i], i as u64), b"").unwrap();
    }
    let region = arena.region().clone();
    let mem = &region.mem;
    let first = mem.load_u32(tower(mt.head, 0), Ordering::Acquire);
    let second = mem.load_u32(tower(first, 0), Ordering::Acquire);
    // second -> first: a level-0 cycle.
    mem.store_u32(tower(second, 0), first, Ordering::Release);

    let reader = mt.reader();
    let mut it = reader.iter();
    it.seek_to_first().unwrap();
    let result = loop {
        match it.next() {
            Ok(()) if it.valid() => {}
            other => break other,
        }
    };
    assert_eq!(result, Err(Error::Corrupt("link cycle")));
    assert_eq!(it.seek(&key(&[9], 9)), Err(Error::Corrupt("link cycle")));
}
