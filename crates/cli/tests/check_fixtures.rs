//! `phdb check` on crafted damage (#695 item (c)): each fixture rewrites one structure of a
//! real database through `pigeonhole-format`'s encoders, with every checksum recomputed, so
//! only the check's own rules can catch it. `check.rs` flips bytes, which a checksum catches;
//! these reach the findings behind the checksums: a manifest that disagrees with an SST's
//! properties, blob files whose records or extents don't add up, overlapping extents, WAL
//! segment chains that loop or fork, and superblock slots damaged in each combination.

mod common;

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use common::{TempDir, phdb};
use pigeonhole::{Family, Options, Pigeonhole};
use pigeonhole_format::block::{Block, BlockAddr, BlockKind, seal, verify};
use pigeonhole_format::compress::Compression;
use pigeonhole_format::cursor::Cursor;
use pigeonhole_format::manifest::{
    Edit, ManifestBlockKind, ManifestHeader, SstMeta, decode_block, encode_block,
};
use pigeonhole_format::sst::{FOOTER_LEN, Footer, Properties};
use pigeonhole_format::superblock::{ExtentRef, SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B, Superblock};
use pigeonhole_format::wal::{FRAGMENT_HEADER_LEN, FRAME_SIZE, SegmentHeader};
use pigeonhole_format::{BlobFileId, FamilyId, FormatVersion, Lsn, StreamId};
use serde_json::Value;

const PAGE: usize = 4096;
const CHILD: &str = "PHDB_TEST_FIXTURE_CRASH_CHILD";

fn read_at(path: &Path, offset: u64, len: usize) -> Vec<u8> {
    let mut f = std::fs::File::open(path).unwrap();
    let mut b = vec![0u8; len];
    f.seek(SeekFrom::Start(offset)).unwrap();
    f.read_exact(&mut b).unwrap();
    b
}

fn write_at(path: &Path, offset: u64, bytes: &[u8]) {
    let mut f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.seek(SeekFrom::Start(offset)).unwrap();
    f.write_all(bytes).unwrap();
    f.sync_all().unwrap();
}

/// Both superblock slots, decoded.
fn slots(path: &Path) -> [Superblock; 2] {
    [SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B]
        .map(|p| Superblock::decode(&read_at(path, p * PAGE as u64, PAGE)).unwrap())
}

/// The page of the newest slot, and the older one.
fn newest(path: &Path) -> (u64, u64) {
    let [a, b] = slots(path);
    if b.sequence > a.sequence {
        (SUPERBLOCK_PAGE_B, SUPERBLOCK_PAGE_A)
    } else {
        (SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B)
    }
}

fn root(path: &Path) -> Superblock {
    let (page, _) = newest(path);
    Superblock::decode(&read_at(path, page * PAGE as u64, PAGE)).unwrap()
}

fn write_superblock(path: &Path, page: u64, sb: &Superblock) {
    let mut buf = [0u8; PAGE];
    sb.encode(&mut buf);
    write_at(path, page * PAGE as u64, &buf);
}

/// Every manifest edit, the snapshot's then the log's, in order.
fn edits(path: &Path) -> Vec<Edit> {
    let sb = root(path);
    let snapshot = sb.snapshot.unwrap();
    let bytes = read_at(path, snapshot.offset(), sb.snapshot_len as usize);
    let (_, mut edits, _) = decode_block(&bytes).unwrap();
    if let Some(log) = sb.log {
        let bytes = read_at(path, log.offset(), sb.log_len as usize);
        let mut at = 0;
        while at < bytes.len() {
            let (_, more, used) = decode_block(&bytes[at..]).unwrap();
            edits.extend(more);
            at += used;
        }
    }
    edits
}

/// Replaces the manifest with one snapshot block holding `edits`, empties the log, and
/// points the newest superblock slot at it.
fn set_edits(path: &Path, edits: &[Edit]) {
    let mut sb = root(path);
    let header = ManifestHeader {
        version: FormatVersion::CURRENT,
        kind: ManifestBlockKind::Snapshot,
        manifest_version: sb.manifest_version,
        edit_count: 0,
        body_len: 0,
    };
    let mut block = Vec::new();
    encode_block(&header, edits, &mut block);
    let snapshot = sb.snapshot.unwrap();
    assert!(
        block.len() as u64 <= snapshot.len(),
        "the manifest outgrew its extent"
    );
    write_at(path, snapshot.offset(), &block);
    sb.snapshot_len = block.len() as u32;
    sb.log_len = 0;
    write_superblock(path, newest(path).0, &sb);
}

/// Rewrites the manifest through `f`.
fn edit_manifest(path: &Path, f: impl FnOnce(&mut Vec<Edit>)) {
    let mut e = edits(path);
    f(&mut e);
    set_edits(path, &e);
}

/// The first SST the manifest adds and no later edit removes.
fn live_sst(edits: &mut [Edit]) -> &mut SstMeta {
    let removed: Vec<_> = edits
        .iter()
        .filter_map(|e| match e {
            Edit::RemoveSst { sst, .. } => Some(*sst),
            _ => None,
        })
        .collect();
    edits
        .iter_mut()
        .find_map(|e| match e {
            Edit::AddSst { meta, .. } if !removed.contains(&meta.id) => Some(meta),
            _ => None,
        })
        .expect("a live SST")
}

fn blob_file(edits: &mut [Edit]) -> &mut Edit {
    edits
        .iter_mut()
        .find(|e| matches!(e, Edit::PutBlobFile { .. }))
        .expect("a blob file")
}

/// An SST's footer, read from the end of its used bytes.
fn footer(path: &Path, meta: &SstMeta) -> Footer {
    let tail = read_at(
        path,
        meta.extent.offset() + meta.len - FOOTER_LEN as u64,
        FOOTER_LEN,
    );
    Footer::decode(&tail).unwrap()
}

/// The logical bytes of the block at `addr` (relative to the SST), and its codec.
fn block(path: &Path, meta: &SstMeta, addr: BlockAddr) -> (Vec<u8>, Compression) {
    let physical = read_at(path, meta.extent.offset() + addr.offset, addr.len as usize);
    let (trailer, payload) = verify(&physical).unwrap();
    assert_eq!(
        trailer.compression,
        Compression::None,
        "fixtures use uncompressed families"
    );
    (payload.to_vec(), trailer.compression)
}

/// Replaces the block at `addr` with `logical`, sealed; it must keep its length.
fn set_block(path: &Path, meta: &SstMeta, addr: BlockAddr, kind: BlockKind, logical: &[u8]) {
    let mut out = Vec::new();
    seal(kind, Compression::None, logical, &mut out).unwrap();
    assert_eq!(
        out.len(),
        addr.len as usize,
        "a rewritten block keeps its length"
    );
    write_at(path, meta.extent.offset() + addr.offset, &out);
}

/// Rewrites an SST's properties block through `f` (same encoded length).
fn edit_properties(path: &Path, meta: &SstMeta, f: impl FnOnce(&mut Properties)) {
    let addr = footer(path, meta).properties;
    let (logical, _) = block(path, meta, addr);
    let mut props = Properties::decode(&logical).unwrap();
    f(&mut props);
    let mut out = Vec::new();
    props.encode(&mut out);
    set_block(path, meta, addr, BlockKind::Properties, &out);
}

/// The address of the SST's first data block (top index, then its first partition).
fn first_data_block(path: &Path, meta: &SstMeta) -> BlockAddr {
    let mut addr = footer(path, meta).top_index;
    for _ in 0..2 {
        let (logical, _) = block(path, meta, addr);
        let mut it = Block::new(logical).unwrap().into_cursor();
        it.seek_to_first().unwrap();
        addr = BlockAddr::decode_varint(it.value()).unwrap();
    }
    addr
}

/// Every data block of the SST, in order.
fn data_blocks(path: &Path, meta: &SstMeta) -> Vec<BlockAddr> {
    let addrs = |addr| {
        let (logical, _) = block(path, meta, addr);
        let mut it = Block::new(logical).unwrap().into_cursor();
        it.seek_to_first().unwrap();
        let mut out = Vec::new();
        while it.valid() {
            out.push(BlockAddr::decode_varint(it.value()).unwrap());
            it.next().unwrap();
        }
        out
    };
    addrs(footer(path, meta).top_index)
        .into_iter()
        .flat_map(addrs)
        .collect()
}

/// A database with one SST (uncompressed 4 KiB blocks, so blocks can be rewritten in place),
/// a blob file,
/// a second, empty family and a second table, closed cleanly.
fn sample(dir: &TempDir) -> PathBuf {
    let path = dir.path().join("c.phdb");
    let db = Pigeonhole::open(&path, Options::default().shards(1).pin_threads(false)).unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family(
            "f",
            Family::default()
                .blob_threshold(100)
                .compression(pigeonhole::Compression::None)
                .block_size(4096),
        )
        .family("g", Family::default())
        .create()
        .unwrap();
    db.table("u")
        .unwrap()
        .family("h", Family::default())
        .create()
        .unwrap();
    for i in 0..200u64 {
        let v = if i % 10 == 0 {
            vec![b'b'; 300]
        } else {
            vec![b's'; 20]
        };
        t.mutate(format!("r{i:04}").as_bytes())
            .put_at("f", b"q", 1_760_000_000_000_000 + i, &v)
            .commit()
            .unwrap();
    }
    db.flush().unwrap();
    db.close().unwrap();
    path
}

/// Run by the child process: about 700 KiB of log in 256 KiB WAL segments, no flush, then
/// an exit without closing.
#[test]
fn fixture_crash_child() {
    let Ok(path) = std::env::var(CHILD) else {
        return;
    };
    let db = Pigeonhole::open(
        &path,
        Options::default()
            .shards(1)
            .pin_threads(false)
            .wal_segment_size(256 << 10),
    )
    .unwrap();
    let t = db
        .table("t")
        .unwrap()
        .family("f", Family::default())
        .create_if_missing()
        .unwrap();
    let v = vec![b'v'; 200];
    for i in 0..3000u32 {
        t.mutate(format!("w{i:05}").as_bytes())
            .put("f", b"q", &v)
            .commit()
            .unwrap();
    }
    std::process::exit(0);
}

/// A crashed database with a WAL of several segments.
fn crashed(dir: &TempDir) -> PathBuf {
    let path = dir.path().join("crashed.phdb");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "fixture_crash_child",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, &path)
        .status()
        .unwrap();
    assert!(status.success());
    path
}

fn wal_path(db: &Path) -> PathBuf {
    let mut name = db.as_os_str().to_owned();
    name.push("-wal-0");
    PathBuf::from(name)
}

const SEGMENT: u64 = 256 << 10;

/// Every slot's segment header in the crashed database's WAL.
fn segment_headers(path: &Path) -> Vec<Option<SegmentHeader>> {
    let wal = wal_path(path);
    let slots = std::fs::metadata(&wal).unwrap().len() / SEGMENT;
    (0..slots)
        .map(|s| SegmentHeader::decode(&read_at(&wal, s * SEGMENT, FRAME_SIZE)).ok())
        .collect()
}

fn write_segment_header(path: &Path, slot: u64, h: &SegmentHeader) {
    let wal = wal_path(path);
    let mut frame: Box<[u8; FRAME_SIZE]> = read_at(&wal, slot * SEGMENT, FRAME_SIZE)
        .into_boxed_slice()
        .try_into()
        .unwrap();
    h.encode(&mut frame);
    write_at(&wal, slot * SEGMENT, &frame[..]);
}

/// The slot holding `epoch`'s segment.
fn slot_of(headers: &[Option<SegmentHeader>], epoch: u32) -> u64 {
    headers
        .iter()
        .position(|h| h.as_ref().is_some_and(|h| h.epoch == epoch))
        .unwrap() as u64
}

/// The segment headers in chain order (oldest first).
fn chain(headers: &[Option<SegmentHeader>]) -> Vec<SegmentHeader> {
    let mut c: Vec<_> = headers.iter().flatten().copied().collect();
    c.sort_by_key(|h| h.epoch);
    c
}

fn check(path: &Path, args: &[&str]) -> (u8, Value) {
    let p = path.to_str().unwrap();
    let out = phdb(&[&["--format", "json", "check"][..], args, &[p]].concat());
    assert!(out.stdout.starts_with('{'), "{out:?}");
    (out.status, serde_json::from_str(&out.stdout).unwrap())
}

fn findings(doc: &Value) -> &Vec<Value> {
    doc["findings"].as_array().unwrap()
}

/// Asserts a finding about `structure` at `severity` whose message contains `text`.
fn has(doc: &Value, structure: &str, severity: &str, text: &str) {
    assert!(
        findings(doc).iter().any(|f| f["structure"] == structure
            && f["severity"] == severity
            && f["message"].as_str().unwrap().contains(text)),
        "no {severity} {structure} finding containing {text:?}: {doc:#}"
    );
}

/// `phdb check` exits 3 with a corruption finding about `structure` containing `text`.
fn corrupt(path: &Path, args: &[&str], structure: &str, text: &str) -> Value {
    let (status, doc) = check(path, args);
    has(&doc, structure, "corruption", text);
    assert_eq!(status, 3, "{doc:#}");
    doc
}

/// `phdb check` refuses the file as a newer build's (an error naming `text`, no report).
fn refused(path: &Path, text: &str) {
    let out = phdb(&["check", path.to_str().unwrap()]);
    assert!(out.stdout.is_empty(), "{out:?}");
    assert!(out.stderr.contains(text), "{out:?}");
    assert!(![0, 3].contains(&out.status), "{out:?}");
}

// Superblock slots.

#[test]
fn a_never_written_older_slot_is_not_a_finding() {
    let dir = TempDir::new("fx-slot-blank");
    let path = sample(&dir);
    let (_, older) = newest(&path);
    write_at(&path, older * PAGE as u64, &[0u8; PAGE]);
    let (status, doc) = check(&path, &[]);
    assert_eq!(status, 0, "{doc:#}");
    assert_eq!(doc["superblocks"], 1);
}

#[test]
fn a_damaged_older_slot_after_a_crash_is_a_warning() {
    let dir = TempDir::new("fx-slot-torn");
    let path = crashed(&dir);
    assert_eq!(check(&path, &[]).1["clean_shutdown"], false);
    let (_, older) = newest(&path);
    write_at(&path, older * PAGE as u64 + 20, &[0xEE; 8]);
    let (status, doc) = check(&path, &[]);
    has(
        &doc,
        "superblock",
        "warning",
        "a crash during a root commit",
    );
    // Warnings alone exit 0 offline.
    assert_eq!(status, 0, "{doc:#}");
}

#[test]
fn no_valid_slot_is_a_file_with_no_root() {
    let dir = TempDir::new("fx-slot-none");
    let path = sample(&dir);
    for page in [SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B] {
        write_at(&path, page * PAGE as u64 + 20, &[0xEE; 8]);
    }
    corrupt(&path, &[], "superblock", "no superblock slot is valid");
}

#[test]
fn a_file_cut_inside_slot_b_has_lost_its_data() {
    let dir = TempDir::new("fx-slot-cut");
    let path = sample(&dir);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(PAGE as u64 + 100)
        .unwrap();
    corrupt(&path, &[], "superblock", "truncated");
}

#[test]
fn an_unknown_incompatible_feature_is_refused_like_a_newer_version() {
    let dir = TempDir::new("fx-slot-feature");
    let path = sample(&dir);
    for (page, mut sb) in [SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B]
        .into_iter()
        .zip(slots(&path))
    {
        sb.features.incompat |= 1 << 40;
        write_superblock(&path, page, &sb);
    }
    refused(&path, "incompatible feature bits");
}

#[test]
fn a_newer_format_version_is_refused() {
    let dir = TempDir::new("fx-slot-version");
    let path = sample(&dir);
    for (page, mut sb) in [SUPERBLOCK_PAGE_A, SUPERBLOCK_PAGE_B]
        .into_iter()
        .zip(slots(&path))
    {
        sb.version = FormatVersion(99);
        write_superblock(&path, page, &sb);
    }
    refused(&path, "99");
}

#[test]
fn a_wal_segment_with_an_unknown_feature_is_refused() {
    let dir = TempDir::new("fx-wal-feature");
    let path = crashed(&dir);
    let headers = segment_headers(&path);
    let last = chain(&headers).pop().unwrap();
    let mut h = last;
    h.features |= 1 << 20;
    write_segment_header(&path, slot_of(&headers, last.epoch), &h);
    refused(&path, "wal segment feature bits");
}

// Allocation.

#[test]
fn an_sst_overlapping_the_manifest_is_an_allocation_finding() {
    let dir = TempDir::new("fx-overlap");
    let path = sample(&dir);
    // A 128 KiB window (aligned to its size) holding a 64 KiB blob or manifest extent: the
    // spans differ and overlap.
    let sb = root(&path);
    let Edit::PutBlobFile { extents, .. } = blob_file(&mut edits(&path)).clone() else {
        unreachable!()
    };
    let target = extents
        .iter()
        .chain([&sb.snapshot.unwrap()])
        .find(|e| e.size_class == 0 && e.page >= 32)
        .copied()
        .expect("a 64 KiB extent past page 32");
    let page = target.page / 32 * 32;
    edit_manifest(&path, |e| {
        live_sst(e).extent = ExtentRef {
            page,
            size_class: 1,
        };
    });
    corrupt(&path, &[], "allocation", "overlaps");
}

#[test]
fn an_sst_on_exactly_a_blob_extent_is_an_allocation_finding() {
    let dir = TempDir::new("fx-overlap-same");
    let path = sample(&dir);
    edit_manifest(&path, |e| {
        let Edit::PutBlobFile { extents, .. } = blob_file(e).clone() else {
            unreachable!()
        };
        live_sst(e).extent = extents[0];
    });
    corrupt(&path, &[], "allocation", "overlaps");
}

// SSTs against the manifest.

type ManifestEdit = fn(&mut Vec<Edit>);

#[test]
fn sst_properties_that_disagree_with_the_manifest_are_corrupt() {
    let cases: [(&str, ManifestEdit); 6] = [
        ("key range", |e| live_sst(e).largest_key.push(0)),
        ("seqnos", |e| live_sst(e).seqno_range.1 += 1),
        ("timestamps", |e| live_sst(e).ts_range.0 -= 1),
        ("entries", |e| live_sst(e).entries += 1),
        ("table", |e| {
            // The other table's tablet and family.
            let u = e
                .iter()
                .find_map(|x| match x {
                    Edit::CreateTable { table, name } if name == "u" => Some(*table),
                    _ => None,
                })
                .unwrap();
            let (tablet_u, h) = (
                e.iter()
                    .find_map(|x| match x {
                        Edit::PutTablet { tablet, table, .. } if *table == u => Some(*tablet),
                        _ => None,
                    })
                    .unwrap(),
                e.iter()
                    .find_map(|x| match x {
                        Edit::PutFamily { table, family, .. } if *table == u => Some(*family),
                        _ => None,
                    })
                    .unwrap(),
            );
            for x in e.iter_mut() {
                if let Edit::AddSst { tablet, family, .. } = x {
                    (*tablet, *family) = (tablet_u, h);
                }
            }
        }),
        ("family", |e| {
            // The table's other family.
            let g = e
                .iter()
                .find_map(|x| match x {
                    Edit::PutFamily { family, name, .. } if name == "g" => Some(*family),
                    _ => None,
                })
                .unwrap();
            for x in e.iter_mut() {
                if let Edit::AddSst { family, .. } = x {
                    *family = g;
                }
            }
        }),
    ];
    for (what, f) in cases {
        let dir = TempDir::new("fx-props");
        let path = sample(&dir);
        edit_manifest(&path, f);
        corrupt(&path, &[], "sst", what);
    }
}

#[test]
fn an_sst_holding_fewer_entries_than_recorded_is_corrupt() {
    let dir = TempDir::new("fx-entries");
    let path = sample(&dir);
    let meta = live_sst(&mut edits(&path)).clone();
    // The properties and the manifest agree with each other, not with the blocks.
    edit_properties(&path, &meta, |p| p.entries += 1);
    edit_manifest(&path, |e| live_sst(e).entries += 1);
    corrupt(&path, &[], "sst", "the manifest records");
}

/// The same key with one byte changed at `at` from the end, keeping its length.
fn bumped(key: &[u8], at: usize, by: u8) -> Vec<u8> {
    let mut k = key.to_vec();
    let i = k.len() - at;
    k[i] = k[i].wrapping_add(by);
    k
}

#[test]
fn an_sst_whose_last_key_is_not_its_largest_is_corrupt() {
    let dir = TempDir::new("fx-largest");
    let path = sample(&dir);
    let meta = live_sst(&mut edits(&path)).clone();
    // A larger row (the row's last byte, before its terminator, qualifier and suffix).
    let largest = bumped(&meta.largest_key, 17 + 5, 1);
    edit_properties(&path, &meta, |p| p.largest_key = largest.clone());
    edit_manifest(&path, |e| live_sst(e).largest_key = largest.clone());
    corrupt(&path, &[], "sst", "not its recorded largest key");
}

#[test]
fn an_entry_before_the_smallest_key_is_corrupt() {
    let dir = TempDir::new("fx-smallest");
    let path = sample(&dir);
    let meta = live_sst(&mut edits(&path)).clone();
    let smallest = bumped(&meta.smallest_key, 17 + 5, 1);
    edit_properties(&path, &meta, |p| p.smallest_key = smallest.clone());
    edit_manifest(&path, |e| live_sst(e).smallest_key = smallest.clone());
    corrupt(&path, &[], "sst", "outside its key range");
}

#[test]
fn keys_out_of_order_in_a_data_block_are_corrupt() {
    let dir = TempDir::new("fx-order");
    let path = sample(&dir);
    let meta = live_sst(&mut edits(&path)).clone();
    let addr = first_data_block(&path, &meta);
    let (mut logical, _) = block(&path, &meta, addr);
    // The second entry shares the first's prefix up to the row's last digit; its unshared
    // bytes start there. A smaller digit puts it before the first.
    // The second entry starts where the first ends: the first is a restart (nothing shared),
    // so its length is three varints (0, its key's length, its value's length), then its key
    // and value (FORMAT §4.2).
    let offset = {
        let mut it = Block::new(logical.as_slice()).unwrap().into_cursor();
        it.seek_to_first().unwrap();
        let (k, v) = (it.key().len(), it.value().len());
        let mut head = Vec::new();
        for n in [0, k, v] {
            pigeonhole_format::varint::put_u64(&mut head, n as u64);
        }
        head.len() + k + v
    };
    let (shared, unshared) = (logical[offset], logical[offset + 1]);
    assert!(shared > 0 && shared < 0x80 && unshared < 0x80);
    logical[offset + 3] = b'/';
    set_block(&path, &meta, addr, BlockKind::Data, &logical);
    corrupt(&path, &[], "sst", "keys out of order");
}

// SSTs against the blob files.

fn blob_family(e: &mut [Edit]) -> &mut FamilyId {
    match blob_file(e) {
        Edit::PutBlobFile { family, .. } => family,
        _ => unreachable!(),
    }
}

#[test]
fn a_pointer_into_another_familys_blob_file_is_corrupt() {
    let dir = TempDir::new("fx-blob-family");
    let path = sample(&dir);
    edit_manifest(&path, |e| blob_family(e).0 += 1);
    corrupt(&path, &[], "sst", "not its own");
}

#[test]
fn a_pointer_into_an_unknown_blob_file_is_corrupt() {
    let dir = TempDir::new("fx-blob-unknown");
    let path = sample(&dir);
    edit_manifest(&path, |e| {
        e.retain(|x| !matches!(x, Edit::PutBlobFile { .. }));
    });
    corrupt(&path, &[], "sst", "which the manifest does not name");
}

#[test]
fn blob_references_that_disagree_with_the_manifest_are_corrupt() {
    let dir = TempDir::new("fx-blob-refs");
    let path = sample(&dir);
    edit_manifest(&path, |e| {
        for x in e.iter_mut() {
            if let Edit::SstBlobRefs { refs, .. } = x
                && let Some(r) = refs.first_mut()
            {
                r.1 += 1;
            }
        }
    });
    corrupt(&path, &[], "sst", "holds blob references");
}

#[test]
fn a_deep_check_reads_every_pointed_record() {
    let dir = TempDir::new("fx-blob-deep");
    let path = sample(&dir);
    let Edit::PutBlobFile { extents, .. } = blob_file(&mut edits(&path)).clone() else {
        unreachable!()
    };
    // Inside the first record's value: its checksum fails for the pointer's read as well.
    write_at(&path, extents[0].offset() + 64 + 16 + 5, &[0xEE]);
    corrupt(&path, &["--deep"], "sst", "pointer into blob file");
}

#[test]
fn a_deep_check_of_an_empty_family_beside_blob_files_is_clean() {
    let dir = TempDir::new("fx-deep-clean");
    let path = sample(&dir);
    let (status, doc) = check(&path, &["--deep"]);
    assert_eq!(status, 0, "{doc:#}");
    assert!(doc["blob_pointers"].as_u64().unwrap() > 0, "{doc:#}");
}

#[test]
fn live_bytes_that_disagree_with_the_pointers_are_a_warning() {
    let dir = TempDir::new("fx-live");
    let path = sample(&dir);
    edit_manifest(&path, |e| {
        if let Edit::PutBlobFile { live_bytes, .. } = blob_file(e) {
            *live_bytes += 1;
        }
    });
    let (status, doc) = check(&path, &["--deep"]);
    has(&doc, "blob", "warning", "live bytes");
    assert_eq!(status, 0, "{doc:#}");
}

// Blob files.

fn edit_blob_file(path: &Path, f: impl FnOnce(&mut BlobFileId, &mut Vec<ExtentRef>, &mut u64)) {
    edit_manifest(path, |e| {
        if let Edit::PutBlobFile {
            blob_file,
            extents,
            total_bytes,
            ..
        } = blob_file(e)
        {
            f(blob_file, extents, total_bytes);
        }
    });
}

#[test]
fn a_blob_file_with_bytes_but_no_extents_is_corrupt() {
    let dir = TempDir::new("fx-blob-noext");
    let path = sample(&dir);
    edit_blob_file(&path, |_, extents, _| extents.clear());
    corrupt(&path, &[], "blob", "but no extents");
}

#[test]
fn a_blob_file_with_mixed_extent_sizes_is_corrupt() {
    let dir = TempDir::new("fx-blob-mixed");
    let path = sample(&dir);
    edit_blob_file(&path, |_, extents, _| {
        extents.push(ExtentRef {
            page: 1 << 12,
            size_class: extents[0].size_class + 1,
        });
    });
    corrupt(&path, &[], "blob", "another size than the first");
}

#[test]
fn a_blob_extent_past_the_end_of_the_file_is_an_allocation_finding() {
    let dir = TempDir::new("fx-blob-eof");
    let path = sample(&dir);
    edit_blob_file(&path, |_, extents, _| {
        let e = extents[0];
        extents.push(ExtentRef {
            page: 1 << 12,
            size_class: e.size_class,
        });
    });
    corrupt(&path, &[], "allocation", "past the end of the file");
}

#[test]
fn a_blob_extent_whose_header_names_another_file_is_corrupt() {
    let dir = TempDir::new("fx-blob-renamed");
    let path = sample(&dir);
    edit_blob_file(&path, |id, _, _| id.0 += 7);
    corrupt(&path, &[], "blob", "names another file or position");
}

#[test]
fn blob_bytes_past_the_extents_capacity_are_corrupt() {
    let dir = TempDir::new("fx-blob-capacity");
    let path = sample(&dir);
    edit_blob_file(&path, |_, extents, total| {
        *total = extents.len() as u64 * (extents[0].len() - 64) + 1;
    });
    corrupt(&path, &[], "blob", "in extents holding");
}

#[test]
fn a_blob_record_cut_by_the_recorded_length_is_corrupt() {
    let dir = TempDir::new("fx-blob-short");
    let path = sample(&dir);
    edit_blob_file(&path, |_, _, total| *total -= 1);
    corrupt(&path, &[], "blob", "past its end");
}

#[test]
fn a_record_header_past_the_recorded_length_is_corrupt() {
    let dir = TempDir::new("fx-blob-header");
    let path = sample(&dir);
    edit_blob_file(&path, |_, _, total| *total += 8);
    corrupt(&path, &[], "blob", "runs past its");
}

// The WAL of a crashed database: three or more 256 KiB segments.

#[test]
fn segments_of_another_database_or_stream_are_corrupt() {
    for (what, f) in [
        (
            "of another database",
            (|h: &mut SegmentHeader| h.db_id[0] ^= 1) as fn(&mut _),
        ),
        ("names stream", |h| h.stream = StreamId(h.stream.0 + 5)),
    ] {
        let dir = TempDir::new("fx-wal-foreign");
        let path = crashed(&dir);
        let headers = segment_headers(&path);
        let last = chain(&headers).pop().unwrap();
        let mut h = last;
        f(&mut h);
        write_segment_header(&path, slot_of(&headers, last.epoch), &h);
        corrupt(&path, &[], "wal", what);
    }
}

#[test]
fn a_segment_after_a_blank_header_is_unreachable() {
    let dir = TempDir::new("fx-wal-unreachable");
    let path = crashed(&dir);
    let headers = segment_headers(&path);
    let c = chain(&headers);
    assert!(c.len() >= 3, "{c:?}");
    let middle = slot_of(&headers, c[1].epoch);
    write_at(&wal_path(&path), middle * SEGMENT, &[0u8; 128]);
    corrupt(&path, &[], "wal", "is not reachable from the checkpoint");
}

#[test]
fn a_segment_with_two_successors_is_corrupt() {
    let dir = TempDir::new("fx-wal-fork");
    let path = crashed(&dir);
    let headers = segment_headers(&path);
    let c = chain(&headers);
    let mut third = c[2];
    third.prev_epoch = c[0].epoch;
    write_segment_header(&path, slot_of(&headers, c[2].epoch), &third);
    corrupt(&path, &[], "wal", "has several successors");
}

#[test]
fn a_segment_chain_that_loops_is_corrupt() {
    let dir = TempDir::new("fx-wal-loop");
    let path = crashed(&dir);
    let (_, doc) = check(&path, &[]);
    let end = &doc["wal"][0]["end"];
    let (end_epoch, end_offset) = (
        end["epoch"].as_u64().unwrap(),
        end["offset"].as_u64().unwrap(),
    );
    let headers = segment_headers(&path);
    let c = chain(&headers);
    // The first segment claims to continue the last one at its end.
    let mut first = c[0];
    first.prev_epoch = end_epoch as u32;
    first.prev_end = end_offset as u32;
    write_segment_header(&path, slot_of(&headers, c[0].epoch), &first);
    corrupt(&path, &[], "wal", "the segment chain loops");
}

/// The first fragment of the first record in `epoch`'s segment: its offset in the WAL file.
fn first_fragment(path: &Path, epoch: u32) -> u64 {
    slot_of(&segment_headers(path), epoch) * SEGMENT + FRAME_SIZE as u64
}

/// Overwrites the payload of the fragment at `at` with `fill`, its CRC recomputed.
fn reseal_fragment(path: &Path, at: u64, fill: u8) {
    let wal = wal_path(path);
    let mut h = read_at(&wal, at, FRAGMENT_HEADER_LEN);
    let len = usize::from(u16::from_le_bytes([h[8], h[9]]));
    let payload = vec![fill; len];
    let crc = pigeonhole_format::checksum::crc32c_append(
        pigeonhole_format::checksum::crc32c(&h[4..]),
        &payload,
    );
    h[..4].copy_from_slice(&crc.to_le_bytes());
    write_at(&wal, at, &[h, payload].concat());
}

#[test]
fn a_record_with_a_valid_checksum_that_does_not_decode_is_corrupt() {
    let dir = TempDir::new("fx-wal-record");
    let path = crashed(&dir);
    let first = chain(&segment_headers(&path))[0].epoch;
    reseal_fragment(&path, first_fragment(&path, first), 0xEE);
    corrupt(
        &path,
        &[],
        "wal",
        "has a valid checksum but does not decode",
    );
}

#[test]
fn a_missing_checkpoint_segment_with_later_segments_is_corrupt() {
    let dir = TempDir::new("fx-wal-ckpt");
    let path = crashed(&dir);
    let c = chain(&segment_headers(&path));
    // A checkpoint inside the second segment, which is then blanked: no segment continues
    // at that offset, and the third is newer.
    edit_manifest(&path, |e| {
        e.retain(|x| !matches!(x, Edit::WalCheckpoint { .. }));
        e.push(Edit::WalCheckpoint {
            stream: StreamId(0),
            lsn: Lsn::new(c[1].epoch, 7),
        });
    });
    // Blank the checkpoint's own segment: later ones remain.
    write_at(
        &wal_path(&path),
        slot_of(&segment_headers(&path), c[1].epoch) * SEGMENT,
        &[0u8; 128],
    );
    corrupt(&path, &[], "wal", "is missing and later segments exist");
}

#[test]
fn a_missing_checkpoint_segment_continued_by_its_successor_is_clean() {
    let dir = TempDir::new("fx-wal-ckpt-chained");
    let path = crashed(&dir);
    let headers = segment_headers(&path);
    let c = chain(&headers);
    // The checkpoint is where the second segment continues the first, and the first never
    // reached the disk: replay starts in the second.
    edit_manifest(&path, |e| {
        e.retain(|x| !matches!(x, Edit::WalCheckpoint { .. }));
        e.push(Edit::WalCheckpoint {
            stream: StreamId(0),
            lsn: Lsn::new(c[1].prev_epoch, c[1].prev_end),
        });
    });
    write_at(
        &wal_path(&path),
        slot_of(&headers, c[0].epoch) * SEGMENT,
        &[0u8; 128],
    );
    let (status, doc) = check(&path, &[]);
    assert!(
        !findings(&doc).iter().any(|f| f["severity"] == "corruption"),
        "{doc:#}"
    );
    assert_eq!(status, 0, "{doc:#}");
}

#[test]
fn no_segment_header_but_a_checkpoint_epoch_is_corrupt() {
    let dir = TempDir::new("fx-wal-blank");
    let path = crashed(&dir);
    let headers = segment_headers(&path);
    let c = chain(&headers);
    edit_manifest(&path, |e| {
        e.retain(|x| !matches!(x, Edit::WalCheckpoint { .. }));
        e.push(Edit::WalCheckpoint {
            stream: StreamId(0),
            lsn: Lsn::new(c[0].epoch, FRAME_SIZE as u32),
        });
    });
    for slot in 0..headers.len() as u64 {
        write_at(&wal_path(&path), slot * SEGMENT, &[0u8; 128]);
    }
    corrupt(&path, &[], "wal", "no valid segment header");
}

#[test]
fn records_after_a_damaged_fragment_in_the_last_segment_are_a_warning() {
    let dir = TempDir::new("fx-wal-gap");
    let path = crashed(&dir);
    let last = chain(&segment_headers(&path)).pop().unwrap().epoch;
    // A torn first fragment: its CRC fails, the records after it are intact.
    let at = first_fragment(&path, last);
    let wal = wal_path(&path);
    let b = read_at(&wal, at + FRAGMENT_HEADER_LEN as u64, 1)[0];
    write_at(&wal, at + FRAGMENT_HEADER_LEN as u64, &[b ^ 0xFF]);
    let (status, doc) = check(&path, &[]);
    has(&doc, "wal", "warning", "but valid records follow");
    assert_eq!(status, 0, "{doc:#}");
}

#[test]
fn a_torn_last_record_is_info() {
    let dir = TempDir::new("fx-wal-torn");
    let path = crashed(&dir);
    let (_, doc) = check(&path, &[]);
    let end = &doc["wal"][0]["end"];
    let (epoch, offset) = (
        end["epoch"].as_u64().unwrap() as u32,
        end["offset"].as_u64().unwrap(),
    );
    // A fragment header of this epoch whose checksum fails, at the end of the data.
    let at = slot_of(&segment_headers(&path), epoch) * SEGMENT + offset;
    let mut h = [0u8; FRAGMENT_HEADER_LEN];
    h[4..8].copy_from_slice(&epoch.to_le_bytes());
    h[8] = 4;
    h[10] = 1;
    write_at(&wal_path(&path), at, &[&h[..], b"torn"].concat());
    let (status, doc) = check(&path, &[]);
    has(&doc, "wal", "info", "ends in a torn write");
    assert_eq!(status, 0, "{doc:#}");
    // The table output names the severity.
    let out = phdb(&["check", path.to_str().unwrap()]);
    assert!(out.stdout.contains("info"), "{out:?}");
}

#[test]
fn a_file_that_is_not_a_database_is_an_error_not_a_report() {
    let dir = TempDir::new("fx-not-db");
    let path = dir.path().join("x.phdb");
    std::fs::write(&path, vec![0x5A; 3 * PAGE]).unwrap();
    // Neither slot has the magic: no root, reported as such.
    let doc = corrupt(&path, &[], "superblock", "bad magic");
    assert_eq!(doc["superblocks"], 0);
}

#[test]
fn a_deep_check_skips_an_sst_it_cannot_read_for_live_bytes() {
    // A damaged first data block: the SST opens, its first seek fails. A damaged last one:
    // the scan fails part way. Either is an SST finding; the live-bytes pass skips the SST.
    for last in [false, true] {
        let dir = TempDir::new("fx-deep-damaged");
        let path = sample(&dir);
        let meta = live_sst(&mut edits(&path)).clone();
        let blocks = data_blocks(&path, &meta);
        assert!(blocks.len() >= 2, "{blocks:?}");
        let addr = if last {
            blocks[blocks.len() - 1]
        } else {
            blocks[0]
        };
        write_at(&path, meta.extent.offset() + addr.offset + 5, &[0xEE]);
        corrupt(&path, &["--deep"], "sst", "SST");
    }
}

/// Offsets (in the WAL file) of the fragments in frame 1 of `epoch`'s segment.
fn fragments_in_frame_one(path: &Path, epoch: u32) -> Vec<u64> {
    let base = first_fragment(path, epoch);
    let frame = read_at(&wal_path(path), base, FRAME_SIZE);
    let mut at = 0;
    let mut out = Vec::new();
    while at + FRAGMENT_HEADER_LEN <= FRAME_SIZE {
        let h = &frame[at..at + FRAGMENT_HEADER_LEN];
        if h.iter().all(|&b| b == 0) {
            break;
        }
        out.push(base + at as u64);
        at += FRAGMENT_HEADER_LEN + usize::from(u16::from_le_bytes([h[8], h[9]]));
    }
    out
}

#[test]
fn records_in_a_later_frame_after_a_damaged_last_fragment_are_a_warning() {
    let dir = TempDir::new("fx-wal-gap-frame");
    let path = crashed(&dir);
    let last = chain(&segment_headers(&path)).pop().unwrap().epoch;
    // The last fragment of frame 1 continues in frame 2; damaged, nothing valid follows it
    // in its frame, and frame 2 starts with the rest of its record.
    let at = *fragments_in_frame_one(&path, last).last().unwrap();
    let wal = wal_path(&path);
    let b = read_at(&wal, at + FRAGMENT_HEADER_LEN as u64, 1)[0];
    write_at(&wal, at + FRAGMENT_HEADER_LEN as u64, &[b ^ 0xFF]);
    let (status, doc) = check(&path, &[]);
    has(&doc, "wal", "warning", "but valid records follow");
    assert_eq!(status, 0, "{doc:#}");
}

#[test]
fn records_a_recovery_already_dropped_are_info() {
    let dir = TempDir::new("fx-wal-resolved");
    let path = crashed(&dir);
    let headers = segment_headers(&path);
    let c = chain(&headers);
    // Damage at the start of the second segment's data, and the third chained right there,
    // as a recovery chains a new segment at the stop.
    let at = first_fragment(&path, c[1].epoch);
    let wal = wal_path(&path);
    let b = read_at(&wal, at + FRAGMENT_HEADER_LEN as u64, 1)[0];
    write_at(&wal, at + FRAGMENT_HEADER_LEN as u64, &[b ^ 0xFF]);
    let mut third = c[2];
    third.prev_end = FRAME_SIZE as u32;
    write_segment_header(&path, slot_of(&headers, c[2].epoch), &third);
    let (status, doc) = check(&path, &[]);
    has(&doc, "wal", "info", "a recovery already continued the log");
    assert_eq!(status, 0, "{doc:#}");
}
