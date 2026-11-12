//! `phdb cluster move P --to NEW [--drop-source]` (D317 ticket 6, ICR 0117): a partition's
//! planned move, with its files copied whole, so its change-feed history and tokens carry
//! over.
//!
//! 1. **Fence and close** P on the node serving it (OLD), then wait for the close's hash of
//!    its files: the main file, every WAL file a clean close keeps, and the feed's.
//! 2. **Copy** each file from OLD (`Admin.BackupDownload`'s `fenced_file`) into an upload
//!    on NEW (`Admin.PartitionUpload`, `RELOCATE`, the epoch `e` the copy is of). Each
//!    upload's id comes from the file's name and hash, so a rerun resumes what arrived.
//! 3. **Stamp** NEW from `e` to `e + 1` and compare the set it installed with OLD's closed
//!    set, file by file: name, size and SHA-256. A mismatch stops the move before the map
//!    changes, so OLD keeps P.
//! 4. **The map:** `relocate --copy`, which keeps P's `history_epoch`. Its stamp is then a
//!    no-op.
//! 5. **Serving:** wait for NEW to serve P at `e + 1`.
//! 6. **`--drop-source`:** only after a comparison that matched, OLD's files move aside
//!    (`drop-partition`). Nothing is deleted.
//!
//! A rerun after the stamp (the move died between the stamp and the end) finds P stamped on
//! NEW. The stamp then installs nothing, so there is no set to compare: the move finishes,
//! says the copy isn't re-verified, and drops nothing. The `drop-partition` command is in
//! its report.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use prost::bytes::Bytes;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::cli::{NodeArgs, channel, connect, relocate, report, request, url_of};
use crate::exit::Failure;
use crate::output::Report;
use crate::serve::proto::{
    self, BackupDownloadRequest, GetMapRequest, LocalPartition, LocalPartitionState, PartitionFile,
    PartitionRequest, PartitionUploadChunk, PartitionUploadHeader, PartitionUploadPurpose,
    PartitionUploadStatusRequest, StatusRequest,
};

/// How often the move asks a node how a step is going.
const POLL: Duration = Duration::from_millis(200);

/// How often a wait says it's still waiting.
const SAY_EVERY: Duration = Duration::from_secs(10);

/// A chunk's size when a node doesn't say (`max_chunk` 0, a node from before it).
const DEFAULT_CHUNK: u64 = 512 << 10;

type Admin = proto::admin_client::AdminClient<tonic::transport::Channel>;

/// A node's channel, as the move uses it.
struct At {
    id: String,
    args: NodeArgs,
    token: Option<String>,
    admin: Admin,
}

impl At {
    async fn new(args: &NodeArgs, id: &str, address: &str) -> Result<Self, Failure> {
        let args = NodeArgs {
            node: url_of(args, address),
            ..args.clone()
        };
        let (ch, token) = channel(&args, &args.node).await?;
        Ok(Self {
            id: id.to_owned(),
            admin: Admin::new(ch).max_decoding_message_size(4 << 20),
            args,
            token,
        })
    }

    fn failed(&self, what: &str, s: &tonic::Status) -> Failure {
        Failure::usage(&format!(
            "node {:?} ({}): {what}: {}",
            self.id,
            self.args.node,
            s.message()
        ))
    }

    /// Partition `p`'s line in the node's `Status`, if it lists it.
    async fn local(&self, p: &str) -> Result<Option<LocalPartition>, Failure> {
        let (mut c, _) = connect(&self.args, &self.args.node).await?;
        let st = c
            .status(request(StatusRequest {}, self.token.as_deref())?)
            .await
            .map_err(|s| self.failed("status", &s))?
            .into_inner();
        Ok(st.partitions.into_iter().find(|lp| lp.id == p))
    }

    /// Fence, close or drop partition `p` here.
    async fn call(&self, p: &str, what: &'static str) -> Result<LocalPartition, Failure> {
        let (mut c, _) = connect(&self.args, &self.args.node).await?;
        let req = request(
            PartitionRequest {
                partition: p.to_owned(),
                force: false,
            },
            self.token.as_deref(),
        )?;
        let reply = match what {
            "fence" => c.fence(req).await,
            "close" => c.close(req).await,
            _ => c.drop_partition(req).await,
        };
        Ok(reply.map_err(|s| self.failed(what, &s))?.into_inner())
    }
}

/// Says every [`SAY_EVERY`] that a wait goes on.
struct Waiting {
    what: String,
    since: Instant,
    said: Instant,
}

impl Waiting {
    fn new(what: String) -> Self {
        let now = Instant::now();
        Self {
            what,
            since: now,
            said: now,
        }
    }

    async fn tick(&mut self, now: &str) {
        if self.said.elapsed() >= SAY_EVERY {
            self.said = Instant::now();
            eprintln!(
                "phdb cluster move: {} ({}s){}",
                self.what,
                self.since.elapsed().as_secs(),
                if now.is_empty() {
                    String::new()
                } else {
                    format!(": {now}")
                }
            );
        }
        tokio::time::sleep(POLL).await;
    }
}

/// `phdb cluster move` (the module docs).
pub(super) async fn run(
    args: &NodeArgs,
    partition: &str,
    to: &str,
    drop_source: bool,
) -> Result<Report, Failure> {
    let (mut c, token) = connect(args, &args.node).await?;
    let m = c
        .get_map(request(GetMapRequest {}, token.as_deref())?)
        .await
        .map_err(|s| Failure::usage(&format!("{}: {}", args.node, s.message())))?
        .into_inner();
    let Some(p) = m.partitions.iter().find(|p| p.id == partition).cloned() else {
        return Err(Failure::usage(&format!(
            "the map (version {}) has no partition {partition:?}",
            m.map_version
        )));
    };
    let address = |id: &str| {
        m.nodes
            .iter()
            .find(|n| n.id == id)
            .map(|n| n.address.clone())
    };
    let Some(new_address) = address(to) else {
        return Err(Failure::usage(&format!("the map has no node {to:?}")));
    };
    if p.node == to {
        // An earlier move finished, or P was never elsewhere: nothing to copy or compare.
        let mut r = report(
            "cluster move",
            vec![
                ("partition", json!(partition)),
                ("node", json!(to)),
                ("epoch", json!(p.epoch)),
                ("verified", json!(false)),
                (
                    "done",
                    json!(format!(
                        "the map already gives {partition} to node {to}: nothing moved, and \
                         nothing was dropped"
                    )),
                ),
            ],
            Value::Null,
        );
        r.status = crate::ExitStatus::SUCCESS;
        return Ok(r);
    }
    let old_address = address(&p.node).unwrap_or_default();
    let old = At::new(args, &p.node, &old_address).await?;
    let new = At::new(args, to, &new_address).await?;
    let e = p.epoch;

    // 1. Fenced and closed on OLD, its files hashed.
    let started = Instant::now();
    let unreachable = |f: Failure| {
        Failure::usage(&format!(
            "{} (a move copies the files of the node serving {partition}: if its process is \
             stopped, promote another node with phdb cluster relocate --old-unreachable, \
             which starts a new feed history)",
            f.message()
        ))
    };
    let lp = old
        .local(partition)
        .await
        .map_err(unreachable)?
        .ok_or_else(|| {
            Failure::usage(&format!(
                "node {:?} doesn't list partition {partition}",
                old.id
            ))
        })?;
    let mut fenced_here = None;
    if lp.state() == LocalPartitionState::Serving {
        old.call(partition, "fence").await?;
        fenced_here = Some(Instant::now());
    }
    let closed_at = Instant::now();
    if lp.state() != LocalPartitionState::Closed {
        old.call(partition, "close").await?;
    }
    let files = closed_files(&old, partition).await?;

    // 2 and 3. Copied and stamped, unless an earlier run stamped it already.
    let stamped_earlier = new
        .local(partition)
        .await?
        .is_some_and(|lp| lp.epoch == e + 1);
    let mut sent = Vec::new();
    let verified = if stamped_earlier {
        false
    } else {
        for f in &files {
            sent.push(copy_file(&old, &new, &m.cluster_id, partition, e, f, &files).await?);
        }
        let installed = stamp(&new, &m.cluster_id, partition, e).await?;
        if installed.is_empty() {
            // Stamped meanwhile by another run: nothing of this one's to compare.
            false
        } else {
            compare(&installed, &files).map_err(|why| {
                Failure::usage(&format!(
                    "node {to:?} installed files that aren't the ones node {:?} closed \
                     ({why}). The map still gives {partition} to {:?} at epoch {e}: check \
                     {to:?}'s files; phdb cluster unfence {partition} on {:?} serves it there \
                     again",
                    old.id, old.id, old.id
                ))
            })?;
            true
        }
    };

    // 4. The map, marked as copied.
    let set = relocate(args, partition, to, false, true).await?;
    let map_at = Instant::now();

    // 5. Serving on NEW.
    let mut wait = Waiting::new(format!(
        "waiting for node {to:?} to serve {partition} at epoch {}",
        e + 1
    ));
    loop {
        match new.local(partition).await {
            // A node serves P only at the map's epoch for it, which is `e + 1` now. (`Status`
            // can't read an open file's epoch: its read-only open is refused.)
            Ok(Some(lp)) if lp.state() == LocalPartitionState::Serving => break,
            Ok(Some(lp)) => wait.tick(&lp.error).await,
            Ok(None) => wait.tick("not listed yet").await,
            Err(f) => wait.tick(f.message()).await,
        }
    }
    let serving_at = Instant::now();

    // 6. OLD's files aside, only after a comparison that matched.
    let drop_command = format!(
        "phdb cluster drop-partition {partition} --node {}",
        old.args.node
    );
    let dropped = if drop_source && verified {
        Some(old.call(partition, "drop").await?)
    } else {
        None
    };

    let bytes: u64 = files.iter().map(|f| f.bytes).sum();
    let ms = |d: Duration| json!(u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
    let mut summary = vec![
        ("partition", json!(partition)),
        ("from", json!(old.id)),
        ("node", json!(to)),
        ("epoch", json!(e + 1)),
        ("files", json!(files.len())),
        ("bytes", json!(bytes)),
        ("verified", json!(verified)),
        (
            // Writes answer `Fenced` from the fence until the map gives NEW the partition.
            "writes_down_ms",
            fenced_here.map_or(Value::Null, |at| ms(map_at - at)),
        ),
        ("reads_down_ms", ms(serving_at - closed_at)),
        ("took_ms", ms(started.elapsed())),
        (
            "map_version",
            set.document
                .get("map_version")
                .cloned()
                .unwrap_or(Value::Null),
        ),
    ];
    summary.push((
        "source",
        match (&dropped, verified) {
            (Some(lp), _) => json!(format!("moved aside on node {}: {}", old.id, lp.file)),
            (None, true) => json!(format!("kept on node {}: {drop_command}", old.id)),
            (None, false) => json!(format!(
                "kept on node {}: copied by an earlier run, not re-verified; check node {to}'s \
                 copy, then {drop_command}",
                old.id
            )),
        },
    ));
    let mut r = report("cluster move", summary, Value::Null);
    r.document.insert(
        "copied".into(),
        json!(
            files
                .iter()
                .enumerate()
                .map(|(i, f)| json!({
                    "name": f.name,
                    "bytes": f.bytes,
                    "sent": sent.get(i).copied().unwrap_or(0),
                }))
                .collect::<Vec<_>>()
        ),
    );
    if let Some(lp) = dropped {
        r.document.insert(
            "dropped".into(),
            serde_json::to_value(&lp).expect("LocalPartition serializes"),
        );
    }
    Ok(r)
}

/// OLD's closed files once their hash is done: a failed hash is retried by closing again.
async fn closed_files(old: &At, p: &str) -> Result<Vec<PartitionFile>, Failure> {
    let mut wait = Waiting::new(format!(
        "waiting for node {:?} to hash {p}'s closed files",
        old.id
    ));
    loop {
        let lp = old
            .local(p)
            .await?
            .ok_or_else(|| Failure::usage(&format!("node {:?} no longer lists {p}", old.id)))?;
        if lp.state() != LocalPartitionState::Closed {
            return Err(Failure::usage(&format!(
                "partition {p} isn't closed on node {:?} any more ({}): something reopened it; \
                 run the move again",
                old.id,
                super::cli::state_text(&lp)
            )));
        }
        if !lp.files.is_empty() {
            if !lp.ok {
                // Its database's own close reported an error: its files may not be whole.
                return Err(Failure::usage(&format!(
                    "partition {p} closed on node {:?}, but: {}. Check it; phdb cluster \
                     unfence {p} and close it again to retry",
                    old.id, lp.error
                )));
            }
            return Ok(lp.files);
        }
        if lp.error.contains("close it again") {
            old.call(p, "close").await?;
        }
        wait.tick(&lp.error).await;
    }
}

/// The upload id of file `f` of partition `p`'s copy at epoch `e`: the same for a rerun,
/// and another for different bytes.
fn upload_id(cluster: &str, p: &str, e: u64, f: &PartitionFile) -> String {
    let mut h = Sha256::new();
    for part in [cluster.as_bytes(), p.as_bytes(), f.name.as_bytes()] {
        h.update((part.len() as u64).to_le_bytes());
        h.update(part);
    }
    h.update(e.to_le_bytes());
    h.update(&f.sha256);
    h.finalize()[..16]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// An upload's messages, as tonic's client stream.
struct Chunks(tokio::sync::mpsc::Receiver<PartitionUploadChunk>);

impl futures_core::Stream for Chunks {
    type Item = PartitionUploadChunk;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

/// Copies file `f` from OLD to an upload on NEW, from what NEW holds of it. Returns the
/// bytes sent.
async fn copy_file(
    old: &At,
    new: &At,
    cluster: &str,
    p: &str,
    e: u64,
    f: &PartitionFile,
    set: &[PartitionFile],
) -> Result<u64, Failure> {
    let id = upload_id(cluster, p, e, f);
    let what = format!("{p}'s file {:?}", f.name);
    let st = new
        .admin
        .clone()
        .partition_upload_status(request(
            PartitionUploadStatusRequest {
                upload_id: id.clone(),
            },
            new.token.as_deref(),
        )?)
        .await
        .map_err(|s| new.failed(&format!("upload status of {what}"), &s))?
        .into_inner();
    if st.complete {
        return Ok(0);
    }
    let from = st.received;
    let max = usize::try_from(if st.max_chunk == 0 {
        DEFAULT_CHUNK
    } else {
        st.max_chunk
    })
    .unwrap_or(usize::MAX);
    let header = PartitionUploadHeader {
        partition: p.to_owned(),
        cluster_id: cluster.to_owned(),
        epoch: e,
        upload_id: id,
        purpose: PartitionUploadPurpose::Relocate as i32,
        file: Some(f.clone()),
        manifest: set.to_vec(),
    };
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let download = async {
        // Owned here, so the upload's stream ends when the download does.
        let tx = tx;
        let mut chunks = old
            .admin
            .clone()
            .backup_download(request(
                BackupDownloadRequest {
                    partition: p.to_owned(),
                    fenced_file: Some(f.name.clone()),
                    offset: from,
                    ..Default::default()
                },
                old.token.as_deref(),
            )?)
            .await
            .map_err(|s| old.failed(&format!("download {what}"), &s))?
            .into_inner();
        let mut at = from;
        let mut header = Some(header);
        let gone = || Failure::usage(&format!("node {:?} ended the upload of {what}", new.id));
        loop {
            let Some(chunk) = chunks
                .message()
                .await
                .map_err(|s| old.failed(&format!("download {what}"), &s))?
            else {
                return Err(Failure::usage(&format!(
                    "node {:?}'s download of {what} ended early: run the move again to resume",
                    old.id
                )));
            };
            if !chunk.data.is_empty() && chunk.offset != at {
                return Err(Failure::usage(&format!(
                    "node {:?} sent {what} from {} where {at} was expected",
                    old.id, chunk.offset
                )));
            }
            let data: Bytes = chunk.data;
            let mut k = 0;
            // At least one message, so a file with nothing left to send still carries its
            // header (an empty file; or every byte arrived but the upload isn't complete).
            while k < data.len() || header.is_some() {
                let n = (data.len() - k).min(max);
                tx.send(PartitionUploadChunk {
                    header: header.take(),
                    offset: at,
                    data: data.slice(k..k + n),
                })
                .await
                .map_err(|_| gone())?;
                k += n;
                at += n as u64;
            }
            if chunk.total != 0 {
                if chunk.total != f.bytes || chunk.sha256 != f.sha256 {
                    return Err(Failure::usage(&format!(
                        "node {:?} sent {what} as {} bytes with another SHA-256 than its \
                         close listed ({} bytes): close it again and rerun the move",
                        old.id, chunk.total, f.bytes
                    )));
                }
                return Ok(at - from);
            }
        }
    };
    let upload = async {
        new.admin
            .clone()
            .partition_upload(request(Chunks(rx), new.token.as_deref())?)
            .await
            .map_err(|s| new.failed(&format!("upload {what}"), &s))
            .map(tonic::Response::into_inner)
    };
    let (sent, uploaded) = tokio::join!(download, upload);
    let sent = sent?;
    let reply = uploaded?;
    if !reply.complete {
        return Err(Failure::usage(&format!(
            "node {:?} holds {} of {what}'s {} bytes, not complete: run the move again to \
             resume",
            new.id, reply.received, f.bytes
        )));
    }
    Ok(sent)
}

/// Stamps NEW from `e` to `e + 1`: the set it installed, or none if it was stamped already.
async fn stamp(new: &At, cluster: &str, p: &str, e: u64) -> Result<Vec<PartitionFile>, Failure> {
    let (mut c, _) = connect(&new.args, &new.args.node).await?;
    Ok(c.stamp(request(
        proto::StampRequest {
            cluster_id: cluster.to_owned(),
            partition: p.to_owned(),
            from_epoch: e,
            to_epoch: e + 1,
            restore: false,
            restore_from_cluster_id: String::new(),
        },
        new.token.as_deref(),
    )?)
    .await
    .map_err(|s| new.failed("stamp", &s))?
    .into_inner()
    .files)
}

/// Whether the set NEW installed is OLD's closed set: the same names, each with the same
/// size and SHA-256. The difference otherwise.
fn compare(installed: &[PartitionFile], closed: &[PartitionFile]) -> Result<(), String> {
    let key = |f: &PartitionFile| (f.name.clone(), f.bytes, f.sha256.clone());
    let mut a: Vec<_> = installed.iter().map(key).collect();
    let mut b: Vec<_> = closed.iter().map(key).collect();
    a.sort();
    b.sort();
    if a == b {
        return Ok(());
    }
    let names = |v: &[(String, u64, Bytes)], w: &[(String, u64, Bytes)]| -> String {
        v.iter()
            .filter(|x| !w.contains(x))
            .map(|(n, bytes, _)| format!("{n:?} ({bytes} bytes)"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    Err(format!(
        "installed but not closed: [{}]; closed but not installed: [{}]",
        names(&a, &b),
        names(&b, &a)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(name: &str, bytes: u64, sha: u8) -> PartitionFile {
        PartitionFile {
            name: name.into(),
            bytes,
            sha256: vec![sha; 32].into(),
        }
    }

    #[test]
    fn the_installed_set_must_be_the_closed_set_file_by_file() {
        let closed = [file("", 10, 1), file("-wal-0", 20, 2), file("-feed", 30, 3)];
        // Any order.
        let mut installed = closed.to_vec();
        installed.reverse();
        assert!(compare(&installed, &closed).is_ok());
        // A different hash, a different size, a missing or an extra file: each refused.
        for bad in [
            vec![file("", 10, 1), file("-wal-0", 20, 9), file("-feed", 30, 3)],
            vec![file("", 11, 1), file("-wal-0", 20, 2), file("-feed", 30, 3)],
            vec![file("", 10, 1), file("-wal-0", 20, 2)],
            vec![
                file("", 10, 1),
                file("-wal-0", 20, 2),
                file("-feed", 30, 3),
                file("-wal-1", 1, 4),
            ],
        ] {
            let why = compare(&bad, &closed).unwrap_err();
            assert!(why.contains("installed but not closed"), "{why}");
        }
        let why = compare(&[file("", 10, 1)], &closed).unwrap_err();
        assert!(
            why.contains("\"-feed\" (30 bytes)"),
            "names what's missing: {why}"
        );
    }

    #[test]
    fn an_upload_id_is_the_same_for_a_rerun_and_another_for_other_bytes() {
        let f = file("-wal-0", 20, 2);
        let id = upload_id("c1", "p1", 4, &f);
        assert_eq!(id.len(), 32);
        assert!(
            id.bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        );
        assert_eq!(id, upload_id("c1", "p1", 4, &f));
        for other in [
            upload_id("c2", "p1", 4, &f),
            upload_id("c1", "p2", 4, &f),
            upload_id("c1", "p1", 5, &f),
            upload_id("c1", "p1", 4, &file("-wal-1", 20, 2)),
            upload_id("c1", "p1", 4, &file("-wal-0", 20, 3)),
        ] {
            assert_ne!(id, other);
        }
    }
}
