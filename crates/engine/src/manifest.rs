//! The single manifest writer: a snapshot block plus a delta log in the page file (decision
//! D7, FORMAT §9). Reads at open, appends a delta per commit, rewrites the snapshot when the
//! log is full or outgrows it.

use std::sync::Arc;

use pigeonhole_format::manifest::{
    Edit, LOG_EXTENT_LEN, MANIFEST_HEADER_LEN, ManifestBlockKind, ManifestHeader, decode_block,
    encode_block,
};
use pigeonhole_format::{FormatVersion, ManifestVersion};
use pigeonhole_io::FileRef;
use pigeonhole_pager::{Extent, OpenedPager, Pager, Root};

use crate::catalog::Catalog;
use crate::{Error, Result};

/// Reads the manifest named by `opened`'s root and rebuilds the catalog from it. Returns the
/// catalog and the live manifest extents (snapshot and log).
pub(crate) fn load(opened: &OpenedPager, shards: usize) -> Result<(Catalog, Vec<Extent>)> {
    let root = opened.root();
    let file = opened.file();
    let mut catalog = Catalog::default();
    let mut live = Vec::new();
    if let Some(snapshot) = root.snapshot {
        live.push(snapshot);
        let bytes = read(file, snapshot, 0, root.snapshot_len as usize)?;
        let (header, edits, used) = decode_block(&bytes)?;
        if header.kind != ManifestBlockKind::Snapshot || used != bytes.len() {
            return Err(Error::Corruption("manifest snapshot block".to_owned()));
        }
        for e in &edits {
            catalog.apply(e, shards)?;
        }
        let mut version = header.manifest_version;
        if let Some(log) = root.log {
            live.push(log);
            let log_bytes = read(file, log, 0, root.log_len as usize)?;
            let mut at = 0;
            while at < log_bytes.len() {
                let (header, edits, used) = decode_block(&log_bytes[at..])?;
                if header.kind != ManifestBlockKind::Delta || header.manifest_version != version + 1
                {
                    return Err(Error::Corruption("manifest delta log".to_owned()));
                }
                version = header.manifest_version;
                for e in &edits {
                    catalog.apply(e, shards)?;
                }
                at += used;
            }
        }
        if version != root.manifest_version {
            return Err(Error::Corruption(
                "manifest version does not match the superblock".to_owned(),
            ));
        }
    } else if root.manifest_version != 0 || root.log.is_some() {
        return Err(Error::Corruption(
            "superblock names a manifest version without a snapshot".to_owned(),
        ));
    }
    Ok((catalog, live))
}

fn read(file: &FileRef, extent: Extent, offset: u64, len: usize) -> Result<Vec<u8>> {
    if offset + len as u64 > extent.len() {
        return Err(Error::Corruption(
            "manifest block extends past its extent".to_owned(),
        ));
    }
    let mut buf = vec![0u8; len];
    file.read_at(&mut buf, extent.offset() + offset)?;
    Ok(buf)
}

/// The writer's manifest state: the current root and the pager that commits it.
#[derive(Debug)]
pub(crate) struct ManifestWriter {
    pager: Arc<Pager>,
    root: Root,
    /// Length of the live snapshot block (for the "log outgrows the snapshot" rule).
    snapshot_len: u32,
    /// Set once a root commit failed: the pager is poisoned (decision D58) and the engine
    /// requires a reopen.
    poisoned: bool,
}

impl ManifestWriter {
    pub(crate) fn new(pager: Arc<Pager>) -> Self {
        let root = pager.root();
        Self {
            pager,
            snapshot_len: root.snapshot_len,
            root,
            poisoned: false,
        }
    }

    fn poisoned_error() -> Error {
        Error::Io(pigeonhole_io::Error::new(
            pigeonhole_io::ErrorKind::Other,
            "an earlier manifest commit failed; reopen the database",
        ))
    }

    /// Commits `edits` as manifest version `current + 1`. `catalog` is the state *after*
    /// `edits`, used when the delta does not fit the log and a new snapshot is written.
    /// Blocks on the root commit's fsyncs; never called on a shard's foreground loop.
    pub(crate) fn commit(&mut self, catalog: &Catalog, edits: &[Edit]) -> Result<ManifestVersion> {
        if self.poisoned {
            return Err(Self::poisoned_error());
        }
        let version = self.root.manifest_version + 1;
        let mut delta = Vec::new();
        encode_block(
            &ManifestHeader {
                version: FormatVersion::CURRENT,
                kind: ManifestBlockKind::Delta,
                manifest_version: version,
                edit_count: 0,
                body_len: 0,
            },
            edits,
            &mut delta,
        );
        let fits_log = match self.root.log {
            Some(_) if self.root.snapshot.is_some() => {
                let end = self.root.log_len as usize + delta.len();
                end <= LOG_EXTENT_LEN
                    && end <= self.snapshot_len.max(MANIFEST_HEADER_LEN as u32) as usize
            }
            _ => false,
        };
        let result = if fits_log {
            self.append_delta(&delta, version)
        } else {
            self.write_snapshot(catalog, version)
        };
        match result {
            Ok(()) => Ok(version),
            Err(e) => {
                self.poisoned = true;
                Err(e)
            }
        }
    }

    fn append_delta(&mut self, delta: &[u8], version: ManifestVersion) -> Result<()> {
        let log = self.root.log.expect("checked by caller");
        self.pager.write(log, u64::from(self.root.log_len), delta)?;
        let root = Root {
            log_len: self.root.log_len + delta.len() as u32,
            manifest_version: version,
            ..self.root
        };
        self.pager.commit_root(root)?;
        self.root = root;
        Ok(())
    }

    fn write_snapshot(&mut self, catalog: &Catalog, version: ManifestVersion) -> Result<()> {
        let mut block = Vec::new();
        encode_block(
            &ManifestHeader {
                version: FormatVersion::CURRENT,
                kind: ManifestBlockKind::Snapshot,
                manifest_version: version,
                edit_count: 0,
                body_len: 0,
            },
            &catalog.snapshot_edits(),
            &mut block,
        );
        let snapshot = self.pager.allocate(block.len() as u64)?;
        let log = match self.pager.allocate(LOG_EXTENT_LEN as u64) {
            Ok(log) => log,
            Err(e) => {
                self.pager.abandon(snapshot);
                return Err(e.into());
            }
        };
        if let Err(e) = self.pager.write(snapshot, 0, &block) {
            self.pager.abandon(snapshot);
            self.pager.abandon(log);
            return Err(e.into());
        }
        let root = Root {
            snapshot: Some(snapshot),
            snapshot_len: block.len() as u32,
            log: Some(log),
            log_len: 0,
            manifest_version: version,
        };
        let old = self.root;
        self.pager.commit_root(root)?;
        self.root = root;
        self.snapshot_len = root.snapshot_len;
        // The old manifest extents are referenced by no view: free them once the new root is
        // durable, which it is now (decision D61 clamps reclaim to the durable root).
        for extent in [old.snapshot, old.log].into_iter().flatten() {
            self.pager.retire(extent, version);
        }
        self.pager.reclaim(version);
        Ok(())
    }

    /// Records a clean close (one more root commit).
    pub(crate) fn mark_clean(&mut self) -> Result<()> {
        if self.poisoned {
            return Err(Self::poisoned_error());
        }
        self.pager.mark_clean().map_err(|e| {
            self.poisoned = true;
            e.into()
        })
    }
}
