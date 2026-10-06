//! Building an SST (FORMAT §5): data blocks streamed to the extent as they fill, then index
//! partitions, top index, filters, properties and, in a separate final write, the footer.

use pigeonhole_format::block::{BlockAddr, BlockBuilder, BlockKind, TRAILER_LEN, seal};
use pigeonhole_format::compress::Compression;
use pigeonhole_format::filter::{FilterBuilder, column_hash, row_hash};
use pigeonhole_format::key::{SUFFIX_LEN, decode_key};
use pigeonhole_format::manifest::SstMeta;
use pigeonhole_format::sst::{FOOTER_LEN, Footer, Properties};
use pigeonhole_format::superblock::ExtentRef;
use pigeonhole_format::{FormatVersion, Kind, SstId};
use pigeonhole_io::FileRef;

use crate::{Error, Result, SstWriterOptions};

/// Pending bytes are written to the file once this much has accumulated.
const FLUSH_BYTES: usize = 1 << 20;

/// Largest target size of an index partition.
const MAX_PARTITION_BYTES: usize = 4096;

/// Upper bound on the encoded size of an entry's three varint headers.
const ENTRY_HEADER_MAX: usize = 30;

/// Upper bound on an index entry's value (`BlockAddr` as two varints).
const ADDR_MAX: usize = 15;

/// Bytes a block's tables add beyond its entries (`R` and `S` counts).
const BLOCK_TAIL: usize = 8;

pub(crate) struct Writer {
    file: FileRef,
    extent: ExtentRef,
    id: SstId,
    opts: SstWriterOptions,
    partition_bytes: usize,
    data: BlockBuilder,
    index: BlockBuilder,
    /// Sealed bytes not yet written, starting at SST offset `flushed`.
    out: Vec<u8>,
    flushed: u64,
    /// Sealed index partitions, laid out after the data blocks at finish.
    index_out: Vec<u8>,
    /// `(last separator, offset within index_out, physical length)` per sealed partition.
    partitions: Vec<(Vec<u8>, u64, u32)>,
    /// The last sealed data block, waiting for the next key to pick its separator.
    pending: Option<BlockAddr>,
    last_key: Vec<u8>,
    max_key_len: usize,
    row_filter: FilterBuilder,
    column_filter: FilterBuilder,
    row_keys: usize,
    column_keys: usize,
    props: Properties,
    scratch: Vec<u8>,
    addr_buf: Vec<u8>,
}

impl std::fmt::Debug for Writer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SstWriter")
            .field("id", &self.id)
            .field("extent", &self.extent)
            .field("entries", &self.props.entries)
            .field("bytes", &(self.flushed + self.out.len() as u64))
            .finish()
    }
}

impl Writer {
    pub(crate) fn new(file: FileRef, extent: ExtentRef, id: SstId, opts: SstWriterOptions) -> Self {
        let props = Properties {
            table: opts.table,
            family: opts.family,
            tablet: opts.tablet,
            seqno_range: (u64::MAX, 0),
            ts_range: (u64::MAX, 0),
            created_micros: opts.created_micros,
            merge_operator: opts.merge_operator.clone(),
            ..Properties::default()
        };
        Self {
            file,
            extent,
            id,
            partition_bytes: opts.block_size.clamp(64, MAX_PARTITION_BYTES),
            data: BlockBuilder::data(opts.restart_interval),
            index: BlockBuilder::index(),
            out: Vec::new(),
            flushed: 0,
            index_out: Vec::new(),
            partitions: Vec::new(),
            pending: None,
            last_key: Vec::new(),
            max_key_len: 0,
            row_filter: FilterBuilder::new(opts.bloom_bits),
            column_filter: FilterBuilder::new(opts.bloom_bits),
            row_keys: 0,
            column_keys: 0,
            props,
            scratch: Vec::new(),
            addr_buf: Vec::new(),
            opts,
        }
    }

    pub(crate) fn extent(&self) -> ExtentRef {
        self.extent
    }

    pub(crate) fn entries(&self) -> u64 {
        self.props.entries
    }

    fn filters(&self) -> bool {
        self.opts.bloom_bits > 0
    }

    /// Physical size bound of a filter block over `keys` keys.
    fn filter_bound(&self, keys: usize) -> usize {
        if !self.filters() {
            return 0;
        }
        let bits = keys * usize::from(self.opts.bloom_bits);
        8 + 64 * (bits.div_ceil(512).max(1)) + TRAILER_LEN
    }

    /// A conservative bound on the finished SST's size if one more entry of these sizes is
    /// added. Every term is an uncompressed size (a block is stored raw when compression does
    /// not help), so the real size never exceeds it.
    fn bound_with(&self, key_len: usize, value_len: usize) -> u64 {
        let max_key = self.max_key_len.max(key_len);
        let entry = ENTRY_HEADER_MAX + key_len + value_len + 8;
        // The open block (sealed, or cut) plus a new block holding the entry.
        let data = self.flushed as usize
            + self.out.len()
            + self.data.estimated_len()
            + TRAILER_LEN
            + entry
            + BLOCK_TAIL
            + TRAILER_LEN;
        // Up to three more index entries (pending, open and new block), each possibly
        // cutting a partition.
        let index_entry = ENTRY_HEADER_MAX + max_key + ADDR_MAX + 4;
        let index = self.index_out.len()
            + self.index.estimated_len()
            + 3 * index_entry
            + 4 * (BLOCK_TAIL + TRAILER_LEN);
        let top = (self.partitions.len() + 4) * index_entry + BLOCK_TAIL + TRAILER_LEN;
        let filters =
            self.filter_bound(self.row_keys + 1) + self.filter_bound(self.column_keys + 1);
        let props = 160 + 2 * (10 + max_key) + 10 + self.opts.merge_operator.len() + TRAILER_LEN;
        (data + index + top + filters + props + FOOTER_LEN) as u64
    }

    pub(crate) fn fits(&self, key_len: usize, value_len: usize) -> bool {
        self.bound_with(key_len, value_len) <= self.extent.len()
    }

    pub(crate) fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.props.entries > 0 && key <= self.last_key.as_slice() {
            return Err(Error::OutOfOrder);
        }
        let parts = decode_key(key)?;
        let column_len = key.len() - SUFFIX_LEN;
        let row_len = parts.row.as_escaped().len();
        let entry = ENTRY_HEADER_MAX + key.len() + value.len();
        if !self.data.is_empty() && self.data.estimated_len() + entry > self.opts.block_size {
            self.seal_data_block()?;
        }
        if let Some(addr) = self.pending.take() {
            self.add_index_entry(Some(key), addr)?;
        }
        self.data.add(key, value)?;

        let first = self.props.entries == 0;
        let last_len = self.last_key.len();
        let new_column = first || self.last_key[..last_len - SUFFIX_LEN] != key[..column_len];
        if new_column {
            // A new row starts a new column too; compare row prefixes only then.
            let new_row = first || !self.last_key.starts_with(&key[..row_len + 2]);
            if new_row {
                self.props.rows += 1;
                self.row_keys += 1;
                if self.filters() {
                    self.row_filter.add_hash(row_hash(&key[..row_len]));
                }
            }
            // For a family marker the column prefix is the marker key `row 00 01 00 00`
            // (FORMAT §6), so a column-filter miss never hides a marker.
            self.column_keys += 1;
            if self.filters() {
                self.column_filter.add_hash(column_hash(&key[..column_len]));
            }
        }

        let p = &mut self.props;
        if first {
            p.smallest_key = key.to_vec();
        }
        p.entries += 1;
        p.raw_key_bytes += key.len() as u64;
        p.raw_value_bytes += value.len() as u64;
        if parts.kind.is_delete() {
            p.deletes += 1;
        }
        if parts.kind == Kind::Merge {
            p.merges += 1;
        }
        p.seqno_range = (
            p.seqno_range.0.min(parts.seqno),
            p.seqno_range.1.max(parts.seqno),
        );
        p.ts_range = (p.ts_range.0.min(parts.ts), p.ts_range.1.max(parts.ts));
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.max_key_len = self.max_key_len.max(key.len());
        Ok(())
    }

    /// Fails with `ExtentFull` if the SST would grow past its extent.
    fn check_room(&self, len: u64) -> Result<()> {
        if len > self.extent.len() {
            return Err(Error::ExtentFull);
        }
        Ok(())
    }

    fn seal_data_block(&mut self) -> Result<()> {
        let offset = self.flushed + self.out.len() as u64;
        let start = self.out.len();
        seal(
            BlockKind::Data,
            self.opts.compression,
            self.data.finish(),
            &mut self.out,
        )?;
        self.data.reset();
        let len = (self.out.len() - start) as u32;
        self.props.data_blocks += 1;
        self.pending = Some(BlockAddr { offset, len });
        self.check_room(offset + u64::from(len))?;
        if self.out.len() >= FLUSH_BYTES {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.out.is_empty() {
            return Ok(());
        }
        self.file
            .write_at(&self.out, self.extent.offset() + self.flushed)?;
        self.flushed += self.out.len() as u64;
        self.out.clear();
        Ok(())
    }

    /// Indexes the last sealed data block, whose last key is `self.last_key`, with a short
    /// separator `s`: `last_key <= s < next` (or `s = last_key` for the final block).
    fn add_index_entry(&mut self, next: Option<&[u8]>, addr: BlockAddr) -> Result<()> {
        separator(&self.last_key, next, &mut self.scratch);
        self.addr_buf.clear();
        addr.encode_varint(&mut self.addr_buf);
        let entry = ENTRY_HEADER_MAX + self.scratch.len() + self.addr_buf.len();
        if !self.index.is_empty() && self.index.estimated_len() + entry > self.partition_bytes {
            self.seal_partition()?;
        }
        self.index.add(&self.scratch, &self.addr_buf)?;
        Ok(())
    }

    fn seal_partition(&mut self) -> Result<()> {
        let offset = self.index_out.len() as u64;
        seal(
            BlockKind::Index,
            self.opts.compression,
            self.index.finish(),
            &mut self.index_out,
        )?;
        let len = (self.index_out.len() as u64 - offset) as u32;
        self.partitions
            .push((self.index.last_key().to_vec(), offset, len));
        self.index.reset();
        self.props.index_partitions += 1;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<SstMeta> {
        if !self.data.is_empty() {
            self.seal_data_block()?;
        }
        if let Some(addr) = self.pending.take() {
            self.add_index_entry(None, addr)?;
        }
        if !self.index.is_empty() {
            self.seal_partition()?;
        }

        // Index partitions follow the data blocks.
        let data_end = self.flushed + self.out.len() as u64;
        self.out.extend_from_slice(&self.index_out);
        let mut top = BlockBuilder::index();
        for (sep, offset, len) in &self.partitions {
            self.addr_buf.clear();
            BlockAddr {
                offset: data_end + offset,
                len: *len,
            }
            .encode_varint(&mut self.addr_buf);
            top.add(sep, &self.addr_buf)?;
        }
        let top_index = self.seal_tail(BlockKind::TopIndex, self.opts.compression, |out| {
            out.extend_from_slice(top.finish())
        })?;

        let (row_filter, column_filter) = if self.filters() {
            let mut rows = std::mem::take(&mut self.row_filter);
            let mut cols = std::mem::take(&mut self.column_filter);
            (
                self.seal_tail(BlockKind::Filter, Compression::None, |out| rows.finish(out))?,
                self.seal_tail(BlockKind::Filter, Compression::None, |out| cols.finish(out))?,
            )
        } else {
            (BlockAddr::default(), BlockAddr::default())
        };

        let p = &mut self.props;
        if p.entries == 0 {
            p.seqno_range = (0, 0);
            p.ts_range = (0, 0);
        }
        p.largest_key = self.last_key.clone();
        let props = self.props.clone();
        let properties = self.seal_tail(BlockKind::Properties, Compression::None, |out| {
            props.encode(out)
        })?;

        let footer = Footer {
            top_index,
            row_filter,
            column_filter,
            properties,
            compression_dict: BlockAddr::default(),
            version: FormatVersion::CURRENT,
            flags: 0,
        };
        let footer_at = self.flushed + self.out.len() as u64;
        let len = footer_at + FOOTER_LEN as u64;
        self.check_room(len)?;
        self.flush()?;
        // The footer goes last, in its own write: an interrupted build never leaves a footer
        // in front of missing blocks unless the disk reorders unsynced writes, and then the
        // block checksums catch it.
        self.file
            .write_at(&footer.encode(), self.extent.offset() + footer_at)?;

        Ok(SstMeta {
            id: self.id,
            extent: self.extent,
            len,
            smallest_key: props.smallest_key,
            largest_key: props.largest_key,
            seqno_range: props.seqno_range,
            ts_range: props.ts_range,
            entries: props.entries,
            deletes: props.deletes,
        })
    }

    /// Seals a tail block built by `fill` into `out` and returns its address.
    fn seal_tail(
        &mut self,
        kind: BlockKind,
        codec: Compression,
        fill: impl FnOnce(&mut Vec<u8>),
    ) -> Result<BlockAddr> {
        self.scratch.clear();
        fill(&mut self.scratch);
        let offset = self.flushed + self.out.len() as u64;
        let start = self.out.len();
        seal(kind, codec, &self.scratch, &mut self.out)?;
        let len = (self.out.len() - start) as u32;
        self.check_room(offset + u64::from(len))?;
        Ok(BlockAddr { offset, len })
    }
}

/// Writes a short separator `s` with `last <= s < next` into `out`: the common prefix plus
/// one incremented byte when that stays below `next`, else `last` itself.
fn separator(last: &[u8], next: Option<&[u8]>, out: &mut Vec<u8>) {
    out.clear();
    if let Some(next) = next {
        let p = last.iter().zip(next).take_while(|(a, b)| a == b).count();
        if p < last.len() && p < next.len() && last[p] < 0xFF && last[p] + 1 < next[p] {
            out.extend_from_slice(&last[..p]);
            out.push(last[p] + 1);
            return;
        }
    }
    out.extend_from_slice(last);
}

#[cfg(test)]
mod tests {
    use super::separator;

    #[test]
    fn separators_lie_between_neighbors() {
        let cases: [(&[u8], &[u8]); 5] = [
            (b"abc", b"abz"),
            (b"abc", b"abd"),
            (b"ab", b"abc"),
            (b"a\xff", b"b"),
            (b"a", b"c"),
        ];
        let mut s = Vec::new();
        for (last, next) in cases {
            separator(last, Some(next), &mut s);
            assert!(
                last <= s.as_slice() && s.as_slice() < next,
                "{last:?} {s:?} {next:?}"
            );
        }
        separator(b"q", None, &mut s);
        assert_eq!(s, b"q");
    }
}
