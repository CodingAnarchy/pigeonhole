use super::*;

/// The token's format version.
pub(super) const TOKEN_VERSION: u8 = 1;

/// A token from a feed of every table (D315; simp724's S1 on #1487). Only it resumes such a
/// feed: a feed of some tables' token sits past commits of the others it never followed, so
/// resuming every table from it would skip them. A build that doesn't know this version
/// refuses the token as not one (`InvalidArgument`), never misreading it.
pub(super) const TOKEN_VERSION_ALL: u8 = 2;

/// A token from a feed of every table that also carries the catalog at its position
/// ([`CatalogModel`], D325, ICR 0136): version 2's layout, then 16 bytes. A resume checks it
/// against the live catalog, so a schema change no catalog entry recorded (a 1.1.x session's,
/// after a rollback) expires the feed. 1.2.0 refuses it (`InvalidArgument`); 1.2.1 reads it as
/// version 2.
pub(super) const TOKEN_VERSION_ALL_CATALOG: u8 = 3;

/// A token's header, as encoded by [`FeedReader::token`]: the one decoder of its layout.
pub(super) struct TokenHeader<'a> {
    pub(super) all_tables: bool,
    pub(super) db_id: [u8; 16],
    pub(super) streams: usize,
    pub(super) delivered: u64,
    pub(super) session: u64,
    /// Each stream's position (16 bytes each), checked to be `streams` of them.
    pub(super) per: &'a [u8],
    /// A feed of every table's last catalog change delivered, and its start's bound (D315).
    pub(super) catalog_seq: u64,
    pub(super) catalog_bound: u64,
    /// Version 3's catalog at the position (D325).
    pub(super) catalog: Option<CatalogModel>,
}

impl<'a> TokenHeader<'a> {
    pub(super) fn parse(bytes: &'a [u8]) -> Result<Self> {
        let bad = || Error::InvalidArgument("not a change feed token".into());
        let (&version, rest) = bytes.split_first().ok_or_else(bad)?;
        if !matches!(
            version,
            TOKEN_VERSION | TOKEN_VERSION_ALL | TOKEN_VERSION_ALL_CATALOG
        ) || rest.len() < 16 + 4 + 8 + 8
        {
            return Err(bad());
        }
        let u64_at = |b: &[u8], at: usize| u64::from_le_bytes(b[at..at + 8].try_into().unwrap());
        let all_tables = version != TOKEN_VERSION;
        let streams = u32::from_le_bytes(rest[16..20].try_into().unwrap()) as usize;
        let mut per = &rest[36..];
        let (mut catalog_seq, mut catalog_bound) = (0, 0);
        let mut catalog = None;
        // Version 3: the catalog at the position, last (D325).
        if version == TOKEN_VERSION_ALL_CATALOG {
            let Some(split) = per.len().checked_sub(CatalogModel::LEN) else {
                return Err(bad());
            };
            catalog = Some(CatalogModel::decode(&per[split..]));
            per = &per[..split];
        }
        if all_tables {
            // The last catalog change delivered and the start's bound, after the streams'
            // positions (D315).
            let Some(split) = per.len().checked_sub(16) else {
                return Err(bad());
            };
            catalog_seq = u64_at(per, split);
            catalog_bound = u64_at(per, split + 8);
            per = &per[..split];
        }
        if per.len() != streams * 16 {
            return Err(bad());
        }
        Ok(Self {
            all_tables,
            db_id: rest[..16].try_into().unwrap(),
            streams,
            delivered: u64_at(rest, 20),
            session: u64_at(rest, 28),
            per,
            catalog_seq,
            catalog_bound,
            catalog,
        })
    }
}

/// Where a token of a feed of every table stands (#1436, ICR 0125): its database, the last
/// seqno delivered (or passed over), and the last catalog change delivered. A replica stores
/// this as where its bootstrap copy starts. `InvalidArgument` for anything but such a token.
pub fn all_tables_token_position(token: &[u8]) -> Result<([u8; 16], u64, u64)> {
    let h = TokenHeader::parse(token)?;
    if !h.all_tables {
        return Err(Error::InvalidArgument(
            "not a token of a feed of every table".into(),
        ));
    }
    Ok((h.db_id, h.delivered, h.catalog_seq))
}

impl FeedReader {
    /// Where this feed is, to resume it with [`FeedStart::Token`]: just past the last
    /// commit [`FeedReader::try_next`] returned, never inside one (§2.5).
    pub fn token(&self) -> Vec<u8> {
        let mut t = Vec::with_capacity(
            1 + 16 + 4 + 8 + 8 + 16 * self.streams.len() + 16 + CatalogModel::LEN,
        );
        t.push(if self.tables.is_empty() {
            TOKEN_VERSION_ALL
        } else {
            TOKEN_VERSION
        });
        t.extend_from_slice(&self.shared.pager.db_id());
        t.extend_from_slice(&(self.streams.len() as u32).to_le_bytes());
        t.extend_from_slice(&self.delivered.to_le_bytes());
        t.extend_from_slice(&self.session.to_le_bytes());
        for (s, st) in self.streams.iter().enumerate() {
            t.extend_from_slice(&self.hint(s).0.to_le_bytes());
            t.extend_from_slice(&st.delivered_end.0.to_le_bytes());
        }
        // A feed of every table: the last catalog change delivered, and its start's bound
        // (D315; simp724's F1b on #1515: a resume passes over what the start did).
        if self.tables.is_empty() {
            t.extend_from_slice(&self.catalog_seq.to_le_bytes());
            t.extend_from_slice(&self.catalog_bound.to_le_bytes());
            // The catalog at the position (D325), once the open worked it out.
            if let Some(m) = &self.model {
                t[0] = TOKEN_VERSION_ALL_CATALOG;
                m.encode(&mut t);
            }
        }
        t
    }

    /// Where stream `s` must be read again from to deliver everything after `delivered`:
    /// the earliest record of it read but not delivered (held or pending, or passed over
    /// by the read-ahead cap, D320), else the cursor.
    pub(super) fn hint(&self, s: usize) -> Lsn {
        let cursor = self.streams[s].cursor.position();
        let mut at = self
            .starts
            .first(s)
            .map_or(cursor, |first| first.min(cursor));
        if let Some(r) = self.ahead.resume[s] {
            at = at.min(r);
        }
        #[cfg(any(test, feature = "test-hooks"))]
        debug_assert_eq!(at, self.scanned_hint(s), "stream {s}'s tracked starts");
        at
    }

    /// [`hint`](Self::hint) by a scan of every pending record: the invariant check of
    /// `starts`.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn scanned_hint(&self, s: usize) -> Lsn {
        let mut at = self.streams[s].cursor.position();
        if let Some(r) = self.ahead.resume[s] {
            at = at.min(r);
        }
        for p in self.ready.values() {
            let (stream, start) = p.at();
            if stream == s {
                at = at.min(start);
            }
        }
        for h in self.held.values() {
            if h.stream == s {
                at = at.min(h.start);
            }
        }
        at
    }

    /// Decodes a token into each stream's start and must-reach position, or the reason it
    /// can't resume.
    #[allow(clippy::type_complexity)]
    pub(super) fn decode_token(
        &mut self,
        bytes: &[u8],
        db_id: [u8; 16],
    ) -> Result<std::result::Result<Vec<(Lsn, Option<Lsn>)>, Expiry>> {
        let h = TokenHeader::parse(bytes)?;
        // A feed of every table resumes only from a token of one (simp724's S1 on #1487). A
        // feed of some tables may resume from an all-tables token: it follows a subset of
        // what that feed did, from the same position.
        if self.tables.is_empty() && !h.all_tables {
            return Err(Error::InvalidArgument(
                "a feed of every table can't resume from a token of a feed of some tables: \
                 the other tables' commits before it would be skipped"
                    .into(),
            ));
        }
        let u64_at = |b: &[u8], at: usize| u64::from_le_bytes(b[at..at + 8].try_into().unwrap());
        if h.db_id != db_id {
            return Err(Error::InvalidArgument(
                "the token is another database's".into(),
            ));
        }
        let (streams, delivered, session, per) = (h.streams, h.delivered, h.session, h.per);
        if h.all_tables {
            self.catalog_seq = h.catalog_seq;
            self.catalog_bound = h.catalog_bound;
            // Checked only by a feed of every table (`deriving`).
            if self.tables.is_empty() {
                self.claim = h.catalog;
                self.claim_known = Some(h.catalog_seq);
            }
        }
        self.delivered = delivered;
        self.session = session;
        if streams != self.shared.shards {
            return Ok(Err(expiry(format!(
                "the shard count changed ({streams} to {})",
                self.shared.shards
            ))));
        }
        if session != 0 && !self.history.holds(session) {
            return Ok(Err(expiry(
                "another session wrote without feeding these tables since the token",
            )));
        }
        let mut starts = Vec::with_capacity(streams);
        for s in 0..streams {
            let hint = Lsn(u64_at(per, s * 16));
            let reached = Lsn(u64_at(per, s * 16 + 8));
            if hint.epoch() != 0 && hint.epoch() < self.floor(s) {
                return Ok(Err(expiry("retention passed the token")));
            }
            starts.push((hint, (reached > hint).then_some(reached)));
        }
        Ok(Ok(starts))
    }
}
