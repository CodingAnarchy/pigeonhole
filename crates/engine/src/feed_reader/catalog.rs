use super::*;

/// A catalog change a feed of every table delivers (D315): one manifest commit's catalog
/// edits, from its `Catalog` sidecar entry.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CatalogEvent {
    /// The commit's `seqno_ceiling`: every commit below it comes before the change, every
    /// commit at or above it after.
    pub seqno_ceiling: Seqno,
    /// The commit's `ts_floor`: every default timestamp before the change is at most this.
    pub timestamp: u64,
    /// The root commit's sequence: it orders catalog changes, and a token records the last
    /// one delivered.
    pub sequence: u64,
    /// Its edits, in the order they applied: `CreateTable`, `PutFamily`, `DropTable`.
    pub edits: Vec<pigeonhole_format::manifest::Edit>,
    /// The name of each table the edits name without a `CreateTable` among them, as the
    /// catalog held it before them: every table an event names is named in the event.
    pub names: Vec<(TableId, String)>,
}

/// Whether a `Catalog` sidecar entry's root landed, as the entries after it say (D315
/// amendment): its Root entry follows (`Yes`), its intent follows or nothing does yet
/// (`Pending`: read it again next time), or something else follows (`Never`).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Rooted {
    Yes,
    Pending,
    Never,
}

/// Whether `catalog` reflects every one of a catalog change's `edits`. Ids only grow and are
/// never reused, and an id is counted only once its change is published: a created table or
/// family is reflected once the counter passed its id, a drop once the table is gone and its
/// id was counted (a catalog from before the table's create lacks it too: feed1248's S1 on
/// #1870). That holds for a prefix of the changes, checked in order, and for each change on
/// its own.
pub(super) fn reflects(
    catalog: &crate::catalog::Catalog,
    edits: &[pigeonhole_format::manifest::Edit],
) -> bool {
    use pigeonhole_format::manifest::Edit;
    edits.iter().all(|e| match e {
        Edit::CreateTable { table, .. } => table.0 < catalog.counters.next_table,
        Edit::PutFamily { family, .. } => family.0 < catalog.counters.next_family,
        Edit::DropTable { table } => {
            table.0 < catalog.counters.next_table && catalog.table(*table).is_none()
        }
        _ => true,
    })
}

/// The catalog at a feed of every table's position, as its token carries it (D325): an
/// order-independent hash of its table ids and its id counters. The catalog changes a feed
/// records are a table created with its families, a family added and a table dropped
/// (D315). Ids are never reused and the counters only rise, so a create or an added family
/// no change recorded raises a counter past what the recorded ones account for, and a drop
/// no change recorded changes the set. The replica's state table (reserved, D315 PR 3) is
/// left out: it comes and goes without a catalog change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CatalogModel {
    /// The wrapping sum of xxh3-64 over each table id's little-endian bytes.
    pub(super) tables: u64,
    pub(super) next_table: u32,
    pub(super) next_family: u32,
}

impl CatalogModel {
    /// The bytes it adds to a token.
    pub(super) const LEN: usize = 16;

    pub(super) fn table_hash(table: TableId) -> u64 {
        pigeonhole_format::checksum::xxh3_64(&table.0.to_le_bytes())
    }

    pub(super) fn of(catalog: &crate::catalog::Catalog) -> Self {
        let tables = catalog
            .tables()
            .map(|t| t.id)
            .filter(|t| !crate::engine::is_reserved_table(*t))
            .fold(0u64, |h, t| h.wrapping_add(Self::table_hash(t)));
        Self {
            tables,
            next_table: catalog.counters.next_table,
            next_family: catalog.counters.next_family,
        }
    }

    /// Moves it past a catalog change.
    pub(super) fn apply(&mut self, edits: &[pigeonhole_format::manifest::Edit]) {
        use pigeonhole_format::manifest::Edit;
        for e in edits {
            match e {
                Edit::CreateTable { table, .. } if !crate::engine::is_reserved_table(*table) => {
                    self.tables = self.tables.wrapping_add(Self::table_hash(*table));
                    self.next_table = self.next_table.max(table.0 + 1);
                }
                Edit::PutFamily { family, .. } => {
                    self.next_family = self.next_family.max(family.0 + 1);
                }
                Edit::DropTable { table } if !crate::engine::is_reserved_table(*table) => {
                    self.tables = self.tables.wrapping_sub(Self::table_hash(*table));
                }
                _ => {}
            }
        }
    }

    pub(super) fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.tables.to_le_bytes());
        out.extend_from_slice(&self.next_table.to_le_bytes());
        out.extend_from_slice(&self.next_family.to_le_bytes());
    }

    /// One number for the whole model (#1853 part C2): xxh3-64 of its encoding. A replica
    /// compares its own catalog's with its primary's feed's at the same position.
    pub(super) fn digest(&self) -> u64 {
        let mut b = [0u8; Self::LEN];
        b[..8].copy_from_slice(&self.tables.to_le_bytes());
        b[8..12].copy_from_slice(&self.next_table.to_le_bytes());
        b[12..].copy_from_slice(&self.next_family.to_le_bytes());
        pigeonhole_format::checksum::xxh3_64(&b)
    }

    pub(super) fn decode(b: &[u8]) -> Self {
        Self {
            tables: u64::from_le_bytes(b[..8].try_into().unwrap()),
            next_table: u32::from_le_bytes(b[8..12].try_into().unwrap()),
            next_family: u32::from_le_bytes(b[12..16].try_into().unwrap()),
        }
    }
}

/// The digest of `catalog`'s model (#1853 part C2): what [`FeedReader::catalog_digest`] says
/// at the same position.
pub(crate) fn catalog_digest_of(catalog: &crate::catalog::Catalog) -> u64 {
    CatalogModel::of(catalog).digest()
}

/// The catalog model at a feed of every table's start, worked out at open from the catalog
/// it sees there and the sidecar's catalog changes (D325). `v` is loaded before the sidecar
/// is read, so it reflects a prefix of the changes, all with their entries in the file.
/// Changes after the start that `v` reflects are taken back out; changes before the start
/// that it doesn't reflect yet (a reader process's view can lag, or a change can be
/// appended but not yet published) are put in.
pub(super) struct Derive {
    pub(super) v: Arc<crate::catalog::Catalog>,
    /// `v`'s model, with the changes before the start it lacks put in.
    pub(super) model: CatalogModel,
    /// Whether the changes after the start seen so far are all reflected by `v`.
    pub(super) reflecting: bool,
    /// The smallest table and family ids a change after the start that `v` reflects
    /// created, and the largest: the counters at the start were at most the smallest, and
    /// the live ones are one past the largest at least.
    pub(super) created_tables: Option<(u32, u32)>,
    pub(super) created_families: Option<(u32, u32)>,
    /// A token's last catalog change: its model holds those up to it and no later one. A
    /// later change its bound passes over (one appended after the token was taken) is
    /// compared as after the start, then put in.
    pub(super) known: Option<u64>,
    pub(super) late: Vec<Vec<pigeonhole_format::manifest::Edit>>,
}

impl Derive {
    pub(super) fn new(v: Arc<crate::catalog::Catalog>, known: Option<u64>) -> Self {
        let model = CatalogModel::of(&v);
        Self {
            v,
            model,
            reflecting: true,
            created_tables: None,
            created_families: None,
            known,
            late: Vec::new(),
        }
    }

    /// A change the start passes over: before the start, unless a token's model can't hold
    /// it yet.
    pub(super) fn passed_over(
        &mut self,
        sequence: u64,
        edits: &[pigeonhole_format::manifest::Edit],
    ) {
        if self.known.is_some_and(|k| sequence > k) {
            self.after(edits);
            self.late.push(edits.to_vec());
        } else {
            self.before(edits);
        }
    }

    /// A change before the start.
    pub(super) fn before(&mut self, edits: &[pigeonhole_format::manifest::Edit]) {
        if !reflects(&self.v, edits) {
            self.model.apply(edits);
        }
    }

    /// A change after the start, in order (D315: reflected as a prefix).
    pub(super) fn after(&mut self, edits: &[pigeonhole_format::manifest::Edit]) {
        use pigeonhole_format::manifest::Edit;
        self.reflecting = self.reflecting && reflects(&self.v, edits);
        if !self.reflecting {
            return;
        }
        let widen = |r: &mut Option<(u32, u32)>, id: u32| {
            *r = Some(r.map_or((id, id), |(lo, hi)| (lo.min(id), hi.max(id))));
        };
        for e in edits {
            match e {
                Edit::CreateTable { table, .. } if !crate::engine::is_reserved_table(*table) => {
                    self.model.tables = self
                        .model
                        .tables
                        .wrapping_sub(CatalogModel::table_hash(*table));
                    widen(&mut self.created_tables, table.0);
                }
                Edit::PutFamily { family, .. } => widen(&mut self.created_families, family.0),
                Edit::DropTable { table } if !crate::engine::is_reserved_table(*table) => {
                    self.model.tables = self
                        .model
                        .tables
                        .wrapping_add(CatalogModel::table_hash(*table));
                }
                _ => {}
            }
        }
    }

    /// The model at the start: `claim`'s (a version 3 token's), if the catalog agrees with
    /// it, else the reason it doesn't; with no claim, the one worked out here.
    pub(super) fn finish(
        mut self,
        claim: Option<CatalogModel>,
    ) -> std::result::Result<CatalogModel, String> {
        let late = std::mem::take(&mut self.late);
        let mut model = self.check(claim)?;
        for edits in &late {
            model.apply(edits);
        }
        Ok(model)
    }

    pub(super) fn check(
        self,
        claim: Option<CatalogModel>,
    ) -> std::result::Result<CatalogModel, String> {
        let live = self.model;
        let Some(claim) = claim else {
            return Ok(CatalogModel {
                tables: live.tables,
                next_table: self
                    .created_tables
                    .map_or(live.next_table, |(lo, _)| lo.min(live.next_table)),
                next_family: self
                    .created_families
                    .map_or(live.next_family, |(lo, _)| lo.min(live.next_family)),
            });
        };
        let expect =
            |at: u32, created: Option<(u32, u32)>| created.map_or(at, |(_, hi)| at.max(hi + 1));
        if live.tables != claim.tables {
            return Err(
                "the catalog's tables aren't the token's with the recorded changes \
                        since: a table was created or dropped"
                    .into(),
            );
        }
        if live.next_table != expect(claim.next_table, self.created_tables) {
            return Err("a table was created".into());
        }
        if live.next_family != expect(claim.next_family, self.created_families) {
            return Err("a family was added".into());
        }
        Ok(claim)
    }
}

impl FeedReader {
    /// A feed of every table (D315): reads the sidecar's new entries and queues the catalog
    /// changes of the feed's history not yet delivered. Called after the frontier F is
    /// loaded: the catalog hold keeps F below a change's ceiling until its entry is appended,
    /// so every change with a ceiling at most F + 1 is in the file by then. At open, `start`
    /// is the catalog a `Now` or `Snapshot` start reflects (the view's, loaded before the
    /// sidecar was read, so no later entry can be reflected): the changes it reflects are
    /// before the start, beside the reader's `catalog_bound` for `Oldest` and `After`. A
    /// token says the last change delivered instead. A passed-over or delivered change's
    /// drops are kept, so a dropped table's later changes are passed over.
    pub(super) fn load_catalog(&mut self, start: Option<&crate::catalog::Catalog>) -> Result<()> {
        // At open, the catalog model at the start is worked out from this first load (D325)
        // and checked against a version 3 token's.
        // A token's start knows the changes up to its last one (`catalog_seq`, set from it).
        let known = self.claim_known;
        let mut derive = self.deriving.take().map(|v| Derive::new(v, known));
        let loaded = self.load_catalog_entries(start, &mut derive);
        if let Some(d) = derive {
            match d.finish(self.claim.take()) {
                Ok(model) => self.model = Some(model),
                Err(why) => {
                    if self.expired.is_none() {
                        self.expired = Some(expiry(format!(
                            "the catalog changed since the token without a record in the \
                             feed ({why}): a session that records no catalog changes (a \
                             1.1.x build's, after a rollback) changed the tables, so the feed \
                             can't deliver those changes"
                        )));
                    }
                }
            }
        }
        loaded
    }

    /// [`load_catalog`](Self::load_catalog)'s read of the sidecar, each catalog change also
    /// given to `derive` at open.
    pub(super) fn load_catalog_entries(
        &mut self,
        start: Option<&crate::catalog::Catalog>,
        derive: &mut Option<Derive>,
    ) -> Result<()> {
        // Before the file is read: a change counted after this is loaded next time.
        if let Some(feed) = &self.writer {
            self.catalog_roots_seen = feed.catalog_roots();
        }
        // Opened once and kept: an idle reader process's poll then costs one length check
        // (simp724's N1 on #1539). The sidecar is only ever appended to, cut or overwritten in
        // place, never replaced by another file.
        let file = match &self.sidecar_file {
            Some(f) => Arc::clone(f),
            None => {
                let path = crate::feed::sidecar_path(&self.shared.path);
                match self.shared.vfs.open(&path, OpenOptions::read()) {
                    Ok(f) => {
                        self.sidecar_file = Some(Arc::clone(&f));
                        f
                    }
                    Err(e) if e.kind == pigeonhole_io::ErrorKind::NotFound => return Ok(()),
                    Err(e) => return Err(e.into()),
                }
            }
        };
        let len = file.len()?;
        let from = self.sidecar_pos.max(FeedHeader::LEN as u64);
        if len <= from {
            return Ok(());
        }
        let mut bytes = vec![0u8; (len - from) as usize];
        file.read_at(&mut bytes, from)?;
        if self.sidecar_pos == 0 {
            let mut head = [0u8; FeedHeader::LEN];
            file.read_at(&mut head, 0)?;
            match FeedHeader::decode(&head) {
                Ok(h) if h.db_id == self.shared.pager.db_id() => {}
                _ => return Ok(()),
            }
        }
        // Up to the first torn entry: the rest is read again once it's whole.
        let (entries, used) = decode_entries_at(&bytes);
        self.sidecar_pos = from + used as u64;
        // Every create and drop the file records, whatever its state (#1853): what explains
        // a table missing from the catalog. Entries past a pending one are read again next
        // time; the sets don't mind.
        for (_, e) in &entries {
            if let FeedEntry::Catalog { edits, .. } = e {
                for edit in edits {
                    match edit {
                        pigeonhole_format::manifest::Edit::DropTable { table } => {
                            self.recorded_drops.insert(*table);
                        }
                        pigeonhole_format::manifest::Edit::CreateTable { table, .. } => {
                            self.recorded_creates.insert(*table);
                        }
                        _ => {}
                    }
                }
            }
        }
        // A catalog change counts once its root commit's Root entry follows it (D315
        // amendment): its entry is appended with the commit's intent, before the commit
        // writes, and a commit that then fails is cut off at the next open, so until the
        // Root is there, it may never have happened. Read from it again next time.
        // A Catalog entry with neither its Root nor its own intent after it can never get its
        // Root: its commit didn't land, and no open resolved it (simp724's F1 on #1526).
        // Waiting there would deliver later commits without the changes after it: expire.
        let rooted: Vec<Rooted> = (0..entries.len())
            .map(
                |i| match (&entries[i].1, entries.get(i + 1).map(|e| &e.1)) {
                    (
                        FeedEntry::Catalog {
                            session, sequence, ..
                        },
                        Some(FeedEntry::Root {
                            session: s,
                            sequence: q,
                        }),
                    ) if s == session && q == sequence => Rooted::Yes,
                    (
                        FeedEntry::Catalog { sequence, .. },
                        Some(FeedEntry::RootIntent { sequence: q, .. }),
                    ) if q == sequence => Rooted::Pending,
                    // The end of what's whole so far: its intent may still be on its way.
                    (FeedEntry::Catalog { .. }, None) => Rooted::Pending,
                    _ => Rooted::Never,
                },
            )
            .collect();
        // The start's catalog reflects a prefix of the changes, in order: once one isn't
        // reflected, no later one is.
        let mut reflecting = true;
        for (i, (at, e)) in entries.into_iter().enumerate() {
            let FeedEntry::Catalog {
                session,
                sequence,
                seqno_ceiling,
                timestamp,
                edits,
                names,
            } = e
            else {
                continue;
            };
            if !self.history.holds(session) {
                continue;
            }
            match rooted[i] {
                Rooted::Yes => {}
                Rooted::Pending => {
                    // It may be published already, and seen by the start (D325).
                    if let Some(d) = derive {
                        d.after(&edits);
                    }
                    self.sidecar_pos = from + at as u64;
                    break;
                }
                Rooted::Never => {
                    self.expired = Some(expiry(format!(
                        "the catalog change of root {sequence} never got its Root entry, and \
                         a later entry follows it: later catalog changes can't be delivered"
                    )));
                    break;
                }
            }
            let before_start = seqno_ceiling <= self.catalog_bound
                || match start {
                    Some(catalog) => {
                        reflecting = reflecting && reflects(catalog, &edits);
                        reflecting
                    }
                    None => sequence <= self.catalog_seq,
                };
            if before_start {
                // Before the position: in its catalog model (D325).
                match (derive.as_mut(), self.model.as_mut()) {
                    (Some(d), _) => d.passed_over(sequence, &edits),
                    (None, Some(m)) => m.apply(&edits),
                    (None, None) => {}
                }
                self.note_drops(&edits);
                self.catalog_seq = self.catalog_seq.max(sequence);
                self.raise_session(session);
                continue;
            }
            if let Some(d) = derive {
                d.after(&edits);
            }
            self.catalog.push_back((
                CatalogEvent {
                    seqno_ceiling,
                    timestamp,
                    sequence,
                    edits,
                    names,
                },
                session,
            ));
        }
        Ok(())
    }

    /// The tables `edits` drop: their later changes are passed over.
    pub(super) fn note_drops(&mut self, edits: &[pigeonhole_format::manifest::Edit]) {
        for e in edits {
            if let pigeonhole_format::manifest::Edit::DropTable { table } = e {
                self.dropped_tables.insert(*table);
            }
        }
    }
}
