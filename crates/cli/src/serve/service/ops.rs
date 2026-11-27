use super::*;

/// One call, decoded. It holds its request by value: routing moves it, never copies it
/// (perf287 on D317's seam).
#[derive(Debug)]
pub(crate) enum ServiceOp {
    Get(proto::GetRequest),
    MultiGet(proto::MultiGetRequest),
    ReadRow(proto::ReadRowRequest),
    Mutate(proto::MutateRequest),
    CheckAndMutate(proto::CheckAndMutateRequest),
    Batch(proto::BatchRequest),
    MutateReturning(proto::MutateRequest),
    Scan(proto::ScanRequest),
    Begin(proto::BeginRequest),
    TxnGet(proto::TxnGetRequest),
    TxnGetRow(proto::TxnGetRowRequest),
    TxnWrite(proto::TxnWriteRequest),
    Commit(proto::TxnCommitRequest),
    Abort(proto::TxnAbortRequest),
    OpenSnapshot(proto::OpenSnapshotRequest),
    ReleaseSnapshot(proto::ReleaseSnapshotRequest),
    Subscribe(proto::SubscribeRequest),
    ListTables(proto::ListTablesRequest),
    CreateTable(proto::CreateTableRequest),
    DropTable(proto::DropTableRequest),
    DescribeTable(proto::DescribeTableRequest),
    Info(proto::Empty),
    Flush(proto::MaintenanceRequest),
    Compact(proto::MaintenanceRequest),
    Backup(proto::BackupRequest),
    BackupDownload(proto::BackupDownloadRequest),
    Metrics(proto::Empty),
    Traverse(proto::TraverseRequest),
    Nearest(proto::NearestRequest),
    CreateIndex(proto::CreateVectorIndexRequest),
    VectorInsert(proto::VectorInsertRequest),
    VectorDelete(proto::VectorDeleteRequest),
    VectorSearch(proto::VectorSearchRequest),
    VectorVerify(proto::VectorVerifyRequest),
    /// A client stream: its first message's index and count, and its messages after.
    VectorBulkLoad(super::super::bulk::BulkLoadOp),
    Promote(proto::Empty),
    Follow(proto::FollowRequest),
    TextCreateIndex(proto::CreateTextIndexRequest),
    TextPut(proto::TextPutRequest),
    TextDelete(proto::TextDeleteRequest),
    TextSearch(proto::TextSearchRequest),
    TextRebuild(proto::TextIndexRequest),
    TextVerify(proto::TextIndexRequest),
    Remember(proto::RememberRequest),
    Recall(proto::RecallRequest),
    MarkUsed(proto::MarkUsedRequest),
    Supersede(proto::SupersedeRequest),
    Link(proto::LinkRequest),
    Forget(proto::ForgetRequest),
    MemoryWatch(proto::MemoryWatchRequest),
    /// A recall's steps on one partition (#1915, ICR 0139): only from another node, or this
    /// one's own spanning recall ([`Service::fanout_part`]).
    RecallPool(proto::RecallPoolRequest),
    RecallBodies(proto::RecallBodiesRequest),
    /// A text search's rounds on one partition of its index (#1722 PR 2b, ICR 0138): only
    /// from another node, or this one's own search across the partitions
    /// ([`Service::fanout_part`]).
    TextStats(proto::TextStatsRequest),
    TextSearchPart(proto::TextSearchPartRequest),
    /// `Transactions.Outcome` (D329 2b).
    Outcome(proto::TxnOutcomeRequest),
}

impl ServiceOp {
    /// The RPC's index in `RPCS`.
    pub(crate) fn index(&self) -> usize {
        match self {
            Self::Get(_) => 0,
            Self::MultiGet(_) => 1,
            Self::ReadRow(_) => 2,
            Self::Mutate(_) => 3,
            Self::CheckAndMutate(_) => 4,
            Self::Batch(_) => 5,
            Self::MutateReturning(_) => 6,
            Self::Scan(_) => SCAN,
            Self::Begin(_) => 8,
            Self::TxnGet(_) => 9,
            Self::TxnGetRow(_) => 10,
            Self::TxnWrite(_) => 11,
            Self::Commit(_) => 12,
            Self::Abort(_) => 13,
            Self::OpenSnapshot(_) => 14,
            Self::ReleaseSnapshot(_) => 15,
            Self::Subscribe(_) => FEED,
            Self::ListTables(_) => 17,
            Self::CreateTable(_) => 18,
            Self::DropTable(_) => 19,
            Self::DescribeTable(_) => 20,
            Self::Info(_) => 21,
            Self::Flush(_) => 22,
            Self::Compact(_) => 23,
            Self::Backup(_) => 24,
            Self::BackupDownload(_) => DOWNLOAD,
            Self::Metrics(_) => 26,
            Self::Traverse(_) => 27,
            Self::Nearest(_) => 28,
            Self::CreateIndex(_) => 29,
            Self::VectorInsert(_) => 30,
            Self::VectorDelete(_) => 31,
            Self::VectorSearch(_) => 32,
            Self::VectorVerify(_) => 33,
            Self::VectorBulkLoad(_) => 34,
            Self::Promote(_) => 35,
            Self::Follow(_) => FOLLOW,
            Self::TextCreateIndex(_) => 37,
            Self::TextPut(_) => 38,
            Self::TextDelete(_) => 39,
            Self::TextSearch(_) => 40,
            Self::TextRebuild(_) => 41,
            Self::TextVerify(_) => 42,
            Self::Remember(_) => 43,
            Self::Recall(_) => 44,
            Self::MarkUsed(_) => 45,
            Self::Supersede(_) => 46,
            Self::Link(_) => 47,
            Self::Forget(_) => 48,
            Self::MemoryWatch(_) => MEMORY_WATCH,
            Self::RecallPool(_) => 50,
            Self::RecallBodies(_) => 51,
            Self::TextStats(_) => 52,
            Self::TextSearchPart(_) => 53,
            Self::Outcome(_) => 54,
        }
    }

    /// The RPC's name, `Service.Method`, for logs.
    /// What [`Service::admit`] checks this call against.
    pub(crate) fn admit(&self) -> Admit {
        Admit {
            what: self.name(),
            scope: self.scope(),
            writes: self.writes(),
            size: self.size(),
            memory: matches!(
                self,
                Self::Remember(_)
                    | Self::Recall(_)
                    | Self::MarkUsed(_)
                    | Self::Supersede(_)
                    | Self::Link(_)
                    | Self::Forget(_)
                    | Self::MemoryWatch(_)
                    | Self::RecallPool(_)
                    | Self::RecallBodies(_)
            ),
            // A node's own token for what a node does to a peer: a replica's follow, and a
            // download of a partition (a replica's bootstrap, and a move's copy of a closed
            // set, which the map's writer runs: D332, D331 PR 5).
            peer: matches!(self, Self::Follow(_))
                || matches!(
                    self,
                    Self::BackupDownload(r)
                        if !r.partition.is_empty() && (r.replica || r.fenced_file.is_some())
                ),
        }
    }

    pub(crate) fn name(&self) -> &'static str {
        RPCS[self.index()]
    }

    pub(crate) fn scope(&self) -> Scope {
        match self {
            // A transaction's reads, and its Commit (one that carries writes is checked
            // for write scope where it's answered: a read-only Commit is a consistent read).
            Self::Get(_)
            | Self::MultiGet(_)
            | Self::ReadRow(_)
            | Self::Scan(_)
            | Self::Begin(_)
            | Self::TxnGet(_)
            | Self::TxnGetRow(_)
            | Self::Commit(_)
            | Self::Abort(_)
            | Self::OpenSnapshot(_)
            | Self::ReleaseSnapshot(_)
            | Self::Subscribe(_)
            | Self::ListTables(_)
            | Self::DescribeTable(_)
            | Self::Info(_)
            | Self::Metrics(_)
            | Self::Traverse(_)
            | Self::Nearest(_)
            | Self::VectorSearch(_)
            | Self::VectorVerify(_)
            | Self::TextSearch(_)
            | Self::TextVerify(_)
            | Self::TextStats(_)
            | Self::TextSearchPart(_)
            | Self::Recall(_)
            | Self::RecallPool(_)
            | Self::RecallBodies(_)
            | Self::MemoryWatch(_) => Scope::Read,
            // An index is a table: creating one is a write, as `Schema.CreateTable` is.
            Self::CreateIndex(_)
            | Self::VectorInsert(_)
            | Self::VectorDelete(_)
            | Self::VectorBulkLoad(_)
            // A text index is a table too; a rebuild rewrites it from its source.
            | Self::TextCreateIndex(_)
            | Self::TextPut(_)
            | Self::TextDelete(_)
            | Self::TextRebuild(_)
            // Memory writes, `Forget` included: forgetting is ordinary agent behaviour, and
            // a token's namespaces bound its reach (ai1249 on D313 §7).
            | Self::Remember(_)
            | Self::MarkUsed(_)
            | Self::Supersede(_)
            | Self::Link(_)
            | Self::Forget(_) => Scope::Write,
            // Maintenance and backups: admin (ICR 0112's Admin).
            // A replica's bootstrap and follow read everything, ids and all (#1436).
            Self::Flush(_)
            | Self::Compact(_)
            | Self::Backup(_)
            | Self::BackupDownload(_)
            | Self::Promote(_)
            | Self::Follow(_) => Scope::Admin,
            // Dropping a table drops its data: admin, as ICR 0112's Schema says.
            Self::DropTable(_) => Scope::Admin,
            Self::CreateTable(_) => Scope::Write,
            Self::Mutate(_)
            | Self::CheckAndMutate(_)
            | Self::Batch(_)
            | Self::MutateReturning(_)
            | Self::TxnWrite(_) => Scope::Write,
            // It may end the transaction it asks about (D329: an abort marker), so it's a
            // write on the home partition (audit120's S3 on the design).
            Self::Outcome(_) => Scope::Write,
        }
    }

    /// The decoded request's size, which the in-flight bytes budget counts.
    pub(crate) fn size(&self) -> usize {
        match self {
            Self::Get(r) => r.encoded_len(),
            Self::MultiGet(r) => r.encoded_len(),
            Self::ReadRow(r) => r.encoded_len(),
            Self::Mutate(r) | Self::MutateReturning(r) => r.encoded_len(),
            Self::CheckAndMutate(r) => r.encoded_len(),
            Self::Batch(r) => r.encoded_len(),
            Self::Scan(r) => r.encoded_len(),
            Self::Begin(r) => r.encoded_len(),
            Self::TxnGet(r) => r.encoded_len(),
            Self::TxnGetRow(r) => r.encoded_len(),
            Self::TxnWrite(r) => r.encoded_len(),
            Self::Commit(r) => r.encoded_len(),
            Self::Abort(r) => r.encoded_len(),
            Self::OpenSnapshot(r) => r.encoded_len(),
            Self::ReleaseSnapshot(r) => r.encoded_len(),
            Self::Subscribe(r) => r.encoded_len(),
            Self::ListTables(r) => r.encoded_len(),
            Self::CreateTable(r) => r.encoded_len(),
            Self::DropTable(r) => r.encoded_len(),
            Self::DescribeTable(r) => r.encoded_len(),
            Self::Info(_)
            | Self::Flush(_)
            | Self::Compact(_)
            | Self::Metrics(_)
            | Self::Promote(_) => 0,
            Self::Backup(r) => r.encoded_len(),
            Self::BackupDownload(r) => r.encoded_len(),
            Self::Traverse(r) => r.encoded_len(),
            Self::Nearest(r) => r.encoded_len(),
            Self::CreateIndex(r) => r.encoded_len(),
            Self::VectorInsert(r) => r.encoded_len(),
            Self::VectorDelete(r) => r.encoded_len(),
            Self::VectorSearch(r) => r.encoded_len(),
            Self::VectorVerify(r) => r.encoded_len(),
            // The messages hold their own budget, each until the loader takes it.
            Self::VectorBulkLoad(op) => op.index.len() + 16,
            Self::Follow(r) => r.encoded_len(),
            Self::TextCreateIndex(r) => r.encoded_len(),
            Self::TextPut(r) => r.encoded_len(),
            Self::TextDelete(r) => r.encoded_len(),
            Self::TextSearch(r) => r.encoded_len(),
            Self::TextRebuild(r) | Self::TextVerify(r) => r.encoded_len(),
            Self::Remember(r) => r.encoded_len(),
            Self::Recall(r) => r.encoded_len(),
            Self::MarkUsed(r) => r.encoded_len(),
            Self::Supersede(r) => r.encoded_len(),
            Self::Link(r) => r.encoded_len(),
            Self::Forget(r) => r.encoded_len(),
            Self::MemoryWatch(r) => r.encoded_len(),
            Self::RecallPool(r) => r.encoded_len(),
            Self::RecallBodies(r) => r.encoded_len(),
            Self::TextStats(r) => r.encoded_len(),
            Self::TextSearchPart(r) => r.encoded_len(),
            Self::Outcome(r) => r.encoded_len(),
        }
    }

    /// Whether the call reads a cluster partition's data, so it needs the partition's lease
    /// when it takes its snapshot (D326): the data reads, a transaction's or snapshot's
    /// start, and a transaction's reads. Not a feed's subscribe (feed1248's F1: its
    /// delivery is gated instead), nor the calls that read no rows (the schema, metrics,
    /// maintenance), nor a transaction's `Commit`: one with writes is gated as a write where
    /// it's answered, and a read-only one reads nothing past its transaction's reads.
    pub(crate) fn reads_partition(&self) -> bool {
        matches!(
            self,
            Self::Get(_)
                | Self::MultiGet(_)
                | Self::ReadRow(_)
                | Self::Scan(_)
                | Self::Begin(_)
                | Self::TxnGet(_)
                | Self::TxnGetRow(_)
                | Self::OpenSnapshot(_)
                | Self::Traverse(_)
                | Self::Nearest(_)
                | Self::VectorSearch(_)
                | Self::TextSearch(_)
                | Self::TextStats(_)
                | Self::TextSearchPart(_)
                | Self::Recall(_)
                | Self::RecallPool(_)
                | Self::RecallBodies(_)
        )
    }

    /// Whether the call writes or changes the schema: a replica refuses it (#1436). Derived
    /// from the scope, so a new write RPC is covered without being listed here (cli692's F2
    /// on #1583): every write-scope RPC writes, and `DropTable` is the one admin-scope
    /// write. A transaction's `Commit` is read scope; one with writes is refused by the
    /// engine's gate (R6).
    /// D335: whether the request asked not to wait for its partition's replica acks
    /// (`replication: REPLICATION_MODE_ASYNC`). The calls without the field opt down only
    /// by the header ([`Caller::replication_async`]).
    pub(crate) fn replication_async(&self) -> bool {
        let mode = match self {
            Self::Mutate(r) | Self::MutateReturning(r) => r.replication,
            Self::CheckAndMutate(r) => r.replication,
            Self::Batch(r) => r.replication,
            Self::Commit(r) => r.replication,
            _ => return false,
        };
        mode == proto::ReplicationMode::Async as i32
    }

    /// D335: raises the request's durability to `GroupSync` for a write that waits for
    /// replica acks: the feed ships only what is durable on the owner, so a `None` or
    /// `Buffered` commit would wait for a later sync anyway. The server's default
    /// (`UNSPECIFIED`) is left alone. `CommitInfo.durability` reports what was applied.
    pub(crate) fn raise_durability(&mut self) {
        let d = match self {
            Self::Mutate(r) | Self::MutateReturning(r) => &mut r.durability,
            Self::CheckAndMutate(r) => &mut r.durability,
            Self::Batch(r) => &mut r.durability,
            _ => return,
        };
        if *d == proto::Durability::None as i32 || *d == proto::Durability::Buffered as i32 {
            *d = proto::Durability::GroupSync as i32;
        }
    }

    pub(crate) fn writes(&self) -> bool {
        self.scope() == Scope::Write || matches!(self, Self::DropTable(_))
    }

    /// Calls `f` with each (table or index, key) the call names, in request order, until `f`
    /// returns `Break` (D331's range gates, D332's sampled read keys). A name is a table's or
    /// an index's: they never clash (`validate_indexes`), and an index that follows a table
    /// takes that table's ranges, so a gate maps it to the table.
    ///
    /// - **A table's rows:** the point reads report their rows; the writes (`Mutate`,
    ///   `MutateReturning`, `CheckAndMutate`, `Batch`) and a transaction's reads, writes and
    ///   commit theirs, a row named twice twice. A write naming no row reports nothing: it's
    ///   refused on its way in.
    /// - **A table's ranges:** a scan and a nearest search report their range (no range: the
    ///   whole table), or a nearest search its keys. A resumed scan reports its whole range,
    ///   not the rest after `after`: a superset, safe for a gate and harmless for a sample.
    /// - **A traversal** reports its seeds. The rows its edges lead to aren't in the request:
    ///   it reads them through the partitions' clipped fan-out, which D331 audits apart.
    /// - **An index's ids:** `Text.Put` and `Text.Delete` (which write the source table's
    ///   row beside its postings) and `Vectors.Insert` and `Vectors.Delete` report their id;
    ///   a bulk load, a text rebuild, a search and a verify report the whole index.
    /// - **Nothing:** every other call, each arm saying why. None writes a table's or an
    ///   index's rows; the memory calls write the memory store's own placement, which no move
    ///   names. So a gate passes a call that reports nothing, and a new call must be sorted
    ///   into an arm before it compiles.
    pub(crate) fn touches(
        &self,
        f: &mut dyn FnMut(&str, Touch<'_>) -> ControlFlow<()>,
    ) -> ControlFlow<()> {
        let mut rows = |rows: &[proto::RowWrites]| {
            for k in rows.iter().filter_map(|r| r.row.as_ref()) {
                f(&k.table, Touch::Row(&k.row))?;
            }
            ControlFlow::Continue(())
        };
        match self {
            Self::Get(r) => f(&r.table, Touch::Row(&r.row)),
            Self::ReadRow(r) => f(&r.table, Touch::Row(&r.row)),
            Self::TxnGet(r) => f(&r.table, Touch::Row(&r.row)),
            Self::TxnGetRow(r) => f(&r.table, Touch::Row(&r.row)),
            Self::MultiGet(r) => {
                for k in &r.keys {
                    f(&r.table, Touch::Row(k))?;
                }
                ControlFlow::Continue(())
            }
            Self::Scan(r) => f(&r.table, Touch::of_range(r.range.as_ref())),
            Self::Nearest(r) => match &r.source {
                Some(proto::nearest_request::Source::Keys(keys)) => {
                    for k in &keys.keys {
                        f(&r.table, Touch::Row(k))?;
                    }
                    ControlFlow::Continue(())
                }
                Some(proto::nearest_request::Source::Range(range)) => {
                    f(&r.table, Touch::of_range(Some(range)))
                }
                None => f(&r.table, Touch::of_range(None)),
            },
            Self::Traverse(r) => {
                for k in &r.seeds {
                    f(&r.table, Touch::Row(k))?;
                }
                ControlFlow::Continue(())
            }
            Self::Mutate(r) | Self::MutateReturning(r) => rows(r.writes.as_slice()),
            Self::CheckAndMutate(r) => rows(r.writes.as_slice()),
            Self::Batch(r) => rows(&r.rows),
            Self::TxnWrite(r) => rows(&r.rows),
            Self::Commit(r) => rows(&r.rows),
            // An index's ids: a text index always follows its source table, and `Text.Put`
            // writes the source table's row keyed by exactly the id (`Index::put_in`; pinned
            // by `a_text_put_writes_the_source_row_its_id_names`), so a gate fences that row.
            // A vector index may follow a table, whose ranges route its ids.
            Self::TextPut(r) => f(&r.index, Touch::Row(&r.id)),
            Self::TextDelete(r) => f(&r.index, Touch::Row(&r.id)),
            Self::VectorInsert(r) => f(&r.index, Touch::Row(&r.id)),
            Self::VectorDelete(r) => f(&r.index, Touch::Row(&r.id)),
            // A whole index: its ids are in the stream, or it reads or rebuilds every one.
            Self::VectorBulkLoad(op) => f(&op.index, Touch::of_range(None)),
            Self::VectorSearch(r) => f(&r.index, Touch::of_range(None)),
            Self::VectorVerify(r) => f(&r.index, Touch::of_range(None)),
            Self::TextSearch(r) => f(&r.index, Touch::of_range(None)),
            Self::TextRebuild(r) | Self::TextVerify(r) => f(&r.index, Touch::of_range(None)),
            Self::TextStats(proto::TextStatsRequest { search, .. })
            | Self::TextSearchPart(proto::TextSearchPartRequest { search, .. }) => match search {
                Some(r) => f(&r.index, Touch::of_range(None)),
                None => ControlFlow::Continue(()),
            },
            // A transaction's or snapshot's start and end, a pinned snapshot's release: no rows
            // (a transaction's routing key picks its home, it reads nothing).
            // `Outcome` (D329) reads its own decision record, in the internal decision table,
            // which no move names.
            Self::Begin(_)
            | Self::Abort(_)
            | Self::Outcome(_)
            | Self::OpenSnapshot(_)
            | Self::ReleaseSnapshot(_)
            // The schema, maintenance and the node's own calls: whole tables or none, never a
            // row's write.
            | Self::ListTables(_)
            | Self::CreateTable(_)
            | Self::DropTable(_)
            | Self::DescribeTable(_)
            | Self::CreateIndex(_)
            | Self::TextCreateIndex(_)
            | Self::Info(_)
            | Self::Flush(_)
            | Self::Compact(_)
            | Self::Backup(_)
            | Self::BackupDownload(_)
            | Self::Metrics(_)
            | Self::Promote(_)
            // Feeds: a subscription and a replica's follow read history, not rows; a feed
            // clips by map range where it's served (D331 PR 3).
            | Self::Subscribe(_)
            | Self::Follow(_)
            | Self::MemoryWatch(_)
            // Agent memory: memory ids in the memory store's placement (`ClusterMap.memory`),
            // which a range move never names (D331 Amendment 3).
            | Self::Remember(_)
            | Self::Recall(_)
            | Self::MarkUsed(_)
            | Self::Supersede(_)
            | Self::Link(_)
            | Self::Forget(_)
            | Self::RecallPool(_)
            | Self::RecallBodies(_) => ControlFlow::Continue(()),
        }
    }
}

/// A key a call names ([`ServiceOp::touches`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Touch<'a> {
    /// One row.
    Row(&'a [u8]),
    /// The rows within these bounds, as the scan's request gives them.
    Range {
        start: Bound<&'a [u8]>,
        end: Bound<&'a [u8]>,
    },
    /// Every row starting with these bytes.
    Prefix(&'a [u8]),
}

impl<'a> Touch<'a> {
    /// A read's key range as its request gives it: no range is the whole table.
    fn of_range(range: Option<&'a proto::KeyRange>) -> Self {
        use proto::key_range::Range;
        match range.and_then(|k| k.range.as_ref()) {
            Some(Range::Prefix(p)) => Touch::Prefix(p),
            Some(Range::Bounds(b)) => Touch::Range {
                start: convert::bound(b.start.as_ref()),
                end: convert::bound(b.end.as_ref()),
            },
            None => Touch::Range {
                start: Bound::Unbounded,
                end: Bound::Unbounded,
            },
        }
    }
}

/// The barriers a cluster component starting at `start` carries for serving rows `rows`
/// (D331 PR 3b): every row's, wherever it starts (feed1248's point 2: a start past `t_from`
/// may come from a checkpoint taken while the merge held the part, with the source still
/// below `final_seqno`). One past `t_from` (or from now) carries no token, so the merge's
/// position for the component stays where it starts; the merge lifts it at once once the
/// source's part is past `final_seqno`, gone or absent.
pub(super) fn barriers_for(
    rows: Vec<super::super::ranges::Moved>,
    start: Option<&proto::subscribe_request::Start>,
) -> Vec<super::super::feed::Barrier> {
    use proto::subscribe_request::Start as S;
    let from = match start {
        None | Some(S::FromOldest(false)) | Some(S::FromSnapshot(_)) => u64::MAX,
        Some(S::FromOldest(true)) => 0,
        Some(S::Resume(t)) => super::super::cluster::token::token_seqno(t).unwrap_or(0),
    };
    rows.into_iter()
        .map(|m| super::super::feed::Barrier {
            t_from: m.seqno,
            token: if from <= m.seqno {
                m.token.into()
            } else {
                prost::bytes::Bytes::new()
            },
            move_id: m.id,
            source: m.source,
            final_seqno: m.final_seqno,
        })
        .collect()
}

#[cfg(test)]
mod barriers_for_tests {
    use super::{barriers_for, proto};
    use proto::subscribe_request::Start as S;

    /// D331 PR 3b (feed1248's point 2): a component of a move's target gets the move's
    /// barrier wherever it starts. At or before `t_from` it carries the hand-over's token;
    /// past it (a checkpoint taken while the merge held the part) or from now, none, so the
    /// merge's position doesn't move. Mutant: give only starts at or before `t_from` a
    /// barrier, and the start past it gets none.
    #[test]
    fn every_start_gets_the_barrier() {
        let token = |seqno: u64| {
            let mut t = vec![1u8];
            t.extend_from_slice(&[7u8; 16]);
            t.extend_from_slice(&1u32.to_le_bytes());
            t.extend_from_slice(&seqno.to_le_bytes());
            t
        };
        let row = super::super::super::ranges::Moved {
            id: "mv1".into(),
            table: "users".into(),
            seqno: 10,
            source: "p1".into(),
            final_seqno: 99,
            token: token(10),
            ..Default::default()
        };
        let at = |start: Option<S>| barriers_for(vec![row.clone()], start.as_ref());
        let before = at(Some(S::Resume(token(5).into())));
        assert_eq!(before.len(), 1);
        assert_eq!(before[0].token.as_ref(), token(10).as_slice());
        let exactly = at(Some(S::Resume(token(10).into())));
        assert_eq!(exactly[0].token.as_ref(), token(10).as_slice());
        for start in [Some(S::Resume(token(20).into())), None] {
            let past = at(start);
            assert_eq!(past.len(), 1, "a start past t_from still gets it");
            assert!(past[0].token.is_empty());
            assert_eq!((past[0].t_from, past[0].final_seqno), (10, 99));
        }
    }
}

#[cfg(test)]
mod touches_tests {
    use std::ops::{Bound, ControlFlow};

    use super::{ServiceOp, Touch, proto};

    /// What `op.touches` reports, owned, stopping after `limit` keys.
    fn touched(op: &ServiceOp, limit: usize) -> Vec<(String, String)> {
        let mut out = Vec::new();
        let _ = op.touches(&mut |table: &str, touch: Touch<'_>| {
            let show = |b: Bound<&[u8]>| match b {
                Bound::Included(k) => format!("[{}", String::from_utf8_lossy(k)),
                Bound::Excluded(k) => format!("({}", String::from_utf8_lossy(k)),
                Bound::Unbounded => "*".to_owned(),
            };
            let key = match touch {
                Touch::Row(k) => String::from_utf8_lossy(k).into_owned(),
                Touch::Prefix(p) => format!("{}…", String::from_utf8_lossy(p)),
                Touch::Range { start, end } => format!("{}..{}", show(start), show(end)),
            };
            out.push((table.to_owned(), key));
            if out.len() == limit {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        });
        out
    }

    fn pairs(v: &[(&str, &str)]) -> Vec<(String, String)> {
        v.iter()
            .map(|&(t, k)| (t.to_owned(), k.to_owned()))
            .collect()
    }

    fn row(table: &str, row: &'static str) -> proto::RowWrites {
        proto::RowWrites {
            row: Some(proto::RowKey {
                table: table.into(),
                row: row.into(),
            }),
            writes: Vec::new(),
        }
    }

    #[test]
    fn point_reads_report_their_rows_in_request_order() {
        let get = ServiceOp::Get(proto::GetRequest {
            table: "t".into(),
            row: "a".into(),
            ..Default::default()
        });
        assert_eq!(touched(&get, usize::MAX), pairs(&[("t", "a")]));
        let multi = ServiceOp::MultiGet(proto::MultiGetRequest {
            table: "t".into(),
            keys: vec!["c".into(), "a".into(), "c".into()],
            ..Default::default()
        });
        assert_eq!(
            touched(&multi, usize::MAX),
            pairs(&[("t", "c"), ("t", "a"), ("t", "c")])
        );
        let txn = ServiceOp::TxnGetRow(proto::TxnGetRowRequest {
            table: "u".into(),
            row: "r".into(),
            ..Default::default()
        });
        assert_eq!(touched(&txn, usize::MAX), pairs(&[("u", "r")]));
    }

    #[test]
    fn a_scan_reports_its_range_as_given() {
        use proto::key_range::Range;
        let scan = |range| {
            ServiceOp::Scan(proto::ScanRequest {
                table: "t".into(),
                range,
                ..Default::default()
            })
        };
        let prefix = scan(Some(proto::KeyRange {
            range: Some(Range::Prefix("ab".into())),
        }));
        assert_eq!(touched(&prefix, usize::MAX), pairs(&[("t", "ab…")]));
        let bounds = scan(Some(proto::KeyRange {
            range: Some(Range::Bounds(proto::Bounds {
                start: Some(proto::Bound {
                    key: "b".into(),
                    inclusive: false,
                }),
                end: Some(proto::Bound {
                    key: "m".into(),
                    inclusive: true,
                }),
            })),
        }));
        assert_eq!(touched(&bounds, usize::MAX), pairs(&[("t", "(b..[m")]));
        let open_end = scan(Some(proto::KeyRange {
            range: Some(Range::Bounds(proto::Bounds {
                start: Some(proto::Bound {
                    key: "b".into(),
                    inclusive: true,
                }),
                end: None,
            })),
        }));
        assert_eq!(touched(&open_end, usize::MAX), pairs(&[("t", "[b..*")]));
        assert_eq!(touched(&scan(None), usize::MAX), pairs(&[("t", "*..*")]));
    }

    #[test]
    fn writes_report_each_row_with_its_own_table() {
        let batch = ServiceOp::Batch(proto::BatchRequest {
            rows: vec![
                row("t", "a"),
                proto::RowWrites::default(),
                row("u", "b"),
                row("t", "a"),
            ],
            ..Default::default()
        });
        assert_eq!(
            touched(&batch, usize::MAX),
            pairs(&[("t", "a"), ("u", "b"), ("t", "a")])
        );
        let mutate = ServiceOp::MutateReturning(proto::MutateRequest {
            writes: Some(row("t", "x")),
            ..Default::default()
        });
        assert_eq!(touched(&mutate, usize::MAX), pairs(&[("t", "x")]));
        let no_row = ServiceOp::Mutate(proto::MutateRequest::default());
        assert!(touched(&no_row, usize::MAX).is_empty());
        let check = ServiceOp::CheckAndMutate(proto::CheckAndMutateRequest {
            writes: Some(row("t", "y")),
            ..Default::default()
        });
        assert_eq!(touched(&check, usize::MAX), pairs(&[("t", "y")]));
        let write = ServiceOp::TxnWrite(proto::TxnWriteRequest {
            rows: vec![row("t", "p")],
            ..Default::default()
        });
        assert_eq!(touched(&write, usize::MAX), pairs(&[("t", "p")]));
        let commit = ServiceOp::Commit(proto::TxnCommitRequest {
            rows: vec![row("u", "q"), row("t", "p")],
            ..Default::default()
        });
        assert_eq!(
            touched(&commit, usize::MAX),
            pairs(&[("u", "q"), ("t", "p")])
        );
    }

    #[test]
    fn break_stops_the_walk() {
        let batch = ServiceOp::Batch(proto::BatchRequest {
            rows: vec![row("t", "a"), row("t", "b"), row("t", "c")],
            ..Default::default()
        });
        assert_eq!(touched(&batch, 1), pairs(&[("t", "a")]));
        assert_eq!(
            batch.touches(&mut |_, _| ControlFlow::Break(())),
            ControlFlow::Break(())
        );
        assert_eq!(
            batch.touches(&mut |_, _| ControlFlow::Continue(())),
            ControlFlow::Continue(())
        );
    }

    #[test]
    fn calls_without_rows_or_ids_report_nothing() {
        for op in [
            ServiceOp::Info(proto::Empty::default()),
            ServiceOp::Begin(proto::BeginRequest::default()),
            ServiceOp::Abort(proto::TxnAbortRequest::default()),
            ServiceOp::TextCreateIndex(proto::CreateTextIndexRequest::default()),
            ServiceOp::Forget(proto::ForgetRequest {
                ids: vec!["a".into()],
            }),
        ] {
            assert!(touched(&op, usize::MAX).is_empty(), "{op:?}");
        }
    }

    #[test]
    fn index_calls_report_their_ids_or_the_whole_index() {
        let put = ServiceOp::TextPut(proto::TextPutRequest {
            index: "x".into(),
            id: "d1".into(),
            ..Default::default()
        });
        assert_eq!(touched(&put, usize::MAX), pairs(&[("x", "d1")]));
        let delete = ServiceOp::VectorDelete(proto::VectorDeleteRequest {
            index: "v".into(),
            id: "a".into(),
        });
        assert_eq!(touched(&delete, usize::MAX), pairs(&[("v", "a")]));
        let rebuild = ServiceOp::TextRebuild(proto::TextIndexRequest { index: "x".into() });
        assert_eq!(touched(&rebuild, usize::MAX), pairs(&[("x", "*..*")]));
        let stats = ServiceOp::TextStats(proto::TextStatsRequest {
            search: Some(proto::TextSearchRequest {
                index: "x".into(),
                ..Default::default()
            }),
        });
        assert_eq!(touched(&stats, usize::MAX), pairs(&[("x", "*..*")]));
    }

    /// What a gate maps `Text.Put`'s id to: the source table's row of the same key.
    #[test]
    fn a_text_put_writes_the_source_row_its_id_names() {
        use pigeonhole::{Family, Options, Pigeonhole};
        use pigeonhole_text::{Index, IndexOptions, Source};
        let dir = pigeonhole::doc_support::temp_dir();
        let db = Pigeonhole::open(dir.join("t.phdb"), Options::default().shards(1)).unwrap();
        let table = db
            .table("docs")
            .unwrap()
            .family("f", Family::default())
            .create()
            .unwrap();
        let index = Index::create(
            &db,
            "x",
            Source::new(&table, "f", b"body"),
            IndexOptions::new(),
        )
        .unwrap();
        index.put(b"d1", "hello there").unwrap();
        let cell = table.get(b"d1", "f", b"body").unwrap().unwrap();
        assert_eq!(cell.value(), b"hello there");
    }

    #[test]
    fn a_nearest_search_reports_its_rows_and_a_traversal_its_seeds() {
        use proto::nearest_request::Source;
        let nearest = |source| {
            ServiceOp::Nearest(proto::NearestRequest {
                table: "t".into(),
                source,
                ..Default::default()
            })
        };
        let keys = nearest(Some(Source::Keys(proto::KeyList {
            keys: vec!["b".into(), "a".into()],
        })));
        assert_eq!(touched(&keys, usize::MAX), pairs(&[("t", "b"), ("t", "a")]));
        let range = nearest(Some(Source::Range(proto::KeyRange {
            range: Some(proto::key_range::Range::Prefix("ab".into())),
        })));
        assert_eq!(touched(&range, usize::MAX), pairs(&[("t", "ab…")]));
        assert_eq!(touched(&nearest(None), usize::MAX), pairs(&[("t", "*..*")]));
        let traverse = ServiceOp::Traverse(proto::TraverseRequest {
            table: "g".into(),
            seeds: vec!["s1".into(), "s2".into()],
            ..Default::default()
        });
        assert_eq!(
            touched(&traverse, usize::MAX),
            pairs(&[("g", "s1"), ("g", "s2")])
        );
    }
}
