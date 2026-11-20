use super::*;

/// A text index's partition can answer a search's round only once it's filled: a cluster
/// creates its indexes unfilled, and fills them from the source in the background (#1722
/// PR 1a). Until then its statistics and hits would be short. `Unavailable`, which an
/// `allow_partial` search skips as it does an unreachable partition.
///
/// The fill only, never a client's `Text.Rebuild` (#1722 PR 2c): a rebuild leaves every
/// document indexed while it runs, as on a single node, where searches go on throughout.
fn filled(index: &pigeonhole_text::Index, name: &str) -> Result<(), ApiError> {
    if index.fill_pending()? {
        return Err(ApiError::unavailable(format!(
            "text index {name:?} is still filling from its source on this partition"
        )));
    }
    Ok(())
}

/// Another node's `QueryStats` bytes, or `FailedPrecondition`: a version this build doesn't
/// read (the nodes' builds differ), or bytes it refuses.
pub(crate) fn unreadable_stats(
    stats: pigeonhole_text::Result<QueryStats>,
) -> Result<QueryStats, ApiError> {
    stats.map_err(|e| {
        ApiError::failed_precondition(format!(
            "a text search's statistics from another node that this node can't read: {e}"
        ))
    })
}

#[cfg(test)]
mod text_part_tests {
    use super::filled;
    use pigeonhole::{Family, Options, Pigeonhole};
    use pigeonhole_text::{Index, IndexOptions, Source};

    /// #1722 PR 2b: a partition whose text index is still filling from its source (a
    /// cluster creates it unfilled) can't answer a search's round: `Unavailable`, which an
    /// `allow_partial` search skips. Once filled, it answers.
    #[test]
    fn a_text_index_still_filling_answers_no_round() {
        let dir = pigeonhole::doc_support::temp_dir();
        let db = Pigeonhole::open(dir.join("t.phdb"), Options::default().shards(1)).unwrap();
        let table = db
            .table("docs")
            .unwrap()
            .family("f", Family::default())
            .create()
            .unwrap();
        let index = Index::create_unfilled(
            &db,
            "x",
            Source::new(&table, "f", b"body"),
            IndexOptions::new(),
        )
        .unwrap();
        let e = filled(&index, "x").unwrap_err();
        assert_eq!(e.code, "Unavailable", "{}", e.message);
        assert!(e.message.contains("still filling"), "{}", e.message);
        index
            .resume_rebuild_with(|_| std::ops::ControlFlow::Continue(()))
            .unwrap();
        filled(&index, "x").unwrap();
        // A client's rebuild (#1722 PR 2c), here stopped after its first document, leaves
        // a rebuild pending but every document indexed: the partition still answers.
        for id in [b"a", b"b"] {
            table
                .mutate(id)
                .put("f", b"body", b"alpha")
                .commit()
                .unwrap();
        }
        let r = index
            .rebuild_with(|_| std::ops::ControlFlow::Break(()))
            .unwrap();
        assert!(r.stopped && index.rebuild_pending().unwrap(), "{r:?}");
        filled(&index, "x").unwrap();
    }
}

impl Service {
    /// Whether a load of `expected` vectors into index `name` fits this node's
    /// `--bulk-load-bytes` with a part on each of `siblings` partitions ([`super::bulk::fits`]):
    /// a cluster's entry node checks its own partitions before reading a load (#1722 PR 3).
    pub(crate) async fn bulk_fits(
        &self,
        name: &str,
        expected: u64,
        siblings: u64,
    ) -> Result<(), ApiError> {
        let index = self.index(name).await?;
        super::bulk::fits(
            &index,
            expected,
            siblings,
            self.limits.bulk_load_bytes as u64,
        )
    }

    /// The IVF index `name`, from the cache, or opened on the blocking pool on a miss.
    async fn index(&self, name: &str) -> Result<Arc<pigeonhole_vector::Index>, ApiError> {
        if let Some(i) = self.part()?.indexes.cached(name) {
            return Ok(i);
        }
        let indexes = Arc::clone(&self.part()?.indexes);
        let name = name.to_owned();
        self.blocking(move || indexes.get(&name)).await
    }

    /// `Vectors.CreateIndex` (ICR 0100): a new index, or with `if_missing` an existing one.
    async fn create_index(
        &self,
        r: proto::CreateVectorIndexRequest,
    ) -> Result<proto::Empty, ApiError> {
        // Where the ids are is a cluster's (#1722): a single node holds them all.
        if !r.follows.is_empty() || r.placement.is_some() {
            return Err(ApiError::invalid(
                "an index's follows and placement are for a cluster node: this server is a \
                 single node, whose index holds every id",
            ));
        }
        let (spec, options) = super::indexes::create_args(&r)?;
        let indexes = Arc::clone(&self.part()?.indexes);
        self.blocking(move || indexes.create(&r.name, spec, options, r.if_missing))
            .await?;
        Ok(proto::Empty {})
    }

    /// `Vectors.Insert`: one vector under `id`, in the index's element type. A write: it runs
    /// detached, on the blocking pool (an insert is a transaction, and may rebalance inline).
    async fn vector_insert(
        &self,
        r: proto::VectorInsertRequest,
        budget: OwnedSemaphorePermit,
    ) -> Result<proto::Empty, ApiError> {
        let v = r
            .vector
            .as_ref()
            .ok_or_else(|| ApiError::invalid("an insert needs a vector"))?;
        let v = convert::elements(v)?;
        let index = self.index(&r.index).await?;
        let pool = Arc::clone(&self.blocking);
        self.detached("Vectors.Insert", budget, async move {
            blocking(&pool, move || {
                use convert::Elements as V;
                let id = &r.id[..];
                match &v {
                    V::F32(x) => index.insert_f32(id, x),
                    V::I8(x) => index.insert_i8(id, x),
                    V::Bits(x) => index.insert_bits(id, x),
                    V::F16(x) => index.insert_f16(id, x),
                    V::Bf16(x) => index.insert_bf16(id, x),
                }?;
                Ok(())
            })
            .await
        })
        .await?;
        Ok(proto::Empty {})
    }

    /// `Vectors.Delete`: the vector under `id`, if any. Detached, on the blocking pool.
    async fn vector_delete(
        &self,
        r: proto::VectorDeleteRequest,
        budget: OwnedSemaphorePermit,
    ) -> Result<proto::Empty, ApiError> {
        let index = self.index(&r.index).await?;
        let pool = Arc::clone(&self.blocking);
        self.detached("Vectors.Delete", budget, async move {
            blocking(&pool, move || {
                index.delete(&r.id)?;
                Ok(())
            })
            .await
        })
        .await?;
        Ok(proto::Empty {})
    }

    /// `Vectors.Search`: the `k` (at most [`MAX_NEAREST_K`]) nearest ids, on the request's
    /// task (`run_async_with_stats`), holding no blocking thread once the index is open.
    async fn vector_search(
        &self,
        r: proto::VectorSearchRequest,
    ) -> Result<proto::VectorSearchResponse, ApiError> {
        if r.k == 0 || r.k > MAX_NEAREST_K {
            return Err(ApiError::invalid(format!("k is 1 to {MAX_NEAREST_K}")));
        }
        let q = r
            .query
            .as_ref()
            .ok_or_else(|| ApiError::invalid("a search needs a query vector"))?;
        let q = convert::elements(q)?;
        let index = self.index(&r.index).await?;
        let k = r.k as usize;
        let mut s = {
            use convert::Elements as V;
            match &q {
                V::F32(x) => index.search_f32(x, k),
                V::I8(x) => index.search_i8(x, k),
                V::Bits(x) => index.search_bits(x, k),
                V::F16(x) => index.search_f16(x, k),
                V::Bf16(x) => index.search_bf16(x, k),
            }
        };
        if r.nprobe > 0 {
            s = s.nprobe(r.nprobe);
        }
        if !r.id_prefix.is_empty() {
            s = s.id_prefix(&r.id_prefix);
        }
        if r.max_probe > 0 {
            s = s.max_probe(r.max_probe);
        }
        let (hits, stats) = s.run_async_with_stats().await?;
        Ok(proto::VectorSearchResponse {
            hits: hits
                .into_iter()
                .map(|h| proto::VectorHit {
                    id: h.id.into(),
                    distance: h.distance,
                })
                .collect(),
            stats: r.stats.then_some(proto::VectorSearchStats {
                lists_probed: stats.lists_probed,
                lists: stats.lists,
            }),
        })
    }

    /// The text index `name`, from the cache, or opened on the blocking pool on a miss.
    async fn text_index(&self, name: &str) -> Result<Arc<pigeonhole_text::Index>, ApiError> {
        if let Some(i) = self.part()?.texts.cached(name) {
            return Ok(i);
        }
        let texts = Arc::clone(&self.part()?.texts);
        let name = name.to_owned();
        self.blocking(move || texts.get(&name)).await
    }

    /// `Text.CreateIndex` (ICR 0105): a new index over its source column, or with
    /// `if_missing` an existing one, as it is.
    async fn text_create_index(
        &self,
        r: proto::CreateTextIndexRequest,
    ) -> Result<proto::Empty, ApiError> {
        let options = super::texts::options(&r)?;
        let table = self.table(&r.source_table).await?;
        let source = pigeonhole_text::Source::new(&table, &r.source_family, &r.source_qualifier);
        let texts = Arc::clone(&self.part()?.texts);
        // Noted under `schema`, where a drop of the source checks for its indexes (#1916),
        // and held until the index (its source stored first) is built: a drop then refuses
        // it, and one that checked first has dropped the source, which the create can't
        // open. The build itself runs without `schema`, which it could hold for minutes.
        // The pool place first (its lock order).
        let place = pool_place(&self.blocking).await?;
        let creating = {
            let _schema = self.part()?.schema.lock().await;
            texts.creating(&r.source_table)
        };
        held(place, move || {
            let _creating = creating;
            texts.create(&r.name, source, options, r.if_missing)
        })
        .await?;
        Ok(proto::Empty {})
    }

    /// `Text.Put`: writes the document's text to the source column and indexes it, in one
    /// transaction. A write: detached, on the blocking pool.
    async fn text_put(
        &self,
        r: proto::TextPutRequest,
        budget: OwnedSemaphorePermit,
    ) -> Result<proto::Empty, ApiError> {
        let index = self.text_index(&r.index).await?;
        let pool = Arc::clone(&self.blocking);
        self.detached("Text.Put", budget, async move {
            blocking(&pool, move || {
                index.put(&r.id, &r.text)?;
                Ok(())
            })
            .await
        })
        .await?;
        Ok(proto::Empty {})
    }

    /// `Text.Delete`: the document's text and its postings, if any. Detached, as `Put`.
    async fn text_delete(
        &self,
        r: proto::TextDeleteRequest,
        budget: OwnedSemaphorePermit,
    ) -> Result<proto::Empty, ApiError> {
        let index = self.text_index(&r.index).await?;
        let pool = Arc::clone(&self.blocking);
        self.detached("Text.Delete", budget, async move {
            blocking(&pool, move || {
                index.delete(&r.id)?;
                Ok(())
            })
            .await
        })
        .await?;
        Ok(proto::Empty {})
    }

    /// `Text.Search`: the `k` (at most [`MAX_NEAREST_K`]) best documents by BM25, with
    /// their text and matches on request. On the blocking pool: a search is sync (D309).
    async fn text_search(
        &self,
        r: proto::TextSearchRequest,
    ) -> Result<proto::TextSearchResponse, ApiError> {
        self.text_search_with(r, None, false).await
    }

    /// A search's round 2 on one partition of a text index across several (#1722 PR 2b,
    /// ICR 0138): its top `k`, scored with every partition's statistics merged. Without
    /// statistics (an index on one partition), its own: the whole index's.
    async fn text_search_part(
        &self,
        r: proto::TextSearchPartRequest,
    ) -> Result<proto::TextSearchResponse, ApiError> {
        let search = r
            .search
            .ok_or_else(|| ApiError::invalid("a text search's part carries no search"))?;
        let stats = if r.stats.is_empty() {
            None
        } else {
            Some(unreadable_stats(QueryStats::from_bytes(&r.stats))?)
        };
        self.text_search_with(search, stats, true).await
    }

    /// A text search, with `stats` to score with (round 2 across partitions), or reading
    /// its own. `part`: a partition's part of a search across the index's partitions,
    /// which a partition whose index is still filling can't answer.
    async fn text_search_with(
        &self,
        r: proto::TextSearchRequest,
        stats: Option<QueryStats>,
        part: bool,
    ) -> Result<proto::TextSearchResponse, ApiError> {
        if r.k == 0 || r.k > MAX_NEAREST_K {
            return Err(ApiError::invalid(format!("k is 1 to {MAX_NEAREST_K}")));
        }
        let query = super::texts::query(&r)?;
        let index = self.text_index(&r.index).await?;
        self.blocking(move || {
            if part {
                filled(&index, &r.index)?;
            }
            let mut s = index.search(query, r.k as usize).highlight(r.highlight);
            if !r.id_prefix.is_empty() {
                s = s.id_prefix(r.id_prefix.to_vec());
            }
            if let Some(stats) = stats {
                s = s.stats(stats);
            }
            let (hits, stats) = s.run_with_stats()?;
            Ok(proto::TextSearchResponse {
                hits: hits.into_iter().map(super::texts::hit).collect(),
                cut_clauses: stats
                    .fuzzy
                    .iter()
                    .filter(|f| f.cut)
                    .map(|f| u32::try_from(f.clause).unwrap_or(u32::MAX))
                    .collect(),
                ..Default::default()
            })
        })
        .await
    }

    /// A search's round 1 on one partition of a text index across several (#1722 PR 2b,
    /// ICR 0138): the partition's statistics for the query, read at one snapshot, as
    /// `QueryStats`' versioned bytes. `k` and `id_prefix` don't change them.
    async fn text_stats(
        &self,
        r: proto::TextStatsRequest,
    ) -> Result<proto::TextStatsResponse, ApiError> {
        let r = r
            .search
            .ok_or_else(|| ApiError::invalid("a text search's statistics carry no search"))?;
        let query = super::texts::query(&r)?;
        let index = self.text_index(&r.index).await?;
        self.blocking(move || {
            filled(&index, &r.index)?;
            // `k` doesn't change the statistics (nor do `id_prefix` or `highlight`), so
            // the request's isn't checked here: the node called checked it once.
            let stats = index.search(query, 1).query_stats()?;
            Ok(proto::TextStatsResponse {
                stats: stats.to_bytes().into(),
            })
        })
        .await
    }

    /// `Text.Rebuild`: rewrites the index from its source column. One at a time, on a
    /// thread of its own off the shards' cores, never the blocking pool (perf287's D1: a
    /// rebuild reads the whole source). A write: it finishes if the client goes away.
    ///
    /// It starts from the first row, whatever a stopped rebuild left (ai1249 on #1756), and
    /// runs a document at a time under the partition's gate: a fence stops it at the next
    /// document, answering `Fenced` with what it did, and it resumes on its own once the
    /// partition takes writes again. A resume holding the node's rebuild slot gives it up at
    /// its next document for this call.
    async fn text_rebuild(
        &self,
        r: proto::TextIndexRequest,
    ) -> Result<proto::TextRebuildResponse, ApiError> {
        let index = self.text_index(&r.index).await?;
        let slot = self
            .text_rebuilds
            .client(self.limits.request_timeout)
            .await?;
        let part = self.part()?;
        let placement = part.bulk.placement.clone();
        let texts = Arc::clone(&part.texts);
        let (tx, rx) = oneshot::channel();
        // The first document runs under the request's admission; each later one enters the
        // gate itself (`Steps`), so a fence's drain waits for one document.
        let held = self.gate.as_ref().map(super::gate::WriteGate::extend);
        let steps_texts = Arc::clone(&texts);
        texts.spawn_rebuild(move || {
            if let Some(p) = placement {
                p.enter();
            }
            let mut steps = super::texts::Steps::new(steps_texts, held, None);
            let r = index.rebuild_with(|_| steps.step());
            let stop = steps.finish();
            drop(index);
            drop(slot);
            let _ = tx.send((r, stop));
        })?;
        let (done, stop) = rx
            .await
            .map_err(|_| ApiError::internal("the rebuild's thread ended early"))?;
        let stats = done?;
        if stats.stopped {
            let did = format!(
                "the rebuild of text index {:?} stopped after {} documents ({} changed), which \
                 stand",
                r.index, stats.documents, stats.changed
            );
            return Err(match stop {
                Some(super::texts::Stop::Fenced(e)) => {
                    texts.resume_soon(&self.text_rebuilds);
                    let why = e.message.trim_end_matches("; nothing was written");
                    ApiError::fenced(format!(
                        "{did}: {why}. It resumes on its own once the partition takes writes \
                         again"
                    ))
                }
                _ => ApiError::from_code(
                    pigeonhole::ErrorCode::Closed,
                    &format!("{did}: the server is shutting down. It resumes when it serves again"),
                ),
            });
        }
        Ok(proto::TextRebuildResponse {
            documents: stats.documents,
            changed: stats.changed,
            invalid_utf8: stats.invalid_utf8,
        })
    }

    /// `Text.Verify`: checks the index against itself and its source, on the blocking pool
    /// (as `Vectors.Verify` does).
    async fn text_verify(
        &self,
        r: proto::TextIndexRequest,
    ) -> Result<proto::TextVerifyResponse, ApiError> {
        let index = self.text_index(&r.index).await?;
        self.blocking(move || {
            let s = index.verify()?;
            Ok(proto::TextVerifyResponse {
                documents: s.documents,
                terms: s.terms,
                postings: s.postings,
                forward_mismatch: s.forward_mismatch,
                source_mismatch: s.source_mismatch,
            })
        })
        .await
    }

    /// `Vectors.Verify`: checks the index's lists against its vectors, on the blocking pool.
    async fn vector_verify(
        &self,
        r: proto::VectorVerifyRequest,
    ) -> Result<proto::VectorVerifyResponse, ApiError> {
        let index = self.index(&r.index).await?;
        self.blocking(move || {
            let stats = index.verify()?;
            Ok(proto::VectorVerifyResponse {
                vectors: stats.vectors,
                lists: stats.list_lens.len() as u64,
            })
        })
        .await
    }
}
