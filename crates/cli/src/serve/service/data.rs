use super::*;

impl Service {
    async fn get(
        &self,
        r: proto::GetRequest,
        caller: &Caller,
    ) -> Result<proto::GetResponse, ApiError> {
        convert::check_aggregate(r.aggregate.as_ref())?;
        let pinned = self.pinned(&r.snapshot_id, caller)?;
        let table = self.table(&r.table).await?;
        let cell = match &pinned {
            Some(p) => {
                table
                    .get_at_async(p.snapshot(), &r.row, &r.family, &r.qualifier)
                    .await?
            }
            None => table.get_async(&r.row, &r.family, &r.qualifier).await?,
        };
        drop(pinned);
        Ok(proto::GetResponse {
            cell: cell.map(|c| {
                let kind = convert::View::of(&table, &r.family);
                convert::cell(&r.family, &r.qualifier, &c, kind, r.aggregate.as_ref())
            }),
        })
    }

    /// On the blocking pool: `KeysRead::read_within` is sync-only (D196). TODO(#398, #1286):
    /// async once multi-get pages have an async form, so a multi-get holds no blocking thread.
    async fn multi_get(
        &self,
        r: proto::MultiGetRequest,
        caller: &Caller,
    ) -> Result<(proto::MultiGetResponse, proto::MultiGetRequest), ApiError> {
        let pinned = self.pinned_selection(r.selection.as_ref(), caller)?;
        let table = self.table(&r.table).await?;
        let db = self.part()?.db.clone();
        let budget = page_bytes(r.page_bytes);
        // The request comes back with the reply, for a strict path's second read (#1661).
        self.blocking(move || {
            let reply = (|| -> Result<proto::MultiGetResponse, ApiError> {
                // One snapshot for the page's reads, the request's or one taken now, so a
                // read again ends exactly where the first measured (#1848). Only a table with
                // a family that has a view (by its kind, whatever the selection asks) is ever
                // read again, so any other reads now, at no snapshot's cost (audit120's S1
                // and B2 on #1872).
                let at = match &pinned {
                    Some(p) => Some(p.snapshot().clone()),
                    None if convert::Kinds::new(&table).any_view() => Some(db.snapshot()?),
                    None => None,
                };
                let select = convert::Select::new(r.selection.as_ref())?.at(at);
                let keys = KeySet::from_unsorted(r.keys.iter().map(|k| &k[..]));
                let after = if r.after.is_empty() {
                    None
                } else {
                    super::cluster::position::refuse_on_a_single_node(&r.after)?;
                    Some(KeysPosition::from_bytes(&r.after).map_err(ApiError::from)?)
                };
                let q = select.aggregate();
                let kinds = convert::Kinds::new(&table);
                // Converted a column at a time, stopping at the first that doesn't fit with its
                // aggregate view, then read again to end the page there (audit120's F4 and its
                // S1 on #1812): no more than the budget and one column is ever built. Both
                // reads see the same snapshot, so the second keeps exactly the columns the
                // first measured; a write between them can't leave it an empty page (#1848).
                let (page, rows) = page_within(
                    &r.table,
                    budget,
                    &kinds,
                    q,
                    |within| {
                        let mut read = select.keys(table.keys(&keys))?;
                        if let Some(after) = &after {
                            read = read.resume_after(after);
                        }
                        Ok(read.read_within(within)?)
                    },
                    |page| page.rows(),
                )?;
                let bytes = rows.iter().map(row_bytes).sum();
                Ok(proto::MultiGetResponse {
                    missing: Vec::new(),
                    rows,
                    next: page
                        .resume()
                        .map(|p| p.to_bytes().into())
                        .unwrap_or_default(),
                    bytes,
                })
            })()?;
            Ok((reply, r))
        })
        .await
    }

    /// `Graph.Traverse` (D304): the hops from the seeds over one edge family, at the pinned
    /// snapshot `snapshot_id` names, or now, cut to `page_bytes` of nodes (D308; 0 is the
    /// server's default, and the server caps it), so a reply is bounded as a `MultiGet`'s is
    /// (simp724's L1 on #1565). Run on the request's task (`run_async` never blocks on a block
    /// it can fetch), so it holds no blocking thread.
    async fn traverse(
        &self,
        r: &proto::TraverseRequest,
        caller: &Caller,
    ) -> Result<proto::TraverseResponse, ApiError> {
        if r.seeds.is_empty() {
            return Err(ApiError::invalid(
                "a traversal starts from at least one seed",
            ));
        }
        if r.hops > 64 {
            return Err(ApiError::invalid("hops is at most 64 (0 is 1)"));
        }
        let pinned = self.pinned(&r.snapshot_id, caller)?;
        let table = self.table(&r.table).await?;
        let seeds = KeySet::from_unsorted(r.seeds.iter().map(|k| &k[..]));
        let mut t = convert::edges(table.traverse(&seeds).family(&r.family), r)?
            .hops(r.hops.max(1))
            .parents(r.parents)
            .byte_budget(page_bytes(r.page_bytes));
        if let Some(p) = &pinned {
            t = t.snapshot(p.snapshot());
        }
        if r.per_node_limit > 0 {
            t = t.per_node_limit(r.per_node_limit);
        }
        if r.hop_limit > 0 {
            t = t.hop_limit(r.hop_limit);
        }
        if r.max_nodes > 0 {
            t = t.max_nodes(r.max_nodes);
        }
        let found = t.run_async().await?;
        let stats = found.stats();
        Ok(proto::TraverseResponse {
            hops: found
                .hops()
                .iter()
                .map(|h| proto::Hop {
                    nodes: h.nodes().iter().map(|k| k.to_vec().into()).collect(),
                    parents: h.parents().map(<[u32]>::to_vec).unwrap_or_default(),
                })
                .collect(),
            stats: Some(proto::TraverseStats {
                rows: stats.rows,
                edges: stats.edges,
                hops_cut: stats.hops_cut,
                cut_by_max_nodes: stats.cut_by_max_nodes,
                cut_by_budget: stats.cut_by_budget,
            }),
            missing: Vec::new(),
        })
    }

    /// `Vectors.Nearest` (D296, D303): the exact top `k` (at most [`MAX_NEAREST_K`]) of one
    /// vector family over a key range or a key list, at the pinned snapshot `snapshot_id`
    /// names, or now, cut to `page_bytes` by D308's measure (0 is the server's default, and
    /// the server caps it), so a reply is bounded as a `MultiGet`'s is. Run on the request's
    /// task, as `Traverse`.
    async fn nearest(
        &self,
        r: &proto::NearestRequest,
        caller: &Caller,
    ) -> Result<proto::NearestResponse, ApiError> {
        use proto::nearest_request::Source;
        if r.k == 0 || r.k > MAX_NEAREST_K {
            return Err(ApiError::invalid(format!("k is 1 to {MAX_NEAREST_K}")));
        }
        let query = r
            .query
            .as_ref()
            .ok_or_else(|| ApiError::invalid("a nearest search needs a query vector"))?;
        let query = convert::nearest_query(query)?;
        let pinned = self.pinned(&r.snapshot_id, caller)?;
        let table = self.table(&r.table).await?;
        let at = pinned.as_ref().map(|p| p.snapshot().clone());
        // The qualifier selection (`qualifiers`, #1915 PR 3), applied as a read's own. Boxed:
        // it lives across the search's await, and a `Selection` in place grew every call's
        // future by its size (serve-get's ceiling on #1957).
        let qualifiers = r.qualifiers.as_ref().map(|q| {
            Box::new(proto::Selection {
                qualifier_prefix: q.prefix.clone(),
                qualifier_start: q.start.clone(),
                qualifier_end: q.end.clone(),
                ..Default::default()
            })
        });
        // A fault seam (tests): a partition acting as a build before `qualifiers`.
        #[cfg(test)]
        let qualifiers = qualifiers.filter(|_| {
            !self.part().is_ok_and(|p| {
                super::cluster::old_nearest(p.db.path(), super::cluster::OldNearest::Parts)
            })
        });
        let select = convert::Select::new(qualifiers.as_deref())?.at(at);
        let k = r.k as usize;
        let search = match &r.source {
            Some(Source::Keys(keys)) => {
                let keys = KeySet::from_unsorted(keys.keys.iter().map(|k| &k[..]));
                let read = select.keys(table.keys(&keys))?.family(&r.family);
                query.on_keys(read, k)
            }
            Some(Source::Range(range)) => {
                query.on_scan(select.scan(&table, Some(range))?.family(&r.family), k)
            }
            None => query.on_scan(select.scan(&table, None)?.family(&r.family), k),
        };
        let page = search.run_within_async(page_bytes(r.page_bytes)).await?;
        let cut_by_budget = page.cut_by_budget();
        let found = page.into_neighbours();
        Ok(proto::NearestResponse {
            neighbours: found
                .into_iter()
                .map(|n| proto::Neighbour {
                    row: n.row.into(),
                    qualifier: n.qualifier.into(),
                    distance: n.distance,
                    timestamp: n.ts,
                })
                .collect(),
            cut_by_budget,
            missing: Vec::new(),
        })
    }

    async fn mutate(
        &self,
        r: proto::MutateRequest,
        budget: OwnedSemaphorePermit,
    ) -> Result<proto::MutateResponse, ApiError> {
        let rw = r
            .writes
            .ok_or_else(|| ApiError::invalid("Mutate needs writes"))?;
        let key = rw
            .row
            .ok_or_else(|| ApiError::invalid("Mutate's writes name a row"))?;
        let table = self.table(&key.table).await?;
        let durability = convert::durability(r.durability);
        let info = self
            .detached("Data.Mutate", budget, async move {
                let mut m = table.mutate(&key.row);
                if let Some(d) = durability {
                    m = m.durability(d);
                }
                Ok(convert::mutation(m, &rw.writes)?.commit_async().await?)
            })
            .await?;
        Ok(proto::MutateResponse {
            commit: Some(convert::commit_info(info)),
        })
    }

    async fn check_and_mutate(
        &self,
        r: proto::CheckAndMutateRequest,
        budget: OwnedSemaphorePermit,
    ) -> Result<proto::CheckAndMutateResponse, ApiError> {
        let rw = r
            .writes
            .ok_or_else(|| ApiError::invalid("CheckAndMutate needs writes"))?;
        let key = rw
            .row
            .ok_or_else(|| ApiError::invalid("CheckAndMutate's writes name a row"))?;
        let condition = convert::condition(r.condition.as_ref())?;
        let table = self.table(&key.table).await?;
        let durability = convert::durability(r.durability);
        let info = self
            .detached("Data.CheckAndMutate", budget, async move {
                let mut m = table.mutate(&key.row);
                if let Some(d) = durability {
                    m = m.durability(d);
                }
                Ok(convert::mutation(m, &rw.writes)?
                    .commit_if_async(&condition)
                    .await?)
            })
            .await?;
        Ok(proto::CheckAndMutateResponse {
            applied: info.is_some(),
            commit: info.map(convert::commit_info),
        })
    }

    async fn batch(
        &self,
        r: proto::BatchRequest,
        budget: OwnedSemaphorePermit,
    ) -> Result<proto::MutateResponse, ApiError> {
        let mut rows = Vec::with_capacity(r.rows.len());
        for rw in r.rows {
            let key = rw
                .row
                .ok_or_else(|| ApiError::invalid("each batch row names its table and row"))?;
            let table = self.table(&key.table).await?;
            rows.push((table, key.row, rw.writes));
        }
        let durability = convert::durability(r.durability);
        let db = self.part()?.db.clone();
        let info = self
            .detached("Data.Batch", budget, async move {
                let mut b = db.write_batch();
                for (table, row, writes) in &rows {
                    convert::batch(&mut b, table, row, writes)?;
                }
                Ok(match durability {
                    Some(d) => b.commit_with_async(d).await?,
                    None => b.commit_async().await?,
                })
            })
            .await?;
        Ok(proto::MutateResponse {
            commit: Some(convert::commit_info(info)),
        })
    }

    async fn mutate_returning(
        &self,
        r: proto::MutateRequest,
        budget: OwnedSemaphorePermit,
    ) -> Result<proto::MutateReturningResponse, ApiError> {
        let rw = r
            .writes
            .ok_or_else(|| ApiError::invalid("MutateReturning needs writes"))?;
        let key = rw
            .row
            .ok_or_else(|| ApiError::invalid("MutateReturning's writes name a row"))?;
        let table = self.table(&key.table).await?;
        let durability = convert::durability(r.durability);
        let returned = self
            .detached("Data.MutateReturning", budget, async move {
                let mut m = table.mutate(&key.row);
                if let Some(d) = durability {
                    m = m.durability(d);
                }
                Ok(convert::mutation(m, &rw.writes)?
                    .commit_returning_async()
                    .await?)
            })
            .await?;
        use proto::returned_cell::Value;
        let cells = returned
            .iter()
            .map(|(family, qualifier, ts, v)| proto::ReturnedCell {
                family: family.to_owned(),
                qualifier: qualifier.to_vec().into(),
                timestamp: ts,
                // The commit succeeded: a read error here is that cell's value, not the
                // call's error (D314).
                value: Some(match v {
                    Ok(Some(n)) => Value::I64(n),
                    Ok(None) => Value::Absent(proto::Empty {}),
                    Err(e) => Value::Error(ApiError::from_engine(&e).detail()),
                }),
            })
            .collect();
        Ok(proto::MutateReturningResponse {
            commit: Some(convert::commit_info(returned.info)),
            cells,
        })
    }
}
