use super::*;

/// A table as `Schema` describes it: its families with every option as stored.
pub(super) fn describe(t: &Table) -> proto::TableResponse {
    proto::TableResponse {
        name: t.name().to_owned(),
        families: t
            .families()
            .into_iter()
            .filter_map(|f| t.family_info(f))
            .map(|i| convert::family_out(&i))
            .collect(),
        unreconciled: Vec::new(),
    }
}

/// Creates table `name` with `families` (with `if_missing`, opens it with the families it
/// lacks added): a sync call, on a blocking thread. An empty name is refused here for every
/// caller (`Schema.CreateTable` refuses it earlier, before its families are read).
fn create_in(
    db: &Pigeonhole,
    name: &str,
    families: Vec<(String, pigeonhole::Family)>,
    if_missing: bool,
) -> Result<Table, ApiError> {
    if name.is_empty() {
        return Err(ApiError::invalid("empty table name"));
    }
    let mut b = db.table(name)?;
    for (n, f) in families {
        b = b.family(&n, f);
    }
    Ok(if if_missing {
        b.create_if_missing()?
    } else {
        b.create()?
    })
}

/// Refuses a name with the cluster's reserved prefix (`__phdb_`, ICR 0117): its tables
/// (`__phdb_cluster`, a partition's identity) are the server's own, never a user call's.
fn reserved(name: &str) -> Result<(), ApiError> {
    if name.starts_with(super::super::cluster::map::RESERVED_PREFIX) {
        return Err(ApiError::invalid(format!(
            "table {name:?}: the {} prefix is reserved",
            super::super::cluster::map::RESERVED_PREFIX
        )));
    }
    Ok(())
}

impl Service {
    /// The table `name`, opened on first use. A miss opens it on the blocking pool (an open
    /// is a sync call, D196) and swaps in a new map.
    ///
    /// Its `or_insert` never displaces a handle a create put in. Creates and drops are
    /// serialized with their cache updates (`schema`), so the only stale entry it can leave
    /// is a handle resolved just before a drop, for a name with no table: its errors match
    /// the catalog, and the next create replaces it.
    ///
    /// A replica's applied catalog changes don't take `schema`: there, an open that
    /// [`tables_changed`](Self::tables_changed) overtook keeps nothing and opens again,
    /// since it may have resolved the name before a drop and create (#1632).
    pub(crate) async fn table(&self, name: &str) -> Result<Table, ApiError> {
        loop {
            let generation = match self.cached(name) {
                Ok(t) => return Ok(t),
                Err(generation) => generation,
            };
            reserved(name)?;
            let db = self.part()?.db.clone();
            let owned = name.to_owned();
            let table = self
                .blocking(move || db.table(&owned)?.open().map_err(ApiError::from))
                .await?;
            if let Some(t) = self.keep_table(name, &table, generation) {
                return Ok(t);
            }
        }
    }

    /// The cached handle for `name`, or the cache's generation to open it at.
    fn cached(&self, name: &str) -> Result<Table, u64> {
        // No partition, no cache: the open after it answers that.
        let Some(part) = &self.part else {
            return Err(0);
        };
        let cache = part.tables.load();
        cache.map.get(name).cloned().ok_or(cache.generation)
    }

    /// Keeps `table` under `name` unless a handle is there already (then that one is
    /// returned). `None`, keeping nothing, if [`tables_changed`](Self::tables_changed) came
    /// after `generation`, the one the open started at (#1632).
    pub(crate) fn keep_table(&self, name: &str, table: &Table, generation: u64) -> Option<Table> {
        let mut kept = None;
        self.part.as_ref()?.tables.rcu(|c| {
            if c.generation != generation {
                kept = None;
                return Arc::clone(c);
            }
            let mut c = TableCache::clone(c);
            kept = Some(
                c.map
                    .entry(name.to_owned())
                    .or_insert_with(|| table.clone())
                    .clone(),
            );
            Arc::new(c)
        });
        kept
    }

    /// `Schema.CreateTable`: the table with its families, created, or with `if_missing`
    /// opened with the families it lacks added. The service's handle for the name is
    /// replaced, so later calls (and feed hubs, keyed by its id) use this table.
    pub(super) async fn create_table(
        &self,
        r: proto::CreateTableRequest,
    ) -> Result<proto::TableResponse, ApiError> {
        reserved(&r.name)?;
        if r.name.is_empty() {
            return Err(ApiError::invalid("a table needs a name"));
        }
        if r.placement.is_some() {
            return Err(ApiError::invalid(
                "a placement is for a cluster node's map (D317): a single node holds every row",
            ));
        }
        let families = r
            .families
            .iter()
            .map(|f| Ok((f.name.clone(), convert::family_in(f)?)))
            .collect::<Result<Vec<_>, ApiError>>()?;
        let table = self.create_with(r.name, families, r.if_missing).await?;
        Ok(describe(&table))
    }

    /// Creates table `name` with `families`, or with `if_missing` opens it with the families
    /// it lacks added: `Schema.CreateTable`'s work, and `/mcp`'s `create_table` tool's. The
    /// service's handle for the name is replaced, so later calls (and feed hubs, keyed by
    /// its id) use this table.
    pub(crate) async fn create_with(
        &self,
        name: String,
        families: Vec<(String, pigeonhole::Family)>,
        if_missing: bool,
    ) -> Result<Table, ApiError> {
        reserved(&name)?;
        // The pool place before `schema` (its lock order).
        let place = pool_place(&self.blocking).await?;
        let _schema = self.part()?.schema.lock().await;
        let db = self.part()?.db.clone();
        let owned = name.clone();
        let table = held(place, move || create_in(&db, &owned, families, if_missing)).await?;
        self.created(name, &table);
        Ok(table)
    }

    /// A created table's handle replaces the name's, and the catalog moves.
    fn created(&self, name: String, table: &Table) {
        let Some(part) = &self.part else { return };
        part.tables.rcu(|c| {
            let mut c = TableCache::clone(c);
            c.map.insert(name.clone(), table.clone());
            c
        });
        part.catalog.fetch_add(1, Ordering::Release);
    }

    /// The user's tables, sorted: the cluster's own (`__phdb_cluster`, ICR 0117) aren't.
    pub(crate) fn user_tables(&self) -> Vec<String> {
        let Some(part) = &self.part else {
            return Vec::new();
        };
        let mut tables = part.db.tables();
        tables.retain(|t| !t.starts_with(super::super::cluster::map::RESERVED_PREFIX));
        tables.sort();
        tables
    }

    /// The catalog's generation: it moves with every create and drop.
    pub(crate) fn catalog(&self) -> u64 {
        self.part
            .as_ref()
            .map_or(0, |p| p.catalog.load(Ordering::Acquire))
    }

    /// `Schema.DropTable`: the table and its data go, and so does the service's handle for
    /// the name. A table created again under it is a new table, with a new id: its feed hubs
    /// are new too, and those on the old one end with their engine feeds.
    pub(super) async fn drop_table(&self, name: String) -> Result<(), ApiError> {
        reserved(&name)?;
        // The memory store this server serves keeps its data in its own tables: dropping one
        // would leave the slot over a dropped table, and a restart would re-create the store
        // empty (cli692's N1 on #1650, #1651). The crate names them.
        if let Some(memory) = self.part()?.memory.get() {
            let store = memory.name();
            if pigeonhole_memory::Store::table_names(store).contains(&name) {
                return Err(ApiError::failed_precondition(format!(
                    "table {name:?} belongs to the memory store {store} this server serves \
                     (--memory {store}): stop the server to drop it"
                )));
            }
        }
        // The pool place before `schema` (its lock order).
        let place = pool_place(&self.blocking).await?;
        let _schema = self.part()?.schema.lock().await;
        let db = self.part()?.db.clone();
        let texts = Arc::clone(&self.part()?.texts);
        let dropped = held(place, move || {
            // A text index reading the table would be left behind, unable to write, rebuild
            // or find its documents, and a new table of the name would become its source
            // (#1916, the coordinator's option (a)). Checked under `schema`, which a text
            // index's create takes too, so none appears between the check and the drop.
            if texts.creating_over(&name) {
                return Err(ApiError::failed_precondition(format!(
                    "a text index over table {name:?} is being created: once it's built, drop \
                     the index before the table"
                )));
            }
            let over = pigeonhole_text::Index::over_table(&db, &name)?;
            if !over.is_empty() {
                return Err(ApiError::failed_precondition(format!(
                    "table {name:?} is the source of the text index{} {}: drop {} first",
                    if over.len() == 1 { "" } else { "es" },
                    over.iter()
                        .map(|i| format!("{i:?}"))
                        .collect::<Vec<_>>()
                        .join(", "),
                    if over.len() == 1 { "it" } else { "them" },
                )));
            }
            Ok(db.drop_table(&name)?)
        })
        .await;
        // Whatever the outcome, the next use resolves every name again: the table's handle,
        // and a vector or text index's if it was one, with its rebalancer (#1588). Every
        // cached handle goes, not only the name's: drops are rare, and an open of the name
        // already running keeps nothing (#1632).
        self.handles_changed();
        dropped
    }
}
