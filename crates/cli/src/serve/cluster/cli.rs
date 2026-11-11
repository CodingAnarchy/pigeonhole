//! `phdb cluster` (D317, ICR 0117): the operator's commands for a partitioned cluster.
//! `init-partition` stamps a partition file offline; `set-map`, `show-map` and `status`
//! talk to a running node (`show-map --file` reads a down node's map file instead).

use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use super::identity::{self, Identity};
use super::map;
use crate::ExitStatus;
use crate::args::Cli;
use crate::exit::Failure;
use crate::output::{Column, Report};
use crate::serve::proto::{self, ClusterMap, GetMapRequest, SetMapRequest, StatusRequest};

/// How `phdb cluster` reaches a node.
#[derive(Debug, Clone, clap::Args)]
pub(crate) struct NodeArgs {
    /// A node's URL: `http://HOST:PORT` or `https://HOST:PORT`.
    #[arg(long, value_name = "URL")]
    pub node: String,
    /// The bearer token to present (admin scope for `set-map`, read for the others), alone
    /// in the file. Also from `PHDB_TOKEN`. Never a flag's value.
    #[arg(long, value_name = "FILE")]
    pub token_file: Option<PathBuf>,
    /// A PEM file of CA certificates to trust beside the system's roots.
    #[arg(long, value_name = "PEM")]
    pub ca: Option<PathBuf>,
}

/// `phdb cluster`'s subcommands.
#[derive(Debug, Clone, clap::Subcommand)]
pub(crate) enum ClusterCommand {
    /// Stamp a database file as a cluster's partition, offline: its identity row
    /// (`__phdb_cluster`) records the cluster, the partition and its epoch. The file is
    /// created if missing, and must be named `<partition>.phdb` in the node's cluster
    /// directory.
    InitPartition {
        /// The partition's file (`DIR/<partition>.phdb`).
        file: PathBuf,
        #[arg(long, value_name = "ID")]
        cluster_id: String,
        #[arg(long, value_name = "ID")]
        partition: String,
        #[arg(long, value_name = "N", default_value_t = 1)]
        epoch: u64,
    },
    /// Replace the cluster's map from a JSON file: the node validates, persists and pushes
    /// it to the others, and the answer says which took it.
    SetMap {
        /// The map, as JSON (`ClusterMap`, ICR 0117).
        file: PathBuf,
        #[command(flatten)]
        node: NodeArgs,
        /// Recovery when the map's writer (the node with the smallest id) is down: set the
        /// map at `--node` rather than the writer. Its version is put above every reachable
        /// node's, and it must name a reachable writer (remove the down one, or let another
        /// id be the smallest). Only for a writer that's down: a writer that's up and cut
        /// off can change the map meanwhile, and its change is lost (D317 amendment 1).
        #[arg(long)]
        force: bool,
    },
    /// Print a node's map, or a map file's (a down node's `DIR/cluster-map.json`).
    ShowMap {
        #[arg(long, value_name = "PATH", conflicts_with = "node")]
        file: Option<PathBuf>,
        #[arg(long, value_name = "URL", required_unless_present = "file")]
        node: Option<String>,
        #[arg(long, value_name = "FILE")]
        token_file: Option<PathBuf>,
        #[arg(long, value_name = "PEM")]
        ca: Option<PathBuf>,
    },
    /// Every node's status, from the map one node has: map versions side by side, so a
    /// push that hasn't reached a node shows.
    Status {
        #[command(flatten)]
        node: NodeArgs,
    },
    /// Back up every partition, each through the node the map gives it, plus the map, into
    /// DIR: `map.json`, `manifest.json` and `<partition>.phdb`. Each copy is its own
    /// consistent snapshot; the set is not one point in time across partitions. Fails if
    /// the map moves a partition while it runs.
    Backup {
        /// A new or empty directory.
        dir: PathBuf,
        #[command(flatten)]
        node: NodeArgs,
        /// Copy only what each writer holds durable at its level (`backup_durable`).
        #[arg(long)]
        durable: bool,
        /// Partitions backed up at once.
        #[arg(long, value_name = "N", default_value_t = 4)]
        parallel: usize,
    },
}

pub(crate) fn run(cli: &Cli, cmd: &ClusterCommand) -> Result<Report, Failure> {
    match cmd {
        ClusterCommand::InitPartition {
            file,
            cluster_id,
            partition,
            epoch,
        } => init_partition(cli, file, cluster_id, partition, *epoch),
        ClusterCommand::SetMap { file, node, force } => {
            let json = std::fs::read_to_string(file)
                .map_err(|e| Failure::io(&format!("read {}", file.display()), &e))?;
            let m = map::parse_json(&json).map_err(|e| Failure::usage(&e))?;
            map::validate(&m, false).map_err(|e| Failure::usage(&e))?;
            runtime()?.block_on(set_map(node, m, *force))
        }
        ClusterCommand::ShowMap {
            file,
            node,
            token_file,
            ca,
        } => {
            let m = match (file, node) {
                (Some(path), _) => {
                    let json = std::fs::read_to_string(path)
                        .map_err(|e| Failure::io(&format!("read {}", path.display()), &e))?;
                    map::parse_json(&json).map_err(|e| Failure::usage(&e))?
                }
                (None, Some(node)) => {
                    let args = NodeArgs {
                        node: node.clone(),
                        token_file: token_file.clone(),
                        ca: ca.clone(),
                    };
                    runtime()?.block_on(async {
                        let (mut c, token) = connect(&args, &args.node).await?;
                        c.get_map(request(GetMapRequest {}, token.as_deref())?)
                            .await
                            .map(tonic::Response::into_inner)
                            .map_err(|s| Failure::usage(&format!("{}: {}", args.node, s.message())))
                    })?
                }
                (None, None) => unreachable!("clap requires one"),
            };
            let doc: Value = serde_json::from_str(&map::canonical_json(&m)).expect("map JSON");
            let summary = vec![
                ("cluster_id", json!(m.cluster_id)),
                ("map_version", json!(m.map_version)),
                ("nodes", json!(m.nodes.len())),
                ("partitions", json!(m.partitions.len())),
                ("tables", json!(m.tables.len())),
            ];
            Ok(report("cluster show-map", summary, doc))
        }
        ClusterCommand::Status { node } => runtime()?.block_on(status(node)),
        ClusterCommand::Backup {
            dir,
            node,
            durable,
            parallel,
        } => runtime()?.block_on(super::backup::backup(node, dir, *durable, *parallel)),
    }
}

fn runtime() -> Result<tokio::runtime::Runtime, Failure> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| Failure::io("start the runtime", &e))
}

pub(super) fn report(
    command: &'static str,
    summary: Vec<(&'static str, Value)>,
    extra: Value,
) -> Report {
    let mut document: serde_json::Map<String, Value> = summary
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect();
    if let Value::Object(o) = extra {
        document.extend(o);
    }
    Report {
        command,
        summary,
        columns: Vec::<Column>::new(),
        rows: Vec::new(),
        document,
        status: ExitStatus::SUCCESS,
    }
}

fn init_partition(
    cli: &Cli,
    file: &Path,
    cluster_id: &str,
    partition: &str,
    epoch: u64,
) -> Result<Report, Failure> {
    if !map::id_ok(cluster_id) || !map::id_ok(partition) {
        return Err(Failure::usage(
            "--cluster-id and --partition are 1-64 of [a-z0-9_-], not starting with -",
        ));
    }
    if epoch == 0 {
        return Err(Failure::usage("--epoch is at least 1"));
    }
    if file.file_stem().and_then(|s| s.to_str()) != Some(partition)
        || file.extension().and_then(|s| s.to_str()) != Some("phdb")
    {
        return Err(Failure::usage(&format!(
            "partition {partition}'s file is named {partition}.phdb in the node's cluster directory"
        )));
    }
    let dir = match file.parent() {
        Some(d) if !d.as_os_str().is_empty() => d,
        _ => Path::new("."),
    };
    let _lock = super::lock_dir(dir)?;
    let db = crate::open::for_write(cli, file, true)?;
    let written = identity::write(
        &db,
        &Identity {
            cluster_id: cluster_id.to_owned(),
            partition_id: partition.to_owned(),
            epoch,
        },
    )
    .map_err(|e| Failure::usage(&format!("{}: {e}", file.display())));
    let closed = db.close().map_err(|e| Failure::db("close", &e));
    written?;
    closed?;
    let summary = vec![
        ("path", json!(file.display().to_string())),
        ("cluster_id", json!(cluster_id)),
        ("partition", json!(partition)),
        ("epoch", json!(epoch)),
    ];
    Ok(report("cluster init-partition", summary, Value::Null))
}

pub(super) fn column(name: &'static str) -> Column {
    Column { name, brief: true }
}

type Client = proto::cluster_client::ClusterClient<tonic::transport::Channel>;

/// A client for the node at `url`, and the token to present.
async fn connect(args: &NodeArgs, url: &str) -> Result<(Client, Option<String>), Failure> {
    let (channel, token) = channel(args, url).await?;
    Ok((proto::cluster_client::ClusterClient::new(channel), token))
}

/// A channel to the node at `url`, and the token to present.
pub(super) async fn channel(
    args: &NodeArgs,
    url: &str,
) -> Result<(tonic::transport::Channel, Option<String>), Failure> {
    let token = match &args.token_file {
        Some(path) => Some(
            std::fs::read_to_string(path)
                .map_err(|e| Failure::io(&format!("read {}", path.display()), &e))?
                .trim()
                .to_owned(),
        ),
        None => std::env::var("PHDB_TOKEN").ok(),
    }
    .filter(|t| !t.is_empty());
    let mut endpoint = tonic::transport::Endpoint::from_shared(url.to_owned())
        .map_err(|e| Failure::usage(&format!("{url}: {e}")))?
        .connect_timeout(std::time::Duration::from_secs(10));
    if url.starts_with("https://") {
        let mut tls = tonic::transport::ClientTlsConfig::new().with_native_roots();
        if let Some(path) = &args.ca {
            let pem = std::fs::read(path)
                .map_err(|e| Failure::io(&format!("read {}", path.display()), &e))?;
            tls = tls.ca_certificate(tonic::transport::Certificate::from_pem(pem));
        }
        endpoint = endpoint
            .tls_config(tls)
            .map_err(|e| Failure::usage(&format!("TLS to {url}: {e}")))?;
    } else if !url.starts_with("http://") {
        return Err(Failure::usage(&format!(
            "{url}: a node's URL is http://HOST:PORT or https://HOST:PORT"
        )));
    }
    let channel = endpoint
        .connect()
        .await
        .map_err(|e| Failure::usage(&format!("connecting to {url}: {e}")))?;
    Ok((channel, token))
}

pub(super) fn request<T>(message: T, token: Option<&str>) -> Result<tonic::Request<T>, Failure> {
    let mut req = tonic::Request::new(message);
    if let Some(t) = token {
        req.metadata_mut().insert(
            "authorization",
            format!("Bearer {t}")
                .parse()
                .map_err(|_| Failure::usage("the token isn't a valid header value"))?,
        );
    }
    Ok(req)
}

/// A node's URL from its map address, in the scheme `--node` uses.
fn url_of(args: &NodeArgs, address: &str) -> String {
    let scheme = if args.node.starts_with("https://") {
        "https"
    } else {
        "http"
    };
    format!("{scheme}://{address}")
}

pub(crate) async fn set_map(
    args: &NodeArgs,
    mut m: ClusterMap,
    force: bool,
) -> Result<Report, Failure> {
    let (mut c, token) = connect(args, &args.node).await?;
    let current = match c
        .get_map(request(GetMapRequest {}, token.as_deref())?)
        .await
    {
        Ok(r) => Some(r.into_inner()),
        Err(s) if s.code() == tonic::Code::NotFound => None,
        Err(s) => return Err(Failure::usage(&format!("{}: {}", args.node, s.message()))),
    };
    let mut at = args.node.clone();
    let reply = if force {
        // Above every reachable node's version, and naming a writer that's reachable.
        let mut ids: Vec<(String, String)> = Vec::new();
        for n in current.iter().flat_map(|c| &c.nodes).chain(&m.nodes) {
            if !ids.iter().any(|(id, _)| *id == n.id) {
                ids.push((n.id.clone(), n.address.clone()));
            }
        }
        let mut top = current.as_ref().map_or(0, |c| c.map_version);
        let mut live = Vec::new();
        for (id, address) in &ids {
            let got = async {
                let (mut c, token) = connect(args, &url_of(args, address)).await?;
                c.status(request(StatusRequest {}, token.as_deref())?)
                    .await
                    .map(tonic::Response::into_inner)
                    .map_err(|s| Failure::usage(s.message()))
            }
            .await;
            if let Ok(s) = got {
                top = top.max(s.map_version);
                live.push(id.clone());
            }
        }
        let w = super::schema::writer(&m).to_owned();
        if !live.contains(&w) {
            return Err(Failure::usage(&format!(
                "the map's writer would be node {w:?} (the smallest id), which can't be \
                 reached: a forced map must name a writer that's up (remove the down node, \
                 or give the writer's role to another id)"
            )));
        }
        m.map_version = m.map_version.max(top + 1);
        c.set_map(request(
            SetMapRequest {
                expected_map_version: 0,
                map: Some(m),
                force: true,
            },
            token.as_deref(),
        )?)
        .await
        .map_err(|s| Failure::usage(&format!("{}: {}", args.node, s.message())))?
        .into_inner()
    } else {
        // Compare-and-set against the current version (0 before a first map), at the map's
        // writer (D317 amendment 1).
        let expected = current.as_ref().map_or(0, |c| c.map_version);
        if let Some(cur) = &current {
            let w = super::schema::writer(cur);
            let address = cur
                .nodes
                .iter()
                .find(|n| n.id == w)
                .map(|n| n.address.clone())
                .unwrap_or_default();
            at = url_of(args, &address);
            if at != args.node {
                c = connect(args, &at).await?.0;
            }
        }
        c.set_map(request(
            SetMapRequest {
                expected_map_version: expected,
                map: Some(m),
                force: false,
            },
            token.as_deref(),
        )?)
        .await
        .map_err(|s| Failure::usage(&format!("{at}: {}", s.message())))?
        .into_inner()
    };
    let summary = vec![
        ("node", json!(at)),
        ("map_version", json!(reply.map_version)),
        (
            "pushed",
            json!(reply.pushed.iter().filter(|p| p.ok).count()),
        ),
        (
            "missed",
            json!(reply.pushed.iter().filter(|p| !p.ok).count()),
        ),
    ];
    let mut r = report("cluster set-map", summary, Value::Null);
    r.columns = ["node", "ok", "error"].into_iter().map(column).collect();
    r.rows = reply
        .pushed
        .iter()
        .map(|p| vec![json!(p.node), json!(p.ok), json!(p.error)])
        .collect();
    r.document.insert(
        "pushed".into(),
        serde_json::to_value(&reply.pushed).expect("NodePush serializes"),
    );
    Ok(r)
}

async fn status(args: &NodeArgs) -> Result<Report, Failure> {
    let (mut c, token) = connect(args, &args.node).await?;
    let m = c
        .get_map(request(GetMapRequest {}, token.as_deref())?)
        .await
        .map_err(|s| Failure::usage(&format!("{}: {}", args.node, s.message())))?
        .into_inner();
    let scheme = if args.node.starts_with("https://") {
        "https"
    } else {
        "http"
    };
    let mut rows = Vec::new();
    let mut nodes = Vec::new();
    let mut behind = 0usize;
    for n in &m.nodes {
        let url = format!("{scheme}://{}", n.address);
        let got = async {
            let (mut c, token) = connect(args, &url).await?;
            c.status(request(StatusRequest {}, token.as_deref())?)
                .await
                .map(tonic::Response::into_inner)
                .map_err(|s| Failure::usage(s.message()))
        }
        .await;
        match got {
            Ok(s) => {
                if s.map_version != m.map_version {
                    behind += 1;
                }
                let ok = s.partitions.iter().filter(|p| p.ok).count();
                rows.push(vec![
                    json!(n.id),
                    json!(n.address),
                    json!(s.map_version),
                    json!(format!("{ok}/{}", s.partitions.len())),
                    json!(s.divergent.join(",")),
                    json!(""),
                ]);
                nodes.push(serde_json::to_value(&s).expect("NodeStatus serializes"));
            }
            Err(e) => {
                behind += 1;
                rows.push(vec![
                    json!(n.id),
                    json!(n.address),
                    Value::Null,
                    Value::Null,
                    json!(""),
                    json!(e.message()),
                ]);
                nodes.push(json!({ "node_id": n.id, "error": e.message() }));
            }
        }
    }
    let summary = vec![
        ("cluster_id", json!(m.cluster_id)),
        ("map_version", json!(m.map_version)),
        ("nodes", json!(m.nodes.len())),
        ("not_current", json!(behind)),
    ];
    let mut r = report("cluster status", summary, json!({ "nodes": nodes }));
    r.columns = [
        "node",
        "address",
        "map_version",
        "partitions_ok",
        "divergent",
        "error",
    ]
    .into_iter()
    .map(column)
    .collect();
    r.rows = rows;
    Ok(r)
}
