//! `phdb info`: format version, size and free space, and per table its tablets, families
//! (with their stored options) and files ([`Pigeonhole::info`](pigeonhole::Pigeonhole::info)).

use std::path::Path;

use pigeonhole::{DatabaseInfo, FamilyInfo, FamilyStorage};
use serde_json::{Map, Value, json};

use crate::args::Cli;
use crate::exit::Failure;
use crate::open;
use crate::output::{Column, Report};

pub(crate) fn run(cli: &Cli, file: &Path) -> Result<Report, Failure> {
    let db = open::for_read(cli, file)?;
    let mode = db.mode();
    let info = db.info().map_err(|e| Failure::db("info", &e));
    db.close()?;
    Ok(report(file, mode, &info?))
}

pub(crate) fn report(file: &Path, mode: &'static str, info: &DatabaseInfo) -> Report {
    let db_id: String = info.db_id.iter().map(|b| format!("{b:02x}")).collect();
    // Shards are not stored in the file: as the writer, phdb opened with its own count, so
    // only a reader (the running writer's count) reports one.
    let shards = if info.reader {
        json!(info.shards)
    } else {
        Value::Null
    };
    let mut rows = Vec::new();
    let mut tables = Vec::new();
    for t in &info.tables {
        let mut families = Vec::new();
        for (f, s) in t.families.iter().zip(&t.storage) {
            let family = family_json(f, s);
            rows.push(row(&t.name, t.tablets, &family));
            families.push(Value::Object(family));
        }
        if families.is_empty() {
            rows.push(row(&t.name, t.tablets, &Map::new()));
        }
        tables.push(json!({"name": t.name, "tablets": t.tablets, "families": families}));
    }

    let features = json!({
        "compat": info.features.compat,
        "ro_compat": info.features.ro_compat,
        "incompat": info.features.incompat,
    });
    let mut document = Map::new();
    document.insert("path".into(), json!(file.display().to_string()));
    document.insert("mode".into(), json!(mode));
    document.insert("read_only".into(), json!(info.read_only));
    document.insert("format_version".into(), json!(info.format_version));
    document.insert("features".into(), features.clone());
    document.insert("db_id".into(), json!(db_id));
    document.insert("shards".into(), shards.clone());
    document.insert("file_bytes".into(), json!(info.file_bytes));
    document.insert("free_bytes".into(), json!(info.free_bytes));
    document.insert("tables".into(), Value::Array(tables));

    Report {
        command: "info",
        summary: vec![
            ("path", json!(file.display().to_string())),
            ("mode", json!(mode)),
            ("read_only", json!(info.read_only)),
            ("format_version", json!(info.format_version)),
            ("features", features),
            ("db_id", json!(db_id)),
            ("shards", shards),
            ("file_bytes", json!(info.file_bytes)),
            ("free_bytes", json!(info.free_bytes)),
            ("tables", json!(info.tables.len())),
        ],
        columns: COLUMNS
            .iter()
            .map(|&(name, brief)| Column { name, brief })
            .collect(),
        rows,
        document,
        status: crate::ExitStatus::SUCCESS,
    }
}

/// The per-family columns of `csv`, `jsonl` and `table` (brief) output.
const COLUMNS: &[(&str, bool)] = &[
    ("table", true),
    ("family", true),
    ("tablets", true),
    ("counter", false),
    ("ssts", true),
    ("sst_bytes", true),
    ("entries", true),
    ("blob_files", false),
    ("blob_bytes", true),
    ("blob_live_bytes", false),
    ("max_versions", true),
    ("ttl_micros", true),
    ("compression", true),
    ("zstd_level", false),
    ("bloom_bits", false),
    ("block_size", false),
    ("blob_threshold", false),
    ("merge_operator", false),
    ("cache_priority", false),
    ("compaction", false),
];

/// One output row: the table's fields, then the family's columns from `family` (missing
/// ones `null`, for a table without families).
fn row(table: &str, tablets: usize, family: &Map<String, Value>) -> Vec<Value> {
    COLUMNS
        .iter()
        .map(|&(name, _)| match name {
            "table" => json!(table),
            "tablets" => json!(tablets),
            "family" => family.get("name").cloned().unwrap_or(Value::Null),
            other => family.get(other).cloned().unwrap_or(Value::Null),
        })
        .collect()
}

fn family_json(f: &FamilyInfo, s: &FamilyStorage) -> Map<String, Value> {
    let mut m = super::schema::family_json(f);
    let levels: Vec<Value> = s
        .levels
        .iter()
        .map(|l| json!({"level": l.level, "ssts": l.ssts, "bytes": l.bytes, "entries": l.entries}))
        .collect();
    m.insert("ssts".into(), json!(s.ssts()));
    m.insert("sst_bytes".into(), json!(s.sst_bytes()));
    m.insert(
        "entries".into(),
        json!(s.levels.iter().map(|l| l.entries).sum::<u64>()),
    );
    m.insert("levels".into(), Value::Array(levels));
    m.insert("blob_files".into(), json!(s.blob_files));
    m.insert("blob_bytes".into(), json!(s.blob_bytes));
    m.insert("blob_live_bytes".into(), json!(s.blob_live_bytes));
    m
}
