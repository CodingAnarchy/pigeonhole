//! `phdb check`: the offline integrity check ([`pigeonhole::check`], ICR 0048), and with
//! `--online` a read-through scan when a writer process has the database open.

use std::path::Path;

use pigeonhole::{
    CheckOptions, CheckReport, ErrorCode, Finding, Pigeonhole, ReaderOptions, Region, RegionKind,
    Severity, Structure,
};
use serde_json::{Map, Value, json};

use crate::ExitStatus;
use crate::args::Cli;
use crate::exit::Failure;
use crate::output::{Column, Report};

pub(crate) fn run(
    cli: &Cli,
    file: &Path,
    deep: bool,
    map: bool,
    online: bool,
) -> Result<Report, Failure> {
    if !file.exists() {
        return Err(Failure::file_not_found(file));
    }
    match pigeonhole::check(file, CheckOptions::default().deep(deep)) {
        Ok(r) => Ok(offline(file, &r, deep, map)),
        Err(e) if e.code() == ErrorCode::WriterLocked && online => online_scan(cli, file),
        Err(e) if e.code() == ErrorCode::WriterLocked => Err(Failure::db_with(
            "check: a writer process has the database open, and the check runs offline; stop \
             it, or pass --online for a read-through check of what a scan reaches"
                .into(),
            &e,
        )),
        Err(e) => Err(Failure::db("check", &e)),
    }
}

fn structure(s: Structure) -> &'static str {
    match s {
        Structure::Superblock => "superblock",
        Structure::Manifest => "manifest",
        Structure::Allocation => "allocation",
        Structure::Sst => "sst",
        Structure::Blob => "blob",
        Structure::Wal => "wal",
        _ => "unknown",
    }
}

fn severity(s: Severity) -> &'static str {
    match s {
        Severity::Corruption => "corruption",
        Severity::Warning => "warning",
        Severity::Info => "info",
        _ => "unknown",
    }
}

const FINDING_COLUMNS: &[&str] = &["structure", "severity", "object", "offset", "message"];
const REGION_COLUMNS: &[&str] = &[
    "kind",
    "offset",
    "len",
    "sst",
    "table",
    "family",
    "level",
    "used",
    "blob_file",
    "index",
];

fn finding_json(f: &Finding) -> Map<String, Value> {
    let mut m = Map::new();
    m.insert("structure".into(), json!(structure(f.structure)));
    m.insert("severity".into(), json!(severity(f.severity)));
    m.insert("object".into(), json!(f.object));
    m.insert("offset".into(), json!(f.offset));
    m.insert("message".into(), json!(f.message));
    m
}

fn region_json(g: &Region) -> Map<String, Value> {
    let mut m = Map::new();
    let kind = match &g.kind {
        RegionKind::Superblock { .. } => "superblock",
        RegionKind::ManifestSnapshot => "manifest-snapshot",
        RegionKind::ManifestLog => "manifest-log",
        RegionKind::Sst { .. } => "sst",
        RegionKind::Blob { .. } => "blob",
        _ => "unknown",
    };
    m.insert("kind".into(), json!(kind));
    m.insert("offset".into(), json!(g.offset));
    m.insert("len".into(), json!(g.len));
    match &g.kind {
        RegionKind::Superblock { slot } => {
            m.insert("slot".into(), json!(slot));
        }
        RegionKind::Sst {
            sst,
            table,
            family,
            level,
            used,
        } => {
            m.insert("sst".into(), json!(sst));
            m.insert("table".into(), json!(table));
            m.insert("family".into(), json!(family));
            m.insert("level".into(), json!(level));
            m.insert("used".into(), json!(used));
        }
        RegionKind::Blob {
            blob_file,
            index,
            family,
        } => {
            m.insert("blob_file".into(), json!(blob_file));
            m.insert("index".into(), json!(index));
            m.insert("family".into(), json!(family));
        }
        _ => {}
    }
    m
}

fn rows(columns: &[&str], objects: &[Map<String, Value>]) -> Vec<Vec<Value>> {
    objects
        .iter()
        .map(|o| {
            columns
                .iter()
                .map(|c| o.get(*c).cloned().unwrap_or(Value::Null))
                .collect()
        })
        .collect()
}

fn offline(file: &Path, r: &CheckReport, deep: bool, map: bool) -> Report {
    let count = |s: Severity| r.findings.iter().filter(|f| f.severity == s).count();
    let c = &r.counts;
    let result = if r.is_ok() { "ok" } else { "corrupt" };
    let summary: Vec<(&'static str, Value)> = vec![
        ("path", json!(file.display().to_string())),
        ("mode", json!("offline")),
        ("result", json!(result)),
        ("corruption", json!(count(Severity::Corruption))),
        ("warnings", json!(count(Severity::Warning))),
        ("info", json!(count(Severity::Info))),
        ("deep", json!(deep)),
        ("clean_shutdown", json!(r.clean_shutdown)),
        ("file_bytes", json!(r.file_bytes)),
        ("superblocks", json!(c.superblocks)),
        ("tables", json!(c.tables)),
        ("ssts", json!(c.ssts)),
        ("sst_entries", json!(c.sst_entries)),
        ("blob_files", json!(c.blob_files)),
        ("blob_records", json!(c.blob_records)),
        ("blob_pointers", json!(c.blob_pointers)),
        ("wal_files", json!(c.wal_files)),
        ("wal_records", json!(c.wal_records)),
    ];
    let findings: Vec<_> = r.findings.iter().map(finding_json).collect();
    let regions: Vec<_> = r.regions.iter().map(region_json).collect();
    let wal: Vec<Value> = r
        .wal
        .iter()
        .map(|w| {
            json!({
                "stream": w.stream,
                "path": w.path.display().to_string(),
                "len": w.len,
                "segment_size": w.segment_size,
                "segments": w.segments,
                "records": w.records,
                "checkpoint": {"epoch": w.checkpoint.0, "offset": w.checkpoint.1},
                "end": {"epoch": w.end.0, "offset": w.end.1},
                "end_slot": w.end_slot,
            })
        })
        .collect();

    let mut document: Map<String, Value> = summary
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect();
    document.insert(
        "findings".into(),
        Value::Array(findings.iter().cloned().map(Value::Object).collect()),
    );
    document.insert("wal".into(), Value::Array(wal));
    if map {
        document.insert(
            "regions".into(),
            Value::Array(regions.iter().cloned().map(Value::Object).collect()),
        );
    }
    let (columns, rows) = if map {
        (REGION_COLUMNS, rows(REGION_COLUMNS, &regions))
    } else {
        (FINDING_COLUMNS, rows(FINDING_COLUMNS, &findings))
    };
    Report {
        command: "check",
        summary,
        columns: columns
            .iter()
            .map(|&name| Column { name, brief: true })
            .collect(),
        rows,
        document,
        status: if r.is_ok() {
            ExitStatus::SUCCESS
        } else {
            ExitStatus::CORRUPTION
        },
    }
}

/// `--online` with a writer running: every table read through a reader process, all
/// versions, at one snapshot. A failed read is a finding (`structure` is `data`): this sees
/// only what a scan reaches.
fn online_scan(cli: &Cli, file: &Path) -> Result<Report, Failure> {
    let mut options = ReaderOptions::default().block_cache(16 << 20);
    if let Some(dir) = &cli.shm_dir {
        options = options.shm_dir(dir);
    }
    let db = Pigeonhole::open_reader(file, options)
        .map_err(|e| Failure::db("check --online: open as a reader", &e))?;
    let snapshot = db
        .snapshot()
        .map_err(|e| Failure::db("check --online: snapshot", &e))?;
    let mut findings = Vec::new();
    let (mut tables, mut rows_read, mut cells) = (0u64, 0u64, 0u64);
    for name in db.tables() {
        tables += 1;
        let table = match db.table(&name) {
            Ok(t) => t,
            Err(e) => {
                findings.push((name.clone(), e));
                continue;
            }
        };
        let iter = table
            .scan_prefix(b"")
            .versions(0)
            .snapshot(&snapshot)
            .iter();
        let mut iter = match iter {
            Ok(i) => i,
            Err(e) => {
                findings.push((name.clone(), e));
                continue;
            }
        };
        loop {
            match iter.next_ref() {
                Ok(Some(row)) => {
                    rows_read += 1;
                    cells += row.len() as u64;
                }
                Ok(None) => break,
                Err(e) => {
                    findings.push((name.clone(), e));
                    break;
                }
            }
        }
    }
    drop(snapshot);
    drop(db);
    let corrupt = findings
        .iter()
        .any(|(_, e)| e.code() == ErrorCode::Corruption);
    let objects: Vec<Map<String, Value>> = findings
        .iter()
        .map(|(table, e)| {
            let mut m = Map::new();
            m.insert("structure".into(), json!("data"));
            let sev = if e.code() == ErrorCode::Corruption {
                "corruption"
            } else {
                "warning"
            };
            m.insert("severity".into(), json!(sev));
            m.insert("table".into(), json!(table));
            m.insert("code".into(), json!(format!("{:?}", e.code())));
            m.insert("message".into(), json!(e.message()));
            m
        })
        .collect();
    let summary: Vec<(&'static str, Value)> = vec![
        ("path", json!(file.display().to_string())),
        ("mode", json!("online")),
        ("result", json!(if corrupt { "corrupt" } else { "ok" })),
        ("tables", json!(tables)),
        ("rows", json!(rows_read)),
        ("cells", json!(cells)),
    ];
    let mut document: Map<String, Value> = summary
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect();
    document.insert(
        "findings".into(),
        Value::Array(objects.iter().cloned().map(Value::Object).collect()),
    );
    const COLUMNS: &[&str] = &["structure", "severity", "table", "code", "message"];
    Ok(Report {
        command: "check",
        summary,
        columns: COLUMNS
            .iter()
            .map(|&name| Column { name, brief: true })
            .collect(),
        rows: rows(COLUMNS, &objects),
        document,
        status: if corrupt {
            ExitStatus::CORRUPTION
        } else if objects.is_empty() {
            ExitStatus::SUCCESS
        } else {
            ExitStatus::ERROR
        },
    })
}
