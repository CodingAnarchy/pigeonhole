//! D311 T5 (#1339): `pigeonhole-memory`'s recall on LongMemEval and LoCoMo, per signal and
//! fused, with its latency, file size per 100k memories, and whether fuzzy keyword terms help.
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example memrecall -- FIXTURE_DIR DB_DIR recall DATASET [ELEMENT]
//! cargo run --release -p pigeonhole-bench --example memrecall -- FIXTURE_DIR DB_DIR size [N]
//! ```
//!
//! `FIXTURE_DIR` is what `baselines/memory/fixture.py` wrote. `DATASET` is `lme_s`, `lme_m` or
//! `locomo`. `ELEMENT` (`f32`, the default, `f16` or `bf16`) is the store's.
//!
//! - **`recall`** builds one store of the dataset (one namespace per LongMemEval question or
//!   LoCoMo conversation). Memories are written in date order, so recency follows the
//!   dataset's dates, and each one links to the previous memory of its session (`follows`,
//!   weight 1), which the graph expansion walks. Then every question runs under each
//!   configuration:
//!   - `vector`, `keyword` and `recency`: that signal alone (its weight 1, the others' 0);
//!   - `fused`: the default weights, and `fused-no-recency`: those but recency's (and use's);
//!   - `fused+graph`: the default weights with `expand(Expand::hops(1))`;
//!   - `keyword-fuzzy`: the text index directly, terms of 5 or more characters as
//!     `Fuzziness::Auto` fuzzy terms, against `keyword-index`, the same search without them.
//!
//!   A question counts at `k` when one of the top `k` hits comes from its evidence:
//!   LongMemEval's answer sessions, or LoCoMo's evidence turns. Prints one JSON line per
//!   configuration (recall@5, recall@10, NDCG@10 over evidence sessions or turns, and the
//!   recall latency's p50/p99 in µs, single-threaded and warm), then the latency by namespace
//!   size for `fused` and `fused+graph`.
//! - **`size`** writes the first `N` (100,000 by default, or as many unique ones as there
//!   are) of `lme_m`'s memories into one namespace for each element type, compacts, and
//!   prints the file size in bytes. With `f32`, it then times `lme_m`'s questions as fused
//!   recalls over that one large namespace: the latency end the IVF question (T8) turns on.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::Instant;

use pigeonhole::{Element, Options, Pigeonhole};
use pigeonhole_memory::{
    EmbeddingModel, Expand, LinkType, Memory, MemoryId, Store, StoreOptions, Weights,
};
use pigeonhole_text::{Fuzziness, Index, Query};
use serde_json::{Value, json};

const STORE: &str = "bench";

struct Mem {
    ns: String,
    session: String,
    unit: String,
    text: String,
}

struct Question {
    ns: String,
    text: String,
    evidence: BTreeSet<String>,
}

fn lines(path: &Path) -> Vec<Value> {
    BufReader::new(File::open(path).unwrap_or_else(|e| panic!("{}: {e}", path.display())))
        .lines()
        .map(|l| serde_json::from_str(&l.unwrap()).unwrap())
        .collect()
}

fn vectors(path: &Path, dim: usize) -> Vec<Vec<f32>> {
    let mut bytes = Vec::new();
    File::open(path).unwrap().read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes.len() % (4 * dim), 0, "{}", path.display());
    bytes
        .chunks_exact(4 * dim)
        .map(|v| {
            v.as_chunks::<4>()
                .0
                .iter()
                .map(|b| f32::from_le_bytes(*b))
                .collect()
        })
        .collect()
}

fn s(v: &Value, k: &str) -> String {
    v[k].as_str().unwrap_or_else(|| panic!("{k}")).to_owned()
}

struct Fixture {
    model: String,
    dim: u32,
    mems: Vec<Mem>,
    mem_vecs: Vec<Vec<f32>>,
    qs: Vec<Question>,
    q_vecs: Vec<Vec<f32>>,
    /// Evidence by session (LongMemEval) or by turn (LoCoMo).
    by_session: bool,
}

fn fixture(dir: &Path, name: &str) -> Fixture {
    let manifest: Value =
        serde_json::from_reader(File::open(dir.join("manifest.json")).unwrap()).unwrap();
    let dim = manifest["dim"].as_u64().unwrap() as u32;
    let mems: Vec<Mem> = lines(&dir.join(format!("{name}.mem.jsonl")))
        .iter()
        .map(|m| Mem {
            ns: s(m, "ns"),
            session: s(m, "session"),
            unit: s(m, "unit"),
            text: s(m, "text"),
        })
        .collect();
    let qs: Vec<Question> = lines(&dir.join(format!("{name}.q.jsonl")))
        .iter()
        .map(|q| Question {
            ns: s(q, "ns"),
            text: s(q, "question"),
            evidence: q["evidence"]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| e.as_str().unwrap().to_owned())
                .collect(),
        })
        .collect();
    let mem_vecs = vectors(&dir.join(format!("{name}.mem.f32")), dim as usize);
    let q_vecs = vectors(&dir.join(format!("{name}.q.f32")), dim as usize);
    assert_eq!(mem_vecs.len(), mems.len());
    assert_eq!(q_vecs.len(), qs.len());
    Fixture {
        model: s(&manifest, "model"),
        dim,
        mems,
        mem_vecs,
        qs,
        q_vecs,
        by_session: name != "locomo",
    }
}

/// `remember`, retried on `Busy` (a write stall past its timeout: the writes outran
/// flushes and compactions), as the error says to; the retries are counted in `busy`.
fn remember(store: &Store, ns: &str, m: Memory, busy: &mut u64) -> MemoryId {
    loop {
        match store.remember(ns, m.clone()) {
            Ok(r) => return r.id().clone(),
            Err(pigeonhole_memory::Error::Db(e)) if e.code() == pigeonhole::ErrorCode::Busy => {
                *busy += 1;
                assert!(*busy < 1_000, "Busy 1,000 times: {e}");
                std::thread::sleep(std::time::Duration::from_millis(500));
            }
            Err(e) => panic!("remember: {e}"),
        }
    }
}

fn element(s: &str) -> Element {
    match s {
        "f16" => Element::F16,
        "bf16" => Element::BF16,
        _ => Element::F32,
    }
}

fn open(dir: &Path) -> Pigeonhole {
    Pigeonhole::open(dir.join("mem.phdb"), Options::default()).unwrap()
}

/// The row key a memory id names (ICR 0107's frozen layout: `{ns}\0m{rid}`).
fn row_key(id: &MemoryId) -> Vec<u8> {
    let b = id.to_bytes();
    let ns_len = u16::from_le_bytes([b[0], b[1]]) as usize;
    [&b[2..2 + ns_len], b"\0m", &b[2 + ns_len..]].concat()
}

/// Writes the fixture's memories: each id's evidence keys (its session or turn; several
/// when exact dedup merged a repeat), by id and by row key.
fn build(f: &Fixture, db: &Pigeonhole, el: Element) -> BTreeMap<Vec<u8>, Vec<String>> {
    let model = EmbeddingModel::new(f.model.clone(), f.dim, el).unwrap();
    let store = Store::create(db, STORE, StoreOptions::new().embedding(model)).unwrap();
    let follows = LinkType::custom(16).unwrap();
    let mut keys: BTreeMap<Vec<u8>, Vec<String>> = BTreeMap::new();
    let mut prev: Option<(String, MemoryId)> = None;
    let mut busy = 0u64;
    let t = Instant::now();
    for (m, v) in f.mems.iter().zip(&f.mem_vecs) {
        let mut mem = Memory::new(m.text.clone())
            .embedding(v.clone())
            .session(m.session.clone());
        if let Some((sess, id)) = &prev
            && *sess == m.session
        {
            mem = mem.link(id.clone(), follows, 1.0);
        }
        let id = remember(&store, &m.ns, mem, &mut busy);
        let key = if f.by_session { &m.session } else { &m.unit };
        keys.entry(row_key(&id)).or_default().push(key.clone());
        prev = Some((m.session.clone(), id));
    }
    println!(
        "{}",
        json!({"build": f.mems.len(), "seconds": t.elapsed().as_secs_f64(), "busy_retries": busy})
    );
    eprintln!(
        "built {} memories in {:.1} s ({busy} Busy retries)",
        f.mems.len(),
        t.elapsed().as_secs_f64()
    );
    drop(store);
    keys
}

/// Recall@5, recall@10 and NDCG@10 of one question's ranked evidence keys.
fn score(ranked: &[Vec<String>], evidence: &BTreeSet<String>) -> (bool, bool, f64) {
    let hit = |k: usize| {
        ranked
            .iter()
            .take(k)
            .any(|ks| ks.iter().any(|x| evidence.contains(x)))
    };
    let mut seen = BTreeSet::new();
    let mut dcg = 0.0;
    for (i, ks) in ranked.iter().take(10).enumerate() {
        if let Some(x) = ks
            .iter()
            .find(|x| evidence.contains(*x) && !seen.contains(*x))
        {
            seen.insert(x.clone());
            dcg += 1.0 / ((i + 2) as f64).log2();
        }
    }
    let ideal: f64 = (0..evidence.len().min(10))
        .map(|i| 1.0 / ((i + 2) as f64).log2())
        .sum();
    (hit(5), hit(10), dcg / ideal)
}

fn percentile(v: &mut [u64], p: f64) -> u64 {
    v.sort_unstable();
    v[((v.len() as f64 - 1.0) * p).round() as usize]
}

#[derive(Default)]
struct Tally {
    n: usize,
    at5: usize,
    at10: usize,
    ndcg: f64,
    us: Vec<u64>,
    by_size: BTreeMap<usize, Vec<u64>>,
}

impl Tally {
    fn add(&mut self, s: (bool, bool, f64), us: u64, ns_size: usize) {
        self.n += 1;
        self.at5 += s.0 as usize;
        self.at10 += s.1 as usize;
        self.ndcg += s.2;
        self.us.push(us);
        // Buckets of powers of two.
        let b = ns_size.next_power_of_two();
        self.by_size.entry(b).or_default().push(us);
    }

    fn print(&mut self, dataset: &str, element: &str, config: &str) {
        let n = self.n as f64;
        println!(
            "{}",
            json!({
                "dataset": dataset, "element": element, "config": config, "questions": self.n,
                "recall@5": self.at5 as f64 / n, "recall@10": self.at10 as f64 / n,
                "ndcg@10": self.ndcg / n,
                "p50_us": percentile(&mut self.us, 0.5), "p99_us": percentile(&mut self.us, 0.99),
            })
        );
    }

    fn print_sizes(&mut self, dataset: &str, config: &str) {
        for (b, us) in &mut self.by_size {
            println!(
                "{}",
                json!({
                    "dataset": dataset, "config": config, "namespace_memories_up_to": b,
                    "questions": us.len(),
                    "p50_us": percentile(us, 0.5), "p99_us": percentile(us, 0.99),
                })
            );
        }
    }
}

fn weights(only: Option<usize>) -> Weights {
    let mut w = Weights::default();
    if only == Some(9) {
        w.recency = 0.0;
        w.uses = 0.0;
        return w;
    }
    if let Some(i) = only {
        let mut v = [0.0; 5];
        v[i] = 1.0;
        w.vector = v[0];
        w.keyword = v[1];
        w.recency = v[2];
        w.importance = v[3];
        w.uses = v[4];
    }
    w
}

/// The query as the fuzzy experiment sends it: words of 5 or more characters as fuzzy terms
/// (lowercased, as the default analyser folds them), the rest as text.
fn fuzzy_query(q: &str) -> Query {
    let mut query = Query::new();
    for w in q
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
    {
        query = if w.chars().count() >= 5 {
            query.fuzzy(w.to_lowercase(), Fuzziness::Auto)
        } else {
            query.text(w)
        };
    }
    query
}

fn recall(fix_dir: &Path, db_dir: &Path, dataset: &str, el_name: &str) {
    let f = fixture(fix_dir, dataset);
    let db = open(db_dir);
    let keys = build(&f, &db, element(el_name));
    db.compact().unwrap();
    let store = Store::open(&db, STORE, &f.model).unwrap();
    let index = Index::open(&db, &format!("mem_{STORE}_text")).unwrap();
    let mut ns_size: BTreeMap<&str, usize> = BTreeMap::new();
    for m in &f.mems {
        *ns_size.entry(&m.ns).or_default() += 1;
    }
    let configs: [(&str, Option<usize>, bool); 6] = [
        ("vector", Some(0), false),
        ("keyword", Some(1), false),
        ("recency", Some(2), false),
        ("fused", None, false),
        // The default weights but recency's (and use's, which these datasets don't have).
        ("fused-no-recency", Some(9), false),
        ("fused+graph", None, true),
    ];
    let ranked_of = |ids: Vec<Vec<u8>>| -> Vec<Vec<String>> {
        ids.iter()
            .map(|k| keys.get(k).cloned().unwrap_or_default())
            .collect()
    };
    for (name, only, graph) in configs {
        let mut t = Tally::default();
        // One untimed pass warms the caches.
        for pass in 0..2 {
            for (q, v) in f.qs.iter().zip(&f.q_vecs) {
                let mut r = store
                    .recall(&[q.ns.as_str()])
                    .text(q.text.clone())
                    .embedding(v.clone())
                    .k(10)
                    .weights(weights(only));
                if graph {
                    r = r.expand(Expand::hops(1));
                }
                let start = Instant::now();
                let hits = r.run().unwrap();
                let us = start.elapsed().as_micros() as u64;
                if pass == 1 {
                    let ranked = ranked_of(hits.iter().map(|h| row_key(&h.id)).collect());
                    t.add(score(&ranked, &q.evidence), us, ns_size[q.ns.as_str()]);
                }
            }
        }
        t.print(dataset, el_name, name);
        if only.is_none() {
            t.print_sizes(dataset, name);
        }
    }
    for (name, fuzzy) in [("keyword-index", false), ("keyword-fuzzy", true)] {
        let mut t = Tally::default();
        for (q, _) in f.qs.iter().zip(&f.q_vecs) {
            let query = if fuzzy {
                fuzzy_query(&q.text)
            } else {
                Query::new().text(q.text.clone())
            };
            let prefix = [q.ns.as_bytes(), b"\0m"].concat();
            let start = Instant::now();
            let hits = index.search(query, 10).id_prefix(prefix).run().unwrap();
            let us = start.elapsed().as_micros() as u64;
            let ranked = ranked_of(hits.into_iter().map(|h| h.id).collect());
            t.add(score(&ranked, &q.evidence), us, ns_size[q.ns.as_str()]);
        }
        t.print(dataset, el_name, name);
    }
    drop(index);
    drop(store);
    db.close().unwrap();
}

fn dir_bytes(p: &Path) -> u64 {
    std::fs::read_dir(p)
        .unwrap()
        .map(|e| e.unwrap().metadata().unwrap().len())
        .sum()
}

fn size(fix_dir: &Path, db_dir: &Path, n: usize) {
    let f = fixture(fix_dir, "lme_m");
    // Unique texts only: exact dedup would merge the rest.
    let mut seen = BTreeSet::new();
    let picked: Vec<usize> = (0..f.mems.len())
        .filter(|&i| seen.insert(f.mems[i].text.as_str()))
        .take(n)
        .collect();
    let n = picked.len();
    // `MEMRECALL_ELEMENTS` (comma-separated) narrows the element types.
    let elements = std::env::var("MEMRECALL_ELEMENTS").unwrap_or_else(|_| "f32,f16,bf16".into());
    for el in elements.split(',') {
        let dir = db_dir.join(format!("size-{el}"));
        std::fs::create_dir_all(&dir).unwrap();
        let db = open(&dir);
        let model = EmbeddingModel::new(f.model.clone(), f.dim, element(el)).unwrap();
        let store = Store::create(&db, STORE, StoreOptions::new().embedding(model)).unwrap();
        let mut busy = 0u64;
        for &i in &picked {
            let m = &f.mems[i];
            remember(
                &store,
                "size",
                Memory::new(m.text.clone())
                    .embedding(f.mem_vecs[i].clone())
                    .session(m.session.clone()),
                &mut busy,
            );
        }
        drop(store);
        db.compact().unwrap();
        // What the data takes: the file less the space no extent holds (a full compaction
        // rewrites and frees, and the file doesn't shrink), split by table and family.
        let info = db.info().unwrap();
        let used = info.file_bytes - info.free_bytes.unwrap_or(0);
        let mut parts = serde_json::Map::new();
        for t in &info.tables {
            for st in &t.storage {
                let b = st.sst_bytes() + st.blob_live_bytes;
                if b > 0 {
                    parts.insert(format!("{}/{}", t.name, st.family), json!(b));
                }
            }
        }
        db.close().unwrap();
        let bytes = dir_bytes(&dir);
        if el == "f32" {
            let db = open(&dir);
            let store = Store::open(&db, STORE, &f.model).unwrap();
            let mut us = Vec::new();
            for pass in 0..2 {
                for (q, v) in f.qs.iter().zip(&f.q_vecs) {
                    let start = Instant::now();
                    let hits = store
                        .recall(&["size"])
                        .text(q.text.clone())
                        .embedding(v.clone())
                        .k(10)
                        .run()
                        .unwrap();
                    assert!(!hits.is_empty());
                    if pass == 1 {
                        us.push(start.elapsed().as_micros() as u64);
                    }
                }
            }
            println!(
                "{}",
                json!({
                    "mode": "size-latency", "memories": n, "questions": us.len(),
                    "p50_us": percentile(&mut us, 0.5), "p99_us": percentile(&mut us, 0.99),
                })
            );
            drop(store);
            db.close().unwrap();
        }
        let text: u64 = picked.iter().map(|&i| f.mems[i].text.len() as u64).sum();
        println!(
            "{}",
            json!({
                "mode": "size", "element": el, "memories": n, "file_bytes": bytes, "busy_retries": busy,
                "used_bytes": used, "used_per_100k": used as f64 * 100_000.0 / n as f64,
                "by_family": parts,
                "text_bytes": text, "embedding_bytes_raw": n as u64 * f.dim as u64 * match el { "f32" => 4, _ => 2 },
            })
        );
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let usage = "memrecall FIXTURE_DIR DB_DIR (recall DATASET [ELEMENT] | size [N])";
    let fix = PathBuf::from(args.get(1).expect(usage));
    let db = PathBuf::from(args.get(2).expect(usage));
    std::fs::create_dir_all(&db).unwrap();
    match args.get(3).map(String::as_str) {
        Some("recall") => recall(
            &fix,
            &db,
            args.get(4).expect(usage),
            args.get(5).map_or("f32", String::as_str),
        ),
        Some("size") => size(
            &fix,
            &db,
            args.get(4).map_or(100_000, |n| n.parse().expect("N")),
        ),
        _ => panic!("{usage}"),
    }
}
