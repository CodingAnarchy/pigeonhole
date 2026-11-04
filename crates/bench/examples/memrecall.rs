//! D311 T5 (#1339): `pigeonhole-memory`'s recall on LongMemEval and LoCoMo, per signal and
//! fused, with its latency, file size per 100k memories, and whether fuzzy keyword terms help.
//!
//! ```text
//! cargo run --release -p pigeonhole-bench --example memrecall -- FIXTURE_DIR DB_DIR recall DATASET [ELEMENT]
//! cargo run --release -p pigeonhole-bench --example memrecall -- FIXTURE_DIR DB_DIR size [N]
//! cargo run --release -p pigeonhole-bench --example memrecall -- FIXTURE_DIR DB_DIR ivf [N]
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
//!   - `fused`: the default weights (recency 0.25), and `fused-no-recency`, `fused-recency-1`
//!     and `fused-recency-0.5`: those with recency's (and use's) at 0, 1 (the default before
//!     1.1.0) or 0.5;
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
    EmbeddingModel, Expand, IvfOptions, LinkType, Memory, MemoryId, Store, StoreOptions, Weights,
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
    let mut written = 0u64;
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
        written += 1;
        if written.is_multiple_of(10_000) {
            // A watchdog can tell a stuck build from a slow one by these lines.
            eprintln!(
                "progress {written} memories, {busy} Busy retries, {:.0} s",
                t.elapsed().as_secs_f64()
            );
        }
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
    if let Some(i @ 10..=12) = only {
        w.recency = [0.0, 1.0, 0.5][i - 10];
        w.uses = w.recency;
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
    let configs: [(&str, Option<usize>, bool); 8] = [
        ("vector", Some(0), false),
        ("keyword", Some(1), false),
        ("recency", Some(2), false),
        ("fused", None, false),
        // The default weights with recency's (and use's, which these datasets don't have)
        // at 0, 1 (the default before 1.1.0) and 0.5.
        ("fused-no-recency", Some(10), false),
        ("fused-recency-1", Some(11), false),
        ("fused-recency-0.5", Some(12), false),
        ("fused+graph", None, true),
    ];
    // Every hit is a memory this run wrote (simp724's N1 on #1402): an unknown id fails
    // rather than scoring as a miss.
    let ranked_of = |ids: Vec<Vec<u8>>| -> Vec<Vec<String>> {
        ids.iter()
            .map(|k| keys.get(k).cloned().expect("a hit this run didn't write"))
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
        let mut found = 0usize;
        // One untimed pass warms the caches, as for the recall rows (simp724's N2 on #1402).
        for pass in 0..2 {
            for (q, _) in f.qs.iter().zip(&f.q_vecs) {
                let query = if fuzzy {
                    fuzzy_query(&q.text)
                } else {
                    Query::new().text(q.text.clone())
                };
                // The store's memory rows are `{ns}\0m{rid}` (ICR 0107's frozen layout), as
                // its own recall passes `id_prefix`.
                let prefix = [q.ns.as_bytes(), b"\0m"].concat();
                let start = Instant::now();
                let hits = index.search(query, 10).id_prefix(prefix).run().unwrap();
                let us = start.elapsed().as_micros() as u64;
                if pass == 1 {
                    found += hits.len();
                    let ranked = ranked_of(hits.into_iter().map(|h| h.id).collect());
                    t.add(score(&ranked, &q.evidence), us, ns_size[q.ns.as_str()]);
                }
            }
        }
        // The index's name and the id prefix are the store's layout: a change there would
        // read as no hits, not fail (cli692's N2 on #1402).
        assert!(
            found > 0,
            "{name}: no hits at all; has the store's index name or layout changed?"
        );
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
            // Fused, and each of its two query-dependent signals alone, so the large
            // namespace's cost can be told apart (perf287's S1 on #1402).
            for (name, only) in [("fused", None), ("vector", Some(0)), ("keyword", Some(1))] {
                let mut us = Vec::new();
                for pass in 0..2 {
                    for (q, v) in f.qs.iter().zip(&f.q_vecs) {
                        let mut r = store.recall(&["size"]).k(10).weights(weights(only));
                        if only != Some(1) {
                            r = r.embedding(v.clone());
                        }
                        if only != Some(0) {
                            r = r.text(q.text.clone());
                        }
                        let start = Instant::now();
                        let hits = r.run().unwrap();
                        assert!(!hits.is_empty());
                        if pass == 1 {
                            us.push(start.elapsed().as_micros() as u64);
                        }
                    }
                }
                let n_q = us.len();
                println!(
                    "{}",
                    json!({
                        "mode": "size-latency", "config": name, "memories": n, "questions": n_q,
                        "p50_us": percentile(&mut us, 0.5), "p90_us": percentile(&mut us, 0.9),
                        "max_us": percentile(&mut us, 1.0),
                    })
                );
            }
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

/// D311 T8 (#1342): IVF recall (`StoreOptions::ivf`, defaults) in one f32 store of `lme_m`'s
/// unique memories: `big` (N, 100,000 by default, less the others), and `sparse` (10,000,
/// remembered interleaved with `big`'s, so it is spread thinly over the index's lists),
/// with 5,000 held back for the writers. Per namespace, over `lme_m`'s 50 questions twice
/// (the second pass timed): fused and vector-only latency, exact and through the index, and
/// the index's recall@10 of the exact vector-only ranking, with the recalls' IVF counts.
/// Then simp724's parallel-writer rows (#1342 C1, #1477): 1, 4, 8 and 16 threads remembering
/// into `big` for `MEMRECALL_WRITER_SECS` each (8 by default), with throughput, remember
/// latency, `Busy` sleeps, transaction retries and conflicts, room and pacing stall time, and
/// flushes.
fn ivf(fix_dir: &Path, db_dir: &Path, n: usize) {
    let f = fixture(fix_dir, "lme_m");
    let mut seen = BTreeSet::new();
    let picked: Vec<usize> = (0..f.mems.len())
        .filter(|&i| seen.insert(f.mems[i].text.as_str()))
        .take(n)
        .collect();
    let (held, rest) = picked.split_at(5_000.min(picked.len() / 4));
    let sparse_n = 10_000.min(rest.len() / 4);
    let dir = db_dir.join("ivf");
    std::fs::create_dir_all(&dir).unwrap();
    let db = open(&dir);
    let model = EmbeddingModel::new(f.model.clone(), f.dim, Element::F32).unwrap();
    let store = Store::create(
        &db,
        STORE,
        StoreOptions::new().embedding(model).ivf(IvfOptions::new()),
    )
    .unwrap();
    let mut busy = 0u64;
    let every = rest.len() / sparse_n.max(1);
    let t = Instant::now();
    for (j, &i) in rest.iter().enumerate() {
        let m = &f.mems[i];
        let ns = if j % every == 0 && j / every < sparse_n {
            "sparse"
        } else {
            "big"
        };
        remember(
            &store,
            ns,
            Memory::new(m.text.clone()).embedding(f.mem_vecs[i].clone()),
            &mut busy,
        );
        if (j + 1) % 10_000 == 0 {
            eprintln!(
                "progress {} memories, {:.0} s",
                j + 1,
                t.elapsed().as_secs_f64()
            );
        }
    }
    println!(
        "{}",
        json!({"mode": "ivf-build", "memories": rest.len(), "sparse": sparse_n,
               "seconds": t.elapsed().as_secs_f64(), "busy_retries": busy})
    );
    for ns in ["big", "sparse"] {
        for (name, only) in [("fused", None), ("vector", Some(0))] {
            let mut lat = [Vec::new(), Vec::new()];
            let (mut found, mut total, mut ivf_ns, mut fallbacks) = (0usize, 0usize, 0, 0);
            for pass in 0..2 {
                for (q, v) in f.qs.iter().zip(&f.q_vecs) {
                    let mut got: [Vec<MemoryId>; 2] = [Vec::new(), Vec::new()];
                    for (k, exact) in [true, false].into_iter().enumerate() {
                        let mut r = store
                            .recall(&[ns])
                            .k(10)
                            .weights(weights(only))
                            .embedding(v.clone())
                            .exact(exact);
                        if only.is_none() {
                            r = r.text(q.text.clone());
                        }
                        let start = Instant::now();
                        let (hits, st) = r.run_with_stats().unwrap();
                        if pass == 1 {
                            lat[k].push(start.elapsed().as_micros() as u64);
                            if !exact {
                                ivf_ns += st.ivf_namespaces;
                                fallbacks += st.ivf_fallbacks;
                            }
                        }
                        got[k] = hits.into_iter().map(|h| h.id).collect();
                    }
                    if pass == 1 && only == Some(0) {
                        let want: BTreeSet<&MemoryId> = got[0].iter().collect();
                        found += got[1].iter().filter(|id| want.contains(id)).count();
                        total += want.len();
                    }
                }
            }
            let [mut exact, mut via] = lat;
            println!(
                "{}",
                json!({
                    "mode": "ivf-recall", "namespace": ns, "config": name,
                    "exact_p50_us": percentile(&mut exact, 0.5), "exact_max_us": percentile(&mut exact, 1.0),
                    "ivf_p50_us": percentile(&mut via, 0.5), "ivf_max_us": percentile(&mut via, 1.0),
                    "recall_at_10_vs_exact": (total > 0).then(|| found as f64 / total as f64),
                    "ivf_namespaces": ivf_ns, "ivf_fallbacks": fallbacks,
                })
            );
        }
    }
    // Parallel writers into `big` (simp724's C1 on #1342; #1477): each thread count writes for
    // a fixed time, so a phase is long enough to include the write stalls it causes. The
    // writers take the held memories in turn, and past them the same ones again under a new
    // text (the same vectors), so no phase runs short.
    let secs = std::env::var("MEMRECALL_WRITER_SECS")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(8.0);
    let cursor = std::sync::atomic::AtomicUsize::new(0);
    for threads in [1usize, 4, 8, 16] {
        let before = db.engine_metrics();
        let busy_all = std::sync::atomic::AtomicU64::new(0);
        let lats = std::sync::Mutex::new(Vec::<u64>::new());
        let t = Instant::now();
        std::thread::scope(|sc| {
            for _ in 0..threads {
                let (store, f, cursor, busy_all, lats) = (&store, &f, &cursor, &busy_all, &lats);
                sc.spawn(move || {
                    let mut busy = 0;
                    let mut mine = Vec::new();
                    while t.elapsed().as_secs_f64() < secs {
                        let k = cursor.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let i = held[k % held.len()];
                        let text = if k < held.len() {
                            f.mems[i].text.clone()
                        } else {
                            format!("{} #{}", f.mems[i].text, k / held.len())
                        };
                        let s = Instant::now();
                        remember(
                            store,
                            "big",
                            Memory::new(text).embedding(f.mem_vecs[i].clone()),
                            &mut busy,
                        );
                        mine.push(s.elapsed().as_micros() as u64);
                    }
                    busy_all.fetch_add(busy, std::sync::atomic::Ordering::Relaxed);
                    lats.lock().unwrap().extend(mine);
                });
            }
        });
        let elapsed = t.elapsed().as_secs_f64();
        let after = db.engine_metrics();
        let mut lat = lats.into_inner().unwrap();
        let n = lat.len();
        let (p50, p99, max) = if n == 0 {
            (0, 0, 0)
        } else {
            (
                percentile(&mut lat, 0.5),
                percentile(&mut lat, 0.99),
                percentile(&mut lat, 1.0),
            )
        };
        println!(
            "{}",
            json!({
                "mode": "ivf-writers", "threads": threads, "memories": n, "seconds": elapsed,
                "per_second": n as f64 / elapsed,
                "p50_us": p50, "p99_us": p99, "max_us": max,
                "busy_sleeps": busy_all.load(std::sync::atomic::Ordering::Relaxed),
                "retries": after.transaction_retries - before.transaction_retries,
                "conflicts": after.transaction_conflicts - before.transaction_conflicts,
                "room_stalls": after.room_stalls - before.room_stalls,
                "room_stall_ms": (after.room_stall_nanos - before.room_stall_nanos) / 1_000_000,
                "pacing_stalls": after.pacing_stalls - before.pacing_stalls,
                "pacing_stall_ms": (after.pacing_stall_nanos - before.pacing_stall_nanos) / 1_000_000,
                "flushes": after.flushes - before.flushes,
            })
        );
    }
    let st = store.verify().unwrap();
    println!(
        "{}",
        json!({"mode": "ivf-verify", "consistent": st.is_consistent(),
               "ivf_mismatched": st.ivf_mismatched, "ivf_counter_drift": st.ivf_counter_drift})
    );
    drop(store);
    db.close().unwrap();
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let usage = "memrecall FIXTURE_DIR DB_DIR (recall DATASET [ELEMENT] | size [N] | ivf [N])";
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
        Some("ivf") => ivf(
            &fix,
            &db,
            args.get(4).map_or(100_000, |n| n.parse().expect("N")),
        ),
        _ => panic!("{usage}"),
    }
}
