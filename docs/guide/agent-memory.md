# Agent memory

`pigeonhole-memory` gives agents long-term memory in a Pigeonhole database (D311). Memories are stored with their text and an embedding, scoped to a namespace, written in transactions, and recalled by meaning, keyword and recency together.

## Creating a store and remembering
A store is one table plus a full-text index. Pin it to the embedding model your application uses: the store records the model's id and dimension, and refuses vectors from any other.

```rust
# use pigeonhole::*;
use pigeonhole_memory::{EmbeddingModel, Memory, Remembered, Store, StoreOptions};
# fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
let model = EmbeddingModel::new("my-embedder-v1", 4, Element::F32)?;
let store = Store::create(&db, "agents", StoreOptions::new().embedding(model))?;
let r = store.remember(
    "agent:alice",
    Memory::new("The March meeting chose Postgres.").embedding(vec![0.9, 0.1, 0.0, 0.2]),
)?;
// The same text again (spacing aside) merges into the first memory.
let again = store.remember(
    "agent:alice",
    Memory::new("The  March meeting chose Postgres. ").embedding(vec![0.9, 0.1, 0.0, 0.2]),
)?;
assert_eq!(again, Remembered::Merged(r.id().clone()));
# db.close()?;
# Ok(())
# }
```

- **Namespaces** are exact strings, such as `agent:alice` or `shared:team`. `agent:alice/x` is a different namespace, not a child. An empty namespace, or one containing `\0`, is refused.
- **A memory** can also carry a kind (episodic, semantic or procedural), an importance from 0 to 1, a source id, a session, links to other memories in its namespace, and a pinned flag.
- **Every write is one transaction:** the memory, its text index entry, its dedup row and both directions of its links commit together. `remember_in` joins your own transaction.
- **Exact dedup:** the same text after trimming and collapsing whitespace merges into the live memory that has it. It keeps the larger importance, adds the source and counts a use. There's no case folding.
- **The model check:** `Store::open(&db, name, model_id)` fails with `Mismatch` for another model. `open_without_embeddings` gives a handle that can delete, link and mark memories used, but can't add or supersede them in a pinned store.
- **The keyword index's normalisation:** `StoreOptions::normalization(Normalization::Nfkc)` makes keyword recall match ligatures and full-width forms (`ﬁle`, `ｆｉｌｅ`) against plain text, for memories gathered from PDFs, OCR or East Asian sources. The default, `Nfc`, only unifies different encodings of the same character. It's fixed at create, and `Store::normalization()` reads it back. To change it, create a new store and remember the old one's memories into it. Python's `Store.create(..., normalization="nfkc")` and `phdb mcp --memory-normalization nfkc` set it too. The full-text guide's [which normalisation to pick](text-search.md#options) has the trade-offs.

## Which embeddings
A store is pinned to its embedding model when it's created, so choose first. There are three ways to get embeddings, plus no model at all (keyword recall):

| | Local (`pigeonhole-embed`) | An endpoint | Your own vectors |
|---|---|---|---|
| **Runs** | in your process, on the CPU | in a service: Ollama, llama.cpp, or a hosted API | wherever you embed today |
| **Memory text leaves the machine** | never | to the endpoint (local or hosted) | your choice |
| **Model** | `bge-small-en-v1.5`, pinned to a revision | any the endpoint serves | any |
| **Cost per short text** | about 7–12 ms of CPU | a network round trip plus the endpoint's time | yours |
| **`phdb mcp` / `phdb serve`** | `--embed-local` (the `embed-local` feature) | `--embed-url` | `--memory-model` with `--memory-dim` |

- **Choose local** for an agent on one machine, offline or air-gapped work, or memory text that mustn't leave the host. It's English-first and truncates at 512 tokens.
- **Choose an endpoint** for a larger or multilingual model, a GPU, or one embedding service shared by many processes.
- **Supply your own** when your application already embeds with some model and you want memory recall in the same space.

## Local embeddings
`pigeonhole-embed` (0.1, on its own version line) computes embeddings in your process, on the CPU, with no service and no network after the first load. It runs [`BAAI/bge-small-en-v1.5`](https://huggingface.co/BAAI/bge-small-en-v1.5) (MIT licence), the model this guide's [recall numbers](#how-well-it-recalls) were measured with, and gives the reference implementation's (sentence-transformers') vectors to within 1e-4 per component.

```toml
[dependencies]
pigeonhole-embed = "0.1"
```

```rust,ignore
use pigeonhole_embed::{Element, LoadOptions, LocalEmbedder, Model};

let embedder = LocalEmbedder::open(Model::BgeSmallEnV15, &LoadOptions::new())?;
let store = Store::create(&db, "agents", StoreOptions::new().embedding(embedder.embedding_model(Element::F32)?))?;

let text = "The March meeting chose Postgres.";
store.remember("agent:alice", Memory::new(text).embedding(embedder.embed_passages(&[text])?.remove(0)))?;
let hits = store
    .recall(&["agent:alice"])
    .embedding(embedder.embed_query("which database did we pick?")?)
    .k(5)
    .run()?;
```

- **Passages and queries are embedded differently.** `embed_passages` is for what you remember. `embed_query` adds the model's retrieval instruction, which recall's measured quality depends on. Don't embed a query as a passage.
- **The pinned id** is `BAAI/bge-small-en-v1.5@5c38ec7c`: the model and its revision. A store created with it refuses vectors from anything else, including the same model served by Ollama or another runtime, whose vectors can differ.
- **The files:** the weights (133 MB) aren't in the crate. The first `open` downloads the model's three files from the pinned revision on huggingface.co into the cache: `$PIGEONHOLE_EMBED_CACHE`, else `~/.cache/pigeonhole/models` on Linux, `~/Library/Caches/pigeonhole/models` on macOS, and `%LOCALAPPDATA%\pigeonhole\models` on Windows. Every file is checked against a SHA-256 pinned in the crate, however it arrived.
- **Offline and air-gapped:** `LoadOptions::offline()` (or `PIGEONHOLE_EMBED_OFFLINE=1`) never downloads, and a missing file fails with the URL to fetch and where to put it. `LoadOptions::dir(path)` loads from a directory you filled yourself. Build with `default-features = false` to leave out the download code entirely.
- **Text past 512 tokens is truncated** (about 350 English words), so split long documents before remembering them.
- **One embedder, shared:** each `LocalEmbedder` holds the weights (about 133 MB), and it's `Send` and `Sync`. Calls from many threads queue for its pool.
- **The store's element:** `embedding_model(Element::F16)` (or `BF16`) pins a half-size store. The embeddings stay `f32`, and the store rounds them as it writes.
- **CPU cost:** the matrix products run on the embedder's own thread pool (`LoadOptions::threads`, by default up to 4), never your application's rayon pool. `LoadOptions::on_thread_start` can place its threads. candle runs large element-wise steps on a process-wide pool of its own, one thread per physical core less one, sized by `CANDLE_NUM_THREADS` and on Linux pinned by `CANDLE_CPU_MASK` (a CPU list). Export both before starting the process (they're read once) to keep embedding work off cores your latency-sensitive threads use. `phdb serve` prints the values to use for its placement. On a 10-core Apple laptop:

| Threads | 16 tokens | 64 tokens | 256 tokens | Batches of 32 (16 tokens) |
|---|---|---|---|---|
| 1 | 12 ms | 33 ms | 130 ms | 148 texts/s |
| 4 | 7.1 ms | 17 ms | 68 ms | 353 texts/s |

  Pass many texts to `embed_passages` when you can: a batch costs far less per text. A query is one text.
- **On Linux, set `CANDLE_CPU_MASK` (or `RAYON_NUM_THREADS`) in the environment.** Without either, candle 0.11 re-reads `/proc/cpuinfo` on every matrix product, which costs about 47 ms per call on a 16-core x86 server, five times the work itself ([huggingface/candle#4070](https://github.com/huggingface/candle/issues/4070)). Either variable avoids it. `RAYON_NUM_THREADS` also sizes your application's global rayon pool, so prefer `CANDLE_CPU_MASK` if that matters. With one set, the same server takes 11 / 37 / 164 ms at 16 / 64 / 256 tokens on 1 thread, and 6.4 / 24 / 98 ms on 4.
- **About 4 threads for interactive use, not all cores.** One text doesn't split well across many threads: on the same 16-core (32-thread) server, 32 threads took 9.4 ms at 16 tokens against 6.4 ms on 4, and 135 against 98 ms at 256 tokens. Only large batches of long texts gain a little. The default, `min(cores, 4)`, is the right choice for `remember` and `recall`; give a dedicated batch embedder more only if you've measured that it helps.
- **Allocator:** each forward pass allocates and frees its tensors. On Linux, glibc's allocator gives large freed blocks back to the system, so the next pass faults them in again (about a fifth of the CPU at 1 thread). A server embedding steadily may prefer jemalloc or mimalloc as its global allocator.

## Recall
```rust
# use pigeonhole::*;
# use pigeonhole_memory::{EmbeddingModel, Memory, Store, StoreOptions};
# fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let store = Store::create(&db, "agents", StoreOptions::new().embedding(EmbeddingModel::new("m", 4, Element::F32)?))?;
# store.remember("agent:alice", Memory::new("The March meeting chose Postgres.").embedding(vec![0.9, 0.1, 0.0, 0.2]))?;
# store.remember("agent:alice", Memory::new("Lunch was good.").embedding(vec![0.0, 0.0, 1.0, 0.0]))?;
let hits = store
    .recall(&["agent:alice"])
    .text("database choice from the meeting")
    .embedding(vec![0.8, 0.2, 0.0, 0.1])
    .k(5)
    .byte_budget(4 << 10)
    .run()?;
assert_eq!(hits[0].text, "The March meeting chose Postgres.");
// Tell the store which memories helped: recall itself never writes.
store.mark_used(&[hits[0].id.clone()])?;
# db.close()?;
# Ok(())
# }
```

- **Candidates:** for each namespace, the 50 most similar by embedding (an exact search), the 50 best keyword matches (BM25 from the text index), and the 50 newest. `candidates(n)` changes the 50. They're counted after the filters below: a session-filtered recall reads on until each signal has its 50 from that session, or runs out.
- **Signals:** every candidate gets each signal:
  - its similarity;
  - its keyword score, if it was a keyword match;
  - its recency, `exp(−age / half_life)` with a half-life of 7 days by default;
  - its importance;
  - its use, `ln(1 + uses)`, decayed since its last use.
- **Fusion:** reciprocal rank fusion, `Σ w / (60 + rank)` over the signals a memory ranks in. Equal values share a rank, so a tie gives no memory an edge, and equal fused scores are ordered by id. `Fusion::Linear` sums min-max-normalised signals instead. `Weights` sets each signal's weight.
- **The byte budget:** each hit counts its text, its id and 16 bytes, plus its signals and embedding when asked for (`with_signals`, `with_embeddings`). Hits are whole or left out, and at least one is returned, even if it alone is over the budget. `run_with_stats()`'s `RecallStats::truncated` says when the budget cut the list.
- **Filters:** superseded memories are left out unless `include_superseded(true)`, and then each such hit's `superseded_by` names its successor. `session(s)` keeps one session.
- **Determinism:** with an explicit `snapshot` and `now_us(time)`, the same call always returns the same result.
- **Either signal alone works:** text alone recalls by keyword and recency, and an embedding alone by similarity and recency.
- **Async:** `run_async().await` and `run_with_stats_async().await` return exactly what `run` and `run_with_stats` do, with every read asynchronous, so a recall doesn't block an executor thread on the cache's misses. Each namespace's vector, keyword and recency lists are read together. The futures are `Send`, and they work in reader processes too. They're behind the `async` feature, on by default as in `pigeonhole`.

## Lifecycle
- **`get(&id)`** returns a memory with all its metadata.
- **`delete(&id)`** removes the memory, its index entry, its dedup row and both directions of its links, in one transaction.
- **`link(&from, &to, type, weight)`** links two memories in a namespace.
- **`supersede(&old, &new)`** keeps the old memory but leaves it out of recall: the fact changed, and its history stays.
- **`mark_used(&ids)`** counts a use. It's how recall's use signal learns.
- **`verify()`** checks the store against itself and its text index.

## Forgetting
A store can forget on its own. Give it a policy at create, or later with `set_forget_policy`:

```rust
# use pigeonhole::*;
use std::time::Duration;
use pigeonhole_memory::{ForgetPolicy, Kind, Memory, Store, StoreOptions};
# fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
let policy = ForgetPolicy::new()
    .min_retention(0.05)
    .max_age(Kind::Episodic, Duration::from_secs(30 * 24 * 3600));
let store = Store::create(&db, "agents", StoreOptions::new().forget(policy))?;
store.remember("agent:alice", Memory::new("A passing remark.").importance(0.01))?;
// The crate's forgetter thread runs the policy hourly; a pass can also run now.
let pass = store.forget().run()?;
assert_eq!(pass.deleted, 1);
# db.close()?;
# Ok(())
# }
```

- **Retention:** `importance · exp(−since_last_use / half_life) + use_weight · ln(1 + uses)`. Memories below `min_retention` are forgotten. The use term doesn't decay, so a memory used often enough stays.
- **`max_age(kind, age)`** forgets a kind's memories `age` after their creation: a TTL. `StoreOptions::episodic_ttl(age)` is the shorthand.
- **Protected memories** are never forgotten: pinned ones, and the sources a live semantic memory was derived from.
- **Each deletion re-checks its memory in its own transaction.** So a `mark_used`, a merge or a new link that lands meanwhile is respected.
- **The `Forgetter`:** the crate runs one on its own thread when the store has a policy. An application that runs its own threads opens with `OpenOptions::new().own_forgetter()` and drives `take_forgetter()` itself.
- **Failures and closing:** a failed pass is retried after 5 s, doubling with each failure in a row up to the policy's interval. `Pigeonhole::close` doesn't wait for a pass: the deletion in flight fails with `Closed` and is dropped whole, so nothing is left half-forgotten. To let a pass finish, drop the `Store` handles before closing.
- **Forgotten memories are deletes,** so the change feed shows them.

## Consolidation
Consolidation folds episodic memories into semantic ones. The summariser is yours, behind the `Consolidator` trait. It's called outside any transaction, so it can call a model.

```rust
# use pigeonhole::*;
use std::time::Duration;
use pigeonhole_memory::{
    ConsolidatePolicy, Consolidator, Kind, Memory, SourcePolicy, Store, StoreOptions, StoredMemory,
};
struct Summarise;
impl Consolidator for Summarise {
    fn consolidate(
        &self,
        group: &[StoredMemory],
    ) -> Result<Vec<Memory>, Box<dyn std::error::Error + Send + Sync>> {
        // A model call in practice.
        let text = group.iter().map(|m| m.text.as_str()).collect::<Vec<_>>().join(" ");
        Ok(vec![Memory::new(format!("Summary: {text}")).importance(0.8)])
    }
}
# fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
let store = Store::create(&db, "agents", StoreOptions::new())?;
store.remember("agent:alice", Memory::new("Standup moved to 10.").session("s1"))?;
store.remember("agent:alice", Memory::new("Standup is in room B.").session("s1"))?;
let policy = ConsolidatePolicy::new().min_age(Duration::ZERO).sources(SourcePolicy::Keep);
let pass = store.consolidate("agent:alice", &Summarise).policy(&policy).run()?;
assert_eq!((pass.committed, pass.created), (1, 1));
# db.close()?;
# Ok(())
# }
```

- **Groups:** episodic memories at least `min_age` old (an hour by default) that no consolidation took in.
  - They're grouped by session first.
  - With an embedding model, a memory then joins a group when its cosine to the group's first memory (its seed) is at least `group_similarity` (0.8).
  - A pass looks at a bounded number of memories, newest first. When it stops short, `ConsolidatePass::resume` says where, and `.after(&id)` continues from there. A pass from the start also picks up memories that have aged in since.
  - A memory with nothing like it is left alone, unless `min_group(1)` asks for rewrites.
- **Each group commits in its own transaction:**
  - the new memories, with `DERIVED_FROM` links to the group and the group's sources;
  - then the source policy: `Keep` (the default) marks the group consolidated, `Delete` deletes it, and `Supersede` supersedes it by the first new memory.
- **Recall and consolidated memories:** recall weights a consolidated memory's importance by `Weights::consolidated` (0.5), so its summary ranks ahead of it.
- **Concurrent passes:** a group that changed while the consolidator ran is skipped, not retried, and so is one another pass took. The next pass regroups. `ConsolidatePass` counts both.
- **A summary equal to one of its sources** merges into that source, which becomes the summary.
- **A consolidator error** stops the pass with `Error::Consolidate { error, pass }`: its own `Err`, a returned memory the store refuses, or a returned link to a memory that's gone. The groups before it stay committed.
- **When to run it:** on demand. Unlike forgetting, there's no thread.

## Following links
`expand` widens a recall to the memories its best candidates link to, or that link to them:
```rust
# use pigeonhole::*;
# use pigeonhole_memory::{EmbeddingModel, Expand, LinkType, Memory, Store, StoreOptions};
# fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
# let store = Store::create(&db, "agents", StoreOptions::new().embedding(EmbeddingModel::new("m", 4, Element::F32)?))?;
let why = store.remember("agent:alice", Memory::new("Postgres won on JSON support.").embedding(vec![0.0, 0.0, 1.0, 0.0]))?.id().clone();
let what = store.remember("agent:alice", Memory::new("The March meeting chose Postgres.").embedding(vec![0.9, 0.1, 0.0, 0.2]))?.id().clone();
store.link(&what, &why, LinkType::custom(16)?, 0.9)?;
let hits = store
    .recall(&["agent:alice"])
    .embedding(vec![0.8, 0.2, 0.0, 0.1])
    .candidates(1)
    .expand(Expand::hops(1))
    .run()?;
assert!(hits.iter().any(|h| h.id == why));
# db.close()?;
# Ok(())
# }
```

- **Seeds:** the top 10 of the fused list. Seed `i` is worth `1 / i`.
- **The graph signal:** a memory reached at a hop gets the best `value × link weight` over the links between it and the previous hop's memories, in either direction. It's ranked and fused like the other signals, with `Weights::graph` (0.5 by default), and is reported as `Signals::graph`. Seeds don't get one, and nothing does without `expand`.
- **Bounds:** `Expand::hops(n)` takes 1 to 4 hops. `.max(m)` (default 20) caps how many memories get a graph signal; each hop keeps its best-valued ones while there's room.
- **The byte budget:** with `byte_budget(B)`, the walk reads at most `B` bytes of nodes, besides the hits' own `B`. A hop that doesn't fit whole is dropped. `RecallStats::expanded` counts the memories reached, and `expansion_truncated` says when `max` or the budget stopped the walk.
- **Filters:** the walk passes only through memories the recall could return, so a superseded memory or another session's doesn't bridge to its neighbours.

## Near duplicates
With an embedding model, a store can catch near duplicates as well as exact ones. Turn it on with `StoreOptions::near_dedup(NearDedup::new(0.95))`, or later with `set_near_dedup`. `remember` then looks up the nearest live memory first.
- **Policies, for a match at or above the similarity:**
  - `Link` (the default) writes the memory with a `SIMILAR_TO` link to the match;
  - `Skip` writes nothing and returns `Remembered::Similar(id)`. **Careful:** `id()` is then the match, a memory this call didn't write. Code that links or pins `remember(..)?.id()` acts on the match once you turn `Skip` on;
  - `Merge` merges into the match as an exact duplicate would.
- **The check runs outside the transaction,** so two near duplicates written at once can both land. Consolidation folds them later. The match is read again inside the transaction, and one deleted or superseded meanwhile is ignored.
- **Summaries skip it:** what a consolidator returns is meant to sit near its sources.
- **`remember_in` doesn't check:** call `near_duplicate(ns, &memory)` before your transaction to do it yourself.

## Large namespaces: IVF recall
Recall searches a namespace's embeddings exactly. That stays fast up to a few thousand memories per namespace. For larger ones, create the store with an IVF index (`pigeonhole-vector`):

```rust
# use pigeonhole::*;
use pigeonhole_memory::{EmbeddingModel, IvfOptions, Memory, Store, StoreOptions};
# fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default())?;
let model = EmbeddingModel::new("my-embedder-v1", 4, Element::F32)?;
let store = Store::create(
    &db,
    "agents",
    StoreOptions::new().embedding(model).ivf(IvfOptions::new().min_namespace_len(10_000)),
)?;
store.remember("shared:docs", Memory::new("Deploys run at noon.").embedding(vec![0.9, 0.1, 0.0, 0.2]))?;
let (hits, stats) = store
    .recall(&["shared:docs"])
    .embedding(vec![0.8, 0.2, 0.0, 0.1])
    .run_with_stats()?;
assert_eq!(hits.len(), 1);
// One memory is far below min_namespace_len: this recall searched exactly.
assert_eq!(stats.ivf_namespaces, 0);
# db.close()?;
# Ok(())
# }
```

- **Per namespace:** a namespace with at least `min_namespace_len` memories with an embedding (10,000 by default) is searched through the index, and a smaller one exactly. A namespace counter, kept in each write, decides at the recall's snapshot.
- **Every write keeps the index in step:** a memory's vector goes into the index in the memory's own transaction, and leaves it when the memory is deleted, forgotten or consolidated away. `verify` checks both at one snapshot: `ivf_missing` and `ivf_stale` count the memories the index lacks and the vectors it holds for no memory, `ivf_mismatched` is their sum and counts against `is_consistent`, and `ivf_counter_drift` is advisory.
- **Exactness:** an IVF search probes the lists nearest the query, so it can miss a memory an exact search finds. `Recall::nprobe(n)` probes more for one recall, and `Recall::exact(true)` searches exactly, for checking. A namespace that's sparse among the lists nearest the query (a small one in a big store) falls back to an exact search on its own. `RecallStats::ivf_namespaces` and `ivf_fallbacks` say which path ran.
- **Cost:** a second copy of each embedding (about 159 MB per 100k memories at `f32`, 82 MB at `f16`), and the index's lists to maintain as memories arrive. The index rebalances in the background.
- **It's fixed at create.** To add an index to an existing store, create a new store with it and remember the memories into it.

On the reference AX102 (`memrecall … ivf`, D311 T8), one f32 store held 89,157 memories in one namespace and 10,000 in another, remembered interleaved so the second is spread thinly over the index's lists. The latency is warm and single-threaded at the default `nprobe`, p50 over LongMemEval-M's 50 questions:

| | exact | IVF | IVF's recall@10 of the exact vector ranking |
|---|---|---|---|
| 89k namespace, vector only | 31.8 ms | 19.2 ms | 0.99 |
| 89k namespace, fused | 65.1 ms | 49.9 ms | |
| 10k namespace, vector only | 3.4 ms | 2.3 ms | 0.97 |
| 10k namespace, fused | 15.3 ms | 13.7 ms | |

- **IVF takes off part of the vector search's cost, not the keyword search's:** about 28 ms of the large namespace's fused recall is keyword search (T5's split).
- **Writers gain up to 16 threads, with or without the index, though less past 8.** Threads remembering into the large namespace, 8 s per thread count, three runs (`memrecall … ivf` and `… flat`, the same store without the index, #1477):

  | writers | memories/s with the index | without it | remember p99 with the index |
  |--:|--:|--:|--:|
  | 1 | 1,320–1,420 | 1,550–1,760 | 1.1–1.3 ms |
  | 4 | 3,230–3,910 | 3,650–4,800 | 3.3–4.2 ms |
  | 8 | 5,820–6,880 | 7,090–7,900 | 5.8–7.9 ms |
  | 16 | 7,850–8,410 | 9,430–10,160 | 10.0–10.6 ms |

  The 4-writer phase runs first after the build, behind its flush and compaction backlog, so it reads a little low.

  - **How many to use:** up to 8 writers when remember latency matters (p99 under about 8 ms), and 16 for bulk imports. More than 16 wasn't measured.
  - **The index costs about 10–20% of write throughput,** for the vector copy and list upkeep.
  - **Writers rarely collide:** one that meets a list mid-split retries its transaction inside `remember`, 1–27 times per 8 s at 4–16 writers, out of 26,000–67,000 memories. A list's version only changes when it splits.
- **Building it:** the 99k memories took 60 s, about the same rate as T5's builds without the index.

## Watching for new memories
`store.watch(&[namespaces])` follows the store's memory events through the database's change feed, so another part of the application, or another process, learns that a memory arrived without polling. The database must be opened with the change feed on (`Options::change_feed`), following the store's table `mem_{name}` (every table by default):

```rust
# use pigeonhole::*;
use pigeonhole_memory::{Memory, MemoryEvent, Store, StoreOptions};
# fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(
    dir.join("guide.phdb"),
    Options::default().change_feed(FeedRetention::bytes(64 << 20)),
)?;
let store = Store::create(&db, "agents", StoreOptions::new())?;
let mut watch = store.watch(&["shared:team"]).open()?;
let r = store.remember("shared:team", Memory::new("The demo moved to Friday."))?;
let commit = watch.next(std::time::Duration::from_secs(1))?.expect("one commit");
assert_eq!(commit.events, vec![MemoryEvent::Remembered(r.id().clone())]);
# db.close()?;
# Ok(())
# }
```

- **Events, one per memory per commit:**
  - `Remembered`: a new memory;
  - `Summarized`: a consolidation's summary;
  - `Superseded { id, by }`;
  - `Deleted`: also from a forget pass or a consolidation's `Delete`;
  - `Updated`: an exact-dedup merge, `mark_used`, a link, a source added, or a consolidation taking the memory in.

  Commits that touch no memory are passed over.
- **An empty namespace list** watches every namespace.
- **Resuming:** `watch.token()` gives a `WatchToken` to store (`as_bytes`/`from_bytes`), and `store.watch(..).resume(&token)` continues after it, in a later open too. `from_snapshot(&s)` starts after what a snapshot sees: read the store at `s`, then watch, and nothing is missed or seen twice.
- **Async, in the writer:** `watch.next_async().await` waits without holding a thread (its future is `Send`), and `poll_next(cx)` is the same for hand-written futures.
- **In a reader process,** `read_store.watch(..)` follows the writer's events from its feed history. There `next_async` is `Unsupported`, and `next` blocks, so call it on a thread of its own.
- **Delivery** is at durability by default. `.visible()` delivers a commit as soon as reads see it, at the cost the change feed describes.
- **If the feed's history is recycled past a watch,** `next` fails with `FeedExpired`. Read the store afresh at a snapshot and watch `from_snapshot` it.

## Consolidating as memories arrive
`store.consolidate_loop(name, &consolidator)` runs consolidation passes as new memories arrive, instead of on a schedule. It watches the store through the change feed (as above), and runs a pass for a namespace once its oldest new memory is the policy's `min_age` old:

```rust
# use pigeonhole::*;
# use pigeonhole_memory::{ConsolidatePolicy, Consolidator, Memory, Store, StoreOptions, StoredMemory};
# struct Summarize;
# impl Consolidator for Summarize {
#     fn consolidate(&self, g: &[StoredMemory]) -> std::result::Result<Vec<Memory>, Box<dyn std::error::Error + Send + Sync>> {
#         Ok(vec![Memory::new(format!("{} notes", g.len()))])
#     }
# }
# fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
# let dir = pigeonhole::doc_support::temp_dir();
# let db = Pigeonhole::open(dir.join("guide.phdb"), Options::default().change_feed(FeedRetention::bytes(64 << 20)))?;
# let store = Store::create(&db, "agents", StoreOptions::new())?;
let policy = ConsolidatePolicy::new().min_age(std::time::Duration::from_secs(3600));
let mut each_hour = store
    .consolidate_loop("main", std::sync::Arc::new(Summarize))
    .policy(&policy);
// In the application's own thread or task: one step reads new memories for up to a second,
// then runs the passes that are due.
let step = each_hour.run_once(std::time::Duration::from_secs(1))?;
assert!(step.passes.is_empty()); // nothing is an hour old yet
# db.close()?;
# Ok(())
# }
```

- **What starts a pass:** a new memory (`Remembered`). A pass's own summaries, merges and `mark_used` don't, so recalling a namespace never triggers its consolidation.
- **Restarts:** the loop keeps its position under its name in the store, including the namespaces still waiting out their `min_age`. A loop started again with that name resumes there. With no saved position, or after its feed history was recycled, it catches up once by scanning for namespaces with unconsolidated memories.
- **One loop per name:** a loop that starts takes the name over, and an older one still running fails its next step with `Error::LoopTakenOver` (code `Conflict`). Passes that race on the same memories conflict group by group, so nothing is consolidated twice.
- **At least once:** a loop stopped between a pass and saving its position runs that pass again, which skips what's already consolidated. A consolidator that fails partway leaves the groups before it committed, and the namespace is retried after `min_age`. One that can't answer for a reason that isn't the group's (it was interrupted, say) can return `ConsolidateStopped` instead: the namespace is then due again at the next step, and `LoopStep::stopped` lists it.
- **Steps:** `run_once(timeout)`'s timeout bounds the wait for new memories (in real time), not the passes. `next_due()` says when the next pass is due, `run(&stop)` loops until a flag is set, and `.clock(..)` replaces the clock `min_age` is measured on, for tests. `interrupter()` gives a handle another thread can use to end a step's wait early: the step then runs what's due, saves and returns.
- **The loop owns what it needs** (a store handle, the consolidator as an `Arc`, its clock), so it can live on a thread of its own.
- **It runs in the writer process,** since passes write. Other processes can still `watch`.

## Reader processes
`Store::open_reader(&reader, name, model_id)` gives a `ReadStore` that recalls, gets and verifies at the reader's snapshots, and can't write. Writes come from the writer process, for example through `phdb mcp`'s memory tools ([Using Pigeonhole from AI agents](ai-agents.md#the-memory-tools)), which serve a store to any MCP client.

## How well it recalls
These numbers come from `crates/bench/examples/memrecall.rs` on the reference AX102 (D311 T5, #1339). The memories and questions come from two public benchmarks, embedded once with `BAAI/bge-small-en-v1.5` (384 dimensions). `crates/bench/baselines/memory/fixture.py` downloads and embeds them, and pins the files' hashes and the model's revision. The published vectors were embedded with sentence-transformers 3.3.1 on transformers 4.57.6. The script now pins 5.6.1 on transformers 5.22.1 (security fixes, #1422), which embeds the same texts to within 3e-7 per component and leaves every query's top 10 unchanged on a 2,000-memory sample, so the numbers below weren't re-run.
- **LongMemEval-S** (Wu et al., 2024): 470 questions, each over its own namespace of about 250 memories, one per user turn and the reply after it. A question counts at `k` when a hit comes from one of its answer sessions.
- **LoCoMo** (Maharana et al., 2024): 1,531 questions over 10 conversations of about 590 turns each. A question counts when a hit is one of its evidence turns.

Recall@5 / recall@10 / NDCG@10, `k = 10`, per signal alone and fused. NDCG ranks the memory hits as returned, and counts each evidence session or turn once, at its first hit. So it's over memories, and may differ from LongMemEval's own session-level NDCG:

| | LongMemEval-S | LoCoMo |
|---|---|---|
| vector | 0.955 / 0.974 / 0.853 | 0.545 / 0.658 / 0.426 |
| keyword | 0.957 / 0.974 / 0.864 | 0.511 / 0.587 / 0.409 |
| recency | 0.047 / 0.109 / 0.045 | 0.003 / 0.011 / 0.004 |
| **fused, default weights** (`recency` 0.25) | 0.964 / 0.981 / 0.871 | 0.583 / 0.658 / 0.454 |
| fused, `recency` at 1 (the default before 1.1.0) | 0.964 / 0.979 / 0.789 | 0.553 / 0.642 / 0.409 |
| fused, `recency` at 0.5 | 0.968 / 0.981 / 0.855 | 0.581 / 0.655 / 0.444 |
| fused, `recency` at 0 | 0.960 / 0.987 / 0.883 | 0.583 / 0.658 / 0.457 |
| fused, `recency` at 1, `expand(Expand::hops(1))` | 0.951 / 0.977 / 0.751 | 0.541 / 0.666 / 0.400 |

- **Fused beats any one signal** at the default weights, on both benchmarks. That's why recency's default is 0.25 and not 1. At 1, recency outweighed what was asked, and ranking quality fell below keyword alone (NDCG 0.789 against 0.864 on LongMemEval-S). Neither benchmark asks about recent events, so recency there is noise, and 0 does slightly better still. But agents' questions often do lean on what happened lately, so the default keeps some pull. Raise it with `Weights` (per call) for an agent that asks mostly about recent events.
- **These runs set `uses` with `recency`,** but neither dataset marks memories used, so the use signal is empty and only recency's weight matters.
- **The graph signal** (measured with `recency` at 1) here follows only each turn's link to the previous turn of its session, the one link every application can write. Recall is about level (+0.008 recall@10 on LoCoMo, about 12 of 1,531 questions), and ranking quality drops on both. Links that carry meaning (entities, `derived_from`) are what it's for.
- **`f16` and `bf16` embeddings** recall the same as `f32`, to within 0.001 on every LongMemEval-S row.
- **Fuzzy keyword terms** (`Fuzziness::Auto` on words of 5 or more letters) don't help. Recall@10 changes by under a point either way, and the keyword search takes 18 to 100 times as long. Recall doesn't take a fuzziness option for that reason.

**Latency,** fused, `k = 10`, warm, one thread, measured with `recency` at 1 (weights don't change the work a recall does). The last two rows have 50 questions, so their "p99" is the slowest of 50:

| namespace size | p50 | p99 | with `expand(hops(1))`, p50 / p99 |
|---|---|---|---|
| ~250 memories (LongMemEval-S) | 2.0 ms | 3.7 ms | 9.9 / 33 ms |
| ~590 (LoCoMo) | 1.3 ms | 4.3 ms | 2.8 / 5.7 ms |
| ~2,400 (LongMemEval-M) | 7.9 ms | 14.6 ms | 15.9 / 37 ms |
| 94,000 in one namespace | 68 ms | 107 ms | |

At 94,000 memories, the exact vector search is 37 ms of that (p50) and the keyword search 28 ms (slowest 66 ms). Both grow with the namespace. Per-user or per-agent namespaces of a few thousand memories stay well under 20 ms. In a single namespace of 100,000, an approximate vector index (IVF, D311 T8) would take the vector part off, but not the keyword part.

**Size,** per 100,000 LongMemEval memories (about 2 KB of text each, 1.1 KB once compressed), in use after a full compaction:

| embeddings | the store | embeddings | the keyword index | the text | other |
|---|---|---|---|---|---|
| `f32` | 962 MB | 159 MB | 637 MB | 116 MB | 50 MB |
| `f16` | 882 MB | 82 MB | 637 MB | 116 MB | 47 MB |
| `bf16` | 879 MB | 82 MB | 637 MB | 116 MB | 44 MB |

- **The keyword index is most of the store** with text this long, about 5.5 times the compressed text. It keeps a posting for every distinct term of every memory (the memory's id, with the term's frequency and the memory's length), plus each memory's own terms so a delete can find its postings.
- **"Other"** is the memories' metadata and use counts, the dedup rows, and what the file's extents hold beyond their data.
- **Half-precision embeddings** save 8%.
