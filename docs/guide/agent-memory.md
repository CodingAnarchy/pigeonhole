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
- **The keyword index's normalisation:** `StoreOptions::normalization(Normalization::Nfkc)` makes keyword recall match ligatures and full-width forms (`ﬁle`, `ｆｉｌｅ`) against plain text, for memories gathered from PDFs, OCR or East Asian sources. The default, `Nfc`, only unifies different encodings of the same character. It's fixed at create, and `Store::normalization()` reads it back. The full-text guide's [which normalisation to pick](text-search.md#options) has the trade-offs.

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

## Reader processes
`Store::open_reader(&reader, name, model_id)` gives a `ReadStore` that recalls, gets and verifies at the reader's snapshots, and can't write. Writes come from the writer process, for example through `phdb mcp`'s memory tools ([Using Pigeonhole from AI agents](ai-agents.md#the-memory-tools)), which serve a store to any MCP client.
