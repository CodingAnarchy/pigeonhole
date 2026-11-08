# pigeonhole-embed

Local text embeddings for [`pigeonhole-memory`](https://crates.io/crates/pigeonhole-memory): a pinned open model run on the CPU in your process, with no embedding service and no network after the first load (D318).

```rust,no_run
use pigeonhole::{Options, Pigeonhole};
use pigeonhole_embed::{Element, LoadOptions, LocalEmbedder, Model};
use pigeonhole_memory::{Memory, Store, StoreOptions};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
# let dir = pigeonhole::doc_support::temp_dir();
let db = Pigeonhole::open(dir.join("app.phdb"), Options::default())?;
// The first open downloads the model's files once (133 MB) and checks their SHA-256s.
let embedder = LocalEmbedder::open(Model::BgeSmallEnV15, &LoadOptions::new())?;
let store = Store::create(&db, "agents", StoreOptions::new().embedding(embedder.embedding_model(Element::F32)?))?;

let text = "The March meeting chose Postgres.";
store.remember("agent:alice", Memory::new(text).embedding(embedder.embed_passages(&[text])?.remove(0)))?;
let hits = store
    .recall(&["agent:alice"])
    .embedding(embedder.embed_query("which database did we pick?")?)
    .k(5)
    .run()?;
# let _ = hits;
# Ok(())
# }
```

- **The model:** [`BAAI/bge-small-en-v1.5`](https://huggingface.co/BAAI/bge-small-en-v1.5) at a pinned revision: 384 dimensions, CLS pooling, unit length. Its vectors match the reference (sentence-transformers) to within 1e-4 per component. A store records the id `BAAI/bge-small-en-v1.5@5c38ec7c` and refuses vectors from any other model.
- **The files** come from the cache (downloaded on first use; the `download` feature, on by default), from a directory you fill (`LoadOptions::dir`), or offline only (`LoadOptions::offline`, `PIGEONHOLE_EMBED_OFFLINE=1`). Every file is checked against a SHA-256 pinned in this crate.
- **CPU only**, pure Rust (candle and tokenizers, no C or C++), on a thread pool of the embedder's own.

The choices between this, an embedding endpoint (`phdb mcp --embed-url`) and client-supplied vectors, and the CPU cost, are in the [agent memory guide](https://github.com/CodingAnarchy/pigeonhole/blob/main/docs/guide/agent-memory.md#local-embeddings).

**Versions:** this crate has its own 0.N line, outside `pigeonhole`'s 1.x line, until its API has been used for a while (D318).

**The model's licence:** bge-small-en-v1.5 is MIT-licensed, by the Beijing Academy of Artificial Intelligence (BAAI). This crate doesn't redistribute its weights: they're fetched from Hugging Face.

License: MIT.
