//! Local text embeddings for [`pigeonhole-memory`](pigeonhole_memory): a pinned open model
//! run on the CPU, in-process (D318, ICR 0119, #1345).
//!
//! A [`LocalEmbedder`] loads a [`Model`]'s weights once, then embeds text with no network and
//! no service: [`embed_passages`](LocalEmbedder::embed_passages) for what a store remembers,
//! [`embed_query`](LocalEmbedder::embed_query) for what it recalls by, and
//! [`embedding_model`](LocalEmbedder::embedding_model) for the record a store pins when it's
//! created.
//!
//! ```no_run
//! use pigeonhole_embed::{LoadOptions, LocalEmbedder, Model};
//! use pigeonhole_memory::{Memory, Store, StoreOptions};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let dir = pigeonhole::doc_support::temp_dir();
//! # let db = pigeonhole::Pigeonhole::open(dir.join("db.phdb"), pigeonhole::Options::default())?;
//! let embedder = LocalEmbedder::open(Model::BgeSmallEnV15, &LoadOptions::new())?;
//! let store = Store::create(&db, "agents", StoreOptions::new().embedding(embedder.embedding_model()))?;
//!
//! let text = "Dana prefers tabs";
//! let vector = embedder.embed_passages(&[text])?.remove(0);
//! store.remember("dana", Memory::new(text).embedding(vector))?;
//!
//! let query = embedder.embed_query("what does Dana prefer?")?;
//! let hits = store.recall(&["dana"]).embedding(query).k(5).run()?;
//! # let _ = hits;
//! # Ok(())
//! # }
//! ```
//!
//! # The model's files
//! The weights aren't in the crate. A model's three files (`model.safetensors`,
//! `tokenizer.json`, `config.json`) come from:
//! - **[`LoadOptions::dir`]**, a directory holding them, for an air-gapped install; or
//! - **the cache**: [`LoadOptions::cache_dir`], else `$PIGEONHOLE_EMBED_CACHE`, else the
//!   operating system's cache directory (`$XDG_CACHE_HOME` or `~/.cache` on Linux,
//!   `~/Library/Caches` on macOS, `%LOCALAPPDATA%` on Windows), under
//!   `pigeonhole/models/<model>@<revision>/`. A file missing from the cache is downloaded once
//!   from the model's pinned revision on huggingface.co (the `download` feature, on by
//!   default), unless [`LoadOptions::offline`] or `PIGEONHOLE_EMBED_OFFLINE=1` says not to.
//!
//! **Every file is checked against a SHA-256 pinned in this crate** before it's used, wherever
//! it came from, so a load either runs exactly the model its [`EmbeddingModel`] id names or
//! fails with [`Error::Checksum`]. A download is written to a temporary file in the cache
//! directory and renamed into place only once it verifies.
//!
//! # Cost
//! Inference is CPU only, on a thread pool of the embedder's own
//! ([`LoadOptions::threads`], by default `min(available parallelism, 4)`), so it never takes
//! the global rayon pool from the application. Texts are embedded in batches of 32.

#![forbid(unsafe_code)]

mod embedder;
mod error;
mod files;
mod model;

pub use embedder::{LoadOptions, LocalEmbedder};
pub use error::{Error, Result};
pub use model::Model;

#[doc(no_inline)]
pub use pigeonhole_memory::EmbeddingModel;

#[cfg(doctest)]
mod readme {
    #[doc = include_str!("../README.md")]
    struct CrateReadme;
}
