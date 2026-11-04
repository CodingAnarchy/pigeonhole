//! Long-term memory for agents, with hybrid recall, on Pigeonhole (D311, #1251, ICR 0107).
//!
//! A [`Store`] is one table plus a [`pigeonhole_text`] index over its memories' text. Each
//! memory lives in a **namespace** (an exact, flat string such as `agent:alice`), carries
//! its text, an optional embedding from the store's pinned model, a kind, an importance,
//! sources, a session, and typed links to other memories.
//!
//! - **Writes are transactions:** a memory, its text index entry, its dedup row and both
//!   directions of its links commit together ([`Store::remember`], [`Store::remember_in`]).
//! - **Exact dedup:** the same text (after trimming and collapsing whitespace) merges into
//!   the live memory that has it ([`Remembered::Merged`]).
//! - **The embedding model is pinned** at create ([`EmbeddingModel`]): a handle names the
//!   model it embeds with, and another model's vectors are refused.
//! - **[`Store::verify`]** checks the store against itself and its text index.
//!
//! - **Recall** ([`Store::recall`]) fuses vector, keyword and recency candidates by RRF
//!   (or a linear blend) within a byte budget, at one snapshot, and works in reader
//!   processes ([`Store::open_reader`], [`ReadStore`]).

mod consolidate;
mod dedup;
mod error;
mod feed_loop;
mod forget;
/// Test seams (ICR 0085's convention): only with the `__unstable-hooks` feature, hidden,
/// unstable, and never needed by applications.
#[cfg(feature = "__unstable-hooks")]
#[doc(hidden)]
pub mod hooks;
mod ivf;
mod layout;
mod recall;
mod store;
mod watch;

pub use consolidate::{
    Consolidate, ConsolidatePass, ConsolidatePolicy, ConsolidateStopped, Consolidator, SourcePolicy,
};
pub use dedup::{DedupPolicy, NearDedup};
pub use error::{Error, Result};
pub use feed_loop::{ConsolidateLoop, LoopInterrupter, LoopStep};
pub use forget::{Forget, ForgetPass, ForgetPolicy, Forgetter, Forgot};
pub use ivf::IvfOptions;
pub use layout::{MemoryId, normalize};
/// The text index's Unicode normalisation ([`StoreOptions::normalization`]), re-exported from
/// `pigeonhole-text`.
pub use pigeonhole_text::Normalization;
pub use recall::{Expand, Fusion, Recall, RecallHit, RecallStats, Signal, Signals, Weights};
pub use store::{
    EmbeddingModel, Kind, Link, LinkType, Marked, Memory, OpenOptions, ReadStore, Remembered,
    Source, Store, StoreOptions, StoreStats, StoredMemory,
};
pub use watch::{MemoryCommit, MemoryEvent, PASS_OVER_PER_POLL, Watch, WatchBuilder, WatchToken};

#[cfg(doctest)]
mod guide {
    #[doc = include_str!("../../../docs/guide/agent-memory.md")]
    struct AgentMemory;
    #[doc = include_str!("../README.md")]
    struct CrateReadme;
}
