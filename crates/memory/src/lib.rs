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
//! Recall (vector, keyword and recency, fused by RRF within a byte budget) and reader
//! processes come in T1's second PR.

mod error;
mod layout;
mod store;

pub use error::{Error, Result};
pub use layout::{MemoryId, normalize};
pub use store::{
    EmbeddingModel, Kind, Link, LinkType, Marked, Memory, Remembered, Source, Store, StoreOptions,
    StoreStats, StoredMemory,
};
