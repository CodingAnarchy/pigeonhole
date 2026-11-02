//! Full-text search with BM25 ranking for Pigeonhole, built on its public API only (decision
//! D309, #1247; API in ICR 0105).
//!
//! An index is one table of ordinary rows over one **source column** (a table, family and
//! qualifier), with each source row a document:
//! - each term's posting list is one wide row, `P{term}`, with one column per document
//!   holding its term frequency and length, plus, with positions on, one more holding its
//!   positions;
//! - each document's forward row, `F{doc}`, holds its terms, so a later write knows what to
//!   remove;
//! - counters hold each term's document frequency and the corpus statistics.
//!
//! The application writes the source column **through the index** ([`Index::put_in`], in
//! its own transaction), so the text, its postings and the statistics commit together. A
//! search at a snapshot never returns a document whose committed text it doesn't match,
//! and never misses one.
//!
//! A [`Query`] is a disjunction (OR) of terms, phrases and text, scored by BM25 with
//! Lucene's idf, evaluated exactly at one snapshot.
//!
//! ```
//! use pigeonhole::{Family, Options, Pigeonhole, RetryPolicy};
//! use pigeonhole_text::{Index, IndexOptions, Source};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let dir = pigeonhole::doc_support::temp_dir();
//! let db = Pigeonhole::open(dir.join("app.phdb"), Options::default().shards(2))?;
//! let docs = db.table("docs")?.family("body", Family::default()).create_if_missing()?;
//! let index = Index::create(&db, "docs_text", Source::new(&docs, "body", b"text"), IndexOptions::new())?;
//!
//! db.transact(&RetryPolicy::default(), |txn| {
//!     txn.put(&docs, b"doc:42", "body", b"title", b"Postgres choice");
//!     index.put_in(txn, b"doc:42", "We chose Postgres in the March meeting")?;
//!     Ok::<_, pigeonhole_text::Error>(())
//! })?;
//! index.put(b"doc:43", "The meeting moved to April")?;
//!
//! // `doc:42` matches every clause; `doc:43` only `meeting`.
//! let hits = index.search(r#"postgres "march meeting" meeting"#, 10).run()?;
//! assert_eq!(hits[0].id, b"doc:42");
//! assert_eq!(hits.len(), 2);
//! # db.close()?;
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]

mod error;
mod index;
mod layout;
mod query;
mod search;
mod tokenizer;

pub use error::{Error, QueryErrorKind, Result};
pub use index::{Index, IndexStats, RebuildStats};
pub use layout::{IndexOptions, Normalization, Source};
pub use query::{Clause, Fuzziness, Query};
pub use search::{Hit, Search, SearchStats};
pub use tokenizer::{Token, Tokenizer};
