### Added
- **`phdb serve`: the BM25 text index** (`Text.CreateIndex`, `Put`, `Delete`, `Search`, `Rebuild`, `Verify`; D313, ICR 0105 and 0112, #1397), over gRPC and HTTP/JSON. `Search` takes the query syntax or clauses in order, with highlights by clause. A rebuild runs on a thread of its own, one at a time.
- **`pigeonhole-text`: `Query::quoted_phrase(text, slop)`**, a phrase given as text and tokenised as documents are: exactly the syntax's `"..."~N` (ICR 0105 amendment).
