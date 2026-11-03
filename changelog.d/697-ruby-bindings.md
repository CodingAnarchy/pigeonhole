### Added
- Ruby bindings, the `pigeonhole` gem (Ruby 3.3 and later), PR 1: the core API over magnus and rb-sys (#697, ICR 0111).
  - **The surface:** `Pigeonhole.open`/`open_reader`, tables, reads and scans (an `Enumerator` at one snapshot), mutations, write batches, transactions and `transact`, cells and rows, families and conditions, and one exception class per error code.
  - **Blocks** scope handles, snapshots, batches and transactions.
  - **The GVL** is released during engine calls.
  - **Packaging:** CI builds native gems as artifacts; nothing is published to RubyGems yet.
