# frozen_string_literal: true

# The public, keyword-taking methods over the native core (whose methods take positional
# arguments and are private, named `native_*`), the block forms and the scan Enumerator.
#
# The block forms are Ruby on purpose (simp724's R2 on #697): a `break`, `throw` or early
# `return` out of a block runs these `ensure` clauses, never unwinding through Rust. A block
# form commits only when its block returns normally (`next` included: it is a normal
# return); any other exit, an exception or a non-local one, discards or rolls back.
module Pigeonhole
  class << self
    # Opens (with `create: true`, creates) the database at `path` as the writer process.
    # With a block, yields the database, closes it when the block exits however it exits,
    # and returns the block's value.
    #
    # `change_feed: n` turns the change feed on (`Table#feed`), keeping up to `n` bytes of
    # history, for at most `feed_age:` seconds if given; `feed_tables:` limits it to those
    # tables. With neither `change_feed:` nor `change_feed_off: true`, an open inherits the
    # last open's feed, so a feed's tokens stay valid across an open that doesn't mention
    # it; `change_feed_off: true` turns it off, which ends its history.
    def open(path, create: false, shards: nil, durability: nil, block_cache: nil, row_cache: nil,
             memtable_budget: nil, shm_dir: nil, read_only_fallback: nil, change_feed: nil,
             change_feed_off: false, feed_age: nil, feed_tables: nil)
      db = native_open(path, create, shards, durability, block_cache, row_cache, memtable_budget,
                       shm_dir, read_only_fallback, change_feed, change_feed_off, feed_age, feed_tables)
      return db unless block_given?

      begin
        yield db
      ensure
        db.close
      end
    end

    # Opens the database at `path` as a reader process of the writer that has it open
    # (`NoWriter` without one). With a block, yields the reader and closes it afterwards.
    def open_reader(path, shm_dir: nil, block_cache: nil, row_cache: nil)
      reader = native_open_reader(path, shm_dir, block_cache, row_cache)
      return reader unless block_given?

      begin
        yield reader
      ensure
        reader.close
      end
    end
  end

  # Yields `handle` to the block and runs `finish(completed)` however the block exits:
  # `completed` is true only when the block returned normally.
  def self.scoped(handle)
    completed = false
    begin
      result = yield handle
      completed = true
      result
    ensure
      if completed
        handle.__send__(:native_commit, nil) if handle.__send__(:native_open?)
      else
        handle.__send__(:native_discard)
      end
    end
  end
  private_class_method :scoped

  # `v` as an Array of keys: an Array as it is, any other Enumerable (a Set) as its `to_a`.
  def self.key_list(v)
    v.is_a?(Enumerable) && !v.is_a?(Array) ? v.to_a : v
  end
  private_class_method :key_list

  # One of a nearest-neighbour search's results: the cell's row and qualifier, its distance
  # from the query (by the family's metric) and its timestamp.
  Neighbour = Data.define(:row, :qualifier, :distance, :ts)

  # A `nearest_within` page (D308): the nearest neighbours that fit the byte budget, nearest
  # first. `bytes` counts each neighbour's row key, qualifier and 16 bytes; `next_bytes`,
  # when the budget left a neighbour out, is that neighbour's size. There's no resume
  # position: ask again with a larger budget or `k`.
  NearestPage = Data.define(:neighbours, :bytes, :next_bytes) do
    # Whether the budget left out a neighbour the search found.
    def cut_by_budget = !next_bytes.nil?

    # The number of neighbours.
    def size = neighbours.size
  end

  # One hop of a traversal: its new nodes, sorted, and with `parents: true` each node's
  # parent as an index into the previous hop's nodes (the traversal's `seeds` for hop 1).
  Hop = Data.define(:nodes, :parents) do
    # The number of nodes.
    def size = nodes.size
  end

  # A traversal's result (D304): its hops in order, the seeds as the engine sorted and
  # deduplicated them (what hop 1's parents index), the frontier rows that had an edge the
  # selection keeps, the edges read, the hops `hop_limit:` or `max_nodes:` cut short, and
  # whether `max_nodes:` or `byte_budget:` stopped it.
  Traversal = Data.define(:seeds, :hops, :rows, :edges, :hops_cut, :cut_by_max_nodes, :cut_by_budget)

  # A `scan_within` page (D308): its rows in key order (a row the budget cut holds the
  # columns that fit); its size (each cell's qualifier, value and 16 bytes, and each row's
  # key once; never more than the budget); whether the budget stopped the read; the size of
  # the first column that didn't fit; and where to resume (`after:`), nil once the scan is
  # exhausted.
  ScanPage = Data.define(:rows, :bytes, :cut_by_budget, :next_bytes, :resume) do
    # The number of rows.
    def size = rows.size
  end

  # One whole commit from a change feed: its sequence number (the order commits arrive in,
  # as `Snapshot#seqno` counts), its commit timestamp (microseconds since the Unix epoch)
  # and its changes in the feed's tables and row prefix, in the order the commit wrote them.
  Commit = Data.define(:seqno, :timestamp, :changes) do
    # The number of changes.
    def size = changes.size
  end

  # One mutation of a commit: its table and family names, its kind (`:put`, `:merge` for a
  # merge operand, a counter increment or an aggregate input, `:delete_cell`,
  # `:delete_column`, or `:delete_family`, which a row delete is one of per family), its row
  # key and qualifier (empty for `:delete_family`), its timestamp, and the value written or
  # merged, typed as `Cell#typed` types it (nil for deletes). Its Strings are frozen, as it
  # is. A kind this build doesn't know (one a later engine adds) arrives as `:unknown`, so a
  # `case` on kinds should have an `else`.
  Change = Data.define(:table, :family, :kind, :row, :qualifier, :timestamp, :value)

  # A `multi_get_within` page (D308), as `ScanPage` describes; its rows are in key order,
  # not aligned with the keys given.
  KeysPage = Data.define(:rows, :bytes, :cut_by_budget, :next_bytes, :resume) do
    # The number of rows.
    def size = rows.size
  end

  # The open database, as the writer process.
  class Database
    # A consistent view of everything committed (with `durable: true`, of what is durable
    # at its level). With a block, yields it and releases it afterwards.
    def snapshot(durable: false)
      s = native_snapshot(durable)
      return s unless block_given?

      begin
        yield s
      ensure
        s.release
      end
    end

    # A batch of writes to any rows and tables, committed atomically by `commit`. With a
    # block, yields it, commits it when the block returns normally (`next` included) and
    # discards it on an exception or any non-local exit (`break`, `throw`, `return`);
    # returns the block's value.
    def write_batch(&)
      b = native_write_batch
      return b unless block_given?

      Pigeonhole.__send__(:scoped, b, &)
    end

    # An optimistic transaction: reads see one snapshot, and `commit` raises `Conflict` if a
    # cell it read changed since. With a block, yields it, commits it when the block returns
    # normally and rolls it back otherwise; returns the block's value.
    def transaction(&)
      t = native_transaction
      return t unless block_given?

      Pigeonhole.__send__(:scoped, t, &)
    end

    # Runs the block with a fresh transaction and commits it, retrying on `Conflict` and
    # `Busy` as the library's `RetryPolicy` does: with backoff, up to `max_attempts`
    # (default 8) or `deadline` seconds. Returns the block's value. The block may run more
    # than once, so it should have no effects outside the transaction. Any other exception,
    # or a non-local exit, rolls the attempt back and ends the call.
    def transact(max_attempts: nil, deadline: nil, durability: nil)
      raise ArgumentError, "transact needs a block" unless block_given?

      Pigeonhole.__send__(:native_retry_check, max_attempts, deadline)
      started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
      attempt = 1
      while true # rubocop:disable Style/InfiniteLoop -- `loop` would swallow StopIteration
        txn = native_transaction
        pause = nil
        begin
          result = yield txn
          txn.commit(durability: durability)
          return result
        rescue Conflict, Busy => e
          elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started
          pause = Pigeonhole.__send__(:native_retry_pause, max_attempts, deadline, e.code_num,
                                      attempt, elapsed)
          raise if pause.nil?
        ensure
          txn.__send__(:native_discard)
        end
        sleep(pause)
        attempt += 1
      end
    end
  end

  # A database opened as a reader process: it sees what the running writer process commits,
  # and can't write.
  class Reader
    # A consistent view of everything the writer committed (with `durable: true`, of what is
    # durable at its level). With a block, yields it and releases it afterwards.
    def snapshot(durable: false)
      s = native_snapshot(durable)
      return s unless block_given?

      begin
        yield s
      ensure
        s.release
      end
    end
  end

  # Builds a table: add families, then `create`, `create_if_missing` or `open`.
  class TableBuilder
    # Adds the family `name` (`options`: a `Family`, or nil for a plain one). Returns self.
    def family(name, options = nil)
      native_family(name, options)
      self
    end
  end

  # A table: reads, and with the writer's handle, writes.
  class Table
    # The newest version of one cell, or nil.
    def get(row, family, qualifier, snapshot: nil)
      native_get(row, family, qualifier, snapshot)
    end

    # One row's cells, or nil when the row has none the read selects. `families` selects
    # families (and their order), `qualifier_prefix` columns, `versions` the versions per
    # column (0 for all), and `column_limit` at most that many columns per family.
    def row(row, families: nil, qualifier_prefix: nil, versions: 1, column_limit: nil, snapshot: nil)
      native_row(row, families, qualifier_prefix, versions, column_limit, snapshot)
    end

    # Rows in key order, as an Enumerator: those starting with `prefix`, or from `start`
    # (inclusive) to `end` (exclusive), or all. Rows are read `chunk` at a time with the
    # GVL released.
    #
    # Each enumeration (`each`, the first `next`, each `rewind`) reads at one snapshot: the
    # given `snapshot:`, or one it takes when it starts (simp724's R3), so its rows are
    # consistent however lazily they are consumed. An external enumeration (`next`) that is
    # abandoned holds its snapshot until the Enumerator is garbage collected.
    def scan(prefix = nil, start: nil, end: nil, families: nil, qualifier_prefix: nil, versions: 1,
             limit: nil, column_limit: nil, snapshot: nil, chunk: 256)
      spec = native_scan_spec(prefix, start, binding.local_variable_get(:end), families,
                              qualifier_prefix, versions, limit, column_limit, chunk)
      Enumerator.new do |y|
        cursor = spec.open(snapshot)
        begin
          until (rows = cursor.next_chunk).empty?
            rows.each { |r| y << r }
          end
        ensure
          cursor.close
        end
      end
    end

    # The rows of `keys` (D299), aligned with `keys`: one entry per key given, in that
    # order, nil for a key whose row has no cell the read keeps, a repeated key repeated.
    # The read itself is one multi-get in key order, with the GVL released.
    def multi_get(keys, families: nil, qualifier_prefix: nil, versions: 1, column_limit: nil,
                  snapshot: nil)
      native_multi_get(Pigeonhole.__send__(:key_list, keys), families, qualifier_prefix, versions,
                       column_limit, snapshot)
    end

    # A page of `multi_get`'s cells, at most `bytes` long (D308), as a `KeysPage`; `after:`
    # resumes where an earlier page stopped (its `resume`). The page's position names the
    # last key returned, so keys added or removed between pages resume at the right key.
    def multi_get_within(keys, bytes:, families: nil, qualifier_prefix: nil, versions: 1,
                         column_limit: nil, snapshot: nil, after: nil)
      rows, *rest = native_multi_get_within(Pigeonhole.__send__(:key_list, keys), bytes, families,
                                            qualifier_prefix, versions, column_limit, snapshot, after)
      KeysPage.new(rows.freeze, *rest)
    end

    # A page of `scan`'s rows, at most `bytes` long (D308), as a `ScanPage`; `after:` resumes
    # where an earlier page stopped (its `resume`). Without a `snapshot:`, each page of a
    # chain reads the latest data.
    def scan_within(prefix = nil, bytes:, start: nil, end: nil, families: nil, qualifier_prefix: nil,
                    versions: 1, limit: nil, column_limit: nil, snapshot: nil, after: nil)
      rows, *rest = native_scan_within(bytes, prefix, start, binding.local_variable_get(:end), families,
                                       qualifier_prefix, versions, limit, column_limit, snapshot, after)
      ScanPage.new(rows.freeze, *rest)
    end

    # A breadth-first traversal (D304) over `family`'s edges (each qualifier names a
    # neighbour row) from `seeds`, `hops` deep, as a `Traversal`. `per_node_limit:` caps the
    # edges read per node, `hop_limit:` the nodes per hop, `max_nodes:` the nodes in all and
    # `byte_budget:` the bytes read; `parents: true` records each node's parent.
    def traverse(seeds, family, hops: 1, per_node_limit: nil, hop_limit: nil, max_nodes: nil,
                 byte_budget: nil, parents: false, qualifier_prefix: nil, snapshot: nil, parallel: nil)
      s, hs, *rest = native_traverse(Pigeonhole.__send__(:key_list, seeds), family, hops,
                                     per_node_limit, hop_limit, max_nodes, byte_budget, parents,
                                     qualifier_prefix, snapshot, parallel)
      Traversal.new(s.freeze, hs.map { |n, p| Hop.new(n.freeze, p&.freeze) }.freeze, *rest)
    end

    # A wide row's pages (D297), at most `size` cells each (a column's versions never split),
    # as a `Pages` that reads each page when it is asked for; `after:` resumes after an
    # earlier walk's `position`.
    def pages(row, size, families: nil, qualifier_prefix: nil, versions: 1, snapshot: nil, after: nil)
      native_pages(row, size, families, qualifier_prefix, versions, snapshot, after)
    end

    # The `k` cells of vector family `family` nearest `query` (D296), as `Neighbour`s in
    # (distance, row, qualifier) order: over `prefix:`, or `start:` to `end:`, or the rows of
    # `keys:`, or the whole table. `query` is read by the family's element type, as
    # `put_vector` reads a vector.
    def nearest(family, query, k, prefix: nil, start: nil, end: nil, keys: nil, qualifier_prefix: nil,
                snapshot: nil, parallel: nil)
      native_nearest(family, query, k, prefix, start, binding.local_variable_get(:end),
                     keys && Pigeonhole.__send__(:key_list, keys), qualifier_prefix, snapshot,
                     parallel, nil).map { |a| Neighbour.new(*a) }.freeze
    end

    # `nearest`'s neighbours, nearest first, that fit `bytes` (D308), as a `NearestPage`. The
    # budget bounds the result, not the work: the search reads and ranks as `nearest` does.
    def nearest_within(family, query, k, bytes:, prefix: nil, start: nil, end: nil, keys: nil,
                       qualifier_prefix: nil, snapshot: nil, parallel: nil)
      found, size, next_bytes = native_nearest(family, query, k, prefix, start,
                                               binding.local_variable_get(:end),
                                               keys && Pigeonhole.__send__(:key_list, keys),
                                               qualifier_prefix, snapshot, parallel, bytes)
      NearestPage.new(found.map { |a| Neighbour.new(*a) }.freeze, size, next_bytes)
    end

    # Subscribes to this table's committed changes (the change feed, D310): a `Feed` of
    # whole commits, once each, in commit order. The database's change feed must be on, set
    # with `change_feed:` or inherited from the last open, and follow this table. A reader
    # process's table feeds too, from the running writer's history. With a block, yields the
    # feed, closes it afterwards and returns the block's value.
    #
    # `tables:` adds more tables to the same feed (a commit across them arrives whole),
    # `prefix:` keeps only changes to rows under it, and `visible: true` delivers a commit as
    # soon as reads see it rather than once it is durable. The feed starts after this call,
    # or after the commits `from_snapshot:` sees, or at the oldest history kept
    # (`from_oldest: true`), or after a stored `resume:` token (`Feed#token`).
    def feed(tables: nil, prefix: nil, visible: false, from_snapshot: nil, from_oldest: false, resume: nil)
      f = native_feed(tables, prefix, visible, from_snapshot, from_oldest, resume)
      return f unless block_given?

      begin
        yield f
      ensure
        f.close
      end
    end

    # Puts one cell and commits it; returns the commit's sequence number. A String is bytes,
    # an Integer an i64 and a Float an f64.
    def put(row, family, qualifier, value, ts: nil, durability: nil)
      native_put(row, family, qualifier, value, ts, durability)
    end

    # Adds `delta` to a counter cell and commits it; returns the commit's sequence number.
    def incr(row, family, qualifier, delta, ts: nil, durability: nil)
      native_incr(row, family, qualifier, delta, ts, durability)
    end

    # Adds `delta` to a counter cell, commits it, and returns the counter's value after the
    # commit: concurrent callers adding 1 from zero get exactly 1..N (D314). `nil` only for a
    # bucket (`ts:`) already past the family's TTL. A failed commit raises its usual
    # exception; ReturnedValueError means the commit applied but reading the value failed, so
    # the increment landed: don't retry it.
    def incr_returning(row, family, qualifier, delta, ts: nil, durability: nil)
      native_incr_returning(row, family, qualifier, delta, ts, durability)
    end

    # Deletes `row`; with `family`, that family; with `qualifier` too, the column; with `ts:`
    # too, that one version. Returns the commit's sequence number.
    def delete(row, family = nil, qualifier = nil, ts: nil, durability: nil)
      native_delete(row, family, qualifier, ts, durability)
    end
  end

  # Writes to one row, committed atomically by `commit` or `commit_if`. Each write method
  # returns self.
  class Mutation
    # Puts a cell: a String (bytes), an Integer (i64) or a Float (f64).
    def put(family, qualifier, value, ts: nil)
      native_put(family, qualifier, value, ts)
      self
    end

    # Puts a cell holding an unsigned 64-bit Integer.
    def put_u64(family, qualifier, value, ts: nil)
      native_put_u64(family, qualifier, value, ts)
      self
    end

    # Adds `delta` to a counter cell.
    def incr(family, qualifier, delta, ts: nil)
      native_incr(family, qualifier, delta, ts)
      self
    end

    # Adds an input to an aggregate family's cell, read by the family's kind.
    def aggregate(family, qualifier, value, ts: nil)
      native_aggregate(family, qualifier, value, ts)
      self
    end

    # Puts a vector in a vector family, read by the family's element type: an Array of
    # Numerics (`:f32`, `:f16`, `:bf16`), an Array of Integers (`:i8`), or the packed bits as
    # a String (`:bit`).
    def put_vector(family, qualifier, vector, ts: nil)
      native_put_vector(family, qualifier, vector, ts)
      self
    end

    # Deletes the row; with `family`, that family; with `qualifier` too, the column; with
    # `ts:` too, that one version.
    def delete(family = nil, qualifier = nil, ts: nil)
      native_delete(family, qualifier, ts)
      self
    end

    # Sets the commit's durability (`:none`, `:buffered`, `:group_sync` or `:sync`).
    def durability(durability)
      native_durability(durability)
      self
    end

    # Commits like `commit`, and returns a frozen Incremented: each counter cell the writes
    # incremented or set, as a read at the commit sees it (D314). Concurrent increments of one
    # cell each get their own value.
    def commit_returning = native_commit_returning.freeze
  end

  # The counter cells a Mutation#commit_returning incremented or set, after its commit.
  class Incremented
    # The counter (or with `ts:`, the bucket) in `family` at `qualifier`, after the commit;
    # nil when a read at the commit sees no cell (one the mutation deleted, or a bucket
    # already past the family's TTL). Raises InvalidArgument if the commit didn't write that
    # cell, and ReturnedValueError if the value couldn't be read after the commit applied: the
    # commit succeeded then, so don't retry it. `to_h` raises it too, for the first unreadable
    # value.
    def value(family, qualifier, ts: nil)
      native_value(family, qualifier, ts)
    end
  end

  # The write methods `WriteBatch` and `Transaction` share: the table and row first; each
  # returns self.
  [WriteBatch, Transaction].each do |klass|
    klass.class_eval do
      # Puts a cell in `table`'s `row`: a String (bytes), an Integer (i64) or a Float (f64).
      def put(table, row, family, qualifier, value, ts: nil)
        native_put(table, row, family, qualifier, value, ts)
        self
      end

      # Puts a cell holding an unsigned 64-bit Integer.
      def put_u64(table, row, family, qualifier, value, ts: nil)
        native_put_u64(table, row, family, qualifier, value, ts)
        self
      end

      # Adds `delta` to a counter cell.
      def incr(table, row, family, qualifier, delta, ts: nil)
        native_incr(table, row, family, qualifier, delta, ts)
        self
      end

      # Adds an input to an aggregate family's cell, read by the family's kind.
      def aggregate(table, row, family, qualifier, value, ts: nil)
        native_aggregate(table, row, family, qualifier, value, ts)
        self
      end

      # Puts a vector in a vector family, read by the family's element type (as
      # `Mutation#put_vector` reads it).
      def put_vector(table, row, family, qualifier, vector, ts: nil)
        native_put_vector(table, row, family, qualifier, vector, ts)
        self
      end

      # Deletes `row`; with `family`, that family; with `qualifier` too, the column; with
      # `ts:` too, that one version.
      def delete(table, row, family = nil, qualifier = nil, ts: nil)
        native_delete(table, row, family, qualifier, ts)
        self
      end

      # Commits; returns the commit's sequence number. A transaction raises `Conflict` if a
      # cell or family it read changed since its snapshot.
      def commit(durability: nil)
        native_commit(durability)
      end
    end
  end

  # A row: its key and cells, by family (in the read's family order), then qualifier, then
  # newest version first. Enumerable over `[family, qualifier, cell]`.
  class Row
    include Enumerable

    # Yields each `[family, qualifier, cell]`; without a block, an Enumerator.
    def each(&)
      return enum_for(:each) { size } unless block_given?

      cells.each(&)
      self
    end
  end

  # A subscription to some tables' committed changes (`Table#feed`). Each wait releases the
  # GVL in short slices with an interrupt check between them, so `Thread#raise`, `Timeout`
  # and Ctrl-C reach a consumer blocked in `next`. Like `Pages`, it's a cursor: every Enumerable
  # method consumes the commits it reads. A live feed has no end, so the methods that read to
  # the end (`to_a`, `map`, `count`, `select`) wait for ever; `first(n)`, `take(n)` and `find`
  # return. To drain what's there, `feed.each(timeout: 1).to_a`.
  class Feed
    include Enumerable

    # The next `Commit`, waiting up to `timeout` seconds (nil waits until one comes); nil if
    # none came in time. Raises `FeedExpired` once the history the feed needs is gone, and
    # from then on.
    def next(timeout: nil)
      to_commit(native_next(timeout))
    end

    # The next `Commit` if one is ready, else nil. Never waits.
    def try_next
      to_commit(native_try_next)
    end

    # Yields each commit as it comes; without `timeout:` it never ends (`break` out), and
    # with it, it stops once a commit doesn't come in time. Without a block, an Enumerator.
    def each(timeout: nil)
      return enum_for(:each, timeout: timeout) unless block_given?

      while (c = self.next(timeout: timeout))
        yield c
      end
      self
    end

    private

    # A commit's Array from the native layer as a `Commit`.
    def to_commit(a)
      return nil if a.nil?

      seqno, timestamp, changes = a
      Commit.new(seqno, timestamp, changes.map { |c| Change.new(*c) }.freeze)
    end
  end

  # A wide row's pages (`Table#pages`), each read when it is asked for: the walk holds no
  # read open between pages. Enumerable over the pages (each a `Row`).
  #
  # **A walk is a cursor, not a collection.** Every Enumerable method consumes the pages it
  # reads: after `first`, a `to_a` returns only the pages after the first one, and a second
  # `each` yields only what's left. To read the row again, call `Table#pages` again (with
  # `after: position` to resume from where a walk stopped).
  class Pages
    include Enumerable

    # Yields each page that is left; without a block, an Enumerator.
    def each
      return enum_for(:each) unless block_given?

      while (page = native_next)
        yield page
      end
      self
    end
  end

  # A column family's options.
  class Family
    class << self
      # A plain family. `max_versions`: versions kept per column (0 keeps all); `ttl`:
      # cells expire this many seconds after their timestamp; `compression`: `:lz4` (the
      # default), `:zstd` or `:none`, with `zstd_level` for zstd.
      def new(max_versions: nil, ttl: nil, bloom_bits: nil, block_size: nil, blob_threshold: nil,
              compression: nil, zstd_level: nil)
        native_new(max_versions, ttl, bloom_bits, block_size, blob_threshold, compression, zstd_level)
      end

      # A counter family: `incr` adds to its cells.
      def counter(max_versions: nil, ttl: nil)
        native_counter(max_versions, ttl)
      end

      # A vector family (D303): `dim` elements of `element` (`:f32`, `:i8`, `:bit`, `:f16` or
      # `:bf16`), compared by `metric` (`:l2`, `:cosine`, `:inner_product` or `:hamming`).
      # Its cells are written with `put_vector`.
      def vector(dim, element, metric, max_versions: nil, ttl: nil)
        native_vector(dim, element, metric, max_versions, ttl)
      end

      # An aggregate family of `kind`: `:sum`, `:min`, `:max`, `:hll`, `:bit_or`, `:bit_and`,
      # `:bit_xor`, `:min_f64`, `:max_f64`, `:sum_f64`, `:mean_var`, `:histogram` (with
      # `bounds:`), `:quantiles` (with optional `mantissa_bits:` and `max_buckets:`) or
      # `:bloom` (with `bits:` and `hashes:`).
      def aggregate(kind, bounds: nil, mantissa_bits: nil, max_buckets: nil, bits: nil, hashes: nil,
                    max_versions: nil, ttl: nil)
        native_aggregate(kind, bounds, mantissa_bits, max_buckets, bits, hashes, max_versions, ttl)
      end
    end
  end
end
