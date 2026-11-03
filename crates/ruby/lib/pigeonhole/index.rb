# frozen_string_literal: true

module Pigeonhole
  # Full-text search (D309, ICR 0105): a BM25 index over one cell of a table's rows, with
  # phrases, fuzzy terms and highlights. It's the `pigeonhole-text` crate; see the guide's
  # "Full-text search" section. Custom tokenizers aren't available from Ruby.
  module Text
    # A search hit: the document's id, its BM25 score, its text (with `highlight: true`) and
    # the matches in it. A match's `range` is a byte range of `text`: take it with
    # `text.byteslice(range)`, since `text[range]` counts characters.
    Hit = Data.define(:id, :score, :text, :matches)

    # Where a clause of the query matched: the clause's index, which occurrence, and the
    # byte range in the hit's text.
    Match = Data.define(:clause, :occurrence, :range)

    # How a fuzzy clause expanded: its clause index, the term, how many terms it matched, and
    # whether `max_expansions` cut it.
    FuzzyStats = Data.define(:clause, :term, :expansions, :cut)

    # What a search read: each fuzzy clause's expansion, and `stale` documents skipped.
    SearchStats = Data.define(:fuzzy, :stale)

    # An index's options, fixed at create.
    IndexOptions = Data.define(:k1, :b, :positions, :stop_words, :normalization, :max_terms, :max_term_bytes)

    # The cell an index covers: its table, family and qualifier.
    Source = Data.define(:table, :family, :qualifier)

    # `verify`'s counts.
    IndexStats = Data.define(:documents, :terms, :postings, :forward_mismatch, :source_mismatch,
                             :counter_drift, :invalid_utf8, :consistent) do
      # Whether the index and its source agree.
      def consistent? = consistent
    end

    # What `rebuild` or `repair` did.
    RebuildStats = Data.define(:documents, :changed, :invalid_utf8)

    # A query, built clause by clause (each method returns a new query), or parsed from the
    # query syntax with `Query.parse`. A String passed to `search` is parsed too.
    class Query
      # A phrase: `words` in order, with up to `slop` other words between them.
      def phrase(words, slop: 0) = native_phrase(words, slop)

      # A fuzzy term, within `fuzziness` edits: `:auto` (by the term's length), 1 or 2.
      def fuzzy(term, fuzziness = :auto) = native_fuzzy(term, fuzziness)
    end

    # The search methods `Index` and `ReadIndex` share.
    module Searches
      # The `k` best documents for `query` (a `Query`, or a String in the query syntax), as
      # `Hit`s, best first. `id_prefix` keeps documents whose id starts with it; `highlight`
      # returns each hit's text and matches; `prefix_length` and `max_expansions` bound a
      # fuzzy clause's expansion.
      def search(query, k, id_prefix: nil, snapshot: nil, highlight: false, prefix_length: nil,
                 max_expansions: nil)
        search_with_stats(query, k, id_prefix:, snapshot:, highlight:, prefix_length:, max_expansions:)[0]
      end

      # `search`, with its `SearchStats`: `[hits, stats]`.
      def search_with_stats(query, k, id_prefix: nil, snapshot: nil, highlight: false, prefix_length: nil,
                            max_expansions: nil)
        hits, fuzzy, stale = native_search(query, k, [id_prefix, snapshot, highlight, prefix_length,
                                                      max_expansions])
        hits = hits.map do |id, score, text, matches|
          Hit.new(id, score, text, matches.map { |c, o, a, b| Match.new(c, o, a...b) }.freeze)
        end
        [hits.freeze, SearchStats.new(fuzzy.map { |f| FuzzyStats.new(*f) }.freeze, stale)]
      end

      # The index's consistency counts (`IndexStats`).
      def verify = IndexStats.new(*native_verify)

      # The options it was created with (`IndexOptions`).
      def options = IndexOptions.new(*native_options)

      # The cell it covers (`Source`).
      def source = Source.new(*native_source)
    end

    # A full-text index in the writer process.
    class Index
      include Searches

      class << self
        # Creates the index `name` over the cell `table`/`family`/`qualifier` of each row,
        # and indexes the rows already there. `positions` keeps word positions (needed for
        # phrases); `normalization` is `:nfc` (the default), `:nfkc` or `:none`.
        def create(db, name, table:, family:, qualifier:, k1: nil, b: nil, positions: nil, stop_words: nil,
                   normalization: nil, max_terms: nil, max_term_bytes: nil)
          native_create(db, name, table, family, qualifier,
                        [k1, b, positions, stop_words, normalization, max_terms, max_term_bytes])
        end

        # Opens the index `name`.
        def open(db, name) = native_open(db, name)

        # Opens the index `name` in a reader process: a `ReadIndex`.
        def open_reader(reader, name) = native_open_reader(reader, name)
      end

      # Rebuilds the index from its source cells (`RebuildStats`).
      def rebuild = RebuildStats.new(*native_rebuild)

      # Repairs what `verify` found (`RebuildStats`).
      def repair = RebuildStats.new(*native_repair)
    end

    # A full-text index in a reader process: `search` and `verify` at the reader's snapshots.
    class ReadIndex
      include Searches
    end
  end

  # The vector index (D306, ICR 0100): an IVF index of vectors by id, for nearest-neighbour
  # search over many vectors. It's the `pigeonhole-vector` crate; see the guide's "Vector
  # index" section.
  module Vector
    # A search hit: the vector's id and its distance from the query.
    Hit = Data.define(:id, :distance)

    # What a search read: the lists it probed, of the index's lists.
    SearchStats = Data.define(:lists_probed, :lists)

    # `verify`'s counts: the vectors, and each list's length.
    IndexStats = Data.define(:vectors, :list_lens)

    # What a bulk load did: the vectors loaded, and the lists it made.
    BulkStats = Data.define(:vectors, :lists)

    # The search methods `Index` and `ReadIndex` share.
    module Searches
      # The `k` nearest vectors to `query` (in the form `insert` takes), as `Hit`s, nearest
      # first. `nprobe` overrides the lists probed; `id_prefix` keeps ids starting with it,
      # widening the search to find `k` (up to `max_probe` lists); `parallel` reads lists on
      # that many threads.
      def search(query, k, nprobe: nil, parallel: nil, snapshot: nil, id_prefix: nil, max_probe: nil)
        search_with_stats(query, k, nprobe:, parallel:, snapshot:, id_prefix:, max_probe:)[0]
      end

      # `search`, with its `SearchStats`: `[hits, stats]`. A prefixed search with fewer than
      # `k` hits and `lists_probed < lists` stopped at `max_probe`.
      def search_with_stats(query, k, nprobe: nil, parallel: nil, snapshot: nil, id_prefix: nil, max_probe: nil)
        hits, stats = native_search(query, k, [nprobe, parallel, snapshot, id_prefix, max_probe])
        [hits.map { |h| Hit.new(*h) }.freeze, SearchStats.new(*stats)]
      end
    end

    # A vector index in the writer process.
    class Index
      include Searches

      class << self
        # Creates the index `name` of vectors of `spec` (a `VectorSpec`). `rebalance` is
        # `:inline` (the default: a write splits or merges a list before it returns) or
        # `:background` (a thread of the crate's own does). With `rebalancer: true`, returns
        # `[index, rebalancer]` instead, for a Ruby thread to drive.
        def create(db, name, spec, nprobe: nil, target_list_len: nil, reassign_neighbours: nil, rebalance: nil,
                   rebalancer: false)
          native_create(db, name, spec, [nprobe, target_list_len, reassign_neighbours, rebalance], rebalancer)
        end

        # Opens the index `name`; with `rebalancer: true`, `[index, rebalancer]`.
        def open(db, name, rebalancer: false) = native_open(db, name, rebalancer)

        # Opens the index `name` in a reader process: a `ReadIndex`.
        def open_reader(reader, name) = native_open_reader(reader, name)
      end

      # The index's consistency counts (`IndexStats`).
      def verify = IndexStats.new(*native_verify)

      # Loads `pairs` (an Enumerable of `[id, vector]`) into an empty index, faster than
      # inserting them one by one. `expected` is about how many there are: it chooses how
      # many lists to start with. The pairs stream to the index `batch` at a time. If
      # enumerating them raises, the batches sent before stay loaded (a bulk load isn't one
      # transaction), and the exception propagates. An interrupt waits for the load of what
      # was sent to end, then takes effect.
      def bulk_load(pairs, expected:, batch: 4096)
        loader = native_bulk_load(expected)
        begin
          pairs.each_slice(batch) { |b| loader.push(b) }
        rescue Exception # rubocop:disable Lint/RescueException
          # End the load (the batches sent stay loaded) and keep the caller's exception: a
          # failure of the load itself mustn't replace it (ai1249's N1 on #1434).
          begin
            loader.__send__(:native_finish)
          rescue Pigeonhole::Error
            nil
          end
          raise
        end
        BulkStats.new(*loader.__send__(:native_finish))
      end
    end

    # A vector index in a reader process: `search` at the reader's snapshots.
    class ReadIndex
      include Searches
    end

    # One transaction's writes to one index, for `Index#after_commit`. Make it inside the
    # transaction's block, so a retried attempt starts with a fresh one.
    class Placed
    end
  end
end
