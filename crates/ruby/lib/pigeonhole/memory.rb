# frozen_string_literal: true

module Pigeonhole
  # Agent memory (D311, ICR 0107): a store of short texts in namespaces, recalled by
  # keywords, an embedding, recency, importance and use, and kept by forget and
  # consolidation passes. It's the `pigeonhole-memory` crate, as Python's `pigeonhole.memory`
  # binds it. See the guide's "Agent memory" section.
  module Memory
    # The store is pinned to another embedding model than the one named (`InvalidArgument`).
    class ModelMismatch < InvalidArgument; end

    # The consolidator block raised, which stopped the pass (`MergeFailed`). `consolidated`
    # is the pass so far, whose groups stay committed, and `cause` the block's exception.
    class ConsolidateError < MergeFailed
      # The `ConsolidatePass` up to the failure.
      attr_reader :consolidated

      def initialize(message = nil, code = nil, code_num = nil, consolidated: nil)
        super(message, code, code_num)
        @consolidated = consolidated
      end
    end

    # Link types: the three the crate defines, and the first an application may use (16 to
    # 255).
    module LinkType
      DERIVED_FROM = 1
      SUPERSEDES = 2
      SIMILAR_TO = 3
      FIRST_CUSTOM = 16
    end

    class MemoryId
      include Comparable
    end

    private_constant :MERGE_FAILED_CODE_NUM

    # What `remember` did: `outcome` is `:new`, `:merged` (an exact duplicate merged into
    # `id`) or `:similar` (a near duplicate under `NearDedup` policy `:skip`: nothing was
    # written, and `id` is the match).
    Remembered = Data.define(:id, :outcome)

    # What `mark_used` did: `missing` are ids that no longer exist.
    Marked = Data.define(:marked, :missing)

    # A link: the linked memory, its type (an Integer, `LinkType`) and weight.
    Link = Data.define(:id, :type, :weight)

    # Where a memory came from, and when it was first seen (µs since the Unix epoch).
    Source = Data.define(:name, :at_us)

    # A stored memory, as `get` reads it.
    StoredMemory = Data.define(:id, :text, :kind, :importance, :uses, :last_used_us, :session,
                               :pinned, :consolidated, :superseded_by, :sources, :links,
                               :backlinks, :embedding) do
      # Whether it's pinned (a forget pass never deletes it).
      def pinned? = pinned

      # Whether a consolidation kept it, marked consolidated.
      def consolidated? = consolidated
    end

    # `verify`'s counts.
    StoreStats = Data.define(:memories, :no_content, :stale_dedup, :broken_links,
                             :missing_embeddings, :dangling_superseded,
                             :text_index_inconsistent) do
      # Whether every count of a problem is zero.
      def consistent?
        [no_content, stale_dedup, broken_links, missing_embeddings, dangling_superseded].all?(&:zero?) &&
          !text_index_inconsistent
      end
    end

    # One signal of a hit: its rank among the candidates that have it (1 is best) and value.
    Signal = Data.define(:rank, :value)

    # A hit's signals (`recall(with_signals: true)`): each nil when the hit lacks it.
    Signals = Data.define(:vector, :keyword, :recency, :importance, :uses, :graph)

    # One recalled memory.
    RecallHit = Data.define(:id, :text, :kind, :importance, :created_us, :superseded_by, :score,
                            :signals, :embedding)

    # What a recall did: `candidates` scored, whether the byte budget cut the hits, and the
    # graph expansion's `expanded` count and whether its budget cut it.
    RecallStats = Data.define(:candidates, :truncated, :expanded, :expansion_truncated)

    # A forget pass's counts, and where to resume (`forget(after:)`).
    ForgetPass = Data.define(:examined, :deleted, :protected, :resume)

    # A consolidation pass's counts, and where to resume (`consolidate(after:)`).
    ConsolidatePass = Data.define(:groups, :committed, :skipped, :created, :merged, :sources, :resume)

    # Recall's signal weights: 1 each by default, but `recency` 0.25 and `graph` and
    # `consolidated` 0.5. A weight of 0 leaves the signal out.
    class Weights
      FIELDS = %i[vector keyword recency importance uses graph consolidated].freeze

      class << self
        def new(vector: nil, keyword: nil, recency: nil, importance: nil, uses: nil, graph: nil,
                consolidated: nil)
          native_new(vector, keyword, recency, importance, uses, graph, consolidated)
        end
      end

      FIELDS.each_with_index do |f, i|
        define_method(f) { to_a[i] }
      end

      def to_h = FIELDS.zip(to_a).to_h

      def inspect = "#<Pigeonhole::Memory::Weights #{to_h.map { |k, v| "#{k}=#{v}" }.join(' ')}>"
    end

    # A recall's graph expansion (T4): the top 10 fused candidates seed a walk of the
    # memories' links in both directions, `hops` deep (1 to `MAX_HOPS`), and every memory it
    # reaches is scored like the others, with a `graph` signal besides. At most `max` get one
    # (20 by default).
    class Expand
      class << self
        def new(hops = 1, max: nil) = native_new(hops, max)
      end

      def inspect = "#<Pigeonhole::Memory::Expand hops=#{hops} max=#{max}>"
    end

    # A store's forget policy (T2). `max_age` maps a kind to seconds; durations are seconds.
    class ForgetPolicy
      class << self
        def new(min_retention: nil, max_age: nil, half_life: nil, use_weight: nil, max_per_pass: nil,
                interval: nil)
          native_new(min_retention, max_age, half_life, use_weight, max_per_pass, interval)
        end
      end
    end

    # How `consolidate` picks and folds groups (T3). `sources` is `:keep` (the default),
    # `:delete` or `:supersede`; `min_age` is seconds.
    class ConsolidatePolicy
      class << self
        def new(min_age: nil, group_similarity: nil, min_group: nil, max_group: nil, max_groups: nil,
                sources: nil)
          native_new(min_age, group_similarity, min_group, max_group, max_groups, sources)
        end
      end
    end

    # A store's near-duplicate dedup (T3): memories at `similarity` (cosine, above 0 and at
    # most 1) or above are near duplicates, handled by `policy`: `:link` (the default),
    # `:skip` or `:merge`.
    class NearDedup
      class << self
        def new(similarity, policy: :link) = native_new(similarity, policy)
      end
    end

    # A memory to write: what a consolidator block returns. `Store#remember` takes the same
    # keywords. `links` is an Array of `[id, link type, weight]`.
    class Memory
      class << self
        def new(text, embedding: nil, kind: nil, importance: nil, source: nil, session: nil, links: nil,
                pinned: false)
          native_new(text, embedding, kind, importance, source, session, links, pinned)
        end
      end
    end

    # Conversions from the native Arrays.
    module Build # :nodoc:
      module_function

      def stored(a)
        id, text, kind, importance, uses, last_used, session, pinned, consolidated, superseded,
          sources, links, backlinks, embedding = a
        StoredMemory.new(id, text, kind, importance, uses, last_used, session, pinned, consolidated,
                         superseded, sources.map { |s| Source.new(*s) }.freeze,
                         links.map { |l| Link.new(*l) }.freeze,
                         backlinks.map { |l| Link.new(*l) }.freeze, embedding)
      end

      def signal(s) = s && Signal.new(*s)

      def hits((hs, stats))
        hits = hs.map do |id, text, kind, importance, created, superseded, score, signals, embedding|
          signals &&= Signals.new(*signals.map { |s| signal(s) })
          RecallHit.new(id, text, kind, importance, created, superseded, score, signals, embedding)
        end
        [hits.freeze, RecallStats.new(*stats)]
      end

      def recall_args(namespaces, text:, embedding:, k:, candidates:, byte_budget:, snapshot:, now_us:,
                      half_life:, fusion:, weights:, include_superseded:, session:, with_signals:,
                      with_embeddings:, expand:)
        [namespaces, text, embedding, k, candidates, byte_budget, snapshot, now_us, half_life, fusion,
         weights, include_superseded, session, with_signals, with_embeddings, expand]
      end
    end
    private_constant :Build

    # The recall methods `Store` and `ReadStore` share.
    module Recalls
      # The memories best matching `text` and/or `embedding` in `namespaces` (an Array, or
      # one String), best first, at one snapshot. See the guide for each keyword.
      def recall(namespaces, text: nil, embedding: nil, k: nil, candidates: nil, byte_budget: nil,
                 snapshot: nil, now_us: nil, half_life: nil, fusion: :rrf, weights: nil,
                 include_superseded: false, session: nil, with_signals: false, with_embeddings: false,
                 expand: nil)
        recall_with_stats(namespaces, text:, embedding:, k:, candidates:, byte_budget:, snapshot:, now_us:,
                                      half_life:, fusion:, weights:, include_superseded:, session:,
                                      with_signals:, with_embeddings:, expand:)[0]
      end

      # `recall`, with its `RecallStats`: `[hits, stats]`.
      def recall_with_stats(namespaces, text: nil, embedding: nil, k: nil, candidates: nil, byte_budget: nil,
                            snapshot: nil, now_us: nil, half_life: nil, fusion: :rrf, weights: nil,
                            include_superseded: false, session: nil, with_signals: false,
                            with_embeddings: false, expand: nil)
        args = Build.recall_args(namespaces, text:, embedding:, k:, candidates:, byte_budget:, snapshot:,
                                             now_us:, half_life:, fusion:, weights:, include_superseded:,
                                             session:, with_signals:, with_embeddings:, expand:)
        Build.hits(native_recall(args))
      end

      # One memory, or nil.
      def get(id)
        a = native_get(id)
        a && Build.stored(a)
      end

      # The store's consistency counts (`StoreStats`).
      def verify = StoreStats.new(*native_verify)
    end

    # A memory store in the writer process: `Store.create` or `Store.open`. Close it (or use
    # the block forms) when done; `Database#close` closes the stores opened on it first.
    class Store
      include Recalls

      class << self
        # Creates the store `name` (tables `mem_NAME` and `mem_NAME_text`) and opens it.
        # `embedding` pins it to a model (without one, recall is by keyword and recency).
        # `forget` (or the `episodic_ttl` shorthand, in seconds) is its forget policy, run on
        # a thread of the crate's own; `near_dedup` its near-duplicate dedup; `normalization`
        # how its text index compares terms (`:nfc`, the default, `:nfkc` or `:none`), fixed
        # at create (#1381). With a block, yields the store and closes it afterwards.
        def create(db, name, embedding: nil, forget: nil, episodic_ttl: nil, near_dedup: nil,
                   normalization: nil, &)
          s = native_create(db, name, embedding, forget, episodic_ttl, near_dedup, normalization)
          block_given? ? scoped(s, &) : s
        end

        # Opens the store `name` for a caller embedding with `model` (an `EmbeddingModel`'s
        # id): `ModelMismatch` for another model, `InvalidArgument` if the store has none.
        # `model: nil` opens it without a model (on a pinned store, that handle can't
        # `remember` or `supersede`).
        def open(db, name, model:, &)
          s = native_open(db, name, model)
          block_given? ? scoped(s, &) : s
        end

        # Opens the store `name` from a reader process: a `ReadStore`.
        def open_reader(reader, name, model:, &)
          s = native_open_reader(reader, name, model)
          block_given? ? scoped(s, &) : s
        end

        private

        # Yields `s` and closes it afterwards.
        def scoped(s)
          yield s
        ensure
          s.close
        end
      end

      # Remembers one memory in namespace `ns`, in one transaction (a `Remembered`). An exact
      # duplicate (after trimming and collapsing whitespace) of a live memory merges into it;
      # with near dedup set, a near duplicate is handled by its policy.
      def remember(ns, text, embedding: nil, kind: nil, importance: nil, source: nil, session: nil,
                   links: nil, pinned: false)
        Remembered.new(*native_remember(ns, text, embedding, kind, importance, source, session, links,
                                        pinned))
      end

      # `remember` inside `txn`, committed with it. It skips near dedup (call
      # `near_duplicate` first if you need it), and its reads are at the transaction's
      # snapshot, so two `remember_in` of one text in one transaction both write.
      def remember_in(txn, ns, text, embedding: nil, kind: nil, importance: nil, source: nil, session: nil,
                      links: nil, pinned: false)
        Remembered.new(*native_remember_in(txn, ns, text, embedding, kind, importance, source, session,
                                           links, pinned))
      end

      # Links `from` to `to` (one namespace) with a link type (`LinkType`, or an
      # application's from 16) and a weight from 0 to 1.
      def link(from, to, link_type, weight = 1.0) = native_link(from, to, link_type, weight)

      # Counts a use of each memory of `ids` (a `Marked`).
      def mark_used(ids) = Marked.new(*native_mark_used(ids))

      # One forget pass (T2) under `policy`, else the store's persisted one
      # (`InvalidArgument` with neither); `after` resumes where an earlier pass stopped.
      def forget(policy: nil, after: nil, now_us: nil) = ForgetPass.new(*native_forget(policy, after, now_us))

      # The live memory of `ns` nearest a memory of `text` and `embedding`, with its
      # similarity (`[id, similarity]`), if it reaches the store's near-dedup similarity:
      # what `remember` checks (`remember_in` doesn't).
      def near_duplicate(ns, text, embedding: nil) = native_near_duplicate(ns, text, embedding)

      # One consolidation pass over `ns` (T3). The block is the consolidator: it gets each
      # group of episodic memories (an Array of `StoredMemory`) and returns an Array of
      # `Memory` to keep in their place, written as semantic (unless procedural) in the
      # group's session. Returns the `ConsolidatePass`.
      #
      # The pass runs on a thread of its own. The block runs on the calling thread, outside
      # every transaction and with no store lock held, so it may call the store (`recall`
      # for context, say) and take its time. A group that changes before its commit is
      # skipped, not retried; the next pass finds it again.
      #
      # A `StandardError` from the block, or a return that isn't an Array of `Memory`, stops
      # the pass with `ConsolidateError`: its `consolidated` is the pass so far and its
      # `cause` the exception. `break`, `throw` or `return` out of the block, any other
      # exception (`Interrupt`, `SystemExit`), and an interrupt (`Thread#raise`, `Timeout`,
      # Ctrl-C) stop the pass too, and then take effect as they are. Either way the pass's
      # thread has ended, the groups committed before stay committed, and running the pass
      # again is safe: it skips memories already consolidated.
      def consolidate(ns, policy: nil, after: nil, now_us: nil)
        raise ArgumentError, "consolidate needs a block: the consolidator" unless block_given?

        # A Proc, not a block (see native_consolidate). `break`, `throw` and `return` in the
        # caller's block still leave this method, through the native frames.
        consolidator = proc { |group| yield group.map { |m| Build.stored(m) }.freeze }
        pass, failure = native_consolidate(ns, policy, after, now_us, consolidator)
        pass = ConsolidatePass.new(*pass)
        return pass unless failure

        raise ConsolidateError.new("the consolidator failed: #{failure.class}: #{failure.message}",
                                   "MergeFailed", MERGE_FAILED_CODE_NUM, consolidated: pass),
              cause: failure
      end
    end

    # A memory store in a reader process: `recall`, `get` and `verify`, at the reader's
    # snapshots.
    class ReadStore
      include Recalls
    end
  end
end
