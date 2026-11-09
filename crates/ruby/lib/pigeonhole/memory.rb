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

    # Another consolidation loop of the same name took over the store (`Conflict`): stop this
    # one. Don't retry it or make a new loop of the name in its place: two supervisors doing
    # that would take the name back and forth for ever.
    class LoopTakenOver < Conflict; end

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
                               :backlinks, :embedding, :summary) do
      # Whether a consolidation wrote it as a summary of its group (T9).
      def summary? = summary

      # Whether it's pinned (a forget pass never deletes it).
      def pinned? = pinned

      # Whether a consolidation kept it, marked consolidated.
      def consolidated? = consolidated
    end

    # `verify`'s counts.
    # `ivf_mismatched` counts the memories with an embedding missing from an IVF index plus
    # the index's vectors with no such memory, read at verify's snapshot (#1427), and
    # `consistent?` counts it. `ivf_counter_drift` (namespaces whose counter is off) is
    # advisory, and `consistent?` leaves it out, as the crate does.
    StoreStats = Data.define(:memories, :no_content, :stale_dedup, :broken_links,
                             :missing_embeddings, :dangling_superseded,
                             :text_index_inconsistent, :ivf_mismatched, :ivf_counter_drift) do
      # Whether every count of a problem is zero.
      def consistent?
        [no_content, stale_dedup, broken_links, missing_embeddings, dangling_superseded].all?(&:zero?) &&
          !text_index_inconsistent && ivf_mismatched.zero?
      end
    end

    # One signal of a hit: its rank among the candidates that have it (1 is best) and value.
    Signal = Data.define(:rank, :value)

    # A hit's signals (`recall(with_signals: true)`): each nil when the hit lacks it.
    Signals = Data.define(:vector, :keyword, :recency, :importance, :uses, :graph)

    # One recalled memory.
    RecallHit = Data.define(:id, :text, :kind, :importance, :created_us, :superseded_by, :score,
                            :signals, :embedding)

    # What a recall did: `candidates` scored, whether the byte budget cut the hits, the graph
    # expansion's `expanded` count and whether its budget cut it, and for a store with an IVF
    # index, the namespaces its vector signal searched by IVF (`ivf_namespaces`) and those
    # that fell back to the exact scan (`ivf_fallbacks`).
    RecallStats = Data.define(:candidates, :truncated, :expanded, :expansion_truncated, :ivf_namespaces,
                              :ivf_fallbacks)

    # A forget pass's counts, and where to resume (`forget(after:)`).
    ForgetPass = Data.define(:examined, :deleted, :protected, :resume)

    # A consolidation pass's counts, and where to resume (`consolidate(after:)`).
    ConsolidatePass = Data.define(:groups, :committed, :skipped, :created, :merged, :sources, :resume)

    # One memory event of a commit (T9): `kind` is `:remembered`, `:summarized` (a summary a
    # consolidation wrote), `:superseded` (`by` is the newer memory), `:deleted`, `:updated`
    # (anything else on the memory: a merge, a use, a link), or `:unknown` for a kind a later
    # engine adds. One per memory per commit, the strongest winning.
    MemoryEvent = Data.define(:kind, :id, :by)

    # One commit's memory events, in commit order (`Watch#next`).
    MemoryCommit = Data.define(:seqno, :timestamp_us, :events) do
      def size = events.size
    end

    # One step of a `ConsolidateLoop`: the events it read, its passes (`[namespace,
    # ConsolidatePass]`), the namespaces whose pass failed (`[namespace, exception]`: a
    # `StandardError` from the block is the block's own), the namespaces whose pass stopped
    # without failing (`[namespace, ConsolidatePass]`, the pass so far: an interrupt refused
    # their groups, and they're due again at the next step), whether an interrupt ended its
    # wait early, when the next pass falls due (µs), and the loop's position as bytes.
    LoopStep = Data.define(:events, :passes, :failed, :stopped, :interrupted, :next_due, :token) do
      # Whether an interrupt ended the step's wait early (a step can also return at once on
      # one left over from the last step).
      def interrupted? = interrupted
    end

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

    # A store's IVF index (D311 T8), set at create: a namespace with at least
    # `min_namespace_len` memories with an embedding (10,000 by default) recalls through the
    # index, a smaller one by the exact scan. `nprobe` and `target_list_len` default to the
    # vector index's.
    class IvfOptions
      class << self
        def new(min_namespace_len: nil, nprobe: nil, target_list_len: nil)
          native_new(min_namespace_len, nprobe, target_list_len)
        end
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
          sources, links, backlinks, embedding, summary = a
        StoredMemory.new(id, text, kind, importance, uses, last_used, session, pinned, consolidated,
                         superseded, sources.map { |s| Source.new(*s) }.freeze,
                         links.map { |l| Link.new(*l) }.freeze,
                         backlinks.map { |l| Link.new(*l) }.freeze, embedding, summary)
      end

      def commit((seqno, ts, events))
        MemoryCommit.new(seqno, ts, events.map { |e| MemoryEvent.new(*e) }.freeze)
      end

      def group(memories) = memories.map { |m| stored(m) }.freeze

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
                      with_embeddings:, expand:, exact:, nprobe:)
        [namespaces, text, embedding, k, candidates, byte_budget, snapshot, now_us, half_life, fusion,
         weights, include_superseded, session, with_signals, with_embeddings, expand, exact, nprobe]
      end
    end
    private_constant :Build

    # The watch method `Store` and `ReadStore` share.
    module Watches
      # A `Watch` of the store's memory events in `namespaces` (every namespace by default),
      # through the database's change feed, which must be on and follow the store's table
      # (`mem_NAME`). It starts now, or after the commits `from_snapshot:` sees, or after a
      # stored `resume:` token (`Watch#token`). `visible: true` delivers a commit as soon as
      # reads see it, rather than once it's durable. With a block, yields the watch and
      # closes it afterwards.
      def watch(namespaces = [], from_snapshot: nil, resume: nil, visible: false)
        w = native_watch([Array(namespaces), from_snapshot, resume, visible])
        return w unless block_given?

        begin
          yield w
        ensure
          w.close
        end
      end
    end

    # A store's memory events (T9). Like `Pigeonhole::Feed` it's a cursor: every Enumerable
    # method consumes the commits it reads, and a live watch has no end (`to_a` waits for
    # ever; `each(timeout: 1).to_a` drains what's there). Waits release the GVL in short
    # slices, so `Thread#raise`, `Timeout` and Ctrl-C reach a consumer blocked in `next`.
    class Watch
      include Enumerable

      # The next `MemoryCommit`, waiting up to `timeout` seconds; nil if none came in time.
      # `timeout: nil` waits until a commit comes or the thread is interrupted (perf2's N2 on
      # #1441). `FeedExpired` once the history it needs is gone.
      def next(timeout: nil)
        b = Native::FiberBridge.current
        c = if b
              r = b.await(native_next_op(b.native),
                          deadline: timeout && Native::FiberBridge.deadline(timeout))
              r.equal?(Native::FiberBridge::TIMED_OUT) ? nil : r
            else
              native_next(timeout)
            end
        c && Build.commit(c)
      end

      # The next `MemoryCommit` if one is ready, else nil.
      def try_next
        c = native_try_next
        c && Build.commit(c)
      end

      # Yields each commit as it comes; with `timeout:`, stops once one doesn't come in time.
      def each(timeout: nil)
        return enum_for(:each, timeout:) unless block_given?

        while (c = self.next(timeout:))
          yield c
        end
        self
      end
    end

    # Feed-driven consolidation (T9): `Store#consolidate_loop`'s steps read the store's new
    # memories and run a pass for each namespace once its memories are `min_age` old, with
    # the loop's block as the consolidator. Its position is saved in the store under the
    # loop's name, so a restart resumes it. One loop per name: a newer one takes over, and
    # this one's next step raises `LoopTakenOver`.
    class ConsolidateLoop
      # One step: reads new memories for up to `timeout` seconds (less if a pass falls due),
      # runs the passes that are due, and saves the position. `timeout` bounds the wait, not
      # the passes, which run the block.
      #
      # A `StandardError` from the block fails that namespace's pass: it's in `failed`, and
      # retried after `min_age`. `throw`, another exception (`Interrupt`), or an interrupt
      # (`Timeout`, `Thread#raise`, Ctrl-C) stops the step: its wait for new memories ends
      # within about 50 ms, the groups it still had are refused (their namespaces stay due,
      # as `stopped` lists them), it saves, and then the interrupt takes effect. The group
      # whose block the interrupt lands in is refused as well; a commit already under way
      # finishes first. (`break` in the block raises `LocalJumpError`, a
      # `StandardError`: the block outlives the `consolidate_loop` call that took it.)
      #
      # Under a fiber scheduler (ICR 0126), the step's wait for new memories and its wait for
      # the block's groups let the thread's other fibers run, and the block runs on the
      # calling fiber, so it may itself yield (an HTTP call under Async, say). A stop or
      # timeout raised into the step's wait loses nothing (the next step reads what it
      # would have); raised into its passes, the pass still ends first, as an interrupt
      # does, and then it takes effect. A loop's first step starts it without waiting.
      def run_once(timeout = 1.0)
        b = Native::FiberBridge.current
        raw = b ? fiber_step(b, timeout) : native_run_once(timeout, @consolidator)
        events, passes, failed, stopped, interrupted, next_due, token = raw
        pairs = ->(list) { list.map { |ns, p| [ns, ConsolidatePass.new(*p)].freeze }.freeze }
        LoopStep.new(events, pairs.(passes), failed.map { |ns, e| [ns, e].freeze }.freeze, pairs.(stopped),
                     interrupted, next_due, token)
      end

      private

      # `run_once` under a scheduler: waits through the bridge, one commit at a time, until
      # `timeout` or the next pass falls due, then runs the step's passes, whose groups it
      # also waits for through the bridge.
      def fiber_step(bridge, timeout)
        started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        events = 0
        while true # rubocop:disable Style/InfiniteLoop -- `loop` would swallow StopIteration
          wait = timeout - (Process.clock_gettime(Process::CLOCK_MONOTONIC) - started)
          due = next_due
          wait = [wait, (due - loop_now_us) / 1e6].min if due
          break if wait <= 0

          n = bridge.await(native_wait_op(bridge.native), deadline: Native::FiberBridge.deadline(wait))
          # 0: the loop hasn't stepped yet (its step starts it), or its history was recycled.
          break if n.equal?(Native::FiberBridge::TIMED_OUT) || n.zero?

          events += n
        end
        native_run_once_fiber(bridge, events, @consolidator)
      end

      # The loop's clock now (µs since the Unix epoch), with its `clock_offset:`.
      def loop_now_us
        Process.clock_gettime(Process::CLOCK_REALTIME, :microsecond) + @clock_offset_us.to_i
      end
    end

    # The recall methods `Store` and `ReadStore` share.
    module Recalls
      # The memories best matching `text` and/or `embedding` in `namespaces` (an Array, or
      # one String), best first, at one snapshot. On a store with an IVF index, `exact: true`
      # forces the exact scan, and `nprobe` overrides the lists the index probes. See the
      # guide for each keyword.
      def recall(namespaces, text: nil, embedding: nil, k: nil, candidates: nil, byte_budget: nil,
                 snapshot: nil, now_us: nil, half_life: nil, fusion: :rrf, weights: nil,
                 include_superseded: false, session: nil, with_signals: false, with_embeddings: false,
                 expand: nil, exact: false, nprobe: nil)
        recall_with_stats(namespaces, text:, embedding:, k:, candidates:, byte_budget:, snapshot:, now_us:,
                                      half_life:, fusion:, weights:, include_superseded:, session:,
                                      with_signals:, with_embeddings:, expand:, exact:, nprobe:)[0]
      end

      # `recall`, with its `RecallStats`: `[hits, stats]`.
      def recall_with_stats(namespaces, text: nil, embedding: nil, k: nil, candidates: nil, byte_budget: nil,
                            snapshot: nil, now_us: nil, half_life: nil, fusion: :rrf, weights: nil,
                            include_superseded: false, session: nil, with_signals: false,
                            with_embeddings: false, expand: nil, exact: false, nprobe: nil)
        args = Build.recall_args(namespaces, text:, embedding:, k:, candidates:, byte_budget:, snapshot:,
                                             now_us:, half_life:, fusion:, weights:, include_superseded:,
                                             session:, with_signals:, with_embeddings:, expand:, exact:,
                                             nprobe:)
        b = Native::FiberBridge.current
        Build.hits(b ? b.await(native_recall_op(b.native, args)) : native_recall(args))
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
      include Watches

      class << self
        # Creates the store `name` (tables `mem_NAME` and `mem_NAME_text`) and opens it.
        # `embedding` pins it to a model (without one, recall is by keyword and recency).
        # `forget` (or the `episodic_ttl` shorthand, in seconds) is its forget policy, run on
        # a thread of the crate's own; `near_dedup` its near-duplicate dedup; `normalization`
        # how its text index compares terms (`:nfc`, the default, `:nfkc` or `:none`), fixed
        # at create (#1381); `ivf` an `IvfOptions`, for an IVF index of its embeddings (D311
        # T8; it needs `embedding`), fixed at create too. With a block, yields the store and
        # closes it afterwards.
        def create(db, name, embedding: nil, forget: nil, episodic_ttl: nil, near_dedup: nil,
                   normalization: nil, ivf: nil, &)
          s = native_create(db, name, embedding, forget, episodic_ttl, near_dedup, normalization, ivf)
          s.instance_variable_set(:@db, db)
          block_given? ? scoped(s, &) : s
        end

        # Opens the store `name` for a caller embedding with `model` (an `EmbeddingModel`'s
        # id): `ModelMismatch` for another model, `InvalidArgument` if the store has none.
        # `model: nil` opens it without a model (on a pinned store, that handle can't
        # `remember` or `supersede`).
        def open(db, name, model:, &)
          s = native_open(db, name, model)
          s.instance_variable_set(:@db, db)
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
      #
      # The writes (`remember`, `delete`, `link`, `supersede`, `mark_used`) and `recall` let
      # the thread's other fibers run while they wait under a fiber scheduler (ICR 0126): a
      # write's commit yields as `Database#transact`'s does, and its other waits (the
      # near-dedup lookup, an IVF store's settle) and `recall` run on a worker thread. A write
      # stopped while it waits ends as a stopped `transact` does (D210).
      def remember(ns, text, embedding: nil, kind: nil, importance: nil, source: nil, session: nil,
                   links: nil, pinned: false)
        args = [ns, text, embedding, kind, importance, source, session, links, pinned]
        b = Native::FiberBridge.current
        return Remembered.new(*native_remember(*args)) unless b

        # The near-dedup lookup reads outside the transaction (a vector search): off the thread.
        prepared = if native_near_dedup_on
                     b.await(native_prepare_remember_op(b.native, *args))
                   else
                     native_prepare_remember(*args)
                   end
        Remembered.new(*fiber_write(b, settle: true) { |t| native_remember_prepared_in(t, prepared) })
      end

      # Deletes the memory `id` and its links; whether it existed.
      def delete(id)
        b = Native::FiberBridge.current
        return native_delete(id) unless b

        fiber_write(b, settle: true) { |t| delete_in(t, id) }
      end

      # Marks `old` superseded by `new` (one namespace), linking them.
      def supersede(old, new)
        b = Native::FiberBridge.current
        return native_supersede(old, new) unless b

        fiber_write(b) { |t| native_supersede_in(t, old, new) }
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
      def link(from, to, link_type, weight = 1.0)
        b = Native::FiberBridge.current
        return native_link(from, to, link_type, weight) unless b

        fiber_write(b) { |t| native_link_in(t, from, to, link_type, weight) }
      end

      # Counts a use of each memory of `ids` (a `Marked`).
      def mark_used(ids)
        b = Native::FiberBridge.current
        Marked.new(*(b ? fiber_write(b) { |t| native_mark_used_in(t, ids) } : native_mark_used(ids)))
      end

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
        consolidator = proc { |group| yield Build.group(group) }
        pass, failure = native_consolidate(ns, policy, after, now_us, consolidator)
        pass = ConsolidatePass.new(*pass)
        return pass unless failure

        raise ConsolidateError.new("the consolidator failed: #{failure.class}: #{failure.message}",
                                   "MergeFailed", MERGE_FAILED_CODE_NUM, consolidated: pass),
              cause: failure
      end

      private

      # A write under a fiber scheduler, as the crate's own runs it (ICR 0123): the store's
      # `*_in` form in a `transact` (retried as the crate retries, its commit yielding), then
      # with `settle:` an IVF store's parked writes handed on, on a worker thread.
      def fiber_write(bridge, settle: false)
        out = @db.transact { |t| yield t }
        bridge.await(native_settle_op(bridge.native)) if settle && native_has_ivf
        out
      end
    end

    class Store
      # A `ConsolidateLoop` named `name` (non-empty, without `:`), whose block is the
      # consolidator, as `consolidate`'s is. `policy:` and `namespaces:` (all by default) are
      # as `consolidate`'s; `clock_offset:` (seconds) shifts the clock `min_age` is measured
      # on, for tests (a Ruby clock can't run inside a step). Needs the change feed, as
      # `watch` does.
      def consolidate_loop(name, policy: nil, namespaces: nil, clock_offset: nil, &block)
        raise ArgumentError, "consolidate_loop needs a block: the consolidator" unless block

        offset_us = clock_offset && (clock_offset * 1_000_000).to_i
        lp = native_consolidate_loop(name, policy, namespaces, offset_us)
        consolidator = proc { |group| block.call(Build.group(group)) }
        lp.instance_variable_set(:@consolidator, consolidator)
        lp.instance_variable_set(:@clock_offset_us, offset_us)
        lp
      end
    end

    # A memory store in a reader process: `recall`, `get` and `verify`, at the reader's
    # snapshots.
    class ReadStore
      include Recalls
      include Watches
    end
  end
end
