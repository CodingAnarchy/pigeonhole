# frozen_string_literal: true

# Mirrors crates/python/tests/test_memory.py: the memory store (D311, ICR 0107), recall,
# readers, forgetting, consolidation and near dedup, plus the binding's own rules:
# lifecycle, a block as the consolidator, and errors.
require_relative "test_helper"
require "timeout"

class TestMemory < Minitest::Test
  include PigeonholeTest

  M = Pigeonhole::Memory
  DAY = 86_400

  def model(dim = 4) = M::EmbeddingModel.new("m", dim, :f32)

  def store(name = "agents", **opts, &) = M::Store.create(db, name, **opts, &)

  def min_age0 = M::ConsolidatePolicy.new(min_age: 0)

  # How many summaries the consolidations wrote (recall's recency signal also returns the
  # memories that don't match the text).
  def summaries(s) = s.recall("n", text: "summary", k: 100).count { |h| h.text.start_with?("summary") }

  # store.rs: remember, get, an exact duplicate merges, verify is consistent.
  def test_remember_get_and_exact_dedup
    store do |s|
      assert_nil s.embedding_model
      assert_equal :nfc, s.normalization
      r = s.remember("user:dana", "Dana prefers tabs", kind: :semantic, importance: 0.8, source: "chat-1",
                                                       session: "s1")
      assert_equal :new, r.outcome
      assert_kind_of M::MemoryId, r.id
      again = s.remember("user:dana", "  Dana prefers   tabs", source: "chat-2")
      assert_equal [:merged, r.id], [again.outcome, again.id]
      m = s.get(r.id)
      assert_equal ["Dana prefers tabs", :semantic, "s1"], [m.text, m.kind, m.session]
      assert_in_delta 0.8, m.importance
      assert_equal %w[chat-1 chat-2], m.sources.map(&:name).sort
      assert m.frozen? && m.text.frozen? && m.sources.frozen?
      # The text form round-trips, and every id argument takes it.
      assert_equal r.id, M::MemoryId.new(r.id.to_s)
      assert_equal r.id, s.get(r.id.to_s).id
      assert_equal r.id, M::MemoryId.from_bytes(r.id.to_bytes)
      assert_equal "user:dana", r.id.namespace
      assert_operator r.id.created_us, :>, 0
      assert_equal 1, [r.id, M::MemoryId.new(r.id.to_s)].uniq.size
      assert_match(/MemoryId/, r.id.inspect)
      assert_predicate s.verify, :consistent?
    end
  end

  def test_ids_that_are_not_ids_are_invalid_argument
    store do |s|
      ["nope", ""].each do |bad|
        assert_raises(Pigeonhole::InvalidArgument) { M::MemoryId.new(bad) }
        assert_raises(Pigeonhole::InvalidArgument) { s.get(bad) }
      end
      assert_raises(Pigeonhole::InvalidArgument) { M::MemoryId.from_bytes("\x05\x00ab".b) }
      assert_bad_call(TypeError) { s.get(42) }
    end
  end

  def test_links_supersede_and_mark_used
    store do |s|
      a = s.remember("n", "the deploy runs on Fridays").id
      b = s.remember("n", "the deploy runs on Thursdays").id
      c = s.remember("other", "unrelated").id
      s.link(b, a, M::LinkType::DERIVED_FROM, 0.5)
      s.link(a, b, 20)
      got = s.get(b)
      assert_equal [[a, 1, 0.5]], got.links.map { |l| [l.id, l.type, l.weight] }
      assert_equal [[a, 20]], got.backlinks.map { |l| [l.id, l.type] }
      [5, 256, -1, 1 << 70].each do |bad|
        assert_raises(Pigeonhole::InvalidArgument) { s.link(a, b, bad) }
      end
      assert_raises(Pigeonhole::InvalidArgument) { s.link(a, c, M::LinkType::SIMILAR_TO) } # across namespaces
      s.supersede(a, b)
      assert_equal b, s.get(a).superseded_by
      assert_equal [b], s.recall("n", text: "deploy").map(&:id)
      both = s.recall(["n"], text: "deploy", include_superseded: true)
      assert_equal [a, b].sort, both.map(&:id).sort
      assert_equal b, both.find { |h| h.id == a }.superseded_by
      m = s.mark_used([b, a.to_s])
      assert_equal [2, []], [m.marked, m.missing]
      assert_equal 1, s.get(b).uses
      assert s.delete(c)
      refute s.delete(c)
      assert_equal [c], s.mark_used([c]).missing
    end
  end

  # recall.rs: the vector signal, keyword-only recall on a pinned store, signals, the byte
  # budget, weights, and one snapshot.
  def test_recall_ranks_signals_budgets_and_snapshots
    store(embedding: model) do |s|
      assert_equal model, s.embedding_model
      a = s.remember("n", "the cache sits in front of the database", embedding: [1.0, 0, 0, 0], importance: 0.9).id
      b = s.remember("n", "lunch is at noon", embedding: [0, 1.0, 0, 0]).id
      assert_raises(Pigeonhole::InvalidArgument) { s.remember("n", "no embedding on a pinned store") }
      assert_raises(Pigeonhole::InvalidArgument) { s.remember("n", "wrong dimension", embedding: [1.0, 0]) }
      assert_equal a, s.recall("n", embedding: [0.9, 0.1, 0, 0])[0].id
      assert_equal b, s.recall("n", embedding: [0.1, 0.9, 0, 0])[0].id
      assert_equal b, s.recall("n", text: "lunch")[0].id
      hit = s.recall("n", text: "cache", with_signals: true, with_embeddings: true)[0]
      assert_equal 1, hit.signals.keyword.rank
      assert_operator hit.signals.keyword.value, :>, 0
      assert_nil hit.signals.vector # no query embedding
      assert_equal [1.0, 0.0, 0.0, 0.0], hit.embedding
      hits, stats = s.recall_with_stats("n", text: "noon", byte_budget: 1)
      assert_equal 1, hits.size # the first hit even past the budget
      assert stats.truncated
      assert_operator stats.candidates, :>=, 1
      # Weights that only count recency put the newer memory first.
      only_recency = M::Weights.new(vector: 0, keyword: 0, importance: 0, uses: 0)
      assert_equal b, s.recall("n", text: "cache", weights: only_recency)[0].id
      assert_in_delta 0.25, only_recency.recency
      assert_equal M::Weights.new, M::Weights.new
      assert_raises(Pigeonhole::InvalidArgument) { s.recall("n") } # neither text nor embedding
      assert_raises(Pigeonhole::InvalidArgument) { s.recall("n", text: "x", fusion: :nope) }
      snap = db.snapshot
      s.remember("n", "a later cache note", embedding: [1.0, 0, 0, 0])
      at = s.recall("n", text: "cache", snapshot: snap)
      refute(at.any? { |h| h.text == "a later cache note" })
      refute_empty at
      assert_equal a, s.recall("n", text: "cache", snapshot: snap, fusion: :linear)[0].id
      snap.release
      assert_raises(Pigeonhole::SnapshotExpired) { s.recall("n", text: "cache", snapshot: snap) }
    end
  end

  # recall.rs's graph expansion (T4): Expand, the graph signal and weight, the stats.
  def test_recall_expands_through_links
    store("graph", embedding: model) do |s|
      far = s.remember("n", "the reason behind it", embedding: [0, 0, 1.0, 0]).id
      seed = s.remember("n", "the decision", embedding: [1.0, 0, 0, 0], links: [[far, 16, 0.8]]).id
      q = { embedding: [1.0, 0, 0, 0], candidates: 1, with_signals: true }
      # Without expand there's no graph signal, whatever its weight.
      plain, st = s.recall_with_stats("n", weights: M::Weights.new(graph: 5.0), **q)
      assert_equal [seed], plain.map(&:id)
      assert(plain.all? { |h| h.signals.graph.nil? })
      assert_equal [0, false], [st.expanded, st.expansion_truncated]
      hits, st = s.recall_with_stats("n", expand: M::Expand.new(1), **q)
      by = hits.to_h { |h| [h.id, h] }
      assert_nil by[seed].signals.graph # a seed
      g = by[far].signals.graph
      assert_equal 1, g.rank
      assert_in_delta 0.8, g.value, 1e-6 # seed 1's value (1) times 0.8
      assert_equal [1, false], [st.expanded, st.expansion_truncated]
      _, st = s.recall_with_stats("n", expand: M::Expand.new(1, max: 0), **q)
      assert_equal 0, st.expanded
      _, st = s.recall_with_stats("n", expand: M::Expand.new, byte_budget: 1, **q)
      assert_equal [0, true], [st.expanded, st.expansion_truncated]
      x = M::Expand.new(2, max: 5)
      assert_equal [2, 5], [x.hops, x.max]
      assert_equal M::Expand.new(2, max: 5), x
      assert_equal 4, M::Expand::MAX_HOPS
      assert_equal 20, M::Expand.new.max
      assert_in_delta 0.5, M::Weights.new.graph
      [0, M::Expand::MAX_HOPS + 1].each do |bad|
        assert_raises(Pigeonhole::InvalidArgument) { s.recall("n", expand: M::Expand.new(bad), **q) }
      end
      assert_raises(Pigeonhole::InvalidArgument) { M::Expand.new(-1) }
      assert_raises(Pigeonhole::InvalidArgument) { M::Expand.new(1, max: -1) }
      assert_raises(Pigeonhole::InvalidArgument) { M::Expand.new(2**40) }
    end
  end

  def test_open_checks_the_model
    store(embedding: model).close
    e = assert_raises(M::ModelMismatch) { M::Store.open(db, "agents", model: "other") }
    assert_kind_of Pigeonhole::InvalidArgument, e
    assert_equal "InvalidArgument", e.code
    M::Store.open(db, "agents", model: "m") { |s| s.remember("n", "x", embedding: [1.0, 0, 0, 0]) }
    M::Store.open(db, "agents", model: nil) do |s|
      assert_equal "x", s.recall("n", text: "x")[0].text
      assert_raises(Pigeonhole::InvalidArgument) { s.remember("n", "a handle without the model can't remember") }
    end
  end

  # #1381: the text index's normalization is fixed at create.
  def test_normalization_is_set_at_create
    store("plain", normalization: :none) { |s| assert_equal :none, s.normalization }
    store("compat", normalization: :nfkc) do |s|
      assert_equal :nfkc, s.normalization
      id = s.remember("n", "ﬁle formats").id # "ﬁle" with the fi ligature
      assert_equal id, s.recall("n", text: "file")[0]&.id
    end
    assert_raises(Pigeonhole::InvalidArgument) { store("bad", normalization: :nfd) }
  end

  # remember_in / delete_in commit with the transaction; remember_in skips near dedup; inside
  # Database#transact, a retry re-reads.
  def test_in_transactions
    store(embedding: model, near_dedup: M::NearDedup.new(0.9, policy: :skip)) do |s|
      first = s.remember("n", "the timeout is 5 s", embedding: [1.0, 0, 0, 0]).id
      near = s.remember("n", "the timeout is five seconds", embedding: [0.99, 0.01, 0, 0])
      assert_equal [:similar, first], [near.outcome, near.id]
      assert_equal first, s.near_duplicate("n", "x", embedding: [0.98, 0.02, 0, 0])[0]
      r = db.transact { |txn| s.remember_in(txn, "n", "the timeout is 6 s", embedding: [0.98, 0.02, 0, 0]) }
      assert_equal :new, r.outcome
      txn = db.transaction
      assert s.delete_in(txn, r.id)
      refute_nil s.get(r.id) # not committed yet
      txn.commit
      assert_nil s.get(r.id)
      txn = db.transaction
      txn.rollback
      assert_raises(Pigeonhole::Error) { s.remember_in(txn, "n", "x", embedding: [1.0, 0, 0, 0]) }
    end
  end

  # forget.rs: a pass under the persisted policy or an override, resume, forget_in.
  def test_forgetting
    store do |s|
      assert_raises(Pigeonhole::InvalidArgument) { s.forget } # no policy anywhere
      assert_nil s.forget_policy
      old = s.remember("n", "an old episode").id
      keep = s.remember("n", "a pinned rule", pinned: true).id
      later = old.created_us + (10 * DAY * 1_000_000)
      p = M::ForgetPolicy.new(max_age: { episodic: DAY }, max_per_pass: 10, half_life: 3 * DAY, use_weight: 0.2,
                              min_retention: 0.01, interval: 60)
      assert_equal({ episodic: DAY.to_f }, p.max_age)
      assert_equal [3 * DAY, 0.2, 10, 60], [p.half_life, p.use_weight, p.max_per_pass, p.interval]
      passed = s.forget(policy: p, now_us: later)
      assert_equal [1, 1, nil], [passed.deleted, passed.protected, passed.resume]
      assert_nil s.get(old)
      refute_nil s.get(keep)
      s.forget_policy = p
      assert_equal p, s.forget_policy
      other = s.remember("n", "another episode").id
      txn = db.transaction
      assert_equal :not_due, s.forget_in(txn, other, other.created_us)
      assert_equal :protected, s.forget_in(txn, keep, later)
      assert_equal :deleted, s.forget_in(txn, other, later)
      txn.commit
      assert_nil s.get(other)
      s.forget_policy = nil
      assert_nil s.forget_policy
      assert_raises(Pigeonhole::InvalidArgument) { s.forget(policy: M::ForgetPolicy.new(interval: 0.1)) }
    end
  end

  # consolidate.rs: a block folds a group; it may call the store; its exception stops the
  # pass with the pass so far.
  def test_consolidation_with_a_block
    store do |s|
      ["met Dana at 9", "Dana asked about tabs", "Dana prefers tabs"].each { |t| s.remember("n", t, session: "s1") }
      seen = []
      p = s.consolidate("n", policy: min_age0) do |group|
        seen << group.map(&:text)
        # Re-entrancy: the block may read the store.
        refute_empty s.recall("n", text: "Dana")
        [M::Memory.new("Dana prefers tabs (summary)", kind: :semantic, importance: 0.9)]
      end
      assert_equal [1, 1, 1, 0], [p.groups, p.committed, p.created, p.skipped]
      assert_equal [3], seen.map(&:size)
      summary = s.recall("n", text: "summary")[0]
      assert_equal :semantic, summary.kind
      assert_equal [M::LinkType::DERIVED_FROM], s.get(summary.id).links.map(&:type).uniq
      # Consolidated memories aren't grouped again.
      assert_equal 0, s.consolidate("n", policy: min_age0) { |_| [] }.groups

      %w[first second].each { |t| s.remember("m", t, session: "s2") }
      boom = Class.new(StandardError)
      e = assert_raises(M::ConsolidateError) { s.consolidate("m", policy: min_age0) { raise boom, "no model" } }
      assert_kind_of Pigeonhole::MergeFailed, e
      assert_equal "MergeFailed", e.code
      assert_kind_of boom, e.cause
      assert_equal 0, e.consolidated.committed
      # (Under `rbs test`, RBS checks the block's return first.)
      e = assert_bad_call(M::ConsolidateError) { s.consolidate("m", policy: min_age0) { "not an Array" } }
      assert_kind_of TypeError, e.cause if e.is_a?(M::ConsolidateError)
      e = assert_bad_call(M::ConsolidateError) { s.consolidate("m", policy: min_age0) { [1] } }
      assert_kind_of TypeError, e.cause if e.is_a?(M::ConsolidateError)
      assert_bad_call(ArgumentError) { s.consolidate("m") }
      # Returning nothing keeps the group, marked consolidated.
      p = s.consolidate("m", policy: min_age0) { [] }
      assert_equal [1, 0], [p.committed, p.created]
      assert(s.recall("m", text: "first second").all? { |h| s.get(h.id).consolidated? })
      assert_predicate s.verify, :consistent?
    end
  end

  # The GVL is released while the pass waits for the block: another thread recalls while
  # the pass runs and the block itself recalls.
  def test_a_consolidation_runs_while_another_thread_recalls
    store do |s|
      6.times { |i| s.remember("n", "note #{i}", session: "s") }
      stop = false
      reader = Thread.new do
        n = 0
        until stop
          s.recall("n", text: "note")
          n += 1
        end
        n
      end
      begin
        p = s.consolidate("n", policy: min_age0) do |_|
          refute_empty s.recall("n", text: "note")
          [M::Memory.new("all the notes")]
        end
      ensure
        stop = true
      end
      assert_equal 1, p.committed
      assert_operator reader.value, :>, 0
    end
  end

  # perf2's C1 on #697: `break`, `throw` and an interrupt stop the pass and join its thread
  # before they take effect; the groups committed before stay committed, and a rerun is safe.
  def test_break_throw_and_interrupts_stop_the_pass
    store do |s|
      %w[a b c].each { |session| ["#{session} x one", "#{session} x two"].each { |t| s.remember("n", t, session:) } }
      calls = 0
      out = s.consolidate("n", policy: min_age0) do |group|
        calls += 1
        break :stopped if calls == 2

        [M::Memory.new("summary #{group[0].session}")]
      end
      assert_equal [:stopped, 2], [out, calls]
      assert_equal 1, summaries(s)
      assert_equal :thrown, catch(:out) { s.consolidate("n", policy: min_age0) { throw :out, :thrown } }
      # An interrupt from another thread while the block runs (Timeout's way).
      assert_raises(Timeout::Error) do
        Timeout.timeout(0.2) { s.consolidate("n", policy: min_age0) { sleep 5 } }
      end
      # Each stop left no thread behind and nothing half-written: a rerun folds the rest.
      p = s.consolidate("n", policy: min_age0) { |g| [M::Memory.new("summary #{g[0].session}")] }
      assert_equal 2, p.committed
      assert_equal 3, summaries(s)
      assert_predicate s.verify, :consistent?
    end
  end

  # The pass's threads alive now, by name; nil where the platform doesn't show them (only
  # Linux's /proc does, which CI runs).
  def consolidation_threads
    return nil unless File.directory?("/proc/self/task")

    Dir["/proc/self/task/*/comm"].count { |f| File.read(f).start_with?("pigeonhole-cons") }
  rescue SystemCallError
    nil
  end

  # ai1249's B1 and perf2's S1 on #1425: an interrupt arriving at any point (inside the
  # block, between waits, at the final join) never returns while the pass's thread still
  # runs. Many passes under tiny random timeouts land interrupts all over the pass.
  def test_an_interrupt_anywhere_leaves_no_thread_behind
    rng = Random.new(1425)
    store do |s|
      60.times { |i| s.remember("n", "note #{i}", session: "s#{i % 20}") }
      200.times do
        begin
          Timeout.timeout(rng.rand * 0.004) do
            s.consolidate("n", policy: min_age0) do |g|
              sleep(rng.rand * 0.002)
              [M::Memory.new("summary #{g[0].session}")]
            end
          end
        rescue Timeout::Error
          nil
        end
        left = consolidation_threads
        assert_equal 0, left, "a consolidation thread outlived its call" if left
      end
      assert_predicate s.verify, :consistent?
    end
  end

  # Python's KeyboardInterrupt case, in Ruby's terms: a StandardError from the block becomes
  # ConsolidateError with the pass so far; Interrupt (not a StandardError) stops the pass and
  # propagates as itself. The groups before either stay committed.
  def test_an_exception_in_the_block_stops_the_pass
    store do |s|
      %w[a b].each { |session| ["#{session} x one", "#{session} x two"].each { |t| s.remember("n", t, session:) } }
      calls = []
      e = assert_raises(M::ConsolidateError) do
        s.consolidate("n", policy: min_age0) do |group|
          calls << group[0].session
          raise ArgumentError, "the model refused" if calls.size == 2

          [M::Memory.new("summary #{group[0].session}")]
        end
      end
      assert_kind_of ArgumentError, e.cause
      assert_equal 1, e.consolidated.committed
      assert_equal "summary #{calls[0]}", s.recall("n", text: "summary")[0].text
      assert_raises(Interrupt) { s.consolidate("n", policy: min_age0) { raise Interrupt } }
      assert_equal 1, summaries(s)
    end
  end

  # simp724's P1 on #1340: close and the block forms, then Closed; Database#close closes
  # the stores opened on it first, forgetter included.
  def test_close_and_database_close
    d = Pigeonhole.open(@path, create: true, shards: 1)
    s = M::Store.create(d, "agents", forget: M::ForgetPolicy.new(max_age: { episodic: DAY }, interval: 1))
    s.remember("n", "x")
    s.close
    s.close # idempotent
    assert_predicate s, :closed?
    assert_raises(Pigeonhole::Closed) { s.recall("n", text: "x") }
    s2 = M::Store.open(d, "agents", model: nil)
    s3 = M::Store.open(d, "agents", model: nil)
    d.close
    [s2, s3].each { |h| assert_raises(Pigeonhole::Closed) { h.remember("n", "y") } }
    # The file reopens cleanly: nothing held it.
    Pigeonhole.open(@path, shards: 1) do |d2|
      M::Store.open(d2, "agents", model: nil) { |st| assert_equal "x", st.recall("n", text: "x")[0].text }
    end
  end

  def test_a_reader_recalls
    store do |s|
      rid = s.remember("n", "the build runs nightly").id
      Pigeonhole.open_reader(@path) do |reader|
        rs = nil
        M::Store.open_reader(reader, "agents", model: nil) do |r|
          rs = r
          assert_equal rid, r.recall("n", text: "build")[0].id
          assert_equal "the build runs nightly", r.get(rid).text
          assert_equal 1, r.verify.memories
          refute_respond_to r, :remember
        end
        assert_raises(Pigeonhole::Closed) { rs.recall("n", text: "build") }
        rs2 = M::Store.open_reader(reader, "agents", model: nil)
        reader.close
        assert_raises(Pigeonhole::Closed) { rs2.get(rid) }
      end
    end
  end

  def test_near_dedup_settings
    store(embedding: model) do |s|
      assert_nil s.near_dedup
      n = M::NearDedup.new(0.95, policy: :merge)
      s.near_dedup = n
      assert_equal n, s.near_dedup
      assert_equal :merge, n.policy
      assert_in_delta 0.95, n.similarity
      s.near_dedup = nil
      assert_nil s.near_dedup
      assert_raises(Pigeonhole::InvalidArgument) { s.near_dedup = M::NearDedup.new(0.0) }
      assert_raises(Pigeonhole::InvalidArgument) { M::NearDedup.new(0.9, policy: :nope) }
    end
    store("plain") { |s| assert_raises(Pigeonhole::InvalidArgument) { s.near_dedup = M::NearDedup.new(0.9) } }
  end

  # simp724's P1: a call already running ends with Closed, not a hang. The block closes the
  # database mid-pass; the group's commit then fails with Closed (perf2's C4 on #697).
  def test_a_close_during_a_consolidation_ends_it_with_closed
    d = Pigeonhole.open(@path, create: true, shards: 1)
    s = M::Store.create(d, "agents")
    %w[one two].each { |t| s.remember("n", t, session: "s") }
    e = assert_raises(Pigeonhole::Closed, M::ConsolidateError) do
      s.consolidate("n", policy: min_age0) do |_|
        d.close
        [M::Memory.new("summary")]
      end
    end
    assert_kind_of Pigeonhole::Closed, e
    assert_raises(Pigeonhole::Closed) { s.recall("n", text: "one") }
  end

  # simp724's L1 on #1395: a store opened during Database#close is either closed by it or
  # refused with Closed; none is left working on a closed database.
  def test_stores_opened_while_the_database_closes_never_outlive_it
    20.times do
      d = Pigeonhole.open(@path, create: true, shards: 1)
      M::Store.create(d, "agents").close unless d.tables.include?("mem_agents")
      opened = []
      go = Queue.new
      opener = Thread.new do
        go.pop
        50.times { opened << M::Store.open(d, "agents", model: nil) }
      rescue Pigeonhole::Closed
        nil
      end
      go << true
      d.close
      opener.join
      opened.each { |st| assert_raises(Pigeonhole::Closed) { st.recall("n", text: "x") } }
    end
  end

  def test_option_values
    w = M::Weights.new(keyword: 2)
    assert_equal 2.0, w.keyword
    assert_equal w, M::Weights.new(keyword: 2.0)
    assert_equal 1, [w, M::Weights.new(keyword: 2)].uniq.size
    assert_equal w.to_a, M::Weights::FIELDS.map { |f| w.public_send(f) }
    c = M::ConsolidatePolicy.new(min_age: 60, sources: :supersede, min_group: 3)
    assert_equal [60.0, :supersede, 3], [c.min_age, c.sources, c.min_group]
    assert_raises(Pigeonhole::InvalidArgument) { M::ConsolidatePolicy.new(sources: :drop) }
    em = model(8)
    assert_equal ["m", 8, :f32], [em.id, em.dim, em.element]
    assert_raises(Pigeonhole::InvalidArgument) { M::EmbeddingModel.new("", 4, :f32) }
    m = M::Memory.new("a note", kind: :procedural, links: [])
    assert_equal "a note", m.text
    assert_raises(Pigeonhole::InvalidArgument) { M::Memory.new("x", kind: :dream) }
    assert_bad_call(TypeError) { M::Memory.new("x", links: [[1, 2]]) }
    assert_bad_call(TypeError) { M::Memory.new("x", embedding: ["a"]) }
  end
end
