# frozen_string_literal: true

# Mirrors crates/python/tests/test_budget.py: D308's byte-budgeted reads (#1308). Scan and
# multi-get pages against a model of D308's size rule, resumed chains, positions' bytes,
# limit against the budget, a traversal's byte budget, and nearest_within.
require_relative "test_helper"

class TestBudget < Minitest::Test
  include PigeonholeTest

  OVERHEAD = 16

  # A read's cells, in order: [row key, family, qualifier, value].
  def cells(rows)
    rows.flat_map { |r| r.map { |f, q, c| [r.key, f, q, c.value] } }
  end

  # D308's page by hand over a read's cells: the longest prefix of columns whose sizes fit
  # (qualifier, value and 16 each, and a row's key once), and the size of the first that
  # didn't (its key included).
  def model_page(flat, budget)
    out = []
    used = 0
    flat.each do |key, f, q, v|
      size = q.bytesize + v.bytesize + OVERHEAD
      first = out.empty? || out.last[0] != key
      need = size + (first ? key.bytesize : 0)
      return [out, used, size + key.bytesize] if used + need > budget

      out << [key, f, q, v]
      used += need
    end
    [out, used, nil]
  end

  def docs
    @docs ||= begin
      t = db.table("docs").family("a").family("b").create
      rng = Random.new(1308)
      60.times do |i|
        m = t.mutate(format("doc:%03d", i))
        (1 + rng.rand(4)).times { |q| m.put(%w[a b].sample(random: rng), "q#{q}", "\0" * (1 + rng.rand(299))) }
        m.commit
      end
      t
    end
  end

  # Each page equals the model's prefix of what's left; resumed after each page in turn,
  # the pages concatenate to the whole scan, exactly once.
  def test_scan_pages_follow_the_size_rule_and_chain_to_the_whole_read
    whole = cells(docs.scan("doc:"))
    [0, 40, 100, 333, 1000, 5000, 1 << 20].each do |budget|
      got = []
      after = nil
      loop do
        page = docs.scan_within("doc:", bytes: budget, after: after)
        want, used, nxt = model_page(whole[got.size..], budget)
        assert_equal want, cells(page.rows), budget
        assert_equal used, page.bytes
        assert_operator page.bytes, :<=, budget
        assert_equal !nxt.nil?, page.cut_by_budget
        nxt.nil? ? assert_nil(page.next_bytes) : assert_equal(nxt, page.next_bytes)
        assert page.frozen? && page.rows.frozen?
        got += cells(page.rows)
        # An empty page cut by the budget would resume at its own start, forever.
        break if page.resume.nil? || page.rows.empty?

        after = page.resume
      end
      assert_equal whole, got if budget >= 1000
    end
  end

  def test_an_empty_page_says_what_the_next_column_needs
    page = docs.scan_within("doc:", bytes: 10)
    assert_equal [[], 0, true], [page.rows, page.bytes, page.cut_by_budget]
    first = docs.scan("doc:").first
    _, q, c = first.first
    assert_equal q.bytesize + c.value.bytesize + OVERHEAD + first.key.bytesize, page.next_bytes
    # The page resumes at its own start: with next_bytes, the read returns that column.
    again = docs.scan_within("doc:", bytes: page.next_bytes, after: page.resume)
    assert_equal 1, cells(again.rows).size
  end

  def test_limit_and_the_budget_stop_the_read_whichever_comes_first
    page = docs.scan_within("doc:", bytes: 1 << 20, limit: 3)
    assert_equal 3, page.size
    refute page.cut_by_budget
    refute_nil page.resume
    assert_equal page.rows.last.key, page.resume.row
    small = docs.scan_within("doc:", bytes: 50, limit: 3)
    assert small.cut_by_budget
    assert_operator small.rows.size, :<=, 1
    assert_raises(ArgumentError) { docs.scan_within("doc:", bytes: 1, start: "doc:") }
  end

  def test_multi_get_pages_resume_by_key_even_when_the_keys_change
    keys = (0...60).step(3).map { |i| format("doc:%03d", i) } + ["doc:zzz"]
    whole = cells(docs.multi_get(keys).compact)
    got = []
    after = nil
    loop do
      page = docs.multi_get_within(keys, bytes: 500, after: after)
      assert_operator page.bytes, :<=, 500
      got += cells(page.rows)
      break if page.resume.nil?

      after = page.resume
    end
    assert_equal whole, got
    # A set rebuilt between pages resumes at the first key at or after the position's.
    first = docs.multi_get_within(keys, bytes: 1500)
    at = first.resume.key
    assert_operator "doc:001", :<, at
    assert_operator at, :<, "doc:zzz"
    later = keys.select { |k| k >= at }.drop(1) + ["doc:001", at]
    nxt = docs.multi_get_within(later, bytes: 1 << 20, after: first.resume)
    assert_operator nxt.rows[0].key, :>=, at
    refute_includes nxt.rows.map(&:key), "doc:001"
    assert_equal whole, cells(first.rows) + cells(nxt.rows)
  end

  def test_positions_round_trip_and_are_refused_across_kinds_and_tables
    sp = docs.scan_within("doc:", bytes: 300).resume
    kp = docs.multi_get_within(%w[doc:000 doc:001 doc:002], bytes: 200).resume
    refute_nil sp
    refute_nil kp
    assert_equal sp, Pigeonhole::ScanPosition.from_bytes(sp.to_bytes)
    assert_equal sp.hash, Pigeonhole::ScanPosition.from_bytes(sp.to_bytes).hash
    assert sp.eql?(Pigeonhole::ScanPosition.from_bytes(sp.to_bytes))
    assert_equal kp, Pigeonhole::KeysPosition.from_bytes(kp.to_bytes)
    refute_equal sp, kp
    assert_raises(Pigeonhole::InvalidArgument) { Pigeonhole::ScanPosition.from_bytes(kp.to_bytes) }
    assert_raises(Pigeonhole::InvalidArgument) { Pigeonhole::KeysPosition.from_bytes(sp.to_bytes) }
    assert_raises(Pigeonhole::InvalidArgument) { Pigeonhole::ScanPosition.from_bytes("not a position") }
    other = db.table("other").family("a").create
    other.put("doc:000", "a", "q", "v")
    assert_raises(Pigeonhole::InvalidArgument) { other.scan_within(bytes: 1000, after: sp) }
    assert_raises(Pigeonhole::InvalidArgument) { other.multi_get_within(["doc:000"], bytes: 1000, after: kp) }
  end

  def graph(name)
    g = db.table(name).family("out").create
    "bcdefgh".each_char { |b| g.mutate("n:a").put("out", "n:#{b}", "").commit }
    g
  end

  def test_a_traversal_stops_at_its_byte_budget
    g = graph("g")
    full = g.traverse(["n:a"], "out")
    refute full.cut_by_budget
    assert_equal 7, full.hops[0].size
    # Each node counts its 3-byte key and 16: 3 fit in 60 bytes.
    cut = g.traverse(["n:a"], "out", byte_budget: 60)
    assert cut.cut_by_budget
    assert_equal full.hops[0].nodes.first(3), cut.hops[0].nodes
  end

  # perf287's S2 on #1330: with parents: true each node counts 4 more bytes.
  def test_parents_add_four_bytes_a_node_to_a_traversals_budget
    g = graph("gp")
    plain = g.traverse(["n:a"], "out", byte_budget: 60)
    with_parents = g.traverse(["n:a"], "out", byte_budget: 60, parents: true)
    assert_equal [3, true], [plain.hops[0].size, plain.cut_by_budget]
    assert_equal [2, true], [with_parents.hops[0].size, with_parents.cut_by_budget]
    assert_equal [0, 0], with_parents.hops[0].parents
    assert_raises(ArgumentError) { g.traverse(["n:a"], "out", byte_budget: 0) }
    refute g.traverse(["n:a"], "out", byte_budget: nil).cut_by_budget
  end

  def test_nearest_within_keeps_the_nearest_that_fit
    v = db.table("v").family("e", Pigeonhole::Family.vector(2, :f32, :l2)).create
    10.times { |i| v.mutate("r#{i}").put_vector("e", "a", [i.to_f, 0.0]).commit }
    near = v.nearest("e", [0.0, 0.0], 5)
    # Each neighbour is a 2-byte key, a 1-byte qualifier and 16: 3 fit in 60.
    page = v.nearest_within("e", [0.0, 0.0], 5, bytes: 60)
    assert_equal near.first(3), page.neighbours
    assert_equal [57, true, 19, 3], [page.bytes, page.cut_by_budget, page.next_bytes, page.size]
    whole = v.nearest_within("e", [0.0, 0.0], 5, bytes: 1 << 20)
    assert_equal near, whole.neighbours
    refute whole.cut_by_budget
    assert_nil whole.next_bytes
  end

  # D308's page over cells with several versions per column: a column is whole or not at
  # all, and the first column that doesn't fit gives next_bytes, its row key included.
  def model_columns(flat, budget)
    cols = []
    flat.each do |key, f, q, v|
      if !cols.empty? && cols.last[0] == [key, f, q]
        cols.last[1] << v
      else
        cols << [[key, f, q], [v]]
      end
    end
    out = []
    used = 0
    last_key = nil
    cols.each do |(key, f, q), vs|
      size = vs.sum { |v| q.bytesize + v.bytesize + OVERHEAD }
      need = size + (key == last_key ? 0 : key.bytesize)
      return [out, used, size + key.bytesize] if used + need > budget

      out += vs.map { |v| [key, f, q, v] }
      used += need
      last_key = key
    end
    [out, used, nil]
  end

  # perf287's S1 on #1330: with versions: 3 a column is up to three cells, kept or left out
  # together, and next_bytes is the whole column's size.
  def test_a_column_of_several_versions_is_kept_whole
    t = db.table("vers").family("f", Pigeonhole::Family.new(max_versions: 3)).create
    rng = Random.new(13_083)
    30.times do |i|
      (1 + rng.rand(3)).times do |q|
        [1, 2, 3].first(1 + rng.rand(3)).each do |ts|
          t.put(format("r:%02d", i), "f", "q#{q}", "\0" * (1 + rng.rand(119)), ts: ts)
        end
      end
    end
    whole = cells(t.scan("r:", versions: 3))
    assert_operator whole.size, :>, whole.map { |k, f, q, _| [k, f, q] }.uniq.size
    [30, 150, 400, 1000].each do |budget|
      got = []
      after = nil
      loop do
        page = t.scan_within("r:", bytes: budget, versions: 3, after: after)
        want, used, nxt = model_columns(whole[got.size..], budget)
        assert_equal want, cells(page.rows), budget
        assert_equal [used, nxt], [page.bytes, page.next_bytes], budget
        got += cells(page.rows)
        break if page.resume.nil? || page.rows.empty?

        after = page.resume
      end
      assert_equal whole, got if budget >= 1000
    end
  end
end
