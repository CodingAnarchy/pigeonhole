# frozen_string_literal: true

# Mirrors crates/python/tests/test_vectors.py: vector families (D303), vectors written from
# Arrays (or packed bits) and read back, and nearest-neighbour search (D296).
require_relative "test_helper"

class TestVectors < Minitest::Test
  include PigeonholeTest

  def t
    @t ||= db.table("v")
             .family("emb", Pigeonhole::Family.vector(4, :f32, :cosine))
             .family("q8", Pigeonhole::Family.vector(3, :i8, :l2))
             .family("sig", Pigeonhole::Family.vector(16, "bit", "hamming"))
             .family("plain")
             .create
  end

  def test_a_vector_family_stores_its_spec
    spec = t.family_info("emb").vector
    assert_equal [4, :f32, :cosine], [spec.dim, spec.element, spec.metric]
    assert_nil t.family_info("plain").vector
    assert_equal 8, Pigeonhole::VectorSpec.new(8, :i8, :inner_product).dim
    assert_equal Pigeonhole::VectorSpec.new(4, :f32, :cosine), spec
    assert spec.eql?(Pigeonhole::VectorSpec.new(4, :f32, :cosine))
    assert_equal 1, [spec, Pigeonhole::VectorSpec.new(4, :f32, :cosine)].uniq.size
    assert_match(/dim=4 element=:f32 metric=:cosine/, spec.inspect)
    assert_raises(ArgumentError) { Pigeonhole::Family.vector(4, :f64, :l2) }
    assert_raises(ArgumentError) { Pigeonhole::VectorSpec.new(4, :f32, :manhattan) }
    assert_bad_call(TypeError) { Pigeonhole::VectorSpec.new(4, 32, :l2) }
  end

  def test_vectors_round_trip
    t.mutate("a").put_vector("emb", "x", [1.0, 2.0, 3.0, 4.5]).commit
    t.mutate("b").put_vector("emb", "x", [1, 0, 0, -1]).commit
    t.mutate("c").put_vector("q8", "x", [1, -2, 127]).put_vector("sig", "x", "\xFF\x01".b).commit
    a = t.get("a", "emb", "x")
    assert_equal [1.0, 2.0, 3.0, 4.5], a.as_vector
    assert_equal a.as_vector, a.typed
    assert_equal [1.0, 0.0, 0.0, -1.0], t.get("b", "emb", "x").as_vector
    assert_equal [1, -2, 127], t.get("c", "q8", "x").as_vector
    bits = t.get("c", "sig", "x").as_vector
    assert_equal "\xFF\x01".b, bits
    assert_equal Encoding::BINARY, bits.encoding
    t.put("p", "plain", "x", "v")
    assert_nil t.get("p", "plain", "x").as_vector
    # Batches and transactions write vectors too.
    db.write_batch { |wb| wb.put_vector(t, "d", "emb", "x", [0, 0, 0, 1]) }
    db.transaction { |txn| txn.put_vector(t, "e", "q8", "x", [3, 2, 1], ts: 7) }
    assert_in_delta 1.0, t.get("d", "emb", "x").as_vector[3]
    e = t.get("e", "q8", "x")
    assert_equal [[3, 2, 1], 7], [e.as_vector, e.ts]
  end

  def test_wrong_vectors_are_refused
    assert_bad_call(TypeError) { t.mutate("a").put_vector("plain", "x", [1.0]) }
    assert_raises(Pigeonhole::FamilyNotFound) { t.mutate("a").put_vector("missing", "x", [1.0]) }
    # A String is a bit vector's form, and an Array a numeric one's: neither converts.
    assert_bad_call(TypeError) { t.mutate("a").put_vector("emb", "x", "\x00\x00\x80\x3F".b) }
    assert_bad_call(TypeError) { t.mutate("a").put_vector("q8", "x", "\x01\x02\x03") }
    assert_bad_call(TypeError) { t.mutate("a").put_vector("sig", "x", [1, 0]) }
    assert_bad_call(TypeError) { t.mutate("a").put_vector("emb", "x", [1.0, "2", 3.0, 4.0]) }
    assert_bad_call(TypeError) { t.mutate("a").put_vector("q8", "x", [1, 2.5, 3]) }
    assert_raises(RangeError) { t.mutate("a").put_vector("q8", "x", [1, 2, 128]) }
    # The dimension is the family's: a wrong one fails the commit.
    assert_raises(Pigeonhole::Error) { t.mutate("a").put_vector("emb", "x", [1.0, 2.0]).commit }
    assert_nil t.get("a", "emb", "x")
  end

  def l2(v, q)
    Math.sqrt(v.zip(q).sum { |a, b| (a - b)**2 })
  end

  # The values f32 storage holds.
  def f32(v)
    v.pack("e*").unpack("e*")
  end

  def test_nearest_equals_a_brute_force_over_a_range_or_keys
    rng = Random.new(1259)
    n = db.table("n").family("emb", Pigeonhole::Family.vector(8, :f32, :l2)).create
    vecs = (0...200).to_h { |i| [format("r%03d", i), Array.new(8) { rng.rand(-1.0..1.0) }] }
    db.write_batch { |wb| vecs.each { |row, v| wb.put_vector(n, row, "emb", "v", v) } }
    query = Array.new(8) { rng.rand(-1.0..1.0) }
    ranked = ->(rows) { rows.sort_by { |r| [l2(f32(vecs[r]), f32(query)), r] } }

    got = n.nearest("emb", query, 5)
    assert_equal ranked.(vecs.keys).first(5), got.map(&:row)
    assert(got.all? { |nb| nb.qualifier == "v" && nb.ts.positive? })
    assert_equal got.map(&:distance).sort, got.map(&:distance)
    assert got.frozen? && got.first.frozen?
    assert got.first.row.frozen? && got.first.qualifier.frozen?
    assert_equal got.first.row, n.nearest("emb", query, 5, parallel: 2).first.row
    in_range = ranked.(vecs.keys.select { |r| r >= "r050" && r < "r100" })
    assert_equal in_range.first(3), n.nearest("emb", query, 3, start: "r050", end: "r100").map(&:row)
    some = %w[r007 r150 r199 r999]
    assert_equal ranked.(some & vecs.keys), n.nearest("emb", query, 10, keys: some).map(&:row)
    assert_equal ranked.(some & vecs.keys), n.nearest("emb", query, 10, keys: Set.new(some)).map(&:row)
    assert_bad_call(TypeError) { n.nearest("emb", "bad", 1) }
    assert_raises(ArgumentError) { n.nearest("emb", query, 1, prefix: "r", keys: ["r001"]) }
    assert_raises(ArgumentError) { n.nearest("emb", query, 1, prefix: "r", start: "r001") }
    assert_bad_call(TypeError) { db.table("v2").family("p").create.nearest("p", query, 1) }
    # Neighbours compare by value.
    assert_equal got.first(2), n.nearest("emb", query, 2)
    assert_kind_of Pigeonhole::Neighbour, got.first
  end

  # #1263: half-precision families take Numerics (rounded to nearest-even) and read back
  # widened, ranking as the widened vectors do in an f32 family.
  def test_half_precision_families_round_and_widen_through_f32
    h = db.table("h")
          .family("h16", Pigeonhole::Family.vector(4, :f16, :l2))
          .family("hb", Pigeonhole::Family.vector(4, :bf16, :cosine))
          .family("w", Pigeonhole::Family.vector(4, :f32, :l2))
          .create
    assert_equal :f16, h.family_info("h16").vector.element
    assert_equal :bf16, h.family_info("hb").vector.element
    exact = [1.5, -0.25, 3.0, 0.0]
    h.mutate("a").put_vector("h16", "", exact).put_vector("hb", "", exact).commit
    assert_equal exact, h.get("a", "h16", "").as_vector
    assert_equal exact, h.get("a", "hb", "").as_vector
    # 1 + 2**-11 is the midpoint between 1 and f16's next value: ties to even (1.0).
    h.mutate("b").put_vector("h16", "", [1 + (2**-11), 1, 1, 1]).commit
    assert_in_delta 1.0, h.get("b", "h16", "").as_vector[0], 0.0
    # 65,520 overflows f16 (its largest is 65,504); bf16's range is f32's.
    assert_raises(Pigeonhole::Error) { h.mutate("c").put_vector("h16", "", [65_520.0, 0, 0, 0]).commit }
    h.mutate("c").put_vector("hb", "", [65_520.0, 1, 0, 0]).commit
    20.times do |i|
      row = format("r%02d", i)
      h.mutate(row).put_vector("h16", "", [i.to_f, (i % 3).to_f, -i.to_f, 0.5]).commit
      h.mutate(row).put_vector("w", "", h.get(row, "h16", "").as_vector).commit
    end
    q = [3.3, 1.1, -2.9, 0.4]
    got = h.nearest("h16", q, 5, prefix: "r").map { |nb| [nb.row, nb.distance] }
    assert_equal h.nearest("w", q, 5, prefix: "r").map { |nb| [nb.row, nb.distance] }, got
  end
end
