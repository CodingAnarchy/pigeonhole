# frozen_string_literal: true

# Random puts, deletes and reads against a Hash model, seeded: the seed is printed on
# failure (and PIGEONHOLE_SEED=<n> replays it). The Ruby counterpart of
# crates/python/tests/test_model.py, at the API's surface (the engine's own model checks cover
# crashes and concurrency).
require_relative "test_helper"

class TestModel < Minitest::Test
  include PigeonholeTest

  KEYS = ["a", "b", "c", "\xff\x00".b, "long" * 8].freeze
  QUALS = ["x", "y", ""].freeze
  CASES = Integer(ENV.fetch("PROPTEST_CASES", "20"))

  def random_value(rng)
    case rng.rand(3)
    when 0 then Array.new(rng.rand(65)) { rng.rand(256) }.pack("C*")
    when 1 then rng.rand(-(2**63)...(2**63))
    else (rng.rand - 0.5) * (10**rng.rand(-5..300))
    end
  end

  def expected(v)
    v.is_a?(String) ? v.b : v
  end

  def run_case(seed, n)
    rng = Random.new(seed)
    model = {}
    path = File.join(@dir, "m#{n}.phdb")
    Pigeonhole.open(path, create: true, shards: 1) do |d|
      t = d.table("t").family("f").create
      rng.rand(41).times do
        case rng.rand(4)
        when 0
          k = KEYS.sample(random: rng)
          q = QUALS.sample(random: rng)
          v = random_value(rng)
          t.put(k, "f", q, v)
          model[[k.b, q.b]] = expected(v)
        when 1
          k = KEYS.sample(random: rng)
          q = QUALS.sample(random: rng)
          t.delete(k, "f", q)
          model.delete([k.b, q.b])
        when 2
          k = KEYS.sample(random: rng).b
          t.delete(k)
          model.delete_if { |(mk, _), _| mk == k }
        else
          d.flush
        end
      end
      model.each { |(k, q), v| assert_equal v, t.get(k, "f", q).typed }
      scanned = {}
      t.scan.each { |r| r.each { |_, q, c| scanned[[r.key, q]] = c.typed } }
      assert_equal model, scanned
    end
  end

  def test_reads_match_a_hash_model
    base = Integer(ENV.fetch("PIGEONHOLE_SEED", Random.new_seed.to_s)) & ((2**63) - 1)
    CASES.times do |n|
      seed = base + n
      begin
        run_case(seed, n)
      rescue Minitest::Assertion, StandardError => e
        raise e.class, "#{e.message}\n(seed #{seed}: PIGEONHOLE_SEED=#{seed} PROPTEST_CASES=1 replays it)",
              e.backtrace
      end
    end
  end
end
