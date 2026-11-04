# frozen_string_literal: true

# The gem's name and its require path differ (`pigeonhole-db` and `require "pigeonhole"`,
# owner decision 2026-11-04). Bundler.require (Rails) requires a gem by its name and
# swallows the LoadError, so a plain `gem "pigeonhole-db"` must load the library too
# (ai1249 on #1481).
require_relative "test_helper"

class TestPackaging < Minitest::Test
  include PigeonholeTest

  GEMSPEC = File.expand_path("../pigeonhole-db.gemspec", __dir__)

  def test_requiring_the_gem_by_its_name_loads_pigeonhole
    out = IO.popen(ruby_child_cmd('require "pigeonhole-db"; print defined?(Pigeonhole::Database)'), &:read)
    assert_equal "constant", out
  end

  def test_the_gem_ships_the_file_bundler_requires
    spec = Gem::Specification.load(GEMSPEC)
    assert_equal "pigeonhole-db", spec.name
    assert_includes spec.files, "lib/pigeonhole-db.rb"
    assert_includes spec.files, "lib/pigeonhole.rb"
  end

  # Bundler.require's own lookup: the gem's name, then its name with "-" as "/". The first
  # must resolve, or a plain `gem "pigeonhole-db"` line loads nothing.
  def test_bundler_requires_name_resolves
    out = IO.popen(ruby_child_cmd(<<~RUBY), &:read)
      begin
        require "pigeonhole-db"
      rescue LoadError
        begin
          require "pigeonhole/db"
        rescue LoadError
          nil
        end
      end
      print defined?(Pigeonhole) ? "loaded" : "not loaded"
    RUBY
    assert_equal "loaded", out
  end
end
