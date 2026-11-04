# frozen_string_literal: true

# Every `ruby` block in docs/guide/ruby.md runs, each with `path` set to a new database file,
# so the guide can't drift from the API, and so does the gem README's example.
require_relative "test_helper"

class TestGuide < Minitest::Test
  include PigeonholeTest

  GUIDE = File.expand_path("../../../docs/guide/ruby.md", __dir__)
  BLOCKS = File.read(GUIDE).scan(/```ruby\n(.*?)```/m).flatten

  def test_the_guide_has_blocks
    assert_operator BLOCKS.size, :>=, 4
  end

  BLOCKS.each_with_index do |code, i|
    define_method("test_guide_block_#{i}") do
      path = File.join(@dir, "guide-#{i}.phdb")
      eval(code, binding, GUIDE) # rubocop:disable Security/Eval
    end
  end

  # The gem README's example too (cli692's R3 on #1451). It names its file, so it runs in the
  # test's directory, and its `p` output is checked rather than printed.
  README = File.expand_path("../README.md", __dir__)
  README_BLOCKS = File.read(README).scan(/```ruby\n(.*?)```/m).flatten

  def test_the_readme_example_runs
    assert_equal 1, README_BLOCKS.size
    out, = capture_io do
      Dir.chdir(@dir) { eval(README_BLOCKS[0], binding, README) } # rubocop:disable Security/Eval
    end
    assert_equal %("Ada Lovelace"\n), out
  end
end
