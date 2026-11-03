# frozen_string_literal: true

# Every `ruby` block in docs/guide/ruby.md runs, each with `path` set to a new database file,
# so the guide can't drift from the API.
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
end
