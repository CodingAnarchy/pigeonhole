# frozen_string_literal: true

# ai1249's P5 on #1399: the binding knows every ErrorCode, by its stable name and number,
# so no engine error falls back to a plain Pigeonhole::Error.
require_relative "test_helper"

class TestErrorCodes < Minitest::Test
  ERROR_RS = File.expand_path("../../pigeonhole/src/error.rs", __dir__)

  # `ErrorCode`'s variants and numbers, from its definition.
  def engine_codes
    src = File.read(ERROR_RS)
    body = src[/pub enum ErrorCode \{(.*?)\n\}/m, 1]
    refute_nil body, "ErrorCode's definition not found in #{ERROR_RS}"
    body.scan(/^\s+([A-Z]\w*) = (\d+),/).to_h { |name, num| [name, Integer(num)] }
  end

  def test_every_engine_code_has_its_class_name_and_number
    native = Pigeonhole::Native::ERROR_CODES
    assert_equal engine_codes, native
    native.each_key do |name|
      klass = Pigeonhole.const_get(name)
      assert_operator klass, :<, Pigeonhole::Error, name
    end
  end

  def test_spent_error_is_an_uncoded_error
    assert_operator Pigeonhole::SpentError, :<, Pigeonhole::Error
    refute Pigeonhole::Native::ERROR_CODES.key?("SpentError")
  end
end
