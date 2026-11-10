# frozen_string_literal: true

# Pigeonhole: an embedded, single-file, wide-column store (BigTable's data model, SQLite's
# deployment model). See the Ruby guide: docs/guide/ruby.md.
require_relative "pigeonhole/version"

begin
  ruby_version = RUBY_VERSION[/\d+\.\d+/]
  require_relative "pigeonhole/#{ruby_version}/pigeonhole_native"
rescue LoadError
  require_relative "pigeonhole/pigeonhole_native"
end

module Pigeonhole
  # A Pigeonhole error: `code` is the stable `ErrorCode` name, `code_num` its number. Each
  # code has its subclass, named as the code (`Pigeonhole::Conflict`, ...). A plain
  # `Pigeonhole::Error`, with no code, is the binding's own: a mutation, batch or
  # transaction used after it was committed, discarded or rolled back.
  class Error
    # The stable `ErrorCode` name (a String) and its number.
    attr_reader :code, :code_num

    def initialize(message = nil, code = nil, code_num = nil)
      super(message)
      @code = code
      @code_num = code_num
    end
  end

  # A counter's value couldn't be read after its commit applied (Table#incr_returning,
  # Incremented's readers): the commit succeeded, so don't retry it. `seqno` is the commit's;
  # `code`, `code_num` and `cause` are the read's error. A failed commit raises its own
  # exception instead.
  class ReturnedValueError
    # The commit's sequence number.
    attr_reader :seqno

    def initialize(message = nil, code = nil, code_num = nil, seqno = nil)
      super(message, code, code_num)
      @seqno = seqno
    end
  end
end

require_relative "pigeonhole/fiber"
require_relative "pigeonhole/api"
require_relative "pigeonhole/memory"
require_relative "pigeonhole/index"
