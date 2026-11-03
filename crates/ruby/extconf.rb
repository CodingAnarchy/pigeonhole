# frozen_string_literal: true

# Builds the native extension from this crate (`pigeonhole-ruby`, whose library target is
# `pigeonhole_native`) with rb_sys. It sits beside Cargo.toml, so the gem's root is the crate.
require "mkmf"
require "rb_sys/mkmf"

# `--locked`: the build uses Cargo.lock's versions, and a stale lock fails rather than being
# rewritten (a source gem's user gets the versions CI tested).
create_rust_makefile("pigeonhole/pigeonhole_native") do |r|
  r.extra_cargo_args += ["--locked"]
end
