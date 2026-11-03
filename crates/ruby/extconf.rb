# frozen_string_literal: true

# Builds the native extension from this crate (`pigeonhole-ruby`, whose library target is
# `pigeonhole_native`) with rb_sys. It sits beside Cargo.toml, so the gem's root is the crate.
require "mkmf"
require "rb_sys/mkmf"

create_rust_makefile("pigeonhole/pigeonhole_native")
