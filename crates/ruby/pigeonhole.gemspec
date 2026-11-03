# frozen_string_literal: true

require_relative "lib/pigeonhole/version"

# A source gem compiles the crate on the user's machine, and the crate depends on
# `../pigeonhole` by path, which a gem can't carry. `rake gem:source` stages one with the path
# crates vendored under vendor/ (rakelib/source_gem.rake, #697) and builds it there; a plain
# `gem build` here, without vendor/, refuses (ai1249's P2 on #1399). Native gems are
# `rake native:<platform> gem` (ruby-gems.yml). Bundler and rake load this file too, and are
# unaffected.
vendored = File.directory?(File.join(__dir__, "vendor"))
if !vendored && File.basename($PROGRAM_NAME) == "gem" && ARGV.first == "build"
  abort "pigeonhole: build the source gem with `bundle exec rake gem:source` (its Cargo.toml " \
        "depends on ../pigeonhole), or a native gem with `bundle exec rake native:<platform> gem`"
end

Gem::Specification.new do |spec|
  spec.name = "pigeonhole"
  spec.version = Pigeonhole::VERSION
  spec.summary = "An embedded, single-file, wide-column store: BigTable's data model, SQLite's deployment model."
  spec.homepage = "https://github.com/CodingAnarchy/pigeonhole"
  spec.license = "MIT"
  spec.authors = ["Matt Tanous"]
  spec.required_ruby_version = ">= 3.3"
  spec.metadata = {
    "source_code_uri" => "https://github.com/CodingAnarchy/pigeonhole/tree/main/crates/ruby",
    "documentation_uri" => "https://github.com/CodingAnarchy/pigeonhole/blob/main/docs/guide/ruby.md",
    "rubygems_mfa_required" => "true",
  }
  files = ["lib/**/*.rb", "sig/**/*.rbs", "extconf.rb", "src/**/*.rs", "Cargo.toml", "Cargo.lock", "README.md", "LICENSE*"]
  files << "vendor/**/*" if vendored
  spec.files = Dir.glob(files, base: __dir__).select { |f| File.file?(File.join(__dir__, f)) }.sort
  spec.require_paths = ["lib"]
  spec.extensions = ["extconf.rb"]
  spec.add_dependency "rb_sys", "~> 0.9"
end
