# frozen_string_literal: true

require_relative "lib/pigeonhole/version"

# A source gem isn't installable yet: the crate depends on `../pigeonhole` by path, which a
# gem can't carry. Until the source-gem PR (#697), only native gems are built
# (`rake native:<platform> gem`, ruby-gems.yml); a plain `gem build` refuses (ai1249's P2 on
# #1399). Bundler and rake load this file too, and are unaffected.
if File.basename($PROGRAM_NAME) == "gem" && ARGV.first == "build"
  abort "pigeonhole: the source gem isn't supported yet (its Cargo.toml depends on " \
        "../pigeonhole); build a native gem with `bundle exec rake native:<platform> gem`"
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
  spec.files = Dir["lib/**/*.rb", "sig/**/*.rbs", "extconf.rb", "src/**/*.rs", "Cargo.toml", "Cargo.lock", "README.md", "LICENSE*"]
  spec.require_paths = ["lib"]
  spec.extensions = ["extconf.rb"]
  spec.add_dependency "rb_sys", "~> 0.9"
end
