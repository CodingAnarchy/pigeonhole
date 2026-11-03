# frozen_string_literal: true

# `rake gem:source`: the source gem, which compiles this crate on the user's machine (#697,
# ICR 0111; the design note and ai1249's review are on #697). The crate depends on the engine
# by path (`../pigeonhole`), which a gem can't carry, so the task stages a copy of the gem in
# pkg/source/ with the path crates vendored under vendor/, as maturin's sdist does for the
# Python package, and builds the gem there. Nothing is published.
#
# Each vendored crate is its tracked files, without tests/, benches/, examples/ and fuzz/, and with a
# manifest that stands alone: the workspace inheritance resolved from the root Cargo.toml,
# path dependencies pointing at the vendored copies, the dev-only sections dropped, and of
# `[lints]` only the check-cfg list kept. crates/ruby/Cargo.lock is copied unchanged: path crates carry no checksum, so it
# still describes the build, and `cargo metadata --locked` in the staged dir proves it.

require "fileutils"
require "json"
require "open3"

module SourceGem
  RUBY_DIR = File.expand_path("..", __dir__)
  ROOT = File.expand_path("../../..", __dir__)
  STAGE = File.join(RUBY_DIR, "pkg", "source")

  # Manifest sections a user's build never reads (with tests/, benches/ and examples/ gone,
  # the explicit targets would name missing files).
  DROPPED = %w[dev-dependencies bench test example badges].freeze
  DEPENDENCY_TABLES = %w[dependencies build-dependencies].freeze
  # Dev-only directories, left out of each vendored crate (`fuzz/` is a cargo-fuzz package of
  # its own).
  DEV_DIRS = %w[tests benches examples fuzz].freeze
  # Dev-only files at a crate's root (perf2's nit 3 on #1414).
  DEV_FILES = %w[public-api.txt].freeze
  # Package fields a vendored crate must state itself, so cargo enforces them.
  REQUIRED = %w[name version edition rust-version license].freeze

  module_function

  def toml
    require "toml-rb"
    TomlRB
  end

  def run(*cmd, chdir:)
    out, status = Open3.capture2(*cmd, chdir: chdir)
    raise "#{cmd.join(' ')} failed in #{chdir}" unless status.success?

    out
  end

  # The packages `cargo metadata --no-deps` reports in `dir`, by name.
  def packages(dir)
    out = run("cargo", "metadata", "--no-deps", "--format-version", "1", chdir: dir)
    JSON.parse(out)["packages"].to_h { |p| [p["name"], p] }
  end

  # The path dependencies `package` declares as normal or build dependencies, optional ones
  # included (ai1249's P1 on #697): a resolve lists only the ones a build activates, but cargo
  # loads every declared path dependency's manifest.
  def path_deps(package)
    package["dependencies"].select { |d| d["path"] && [nil, "build"].include?(d["kind"]) }
  end

  # The path crates this crate needs, as name => directory name under crates/.
  def closure
    workspace = packages(ROOT)
    todo = path_deps(packages(RUBY_DIR).fetch("pigeonhole-ruby")).map { |d| d["name"] }
    dirs = {}
    until todo.empty?
      name = todo.shift
      next if dirs.key?(name)

      package = workspace.fetch(name) { raise "#{name} isn't a member of the root workspace" }
      dirs[name] = File.basename(File.dirname(package["manifest_path"]))
      todo.concat(path_deps(package).map { |d| d["name"] })
    end
    dirs
  end

  # `spec` with a `workspace = true` dependency resolved from the root's
  # `[workspace.dependencies]`, and a path pointing at its vendored copy (`prefix` + its
  # directory).
  def dependency(key, spec, inherited, dirs, prefix)
    spec = { "version" => spec } if spec.is_a?(String)
    spec = spec.dup
    if spec.delete("workspace")
      base = inherited.fetch(key) { raise "#{key} isn't in [workspace.dependencies]" }
      base = base.is_a?(String) ? { "version" => base } : base.dup
      features = Array(base["features"]) | Array(spec["features"])
      spec = base.merge(spec)
      spec["features"] = features unless features.empty?
    end
    if spec.key?("path")
      name = spec["package"] || key
      spec["path"] = "#{prefix}#{dirs.fetch(name) { raise "#{name} isn't vendored" }}"
    end
    spec
  end

  # Each dependency table of `manifest`, its target-specific ones included.
  def dependency_tables(manifest)
    tables = DEPENDENCY_TABLES.filter_map { |t| manifest[t] }
    manifest.fetch("target", {}).each_value do |target|
      target.delete("dev-dependencies")
      tables.concat(DEPENDENCY_TABLES.filter_map { |t| target[t] })
    end
    tables
  end

  # The vendored crate `dir`'s manifest: standalone, as described at the top.
  def manifest(dir, root, dirs)
    m = toml.load_file(File.join(ROOT, "crates", dir, "Cargo.toml"))
    package = m.fetch("package")
    inherited = root.fetch("workspace").fetch("package")
    package.each do |field, value|
      next unless value.is_a?(Hash) && value["workspace"]

      package[field] = inherited.fetch(field) { raise "#{dir}: #{field} isn't in [workspace.package]" }
    end
    # A crate's README is tracked in its own directory, so it's vendored; drop the field only
    # if a crate ever names one that isn't.
    readme = package["readme"]
    package.delete("readme") if readme.is_a?(String) && !File.file?(File.join(ROOT, "crates", dir, readme))
    missing = REQUIRED.reject { |f| package.key?(f) }
    raise "#{dir}: the manifest doesn't state #{missing.join(', ')}" unless missing.empty?

    DROPPED.each { |s| m.delete(s) }
    if m.key?("lints")
      # A crate's own lint table would be replaced silently (ai1249's N5 on #1414).
      raise "#{dir}: [lints] isn't `workspace = true`; teach the task about it" unless m["lints"] == { "workspace" => true }

      m["lints"] = lints(root)
    end
    deps = root.fetch("workspace").fetch("dependencies")
    dependency_tables(m).each do |table|
      table.each { |key, spec| table[key] = dependency(key, spec, deps, dirs, "../") }
    end
    m
  end

  # A vendored crate's `[lints]`, for `lints.workspace = true`, which has no workspace to
  # inherit from outside the repo. It keeps only `unexpected_cfgs`, with its check-cfg list
  # (simp724's S1 on #697: without it, the `cfg(loom)` code warns on every user's build), at
  # no more than "warn". The rest goes (ai1249's P3): a `deny` level could fail a user's build
  # on a newer toolchain, and the other lints don't concern a user.
  def lints(root)
    cfgs = root.dig("workspace", "lints", "rust", "unexpected_cfgs")
    return {} unless cfgs.is_a?(Hash)

    { "rust" => { "unexpected_cfgs" => cfgs.merge("level" => "warn") } }
  end

  # Fails unless every path dependency in `manifest` (at `dir` in the staged gem) resolves to
  # a vendored crate (simp724's note on #697): one reaching back into the repo would build
  # here and fail on a user's machine.
  def check_paths(manifest, dir)
    vendor = File.join(STAGE, "vendor")
    dependency_tables(manifest).each do |table|
      table.each_value do |spec|
        next unless spec.is_a?(Hash) && spec["path"]

        target = File.expand_path(spec["path"], dir)
        unless File.dirname(target) == vendor && File.file?(File.join(target, "Cargo.toml"))
          raise "#{dir}: the path dependency #{spec['path']} isn't a vendored crate"
        end
      end
    end
  end

  # A `#[cfg(doctest)]` module's body as the crates write it: `#[doc = include_str!(..)]`
  # attributes on unit structs, `#[cfg(..)]` attributes and comments, none with a brace.
  DOCTEST_BODY = %r{\A(?:\s+|//[^\n]*|#\[doc\s*=\s*include_str!\(\s*"[^"]*"\s*\)\]|#\[cfg\([^\]{}]*\)\]|struct\s+\w+\s*;)*\z}

  # `src` with each `#[cfg(doctest)] mod name { ... }` block removed: they compile only for
  # doctests, so what they include (the guide, the root README) never reaches a gem build.
  # The end of a block is found by counting braces, which is only sound for bodies of doc
  # attributes, so any other body fails staging rather than being misread (ai1249's N2 on
  # #1414).
  def without_doctests(src, file)
    out = +""
    rest = src
    while (m = rest.match(/#\[cfg\(doctest\)\]\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+\w+\s*\{/))
      out << m.pre_match
      depth = 1
      i = m.end(0)
      while depth.positive? && i < rest.length
        depth += 1 if rest[i] == "{"
        depth -= 1 if rest[i] == "}"
        i += 1
      end
      body = rest[m.end(0)...(i - 1)]
      unless depth.zero? && body.match?(DOCTEST_BODY)
        raise "#{file}: a #[cfg(doctest)] module holds more than doc includes; teach the task about it"
      end

      rest = rest[i..] || ""
    end
    out << rest
  end

  INCLUDE = /\binclude(?:_str|_bytes)?!\s*\(\s*(concat!\s*\(\s*env!\s*\(\s*"CARGO_MANIFEST_DIR"\s*\)\s*,\s*)?"([^"]+)"/
  PATH_ATTR = /#\[path\s*=\s*"([^"]+)"\]/

  # Fails, naming them, on files a vendored crate's code includes that aren't in the vendored
  # copy: one outside the crate's directory, or in a dropped tests/, benches/ or examples/
  # (ai1249's P2 and Q2 on #697). Otherwise that build would break only on a user's machine.
  def check_includes(crate_dir)
    bad = []
    Dir.glob("**/*.rs", base: crate_dir).each do |rel|
      file = File.join(crate_dir, rel)
      src = without_doctests(File.read(file), rel)
      targets = src.scan(INCLUDE).map do |manifest_dir, path|
        manifest_dir ? File.join(crate_dir, path) : File.expand_path(path, File.dirname(file))
      end
      targets += src.scan(PATH_ATTR).map { |(path)| File.expand_path(path, File.dirname(file)) }
      targets.each do |t|
        bad << "#{rel} includes #{t}" unless t.start_with?("#{crate_dir}/") && File.file?(t)
      end
    end
    raise "#{File.basename(crate_dir)}: #{bad.join('; ')}" unless bad.empty?
  end

  # Copies crate `dir`'s tracked files, less the dev-only directories, into `to`.
  def copy_crate(dir, to)
    files = run("git", "ls-files", "-z", "crates/#{dir}", chdir: ROOT).split("\0")
    files.each do |f|
      rel = f.delete_prefix("crates/#{dir}/")
      next if DEV_DIRS.include?(rel.split("/").first) || DEV_FILES.include?(rel)

      dest = File.join(to, rel)
      FileUtils.mkdir_p(File.dirname(dest))
      FileUtils.cp(File.join(ROOT, f), dest)
    end
    license = File.join(to, "LICENSE")
    FileUtils.cp(File.join(ROOT, "LICENSE"), license) unless File.file?(license)
  end

  HEADER = "# Generated by `rake gem:source` (crates/ruby/rakelib/source_gem.rake): %s\n\n"

  def stage
    # pkg/source is this task's own output; it starts empty each time.
    FileUtils.rm_rf(File.join(RUBY_DIR, "pkg", "source"))
    FileUtils.mkdir_p(STAGE)
    gemspec = Gem::Specification.load(File.join(RUBY_DIR, "pigeonhole.gemspec"))
    (gemspec.files + ["pigeonhole.gemspec"]).each do |f|
      FileUtils.mkdir_p(File.join(STAGE, File.dirname(f)))
      FileUtils.cp(File.join(RUBY_DIR, f), File.join(STAGE, f))
    end

    root = toml.load_file(File.join(ROOT, "Cargo.toml"))
    dirs = closure
    manifests = {}
    dirs.each_value do |dir|
      to = File.join(STAGE, "vendor", dir)
      copy_crate(dir, to)
      vendored = manifest(dir, root, dirs)
      File.write(File.join(to, "Cargo.toml"), format(HEADER, "a vendored copy of crates/#{dir}") + toml.dump(vendored))
      check_includes(to)
      manifests[to] = vendored
    end

    # The gem's own manifest: crates/ruby's, its path dependencies on the vendored copies, and
    # the vendored crates kept out of its workspace.
    m = toml.load_file(File.join(RUBY_DIR, "Cargo.toml"))
    dependency_tables(m).each do |table|
      table.each { |key, spec| table[key] = dependency(key, spec, {}, dirs, "vendor/") }
    end
    m["workspace"] = { "exclude" => ["vendor"] }
    # A user's build keeps the release profile's optimisations but not its debug info, which
    # only slows the link and grows the installed extension (ai1249's N4 on #1414).
    m.dig("profile", "release")&.delete("debug")
    File.write(File.join(STAGE, "Cargo.toml"),
               format(HEADER, "crates/ruby/Cargo.toml for the source gem") + toml.dump(m))
    manifests[STAGE] = m
    manifests.each { |dir, manifest| check_paths(manifest, dir) }

    # The lock still describes the build: no path crate's version or dependencies changed.
    run("cargo", "metadata", "--locked", "--format-version", "1", chdir: STAGE)
  end
end

namespace :gem do
  desc "Build the source gem in pkg/source/, with the engine's path crates vendored (#697)"
  task :source do
    SourceGem.stage
    # Outside Bundler's environment: under `bundle exec`, the subprocess would load the
    # checkout's gemspec (the Gemfile's `gemspec`) and then the staged one, defining
    # Pigeonhole::VERSION twice (perf2's nit 1 on #1414).
    Bundler.with_unbundled_env do
      Dir.chdir(SourceGem::STAGE) { sh "gem", "build", "pigeonhole.gemspec" }
    end
  end
end
