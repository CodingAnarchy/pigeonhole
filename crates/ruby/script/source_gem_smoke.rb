# frozen_string_literal: true

# A smoke test for an installed gem, the source gem in particular (ruby-gems.yml's
# source-gem job, #697): run from outside the repo, it loads the installed `pigeonhole-db`
# gem, not lib/, and exercises a write, a read, a scan and a vector search.
require "pigeonhole"
require "tmpdir"

abort "loaded #{$LOADED_FEATURES.grep(/pigeonhole\.rb\z/).first}, not an installed gem" unless
  Gem.loaded_specs.key?("pigeonhole-db")

Dir.mktmpdir("phdb-smoke") do |dir|
  Pigeonhole.open(File.join(dir, "smoke.phdb"), create: true, shards: 1) do |db|
    t = db.table("t")
          .family("f")
          .family("emb", Pigeonhole::Family.vector(2, :f32, :l2))
          .create
    t.put("a", "f", "q", "1")
    t.mutate("b").put("f", "q", "2").put_vector("emb", "v", [1.0, 0.0]).commit
    t.mutate("c").put_vector("emb", "v", [0.0, 1.0]).commit
    raise "get" unless t.get("a", "f", "q").value == "1"
    raise "scan" unless t.scan.map(&:key) == %w[a b c]
    raise "nearest" unless t.nearest("emb", [0.9, 0.1], 1).map(&:row) == ["b"]
  end
end
puts "pigeonhole-db #{Pigeonhole::VERSION}: the installed gem works"
