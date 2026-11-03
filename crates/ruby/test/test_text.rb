# frozen_string_literal: true

# The full-text index (D309, ICR 0105; #697 PR 3b): writes in and out of transactions,
# queries parsed and built, phrases, fuzzy terms, highlights by byte range, prefixes,
# readers, and the maintenance calls. Mirrors crates/text/tests and the text-search guide.
require_relative "test_helper"

class TestText < Minitest::Test
  include PigeonholeTest

  T = Pigeonhole::Text

  def notes = @notes ||= db.table("notes").family("body").create

  def index(**opts) = @index ||= T::Index.create(db, "notes_text", table: notes, family: "body", qualifier: "text",
                                                                    **opts)

  def test_writes_and_searches
    i = index
    db.transact do |txn|
      txn.put(notes, "note:1", "body", "author", "ada")
      i.put_in(txn, "note:1", "We chose Postgres in the March meeting")
    end
    i.put("note:2", "The March offsite moved to April")
    assert_equal "We chose Postgres in the March meeting", notes.get("note:1", "body", "text").value
    hits = i.search("march", 10)
    assert_equal %w[note:1 note:2].sort, hits.map(&:id).sort
    assert hits.frozen? && hits[0].frozen? && hits[0].id.frozen?
    assert_equal Encoding::BINARY, hits[0].id.encoding
    assert_operator hits[0].score, :>, 0
    assert_nil hits[0].text # no highlight asked
    assert i.delete("note:2")
    refute i.delete("note:2")
    assert_equal ["note:1"], i.search("march", 10).map(&:id)
    txn = db.transaction
    assert i.delete_in(txn, "note:1")
    assert_equal ["note:1"], i.search("march", 10).map(&:id) # not committed yet
    txn.commit
    assert_empty i.search("march", 10)
  end

  def test_phrases_fuzzy_and_built_queries
    i = index
    i.put("a", "the march meeting chose postgres")
    i.put("b", "march notes from a long meeting")
    assert_equal "a", i.search('"march meeting" postgres', 10)[0].id
    sloppy = i.search('"march meeting"~4', 10)
    assert_equal 2, sloppy.size
    assert_operator sloppy[0].score, :>, sloppy[1].score
    q = T::Query.new.phrase(%w[march meeting], slop: 4)
    assert_equal sloppy, i.search(q, 10)
    # A parsed quoted phrase is its own clause kind; it searches as the built one does.
    assert_equal sloppy, i.search(T::Query.parse('"march meeting"~4'), 10)
    assert_equal T::Query.new.phrase(%w[march meeting], slop: 4), q
    refute_equal q, T::Query.new.phrase(%w[march meeting])
    fuzzy, stats = i.search_with_stats("Postgress~", 10)
    assert_equal "a", fuzzy[0].id
    assert_equal ["postgress"], stats.fuzzy.map(&:term)
    assert_operator stats.fuzzy[0].expansions, :>=, 1
    assert_equal "a", i.search(T::Query.new.fuzzy("postgress", 2), 10)[0].id
    assert_equal "a", i.search(T::Query.new.term("postgres").text("the meeting"), 10)[0].id
    assert_raises(Pigeonhole::InvalidArgument) { T::Query.parse('"unclosed') }
    assert_raises(Pigeonhole::InvalidArgument) { i.search('"unclosed', 10) } # parsed when it runs
    assert_raises(Pigeonhole::InvalidArgument) { T::Query.new.fuzzy("x", 3) }
    assert_bad_call(TypeError) { i.search(42, 10) }
  end

  # ai1249's P3 on #697: a match's range is a byte range of the hit's text.
  def test_highlights_are_byte_ranges
    i = index
    i.put("d", "Café déjà vu: the café opens")
    hit = i.search("café", 1, highlight: true)[0]
    assert_equal "Café déjà vu: the café opens", hit.text
    assert hit.text.frozen?
    assert_equal %w[Café café], hit.matches.map { |m| hit.text.byteslice(m.range) }
    assert(hit.matches.all? { |m| m.clause.zero? })
    assert_equal [0, 1], hit.matches.map(&:occurrence).sort
  end

  def test_prefixes_snapshots_and_readers
    i = index
    i.put("user:1:a", "deploy on friday")
    i.put("user:2:a", "deploy on monday")
    assert_equal ["user:1:a"], i.search("deploy", 10, id_prefix: "user:1:").map(&:id)
    snap = db.snapshot
    i.put("user:3:a", "deploy on tuesday")
    assert_equal 2, i.search("deploy", 10, snapshot: snap).size
    snap.release
    Pigeonhole.open_reader(@path) do |r|
      ri = T::Index.open_reader(r, "notes_text")
      assert_equal 3, ri.search("deploy", 10).size
      assert_predicate ri.verify, :consistent?
      assert_equal "notes", ri.source.table
    end
  end

  def test_options_source_and_maintenance
    i = index(positions: true, normalization: :nfkc, k1: 1.5)
    o = i.options
    assert_equal [1.5, true, :nfkc], [o.k1, o.positions, o.normalization]
    assert_equal({ table: "notes", family: "body", qualifier: "text" }, i.source.to_h)
    i.put("x", "the ﬁle formats") # "ﬁle" with the fi ligature
    assert_equal "x", i.search("file", 1)[0]&.id
    v = i.verify
    assert_predicate v, :consistent?
    assert_equal 1, v.documents
    r = i.rebuild
    assert_equal [1, 0], [r.documents, r.changed]
    assert_equal 1, i.repair.documents
    reopened = T::Index.open(db, "notes_text")
    assert_equal "x", reopened.search("file", 1)[0].id
    assert_raises(Pigeonhole::InvalidArgument) do
      T::Index.create(db, "bad", table: notes, family: "body", qualifier: "t", normalization: :nfd)
    end
  end
end
