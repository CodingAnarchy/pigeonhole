"""Vector families (D303) through the bindings: specs, writes from sequences and buffers, and
vector cells read back as arrays. Mirrors crates/engine/tests/it/vector_family.rs at the API's
surface."""

import array

import pytest

import pigeonhole as ph


@pytest.fixture
def t(db):
    return (
        db.table("v")
        .family("emb", ph.Family.vector(4, ph.Element.F32, ph.Metric.COSINE))
        .family("q8", ph.Family.vector(3, ph.Element.I8, ph.Metric.L2))
        .family("sig", ph.Family.vector(16, "bit", "hamming"))
        .family("plain")
        .create()
    )


def test_a_vector_family_stores_its_spec(t):
    spec = t.family_info("emb").vector
    assert (spec.dim, spec.element, spec.metric) == (4, "f32", "cosine")
    assert t.family_info("plain").vector is None
    assert ph.VectorSpec(8, ph.Element.I8, ph.Metric.INNER_PRODUCT).dim == 8
    with pytest.raises(ValueError):
        ph.Family.vector(4, "f16", "l2")


def test_vectors_round_trip_from_sequences_and_buffers(db, t):
    t.mutate(b"a").put_vector("emb", b"x", [1.0, 2.0, 3.0, 4.5]).commit()
    t.mutate(b"b").put_vector("emb", b"x", array.array("f", [0.5, 0.25, 0, -1])).commit()
    t.mutate(b"c").put_vector("q8", b"x", (1, -2, 127)).put_vector("sig", b"x", b"\xff\x01").commit()
    a = t.get(b"a", "emb", b"x")
    assert a.as_vector() == array.array("f", [1.0, 2.0, 3.0, 4.5])
    assert a.typed == a.as_vector()
    assert t.get(b"b", "emb", b"x").as_vector().tolist() == [0.5, 0.25, 0.0, -1.0]
    assert t.get(b"c", "q8", b"x").as_vector() == array.array("b", [1, -2, 127])
    assert t.get(b"c", "sig", b"x").as_vector() == b"\xff\x01"
    # Batches and transactions write vectors too.
    db.write_batch().put_vector(t, b"d", "emb", b"x", [0, 0, 0, 1]).commit()
    with db.transaction() as txn:
        txn.put_vector(t, b"e", "q8", b"x", [3, 2, 1])
    assert t.get(b"d", "emb", b"x").as_vector()[3] == 1.0
    assert t.get(b"e", "q8", b"x").as_vector().tolist() == [3, 2, 1]


def test_numpy_arrays_write_and_read_without_conversion(t):
    np = pytest.importorskip("numpy")
    v = np.arange(4, dtype=np.float32) / 2
    t.mutate(b"n").put_vector("emb", b"x", v).commit()
    back = np.frombuffer(t.get(b"n", "emb", b"x").as_vector(), dtype=np.float32)
    assert (back == v).all()


def test_wrong_vectors_are_refused(t):
    with pytest.raises(TypeError):
        t.mutate(b"a").put_vector("plain", b"x", [1.0])
    with pytest.raises(TypeError):
        t.mutate(b"a").put_vector("emb", b"x", "not a vector")
    with pytest.raises(ph.FamilyNotFound):
        t.mutate(b"a").put_vector("missing", b"x", [1.0])
    # The dimension is the family's: a wrong one fails the commit.
    with pytest.raises(ph.Error):
        t.mutate(b"a").put_vector("emb", b"x", [1.0, 2.0]).commit()
    assert t.get(b"a", "emb", b"x") is None
