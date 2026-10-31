"""Property test: random puts, deletes and reads against a dict model, under one seed
Hypothesis prints on failure. Mirrors the role of crates/pigeonhole/tests/model.rs at the
API's surface (the engine's own model checks cover crashes and concurrency)."""

import os

from hypothesis import given, settings, strategies as st

import pigeonhole as ph

KEYS = st.sampled_from([b"a", b"b", b"c", b"\xff\x00", b"long" * 8])
QUALS = st.sampled_from([b"x", b"y", b""])
VALUES = st.one_of(st.binary(max_size=64), st.integers(-(2**63), 2**63 - 1), st.floats(allow_nan=False))
OPS = st.lists(
    st.one_of(
        st.tuples(st.just("put"), KEYS, QUALS, VALUES),
        st.tuples(st.just("delete_column"), KEYS, QUALS),
        st.tuples(st.just("delete_row"), KEYS),
        st.tuples(st.just("flush")),
    ),
    max_size=40,
)


def expected_typed(v):
    return v if not isinstance(v, (bytes, bytearray)) else bytes(v)


@settings(max_examples=int(os.environ.get("PROPTEST_CASES", "60")), deadline=None)
@given(OPS)
def test_reads_match_a_dict_model(tmp_path_factory, ops):
    path = tmp_path_factory.mktemp("model") / "m.phdb"
    model = {}
    with ph.open(path, create=True, shards=1) as db:
        t = db.table("t").family("f").create()
        for op in ops:
            if op[0] == "put":
                _, k, q, v = op
                t.put(k, "f", q, v)
                model[(k, q)] = expected_typed(v)
            elif op[0] == "delete_column":
                _, k, q = op
                t.delete(k, "f", q)
                model.pop((k, q), None)
            elif op[0] == "delete_row":
                _, k = op
                t.delete(k)
                for key in [key for key in model if key[0] == k]:
                    del model[key]
            else:
                db.flush()
        for (k, q), v in model.items():
            assert t.get(k, "f", q).typed == v
        scanned = {(r.key, q): c.typed for r in t.scan() for _, q, c in r}
        assert scanned == model
