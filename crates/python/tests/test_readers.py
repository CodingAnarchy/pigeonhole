"""Mirrors crates/pigeonhole/tests/readers.rs: reader processes see the writer's commits and
can't write; without a writer, opening one fails with NoWriter."""

import subprocess
import sys
import textwrap

import pytest

import pigeonhole as ph


def test_a_reader_with_no_writer_running_gets_no_writer(path):
    """readers.rs: a_reader_with_no_writer_running_gets_no_writer"""
    ph.open(path, create=True, shards=1).close()
    with pytest.raises(ph.NoWriter):
        ph.open_reader(path)


def test_a_reader_process_sees_commits_and_cannot_write(db, path):
    """A reader in another process (the multi-process case readers exist for)."""
    t = db.table("t").family("f").create()
    t.put(b"r", "f", b"q", b"v1")
    script = textwrap.dedent(
        f"""
        import sys, pigeonhole as ph
        with ph.open_reader({str(path)!r}) as r:
            t = r.table("t")
            print(t.get(b"r", "f", b"q").value.decode())
            sys.stdout.flush()
            sys.stdin.readline()            # the writer commits again meanwhile
            print(t.get(b"r", "f", b"q").value.decode())
            try:
                t.put(b"r", "f", b"q", b"x")
            except ph.ReadOnly as e:
                print(e.code)
        """
    )
    child = subprocess.Popen(
        [sys.executable, "-c", script], stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True
    )
    assert child.stdout.readline().strip() == "v1"
    t.put(b"r", "f", b"q", b"v2")
    out, _ = child.communicate("go\n", timeout=60)
    assert out.split() == ["v2", "ReadOnly"]
    assert child.returncode == 0


def test_a_reader_in_process_reads_and_scans(db, path):
    """A reader handle in the writer's own process, as the Rust tests open one."""
    t = db.table("t").family("f").create()
    for i in range(3):
        t.put(f"r{i}", "f", b"q", i)
    with ph.open_reader(path) as r:
        assert r.tables() == ["t"]
        rt = r.table("t")
        assert [row.key for row in rt.scan()] == [b"r0", b"r1", b"r2"]
        with r.snapshot() as s:
            t.put(b"r9", "f", b"q", 9)
            assert len(list(rt.scan(snapshot=s))) == 3
        with pytest.raises(ph.ReadOnly):
            rt.mutate(b"r")
