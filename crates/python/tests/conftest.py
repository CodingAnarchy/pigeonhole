"""Shared fixtures: a fresh database per test, with one shard so tests stay light."""

import pytest

import pigeonhole as ph


@pytest.fixture
def path(tmp_path):
    return tmp_path / "t.phdb"


@pytest.fixture
def db(path):
    d = ph.open(path, create=True, shards=1)
    yield d
    d.close()
