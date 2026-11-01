"""Every `python` block in docs/guide/python.md runs, each in a fresh directory with `path`
set to a new database file, so the guide can't drift from the API."""

import re
from pathlib import Path

import pytest

GUIDE = Path(__file__).resolve().parents[3] / "docs" / "guide" / "python.md"
BLOCKS = re.findall(r"```python\n(.*?)```", GUIDE.read_text(), re.S)


def test_the_guide_has_examples():
    assert len(BLOCKS) >= 5


@pytest.mark.parametrize("block", BLOCKS, ids=[f"block{i}" for i in range(len(BLOCKS))])
def test_a_guide_example_runs(block, tmp_path, capsys):
    exec(compile(block, str(GUIDE), "exec"), {"path": tmp_path / "guide.phdb"})
