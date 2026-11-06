"""src/ohra/ext/clock.js, the page side of ``synced``: its offset math, run under node."""

import shutil
import subprocess
from pathlib import Path

import pytest

pytest.importorskip("fastapi")

from ohra.ext.fastapi import CLOCK_JS  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]


def test_clock_js_ships_in_the_package():
    assert CLOCK_JS.is_file()
    assert CLOCK_JS.parent == ROOT / "src" / "ohra" / "ext"


@pytest.mark.skipif(shutil.which("node") is None, reason="needs node")
def test_the_offset_its_error_bound_and_the_delay_of_at():
    subprocess.run(["node", "tests/clock_js_check.mjs"], check=True, cwd=ROOT)
