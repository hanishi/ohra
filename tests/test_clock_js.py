"""src/ohra/ext/clock.ts, the page side of ``synced``: its offset math, run under node on the
clock.js built from it."""

import shutil
import subprocess
from pathlib import Path

import pytest

pytest.importorskip("fastapi")

from ohra.ext.fastapi import CLOCK_JS  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
BUILD = (
    "npx --yes esbuild@0.28.2 src/ohra/ext/clock.ts --format=esm --outfile=src/ohra/ext/clock.js"
)


def test_clock_js_is_built_from_clock_ts_in_the_package():
    assert CLOCK_JS.parent == ROOT / "src" / "ohra" / "ext"
    assert CLOCK_JS.with_name("clock.ts").is_file()
    assert CLOCK_JS.is_file(), f"clock.js is built from clock.ts: run {BUILD}"


@pytest.mark.skipif(shutil.which("node") is None, reason="needs node")
def test_the_offset_its_error_bound_and_the_delay_of_at():
    subprocess.run(["node", "tests/clock_js_check.mjs"], check=True, cwd=ROOT)
