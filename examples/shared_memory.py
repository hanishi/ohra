"""Benchmark: handing a large array to another process by pickling it versus
passing a SharedArray handle. The worker sums the array either way.

    uv run --extra numpy python examples/shared_memory.py
"""

import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pekko_py.ext.shm import SharedArray

N = 50_000_000  # doubles: 400MB


def sum_pickled(arr: np.ndarray) -> float:
    return float(arr.sum())


def sum_shared(handle: SharedArray) -> float:
    with handle.open() as arr:
        return float(arr.sum())


def main() -> None:
    with ProcessPoolExecutor(1) as pool:
        pool.submit(sum_pickled, np.zeros(10)).result()  # warm the worker up

        t0 = time.perf_counter()
        handle = SharedArray.empty((N,))
        with handle.open(writeable=True) as arr:
            arr[...] = 1.0
        fill_s = time.perf_counter() - t0

        arr = np.ones(N)
        t0 = time.perf_counter()
        pool.submit(sum_pickled, arr).result()
        pickled_s = time.perf_counter() - t0

        t0 = time.perf_counter()
        pool.submit(sum_shared, handle).result()
        shared_s = time.perf_counter() - t0
        handle.unlink()

    mb = N * 8 / 1e6
    print(f"{mb:.0f}MB array to a worker process and back with its sum")
    print(f"pickle         {pickled_s * 1000:8.0f}ms  (copy on send, copy on receive)")
    print(f"shared handle  {shared_s * 1000:8.0f}ms  (the handle is three small fields)")
    print(f"filling the segment once: {fill_s * 1000:.0f}ms; amortized over every send after that")


if __name__ == "__main__":
    main()
