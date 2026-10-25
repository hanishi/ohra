# Appendix C: Performance

From `examples/benchmark.py` on an Apple Silicon laptop with Python 3.14 and the default event loop. Run it yourself; the numbers move with the machine.

| path | rate | cost |
|---|---|---|
| `tell` into a busy actor, handler returns `same` | 1.3M msg/s | 0.8µs per message |
| `tell`, handler returns a new behavior each time | 630k msg/s | 1.6µs per message |
| `tell` through a pool of 8 routees | 630k msg/s | 1.6µs per message |
| ping-pong between two actors | 31k round trips/s | 32µs per round trip |
| `ask` from outside, one at a time | 30k/s | 33µs per ask |
| spawn and stop an actor | 77k/s | 13µs per actor |

The two regimes matter more than the figures. A busy actor drains its mailbox without yielding, so a message costs a deque pop, a `match` and a call. A message to an idle actor costs an asyncio task switch, about 16µs, which is what every asyncio design pays and what `batching` amortizes. Returning a new behavior per message allocates a closure and a `Receive`, which is the price of the functional style; keep `Behaviors.same` with state in a per-actor cell on hot paths. The other benchmarks under `examples/` cover threads, batching, population shards and shared memory.

---

[← Appendix B: Known deviations from Apache Pekko](appendix-deviations.md) · [Contents](README.md)
