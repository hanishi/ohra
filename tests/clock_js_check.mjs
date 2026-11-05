// offsetFrom in src/ohra/ext/clock.js, run by tests/test_clock_js.py.
import assert from "node:assert/strict";
import { offsetFrom, SAMPLE_WINDOW } from "../src/ohra/ext/clock.js";

// the fastest round trip wins: its server time minus the page time halfway through it
assert.equal(offsetFrom([{ c: 0, r: 0.2, s: 10 }, { c: 1, r: 1.02, s: 11.5 }]).toFixed(2), "10.49");

// only the last SAMPLE_WINDOW count: a faster sample before them is forgotten (a clock drifts)
const old = { c: 0, r: 0.001, s: 100 };
const recent = Array.from({ length: SAMPLE_WINDOW }, (_, i) => ({ c: i + 1, r: i + 1.1, s: i + 6.05 }));
assert.equal(SAMPLE_WINDOW, 6);
assert.equal(offsetFrom([old, ...recent]).toFixed(2), "5.00");
