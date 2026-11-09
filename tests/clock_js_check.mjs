// offsetFrom in src/ohra/ext/clock.js, built from clock.ts, run by tests/test_clock_js.py.
import assert from "node:assert/strict";
import { connect, delayFor, errorFrom, offsetFrom, SAMPLE_WINDOW } from "../src/ohra/ext/clock.js";

// every page imports connect: the release runs this on the minified file, so a lost export fails it
assert.equal(typeof connect, "function");

// the fastest round trip wins: its server time minus the page time halfway through it
assert.equal(offsetFrom([{ c: 0, r: 0.2, s: 10 }, { c: 1, r: 1.02, s: 11.5 }]).toFixed(2), "10.49");

// only the last SAMPLE_WINDOW count: a faster sample before them is forgotten (a clock drifts)
const old = { c: 0, r: 0.001, s: 100 };
const recent = Array.from({ length: SAMPLE_WINDOW }, (_, i) => ({ c: i + 1, r: i + 1.1, s: i + 6.05 }));
assert.equal(SAMPLE_WINDOW, 6);
assert.equal(offsetFrom([old, ...recent]).toFixed(2), "5.00");

// at() waits for t however far ahead it is, and runs at once once it has passed
assert.equal(delayFor(110, 100, 0), 10000);
assert.equal(delayFor(100.25, 100, 0), 250);
assert.equal(delayFor(99, 100, 0), 0);

// the error bound is half the round trip of the sample the offset came from
assert.equal(errorFrom([{ c: 0, r: 0.2, s: 10 }, { c: 1, r: 1.02, s: 11.5 }]).toFixed(3), "0.010");
assert.equal(errorFrom([old, ...recent]).toFixed(3), "0.050");
