/**
 * Tests for the benchmark harness itself.
 *
 * A harness nobody tests is a harness that reports whatever it happens to compute, and this one
 * exists to settle an argument between two transports. Three things have to be true of it before
 * any of its numbers are worth quoting:
 *
 *   1. The statistics are the statistics, and an empty sample is an error rather than a zero.
 *   2. It refuses to bind the port that serves live Claude Code sessions.
 *   3. It really does observe deliveries end to end, and it says so loudly when it does not.
 *
 * The third one is the expensive one, so it runs once at a deliberately small sample size. The
 * point of the smoke run is that the plumbing carries real messages, not that four samples
 * characterise anything: the guard is expected to FAIL that run for being short, and the test
 * asserts exactly that.
 */

import { test, expect, afterAll } from "bun:test";
import {
  broadcastArguments,
  runBenchmark,
  renderReport,
  type DeliveryPhaseResult,
} from "./delivery-latency.ts";
import { assertSafeRange, PRODUCTION_BROKER_PORT } from "./rig.ts";
import { percentile, round, seededRandom, summarize } from "./stats.ts";
import { cleanupTempDirs } from "../testsupport";

/**
 * The harness's own range.
 *
 * Kept clear of 7810-7824, which testsupport hands to the other suites, AND of 7850-7869, where
 * broker.wal.test.ts and server.logging.test.ts pick a port at random and bind it WITHOUT going
 * through testsupport's reservation. A benchmark sharing a port with one of those does not fail: it
 * quietly measures the wrong broker, which is the failure this whole harness is built to prevent.
 */
const BENCH_RANGE = { min: 7770, max: 7789 };

afterAll(() => {
  // The rigs tear down their own processes; this only sweeps directories the harness registered
  // with testsupport. Never cleanupAll, which would kill processes belonging to other suites.
  cleanupTempDirs();
});

test("percentiles are nearest rank and never invented", () => {
  const samples = [10, 20, 30, 40];
  expect(percentile(samples, 0)).toBe(10);
  expect(percentile(samples, 0.5)).toBe(20);
  expect(percentile(samples, 1)).toBe(40);
  // Nearest rank, so every answer is a value that is actually in the sample.
  for (const fraction of [0, 0.1, 0.5, 0.95, 1]) {
    expect(samples).toContain(percentile(samples, fraction));
  }
});

test("an empty sample is an error, not a zero", () => {
  // The failure mode this whole harness is built against: a number that came from no measurement.
  expect(() => percentile([], 0.5)).toThrow(/empty sample/i);
  expect(() => summarize([])).toThrow(/nothing was measured/i);
});

test("summarize reports the shape of the sample", () => {
  const stats = summarize([100, 200, 300, 400, 500]);
  expect(stats).toEqual({ count: 5, min: 100, median: 300, p95: 500, max: 500, mean: 300 });
});

test("the seeded generator makes a run reproducible", () => {
  const first = Array.from({ length: 8 }, seededRandom(42));
  const second = Array.from({ length: 8 }, seededRandom(42));
  const other = Array.from({ length: 8 }, seededRandom(43));
  expect(first).toEqual(second);
  expect(first).not.toEqual(other);
  for (const value of first) expect(value).toBeGreaterThanOrEqual(0);
  for (const value of first) expect(value).toBeLessThan(1);
});

test("a port range containing the live broker's port is refused", () => {
  expect(() => assertSafeRange({ min: 7890, max: 7910 })).toThrow(/7899/);
  expect(() => assertSafeRange({ min: PRODUCTION_BROKER_PORT, max: PRODUCTION_BROKER_PORT })).toThrow(
    /live Claude Code sessions/
  );
  expect(() => assertSafeRange(BENCH_RANGE)).not.toThrow();
});

test("broadcast arguments are read from the tool's schema, never guessed", () => {
  const withScope = {
    name: "broadcast_message",
    inputSchema: {
      properties: { message: {}, scope: {} },
      required: ["message"],
    },
  };
  expect(broadcastArguments(withScope, "hello")).toEqual({ message: "hello", scope: "machine" });

  const messageOnly = {
    name: "broadcast_message",
    inputSchema: { properties: { message: {} }, required: ["message"] },
  };
  expect(broadcastArguments(messageOnly, "hello")).toEqual({ message: "hello" });

  // A schema demanding something the harness cannot invent is skipped rather than called blind:
  // a wrong call produces timeouts, and timeouts look like a slow transport.
  const unknownRequired = {
    name: "broadcast_message",
    inputSchema: { properties: { message: {} }, required: ["message", "recipients"] },
  };
  expect(broadcastArguments(unknownRequired, "hello")).toBeNull();
  expect(broadcastArguments({ name: "broadcast_message" }, "hello")).toBeNull();
});

test("round keeps one decimal by default", () => {
  expect(round(491.2649)).toBe(491.3);
  expect(round(491.2649, 2)).toBe(491.26);
});

test(
  "the harness observes real end-to-end deliveries, and fails loudly on a short sample",
  async () => {
    const report = await runBenchmark({
      samples: 4,
      burst: 3,
      burstRounds: 1,
      idleWindowMs: 4000,
      deliveryTimeoutMs: 15_000,
      portRange: BENCH_RANGE,
      skip: { unicast: false, burst: false, broadcast: false, idle: false },
      log: () => {},
    });

    // The measurement really happened: every message issued was seen arriving at the receiver.
    const unicast = report.phases.unicast as DeliveryPhaseResult;
    expect(unicast.delivered).toBe(4);
    expect(unicast.missed).toEqual([]);
    expect(unicast.stats).not.toBeNull();
    // A poll transport cannot deliver instantly and cannot take forever. Bounds this loose only
    // fail when nothing was actually measured, which is the point.
    expect(unicast.stats!.min).toBeGreaterThan(0);
    expect(unicast.stats!.max).toBeLessThan(15_000);

    const burst = report.phases.burst as DeliveryPhaseResult;
    expect(burst.delivered).toBe(3);
    expect(burst.missed).toEqual([]);

    // dev has no broadcast_message. On a branch that has one this becomes a measured phase, with no
    // edit to the harness, which is the property that lets one harness compare branches.
    const broadcast = report.phases.broadcast;
    if (broadcast === null) throw new Error("the broadcast phase was not attempted");
    if ("skipped" in broadcast) {
      expect(broadcast.reason).toMatch(/broadcast_message/);
    } else {
      expect(broadcast.delivered).toBeGreaterThan(0);
    }

    // An idle session still talks to its broker. Zero would mean the client was dead.
    const idle = report.phases.idle;
    if (idle === null) throw new Error("the idle phase was not attempted");
    if (!("skipped" in idle)) {
      expect(idle.totalRequests + idle.longLivedRequests).toBeGreaterThan(0);
      expect(idle.sessions).toBe(1);
    }

    // Four samples is not a distribution, and the harness says so rather than printing a table.
    expect(report.valid).toBe(false);
    expect(report.problems.join(" ")).toMatch(/too few/);
    expect(renderReport(report)).toContain("Delivery guard: FAIL");
  },
  180_000
);
