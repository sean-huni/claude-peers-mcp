/**
 * Unit tests for the poll loop's failure backoff.
 *
 * The loop polls once a second for the life of the session. When the broker is
 * down it previously logged one line per second into Claude Code's MCP log file
 * on disk, forever: about 86,000 identical lines a day, per session. The policy
 * under test is: retry less and less often, say something the first time, say
 * something again only when the situation changes or after a long quiet period,
 * and say so once when the broker comes back.
 *
 * Time is injected rather than slept, so the multi-minute behaviour is testable
 * in microseconds.
 */

import { test, expect } from "bun:test";
import { PollBackoff } from "./poll-backoff.ts";

const OPTS = { baseDelayMs: 1000, maxDelayMs: 60_000, factor: 2, quietMs: 300_000 };

test("the first failure is always reported", () => {
  const b = new PollBackoff(OPTS);
  expect(b.ready(0)).toBe(true);
  const line = b.noteFailure("Unable to connect", 0);
  expect(line).toContain("Unable to connect");
});

test("repeated identical failures are not reported again", () => {
  const b = new PollBackoff(OPTS);
  expect(b.noteFailure("Unable to connect", 0)).not.toBeNull();

  let now = 0;
  let logged = 0;
  // Ten minutes of a dead broker, polled every second.
  for (let tick = 1; tick <= 600; tick++) {
    now = tick * 1000;
    if (!b.ready(now)) continue;
    if (b.noteFailure("Unable to connect", now) !== null) logged++;
  }
  // Only the periodic reminder, never the per-second flood.
  expect(logged).toBeLessThanOrEqual(3);
});

test("a dead broker is polled less and less often, up to a cap", () => {
  const b = new PollBackoff(OPTS);
  const delays: number[] = [];
  let now = 0;
  for (let i = 0; i < 10; i++) {
    b.noteFailure("down", now);
    delays.push(b.delayMs);
    now += b.delayMs;
  }
  expect(delays.slice(0, 5)).toEqual([1000, 2000, 4000, 8000, 16_000]);
  expect(delays.at(-1)).toBe(60_000);
  expect(Math.max(...delays)).toBe(60_000);
});

test("the loop is held off until the backoff window expires", () => {
  const b = new PollBackoff(OPTS);
  b.noteFailure("down", 0);
  expect(b.ready(999)).toBe(false);
  expect(b.ready(1000)).toBe(true);
});

test("a change of error is reported even while suppressed", () => {
  const b = new PollBackoff(OPTS);
  b.noteFailure("Unable to connect", 0);
  expect(b.noteFailure("Unable to connect", 1000)).toBeNull();
  const changed = b.noteFailure("Broker error (/poll-messages): 401 unauthorized", 3000);
  expect(changed).toContain("401 unauthorized");
});

test("a broker that never returns is still reported periodically", () => {
  // Silence forever is its own bug: a session that can never reach its broker
  // must say so, just not sixty times a minute.
  const b = new PollBackoff(OPTS);
  b.noteFailure("down", 0);
  expect(b.noteFailure("down", 60_000)).toBeNull();
  const reminder = b.noteFailure("down", 300_000);
  expect(reminder).not.toBeNull();
  expect(reminder).toContain("still unreachable");
});

test("a reminder counts the failures it stands in for", () => {
  const b = new PollBackoff(OPTS);
  b.noteFailure("down", 0);
  b.noteFailure("down", 1000);
  b.noteFailure("down", 3000);
  const reminder = b.noteFailure("down", 300_000)!;
  expect(reminder).toContain("4"); // four attempts so far
  expect(reminder).toContain("suppressed");
});

test("recovery is reported exactly once", () => {
  const b = new PollBackoff(OPTS);
  b.noteFailure("down", 0);
  b.noteFailure("down", 1000);
  const recovered = b.noteSuccess(5000);
  expect(recovered).toContain("reachable again");
  expect(b.noteSuccess(6000)).toBeNull();
});

test("a healthy loop logs nothing at all", () => {
  const b = new PollBackoff(OPTS);
  for (let tick = 0; tick < 1000; tick++) {
    expect(b.ready(tick * 1000)).toBe(true);
    expect(b.noteSuccess(tick * 1000)).toBeNull();
  }
});

test("recovery resets the backoff so the next outage is reported at once", () => {
  const b = new PollBackoff(OPTS);
  b.noteFailure("down", 0);
  b.noteFailure("down", 1000);
  b.noteSuccess(5000);
  expect(b.ready(5000)).toBe(true);
  expect(b.delayMs).toBe(0);
  expect(b.noteFailure("down again", 6000)).toContain("down again");
});
