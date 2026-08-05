/**
 * The port pool must not overlap any port a suite hardcodes.
 *
 * Earned on 2026-08-05. The pool held exactly as many ports as there were callers, so widening it
 * was correct, and the widening swallowed three suites' private hardcoded ranges (recovery
 * 7831-7839, broker.wal 7850-7859, server.logging 7860-7869). `reserveFreePort` could then hand a
 * suite a port another suite was about to bind. `isPortFree` cannot prevent this: a suite that
 * picks its port at module load and binds seconds later passes the check and collides anyway.
 *
 * The symptom was the worst kind. Tests failed with enormous durations and moved around between
 * runs, which reads as flakiness or machine load, so the first instinct is to re-run rather than
 * to look. Two full runs were spent blaming leftover processes.
 *
 * The three suites now reserve from the pool like everyone else. This test exists so nobody
 * reintroduces a private range, and so widening the pool again cannot silently swallow one.
 */

import { test, expect } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

const PORT_MIN = Number.parseInt(process.env.CLAUDE_PEERS_TEST_PORT_MIN ?? "7830", 10);
const PORT_MAX = Number.parseInt(process.env.CLAUDE_PEERS_TEST_PORT_MAX ?? "7889", 10);

/** The live broker. Never in the pool, never bound by a test. */
const PRODUCTION_PORT = 7899;

/**
 * Ports a file names literally, excluding the ones that are definitions rather than uses.
 *
 * hygiene.test.ts deliberately squats fixed ports OUTSIDE the pool to prove that exhaustion is
 * reported rather than silently satisfied with an occupied port. Those are the point of that
 * suite, so they are read here and asserted to stay outside, not exempted.
 */
function literalPorts(source: string): number[] {
  const withoutPoolDefinition = source
    .split("\n")
    .filter((line) => !/PORT_MIN|PORT_MAX|CLAUDE_PEERS_TEST_PORT/.test(line))
    // Comments explain the ranges; they are not bindings.
    .filter((line) => !/^\s*(\/\/|\*|\/\*)/.test(line))
    .join("\n");
  const found = new Set<number>();
  for (const m of withoutPoolDefinition.matchAll(/\b(78\d\d)\b/g)) {
    found.add(Number.parseInt(m[1]!, 10));
  }
  return [...found];
}

function testFiles(): string[] {
  return readdirSync(import.meta.dir)
    .filter((f) => f.endsWith(".test.ts"))
    .filter((f) => f !== "portpool.test.ts");
}

test("no test file hardcodes a port inside the reservable pool", () => {
  const offenders: string[] = [];
  for (const file of testFiles()) {
    const source = readFileSync(join(import.meta.dir, file), "utf8");
    for (const port of literalPorts(source)) {
      if (port >= PORT_MIN && port <= PORT_MAX) offenders.push(`${file}: ${port}`);
    }
  }
  // Named individually: "some file collides" sends the reader hunting.
  expect(offenders, `hardcoded ports inside the pool ${PORT_MIN}-${PORT_MAX}`).toEqual([]);
});

test("no test file binds the production broker port", () => {
  const offenders: string[] = [];
  for (const file of testFiles()) {
    const source = readFileSync(join(import.meta.dir, file), "utf8");
    if (literalPorts(source).includes(PRODUCTION_PORT)) offenders.push(file);
  }
  expect(offenders, `${PRODUCTION_PORT} is the user's live broker`).toEqual([]);
});

test("the pool excludes the production broker port", () => {
  expect(PRODUCTION_PORT < PORT_MIN || PRODUCTION_PORT > PORT_MAX).toBe(true);
});

test("the pool is large enough for every caller, with headroom", () => {
  // A pool the same size as its caller count fails on the next test file added, which is how
  // 7810-7824 broke: fifteen ports, fifteen callers, and the Codex branch added a sixteenth.
  let callers = 0;
  for (const file of [...testFiles(), "testsupport.ts"]) {
    const source = readFileSync(join(import.meta.dir, file), "utf8");
    // Reservations against an explicit sub-range (hygiene's squat probe) do not draw from the pool.
    for (const m of source.matchAll(/reserveFreePort\((\s*\))?/g)) {
      if (m[1] !== undefined) callers += 1;
    }
  }
  const size = PORT_MAX - PORT_MIN + 1;
  expect(callers, "sanity: the suite should reserve at least a few ports").toBeGreaterThan(5);
  expect(size, `pool ${PORT_MIN}-${PORT_MAX} holds ${size} for ${callers} callers`).toBeGreaterThan(
    callers * 2
  );
});
