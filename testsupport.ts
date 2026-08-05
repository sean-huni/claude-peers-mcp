/**
 * Shared helpers for the test suites.
 *
 * The suites spawn real brokers, real MCP servers and real placeholder processes, and they write
 * real files, so every run creates operating-system state that outlives the test process unless
 * something removes it. Runs used to leave roughly ten `peers-test-*` directories behind and, when
 * a suite failed or was killed, brokers still holding test ports. The squatters were the expensive
 * part: a port was picked at random and bound blindly, so a leftover broker answered /health, the
 * next run decided its broker was up, and every test then talked to a stranger with a different
 * database.
 *
 * This module owns that bookkeeping in one place: ports are confirmed free before use, and
 * everything created through it is registered for removal at exit whether the run ends normally,
 * fails, or is interrupted.
 */

import { mkdtempSync, realpathSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { listeningBrokerPids } from "./shared/procs";

type Proc = { pid: number; kill: (signal?: number | NodeJS.Signals) => void };

// 7830-7889: sixty ports for a suite that currently reserves fifteen. The old 7810-7824 window
// held exactly as many ports as there were callers, so the next test file to reserve one failed
// the whole run with "no free port" (hit 2026-08-05 when the Codex branch was integrated). The
// range deliberately starts above 7826, which hygiene.test.ts squats on purpose to prove
// exhaustion is reported rather than silently returning an occupied port, and ends well below
// the production broker on 7899.
const PORT_MIN = Number.parseInt(process.env.CLAUDE_PEERS_TEST_PORT_MIN ?? "7830", 10);
const PORT_MAX = Number.parseInt(process.env.CLAUDE_PEERS_TEST_PORT_MAX ?? "7889", 10);

const handedOut = new Set<number>();
const trackedDirs = new Set<string>();
const trackedProcs = new Set<Proc>();

/**
 * Whether the port can actually be bound right now.
 *
 * Asking the operating system by binding is the only answer worth having. A leftover broker
 * answers /health perfectly well, so probing the port with an HTTP request cannot distinguish
 * "free" from "occupied by something that looks like what I am about to start".
 */
export function isPortFree(port: number): boolean {
  try {
    const server = Bun.serve({ port, hostname: "127.0.0.1", fetch: () => new Response("") });
    server.stop(true);
    return true;
  } catch {
    return false;
  }
}

/**
 * A port in the range reserved for tests that nothing is listening on.
 *
 * Throws rather than returning an occupied port: a suite that fails immediately with the reason
 * costs a minute, and one that runs against a squatter costs an afternoon.
 */
export function reserveFreePort(min: number = PORT_MIN, max: number = PORT_MAX): number {
  const span = max - min + 1;
  const start = Math.floor(Math.random() * span);
  for (let i = 0; i < span; i++) {
    const port = min + ((start + i) % span);
    if (handedOut.has(port)) continue;
    if (!isPortFree(port)) continue;
    handedOut.add(port);
    return port;
  }
  throw new Error(
    `no free port in ${min}-${max}: every port is occupied, most likely by brokers left behind by ` +
      `an earlier run. Check with: lsof -ti :${min}-${max} -sTCP:LISTEN`
  );
}

/** Hand a reserved port back, for callers that probe without ever binding. */
export function releasePort(port: number): void {
  handedOut.delete(port);
}

/** A temporary directory that will be removed when the process ends, however it ends. */
export function trackedTempDir(prefix: string): string {
  // realpath matters: TMPDIR is a symlink on macOS (/var -> /private/var), so the raw path never
  // matches the cwd a spawned process reports back.
  const dir = realpathSync(mkdtempSync(join(realpathSync(tmpdir()), prefix)));
  trackedDirs.add(dir);
  return dir;
}

/** Register a spawned process so it is killed even if the suite never reaches its afterAll. */
export function trackProcess<T extends Proc>(proc: T): T {
  trackedProcs.add(proc);
  return proc;
}

export function killTrackedProcesses(): void {
  for (const proc of trackedProcs) {
    try {
      proc.kill("SIGKILL");
    } catch {
      // already gone
    }
  }
  trackedProcs.clear();
}

export function cleanupTempDirs(): void {
  for (const dir of trackedDirs) rmSync(dir, { recursive: true, force: true });
  trackedDirs.clear();
}

/**
 * Stop the broker on a test port if one is still listening.
 *
 * Deliberately not `lsof -ti :PORT | xargs kill`: that signals every process holding a socket on
 * the port, which includes the test runner itself once Bun has pooled a connection to it.
 */
export function sweepBrokerOnPort(port: number): void {
  for (const pid of listeningBrokerPids(port)) {
    try {
      process.kill(pid, "SIGKILL");
    } catch {
      // already gone
    }
  }
}

/** Everything a suite is expected to hand back. Safe to call more than once. */
export function cleanupAll(): void {
  try {
    killTrackedProcesses();
  } finally {
    cleanupTempDirs();
  }
}

// A failing assertion runs afterAll, but a timeout that takes the runner down with it does not.
// These are the last line of defence against a run leaving brokers behind.
process.once("exit", cleanupAll);
for (const signal of ["SIGINT", "SIGTERM", "SIGHUP"] as const) {
  process.once(signal, () => {
    cleanupAll();
    process.exit(1);
  });
}
