/**
 * Tests for `cli.ts kill-broker`.
 *
 * Stopping the broker used to stop live Claude Code sessions with it. The command asked
 * `lsof -ti :PORT` which pid to signal, and that question returns every process holding a socket on
 * the port: the listener AND every connected client. So the MCP servers of running sessions were
 * SIGTERMed alongside the daemon they were talking to.
 *
 * These tests pin both halves of the fix: only the LISTENING socket is considered, and the process
 * behind it must actually be the broker before it is signalled, so a stale port taken over by
 * something unrelated cannot be killed by this command.
 */

import { test, expect, afterAll } from "bun:test";
import { writeFileSync } from "node:fs";
import { join } from "node:path";
import {
  cleanupAll,
  reserveFreePort,
  sweepBrokerOnPort,
  trackProcess,
  trackedTempDir,
} from "./testsupport";

type Proc = ReturnType<typeof Bun.spawn>;

const usedPorts: number[] = [];

// Unconditional, and the sweep runs even if killing the tracked processes throws.
afterAll(() => {
  try {
    cleanupAll();
  } finally {
    for (const port of usedPorts) sweepBrokerOnPort(port);
  }
});

function scratch(): string {
  return trackedTempDir("peers-killtest-");
}

function port(): number {
  const chosen = reserveFreePort();
  usedPorts.push(chosen);
  return chosen;
}

/** Every pid holding a socket on the port, which is the question the CLI used to ask. */
function pidsOnPort(port: number): number[] {
  const out = Bun.spawnSync(["lsof", "-ti", `:${port}`]);
  return new TextDecoder()
    .decode(out.stdout)
    .trim()
    .split("\n")
    .map((line) => Number.parseInt(line, 10))
    .filter((pid) => Number.isInteger(pid));
}

async function waitFor(predicate: () => boolean, what: string): Promise<void> {
  for (let i = 0; i < 100; i++) {
    if (predicate()) return;
    await Bun.sleep(100);
  }
  throw new Error(`timed out waiting for ${what}`);
}

/**
 * Health is probed through curl rather than `fetch` on purpose. Bun pools connections, so a fetch
 * from the test runner leaves the runner itself holding a socket on the broker port: under the
 * unfixed CLI it is one of the pids `lsof` returns, and the runner is SIGTERMed mid-test instead
 * of reporting a failure. Keeping the runner off the port is what makes this test able to fail.
 */
function healthy(port: number): boolean {
  const out = Bun.spawnSync([
    "curl",
    "-s",
    "-o",
    "/dev/null",
    "-w",
    "%{http_code}",
    "--max-time",
    "2",
    `http://127.0.0.1:${port}/health`,
  ]);
  return new TextDecoder().decode(out.stdout).trim() === "200";
}

async function startBroker(port: number, dir: string): Promise<Proc> {
  const proc = trackProcess(
    Bun.spawn(["bun", `${import.meta.dir}/broker.ts`], {
      env: {
        ...process.env,
        CLAUDE_PEERS_PORT: String(port),
        CLAUDE_PEERS_DB: join(dir, "broker.db"),
      },
      stdout: "ignore",
      stderr: "ignore",
    })
  );
  await waitFor(() => healthy(port), `broker on ${port}`);
  return proc;
}

/**
 * A stand-in for a connected session: a process whose only job is to hold an open TCP connection
 * to the broker. This is what a live MCP server looks like to `lsof`, minus the protocol.
 */
async function startConnectedClient(port: number, dir: string): Promise<Proc> {
  const script = join(dir, "connected-client.ts");
  writeFileSync(
    script,
    `const socket = await Bun.connect({
  hostname: "127.0.0.1",
  port: ${port},
  socket: { data() {}, close() {}, error() {} },
});
void socket;
await new Promise(() => {});
`
  );
  const proc = trackProcess(Bun.spawn(["bun", script], { stdout: "ignore", stderr: "ignore" }));
  await waitFor(() => pidsOnPort(port).includes(proc.pid), `client ${proc.pid} to connect`);
  return proc;
}

/** Something that answers /health like a broker but is not one, as a reused port would be. */
async function startImpostor(port: number, dir: string): Promise<Proc> {
  const script = join(dir, "impostor.ts");
  writeFileSync(
    script,
    `Bun.serve({
  port: ${port},
  hostname: "127.0.0.1",
  fetch: () => Response.json({ status: "ok", peers: 0 }),
});
await new Promise(() => {});
`
  );
  const proc = trackProcess(Bun.spawn(["bun", script], { stdout: "ignore", stderr: "ignore" }));
  await waitFor(() => healthy(port), `impostor on ${port}`);
  return proc;
}

/**
 * Whether a spawned process is still alive after a grace period.
 *
 * `Subprocess.exitCode` is not enough: it stays null until the child is reaped, so a process that
 * has already been SIGTERMed still reads as running. Racing the `exited` promise forces the answer.
 */
async function stillRunning(proc: Proc, ms = 1500): Promise<boolean> {
  const exited = await Promise.race([
    proc.exited.then(() => true),
    Bun.sleep(ms).then(() => false),
  ]);
  return !exited;
}

async function killBroker(port: number): Promise<string> {
  const proc = Bun.spawn(["bun", `${import.meta.dir}/cli.ts`, "kill-broker"], {
    env: { ...process.env, CLAUDE_PEERS_PORT: String(port) },
    stdout: "pipe",
    stderr: "pipe",
  });
  const out = await new Response(proc.stdout).text();
  const err = await new Response(proc.stderr).text();
  await proc.exited;
  return out + err;
}

test("kill-broker stops the broker without killing the sessions connected to it", async () => {
  const dir = scratch();
  const testPort = port();
  const broker = await startBroker(testPort, dir);
  const client = await startConnectedClient(testPort, dir);

  // The premise of the bug: the client shares the port with the broker, so a pid lookup that is
  // not restricted to the listening socket returns both.
  expect(pidsOnPort(testPort)).toContain(client.pid);
  expect(pidsOnPort(testPort)).toContain(broker.pid);

  await killBroker(testPort);

  expect(await stillRunning(client)).toBe(true);
  expect(healthy(testPort)).toBe(false);
}, 30_000);

test("kill-broker leaves a process that is not the broker alone", async () => {
  const dir = scratch();
  const testPort = port();
  const impostor = await startImpostor(testPort, dir);

  const out = await killBroker(testPort);

  expect(await stillRunning(impostor)).toBe(true);
  expect(out).toContain("No broker process");
}, 30_000);
