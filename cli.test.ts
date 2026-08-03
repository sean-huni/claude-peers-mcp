/**
 * Tests for the inspection CLI.
 *
 * The CLI broke silently when broker authentication landed: it sent no
 * credential, every route answered 401, and the error handler reported that as
 * "Broker is not running." A working broker looked like a dead one.
 */

import { test, expect, beforeAll, afterAll } from "bun:test";
import { join } from "node:path";
import {
  cleanupAll,
  reserveFreePort,
  sweepBrokerOnPort,
  trackProcess,
  trackedTempDir,
} from "./testsupport";

const PORT = reserveFreePort();
const WORK = trackedTempDir("peers-clitest-");
const DB = join(WORK, "broker.db");
const ENV = {
  ...process.env,
  CLAUDE_PEERS_PORT: String(PORT),
  CLAUDE_PEERS_DB: DB,
  // Never the real one under $HOME: the CLI resolves a session pid from the process tree, and on a
  // developer machine that tree ends at a live Claude Code session whose queue is not ours to write.
  CLAUDE_PEERS_SPOOL_DIR: join(WORK, "spool"),
};
const BASE = `http://127.0.0.1:${PORT}`;

let broker: ReturnType<typeof Bun.spawn>;

function livePid(): number {
  return trackProcess(Bun.spawn(["sleep", "60"], { stdout: "ignore", stderr: "ignore" })).pid;
}

async function cli(...args: string[]): Promise<string> {
  const proc = Bun.spawn(["bun", `${import.meta.dir}/cli.ts`, ...args], {
    env: ENV,
    stdout: "pipe",
    stderr: "pipe",
  });
  const out = await new Response(proc.stdout).text();
  const err = await new Response(proc.stderr).text();
  await proc.exited;
  return out + err;
}

beforeAll(async () => {
  broker = trackProcess(
    Bun.spawn(["bun", `${import.meta.dir}/broker.ts`], {
      env: ENV,
      stdout: "ignore",
      stderr: "ignore",
    })
  );
  for (let i = 0; i < 40; i++) {
    await Bun.sleep(100);
    try {
      if ((await fetch(`${BASE}/health`)).ok) return;
    } catch {
      // still booting
    }
  }
  throw new Error(`broker did not come up on ${PORT}`);
});

// Unconditional: this runs after a passing suite, a failing assertion and a throwing beforeAll
// alike, and each step is isolated so an early failure cannot skip the ones after it.
afterAll(() => {
  try {
    cleanupAll();
  } finally {
    sweepBrokerOnPort(PORT);
  }
});

test("status reports a running broker rather than claiming it is down", async () => {
  const out = await cli("status");
  expect(out).toContain("Broker: ok");
  expect(out).not.toContain("Broker is not running");
});

test("peers lists a registered session", async () => {
  const reg = await fetch(`${BASE}/register`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      pid: livePid(),
      cwd: "/tmp/cli-peer",
      git_root: null,
      tty: null,
      summary: "a peer the CLI should see",
    }),
  }).then((r) => r.json() as Promise<{ id: string }>);

  const out = await cli("peers");
  expect(out).toContain(reg.id);
  expect(out).toContain("a peer the CLI should see");
  expect(out).not.toContain("Broker is not running");
});

test("the CLI does not leave its transient peer registered", async () => {
  await cli("peers");
  await Bun.sleep(300);
  const out = await cli("peers");
  expect(out).not.toContain("CLI (transient)");
});

test("send delivers a message that the recipient can collect", async () => {
  const reg = await fetch(`${BASE}/register`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      pid: livePid(),
      cwd: "/tmp/cli-target",
      git_root: null,
      tty: null,
      summary: "",
    }),
  }).then((r) => r.json() as Promise<{ id: string; token: string }>);

  const out = await cli("send", reg.id, "hello from the CLI");
  expect(out).not.toContain("Broker is not running");

  const inbox = await fetch(`${BASE}/poll-messages`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${reg.token}` },
    body: JSON.stringify({ id: reg.id }),
  }).then((r) => r.json() as Promise<{ messages: { text: string }[] }>);

  expect(inbox.messages.map((m) => m.text)).toContain("hello from the CLI");
});
