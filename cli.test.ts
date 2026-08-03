/**
 * Tests for the inspection CLI.
 *
 * The CLI broke silently when broker authentication landed: it sent no
 * credential, every route answered 401, and the error handler reported that as
 * "Broker is not running." A working broker looked like a dead one.
 */

import { test, expect, beforeAll, afterAll } from "bun:test";
import { rmSync } from "node:fs";

const PORT = 7901 + Math.floor(Math.random() * 15);
const DB = `${process.env.TMPDIR ?? "/tmp"}/claude-peers-clitest-${PORT}.db`;
const ENV = { ...process.env, CLAUDE_PEERS_PORT: String(PORT), CLAUDE_PEERS_DB: DB };
const BASE = `http://127.0.0.1:${PORT}`;

let broker: ReturnType<typeof Bun.spawn>;
const holders: ReturnType<typeof Bun.spawn>[] = [];

function livePid(): number {
  const p = Bun.spawn(["sleep", "60"], { stdout: "ignore", stderr: "ignore" });
  holders.push(p);
  return p.pid;
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
  for (const suffix of ["", "-wal", "-shm"]) rmSync(`${DB}${suffix}`, { force: true });
  broker = Bun.spawn(["bun", `${import.meta.dir}/broker.ts`], {
    env: ENV,
    stdout: "ignore",
    stderr: "ignore",
  });
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

afterAll(() => {
  for (const h of holders) h.kill();
  broker?.kill();
  for (const suffix of ["", "-wal", "-shm"]) rmSync(`${DB}${suffix}`, { force: true });
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
