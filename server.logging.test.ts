/**
 * End-to-end test for the MCP server's behaviour when the broker dies.
 *
 * server.ts polls once a second for the life of the session, and its stderr is
 * captured by Claude Code into an MCP log file on disk. With no backoff, a dead
 * broker produced one "Poll error" line per second, forever: measured at 20
 * lines in 20 seconds, roughly 86,000 lines a day for every open session. The
 * fix must be quiet without being silent, and must announce the recovery.
 *
 * This drives the real server process against a real broker and reads its real
 * stderr, because the defect only exists in the wiring.
 */

import { test, expect, afterAll } from "bun:test";
import { rmSync, mkdtempSync, realpathSync } from "node:fs";

// Assigned port range for this suite: 7860-7869. Never 7899, which is a live
// broker owned by the user.
const PORT = 7860 + Math.floor(Math.random() * 10);
const DB = `${process.env.TMPDIR ?? "/tmp"}/claude-peers-logtest-${PORT}.db`;
const BASE = `http://127.0.0.1:${PORT}`;
const ENV = {
  ...process.env,
  CLAUDE_PEERS_PORT: String(PORT),
  CLAUDE_PEERS_DB: DB,
  // Push mode, so the poll loop has somewhere to deliver and does not bail out
  // before it ever reaches the broker.
  CLAUDE_PEERS_CHANNEL: "always",
};

/** The observation window after the broker dies, in seconds. */
const WINDOW_S = 12;
/**
 * The most stderr lines the outage may cost over that window. One is the
 * expected count: the first failure. Two allows for a reminder landing on the
 * boundary. Twelve is what the defect produced.
 */
const MAX_LINES = 2;

const spawned: ReturnType<typeof Bun.spawn>[] = [];

function track<T extends ReturnType<typeof Bun.spawn>>(p: T): T {
  spawned.push(p);
  return p;
}

async function startBroker() {
  const broker = track(
    Bun.spawn(["bun", `${import.meta.dir}/broker.ts`], {
      env: ENV,
      stdout: "ignore",
      stderr: "ignore",
    })
  );
  for (let i = 0; i < 60; i++) {
    await Bun.sleep(100);
    try {
      if ((await fetch(`${BASE}/health`)).ok) return broker;
    } catch {
      // still booting
    }
  }
  throw new Error(`broker did not come up on ${PORT}`);
}

/** A server process whose stderr is accumulated line by line. */
function spawnServer(cwd: string) {
  const proc = track(
    Bun.spawn(["bun", `${import.meta.dir}/server.ts`], {
      cwd,
      env: ENV,
      stdin: "pipe",
      stdout: "ignore",
      stderr: "pipe",
    })
  );

  const lines: string[] = [];
  (async () => {
    const decoder = new TextDecoder();
    let buf = "";
    for await (const chunk of proc.stderr as any) {
      buf += decoder.decode(chunk);
      let nl: number;
      while ((nl = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, nl).trim();
        buf = buf.slice(nl + 1);
        if (line) lines.push(line);
      }
    }
  })();

  return { proc, lines };
}

async function waitForLine(lines: string[], re: RegExp, timeoutMs: number): Promise<string> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const hit = lines.find((l) => re.test(l));
    if (hit) return hit;
    await Bun.sleep(100);
  }
  throw new Error(`no stderr line matching ${re} within ${timeoutMs}ms:\n${lines.join("\n")}`);
}

function workdir(): string {
  return realpathSync(
    mkdtempSync(`${(process.env.TMPDIR ?? "/tmp").replace(/\/$/, "")}/peers-logtest-`)
  );
}

/** Lines the outage is responsible for. */
function outageLines(lines: string[]): string[] {
  return lines.filter((l) => /poll error|unreachable/i.test(l));
}

afterAll(() => {
  // Kill only the pids this suite started, by pid. No port sweeping.
  for (const p of spawned) {
    try {
      p.kill();
    } catch {
      // already gone
    }
  }
  for (const suffix of ["", "-wal", "-shm"]) rmSync(`${DB}${suffix}`, { force: true });
});

test("a dead broker does not flood the MCP log, and recovery is announced", async () => {
  for (const suffix of ["", "-wal", "-shm"]) rmSync(`${DB}${suffix}`, { force: true });

  let broker = await startBroker();
  const server = spawnServer(workdir());
  await waitForLine(server.lines, /Registered as peer/, 30_000);

  // The broker dies underneath a healthy session.
  const before = server.lines.length;
  broker.kill();
  await broker.exited;

  await Bun.sleep(WINDOW_S * 1000);
  const during = outageLines(server.lines.slice(before));

  // Not silent: an unreachable broker must be visible somewhere.
  expect(during.length).toBeGreaterThanOrEqual(1);
  expect(during[0]).toMatch(/poll error/i);
  // Not a flood: the defect logged one line per second.
  expect(during.length).toBeLessThanOrEqual(MAX_LINES);

  // The broker comes back; the session must notice and say so, once.
  const mark = server.lines.length;
  broker = await startBroker();
  const recovered = await waitForLine(server.lines, /reachable again/i, 90_000);
  expect(recovered).toMatch(/reachable again/i);

  await Bun.sleep(4000);
  const recoveries = server.lines.slice(mark).filter((l) => /reachable again/i.test(l));
  expect(recoveries).toHaveLength(1);
}, 180_000);
