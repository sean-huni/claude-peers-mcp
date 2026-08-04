/**
 * Startup must not block on the auto-summary.
 *
 * The summary is a nicety: the code has always had a late-apply path that pushes it to the broker
 * whenever it arrives. Startup nevertheless raced it against a 3000ms timer BEFORE connecting the
 * MCP transport, so every session paid for a network round trip it had already made optional.
 * Measured on dev before the fix: median 2474ms to the initialize reply, of which 2001ms was the
 * one Anthropic call.
 *
 * These tests pin both halves of the contract, because a "fix" that simply deleted the summary
 * would satisfy the first one alone:
 *
 *   1. a slow summary does not delay the initialize reply
 *   2. the summary still lands, after registration
 *
 * The Anthropic endpoint is stubbed and deliberately slow rather than real. A test that called the
 * live API would pass vacuously on any machine with no credential, where generateSummary returns
 * null immediately and there is nothing to block on: exactly the monoculture that hides the defect.
 */

import { test, expect, beforeAll, afterAll } from "bun:test";
import { join } from "node:path";
import { Database } from "bun:sqlite";
import {
  cleanupAll,
  reserveFreePort,
  sweepBrokerOnPort,
  trackProcess,
  trackedTempDir,
} from "./testsupport";

const PORT = reserveFreePort();
const WORK = trackedTempDir("peers-startup-");
const DB = join(WORK, "broker.db");

/** How long the stubbed Anthropic endpoint stalls before answering. Set per test. */
let summaryDelayMs = 5_000;

const anthropic = Bun.serve({
  port: 0,
  hostname: "127.0.0.1",
  async fetch() {
    await Bun.sleep(summaryDelayMs);
    return Response.json({
      id: "msg_stub",
      type: "message",
      role: "assistant",
      model: "claude-haiku-4-5",
      content: [{ type: "text", text: "Stubbed summary from a deliberately slow endpoint." }],
      stop_reason: "end_turn",
      stop_sequence: null,
      usage: { input_tokens: 1, output_tokens: 1 },
    });
  },
});

const ENV = {
  ...process.env,
  CLAUDE_PEERS_PORT: String(PORT),
  CLAUDE_PEERS_DB: DB,
  // Never the developer's real spool: these servers would otherwise resolve a session pid by
  // walking their own process tree and queue test traffic into a live session.
  CLAUDE_PEERS_SPOOL_DIR: join(WORK, "spool"),
  ANTHROPIC_API_KEY: "test-key-not-used-against-the-real-api",
  ANTHROPIC_BASE_URL: `http://127.0.0.1:${anthropic.port}`,
};

let broker: ReturnType<typeof Bun.spawn>;

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
      if ((await fetch(`http://127.0.0.1:${PORT}/health`)).ok) return;
    } catch {
      // still booting
    }
  }
  throw new Error(`broker did not come up on ${PORT}`);
});

afterAll(() => {
  try {
    cleanupAll();
  } finally {
    anthropic.stop(true);
    sweepBrokerOnPort(PORT);
  }
});

/**
 * Start one MCP server and return how long its initialize reply took.
 *
 * Each server is pointed at a live process of its own, so it does not share a spool with its
 * siblings or with the developer's session.
 */
async function timeToInitialize(): Promise<{ ms: number; proc: ReturnType<typeof Bun.spawn> }> {
  const session = trackProcess(Bun.spawn(["sleep", "120"], { stdout: "ignore", stderr: "ignore" }));
  const started = performance.now();
  const proc = trackProcess(
    Bun.spawn(["bun", `${import.meta.dir}/server.ts`], {
      env: { ...ENV, CLAUDE_PEERS_SESSION_PID: String(session.pid) },
      stdin: "pipe",
      stdout: "pipe",
      stderr: "ignore",
    })
  );

  proc.stdin.write(
    JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "initialize",
      params: {
        protocolVersion: "2024-11-05",
        capabilities: {},
        clientInfo: { name: "startup-test", version: "1" },
      },
    }) + "\n"
  );
  await proc.stdin.flush();

  const decoder = new TextDecoder();
  let buf = "";
  for await (const chunk of proc.stdout as any) {
    buf += decoder.decode(chunk);
    for (const line of buf.split("\n")) {
      if (line.includes('"id":1')) {
        return { ms: performance.now() - started, proc };
      }
    }
  }
  throw new Error("server exited before replying to initialize");
}

test("a slow auto-summary does not delay the initialize reply", async () => {
  // Five seconds is longer than the 3000ms race that used to sit in front of the transport, so a
  // regression cannot hide inside the old cap: it would show up here as ~3000ms, not as 5000ms.
  summaryDelayMs = 5_000;
  const { ms } = await timeToInitialize();
  expect(ms).toBeLessThan(1_000);
}, 30_000);

test("the auto-summary still reaches the broker, after registration", async () => {
  summaryDelayMs = 800;
  const { ms } = await timeToInitialize();
  expect(ms).toBeLessThan(1_000);

  // The peer is registered well before the summary exists, which is the whole point: the row is
  // there first and gains its summary later.
  const db = new Database(DB, { readonly: true });
  let summary: string | null = null;
  for (let i = 0; i < 60; i++) {
    await Bun.sleep(250);
    const rows = db.query("SELECT summary FROM peers WHERE summary IS NOT NULL").all() as {
      summary: string;
    }[];
    const hit = rows.find((r) => r.summary.includes("deliberately slow endpoint"));
    if (hit) {
      summary = hit.summary;
      break;
    }
  }
  db.close();

  expect(summary).toContain("Stubbed summary from a deliberately slow endpoint.");
}, 45_000);
