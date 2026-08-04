/**
 * ask_peer end to end, over the real MCP stdio surface.
 *
 * The contract has three halves and each gets its own proof:
 *
 *   1. the answer comes back through the BLOCKING call, not the inbox
 *   2. the answer is NOT also rendered as an ordinary inbound message (no
 *      double delivery: the waiter consumes it in the one delivery path)
 *   3. no answer means a timeout NOTICE, never a hang, and the question still
 *      reached the peer
 *
 * Inspired by the synaq fork's ask_peer; the correlation design (client-minted
 * token in the message text, waiter routing in the delivery loop) is our own.
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
const WORK = trackedTempDir("peers-ask-");
const DB = join(WORK, "broker.db");
const ENV = {
  ...process.env,
  CLAUDE_PEERS_PORT: String(PORT),
  CLAUDE_PEERS_DB: DB,
  // Never the real spool (see server.test.ts for why).
  CLAUDE_PEERS_SPOOL_DIR: join(WORK, "spool"),
};

function spawnClient(cwd: string, extraEnv: Record<string, string> = {}) {
  const session = trackProcess(Bun.spawn(["sleep", "600"], { stdout: "ignore", stderr: "ignore" }));
  const proc = trackProcess(
    Bun.spawn(["bun", `${import.meta.dir}/server.ts`], {
      cwd,
      env: { ...ENV, CLAUDE_PEERS_SESSION_PID: String(session.pid), ...extraEnv },
      stdin: "pipe",
      stdout: "pipe",
      stderr: "ignore",
    })
  );

  const replies = new Map<number, any>();
  const notifications: any[] = [];

  (async () => {
    const decoder = new TextDecoder();
    let buf = "";
    for await (const chunk of proc.stdout as any) {
      buf += decoder.decode(chunk);
      let nl: number;
      while ((nl = buf.indexOf("\n")) >= 0) {
        const line = buf.slice(0, nl);
        buf = buf.slice(nl + 1);
        if (!line.trim()) continue;
        try {
          const msg = JSON.parse(line);
          if (msg.id != null) replies.set(msg.id, msg);
          else if (msg.method) notifications.push(msg);
        } catch {
          // not a JSON-RPC frame
        }
      }
    }
  })();

  return {
    proc,
    notifications,
    async call(id: number, method: string, params: unknown, patienceMs = 25_000) {
      proc.stdin.write(JSON.stringify({ jsonrpc: "2.0", id, method, params }) + "\n");
      await proc.stdin.flush();
      const deadline = Date.now() + patienceMs;
      while (Date.now() < deadline) {
        if (replies.has(id)) return replies.get(id);
        await Bun.sleep(100);
      }
      throw new Error(`timed out waiting for reply id=${id}`);
    },
    async initialize(id: number) {
      return this.call(id, "initialize", {
        protocolVersion: "2024-11-05",
        capabilities: {},
        clientInfo: { name: "ask-test", version: "1" },
      });
    },
    async tool(id: number, name: string, args: Record<string, unknown> = {}, patienceMs = 25_000) {
      const res = await this.call(id, "tools/call", { name, arguments: args }, patienceMs);
      return String(res.result.content[0].text);
    },
  };
}

let broker: ReturnType<typeof Bun.spawn>;

beforeAll(async () => {
  broker = trackProcess(
    Bun.spawn(["bun", `${import.meta.dir}/broker.ts`], { env: ENV, stdout: "ignore", stderr: "ignore" })
  );
  for (let i = 0; i < 40; i++) {
    await Bun.sleep(100);
    try {
      if ((await fetch(`http://127.0.0.1:${PORT}/health`)).ok) return;
    } catch {
      // booting
    }
  }
  throw new Error(`broker did not come up on ${PORT}`);
});

afterAll(() => {
  try {
    cleanupAll();
  } finally {
    sweepBrokerOnPort(PORT);
  }
});

test("the answer returns through the blocking ask, and is not double-delivered to the inbox", async () => {
  const dirA = trackedTempDir("peers-ask-a-");
  const dirB = trackedTempDir("peers-ask-b-");
  const asker = spawnClient(dirA, { CLAUDE_PEERS_CHANNEL: "always", CLAUDE_PEERS_NAME: "asker" });
  const answerer = spawnClient(dirB, { CLAUDE_PEERS_CHANNEL: "always", CLAUDE_PEERS_NAME: "oracle" });
  await asker.initialize(1);
  await answerer.initialize(1);
  await Bun.sleep(1500);

  // The answerer's side runs concurrently: wait for the pushed question, parse
  // the ask token out of its text, reply via send_message with in_reply_to.
  // This harness does exactly what the receiving Claude is instructed to do.
  const answering = (async () => {
    for (let i = 0; i < 100; i++) {
      const pushed = answerer.notifications.filter((n) => String(n.method).includes("channel"));
      if (pushed.length > 0) {
        const text = String(pushed[0].params.content);
        const token = text.match(/Blocking ask #([a-z0-9]+)/)?.[1];
        if (!token) throw new Error(`no ask token in pushed text: ${text}`);
        expect(text).toContain("What is the release tag format?");
        return answerer.tool(10, "send_message", {
          to_id: "asker",
          message: "Digits-only 3-component SemVer, no v prefix.",
          in_reply_to: token,
        });
      }
      await Bun.sleep(100);
    }
    throw new Error("question never reached the answerer");
  })();

  const answer = await asker.tool(
    2,
    "ask_peer",
    { to_id: "oracle", question: "What is the release tag format?", timeout_seconds: 30 },
    45_000
  );
  await answering;

  expect(answer).toContain("Answer from");
  expect(answer).toContain("Digits-only 3-component SemVer");

  // No double delivery: the reply must not ALSO have been pushed to the asker
  // as an ordinary inbound message. Settle first so a straggler would arrive.
  await Bun.sleep(2500);
  const askerInbound = asker.notifications.filter((n) => String(n.method).includes("channel"));
  const leaked = askerInbound.filter((n) =>
    String(n.params?.content ?? "").includes("Digits-only 3-component SemVer")
  );
  expect(leaked).toHaveLength(0);

  // And the inbox agrees: nothing queued for the asker.
  expect(await asker.tool(3, "check_messages")).toContain("No new messages");
}, 90_000);

test("an unanswered ask returns a timeout notice, and the question still reached the peer", async () => {
  const dirA = trackedTempDir("peers-ask-c-");
  const dirB = trackedTempDir("peers-ask-d-");
  const asker = spawnClient(dirA, { CLAUDE_PEERS_CHANNEL: "always" });
  const silent = spawnClient(dirB, { CLAUDE_PEERS_CHANNEL: "always" });
  await asker.initialize(1);
  await silent.initialize(1);
  await Bun.sleep(1500);

  const silentId = (await asker.tool(2, "list_peers", { scope: "machine" }))
    .split(/\n(?=ID: )/)
    .find((b) => b.includes(`CWD: ${dirB}`))
    ?.match(/ID: ([a-z0-9]{8})/)?.[1];
  expect(silentId).toBeDefined();

  const t0 = performance.now();
  const result = await asker.tool(
    3,
    "ask_peer",
    { to_id: silentId!, question: "Anyone home?", timeout_seconds: 5 },
    30_000
  );
  const elapsed = performance.now() - t0;

  expect(result).toContain("No answer");
  expect(result).toContain("5s");
  // It waited the timeout out, and did not hang far past it.
  expect(elapsed).toBeGreaterThan(4_500);
  expect(elapsed).toBeLessThan(20_000);

  // The question was still delivered to the silent peer.
  await Bun.sleep(1000);
  const seen = silent.notifications.filter((n) =>
    String(n.params?.content ?? "").includes("Anyone home?")
  );
  expect(seen.length).toBeGreaterThan(0);
}, 60_000);

test("a reply whose waiter is gone degrades to an ordinary message instead of vanishing", async () => {
  const dirA = trackedTempDir("peers-ask-e-");
  const dirB = trackedTempDir("peers-ask-f-");
  const a = spawnClient(dirA, { CLAUDE_PEERS_CHANNEL: "always" });
  const b = spawnClient(dirB, { CLAUDE_PEERS_CHANNEL: "always" });
  await a.initialize(1);
  await b.initialize(1);
  await Bun.sleep(1500);

  const aId = (await b.tool(2, "list_peers", { scope: "machine" }))
    .split(/\n(?=ID: )/)
    .find((blk) => blk.includes(`CWD: ${dirA}`))
    ?.match(/ID: ([a-z0-9]{8})/)?.[1];
  expect(aId).toBeDefined();

  // A reply to an ask token nobody is waiting on (the ask timed out long ago,
  // or the session restarted): it must arrive as a normal message.
  await b.tool(3, "send_message", {
    to_id: aId!,
    message: "late answer to a dead ask",
    in_reply_to: "deadbeef",
  });
  await Bun.sleep(2500);

  const arrived = a.notifications.filter((n) =>
    String(n.params?.content ?? "").includes("late answer to a dead ask")
  );
  expect(arrived.length).toBeGreaterThan(0);
}, 60_000);
