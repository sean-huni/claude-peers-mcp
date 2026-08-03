/**
 * End-to-end tests for the MCP server's half of the push transport.
 *
 * The broker suite proves a frame leaves the broker. What matters to a session is whether the
 * message reaches it, whether it reaches it once, and what happens when the stream is not there.
 * These drive real server processes over stdio, because the interesting behaviour is the
 * interaction between two schedulers running at once: the stream, which fires on an insert, and
 * the poll, which fires on a clock. Either can see a message first, and exactly one must render it.
 *
 * The poll interval is turned up where a test needs to attribute a delivery. With the base
 * interval at twenty seconds, nothing the poll does can explain a message arriving in one, so a
 * delivery inside the window can only have come from the stream.
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
const WORK = trackedTempDir("peers-ssesrv-");
const DB = join(WORK, "broker.db");
const BASE = `http://127.0.0.1:${PORT}`;

const ENV = {
  ...process.env,
  CLAUDE_PEERS_PORT: String(PORT),
  CLAUDE_PEERS_DB: DB,
  // Never the real one under $HOME. Without this the servers spawned here resolve a session pid by
  // walking their own process tree, land on the developer's live Claude Code session, and spool
  // test traffic into a queue a hook is actively draining into someone's context.
  CLAUDE_PEERS_SPOOL_DIR: join(WORK, "spool"),
};

interface Client {
  proc: ReturnType<typeof Bun.spawn>;
  notifications: any[];
  channelTexts(): string[];
  call(id: number, method: string, params: unknown): Promise<any>;
  initialize(id: number): Promise<any>;
  tool(id: number, name: string, args?: Record<string, unknown>): Promise<string>;
}

const clients: Client[] = [];

/**
 * A server process standing in for one Claude Code session.
 *
 * Each is given its own session pid rather than deriving one. Derivation finds the ancestor Claude
 * Code session, which every client here shares, so they would all spool into one queue and drain
 * each other's messages.
 */
function spawnClient(cwd: string, env: Record<string, string> = {}): Client {
  const session = trackProcess(Bun.spawn(["sleep", "600"], { stdout: "ignore", stderr: "ignore" }));
  const proc = trackProcess(
    Bun.spawn(["bun", `${import.meta.dir}/server.ts`], {
      cwd,
      env: { ...ENV, CLAUDE_PEERS_SESSION_PID: String(session.pid), ...env },
      stdin: "pipe",
      stdout: "pipe",
      stderr: "ignore",
    })
  );

  const replies = new Map<number, any>();
  const notifications: any[] = [];

  // One persistent reader. Re-entering `for await` on stdout cancels the stream, which silently
  // truncates every later response.
  (async () => {
    const decoder = new TextDecoder();
    let buf = "";
    try {
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
            else if (msg.method) notifications.push({ ...msg, received_at: Date.now() });
          } catch {
            // not a JSON-RPC frame
          }
        }
      }
    } catch {
      // The process was killed out from under the reader, which some tests do on purpose.
    }
  })();

  const client: Client = {
    proc,
    notifications,
    channelTexts() {
      return notifications
        .filter((n) => String(n.method).includes("channel"))
        .map((n) => String(n.params?.content ?? ""));
    },
    async call(id, method, params) {
      proc.stdin.write(JSON.stringify({ jsonrpc: "2.0", id, method, params }) + "\n");
      await proc.stdin.flush();
      for (let i = 0; i < 250; i++) {
        if (replies.has(id)) return replies.get(id);
        await Bun.sleep(100);
      }
      throw new Error(`timed out waiting for reply id=${id}`);
    },
    async initialize(id) {
      return this.call(id, "initialize", {
        protocolVersion: "2024-11-05",
        capabilities: {},
        clientInfo: { name: "test", version: "1" },
      });
    },
    async tool(id, name, args = {}) {
      const res = await this.call(id, "tools/call", { name, arguments: args });
      return String(res.result.content[0].text);
    },
  };

  clients.push(client);
  return client;
}

/** Resolve a peer id by working directory: clients from earlier tests stay registered. */
function peerIdAt(listing: string, cwd: string): string {
  for (const block of listing.split(/\n(?=ID: )/)) {
    if (!block.includes(`CWD: ${cwd}`)) continue;
    const id = block.match(/ID: ([a-z0-9]{8})/)?.[1];
    if (id) return id;
  }
  throw new Error(`no peer at ${cwd} in:\n${listing}`);
}

function workdir(): string {
  return trackedTempDir("peers-ssesrv-cwd-");
}

async function waitFor(predicate: () => boolean, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return true;
    await Bun.sleep(10);
  }
  return predicate();
}

function health(): Promise<{ status: string; peers: number; subscribers: number }> {
  return fetch(`${BASE}/health`).then((r) => r.json() as any);
}

function post<T>(path: string, body: unknown, token?: string): Promise<T> {
  return fetch(`${BASE}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(body),
  }).then((r) => r.json() as Promise<T>);
}

/**
 * A peer with no MCP server behind it, for driving traffic at a rate a tool call cannot.
 *
 * A tool call costs a stdio round trip, which paces sends far too slowly to make two delivery
 * passes overlap. The pid must belong to something alive: peers whose process is gone are reaped.
 */
function registerRaw(cwd: string): Promise<{ id: string; token: string }> {
  const pid = trackProcess(Bun.spawn(["sleep", "600"], { stdout: "ignore", stderr: "ignore" })).pid;
  return post("/register", { pid, cwd, git_root: null, tty: null, summary: "" });
}

async function startBroker(env: Record<string, string> = {}) {
  const proc = trackProcess(
    Bun.spawn(["bun", `${import.meta.dir}/broker.ts`], {
      env: { ...ENV, ...env },
      stdout: "ignore",
      stderr: "ignore",
    })
  );
  for (let i = 0; i < 60; i++) {
    await Bun.sleep(100);
    try {
      if ((await fetch(`${BASE}/health`)).ok) return proc;
    } catch {
      // still booting
    }
  }
  throw new Error(`broker did not come up on ${PORT}`);
}

let broker: ReturnType<typeof Bun.spawn>;

beforeAll(async () => {
  broker = await startBroker();
});

afterAll(() => {
  try {
    cleanupAll();
  } finally {
    sweepBrokerOnPort(PORT);
  }
});

test("a message is rendered from the stream, without waiting for a poll", async () => {
  // The poll interval is twenty seconds here, so a delivery inside two cannot be its work. This is
  // the whole claim of the change: the wait is no longer half an interval.
  const senderDir = workdir();
  const receiverDir = workdir();
  const sender = spawnClient(senderDir, { CLAUDE_PEERS_POLL_MS: "20000" });
  const receiver = spawnClient(receiverDir, {
    CLAUDE_PEERS_POLL_MS: "20000",
    CLAUDE_PEERS_CHANNEL: "always",
  });

  await sender.initialize(1);
  await receiver.initialize(1);
  await Bun.sleep(3500); // registration, auto-summary and the subscription settling

  const target = peerIdAt(await sender.tool(2, "list_peers", { scope: "machine" }), receiverDir);
  const sentAt = Date.now();
  await sender.tool(3, "send_message", { to_id: target, message: "pushed by the stream" });

  expect(await waitFor(() => receiver.channelTexts().length > 0, 2000)).toBe(true);
  expect(receiver.channelTexts()[0]).toBe("pushed by the stream");
  expect(Date.now() - sentAt).toBeLessThan(2000);
}, 40_000);

test("a message is never rendered twice when the stream and the poll race", async () => {
  // Both schedulers are live and the poll is turned DOWN to 20ms, so it collides with the stream
  // as often as possible. The burst is sent straight over HTTP rather than through a second MCP
  // server: twenty messages arriving at once make one delivery pass long enough to span several
  // poll ticks, which is the condition the guard exists for. Without it the passes overlap, each
  // polls, each finds the same ids absent from the rendered set because the id is only recorded
  // after the push, and each pushes them.
  const receiverDir = workdir();
  const receiver = spawnClient(receiverDir, {
    CLAUDE_PEERS_POLL_MS: "20",
    CLAUDE_PEERS_POLL_IDLE_MS: "20",
    CLAUDE_PEERS_CHANNEL: "always",
  });

  await receiver.initialize(1);
  await Bun.sleep(3500);

  const sender = await registerRaw("/tmp/sse-race-sender");
  const listing = await post<{ id: string; cwd: string }[]>(
    "/list-peers",
    { scope: "machine", cwd: "/tmp/sse-race-sender", git_root: null, exclude_id: sender.id },
    sender.token
  );
  const target = listing.find((p) => p.cwd === receiverDir);
  expect(target).toBeDefined();

  const texts = Array.from({ length: 20 }, (_, i) => `burst-${i}`);
  await Promise.all(
    texts.map((text) =>
      post("/send-message", { from_id: sender.id, to_id: target!.id, text }, sender.token)
    )
  );

  expect(await waitFor(() => receiver.channelTexts().length >= texts.length, 10_000)).toBe(true);
  await Bun.sleep(1500); // room for a duplicate to show up late

  const rendered = receiver.channelTexts();
  expect([...new Set(rendered)].sort()).toEqual([...texts].sort());
  expect(rendered.length).toBe(texts.length);
}, 40_000);

test("the broker forgets a subscriber when its session exits", async () => {
  const before = (await health()).subscribers;
  const receiver = spawnClient(workdir(), { CLAUDE_PEERS_CHANNEL: "always" });
  await receiver.initialize(1);

  await waitForSubscribers(before + 1);

  receiver.proc.kill("SIGKILL");
  await waitForSubscribers(before);
}, 40_000);

async function waitForSubscribers(expected: number): Promise<void> {
  const deadline = Date.now() + 10_000;
  let seen = -1;
  while (Date.now() < deadline) {
    seen = (await health()).subscribers;
    if (seen === expected) return;
    await Bun.sleep(50);
  }
  expect(seen).toBe(expected);
}

test("a broker with no stream still delivers, on the poll", async () => {
  // The safety net. The broker is killed under an established subscription and replaced by one
  // that does not serve the transport at all, which is exactly what an older broker looks like.
  // Delivery must degrade to the previous behaviour, never to silence.
  const senderDir = workdir();
  const receiverDir = workdir();
  const sender = spawnClient(senderDir);
  const receiver = spawnClient(receiverDir, { CLAUDE_PEERS_CHANNEL: "always" });

  await sender.initialize(1);
  await receiver.initialize(1);
  await Bun.sleep(3500);
  expect((await health()).subscribers).toBeGreaterThan(0);

  // Same database, same port, no push transport.
  broker.kill("SIGKILL");
  await broker.exited;
  broker = await startBroker({ CLAUDE_PEERS_SSE: "off" });
  expect((await health()).subscribers).toBe(0);

  // Long enough for the reconnection attempts to have backed off past the window asserted below,
  // so what is measured is the poll and not a retry that happened to land. This is the state a
  // session settles into when the transport is simply not available: the stream is a distant
  // retry, and everything that arrives has to arrive on the clock.
  await Bun.sleep(9000);

  const target = peerIdAt(await sender.tool(2, "list_peers", { scope: "machine" }), receiverDir);
  await sender.tool(3, "send_message", { to_id: target, message: "delivered by the fallback" });

  expect(await waitFor(() => receiver.channelTexts().length > 0, 3000)).toBe(true);
  expect(receiver.channelTexts()).toContain("delivered by the fallback");
}, 60_000);
