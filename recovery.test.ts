/**
 * Session recovery: losing the database must not be permanent.
 *
 * Two failures live here, and they are the same failure seen from each end.
 * The broker keeps a single SQLite handle opened at boot, so deleting the file
 * underneath it left every route answering `disk I/O error` forever, with no
 * attempt to recreate anything. And session identity lived only in the MCP
 * server's memory, so once the broker came back against an empty database every
 * live session was refused 401 on every call, permanently, with restarting each
 * Claude Code session the only remedy.
 *
 * See https://12factor.net/disposability: a process must be able to lose its
 * backing service and come back, without a human restarting the fleet.
 */

import { test, expect, afterAll } from "bun:test";
import { existsSync, mkdtempSync, realpathSync, rmSync, statSync } from "node:fs";

// This range is reserved for this suite. 7899 is the user's live broker and is
// never touched here.
const PORTS = {
  brokerLoss: 7831,
  tokenAfterLoss: 7833,
  sessionReset: 7835,
  singleFlight: 7837,
  boundedRetry: 7839,
};

const spawned: ReturnType<typeof Bun.spawn>[] = [];
const tempDbs: string[] = [];
const tempDirs: string[] = [];

function tmpDb(port: number): string {
  const path = `${(process.env.TMPDIR ?? "/tmp").replace(/\/$/, "")}/claude-peers-recovery-${port}.db`;
  removeDatabase(path);
  tempDbs.push(path);
  return path;
}

function removeDatabase(path: string): void {
  for (const suffix of ["", "-wal", "-shm"]) rmSync(`${path}${suffix}`, { force: true });
}

/** Own directory per client, so a peer is addressable by cwd unambiguously. */
function workdir(): string {
  // realpath: TMPDIR is a symlink on macOS, and the raw path never matches the
  // cwd the server reports back through list_peers.
  const dir = realpathSync(mkdtempSync(`${(process.env.TMPDIR ?? "/tmp").replace(/\/$/, "")}/peers-recovery-`));
  tempDirs.push(dir);
  return dir;
}

async function startBroker(port: number, db: string) {
  const proc = Bun.spawn(["bun", `${import.meta.dir}/broker.ts`], {
    env: { ...process.env, CLAUDE_PEERS_PORT: String(port), CLAUDE_PEERS_DB: db },
    stdout: "ignore",
    stderr: "ignore",
  });
  spawned.push(proc);
  for (let i = 0; i < 40; i++) {
    await Bun.sleep(100);
    try {
      if ((await fetch(`http://127.0.0.1:${port}/health`)).ok) return proc;
    } catch {
      // still booting
    }
  }
  throw new Error(`broker did not come up on ${port}`);
}

function rawPost(port: number, path: string, body: unknown, token?: string) {
  return fetch(`http://127.0.0.1:${port}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(body),
  });
}

async function register(port: number, cwd: string) {
  const res = await rawPost(port, "/register", {
    pid: process.pid,
    cwd,
    git_root: null,
    tty: null,
    summary: "",
  });
  expect(res.status).toBe(200);
  return (await res.json()) as { id: string; token: string };
}

/** Poll a condition rather than sleeping a guessed interval. */
async function within<T>(ms: number, attempt: () => Promise<T | null>): Promise<T> {
  const deadline = Date.now() + ms;
  let last: unknown = null;
  while (Date.now() < deadline) {
    try {
      const value = await attempt();
      if (value !== null) return value;
    } catch (e) {
      last = e;
    }
    await Bun.sleep(150);
  }
  throw new Error(`condition not met within ${ms}ms${last ? `: ${String(last)}` : ""}`);
}

function spawnClient(cwd: string, port: number, db: string, channel: "always" | "never") {
  const proc = Bun.spawn(["bun", `${import.meta.dir}/server.ts`], {
    cwd,
    env: {
      ...process.env,
      CLAUDE_PEERS_PORT: String(port),
      CLAUDE_PEERS_DB: db,
      CLAUDE_PEERS_CHANNEL: channel,
      CLAUDE_PEERS_SPOOL_DIR: workdir(),
      // No auto-summary here. It is a real API call, and when it lands after
      // startup it issues a /set-summary of its own, which under load arrives
      // in the middle of a test that is counting registrations. Pointing the
      // SDK at a closed port makes it fail instantly and stay out of the way.
      ANTHROPIC_API_KEY: "recovery-test-offline",
      ANTHROPIC_BASE_URL: "http://127.0.0.1:9",
    },
    stdin: "pipe",
    stdout: "pipe",
    stderr: "ignore",
  });
  spawned.push(proc);

  const replies = new Map<number, any>();

  // One persistent reader: re-entering `for await` on stdout cancels the stream
  // and silently truncates every later response.
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
        } catch {
          // not a JSON-RPC frame
        }
      }
    }
  })();

  return {
    proc,
    async call(id: number, method: string, params: unknown) {
      proc.stdin.write(JSON.stringify({ jsonrpc: "2.0", id, method, params }) + "\n");
      await proc.stdin.flush();
      for (let i = 0; i < 250; i++) {
        if (replies.has(id)) return replies.get(id);
        await Bun.sleep(100);
      }
      throw new Error(`timed out waiting for reply id=${id}`);
    },
    async initialize(id: number) {
      return this.call(id, "initialize", {
        protocolVersion: "2024-11-05",
        capabilities: {},
        clientInfo: { name: "recovery-test", version: "1" },
      });
    },
    async tool(id: number, name: string, args: Record<string, unknown> = {}) {
      const res = await this.call(id, "tools/call", { name, arguments: args });
      return String(res.result.content[0].text);
    },
  };
}

function peerIdAt(listing: string, cwd: string): string {
  for (const block of listing.split(/\n(?=ID: )/)) {
    if (!block.includes(`CWD: ${cwd}`)) continue;
    const id = block.match(/ID: ([a-z0-9]{8})/)?.[1];
    if (id) return id;
  }
  throw new Error(`no peer at ${cwd} in:\n${listing}`);
}

/** Kill the broker, destroy its database, and bring it back empty. */
async function resetBroker(proc: ReturnType<typeof Bun.spawn>, port: number, db: string) {
  proc.kill();
  await Bun.sleep(400);
  removeDatabase(db);
  return startBroker(port, db);
}

afterAll(() => {
  for (const proc of spawned) {
    try {
      proc.kill();
    } catch {
      // already gone
    }
  }
  for (const path of tempDbs) removeDatabase(path);
  for (const dir of tempDirs) rmSync(dir, { force: true, recursive: true });
});

test("the broker recreates its database after the file is deleted underneath it", async () => {
  // Previously the handle went to a dead vnode and stayed there: /health and
  // every POST answered 500 `disk I/O error` for the life of the process, and
  // the file was never recreated.
  const port = PORTS.brokerLoss;
  const db = tmpDb(port);
  await startBroker(port, db);

  const before = await register(port, "/tmp/recovery-before");
  expect(before.id).toMatch(/^[a-z0-9]{8}$/);

  removeDatabase(db);

  const health = await within(6000, async () => {
    const res = await fetch(`http://127.0.0.1:${port}/health`);
    return res.ok ? ((await res.json()) as { status: string }) : null;
  });
  expect(health.status).toBe("ok");

  // Registration works again, which is what lets a live session recover.
  const after = await register(port, "/tmp/recovery-after");
  expect(after.token).not.toBe(before.token);
  expect((await rawPost(port, "/heartbeat", { id: after.id }, after.token)).status).toBe(200);

  // The recreated file keeps the permissions the original had: it holds every
  // inter-agent message, and SQLite creates it 0644 by default.
  expect(existsSync(db)).toBe(true);
  expect(statSync(db).mode & 0o077).toBe(0);
}, 30_000);

test("a token minted before the loss is refused with 401, not a 500", async () => {
  // The client's whole recovery path keys on 401. If the broker answers 500
  // instead, a session cannot tell "your identity is gone, register again" from
  // "the broker is briefly unwell", and re-registering on the latter would be a
  // registration storm.
  const port = PORTS.tokenAfterLoss;
  const db = tmpDb(port);
  await startBroker(port, db);

  const peer = await register(port, "/tmp/recovery-stale-token");
  removeDatabase(db);
  await within(6000, async () => ((await fetch(`http://127.0.0.1:${port}/health`)).ok ? true : null));

  const refused = await rawPost(port, "/poll-messages", { id: peer.id }, peer.token);
  expect(refused.status).toBe(401);
}, 30_000);

test("a session re-registers and keeps messaging after the broker database is reset", async () => {
  // The 401-forever defect. Both sessions stay up across the reset; without a
  // re-registration path every later call is refused and the only remedy is
  // restarting each Claude Code session.
  const port = PORTS.sessionReset;
  const db = tmpDb(port);
  let broker = await startBroker(port, db);

  const dirA = workdir();
  const dirB = workdir();
  // A has a channel, so its poll loop runs and must recover on its own, with no
  // tool call at all. B has none, so it recovers on its first tool call.
  const a = spawnClient(dirA, port, db, "always");
  const b = spawnClient(dirB, port, db, "never");

  await a.initialize(1);
  await b.initialize(1);
  await Bun.sleep(3500); // registration plus the auto-summary window

  const idA0 = peerIdAt(await b.tool(2, "list_peers", { scope: "machine" }), dirA);
  const idB0 = peerIdAt(await a.tool(2, "list_peers", { scope: "machine" }), dirB);

  broker = await resetBroker(broker, port, db);

  // B's first call after the reset is refused, re-registers, and retries.
  expect(await b.tool(3, "check_messages")).toContain("No new messages");

  // A never called a tool: its poll loop is what has to notice and recover, and
  // the new id has to reach the outgoing bodies, not just a variable.
  const idA1 = await within(8000, async () => {
    const listing = await b.tool(100 + Math.floor(Math.random() * 800), "list_peers", { scope: "machine" });
    try {
      return peerIdAt(listing, dirA);
    } catch {
      return null;
    }
  });
  expect(idA1).not.toBe(idA0);

  const idB1 = peerIdAt(await a.tool(4, "list_peers", { scope: "machine" }), dirB);
  expect(idB1).not.toBe(idB0);

  // send_message carries the caller's id as from_id, so this only works if the
  // re-registration rewrote the body rather than only the module variable.
  expect(await a.tool(5, "send_message", { to_id: idB1, message: "survived the reset" })).toContain(
    "Message sent"
  );
  await Bun.sleep(1500);
  expect(await b.tool(6, "check_messages")).toContain("survived the reset");
}, 60_000);

/**
 * A broker stand-in that counts registrations and can revoke an identity on cue.
 *
 * The real broker cannot hold a registration open, and without a window there is
 * no storm to observe: whoever refuses first has already finished registering by
 * the time the next caller is refused. This one delays /register, so every
 * caller refused during that window is genuinely concurrent.
 *
 * /poll-messages is deliberately never refused, so the server's own poll loop
 * stays out of the count and the calls under test are the only ones recovering.
 */
function fakeBroker(port: number) {
  const state = {
    registrations: 0,
    validId: "",
    validToken: "",
    registerDelayMs: 0,
    /** Mint identities that are refused immediately, as a broker losing its database repeatedly would. */
    mintDeadIdentities: false,
  };

  const server = Bun.serve({
    port,
    hostname: "127.0.0.1",
    async fetch(req) {
      const path = new URL(req.url).pathname;
      if (req.method !== "POST") return Response.json({ status: "ok", peers: 0 });

      const body = (await req.json()) as Record<string, unknown>;

      if (path === "/register") {
        state.registrations++;
        const n = state.registrations;
        if (state.registerDelayMs) await Bun.sleep(state.registerDelayMs);
        const id = `sess${String(n).padStart(4, "0")}`;
        const token = `token${String(n).padStart(4, "0")}`;
        state.validId = state.mintDeadIdentities ? "__revoked__" : id;
        state.validToken = state.mintDeadIdentities ? "__revoked__" : token;
        return Response.json({ id, token });
      }

      if (path === "/poll-messages") return Response.json({ messages: [] });

      const presented = (req.headers.get("authorization") ?? "").replace(/^Bearer /, "");
      const claimed = body[CALLER_FIELD[path] ?? "id"];
      if (presented !== state.validToken || claimed !== state.validId) {
        return Response.json({ error: "unauthorized" }, { status: 401 });
      }

      if (path === "/list-peers") return Response.json([]);
      return Response.json({ ok: true, acked: 0 });
    },
  });

  return { state, stop: () => server.stop(true) };
}

/** Mirrors the map in server.ts: which body field names the calling peer. */
const CALLER_FIELD: Record<string, string> = {
  "/heartbeat": "id",
  "/set-summary": "id",
  "/list-peers": "exclude_id",
  "/send-message": "from_id",
  "/poll-messages": "id",
  "/ack-messages": "peer_id",
  "/unregister": "id",
};

test("concurrent refusals cause exactly one re-registration, not a storm", async () => {
  // Three calls are in flight when the identity dies. Each is refused, and
  // without single-flight each registers a peer of its own, leaving the session
  // holding several identities, only the last of which anyone can reach, and
  // the broker holding rows nobody will ever answer to.
  const port = PORTS.singleFlight;
  const broker = fakeBroker(port);
  try {
    const client = spawnClient(workdir(), port, tmpDb(port), "never");
    await client.initialize(1);
    await within(10_000, async () => (broker.state.registrations === 1 ? true : null));

    // The identity dies, and registering is slow enough that every refusal
    // lands inside the same window.
    broker.state.validId = "__gone__";
    broker.state.validToken = "__gone__";
    broker.state.registerDelayMs = 500;

    const results = await Promise.all([
      client.tool(2, "list_peers", { scope: "machine" }),
      client.tool(3, "list_peers", { scope: "directory" }),
      client.tool(4, "set_summary", { summary: "still here" }),
    ]);
    const registrations = broker.state.registrations;

    expect(registrations).toBe(2); // one at startup, one recovery shared by all three
    for (const text of results) expect(text).not.toContain("Error");
  } finally {
    broker.stop();
  }
}, 60_000);

test("a refusal that survives re-registration is reported, not retried forever", async () => {
  // Recovery has to be bounded. A broker that refuses the freshly minted
  // identity too, because its database is being lost repeatedly, must produce
  // one failed call rather than a session registering in a loop.
  const port = PORTS.boundedRetry;
  const broker = fakeBroker(port);
  try {
    const client = spawnClient(workdir(), port, tmpDb(port), "never");
    await client.initialize(1);
    await within(10_000, async () => (broker.state.registrations === 1 ? true : null));

    broker.state.validId = "__gone__";
    broker.state.validToken = "__gone__";
    broker.state.mintDeadIdentities = true;

    const before = broker.state.registrations;
    const text = await client.tool(2, "list_peers", { scope: "machine" });
    expect(text).toContain("401");
    expect(broker.state.registrations - before).toBe(1); // exactly one attempt
  } finally {
    broker.stop();
  }
}, 60_000);
