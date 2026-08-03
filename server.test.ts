/**
 * End-to-end tests for the MCP server itself, driven over stdio.
 *
 * These exist because the broker tests exercise the HTTP surface directly and
 * therefore cannot see how a real client behaves. The delivery path depends on
 * whether the client can render channel notifications, and a client that cannot
 * previously lost messages outright: the server pushed into the void, then
 * acknowledged, deleting the message before check_messages could return it.
 */

import { test, expect, beforeAll, afterAll } from "bun:test";
import { rmSync, mkdtempSync, realpathSync } from "node:fs";

const PORT = 7961 + Math.floor(Math.random() * 30);
const DB = `${process.env.TMPDIR ?? "/tmp"}/claude-peers-servertest-${PORT}.db`;
const ENV = { ...process.env, CLAUDE_PEERS_PORT: String(PORT), CLAUDE_PEERS_DB: DB };

const clients: { proc: ReturnType<typeof Bun.spawn> }[] = [];

function spawnClient(cwd: string, withChannel: boolean, channelMode?: string) {
  const proc = Bun.spawn(["bun", `${import.meta.dir}/server.ts`], {
    cwd,
    env: channelMode ? { ...ENV, CLAUDE_PEERS_CHANNEL: channelMode } : ENV,
    stdin: "pipe",
    stdout: "pipe",
    stderr: "ignore",
  });

  const replies = new Map<number, any>();
  const notifications: any[] = [];

  // One persistent reader. Re-entering `for await` on stdout cancels the
  // stream, which silently truncates every later response.
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

  const client = {
    proc,
    notifications,
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
        capabilities: withChannel ? { experimental: { "claude/channel": {} } } : {},
        clientInfo: { name: "test", version: "1" },
      });
    },
    async tool(id: number, name: string, args: Record<string, unknown> = {}) {
      const res = await this.call(id, "tools/call", { name, arguments: args });
      return String(res.result.content[0].text);
    },
  };

  clients.push(client);
  return client;
}

/**
 * Resolve a peer id by working directory.
 *
 * Clients from earlier tests stay registered until afterAll, so picking the
 * first id in the listing targets whichever session happened to register first.
 */
function peerIdAt(listing: string, cwd: string): string {
  const blocks = listing.split(/\n(?=ID: )/);
  for (const block of blocks) {
    if (!block.includes(`CWD: ${cwd}`)) continue;
    const id = block.match(/ID: ([a-z0-9]{8})/)?.[1];
    if (id) return id;
  }
  throw new Error(`no peer at ${cwd} in:\n${listing}`);
}

/** Each client gets its own directory so it is addressable unambiguously. */
function workdir(): string {
  // realpath matters: TMPDIR is a symlink on macOS (/var -> /private/var) and
  // may carry a trailing slash, so the raw path never matches the cwd the
  // server reports back through list_peers.
  return realpathSync(mkdtempSync(`${(process.env.TMPDIR ?? "/tmp").replace(/\/$/, "")}/peers-test-`));
}

let broker: ReturnType<typeof Bun.spawn>;

beforeAll(async () => {
  for (const suffix of ["", "-wal", "-shm"]) rmSync(`${DB}${suffix}`, { force: true });

  // Start the broker here rather than letting the first server auto-spawn it.
  // A detached grandchild does not settle reliably inside the test runner, and
  // every client then blocks waiting for a broker that never answers.
  broker = Bun.spawn(["bun", `${import.meta.dir}/broker.ts`], {
    env: ENV,
    stdout: "ignore",
    stderr: "ignore",
  });
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

afterAll(async () => {
  for (const c of clients) c.proc.kill();
  broker?.kill();
  try {
    await fetch(`http://127.0.0.1:${PORT}/health`).then((r) => r.text());
  } catch {
    // broker already gone
  }
  Bun.spawnSync(["sh", "-c", `lsof -ti :${PORT} | xargs kill`]);
  for (const suffix of ["", "-wal", "-shm"]) rmSync(`${DB}${suffix}`, { force: true });
});

test("a client that cannot render channel notifications still receives messages", async () => {
  // The regression this file exists for. Previously the server pushed the
  // notification regardless of client capability, acknowledged it, and deleted
  // the row, so check_messages reported "No new messages" and the message was
  // unrecoverable.
  const senderDir = workdir();
  const receiverDir = workdir();
  const sender = spawnClient(senderDir, true);
  const receiver = spawnClient(receiverDir, false); // no channel capability

  await sender.initialize(1);
  await receiver.initialize(1);
  await Bun.sleep(3000); // registration and auto-summary

  const target = peerIdAt(await sender.tool(2, "list_peers", { scope: "machine" }), receiverDir);
  await sender.tool(3, "send_message", { to_id: target, message: "delivered without a channel" });
  await Bun.sleep(2500);

  const inbox = await receiver.tool(2, "check_messages");
  expect(inbox).toContain("delivered without a channel");
}, 30_000);

test("check_messages acknowledges, so a message is not returned twice", async () => {
  const senderDir = workdir();
  const receiverDir = workdir();
  const sender = spawnClient(senderDir, true);
  const receiver = spawnClient(receiverDir, false);

  await sender.initialize(1);
  await receiver.initialize(1);
  await Bun.sleep(3000);

  const target = peerIdAt(await sender.tool(2, "list_peers", { scope: "machine" }), receiverDir);
  await sender.tool(3, "send_message", { to_id: target, message: "exactly once" });
  await Bun.sleep(2500);

  expect(await receiver.tool(2, "check_messages")).toContain("exactly once");
  expect(await receiver.tool(3, "check_messages")).toContain("No new messages");
}, 30_000);

test("a channel-enabled session is still pushed to immediately", async () => {
  // The gate must not cost the fast path its push. Note the signal is the
  // session's channel mode, NOT an advertised client capability: Claude Code
  // never advertises one.
  const senderDir = workdir();
  const receiverDir = workdir();
  const sender = spawnClient(senderDir, false);
  const receiver = spawnClient(receiverDir, false, "always");

  await sender.initialize(1);
  await receiver.initialize(1);
  await Bun.sleep(3000);

  const target = peerIdAt(await sender.tool(2, "list_peers", { scope: "machine" }), receiverDir);
  await sender.tool(3, "send_message", { to_id: target, message: "pushed over the channel" });
  await Bun.sleep(2500);

  const pushed = receiver.notifications.filter((n) => String(n.method).includes("channel"));
  expect(pushed.length).toBeGreaterThan(0);
  expect(JSON.stringify(pushed[0].params)).toContain("pushed over the channel");
}, 30_000);

test("a real Claude Code client is pushed to, despite advertising no experimental capability", async () => {
  // The production shape. Claude Code's initialize frame carries only
  // {roots, elicitation}: it never advertises experimental["claude/channel"],
  // even when launched with --dangerously-load-development-channels. Gating the
  // push on that capability therefore disabled instant delivery for every real
  // session while the tests, which invented the capability, stayed green.
  const senderDir = workdir();
  const receiverDir = workdir();
  const sender = spawnClient(senderDir, false);
  const receiver = spawnClient(receiverDir, false, "always"); // advertises nothing

  await sender.initialize(1);
  await receiver.initialize(1);
  await Bun.sleep(3000);

  const target = peerIdAt(await sender.tool(2, "list_peers", { scope: "machine" }), receiverDir);
  await sender.tool(3, "send_message", { to_id: target, message: "pushed without advertising" });
  await Bun.sleep(2500);

  const pushed = receiver.notifications.filter((n) => String(n.method).includes("channel"));
  expect(pushed.length).toBeGreaterThan(0);
  expect(JSON.stringify(pushed[0].params)).toContain("pushed without advertising");
}, 30_000);

test("channel mode never falls back to check_messages", async () => {
  const senderDir = workdir();
  const receiverDir = workdir();
  const sender = spawnClient(senderDir, false);
  const receiver = spawnClient(receiverDir, true, "never"); // advertises, but disabled

  await sender.initialize(1);
  await receiver.initialize(1);
  await Bun.sleep(3000);

  const target = peerIdAt(await sender.tool(2, "list_peers", { scope: "machine" }), receiverDir);
  await sender.tool(3, "send_message", { to_id: target, message: "queued not pushed" });
  await Bun.sleep(2500);

  expect(receiver.notifications.filter((n) => String(n.method).includes("channel"))).toHaveLength(0);
  expect(await receiver.tool(2, "check_messages")).toContain("queued not pushed");
}, 30_000);
