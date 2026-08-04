/**
 * Peer names through the real MCP stdio surface.
 *
 * The broker tests prove the storage and auth; these prove what a session
 * actually experiences: claiming a name with set_name, reading it back with
 * whoami, being addressed by name from another session, and the pushed channel
 * notification carrying the sender's name.
 *
 * The meta-strings assertion exists because of a lesson imported from the synaq
 * fork: Claude Code Zod-validates channel notifications and a single non-string
 * meta value kills the whole stdio connection. Every value pushed must be a string.
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
const WORK = trackedTempDir("peers-servernames-");
const DB = join(WORK, "broker.db");
const ENV = {
  ...process.env,
  CLAUDE_PEERS_PORT: String(PORT),
  CLAUDE_PEERS_DB: DB,
  // Never the real spool: servers here would otherwise walk their process tree
  // to the developer's live session and queue test traffic into it.
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
        clientInfo: { name: "names-test", version: "1" },
      });
    },
    async tool(id: number, name: string, args: Record<string, unknown> = {}) {
      const res = await this.call(id, "tools/call", { name, arguments: args });
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

test("set_name + whoami roundtrip, and a second session cannot take the name", async () => {
  const dirA = trackedTempDir("peers-names-a-");
  const dirB = trackedTempDir("peers-names-b-");
  const a = spawnClient(dirA);
  const b = spawnClient(dirB);
  await a.initialize(1);
  await b.initialize(1);
  await Bun.sleep(1500); // registration

  expect(await a.tool(2, "set_name", { name: "atlas" })).toContain('Name claimed: "atlas"');

  const who = await a.tool(3, "whoami");
  expect(who).toContain("Name: atlas");
  expect(who).toContain(`CWD: ${dirA}`);

  const clash = await b.tool(2, "set_name", { name: "Atlas" });
  expect(clash).toContain("Could not claim name");
});

test("a message sent to a NAME is delivered, and the pushed meta carries from_name with every value a string", async () => {
  const dirS = trackedTempDir("peers-names-s-");
  const dirR = trackedTempDir("peers-names-r-");
  const sender = spawnClient(dirS);
  const receiver = spawnClient(dirR, { CLAUDE_PEERS_CHANNEL: "always" });
  await sender.initialize(1);
  await receiver.initialize(1);
  await Bun.sleep(1500);

  expect(await sender.tool(2, "set_name", { name: "hermes" })).toContain("Name claimed");
  expect(await receiver.tool(2, "set_name", { name: "hestia" })).toContain("Name claimed");

  // Addressed by NAME, not id
  expect(await sender.tool(3, "send_message", { to_id: "hestia", message: "named delivery" })).toContain(
    "Message sent"
  );
  await Bun.sleep(2500);

  const pushed = receiver.notifications.filter((n) => String(n.method).includes("channel"));
  expect(pushed.length).toBeGreaterThan(0);
  const params = pushed[0].params;
  expect(String(params.content)).toContain("named delivery");
  expect(params.meta.from_name).toBe("hermes");
  // The synaq lesson: one non-string meta value kills the stdio connection.
  for (const [k, v] of Object.entries(params.meta)) {
    expect(typeof v, `meta.${k} must be a string`).toBe("string");
  }
}, 30_000);

test("CLAUDE_PEERS_NAME claims the name at startup without a tool call", async () => {
  const dirE = trackedTempDir("peers-names-e-");
  const dirW = trackedTempDir("peers-names-w-");
  const envNamed = spawnClient(dirE, { CLAUDE_PEERS_NAME: "env-claimed" });
  const watcher = spawnClient(dirW);
  await envNamed.initialize(1);
  await watcher.initialize(1);
  await Bun.sleep(1500);

  const listing = await watcher.tool(2, "list_peers", { scope: "machine" });
  const block = listing.split(/\n(?=ID: )/).find((s) => s.includes(`CWD: ${dirE}`));
  expect(block).toBeDefined();
  expect(block!).toContain("Name: env-claimed");
});
