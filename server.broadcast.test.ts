/**
 * End-to-end test for the broadcast_message tool, driven over stdio like a real client.
 *
 * The broker tests prove the fan-out. What they cannot see is what happens after it, and that is
 * the part broadcast could plausibly get wrong: a fanned-out message must arrive by whichever
 * delivery path each recipient session has, not by whichever path the SENDER has. So the two
 * receivers here differ in exactly that respect, one channel-enabled and one not, and each is
 * asserted on its own path.
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
const WORK = trackedTempDir("peers-bcastsrvtest-");
const DB = join(WORK, "broker.db");
const ENV = {
  ...process.env,
  CLAUDE_PEERS_PORT: String(PORT),
  CLAUDE_PEERS_DB: DB,
  // Never the real one under $HOME. Without this the servers spawned here resolve a session pid by
  // walking their own process tree, land on the developer's live Claude Code session, and spool
  // test traffic into a queue a hook is actively draining into someone's context.
  CLAUDE_PEERS_SPOOL_DIR: join(WORK, "spool"),
};

/** Each client is told which session it belongs to, so it gets an isolated queue of its own. */
function spawnClient(cwd: string, channelMode?: string) {
  const session = trackProcess(Bun.spawn(["sleep", "600"], { stdout: "ignore", stderr: "ignore" }));
  const proc = trackProcess(
    Bun.spawn(["bun", `${import.meta.dir}/server.ts`], {
      cwd,
      env: {
        ...ENV,
        CLAUDE_PEERS_SESSION_PID: String(session.pid),
        ...(channelMode ? { CLAUDE_PEERS_CHANNEL: channelMode } : {}),
      },
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
        clientInfo: { name: "test", version: "1" },
      });
    },
    async tool(id: number, name: string, args: Record<string, unknown> = {}) {
      const res = await this.call(id, "tools/call", { name, arguments: args });
      return String(res.result.content[0].text);
    },
    async tools(id: number): Promise<{ name: string; description: string }[]> {
      const res = await this.call(id, "tools/list", {});
      return res.result.tools;
    },
  };
}

/** Each client gets its own tracked directory so it is addressable unambiguously. */
function workdir(): string {
  return trackedTempDir("peers-bcast-");
}

let broker: ReturnType<typeof Bun.spawn>;

beforeAll(async () => {
  // Started here rather than letting the first server auto-spawn it: a detached grandchild does not
  // settle reliably inside the test runner.
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
    sweepBrokerOnPort(PORT);
  }
});

test("one broadcast reaches a channel session and a spooled session by their own paths", async () => {
  const sender = spawnClient(workdir());
  const pushed = spawnClient(workdir(), "always"); // renders channel notifications
  const spooled = spawnClient(workdir(), "never"); // must fall back to the queue

  await sender.initialize(1);
  await pushed.initialize(1);
  await spooled.initialize(1);
  await Bun.sleep(3000); // registration and auto-summary

  const ack = await sender.tool(2, "broadcast_message", {
    message: "the auth contract changed, pull before you build",
    scope: "machine",
  });
  expect(ack).toContain("delivered to 2 peer");
  await Bun.sleep(2500);

  const notes = pushed.notifications.filter((n) => String(n.method).includes("channel"));
  expect(notes.length).toBeGreaterThan(0);
  expect(JSON.stringify(notes[0].params)).toContain("the auth contract changed");

  // The other receiver has no channel, so nothing was pushed at it and the queue is the only path.
  expect(spooled.notifications.filter((n) => String(n.method).includes("channel"))).toHaveLength(0);
  expect(await spooled.tool(2, "check_messages")).toContain("the auth contract changed");

  // The sender is not its own audience.
  expect(await sender.tool(3, "check_messages")).toContain("No new messages");
}, 40_000);

test("the broadcast tool is advertised and says when to prefer a unicast", async () => {
  // An over-eager broadcast is noise in every other session on the machine, so the description has
  // to steer the model toward send_message for anything addressed to one peer.
  const client = spawnClient(workdir());
  await client.initialize(1);

  const tools = await client.tools(2);
  const broadcast = tools.find((t) => t.name === "broadcast_message");
  expect(broadcast).toBeDefined();
  // Merely MENTIONING send_message is not guidance: the description has to name it as the thing to
  // do instead, and say why, or the model has no basis for choosing between them. Asserted as the
  // phrase rather than the bare word because the word also appears in a later sentence, so a check
  // for it alone stayed green when the guidance itself was deleted.
  expect(broadcast!.description).toMatch(/send_message instead/);
  expect(broadcast!.description).toMatch(/noise|interrupt/i);
}, 30_000);
