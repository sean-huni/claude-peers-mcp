/**
 * A non-Claude agent session joining the network as a first-class peer.
 *
 * Codex CLI can consume MCP servers, so it can run THIS server: no second implementation and no
 * adapter. Three signals are Claude-shaped, and two of them are carried by an environment seam that
 * already existed:
 *
 *   - channel push, which only Claude Code renders (`CLAUDE_PEERS_CHANNEL=never`),
 *   - the spool directory (`CLAUDE_PEERS_SPOOL_DIR`),
 *   - the session identity the spool is keyed on, which is the one that is NOT carried.
 *
 * `CLAUDE_PEERS_SESSION_PID` cannot serve the third. Measured against codex-cli 0.145.0 on darwin:
 * Codex passes `env` values from config.toml literally, so `CLAUDE_PEERS_SESSION_PID=$PPID` reaches
 * the server as the five characters `$PPID`, and a session's pid is not knowable when the config
 * entry is written in any case. The identity therefore has to come from the process tree, and the
 * walk that finds it had to learn that `codex` bounds a session exactly as `claude` does. That one
 * predicate is the whole production change; these tests are what forced it.
 *
 * The vendor cannot be spawned in CI, and a test that needed a ChatGPT account would be a test of
 * OpenAI's uptime rather than of this code. So what is tested here is the SEAM: the server is run
 * with every Claude-specific signal absent, under an ancestor that is not `claude`, and is required
 * to register, be seen, send, receive, and queue for a drain exactly as a Claude session does.
 *
 * The ancestor is a symlink named `codex` pointing at a shell. That is not a mock of the session
 * resolver: the resolver reads `ps -o command=` and takes the executable, and the real Codex CLI is
 * a binary named `codex` on PATH, so what it sees here is byte-for-byte what it sees in a real
 * Codex session. Verified the same way: the MCP server's parent process is `codex` itself, with no
 * wrapper in between, and four levels above it sits the `claude` that started the Codex session.
 */

import { test, expect, beforeAll, afterAll } from "bun:test";
import { existsSync, readFileSync, symlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { findSessionPid } from "./spool";
import {
  cleanupAll,
  reserveFreePort,
  sweepBrokerOnPort,
  trackProcess,
  trackedTempDir,
} from "./testsupport";

const PORT = reserveFreePort();
const WORK = trackedTempDir("peers-codextest-");
const SPOOL = join(WORK, "spool");

// The override is the one thing that would make every assertion below meaningless: inherited, it
// would answer the session question before the walk ever ran, and the codex peer would pass while
// resolving nothing. Cleared in this process (findSessionPid reads it here, in-process) and omitted
// from the child environment, so the walk is genuinely what is under test.
delete process.env.CLAUDE_PEERS_SESSION_PID;

const { CLAUDE_PEERS_SESSION_PID: _ignored, ...INHERITED } = process.env;
const ENV = {
  ...INHERITED,
  CLAUDE_PEERS_PORT: String(PORT),
  CLAUDE_PEERS_DB: join(WORK, "broker.db"),
  // Never the real one under $HOME. A server here may resolve a session pid of its own, and
  // without this it would write into a queue a live session's hook is draining into someone's
  // context.
  CLAUDE_PEERS_SPOOL_DIR: SPOOL,
};

/** Pids of processes started outside Bun.spawn's bookkeeping, killed unconditionally at the end. */
const strayPids: number[] = [];

/**
 * A directory holding one shim per agent executable name.
 *
 * Symlinks to a shell, because the resolver matches on the executable name and a shell can hold a
 * child open. Copying would work identically and cost a megabyte.
 */
function agentShimDir(): string {
  const dir = trackedTempDir("peers-agentbin-");
  for (const name of ["claude", "codex"]) symlinkSync("/bin/zsh", join(dir, name));
  return dir;
}

function spawnCodexAncestor(script: string, cwd?: string) {
  const codex = join(agentShimDir(), "codex");
  return trackProcess(
    Bun.spawn([codex, "-c", script], {
      ...(cwd ? { cwd } : {}),
      env: ENV,
      stdin: "pipe",
      stdout: "pipe",
      stderr: "ignore",
    })
  );
}

/** The direct children of a pid, so a shim's child is killed with it rather than orphaned. */
function childrenOf(pid: number): number[] {
  const out = new TextDecoder().decode(Bun.spawnSync(["pgrep", "-P", String(pid)]).stdout);
  return out
    .split("\n")
    .map((line) => Number.parseInt(line.trim(), 10))
    .filter((n) => Number.isFinite(n) && n > 1);
}

async function waitForFile(path: string, timeoutMs = 10_000): Promise<string> {
  for (let i = 0; i < timeoutMs / 100; i++) {
    if (existsSync(path)) {
      const text = readFileSync(path, "utf8").trim();
      if (text.length > 0) return text;
    }
    await Bun.sleep(100);
  }
  throw new Error(`${path} never appeared`);
}

/**
 * An MCP client speaking JSON-RPC over the pipes of a spawned process.
 *
 * Lifted from server.test.ts, including the one persistent reader: re-entering `for await` on
 * stdout cancels the stream and silently truncates every later response.
 */
type McpProc = Bun.Subprocess<"pipe", "pipe", "ignore">;

function mcpClient(proc: McpProc) {
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
    /**
     * Codex advertises no experimental capabilities, so this is what the server actually sees from
     * a non-Claude client.
     */
    async initialize(id: number, capabilities: Record<string, unknown> = {}) {
      return this.call(id, "initialize", {
        protocolVersion: "2024-11-05",
        capabilities,
        clientInfo: { name: "codex", version: "0.145.0" },
      });
    },
    async tool(id: number, name: string, args: Record<string, unknown> = {}) {
      const res = await this.call(id, "tools/call", { name, arguments: args });
      return String(res.result.content[0].text);
    },
  };
}

/** A Claude-style peer: a resolvable session of its own, no channel, exactly as today's tests run. */
function spawnClaudePeer(cwd: string) {
  const session = trackProcess(Bun.spawn(["sleep", "600"], { stdout: "ignore", stderr: "ignore" }));
  const proc = trackProcess(
    Bun.spawn(["bun", `${import.meta.dir}/server.ts`], {
      cwd,
      env: { ...ENV, CLAUDE_PEERS_SESSION_PID: String(session.pid), CLAUDE_PEERS_CHANNEL: "never" },
      stdin: "pipe",
      stdout: "pipe",
      stderr: "ignore",
    })
  );
  return mcpClient(proc);
}

/**
 * A Codex-style peer: the unmodified server under an ancestor named `codex`, with NO
 * CLAUDE_PEERS_SESSION_PID, so the session identity has to be derived the way a real Codex session
 * forces it to be.
 *
 * `; true` matters: zsh replaces itself with a lone command, which would delete the very ancestor
 * this is here to provide.
 */
function spawnCodexPeer(cwd: string) {
  const proc = spawnCodexAncestor(`bun ${JSON.stringify(`${import.meta.dir}/server.ts`)}; true`, cwd);
  return { codexPid: proc.pid, client: mcpClient(proc) };
}

function peerIdAt(listing: string, cwd: string): string {
  const blocks = listing.split(/\n(?=ID: )/);
  for (const block of blocks) {
    if (!block.includes(`CWD: ${cwd}`)) continue;
    const id = block.match(/ID: ([a-z0-9]{8})/)?.[1];
    if (id) return id;
  }
  throw new Error(`no peer at ${cwd} in:\n${listing}`);
}

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
    for (const pid of strayPids) {
      try {
        process.kill(pid, "SIGKILL");
      } catch {
        // already gone
      }
    }
    cleanupAll();
  } finally {
    sweepBrokerOnPort(PORT);
  }
});

test("a session under a codex ancestor resolves that session, not a claude one further up", async () => {
  // The whole point of the walk is that the queue belongs to ONE session. A Codex session started
  // from a Claude Code session (a subagent shelling out, which is the normal way it happens here)
  // has a `claude` ancestor several levels up, and spooling to it would put one agent's messages in
  // another agent's context.
  const dir = trackedTempDir("peers-codexpid-");
  const childPidFile = join(dir, "child.pid");
  const shim = spawnCodexAncestor(
    `sleep 600 & echo $! > ${JSON.stringify(childPidFile)}; wait`
  );
  const childPid = Number.parseInt(await waitForFile(childPidFile), 10);
  strayPids.push(childPid);

  expect(findSessionPid(childPid)).toBe(shim.pid);
}, 20_000);

test("a codex ancestor nested under a claude ancestor wins, because it is nearer", async () => {
  // The hazard this exists for, built deterministically rather than relying on the runner happening
  // to have a `claude` above it. Measured on darwin with codex-cli 0.145.0: a Codex session started
  // from a Claude Code session has `codex` as the MCP server's parent and a live `claude` four
  // levels above that. Resolving the further one is not a crash, it is one agent's messages spooled
  // into another agent's context, so "nearest wins" is the assertion that matters.
  const dir = agentShimDir();
  const claudeBin = join(dir, "claude");
  const codexBin = join(dir, "codex");
  const childPidFile = join(dir, "child.pid");

  // A script file rather than another -c string: the inner shell is invoked AS `codex`, so argv[0]
  // is what the resolver reads, and nesting quoted -c strings three deep is unreadable.
  const inner = join(dir, "inner.sh");
  writeFileSync(inner, `sleep 600 & echo $! > ${JSON.stringify(childPidFile)}\nwait\n`);

  // `; true` at each level: zsh replaces itself with a lone final command, which would delete the
  // very ancestor the level exists to provide.
  const claudeProc = trackProcess(
    Bun.spawn([claudeBin, "-c", `${JSON.stringify(codexBin)} ${JSON.stringify(inner)}; true`], {
      env: ENV,
      stdout: "ignore",
      stderr: "ignore",
    })
  );

  const childPid = Number.parseInt(await waitForFile(childPidFile), 10);
  strayPids.push(childPid);

  // Fail closed rather than assert against undefined: if the codex level never started, the
  // comparison below would be between two absent things and would report a pass.
  const [codexPid] = childrenOf(claudeProc.pid);
  if (codexPid === undefined) throw new Error("the codex shim never started, so nothing was nested");
  strayPids.push(codexPid);

  expect(findSessionPid(childPid)).toBe(codexPid);
  expect(findSessionPid(childPid)).not.toBe(claudeProc.pid);
}, 20_000);

test("no agent ancestor at all resolves to null rather than a guess", async () => {
  // An orphan: the shell exits immediately, so the child reparents to pid 1 and the walk runs out
  // of tree. Returning null is what keeps a session that cannot be identified from writing into
  // somebody else's queue.
  const dir = trackedTempDir("peers-orphan-");
  const pidFile = join(dir, "orphan.pid");
  Bun.spawnSync(["/bin/sh", "-c", `sleep 600 & echo $! > ${JSON.stringify(pidFile)}`]);
  const orphanPid = Number.parseInt(await waitForFile(pidFile), 10);
  strayPids.push(orphanPid);

  expect(findSessionPid(orphanPid)).toBeNull();
}, 20_000);

test("a codex session registers, is listed, and exchanges messages with a claude session", async () => {
  const claudeDir = trackedTempDir("peers-claude-");
  const codexDir = trackedTempDir("peers-codex-");
  const claude = spawnClaudePeer(claudeDir);
  const { client: codex, codexPid } = spawnCodexPeer(codexDir);
  strayPids.push(...childrenOf(codexPid));

  await claude.initialize(1);
  // No experimental capabilities: this is the real Codex initialize frame.
  await codex.initialize(1);
  await Bun.sleep(4000); // registration and auto-summary
  strayPids.push(...childrenOf(codexPid));

  // Seen by the Claude session, in the same listing as any other peer.
  const listing = await claude.tool(2, "list_peers", { scope: "machine" });
  const codexId = peerIdAt(listing, codexDir);

  // Claude to Codex.
  await claude.tool(3, "send_message", { to_id: codexId, message: "ping from claude" });
  await Bun.sleep(2500);
  expect(await codex.tool(2, "check_messages")).toContain("ping from claude");

  // Codex to Claude.
  const claudeId = peerIdAt(await codex.tool(3, "list_peers", { scope: "machine" }), claudeDir);
  await codex.tool(4, "send_message", { to_id: claudeId, message: "pong from codex" });
  await Bun.sleep(2500);
  expect(await claude.tool(4, "check_messages")).toContain("pong from codex");
}, 60_000);

test("a codex session queues inbound messages under its own session id", async () => {
  // Without a channel the poll loop spools, and the file name IS the session identity. A Codex
  // session that cannot resolve one spools nowhere, which leaves nothing for a drain to find.
  const claudeDir = trackedTempDir("peers-claude-");
  const codexDir = trackedTempDir("peers-codex-");
  const claude = spawnClaudePeer(claudeDir);
  const { client: codex, codexPid } = spawnCodexPeer(codexDir);
  strayPids.push(...childrenOf(codexPid));

  await claude.initialize(1);
  await codex.initialize(1);
  await Bun.sleep(4000);
  strayPids.push(...childrenOf(codexPid));

  const codexId = peerIdAt(await claude.tool(2, "list_peers", { scope: "machine" }), codexDir);
  await claude.tool(3, "send_message", { to_id: codexId, message: "queued for the drain" });

  const spoolFile = join(SPOOL, `${codexPid}.jsonl`);
  for (let i = 0; i < 60; i++) {
    if (existsSync(spoolFile)) break;
    await Bun.sleep(250);
  }
  expect(existsSync(spoolFile)).toBe(true);
  expect(readFileSync(spoolFile, "utf8")).toContain("queued for the drain");
}, 60_000);
