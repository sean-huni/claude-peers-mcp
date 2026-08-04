#!/usr/bin/env bun
/**
 * claude-peers CLI
 *
 * Utility commands for managing the broker and inspecting peers.
 *
 * Usage:
 *   bun cli.ts status          Show broker status and all peers
 *   bun cli.ts peers           List all peers
 *   bun cli.ts send <id> <msg> Send a message to a peer
 *   bun cli.ts broadcast <msg> Send a message to every peer on the machine
 *   bun cli.ts inbox           Drain messages spooled for THIS session (used by the hook)
 *   bun cli.ts kill-broker     Stop the broker daemon
 *
 * Codex delivery (see docs/codex-delivery.md):
 *   bun cli.ts codex-inbox         Hook entry point: Codex hook JSON in, hook JSON out
 *   bun cli.ts codex-nudge-status  Codex sessions holding mail they cannot collect yet
 */

import { drainSpool, executableMatches, findSessionPid, hasSpooled, spoolDir } from "./spool";
import { drainForHook, lastHookActivity, nudgeDecision } from "./codexdrain";
import { listeningBrokerPids } from "./shared/procs";
import { existsSync } from "node:fs";

const BROKER_PORT = parseInt(process.env.CLAUDE_PEERS_PORT ?? "7899", 10);
const BROKER_URL = `http://127.0.0.1:${BROKER_PORT}`;

/**
 * The broker authenticates every route except /health and /register, so the
 * CLI registers a short-lived peer of its own to obtain a token. It is a
 * client like any other rather than a privileged back door.
 */
let cliPeerId: string | null = null;
let cliToken: string | null = null;

async function ensureCliIdentity(): Promise<void> {
  if (cliToken) return;
  const res = await fetch(`${BROKER_URL}/register`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      pid: process.pid,
      cwd: process.cwd(),
      git_root: null,
      tty: null,
      summary: "claude-peers CLI (transient)",
    }),
    signal: AbortSignal.timeout(3000),
  });
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
  const reg = (await res.json()) as { id: string; token: string };
  cliPeerId = reg.id;
  cliToken = reg.token;
}

/** Drop the transient CLI peer so it never shows up in someone's list_peers. */
async function releaseCliIdentity(): Promise<void> {
  if (!cliPeerId || !cliToken) return;
  try {
    await fetch(`${BROKER_URL}/unregister`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: `Bearer ${cliToken}` },
      body: JSON.stringify({ id: cliPeerId }),
      signal: AbortSignal.timeout(3000),
    });
  } catch {
    // The stale-peer sweep collects it anyway once this process exits.
  }
}

async function brokerFetch<T>(path: string, body?: unknown): Promise<T> {
  const opts: RequestInit = body
    ? {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(cliToken ? { Authorization: `Bearer ${cliToken}` } : {}),
        },
        body: JSON.stringify(body),
      }
    : {};
  const res = await fetch(`${BROKER_URL}${path}`, {
    ...opts,
    signal: AbortSignal.timeout(3000),
  });
  if (!res.ok) {
    throw new Error(`${res.status}: ${await res.text()}`);
  }
  return res.json() as Promise<T>;
}

/** Every session pid with a queue on disk. */
function spooledSessionPids(): number[] {
  const dir = spoolDir();
  if (!existsSync(dir)) return [];
  const pids: number[] = [];
  for (const entry of new Bun.Glob("*.jsonl").scanSync(dir)) {
    const pid = Number.parseInt(entry.replace(/\.jsonl$/, ""), 10);
    if (!Number.isNaN(pid)) pids.push(pid);
  }
  return pids;
}

function isAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

const cmd = process.argv[2];

// Register up front rather than lazily: request bodies below embed cliPeerId,
// and a lazy registration inside brokerFetch would leave it null at build time.
// The two codex-* commands run from a hook on every turn and must never register a peer: a
// transient identity per turn would fill everyone's list_peers with ghosts, and a hook has no
// business holding a token that can read a session's messages.
if (
  cmd &&
  !["kill-broker", "inbox", "codex-inbox", "codex-nudge-status", "help", "--help", "-h", undefined].includes(
    cmd
  )
) {
  try {
    await ensureCliIdentity();
  } catch {
    console.log("Broker is not running.");
    process.exit(1);
  }
}

switch (cmd) {
  case "status": {
    try {
      const health = await brokerFetch<{ status: string; peers: number }>("/health");
      console.log(`Broker: ${health.status} (${health.peers} peer(s) registered)`);
      console.log(`URL: ${BROKER_URL}`);

      if (health.peers > 0) {
        const peers = await brokerFetch<
          Array<{
            id: string;
            pid: number;
            cwd: string;
            git_root: string | null;
            tty: string | null;
            summary: string;
            last_seen: string;
          }>
        >("/list-peers", {
          scope: "machine",
          cwd: "/",
          git_root: null,
          exclude_id: cliPeerId,
        });

        console.log("\nPeers:");
        for (const p of peers) {
          console.log(`  ${p.id}  PID:${p.pid}  ${p.cwd}`);
          if (p.summary) console.log(`         ${p.summary}`);
          if (p.tty) console.log(`         TTY: ${p.tty}`);
          console.log(`         Last seen: ${p.last_seen}`);
        }
      }
    } catch {
      console.log("Broker is not running.");
    }
    break;
  }

  case "peers": {
    try {
      const peers = await brokerFetch<
        Array<{
          id: string;
          pid: number;
          cwd: string;
          git_root: string | null;
          tty: string | null;
          summary: string;
          last_seen: string;
        }>
      >("/list-peers", {
        scope: "machine",
        cwd: "/",
        git_root: null,
        exclude_id: cliPeerId,
      });

      if (peers.length === 0) {
        console.log("No peers registered.");
      } else {
        for (const p of peers) {
          const parts = [`${p.id}  PID:${p.pid}  ${p.cwd}`];
          if (p.summary) parts.push(`  Summary: ${p.summary}`);
          console.log(parts.join("\n"));
        }
      }
    } catch {
      console.log("Broker is not running.");
    }
    break;
  }

  case "send": {
    const toId = process.argv[3];
    const msg = process.argv.slice(4).join(" ");
    if (!toId || !msg) {
      console.error("Usage: bun cli.ts send <peer-id> <message>");
      process.exit(1);
    }
    try {
      const result = await brokerFetch<{ ok: boolean; error?: string }>("/send-message", {
        from_id: cliPeerId,
        to_id: toId,
        text: msg,
      });
      if (result.ok) {
        console.log(`Message sent to ${toId}`);
      } else {
        console.error(`Failed: ${result.error}`);
      }
    } catch (e) {
      console.error(`Error: ${e instanceof Error ? e.message : String(e)}`);
    }
    break;
  }

  /**
   * Send one message to every session on the machine.
   *
   * Machine scope only, deliberately: the transient CLI peer registers with cwd of wherever the
   * command was typed and no git root, so a directory or repo scope here would select on this
   * shell's location rather than on anything the operator meant.
   */
  case "broadcast": {
    const msg = process.argv.slice(3).join(" ");
    if (!msg) {
      console.error("Usage: bun cli.ts broadcast <message>");
      process.exit(1);
    }
    try {
      const result = await brokerFetch<{ ok: boolean; delivered_to: number }>("/broadcast-message", {
        from_id: cliPeerId,
        text: msg,
        scope: "machine",
        cwd: process.cwd(),
        git_root: null,
      });
      // Zero is not a failure, so it is reported rather than treated as an error.
      console.log(`Broadcast delivered to ${result.delivered_to} peer(s).`);
    } catch (e) {
      console.error(`Error: ${e instanceof Error ? e.message : String(e)}`);
    }
    break;
  }

  /**
   * Drain whatever was spooled for the session this command is running inside.
   *
   * Exists for the Claude Code hook, which is what makes delivery automatic when the channel is
   * unavailable. Prints nothing and exits 0 when the queue is empty, because it runs on every tool
   * call and must be silent unless there is something to say.
   *
   * Reads a local file and never touches the broker: a hook must not need a peer identity, and
   * registering a transient peer per tool call would fill the peer list with ghosts.
   */
  case "inbox": {
    const explicit = process.argv[3];
    const sessionPid = explicit ? Number.parseInt(explicit, 10) : findSessionPid();
    if (sessionPid === null || Number.isNaN(sessionPid)) {
      // Not inside a Claude Code session, so there is no queue to read. Silent and successful: a
      // hook that printed a diagnostic here would print it on every tool call forever.
      break;
    }

    const messages = drainSpool(sessionPid);
    if (messages.length === 0) break;

    console.log(
      `${messages.length} new peer message(s). Read them, then reply with the send_message tool.`
    );
    for (const message of messages) {
      console.log("");
      console.log(`From peer ${message.from_id} at ${message.sent_at}`);
      if (message.from_summary) console.log(`They are working on: ${message.from_summary}`);
      if (message.from_cwd) console.log(`Their directory: ${message.from_cwd}`);
      console.log("");
      console.log(message.text);
    }
    break;
  }

  /**
   * The Codex counterpart of `inbox`: reads Codex's hook JSON from stdin and answers with the
   * hook output Codex expects on stdout.
   *
   * Separate from `inbox` because the two hosts differ in the part that matters. Claude Code takes
   * plain stdout as context on any event this hook is registered for; Codex takes structured JSON
   * whose shape depends on WHICH event fired, and half its events cannot carry a message at all.
   * Draining on one of those would empty the queue into nothing, so the event has to be read before
   * the spool is touched. `drainForHook` is that ordering, and it is where the tests live.
   *
   * Always exits 0. Codex reads a non-zero exit from a Stop hook as a decision to block.
   */
  case "codex-inbox": {
    const raw = await Bun.stdin.text();
    // The pid override exists for tests; in a real session the walk finds the codex process this
    // hook is a descendant of.
    console.log(drainForHook(raw));
    break;
  }

  /**
   * Reports which sessions are holding mail they have no way to collect.
   *
   * Read-only on purpose. Draining from out here would consume the message without ever showing it
   * to the model, which is strictly worse than leaving it queued, so this decides and prints and
   * never touches the spool.
   *
   * Codex sessions only. A Claude Code session spools too, and its hook fires on every tool call
   * without ever writing an activity stamp, so including it would report every one of them as
   * needing a nudge forever: a command whose output is dominated by false positives is one nobody
   * reads. A queue whose owner is GONE is still reported, whatever host it belonged to, because
   * that one is nobody's normal case.
   */
  case "codex-nudge-status": {
    const idleAfterMs = Number.parseInt(process.env.CLAUDE_PEERS_IDLE_MS ?? "30000", 10);
    const now = Date.now();
    let reported = 0;

    for (const pid of spooledSessionPids()) {
      const alive = isAlive(pid);
      if (alive && !executableMatches(pid, /(^|\/)codex$/)) continue;

      const verdict = nudgeDecision({
        hasMail: hasSpooled(pid),
        lastActivityMs: lastHookActivity(pid),
        nowMs: now,
        alive,
        idleAfterMs,
      });
      if (verdict === "no-mail") continue;
      reported++;
      console.log(`${pid}\t${verdict}`);
    }
    if (reported === 0) console.log("No session is holding undelivered mail.");
    break;
  }

  case "kill-broker": {
    try {
      const health = await brokerFetch<{ status: string; peers: number }>("/health");
      console.log(`Broker has ${health.peers} peer(s). Shutting down...`);
      // Only the process LISTENING on the port, and only if it really is the broker. Signalling
      // every pid holding a socket here used to SIGTERM the connected sessions as well.
      const pids = listeningBrokerPids(BROKER_PORT);
      if (pids.length === 0) {
        console.log(`No broker process is listening on ${BROKER_PORT}. Nothing was stopped.`);
        break;
      }
      for (const pid of pids) {
        process.kill(pid, "SIGTERM");
      }
      console.log("Broker stopped.");
    } catch {
      console.log("Broker is not running.");
    }
    break;
  }

  default:
    console.log(`claude-peers CLI

Usage:
  bun cli.ts status          Show broker status and all peers
  bun cli.ts peers           List all peers
  bun cli.ts send <id> <msg> Send a message to a peer
  bun cli.ts broadcast <msg> Send a message to every peer on the machine
  bun cli.ts kill-broker     Stop the broker daemon

Codex delivery:
  bun cli.ts codex-inbox         Hook entry point: Codex hook JSON in, hook JSON out
  bun cli.ts codex-nudge-status  Codex sessions holding mail they cannot collect yet`);
}

// Never leave the transient CLI peer registered: it would otherwise appear in
// every session's list_peers until the stale sweep notices the process is gone.
await releaseCliIdentity();
