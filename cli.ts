#!/usr/bin/env bun
/**
 * claude-peers CLI
 *
 * Utility commands for managing the broker and inspecting peers.
 *
 * Usage:
 *   bun cli.ts status          — Show broker status and all peers
 *   bun cli.ts peers           — List all peers
 *   bun cli.ts send <id> <msg> — Send a message to a peer
 *   bun cli.ts kill-broker     — Stop the broker daemon
 */

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

const cmd = process.argv[2];

// Register up front rather than lazily: request bodies below embed cliPeerId,
// and a lazy registration inside brokerFetch would leave it null at build time.
if (cmd && !["kill-broker", "help", "--help", "-h", undefined].includes(cmd)) {
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

  case "kill-broker": {
    try {
      const health = await brokerFetch<{ status: string; peers: number }>("/health");
      console.log(`Broker has ${health.peers} peer(s). Shutting down...`);
      // Find and kill the broker process on the port
      const proc = Bun.spawnSync(["lsof", "-ti", `:${BROKER_PORT}`]);
      const pids = new TextDecoder()
        .decode(proc.stdout)
        .trim()
        .split("\n")
        .filter((p) => p);
      for (const pid of pids) {
        process.kill(parseInt(pid), "SIGTERM");
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
  bun cli.ts kill-broker     Stop the broker daemon`);
}

// Never leave the transient CLI peer registered: it would otherwise appear in
// every session's list_peers until the stale sweep notices the process is gone.
await releaseCliIdentity();
