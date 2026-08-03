#!/usr/bin/env bun
/**
 * Send-to-render latency, measured the same way on any checkout.
 *
 * The question this answers is the only one that decides whether the push transport was worth
 * building: how long is it between a peer sending a message and the receiving session actually
 * seeing it. Everything else (requests per minute, frames per hour) is arithmetic on a
 * configured interval, but latency is a distribution and has to be sampled.
 *
 * METHOD
 *
 *   - One broker and one receiving MCP server are started from --root, so the harness can be run
 *     against a checkout of `dev` and against a branch without being modified in between. The
 *     harness is the constant; the code under it is the variable.
 *   - The receiver runs with CLAUDE_PEERS_CHANNEL=always, which is the shape of a real session
 *     launched with --dangerously-load-development-channels: it renders by channel notification.
 *   - The sender is a plain HTTP peer rather than a second MCP server. /send-message is untouched
 *     by this work and identical on both sides, and a tool call would add a stdio round trip to
 *     every sample that has nothing to do with the transport being measured.
 *   - t0 is taken immediately before the POST that inserts the message. t1 is taken when the
 *     receiver's channel notification carrying that exact text is parsed off its stdout. The
 *     difference therefore contains the whole path: insert, notification or poll, the sender
 *     lookup the server does, and the JSON-RPC frame reaching the session.
 *   - Samples are spaced by a RANDOM interval of 1.2 to 2.2 seconds. This matters on the polling
 *     baseline: a fixed gap would alias against the one second cycle and could report any answer
 *     between best and worst case as if it were typical. A random gap lands uniformly in the
 *     cycle, which is what makes the median meaningful.
 *
 * Usage:
 *   bun bench/send-to-render.ts --root . --port 7840 --n 30 --label after
 *   bun bench/send-to-render.ts --root ../dev-checkout --port 7841 --n 30 --label before
 */

import { mkdtempSync, realpathSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

function arg(name: string, fallback: string): string {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1]! : fallback;
}

const ROOT = realpathSync(arg("root", process.cwd()));
const PORT = Number.parseInt(arg("port", "7840"), 10);
const SAMPLES = Number.parseInt(arg("n", "30"), 10);
const LABEL = arg("label", "run");
const BASE = `http://127.0.0.1:${PORT}`;

const WORK = realpathSync(mkdtempSync(join(realpathSync(tmpdir()), "peers-bench-")));
const spawned: { kill: (s?: any) => void }[] = [];

function cleanup(): void {
  for (const proc of spawned) {
    try {
      proc.kill("SIGKILL");
    } catch {
      // already gone
    }
  }
  rmSync(WORK, { recursive: true, force: true });
}
process.once("exit", cleanup);
for (const sig of ["SIGINT", "SIGTERM"] as const) {
  process.once(sig, () => {
    cleanup();
    process.exit(1);
  });
}

const ENV: Record<string, string> = {
  ...(process.env as Record<string, string>),
  CLAUDE_PEERS_PORT: String(PORT),
  CLAUDE_PEERS_DB: join(WORK, "broker.db"),
  // Never the real one: a spool under $HOME would put benchmark traffic into a live session.
  CLAUDE_PEERS_SPOOL_DIR: join(WORK, "spool"),
};

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

function track<T extends { kill: (s?: any) => void }>(proc: T): T {
  spawned.push(proc);
  return proc;
}

async function startBroker(): Promise<void> {
  track(
    Bun.spawn(["bun", join(ROOT, "broker.ts")], { env: ENV, stdout: "ignore", stderr: "ignore" })
  );
  for (let i = 0; i < 60; i++) {
    await Bun.sleep(100);
    try {
      if ((await fetch(`${BASE}/health`)).ok) return;
    } catch {
      // still booting
    }
  }
  throw new Error(`broker did not come up on ${PORT}`);
}

/** Timestamps at which each rendered text was seen, keyed by the text itself. */
const renderedAt = new Map<string, number>();

async function startReceiver(cwd: string): Promise<void> {
  const session = track(Bun.spawn(["sleep", "900"], { stdout: "ignore", stderr: "ignore" }));
  const proc = track(
    Bun.spawn(["bun", join(ROOT, "server.ts")], {
      cwd,
      env: { ...ENV, CLAUDE_PEERS_SESSION_PID: String(session.pid), CLAUDE_PEERS_CHANNEL: "always" },
      stdin: "pipe",
      stdout: "pipe",
      stderr: "ignore",
    })
  );

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
          if (msg.method && String(msg.method).includes("channel")) {
            const text = String(msg.params?.content ?? "");
            if (!renderedAt.has(text)) renderedAt.set(text, performance.now());
          }
        } catch {
          // not a JSON-RPC frame
        }
      }
    }
  })();

  proc.stdin.write(
    JSON.stringify({
      jsonrpc: "2.0",
      id: 1,
      method: "initialize",
      params: {
        protocolVersion: "2024-11-05",
        capabilities: {},
        clientInfo: { name: "bench", version: "1" },
      },
    }) + "\n"
  );
  await proc.stdin.flush();
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return NaN;
  // Nearest rank. With thirty samples an interpolated p95 would be inventing precision.
  const rank = Math.ceil((p / 100) * sorted.length);
  return sorted[Math.min(sorted.length - 1, Math.max(0, rank - 1))]!;
}

async function main(): Promise<void> {
  await startBroker();

  const receiverCwd = realpathSync(mkdtempSync(join(WORK, "receiver-")));
  await startReceiver(receiverCwd);
  // Registration, the auto-summary window the server allows itself, and the subscription settling.
  await Bun.sleep(6000);

  const senderPid = track(Bun.spawn(["sleep", "900"], { stdout: "ignore", stderr: "ignore" })).pid;
  const sender = await post<{ id: string; token: string }>("/register", {
    pid: senderPid,
    cwd: join(WORK, "sender"),
    git_root: null,
    tty: null,
    summary: "",
  });

  const peers = await post<{ id: string; cwd: string }[]>(
    "/list-peers",
    { scope: "machine", cwd: join(WORK, "sender"), git_root: null, exclude_id: sender.id },
    sender.token
  );
  const target = peers.find((p) => p.cwd === receiverCwd);
  if (!target) throw new Error(`receiver never registered; saw ${JSON.stringify(peers)}`);

  const latencies: number[] = [];
  let lost = 0;

  for (let i = 0; i < SAMPLES; i++) {
    const text = `bench-${LABEL}-${i}-${Math.random().toString(36).slice(2, 8)}`;
    const t0 = performance.now();
    await post("/send-message", { from_id: sender.id, to_id: target.id, text }, sender.token);

    const deadline = Date.now() + 15_000;
    while (!renderedAt.has(text) && Date.now() < deadline) await Bun.sleep(1);
    const seen = renderedAt.get(text);
    if (seen === undefined) lost++;
    else latencies.push(seen - t0);

    // Random, so the samples land uniformly across a poll cycle instead of aliasing against it.
    await Bun.sleep(1200 + Math.floor(Math.random() * 1000));
  }

  const sorted = [...latencies].sort((a, b) => a - b);
  const mean = sorted.reduce((a, b) => a + b, 0) / (sorted.length || 1);
  const round = (n: number) => Math.round(n * 10) / 10;

  console.log(
    JSON.stringify(
      {
        label: LABEL,
        root: ROOT,
        samples: latencies.length,
        lost,
        min_ms: round(sorted[0] ?? NaN),
        median_ms: round(percentile(sorted, 50)),
        p95_ms: round(percentile(sorted, 95)),
        max_ms: round(sorted[sorted.length - 1] ?? NaN),
        mean_ms: round(mean),
      },
      null,
      2
    )
  );
}

main()
  .then(() => {
    cleanup();
    process.exit(0);
  })
  .catch((e) => {
    console.error(`bench failed: ${e instanceof Error ? e.stack : String(e)}`);
    cleanup();
    process.exit(1);
  });
