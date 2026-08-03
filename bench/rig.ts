/**
 * The apparatus the benchmark measures through: brokers, MCP clients, and a request counter.
 *
 * Everything here is the real thing. The broker is `broker.ts`, the clients are `server.ts` driven
 * over stdio exactly as Claude Code drives them, and the notification the receiver emits is the
 * same channel notification a real session renders. Nothing is stubbed, because a benchmark of a
 * stub measures the stub.
 *
 * ISOLATION IS THE FIRST REQUIREMENT, NOT A NICETY. Live Claude Code sessions run on this machine
 * against a broker on port 7899, and `findSessionPid` walks the process tree, so a client spawned
 * without its own session pid resolves the DEVELOPER'S session and spools benchmark traffic into a
 * queue that a hook is actively draining into someone's context. Every client this module creates
 * therefore gets:
 *   - its own CLAUDE_PEERS_SESSION_PID, pointing at a placeholder process of its own,
 *   - its own CLAUDE_PEERS_SPOOL_DIR,
 *   - its own working directory,
 * and every port is drawn from a configured range that is asserted not to contain 7899.
 */

import { join } from "node:path";
import { mkdirSync } from "node:fs";
import { rmSync } from "node:fs";
import {
  isPortFree,
  releasePort,
  reserveFreePort,
  sweepBrokerOnPort,
  trackProcess,
  trackedTempDir,
} from "../testsupport";

/** The port the live estate uses. Never bound, never probed for killing, never in a range. */
export const PRODUCTION_BROKER_PORT = 7899;

const REPO_ROOT = join(import.meta.dir, "..");

export interface PortRange {
  min: number;
  max: number;
}

/**
 * Refuse a range that contains the live broker's port.
 *
 * Checked here rather than trusted from the caller: a benchmark that binds 7899 does not produce a
 * bad number, it takes down every running session on the machine.
 */
export function assertSafeRange(range: PortRange): void {
  if (range.min > range.max) {
    throw new Error(`port range ${range.min}-${range.max} is inverted`);
  }
  if (PRODUCTION_BROKER_PORT >= range.min && PRODUCTION_BROKER_PORT <= range.max) {
    throw new Error(
      `port range ${range.min}-${range.max} contains ${PRODUCTION_BROKER_PORT}, which serves live ` +
        `Claude Code sessions. Choose a range that excludes it.`
    );
  }
}

export interface ObservedNotification {
  method: string;
  params: Record<string, unknown>;
  /** Harness clock, taken when the frame came off the client's stdout. */
  observedAtMs: number;
}

interface Waiter {
  matches: (notification: ObservedNotification) => boolean;
  resolve: (notification: ObservedNotification) => void;
}

export interface BenchClient {
  readonly name: string;
  readonly cwd: string;
  readonly pid: number;
  /** Harness clock at spawn, so startup cost can be reported and then excluded. */
  readonly spawnedAtMs: number;
  /** Milliseconds from spawn to the client appearing in the broker's peer list. Never timed work. */
  registrationMs: number | null;
  peerId: string | null;
  readonly notifications: ObservedNotification[];
  alive(): boolean;
  initialize(): Promise<unknown>;
  listTools(): Promise<ToolDescriptor[]>;
  tool(name: string, args?: Record<string, unknown>): Promise<string>;
  /**
   * Write a tool call frame and return the harness clock reading taken immediately before the
   * write. This is the "instant the tool call is issued" that every latency here is measured from.
   */
  issueTool(name: string, args: Record<string, unknown>): Promise<number>;
  awaitNotification(
    matches: (notification: ObservedNotification) => boolean,
    timeoutMs: number
  ): Promise<ObservedNotification>;
  kill(): void;
}

export interface ToolDescriptor {
  name: string;
  description?: string;
  inputSchema?: {
    type?: string;
    properties?: Record<string, unknown>;
    required?: string[];
  };
}

/** Counts, per path, of the requests that reached the broker through the counting proxy. */
export interface RequestTally {
  total: number;
  byPath: Record<string, number>;
  /**
   * Requests that stayed open longer than `streamThresholdMs`. A server-sent-events transport is
   * one long-lived request rather than sixty short ones, and a raw request count would flatter it
   * without saying why.
   */
  streams: number;
}

export interface CountingProxy {
  port: number;
  tally(): RequestTally;
  reset(): void;
  stop(): void;
}

/**
 * One benchmark run's worth of operating-system state, and the means to hand all of it back.
 *
 * Deliberately owns its own lists rather than calling testsupport's `cleanupAll`: when the harness
 * runs inside `bun test`, that function would kill processes belonging to other suites in the same
 * process. Everything is ALSO registered with testsupport, so the process-exit handler there is the
 * backstop if this teardown never runs.
 */
export class BenchRig {
  private readonly procs: { pid: number; kill: (signal?: number | NodeJS.Signals) => void }[] = [];
  private readonly dirs: string[] = [];
  private readonly ports: number[] = [];
  private readonly proxies: CountingProxy[] = [];
  private readonly clients: BenchClient[] = [];

  constructor(private readonly range: PortRange) {
    assertSafeRange(range);
  }

  port(): number {
    const port = reserveFreePort(this.range.min, this.range.max);
    if (port === PRODUCTION_BROKER_PORT) {
      throw new Error("refusing to use the live broker port");
    }
    this.ports.push(port);
    return port;
  }

  /**
   * A port in the range that nothing is listening on and that this run never binds.
   *
   * Used to point the Anthropic SDK at a closed socket so the startup auto-summary fails on connect
   * instead of making a real network call of unbounded duration.
   */
  closedPort(): number {
    const port = this.port();
    if (!isPortFree(port)) {
      throw new Error(`port ${port} was handed out but is not free`);
    }
    return port;
  }

  dir(prefix: string): string {
    const dir = trackedTempDir(prefix);
    this.dirs.push(dir);
    return dir;
  }

  /**
   * Start a broker on its own port with its own database file.
   *
   * Started here rather than left to a client's `ensureBroker`: that spawns a detached grandchild,
   * which does not settle reliably under a harness, and every client then blocks on a broker that
   * never answers.
   */
  async startBroker(env: Record<string, string>): Promise<number> {
    const port = this.port();
    const proc = trackProcess(
      Bun.spawn(["bun", join(REPO_ROOT, "broker.ts")], {
        env: { ...env, CLAUDE_PEERS_PORT: String(port) },
        stdout: "ignore",
        stderr: "ignore",
      })
    );
    this.procs.push(proc);
    for (let attempt = 0; attempt < 60; attempt++) {
      await Bun.sleep(100);
      try {
        if ((await fetch(`http://127.0.0.1:${port}/health`)).ok) return port;
      } catch {
        // still booting
      }
    }
    throw new Error(`broker did not come up on ${port} within 6 seconds`);
  }

  /**
   * A reverse proxy in front of the broker that counts what passes through it.
   *
   * The broker keeps no request counters, and adding some would change the code under measurement.
   * Counting outside it measures whatever transport a branch happens to use, including one that
   * does not exist yet, which is the only way this harness runs unchanged on every branch.
   *
   * Bodies are buffered (they are small JSON objects) but RESPONSES are returned as they stream, so
   * a server-sent-events response is proxied rather than swallowed.
   */
  startCountingProxy(brokerPort: number, streamThresholdMs = 5000): CountingProxy {
    const port = this.port();
    let byPath: Record<string, number> = {};
    let total = 0;
    let streams = 0;

    const server = Bun.serve({
      port,
      hostname: "127.0.0.1",
      idleTimeout: 0,
      async fetch(req) {
        const url = new URL(req.url);
        const path = url.pathname;
        total += 1;
        byPath[path] = (byPath[path] ?? 0) + 1;
        const openedAt = performance.now();

        const headers = new Headers();
        req.headers.forEach((value, key) => {
          if (key === "host" || key === "connection" || key === "content-length") return;
          headers.set(key, value);
        });

        const body = req.method === "GET" || req.method === "HEAD" ? undefined : await req.text();
        try {
          const upstream = await fetch(`http://127.0.0.1:${brokerPort}${path}${url.search}`, {
            method: req.method,
            headers,
            body,
            redirect: "manual",
          });
          const elapsed = performance.now() - openedAt;
          if (elapsed >= streamThresholdMs) streams += 1;
          return upstream;
        } catch (e) {
          return Response.json(
            { error: `proxy could not reach broker: ${e instanceof Error ? e.message : String(e)}` },
            { status: 502 }
          );
        }
      },
    });

    const proxy: CountingProxy = {
      port,
      tally: () => ({ total, byPath: { ...byPath }, streams }),
      reset: () => {
        total = 0;
        byPath = {};
        streams = 0;
      },
      stop: () => server.stop(true),
    };
    this.proxies.push(proxy);
    return proxy;
  }

  /**
   * Spawn one MCP server and drive it over stdio, the way Claude Code does.
   *
   * `channel` is forced to "always" by default because the measured event is the channel
   * notification. Detection reads the parent process's argv for the development-channels flag,
   * which a harness parent does not carry, so without the override every client would take the
   * spool path and there would be no notification to time.
   */
  spawnClient(options: {
    name: string;
    brokerPort: number;
    workRoot: string;
    env: Record<string, string>;
    channel?: "always" | "never";
  }): BenchClient {
    const cwd = join(options.workRoot, options.name);
    mkdirSync(cwd, { recursive: true });
    const spool = join(options.workRoot, `${options.name}-spool`);
    mkdirSync(spool, { recursive: true, mode: 0o700 });

    // A placeholder process of this client's own, so its session identity is never a real session.
    const session = trackProcess(
      Bun.spawn(["sleep", "900"], { stdout: "ignore", stderr: "ignore" })
    );
    this.procs.push(session);

    const spawnedAt = performance.now();
    const proc = trackProcess(
      Bun.spawn(["bun", join(REPO_ROOT, "server.ts")], {
        cwd,
        env: {
          ...options.env,
          CLAUDE_PEERS_PORT: String(options.brokerPort),
          CLAUDE_PEERS_SPOOL_DIR: spool,
          CLAUDE_PEERS_SESSION_PID: String(session.pid),
          CLAUDE_PEERS_CHANNEL: options.channel ?? "always",
        },
        stdin: "pipe",
        stdout: "pipe",
        stderr: "ignore",
      })
    );
    this.procs.push(proc);

    const replies = new Map<number, { result?: unknown; error?: unknown }>();
    const notifications: ObservedNotification[] = [];
    const waiters: Waiter[] = [];
    let nextId = 1;

    // One persistent reader for the life of the client. Re-entering `for await` on stdout cancels
    // the stream, which silently truncates every later frame, and a truncated frame in a latency
    // harness reads as "the message never arrived".
    void (async () => {
      const decoder = new TextDecoder();
      let buffered = "";
      try {
        for await (const chunk of proc.stdout as ReadableStream<Uint8Array>) {
          // Taken before parsing, so JSON work is not counted as transport time.
          const observedAtMs = performance.now();
          buffered += decoder.decode(chunk, { stream: true });
          let newline = buffered.indexOf("\n");
          while (newline >= 0) {
            const line = buffered.slice(0, newline);
            buffered = buffered.slice(newline + 1);
            newline = buffered.indexOf("\n");
            if (!line.trim()) continue;
            let frame: Record<string, unknown>;
            try {
              frame = JSON.parse(line) as Record<string, unknown>;
            } catch {
              continue; // not a JSON-RPC frame
            }
            if (frame.id != null) {
              replies.set(Number(frame.id), frame as { result?: unknown; error?: unknown });
              continue;
            }
            if (typeof frame.method !== "string") continue;
            const observed: ObservedNotification = {
              method: frame.method,
              params: (frame.params ?? {}) as Record<string, unknown>,
              observedAtMs,
            };
            notifications.push(observed);
            for (let i = waiters.length - 1; i >= 0; i--) {
              const waiter = waiters[i] as Waiter;
              if (!waiter.matches(observed)) continue;
              waiters.splice(i, 1);
              waiter.resolve(observed);
            }
          }
        }
      } catch {
        // The client was killed. Nothing left to read, and teardown is already under way.
      }
    })();

    async function call(method: string, params: unknown, timeoutMs = 30_000): Promise<unknown> {
      const id = nextId++;
      proc.stdin.write(JSON.stringify({ jsonrpc: "2.0", id, method, params }) + "\n");
      await proc.stdin.flush();
      const deadline = Date.now() + timeoutMs;
      while (Date.now() < deadline) {
        const reply = replies.get(id);
        if (reply) {
          replies.delete(id);
          if (reply.error) throw new Error(`${method} failed: ${JSON.stringify(reply.error)}`);
          return reply.result;
        }
        await Bun.sleep(5);
      }
      throw new Error(`timed out after ${timeoutMs} ms waiting for a reply to ${method}`);
    }

    const client: BenchClient = {
      name: options.name,
      cwd,
      pid: proc.pid,
      spawnedAtMs: spawnedAt,
      registrationMs: null,
      peerId: null,
      notifications,
      alive: () => proc.killed === false && proc.exitCode === null,
      initialize: () =>
        call("initialize", {
          protocolVersion: "2024-11-05",
          capabilities: {},
          clientInfo: { name: "claude-peers-bench", version: "1" },
        }),
      async listTools() {
        const result = (await call("tools/list", {})) as { tools?: ToolDescriptor[] };
        return result.tools ?? [];
      },
      async tool(name, args = {}) {
        const result = (await call("tools/call", { name, arguments: args })) as {
          content?: { text?: string }[];
        };
        return String(result.content?.[0]?.text ?? "");
      },
      async issueTool(name, args) {
        const id = nextId++;
        const frame =
          JSON.stringify({
            jsonrpc: "2.0",
            id,
            method: "tools/call",
            params: { name, arguments: args },
          }) + "\n";
        // The clock reading and the write are adjacent on purpose: everything between them would
        // be counted as delivery latency that the system under test never spent.
        const issuedAtMs = performance.now();
        proc.stdin.write(frame);
        void proc.stdin.flush();
        return issuedAtMs;
      },
      awaitNotification(matches, timeoutMs) {
        const already = notifications.find(matches);
        if (already) return Promise.resolve(already);
        return new Promise<ObservedNotification>((resolve, reject) => {
          const waiter: Waiter = { matches, resolve };
          waiters.push(waiter);
          setTimeout(() => {
            const index = waiters.indexOf(waiter);
            if (index >= 0) waiters.splice(index, 1);
            reject(new Error(`no matching notification within ${timeoutMs} ms`));
          }, timeoutMs).unref?.();
        });
      },
      kill() {
        try {
          proc.kill("SIGKILL");
        } catch {
          // already gone
        }
        try {
          session.kill("SIGKILL");
        } catch {
          // already gone
        }
      },
    };

    this.clients.push(client);
    return client;
  }

  /**
   * Wait until the broker can see every client, and record how long each took to get there.
   *
   * Asked of /health rather than of a client, so confirming registration adds no requests to the
   * count the idle phase is taking, and so a client with no peers to list (the idle phase has
   * exactly one) can still be confirmed. Registration, and the startup auto-summary that precedes
   * it, are excluded from every latency this harness reports: this is where that exclusion is
   * enforced, because nothing is timed until this returns.
   */
  async awaitRegistrations(
    brokerPort: number,
    clients: BenchClient[],
    timeoutMs = 30_000
  ): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      try {
        const res = await fetch(`http://127.0.0.1:${brokerPort}/health`);
        if (res.ok) {
          const body = (await res.json()) as { peers?: number };
          if (body.peers === clients.length) {
            const now = performance.now();
            for (const client of clients) {
              if (client.registrationMs === null) client.registrationMs = now - client.spawnedAtMs;
            }
            return;
          }
        }
      } catch {
        // broker still settling
      }
      await Bun.sleep(100);
    }
    throw new Error(
      `only some of ${clients.length} clients registered with the broker on ${brokerPort} ` +
        `within ${timeoutMs} ms`
    );
  }

  /**
   * Resolve each target's peer id from the observer's peer listing.
   *
   * Matched on working directory, because clients register in whatever order they finish starting.
   */
  async resolvePeers(
    brokerPort: number,
    observer: BenchClient,
    targets: BenchClient[],
    timeoutMs = 30_000
  ): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const listing = await observer.tool("list_peers", { scope: "machine" });
      let missing = false;
      for (const target of targets) {
        if (target.peerId) continue;
        const id = peerIdAt(listing, target.cwd);
        if (id) {
          target.peerId = id;
        } else {
          missing = true;
        }
      }
      if (!missing) return;
      await Bun.sleep(250);
    }
    const unresolved = targets.filter((t) => !t.peerId).map((t) => t.name);
    throw new Error(
      `these clients never registered with the broker on ${brokerPort}: ${unresolved.join(", ")}`
    );
  }

  /**
   * Every port this rig has taken so far, so a report can name the ports its numbers came from.
   *
   * Worth recording because a benchmark that silently talked to somebody else's broker reports a
   * latency for the wrong process, and nothing in the number itself says so. Two suites in this
   * repo bind ports directly rather than through testsupport's reservation, which is exactly how
   * that happens.
   */
  usedPorts(): number[] {
    return [...this.ports];
  }

  /** Stop everything this rig created, and only what this rig created. */
  teardown(): void {
    for (const client of this.clients) client.kill();
    this.clients.length = 0;
    for (const proxy of this.proxies) {
      try {
        proxy.stop();
      } catch {
        // already stopped
      }
    }
    this.proxies.length = 0;
    for (const proc of this.procs) {
      try {
        proc.kill("SIGKILL");
      } catch {
        // already gone
      }
    }
    this.procs.length = 0;
    for (const port of this.ports) {
      // Only pids that are LISTENING on the port and are running broker.ts: never an lsof sweep,
      // which would signal connected clients, and on a shared machine that means other people's.
      sweepBrokerOnPort(port);
      releasePort(port);
    }
    this.ports.length = 0;
    for (const dir of this.dirs) rmSync(dir, { recursive: true, force: true });
    this.dirs.length = 0;
  }
}

/**
 * The peer id of the entry whose working directory matches, or null.
 *
 * Matched on cwd rather than position: clients register in whatever order they finish starting, so
 * "the first id in the listing" is a different client from run to run.
 */
export function peerIdAt(listing: string, cwd: string): string | null {
  for (const block of listing.split(/\n(?=ID: )/)) {
    if (!block.includes(`CWD: ${cwd}`)) continue;
    const id = block.match(/ID: ([a-z0-9]{8})/)?.[1];
    if (id) return id;
  }
  return null;
}

/** True when the frame is the channel push this harness times. */
export function isChannelNotification(notification: ObservedNotification): boolean {
  return notification.method.includes("channel");
}

/** The message text carried by a channel notification, for matching a marker against. */
export function notificationText(notification: ObservedNotification): string {
  const content = notification.params.content;
  return typeof content === "string" ? content : JSON.stringify(notification.params);
}
