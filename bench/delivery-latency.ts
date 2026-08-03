#!/usr/bin/env bun
/**
 * Message-delivery benchmark for claude-peers.
 *
 * WHAT THIS EXISTS FOR. Two transports are being built in parallel: a one-to-many send, and a
 * server-sent-events push replacing the broker-to-server poll. The design says a transport "wins on
 * the numbers or it does not land", so the numbers have to come from one harness that runs
 * unchanged on `dev`, on either feature branch, and on the combination. Anything that had to be
 * edited per branch would be comparing two harnesses, not two transports.
 *
 * WHAT IS TIMED. Send-to-render, end to end: from the harness clock reading taken immediately
 * before the sender's `send_message` tool-call frame is written to its MCP server's stdin, to the
 * harness clock reading taken when the receiving MCP server's channel notification frame comes off
 * its stdout. Both readings are taken by one process from one monotonic clock, so no clock skew
 * enters. Every hop the design cares about is inside that interval: the sender's tool handler, the
 * HTTP POST to the broker, the SQLite insert, whatever carries the message to the receiving server,
 * and that server's push.
 *
 * WHAT IS EXCLUDED, AND WHY.
 *   - Process startup and broker startup. Both happen before any measurement, and the harness
 *     refuses to time anything until every client is visible in the broker's peer list.
 *   - The startup auto-summary, which makes a real call to the Anthropic API and would otherwise
 *     add seconds of network time to the first sample. The Anthropic SDK is pointed at a closed
 *     port on the loopback interface, so it fails on connect in microseconds instead. The
 *     per-client registration time is reported, which is the evidence that it did.
 *   - Warm-up messages. The first delivery in a fresh process pays one-off costs (module loading in
 *     the tool handler path, the first HTTP connection to the broker) that no later message pays.
 *   - The last hop, from the MCP server to the Claude session's rendered context. No harness can see
 *     it, so "render" here means the notification leaving the server, which is the last event any
 *     observer outside Claude Code can honestly timestamp.
 *
 * WHY THE GAPS BETWEEN SAMPLES ARE RANDOM. The current transport is a 1 second poll, so latency is
 * determined by where in the poll cycle the message lands. Send at a FIXED delay after the previous
 * delivery and every sample lands at the same phase of the cycle, and the harness reports a
 * constant with a straight face. The gap is therefore drawn uniformly across a whole poll interval,
 * from a seeded generator so the run is still reproducible.
 *
 * WHAT COULD STILL MISLEAD. See bench/README.md. The short version: this measures a machine with
 * two to four idle clients on it, not a machine with a developer's real workload on it.
 */

import { join } from "node:path";
import { writeFileSync } from "node:fs";
import {
  BenchRig,
  isChannelNotification,
  notificationText,
  type BenchClient,
  type PortRange,
  type RequestTally,
  type ToolDescriptor,
} from "./rig.ts";
import { round, seededRandom, summarize, type LatencyStats } from "./stats.ts";

// --- Configuration ---

export interface BenchmarkConfig {
  /** Unicast latency samples. Thirty is the floor: a handful cannot characterise a uniform spread. */
  samples: number;
  /** Messages sent back to back in one burst. */
  burst: number;
  /** How many bursts, each preceded by a randomised gap so the rounds span the poll cycle. */
  burstRounds: number;
  /** Broadcast rounds, each one message to every receiver. */
  broadcastSamples: number;
  /** Receivers in the broadcast phase. */
  broadcastReceivers: number;
  /** How long the idle phase counts broker requests for. */
  idleWindowMs: number;
  /** Only used to size the randomised gap between samples, never assumed by the measurement. */
  pollIntervalMs: number;
  /** How long a single message may take before it counts as never delivered. */
  deliveryTimeoutMs: number;
  seed: number;
  portRange: PortRange;
  skip: { unicast: boolean; burst: boolean; broadcast: boolean; idle: boolean };
  log: (line: string) => void;
}

export const DEFAULT_CONFIG: BenchmarkConfig = {
  samples: 30,
  burst: 10,
  burstRounds: 5,
  broadcastSamples: 30,
  broadcastReceivers: 3,
  idleWindowMs: 60_000,
  pollIntervalMs: 1000,
  deliveryTimeoutMs: 20_000,
  seed: 20260803,
  portRange: { min: 7770, max: 7789 },
  skip: { unicast: false, burst: false, broadcast: false, idle: false },
  log: (line) => console.error(line),
};

// --- Report shape ---

export interface DeliveryPhaseResult {
  phase: string;
  description: string;
  attempted: number;
  delivered: number;
  missed: string[];
  stats: LatencyStats | null;
  samplesMs: number[];
  extra?: Record<string, unknown>;
}

export interface SkippedPhase {
  phase: string;
  skipped: true;
  reason: string;
}

export interface IdlePhaseResult {
  phase: "idle";
  description: string;
  windowMs: number;
  sessions: number;
  requestsPerSessionPerMinute: number;
  totalRequests: number;
  byPath: Record<string, number>;
  longLivedRequests: number;
  requestsBeforeWindow: number;
  brokerPort: number;
  proxyPort: number;
  ports: number[];
}

export interface BenchmarkReport {
  tool: string;
  schemaVersion: number;
  startedAt: string;
  durationMs: number;
  environment: Record<string, string | boolean | null>;
  config: Record<string, unknown>;
  /** Which ports each phase actually bound, so a number can be traced to the process that made it. */
  portsUsed: Record<string, number[]>;
  features: { tools: string[]; broadcastMessage: boolean };
  method: string[];
  phases: {
    unicast: DeliveryPhaseResult | SkippedPhase | null;
    burst: DeliveryPhaseResult | SkippedPhase | null;
    broadcast: DeliveryPhaseResult | SkippedPhase | null;
    idle: IdlePhaseResult | SkippedPhase | null;
  };
  valid: boolean;
  problems: string[];
}

function isSkipped(phase: unknown): phase is SkippedPhase {
  return typeof phase === "object" && phase !== null && "skipped" in phase;
}

// --- Environment capture ---

function git(args: string[]): string {
  try {
    const proc = Bun.spawnSync(["git", ...args], { cwd: join(import.meta.dir, "..") });
    return new TextDecoder().decode(proc.stdout).trim();
  } catch {
    return "";
  }
}

function captureEnvironment(): Record<string, string | boolean | null> {
  return {
    branch: git(["rev-parse", "--abbrev-ref", "HEAD"]) || null,
    commit: git(["rev-parse", "--short", "HEAD"]) || null,
    workingTreeDirty: git(["status", "--porcelain"]).length > 0,
    bun: Bun.version,
    platform: `${process.platform}-${process.arch}`,
  };
}

// --- The method, printed with every run ---

function methodLines(config: BenchmarkConfig): string[] {
  return [
    "Timed: from the clock reading taken immediately before the sender's send_message frame is",
    "  written to its MCP server's stdin, to the reading taken when the receiving MCP server's",
    "  channel notification frame comes off its stdout. One process, one monotonic clock.",
    `Samples: unicast ${config.samples}; burst ${config.burstRounds} rounds of ${config.burst} ` +
      `back to back; broadcast ${config.broadcastSamples} rounds to ${config.broadcastReceivers} ` +
      `receivers, if that tool exists on this branch.`,
    `Gaps between unicast samples are uniform over 0 to ${config.pollIntervalMs} ms from seed ` +
      `${config.seed}, so samples land at random points in a poll cycle rather than one fixed point.`,
    "Excluded: process and broker startup, peer registration, the startup auto-summary (the",
    "  Anthropic SDK is pointed at a closed loopback port so it fails on connect), and two warm-up",
    "  messages per phase.",
    "Not observable: the final hop from MCP server into the Claude session's rendered context.",
    "Percentiles are nearest rank, never interpolated: every figure is a delivery that happened.",
  ];
}

// --- Shared phase plumbing ---

interface PhaseEnvironment {
  base: Record<string, string>;
  closedPort: number;
}

function markerFor(phase: string, nonce: string, round1: number, receiver = 0): string {
  return `bench:${phase}:${nonce}:${round1}:${receiver}`;
}

function waitFor(client: BenchClient, marker: string, timeoutMs: number) {
  return client.awaitNotification(
    (n) => isChannelNotification(n) && notificationText(n).includes(marker),
    timeoutMs
  );
}

/**
 * Two messages that are sent, awaited and thrown away.
 *
 * Also the proof that the path works at all: the sender's own tool reply is inspected here, which
 * the timed sends deliberately do not do (waiting for the reply before waiting for the notification
 * would serialise the burst). A phase whose warm-up fails stops immediately rather than producing
 * thirty timeouts and a table of nothing.
 */
async function warmUp(
  sender: BenchClient,
  receivers: BenchClient[],
  send: (message: string) => Promise<string>,
  phase: string,
  nonce: string,
  timeoutMs: number
): Promise<string> {
  let lastReply = "";
  for (let i = 0; i < 2; i++) {
    const marker = markerFor(`${phase}-warmup`, nonce, i);
    const waits = receivers.map((r) => waitFor(r, marker, timeoutMs));
    lastReply = await send(`${marker} warm-up, not measured`);
    if (/error|not found|failed/i.test(lastReply)) {
      throw new Error(`warm-up send was refused: ${lastReply}`);
    }
    await Promise.all(waits);
  }
  return lastReply;
}

function phaseResult(
  phase: string,
  description: string,
  attempted: number,
  samplesMs: number[],
  missed: string[],
  extra?: Record<string, unknown>
): DeliveryPhaseResult {
  // Rounded to microsecond-free hundredths of a millisecond. The raw clock carries far more digits
  // than the measurement is worth, and printing them implies a precision that a stdio pipe and a
  // 1 second poll do not have.
  const stats = samplesMs.length > 0 ? summarize(samplesMs) : null;
  return {
    phase,
    description,
    attempted,
    delivered: samplesMs.length,
    missed,
    stats: stats
      ? {
          count: stats.count,
          min: round(stats.min, 2),
          median: round(stats.median, 2),
          p95: round(stats.p95, 2),
          max: round(stats.max, 2),
          mean: round(stats.mean, 2),
        }
      : null,
    samplesMs: samplesMs.map((value) => round(value, 2)),
    ...(extra ? { extra } : {}),
  };
}

async function startPhase(
  config: BenchmarkConfig,
  env: PhaseEnvironment,
  prefix: string,
  receivers: number,
  useProxy: boolean
): Promise<{
  rig: BenchRig;
  brokerPort: number;
  clientPort: number;
  work: string;
  proxy: ReturnType<BenchRig["startCountingProxy"]> | null;
  sender: BenchClient;
  receiverClients: BenchClient[];
  ports: number[];
}> {
  const rig = new BenchRig(config.portRange);
  const work = rig.dir(prefix);
  const brokerEnv = { ...env.base, CLAUDE_PEERS_DB: join(work, "broker.db") };
  const brokerPort = await rig.startBroker(brokerEnv);
  const proxy = useProxy ? rig.startCountingProxy(brokerPort) : null;
  const clientPort = proxy ? proxy.port : brokerPort;

  const sender = rig.spawnClient({
    name: "sender",
    brokerPort: clientPort,
    workRoot: work,
    env: brokerEnv,
  });
  const receiverClients: BenchClient[] = [];
  for (let i = 0; i < receivers; i++) {
    receiverClients.push(
      rig.spawnClient({
        name: `receiver-${i}`,
        brokerPort: clientPort,
        workRoot: work,
        env: brokerEnv,
      })
    );
  }

  await sender.initialize();
  await Promise.all(receiverClients.map((client) => client.initialize()));
  await rig.awaitRegistrations(brokerPort, [sender, ...receiverClients]);
  if (receiverClients.length > 0) {
    await rig.resolvePeers(brokerPort, sender, receiverClients);
  }
  return {
    rig,
    brokerPort,
    clientPort,
    work,
    proxy,
    sender,
    receiverClients,
    ports: rig.usedPorts(),
  };
}

/**
 * How long each client took to become visible to the broker.
 *
 * Carried into every phase result as the evidence that startup really is excluded, and in
 * particular that the auto-summary short-circuited: a client that reached the live Anthropic API
 * would sit in server.ts's three second race before registering, and these figures would say so.
 */
function registrationMs(clients: BenchClient[]): Record<string, number> {
  const out: Record<string, number> = {};
  for (const client of clients) out[client.name] = round(client.registrationMs ?? -1);
  return out;
}

// --- Phase 1: unicast send-to-render latency ---

async function runUnicastPhase(
  config: BenchmarkConfig,
  env: PhaseEnvironment
): Promise<DeliveryPhaseResult> {
  const nonce = Math.random().toString(36).slice(2, 8);
  const { rig, sender, receiverClients, ports } = await startPhase(
    config,
    env,
    "peers-bench-unicast-",
    1,
    false
  );
  try {
    const receiver = receiverClients[0] as BenchClient;
    const targetId = receiver.peerId as string;
    await warmUp(
      sender,
      [receiver],
      (message) => sender.tool("send_message", { to_id: targetId, message }),
      "unicast",
      nonce,
      config.deliveryTimeoutMs
    );

    const random = seededRandom(config.seed);
    const samples: number[] = [];
    const missed: string[] = [];
    for (let i = 0; i < config.samples; i++) {
      // Decorrelate from the poll cycle. Without this every sample lands at the same phase and the
      // reported spread is an artefact of the harness rather than a property of the transport.
      await Bun.sleep(Math.floor(random() * config.pollIntervalMs));
      const marker = markerFor("unicast", nonce, i);
      const arrival = waitFor(receiver, marker, config.deliveryTimeoutMs);
      const issuedAtMs = await sender.issueTool("send_message", {
        to_id: targetId,
        message: `${marker} sample ${i + 1} of ${config.samples}`,
      });
      try {
        const observed = await arrival;
        samples.push(observed.observedAtMs - issuedAtMs);
      } catch {
        missed.push(marker);
      }
      config.log(`  unicast ${i + 1}/${config.samples}`);
    }

    return phaseResult(
      "unicast",
      "One sender, one receiver, one message at a time with a randomised gap.",
      config.samples,
      samples,
      missed,
      { registrationMs: registrationMs([sender, receiver]), ports }
    );
  } finally {
    rig.teardown();
  }
}

// --- Phase 2: burst ---

async function runBurstPhase(
  config: BenchmarkConfig,
  env: PhaseEnvironment
): Promise<DeliveryPhaseResult> {
  const nonce = Math.random().toString(36).slice(2, 8);
  const { rig, sender, receiverClients, ports } = await startPhase(
    config,
    env,
    "peers-bench-burst-",
    1,
    false
  );
  try {
    const receiver = receiverClients[0] as BenchClient;
    const targetId = receiver.peerId as string;
    await warmUp(
      sender,
      [receiver],
      (message) => sender.tool("send_message", { to_id: targetId, message }),
      "burst",
      nonce,
      config.deliveryTimeoutMs
    );

    const random = seededRandom(config.seed);
    const samples: number[] = [];
    const missed: string[] = [];
    const spans: number[] = [];
    // Latency minus the fastest delivery in the SAME round. Absolute burst latency is dominated by
    // where in the poll cycle the round landed, which is not what a burst is being asked about;
    // subtracting the round's leader removes that offset and leaves the queueing cost of being the
    // second, third and tenth message in a burst. This is the number to compare across transports.
    const excessOverRoundLeader: number[] = [];

    // Several rounds, not one. A single burst is issued at a fixed offset from the warm-up's
    // delivery, which on a poll transport pins every message in it to the same point in the poll
    // cycle and reports one arbitrary phase as if it were the answer. A randomised gap before each
    // round spreads the rounds across the cycle.
    for (let roundIndex = 0; roundIndex < config.burstRounds; roundIndex++) {
      await Bun.sleep(Math.floor(random() * config.pollIntervalMs));

      // Every waiter is registered before the first frame is written: registering them as the
      // messages go out would race the arrivals, and a lost race reads as a lost message.
      const markers = Array.from({ length: config.burst }, (_, i) =>
        markerFor("burst", nonce, roundIndex, i)
      );
      const arrivals = markers.map((marker) =>
        waitFor(receiver, marker, config.deliveryTimeoutMs + config.burst * 1000)
      );

      const issued: number[] = [];
      for (let i = 0; i < config.burst; i++) {
        issued.push(
          await sender.issueTool("send_message", {
            to_id: targetId,
            message: `${markers[i]} burst ${i + 1} of ${config.burst}`,
          })
        );
      }

      const observedAt: number[] = [];
      const roundLatencies: number[] = [];
      for (let i = 0; i < config.burst; i++) {
        try {
          const observed = await (arrivals[i] as Promise<{ observedAtMs: number }>);
          const latency = observed.observedAtMs - (issued[i] as number);
          samples.push(latency);
          roundLatencies.push(latency);
          observedAt.push(observed.observedAtMs);
        } catch {
          missed.push(markers[i] as string);
        }
      }
      if (roundLatencies.length > 0) {
        const leader = Math.min(...roundLatencies);
        for (const latency of roundLatencies) excessOverRoundLeader.push(latency - leader);
      }
      // The gap between the first and last arrival of one burst. This is the head-of-line number:
      // a transport that fans a burst out in one go keeps it near zero, and one that pays a round
      // trip per message does not. It is also the only burst figure that is not dominated by where
      // in the poll cycle the round happened to land.
      if (observedAt.length > 1) spans.push(Math.max(...observedAt) - Math.min(...observedAt));
      config.log(`  burst round ${roundIndex + 1}/${config.burstRounds}`);
    }

    return phaseResult(
      "burst",
      `${config.burstRounds} rounds of ${config.burst} messages issued back to back with no gap, ` +
        `one sender, one receiver.`,
      config.burst * config.burstRounds,
      samples,
      missed,
      {
        rounds: config.burstRounds,
        perBurst: config.burst,
        deliverySpanMsPerRound: spans.map((span) => round(span, 2)),
        worstDeliverySpanMs: spans.length > 0 ? round(Math.max(...spans), 2) : null,
        excessOverRoundLeaderMs:
          excessOverRoundLeader.length > 0
            ? {
                median: round(summarize(excessOverRoundLeader).median, 2),
                p95: round(summarize(excessOverRoundLeader).p95, 2),
                max: round(summarize(excessOverRoundLeader).max, 2),
              }
            : null,
        registrationMs: registrationMs([sender, receiver]),
        ports,
      }
    );
  } finally {
    rig.teardown();
  }
}

// --- Phase 3: broadcast, only if the branch has it ---

/**
 * Arguments for whatever `broadcast_message` turns out to look like, or null if it cannot be called
 * blind.
 *
 * The tool does not exist on `dev` and is being written on another branch right now, so its schema
 * is read at runtime rather than assumed. Refusing to guess is deliberate: a wrong guess produces a
 * tool error, thirty timeouts and a table of zeroes, which is exactly the kind of result this
 * harness is supposed to make impossible.
 */
export function broadcastArguments(
  tool: ToolDescriptor,
  message: string
): Record<string, unknown> | null {
  const properties = tool.inputSchema?.properties ?? {};
  const required = tool.inputSchema?.required ?? [];
  const args: Record<string, unknown> = {};
  if ("message" in properties) args.message = message;
  else if ("text" in properties) args.text = message;
  else return null;
  if ("scope" in properties) args.scope = "machine";
  const unsatisfied = required.filter((name) => !(name in args));
  return unsatisfied.length === 0 ? args : null;
}

async function runBroadcastPhase(
  config: BenchmarkConfig,
  env: PhaseEnvironment,
  tool: ToolDescriptor
): Promise<DeliveryPhaseResult | SkippedPhase> {
  const nonce = Math.random().toString(36).slice(2, 8);
  const { rig, sender, receiverClients, ports } = await startPhase(
    config,
    env,
    "peers-bench-broadcast-",
    config.broadcastReceivers,
    false
  );
  try {
    const probe = broadcastArguments(tool, "probe");
    if (!probe) {
      return {
        phase: "broadcast",
        skipped: true,
        reason:
          `broadcast_message is present but its input schema is not one this harness can call ` +
          `blind: ${JSON.stringify(tool.inputSchema ?? {})}`,
      };
    }

    const warmReply = await warmUp(
      sender,
      receiverClients,
      (message) =>
        sender.tool("broadcast_message", broadcastArguments(tool, message) as Record<string, unknown>),
      "broadcast",
      nonce,
      config.deliveryTimeoutMs
    );

    const random = seededRandom(config.seed);
    const samples: number[] = [];
    const missed: string[] = [];
    const perReceiver: Record<string, number[]> = {};
    for (const receiver of receiverClients) perReceiver[receiver.name] = [];

    for (let i = 0; i < config.broadcastSamples; i++) {
      await Bun.sleep(Math.floor(random() * config.pollIntervalMs));
      const marker = markerFor("broadcast", nonce, i);
      const arrivals = receiverClients.map((receiver) => ({
        receiver,
        promise: waitFor(receiver, marker, config.deliveryTimeoutMs),
      }));
      const args = broadcastArguments(tool, `${marker} broadcast ${i + 1}`) as Record<
        string,
        unknown
      >;
      const issuedAtMs = await sender.issueTool("broadcast_message", args);
      for (const { receiver, promise } of arrivals) {
        try {
          const observed = await promise;
          const latency = observed.observedAtMs - issuedAtMs;
          samples.push(latency);
          (perReceiver[receiver.name] as number[]).push(round(latency, 2));
        } catch {
          missed.push(`${marker}@${receiver.name}`);
        }
      }
      config.log(`  broadcast ${i + 1}/${config.broadcastSamples}`);
    }

    return phaseResult(
      "broadcast",
      `One sender, ${config.broadcastReceivers} receivers, one broadcast at a time. Each receiver's ` +
        `delivery is one sample, timed from the single send.`,
      config.broadcastSamples * config.broadcastReceivers,
      samples,
      missed,
      {
        receivers: config.broadcastReceivers,
        perReceiver,
        warmUpReply: warmReply,
        registrationMs: registrationMs([sender, ...receiverClients]),
        ports,
      }
    );
  } finally {
    rig.teardown();
  }
}

// --- Phase 4: idle request volume ---

async function runIdlePhase(
  config: BenchmarkConfig,
  env: PhaseEnvironment
): Promise<{ result: IdlePhaseResult; problems: string[] }> {
  const { rig, brokerPort, proxy, sender, ports } = await startPhase(
    config,
    env,
    "peers-bench-idle-",
    0,
    true
  );
  const problems: string[] = [];
  try {
    const counter = proxy as NonNullable<typeof proxy>;

    // Registration is confirmed against the broker directly, so confirming it does not itself add
    // requests to the count. There is exactly one client, so one peer is the whole population.
    const registered = await waitForPeerCount(brokerPort, 1, 30_000);
    if (!registered) throw new Error("the idle-phase client never registered with the broker");

    // Let the first poll and heartbeat cycles settle before counting, so the window is steady state.
    await Bun.sleep(3000);
    const before = counter.tally();
    counter.reset();
    await Bun.sleep(config.idleWindowMs);
    const tally: RequestTally = counter.tally();

    // A count taken from a client that had died is a count of nothing dressed up as a measurement.
    if (!sender.alive()) problems.push("idle: the client exited during the measurement window");
    if (!(await waitForPeerCount(brokerPort, 1, 2000))) {
      problems.push("idle: the client was no longer registered at the end of the window");
    }
    if (tally.total === 0 && tally.streams === 0) {
      problems.push(
        "idle: zero broker requests and zero long-lived connections in the window, which means the " +
          "client was not talking to the broker at all rather than that the transport is quiet"
      );
    }

    const minutes = config.idleWindowMs / 60_000;
    return {
      result: {
        phase: "idle",
        description:
          "One registered session, no traffic. Requests counted by a reverse proxy in front of the " +
          "broker, so whatever transport the branch uses is counted without instrumenting it.",
        windowMs: config.idleWindowMs,
        sessions: 1,
        requestsPerSessionPerMinute: round(tally.total / minutes, 1),
        totalRequests: tally.total,
        byPath: tally.byPath,
        longLivedRequests: tally.streams,
        requestsBeforeWindow: before.total,
        brokerPort,
        proxyPort: counter.port,
        ports,
      },
      problems,
    };
  } finally {
    rig.teardown();
  }
}

async function waitForPeerCount(port: number, expected: number, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://127.0.0.1:${port}/health`);
      if (res.ok) {
        const body = (await res.json()) as { peers?: number };
        if (body.peers === expected) return true;
      }
    } catch {
      // broker busy or still starting
    }
    await Bun.sleep(200);
  }
  return false;
}

// --- Feature detection ---

/**
 * Ask a live server what it can do, rather than reading the source or the branch name.
 *
 * One throwaway broker and one throwaway client, before any measurement, because the answer decides
 * which phases run. This is the whole reason the harness needs no edit per branch.
 */
async function detectFeatures(
  config: BenchmarkConfig,
  env: PhaseEnvironment
): Promise<{ tools: ToolDescriptor[]; ports: number[] }> {
  const rig = new BenchRig(config.portRange);
  try {
    const work = rig.dir("peers-bench-detect-");
    const brokerEnv = { ...env.base, CLAUDE_PEERS_DB: join(work, "broker.db") };
    const brokerPort = await rig.startBroker(brokerEnv);
    const client = rig.spawnClient({
      name: "probe",
      brokerPort,
      workRoot: work,
      env: brokerEnv,
    });
    await client.initialize();
    return { tools: await client.listTools(), ports: rig.usedPorts() };
  } finally {
    rig.teardown();
  }
}

// --- Orchestration ---

export async function runBenchmark(
  overrides: Partial<BenchmarkConfig> = {}
): Promise<BenchmarkReport> {
  const config: BenchmarkConfig = {
    ...DEFAULT_CONFIG,
    ...overrides,
    skip: { ...DEFAULT_CONFIG.skip, ...(overrides.skip ?? {}) },
  };
  if (config.samples < 1) throw new Error("samples must be at least 1");

  const startedAt = new Date().toISOString();
  const startedAtMs = performance.now();
  const problems: string[] = [];

  // Held for the whole run so no phase can bind it. The Anthropic SDK is pointed here, so the
  // startup auto-summary fails on connect instead of making a real API call.
  const rootRig = new BenchRig(config.portRange);
  const closedPort = rootRig.closedPort();
  const env: PhaseEnvironment = {
    closedPort,
    base: {
      ...(process.env as Record<string, string>),
      ANTHROPIC_BASE_URL: `http://127.0.0.1:${closedPort}`,
      // A placeholder, so the SDK never reaches for the Claude Code OAuth token in the Keychain.
      ANTHROPIC_API_KEY: "benchmark-offline-no-network",
      CLAUDE_PEERS_CHECKPOINT_MS: "1000",
    },
  };

  const phases: BenchmarkReport["phases"] = {
    unicast: null,
    burst: null,
    broadcast: null,
    idle: null,
  };
  let tools: ToolDescriptor[] = [];
  // Recorded so a reader can audit which ports each number came from. The harness reserves by
  // binding, but two suites in this repo bind ports directly without reserving, so "which port did
  // this actually use" is a question the report has to be able to answer.
  const portsUsed: Record<string, number[]> = { closedAnthropicPort: [closedPort] };

  try {
    config.log("detecting available tools...");
    const detected = await detectFeatures(config, env);
    tools = detected.tools;
    portsUsed.detect = detected.ports;
    const broadcastTool = tools.find((tool) => tool.name === "broadcast_message");

    if (!config.skip.unicast) {
      config.log(`phase 1/4: unicast latency, ${config.samples} samples`);
      phases.unicast = await runUnicastPhase(config, env);
      portsUsed.unicast = (phases.unicast.extra?.ports as number[]) ?? [];
    }
    if (!config.skip.burst) {
      config.log(`phase 2/4: burst of ${config.burst}`);
      phases.burst = await runBurstPhase(config, env);
      portsUsed.burst = (phases.burst.extra?.ports as number[]) ?? [];
    }
    if (!config.skip.broadcast) {
      if (broadcastTool) {
        config.log(`phase 3/4: broadcast, ${config.broadcastSamples} rounds`);
        phases.broadcast = await runBroadcastPhase(config, env, broadcastTool);
        if (!isSkipped(phases.broadcast)) {
          portsUsed.broadcast = (phases.broadcast.extra?.ports as number[]) ?? [];
        }
      } else {
        phases.broadcast = {
          phase: "broadcast",
          skipped: true,
          reason: "no broadcast_message tool on this branch",
        };
      }
    }
    if (!config.skip.idle) {
      config.log(`phase 4/4: idle request volume over ${config.idleWindowMs} ms`);
      const idle = await runIdlePhase(config, env);
      phases.idle = idle.result;
      portsUsed.idle = idle.result.ports;
      problems.push(...idle.problems);
    }
  } finally {
    rootRig.teardown();
  }

  // THE GUARD. A latency drawn from a run where messages did not arrive is worse than no latency,
  // because it looks like evidence. Every delivery phase must have delivered everything it sent.
  for (const phase of [phases.unicast, phases.burst, phases.broadcast]) {
    if (!phase || isSkipped(phase)) continue;
    if (phase.delivered === 0) {
      problems.push(`${phase.phase}: nothing was delivered, so there is nothing to report`);
    } else if (phase.delivered < phase.attempted) {
      problems.push(
        `${phase.phase}: only ${phase.delivered} of ${phase.attempted} messages arrived; the ` +
          `sample is short and the percentiles are drawn from survivors only`
      );
    }
  }
  if (phases.unicast && !isSkipped(phases.unicast) && phases.unicast.delivered < 30) {
    problems.push(
      `unicast: ${phases.unicast.delivered} samples is too few to characterise a distribution ` +
        `whose spread is the point; at least 30 is expected`
    );
  }

  return {
    tool: "claude-peers delivery benchmark",
    schemaVersion: 1,
    startedAt,
    durationMs: Math.round(performance.now() - startedAtMs),
    environment: captureEnvironment(),
    config: {
      samples: config.samples,
      burst: config.burst,
      burstRounds: config.burstRounds,
      broadcastSamples: config.broadcastSamples,
      broadcastReceivers: config.broadcastReceivers,
      idleWindowMs: config.idleWindowMs,
      pollIntervalMs: config.pollIntervalMs,
      deliveryTimeoutMs: config.deliveryTimeoutMs,
      seed: config.seed,
      portRange: config.portRange,
      skip: config.skip,
    },
    portsUsed,
    features: {
      tools: tools.map((tool) => tool.name).sort(),
      broadcastMessage: tools.some((tool) => tool.name === "broadcast_message"),
    },
    method: methodLines(config),
    phases,
    valid: problems.length === 0,
    problems,
  };
}

// --- Rendering ---

function pad(value: string, width: number, right = false): string {
  return right ? value.padStart(width) : value.padEnd(width);
}

function statsRow(label: string, phase: DeliveryPhaseResult | SkippedPhase | null): string {
  if (phase === null) return `  ${pad(label, 30)}skipped by configuration`;
  if (isSkipped(phase)) return `  ${pad(label, 30)}skipped: ${phase.reason}`;
  if (!phase.stats) return `  ${pad(label, 30)}NO DELIVERIES, nothing measured`;
  const s = phase.stats;
  return (
    `  ${pad(label, 30)}` +
    `${pad(String(s.count), 5, true)} ` +
    `${pad(round(s.min).toFixed(1), 8, true)} ` +
    `${pad(round(s.median).toFixed(1), 8, true)} ` +
    `${pad(round(s.p95).toFixed(1), 8, true)} ` +
    `${pad(round(s.max).toFixed(1), 8, true)} ` +
    `${pad(round(s.mean).toFixed(1), 8, true)}`
  );
}

export function renderReport(report: BenchmarkReport): string {
  const env = report.environment;
  const lines: string[] = [];
  lines.push("");
  lines.push("claude-peers message-delivery benchmark");
  lines.push(
    `  branch ${env.branch ?? "?"} @ ${env.commit ?? "?"}` +
      `${env.workingTreeDirty ? " (working tree dirty)" : ""}, bun ${env.bun}, ${env.platform}`
  );
  lines.push(`  started ${report.startedAt}, took ${(report.durationMs / 1000).toFixed(1)} s`);
  lines.push(
    `  ports bound: ` +
      Object.entries(report.portsUsed)
        .map(([phase, ports]) => `${phase} ${ports.join("/") || "none"}`)
        .join(", ")
  );
  lines.push(
    `  tools present: ${report.features.tools.join(", ") || "(none)"}` +
      `  |  broadcast_message: ${report.features.broadcastMessage ? "yes" : "no"}`
  );
  lines.push("");
  lines.push("Method");
  for (const line of report.method) lines.push(`  ${line}`);
  lines.push("");
  lines.push("Send-to-render latency, milliseconds");
  lines.push(
    `  ${pad("measurement", 30)}${pad("n", 5, true)} ${pad("min", 8, true)} ` +
      `${pad("p50", 8, true)} ${pad("p95", 8, true)} ${pad("max", 8, true)} ${pad("mean", 8, true)}`
  );
  lines.push(statsRow("unicast", report.phases.unicast));
  const burst = report.phases.burst;
  lines.push(
    statsRow(
      burst && !isSkipped(burst)
        ? `burst, ${burst.extra?.rounds ?? "?"} x ${burst.extra?.perBurst ?? "?"}`
        : "burst",
      burst
    )
  );
  const broadcast = report.phases.broadcast;
  lines.push(
    statsRow(
      broadcast && !isSkipped(broadcast)
        ? `broadcast to ${(broadcast.extra?.receivers as number) ?? "?"}`
        : "broadcast",
      broadcast
    )
  );
  if (burst && !isSkipped(burst) && burst.extra) {
    const excess = burst.extra.excessOverRoundLeaderMs as {
      median: number;
      p95: number;
      max: number;
    } | null;
    lines.push(
      "  Burst percentiles above inherit the poll phase each round happened to land in. The two"
    );
    lines.push("  figures below do not, and are what a burst is actually being asked about:");
    lines.push(
      `    queueing cost, latency minus the fastest delivery of the same round: ` +
        (excess ? `p50 ${excess.median} ms, p95 ${excess.p95} ms, max ${excess.max} ms` : "none")
    );
    lines.push(
      `    first-to-last arrival spread within one burst: worst ` +
        `${burst.extra.worstDeliverySpanMs} ms (per round: ` +
        `${(burst.extra.deliverySpanMsPerRound as number[]).join(", ")})`
    );
  }
  lines.push("");
  lines.push("Idle broker load, no traffic at all");
  const idle = report.phases.idle;
  if (idle === null) {
    lines.push("  skipped by configuration");
  } else if (isSkipped(idle)) {
    lines.push(`  skipped: ${idle.reason}`);
  } else {
    lines.push(
      `  ${pad("requests per session per minute", 34)}${idle.requestsPerSessionPerMinute.toFixed(1)}`
    );
    lines.push(
      `  ${pad("window", 34)}${(idle.windowMs / 1000).toFixed(0)} s, ` +
        `${idle.totalRequests} requests, ${idle.longLivedRequests} long lived`
    );
    for (const [path, count] of Object.entries(idle.byPath).sort((a, b) => b[1] - a[1])) {
      lines.push(`  ${pad(`  ${path}`, 34)}${count}`);
    }
  }
  lines.push("");
  if (report.valid) {
    lines.push("Delivery guard: PASS, every message sent was accounted for.");
  } else {
    lines.push("Delivery guard: FAIL. These numbers must not be quoted.");
    for (const problem of report.problems) lines.push(`  - ${problem}`);
  }
  lines.push("");
  return lines.join("\n");
}

// --- Command line ---

function parseArgs(argv: string[]): Partial<BenchmarkConfig> & { jsonOut?: string } {
  const out: Partial<BenchmarkConfig> & { jsonOut?: string } = {};
  const skip = { ...DEFAULT_CONFIG.skip };
  let touchedSkip = false;
  const range = { ...DEFAULT_CONFIG.portRange };
  let touchedRange = false;

  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i] as string;
    const next = () => {
      const value = argv[++i];
      if (value === undefined) throw new Error(`${arg} needs a value`);
      return value;
    };
    switch (arg) {
      case "--samples":
        out.samples = Number.parseInt(next(), 10);
        break;
      case "--burst":
        out.burst = Number.parseInt(next(), 10);
        break;
      case "--burst-rounds":
        out.burstRounds = Number.parseInt(next(), 10);
        break;
      case "--broadcast-samples":
        out.broadcastSamples = Number.parseInt(next(), 10);
        break;
      case "--broadcast-receivers":
        out.broadcastReceivers = Number.parseInt(next(), 10);
        break;
      case "--idle-window-ms":
        out.idleWindowMs = Number.parseInt(next(), 10);
        break;
      case "--poll-interval-ms":
        out.pollIntervalMs = Number.parseInt(next(), 10);
        break;
      case "--delivery-timeout-ms":
        out.deliveryTimeoutMs = Number.parseInt(next(), 10);
        break;
      case "--seed":
        out.seed = Number.parseInt(next(), 10);
        break;
      case "--port-min":
        range.min = Number.parseInt(next(), 10);
        touchedRange = true;
        break;
      case "--port-max":
        range.max = Number.parseInt(next(), 10);
        touchedRange = true;
        break;
      case "--json-out":
        out.jsonOut = next();
        break;
      case "--skip-unicast":
        skip.unicast = true;
        touchedSkip = true;
        break;
      case "--skip-burst":
        skip.burst = true;
        touchedSkip = true;
        break;
      case "--skip-broadcast":
        skip.broadcast = true;
        touchedSkip = true;
        break;
      case "--skip-idle":
        skip.idle = true;
        touchedSkip = true;
        break;
      case "--quiet":
        out.log = () => {};
        break;
      case "--help":
      case "-h":
        console.log(USAGE);
        process.exit(0);
        break;
      default:
        throw new Error(`unknown argument ${arg}. Try --help.`);
    }
  }
  if (touchedSkip) out.skip = skip;
  if (touchedRange) out.portRange = range;
  return out;
}

const USAGE = `claude-peers message-delivery benchmark

  bun bench/delivery-latency.ts [options]

Options (defaults in brackets):
  --samples N              unicast latency samples [30]
  --burst N                messages sent back to back in one burst [10]
  --burst-rounds N         how many bursts [5]
  --broadcast-samples N    broadcast rounds, if the tool exists [30]
  --broadcast-receivers N  receivers in the broadcast phase [3]
  --idle-window-ms N       how long to count idle broker requests [60000]
  --poll-interval-ms N     sizes the randomised gap between samples [1000]
  --delivery-timeout-ms N  when a message counts as never delivered [20000]
  --seed N                 seed for the sample gaps [20260803]
  --port-min N             first port the harness may bind [7770]
  --port-max N             last port the harness may bind [7789]
  --json-out PATH          also write the machine-readable report here
  --skip-unicast | --skip-burst | --skip-broadcast | --skip-idle
  --quiet                  no progress lines on stderr

The range must not contain 7899: that port serves live Claude Code sessions and
the harness refuses to run if it is included.

Exit code is 1 when the delivery guard fails, so a short sample cannot be
mistaken for a result by a script.`;

async function main(): Promise<void> {
  const parsed = parseArgs(process.argv.slice(2));
  const { jsonOut, ...overrides } = parsed;
  const report = await runBenchmark(overrides);
  const json = JSON.stringify(report, null, 2);
  console.log(renderReport(report));
  console.log("=== JSON ===");
  console.log(json);
  if (jsonOut) {
    writeFileSync(jsonOut, json + "\n");
    console.error(`wrote ${jsonOut}`);
  }
  process.exit(report.valid ? 0 : 1);
}

if (import.meta.main) {
  main().catch((e) => {
    console.error(`benchmark failed: ${e instanceof Error ? (e.stack ?? e.message) : String(e)}`);
    process.exit(1);
  });
}
