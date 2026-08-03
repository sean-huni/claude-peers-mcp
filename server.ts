#!/usr/bin/env bun
/**
 * claude-peers MCP server
 *
 * Spawned by Claude Code as a stdio MCP server (one per instance).
 * Connects to the shared broker daemon for peer discovery and messaging.
 * Declares claude/channel capability to push inbound messages immediately.
 *
 * Usage:
 *   claude --dangerously-load-development-channels server:claude-peers
 *
 * With .mcp.json:
 *   { "claude-peers": { "command": "bun", "args": ["./server.ts"] } }
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import type {
  PeerId,
  Peer,
  RegisterResponse,
  PollMessagesResponse,
  Message,
} from "./shared/types.ts";
import { fileURLToPath } from "node:url";
import pkg from "./package.json";
import { drainSpool, findSessionPid, spoolMessage, sweepDeadSpools } from "./spool";
import { PollBackoff } from "./poll-backoff.ts";
import {
  generateSummary,
  getGitBranch,
  getRecentFiles,
} from "./shared/summarize.ts";

// --- Configuration ---

const BROKER_PORT = parseInt(process.env.CLAUDE_PEERS_PORT ?? "7899", 10);
const BROKER_URL = `http://127.0.0.1:${BROKER_PORT}`;
const POLL_INTERVAL_MS = 1000;
const HEARTBEAT_INTERVAL_MS = 15_000;
// A broker that is down must not cost a log line per poll. Config from the
// environment so an operator can tune the noise without a rebuild.
const POLL_BACKOFF_MAX_MS = parseInt(
  process.env.CLAUDE_PEERS_POLL_BACKOFF_MAX_MS ?? "60000",
  10
);
const POLL_QUIET_MS = parseInt(process.env.CLAUDE_PEERS_POLL_QUIET_MS ?? "300000", 10);
// fileURLToPath, not .pathname: the latter is percent-encoded, so any install
// directory containing a space yields a module-not-found at broker launch.
const BROKER_SCRIPT = fileURLToPath(new URL("./broker.ts", import.meta.url));
// Sourced from package.json so the version reported to MCP clients cannot
// drift from the released one.
const VERSION = (pkg as { version: string }).version;

// --- Broker communication ---

async function brokerFetch<T>(path: string, body: unknown): Promise<T> {
  const res = await fetch(`${BROKER_URL}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(myToken ? { Authorization: `Bearer ${myToken}` } : {}),
    },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`Broker error (${path}): ${res.status} ${err}`);
  }
  return res.json() as Promise<T>;
}

async function isBrokerAlive(): Promise<boolean> {
  try {
    const res = await fetch(`${BROKER_URL}/health`, { signal: AbortSignal.timeout(2000) });
    return res.ok;
  } catch {
    return false;
  }
}

async function ensureBroker(): Promise<void> {
  if (await isBrokerAlive()) {
    log("Broker already running");
    return;
  }

  log("Starting broker daemon...");
  const proc = Bun.spawn(["bun", BROKER_SCRIPT], {
    stdio: ["ignore", "ignore", "inherit"],
    // Detach so the broker survives if this MCP server exits
    // On macOS/Linux, the broker will keep running
  });

  // Unref so this process can exit without waiting for the broker
  proc.unref();

  // Wait for it to come up
  for (let i = 0; i < 30; i++) {
    await new Promise((r) => setTimeout(r, 200));
    if (await isBrokerAlive()) {
      log("Broker started");
      return;
    }
  }
  throw new Error("Failed to start broker daemon after 6 seconds");
}

// --- Utility ---

function log(msg: string) {
  // MCP stdio servers must only use stderr for logging (stdout is the MCP protocol)
  console.error(`[claude-peers] ${msg}`);
}

async function getGitRoot(cwd: string): Promise<string | null> {
  try {
    const proc = Bun.spawn(["git", "rev-parse", "--show-toplevel"], {
      cwd,
      stdout: "pipe",
      stderr: "ignore",
    });
    const text = await new Response(proc.stdout).text();
    const code = await proc.exited;
    if (code === 0) {
      return text.trim();
    }
  } catch {
    // not a git repo
  }
  return null;
}

function getTty(): string | null {
  try {
    // Try to get the parent's tty from the process tree
    const ppid = process.ppid;
    if (ppid) {
      const proc = Bun.spawnSync(["ps", "-o", "tty=", "-p", String(ppid)]);
      const tty = new TextDecoder().decode(proc.stdout).trim();
      if (tty && tty !== "?" && tty !== "??") {
        return tty;
      }
    }
  } catch {
    // ignore
  }
  return null;
}

// --- State ---

let myId: PeerId | null = null;
// Minted by the broker at registration; proves this process owns myId.
let myToken: string | null = null;
let myCwd = process.cwd();
let myGitRoot: string | null = null;

// --- MCP Server ---

const mcp = new Server(
  { name: "claude-peers", version: VERSION },
  {
    capabilities: {
      experimental: { "claude/channel": {} },
      tools: {},
    },
    instructions: `You are connected to the claude-peers network. Other Claude Code instances on this machine can see you and send you messages.

IMPORTANT: When you receive a <channel source="claude-peers" ...> message, RESPOND IMMEDIATELY. Do not wait until your current task is finished. Pause what you are doing, reply to the message using send_message, then resume your work. Treat incoming peer messages like a coworker tapping you on the shoulder — answer right away, even if you're in the middle of something.

Read the from_id, from_summary, and from_cwd attributes to understand who sent the message. Reply by calling send_message with their from_id.

Available tools:
- list_peers: Discover other Claude Code instances (scope: machine/directory/repo)
- send_message: Send a message to another instance by ID
- set_summary: Set a 1-2 sentence summary of what you're working on (visible to other peers)
- check_messages: Manually check for new messages

When you start, proactively call set_summary to describe what you're working on. This helps other instances understand your context.`,
  }
);

// --- Tool definitions ---

const TOOLS = [
  {
    name: "list_peers",
    description:
      "List other Claude Code instances running on this machine. Returns their ID, working directory, git repo, and summary.",
    inputSchema: {
      type: "object" as const,
      properties: {
        scope: {
          type: "string" as const,
          enum: ["machine", "directory", "repo"],
          description:
            'Scope of peer discovery. "machine" = all instances on this computer. "directory" = same working directory. "repo" = same git repository (including worktrees or subdirectories).',
        },
      },
      required: ["scope"],
    },
  },
  {
    name: "send_message",
    description:
      "Send a message to another Claude Code instance by peer ID. The message will be pushed into their session immediately via channel notification.",
    inputSchema: {
      type: "object" as const,
      properties: {
        to_id: {
          type: "string" as const,
          description: "The peer ID of the target Claude Code instance (from list_peers)",
        },
        message: {
          type: "string" as const,
          description: "The message to send",
        },
      },
      required: ["to_id", "message"],
    },
  },
  {
    name: "set_summary",
    description:
      "Set a brief summary (1-2 sentences) of what you are currently working on. This is visible to other Claude Code instances when they list peers.",
    inputSchema: {
      type: "object" as const,
      properties: {
        summary: {
          type: "string" as const,
          description: "A 1-2 sentence summary of your current work",
        },
      },
      required: ["summary"],
    },
  },
  {
    name: "check_messages",
    description:
      "Manually check for new messages from other Claude Code instances. Messages are normally pushed automatically via channel notifications, but you can use this as a fallback.",
    inputSchema: {
      type: "object" as const,
      properties: {},
    },
  },
];

// --- Tool handlers ---

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: TOOLS,
}));

mcp.setRequestHandler(CallToolRequestSchema, async (req) => {
  const { name, arguments: args } = req.params;

  switch (name) {
    case "list_peers": {
      const scope = (args as { scope: string }).scope as "machine" | "directory" | "repo";
      try {
        const peers = await brokerFetch<Peer[]>("/list-peers", {
          scope,
          cwd: myCwd,
          git_root: myGitRoot,
          exclude_id: myId,
        });

        if (peers.length === 0) {
          return {
            content: [
              {
                type: "text" as const,
                text: `No other Claude Code instances found (scope: ${scope}).`,
              },
            ],
          };
        }

        const lines = peers.map((p) => {
          const parts = [
            `ID: ${p.id}`,
            `PID: ${p.pid}`,
            `CWD: ${p.cwd}`,
          ];
          if (p.git_root) parts.push(`Repo: ${p.git_root}`);
          if (p.tty) parts.push(`TTY: ${p.tty}`);
          if (p.summary) parts.push(`Summary: ${p.summary}`);
          parts.push(`Last seen: ${p.last_seen}`);
          return parts.join("\n  ");
        });

        return {
          content: [
            {
              type: "text" as const,
              text: `Found ${peers.length} peer(s) (scope: ${scope}):\n\n${lines.join("\n\n")}`,
            },
          ],
        };
      } catch (e) {
        return {
          content: [
            {
              type: "text" as const,
              text: `Error listing peers: ${e instanceof Error ? e.message : String(e)}`,
            },
          ],
          isError: true,
        };
      }
    }

    case "send_message": {
      const { to_id, message } = args as { to_id: string; message: string };
      if (!myId) {
        return {
          content: [{ type: "text" as const, text: "Not registered with broker yet" }],
          isError: true,
        };
      }
      try {
        const result = await brokerFetch<{ ok: boolean; error?: string }>("/send-message", {
          from_id: myId,
          to_id,
          text: message,
        });
        if (!result.ok) {
          return {
            content: [{ type: "text" as const, text: `Failed to send: ${result.error}` }],
            isError: true,
          };
        }
        return {
          content: [{ type: "text" as const, text: `Message sent to peer ${to_id}` }],
        };
      } catch (e) {
        return {
          content: [
            {
              type: "text" as const,
              text: `Error sending message: ${e instanceof Error ? e.message : String(e)}`,
            },
          ],
          isError: true,
        };
      }
    }

    case "set_summary": {
      const { summary } = args as { summary: string };
      if (!myId) {
        return {
          content: [{ type: "text" as const, text: "Not registered with broker yet" }],
          isError: true,
        };
      }
      try {
        await brokerFetch("/set-summary", { id: myId, summary });
        return {
          content: [{ type: "text" as const, text: `Summary updated: "${summary}"` }],
        };
      } catch (e) {
        return {
          content: [
            {
              type: "text" as const,
              text: `Error setting summary: ${e instanceof Error ? e.message : String(e)}`,
            },
          ],
          isError: true,
        };
      }
    }

    case "check_messages": {
      if (!myId) {
        return {
          content: [{ type: "text" as const, text: "Not registered with broker yet" }],
          isError: true,
        };
      }
      try {
        // Anything the poll loop already spooled is drained here too, so this tool keeps working
        // exactly as it did when the hook is not installed. Without this, spooling would MOVE
        // messages out of reach: check_messages would answer "no new messages" while they sat in a
        // file nobody was reading, which is worse than the problem the spool solves. Caught by
        // three existing tests, which is what they were for.
        const spooled = sessionPid === null ? [] : drainSpool(sessionPid);
        const result = await brokerFetch<PollMessagesResponse>("/poll-messages", { id: myId });

        if (spooled.length === 0 && result.messages.length === 0) {
          return {
            content: [{ type: "text" as const, text: "No new messages." }],
          };
        }

        // Spooled first: they arrived earlier, and a conversation read out of order is not a
        // conversation.
        const lines = [
          ...spooled.map((m) => `From ${m.from_id} (${m.sent_at}):\n${m.text}`),
          ...result.messages.map((m) => `From ${m.from_id} (${m.sent_at}):\n${m.text}`),
        ];
        const total = spooled.length + result.messages.length;
        // Rendered to the caller, so acknowledge it. Polling no longer
        // consumes, so without this the same message is returned every time.
        await ackMessages(result.messages.map((m) => m.id));
        for (const m of result.messages) pushedMessageIds.add(m.id);
        return {
          content: [
            {
              type: "text" as const,
              text: `${total} new message(s):\n\n${lines.join("\n\n---\n\n")}`,
            },
          ],
        };
      } catch (e) {
        return {
          content: [
            {
              type: "text" as const,
              text: `Error checking messages: ${e instanceof Error ? e.message : String(e)}`,
            },
          ],
          isError: true,
        };
      }
    }

    default:
      throw new Error(`Unknown tool: ${name}`);
  }
});

// --- Polling loop for inbound messages ---

// Messages already pushed this process lifetime. The broker no longer consumes
// on poll, so without this the same message is re-pushed on every 1s cycle.
const pushedMessageIds = new Set<number>();

/**
 * Acknowledge messages, which deletes them broker-side.
 *
 * Only called once a message has actually been rendered to the user, so a
 * message is never destroyed by the mere act of polling for it.
 */
async function ackMessages(ids: number[]): Promise<void> {
  if (!myId || ids.length === 0) return;
  try {
    await brokerFetch("/ack-messages", { peer_id: myId, message_ids: ids });
  } catch {
    // Leaving it unacked is safe: it stays queued and is retried next cycle.
  }
}

/**
 * Whether this session can render channel notifications.
 *
 * Claude Code does NOT advertise the channel as an MCP capability. Its
 * initialize frame carries only {roots, elicitation}, with no experimental
 * field, even when launched with --dangerously-load-development-channels.
 * Gating on a client capability therefore disables push for every real
 * session, which is exactly the bug this replaces.
 *
 * The flag is instead visible in the parent process's argv, where it names the
 * servers allowed to push: `--dangerously-load-development-channels
 * server:claude-peers`. That is the only honest signal available, so read it
 * once at startup. CLAUDE_PEERS_CHANNEL=always|never overrides, for tests and
 * for hosts where reading the parent is not possible.
 *
 * Pushing to a session that cannot render is worse than not pushing, because
 * the message is acknowledged and deleted unseen. So when detection is
 * genuinely impossible the safe answer is no push: the message stays queued
 * and check_messages still delivers it.
 */
const SERVER_NAME = "claude-peers";

function detectChannelEnabled(): boolean {
  const override = process.env.CLAUDE_PEERS_CHANNEL;
  if (override === "always") return true;
  if (override === "never") return false;

  try {
    const parent = Bun.spawnSync(["ps", "-o", "command=", "-p", String(process.ppid)]);
    const argv = new TextDecoder().decode(parent.stdout);
    if (!argv.includes("dangerously-load-development-channels")) return false;
    // The flag lists which servers may push. Only claim the channel when this
    // server is one of them.
    return argv.includes(`server:${SERVER_NAME}`);
  } catch {
    return false;
  }
}

const channelEnabled = detectChannelEnabled();

function clientRendersChannel(): boolean {
  return channelEnabled;
}

/**
 * The session this server belongs to, for spooled delivery.
 *
 * Resolved once: the parent cannot change, and re-deriving it on every cycle would run two `ps`
 * calls a second for the life of the session.
 */
const sessionPid: number | null = findSessionPid();

/**
 * Retry schedule and log rate limiting for the loop below.
 *
 * The timer still fires every second, because that is the latency a reachable
 * broker deserves. When the broker is unreachable this holds the loop off on a
 * growing interval and suppresses the repeat log lines, which otherwise filled
 * the session's MCP log file at one line a second for as long as the outage
 * lasted.
 */
const pollBackoff = new PollBackoff({
  baseDelayMs: POLL_INTERVAL_MS,
  maxDelayMs: POLL_BACKOFF_MAX_MS,
  quietMs: POLL_QUIET_MS,
});

async function pollAndPushMessages() {
  if (!myId) return;
  // Without a channel AND without a resolvable session there is nowhere to deliver, so the message
  // stays queued for check_messages. That is the only remaining case where a message waits to be
  // asked for.
  if (!clientRendersChannel() && sessionPid === null) return;
  // Serving out a backoff window from an earlier failure.
  if (!pollBackoff.ready(Date.now())) return;

  try {
    const result = await brokerFetch<PollMessagesResponse>("/poll-messages", { id: myId });
    // Reached the broker, so an outage that was reported earlier is now over.
    // Said once, then the loop goes quiet again.
    const recovered = pollBackoff.noteSuccess(Date.now());
    if (recovered) log(recovered);
    const fresh = result.messages.filter((m) => !pushedMessageIds.has(m.id));

    for (const msg of fresh) {
      // Look up the sender's info for context
      let fromSummary = "";
      let fromCwd = "";
      try {
        const peers = await brokerFetch<Peer[]>("/list-peers", {
          scope: "machine",
          cwd: myCwd,
          git_root: myGitRoot,
          exclude_id: myId,
        });
        const sender = peers.find((p) => p.id === msg.from_id);
        if (sender) {
          fromSummary = sender.summary;
          fromCwd = sender.cwd;
        }
      } catch {
        // Non-critical, proceed without sender info
      }

      if (clientRendersChannel()) {
        // Push as channel notification — this is what makes it immediate
        await mcp.notification({
          method: "notifications/claude/channel",
          params: {
            content: msg.text,
            meta: {
              from_id: msg.from_id,
              from_summary: fromSummary,
              from_cwd: fromCwd,
              sent_at: msg.sent_at,
            },
          },
        });
        log(`Pushed message from ${msg.from_id} (${msg.text.length} chars)`);
      } else {
        // No channel, so write it where a hook will find it. Throws on a failed write, which skips
        // the ack below and leaves the message queued for the next cycle: the same durability rule
        // the push path relies on.
        spoolMessage(sessionPid!, {
          id: msg.id,
          from_id: msg.from_id,
          from_summary: fromSummary,
          from_cwd: fromCwd,
          sent_at: msg.sent_at,
          text: msg.text,
        });
        log(`Spooled message from ${msg.from_id} (${msg.text.length} chars)`);
      }

      // Handed to something that will render it, so it is safe to destroy broker-side. Acking only
      // after delivery is what makes it durable: a crash in between leaves the message queued.
      //
      // Log the fact, never the text: stderr is captured to a log file that has none of the
      // database's permission, secure_delete or TTL protections.
      pushedMessageIds.add(msg.id);
      await ackMessages([msg.id]);
    }
  } catch (e) {
    // Broker might be down temporarily, don't crash. The failure also backs the
    // loop off and rate limits this line: an unreachable broker used to write
    // one line a second into the session's MCP log file, indefinitely. It is
    // still reported the first time, on any change of error, and periodically
    // for as long as it lasts, because a session that cannot reach its broker
    // is broken and silence would hide that.
    const line = pollBackoff.noteFailure(e instanceof Error ? e.message : String(e), Date.now());
    if (line) log(line);
  }
}

// --- Startup ---

async function main() {
  // 1. Ensure broker is running
  await ensureBroker();

  // 2. Gather context
  myCwd = process.cwd();
  myGitRoot = await getGitRoot(myCwd);
  const tty = getTty();

  log(`CWD: ${myCwd}`);
  log(`Git root: ${myGitRoot ?? "(none)"}`);
  log(`TTY: ${tty ?? "(unknown)"}`);

  // 3. Generate initial summary via Claude (non-blocking, best-effort)
  let initialSummary = "";
  const summaryPromise = (async () => {
    try {
      const branch = await getGitBranch(myCwd);
      const recentFiles = await getRecentFiles(myCwd);
      const summary = await generateSummary({
        cwd: myCwd,
        git_root: myGitRoot,
        git_branch: branch,
        recent_files: recentFiles,
      });
      if (summary) {
        initialSummary = summary;
        log(`Auto-summary: ${summary}`);
      }
    } catch (e) {
      log(`Auto-summary failed (non-critical): ${e instanceof Error ? e.message : String(e)}`);
    }
  })();

  // Wait briefly for summary, but don't block startup
  await Promise.race([summaryPromise, new Promise((r) => setTimeout(r, 3000))]);

  // 4. Register with broker
  const reg = await brokerFetch<RegisterResponse>("/register", {
    pid: process.pid,
    cwd: myCwd,
    git_root: myGitRoot,
    tty,
    summary: initialSummary,
  });
  myId = reg.id;
  myToken = reg.token;
  log(`Registered as peer ${myId}`);

  // If summary generation is still running, update it when done
  if (!initialSummary) {
    summaryPromise.then(async () => {
      if (initialSummary && myId) {
        try {
          await brokerFetch("/set-summary", { id: myId, summary: initialSummary });
          log(`Late auto-summary applied: ${initialSummary}`);
        } catch {
          // Non-critical
        }
      }
    });
  }

  // 5. Connect MCP over stdio
  await mcp.connect(new StdioServerTransport());
  log("MCP connected");

  // 6. Start polling for inbound messages
  //
  // Clear out queues belonging to sessions that have since exited. A pid is reused eventually, and
  // inheriting a dead session's unread messages would deliver somebody else's conversation into
  // this one. Cheap, and once per process is enough: the risk arrives with a NEW session, which
  // runs this itself.
  try {
    sweepDeadSpools();
  } catch {
    // A queue that cannot be swept is not a reason to refuse to start.
  }
  const pollTimer = setInterval(pollAndPushMessages, POLL_INTERVAL_MS);

  // 7. Start heartbeat
  const heartbeatTimer = setInterval(async () => {
    if (myId) {
      try {
        await brokerFetch("/heartbeat", { id: myId });
      } catch {
        // Non-critical
      }
    }
  }, HEARTBEAT_INTERVAL_MS);

  // 8. Clean up on exit
  const cleanup = async () => {
    clearInterval(pollTimer);
    clearInterval(heartbeatTimer);
    if (myId) {
      try {
        await brokerFetch("/unregister", { id: myId });
        log("Unregistered from broker");
      } catch {
        // Best effort
      }
    }
    process.exit(0);
  };

  process.on("SIGINT", cleanup);
  process.on("SIGTERM", cleanup);
}

main().catch((e) => {
  log(`Fatal: ${e instanceof Error ? e.message : String(e)}`);
  process.exit(1);
});
