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
  BroadcastMessageResponse,
  PollMessagesResponse,
  Message,
} from "./shared/types.ts";
import { fileURLToPath } from "node:url";
import pkg from "./package.json";
import { drainSpool, findHostSessionPid, spoolMessage, sweepDeadSpools } from "./spool";
import { PollBackoff } from "./poll-backoff.ts";
import {
  generateSummary,
  getGitBranch,
  getRecentFiles,
} from "./shared/summarize.ts";

// --- Configuration ---

const BROKER_PORT = parseInt(process.env.CLAUDE_PEERS_PORT ?? "7899", 10);
const BROKER_URL = `http://127.0.0.1:${BROKER_PORT}`;
// The interval the poll runs at when it is the only transport. This is what the whole session used
// to run at, and what it returns to whenever the stream is not there.
const POLL_INTERVAL_MS = parseInt(process.env.CLAUDE_PEERS_POLL_MS ?? "1000", 10);
// The interval the poll runs at while the stream is healthy. The poll stops being the transport
// and becomes the audit that the transport is working, so it can be thirty times cheaper: about
// 2,880 requests a day instead of 86,400.
const POLL_IDLE_INTERVAL_MS = parseInt(process.env.CLAUDE_PEERS_POLL_IDLE_MS ?? "30000", 10);
// Kill switch for the push transport. Off leaves exactly the previous behaviour, which is what
// makes "before" measurable on this code and gives an operator somewhere to stand if the stream
// ever misbehaves.
const STREAM_ENABLED = (process.env.CLAUDE_PEERS_STREAM ?? "on") !== "off";
// How long a stream must survive before it counts as healthy rather than lucky. Without this, a
// broker that accepts a subscription and drops it immediately is reconnected to in a tight loop,
// because every connection looks like a recovery.
const STREAM_STABLE_MS = parseInt(process.env.CLAUDE_PEERS_STREAM_STABLE_MS ?? "5000", 10);
// How long a stream may say nothing at all, keepalives included, before it is presumed dead. Must
// stay comfortably above the broker's keepalive period (25s), because this is what distinguishes a
// quiet stream from a socket that was blackholed without a FIN. Without it, that socket is
// believed healthy forever and the session sits on the slow poll interval rather than the fast one.
const STREAM_IDLE_TIMEOUT_MS = parseInt(process.env.CLAUDE_PEERS_STREAM_IDLE_MS ?? "75000", 10);
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

/**
 * Which body field carries this session's own id, per route.
 *
 * The broker authenticates the token against the peer the body names, so a
 * re-registration has to rewrite the body as well as the module state. Missing
 * this leaves the retry presenting a fresh token for a peer id that no longer
 * exists, which is refused exactly like the original call.
 */
const CALLER_FIELD: Record<string, string> = {
  "/heartbeat": "id",
  "/set-summary": "id",
  "/list-peers": "exclude_id",
  "/send-message": "from_id",
  "/broadcast-message": "from_id",
  "/poll-messages": "id",
  "/ack-messages": "peer_id",
  "/unregister": "id",
};

function withCallerId(path: string, body: unknown): unknown {
  const field = CALLER_FIELD[path];
  if (!field || typeof body !== "object" || body === null) return body;
  return { ...(body as Record<string, unknown>), [field]: myId };
}

/** In-flight re-registration, so concurrent refusals share one attempt. */
let reregistration: Promise<void> | null = null;

/**
 * Push the remembered name to the broker under the current identity.
 *
 * Throws on transport failure; resolves with the broker's verdict otherwise.
 * A "taken" verdict is surfaced to the caller rather than retried: names are
 * contended by design and the holder is not going to release it because we ask twice.
 */
async function applyName(): Promise<{ ok: boolean; error?: string }> {
  const result = await brokerFetch<{ ok: boolean; error?: string }>("/set-name", {
    id: myId,
    name: myName,
  });
  if (result.ok) log(`Peer name claimed: ${myName}`);
  else log(`Peer name not claimed (${result.error}); running unnamed`);
  return result;
}

async function registerWithBroker(): Promise<RegisterResponse> {
  return brokerFetch<RegisterResponse>(
    "/register",
    {
      pid: process.pid,
      cwd: myCwd,
      git_root: myGitRoot,
      tty: myTty,
      summary: mySummary,
    },
    false
  );
}

/**
 * Take a new identity after the broker stopped recognising the old one.
 *
 * Session identity lived only in this process's memory and was established once
 * at startup, so a broker restarted against an empty database refused every
 * call with 401, permanently, and the only remedy was restarting the Claude
 * Code session. See https://12factor.net/disposability.
 *
 * Single-flight and bounded: several calls are usually in flight when the
 * identity dies (the poll loop, the heartbeat, whatever tool the model is
 * running), and one registration per refusal would leave the session holding
 * several identities, only the last of which anyone could reach. A caller whose
 * identity was already replaced while it waited simply retries with the new one.
 */
async function recoverIdentity(staleId: PeerId | null): Promise<void> {
  if (myId !== staleId) return; // already replaced by another caller
  if (reregistration) return reregistration;

  reregistration = (async () => {
    try {
      const reg = await registerWithBroker();
      myId = reg.id;
      myToken = reg.token;
      // Message ids restart from 1 in a rebuilt database, so ids remembered
      // from the old one would suppress genuinely new messages as duplicates.
      pushedMessageIds.clear();
      // The open stream is subscribed to an identity the broker has forgotten, so it can never
      // carry anything again. Dropping it makes the loop reconnect under the new one.
      restartStream();
      // The broker's name column died with the old row (or the old database).
      // Re-claim, best-effort: a collision here means another session took the
      // name in the meantime, which set_name would have refused identically.
      if (myName) {
        applyName().catch(() => {});
      }
      log(`Re-registered as peer ${myId} after the broker rejected the previous identity`);
    } finally {
      reregistration = null;
    }
  })();

  return reregistration;
}

async function brokerFetch<T>(path: string, body: unknown, recoverable = true): Promise<T> {
  // Captured before the call: by the time a 401 comes back, another caller may
  // already have re-registered, and this request only needs to adopt that.
  const identityAtRequest = myId;

  const res = await fetch(`${BROKER_URL}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(myToken ? { Authorization: `Bearer ${myToken}` } : {}),
    },
    body: JSON.stringify(body),
  });

  // 401 is the broker saying this peer id and token are unknown to it, which
  // after a database loss is true of every live session. Register again and
  // retry once. Once, never in a loop: a second refusal is a real failure and
  // is reported rather than retried.
  if (res.status === 401 && recoverable && path !== "/register") {
    await recoverIdentity(identityAtRequest);
    return brokerFetch<T>(path, withCallerId(path, body), false);
  }

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
// Kept so a re-registration can reconstruct the same peer rather than a
// nameless one: the broker's copy of both is lost with its database.
let myTty: string | null = null;
let mySummary = "";
// The claimed peer name, remembered here so re-registration and broker-database
// loss can re-claim it. Empty string means unnamed.
let myName = (process.env.CLAUDE_PEERS_NAME ?? "").trim();

// Blocking asks in flight, keyed by their correlation token. The delivery loop
// resolves a waiter instead of rendering the reply as an inbound message.
const pendingAsks = new Map<
  string,
  {
    // The peer this ask was actually delivered to. A reply is only an answer if
    // it comes from them: the token travels in a message body, so any peer that
    // learns or guesses one could otherwise resolve somebody else's ask.
    expect: string;
    resolve: (reply: { text: string; from_id: string }) => void;
  }
>();

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
- broadcast_message: Send one message to every other instance in scope, for news that concerns them all. Prefer send_message whenever one peer is the audience.
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
      "Send a message to another Claude Code instance by peer ID or peer name. The message will be pushed into their session immediately via channel notification.",
    inputSchema: {
      type: "object" as const,
      properties: {
        to_id: {
          type: "string" as const,
          description:
            'The peer ID or peer name of the target Claude Code instance (both shown by list_peers, e.g. "a1b2c3d4" or "zod")',
        },
        message: {
          type: "string" as const,
          description: "The message to send",
        },
        in_reply_to: {
          type: "string" as const,
          description:
            "When answering a blocking ask: the ask token quoted in that message (e.g. from " +
            '"[Blocking ask #a1b2c3d4 ...]"). Routes your answer to the waiting session instead ' +
            "of its normal inbox.",
        },
      },
      required: ["to_id", "message"],
    },
  },
  {
    name: "ask_peer",
    description:
      "Ask another Claude Code instance a question and WAIT for its answer, up to a timeout. " +
      "Blocking: use it when you need the answer to continue (a decision, a value, a confirmation), " +
      "and send_message when you do not. The peer sees your question as a normal message with " +
      "reply instructions embedded; if it does not answer in time you get a timeout notice and " +
      "any late answer arrives as an ordinary message.",
    inputSchema: {
      type: "object" as const,
      properties: {
        to_id: {
          type: "string" as const,
          description: "The peer ID or peer name to ask (from list_peers)",
        },
        question: {
          type: "string" as const,
          description: "The question. Make it self-contained: the peer answers with only this text as context.",
        },
        timeout_seconds: {
          type: "number" as const,
          description: "How long to wait for the answer. Default 60, min 5, max 300.",
        },
      },
      required: ["to_id", "question"],
    },
  },
  {
    name: "broadcast_message",
    description:
      "Send one message to EVERY other Claude Code instance in scope at once. You are never sent " +
      "your own broadcast. Use this only when the information genuinely concerns every session: a " +
      "shared contract or schema changed, a shared branch moved, a shared resource is down, or you " +
      "are about to do something that would disrupt others. Anything addressed to one peer, " +
      "including a reply, a question or a status answer, must use send_message instead: every " +
      "broadcast interrupts every other session, so an unnecessary one is pure noise for people " +
      "working on something else. If you are unsure who needs to know, call list_peers and " +
      "send_message the ones who do.",
    inputSchema: {
      type: "object" as const,
      properties: {
        message: {
          type: "string" as const,
          description: "The message to send to every peer in scope",
        },
        scope: {
          type: "string" as const,
          enum: ["machine", "directory", "repo"],
          description:
            'Who receives it, mirroring list_peers. "machine" (default) = every instance on this ' +
            'computer. "directory" = instances in the same working directory. "repo" = instances ' +
            "in the same git repository, including worktrees and subdirectories. Prefer the " +
            "narrowest scope that reaches the people who need to know.",
        },
      },
      required: ["message"],
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
  {
    name: "set_name",
    description:
      "Claim a human-readable peer name for this session (e.g. the name your user calls you). " +
      "Other sessions can then send_message to the name instead of the 8-char ID. Names are " +
      "unique per broker: claiming one another live peer holds fails. Also settable at launch " +
      "via the CLAUDE_PEERS_NAME environment variable.",
    inputSchema: {
      type: "object" as const,
      properties: {
        name: {
          type: "string" as const,
          description:
            "1-32 chars, letters/digits/space/dash/underscore, starting alphanumeric. Case-insensitively unique.",
        },
      },
      required: ["name"],
    },
  },
  {
    name: "whoami",
    description:
      "Report this session's own peer identity: ID, name (if any), working directory, git root, and current summary. Use it to tell a user or peer how this session can be addressed.",
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
          if (p.name) parts.push(`Name: ${p.name}`);
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
      const { to_id, message, in_reply_to } = args as {
        to_id: string;
        message: string;
        in_reply_to?: string;
      };
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
          ...(in_reply_to ? { reply_to: String(in_reply_to) } : {}),
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

    case "ask_peer": {
      const { to_id, question, timeout_seconds } = args as {
        to_id: string;
        question: string;
        timeout_seconds?: number;
      };
      if (!myId) {
        return {
          content: [{ type: "text" as const, text: "Not registered with broker yet" }],
          isError: true,
        };
      }
      const waitMs = Math.min(300, Math.max(5, Number(timeout_seconds) || 60)) * 1000;
      // Client-minted correlation token: survives broker-database loss (a row id
      // would not) and is a string end to end, so it can never poison channel meta.
      const askId = Math.random().toString(36).slice(2, 10);
      const replyAddress = myName || myId;

      // The instructions ride in the TEXT, not in meta: the receiving model reads
      // the text, and a peer running an older server still sees how to answer
      // (its plain reply then arrives after our timeout as an ordinary message).
      const framed =
        `${question}\n\n` +
        `[Blocking ask #${askId} — the sender is WAITING on this right now. Answer immediately ` +
        `with the send_message tool: to_id="${replyAddress}", in_reply_to="${askId}", and put the ` +
        `complete answer in message.]`;

      try {
        const sent = await brokerFetch<{ ok: boolean; error?: string; to_id?: string }>(
          "/send-message",
          { from_id: myId, to_id, text: framed }
        );
        if (!sent.ok) {
          return {
            content: [{ type: "text" as const, text: `Failed to ask: ${sent.error}` }],
            isError: true,
          };
        }

        const reply = await new Promise<{ text: string; from_id: string } | null>((resolve) => {
          const timer = setTimeout(() => {
            pendingAsks.delete(askId);
            resolve(null);
          }, waitMs);
          pendingAsks.set(askId, {
            // Resolved id when the broker reported one; otherwise what the caller
            // typed, which is correct whenever it was already an id.
            expect: sent.to_id ?? to_id,
            resolve: (r) => {
              clearTimeout(timer);
              resolve(r);
            },
          });
        });

        if (reply === null) {
          return {
            content: [
              {
                type: "text" as const,
                text:
                  `No answer from ${to_id} within ${waitMs / 1000}s (ask #${askId}). The question ` +
                  `was still delivered; a late answer will arrive as an ordinary message.`,
              },
            ],
          };
        }
        return {
          content: [
            {
              type: "text" as const,
              text: `Answer from ${reply.from_id} (ask #${askId}):\n\n${reply.text}`,
            },
          ],
        };
      } catch (e) {
        pendingAsks.delete(askId);
        return {
          content: [
            {
              type: "text" as const,
              text: `Error asking peer: ${e instanceof Error ? e.message : String(e)}`,
            },
          ],
          isError: true,
        };
      }
    }

    case "broadcast_message": {
      const { message, scope = "machine" } = args as {
        message: string;
        scope?: "machine" | "directory" | "repo";
      };
      if (!myId) {
        return {
          content: [{ type: "text" as const, text: "Not registered with broker yet" }],
          isError: true,
        };
      }
      try {
        const result = await brokerFetch<BroadcastMessageResponse>("/broadcast-message", {
          from_id: myId,
          text: message,
          scope,
          cwd: myCwd,
          git_root: myGitRoot,
        });
        // Reaching nobody is reported plainly rather than as a failure: being the only session in
        // scope is an ordinary state, and an error here would invite a pointless retry.
        if (result.delivered_to === 0) {
          return {
            content: [
              {
                type: "text" as const,
                text: `Broadcast delivered to 0 peer(s) (scope: ${scope}). No other instances are in scope.`,
              },
            ],
          };
        }
        return {
          content: [
            {
              type: "text" as const,
              text: `Broadcast delivered to ${result.delivered_to} peer(s) (scope: ${scope}).`,
            },
          ],
        };
      } catch (e) {
        return {
          content: [
            {
              type: "text" as const,
              text: `Error broadcasting message: ${e instanceof Error ? e.message : String(e)}`,
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
        // Remembered so a later re-registration carries it: the broker's copy
        // dies with its database.
        mySummary = summary;
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

    case "set_name": {
      const { name: wanted } = args as { name: string };
      if (!myId) {
        return {
          content: [{ type: "text" as const, text: "Not registered with broker yet" }],
          isError: true,
        };
      }
      try {
        const result = await brokerFetch<{ ok: boolean; error?: string }>("/set-name", {
          id: myId,
          name: wanted,
        });
        if (!result.ok) {
          return {
            content: [{ type: "text" as const, text: `Could not claim name: ${result.error}` }],
            isError: true,
          };
        }
        // Remembered so a later re-registration can re-claim it: the broker's
        // copy dies with its database, same as the summary.
        myName = wanted.trim();
        return {
          content: [
            {
              type: "text" as const,
              text: `Name claimed: "${myName}". Peers can now send_message to it directly.`,
            },
          ],
        };
      } catch (e) {
        return {
          content: [
            {
              type: "text" as const,
              text: `Error setting name: ${e instanceof Error ? e.message : String(e)}`,
            },
          ],
          isError: true,
        };
      }
    }

    case "whoami": {
      const parts = [
        `ID: ${myId ?? "(not registered)"}`,
        `Name: ${myName || "(none — claim one with set_name)"}`,
        `CWD: ${myCwd}`,
        `Repo: ${myGitRoot ?? "(none)"}`,
        `Summary: ${mySummary || "(none)"}`,
      ];
      return {
        content: [{ type: "text" as const, text: parts.join("\n") }],
      };
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
 * Either host: this same server is started by Claude Code and by Codex, and it cannot know which
 * from its own arguments. Resolving only `claude` here is the failure that makes the whole Codex
 * path look plausible and deliver nothing: under Codex the walk finds no claude, the poll loop
 * below bails out for want of anywhere to put a message, and the drain hook then runs on every turn
 * against a queue that is empty because nothing ever wrote to it.
 *
 * Resolved once: the parent cannot change, and re-deriving it on every cycle would run a `ps` per
 * ancestor per second for the life of the session.
 */
const sessionPid: number | null = findHostSessionPid();

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
      // A reply to a blocking ask is consumed by the waiter, never rendered as
      // a second inbound message. Routing it HERE, in the one delivery path,
      // is what makes double delivery impossible rather than merely unlikely:
      // the stream and the poll both funnel into this loop, and a message is
      // either a waiter's answer or an ordinary message, decided once.
      if (msg.reply_to) {
        const waiter = pendingAsks.get(msg.reply_to);
        if (waiter && waiter.expect === msg.from_id) {
          pendingAsks.delete(msg.reply_to);
          pushedMessageIds.add(msg.id);
          await ackMessages([msg.id]);
          waiter.resolve({ text: msg.text, from_id: msg.from_id });
          continue;
        }
        // Either no waiter (it timed out, or this session restarted), or a reply
        // quoting the token from a peer we did not ask. Both fall through and
        // deliver as an ordinary message rather than dropping it: an unexpected
        // answer is still somebody talking to this session.
      }

      // Look up the sender's info for context
      let fromSummary = "";
      let fromCwd = "";
      let fromName = "";
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
          fromName = sender.name ?? "";
        }
      } catch {
        // Non-critical, proceed without sender info
      }

      if (clientRendersChannel()) {
        // Push as channel notification — this is what makes it immediate.
        //
        // Every meta value MUST be a string: Claude Code Zod-validates channel
        // notifications, and a non-string value does not merely drop the event,
        // the handler error kills the whole stdio MCP connection (lesson
        // imported from the synaq fork, learned there 2026-07-30).
        await mcp.notification({
          method: "notifications/claude/channel",
          params: {
            content: msg.text,
            meta: {
              from_id: msg.from_id,
              from_name: fromName,
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
          from_name: fromName,
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

// --- One delivery at a time ---
//
// There are now two things that ask for a delivery: the stream, which fires when the broker takes
// a message, and the poll, which fires on a clock. Both call the SAME function, so there is one
// place that renders a message and one that acknowledges it. What there must not be is two of them
// running at once: each would poll, each would find the id absent from pushedMessageIds, and each
// would push the same message into the session, because the id is only recorded after the push and
// the push is preceded by an awaited lookup of the sender.
//
// So requests coalesce. A request arriving during a delivery does not start a second one, it marks
// that another pass is owed, and the pass runs when the current one finishes. That also keeps the
// stream honest under a burst: five frames arriving together cost one extra pass, not five.

let deliveryInFlight: Promise<void> | null = null;
let deliveryRequested = false;

async function deliverPending(): Promise<void> {
  if (deliveryInFlight) {
    deliveryRequested = true;
    return deliveryInFlight;
  }
  do {
    // Cleared before the pass, so anything requested DURING the pass is seen by the loop below.
    deliveryRequested = false;
    deliveryInFlight = pollAndPushMessages();
    try {
      await deliveryInFlight;
    } finally {
      deliveryInFlight = null;
    }
  } while (deliveryRequested);
}

// --- The push transport ---
//
// The broker holds one event stream per subscribed peer and writes a frame when a message lands in
// its mailbox. The frame carries no text: it says only that there is something to fetch, and the
// delivery that follows is the same one the poll runs. That is what makes double delivery
// impossible by construction rather than by agreement between two code paths.
//
// The poll is kept, at a longer interval while the stream is healthy. A transport that can drop
// must degrade to the previous behaviour and never to silence, and "the stream is healthy" is a
// belief this process holds about a socket, so the poll is what checks the belief.

let streamUp = false;
let streamAbort: AbortController | null = null;
let stoppingStream = false;

const streamBackoff = new PollBackoff({
  baseDelayMs: 1_000,
  maxDelayMs: POLL_BACKOFF_MAX_MS,
  quietMs: POLL_QUIET_MS,
});

function pollIntervalMs(): number {
  return streamUp ? POLL_IDLE_INTERVAL_MS : POLL_INTERVAL_MS;
}

/** Drop the current stream, if any. The loop reconnects. */
function restartStream(): void {
  streamAbort?.abort();
}

/**
 * Hold one subscription open, delivering on every frame.
 *
 * Resolves when the broker ends the stream, throws when it cannot be established. Either way the
 * caller treats it as the stream being gone.
 */
async function runStream(): Promise<void> {
  // Captured before the call: a 401 that arrives after another caller has already re-registered
  // must adopt the new identity rather than register a third one.
  const identityAtRequest = myId;
  const abort = new AbortController();
  streamAbort = abort;

  const res = await fetch(`${BROKER_URL}/subscribe?id=${encodeURIComponent(myId!)}`, {
    headers: myToken ? { Authorization: `Bearer ${myToken}` } : {},
    signal: abort.signal,
  });

  if (res.status === 401) {
    // Same meaning as a 401 on any other route: this identity is unknown to the broker.
    await recoverIdentity(identityAtRequest);
    throw new Error("subscription refused; re-registered");
  }
  if (!res.ok || !res.body) {
    // A broker predating this transport answers 404 here, and so does one with it switched off.
    // Neither is a failure of the session, only of the fast path.
    throw new Error(`subscribe returned ${res.status}`);
  }

  streamUp = true;
  // A stream that lives long enough is evidence the broker is healthy, so the next outage starts
  // its backoff from the beginning rather than from where the last one left off.
  const stable = setTimeout(() => {
    if (streamBackoff.noteSuccess(Date.now())) log("Message stream is stable again");
  }, STREAM_STABLE_MS);
  stable.unref?.();

  // A socket that dies without a FIN never ends this loop, so silence is timed rather than
  // trusted. The broker's keepalive comments are what make silence meaningful.
  let lastFrameAt = Date.now();
  const watchdog = setInterval(() => {
    if (Date.now() - lastFrameAt > STREAM_IDLE_TIMEOUT_MS) abort.abort();
  }, Math.max(1000, Math.floor(STREAM_IDLE_TIMEOUT_MS / 3)));
  watchdog.unref?.();

  try {
    // Anything that arrived while there was no stream is fetched now rather than waited for.
    await deliverPending();

    const decoder = new TextDecoder();
    let buf = "";
    for await (const chunk of res.body as AsyncIterable<Uint8Array>) {
      lastFrameAt = Date.now();
      buf += decoder.decode(chunk, { stream: true });
      let sep: number;
      while ((sep = buf.indexOf("\n\n")) >= 0) {
        const frame = buf.slice(0, sep);
        buf = buf.slice(sep + 2);
        // Keepalives are comments and carry no event, so they fall through here silently.
        if (frame.includes("event: message")) await deliverPending();
      }
    }
  } finally {
    clearTimeout(stable);
    clearInterval(watchdog);
    streamUp = false;
    if (streamAbort === abort) streamAbort = null;
  }
}

/**
 * One turn of the safety net.
 *
 * The timer keeps its old period, and the interval that actually matters is enforced here, so a
 * stream that drops is polled at the base interval on the very next tick rather than up to a
 * healthy interval later.
 */
let lastPollAt = 0;

async function pollTick(): Promise<void> {
  const now = Date.now();
  if (now - lastPollAt < pollIntervalMs()) return;
  lastPollAt = now;
  await deliverPending();
}

/** Keep a subscription up for the life of the process, backing off when it will not stay. */
async function streamLoop(): Promise<void> {
  while (!stoppingStream) {
    const now = Date.now();
    if (!streamBackoff.ready(now)) {
      await Bun.sleep(200);
      continue;
    }

    let reason = "the broker closed the stream";
    try {
      await runStream();
    } catch (e) {
      reason = e instanceof Error ? e.message : String(e);
    }
    if (stoppingStream) return;

    // Reported the first time, on a change of cause, and periodically after that. A session
    // running on the fallback is working but slower, which is worth knowing and not worth saying
    // once a second.
    if (streamBackoff.noteFailure(reason, Date.now())) {
      log(
        `Message stream unavailable (${reason}); falling back to polling every ` +
          `${POLL_INTERVAL_MS}ms, retrying the stream in ${streamBackoff.delayMs}ms`
      );
    }
    // The stream is down, so the poll is the transport again: run one now rather than leaving a
    // message sitting until the next tick.
    await deliverPending();
  }
}

// --- Startup ---

async function main() {
  // 1. Ensure broker is running
  await ensureBroker();

  // 2. Gather context
  myCwd = process.cwd();
  myGitRoot = await getGitRoot(myCwd);
  myTty = getTty();

  log(`CWD: ${myCwd}`);
  log(`Git root: ${myGitRoot ?? "(none)"}`);
  log(`TTY: ${myTty ?? "(unknown)"}`);

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
        mySummary = summary;
        log(`Auto-summary: ${summary}`);
      }
    } catch (e) {
      log(`Auto-summary failed (non-critical): ${e instanceof Error ? e.message : String(e)}`);
    }
  })();

  // Deliberately NOT awaited.
  //
  // This used to race the summary against a 3000ms timer here, before the transport was connected,
  // and it cost every session most of its startup: measured on dev at a median 2474ms to the
  // initialize reply, of which 2001ms was this one network call. The wait bought nothing, because
  // the late-apply below has always existed and covers the same ground. Registering first and
  // filling the summary in afterwards takes the same session to 72ms.
  //
  // The peer is therefore visible to others for a second or two carrying no summary. That is the
  // trade, and it is the cheap side of it.

  // 4. Register with broker
  const reg = await registerWithBroker();
  myId = reg.id;
  myToken = reg.token;
  log(`Registered as peer ${myId}`);

  // CLAUDE_PEERS_NAME: claim at startup, non-blocking and non-fatal. A "taken"
  // verdict logs and runs unnamed rather than failing the session over a label.
  if (myName) {
    applyName().catch((e) =>
      log(`Peer name claim failed (non-critical): ${e instanceof Error ? e.message : String(e)}`)
    );
  }

  // Apply the summary whenever it lands. Unconditional now: nothing above waits for it, so it is
  // never already present at this point.
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
  const pollTimer = setInterval(pollTick, POLL_INTERVAL_MS);

  // 6b. Subscribe to the broker's push transport.
  //
  // Not awaited: a broker that cannot serve it must cost the session nothing but latency, and the
  // loop above is already a complete delivery path on its own.
  if (STREAM_ENABLED) {
    streamLoop().catch((e) => log(`Message stream loop ended: ${e instanceof Error ? e.message : String(e)}`));
  } else {
    log("Message stream disabled; polling only");
  }

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
    stoppingStream = true;
    restartStream();
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
