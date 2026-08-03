#!/usr/bin/env bun
/**
 * claude-peers broker daemon
 *
 * A singleton HTTP server on localhost:7899 backed by SQLite.
 * Tracks all registered Claude Code peers and routes messages between them.
 *
 * Auto-launched by the MCP server if not already running.
 * Run directly: bun broker.ts
 */

import { Database } from "bun:sqlite";
import { chmodSync } from "node:fs";
import { randomBytes, timingSafeEqual } from "node:crypto";
import type {
  RegisterRequest,
  RegisterResponse,
  HeartbeatRequest,
  SetSummaryRequest,
  ListPeersRequest,
  SendMessageRequest,
  PollMessagesRequest,
  PollMessagesResponse,
  Peer,
  Message,
  AckMessagesRequest,
  AckMessagesResponse,
} from "./shared/types.ts";

const PORT = parseInt(process.env.CLAUDE_PEERS_PORT ?? "7899", 10);
const DB_PATH = process.env.CLAUDE_PEERS_DB ?? `${process.env.HOME}/.claude-peers.db`;
const MSG_TTL_MS = parseInt(process.env.CLAUDE_PEERS_MSG_TTL ?? "3600000", 10);

// --- Database setup ---

const db = new Database(DB_PATH);
db.run("PRAGMA journal_mode = WAL");
db.run("PRAGMA busy_timeout = 3000");
// Zero freed pages rather than leaving message text recoverable in the freelist.
db.run("PRAGMA secure_delete = ON");

// The database holds every inter-agent message. Default file creation is 0644,
// which leaves it readable by any account on the machine.
for (const suffix of ["", "-wal", "-shm"]) {
  try {
    chmodSync(`${DB_PATH}${suffix}`, 0o600);
  } catch {
    // -wal/-shm may not exist yet; they are re-chmodded on the next boot.
  }
}

db.run(`
  CREATE TABLE IF NOT EXISTS peers (
    id TEXT PRIMARY KEY,
    pid INTEGER NOT NULL,
    cwd TEXT NOT NULL,
    git_root TEXT,
    tty TEXT,
    summary TEXT NOT NULL DEFAULT '',
    registered_at TEXT NOT NULL,
    last_seen TEXT NOT NULL,
    token TEXT
  )
`);

db.run(`
  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_id TEXT NOT NULL,
    to_id TEXT NOT NULL,
    text TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (from_id) REFERENCES peers(id),
    FOREIGN KEY (to_id) REFERENCES peers(id)
  )
`);

// Add columns an older database predates. Every column the insert below writes
// must appear here, or the broker crashes at boot against a legacy file.
{
  const existing = new Set(
    (db.query("PRAGMA table_info(peers)").all() as { name: string }[]).map((c) => c.name)
  );
  if (!existing.has("token")) db.run("ALTER TABLE peers ADD COLUMN token TEXT");
}

// Clean up stale peers (PIDs that no longer exist) on startup
function cleanStalePeers() {
  const peers = db.query("SELECT id, pid FROM peers").all() as { id: string; pid: number }[];
  for (const peer of peers) {
    try {
      // Check if process is still alive (signal 0 doesn't kill, just checks)
      process.kill(peer.pid, 0);
    } catch {
      // Process doesn't exist, remove it
      db.run("DELETE FROM peers WHERE id = ?", [peer.id]);
      db.run("DELETE FROM messages WHERE to_id = ?", [peer.id]);
    }
  }
}

cleanStalePeers();

// Periodically clean stale peers (every 30s)
setInterval(cleanStalePeers, 30_000);


// --- Prepared statements ---

const insertPeer = db.prepare(`
  INSERT INTO peers (id, pid, cwd, git_root, tty, summary, registered_at, last_seen, token)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const selectToken = db.prepare(`
  SELECT token FROM peers WHERE id = ?
`);

const updateLastSeen = db.prepare(`
  UPDATE peers SET last_seen = ? WHERE id = ?
`);

const updateSummary = db.prepare(`
  UPDATE peers SET summary = ? WHERE id = ?
`);

const deletePeer = db.prepare(`
  DELETE FROM peers WHERE id = ?
`);

const selectAllPeers = db.prepare(`
  SELECT * FROM peers
`);

const selectPeersByDirectory = db.prepare(`
  SELECT * FROM peers WHERE cwd = ?
`);

const selectPeersByGitRoot = db.prepare(`
  SELECT * FROM peers WHERE git_root = ?
`);

const insertMessage = db.prepare(`
  INSERT INTO messages (from_id, to_id, text, sent_at, delivered)
  VALUES (?, ?, ?, ?, 0)
`);

const selectUndelivered = db.prepare(`
  SELECT * FROM messages WHERE to_id = ? AND delivered = 0 ORDER BY sent_at ASC
`);

// Acknowledgement deletes rather than flagging: a delivered message has no
// further use, and retaining it leaves plaintext in the file indefinitely.
//
// The to_id predicate keeps a cooperating client from acking mail that is not
// its own. It is NOT a security boundary: peer_id is supplied by the caller and
// peer ids are public via /list-peers, so any local process can delete another
// peer's queued mail. Closing that needs request authentication, which the
// broker does not yet have. See docs/specs/RECOVERY.md, prerequisite 1.
const ackMessage = db.prepare(`
  DELETE FROM messages WHERE id = ? AND to_id = ?
`);

// Undelivered mail is swept once its TTL expires, otherwise rows now accumulate
// forever: polling no longer consumes them.
const sweepExpired = db.prepare(`
  DELETE FROM messages WHERE sent_at < ?
`);

sweepExpiredMessages();
setInterval(sweepExpiredMessages, 60_000);

// --- Generate peer ID ---

function generateId(): string {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let id = "";
  for (let i = 0; i < 8; i++) {
    id += chars[Math.floor(Math.random() * chars.length)];
  }
  return id;
}

// --- Request handlers ---

function handleRegister(body: RegisterRequest): RegisterResponse {
  const id = generateId();
  const now = new Date().toISOString();

  // Remove any existing registration for this PID (re-registration)
  const existing = db.query("SELECT id FROM peers WHERE pid = ?").get(body.pid) as { id: string } | null;
  if (existing) {
    deletePeer.run(existing.id);
  }

  const token = randomBytes(32).toString("hex");
  insertPeer.run(id, body.pid, body.cwd, body.git_root, body.tty, body.summary, now, now, token);
  return { id, token };
}

function handleHeartbeat(body: HeartbeatRequest): void {
  updateLastSeen.run(new Date().toISOString(), body.id);
}

function handleSetSummary(body: SetSummaryRequest): void {
  updateSummary.run(body.summary, body.id);
}

function handleListPeers(body: ListPeersRequest): Peer[] {
  let peers: Peer[];

  switch (body.scope) {
    case "machine":
      peers = selectAllPeers.all() as Peer[];
      break;
    case "directory":
      peers = selectPeersByDirectory.all(body.cwd) as Peer[];
      break;
    case "repo":
      if (body.git_root) {
        peers = selectPeersByGitRoot.all(body.git_root) as Peer[];
      } else {
        // No git root, fall back to directory
        peers = selectPeersByDirectory.all(body.cwd) as Peer[];
      }
      break;
    default:
      peers = selectAllPeers.all() as Peer[];
  }

  // Exclude the requesting peer
  if (body.exclude_id) {
    peers = peers.filter((p) => p.id !== body.exclude_id);
  }

  // Verify each peer's process is still alive
  return peers.filter((p) => {
    try {
      process.kill(p.pid, 0);
      return true;
    } catch {
      // Clean up dead peer
      deletePeer.run(p.id);
      return false;
    }
  });
}

function handleSendMessage(body: SendMessageRequest): { ok: boolean; error?: string } {
  // Verify target exists
  const target = db.query("SELECT id FROM peers WHERE id = ?").get(body.to_id) as { id: string } | null;
  if (!target) {
    return { ok: false, error: `Peer ${body.to_id} not found` };
  }

  insertMessage.run(body.from_id, body.to_id, body.text, new Date().toISOString());
  return { ok: true };
}

function handlePollMessages(body: PollMessagesRequest): PollMessagesResponse {
  // Polling deliberately does NOT consume. Upstream marked messages delivered
  // here, so a message was lost outright whenever the client polled but failed
  // to render it. The client now acks once it has actually shown the message.
  const messages = selectUndelivered.all(body.id) as Message[];
  return { messages };
}

function handleAckMessages(body: AckMessagesRequest): AckMessagesResponse {
  let acked = 0;
  for (const id of body.message_ids) {
    acked += ackMessage.run(id, body.peer_id).changes;
  }
  return { ok: true, acked };
}

function sweepExpiredMessages(): void {
  const cutoff = new Date(Date.now() - MSG_TTL_MS).toISOString();
  sweepExpired.run(cutoff);
}

function handleUnregister(body: { id: string }): void {
  deletePeer.run(body.id);
}

// --- Authentication ---

/**
 * Which body field names the calling peer, per route. The handler then trusts
 * the authenticated identity rather than the body, so a caller cannot act as
 * another peer by naming it.
 */
const CALLER_FIELD: Record<string, string> = {
  "/heartbeat": "id",
  "/set-summary": "id",
  "/list-peers": "exclude_id",
  "/send-message": "from_id",
  "/poll-messages": "id",
  "/ack-messages": "peer_id",
  "/unregister": "id",
};

/**
 * Verify the bearer token against the peer the body claims to be.
 *
 * Peer ids are public via /list-peers, so the id alone proves nothing. The
 * token is 256 bits minted at registration and never leaves the owning process.
 */
function isAuthorized(req: Request, path: string, body: Record<string, unknown>): boolean {
  const field = CALLER_FIELD[path];
  if (!field) return true;

  const claimed = body[field];
  // /list-peers may omit exclude_id; an anonymous read is still a read of every
  // session's cwd and summary, so it is refused rather than allowed through.
  if (typeof claimed !== "string" || claimed.length === 0) return false;

  const row = selectToken.get(claimed) as { token: string | null } | null;
  if (!row?.token) return false;

  const presented = (req.headers.get("authorization") ?? "").replace(/^Bearer /, "");
  const a = Buffer.from(presented);
  const b = Buffer.from(row.token);
  // timingSafeEqual throws on a length mismatch, which is itself the answer.
  return a.length === b.length && timingSafeEqual(a, b);
}

// --- HTTP Server ---

Bun.serve({
  port: PORT,
  hostname: "127.0.0.1",
  async fetch(req) {
    const url = new URL(req.url);
    const path = url.pathname;

    if (req.method !== "POST") {
      if (path === "/health") {
        return Response.json({ status: "ok", peers: (selectAllPeers.all() as Peer[]).length });
      }
      return new Response("claude-peers broker", { status: 200 });
    }

    try {
      const body = await req.json();

      if (!isAuthorized(req, path, body as Record<string, unknown>)) {
        return Response.json({ error: "unauthorized" }, { status: 401 });
      }

      switch (path) {
        case "/register":
          return Response.json(handleRegister(body as RegisterRequest));
        case "/heartbeat":
          handleHeartbeat(body as HeartbeatRequest);
          return Response.json({ ok: true });
        case "/set-summary":
          handleSetSummary(body as SetSummaryRequest);
          return Response.json({ ok: true });
        case "/list-peers":
          return Response.json(handleListPeers(body as ListPeersRequest));
        case "/send-message":
          return Response.json(handleSendMessage(body as SendMessageRequest));
        case "/poll-messages":
          return Response.json(handlePollMessages(body as PollMessagesRequest));
        case "/ack-messages":
          return Response.json(handleAckMessages(body as AckMessagesRequest));
        case "/unregister":
          handleUnregister(body as { id: string });
          return Response.json({ ok: true });
        default:
          return Response.json({ error: "not found" }, { status: 404 });
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      return Response.json({ error: msg }, { status: 500 });
    }
  },
});

console.error(`[claude-peers broker] listening on 127.0.0.1:${PORT} (db: ${DB_PATH})`);
