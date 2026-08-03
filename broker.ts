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
import { chmodSync, statSync } from "node:fs";
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
// How long after a delete the write-ahead log is checkpointed away. See
// scheduleCheckpoint below for why this is a debounce rather than immediate.
const CHECKPOINT_MS = parseInt(process.env.CLAUDE_PEERS_CHECKPOINT_MS ?? "1000", 10);

// --- Database setup ---

function openDatabase(): Database {
  const handle = new Database(DB_PATH);
  // busy_timeout first: journal_mode = WAL is the statement that contends when
  // several brokers race on a cold database, and without a retry window all of
  // them die with SQLITE_BUSY, leaving no broker at all.
  handle.run("PRAGMA busy_timeout = 3000");
  handle.run("PRAGMA journal_mode = WAL");
  // Zero freed pages rather than leaving message text recoverable in the freelist.
  handle.run("PRAGMA secure_delete = ON");

  // The database holds every inter-agent message. Default file creation is 0644,
  // which leaves it readable by any account on the machine.
  for (const suffix of ["", "-wal", "-shm"]) {
    try {
      chmodSync(`${DB_PATH}${suffix}`, 0o600);
    } catch {
      // -wal/-shm may not exist yet; they are re-chmodded on the next boot.
    }
  }

  handle.run(`
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

  handle.run(`
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
  const existing = new Set(
    (handle.query("PRAGMA table_info(peers)").all() as { name: string }[]).map((c) => c.name)
  );
  if (!existing.has("token")) handle.run("ALTER TABLE peers ADD COLUMN token TEXT");

  return handle;
}

let db = openDatabase();

// --- Surviving the loss of the database file ---
//
// The handle is opened once and follows the inode, not the path. Delete
// ~/.claude-peers.db underneath a running broker and every statement fails with
// SQLITE_IOERR_VNODE forever: /health, /register, everything, for the life of
// the process, and the file is never recreated. That is a Disposability failure
// (https://12factor.net/disposability): the broker cannot survive losing its
// backing store.
//
// RECREATE IN PLACE RATHER THAN EXIT. Exiting to be respawned would be the
// usual answer, but nothing supervises this daemon. It is spawned detached by
// whichever MCP server found it missing at ITS startup (server.ts ensureBroker),
// so an exit leaves no broker at all until somebody opens a new Claude Code
// session, and every running session loses peering in the meantime. Recreating
// costs one file open and two CREATE TABLE statements, and leaves the port
// held throughout, so live sessions see one refused call rather than a dead
// socket. What is NOT recovered is the data: peers and queued messages are gone
// with the file, so sessions re-register (server.ts) and messages queued for the
// old peer ids are lost. That is inherent to the file being deleted.

/** Identity of the file the handle should be pointing at, or null when gone. */
function databaseFileId(): string | null {
  if (DB_PATH === ":memory:" || DB_PATH === "") return "memory";
  try {
    const s = statSync(DB_PATH);
    return `${s.dev}:${s.ino}`;
  } catch {
    return null;
  }
}

let databaseId = databaseFileId();

/** Errors that mean the handle no longer has a usable database behind it. */
function isDatabaseLost(e: unknown): boolean {
  const code = String((e as { code?: string })?.code ?? "");
  if (/^SQLITE_(IOERR|CORRUPT|NOTADB|READONLY|CANTOPEN)/.test(code)) return true;
  const msg = e instanceof Error ? e.message : String(e);
  return /disk i\/o error|no such table|not a database|disk image is malformed|readonly database|unable to open database/i.test(
    msg
  );
}

function reopenDatabase(reason: string): void {
  try {
    db.close(false);
  } catch {
    // A handle onto a deleted file often refuses to close cleanly. Dropping the
    // reference is what matters.
  }
  db = openDatabase();
  databaseId = databaseFileId();
  stmts = prepareStatements();
  console.error(
    `[claude-peers broker] database ${reason}; schema recreated at ${DB_PATH}. ` +
      `Registered peers and queued messages are gone; sessions re-register on their next call.`
  );
}

/** Reopen before serving if the file on disk is no longer the one we hold. */
function ensureDatabase(): void {
  const current = databaseFileId();
  if (current !== null && current === databaseId) return;
  reopenDatabase(current === null ? "file disappeared" : "file was replaced");
}

/**
 * Run a database-touching operation, recovering once if the store is gone.
 *
 * The proactive check above catches deletion and replacement; this catches the
 * rest, such as a file truncated in place, where the inode is unchanged but the
 * content is no longer a database.
 */
function withDatabase<T>(fn: () => T): T {
  try {
    return fn();
  } catch (e) {
    if (!isDatabaseLost(e)) throw e;
    reopenDatabase("reported an I/O failure");
    return fn();
  }
}

// --- Retention: getting deleted text off the disk ---

/**
 * Checkpoint the write-ahead log so a delete is actually a delete.
 *
 * secure_delete zeroes freed pages in the MAIN database file, but in WAL mode
 * the page images written when the message was inserted already sit in the
 * -wal, where nothing rewrites them. The broker is killed rather than closed,
 * so no implicit checkpoint ever runs and the plaintext of every acknowledged
 * message stays readable in that file for the life of the database. Verified
 * before this fix: the canary was absent from the .db and present in the -wal,
 * and survived SIGTERM in a 45 KB file.
 *
 * TRUNCATE rather than PASSIVE or FULL: PASSIVE and FULL copy the frames back
 * into the database but leave the -wal file at its high water mark, with the
 * stale frames still legible in it. TRUNCATE takes the file to zero length,
 * which is the only variant that removes the bytes.
 *
 * Alternatives weighed:
 *   - journal_mode = DELETE or TRUNCATE. This trades away WAL's concurrent
 *     readers for a rollback journal that carries the same original page
 *     images, deleted rather than zeroed at commit. It costs concurrency
 *     without actually being safer.
 *   - Checkpointing inside every delete. Correct but wasteful: an ack of ten
 *     messages would pay ten checkpoints, and the sweep would pay one per
 *     expired row, all for the same handful of pages.
 *   - Checkpointing only at a clean shutdown. Worthless here, because the
 *     broker is normally killed, which is exactly how the defect survived.
 * So: debounce. A delete schedules a checkpoint CHECKPOINT_MS later, and a
 * burst of deletes collapses into one. The contract is that deleted text is
 * gone from every file on disk within CHECKPOINT_MS of the last delete, one
 * second by default.
 */
function checkpointNow(): void {
  try {
    db.run("PRAGMA wal_checkpoint(TRUNCATE)");
  } catch {
    // A reader can hold the checkpoint off. The next delete reschedules it, and
    // the shutdown path retries, so a missed cycle is not a leak.
  }
}

let checkpointTimer: ReturnType<typeof setTimeout> | null = null;

function scheduleCheckpoint(): void {
  if (checkpointTimer) return;
  checkpointTimer = setTimeout(() => {
    checkpointTimer = null;
    checkpointNow();
  }, CHECKPOINT_MS);
  // Never hold the process open for a checkpoint; the shutdown path runs one.
  checkpointTimer.unref?.();
}

/** Called by every path that deletes rows carrying message text. */
function noteDeletion(changes: number): void {
  if (changes > 0) scheduleCheckpoint();
}

/**
 * Whether a pid belongs to a running process.
 *
 * EPERM means the process exists but is owned by another user, which is the
 * opposite of dead. Treating it as dead deletes a live peer and its mail.
 */
function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (e) {
    return (e as NodeJS.ErrnoException)?.code === "EPERM";
  }
}

// Clean up stale peers (PIDs that no longer exist) on startup
function cleanStalePeers() {
  const peers = db.query("SELECT id, pid FROM peers").all() as { id: string; pid: number }[];
  for (const peer of peers) {
    if (!isProcessAlive(peer.pid)) {
      db.run("DELETE FROM peers WHERE id = ?", [peer.id]);
      db.run("DELETE FROM messages WHERE to_id = ?", [peer.id]);
      // Their undelivered mail is now freed pages; get it off the disk.
      noteDeletion(1);
    }
  }
}

/**
 * Maintenance that must never take the daemon down.
 *
 * An exception thrown out of a timer callback is uncaught and kills the
 * process, so a database that went missing between ticks would end the broker
 * rather than be recovered from.
 */
function periodic(fn: () => void): () => void {
  return () => {
    try {
      ensureDatabase();
      withDatabase(fn);
    } catch (e) {
      console.error(
        `[claude-peers broker] maintenance skipped: ${e instanceof Error ? e.message : String(e)}`
      );
    }
  };
}

// --- Prepared statements ---
//
// Rebuilt whenever the database is reopened: a statement is bound to the handle
// that prepared it, so a stale one keeps failing against the dead file.

function prepareStatements() {
  return {
    insertPeer: db.prepare(`
      INSERT INTO peers (id, pid, cwd, git_root, tty, summary, registered_at, last_seen, token)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `),

    selectToken: db.prepare(`
      SELECT token FROM peers WHERE id = ?
    `),

    updateLastSeen: db.prepare(`
      UPDATE peers SET last_seen = ? WHERE id = ?
    `),

    updateSummary: db.prepare(`
      UPDATE peers SET summary = ? WHERE id = ?
    `),

    deletePeer: db.prepare(`
      DELETE FROM peers WHERE id = ?
    `),

    // Never SELECT * here: the row carries the auth token, and these results are
    // serialised straight to any caller. Project the public columns only.
    selectAllPeers: db.prepare(`
      SELECT id, pid, cwd, git_root, tty, summary, registered_at, last_seen FROM peers
    `),

    selectPeersByDirectory: db.prepare(`
      SELECT id, pid, cwd, git_root, tty, summary, registered_at, last_seen FROM peers WHERE cwd = ?
    `),

    selectPeersByGitRoot: db.prepare(`
      SELECT id, pid, cwd, git_root, tty, summary, registered_at, last_seen FROM peers WHERE git_root = ?
    `),

    insertMessage: db.prepare(`
      INSERT INTO messages (from_id, to_id, text, sent_at, delivered)
      VALUES (?, ?, ?, ?, 0)
    `),

    selectUndelivered: db.prepare(`
      SELECT * FROM messages WHERE to_id = ? AND delivered = 0 ORDER BY sent_at ASC
    `),

    // Acknowledgement deletes rather than flagging: a delivered message has no
    // further use, and retaining it leaves plaintext in the file indefinitely.
    //
    // The to_id predicate scopes the delete to the caller's own mailbox. It is
    // backed by the bearer check in isAuthorized below, which resolves the caller
    // from the token rather than from the body.
    ackMessage: db.prepare(`
      DELETE FROM messages WHERE id = ? AND to_id = ?
    `),

    // Undelivered mail is swept once its TTL expires, otherwise rows now accumulate
    // forever: polling no longer consumes them.
    sweepExpired: db.prepare(`
      DELETE FROM messages WHERE sent_at < ?
    `),
  };
}

let stmts = prepareStatements();

periodic(cleanStalePeers)();
// Periodically clean stale peers (every 30s)
setInterval(periodic(cleanStalePeers), 30_000);

periodic(sweepExpiredMessages)();
setInterval(periodic(sweepExpiredMessages), 60_000);

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

  // Reclaim a row whose process is gone. Registration is unauthenticated and
  // pid is caller-supplied, so evicting a LIVE peer here would let any local
  // process kill every session's peering with one request.
  const existing = db.query("SELECT id, pid FROM peers WHERE pid = ?").get(body.pid) as
    | { id: string; pid: number }
    | null;
  if (existing && !isProcessAlive(existing.pid)) {
    stmts.deletePeer.run(existing.id);
  }

  const token = randomBytes(32).toString("hex");
  stmts.insertPeer.run(id, body.pid, body.cwd, body.git_root, body.tty, body.summary, now, now, token);
  return { id, token };
}

function handleHeartbeat(body: HeartbeatRequest): void {
  stmts.updateLastSeen.run(new Date().toISOString(), body.id);
}

function handleSetSummary(body: SetSummaryRequest): void {
  stmts.updateSummary.run(body.summary, body.id);
}

function handleListPeers(body: ListPeersRequest): Peer[] {
  let peers: Peer[];

  switch (body.scope) {
    case "machine":
      peers = stmts.selectAllPeers.all() as Peer[];
      break;
    case "directory":
      peers = stmts.selectPeersByDirectory.all(body.cwd) as Peer[];
      break;
    case "repo":
      if (body.git_root) {
        peers = stmts.selectPeersByGitRoot.all(body.git_root) as Peer[];
      } else {
        // No git root, fall back to directory
        peers = stmts.selectPeersByDirectory.all(body.cwd) as Peer[];
      }
      break;
    default:
      peers = stmts.selectAllPeers.all() as Peer[];
  }

  // Exclude the requesting peer
  if (body.exclude_id) {
    peers = peers.filter((p) => p.id !== body.exclude_id);
  }

  // Verify each peer's process is still alive
  return peers.filter((p) => {
    if (isProcessAlive(p.pid)) return true;
    stmts.deletePeer.run(p.id);
    return false;
  });
}

function handleSendMessage(body: SendMessageRequest): { ok: boolean; error?: string } {
  // Verify target exists
  const target = db.query("SELECT id FROM peers WHERE id = ?").get(body.to_id) as { id: string } | null;
  if (!target) {
    return { ok: false, error: `Peer ${body.to_id} not found` };
  }

  stmts.insertMessage.run(body.from_id, body.to_id, body.text, new Date().toISOString());
  return { ok: true };
}

function handlePollMessages(body: PollMessagesRequest): PollMessagesResponse {
  // Polling deliberately does NOT consume. Upstream marked messages delivered
  // here, so a message was lost outright whenever the client polled but failed
  // to render it. The client now acks once it has actually shown the message.
  const messages = stmts.selectUndelivered.all(body.id) as Message[];
  return { messages };
}

function handleAckMessages(body: AckMessagesRequest): AckMessagesResponse {
  let acked = 0;
  for (const id of body.message_ids) {
    acked += stmts.ackMessage.run(id, body.peer_id).changes;
  }
  // A burst of acks collapses into one checkpoint rather than one each.
  noteDeletion(acked);
  return { ok: true, acked };
}

function sweepExpiredMessages(): void {
  const cutoff = new Date(Date.now() - MSG_TTL_MS).toISOString();
  // Expired mail was never acknowledged, so this is the only thing that takes
  // it off the disk.
  noteDeletion(stmts.sweepExpired.run(cutoff).changes);
}

function handleUnregister(body: { id: string }): void {
  noteDeletion(stmts.deletePeer.run(body.id).changes);
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

  const row = stmts.selectToken.get(claimed) as { token: string | null } | null;
  if (!row?.token) return false;

  const presented = (req.headers.get("authorization") ?? "").replace(/^Bearer /, "");
  const a = Buffer.from(presented);
  const b = Buffer.from(row.token);
  // timingSafeEqual throws on a length mismatch, which is itself the answer.
  return a.length === b.length && timingSafeEqual(a, b);
}

// --- HTTP Server ---

/**
 * Authorise and route one request.
 *
 * Separated from fetch so the whole of it, the token lookup included, can be
 * retried on a fresh handle when the database turns out to be gone.
 */
function dispatch(req: Request, path: string, body: unknown): Response {
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
}

Bun.serve({
  port: PORT,
  hostname: "127.0.0.1",
  async fetch(req) {
    const url = new URL(req.url);
    const path = url.pathname;

    // Before anything reads or writes: if the file was deleted or replaced
    // since the last request, this request runs against a rebuilt schema
    // instead of a dead handle.
    ensureDatabase();

    if (req.method !== "POST") {
      if (path === "/health") {
        try {
          const peers = withDatabase(() => (stmts.selectAllPeers.all() as Peer[]).length);
          return Response.json({ status: "ok", peers });
        } catch (e) {
          const msg = e instanceof Error ? e.message : String(e);
          return Response.json({ status: "error", error: msg }, { status: 500 });
        }
      }
      return new Response("claude-peers broker", { status: 200 });
    }

    try {
      const body = await req.json();
      return withDatabase(() => dispatch(req, path, body));
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      return Response.json({ error: msg }, { status: 500 });
    }
  },
});

// --- Shutdown ---

/**
 * Close the database rather than letting the process be torn down around it.
 *
 * The broker is normally killed, not stopped, so this is the common path and
 * not an edge case. A pending debounced checkpoint is run first: a signal that
 * arrives inside the debounce window must not be the thing that leaves the
 * deleted text on disk. Closing then removes the -wal and -shm entirely.
 */
let shuttingDown = false;

function shutdown(): void {
  if (shuttingDown) return;
  shuttingDown = true;
  if (checkpointTimer) {
    clearTimeout(checkpointTimer);
    checkpointTimer = null;
  }
  checkpointNow();
  try {
    db.close();
  } catch {
    // Already closed, or closed under us. Nothing left to protect.
  }
  process.exit(0);
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
process.on("SIGHUP", shutdown);

console.error(`[claude-peers broker] listening on 127.0.0.1:${PORT} (db: ${DB_PATH})`);
