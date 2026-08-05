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
  SetNameRequest,
  SetNameResponse,
  ListPeersRequest,
  SendMessageRequest,
  BroadcastMessageRequest,
  BroadcastMessageResponse,
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
// The push transport. Off makes /subscribe a 404, which is exactly what a client sees when it
// talks to a broker predating this, so the degraded path is reachable without an old binary.
const SSE_ENABLED = (process.env.CLAUDE_PEERS_SSE ?? "on") !== "off";
// Comment frames down an idle stream. Nothing on loopback needs them to stay alive, but they are
// what turns a half-open connection into a write error, which is how a subscriber that has gone
// away without a FIN gets reaped.
const SSE_KEEPALIVE_MS = parseInt(process.env.CLAUDE_PEERS_SSE_KEEPALIVE_MS ?? "25000", 10);

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
      reply_to TEXT,
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
  if (!existing.has("name")) handle.run("ALTER TABLE peers ADD COLUMN name TEXT");

  const msgCols = new Set(
    (handle.query("PRAGMA table_info(messages)").all() as { name: string }[]).map((c) => c.name)
  );
  if (!msgCols.has("reply_to")) handle.run("ALTER TABLE messages ADD COLUMN reply_to TEXT");

  createNameUniqueIndex(handle);

  return handle;
}

/**
 * Enforce name uniqueness in the SCHEMA, not just in handleSetName.
 *
 * handleSetName reads then writes, which is only atomic because Bun.serve runs
 * one JS thread and bun:sqlite is synchronous. That is a property of today's
 * runtime, not a declared invariant: add `reusePort`, a worker, or a single
 * `await` between the check and the write, and two callers can both pass the
 * check and both claim the name. Since a name is a routable address, the
 * consequence is not a cosmetic duplicate, it is a message delivered to an
 * arbitrary one of two peers.
 *
 * PARTIAL index (`WHERE name IS NOT NULL`) because unnamed peers are the common
 * case and SQLite would otherwise treat every NULL as distinct anyway; being
 * explicit says the intent. COLLATE NOCASE so it agrees with the lookup, which
 * is also case-insensitive: an index with different collation than the query
 * would be silently unused AND would not enforce the rule anybody relies on.
 */
function createNameUniqueIndex(handle: Database): void {
  const create = () =>
    handle.run(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_peers_name_unique
         ON peers(name COLLATE NOCASE) WHERE name IS NOT NULL`
    );
  try {
    create();
  } catch {
    // A database written before this index existed can already hold duplicates.
    // Refusing to boot over old data would be worse than the duplicates are, so
    // resolve them: the earliest registration keeps the name and the rest are
    // un-named. They are still reachable by id, and a live session re-claims its
    // name on its next set_name.
    handle.run(`
      UPDATE peers SET name = NULL
      WHERE name IS NOT NULL
        AND id NOT IN (
          SELECT id FROM peers p2
          WHERE p2.name IS NOT NULL
            AND p2.registered_at = (
              SELECT MIN(p3.registered_at) FROM peers p3
              WHERE p3.name = p2.name COLLATE NOCASE
            )
          GROUP BY p2.name COLLATE NOCASE
        )
    `);
    create();
    console.error(
      "[claude-peers broker] duplicate peer names found in an existing database; " +
        "kept the earliest registration for each and cleared the rest. Affected sessions " +
        "re-claim their name on their next set_name."
    );
  }
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

// --- Push transport: the subscriber registry ---
//
// Each registered peer may hold one server-sent-events connection, and an insert into its mailbox
// wakes it. That replaces the client asking every second whether anything has happened, which was
// the whole of the broker-to-server hop: about 86,400 requests a session a day, nearly all of them
// answering "nothing".
//
// The frame carries no message text, only the fact that there is mail. The client then runs the
// delivery it already had, so there is exactly one path that renders a message and one that
// acknowledges it, and the stream cannot deliver something the poll would deliver again. It also
// keeps message text out of a transport with no acknowledgement: a frame written into a socket
// nobody reads is lost, and losing a wake-up costs a second of latency, while losing the only copy
// of a message costs the message.

interface Subscriber {
  peerId: string;
  controller: ReadableStreamDefaultController<Uint8Array>;
  closed: boolean;
}

const subscribers = new Map<string, Set<Subscriber>>();
const encoder = new TextEncoder();

function subscriberCount(): number {
  let n = 0;
  for (const set of subscribers.values()) n += set.size;
  return n;
}

function addSubscriber(sub: Subscriber): void {
  let set = subscribers.get(sub.peerId);
  if (!set) {
    set = new Set();
    subscribers.set(sub.peerId, set);
  }
  set.add(sub);
}

/**
 * Forget a subscriber and close its stream.
 *
 * Idempotent by design: a disconnect can be observed twice, once by the request's abort signal and
 * once by the stream being cancelled, and the second observation must be free rather than an
 * error. The empty set is deleted rather than left behind, so a broker that has served thousands
 * of short sessions does not accumulate a key per peer id it has ever seen.
 */
function removeSubscriber(sub: Subscriber): void {
  if (sub.closed) return;
  sub.closed = true;
  const set = subscribers.get(sub.peerId);
  if (set) {
    set.delete(sub);
    if (set.size === 0) subscribers.delete(sub.peerId);
  }
  try {
    sub.controller.close();
  } catch {
    // Already closed by the runtime when the socket went away.
  }
}

/** Write one frame, dropping the subscriber if the socket no longer accepts it. */
function send(sub: Subscriber, frame: string): void {
  if (sub.closed) return;
  try {
    sub.controller.enqueue(encoder.encode(frame));
  } catch {
    removeSubscriber(sub);
  }
}

/**
 * Tell a peer that its mailbox changed.
 *
 * Called from the statement wrapper below rather than from a route handler, so every insert
 * notifies, including ones made by routes that do not exist yet.
 */
function notifyMailbox(peerId: string): void {
  const set = subscribers.get(peerId);
  if (!set) return;
  for (const sub of [...set]) send(sub, "event: message\ndata: {}\n\n");
}

/** Close every stream belonging to a peer that no longer exists. */
function closeSubscribers(peerId: string): void {
  const set = subscribers.get(peerId);
  if (!set) return;
  for (const sub of [...set]) removeSubscriber(sub);
}

if (SSE_ENABLED && SSE_KEEPALIVE_MS > 0) {
  const timer = setInterval(() => {
    for (const set of [...subscribers.values()]) {
      for (const sub of [...set]) send(sub, ": keepalive\n\n");
    }
  }, SSE_KEEPALIVE_MS);
  // A keepalive is never a reason to keep the process alive.
  timer.unref?.();
}

// --- Prepared statements ---
//
// Rebuilt whenever the database is reopened: a statement is bound to the handle
// that prepared it, so a stale one keeps failing against the dead file.

function prepareStatements() {
  const insertMessage = db.prepare(`
    INSERT INTO messages (from_id, to_id, text, sent_at, delivered, reply_to)
    VALUES (?, ?, ?, ?, 0, ?)
  `);

  const deletePeer = db.prepare(`
    DELETE FROM peers WHERE id = ?
  `);

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

    updateName: db.prepare(`
      UPDATE peers SET name = ? WHERE id = ?
    `),

    // Case-insensitive: "Zod" and "zod" are one handle. The exclusion arm lets a
    // peer re-assert its own name without colliding with itself.
    selectPeerByName: db.prepare(`
      SELECT id, pid FROM peers WHERE name = ? COLLATE NOCASE AND id != ?
    `),

    // Wrapped rather than exposed raw, so every caller that removes a peer also drops its streams.
    // A stream for an id the broker has forgotten can never carry anything again; left open it is
    // a descriptor and a controller held for nobody.
    deletePeer: {
      run(id: string) {
        const changes = deletePeer.run(id);
        if (changes.changes > 0) closeSubscribers(id);
        return changes;
      },
    },

    // Never SELECT * here: the row carries the auth token, and these results are
    // serialised straight to any caller. Project the public columns only.
    selectAllPeers: db.prepare(`
      SELECT id, pid, cwd, git_root, tty, summary, name, registered_at, last_seen FROM peers
    `),

    selectPeersByDirectory: db.prepare(`
      SELECT id, pid, cwd, git_root, tty, summary, name, registered_at, last_seen FROM peers WHERE cwd = ?
    `),

    selectPeersByGitRoot: db.prepare(`
      SELECT id, pid, cwd, git_root, tty, summary, name, registered_at, last_seen FROM peers WHERE git_root = ?
    `),

    // THE NOTIFICATION HOOK. Wrapping the statement rather than calling notifyMailbox from
    // /send-message puts the push at the point where a mailbox actually changes, so a route added
    // later (a broadcast writing one row per recipient, say) wakes its recipients without knowing
    // this transport exists, and cannot forget to.
    //
    // Safe inside a transaction: the frame is enqueued into a stream the runtime flushes after the
    // current synchronous work, so the client's follow-up poll cannot run before the commit.
    insertMessage: {
      run(fromId: string, toId: string, text: string, sentAt: string, replyTo: string | null = null) {
        const changes = insertMessage.run(fromId, toId, text, sentAt, replyTo);
        notifyMailbox(toId);
        return changes;
      },
    },

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

/**
 * A name is a routable address (send_message accepts it in place of an id), so
 * it is validated like one: bounded, printable, and unique among registered
 * peers. Uniqueness at SET time is what keeps resolution at SEND time
 * unambiguous by construction; without it, every send would need a tiebreak.
 */
const NAME_RULE = /^[A-Za-z0-9][A-Za-z0-9 _-]{0,31}$/;

function handleSetName(body: SetNameRequest): SetNameResponse {
  const name = String(body.name ?? "").trim();
  if (!NAME_RULE.test(name)) {
    return { ok: false, error: "invalid: 1-32 chars, letters/digits/space/dash/underscore, must start alphanumeric" };
  }
  const holder = stmts.selectPeerByName.get(name, body.id) as { id: string; pid: number } | null;
  if (holder) {
    // A DEAD holder is not a holder. A session that restarts re-registers under a
    // new id while its previous row survives until the next stale sweep, so
    // without this a restarting session is refused its own name by its own
    // corpse, for up to the sweep interval. That is precisely the window in
    // which sessions restart, so it was the common case rather than the edge.
    //
    // Reap it here rather than waiting: cleanStalePeers would do exactly this,
    // and doing it now keeps the name resolvable to the living peer immediately.
    if (isProcessAlive(holder.pid)) {
      return { ok: false, error: `taken ${holder.id}` };
    }
    stmts.deletePeer.run(holder.id);
    noteDeletion(1);
  }
  try {
    stmts.updateName.run(name, body.id);
  } catch (e) {
    // The unique index caught what the check above could not: another caller
    // claimed this name between the read and the write. Impossible on today's
    // single-threaded runtime, which is exactly why it must not be a 500 the
    // day that changes. Same verdict a losing caller gets from the check.
    if (/UNIQUE constraint failed/i.test(e instanceof Error ? e.message : String(e))) {
      return { ok: false, error: "taken (lost a concurrent claim)" };
    }
    throw e;
  }
  return { ok: true };
}

/**
 * The peers a scope selects, minus the caller, minus anything whose process has died.
 *
 * Shared by /list-peers and /broadcast-message so a broadcast's audience is exactly the peers the
 * caller would have been shown. Two copies of this would be two definitions of "peer", and the one
 * nobody looked at would be the one messages went to.
 */
function selectPeers(body: ListPeersRequest): Peer[] {
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

function handleListPeers(body: ListPeersRequest): Peer[] {
  return selectPeers(body);
}

function handleSendMessage(body: SendMessageRequest): {
  ok: boolean;
  error?: string;
  to_id?: string;
} {
  // to_id is an id first, a name second. Ids are 8 lowercase hex-ish chars and
  // names must start alphanumeric with a 32-char cap, so a string can match
  // both tables; the id lookup winning keeps old callers' behaviour exact.
  let targetId = (
    db.query("SELECT id FROM peers WHERE id = ?").get(body.to_id) as { id: string } | null
  )?.id;
  if (!targetId) {
    targetId = (
      db.query("SELECT id FROM peers WHERE name = ? COLLATE NOCASE").get(body.to_id) as {
        id: string;
      } | null
    )?.id;
  }
  if (!targetId) {
    return { ok: false, error: `Peer ${body.to_id} not found (no such id or name)` };
  }

  // Bounded and stringly: the token is minted by the asking client and echoed
  // back verbatim; the broker only stores and returns it.
  const replyTo =
    typeof body.reply_to === "string" && body.reply_to.length > 0 && body.reply_to.length <= 64
      ? body.reply_to
      : null;
  stmts.insertMessage.run(body.from_id, targetId, body.text, new Date().toISOString(), replyTo);
  // The resolved id, not what the caller wrote: addressing by name must tell the
  // caller WHICH peer it reached, which is what lets ask_peer bind its waiter to
  // that peer instead of trusting any reply that quotes the token.
  return { ok: true, to_id: targetId };
}

/**
 * Fan out one message to every peer the scope selects, the sender excluded.
 *
 * Fan-out on WRITE: one row per recipient, so every mechanism already built for unicast applies
 * unchanged (per-peer acknowledgement, the TTL sweep, the channel gate, the spool fallback). Fan-out
 * on read would need a cursor table, new acknowledgement semantics and a second delivery path, all
 * to serve the two to five sessions that exist on one machine.
 *
 * The insert is one transaction, so a failure part way through leaves no half-delivered broadcast.
 * AFTERWARDS the copies are independent by design: each is acked, reaped and expired on its own,
 * exactly like a unicast, so a dead peer's copy being swept cannot disturb anybody else's.
 *
 * Zero recipients is a success. Being the only session in a directory is ordinary, and reporting it
 * as an error would make the tool cry wolf.
 */
function handleBroadcastMessage(body: BroadcastMessageRequest): BroadcastMessageResponse {
  const recipients = selectPeers({
    scope: body.scope ?? "machine",
    cwd: body.cwd,
    git_root: body.git_root,
    exclude_id: body.from_id,
  });

  const sentAt = new Date().toISOString();
  db.transaction(() => {
    for (const peer of recipients) {
      stmts.insertMessage.run(body.from_id, peer.id, body.text, sentAt);
    }
  })();

  return { ok: true, delivered_to: recipients.length };
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
  // A name is an address: letting an unauthenticated caller rename a peer is
  // letting them redirect that peer's inbound mail.
  "/set-name": "id",
  "/list-peers": "exclude_id",
  "/send-message": "from_id",
  // Unauthenticated this would be the loudest route in the system: one call injecting a message
  // into every session on the machine at once.
  "/broadcast-message": "from_id",
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
function ownsPeer(req: Request, claimed: unknown): boolean {
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

function isAuthorized(req: Request, path: string, body: Record<string, unknown>): boolean {
  const field = CALLER_FIELD[path];
  if (!field) return true;
  return ownsPeer(req, body[field]);
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
    case "/set-name":
      return Response.json(handleSetName(body as SetNameRequest));
    case "/list-peers":
      return Response.json(handleListPeers(body as ListPeersRequest));
    case "/send-message":
      return Response.json(handleSendMessage(body as SendMessageRequest));
    case "/broadcast-message":
      return Response.json(handleBroadcastMessage(body as BroadcastMessageRequest));
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

/**
 * Open an event stream for one authenticated peer.
 *
 * A subscription is a read of a mailbox, so it is authenticated exactly as /poll-messages is:
 * peer ids are public via /list-peers, and an unauthenticated stream would let any local process
 * watch another session's mail arrive.
 */
function handleSubscribe(req: Request, peerId: string): Response {
  let sub: Subscriber | null = null;

  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      sub = { peerId, controller, closed: false };
      addSubscriber(sub);
      // Sent before anything else so the client can tell an accepted subscription from a
      // connection that was merely opened. Its poll interval depends on knowing the difference.
      controller.enqueue(encoder.encode(": subscribed\n\n"));
    },
    cancel() {
      if (sub) removeSubscriber(sub);
    },
  });

  // Both, deliberately. A client that aborts its fetch and a socket that dies are the same event
  // to this registry but not to the runtime, and only one of the two is guaranteed to be observed;
  // removeSubscriber is idempotent so seeing both costs nothing.
  req.signal.addEventListener("abort", () => {
    if (sub) removeSubscriber(sub);
  });

  return new Response(body, {
    status: 200,
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      // Nothing proxies loopback today, but a buffering intermediary would defeat the entire
      // point of the transport, so say so.
      "X-Accel-Buffering": "no",
    },
  });
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
          // Reported so a leak in the registry is observable from outside the process rather than
          // only in a heap dump.
          return Response.json({ status: "ok", peers, subscribers: subscriberCount() });
        } catch (e) {
          const msg = e instanceof Error ? e.message : String(e);
          return Response.json({ status: "error", error: msg }, { status: 500 });
        }
      }
      if (path === "/subscribe") {
        // A 404 is what a broker predating this transport returns, and what the kill switch
        // returns, so a client cannot tell the two apart and falls back to polling for both.
        if (!SSE_ENABLED) return Response.json({ error: "not found" }, { status: 404 });
        const peerId = url.searchParams.get("id");
        if (!withDatabase(() => ownsPeer(req, peerId))) {
          return Response.json({ error: "unauthorized" }, { status: 401 });
        }
        return handleSubscribe(req, peerId!);
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
