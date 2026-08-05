/**
 * Peer names at the broker boundary.
 *
 * A name is a routable address: send_message accepts it in place of an id. That
 * is why these tests treat naming like an auth surface (rename = redirect the
 * peer's mail) and why uniqueness is enforced at SET time rather than resolved
 * by tiebreak at SEND time.
 *
 * Inspired by the JasonDictos fork's peer-names feature; implementation is our own.
 */

import { test, expect, beforeAll, afterAll } from "bun:test";
import { join } from "node:path";
import { Database } from "bun:sqlite";
import {
  cleanupAll,
  reserveFreePort,
  sweepBrokerOnPort,
  trackProcess,
  trackedTempDir,
} from "./testsupport";

const PORT = reserveFreePort();
const WORK = trackedTempDir("peers-names-");
const DB = join(WORK, "broker.db");
const B = `http://127.0.0.1:${PORT}`;
const ENV = {
  ...process.env,
  CLAUDE_PEERS_PORT: String(PORT),
  CLAUDE_PEERS_DB: DB,
  CLAUDE_PEERS_SPOOL_DIR: join(WORK, "spool"),
};

const held: ReturnType<typeof Bun.spawn>[] = [];

/** Register a peer backed by a real live process, so liveness checks pass. */
async function reg(cwd: string): Promise<{ id: string; token: string }> {
  const proc = trackProcess(Bun.spawn(["sleep", "300"], { stdout: "ignore", stderr: "ignore" }));
  held.push(proc);
  const res = await fetch(`${B}/register`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ pid: proc.pid, cwd, git_root: null, tty: null, summary: "" }),
  });
  return (await res.json()) as { id: string; token: string };
}

async function post(path: string, body: unknown, token?: string) {
  const res = await fetch(`${B}${path}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(body),
  });
  return { status: res.status, body: (await res.json().catch(() => null)) as any };
}

let broker: ReturnType<typeof Bun.spawn>;

beforeAll(async () => {
  broker = trackProcess(
    Bun.spawn(["bun", `${import.meta.dir}/broker.ts`], { env: ENV, stdout: "ignore", stderr: "ignore" })
  );
  for (let i = 0; i < 40; i++) {
    await Bun.sleep(100);
    try {
      if ((await fetch(`${B}/health`)).ok) return;
    } catch {
      // booting
    }
  }
  throw new Error(`broker did not come up on ${PORT}`);
});

afterAll(() => {
  try {
    cleanupAll();
  } finally {
    sweepBrokerOnPort(PORT);
  }
});

test("set-name requires the peer's own token", async () => {
  const a = await reg("/tmp/names-a");
  const b = await reg("/tmp/names-b");

  // No token at all
  expect((await post("/set-name", { id: a.id, name: "intruder" })).status).toBe(401);
  // Another peer's token: knowing a public id must not grant renaming it
  expect((await post("/set-name", { id: a.id, name: "intruder" }, b.token)).status).toBe(401);
  // The owner
  const own = await post("/set-name", { id: a.id, name: "alpha" }, a.token);
  expect(own.status).toBe(200);
  expect(own.body.ok).toBe(true);
});

test("a name another registered peer holds is refused, case-insensitively", async () => {
  const a = await reg("/tmp/names-c");
  const b = await reg("/tmp/names-d");
  expect((await post("/set-name", { id: a.id, name: "Zod" }, a.token)).body.ok).toBe(true);

  const clash = await post("/set-name", { id: b.id, name: "zod" }, b.token);
  expect(clash.body.ok).toBe(false);
  expect(String(clash.body.error)).toContain("taken");

  // Re-asserting your OWN name is not a collision
  expect((await post("/set-name", { id: a.id, name: "Zod" }, a.token)).body.ok).toBe(true);
});

test("a name held by a DEAD peer can be reclaimed, so a restarted session keeps its name", async () => {
  // Found by chaos testing, 2026-08-05. A session that restarts re-registers under a
  // new peer id, while its previous row survives until the 30s stale sweep. The old
  // row still held the name, so the restarted session was told "taken" by its own
  // corpse and ran unnamed. CLAUDE_PEERS_NAME made this the default experience of
  // every restart inside that window, which is exactly when people restart.
  const proc = trackProcess(Bun.spawn(["sleep", "300"], { stdout: "ignore", stderr: "ignore" }));
  held.push(proc);
  const doomed = await (
    await fetch(`${B}/register`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ pid: proc.pid, cwd: "/tmp/names-dead", git_root: null, tty: null, summary: "" }),
    })
  ).json() as { id: string; token: string };
  expect((await post("/set-name", { id: doomed.id, name: "phoenix" }, doomed.token)).body.ok).toBe(true);

  // The process dies. The row is still there: the sweep runs on its own clock.
  proc.kill(9);
  await Bun.sleep(300);

  const reborn = await reg("/tmp/names-reborn");
  const claim = await post("/set-name", { id: reborn.id, name: "phoenix" }, reborn.token);
  expect(claim.body.error ?? "").not.toContain("taken");
  expect(claim.body.ok).toBe(true);

  // And the name resolves to the LIVING peer, not the corpse.
  const sender = await reg("/tmp/names-sender");
  expect(
    (await post("/send-message", { from_id: sender.id, to_id: "phoenix", text: "risen" }, sender.token))
      .body.ok
  ).toBe(true);
  const inbox = await post("/poll-messages", { id: reborn.id }, reborn.token);
  expect((inbox.body.messages as any[]).map((m) => m.text)).toContain("risen");
});

test("a name held by a LIVE peer is still refused after the dead-peer change", async () => {
  // The guard above must not become "anyone can take any name": liveness is the
  // only thing that changed, so a living holder still wins.
  const holder = await reg("/tmp/names-live-holder");
  const rival = await reg("/tmp/names-live-rival");
  expect((await post("/set-name", { id: holder.id, name: "occupied" }, holder.token)).body.ok).toBe(true);
  const denied = await post("/set-name", { id: rival.id, name: "occupied" }, rival.token);
  expect(denied.body.ok).toBe(false);
  expect(String(denied.body.error)).toContain("taken");
});

test("invalid names are refused with the rule spelled out", async () => {
  const a = await reg("/tmp/names-e");
  for (const bad of ["", "   ", "-leading-dash", "a".repeat(33), "new\nline", "semi;colon"]) {
    const res = await post("/set-name", { id: a.id, name: bad }, a.token);
    expect(res.body.ok).toBe(false);
    expect(String(res.body.error)).toContain("invalid");
  }
});

test("list-peers carries the name; send-message resolves it to the same mailbox as the id", async () => {
  const a = await reg("/tmp/names-f");
  const b = await reg("/tmp/names-g");
  expect((await post("/set-name", { id: b.id, name: "receiver-name" }, b.token)).body.ok).toBe(true);

  const listing = await post(
    "/list-peers",
    { scope: "machine", cwd: "/", git_root: null, exclude_id: a.id },
    a.token
  );
  const row = (listing.body as any[]).find((p) => p.id === b.id);
  expect(row.name).toBe("receiver-name");

  // One send by id, one by name (case differs): both must land in b's mailbox
  expect(
    (await post("/send-message", { from_id: a.id, to_id: b.id, text: "by id" }, a.token)).body.ok
  ).toBe(true);
  expect(
    (await post("/send-message", { from_id: a.id, to_id: "Receiver-Name", text: "by name" }, a.token))
      .body.ok
  ).toBe(true);

  const inbox = await post("/poll-messages", { id: b.id }, b.token);
  const texts = (inbox.body.messages as any[]).map((m) => m.text).sort();
  expect(texts).toEqual(["by id", "by name"]);
  // Stored rows carry the canonical id, never the name
  for (const m of inbox.body.messages) expect(m.to_id).toBe(b.id);
});

test("an unknown name fails the send rather than dropping the message", async () => {
  const a = await reg("/tmp/names-h");
  const res = await post("/send-message", { from_id: a.id, to_id: "nobody-here", text: "x" }, a.token);
  expect(res.body.ok).toBe(false);
  expect(String(res.body.error)).toContain("not found");
});

test("reply_to is stored only when it is a bounded string", async () => {
  // The constraint sweep (2026-08-05) mutated this validation away and the whole
  // suite stayed green: nothing tested it. reply_to is echoed back to clients and
  // used as a Map key by the asking session, so a non-string or unbounded value
  // is both a correlation hazard and unbounded attacker-controlled storage.
  const a = await reg("/tmp/reply-a");
  const b = await reg("/tmp/reply-b");

  const cases: { label: string; value: unknown; keep: boolean }[] = [
    { label: "ordinary token", value: "abc12345", keep: true },
    { label: "64 chars (the limit)", value: "x".repeat(64), keep: true },
    { label: "65 chars (over the limit)", value: "x".repeat(65), keep: false },
    { label: "empty string", value: "", keep: false },
    { label: "number", value: 12345, keep: false },
    { label: "object", value: { evil: true }, keep: false },
    { label: "array", value: ["a"], keep: false },
    { label: "boolean", value: true, keep: false },
  ];

  for (const c of cases) {
    const text = `reply-case:${c.label}`;
    const sent = await post(
      "/send-message",
      { from_id: a.id, to_id: b.id, text, reply_to: c.value },
      a.token
    );
    // A bad reply_to must never fail the SEND: the message is still a message.
    expect(sent.body.ok, `${c.label} should still send`).toBe(true);
  }

  const inbox = await post("/poll-messages", { id: b.id }, b.token);
  const rows = inbox.body.messages as any[];
  for (const c of cases) {
    const row = rows.find((m) => m.text === `reply-case:${c.label}`);
    expect(row, `${c.label} must have been delivered`).toBeDefined();
    if (c.keep) {
      expect(row.reply_to, `${c.label} should be kept verbatim`).toBe(c.value);
    } else {
      expect(row.reply_to, `${c.label} should be dropped to null`).toBeNull();
    }
    // Whatever survives is a string or null, never another type: the asking
    // session looks this up in a string-keyed Map.
    expect(["string", "object"]).toContain(typeof row.reply_to);
  }
});

test("the schema itself enforces name uniqueness, not just the handler", async () => {
  // handleSetName reads-then-writes, which is atomic only because Bun.serve runs
  // one JS thread. That is a runtime property, not a declared invariant, so the
  // rule is pinned in the schema where a future reusePort/worker/await cannot
  // quietly repeal it. This test asserts the INDEX exists and bites, which the
  // handler-level tests cannot distinguish from the handler merely being lucky.
  const holder = await reg("/tmp/idx-holder");
  expect((await post("/set-name", { id: holder.id, name: "indexed" }, holder.token)).body.ok).toBe(true);

  const db = new Database(DB, { readonly: true });
  const idx = db
    .query("SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'idx_peers_name_unique'")
    .get() as { sql: string } | null;
  expect(idx, "the unique index must exist").not.toBeNull();
  expect(idx!.sql).toContain("UNIQUE");
  expect(idx!.sql).toContain("NOCASE");
  db.close();

  // Bypass the handler entirely and write a duplicate straight at the table.
  // The constraint must refuse it; if it does not, the handler is the only guard.
  const direct = new Database(DB);
  const other = await reg("/tmp/idx-other");
  let refused = false;
  try {
    direct.run("UPDATE peers SET name = ? WHERE id = ?", ["INDEXED", other.id]);
  } catch (e) {
    refused = /UNIQUE constraint failed/i.test(e instanceof Error ? e.message : String(e));
  }
  direct.close();
  expect(refused, "a direct duplicate write must hit the unique constraint").toBe(true);
});

test("a legacy database holding duplicate names boots and keeps the earliest", async () => {
  // Pre-index databases can already contain duplicates. Refusing to boot over old
  // data would be worse than the duplicates, so the migration resolves them.
  const dupDb = join(WORK, "dupes.db");
  const seed = new Database(dupDb, { create: true });
  seed.run(`
    CREATE TABLE peers (
      id TEXT PRIMARY KEY, pid INTEGER NOT NULL, cwd TEXT NOT NULL, git_root TEXT, tty TEXT,
      summary TEXT NOT NULL DEFAULT '', name TEXT, registered_at TEXT NOT NULL,
      last_seen TEXT NOT NULL, token TEXT
    )
  `);
  seed.run(`
    CREATE TABLE messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT, from_id TEXT NOT NULL, to_id TEXT NOT NULL,
      text TEXT NOT NULL, sent_at TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0, reply_to TEXT
    )
  `);
  // Real live pids: the broker reaps rows whose process is dead at boot, which
  // would delete these rows for a reason that has nothing to do with the index
  // and leave the test unable to tell the two explanations apart.
  const spawnAlive = () =>
    trackProcess(Bun.spawn(["sleep", "300"], { stdout: "ignore", stderr: "ignore" }));
  const aliveA = spawnAlive();
  const aliveB = spawnAlive();
  const aliveC = spawnAlive();
  held.push(aliveA, aliveB, aliveC);
  const ins = seed.prepare(
    "INSERT INTO peers (id,pid,cwd,summary,name,registered_at,last_seen,token) VALUES (?,?,?,'',?,?,?,'t')"
  );
  ins.run("aaaaaaaa", aliveA.pid, "/tmp/dup-a", "twin", "2020-01-01T00:00:00.000Z", "2020-01-01T00:00:00.000Z");
  ins.run("bbbbbbbb", aliveB.pid, "/tmp/dup-b", "TWIN", "2021-01-01T00:00:00.000Z", "2021-01-01T00:00:00.000Z");
  ins.run("cccccccc", aliveC.pid, "/tmp/dup-c", "solo", "2021-01-01T00:00:00.000Z", "2021-01-01T00:00:00.000Z");
  seed.close();

  const port2 = reserveFreePort();
  trackProcess(
    Bun.spawn(["bun", `${import.meta.dir}/broker.ts`], {
      env: { ...ENV, CLAUDE_PEERS_PORT: String(port2), CLAUDE_PEERS_DB: dupDb },
      stdout: "ignore",
      stderr: "ignore",
    })
  );
  try {
    let up = false;
    for (let i = 0; i < 40 && !up; i++) {
      await Bun.sleep(100);
      up = await fetch(`http://127.0.0.1:${port2}/health`).then((r) => r.ok).catch(() => false);
    }
    expect(up, "the broker must boot over a database with duplicate names").toBe(true);

    const after = new Database(dupDb, { readonly: true });
    const rows = after.query("SELECT id, name FROM peers ORDER BY id").all() as {
      id: string;
      name: string | null;
    }[];
    after.close();
    // Earliest registration keeps it; the later twin is cleared; unrelated name untouched.
    const byId = (id: string) => {
      const row = rows.find((r) => r.id === id);
      // A missing row is a DIFFERENT failure from a wrong name (the stale-peer
      // sweep reaping it), so say which one happened rather than dereferencing.
      expect(row, `peer ${id} should still exist after the migration`).toBeDefined();
      return row!;
    };
    expect(byId("aaaaaaaa").name).toBe("twin");
    expect(byId("bbbbbbbb").name).toBeNull();
    expect(byId("cccccccc").name).toBe("solo");
  } finally {
    sweepBrokerOnPort(port2);
  }
});

test("a legacy database without the name column is migrated at boot", async () => {
  // Build a pre-names schema file, then point a fresh broker at it.
  const legacyDb = join(WORK, "legacy.db");
  const legacy = new Database(legacyDb, { create: true });
  legacy.run(`
    CREATE TABLE peers (
      id TEXT PRIMARY KEY, pid INTEGER NOT NULL, cwd TEXT NOT NULL, git_root TEXT, tty TEXT,
      summary TEXT NOT NULL DEFAULT '', registered_at TEXT NOT NULL, last_seen TEXT NOT NULL, token TEXT
    )
  `);
  legacy.run(`
    CREATE TABLE messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT, from_id TEXT NOT NULL, to_id TEXT NOT NULL,
      text TEXT NOT NULL, sent_at TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0
    )
  `);
  legacy.close();

  const port2 = reserveFreePort();
  const b2 = trackProcess(
    Bun.spawn(["bun", `${import.meta.dir}/broker.ts`], {
      env: { ...ENV, CLAUDE_PEERS_PORT: String(port2), CLAUDE_PEERS_DB: legacyDb },
      stdout: "ignore",
      stderr: "ignore",
    })
  );
  try {
    let up = false;
    for (let i = 0; i < 40 && !up; i++) {
      await Bun.sleep(100);
      up = await fetch(`http://127.0.0.1:${port2}/health`).then((r) => r.ok).catch(() => false);
    }
    expect(up).toBe(true);

    const proc = trackProcess(Bun.spawn(["sleep", "300"], { stdout: "ignore", stderr: "ignore" }));
    held.push(proc);
    const regRes = await fetch(`http://127.0.0.1:${port2}/register`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ pid: proc.pid, cwd: "/tmp/legacy", git_root: null, tty: null, summary: "" }),
    });
    const peer = (await regRes.json()) as { id: string; token: string };
    const named = await fetch(`http://127.0.0.1:${port2}/set-name`, {
      method: "POST",
      headers: { "content-type": "application/json", Authorization: `Bearer ${peer.token}` },
      body: JSON.stringify({ id: peer.id, name: "migrated" }),
    });
    expect(((await named.json()) as any).ok).toBe(true);
  } finally {
    sweepBrokerOnPort(port2);
  }
});
