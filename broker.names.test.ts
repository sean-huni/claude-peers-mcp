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
