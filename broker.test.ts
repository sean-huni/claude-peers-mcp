/**
 * End-to-end tests for the broker daemon.
 *
 * Each test runs against a real broker process with an isolated database, so
 * these exercise the actual HTTP surface and the actual SQLite behaviour rather
 * than mocks. The delivery semantics here are load bearing: upstream lost
 * messages by consuming them on poll, and the fix is only meaningful if the
 * non-consuming poll and the acknowledgement stay in step.
 */

import { test, expect, beforeAll, afterAll } from "bun:test";
import { statSync, rmSync } from "node:fs";
import { Database } from "bun:sqlite";

const PORT = 7920 + Math.floor(Math.random() * 40);
const DB = `${process.env.TMPDIR ?? "/tmp"}/claude-peers-test-${PORT}.db`;
const BASE = `http://127.0.0.1:${PORT}`;

let broker: ReturnType<typeof Bun.spawn>;

function post<T>(path: string, body: unknown): Promise<T> {
  return fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  }).then((r) => r.json() as Promise<T>);
}

function register(cwd: string, gitRoot: string | null = null) {
  return post<{ id: string }>("/register", {
    pid: process.pid,
    cwd,
    git_root: gitRoot,
    tty: null,
    summary: "",
  });
}

type Msg = { id: number; from_id: string; to_id: string; text: string; sent_at: string };
const poll = (id: string) => post<{ messages: Msg[] }>("/poll-messages", { id });
const ack = (peer_id: string, message_ids: number[]) =>
  post<{ ok: boolean; acked: number }>("/ack-messages", { peer_id, message_ids });

beforeAll(async () => {
  for (const suffix of ["", "-wal", "-shm"]) rmSync(`${DB}${suffix}`, { force: true });

  broker = Bun.spawn(["bun", `${import.meta.dir}/broker.ts`], {
    env: { ...process.env, CLAUDE_PEERS_PORT: String(PORT), CLAUDE_PEERS_DB: DB },
    stdout: "ignore",
    stderr: "ignore",
  });

  for (let i = 0; i < 40; i++) {
    await Bun.sleep(100);
    try {
      if ((await fetch(`${BASE}/health`)).ok) return;
    } catch {
      // still booting
    }
  }
  throw new Error(`broker did not come up on ${PORT}`);
});

afterAll(() => {
  broker?.kill();
  for (const suffix of ["", "-wal", "-shm"]) rmSync(`${DB}${suffix}`, { force: true });
});

test("a registered peer is discoverable and excludes itself", async () => {
  const a = await register("/tmp/peer-a");
  const b = await register("/tmp/peer-b");

  const seen = await post<{ id: string }[]>("/list-peers", {
    scope: "machine",
    cwd: "/tmp/peer-a",
    git_root: null,
    exclude_id: a.id,
  });

  const ids = seen.map((p) => p.id);
  expect(ids).toContain(b.id);
  expect(ids).not.toContain(a.id);
});

test("a message sent to an unknown peer is refused", async () => {
  const a = await register("/tmp/sender");
  const res = await post<{ ok: boolean; error?: string }>("/send-message", {
    from_id: a.id,
    to_id: "nosuchpeer",
    text: "hello",
  });
  expect(res.ok).toBe(false);
  expect(res.error).toContain("not found");
});

test("polling does NOT consume the message", async () => {
  // Regression guard. Upstream marked messages delivered inside /poll-messages,
  // so a message was destroyed even when the client failed to render it.
  const a = await register("/tmp/poll-a");
  await post("/send-message", { from_id: a.id, to_id: a.id, text: "survives polling" });

  const first = await poll(a.id);
  const second = await poll(a.id);
  const third = await poll(a.id);

  expect(first.messages).toHaveLength(1);
  expect(second.messages).toHaveLength(1);
  expect(third.messages).toHaveLength(1);
  expect(third.messages[0]!.text).toBe("survives polling");
});

test("acknowledging deletes the row rather than flagging it", async () => {
  const a = await register("/tmp/ack-a");
  await post("/send-message", { from_id: a.id, to_id: a.id, text: "delete me" });
  const [msg] = (await poll(a.id)).messages;

  const res = await ack(a.id, [msg!.id]);
  expect(res).toEqual({ ok: true, acked: 1 });
  expect((await poll(a.id)).messages).toHaveLength(0);

  // Retention matters as much as delivery: the text must be gone from the
  // file, not merely hidden behind a delivered flag.
  const rows = new Database(DB, { readonly: true })
    .query("SELECT COUNT(*) AS n FROM messages WHERE id = ?")
    .get(msg!.id) as { n: number };
  expect(rows.n).toBe(0);
});

test("acknowledging is idempotent and ignores unknown ids", async () => {
  const a = await register("/tmp/ack-idem");
  await post("/send-message", { from_id: a.id, to_id: a.id, text: "once" });
  const [msg] = (await poll(a.id)).messages;

  expect((await ack(a.id, [msg!.id])).acked).toBe(1);
  expect((await ack(a.id, [msg!.id])).acked).toBe(0);
  expect((await ack(a.id, [999_999])).acked).toBe(0);
});

test("a client loop renders each message exactly once", async () => {
  // Models what server.ts does. Without the in-process id set, a non-consuming
  // poll re-pushes the same message on every one second cycle, forever.
  const a = await register("/tmp/loop-a");
  await post("/send-message", { from_id: a.id, to_id: a.id, text: "render once" });

  const pushed = new Set<number>();
  let renders = 0;
  for (let cycle = 0; cycle < 3; cycle++) {
    for (const m of (await poll(a.id)).messages) {
      if (pushed.has(m.id)) continue;
      renders++;
      pushed.add(m.id);
      await ack(a.id, [m.id]);
    }
  }

  expect(renders).toBe(1);
  expect((await poll(a.id)).messages).toHaveLength(0);
});

test("messages are delivered in the order they were sent", async () => {
  const a = await register("/tmp/order-a");
  for (const text of ["first", "second", "third"]) {
    await post("/send-message", { from_id: a.id, to_id: a.id, text });
    await Bun.sleep(5); // sent_at has millisecond resolution
  }

  const texts = (await poll(a.id)).messages.map((m) => m.text);
  expect(texts).toEqual(["first", "second", "third"]);
});

test("the database file is not readable by other accounts", async () => {
  // It holds every inter-agent message; SQLite creates it 0644 by default.
  const mode = statSync(DB).mode & 0o777;
  expect(mode & 0o077).toBe(0);
});

test("KNOWN GAP: any caller can acknowledge another peer's mail", async () => {
  // Documents a real hole rather than asserting the behaviour is correct.
  // peer_id is supplied by the caller and peer ids are public via /list-peers,
  // so a stray local process can delete a session's queued messages. Closing
  // this needs request authentication, which the broker does not yet have.
  // When auth lands, this test should flip to expecting acked === 0.
  const victim = await register("/tmp/victim");
  await post("/send-message", { from_id: victim.id, to_id: victim.id, text: "private" });
  const [msg] = (await poll(victim.id)).messages;

  const stolen = await ack(victim.id, [msg!.id]); // caller simply claims the id
  expect(stolen.acked).toBe(1);
  expect((await poll(victim.id)).messages).toHaveLength(0);
});
