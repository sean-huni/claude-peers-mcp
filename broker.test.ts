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

function post<T>(path: string, body: unknown, token?: string): Promise<T> {
  return fetch(`${BASE}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(body),
  }).then((r) => r.json() as Promise<T>);
}

function rawPost(path: string, body: unknown, token?: string) {
  return fetch(`${BASE}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(body),
  });
}

// The broker evicts any prior peer sharing a pid, so each test peer needs its
// own live process. Real pids matter: peers whose process is gone get reaped.
const holders: ReturnType<typeof Bun.spawn>[] = [];
function livePid(): number {
  const p = Bun.spawn(["sleep", "60"], { stdout: "ignore", stderr: "ignore" });
  holders.push(p);
  return p.pid;
}

function register(cwd: string, gitRoot: string | null = null) {
  return post<{ id: string; token: string }>("/register", {
    pid: livePid(),
    cwd,
    git_root: gitRoot,
    tty: null,
    summary: "",
  });
}

type Msg = { id: number; from_id: string; to_id: string; text: string; sent_at: string };
const poll = (id: string, token: string) =>
  post<{ messages: Msg[] }>("/poll-messages", { id }, token);
const ack = (peer_id: string, message_ids: number[], token: string) =>
  post<{ ok: boolean; acked: number }>("/ack-messages", { peer_id, message_ids }, token);

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
  for (const h of holders) h.kill();
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
  }, a.token);

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
  }, a.token);
  expect(res.ok).toBe(false);
  expect(res.error).toContain("not found");
});

test("polling does NOT consume the message", async () => {
  // Regression guard. Upstream marked messages delivered inside /poll-messages,
  // so a message was destroyed even when the client failed to render it.
  const a = await register("/tmp/poll-a");
  await post("/send-message", { from_id: a.id, to_id: a.id, text: "survives polling" }, a.token);

  const first = await poll(a.id, a.token);
  const second = await poll(a.id, a.token);
  const third = await poll(a.id, a.token);

  expect(first.messages).toHaveLength(1);
  expect(second.messages).toHaveLength(1);
  expect(third.messages).toHaveLength(1);
  expect(third.messages[0]!.text).toBe("survives polling");
});

test("acknowledging deletes the row rather than flagging it", async () => {
  const a = await register("/tmp/ack-a");
  await post("/send-message", { from_id: a.id, to_id: a.id, text: "delete me" }, a.token);
  const [msg] = (await poll(a.id, a.token)).messages;

  const res = await ack(a.id, [msg!.id], a.token);
  expect(res).toEqual({ ok: true, acked: 1 });
  expect((await poll(a.id, a.token)).messages).toHaveLength(0);

  // Retention matters as much as delivery: the text must be gone from the
  // file, not merely hidden behind a delivered flag.
  const rows = new Database(DB, { readonly: true })
    .query("SELECT COUNT(*) AS n FROM messages WHERE id = ?")
    .get(msg!.id) as { n: number };
  expect(rows.n).toBe(0);
});

test("acknowledging is idempotent and ignores unknown ids", async () => {
  const a = await register("/tmp/ack-idem");
  await post("/send-message", { from_id: a.id, to_id: a.id, text: "once" }, a.token);
  const [msg] = (await poll(a.id, a.token)).messages;

  expect((await ack(a.id, [msg!.id], a.token)).acked).toBe(1);
  expect((await ack(a.id, [msg!.id], a.token)).acked).toBe(0);
  expect((await ack(a.id, [999_999], a.token)).acked).toBe(0);
});

test("a client loop renders each message exactly once", async () => {
  // Models what server.ts does. Without the in-process id set, a non-consuming
  // poll re-pushes the same message on every one second cycle, forever.
  const a = await register("/tmp/loop-a");
  await post("/send-message", { from_id: a.id, to_id: a.id, text: "render once" }, a.token);

  const pushed = new Set<number>();
  let renders = 0;
  for (let cycle = 0; cycle < 3; cycle++) {
    for (const m of (await poll(a.id, a.token)).messages) {
      if (pushed.has(m.id)) continue;
      renders++;
      pushed.add(m.id);
      await ack(a.id, [m.id], a.token);
    }
  }

  expect(renders).toBe(1);
  expect((await poll(a.id, a.token)).messages).toHaveLength(0);
});

test("messages are delivered in the order they were sent", async () => {
  const a = await register("/tmp/order-a");
  for (const text of ["first", "second", "third"]) {
    await post("/send-message", { from_id: a.id, to_id: a.id, text }, a.token);
    await Bun.sleep(5); // sent_at has millisecond resolution
  }

  const texts = (await poll(a.id, a.token)).messages.map((m) => m.text);
  expect(texts).toEqual(["first", "second", "third"]);
});

test("the database file is not readable by other accounts", async () => {
  // It holds every inter-agent message; SQLite creates it 0644 by default.
  const mode = statSync(DB).mode & 0o777;
  expect(mode & 0o077).toBe(0);
});

test("a caller cannot act as another peer without its token", async () => {
  // Peer ids are public via /list-peers, so the id alone must prove nothing.
  // This previously succeeded: any local process could delete a session's
  // queued mail simply by naming it.
  const victim = await register("/tmp/victim");
  await post("/send-message", { from_id: victim.id, to_id: victim.id, text: "private" }, victim.token);
  const [msg] = (await poll(victim.id, victim.token)).messages;

  // No token at all.
  expect((await rawPost("/ack-messages", { peer_id: victim.id, message_ids: [msg!.id] })).status).toBe(401);

  // A valid token, but belonging to a different peer.
  const other = await register("/tmp/other");
  expect(
    (await rawPost("/ack-messages", { peer_id: victim.id, message_ids: [msg!.id] }, other.token)).status
  ).toBe(401);

  // Reading and injecting are refused on the same basis.
  expect((await rawPost("/poll-messages", { id: victim.id })).status).toBe(401);
  expect((await rawPost("/list-peers", { scope: "machine", cwd: "/tmp", git_root: null })).status).toBe(401);
  expect(
    (await rawPost("/send-message", { from_id: victim.id, to_id: victim.id, text: "forged" })).status
  ).toBe(401);

  // The victim's mail survived every attempt.
  expect((await poll(victim.id, victim.token)).messages).toHaveLength(1);
});

test("registration mints a high entropy token", async () => {
  const a = await register("/tmp/entropy");
  expect(a.token).toMatch(/^[0-9a-f]{64}$/); // 256 bits, hex
  const b = await register("/tmp/entropy-2");
  expect(b.token).not.toBe(a.token);
});
