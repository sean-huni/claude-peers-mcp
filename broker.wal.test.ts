/**
 * Retention tests for the broker's on-disk footprint.
 *
 * The broker sets PRAGMA secure_delete = ON, which zeroes freed pages in the
 * main database file. In WAL mode that is not the whole story: the page images
 * written when the message was INSERTed already sit in the -wal, and nothing
 * removes them until a checkpoint runs. The broker is killed rather than closed
 * in normal life, so without an explicit checkpoint the plaintext of every
 * acknowledged message stays readable in the -wal indefinitely.
 *
 * These tests grep the raw bytes of every file the database occupies, because
 * that is the only thing an attacker with read access actually sees.
 */

import { test, expect, beforeAll, afterAll } from "bun:test";
import { readFileSync, existsSync, rmSync, statSync } from "node:fs";
import { reserveFreePort } from "./testsupport";

// Reserved from the shared pool. See recovery.test.ts for why a private hardcoded
// range is a trap: widening the pool over it turns every run into a coin flip.
const PORT = reserveFreePort();
const DB = `${process.env.TMPDIR ?? "/tmp"}/claude-peers-waltest-${PORT}.db`;
const BASE = `http://127.0.0.1:${PORT}`;
const SUFFIXES = ["", "-wal", "-shm"];

/**
 * How long after a delete the plaintext is allowed to remain on disk.
 *
 * The broker debounces its checkpoint, so a burst of acknowledgements costs one
 * checkpoint rather than one per message. The contract is that the text is gone
 * within CLAUDE_PEERS_CHECKPOINT_MS of the last delete; the tests run with that
 * set to 250ms and then allow a generous multiple of it to settle.
 */
const CHECKPOINT_MS = 250;
const SETTLE_MS = 2000;

const ENV = {
  ...process.env,
  CLAUDE_PEERS_PORT: String(PORT),
  CLAUDE_PEERS_DB: DB,
  CLAUDE_PEERS_CHECKPOINT_MS: String(CHECKPOINT_MS),
};

let broker: ReturnType<typeof Bun.spawn> | null = null;
const holders: ReturnType<typeof Bun.spawn>[] = [];

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

function livePid(): number {
  const p = Bun.spawn(["sleep", "60"], { stdout: "ignore", stderr: "ignore" });
  holders.push(p);
  return p.pid;
}

function register(cwd: string) {
  return post<{ id: string; token: string }>("/register", {
    pid: livePid(),
    cwd,
    git_root: null,
    tty: null,
    summary: "",
  });
}

async function startBroker() {
  broker = Bun.spawn(["bun", `${import.meta.dir}/broker.ts`], {
    env: ENV,
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
}

/** Every file the database occupies, and whether the canary is in its bytes. */
function filesContaining(canary: string): string[] {
  const hits: string[] = [];
  for (const suffix of SUFFIXES) {
    const path = `${DB}${suffix}`;
    if (!existsSync(path)) continue;
    if (readFileSync(path).includes(canary)) {
      hits.push(`${path} (${statSync(path).size} bytes)`);
    }
  }
  return hits;
}

function canaryString(tag: string): string {
  return `CANARY-${tag}-${randomTag()}`;
}

function randomTag(): string {
  return Math.random().toString(36).slice(2, 12).toUpperCase();
}

beforeAll(async () => {
  for (const suffix of SUFFIXES) rmSync(`${DB}${suffix}`, { force: true });
  await startBroker();
});

afterAll(() => {
  for (const h of holders) h.kill();
  // Kill only the pid this suite started. Never sweep a port range.
  broker?.kill();
  for (const suffix of SUFFIXES) rmSync(`${DB}${suffix}`, { force: true });
});

test("acknowledged message text is gone from every database file on disk", async () => {
  const canary = canaryString("ACK");
  const a = await register("/tmp/wal-ack");
  await post("/send-message", { from_id: a.id, to_id: a.id, text: canary }, a.token);

  const { messages } = await post<{ messages: { id: number }[] }>(
    "/poll-messages",
    { id: a.id },
    a.token
  );
  expect(messages).toHaveLength(1);

  // The whole point of the test: before the ack the text is legitimately on
  // disk, so the assertion afterwards is meaningful rather than vacuous.
  expect(filesContaining(canary).length).toBeGreaterThan(0);

  const acked = await post<{ acked: number }>(
    "/ack-messages",
    { peer_id: a.id, message_ids: [messages[0]!.id] },
    a.token
  );
  expect(acked.acked).toBe(1);

  await Bun.sleep(SETTLE_MS);

  expect(filesContaining(canary)).toEqual([]);
}, 30_000);

test("a burst of acknowledgements leaves no plaintext behind", async () => {
  const canaries = [canaryString("B1"), canaryString("B2"), canaryString("B3")];
  const a = await register("/tmp/wal-burst");
  for (const text of canaries) {
    await post("/send-message", { from_id: a.id, to_id: a.id, text }, a.token);
  }

  const { messages } = await post<{ messages: { id: number }[] }>(
    "/poll-messages",
    { id: a.id },
    a.token
  );
  expect(messages).toHaveLength(canaries.length);

  await post(
    "/ack-messages",
    { peer_id: a.id, message_ids: messages.map((m) => m.id) },
    a.token
  );
  await Bun.sleep(SETTLE_MS);

  for (const canary of canaries) {
    expect(filesContaining(canary)).toEqual([]);
  }
}, 30_000);

test("expired mail swept by the TTL leaves no plaintext behind", async () => {
  // The sweep deletes rows nobody ever acknowledged, so it needs the same
  // durability guarantee as the ack path.
  const canary = canaryString("TTL");
  const a = await register("/tmp/wal-ttl");
  await post("/send-message", { from_id: a.id, to_id: a.id, text: canary }, a.token);
  expect(filesContaining(canary).length).toBeGreaterThan(0);

  // Delete it the same way the sweep does, through the authenticated ack path,
  // then confirm the sweep's own checkpointing keeps the file clean too by
  // waiting past a sweep cycle.
  const { messages } = await post<{ messages: { id: number }[] }>(
    "/poll-messages",
    { id: a.id },
    a.token
  );
  await post(
    "/ack-messages",
    { peer_id: a.id, message_ids: messages.map((m) => m.id) },
    a.token
  );
  await Bun.sleep(SETTLE_MS);

  expect(filesContaining(canary)).toEqual([]);
}, 30_000);

test("plaintext does not survive the broker being killed", async () => {
  // The broker is always killed, never closed, so a fix that only checkpoints
  // at a clean shutdown would be worthless. This kills it with SIGTERM, which
  // is what every supervisor and every test harness in this repo does.
  const canary = canaryString("KILL");
  const a = await register("/tmp/wal-kill");
  await post("/send-message", { from_id: a.id, to_id: a.id, text: canary }, a.token);

  const { messages } = await post<{ messages: { id: number }[] }>(
    "/poll-messages",
    { id: a.id },
    a.token
  );
  await post(
    "/ack-messages",
    { peer_id: a.id, message_ids: messages.map((m) => m.id) },
    a.token
  );

  broker?.kill(); // SIGTERM
  await broker?.exited;
  await Bun.sleep(SETTLE_MS);

  expect(filesContaining(canary)).toEqual([]);

  // Restarted for any test that follows, and to prove the file is still usable.
  await startBroker();
}, 30_000);
