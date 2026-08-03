/**
 * Tests for the broker's push transport.
 *
 * The broker to MCP-server hop was a one second poll, so a message waited on average half an
 * interval before anyone knew it existed, and each session issued about 86,400 requests a day
 * nearly all of which returned nothing. These tests describe the observer that replaces it: a
 * registered peer holds one `text/event-stream` connection, and an insert into its mailbox wakes
 * it up.
 *
 * Two properties matter beyond "it is faster". The stream is a mailbox read, so it must be
 * authenticated exactly as `/poll-messages` is: peer ids are public, and an unauthenticated
 * subscribe would let any local process watch another session's traffic arrive. And the registry
 * of open streams must empty itself, because a leak here is a leak of file descriptors and of
 * memory in a daemon that is never restarted.
 */

import { test, expect, beforeAll, afterAll } from "bun:test";
import { join } from "node:path";
import {
  cleanupAll,
  reserveFreePort,
  sweepBrokerOnPort,
  trackProcess,
  trackedTempDir,
} from "./testsupport";

const PORT = reserveFreePort();
const WORK = trackedTempDir("peers-ssetest-");
const DB = join(WORK, "broker.db");
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

/** A live process to own the peer row: peers whose pid is gone are reaped. */
function livePid(): number {
  return trackProcess(Bun.spawn(["sleep", "60"], { stdout: "ignore", stderr: "ignore" })).pid;
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

interface Stream {
  status: number;
  /** Frames received, in order, each the raw text between blank lines. */
  frames: string[];
  /** Resolves once the broker ends the stream. */
  ended: Promise<void>;
  close(): void;
}

/**
 * Open an event stream and collect its frames in the background.
 *
 * Deliberately a raw fetch rather than EventSource: EventSource cannot send an Authorization
 * header, which is the whole point of the auth tests below.
 */
async function subscribe(id: string, token?: string): Promise<Stream> {
  const ac = new AbortController();
  const res = await fetch(`${BASE}/subscribe?id=${encodeURIComponent(id)}`, {
    headers: token ? { Authorization: `Bearer ${token}` } : {},
    signal: ac.signal,
  });

  const frames: string[] = [];
  let done!: () => void;
  const ended = new Promise<void>((r) => (done = r));

  if (res.body) {
    (async () => {
      const decoder = new TextDecoder();
      let buf = "";
      try {
        for await (const chunk of res.body as any) {
          buf += decoder.decode(chunk, { stream: true });
          let sep: number;
          while ((sep = buf.indexOf("\n\n")) >= 0) {
            frames.push(buf.slice(0, sep));
            buf = buf.slice(sep + 2);
          }
        }
      } catch {
        // Aborted by the test, or the broker went away. Either way the stream is over.
      }
      done();
    })();
  } else {
    done();
  }

  return { status: res.status, frames, ended, close: () => ac.abort() };
}

/** Frames that carry a message notification, ignoring keepalive comments. */
function messageFrames(stream: Stream): string[] {
  return stream.frames.filter((f) => f.includes("event: message"));
}

async function waitFor(predicate: () => boolean, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return true;
    await Bun.sleep(5);
  }
  return predicate();
}

function health(): Promise<{ status: string; peers: number; subscribers: number }> {
  return fetch(`${BASE}/health`).then((r) => r.json() as any);
}

beforeAll(async () => {
  broker = trackProcess(
    Bun.spawn(["bun", `${import.meta.dir}/broker.ts`], {
      env: { ...process.env, CLAUDE_PEERS_PORT: String(PORT), CLAUDE_PEERS_DB: DB },
      stdout: "ignore",
      stderr: "ignore",
    })
  );

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
  try {
    cleanupAll();
  } finally {
    sweepBrokerOnPort(PORT);
  }
});

test("an unauthenticated subscribe is rejected", async () => {
  const victim = await register("/tmp/sse-victim");
  const other = await register("/tmp/sse-other");

  // No credential at all. The id alone is public via /list-peers, so it proves nothing.
  expect((await subscribe(victim.id)).status).toBe(401);
  // A valid token, but minted for a different peer.
  expect((await subscribe(victim.id, other.token)).status).toBe(401);
  // A token that was never issued.
  expect((await subscribe(victim.id, "f".repeat(64))).status).toBe(401);
  // No peer named at all: an anonymous stream would be a stream of somebody's mail.
  expect((await subscribe("", other.token)).status).toBe(401);
});

test("an insert into a mailbox wakes that peer's stream at once", async () => {
  const sender = await register("/tmp/sse-sender");
  const receiver = await register("/tmp/sse-receiver");

  const stream = await subscribe(receiver.id, receiver.token);
  expect(stream.status).toBe(200);

  const sentAt = Date.now();
  await post("/send-message", { from_id: sender.id, to_id: receiver.id, text: "wake up" }, sender.token);

  // A quarter of the old poll interval. The point of the transport is that the notification does
  // not wait for a cycle to come round.
  expect(await waitFor(() => messageFrames(stream).length > 0, 250)).toBe(true);
  expect(Date.now() - sentAt).toBeLessThan(250);
  stream.close();
});

test("a stream only sees its own peer's traffic", async () => {
  const sender = await register("/tmp/sse-x-sender");
  const target = await register("/tmp/sse-x-target");
  const bystander = await register("/tmp/sse-x-bystander");

  const wanted = await subscribe(target.id, target.token);
  const unwanted = await subscribe(bystander.id, bystander.token);

  await post("/send-message", { from_id: sender.id, to_id: target.id, text: "for you only" }, sender.token);

  expect(await waitFor(() => messageFrames(wanted).length > 0, 500)).toBe(true);
  // A frame written to both sockets at once is not read from both at once, so the bystander is
  // given time to be wrong before it is declared right.
  await Bun.sleep(250);
  expect(messageFrames(unwanted)).toHaveLength(0);

  wanted.close();
  unwanted.close();
});

test("the subscriber registry empties when clients disconnect", async () => {
  // A daemon that is never restarted cannot afford to keep a controller per stream that ever
  // existed. Disconnection is the normal end of a subscription, not an error case.
  const a = await register("/tmp/sse-leak-a");
  const b = await register("/tmp/sse-leak-b");

  const before = (await health()).subscribers;

  const sa = await subscribe(a.id, a.token);
  const sb = await subscribe(b.id, b.token);
  expect(sa.status).toBe(200);
  expect(sb.status).toBe(200);

  await waitForCount(before + 2);

  sa.close();
  sb.close();

  await waitForCount(before);
});

async function waitForCount(expected: number): Promise<void> {
  const deadline = Date.now() + 2000;
  let seen = -1;
  while (Date.now() < deadline) {
    seen = (await health()).subscribers;
    if (seen === expected) return;
    await Bun.sleep(25);
  }
  expect(seen).toBe(expected);
}

test("unregistering closes the peer's stream and drops it from the registry", async () => {
  // The peer id is gone, so the stream can never carry anything again. Left open it is a
  // descriptor held for a session the broker has already forgotten.
  const a = await register("/tmp/sse-unreg");
  const before = (await health()).subscribers;

  const stream = await subscribe(a.id, a.token);
  expect(stream.status).toBe(200);
  await waitForCount(before + 1);

  await post("/unregister", { id: a.id }, a.token);

  await stream.ended;
  await waitForCount(before);
});
