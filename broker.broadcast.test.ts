/**
 * End-to-end tests for POST /broadcast-message.
 *
 * Broadcast is fan-out on write: one message row per recipient, inserted in one transaction, so
 * every mechanism already built for unicast (per-peer acknowledgement, the TTL sweep, the channel
 * gate, the spool fallback) applies unchanged. The properties worth pinning down are therefore the
 * ones fan-out introduces and unicast never had: who exactly the recipients are, that the sender is
 * not one of them, that nobody listening is not an error, and that the copies are independent of
 * each other afterwards.
 *
 * Recipient selection is deliberately the SAME selection /list-peers performs. These tests assert
 * the counts directly rather than comparing against /list-peers, so a refactor that broke both in
 * the same way would still be caught.
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
const WORK = trackedTempDir("peers-bcasttest-");
const DB = join(WORK, "broker.db");
const BASE = `http://127.0.0.1:${PORT}`;

let broker: ReturnType<typeof Bun.spawn>;

function post<T>(path: string, body: unknown, token?: string): Promise<T> {
  return rawPost(path, body, token).then((r) => r.json() as Promise<T>);
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

// Peers whose process is gone are reaped by the selection, so every test peer needs a real live pid.
function livePid(): number {
  return trackProcess(Bun.spawn(["sleep", "120"], { stdout: "ignore", stderr: "ignore" })).pid;
}

type Peer = { id: string; token: string };

function register(cwd: string, gitRoot: string | null = null): Promise<Peer> {
  return post<Peer>("/register", {
    pid: livePid(),
    cwd,
    git_root: gitRoot,
    tty: null,
    summary: "",
  });
}

type Msg = { id: number; from_id: string; to_id: string; text: string; sent_at: string };

const poll = (peer: Peer) => post<{ messages: Msg[] }>("/poll-messages", { id: peer.id }, peer.token);
const texts = async (peer: Peer) => (await poll(peer)).messages.map((m) => m.text);
const ack = (peer: Peer, ids: number[]) =>
  post<{ ok: boolean; acked: number }>(
    "/ack-messages",
    { peer_id: peer.id, message_ids: ids },
    peer.token
  );

type BroadcastResult = { ok: boolean; delivered_to: number };

function broadcast(
  sender: Peer,
  text: string,
  extra: Record<string, unknown> = {}
): Promise<BroadcastResult> {
  return post<BroadcastResult>(
    "/broadcast-message",
    { from_id: sender.id, text, cwd: "/tmp", git_root: null, ...extra },
    sender.token
  );
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

// FIRST, deliberately: machine scope counts every peer this broker knows about, so the only moment
// that count is exactly knowable is before any other test has registered anything. Every later test
// scopes to a directory or a repo of its own and is therefore order independent.
test("machine scope reaches every other peer and never the sender", async () => {
  const sender = await register("/tmp/bcast-machine-sender");
  const b = await register("/tmp/bcast-machine-b");
  const c = await register("/tmp/bcast-machine-c");

  const res = await broadcast(sender, "everyone pull before you build", { scope: "machine" });
  expect(res).toEqual({ ok: true, delivered_to: 2 });

  expect(await texts(b)).toEqual(["everyone pull before you build"]);
  expect(await texts(c)).toEqual(["everyone pull before you build"]);
  // The sender is a peer on this machine like any other, so excluding it is a decision, not a
  // side effect of how the rows happen to be selected.
  expect(await texts(sender)).toEqual([]);
});

test("directory scope reaches only peers sharing the sender's working directory", async () => {
  const dir = "/tmp/bcast-dir-shared";
  const sender = await register(dir);
  const inside = await register(dir);
  const outside = await register("/tmp/bcast-dir-elsewhere");

  const res = await broadcast(sender, "same directory only", { scope: "directory", cwd: dir });
  expect(res).toEqual({ ok: true, delivered_to: 1 });

  expect(await texts(inside)).toEqual(["same directory only"]);
  expect(await texts(outside)).toEqual([]);
});

test("repo scope reaches peers sharing the git root, across different directories", async () => {
  const root = "/tmp/bcast-repo-root";
  const sender = await register(`${root}/api`, root);
  const worktree = await register(`${root}/web`, root);
  const subdir = await register(`${root}/api/src`, root);
  const otherRepo = await register("/tmp/bcast-other-repo", "/tmp/bcast-other-repo");

  const res = await broadcast(sender, "the auth contract changed", {
    scope: "repo",
    cwd: `${root}/api`,
    git_root: root,
  });
  expect(res).toEqual({ ok: true, delivered_to: 2 });

  expect(await texts(worktree)).toEqual(["the auth contract changed"]);
  expect(await texts(subdir)).toEqual(["the auth contract changed"]);
  expect(await texts(otherRepo)).toEqual([]);
});

test("a broadcast with nobody to hear it succeeds with delivered_to 0", async () => {
  // Nobody listening is not a failure. Returning an error here would make the tool report a problem
  // for the ordinary case of being the only session in a directory.
  const alone = await register("/tmp/bcast-alone");

  const res = await rawPost(
    "/broadcast-message",
    {
      from_id: alone.id,
      text: "anyone there",
      scope: "directory",
      cwd: "/tmp/bcast-alone",
      git_root: null,
    },
    alone.token
  );

  expect(res.status).toBe(200);
  expect(await res.json()).toEqual({ ok: true, delivered_to: 0 });
  expect(await texts(alone)).toEqual([]);
});

test("scope defaults to machine when the caller omits it", async () => {
  const sender = await register("/tmp/bcast-default-scope");

  const implicit = await broadcast(sender, "no scope given");
  const explicit = await broadcast(sender, "machine scope given", { scope: "machine" });

  expect(implicit.delivered_to).toBeGreaterThan(0);
  expect(implicit.delivered_to).toBe(explicit.delivered_to);
});

test("broadcasting without the sender's own token is refused", async () => {
  // Peer ids are public via /list-peers, so an id alone must not be enough to inject a message into
  // every session on the machine at once. This is the loudest route in the system to leave open.
  const victim = await register("/tmp/bcast-auth-victim");
  const listener = await register("/tmp/bcast-auth-listener");
  const stranger = await register("/tmp/bcast-auth-stranger");

  const body = {
    from_id: victim.id,
    text: "forged broadcast",
    scope: "machine",
    cwd: "/tmp",
    git_root: null,
  };

  expect((await rawPost("/broadcast-message", body)).status).toBe(401);
  expect((await rawPost("/broadcast-message", body, stranger.token)).status).toBe(401);
  expect((await rawPost("/broadcast-message", { ...body, from_id: "" }, victim.token)).status).toBe(
    401
  );

  // Nothing was delivered by any of the refused attempts.
  expect(await texts(listener)).toEqual([]);
});

test("each recipient's copy is acknowledged independently of the others", async () => {
  // Fan-out on write means the copies share nothing but their text. One peer reading and acking
  // must not consume, hide or expire another peer's copy, and a reap of a dead peer must not either.
  const dir = "/tmp/bcast-independent";
  const sender = await register(dir);
  const first = await register(dir);
  const second = await register(dir);

  const res = await broadcast(sender, "acked one at a time", { scope: "directory", cwd: dir });
  expect(res.delivered_to).toBe(2);

  const firstCopy = (await poll(first)).messages;
  const secondCopy = (await poll(second)).messages;
  expect(firstCopy).toHaveLength(1);
  expect(secondCopy).toHaveLength(1);
  // Distinct rows, not one row two peers happen to see.
  expect(firstCopy[0]!.id).not.toBe(secondCopy[0]!.id);

  expect((await ack(first, [firstCopy[0]!.id])).acked).toBe(1);

  expect(await texts(first)).toEqual([]);
  expect(await texts(second)).toEqual(["acked one at a time"]);

  // And the survivor is still ackable on its own terms afterwards.
  expect((await ack(second, [secondCopy[0]!.id])).acked).toBe(1);
  expect(await texts(second)).toEqual([]);
});
