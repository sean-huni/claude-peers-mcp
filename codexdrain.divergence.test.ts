/**
 * Host-pid divergence must be LOUD.
 *
 * The producer (the MCP server) and the consumer (this delivery hook) each resolve a host session
 * pid independently, by walking their own ancestors. When the two answers differ, mail is written
 * to one queue file and read from another. Every component then behaves exactly as designed: the
 * hook finds an empty queue, returns valid JSON, exits 0, and reports no mail. Forever.
 *
 * Found while simulating a Claude peer against a Codex peer (2026-08-05): a harness that spawned
 * the hook under a DIFFERENT codex process than the server produced exactly this. The producer
 * wrote 53931.jsonl, the hook answered `{}`, and one message sat unread with nothing anywhere
 * saying so. It was a harness bug, but a real deployment can diverge the same way, and the
 * symptom would be identical and equally invisible.
 *
 * So: an empty queue while ANOTHER LIVE session's queue holds messages is not "no mail", it is a
 * misconfiguration, and it is reported through the same channel the mail would have used.
 */

import { test, expect, beforeEach, afterEach } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { drainForHook } from "./codexdrain";

let dir: string;
const held: ReturnType<typeof Bun.spawn>[] = [];
const previous = process.env.CLAUDE_PEERS_SPOOL_DIR;

/** A queue file belonging to a process that is genuinely alive. */
function liveQueue(count: number): number {
  const proc = Bun.spawn(["sleep", "120"], { stdout: "ignore", stderr: "ignore" });
  held.push(proc);
  const lines = Array.from({ length: count }, (_, i) =>
    JSON.stringify({
      id: i + 1,
      from_id: "aaaaaaaa",
      from_summary: "",
      from_cwd: "/tmp",
      sent_at: new Date(0).toISOString(),
      text: `stranded message ${i + 1}`,
    })
  );
  writeFileSync(join(dir, `${proc.pid}.jsonl`), lines.join("\n") + "\n", { mode: 0o600 });
  return proc.pid;
}

/** A pid that is alive but owns no queue: this session, in the tests below. */
function liveSession(): number {
  const proc = Bun.spawn(["sleep", "120"], { stdout: "ignore", stderr: "ignore" });
  held.push(proc);
  return proc.pid;
}

const stopEvent = JSON.stringify({
  hook_event_name: "Stop",
  session_id: "divergence",
  stop_hook_active: false,
});

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "peers-divergence-"));
  process.env.CLAUDE_PEERS_SPOOL_DIR = dir;
});

afterEach(() => {
  for (const p of held.splice(0)) p.kill();
  rmSync(dir, { recursive: true, force: true });
  if (previous === undefined) delete process.env.CLAUDE_PEERS_SPOOL_DIR;
  else process.env.CLAUDE_PEERS_SPOOL_DIR = previous;
});

test("an empty queue with no other queues stays silent", () => {
  // The overwhelmingly common case. It must not become chatty, or the real signal drowns.
  const mine = liveSession();
  expect(drainForHook(stopEvent, mine)).toBe("{}");
});

test("an empty queue is NOT reported as divergence when the other queue's owner is dead", async () => {
  // A dead session's leftovers are sweepDeadSpools' job. Reporting them as divergence would fire
  // on every machine that has ever had a session exit, which is every machine.
  const mine = liveSession();
  const corpse = Bun.spawn(["sleep", "120"], { stdout: "ignore", stderr: "ignore" });
  const deadPid = corpse.pid;
  corpse.kill();
  // AWAIT the exit, do not just signal it. A killed child that has not been reaped is a zombie,
  // and `process.kill(pid, 0)` succeeds on a zombie, so an un-awaited kill leaves a pid that
  // reads as alive and makes this test fail for a reason that has nothing to do with the code.
  await corpse.exited;
  writeFileSync(
    join(dir, `${deadPid}.jsonl`),
    JSON.stringify({ id: 1, from_id: "b", from_summary: "", from_cwd: "/tmp", sent_at: "", text: "x" }) + "\n"
  );
  expect(drainForHook(stopEvent, mine)).toBe("{}");
});

test("an empty queue beside a LIVE foreign queue is reported, not swallowed", () => {
  const strandedPid = liveQueue(3);
  const mine = liveSession();

  const out = drainForHook(stopEvent, mine);
  expect(out, "divergence must not be reported as an empty result").not.toBe("{}");

  const parsed = JSON.parse(out) as { decision?: string; reason?: string };
  // It rides the same delivery channel the mail would have, so the model actually sees it.
  expect(parsed.decision).toBe("block");
  const reason = String(parsed.reason);
  expect(reason).toContain("claude-peers");
  // Both pids named, so the reader can compare them without more tooling.
  expect(reason).toContain(String(mine));
  expect(reason).toContain(String(strandedPid));
  expect(reason).toContain("3 message(s)");
});

test("the stranded queue is NOT drained by the divergence report", () => {
  // Reporting must be non-destructive: those messages belong to the other session, and drainSpool
  // is irreversible. Consuming them here would turn a visible misconfiguration into real data loss.
  const strandedPid = liveQueue(2);
  const mine = liveSession();

  drainForHook(stopEvent, mine);

  const text = readFileSync(join(dir, `${strandedPid}.jsonl`), "utf8");
  expect(text.split("\n").filter((l) => l.trim())).toHaveLength(2);
});

test("a real message in OUR queue still delivers normally, and reports no divergence", () => {
  // The detector must not shadow the happy path: our own mail wins.
  const strandedPid = liveQueue(1);
  const mine = liveSession();
  writeFileSync(
    join(dir, `${mine}.jsonl`),
    JSON.stringify({
      id: 9,
      from_id: "cccccccc",
      from_summary: "",
      from_cwd: "/tmp",
      sent_at: new Date(0).toISOString(),
      text: "mail for me",
    }) + "\n"
  );

  const out = drainForHook(stopEvent, mine);
  const parsed = JSON.parse(out) as { decision?: string; reason?: string };
  expect(parsed.decision).toBe("block");
  expect(String(parsed.reason)).toContain("mail for me");
  expect(String(parsed.reason), "our own mail must not trigger the divergence text").not.toContain(
    "disagree about which session owns"
  );
  expect(strandedPid).toBeGreaterThan(0);
});
