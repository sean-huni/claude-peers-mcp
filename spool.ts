/**
 * The delivery path for sessions that cannot be pushed to.
 *
 * Channel push only works when Claude Code is launched with
 * `--dangerously-load-development-channels server:claude-peers`. Started the ordinary way, from
 * `.mcp.json`, the tools work and NOTHING ever surfaces an inbound message: it sits in the broker
 * until the model happens to call check_messages, which it has no reason to do. A messaging system
 * whose delivery depends on the recipient guessing that something arrived is not a messaging
 * system.
 *
 * So when push is unavailable the message is written here instead, and a Claude Code hook drains
 * this file into the session's context. The hook is the transport; this file is the queue between
 * them.
 *
 * WHY A FILE AND NOT THE BROKER. The hook is a short-lived process with no peer identity and no
 * auth token, and handing it one would put a credential that can read a session's messages into
 * every hook invocation. The MCP server already holds that token, already polls, and already knows
 * which session it belongs to. Writing a file it owns keeps the token where it is.
 *
 * IDENTIFYING A SESSION. Keyed on the pid of the HOST process, `claude` or `codex`, which both
 * sides can determine without agreeing on anything: the server is its child, and the hook is its
 * descendant. Cwd is not enough, because two sessions in one checkout is the normal case here, and
 * that is exactly when this matters most.
 *
 * The Codex half of the story, which events can carry a message and which cannot, is in
 * codexdrain.ts. This file only has to know that a second host exists and key its queue the same
 * way: see findHostSessionPid, and docs/codex-delivery.md.
 */
import { appendFileSync, mkdirSync, readFileSync, renameSync, rmSync, existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

export interface SpooledMessage {
  id: number;
  from_id: string;
  // Sender's claimed peer name, empty when unnamed. Optional so spool files
  // written by an older server still parse.
  from_name?: string;
  from_summary: string;
  from_cwd: string;
  sent_at: string;
  text: string;
}

export function spoolDir(): string {
  return process.env.CLAUDE_PEERS_SPOOL_DIR ?? join(homedir(), ".claude-peers", "inbox");
}

export function spoolPath(sessionPid: number): string {
  return join(spoolDir(), `${sessionPid}.jsonl`);
}

/**
 * Appends one message, creating the directory on first use.
 *
 * Line-delimited JSON and append-only so a crash mid-write costs at most the line being written,
 * and never the messages already queued. The caller acks the broker only after this returns, which
 * is what makes a lost write a retry rather than a lost message.
 */
export function spoolMessage(sessionPid: number, message: SpooledMessage): void {
  mkdirSync(spoolDir(), { recursive: true, mode: 0o700 });
  appendFileSync(spoolPath(sessionPid), JSON.stringify(message) + "\n", { mode: 0o600 });
}

/**
 * Reads and REMOVES everything queued for a session.
 *
 * Renames before reading, so a message arriving mid-drain lands in a fresh file rather than being
 * deleted unread. Truncating in place would lose anything written between the read and the
 * truncate, which on a 1s poll loop is a real window and not a theoretical one.
 */
export function drainSpool(sessionPid: number): SpooledMessage[] {
  const path = spoolPath(sessionPid);
  if (!existsSync(path)) return [];

  const taken = `${path}.draining`;
  try {
    renameSync(path, taken);
  } catch {
    // Another drain got there first. Nothing to do, and nothing lost.
    return [];
  }

  try {
    return readFileSync(taken, "utf8")
      .split("\n")
      .filter((line) => line.trim().length > 0)
      .flatMap((line) => {
        try {
          return [JSON.parse(line) as SpooledMessage];
        } catch {
          // One unreadable line must not swallow the rest of the queue.
          return [];
        }
      });
  } finally {
    rmSync(taken, { force: true });
  }
}

/** True when anything is waiting. Cheap enough to call on every tool invocation. */
export function hasSpooled(sessionPid: number): boolean {
  return existsSync(spoolPath(sessionPid));
}

const CLAUDE = /(^|\/)claude$/;
const CODEX = /(^|\/)codex$/;

/**
 * Every host agent that can own a queue.
 *
 * Both hosts spool into this one directory keyed on their own pid, so the only question a producer
 * has to answer is which session it is running inside.
 */
const HOSTS = [CLAUDE, CODEX];

/**
 * The pid of the `claude` process this one belongs to.
 *
 * Walks the parent chain rather than reading a single level, because the caller may be a hook
 * running under a shell under Claude Code, and the depth is not fixed. Returns null rather than
 * guessing: a wrong pid would write into another session's queue, which is worse than not
 * delivering.
 */
export function findSessionPid(startPid: number = process.pid, maxDepth = 12): number | null {
  return findAgentSessionPid(CLAUDE, startPid, maxDepth);
}

/**
 * The pid of the host agent session this process belongs to, whichever host that is.
 *
 * What the MCP server calls, because it is started by both and cannot know which. NEAREST ancestor
 * wins, and that is a correctness rule rather than a shortcut: a Codex session launched from a
 * Claude Code shell has BOTH above it, and the mail belongs to the codex turn whose hook will
 * deliver it, not to the claude session that happened to spawn it. Checking every host at each
 * level, rather than walking once per host, is what makes "nearest" mean nearest.
 */
export function findHostSessionPid(startPid: number = process.pid, maxDepth = 12): number | null {
  return findAgentSessionPid(HOSTS, startPid, maxDepth);
}

/**
 * The same walk, ending at a named executable.
 *
 * Codex sessions queue into the same spool and are found the same way, so the only thing that
 * differs is which executable ends the walk. Kept as one function rather than two copies, because
 * the subtle part is the executable-vs-command-line match below and a second copy of it would
 * drift.
 */
export function findAgentSessionPid(
  executable: RegExp | RegExp[],
  startPid: number = process.pid,
  maxDepth = 12
): number | null {
  // Escape hatch for tests and for hosts where the process tree is not readable. Never set in a
  // real session: two sessions sharing this value would share a queue.
  const override = process.env.CLAUDE_PEERS_SESSION_PID;
  if (override) {
    const parsed = Number.parseInt(override, 10);
    if (!Number.isNaN(parsed)) return parsed;
  }

  const wanted = Array.isArray(executable) ? executable : [executable];
  let pid = startPid;
  for (let depth = 0; depth < maxDepth; depth++) {
    const parent = parentOf(pid);
    if (parent === null || parent <= 1) return null;
    // One `ps` per level, tested against every host, rather than one walk per host: two walks would
    // return the OUTER match when both hosts are above us, which is the wrong session.
    const exe = executableOf(parent);
    if (wanted.some((pattern) => pattern.test(exe))) return parent;
    pid = parent;
  }
  return null;
}

function parentOf(pid: number): number | null {
  const result = Bun.spawnSync(["ps", "-o", "ppid=", "-p", String(pid)]);
  const raw = new TextDecoder().decode(result.stdout).trim();
  const parsed = Number.parseInt(raw, 10);
  return Number.isNaN(parsed) ? null : parsed;
}

/**
 * The executable a pid is running, or "" if it is gone.
 *
 * The binary, not merely the word: a shell whose command line MENTIONS claude, which is most of
 * them in this estate, must not be mistaken for the session itself.
 */
export function executableOf(pid: number): string {
  const result = Bun.spawnSync(["ps", "-o", "command=", "-p", String(pid)]);
  const command = new TextDecoder().decode(result.stdout).trim();
  return command.split(/\s+/)[0] ?? "";
}

/** Whether a live pid is running the named executable. False for a pid that is gone. */
export function executableMatches(pid: number, executable: RegExp): boolean {
  const exe = executableOf(pid);
  return exe !== "" && executable.test(exe);
}

/**
 * Removes everything belonging to sessions that are no longer running.
 *
 * BOTH files a session leaves here: the queue itself and the `.seen` activity stamp the Codex hook
 * writes beside it. Sweeping only the queue leaks one stamp per session that ever ran a hook, in a
 * directory nothing else tidies, and the leak is invisible because a stale stamp is 13 bytes and
 * changes no behaviour.
 */
/**
 * Queue files that exist, are non-empty, and belong to a LIVING process other than `mine`.
 *
 * This exists to make one specific failure visible. The producer (the MCP server, walking its
 * ancestors) and the consumer (the delivery hook, walking its own) each resolve a host pid
 * independently. When those two answers differ, mail is written to one file and read from
 * another: the hook reports "no mail" forever, exits 0 with valid JSON, and looks perfectly
 * healthy while the queue grows beside it. Nothing in the system notices, because every
 * component is behaving exactly as designed.
 *
 * Returning the other live queues lets the caller say so out loud instead.
 */
export function foreignLiveSpools(mine: number | null): { pid: number; count: number }[] {
  const dir = spoolDir();
  if (!existsSync(dir)) return [];
  const found: { pid: number; count: number }[] = [];
  for (const entry of new Bun.Glob("*.jsonl").scanSync(dir)) {
    const pid = Number.parseInt(entry.replace(/\.jsonl$/, ""), 10);
    if (Number.isNaN(pid) || pid === mine) continue;
    try {
      // A dead session's leftovers are sweepDeadSpools' problem, not a divergence.
      process.kill(pid, 0);
    } catch {
      continue;
    }
    try {
      const lines = readFileSync(join(dir, entry), "utf8").split("\n").filter((l) => l.trim());
      if (lines.length > 0) found.push({ pid, count: lines.length });
    } catch {
      // Unreadable is not evidence of divergence.
    }
  }
  return found;
}

export function sweepDeadSpools(): void {
  const dir = spoolDir();
  if (!existsSync(dir)) return;
  for (const entry of new Bun.Glob("*.{jsonl,seen}").scanSync(dir)) {
    const pid = Number.parseInt(entry.replace(/\.(jsonl|seen)$/, ""), 10);
    if (Number.isNaN(pid)) continue;
    try {
      process.kill(pid, 0);
    } catch {
      rmSync(join(dir, entry), { force: true });
    }
  }
}
