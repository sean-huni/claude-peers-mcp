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
 * IDENTIFYING A SESSION. Keyed on the pid of the agent process, which both sides can determine
 * without agreeing on anything: the server is its child, and the hook is its descendant. Cwd is not
 * enough, because two sessions in one checkout is the normal case here, and that is exactly when
 * this matters most.
 */
import { appendFileSync, mkdirSync, readFileSync, renameSync, rmSync, existsSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

export interface SpooledMessage {
  id: number;
  from_id: string;
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

/**
 * The pid of the agent session process this one belongs to.
 *
 * Walks the parent chain rather than reading a single level, because the caller may be a hook
 * running under a shell under the agent, and the depth is not fixed. Returns null rather than
 * guessing: a wrong pid would write into another session's queue, which is worse than not
 * delivering.
 *
 * STOPS AT THE NEAREST AGENT, which is the whole reason this is a walk and not a search for
 * `claude` specifically. A Codex session is very often started FROM a Claude Code session (a
 * subagent shelling out, which is the normal way it happens here), so its process tree really does
 * contain a live `claude` several levels up. Verified on darwin, codex-cli 0.145.0: the MCP server's
 * parent is `codex` itself, and four levels above it sits the `claude` that spawned it. Walking past
 * the `codex` to that `claude` does not fail loudly, it silently spools one agent's messages into
 * another agent's context, which is precisely the misrouting the null-rather-than-guess rule exists
 * to prevent.
 */
export function findSessionPid(startPid: number = process.pid, maxDepth = 12): number | null {
  // Escape hatch for tests and for hosts where the process tree is not readable. Never set in a
  // real session: two sessions sharing this value would share a queue.
  const override = process.env.CLAUDE_PEERS_SESSION_PID;
  if (override) {
    const parsed = Number.parseInt(override, 10);
    if (!Number.isNaN(parsed)) return parsed;
  }

  let pid = startPid;
  for (let depth = 0; depth < maxDepth; depth++) {
    const parent = parentOf(pid);
    if (parent === null || parent <= 1) return null;
    if (isAgentSession(parent)) return parent;
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
 * The executables that constitute an agent session, and therefore a queue of their own.
 *
 * Both host this MCP server over stdio and both spawn it as a direct child, so either one being the
 * nearest such ancestor identifies the session the server belongs to.
 *
 * Exact names only. `codex-fugu` is deliberately absent: it is the same CLI pointed at a different
 * provider for one-shot second opinions, never a session that holds a peer identity, and matching it
 * would hand a queue to a process that exits before anything could drain it.
 */
const AGENT_EXECUTABLES = /(^|\/)(claude|codex)$/;

function isAgentSession(pid: number): boolean {
  const result = Bun.spawnSync(["ps", "-o", "command=", "-p", String(pid)]);
  const command = new TextDecoder().decode(result.stdout).trim();
  // The binary, not merely the word: a shell whose command line MENTIONS claude or codex, which is
  // most of them in this estate, must not be mistaken for the session itself.
  const executable = command.split(/\s+/)[0] ?? "";
  return AGENT_EXECUTABLES.test(executable);
}

/** Removes queues belonging to sessions that are no longer running. */
export function sweepDeadSpools(): void {
  const dir = spoolDir();
  if (!existsSync(dir)) return;
  for (const entry of new Bun.Glob("*.jsonl").scanSync(dir)) {
    const pid = Number.parseInt(entry.replace(/\.jsonl$/, ""), 10);
    if (Number.isNaN(pid)) continue;
    try {
      process.kill(pid, 0);
    } catch {
      rmSync(join(dir, entry), { force: true });
    }
  }
}
