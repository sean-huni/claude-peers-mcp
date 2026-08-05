/**
 * Delivery into a Codex CLI session.
 *
 * A Claude Code session is pushed to over an MCP channel and the message arrives while the model is
 * mid-thought. Codex has no such channel, so its mail lands in the spool and waits. Nothing tells
 * the session it has mail, and a messaging system whose delivery depends on the recipient guessing
 * is not a messaging system.
 *
 * WHAT CODEX ACTUALLY GIVES US (verified against codex-cli 0.145.0, not inferred from another
 * project's README). It has a hook system, `$CODEX_HOME/hooks.json`, with eleven events. Two of
 * them can put text in front of the model:
 *
 *   - `additionalContext` on SessionStart, UserPromptSubmit, PreToolUse, PostToolUse and
 *     SubagentStart. The string is prepended to the turn and the model reads it as context.
 *   - `decision: "block"` with a `reason` on Stop and SubagentStop. This one is the important one:
 *     it refuses to let the finished turn end and feeds `reason` back to the model, which then
 *     keeps going. It fires when a turn completes, so mail reaches an unattended session without
 *     anyone typing.
 *
 * Every other event, SessionEnd and the compaction pair and PermissionRequest, has no field to put
 * a message in. Wiring a drain to one of those would look like it worked, empty the queue, and
 * deliver nothing. That asymmetry is the whole reason this module exists as its own thing rather
 * than as a line in a shell script: `deliveryMode` is the guard, and nothing drains without it.
 *
 * WHAT IT DOES NOT GIVE US. There is no way in to a session that is sitting idle at the prompt. No
 * event fires, and `codex exec resume` runs a separate process against the same rollout file rather
 * than reaching the open TUI. So the residual gap is narrow but real, and `nudgeDecision` names it
 * rather than papering over it. See docs/codex-delivery.md.
 */

import {
  drainSpool,
  findAgentSessionPid,
  foreignLiveSpools,
  hasSpooled,
  spoolDir,
  spoolPath,
  type SpooledMessage,
} from "./spool";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

/** How a given hook invocation can get text in front of the model, if at all. */
export type DeliveryMode = "additionalContext" | "blockAndContinue" | "none";

/** The subset of Codex's hook input this path cares about. */
export interface CodexHookInput {
  hook_event_name: string;
  stop_hook_active: boolean;
  session_id: string | null;
}

/**
 * Events whose output schema carries an `additionalContext` string.
 *
 * Read off the schemas embedded in the 0.145.0 binary. Adding to this list without checking the
 * schema for the target build is how mail starts disappearing.
 */
const CONTEXT_EVENTS = new Set([
  "SessionStart",
  "UserPromptSubmit",
  "PreToolUse",
  "PostToolUse",
  "SubagentStart",
]);

/** Events that carry text only by refusing to stop. */
const BLOCKING_EVENTS = new Set(["Stop", "SubagentStop"]);

/**
 * Whether this invocation has anywhere to put a message.
 *
 * Unknown events return "none". A future Codex build may add an event with a context channel, but
 * assuming it has one costs the queue; assuming it does not costs one delivery cycle.
 */
export function deliveryMode(event: string, stopHookActive: boolean): DeliveryMode {
  if (CONTEXT_EVENTS.has(event)) return "additionalContext";
  // stop_hook_active means a previous block already resumed this turn. Blocking again is how a
  // session spins forever, and Codex passes the flag precisely so hooks can decline the second one.
  if (BLOCKING_EVENTS.has(event) && !stopHookActive) return "blockAndContinue";
  return "none";
}

/**
 * Reads the JSON Codex writes to the hook's stdin.
 *
 * Returns null rather than throwing on anything unexpected. A hook that dies takes its exit code
 * with it, and a non-zero exit from a Stop hook is read by Codex as a blocking decision, so a parse
 * error here could wedge a session rather than merely failing to deliver.
 */
export function parseHookInput(raw: string): CodexHookInput | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return null;
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return null;

  const record = parsed as Record<string, unknown>;
  const event = record["hook_event_name"];
  if (typeof event !== "string" || event.length === 0) return null;

  return {
    hook_event_name: event,
    // Only Stop and SubagentStop carry the flag; absent means this is the first pass.
    stop_hook_active: record["stop_hook_active"] === true,
    session_id: typeof record["session_id"] === "string" ? record["session_id"] : null,
  };
}

/** Builds the exact JSON shape Codex validates for this delivery mode. */
export function renderDelivery(mode: DeliveryMode, event: string, text: string): string {
  if (mode === "additionalContext") {
    // hookEventName is required and must equal the firing event, or Codex rejects the output.
    return JSON.stringify({
      hookSpecificOutput: { hookEventName: event, additionalContext: text },
    });
  }
  if (mode === "blockAndContinue") {
    // Codex refuses decision:block without a non-empty reason, so the message rides in reason.
    return JSON.stringify({ decision: "block", reason: text });
  }
  return "{}";
}

/** The pid of the `codex` process this hook is a descendant of. */
export function findCodexSessionPid(startPid: number = process.pid): number | null {
  return findAgentSessionPid(/(^|\/)codex$/, startPid);
}

/**
 * An empty queue is the normal case, and is reported as nothing. A DIVERGENT queue is not.
 *
 * If this hook resolved host pid A while the MCP server resolved host pid B, mail lands in B's
 * file and this drain reads A's. The queue here stays empty forever, the hook keeps exiting 0
 * with valid JSON, and the messages pile up in a file nobody reads. That is indistinguishable
 * from "no mail" unless somebody looks, so look: any OTHER live session's queue holding
 * messages, while ours is empty, is the signature.
 *
 * Reported through the same delivery channel the mail would have used, because a warning on
 * stderr is a warning nobody sees.
 */
function reportEmptyOrDivergence(pid: number, mode: DeliveryMode, event: string): string {
  const foreign = foreignLiveSpools(pid);
  if (foreign.length === 0) return "{}";
  const detail = foreign.map((f) => `pid ${f.pid} (${f.count} message(s))`).join(", ");
  return renderDelivery(
    mode,
    event,
    `claude-peers: this session resolved host pid ${pid} and its queue (${spoolPath(pid)}) is empty, ` +
      `but ${detail} belongs to another LIVE session and is not. The producer and this hook ` +
      `disagree about which session owns this queue, so mail is being written where nothing reads it. ` +
      `Check that the MCP server and this hook share the same codex ancestor.`
  );
}

/**
 * The whole hook path: decide, drain, render.
 *
 * The ordering is the contract. Delivery mode is settled BEFORE the queue is touched, so an event
 * that cannot deliver never removes a message. `drainSpool` is destructive and there is no putting
 * a message back, which makes "drain, then work out where to send it" the one arrangement that can
 * silently lose mail.
 */
export function drainForHook(raw: string, sessionPid?: number | null): string {
  const input = parseHookInput(raw);
  if (input === null) return "{}";

  const pid = sessionPid ?? findCodexSessionPid();
  if (pid === null) return "{}";

  // Recorded for every event, not only deliverable ones: the nudge decision needs to know the
  // session is turning at all, and a session whose last event was SessionEnd is exactly the case
  // the watchdog must not mistake for idle-with-mail.
  recordHookActivity(pid);

  const mode = deliveryMode(input.hook_event_name, input.stop_hook_active);
  if (mode === "none") return "{}";

  // Cheap check first: the common case is an empty queue on every tool call.
  if (!hasSpooled(pid)) return reportEmptyOrDivergence(pid, mode, input.hook_event_name);

  const messages = drainSpool(pid);
  if (messages.length === 0) return "{}";

  return renderDelivery(mode, input.hook_event_name, formatMessages(messages));
}

/** The text the model sees. Same wording as the Claude Code path, so a peer reads the same either side. */
export function formatMessages(messages: SpooledMessage[]): string {
  const lines: string[] = [
    `${messages.length} new peer message(s). Read them, then reply with the send_message tool.`,
  ];
  for (const message of messages) {
    lines.push("");
    lines.push(`From peer ${message.from_id} at ${message.sent_at}`);
    if (message.from_summary) lines.push(`They are working on: ${message.from_summary}`);
    if (message.from_cwd) lines.push(`Their directory: ${message.from_cwd}`);
    lines.push("");
    lines.push(message.text);
  }
  return lines.join("\n");
}

/* ------------------------------------------------------------------ *
 * Idle detection
 * ------------------------------------------------------------------ */

function activityPath(sessionPid: number): string {
  return join(spoolDir(), `${sessionPid}.seen`);
}

/** Stamps the moment a hook last fired for this session. */
export function recordHookActivity(sessionPid: number, at: number = Date.now()): void {
  try {
    mkdirSync(spoolDir(), { recursive: true, mode: 0o700 });
    writeFileSync(activityPath(sessionPid), String(at), { mode: 0o600 });
  } catch {
    // Losing a stamp costs the watchdog some precision. It must never cost the delivery.
  }
}

/** When a hook last fired, or null if one never has. */
export function lastHookActivity(sessionPid: number): number | null {
  const path = activityPath(sessionPid);
  if (!existsSync(path)) return null;
  try {
    const parsed = Number.parseInt(readFileSync(path, "utf8").trim(), 10);
    return Number.isNaN(parsed) ? null : parsed;
  } catch {
    return null;
  }
}

export type NudgeVerdict = "no-mail" | "deliver-on-next-turn" | "nudge" | "session-gone";

export interface NudgeInputs {
  hasMail: boolean;
  lastActivityMs: number | null;
  nowMs: number;
  alive: boolean;
  idleAfterMs: number;
}

/**
 * Whether a session holding mail needs waking, or will collect it by itself.
 *
 * A session that ran a hook recently is mid-turn, and its Stop hook will deliver when the turn
 * ends. Nudging that one is pure noise. A session quiet for longer has already passed its Stop and
 * is sitting at the prompt, where nothing will fire again until the user types.
 *
 * This function decides; it never drains. Draining from outside the session consumes the message
 * without ever showing it to the model, which is worse than leaving it queued.
 */
export function nudgeDecision(input: NudgeInputs): NudgeVerdict {
  if (!input.alive) return "session-gone";
  if (!input.hasMail) return "no-mail";
  if (input.lastActivityMs === null) return "nudge";
  return input.nowMs - input.lastActivityMs < input.idleAfterMs ? "deliver-on-next-turn" : "nudge";
}
