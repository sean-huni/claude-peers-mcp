/**
 * Tests for the Codex delivery path.
 *
 * The property that matters most here is the one a messaging system dies of quietly: mail must
 * never leave the queue unless it is going somewhere the model will actually read it. Codex fires
 * hooks on eleven events, but only some of them have an output channel that reaches the turn, and
 * the difference is invisible at the call site. A drain wired to SessionEnd would look like it
 * worked, empty the queue on every exit, and deliver nothing, forever.
 *
 * So most of what follows is about refusing to drain, not about draining.
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  drainSpool,
  spoolMessage,
  hasSpooled,
  spoolPath,
  sweepDeadSpools,
  type SpooledMessage,
} from "./spool";
import {
  deliveryMode,
  drainForHook,
  nudgeDecision,
  parseHookInput,
  recordHookActivity,
  lastHookActivity,
  renderDelivery,
} from "./codexdrain";

let dir: string;
const SESSION = 4242;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "peers-codex-"));
  process.env.CLAUDE_PEERS_SPOOL_DIR = dir;
  process.env.CLAUDE_PEERS_SESSION_PID = String(SESSION);
});

afterEach(() => {
  delete process.env.CLAUDE_PEERS_SPOOL_DIR;
  delete process.env.CLAUDE_PEERS_SESSION_PID;
  rmSync(dir, { recursive: true, force: true });
});

function message(id: number, text: string): SpooledMessage {
  return {
    id,
    from_id: "peer1234",
    from_summary: "Working on the mobile client",
    from_cwd: "/Users/sean/env/repo",
    sent_at: "2026-08-04T14:00:00.000Z",
    text,
  };
}

function hookInput(event: string, extra: Record<string, unknown> = {}): string {
  return JSON.stringify({
    session_id: "019fcb79-1899-7671-a25e-0e5e9b15cb99",
    cwd: "/repo",
    hook_event_name: event,
    model: "gpt-5.6-sol",
    permission_mode: "default",
    transcript_path: null,
    ...extra,
  });
}

/**
 * Which events can carry text back into the turn.
 *
 * Taken from the output schemas embedded in codex-cli 0.145.0, not from another project's README:
 * `additionalContext` appears only in the SessionStart, UserPromptSubmit, PreToolUse, PostToolUse
 * and SubagentStart output schemas, and Stop/SubagentStop carry text only through
 * `decision: "block"` with a `reason`. Everything else has nowhere to put a message.
 */
describe("which events can actually deliver", () => {
  test("the context-injecting events report additionalContext", () => {
    for (const event of [
      "SessionStart",
      "UserPromptSubmit",
      "PreToolUse",
      "PostToolUse",
      "SubagentStart",
    ]) {
      expect(deliveryMode(event, false)).toBe("additionalContext");
    }
  });

  test("Stop delivers by blocking its own completion", () => {
    // The one that matters for an unattended session: it fires when the turn ends, so mail lands
    // without the user typing anything.
    expect(deliveryMode("Stop", false)).toBe("blockAndContinue");
    expect(deliveryMode("SubagentStop", false)).toBe("blockAndContinue");
  });

  test("a Stop already resumed by a previous block cannot deliver again", () => {
    // stop_hook_active is Codex's loop guard. Blocking again here is how a session spins forever.
    expect(deliveryMode("Stop", true)).toBe("none");
    expect(deliveryMode("SubagentStop", true)).toBe("none");
  });

  test("events with no output channel report none", () => {
    for (const event of ["SessionEnd", "PreCompact", "PostCompact", "PermissionRequest"]) {
      expect(deliveryMode(event, false)).toBe("none");
    }
  });

  test("an event this build does not have is treated as undeliverable", () => {
    // Unknown must fail closed. Guessing that a new event can carry context is how mail gets eaten.
    expect(deliveryMode("SomeFutureEvent", false)).toBe("none");
  });
});

describe("the wire format Codex expects", () => {
  test("a context delivery names its own event, which Codex requires", () => {
    const out = JSON.parse(renderDelivery("additionalContext", "UserPromptSubmit", "you have mail"));

    expect(out).toEqual({
      hookSpecificOutput: {
        hookEventName: "UserPromptSubmit",
        additionalContext: "you have mail",
      },
    });
  });

  test("a Stop delivery blocks with the message as the reason", () => {
    const out = JSON.parse(renderDelivery("blockAndContinue", "Stop", "you have mail"));

    // Codex rejects decision:block without a non-empty reason, so the text has to ride in reason.
    expect(out).toEqual({ decision: "block", reason: "you have mail" });
  });

  test("nothing to say is an empty object, not empty output", () => {
    expect(renderDelivery("none", "Stop", "ignored")).toBe("{}");
  });
});

describe("draining only when the mail can be delivered", () => {
  test("a deliverable event drains the queue and carries the text", () => {
    spoolMessage(SESSION, message(1, "the broker is up"));

    const out = JSON.parse(drainForHook(hookInput("UserPromptSubmit")));

    expect(out.hookSpecificOutput.additionalContext).toContain("the broker is up");
    expect(hasSpooled(SESSION)).toBe(false);
  });

  test("Stop drains and delivers through reason", () => {
    spoolMessage(SESSION, message(1, "the broker is up"));

    const out = JSON.parse(drainForHook(hookInput("Stop", { stop_hook_active: false })));

    expect(out.decision).toBe("block");
    expect(out.reason).toContain("the broker is up");
    expect(hasSpooled(SESSION)).toBe(false);
  });

  test("an undeliverable event leaves every message in the queue", () => {
    // The whole point. SessionEnd fires on every exit; a drain here would silently eat the queue.
    spoolMessage(SESSION, message(1, "must survive"));

    expect(drainForHook(hookInput("SessionEnd"))).toBe("{}");
    expect(drainSpool(SESSION).map((m) => m.text)).toEqual(["must survive"]);
  });

  test("a re-entered Stop leaves the queue alone", () => {
    spoolMessage(SESSION, message(1, "must survive"));

    expect(drainForHook(hookInput("Stop", { stop_hook_active: true }))).toBe("{}");
    expect(drainSpool(SESSION).map((m) => m.text)).toEqual(["must survive"]);
  });

  test("unparseable hook input drains nothing", () => {
    spoolMessage(SESSION, message(1, "must survive"));

    expect(drainForHook("not json at all")).toBe("{}");
    expect(drainSpool(SESSION).map((m) => m.text)).toEqual(["must survive"]);
  });

  test("an empty queue on a deliverable event says nothing", () => {
    expect(drainForHook(hookInput("UserPromptSubmit"))).toBe("{}");
  });

  test("a queue file holding nothing readable says nothing either", () => {
    // The state the test above cannot reach: hasSpooled only sees that the FILE exists, so a queue
    // whose every line is unreadable, or one another drain took first, gets past it holding no
    // messages. Announcing "0 new peer message(s)" is a nag with nothing attached to it, and on
    // Stop it is a nag that refuses to let the turn end.
    writeFileSync(spoolPath(SESSION), "{ half a line from a crash\n");

    expect(drainForHook(hookInput("Stop", { stop_hook_active: false }))).toBe("{}");
  });

  test("every queued message appears in one delivery", () => {
    spoolMessage(SESSION, message(1, "first"));
    spoolMessage(SESSION, message(2, "second"));

    const out = JSON.parse(drainForHook(hookInput("Stop", { stop_hook_active: false })));

    expect(out.reason).toContain("first");
    expect(out.reason).toContain("second");
  });

  test("the delivery says who sent it, so a reply can be addressed", () => {
    spoolMessage(SESSION, message(1, "ping"));

    const out = JSON.parse(drainForHook(hookInput("UserPromptSubmit")));

    expect(out.hookSpecificOutput.additionalContext).toContain("peer1234");
  });
});

describe("reading Codex hook input", () => {
  test("the event name and loop guard are picked out", () => {
    const parsed = parseHookInput(hookInput("Stop", { stop_hook_active: true }));

    expect(parsed?.hook_event_name).toBe("Stop");
    expect(parsed?.stop_hook_active).toBe(true);
  });

  test("a missing loop guard reads as not-yet-blocked", () => {
    // Only Stop and SubagentStop carry the field; every other event omits it.
    expect(parseHookInput(hookInput("SessionStart"))?.stop_hook_active).toBe(false);
  });

  test("garbage is null rather than a throw, because a hook must never break a session", () => {
    expect(parseHookInput("{oh dear")).toBeNull();
    expect(parseHookInput("[]")).toBeNull();
    expect(parseHookInput("{}")).toBeNull();
  });
});

describe("deciding whether an idle session needs a nudge", () => {
  const IDLE_AFTER = 30_000;
  const NOW = 1_000_000;

  test("mail plus a session that just ran a hook needs no nudge", () => {
    // It is mid-turn. Stop fires when the turn ends and the mail goes with it.
    expect(
      nudgeDecision({
        hasMail: true,
        lastActivityMs: NOW - 1_000,
        nowMs: NOW,
        alive: true,
        idleAfterMs: IDLE_AFTER,
      })
    ).toBe("deliver-on-next-turn");
  });

  test("mail plus a session that has been quiet needs a nudge", () => {
    expect(
      nudgeDecision({
        hasMail: true,
        lastActivityMs: NOW - 120_000,
        nowMs: NOW,
        alive: true,
        idleAfterMs: IDLE_AFTER,
      })
    ).toBe("nudge");
  });

  test("a session that never ran a hook and has mail needs a nudge", () => {
    expect(
      nudgeDecision({
        hasMail: true,
        lastActivityMs: null,
        nowMs: NOW,
        alive: true,
        idleAfterMs: IDLE_AFTER,
      })
    ).toBe("nudge");
  });

  test("no mail is never a nudge, however long it has been quiet", () => {
    expect(
      nudgeDecision({
        hasMail: false,
        lastActivityMs: NOW - 999_999,
        nowMs: NOW,
        alive: true,
        idleAfterMs: IDLE_AFTER,
      })
    ).toBe("no-mail");
  });

  test("a dead session is never nudged, even holding mail", () => {
    expect(
      nudgeDecision({
        hasMail: true,
        lastActivityMs: null,
        nowMs: NOW,
        alive: false,
        idleAfterMs: IDLE_AFTER,
      })
    ).toBe("session-gone");
  });
});

describe("hook activity", () => {
  test("an activity stamp survives to be read back", () => {
    recordHookActivity(SESSION, 1_700_000_000_000);

    expect(lastHookActivity(SESSION)).toBe(1_700_000_000_000);
  });

  test("a session that never ran a hook has no stamp", () => {
    expect(lastHookActivity(9_998)).toBeNull();
  });

  test("a deliverable hook invocation records that the session is alive and turning", () => {
    spoolMessage(SESSION, message(1, "x"));

    const before = Date.now();
    drainForHook(hookInput("UserPromptSubmit"));

    const stamp = lastHookActivity(SESSION);
    expect(stamp).not.toBeNull();
    expect(stamp!).toBeGreaterThanOrEqual(before);
  });

  test("an undeliverable event still records activity", () => {
    // The nudge decision needs to know the session is turning, whether or not this particular
    // event could carry mail. Recording only on delivery would make a busy session look idle.
    drainForHook(hookInput("SessionEnd"));

    expect(lastHookActivity(SESSION)).not.toBeNull();
  });

  test("a dead session's stamp is swept, not left behind forever", () => {
    // A stamp outlives the queue it sits beside: the session drains its mail and then exits, so
    // this is the ordinary end of every Codex session rather than a rare one. The sweep used to
    // look only for queues, which leaks one file per session into a directory nothing else tidies,
    // and is invisible because a stale stamp is a dozen bytes and changes no behaviour.
    const dead = 999_999;
    recordHookActivity(dead);
    expect(lastHookActivity(dead)).not.toBeNull();

    sweepDeadSpools();

    expect(lastHookActivity(dead)).toBeNull();
  });

  test("a LIVE session keeps its stamp", () => {
    // The control. A sweep that removed every stamp would pass the test above on its own, and cost
    // the nudge decision the one fact it has.
    recordHookActivity(process.pid);

    sweepDeadSpools();

    expect(lastHookActivity(process.pid)).not.toBeNull();
  });
});
