/**
 * Tests for the plumbing between the drain logic and a real Codex session: the hook script Codex
 * executes, the installer that registers it, and the status command that reports what is stuck.
 *
 * All three are the kind of code normally left untested because "it's just plumbing", and the first
 * two have a failure mode that is expensive and silent. The script runs as a Stop hook, where a
 * non-zero exit is read by Codex as a decision to block, so a crash here does not merely fail to
 * deliver, it wedges the session in a loop. The installer edits a file the user already owns.
 */

import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { spoolMessage, drainSpool, type SpooledMessage } from "./spool";
import { installCodexHook, hooksPath, HOOK_EVENTS } from "./bin/install-codex-hook";

const REPO = import.meta.dir;
const SESSION = 5150;

let spool: string;
let codexHome: string;

beforeEach(() => {
  spool = mkdtempSync(join(tmpdir(), "peers-hookspool-"));
  codexHome = mkdtempSync(join(tmpdir(), "peers-codexhome-"));
  // Set before any spoolMessage call, not after. Setting it late once sent a test message to the
  // real ~/.claude-peers/inbox, which is the sort of thing a test suite must not do to a machine
  // that has live sessions on it.
  process.env.CLAUDE_PEERS_SPOOL_DIR = spool;
});

afterEach(() => {
  delete process.env.CLAUDE_PEERS_SPOOL_DIR;
  rmSync(spool, { recursive: true, force: true });
  rmSync(codexHome, { recursive: true, force: true });
});

function message(text: string): SpooledMessage {
  return {
    id: 1,
    from_id: "peer1234",
    from_summary: "Working on the broker",
    from_cwd: "/repo",
    sent_at: "2026-08-04T14:00:00.000Z",
    text,
  };
}

/** Run the real hook script exactly as Codex runs it: JSON on stdin, JSON expected on stdout. */
async function runHook(
  stdin: string,
  envOverrides: Record<string, string> = {}
): Promise<{ out: string; code: number }> {
  const child = Bun.spawn(["bash", join(REPO, "hooks", "codex-peers-inbox.sh")], {
    env: {
      ...process.env,
      CLAUDE_PEERS_SPOOL_DIR: spool,
      CLAUDE_PEERS_SESSION_PID: String(SESSION),
      ...envOverrides,
    },
    stdin: new TextEncoder().encode(stdin),
    stdout: "pipe",
    stderr: "pipe",
  });
  const out = await new Response(child.stdout).text();
  await child.exited;
  return { out, code: child.exitCode ?? -1 };
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

describe("the hook script Codex executes", () => {
  test("a queued message comes out as a Stop block carrying the text", async () => {
    spoolMessage(SESSION, message("the broker is up"));

    const { out, code } = await runHook(hookInput("Stop", { stop_hook_active: false }));

    expect(code).toBe(0);
    const parsed = JSON.parse(out);
    expect(parsed.decision).toBe("block");
    expect(parsed.reason).toContain("the broker is up");
  }, 30_000);

  test("an empty queue produces valid empty JSON and exit 0", async () => {
    const { out, code } = await runHook(hookInput("UserPromptSubmit"));

    expect(code).toBe(0);
    expect(JSON.parse(out)).toEqual({});
  }, 30_000);

  test("garbage on stdin still exits 0 with valid JSON", async () => {
    // A Stop hook exiting non-zero is read as a blocking decision. Crashing here would wedge the
    // session, so the script has to fail open even on input it cannot understand.
    const { out, code } = await runHook("not json at all");

    expect(code).toBe(0);
    expect(JSON.parse(out)).toEqual({});
  }, 30_000);

  test("no bun on PATH still exits 0 with valid JSON, leaving the mail queued", async () => {
    // The failure path that matters and is otherwise never exercised: every happy-path test above
    // passes whether or not the script fails open, because the CLI always answers. A Stop hook that
    // exits non-zero is read by Codex as a blocking decision with no reason, so this is the
    // difference between a broken install being invisible and a broken install wedging a session.
    spoolMessage(SESSION, message("must survive a broken install"));

    // A PATH that still has bash, so the script itself runs, but not bun. Emptying PATH entirely
    // would fail to spawn bash and prove nothing about the script.
    const { out, code } = await runHook(hookInput("Stop", { stop_hook_active: false }), {
      PATH: "/usr/bin:/bin",
    });

    expect(code).toBe(0);
    expect(JSON.parse(out)).toEqual({});
    expect(drainSpool(SESSION).map((m) => m.text)).toEqual(["must survive a broken install"]);
  }, 30_000);

  test("a bun that answers with nothing still produces valid JSON", async () => {
    // The other half of failing open, and the half no happy-path test can reach: `command -v bun`
    // succeeds, the CLI exits 0, and it prints nothing. Empty output is not valid JSON, and Codex
    // reports invalid hook output as an error on a Stop hook. A stub on PATH is the only way to
    // produce that, because the real CLI always prints at least `{}`.
    const stubDir = mkdtempSync(join(tmpdir(), "peers-stubbun-"));
    writeFileSync(join(stubDir, "bun"), "#!/bin/sh\nexit 0\n", { mode: 0o755 });
    spoolMessage(SESSION, message("must survive a silent CLI"));

    try {
      const { out, code } = await runHook(hookInput("Stop", { stop_hook_active: false }), {
        PATH: `${stubDir}:/usr/bin:/bin`,
      });

      expect(code).toBe(0);
      expect(JSON.parse(out)).toEqual({});
      expect(drainSpool(SESSION).map((m) => m.text)).toEqual(["must survive a silent CLI"]);
    } finally {
      rmSync(stubDir, { recursive: true, force: true });
    }
  }, 30_000);

  test("an undeliverable event leaves the message queued", async () => {
    spoolMessage(SESSION, message("must survive"));

    const { out, code } = await runHook(hookInput("SessionEnd"));

    expect(code).toBe(0);
    expect(JSON.parse(out)).toEqual({});
    expect(drainSpool(SESSION).map((m) => m.text)).toEqual(["must survive"]);
  }, 30_000);
});

describe("installing the hook", () => {
  test("a fresh CODEX_HOME gets the hook on every delivering event", () => {
    installCodexHook({ codexHome, repo: REPO });

    const config = JSON.parse(readFileSync(hooksPath(codexHome), "utf8"));
    for (const event of HOOK_EVENTS) {
      expect(config.hooks[event]).toHaveLength(1);
      expect(config.hooks[event][0].hooks[0].command).toContain("codex-peers-inbox.sh");
      expect(config.hooks[event][0].hooks[0].type).toBe("command");
    }
  });

  test("installing twice does not register the hook twice", () => {
    // Codex runs every registered handler, so a duplicate would deliver the mail to a queue that
    // the first handler has already emptied, and the second would emit an empty block.
    installCodexHook({ codexHome, repo: REPO });
    installCodexHook({ codexHome, repo: REPO });

    const config = JSON.parse(readFileSync(hooksPath(codexHome), "utf8"));
    for (const event of HOOK_EVENTS) {
      expect(config.hooks[event]).toHaveLength(1);
    }
  });

  test("a hook someone else installed is left exactly where it was", () => {
    writeFileSync(
      hooksPath(codexHome),
      JSON.stringify({
        hooks: {
          Stop: [{ hooks: [{ type: "command", command: "/usr/local/bin/someone-elses-hook" }] }],
          PreToolUse: [{ hooks: [{ type: "command", command: "/usr/local/bin/guard" }] }],
        },
      })
    );

    installCodexHook({ codexHome, repo: REPO });

    const config = JSON.parse(readFileSync(hooksPath(codexHome), "utf8"));
    // Ours appended to Stop, theirs still first and untouched.
    expect(config.hooks.Stop[0].hooks[0].command).toBe("/usr/local/bin/someone-elses-hook");
    expect(config.hooks.Stop).toHaveLength(2);
    // An event we do not register must be left completely alone.
    expect(config.hooks.PreToolUse).toEqual([
      { hooks: [{ type: "command", command: "/usr/local/bin/guard" }] },
    ]);
  });

  test("a corrupt hooks.json is refused rather than overwritten", () => {
    // The user's file. Replacing it because it did not parse would destroy hooks this project
    // knows nothing about.
    writeFileSync(hooksPath(codexHome), "{ this is not json");

    expect(() => installCodexHook({ codexHome, repo: REPO })).toThrow(/could not be parsed/i);
    expect(readFileSync(hooksPath(codexHome), "utf8")).toBe("{ this is not json");
  });

  test("Stop is registered, because it is the only event an unattended session reaches", () => {
    expect(HOOK_EVENTS).toContain("Stop");
  });

  test("no event without an output channel is ever registered", () => {
    // Registering SessionEnd or a compaction event would drain the queue into nothing.
    for (const event of ["SessionEnd", "PreCompact", "PostCompact", "PermissionRequest"]) {
      expect(HOOK_EVENTS).not.toContain(event);
    }
  });
});

describe("reporting what is stuck", () => {
  /** Run the status command against this suite's spool, with no broker and no peer identity. */
  async function nudgeStatus(): Promise<{ out: string; code: number }> {
    const env = { ...process.env, CLAUDE_PEERS_SPOOL_DIR: spool } as Record<string, string>;
    // The command resolves nothing from the environment, and a stray override here would make the
    // result depend on whoever ran the suite.
    delete env.CLAUDE_PEERS_SESSION_PID;

    const child = Bun.spawn(["bun", join(REPO, "cli.ts"), "codex-nudge-status"], {
      env,
      stdout: "pipe",
      stderr: "pipe",
    });
    const out = await new Response(child.stdout).text();
    await child.exited;
    return { out, code: child.exitCode ?? -1 };
  }

  test("a queue whose session has exited is reported", async () => {
    const gone = 999_999;
    spoolMessage(gone, message("nobody will ever read this"));

    const { out, code } = await nudgeStatus();

    expect(code).toBe(0);
    expect(out).toContain(`${gone}\tsession-gone`);
  }, 30_000);

  test("a live session that is not Codex is not reported at all", async () => {
    // This test process is a live `bun`, which stands in for the live `claude` case: a Claude Code
    // session spools too, and its hook collects on the next tool call without ever writing an
    // activity stamp. Reporting those would mark every one of them as needing a nudge forever, and
    // a report that is mostly false positives is a report nobody reads.
    spoolMessage(process.pid, message("collected by the other hook"));

    const { out } = await nudgeStatus();

    expect(out).not.toContain(String(process.pid));
    expect(out).toContain("No session is holding undelivered mail.");
  }, 30_000);
});
