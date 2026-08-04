import { afterAll, afterEach, beforeEach, expect, test, describe } from "bun:test";
import {
  mkdtempSync,
  rmSync,
  symlinkSync,
  writeFileSync,
  existsSync,
  readFileSync,
  statSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  drainSpool,
  findHostSessionPid,
  findSessionPid,
  hasSpooled,
  spoolMessage,
  spoolPath,
  sweepDeadSpools,
  type SpooledMessage,
} from "./spool";

let dir: string;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), "peers-spool-"));
  process.env.CLAUDE_PEERS_SPOOL_DIR = dir;
});

afterEach(() => {
  delete process.env.CLAUDE_PEERS_SPOOL_DIR;
  rmSync(dir, { recursive: true, force: true });
});

function message(id: number, text: string): SpooledMessage {
  return {
    id,
    from_id: "peer1234",
    from_summary: "Working on the mobile client",
    from_cwd: "/Users/sean/env/repo",
    sent_at: "2026-08-03T14:00:00.000Z",
    text,
  };
}

describe("spooling", () => {
  test("a spooled message comes back with every field intact", () => {
    spoolMessage(4242, message(1, "the drain is in"));

    const drained = drainSpool(4242);

    expect(drained).toHaveLength(1);
    expect(drained[0]).toEqual(message(1, "the drain is in"));
  });

  test("messages keep their order, because a conversation read backwards is not a conversation", () => {
    spoolMessage(4242, message(1, "first"));
    spoolMessage(4242, message(2, "second"));
    spoolMessage(4242, message(3, "third"));

    expect(drainSpool(4242).map((m) => m.text)).toEqual(["first", "second", "third"]);
  });

  test("draining removes them, so the same message is not delivered twice", () => {
    spoolMessage(4242, message(1, "once"));

    expect(drainSpool(4242)).toHaveLength(1);
    expect(drainSpool(4242)).toHaveLength(0);
  });

  test("two sessions in one checkout do not read each other's messages", () => {
    // The case this whole keying scheme exists for. Cwd cannot separate these, and here that is
    // the normal arrangement rather than an edge case.
    spoolMessage(1111, message(1, "for the backend session"));
    spoolMessage(2222, message(2, "for the mobile session"));

    expect(drainSpool(1111).map((m) => m.text)).toEqual(["for the backend session"]);
    expect(drainSpool(2222).map((m) => m.text)).toEqual(["for the mobile session"]);
  });

  test("an empty queue is not an error", () => {
    expect(drainSpool(9999)).toEqual([]);
    expect(hasSpooled(9999)).toBe(false);
  });

  test("one corrupt line does not swallow the messages around it", () => {
    spoolMessage(4242, message(1, "before"));
    writeFileSync(spoolPath(4242), "{not json at all\n", { flag: "a" });
    spoolMessage(4242, message(2, "after"));

    // A half-written line from a crash must cost that line and nothing else.
    expect(drainSpool(4242).map((m) => m.text)).toEqual(["before", "after"]);
  });

  test("a message arriving mid-drain is not destroyed unread", () => {
    spoolMessage(4242, message(1, "already queued"));

    // drainSpool renames before reading. Anything written after that rename lands in a fresh file,
    // which is the difference between this and truncating in place: on a one-second poll loop the
    // window between read and truncate is real, and a message lost in it is lost silently.
    const drained = drainSpool(4242);
    spoolMessage(4242, message(2, "arrived during the drain"));

    expect(drained.map((m) => m.text)).toEqual(["already queued"]);
    expect(drainSpool(4242).map((m) => m.text)).toEqual(["arrived during the drain"]);
  });

  test("the queue is not world-readable, because messages are private", () => {
    spoolMessage(4242, message(1, "sensitive"));

    // statSync directly: Bun.file(...).stat is a method reference, so using it
    // as a condition is always true and typechecks as an error.
    const mode = statSync(spoolPath(4242)).mode & 0o777;
    expect(mode & 0o077).toBe(0);
  });

  test("no leftover .draining file survives a drain", () => {
    spoolMessage(4242, message(1, "x"));
    drainSpool(4242);

    expect(existsSync(`${spoolPath(4242)}.draining`)).toBe(false);
  });
});

describe("sweeping", () => {
  test("a queue for a dead session is removed", () => {
    // Pid 1 is init and always alive; a very high pid is reliably not running here.
    const dead = 999_999;
    spoolMessage(dead, message(1, "nobody will ever read this"));
    expect(existsSync(spoolPath(dead))).toBe(true);

    sweepDeadSpools();

    expect(existsSync(spoolPath(dead))).toBe(false);
  });

  test("a queue for a LIVE session is left alone", () => {
    // The control. A sweep that deleted everything would pass the test above on its own.
    spoolMessage(process.pid, message(1, "still wanted"));

    sweepDeadSpools();

    expect(existsSync(spoolPath(process.pid))).toBe(true);
    expect(drainSpool(process.pid)).toHaveLength(1);
  });
});

describe("finding the session", () => {
  /**
   * Whether this suite is itself running inside Claude Code.
   *
   * The two cases assert opposite things and both are real: run from a terminal there is no claude
   * ancestor, and run from an agent there is. Asserting only one of them makes the suite pass or
   * fail on who happened to type `bun test`, which is how a harness ends up testing its own
   * environment instead of the code.
   */
  const found = findSessionPid();

  test("resolves the claude process when there is one above us", () => {
    if (found === null) return; // not under Claude Code, covered by the next test

    const command = new TextDecoder()
      .decode(Bun.spawnSync(["ps", "-o", "command=", "-p", String(found)]).stdout)
      .trim();
    // The pid it found must actually BE claude. Without this the walk could return any ancestor
    // and the test would still pass.
    expect(command.split(/\s+/)[0]).toMatch(/(^|\/)claude$/);
  });

  test("returns null rather than guessing when no claude is above us", () => {
    if (found !== null) return; // running under Claude Code, covered by the previous test

    expect(findSessionPid()).toBeNull();
  });

  test("gives up rather than walking all the way to init", () => {
    // A bounded walk, so a deep process tree cannot cost an unbounded number of `ps` calls.
    expect(findSessionPid(process.pid, 0)).toBeNull();
  });

  test("does not mistake a process that merely mentions claude for the session", () => {
    // Every shell in this estate has "claude" somewhere in its command line. Matching on the
    // executable rather than the whole string is the only thing keeping this honest, and pid 1 is
    // a stable stand-in for "an ancestor that is not claude".
    expect(findSessionPid(1, 4)).toBeNull();
  });
});

/**
 * Resolving the host session under a REAL process tree.
 *
 * The producer half of the Codex path, and the half that is easy to leave out: everything else can
 * be built and tested while nothing ever writes to the queue, and the symptom is a hook that runs
 * on every turn and correctly reports that there is no mail.
 *
 * These build the tree rather than asserting about a regular expression. A `ps` reports the path a
 * process was launched with, so a symlink to bun named `codex` IS a codex as far as this walk can
 * tell, and that is exactly the thing under test.
 */
describe("finding the host session, whichever host it is", () => {
  let dir: string;

  /** A script that runs `argv` and relays its stdout, so the tree below can be built one level at a time. */
  function relay(target: string[]): string {
    return (
      `const child = Bun.spawnSync(${JSON.stringify(target)}, { stdout: "pipe", stderr: "inherit" });\n` +
      `process.stdout.write(child.stdout);\n`
    );
  }

  function hostTree(): string {
    if (!dir) {
      dir = mkdtempSync(join(tmpdir(), "peers-hosts-"));
      // Named exactly as the real binaries are, because the walk matches on the executable.
      symlinkSync(process.execPath, join(dir, "codex"));
      symlinkSync(process.execPath, join(dir, "claude"));

      writeFileSync(
        join(dir, "probe.ts"),
        `import { findHostSessionPid, executableOf } from ${JSON.stringify(join(import.meta.dir, "spool"))};\n` +
          `const pid = findHostSessionPid();\n` +
          `console.log(JSON.stringify({ pid, exe: pid === null ? null : executableOf(pid) }));\n`
      );
      // The probe must be a CHILD of the host, never the host itself. `<dir>/codex probe.ts` is one
      // process, not two: the walk starts at the parent and would sail straight past it, which is
      // the real shape of the arrangement anyway, since an MCP server is spawned BY its host.
      writeFileSync(join(dir, "spawn-probe.ts"), relay([process.execPath, join(dir, "probe.ts")]));
      writeFileSync(
        join(dir, "spawn-codex.ts"),
        relay([join(dir, "codex"), join(dir, "spawn-probe.ts")])
      );
    }
    return dir;
  }

  /** Env with the test escape hatch removed, so the walk has to do the real work. */
  function realWalkEnv(): Record<string, string> {
    const env = { ...process.env } as Record<string, string>;
    delete env.CLAUDE_PEERS_SESSION_PID;
    return env;
  }

  function runUnder(...argv: string[]): { pid: number | null; exe: string | null } {
    const result = Bun.spawnSync(argv, { env: realWalkEnv(), stdout: "pipe", stderr: "pipe" });
    const out = new TextDecoder().decode(result.stdout).trim();
    const err = new TextDecoder().decode(result.stderr).trim();
    // Fail closed and loudly. A crashed child prints nothing, and JSON.parse("") throwing here is
    // the difference between a diagnosed failure and a test that quietly stops testing anything.
    if (out.length === 0) throw new Error(`probe produced no output. stderr: ${err}`);
    return JSON.parse(out);
  }

  afterAll(() => {
    if (dir) rmSync(dir, { recursive: true, force: true });
  });

  test("a codex ancestor is found, so a Codex session has somewhere to spool", () => {
    // Without this the MCP server started by Codex resolves no session at all, the poll loop skips
    // every message for want of anywhere to put it, and the drain hook empties a queue that nothing
    // ever filled. The whole delivery path is then indistinguishable from working.
    const home = hostTree();

    const found = runUnder(join(home, "codex"), join(home, "spawn-probe.ts"));

    expect(found.pid).not.toBeNull();
    expect(found.exe).toMatch(/(^|\/)codex$/);
  }, 30_000);

  test("a claude ancestor is still found by the same walk", () => {
    // The control for the test above. Teaching this walk about Codex must not cost it Claude Code,
    // and losing that would be silent: every Claude session would simply stop spooling, which looks
    // exactly like a quiet day.
    const home = hostTree();

    const found = runUnder(join(home, "claude"), join(home, "spawn-probe.ts"));

    expect(found.pid).not.toBeNull();
    expect(found.exe).toMatch(/(^|\/)claude$/);
  }, 30_000);

  test("the NEAREST host wins when a codex is running inside a claude", () => {
    // A Codex session started from a Claude Code shell has both above it. The mail belongs to the
    // codex turn whose hook will deliver it: resolving the outer claude would spool one session's
    // messages into another session's queue, where the right hook never looks.
    const home = hostTree();

    const found = runUnder(join(home, "claude"), join(home, "spawn-codex.ts"));

    expect(found.exe).toMatch(/(^|\/)codex$/);
  }, 30_000);

  test("neither host above us is still null rather than a guess", () => {
    // pid 1 is launchd, and nothing above it is an agent session. A walk that returned some
    // ancestor anyway would write into a queue belonging to nobody.
    expect(findHostSessionPid(1, 4)).toBeNull();
  });
});
