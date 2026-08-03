import { afterEach, beforeEach, expect, test, describe } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync, existsSync, readFileSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  drainSpool,
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
