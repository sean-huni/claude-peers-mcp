/**
 * Tests for the hygiene of the test suites themselves.
 *
 * A run used to leave the machine dirtier than it found it: roughly ten `peers-test-*` directories
 * per run under TMPDIR, and broker processes still holding test ports whenever a suite failed or
 * was killed. The squatters were the expensive part. A port was picked at random and bound blindly,
 * so a leftover broker from an earlier run answered /health, the suite decided the broker was up,
 * and every test then talked to a stranger with a different database.
 *
 * So: port selection must confirm the port is actually free and say so plainly when it is not, and
 * cleanup must happen whether or not the tests pass.
 */

import { test, expect, afterAll } from "bun:test";
import { mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { releasePort, reserveFreePort } from "./testsupport";
import { listeningPids } from "./shared/procs";

/** Ports this file occupies itself, kept out of the range the suites draw from. */
const SQUAT_LOW = 7825;
const SQUAT_HIGH = 7826;
/** Ports handed to the child suite runs below, one clean and one deliberately occupied. */
const CHILD_PORT_FREE = 7827;
const CHILD_PORT_TAKEN = 7828;

const squatters = new Map<number, ReturnType<typeof Bun.serve>>();
const dirs: string[] = [];
const children: ReturnType<typeof Bun.spawn>[] = [];

afterAll(() => {
  for (const server of squatters.values()) server.stop(true);
  squatters.clear();
  for (const child of children) {
    try {
      child.kill("SIGKILL");
    } catch {
      // already gone
    }
  }
  for (const dir of dirs) rmSync(dir, { recursive: true, force: true });
});

function squat(port: number): void {
  if (squatters.has(port)) return;
  squatters.set(
    port,
    Bun.serve({ port, hostname: "127.0.0.1", fetch: () => new Response("not the broker") })
  );
}

function scratchTmp(): string {
  const dir = mkdtempSync(join(tmpdir(), "peers-hygiene-"));
  dirs.push(dir);
  return dir;
}

/** Run one of the suites in its own process, with its own TMPDIR and its own port range. */
async function runSuite(
  file: string,
  filter: string | null,
  tmp: string,
  port: number
): Promise<{ out: string; exitCode: number }> {
  const args = ["bun", "test", file, ...(filter ? ["-t", filter] : [])];
  const child = Bun.spawn(args, {
    cwd: import.meta.dir,
    env: {
      ...process.env,
      TMPDIR: tmp,
      CLAUDE_PEERS_TEST_PORT_MIN: String(port),
      CLAUDE_PEERS_TEST_PORT_MAX: String(port),
    },
    stdout: "pipe",
    stderr: "pipe",
  });
  children.push(child);
  const [out, err] = await Promise.all([
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  await child.exited;
  return { out: out + err, exitCode: child.exitCode ?? -1 };
}

/** Anything this project created and failed to remove. */
function peersLeftovers(dir: string): string[] {
  return readdirSync(dir).filter((entry) => entry.includes("peers"));
}

test("port selection skips a port something else is already listening on", () => {
  squat(SQUAT_LOW);

  // Repeated, because the old picker was random: one lucky draw proves nothing.
  for (let i = 0; i < 20; i++) {
    const port = reserveFreePort(SQUAT_LOW, SQUAT_HIGH);
    expect(port).toBe(SQUAT_HIGH);
    releasePort(port);
  }
});

test("port selection fails fast and says why when the whole range is taken", () => {
  squat(SQUAT_LOW);
  squat(SQUAT_HIGH);

  const started = Date.now();
  expect(() => reserveFreePort(SQUAT_LOW, SQUAT_HIGH)).toThrow(/no free port/i);
  expect(Date.now() - started).toBeLessThan(5_000);
});

test("a suite run leaves no temp directories and no listening processes behind", async () => {
  const tmp = scratchTmp();

  const { out } = await runSuite("server.test.ts", "acknowledges", tmp, CHILD_PORT_FREE);
  expect(out).toContain("1 pass");

  expect(peersLeftovers(tmp)).toEqual([]);
  expect(listeningPids(CHILD_PORT_FREE)).toEqual([]);
}, 120_000);

test("a suite whose port is occupied fails fast instead of testing against a stranger", async () => {
  squat(CHILD_PORT_TAKEN);
  const tmp = scratchTmp();

  const started = Date.now();
  const { out, exitCode } = await runSuite("cli.test.ts", null, tmp, CHILD_PORT_TAKEN);

  expect(out).toMatch(/no free port/i);
  expect(exitCode).not.toBe(0);
  expect(Date.now() - started).toBeLessThan(30_000);
  expect(peersLeftovers(tmp)).toEqual([]);
}, 120_000);
