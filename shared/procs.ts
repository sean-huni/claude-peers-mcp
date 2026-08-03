/**
 * Identifying the broker process behind a port.
 *
 * `lsof -ti :PORT` answers "which processes hold a socket on this port", which is not the question
 * anyone stopping the broker is asking: it includes every CONNECTED CLIENT, so the MCP servers of
 * live Claude Code sessions come back alongside the daemon. Signalling that list stops the
 * sessions too.
 *
 * The listening socket is the daemon. Even then the pid is only a claim about a port, and ports
 * get reused, so the process is confirmed to be the broker before anyone signals it.
 */

const BROKER_SCRIPT_NAME = "broker.ts";

function output(cmd: string[]): string {
  const proc = Bun.spawnSync(cmd);
  return new TextDecoder().decode(proc.stdout).trim();
}

/** Pids owning the LISTENING socket on the port. Connected clients are not included. */
export function listeningPids(port: number): number[] {
  return output(["lsof", "-ti", `:${port}`, "-sTCP:LISTEN"])
    .split("\n")
    .map((line) => Number.parseInt(line.trim(), 10))
    .filter((pid) => Number.isInteger(pid) && pid > 0);
}

/** The full command line of a pid, or an empty string if it is already gone. */
export function commandLine(pid: number): string {
  return output(["ps", "-o", "command=", "-p", String(pid)]);
}

/**
 * Pids that are listening on the port AND are running the broker script.
 *
 * The command-line check is what makes signalling safe: if the broker has died and something
 * unrelated has taken the port over, this returns nothing rather than a stranger's pid.
 */
export function listeningBrokerPids(port: number): number[] {
  return listeningPids(port).filter((pid) => commandLine(pid).includes(BROKER_SCRIPT_NAME));
}
