#!/usr/bin/env bun
/**
 * Registers the peer-inbox hook in a Codex CLI installation.
 *
 * Usage:
 *   bun bin/install-codex-hook.ts            Install into $CODEX_HOME (default ~/.codex)
 *   bun bin/install-codex-hook.ts --dry-run  Print what would be written and change nothing
 *
 * WHICH EVENTS, AND WHY ONLY THESE. Codex fires eleven hook events but only some can put text in
 * front of the model, and the rest would drain the queue into nothing. Three are enough:
 *
 *   Stop              fires when a turn completes, and delivers by refusing to complete. This is
 *                     the load-bearing one: it is the only event an unattended session reaches.
 *   UserPromptSubmit  fires when the user types, so mail is already in context for that turn.
 *   SessionStart      catches whatever was queued while the session was not running.
 *
 * PreToolUse and PostToolUse can also carry context and would deliver sooner mid-task, but they
 * fire on every single tool call, and paying a process spawn per tool call to shorten a wait that
 * Stop already bounds to one turn is a bad trade. Stop is the mechanism; the other two are just the
 * cheap moments where mail is already on the way in.
 *
 * TRUST. Codex will not run a hook it has not been shown: it hashes the config and records the hash
 * under `hooks.state."<path>".trusted_hash` in config.toml, and the TUI asks for approval at
 * startup whenever the hash changes. So installing is not enough, the user has to approve it once,
 * and again after any edit to this file. That is deliberate on Codex's part and this installer does
 * not try to route around it: writing a trust hash on the user's behalf would defeat the only thing
 * standing between a config file and arbitrary code execution.
 */

import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join, resolve } from "node:path";

/**
 * The events this hook is registered on.
 *
 * Every one of these has a verified output channel that reaches the model. Nothing else belongs
 * here: see the note above, and `deliveryMode` in codexdrain.ts for the full table.
 */
export const HOOK_EVENTS = ["SessionStart", "UserPromptSubmit", "Stop"] as const;

/** Codex reads user hooks from this file. Verified against codex-cli 0.145.0. */
export function hooksPath(codexHome: string): string {
  return join(codexHome, "hooks.json");
}

export function defaultCodexHome(): string {
  return process.env.CODEX_HOME ?? join(homedir(), ".codex");
}

interface HookHandler {
  type: string;
  command: string;
}

interface MatcherGroup {
  matcher?: string;
  hooks: HookHandler[];
}

interface HooksConfig {
  hooks: Record<string, MatcherGroup[]>;
  [key: string]: unknown;
}

export interface InstallOptions {
  codexHome: string;
  repo: string;
  dryRun?: boolean;
}

/** The command Codex will run. Absolute, because a hook has no useful working directory. */
export function hookCommand(repo: string): string {
  return join(resolve(repo), "hooks", "codex-peers-inbox.sh");
}

/**
 * Merges this hook into the user's Codex hooks config.
 *
 * Additive and idempotent. The file belongs to the user and may already carry hooks from anything
 * else, so nothing is ever removed and a config that does not parse is refused rather than
 * replaced: overwriting it would destroy hooks this project knows nothing about, and the user would
 * find out when one silently stopped running.
 */
export function installCodexHook(options: InstallOptions): { path: string; content: string } {
  const path = hooksPath(options.codexHome);
  const command = hookCommand(options.repo);

  const config = readExistingConfig(path);

  for (const event of HOOK_EVENTS) {
    const groups = config.hooks[event] ?? [];
    // Codex runs every registered handler. A second copy of ours would find the queue already
    // emptied by the first and emit a block with nothing in it.
    const alreadyThere = groups.some((group) =>
      group.hooks?.some((handler) => handler.command === command)
    );
    if (!alreadyThere) groups.push({ hooks: [{ type: "command", command }] });
    config.hooks[event] = groups;
  }

  const content = JSON.stringify(config, null, 2) + "\n";
  if (!options.dryRun) {
    mkdirSync(options.codexHome, { recursive: true });
    writeFileSync(path, content, { mode: 0o600 });
  }
  return { path, content };
}

function readExistingConfig(path: string): HooksConfig {
  if (!existsSync(path)) return { hooks: {} };

  const raw = readFileSync(path, "utf8");
  if (raw.trim().length === 0) return { hooks: {} };

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    throw new Error(
      `${path} could not be parsed as JSON (${error instanceof Error ? error.message : String(error)}). ` +
        `Refusing to overwrite it: fix or move the file, then run this again.`
    );
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error(`${path} could not be parsed as a hooks config object. Refusing to overwrite it.`);
  }

  const config = parsed as HooksConfig;
  if (typeof config.hooks !== "object" || config.hooks === null || Array.isArray(config.hooks)) {
    config.hooks = {};
  }
  return config;
}

if (import.meta.main) {
  const dryRun = process.argv.includes("--dry-run");
  const codexHome = defaultCodexHome();
  const repo = join(import.meta.dir, "..");

  const { path, content } = installCodexHook({ codexHome, repo, dryRun });

  if (dryRun) {
    console.log(`Would write ${path}:\n`);
    console.log(content);
  } else {
    console.log(`Registered the peer inbox hook in ${path}`);
    console.log(`Events: ${HOOK_EVENTS.join(", ")}`);
    console.log("");
    console.log("Codex will not run this hook until you approve it.");
    console.log("Start `codex` and accept the hook review prompt. You will be asked again after");
    console.log("any change to hooks.json, because the trust is recorded as a hash of the file.");
  }
}
