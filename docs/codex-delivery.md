# Delivery into a Codex CLI session

A Claude Code session is pushed to over an MCP channel and a peer message arrives while the model is
mid-thought. Codex CLI has no such channel. Its mail lands in the same per-session spool file and
waits, and nothing tells the session it has any, which makes it a messaging system whose delivery
depends on the recipient guessing that something arrived.

Codex does have a hook system. This is what it can and cannot carry, how the two halves of the path
fit together, and the one case that still has no answer.

## The facts, and how each was established

Everything below was read off the `codex-cli 0.145.0` binary on this machine rather than taken from
another project's documentation. The binary embeds its own JSON schemas, so the schemas are the
source and they can be re-derived on any build:

```bash
strings -a "$(readlink -f "$(command -v codex)")" > /tmp/codex-strings.txt
grep -n "HookSpecificOutputWire\|BlockDecisionWire\|stop_hook_active" /tmp/codex-strings.txt
```

| Fact | Value | Where it came from |
|---|---|---|
| Hook config file | `$CODEX_HOME/hooks.json` | `hooks.json` beside `engine/command_runner.rs` in the strings dump |
| Event count | 11 | the `HookEventsToml` field list |
| Events with `additionalContext` | 5 | one `*HookSpecificOutputWire` schema each, listed below |
| Events with `decision: "block"` | 2 | `stop.command.output` and `subagent-stop.command.output` |
| Events that can carry nothing | 4 | no output field in their schema |
| Trust model | SHA recorded at `hooks.state."<path>".trusted_hash` in `config.toml` | `HookStateTomlenabledtrusted_hash` in the strings dump |
| Hook invocation | appears to go through `$SHELL -lc` | weaker than the rest: `SHELL`, `-lc` and `hooks.json` are adjacent literals in the `command_runner` constant pool, which is not a parsed schema. Nothing here depends on it |

## Which events can put text in front of the model

This asymmetry is the whole reason the drain is a tested module rather than a line of shell. A drain
wired to an event with no output field would look like it worked, empty the queue on every firing,
and deliver nothing, forever.

| Event | Channel | Registered |
|---|---|---|
| `SessionStart` | `additionalContext` | yes |
| `UserPromptSubmit` | `additionalContext` | yes |
| `Stop` | `decision: "block"` with a `reason` | yes |
| `SubagentStop` | `decision: "block"` with a `reason` | no, see below |
| `PreToolUse` | `additionalContext` | no, see below |
| `PostToolUse` | `additionalContext` | no, see below |
| `SubagentStart` | `additionalContext` | no, see below |
| `SessionEnd` | none | never |
| `PreCompact` | none | never |
| `PostCompact` | none | never |
| `PermissionRequest` | none, its `hookSpecificOutput` carries only a `decision` | never |

`Stop` is the load-bearing one. It fires when a turn completes, and a hook answering
`{"decision":"block","reason":"..."}` refuses to let the turn end and feeds the reason back to the
model, which then keeps going. That is how mail reaches a session nobody is typing into.

`PreToolUse` and `PostToolUse` would deliver sooner mid-task, and the Claude Code hook uses exactly
that trick. They are left out here because they fire on every single tool call, and paying a process
spawn per tool call to shorten a wait that `Stop` already bounds to one turn is a bad trade.
`SubagentStart` and `SubagentStop` are left out for the same reason plus a worse one: mail delivered
into a subagent is read by the subagent, not by the session that can reply to it.

`deliveryMode` in `codexdrain.ts` is the guard, and nothing drains without it. An event this build
does not have is treated as undeliverable, because assuming a new event can carry context costs the
queue, and assuming it cannot costs one delivery cycle.

## The two halves

Delivery only works if something writes to the queue and something else reads it. Both halves have
to know that Codex exists, and the producer half is the easier one to leave out: everything can be
built and tested while nothing ever writes, and the symptom is a hook that runs on every turn and
correctly reports that there is no mail.

**Producing.** The MCP server is started by both hosts and cannot tell which from its own arguments,
so `findHostSessionPid` walks its ancestors for a `claude` or a `codex`, nearest first. Nearest is a
correctness rule rather than a shortcut: a Codex session started from a Claude Code shell has both
above it, and the mail belongs to the codex turn whose hook will deliver it.

**Consuming.** `hooks/codex-peers-inbox.sh` runs `bun cli.ts codex-inbox`, which reads Codex's hook
JSON from stdin and answers with the hook JSON Codex expects on stdout. The ordering inside
`drainForHook` is the contract: delivery mode is settled BEFORE the queue is touched, because
`drainSpool` is destructive and there is no putting a message back.

The script must always exit 0 with valid JSON. A non-zero exit from a `Stop` hook is read by Codex as
a blocking decision, so a crash there would not merely fail to deliver, it would wedge the session.
Every failure path prints `{}` and exits 0, leaving the mail queued for the next event.

## Installing

```bash
bun bin/install-codex-hook.ts --dry-run   # print what would be written, change nothing
bun bin/install-codex-hook.ts             # merge into $CODEX_HOME/hooks.json
```

The installer is additive and idempotent. It never removes anything, and it refuses a `hooks.json`
that does not parse rather than replacing it, because that file belongs to the user and may carry
hooks this project knows nothing about.

**Codex will not run the hook until you approve it.** It hashes the config and records the hash in
`config.toml`, and the TUI asks at startup whenever the hash changes. Start `codex`, accept the hook
review prompt, and expect to be asked again after any edit to `hooks.json`. The installer does not
write a trust hash on your behalf: doing so would defeat the only thing standing between a config
file and arbitrary code execution.

## The gap that is still open

**There is no way into a session sitting idle at the prompt.** No event fires while a session waits
for input, so a message arriving then waits for the user to type. `codex exec resume` does not help:
it runs a separate process against the same rollout file rather than reaching the open TUI.

The gap is narrow, because `Stop` covers every session that is working and `UserPromptSubmit` covers
every session whose user comes back. It is real, and it is named rather than papered over:

```bash
bun cli.ts codex-nudge-status
```

reports Codex sessions holding mail they cannot collect yet, using the `.seen` activity stamp each
hook invocation writes. A session that ran a hook recently is mid-turn and its `Stop` will deliver;
one that has been quiet longer has already passed its `Stop` and is sitting at the prompt. The
command only reports. Draining from outside the session would consume the message without ever
showing it to the model, which is strictly worse than leaving it queued.

## What is verified by test, and what is not

| Claim | How |
|---|---|
| Which events can deliver, and that the others never drain | `codexdrain.test.ts`, unit |
| The exact JSON shape Codex validates for each mode | `codexdrain.test.ts`, unit, against the schemas above |
| The hook script always exits 0 with valid JSON, including with no `bun` and with a silent `bun` | `codexhook.test.ts`, runs the real script |
| The installer merges, is idempotent, and refuses a corrupt file | `codexhook.test.ts`, real files in a throwaway `CODEX_HOME` |
| A `codex` ancestor resolves to a spool key, and the nearest host wins | `spool.test.ts`, real process tree built from symlinks |
| Codex accepts `hooks.json` and runs the hook in a live turn | **not automated.** A live turn cannot be driven from a test |

The last row is the honest boundary. The wire format is verified against the binary's own schemas and
the script is verified by running it, but no test in this repo has watched a real Codex turn read a
message, because doing so needs an authenticated session and a human at a terminal.
