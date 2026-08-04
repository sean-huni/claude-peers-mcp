#!/usr/bin/env bash
#
# Delivers peer messages into a Codex CLI session.
#
# Codex has no MCP channel to push to, so mail lands in the spool and waits. This hook is the only
# thing that puts it in front of the model. Register it with bin/install-codex-hook.ts, which wires
# it to SessionStart, UserPromptSubmit and Stop.
#
# Stop is the one that matters. It fires when a turn completes, and a hook that answers
# `{"decision":"block","reason":"..."}` refuses to let the turn end and feeds the reason back to the
# model. That is how mail reaches a session nobody is typing into.
#
# THIS SCRIPT MUST ALWAYS EXIT 0 WITH VALID JSON ON STDOUT. Codex reads a non-zero exit from a Stop
# hook as a blocking decision, so a crash here would not merely fail to deliver, it would wedge the
# session. Every failure path below therefore prints `{}` and exits 0, leaving the mail queued for
# the next event rather than risking the session.
#
# All the real logic, and in particular the rule about WHICH events can carry a message at all,
# lives in codexdrain.ts where it is tested. This file only has to move bytes and never die.
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Read stdin whatever happens: Codex writes the event JSON there and an unread pipe can block it.
INPUT="$(cat)"

command -v bun >/dev/null 2>&1 || { printf '{}'; exit 0; }

OUTPUT="$(printf '%s' "$INPUT" | bun "$REPO/cli.ts" codex-inbox 2>/dev/null)" || { printf '{}'; exit 0; }

# An empty result is not valid JSON, and Codex reports invalid hook output as an error.
[[ -z "$OUTPUT" ]] && { printf '{}'; exit 0; }

printf '%s' "$OUTPUT"
exit 0
