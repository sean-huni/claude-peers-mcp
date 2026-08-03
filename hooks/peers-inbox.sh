#!/usr/bin/env bash
#
# Delivers peer messages into a Claude Code session that cannot be pushed to.
#
# Channel push needs `--dangerously-load-development-channels server:claude-peers`. Started the
# ordinary way, from .mcp.json, nothing ever surfaces an inbound message: it waits in the broker
# until the model happens to call check_messages, which it has no reason to do. This hook is the
# delivery path for that case.
#
# Register it on UserPromptSubmit and PostToolUse. PostToolUse is what makes it feel immediate: an
# agent mid-task is calling tools constantly, so a message lands within a tool call or two rather
# than at the end of the turn.
#
# It must therefore be CHEAP. The common case is an empty queue, which costs one stat and no
# network, and prints nothing. A hook that spoke on every tool call would be worse than the problem
# it solves.
#
# Fails open, always. A broken hook must never block a session: any error exits 0 silently, and the
# messages simply stay queued for the next invocation.
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

command -v bun >/dev/null 2>&1 || exit 0

# The queue is keyed on the pid of the `claude` process, which this hook is a descendant of. The
# CLI walks up to find it, so nothing has to be passed in and nothing has to agree on a session id.
OUTPUT="$(bun "$REPO/cli.ts" inbox 2>/dev/null)" || exit 0
[[ -z "$OUTPUT" ]] && exit 0

# Claude Code reads stdout from UserPromptSubmit as additional context. PostToolUse wants the
# structured form, and a hook that emits both shapes works in either position without a second
# script to keep in step.
if [[ "${CLAUDE_HOOK_EVENT:-}" == "PostToolUse" ]]; then
  python3 - "$OUTPUT" <<'PY' 2>/dev/null || printf '%s\n' "$OUTPUT"
import json, sys
print(json.dumps({
    "hookSpecificOutput": {
        "hookEventName": "PostToolUse",
        "additionalContext": sys.argv[1],
    }
}))
PY
else
  printf '%s\n' "$OUTPUT"
fi
