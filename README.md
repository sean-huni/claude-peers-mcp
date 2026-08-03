# claude-peers

An MCP server that lets multiple Claude Code sessions **on the same machine** discover each
other and exchange messages. Open five terminals across five projects, and any session can list
the others, see what they are working on, and send them a message.

```
  Terminal 1 (poker-engine)          Terminal 2 (eel)
  ┌───────────────────────┐          ┌──────────────────────┐
  │ Claude A              │          │ Claude B             │
  │ "send a message to    │  ──────> │                      │
  │  peer xyz: what files │          │ message arrives,     │
  │  are you editing?"    │  <────── │  Claude B responds   │
  │                       │          │                      │
  └───────────────────────┘          └──────────────────────┘
```

**One machine only.** The broker binds `127.0.0.1` and peer liveness is checked with
`process.kill(pid, 0)`, which only works for local processes. There is no cross-machine
federation, and messages are not encrypted in transit or at rest. A design for federation is
parked on the `feat-encryption` branch and is not implemented.

## Requirements

- [Bun](https://bun.sh) (developed against 1.3.x). Bun is required, not optional: the broker
  uses `bun:sqlite` and `Bun.serve`, and the MCP server spawns the broker as `bun broker.ts`.
- Claude Code. Instant channel push needs a build that supports
  `--dangerously-load-development-channels` (verified on 2.1.220).
- **macOS** for the zero-config auto-summary, which reads the Claude Code OAuth token from the
  Keychain via `security find-generic-password -s "Claude Code-credentials"`. On any other
  platform `readKeychainToken` returns `null` immediately; everything else still works, and
  Claude just sets its own summary with `set_summary`. Setting `ANTHROPIC_API_KEY` removes the
  macOS dependency entirely.

## Install

```bash
git clone https://github.com/sean-huni/claude-peers-mcp.git ~/claude-peers-mcp
cd ~/claude-peers-mcp
bun install
```

Register the MCP server once, at user scope, so it is available in every Claude Code session
from any directory:

```bash
claude mcp add --scope user --transport stdio claude-peers -- bun ~/claude-peers-mcp/server.ts
```

Use an **absolute** path. The `.mcp.json` committed in this repo uses the relative
`./server.ts`, which only resolves when Claude Code is started from the clone directory. If you
register at user scope, do not also rely on the project-scoped `.mcp.json`: Claude Code will
report the two as conflicting endpoints.

Verify:

```bash
claude mcp list          # claude-peers should report Connected
```

## Usage across multiple terminals

Start Claude Code the same way in **every** terminal you want on the network:

```bash
claude --dangerously-load-development-channels server:claude-peers
```

### What that flag does, and what happens without it

This is the one thing worth understanding before anything else.

The flag makes Claude Code advertise the experimental `claude/channel` client capability. The
MCP server checks for it on every poll cycle (`clientRendersChannel()` in `server.ts`) and
**only pushes when it is present**:

| | Launched **with** the flag | Launched **without** it |
| --- | --- | --- |
| Delivery | Pushed into the session within ~1s of being sent | Message waits in the broker queue |
| Claude notices | Immediately, unprompted, mid-task | Only when it calls `check_messages` |
| Messages lost | No | No |

**Without the flag, messages still arrive.** They are not dropped and they are not silently
discarded. They stay queued in the broker until the receiving session calls the
`check_messages` tool, which is how you or Claude pull them. This is deliberate: pushing to a
client that cannot render a channel notification would acknowledge and delete a message nobody
ever saw, which is exactly the bug fixed in 0.1.1.

So: with the flag you get a tap on the shoulder; without it you get an inbox you have to check.

Queued messages are not kept forever. Undelivered mail older than `CLAUDE_PEERS_MSG_TTL`
(default one hour) is swept every 60 seconds, so a session that never checks its messages will
lose them.

A shell alias saves the typing:

```bash
alias claudepeers='claude --dangerously-load-development-channels server:claude-peers'
```

### Try it

Start Claude Code in two terminals, in different projects. In either one, ask:

> List all peers on this machine

You get each running session's peer ID, PID, working directory, git repo, TTY and summary.
Then:

> Send a message to peer [id]: "what are you working on?"

With the channel flag the other session sees it within about a second. Without it, ask the
other session to check its messages.

The broker daemon starts automatically the first time a session registers; you never start it
by hand.

## Tools

| Tool | What it does |
| --- | --- |
| `list_peers` | Lists other sessions. Requires a `scope`: `machine` (everything on this computer), `directory` (same cwd), or `repo` (same git root, so worktrees and subdirectories match). Always excludes the caller, and drops peers whose PID is gone. |
| `send_message` | Queues a message for another session, by the peer ID from `list_peers`. Fails if the target ID is unknown. Delivery is push or pull depending on the receiver's channel capability, above. |
| `set_summary` | Sets a 1-2 sentence description of what this session is doing. Other sessions see it in `list_peers`. Overwrites any auto-generated summary. |
| `check_messages` | Pulls queued messages for this session and acknowledges them, so each message is returned exactly once. This is the delivery path for a session started without the channel flag. |

## How it works

```
                    ┌───────────────────────────┐
                    │  broker daemon            │
                    │  127.0.0.1:7899 + SQLite  │
                    └──────┬───────────────┬────┘
                           │               │
                      MCP server A    MCP server B
                      (stdio)         (stdio)
                           │               │
                      Claude A         Claude B
```

- **broker.ts** is a singleton HTTP daemon on `127.0.0.1:7899` backed by a `bun:sqlite`
  database at `~/.claude-peers.db`. One per machine, auto-launched and detached by the first
  MCP server that finds it missing.
- **server.ts** is an MCP stdio server, one per Claude Code session. It registers with the
  broker, heartbeats every 15s, polls for mail every 1s, and exposes the four tools.
- Peers whose process has exited are reaped: on broker startup, every 30 seconds, and again
  during each `list_peers` call.

**Delivery is durable.** Polling deliberately does *not* consume: the broker returns queued
messages and leaves them in place. The client sends an explicit `/ack-messages` only after the
message has actually been rendered, and the ack `DELETE`s the row. A crash between poll and
render therefore leaves the message queued for the next cycle instead of losing it.

## Auto-summary

On startup each session asks Claude for a one-line summary of what you are probably working on,
derived from the working directory, git root, branch and recently changed files. Other sessions
see it in `list_peers`. It is best-effort and non-blocking: startup waits at most 3 seconds,
then applies the summary late if it arrives after registration.

The model is `claude-haiku-4-5`, chosen because it is the cheap tier a one-line summary needs
and the only tier a Claude Code subscription token can reach.

Credentials resolve in this order (`shared/summarize.ts`):

1. `ANTHROPIC_API_KEY` if set: a Console API key, billed per token.
2. Otherwise, on macOS, the Claude Code OAuth token from the Keychain. Nothing to configure if
   you are signed in to Claude Code. It is re-read on every call and never cached, because
   Claude Code rotates it every few hours, and an expired token is treated as absent.

If neither is available, or the call fails or is refused, the summary is simply skipped and
Claude sets its own with `set_summary`.

## Configuration

| Environment variable | Default | Description |
| --- | --- | --- |
| `CLAUDE_PEERS_PORT` | `7899` | Broker port. Read by `broker.ts`, `server.ts` and `cli.ts`, so it must be set consistently for all three. |
| `CLAUDE_PEERS_DB` | `~/.claude-peers.db` | SQLite database path. Read only by `broker.ts`. |
| `CLAUDE_PEERS_MSG_TTL` | `3600000` (1 hour, ms) | How long undelivered mail survives. Swept every 60 seconds. |
| `ANTHROPIC_API_KEY` | unset | Optional. Auto-summary credential; falls back to the macOS Keychain OAuth token. |

## Security

What the broker actually does:

- **Per-session bearer token.** `/register` mints a 256-bit random token and returns it to the
  registering process. Every other route (`/heartbeat`, `/set-summary`, `/list-peers`,
  `/send-message`, `/poll-messages`, `/ack-messages`, `/unregister`) requires it, matched
  against the peer named in the request body and compared with `timingSafeEqual`. Peer IDs are
  public via `/list-peers`, so the ID alone proves nothing. Anonymous `/list-peers` is refused
  rather than allowed through, because reading every session's cwd and summary is itself
  disclosure.
- **Localhost only.** `Bun.serve` binds `hostname: "127.0.0.1"`. Nothing is exposed to the
  network.
- **Database permissions.** The database and its `-wal` / `-shm` siblings are `chmod 0600` on
  every boot; SQLite creates them `0644` by default.
- **No plaintext retention.** An acknowledged message is `DELETE`d rather than flagged, and
  `PRAGMA secure_delete = ON` zeroes the freed pages instead of leaving message text
  recoverable in the freelist. Unacknowledged mail is swept at TTL.

What this does **not** protect against, stated plainly:

- **Any process running as your user can read everything.** The token lives in the MCP server's
  memory and in the `peers` table of a `0600` file your own UID owns. `0600` stops other
  accounts on the machine, not you or anything you run.
- **Messages exist in plaintext elsewhere anyway.** Every message pushed into or pulled by a
  session is written to that session's transcript at `~/.claude/projects/**/*.jsonl`, outside
  this project's control. Deleting a row from the broker database does not remove it from
  there.
- **No encryption at all**, in transit or at rest. No signatures, no key exchange, no replay
  protection.
- **`cli.ts` predates the authentication change** and sends no `Authorization` header, so
  `peers` and `send` now get `401` from the broker. That is the auth working, but it means the
  CLI is not a way around it either.

The short version: this raises the bar against *other accounts* on a shared machine, and does
nothing against anything running as you.

## CLI

```bash
cd ~/claude-peers-mcp

bun cli.ts status            # broker health and, if any, the peer list
bun cli.ts kill-broker       # stop the broker daemon (kills whatever holds the port)
bun cli.ts peers             # currently broken: 401, see below
bun cli.ts send <id> <msg>   # currently broken: 401, see below
```

`status` and `kill-broker` work because they use the unauthenticated `GET /health`. `peers` and
`send` call authenticated routes without a token and fail with `401`, which `peers` reports
misleadingly as "Broker is not running."; `status` prints the same line after the health line
whenever at least one peer is registered. Use the `list_peers` and `send_message` tools from
inside a Claude Code session instead.

## Tests

```bash
bun test
```

13 tests across `broker.test.ts` and `server.test.ts`. Both files spawn a real broker process on
a randomized port with a throwaway database, and `server.test.ts` additionally drives real
`server.ts` processes over stdio, so nothing here is mocked. They cover peer discovery and self-exclusion, refusal of messages
to unknown peers, that polling does not consume, that acking deletes and is idempotent,
ordering, exactly-once rendering, the `0600` file mode, token-less impersonation being
rejected, token entropy, and both delivery paths: a channel-capable client being pushed to, and
a channel-less client still receiving via `check_messages`.

## Troubleshooting

**A stale broker keeps serving old code.** The broker is a long-lived detached daemon and is
only started when the port is free. After `git pull`, the old broker process is still running
the old code, and every new session will attach to it. Symptoms are changes that appear to have
no effect at all.

Check what is there:

```bash
curl -s http://127.0.0.1:7899/health     # {"status":"ok","peers":N} means a broker is up
lsof -ti :7899                           # its PID
ps -o lstart=,command= -p "$(lsof -ti :7899)"   # started before or after your pull?
```

Restart it:

```bash
bun cli.ts kill-broker                   # or: kill "$(lsof -ti :7899)"
```

The next Claude Code session to start will bring a fresh broker up automatically. Peers
re-register on their next start; an already-running session will not re-register itself.

**No peers listed.** Confirm the other session really loaded the MCP server (`claude mcp list`,
or look for `[claude-peers] Registered as peer …` on its stderr), and that both sessions use
the same `CLAUDE_PEERS_PORT`. Also check `scope`: `directory` and `repo` match only sessions in
the same cwd or git root.

**Messages sent but never seen.** The receiving session was almost certainly started without
`--dangerously-load-development-channels server:claude-peers`. Ask it to call `check_messages`,
and mind the one-hour TTL.

**Broker will not start.** `server.ts` gives up after 6 seconds with "Failed to start broker
daemon". Run `bun broker.ts` in a terminal to see the real error: usually the port is taken by
something else, or `CLAUDE_PEERS_DB` points somewhere unwritable.

## License

MIT. See [LICENSE](LICENSE).
