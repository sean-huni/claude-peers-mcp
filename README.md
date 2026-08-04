# claude-peers

Let your Claude Code instances find each other and talk. When you're running 5 sessions across different projects, any Claude can discover the others and send messages that arrive instantly.

```
  Terminal 1 (poker-engine)          Terminal 2 (eel)
  ┌───────────────────────┐          ┌──────────────────────┐
  │ Claude A              │          │ Claude B             │
  │ "send a message to    │  ──────> │                      │
  │  peer xyz: what files │          │ <channel> arrives    │
  │  are you editing?"    │  <────── │  instantly, Claude B │
  │                       │          │  responds            │
  └───────────────────────┘          └──────────────────────┘
```

## Quick start

### 1. Install

```bash
git clone https://github.com/louislva/claude-peers-mcp.git ~/claude-peers-mcp   # or wherever you like
cd ~/claude-peers-mcp
bun install
```

### 2. Register the MCP server

This makes claude-peers available in every Claude Code session, from any directory:

```bash
claude mcp add --scope user --transport stdio claude-peers -- bun ~/claude-peers-mcp/server.ts
```

Replace `~/claude-peers-mcp` with wherever you cloned it.

### 3. Run Claude Code with the channel

```bash
claude --dangerously-skip-permissions --dangerously-load-development-channels server:claude-peers
```

That's it. The broker daemon starts automatically the first time.

> **Tip:** Add it to an alias so you don't have to type it every time:
>
> ```bash
> alias claudepeers='claude --dangerously-load-development-channels server:claude-peers'
> ```

### 4. Open a second session and try it

In another terminal, start Claude Code the same way. Then ask either one:

> List all peers on this machine

It'll show every running instance with their working directory, git repo, and a summary of what they're doing. Then:

> Send a message to peer [id]: "what are you working on?"

The other Claude receives it immediately and responds.

## What Claude can do

| Tool             | What it does                                                                   |
| ---------------- | ------------------------------------------------------------------------------ |
| `list_peers`     | Find other Claude Code instances, scoped to `machine`, `directory`, or `repo`  |
| `send_message`   | Send a message to another instance by ID or name (arrives instantly via channel push) |
| `ask_peer`       | Ask another instance a question and WAIT for its answer, up to a timeout (see below) |
| `broadcast_message` | Send one message to every other instance in scope at once (see below)       |
| `set_summary`    | Describe what you're working on (visible to other peers)                       |
| `set_name`       | Claim a human-readable peer name, addressable by `send_message` (see below)    |
| `whoami`         | Report this session's own ID, name, cwd, repo, and summary                     |
| `check_messages` | Manually check for messages (fallback if not using channel mode)               |

## Peer names

A session can claim a name and be addressed by it, so "send this to zod" works without anyone
copying 8-char IDs around:

```
User: your peer name is Zod
Claude: [calls set_name("Zod")] Name claimed.

# in any other session:
Claude: [calls send_message(to_id: "zod", ...)]   # case-insensitive
```

- Claim at launch with `CLAUDE_PEERS_NAME=Zod` in the environment, or at any time with the
  `set_name` tool.
- Names are 1-32 chars (letters, digits, space, dash, underscore, starting alphanumeric) and
  **unique per broker, case-insensitively**: claiming a name another live peer holds fails with
  `taken <id>`. Uniqueness at claim time is what keeps send-time resolution unambiguous.
- Renaming requires the peer's own bearer token: a name is a routable address, so an
  unauthenticated rename would redirect that peer's inbound mail.
- `list_peers` shows names; inbound pushes carry the sender's name in `meta.from_name`.
- IDs keep working everywhere a name works. On a collision (`Peer x not found`, `taken ...`),
  fall back to the ID from `list_peers`.

## Asking and waiting: `ask_peer`

`send_message` is fire-and-forget. `ask_peer(to_id, question, timeout_seconds?)` blocks until the
peer answers or the timeout lapses (default 60s, 5-300):

- The peer receives the question as a normal message with reply instructions embedded: it answers
  with `send_message(..., in_reply_to: "<ask token>")`, and that answer resolves the waiting call.
- The answer is consumed by the waiting `ask_peer` call and is **not** also delivered as a second
  inbound message.
- No answer in time returns a timeout notice; the question was still delivered, and a late answer
  arrives as an ordinary message rather than vanishing.
- **Only the peer you asked can answer.** The waiting call is bound to that peer, so a reply
  quoting the token from anybody else is delivered as an ordinary message instead of resolving
  your ask. The token travels inside a message body, so treating it alone as proof of identity
  would trust every process that can see one.
- Use `ask_peer` when the answer gates your next step (a decision, a value, a confirmation);
  use `send_message` for everything else.

## Broadcasting

`broadcast_message(message, scope?)` says one thing to every other session at once, instead of
enumerating peers and repeating a `send_message` per peer.

| `scope` | Who receives it |
| ------- | --------------- |
| `machine` (default) | Every other instance on this computer |
| `directory` | Every other instance whose working directory is the sender's |
| `repo` | Every other instance in the same git repository, including worktrees and subdirectories. Falls back to `directory` for a sender with no git root |

The scopes are the same ones `list_peers` uses, and the audience is exactly the peers `list_peers`
would have returned for that scope: one selection, used by both.

Things worth knowing:

- **The sender is always excluded.** You never receive your own broadcast, at any scope.
- **Reaching nobody is a success, not an error.** Being the only session in scope is ordinary, so
  the tool reports `0 peer(s)` rather than failing.
- **Delivery is per recipient.** One message row is written per peer, so each copy arrives by that
  session's own path (channel push, or the spooled queue when the channel is unavailable), and each
  is acknowledged and expired on its own. One recipient reading its copy has no effect on anybody
  else's.
- **Prefer `send_message` whenever one peer is the audience.** A broadcast interrupts every session
  in scope, so an unnecessary one is noise for people working on something else. Broadcast is for
  news that genuinely concerns everyone: a shared contract or schema changed, a shared branch moved,
  a shared resource is down.

## How it works

A **broker daemon** runs on `localhost:7899` with a SQLite database. Each Claude Code session spawns an MCP server that registers with the broker and subscribes to it. Both hops push: the broker writes a `text/event-stream` frame the moment a message lands in a peer's mailbox, and the MCP server passes it into the session via the [claude/channel](https://code.claude.com/docs/en/channels-reference) protocol, so Claude sees it immediately.

```
                    ┌───────────────────────────┐
                    │  broker daemon            │
                    │  localhost:7899 + SQLite  │
                    └──────┬───────────────┬────┘
                           │               │
                      MCP server A    MCP server B
                      (stdio)         (stdio)
                           │               │
                      Claude A         Claude B
```

The broker auto-launches when the first session starts. It cleans up dead peers automatically. Everything is localhost-only.

### The push transport, and the poll behind it

`GET /subscribe?id=<peer>` with the peer's bearer token opens an event stream. The broker notifies
at the point a row is inserted into `messages`, so every route that queues mail wakes its recipient
without having to remember to. The frame says only that there is something to fetch: the client
then runs the same delivery the poll runs, which is what makes it impossible for a message to be
rendered twice by two transports.

The poll is still there. While the stream is healthy it runs every 30 seconds as an audit rather
than as the transport; the moment the stream is gone it returns to one second, which is exactly
what the session did before. A broker that does not serve `/subscribe` at all, an older one or one
started with `CLAUDE_PEERS_SSE=off`, is therefore not a broker a session goes deaf against, only a
slower one.

Both halves of the transport are switchable, and the timings are tunable: see the delivery
transport table under [Configuration](#configuration). Turning `CLAUDE_PEERS_SSE` off at the broker
or `CLAUDE_PEERS_STREAM` off at a session is how the polling-only path is exercised deliberately.

Measure it on your own machine with the harness, which runs unmodified against any checkout:

```bash
bun bench/send-to-render.ts --root . --port 7840 --n 30 --label after
```

## Auto-summary

Each instance generates a brief summary on startup using Claude Haiku. The summary describes what you're likely working on based on your directory, git branch, and recent files. Other instances see this when they call `list_peers`.

Credentials resolve in this order:

1. `ANTHROPIC_API_KEY` if set: a Console API key, billed per token.
2. Otherwise, on macOS, the local Claude Code OAuth token read from the Keychain (`Claude Code-credentials`). No configuration needed if you are signed in to Claude Code.

The Keychain token is read fresh on every call, never cached, because Claude Code rotates it every few hours. If neither credential is available, the summary is skipped and Claude sets its own via the `set_summary` tool.

Without the API key, Claude sets its own summary via the `set_summary` tool.

## Delivery without the channel

Channel push needs `--dangerously-load-development-channels server:claude-peers`. Started the
ordinary way, from `.mcp.json`, the tools all work and **nothing ever surfaces an inbound
message**: it waits in the broker until the model happens to call `check_messages`, which it has
no reason to do. A messaging system whose delivery depends on the recipient guessing that
something arrived is not a messaging system.

So when the channel is unavailable the poll loop writes each message to a per-session queue under
`~/.claude-peers/inbox/<claude-pid>.jsonl`, and a hook drains it into the session:

```jsonc
// ~/.claude/settings.json
{
  "hooks": {
    "UserPromptSubmit": [
      { "hooks": [{ "type": "command",
                    "command": "/path/to/claude-peers-mcp/hooks/peers-inbox.sh" }] }
    ],
    "PostToolUse": [
      { "matcher": "*",
        "hooks": [{ "type": "command",
                    "command": "/path/to/claude-peers-mcp/hooks/peers-inbox.sh" }] }
    ]
  }
}
```

`PostToolUse` is what makes it feel immediate: an agent mid-task calls tools constantly, so a
message lands within a tool call or two rather than at the end of the turn. The hook costs one
`stat` when the queue is empty, prints nothing, and fails open, so a broken hook can never block a
session.

Notes on the design:

- **The queue is keyed on the `claude` process id**, not on the working directory. Two sessions in
  one checkout is the normal case, and it is exactly when misrouting would matter.
- **The hook never touches the broker.** It reads a local file, so it needs no peer identity and no
  auth token; handing one to every hook invocation would put a credential that can read a session's
  messages into every tool call.
- **`check_messages` drains the queue too**, so with no hook installed behaviour is exactly what it
  was. Without that, spooling would MOVE messages out of reach rather than deliver them.
- **Only one path ever fires.** With the channel on, nothing is spooled, so a message is never
  delivered twice.
- Queues belonging to exited sessions are swept at startup, because pids are reused.

## Losing the database

`~/.claude-peers.db` can go away underneath a running system: you delete it, a cleanup script does,
or the broker is restarted against a fresh one. Neither side needs a human to put it back.

- **The broker recreates the schema in place.** It notices that the file it holds is no longer the
  file on disk (or that a statement failed with an I/O error), reopens, and recreates the tables.
  It does not exit: nothing supervises this daemon. It is spawned detached by whichever MCP server
  found it missing at startup, so exiting would leave no broker at all until someone opened a new
  session.
- **Sessions re-register themselves.** Their peer row and bearer token die with the database, so the
  broker answers 401. On the first refusal a session registers again, adopts the new id and token,
  and retries the call once. This is single-flight, so several calls refused at the same moment
  share one registration, and it is bounded: a refusal that survives re-registration is reported as
  an error rather than retried in a loop.

**Messages queued for the old peer id are lost.** They lived in the deleted database, and the peer
id they were addressed to no longer exists. There is no replay: the sender is not told, and the
recipient never sees them. Anything already spooled to disk for a hook-delivered session survives,
because that queue is a separate file. **Your peer id changes** when this happens, so an id another
session noted earlier stops resolving; `list_peers` shows the new one.

## Known limitation: a peer identity does not outlive its process

Verified by chaos testing, 2026-08-05. **Peer identity is bound to the MCP server's process, not to
your Claude Code session.** If that process dies and restarts, it registers as a NEW peer, and the
old row is reaped by the stale-peer sweep along with any mail still queued for it.

Measured: with 33 messages outstanding when the receiving process was `SIGKILL`ed, everything
already handed over survived with zero duplicates, and everything still queued for the old id was
destroyed. The sender was told `ok` at send time and is never told otherwise.

What this means in practice:

- A message is durable **once delivered**: acknowledgement happens after the receiving session has
  been handed the message, so a crash in between costs a duplicate at worst, never a loss. Verified:
  a broker `SIGKILL` mid-stream re-delivered 60 of 60 with zero duplicates.
- Mail in flight to a session whose MCP server restarts is **not** durable.
- Your **name** does survive a restart: a restarting session reclaims it from its own dead peer
  rather than being refused by its own corpse.

Fixing this properly means a session-stable identity rather than a process-bound one, which is a
design change and not yet made here. Until then, treat `ask_peer` timeouts and unanswered messages
around a restart as expected rather than as bugs.

## Updating and relaunching

There is **no build step**. Bun runs the TypeScript directly, so "the latest code" just means the
latest files on disk when a process starts. Nothing is compiled and there is no `dist/`.

Two things update independently:

| What | How | When it takes effect |
|---|---|---|
| Claude Code itself | `claude update` | Next time you start `claude` |
| This repo | `git pull` | Next time each process starts (see below) |

### The three processes, and which ones you must restart

1. **The broker** is a singleton daemon on port 7899 that **outlives your sessions**. Restarting a
   terminal does NOT restart it. This is the one people miss: you pull a fix, restart everything,
   and the old broker is still serving the old code.
2. **One MCP server per session** (`bun server.ts`), started by Claude Code. It reads the source at
   launch, so it needs a session restart.
3. **Claude Code** itself, one per terminal.

### Full relaunch

```bash
cd /path/to/claude-peers-mcp

# 1. Get the latest code and dependencies
git pull
bun install

# 2. Stop the broker. It respawns automatically on the next session start.
#    -sTCP:LISTEN matters: without it lsof also returns every CONNECTED client,
#    so a bare `lsof -ti :7899 | xargs kill` kills your MCP servers too.
lsof -ti :7899 -sTCP:LISTEN | xargs kill 2>/dev/null

# 3. Confirm it is actually down before continuing (this should fail)
curl -s --max-time 2 localhost:7899/health || echo "broker is down, good"

# 4. Update Claude Code itself
claude update
```

Then **exit every Claude Code session** and relaunch each terminal:

```bash
claude --dangerously-load-development-channels server:claude-peers
```

### Verify the new code is actually live

Do not assume the restart worked. Check:

```bash
# Prints the broker status and every registered session.
# "Broker request failed" or a missing peer list means something did not restart.
bun cli.ts peers

# Confirm each session sees the others
#   in any session, ask Claude to call list_peers with scope "machine"
```

A quick way to tell whether the broker is stale: check how long it has been running against when
you last pulled.

```bash
ps -o lstart=,command= -p "$(lsof -ti :7899 -sTCP:LISTEN)"
```

If that timestamp is older than your last `git pull`, the broker is running old code.

## CLI

You can also inspect and interact from the command line:

```bash
cd ~/claude-peers-mcp

bun cli.ts status            # broker status + all peers
bun cli.ts peers             # list peers, showing "<id> (<name>)" for named ones
bun cli.ts send <id> <msg>   # send a message into a Claude session (a peer NAME works too)
bun cli.ts broadcast <msg>   # send a message into every Claude session on the machine
bun cli.ts kill-broker       # stop the broker
```

## Configuration

Every setting has a working default, so a fresh clone runs with no environment at all. These are
the override points.

**Identity and storage**

| Environment variable      | Default                    | Description                                          |
| ------------------------- | -------------------------- | ---------------------------------------------------- |
| `CLAUDE_PEERS_PORT`       | `7899`                     | Broker port                                          |
| `CLAUDE_PEERS_DB`         | `~/.claude-peers.db`       | SQLite database path                                 |
| `CLAUDE_PEERS_NAME`       | unset                      | Claim this peer name at startup (see Peer names)     |
| `CLAUDE_PEERS_SPOOL_DIR`  | `~/.claude-peers/inbox`    | Per-session queue for sessions without channel push  |
| `ANTHROPIC_API_KEY`       | Keychain OAuth token       | Auto-summary credential (optional)                   |

**Delivery transport**

| Environment variable            | Default | Description                                                |
| ------------------------------- | ------- | ---------------------------------------------------------- |
| `CLAUDE_PEERS_SSE`              | `on`    | Broker: `off` makes `/subscribe` a 404                     |
| `CLAUDE_PEERS_STREAM`           | `on`    | Server: `off` disables subscribing, polling only           |
| `CLAUDE_PEERS_CHANNEL`          | detect  | `always` or `never` to override channel detection          |
| `CLAUDE_PEERS_POLL_MS`          | `1000`  | Poll interval while there is no healthy stream             |
| `CLAUDE_PEERS_POLL_IDLE_MS`     | `30000` | Poll interval while the stream is healthy                  |
| `CLAUDE_PEERS_SSE_KEEPALIVE_MS` | `25000` | Broker: comment frames down an idle stream                 |
| `CLAUDE_PEERS_STREAM_IDLE_MS`   | `75000` | Server: silence after which a stream is presumed dead      |
| `CLAUDE_PEERS_STREAM_STABLE_MS` | `5000`  | How long a stream must hold before its backoff resets      |

**Retention and backoff**

| Environment variable               | Default   | Description                                              |
| ---------------------------------- | --------- | -------------------------------------------------------- |
| `CLAUDE_PEERS_MSG_TTL`             | `3600000` | Undelivered messages are swept after this many ms (1h)   |
| `CLAUDE_PEERS_CHECKPOINT_MS`       | `1000`    | Debounce before the WAL is checkpointed after a delete   |
| `CLAUDE_PEERS_POLL_BACKOFF_MAX_MS` | `60000`   | Ceiling on poll backoff while the broker is unreachable  |
| `CLAUDE_PEERS_POLL_QUIET_MS`       | `300000`  | How often a sustained broker outage is re-reported       |

**Test-only.** These exist so a test run cannot collide with a live session, and are not
meant for normal use: `CLAUDE_PEERS_SESSION_PID` (pin the host session instead of walking the
process tree), `CLAUDE_PEERS_TEST_PORT_MIN` / `CLAUDE_PEERS_TEST_PORT_MAX` (default `7810`-`7824`,
the range suites draw ports from, deliberately excluding the live `7899`).


## Benchmarking delivery

```bash
bun run bench                                   # full run, roughly 3 to 4 minutes
bun bench/delivery-latency.ts --json-out out.json
```

Measures send-to-render latency, burst behaviour, one-to-many latency when a `broadcast_message`
tool exists, and the broker request volume of an idle session. It feature-detects, so the same
harness runs unchanged on every branch, and it exits non-zero rather than reporting statistics drawn
from messages that never arrived. Method, guards and the ways the numbers could still mislead are in
[`bench/README.md`](bench/README.md).

## Quicker Launch

The sequence after dev is confirmed

execute: `cd /Users/sean/env/repo/ai/claude-peers-mcp`
execute: `bun install`
execute: `lsof -ti :7899 -sTCP:LISTEN | xargs kill        # broker outlives sessions; must die`

Then exit and relaunch each Claude Code session

## Requirements

- [Bun](https://bun.sh)
- Claude Code v2.1.80+
- claude.ai login (channels require it; API key auth won't work)
