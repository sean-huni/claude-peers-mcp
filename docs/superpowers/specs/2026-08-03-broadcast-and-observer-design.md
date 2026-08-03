# Broadcast messaging, and an observer transport, measured against each other

Date: 2026-08-03. Status: approved, implementation delegated.

## Problem

A session can message exactly one peer at a time. Telling every other session
something ("the auth contract changed, pull before you build") means repeating a
unicast per peer, which the sender has to enumerate first.

Separately, the broker to MCP-server hop is a 1 second poll. Measured on the
current code: 245 ms best, 491 ms median, 498 ms worst, distributed uniformly
across the interval, which is the signature of a poll landing at a random point
in its cycle. Each session issues about 86,400 poll requests and 5,760
heartbeats a day, nearly all returning empty.

The last hop already pushes: the MCP server sends a channel notification to its
Claude session. Only the first hop polls, so the system is half observer
already, by accident rather than decision.

## Objective

Deliver both, measure both honestly, and pick on evidence rather than taste.

Success is a number, not an opinion: end-to-end send-to-render latency and
request volume, measured with the same harness against the same workload.

## Decision 1: broadcast uses fan-out on write

Broadcast is publish/subscribe. Its canonical decision is fan-out on write (one
row per recipient) versus fan-out on read (one row plus a per-peer cursor).

Fan-out on write is chosen because N is 2 to 5 sessions on one machine. It
reuses every mechanism already built and recently repaired: per-peer
acknowledgement, the TTL sweep, WAL checkpointing, the channel push gate and the
spool fallback. Fan-out on read would need a new cursor table, new
acknowledgement semantics and a second delivery path, buying nothing at this
scale.

Revisit if a peer count ever reaches the hundreds, which for sessions on one
machine it will not.

## Decision 2: the observer transport is a separate change

Broadcast does not require it. Polling is per-peer and constant, so a broadcast
to three peers is three rows, not three times the polling, and latency is
unchanged. Bundling them would make it impossible to attribute a regression.

They ship as two branches, measured independently and in combination.

## Scope

In scope:

- `broadcast_message(message, scope?)` where scope mirrors `list_peers`:
  `machine` (default), `directory`, `repo`.
- `POST /broadcast-message`, authenticated like every other route, caller
  identified by `from_id`. Resolves recipients with the existing peer selection,
  excludes the sender, inserts one row per recipient in one transaction, returns
  `{ok, delivered_to}`.
- An SSE push from broker to MCP server, with the poll retained as a fallback so
  a dropped stream degrades to current behaviour rather than to silence.
- A benchmark harness that measures both, reproducibly.

Out of scope: fan-out on read, WebSocket, any change to the channel or spool
delivery paths, cross-machine transport.

## Edge cases decided now

- Zero recipients returns `{ok: true, delivered_to: 0}`. Nobody listening is not
  a failure.
- The sender never receives its own broadcast.
- A broadcast is not atomic across recipients. Each copy lives and dies on its
  own, matching the existing per-peer model, so a reap of one dead peer cannot
  affect another's copy.
- With SSE enabled the poll must not double-deliver. Exactly one path renders a
  given message.

## Testing

Broker level: fan-out count per scope, sender exclusion, zero-peer case, auth
rejection, independent acknowledgement per recipient. Server level: two
receivers, one channel-enabled and one not, each receiving by its own path.
Every test proves it bites by re-introducing the defect.

## Measurement

The deciding numbers, same harness on each branch:

- Median and p95 send-to-render latency, unicast and broadcast.
- Broker requests per session per minute while idle.
- Latency under a burst, to expose any queueing.

Baseline is current `dev`. A transport wins on the numbers or it does not land.

## Isolation

Live Claude Code sessions are running on this machine against the broker on port
7899. Test work uses assigned port ranges only, never 7899, and gives every test
client its own spool directory and session pid, because `findSessionPid` walks
the process tree and would otherwise write test messages into a live session's
inbox.
