# Message-delivery benchmark

The design (`docs/superpowers/specs/2026-08-03-broadcast-and-observer-design.md`) says a transport
"wins on the numbers or it does not land". This is where the numbers come from.

```bash
bun bench/delivery-latency.ts                       # full run, roughly 3 to 4 minutes
bun bench/delivery-latency.ts --json-out result.json
bun bench/delivery-latency.ts --help
```

Exit code is 0 only when every message the harness sent was accounted for. A short sample exits 1,
so a script cannot mistake survivors for a result.

## What it measures

| Phase | Question | Output |
| --- | --- | --- |
| unicast | How long from a `send_message` tool call to the receiver's channel notification? | min, p50, p95, max, mean over 30 samples |
| burst | Does a back-to-back burst queue up or head-of-line block? | the same percentiles, plus two phase-independent figures: the queueing cost (each message's latency minus the fastest delivery of its own round) and the first-to-last arrival spread inside each burst |
| broadcast | The same, one-to-many with 3 receivers | the same percentiles, per receiver as well as pooled. Skipped cleanly when `broadcast_message` does not exist |
| idle | What does one session cost the broker with no traffic at all? | requests per session per minute, broken down by path, plus a count of long-lived (streaming) requests |

## How it runs unchanged on every branch

Nothing about the transport is assumed.

- **Tools are discovered, not declared.** The harness starts one throwaway server, calls
  `tools/list`, and decides from the answer whether the broadcast phase runs. When
  `broadcast_message` lands on `feat-broadcast-message`, that phase starts measuring with no edit
  here. Its arguments are built from the tool's own `inputSchema`; a schema the harness cannot fill
  in blind is skipped with the schema quoted in the reason, rather than called wrongly and reported
  as a slow transport.
- **Request volume is counted outside the broker.** A reverse proxy in front of the broker counts
  what passes through it. That counts a poll transport, a server-sent-events transport, and anything
  else, without instrumenting the code under measurement. A request that stays open for more than
  five seconds is counted separately as a stream, so an observer transport is not simply credited
  with "fewer requests" without saying what replaced them.
- **The measured event is the channel notification**, which both the poll transport and the planned
  observer transport end with. Only the hop before it changes.

## What is timed

From the clock reading taken immediately before the sender's `send_message` frame is written to its
MCP server's stdin, to the reading taken when the receiving MCP server's channel notification frame
comes off its stdout. One process, one monotonic clock, so no clock skew enters.

Inside that interval: the sender's tool handler, the HTTP POST to the broker, the SQLite insert,
whatever carries the message to the receiving server, the sender-context lookup that server does,
and the notification write.

## What is excluded

- **Process and broker startup.** Nothing is timed until every client is visible in the broker's
  `/health` peer count.
- **The startup auto-summary**, which makes a real Anthropic API call and would otherwise add
  seconds of network time to the first sample. `ANTHROPIC_BASE_URL` is pointed at a closed loopback
  port that the harness reserves and never binds, and `ANTHROPIC_API_KEY` is set to a placeholder so
  the SDK never reaches for the Claude Code OAuth token in the Keychain. The per-client registration
  time in the JSON is the evidence it worked: a client that reached the real API would sit in
  `server.ts`'s three second race before registering.
- **Two warm-up messages per phase.** The first delivery in a fresh process pays one-off costs no
  later message pays. The warm-up doubles as the proof that the path works: its tool reply is
  inspected, and a refused warm-up stops the phase instead of producing timeouts.

## Guards against measuring nothing

A latency drawn from a run where no message arrived is worse than no latency, because it looks like
evidence.

- Every delivery phase counts what it sent and what arrived. Short is a `problems` entry, `valid:
  false`, a `Delivery guard: FAIL` line, and exit code 1.
- Zero deliveries never produce statistics at all: `summarize([])` throws rather than returning
  zeros, and the table prints `NO DELIVERIES, nothing measured`.
- The unicast phase additionally insists on at least 30 delivered samples. The transport under test
  spreads latency across a whole poll interval, and five samples cannot tell 490 ms from 620 ms.
- The idle phase asserts the client was still alive and still registered at the end of the window,
  and that it made at least one request or held at least one stream. A quiet transport and a dead
  client both produce a small number; only one of them is a result.

## Why the gaps between samples are random

The current transport polls every second, so latency is decided by where in the poll cycle the
message lands. Send at a fixed delay after the previous delivery and every sample lands at the same
phase, and the harness would report a constant as though it were a distribution. Gaps are therefore
drawn uniformly across a poll interval, from a seeded generator (`--seed`) so a run is still
reproducible. The burst phase runs several rounds for the same reason: one burst samples one phase.

Even across rounds, the burst phase's absolute percentiles inherit the poll phase of the handful of
rounds that ran, so they move by hundreds of milliseconds between runs while nothing about the
transport changed. Read the queueing cost and the arrival spread instead: both are measured within a
round, so the poll offset cancels, and both were stable at single-digit milliseconds across repeat
runs on `dev`.

Percentiles are nearest rank, never interpolated, so every figure printed is a delivery that
actually happened and can be found in `samplesMs`.

## Isolation

Live Claude Code sessions run on this machine against a broker on port 7899.

- The harness binds only ports in its configured range (7770 to 7789 by default) and **refuses to
  start if that range contains 7899**. The default deliberately avoids 7810-7824, which
  `testsupport.ts` hands to the test suites, and 7850-7869, where `broker.wal.test.ts` and
  `server.logging.test.ts` pick a port at random and bind it WITHOUT going through that reservation.
  Sharing a port with one of those does not crash the harness: it makes the harness measure somebody
  else's broker and report the number as if it were this one's. Every run records the ports it
  actually bound, under `portsUsed` in the JSON and on the header line of the table, so a past
  result can be audited rather than trusted.
- Every client gets its own `CLAUDE_PEERS_SESSION_PID` pointing at a placeholder process of its own,
  its own `CLAUDE_PEERS_SPOOL_DIR`, its own working directory, and its own broker with its own
  database file. Without the session pid, `findSessionPid` walks the process tree, lands on a real
  Claude Code session, and spools benchmark traffic into a queue a hook is draining into somebody's
  context.
- Teardown kills only the processes this run spawned, by pid, and sweeps a port only for a process
  that is both listening on it and running `broker.ts`. Never `lsof | xargs kill` over a range: that
  signals connected clients too.

## Ways these numbers could still mislead

Read this before quoting them.

1. **"Render" stops at the MCP server.** The last hop, from the notification into the Claude
   session's rendered context, is invisible to any external observer. If a transport made delivery
   instant but Claude Code rendered it lazily, this harness would not see the difference.
2. **An idle machine is not a working machine.** Every client here does nothing but wait. A real
   session's MCP server is competing with a busy Claude Code process, a build, and a language
   server. Expect real latencies to be worse than these, and expect the gap to widen for a transport
   whose critical path is CPU-bound rather than timer-bound.
3. **Two to four peers, one machine, loopback.** Fan-out costs, connection limits and SQLite
   contention are all measured at a scale where none of them bite. That is the scale the design
   commits to, but a result here says nothing about a hundred peers.
4. **A poll transport's distribution is dominated by the poll, not by the work.** On `dev` the
   median is close to half the poll interval by construction. Improvements to everything else are
   invisible underneath it. Comparing p95 across transports compares two different mechanisms, not
   two speeds of the same mechanism.
5. **The idle count includes the harness's own proxy hop.** The proxy adds a localhost hop to every
   request in the idle phase, so the idle phase is not the phase to read latency from. The latency
   phases run without the proxy for exactly this reason.
6. **A run reflects one machine at one moment.** Nothing here is repeated over time, so a laptop
   that thermally throttled, or a background `bun install`, shows up as a slow transport. Re-run
   before believing a difference of tens of milliseconds; the sub-poll-interval part of the signal
   is smaller than the noise the machine can produce.
7. **The seed makes gaps reproducible, not results.** Two runs with the same seed send at the same
   offsets; they do not observe the same latencies, because the operating system is not seeded.
8. **A branch could pass the delivery guard and still be wrong.** The guard proves messages arrived,
   not that they arrived once, in order, or to the right peer. Those are the test suite's job, and
   this harness is not a substitute for it.
