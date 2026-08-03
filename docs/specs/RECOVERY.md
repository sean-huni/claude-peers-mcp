# RECOVERY: how to pick up the parked encryption branch cold

Audience: whoever opens this branch months from now with no memory of it, including Sean.
Goal: be oriented in under 15 minutes and know whether to revive, rebase, or leave it alone.

Written 2026-08-03. Every factual claim below was read out of this repo or its git history.
Anything that could not be verified from the repo is labelled **unverified**.

---

## 1. Orientation

**What this branch is.** `feat-encryption` holds a design document and nothing
else: `docs/specs/e2e-encryption-federation.md` (160 lines, added by commit `e2c9dfc`) plus
this recovery note. It specifies room-key end-to-end encryption and cross-machine federation
for the claude-peers broker.

**What this branch is not.** It is not an implementation. At the time of writing there is no
cryptography anywhere in the broker: a grep of `broker.ts` at the hardening branch tip for
`ed25519|signature|aead|nonce|encrypt` returns nothing.

**The one-sentence trigger for un-parking it:**

> You are about to run Claude Code agents on more than one machine.

Until that is true, this branch buys close to zero threat reduction. The spec states the
reasoning in its "Why this is parked and not built" section: on one machine the key, the DB,
the socket and the broker's memory are all readable by anything running as the same UID, and
every message already sits in plaintext in `~/.claude/projects/**/*.jsonl` regardless.

### The distinction you will misremember

This is the single thing most likely to be garbled on re-reading, so it is stated twice.

| | `feat-peers-hardening` | `feat-encryption` (this branch) |
| --- | --- | --- |
| Concern | **LOCAL data on ONE machine** | **CROSS-MACHINE confidentiality** |
| Question it answers | "Can another process on this Mac read my agents' messages off disk, and do messages survive a failed render?" | "Can a broker operator on someone else's box read my agents' messages in flight?" |
| Status | Active, shipping now | Parked, design only |
| Threat model | Same-machine file permissions, data retention, delivery durability | Remote broker operator, network path, departed room members |

They are not two halves of one feature and neither supersedes the other. The hardening branch
is about **data at rest and delivery semantics on your own machine**. This branch is about
**confidentiality of data in transit between machines**. If you find yourself thinking "we
already did the security work on the other branch", you have made exactly the mistake this
table exists to prevent.

---

## 2. Branch map

State as of 2026-08-03, 14:18 +0200. **The hardening branch was moving while this was written**
(its tip advanced from `289a293` to `f59d200` mid-session), so re-derive rather than trusting
the SHAs below.

```
640183f  main                       Merge pull request #45 from louislva/claude/add-mit-license-2EbSt
   |
135d165  (merge base)               {JIRA-TICKET} Switch auto-summary from OpenAI to Claude
   |\
   | \
   |  e2c9dfc  ENCRYPTION (here)    {JIRA-TICKET} Park the E2E encryption and federation design
   |
   289a293  HARDENING              {JIRA-TICKET} Make message delivery durable and stop retaining plaintext
   |
   f59d200  HARDENING (tip)        {JIRA-TICKET} Acknowledge pushed messages so they are not re-delivered
```

> The `{JIRA-TICKET}` prefixes in the commit subjects above are literal: they are what those
> commits actually say, and published history is corrected forward, never rewritten. The
> **branches** were later renamed to drop the prefix, so every command in this file uses the
> current names: `feat-encryption` and `feat-peers-hardening`.

- `main` is upstream (`louislva/claude-peers-mcp`) and is behind both feature branches. It does
  not contain the auto-summary switch.
- The **merge base of the two feature branches is `135d165`**, not `main`. Both branched after
  the auto-summary commit.
- **Hardening is ahead.** It carries three files of real code change against the merge base:
  `broker.ts`, `server.ts`, `shared/types.ts`.
- **Encryption is docs-only.** It carries no code change at all against the merge base.

### What lives on each branch

**Hardening (`broker.ts` +55/-7, `server.ts` +27/-1, `shared/types.ts` +10):**

- Polling no longer consumes. Upstream `handlePollMessages` ran `markDelivered.run(msg.id)`
  over every polled row (`main:broker.ts:211-219`), so a message was lost outright if the
  client polled and then failed to render it.
- New `/ack-messages` endpoint. Acknowledgement `DELETE`s the row rather than flagging it, and
  is scoped `WHERE id = ? AND to_id = ?` so a cooperating peer cannot ack another peer's mail.
- Client side: `server.ts` acks each message only after a successful push, tracks pushed ids in
  a `pushedMessageIds` set, and treats a failed ack as non-fatal.
- `chmodSync(..., 0o600)` on the DB and its `-wal` / `-shm` siblings, which SQLite creates 0644.
- `PRAGMA secure_delete = ON`.
- TTL sweep of undelivered mail, `CLAUDE_PEERS_MSG_TTL` defaulting to 3600000 ms, swept every
  60 seconds. Needed because polling no longer consumes and rows would otherwise accumulate.

**Encryption (this branch):**

- `docs/specs/e2e-encryption-federation.md`
- `docs/specs/RECOVERY.md` (this file)

### Commands to see the current state

Run these from `/Users/sean/env/repo/ai/claude-peers-mcp`.

```bash
# Where every branch actually is, right now.
git branch -v
git log --oneline main
git log --oneline feat-peers-hardening
git log --oneline feat-encryption

# The real divergence point, and what hardening added since.
git merge-base feat-encryption feat-peers-hardening
git diff --stat "$(git merge-base feat-encryption feat-peers-hardening)" \
  feat-peers-hardening

# Read hardening code WITHOUT checking that branch out.
git show 'feat-peers-hardening:broker.ts'
git show 'feat-peers-hardening:shared/types.ts'
```

> **Worktree note.** While this document was written, the hardening branch was checked out in a
> **linked worktree**, not in the main working directory. `git branch -v` marks such a branch
> with a leading `+`. Check `git worktree list` before assuming you can switch to it: a branch
> checked out elsewhere cannot be checked out here, and that is a feature, not an error.

> **zsh trap that cost real time.** `git show "$REF:server.ts"` silently misbehaves in zsh when
> `$REF` is a variable, because `:s` parses as a parameter-expansion modifier. It returned false
> negatives that looked like "this code does not exist". Use a literal quoted ref
> (`git show 'feat-peers-hardening:server.ts'`) or brace the variable
> (`"${REF}:server.ts"`).

---

## 3. The seam: mainline versus parked

This is the checklist that stops this branch rotting into a stale fork of `broker.ts`. The
split is defined in the spec under "Seam: what lands on mainline vs what stays parked" and is
restated here as a checklist, not amended. If you change the split, change the spec, not just
this file.

**Lands on MAINLINE.** Each item is justified on a single localhost machine by replay and
reattribution defence alone, so none of it needs to wait for federation.

- [ ] Canonical message header `{v, scope, from, to, seq, ts}`, JSON with sorted keys.
- [ ] Ed25519 sender signature over `sha256(canon(header) || body)`.
- [ ] Keypair per MCP server instance, in memory only, public key published at `register`.
- [ ] Recipients pin `peer_id -> pubkey` on first sight and reject on change.
- [ ] AEAD call site present, with AAD bound to `canon(header)`.
- [ ] Reject `seq <= last_seen[from]`.
- [ ] Reject `|now - ts| > 300s`.
- [ ] Render peer content as delimited untrusted data (prompt-injection mitigation).

**STAYS PARKED here.** Only meaningful once the broker is remote.

- [ ] Room key itself.
- [ ] Room key distribution and join credentials.
- [ ] Rotation on membership shrink.
- [ ] Federation transport.

**Why the split holds.** The spec's load-bearing claim is that *the call sites do not move
between the two*. Activating this spec swaps a local key for a room key and adds distribution
around it. So the mainline half can ship first, on its own merits, and this branch stays a thin
delta instead of a divergent rewrite of the broker.

**Health check for this branch.** If mainline ever grows an AEAD call site whose AAD is not the
canonical header, or a signature scheme that is not per-instance Ed25519 pinned on first sight,
the seam has broken and this spec needs re-deriving rather than rebasing.

---

## 4. Rebase and revival runbook

Assumes hardening has moved on and you now want this branch current. Nothing here rewrites a
shared branch.

### Step 0: confirm you are allowed to move

```bash
cd /Users/sean/env/repo/ai/claude-peers-mcp
git worktree list          # is hardening checked out elsewhere?
git status                 # must be clean before anything else
git switch feat-encryption
```

### Step 1: integrate on THIS branch, never on the trunk

Per Sean's standing rule, conflicts are resolved on the feature branch, and the integration
trunk wins by default. Here the hardening branch is the trunk analogue: it is the branch other
work builds on.

```bash
git fetch origin
git switch feat-encryption
git merge feat-peers-hardening    # resolve HERE, hardening wins by default
```

Do not merge this branch into hardening to "sync" it, and do not rebase hardening onto anything.

### Step 2: expected conflicts

**Today, a clean merge is expected.** This branch touches only `docs/`, and hardening touches
only `broker.ts`, `server.ts`, `shared/types.ts`. There is no file overlap, so the merge should
be a fast-forward of the code files with no conflict at all. Verify with:

```bash
git diff --stat feat-encryption feat-peers-hardening -- docs/
```

**Once this branch carries code, expect conflicts in exactly two files.**

`shared/types.ts`. Both sides append interfaces to the same tail region. At the hardening tip
the interfaces sit in this order: `Peer` (line 4), `Message` (15), `RegisterRequest` (26),
`RegisterResponse` (34), `HeartbeatRequest` (38), `SetSummaryRequest` (42), `ListPeersRequest`
(47), `SendMessageRequest` (55), `PollMessagesRequest` (61), `PollMessagesResponse` (65),
`AckMessagesRequest` (69), `AckMessagesResponse` (74).

- Resolution: **take both sides.** These are additive interface declarations, and an
  append-versus-append conflict is textual, not semantic. Keep hardening's ordering and add the
  encryption types after it.
- The one real semantic collision to watch: `Message` and `SendMessageRequest` both carry a
  `text` field that the encryption work will want to replace with a header plus ciphertext.
  Hardening wins on field naming and on anything to do with `delivered` / ack scoping. Introduce
  new fields alongside `text` rather than renaming it, so the ack path keeps compiling.

`broker.ts`. At the hardening tip the relevant call sites are `handleSendMessage` (line 229,
with the actual `insertMessage.run` at 236), `handlePollMessages` (240), `handleAckMessages`
(248), the `Bun.serve` block (267, `hostname: "127.0.0.1"` at 269) and the route switch (cases
at 285 through 301).

- Resolution rule: **hardening's delivery and retention semantics are non-negotiable and win
  every time.** Specifically, do not let an encryption change reintroduce consume-on-poll, do
  not turn the ack `DELETE` back into an `UPDATE ... SET delivered = 1`, and do not drop the
  `chmodSync` block, `PRAGMA secure_delete`, or the TTL sweep.
- The encryption change should be **additive at the same call sites**: sign and seal in
  `handleSendMessage` before `insertMessage.run`, verify and open on the read path. If a
  resolution requires restructuring `handlePollMessages` or `handleAckMessages`, stop: that is
  the seam breaking, and it means the mainline half of section 3 was never landed.

### Step 3: prove it still works before going near anything else

The hardening commits were verified against a live broker, so hold the revival to the same bar.
The messages of `289a293` and `f59d200` record what was checked: poll is non-consuming, a
foreign ack returns `acked=0`, an owner ack returns `acked=1`, the row is gone from the table,
the file lands 0600, and a single message renders once across three poll cycles with an empty
queue afterwards. Reproduce all of that, not a subset.

```bash
bun install
bun test                       # note: no test files existed at time of writing (unverified coverage)
bun cli.ts status
ls -l ~/.claude-peers.db       # must be -rw------- once a hardened broker has run
```

---

## 5. Decision log

**Decision.** Do not build room-key E2E encryption or federation now. Capture the design, park
the branch, and ship local hardening instead.

**Decided by.** Sean.

**When.** 2026-08-03. Commit `e2c9dfc` is authored `Mon Aug 3 14:13:32 2026 +0200` by
Sean Huni. The hardening work landed the same afternoon (`289a293` at 14:15:56, `f59d200` at
14:18:24).

**Evidence that the real localhost defect is authorization, not confidentiality.** All three
verified directly:

1. **The broker had zero authentication.** A grep of upstream `main:broker.ts` for
   `Authorization|Bearer|token|auth` returns **no matches**. There is no caller identity at all:
   `Bun.serve({ hostname: "127.0.0.1" })` restricts by network interface, not by user, so any
   local UID can register as a peer, enumerate every session's `cwd` / `git_root` / `summary`,
   and inject text into another agent's context. Still true at the hardening tip `f59d200`: the
   same grep returns nothing, and `handleAckMessages` trusts the caller-supplied `body.peer_id`.
   The ack scoping is therefore a correctness guard against a cooperating client, not a security
   boundary against an attacker. **No cipher touches any of this**, which is the whole argument
   for parking.
2. **The DB was world-readable 0644 plaintext.** Upstream `broker.ts` contains no `chmod` and no
   `secure_delete` (grep returns no matches). Observed live on disk before the hardened broker
   restarted: `-rw-r--r--@ 1 sean staff 4096 /Users/sean/.claude-peers.db`, with `-shm` and
   `-wal` siblings at the same mode.
3. **Delivery flagged rather than deleted.** Upstream prepared `markDelivered` as
   `UPDATE messages SET delivered = 1 WHERE id = ?` (`main:broker.ts:121-123`) and called it
   from `handlePollMessages` on every polled row (`main:broker.ts:211-219`). Two consequences:
   a message polled but not rendered was lost, and delivered message text was retained in the
   file indefinitely.

**Supporting argument (from the spec, not re-derived here).** Same-UID processes can read the
key, the DB, the socket and the broker's memory; the macOS Keychain does not change this. And
every message already exists in plaintext in `~/.claude/projects/**/*.jsonl` plus the provider's
request logs, which makes the broker database the *smaller* of the two plaintext stores.
Encrypting it while the transcript sits in the clear is theatre.

### Rejected options

**Rejected: adopt `nguyenvanduocit/claude-room` wholesale.** Described in the spec as 23 commits
ahead of upstream, with a Cloudflare Worker broker and XSalsa20-Poly1305 rooms. Rejected on its
crypto, with the following defects cited in the spec and attributed to that project's source.
*These line references come from the spec and could not be re-verified here, because that
repository is not present in this checkout: treat the citations as the spec's, not as
independently confirmed.*

- **Fail-open metadata decrypt.** `decryptPeerInfo` does `catch { return peer; }`
  (`server.ts:164-176`), returning the raw broker-supplied object. A hostile broker sends a
  `peer_joined` whose `display_name` is plaintext prompt injection; authentication fails, the
  catch fires, and it renders verbatim. One failing field poisons all three, since a single try
  block wraps them.
- **No sender authentication and no replay protection.** `from_id` is stamped by the broker and
  nothing binds ciphertext to sender, room, or sequence, so a hostile broker can reattribute at
  will and stored frames can be replayed.
- **No AAD and no domain separation**, so a ciphertext lifted from a `summary` can be injected
  as a `message`.
- **It returns the room key inside an MCP tool result** (`server.ts:427-432`). This is the most
  consequential mistake: it writes the key verbatim into the local transcript JSONL and ships it
  to the model provider on the next turn.
- Also cited: keyless rooms claimable first-come, unvalidated hex key material where a typo
  yields zero bytes, the room name never actually encrypted, and "private" DMs readable by any
  room member because one symmetric key per room gives no compartmentalisation.

**Rejected: adopt `wise-toddler` federation.** Reviewed and not adopted: near-zero value for a
single-machine user, and the spec records that its README concedes `ssh -R` tunnelling bypasses
the loopback guard. The stated preference is to extend the Unix-socket plus token model over a
mutually authenticated transport rather than use bearer friend-keys.

> **unverified.** The additional specifics that motivated this rejection in conversation, namely
> a bearer friend-key sent over plaintext HTTP, a `0.0.0.0` bind, and cross-friend
> impersonation, are **not** recorded in `e2e-encryption-federation.md` and that repository is
> not present in this checkout. They could not be confirmed from anything in this repo. Re-verify
> against the source before citing them as fact or re-litigating the decision.

---

## 6. Prerequisites for the day this is un-parked

The room key is not worth adding until all of these are already true on mainline. Working
through them in order is also the cheapest way to discover that you did not need this branch yet.

- [ ] **The trigger has actually fired.** Agents genuinely need to talk across two machines.
      A second machine you *might* use later does not count.
- [ ] **The broker has authentication at all.** This is the real defect and it is still open:
      the grep in section 5 returns nothing at the hardening tip. Confidentiality on top of an
      unauthenticated broker protects the wrong thing. Any caller must prove identity before
      `/register`, `/list-peers`, `/send-message`, `/poll-messages` and `/ack-messages` do
      anything, and `body.peer_id` on the ack path must stop being self-asserted.
- [ ] **The mainline half of the seam has landed and is green:** canonical header, Ed25519
      per-instance keypair with pubkey published at `register` and pinned on first sight, AAD
      bound to `canon(header)`, `seq` monotonicity, and the 300 second timestamp window. Every
      unchecked box in section 3's mainline list is a blocker.
- [ ] **Peer content is rendered as delimited untrusted data.** `server.ts:153` instructs the
      recipient to "RESPOND IMMEDIATELY" to inbound peer messages, which makes this a
      prompt-injection channel carrying agent authority. Cryptography authenticates a sender; it
      does not make the sender benign.
- [ ] **The hardening guarantees are intact and tested:** non-consuming poll, ack that deletes,
      ack scoped to the caller's mailbox, 0600 on the DB and its `-wal` / `-shm` siblings,
      `PRAGMA secure_delete`, and the TTL sweep.
- [ ] **There is an automated test suite.** At the time of writing the verification recorded in
      the hardening commit messages was manual, against a live broker. Reviving crypto work with
      no regression net is how the delivery guarantees get silently broken.
- [ ] **A transport decision exists.** The spec's stated preference is Unix socket plus token,
      extended over a mutually authenticated transport. Bearer friend-keys are already rejected.
      Pick and write down the replacement before touching key material.
- [ ] **Key storage and distribution are decided:** 32 bytes from `crypto.getRandomValues`, no
      KDF absent a passphrase, out-of-band distribution via a CLI command, per-room Keychain
      entries. And the two hard prohibitions from the spec hold: **never** return the key in an
      MCP tool result, **never** document exporting it from a shell rc.
- [ ] **Rotation on membership shrink is in the plan**, not deferred. Without it a departed peer
      decrypts everything, forever.
- [ ] **The README stops over-claiming.** The spec's final section lists what remains
      unprotected under any of this: same-UID processes, the transcripts, metadata at a remote
      broker, a compromised peer, and prompt injection from a legitimate member. That belongs in
      the README rather than a stronger claim.

---

## Where to read next

1. `docs/specs/e2e-encryption-federation.md` in this branch. It is the authority. This file is
   navigation around it and deliberately does not restate its design decisions.
2. `git show 'feat-peers-hardening:broker.ts'` for what actually shipped locally.
3. `README.md` for the user-facing model: broker daemon on `localhost:7899` with SQLite, one MCP
   server per Claude Code session, one second polling, channel push into the session.
