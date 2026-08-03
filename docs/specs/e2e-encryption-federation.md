# E2E encryption and federation (PARKED)

Status: **parked, not implemented.** Captured 2026-08-03.

Activate this spec at the moment the broker stops being localhost-only, meaning the
first time two Claude Code agents on **different machines** need to talk. Until then it
buys close to zero threat reduction and costs a UX tax plus a new secret to leak.

Sources: adversarial teardown of `nguyenvanduocit/claude-room` (23 commits ahead of
upstream, Cloudflare Worker broker + XSalsa20-Poly1305 rooms) and of
`wise-toddler/claude-peers-mcp` (friend-key federation). Every defect listed below was
read out of that code, not hypothesised.

## Why this is parked and not built

The confidentiality goal of a room-key design is "the broker operator cannot read my
messages". While the broker is your own process, under your own UID, on your own
machine, that goal is vacuous:

- The key, the DB, the socket and the broker's memory are all readable by anything
  running as the same user. The macOS Keychain does not change this: `security
  find-generic-password` succeeds for any process running as you, and the per-item ACL
  that would restrict it binds to signed application binaries, which `bun script.ts`
  cannot usefully be.
- Every message already exists in plaintext in `~/.claude/projects/**/*.jsonl`, because
  the agent had to read it, and in the model provider's request logs. **The broker
  database is the smaller of the two plaintext stores.** Encrypting it while the
  transcript sits in the clear is security theatre.

The real localhost defect is **authorization, not confidentiality**, and it is being
fixed on the hardening branch instead. `Bun.serve({ hostname: "127.0.0.1" })` restricts
by interface, not by user: any local UID can register as a peer, enumerate every
session's `cwd` / `git_root` / `summary`, and inject text into another agent's context.
Because the MCP server instructs recipients to "RESPOND IMMEDIATELY", that is a
prompt-injection channel carrying agent authority. No cipher touches it.

## Seam: what lands on mainline vs what stays parked

Deliberate split, so this branch stays small and rebases cleanly instead of rotting
into a stale fork of `broker.ts`.

**Mainline (justified on localhost by replay/reattribution defence alone):**

- Canonical message header: `{v, scope, from, to, seq, ts}`, JSON with sorted keys.
- Ed25519 sender signature over `sha256(canon(header) || body)`. Keypair per MCP server
  instance, in memory only, public key published at `register`. Recipients pin
  `peer_id -> pubkey` on first sight and reject on change.
- AEAD call site with AAD bound to `canon(header)`.
- Reject `seq <= last_seen[from]`; reject `|now - ts| > 300s`.

**Parked here (only meaningful once the broker is remote):**

- Room key, its distribution, join credentials, rotation, federation transport.

The call sites do not move between the two. Activating this spec swaps a local key for
a room key and adds distribution around it.

## Design

### AEAD

XChaCha20-Poly1305 (libsodium) or AES-256-GCM (`crypto.subtle`, dependency-free).

**Do not use `nacl.secretbox`.** It has no AAD parameter, which is precisely what forced
claude-room into the cross-field lift defect below. Their nonce handling is otherwise
correct and worth copying: fresh 24-byte random nonces give a ~2^96 birthday bound, so
there is no reuse hazard in a stateless protocol.

AAD is the canonical header, including `scope`, `from`, `to`, `seq`, `ts`.

### Signature stays

The room key gives confidentiality; the Ed25519 signature gives attribution. Both are
needed. This is the piece claude-room is missing and the reason a hostile broker there
can reattribute at will. With both, a hostile broker is reduced to dropping and delaying.

### Key generation and distribution

- 32 bytes from `crypto.getRandomValues`. No KDF, because there is no passphrase. If a
  human-typed passphrase is ever added, use Argon2id (interactive opslimit, 64 MiB), never
  PBKDF2/SHA-256.
- Distribute out of band via a CLI command (`claude-peers invite`) printing to the
  user's terminal.
- **Never return the key in an MCP tool result.** claude-room does exactly this
  (`server.ts:427-432`), which writes the key verbatim into the local transcript JSONL
  and ships it to the model provider on the next turn. This is their most consequential
  key-management mistake.
- **Never document `export CLAUDE_ROOM_ID=...` in a shell rc.** Their setup skill
  recommends appending it to `~/.bashrc` (mode 0644, frequently committed to dotfiles
  repos, and visible via process environment to the same user).
- Store per-room keys in the Keychain under a per-room service name; reference the room
  by ID.

### Broker join credential

Derive as `HKDF-SHA256(room_key, info="claude-peers/join/v1")`, send in an
`Authorization` header, give it an expiry.

Not `SHA-256(key)` and never in a URL query string. claude-room puts `key_hash` on the
WebSocket query string (`server.ts:98`), so it lands in Cloudflare request logs and any
intermediary; since that hash is a permanent non-expiring join credential, anyone who
reads a log gets durable room access. They recognised the problem for the history
endpoint and moved it to a POST header, but left the handshake and `/info` on the query
string.

### Rotation

Re-key on membership shrink. Without it a departed peer decrypts everything, forever.
claude-room has no rotation and no forward secrecy.

## Defects to avoid (all read from claude-room's source)

1. **Fail-open metadata decrypt.** `decryptPeerInfo` (`server.ts:164-176`) does
   `catch { return peer; }`, returning the raw broker-supplied object. A broker that
   wants attacker-chosen text in front of the LLM sends a `peer_joined` whose
   `display_name` is plaintext prompt injection; authentication fails, the catch fires,
   and it renders verbatim. One failing field poisons all three, since a single try block
   wraps them. **Rule: a field that fails to authenticate is replaced with a fixed
   literal, never with broker-supplied bytes.** Their message path does this correctly,
   so the asymmetry looks like an oversight.
2. **No AAD, no domain separation.** The same key encrypts bodies, `display_name`,
   `summary` and `project_hint` with nothing distinguishing them, so a ciphertext lifted
   from a `summary` can be injected as a `message`.
3. **No sender authentication, no replay protection.** `from_id` is stamped by the
   broker; nothing binds ciphertext to sender, room, or sequence. 50 frames sit in
   storage ready to be replayed. Fixed here by the Ed25519 signature + `seq` + `ts`.
4. **Keyless rooms claimable first-come.** The Worker never requires `key_hash` at
   creation and the gate is conditional on the hash existing (`room.ts:64`), so a room
   with no stored hash accepts any connection, and whoever registers first owns it. The
   invariant is enforced only by client goodwill.
5. **Unvalidated key material.** `hexToBytes` never validates: `parseInt("zz", 16)` is
   `NaN`, coerced to `0`, so a typo'd invite code silently yields a key with zero bytes.
   Validate `/^[0-9a-f]{64}$/`.
6. **Room name never encrypted**, despite the README claiming the broker only sees
   ciphertext.
7. **"Private" DMs are not private from room members.** `to_id` routing is enforced only
   by the broker; the ciphertext is under the shared room key, so any member holding the
   frame can read it. One symmetric key per room means no compartmentalisation and no
   defence against a malicious member, ever.

## Federation transport (wise-toddler)

Reviewed but not adopted: near-zero value for a single-machine user, and its README
concedes that `ssh -R` tunnelling bypasses the loopback guard. Revisit alongside this
spec; prefer the Unix-socket + token model from the hardening branch extended over a
mutually authenticated transport, rather than bearer friend-keys.

## What remains unprotected under any of this

State this in the README rather than claiming more:

1. **Any process running as the same user.** It reads the key, the socket, the DB, and
   the broker's memory. No design in this space fixes that.
2. **The transcripts.** `~/.claude/projects/**/*.jsonl` plus provider request logs.
3. **Metadata at a remote broker:** membership, timing, sizes, `to_id`, join/leave.
4. **A compromised peer.** Shared-key rooms have no compartmentalisation and no forward
   secrecy without a ratchet.
5. **Prompt injection from a legitimate member.** Cryptography authenticates the sender;
   it does not make the sender benign. Mitigation is rendering peer content as delimited
   untrusted data, which lands on mainline.
