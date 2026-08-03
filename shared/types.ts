// Unique ID for each Claude Code instance (generated on registration)
export type PeerId = string;

export interface Peer {
  id: PeerId;
  pid: number;
  cwd: string;
  git_root: string | null;
  tty: string | null;
  summary: string;
  registered_at: string; // ISO timestamp
  last_seen: string; // ISO timestamp
}

export interface Message {
  id: number;
  from_id: PeerId;
  to_id: PeerId;
  text: string;
  sent_at: string; // ISO timestamp
  delivered: boolean;
}

// --- Broker API types ---

export interface RegisterRequest {
  pid: number;
  cwd: string;
  git_root: string | null;
  tty: string | null;
  summary: string;
}

export interface RegisterResponse {
  id: PeerId;
  token: string;
}

export interface HeartbeatRequest {
  id: PeerId;
}

export interface SetSummaryRequest {
  id: PeerId;
  summary: string;
}

export interface ListPeersRequest {
  scope: "machine" | "directory" | "repo";
  // The requesting peer's context (used for filtering)
  cwd: string;
  git_root: string | null;
  exclude_id?: PeerId;
}

export interface SendMessageRequest {
  from_id: PeerId;
  to_id: PeerId;
  text: string;
}

/**
 * One message to every peer the scope selects, the sender excluded.
 *
 * Carries the same cwd and git_root as ListPeersRequest because recipients are resolved by the
 * same selection: a broadcast's audience is exactly the peers list_peers would have returned.
 */
export interface BroadcastMessageRequest {
  from_id: PeerId;
  text: string;
  // Omitted means "machine": the widest scope, and the one a caller who did not think about scope
  // almost certainly meant.
  scope?: "machine" | "directory" | "repo";
  cwd: string;
  git_root: string | null;
}

export interface BroadcastMessageResponse {
  ok: boolean;
  // Zero is a success, not an error. Nobody listening is an ordinary state.
  delivered_to: number;
}

export interface PollMessagesRequest {
  id: PeerId;
}

export interface PollMessagesResponse {
  messages: Message[];
}

export interface AckMessagesRequest {
  peer_id: string;
  message_ids: number[];
}

export interface AckMessagesResponse {
  ok: boolean;
  acked: number;
}
