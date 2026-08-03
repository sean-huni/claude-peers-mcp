/**
 * Failure backoff and log rate limiting for the inbound poll loop.
 *
 * The loop runs once a second for the whole life of a session, and its stderr
 * lands in an MCP log file on disk. A broker that is down therefore costs one
 * line per second, forever, which is around 86,000 identical lines a day per
 * session. Retrying every second is no more useful than saying so every second.
 *
 * The policy:
 *   - retry on an exponentially growing interval, capped, so a dead broker is
 *     probed roughly once a minute rather than sixty times;
 *   - report the first failure immediately, because a session that cannot reach
 *     its broker is broken and the operator needs to know;
 *   - after that report only a state change (a different error) or a periodic
 *     reminder, each carrying the count of what it stands in for;
 *   - report the recovery once.
 *
 * All time is passed in, so the multi-minute behaviour is testable without
 * sleeping and the class holds no timers of its own.
 */

export interface PollBackoffOptions {
  /** Delay after the first failure. */
  baseDelayMs?: number;
  /** Ceiling for the delay, so a long outage still gets probed. */
  maxDelayMs?: number;
  /** Growth factor per consecutive failure. */
  factor?: number;
  /** Minimum gap between two reports of an unchanged, ongoing outage. */
  quietMs?: number;
}

const DEFAULTS: Required<PollBackoffOptions> = {
  baseDelayMs: 1_000,
  maxDelayMs: 60_000,
  factor: 2,
  quietMs: 300_000,
};

function humanDelay(ms: number): string {
  return ms >= 1_000 ? `${Math.round(ms / 1_000)}s` : `${ms}ms`;
}

export class PollBackoff {
  private readonly opts: Required<PollBackoffOptions>;
  private failures = 0;
  private nextAttemptAt = 0;
  private currentDelayMs = 0;
  private lastReason: string | null = null;
  private lastLoggedAt = 0;
  private suppressed = 0;

  constructor(options: PollBackoffOptions = {}) {
    this.opts = { ...DEFAULTS, ...options };
  }

  /** Consecutive failures since the last success. */
  get failureCount(): number {
    return this.failures;
  }

  /** The interval currently being waited out. Zero when healthy. */
  get delayMs(): number {
    return this.currentDelayMs;
  }

  /** Whether the loop should attempt a poll at this instant. */
  ready(now: number): boolean {
    return now >= this.nextAttemptAt;
  }

  /**
   * Record a failed poll.
   *
   * @returns the line to log, or null when this failure is covered by one
   * already logged.
   */
  noteFailure(reason: string, now: number): string | null {
    this.failures++;
    const growth = Math.pow(this.opts.factor, this.failures - 1);
    this.currentDelayMs = Math.min(this.opts.baseDelayMs * growth, this.opts.maxDelayMs);
    this.nextAttemptAt = now + this.currentDelayMs;

    const first = this.failures === 1;
    const changed = reason !== this.lastReason;
    const quietExpired = now - this.lastLoggedAt >= this.opts.quietMs;
    this.lastReason = reason;

    if (!first && !changed && !quietExpired) {
      this.suppressed++;
      return null;
    }

    const covered = this.suppressed;
    this.suppressed = 0;
    this.lastLoggedAt = now;

    const retry = `retrying in ${humanDelay(this.currentDelayMs)}`;
    const alsoCovered = covered > 0 ? `; ${covered} identical error(s) suppressed` : "";

    if (first) {
      return `Poll error: ${reason} (${retry})`;
    }
    if (changed) {
      return `Poll error: ${reason} (attempt ${this.failures}, ${retry}${alsoCovered})`;
    }
    return `Broker still unreachable after ${this.failures} attempts: ${reason} (${retry}${alsoCovered})`;
  }

  /**
   * Record a successful poll.
   *
   * @returns the recovery line the first time it follows a failure, else null.
   * A healthy loop is silent.
   */
  noteSuccess(now: number): string | null {
    if (this.failures === 0) return null;

    const failed = this.failures;
    const covered = this.suppressed;
    this.failures = 0;
    this.suppressed = 0;
    this.currentDelayMs = 0;
    this.nextAttemptAt = 0;
    this.lastReason = null;
    this.lastLoggedAt = 0;

    const alsoCovered = covered > 0 ? ` (${covered} error(s) were suppressed)` : "";
    return `Broker reachable again after ${failed} failed attempt(s)${alsoCovered}`;
  }
}
