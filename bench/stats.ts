/**
 * Summary statistics for latency samples.
 *
 * Nearest-rank percentiles, never interpolated. Every number this module reports is a latency that
 * an actual message actually experienced, so a reader can go and find the sample that produced it.
 * An interpolated p95 of 512 ms when no message ever took 512 ms is a number nobody can check, and
 * the whole point of this harness is that the numbers can be checked.
 *
 * Nearest rank means the median of an even-sized sample is the upper of the two middle values
 * rather than their average. Over 30 samples of a roughly uniform distribution the difference is
 * a few milliseconds, and the honesty is worth more than the few milliseconds.
 */

export interface LatencyStats {
  /** How many samples went into these numbers. Zero samples is an error, not a statistic. */
  count: number;
  min: number;
  median: number;
  p95: number;
  max: number;
  /** Reported for shape only: a mean near the midpoint of min and max is the signature of a poll. */
  mean: number;
}

/**
 * The value at a fraction of the way through the sample, by nearest rank.
 *
 * Throws on an empty sample rather than returning 0 or NaN. A benchmark that reports zero because
 * nothing was measured is the exact failure this harness is built to make impossible.
 */
export function percentile(samples: readonly number[], fraction: number): number {
  if (samples.length === 0) {
    throw new Error("percentile of an empty sample: nothing was measured");
  }
  if (!(fraction >= 0 && fraction <= 1)) {
    throw new Error(`percentile fraction must be within 0..1, got ${fraction}`);
  }
  const sorted = [...samples].sort((a, b) => a - b);
  const rank = Math.max(1, Math.ceil(fraction * sorted.length));
  return sorted[rank - 1] as number;
}

export function summarize(samples: readonly number[]): LatencyStats {
  if (samples.length === 0) {
    throw new Error("cannot summarize an empty sample: nothing was measured");
  }
  const total = samples.reduce((sum, value) => sum + value, 0);
  return {
    count: samples.length,
    min: percentile(samples, 0),
    median: percentile(samples, 0.5),
    p95: percentile(samples, 0.95),
    max: percentile(samples, 1),
    mean: total / samples.length,
  };
}

/** One decimal place, as a string, so a table column lines up and a JSON reader still gets a number. */
export function round(value: number, decimals = 1): number {
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
}

/**
 * A seeded pseudo-random generator, so a run is reproducible.
 *
 * The gaps between samples are randomised on purpose (see the harness: a fixed gap samples the poll
 * cycle at one fixed phase and reports a constant as if it were a distribution). Randomised and
 * reproducible are not in tension as long as the seed is an input, which it is.
 *
 * mulberry32: small, fast, and good enough for choosing a delay in milliseconds.
 */
export function seededRandom(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
