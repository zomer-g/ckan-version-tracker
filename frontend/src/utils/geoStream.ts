/**
 * Shared streaming primitives for parsing large GeoJSON FeatureCollections
 * without loading the whole text into a single JS string. Used by
 * GovmapView (auto-discover filter mode) and GrowthPage (preset layer
 * mode) — wherever a layer might be too big to fit in mobile-tab memory.
 *
 * The pipeline:
 *   1. `downloadToBlob` drains the HTTP response into a Blob, reporting
 *      progress. Magic-byte sniffing detects gzip so a single helper
 *      handles both compressed and uncompressed odata responses (the
 *      `/download` route strips the `.gz` suffix from filenames, so we
 *      can't rely on URL extension).
 *   2. `parseStream` re-reads the cached Blob (zero network for repeat
 *      passes), pipes through DecompressionStream when needed, and
 *      yields values matching a JSONPath one at a time via `onValue`.
 *
 * Why a Blob, not chunks-in-an-array: enables the two-pass mobile
 * "filter-first" UX — pass 1 parses `$.features.*.properties` to build
 * the filter sidebar, pass 2 parses `$.features.*` filtered by the
 * user's selection. Pass 2 reads from the cached body, so the user only
 * pays the cellular cost once.
 */
import { JSONParser } from "@streamparser/json";

export async function downloadToBlob(args: {
  url: string;
  isCancelled: () => boolean;
  onProgress: (frac: number) => void;
}): Promise<{ blob: Blob; isGz: boolean }> {
  const { url, isCancelled, onProgress } = args;
  const resp = await fetch(url);
  if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
  if (!resp.body) throw new Error("Response has no body");
  const totalRaw = resp.headers.get("content-length");
  const total = totalRaw ? Number(totalRaw) : 0;
  const reader = resp.body.getReader();
  const chunks: Uint8Array[] = [];
  let received = 0;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    if (isCancelled()) {
      await reader.cancel().catch(() => {});
      throw new Error("cancelled");
    }
    const { done, value } = await reader.read();
    if (done) break;
    if (value) {
      chunks.push(value);
      received += value.byteLength;
      if (total > 0) onProgress(received / total);
    }
  }
  const isGz =
    chunks.length > 0 &&
    chunks[0].length >= 2 &&
    chunks[0][0] === 0x1f &&
    chunks[0][1] === 0x8b;
  return { blob: new Blob(chunks), isGz };
}

export async function parseStream<T>(args: {
  blob: Blob;
  isGz: boolean;
  path: string;
  onValue: (v: T) => void;
  isCancelled: () => boolean;
}): Promise<void> {
  const { blob, isGz, path, onValue, isCancelled } = args;
  const baseStream = blob.stream();
  const stream =
    isGz && typeof DecompressionStream !== "undefined"
      ? baseStream.pipeThrough(new DecompressionStream("gzip"))
      : baseStream;
  const parser = new JSONParser({ paths: [path], keepStack: false });
  let parserError: Error | null = null;
  parser.onValue = (info: { value?: unknown }) => {
    if (info.value !== undefined) onValue(info.value as T);
  };
  parser.onError = (e: Error) => {
    parserError = e;
  };
  const reader = stream.getReader();
  // eslint-disable-next-line no-constant-condition
  while (true) {
    if (isCancelled()) {
      await reader.cancel().catch(() => {});
      throw new Error("cancelled");
    }
    const { done, value } = await reader.read();
    if (done) break;
    if (value) {
      parser.write(value);
      if (parserError) throw parserError;
    }
  }
  try {
    parser.end();
  } catch {
    // Mid-stream end is intentional in some callers — swallow.
  }
}
