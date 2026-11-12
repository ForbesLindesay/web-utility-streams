// Utilities shared by the benchmark suites in `suites/`.
//
// Each suite's cases return the number of records (or chunks) they read and
// the sum of their ids, so the runner can check every library got the right
// result. Encoder cases return the number of characters they output, and in a
// separate, untimed "verify" run they also parse their output back, so the
// runner can check it contains the right records. Cases can also return a
// `finish` function, which is called after the timer has stopped, to check
// their result.

import { createReadStream, type ReadStream } from "node:fs";
import { open } from "node:fs/promises";
import type { EventEmitter } from "node:events";
import { Readable } from "node:stream";
import { records, type DataFormat, type DataRecord } from "./data.ts";

/**
 * What the runner tells each case about the measurement
 */
export interface RunOptions {
  /** The data file to read (for suites that read a data file) */
  file: string;
  /** The number of records (or chunks) for the size being measured */
  count: number;
  /** The size being measured, in bytes */
  size: number;
  /** Whether this is an untimed run, where encoders check their output */
  verify: boolean;
}

/**
 * The result of a case, which the runner checks
 */
export interface CaseResult {
  /** The number of records (or chunks) read, which must match `count` */
  count?: number;
  /** The sum of their ids, which must match the expected sum */
  idSum?: number;
  /** The number of characters an encoder output */
  chars?: number;
  /** A value that isn't checked, so the work can't be optimised away */
  ignored?: number;
  /** The number of bytes a consumer read, which must match the file's size */
  bytes?: number;
  /** An error message, if the case found a problem */
  error?: string;
}

/**
 * What a case's `run` function returns. `finish` is called after the timer
 * has stopped, so a case can check its result without it being timed.
 */
export interface RunResult extends CaseResult {
  finish?: () => CaseResult | Promise<CaseResult>;
}

/**
 * What the worker process reports to the runner for each measurement
 */
export interface WorkerResult extends CaseResult {
  /** The time the case took to run, in milliseconds */
  ms: number;
  /** The peak resident set size of the worker process */
  maxRssBytes: number;
}

export interface BenchmarkCase {
  /** The library's name, or "native" or "baseline" */
  library: string;
  /** How the library is used */
  name: string;
  /** Whether this case uses this package (shown in bold) */
  ours?: boolean;
  /** Whether this is a baseline, shown for context at the end of the table */
  baseline?: boolean;
  /** The largest size, in bytes, that this case can handle */
  maxSize?: number;
  run: (options: RunOptions) => RunResult | Promise<RunResult>;
}

export interface Suite {
  description: string;
  /** The format of the data file this suite reads, or that encoders produce */
  format?: DataFormat;
  /** Whether the cases encode generated records, rather than reading a file */
  encoder?: boolean;
  /** Whether the cases read a generated stream of `{ id }` chunks */
  generated?: boolean;
  cases: BenchmarkCase[];
}

/**
 * Anything with an id, that can be counted towards `idSum`
 */
export interface WithId {
  id: number;
}

/**
 * Treat a parsed JSON value as a record. The runner checks the count and id
 * sum, so a wrong value will be noticed.
 */
export const asRecord = (value: unknown): WithId => value as WithId;

/**
 * The number of records (or chunks) read, and the sum of their ids
 */
export interface Counted {
  count: number;
  idSum: number;
}

export const OURS = "web-utility-streams";

// Whole-file baselines need the entire file in a single string, and V8's
// maximum string length is about 512MB, so they're only run for small files.
export const MAX_IN_MEMORY_BYTES = 512e6;

/**
 * Open a file as a web ReadableStream, using `FileHandle.readableWebStream()`,
 * which reads the file in 16KiB chunks. Libraries that use web streams read
 * the file this way.
 */
export async function webFile(file: string): Promise<{
  stream: ReadableStream<Uint8Array>;
  close: () => Promise<void>;
}> {
  const handle = await open(file);
  return {
    stream: handle.readableWebStream() as ReadableStream<Uint8Array>,
    close: () => handle.close(),
  };
}

/**
 * Open a file as a Node.js Readable stream, using `fs.createReadStream`,
 * which reads the file in 64KiB chunks, for libraries that use Node.js
 * streams.
 */
export async function nodeFile(file: string): Promise<ReadStream> {
  const readable = createReadStream(file);
  return readable;
}

/**
 * Read a data file as a web stream, pass it through the streams created by
 * `build`, and count the values that come out, using `getRecord` to find
 * each value's id.
 */
export async function countWeb<T>(
  file: string,
  build: (stream: ReadableStream<Uint8Array>) => AsyncIterable<T>,
  getRecord: (value: T) => WithId = (value) => value as WithId,
): Promise<Counted> {
  const { stream, close } = await webFile(file);
  try {
    let count = 0;
    let idSum = 0;
    for await (const value of build(stream)) {
      count++;
      idSum += getRecord(value).id;
    }
    return { count, idSum };
  } finally {
    await close();
  }
}

export async function countIterable<T>(
  iterable: AsyncIterable<T> | Iterable<T>,
  getRecord: (value: T) => WithId = (value) => value as WithId,
): Promise<Counted> {
  let count = 0;
  let idSum = 0;
  for await (const value of iterable) {
    count++;
    idSum += getRecord(value).id;
  }
  return { count, idSum };
}

/**
 * A web ReadableStream of `count` generated records
 */
export function webRecords(count: number): ReadableStream<DataRecord> {
  return ReadableStream.from(records(count));
}

/**
 * Consume encoded output. When verifying, the output is kept so it can be
 * checked by `parse`.
 */
export function textSink(
  verify: boolean,
  parse: (text: string) => Iterable<WithId>,
): { write: (chunk: string | Buffer) => void; result: () => CaseResult } {
  let chars = 0;
  let text = verify ? "" : undefined;
  return {
    write(chunk) {
      const string = typeof chunk === "string" ? chunk : chunk.toString();
      chars += string.length;
      if (text !== undefined) text += string;
    },
    result() {
      if (text === undefined) return { chars };
      let count = 0;
      let idSum = 0;
      for (const value of parse(text)) {
        count++;
        idSum += value.id;
      }
      return { chars, count, idSum };
    },
  };
}

export async function sinkWeb(
  readable: AsyncIterable<string>,
  verify: boolean,
  parse: (text: string) => Iterable<WithId>,
): Promise<CaseResult> {
  const sink = textSink(verify, parse);
  for await (const chunk of readable) sink.write(chunk);
  return sink.result();
}

/**
 * Consume encoded output from a Node.js stream
 */
export function sinkEvents(
  stream: EventEmitter,
  verify: boolean,
  parse: (text: string) => Iterable<WithId>,
): Promise<CaseResult> {
  const sink = textSink(verify, parse);
  return new Promise((resolve, reject) => {
    stream.on("data", (chunk: string | Buffer) => sink.write(chunk));
    stream.on("end", () => resolve(sink.result()));
    stream.on("error", reject);
  });
}

export function* parseNdjson(text: string): Generator<WithId> {
  for (const line of text.split("\n")) if (line) yield JSON.parse(line);
}

/**
 * `stream-chain/web` returns `{ readable, writable }`, but its types say they
 * could be null, so this checks they aren't, so it can be passed to
 * `pipeThrough`.
 */
export function asTransform<T>(chain: {
  readable: ReadableStream<T> | null;
  writable: WritableStream<never> | null;
}): ReadableWritablePair<T, Uint8Array | string> {
  const { readable, writable } = chain;
  if (!readable || !writable) throw new Error("Expected a transform stream");
  return {
    readable,
    writable: writable as WritableStream<Uint8Array | string>,
  };
}

/**
 * The read-the-file-only baseline, shared by every decoder suite
 */
export const readFileBaseline: BenchmarkCase = {
  library: "baseline",
  name: "Read the file through TextDecoderStream only",
  baseline: true,
  run: async ({ file }) => {
    const { stream, close } = await webFile(file);
    try {
      let chars = 0;
      for await (const chunk of stream.pipeThrough(new TextDecoderStream())) {
        chars += chunk.length;
      }
      return { chars };
    } finally {
      await close();
    }
  },
};

/**
 * Read the whole file with one of the consumer functions. The result is
 * checked after the timer has stopped.
 */
export async function consumeFile<T>(
  file: string,
  consume: (stream: ReadableStream<Uint8Array>) => Promise<T>,
  check: (result: T) => CaseResult,
): Promise<RunResult> {
  const { stream, close } = await webFile(file);
  try {
    const result = await consume(stream);
    return { finish: () => check(result) };
  } finally {
    await close();
  }
}

// The suites for the general purpose streams and utilities measure the
// overhead each stream adds per chunk, so their input is a cheaply generated
// stream of `{ id }` objects rather than realistic records. The number of
// chunks for each size is the number of records in a data file of that size,
// so results can be checked by counting the chunks and summing their ids.

/**
 * A chunk that the filter benchmarks remove. The input has one of these
 * after every `{ id }` chunk.
 */
export const NOISE = { noise: true } as const;
export type Noise = typeof NOISE;

export interface IdChunk {
  id: number;
}

// The source enqueues this many chunks per pull, so that the source itself
// is as cheap as possible.
const BATCH = 64;

/**
 * A web ReadableStream of `count` chunks `{ id }`, with ids starting at
 * `start`. With `noise`, every chunk is followed by `NOISE`.
 */
export function idStream(
  count: number,
  options?: { start?: number; noise?: false },
): ReadableStream<IdChunk>;
export function idStream(
  count: number,
  options: { start?: number; noise: true },
): ReadableStream<IdChunk | Noise>;
export function idStream(
  count: number,
  { start = 0, noise = false }: { start?: number; noise?: boolean } = {},
): ReadableStream<IdChunk | Noise> {
  let i = start;
  const end = start + count;
  return new ReadableStream<IdChunk | Noise>(
    {
      pull(controller) {
        for (let n = 0; n < BATCH && i < end; n++) {
          controller.enqueue({ id: i++ });
          if (noise) controller.enqueue(NOISE);
        }
        if (i >= end) controller.close();
      },
    },
    { highWaterMark: BATCH },
  );
}

/**
 * The same chunks as `idStream`, as a Node.js Readable stream
 */
export function idReadable(
  count: number,
  { noise = false }: { noise?: boolean } = {},
): Readable {
  return Readable.from(
    (function* () {
      for (let i = 0; i < count; i++) {
        yield { id: i };
        if (noise) yield NOISE;
      }
    })(),
  );
}

/**
 * Split `count` ids into `parts` streams of consecutive ids
 */
export function idStreams(
  count: number,
  parts: number,
): ReadableStream<IdChunk>[] {
  const streams: ReadableStream<IdChunk>[] = [];
  for (let part = 0; part < parts; part++) {
    const start = Math.floor((count * part) / parts);
    const end = Math.floor((count * (part + 1)) / parts);
    streams.push(idStream(end - start, { start }));
  }
  return streams;
}

/**
 * Count the chunks of a web stream, or any async iterable, and sum their ids
 */
export async function countIds(
  iterable: AsyncIterable<IdChunk>,
): Promise<Counted> {
  let count = 0;
  let idSum = 0;
  for await (const value of iterable) {
    count++;
    idSum += value.id;
  }
  return { count, idSum };
}

/**
 * Reading the input with no other streams, shared by most suites
 */
export const sourceBaseline: BenchmarkCase = {
  library: "baseline",
  name: "Read the source stream only",
  baseline: true,
  run: ({ count }) => countIds(idStream(count)),
};

/**
 * Reading the input with noise, for the filter suites. Every chunk is counted,
 * so this isn't checked.
 */
export const noisySourceBaseline: BenchmarkCase = {
  library: "baseline",
  name: "Read the source stream (with noise) only",
  baseline: true,
  run: async ({ count }) => {
    let chunks = 0;
    for await (const _ of idStream(count, { noise: true })) chunks++;
    return { ignored: chunks };
  },
};

export const copy = (value: IdChunk): IdChunk => ({ id: value.id });

export const isNotNoise = (value: IdChunk | Noise): value is IdChunk =>
  value !== NOISE;

export const SOURCE_COUNT = 10;
